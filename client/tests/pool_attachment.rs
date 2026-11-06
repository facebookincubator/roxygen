use std::cell::Cell;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::error::Error as StdError;
use std::fmt;
use std::future::Future;
use std::future::poll_fn;
use std::io;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::panic_any;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread::ThreadId;
use std::thread::current;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use roxygen_codec::CodecSetting;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::http::CodecProtocol;
use roxygen_core::http::HttpMethod;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::managed::RuntimeConfig;
use roxygen_runtime::managed::RuntimeHandle;
use roxygen_runtime::managed::RuntimeOwner;
use roxygen_runtime::task_group::TaskGroupError;
use roxygen_runtime::wait::CancellableBaton;
use roxygen_runtime::wait::WaitStatus;
use roxygen_runtime::worker::SubmitError;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerState;
use roxygen_server::server::ServeTransportOptions;
use roxygen_server::server::ServerConnection;
use roxygen_server::server::serve_transport;
use roxygen_session::session::LifecycleEvent;
use roxygen_session::session::LifecycleObserver;
use roxygen_session::uniplex::UniplexResponse;
use roxygen_transport::byte_stream::ByteStreamLimits;
use roxygen_transport::established::EstablishedIo;
use roxygen_transport::established::adopt_local;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::DuplexStream;
use tokio::io::ReadBuf;
use tokio::sync::Notify;
use tokio::sync::oneshot;
use tokio::sync::watch;

use crate::connector::ClientAttachOptions;
use crate::connector::ClientSession;
use crate::connector::ConnectionParams;
use crate::connector::LocalClientFuture;
use crate::connector::SessionConnector;
use crate::connector::SessionParams;
use crate::pool::AttachedConnectFailure;
use crate::pool::AttachedPoolParams;
use crate::pool::AttachedSessionFactory;
use crate::pool::AttachedSessionFactoryWithCause;
use crate::pool::GetSessionResult;
use crate::pool::HttpSessionPool;
use crate::pool::IdleSessionObserver;
use crate::pool::PoolError;
use crate::pool::PoolObserver;
use crate::pool::PoolParams;

const TIMEOUT: Duration = Duration::from_secs(10);

fn wait<Work: Future>(work: Work) -> Work::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(TIMEOUT, work)
                .await
                .expect("bounded pool scenario")
        })
}

fn run_case<Factory, Work>(case: Factory)
where
    Factory: FnOnce(WorkerContext, RuntimeHandle) -> Work + Send + 'static,
    Work: Future<Output = ()> + 'static,
{
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 2,
    })
    .unwrap();
    let runtime = owner.handle();
    let scenario_runtime = runtime.clone();
    wait(
        runtime
            .worker(0)
            .unwrap()
            .call_local(move |context| case(context, scenario_runtime))
            .unwrap(),
    )
    .unwrap();
    wait(runtime.quiesce(TIMEOUT)).expect("pool/session/transport scopes join");
    assert_eq!(
        owner
            .join()
            .expect("physical owner join outside Tokio")
            .workers_forced,
        0
    );
}

async fn until(label: &str, condition: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting until {label}"));
}

async fn pending<Work: Future + ?Sized>(mut work: Pin<&mut Work>) {
    poll_fn(|context| {
        assert!(
            work.as_mut().poll(context).is_pending(),
            "operation must remain pending at this barrier"
        );
        Poll::Ready(())
    })
    .await;
}

struct FactoryFn<Make>(Make);

impl<Make> AttachedSessionFactory for FactoryFn<Make>
where
    Make: Fn(WorkerContext, CancellationToken) -> LocalClientFuture<Result<ClientSession, HttpError>>
        + 'static,
{
    fn create(
        &self,
        context: WorkerContext,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<ClientSession, HttpError>> {
        assert_eq!(timeout, TIMEOUT);
        (self.0)(context, cancellation)
    }
}

fn factory<Make>(make: Make) -> Rc<dyn AttachedSessionFactory>
where
    Make: Fn(WorkerContext, CancellationToken) -> LocalClientFuture<Result<ClientSession, HttpError>>
        + 'static,
{
    Rc::new(FactoryFn(make))
}

struct TimeoutFactory<Make>(Make);

impl<Make> AttachedSessionFactory for TimeoutFactory<Make>
where
    Make: Fn(
            WorkerContext,
            Duration,
            CancellationToken,
        ) -> LocalClientFuture<Result<ClientSession, HttpError>>
        + 'static,
{
    fn create(
        &self,
        context: WorkerContext,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<ClientSession, HttpError>> {
        (self.0)(context, timeout, cancellation)
    }
}

impl<Make> SessionConnector for TimeoutFactory<Make>
where
    Self: AttachedSessionFactory,
{
    fn connect(
        &self,
        context: WorkerContext,
        _address: SocketAddress,
        timeout: Duration,
        _connection: ConnectionParams,
        _session: SessionParams,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<ClientSession, HttpError>> {
        self.create(context, timeout, cancellation)
    }
}

fn timeout_factory<Make>(make: Make) -> Rc<TimeoutFactory<Make>>
where
    Make: Fn(
            WorkerContext,
            Duration,
            CancellationToken,
        ) -> LocalClientFuture<Result<ClientSession, HttpError>>
        + 'static,
{
    Rc::new(TimeoutFactory(make))
}

struct CauseFactory<Make>(Make);

impl<Make> AttachedSessionFactoryWithCause for CauseFactory<Make>
where
    Make: Fn(
            WorkerContext,
            Duration,
            CancellationToken,
        ) -> LocalClientFuture<Result<ClientSession, AttachedConnectFailure>>
        + 'static,
{
    fn create(
        &self,
        context: WorkerContext,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<ClientSession, AttachedConnectFailure>> {
        (self.0)(context, timeout, cancellation)
    }
}

fn cause_factory<Make>(make: Make) -> Rc<dyn AttachedSessionFactoryWithCause>
where
    Make: Fn(
            WorkerContext,
            Duration,
            CancellationToken,
        ) -> LocalClientFuture<Result<ClientSession, AttachedConnectFailure>>
        + 'static,
{
    Rc::new(CauseFactory(make))
}

fn cause_pool(
    context: &WorkerContext,
    options: AttachedPoolParams,
    make: Rc<dyn AttachedSessionFactoryWithCause>,
) -> Rc<HttpSessionPool> {
    Rc::new(
        HttpSessionPool::try_with_attached_factory_with_cause(
            context.clone(),
            options,
            TransportSecurity::Plaintext,
            make,
        )
        .unwrap(),
    )
}

thread_local! {
    static CAUSE_DROP_HOOK: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
}

#[derive(Clone, Default)]
struct CauseProbe {
    identity: Arc<()>,
    drops: Arc<AtomicUsize>,
    diagnostics: Arc<AtomicUsize>,
    primary: Arc<AtomicUsize>,
    secondary: Arc<AtomicUsize>,
    dropped: Arc<Notify>,
}

impl CauseProbe {
    fn failure(&self, panic_on_drop: bool) -> AttachedConnectFailure {
        AttachedConnectFailure::with_cause(
            HttpError::with_message(HttpErrorCode::CONNECT_ERROR, b"attached connect failed"),
            Arc::new(OriginalConnectCause {
                probe: self.clone(),
                nested: io::Error::from(io::ErrorKind::TimedOut),
                panic_on_drop,
            }),
        )
    }

    async fn wait_dropped(&self) {
        loop {
            let changed = self.dropped.notified();
            if self.drops.load(Ordering::SeqCst) != 0 {
                return;
            }
            changed.await;
        }
    }
}

struct OriginalConnectCause {
    probe: CauseProbe,
    nested: io::Error,
    panic_on_drop: bool,
}

impl fmt::Display for OriginalConnectCause {
    fn fmt(&self, _formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.probe.diagnostics.fetch_add(1, Ordering::SeqCst);
        panic!("external diagnostic panicked");
    }
}

impl fmt::Debug for OriginalConnectCause {
    fn fmt(&self, _formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.probe.diagnostics.fetch_add(1, Ordering::SeqCst);
        panic!("external diagnostic panicked");
    }
}

impl StdError for OriginalConnectCause {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.nested)
    }
}

impl Drop for OriginalConnectCause {
    fn drop(&mut self) {
        let hook = CAUSE_DROP_HOOK.with(|slot| slot.borrow_mut().take());
        if let Some(hook) = hook {
            hook();
        }
        self.probe.drops.fetch_add(1, Ordering::SeqCst);
        self.probe.dropped.notify_one();
        if self.panic_on_drop {
            panic_any(DroppingPanicPayload {
                primary: Arc::clone(&self.probe.primary),
                secondary: Arc::clone(&self.probe.secondary),
            });
        }
    }
}

fn acquisition_error(result: Result<GetSessionResult, PoolError>) -> PoolError {
    match result {
        Ok(_) => panic!("scripted connection failure must not acquire a session"),
        Err(error) => error,
    }
}

fn assert_cause<'a>(error: &'a PoolError, probe: &CauseProbe) -> &'a AttachedConnectFailure {
    let PoolError::AttachedConnectFailed { source } = error else {
        panic!("expected the selected external connector cause");
    };
    assert_eq!(source.http_error().code(), HttpErrorCode::CONNECT_ERROR);
    assert_eq!(source.http_error().message(), b"attached connect failed");
    let chained = error
        .source()
        .unwrap()
        .downcast_ref::<AttachedConnectFailure>()
        .unwrap();
    assert!(std::ptr::eq(chained, source));
    let original = chained
        .source()
        .unwrap()
        .downcast_ref::<OriginalConnectCause>()
        .unwrap();
    assert!(Arc::ptr_eq(&original.probe.identity, &probe.identity));
    assert!(std::ptr::eq(
        original,
        source
            .external_cause()
            .unwrap()
            .downcast_ref::<OriginalConnectCause>()
            .unwrap(),
    ));
    assert_eq!(
        original
            .source()
            .unwrap()
            .downcast_ref::<io::Error>()
            .unwrap()
            .kind(),
        io::ErrorKind::TimedOut,
    );
    source
}

struct CauseFuture {
    failure: Option<AttachedConnectFailure>,
    entered: Option<oneshot::Sender<()>>,
    release: Option<oneshot::Receiver<()>>,
    destroyed: Option<oneshot::Sender<()>>,
    owner: ThreadId,
    panic_on_drop: bool,
}

impl Future for CauseFuture {
    type Output = Result<ClientSession, AttachedConnectFailure>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        assert_eq!(current().id(), self.owner);
        if let Some(entered) = self.entered.take() {
            entered.send(()).unwrap();
        }
        if let Some(release) = self.release.as_mut() {
            match Pin::new(release).poll(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result.unwrap(),
            }
        }
        self.release = None;
        Poll::Ready(Err(self.failure.take().unwrap()))
    }
}

impl Drop for CauseFuture {
    fn drop(&mut self) {
        assert_eq!(current().id(), self.owner);
        if let Some(destroyed) = self.destroyed.take() {
            let _ = destroyed.send(());
        }
        assert!(
            !self.panic_on_drop,
            "completed cause future destructor panicked"
        );
    }
}

struct CauseGate {
    entered: oneshot::Receiver<()>,
    release: oneshot::Sender<()>,
    destroyed: oneshot::Receiver<()>,
}

fn gated_cause(failure: AttachedConnectFailure) -> (CauseFuture, CauseGate) {
    let (entered, entry_seen) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let (destroyed, destruction_seen) = oneshot::channel();
    (
        CauseFuture {
            failure: Some(failure),
            entered: Some(entered),
            release: Some(released),
            destroyed: Some(destroyed),
            owner: current().id(),
            panic_on_drop: false,
        },
        CauseGate {
            entered: entry_seen,
            release,
            destroyed: destruction_seen,
        },
    )
}

#[derive(Default)]
struct CausePending {
    count: Cell<i32>,
    changed: Notify,
}

impl PoolObserver for CausePending {
    fn on_pending_connections_changed(&self, delta: i32) {
        self.count.set(self.count.get() + delta);
        self.changed.notify_one();
    }
}

impl CausePending {
    async fn wait_for(&self, expected: i32) {
        loop {
            let changed = self.changed.notified();
            if self.count.get() == expected {
                return;
            }
            changed.await;
        }
    }
}

#[test]
fn attached_failure_immediate_preserves_original_source_chain() {
    fn assert_send_sync<Value: Send + Sync + 'static>() {}
    assert_send_sync::<AttachedConnectFailure>();
    assert_send_sync::<PoolError>();
    for panic_on_drop in [false, true] {
        run_case(move |context, _runtime| async move {
            let probe = CauseProbe::default();
            let failure = probe.failure(false);
            let original = Arc::downgrade(failure.external_cause().unwrap());
            let (destroyed, destruction_seen) = oneshot::channel();
            let future = RefCell::new(Some(CauseFuture {
                failure: Some(failure),
                entered: None,
                release: None,
                destroyed: Some(destroyed),
                owner: current().id(),
                panic_on_drop,
            }));
            let pool = cause_pool(
                &context,
                params(1),
                cause_factory(move |_, timeout, _| {
                    assert_eq!(timeout, TIMEOUT);
                    Box::pin(future.borrow_mut().take().unwrap())
                }),
            );
            let error = acquisition_error(
                pool.get_session_with_reservation(CancellationToken::never())
                    .await,
            );
            destruction_seen.await.unwrap();
            let failure = assert_cause(&error, &probe);
            assert_eq!(error.to_string(), "Connect Failed: attached connect failed");
            let _ = format!("{error:?}");
            assert_eq!(probe.diagnostics.load(Ordering::SeqCst), 0);
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    format!("{}", failure.source().unwrap())
                }))
                .is_err()
            );
            assert_eq!(probe.diagnostics.load(Ordering::SeqCst), 1);
            let (native, external) = failure.clone().into_parts();
            assert_eq!(native.code(), HttpErrorCode::CONNECT_ERROR);
            assert!(Arc::ptr_eq(
                &original.upgrade().unwrap(),
                external.as_ref().unwrap()
            ));
            drop(external);
            drop(error);
            assert_eq!(probe.drops.load(Ordering::SeqCst), 1);
            assert!(original.upgrade().is_none());
            let joined = pool.drain_and_join().await;
            if panic_on_drop {
                assert!(matches!(joined, Err(PoolError::CleanupPanicked)));
            } else {
                joined.unwrap();
            }
            assert_eq!(pool.task_count_for_test(), 0);
        });
    }
}

#[test]
fn attached_failure_retry_selects_exact_final_cause() {
    for native_last in [false, true] {
        run_case(move |context, _runtime| async move {
            let first = CauseProbe::default();
            let second = CauseProbe::default();
            let last = CauseProbe::default();
            let final_error = if native_last {
                AttachedConnectFailure::new(HttpError::with_message(
                    HttpErrorCode::READ_TIMEOUT,
                    b"native final failure",
                ))
            } else {
                last.failure(false)
            };
            let plans = RefCell::new(VecDeque::from([
                first.failure(true),
                second.failure(false),
                final_error,
            ]));
            let calls = Rc::new(Cell::new(0));
            let factory_calls = Rc::clone(&calls);
            let mut options = params(1);
            options.max_connection_attempts = 3;
            let pool = cause_pool(
                &context,
                options,
                cause_factory(move |_, _, _| {
                    factory_calls.set(factory_calls.get() + 1);
                    Box::pin(std::future::ready(Err(plans
                        .borrow_mut()
                        .pop_front()
                        .unwrap())))
                }),
            );
            let weak = Rc::downgrade(&pool);
            let reentered = Rc::new(Cell::new(false));
            let drop_reentered = Rc::clone(&reentered);
            CAUSE_DROP_HOOK.with(|slot| {
                *slot.borrow_mut() = Some(Box::new(move || {
                    weak.upgrade()
                        .unwrap()
                        .set_max_connection_attempts(3)
                        .unwrap();
                    drop_reentered.set(true);
                }));
            });
            let error = acquisition_error(
                pool.get_session_with_reservation(CancellationToken::never())
                    .await,
            );
            assert!(
                reentered.get(),
                "superseded cause disposal is outside pool borrows"
            );
            assert_eq!(calls.get(), 3);
            assert_eq!(first.drops.load(Ordering::SeqCst), 1);
            assert_eq!(first.primary.load(Ordering::SeqCst), 1);
            assert_eq!(first.secondary.load(Ordering::SeqCst), 1);
            assert_eq!(second.drops.load(Ordering::SeqCst), 1);
            if native_last {
                let PoolError::ConnectFailed { source } = &error else {
                    panic!("native final failure must not inherit an earlier cause");
                };
                assert_eq!(source.code(), HttpErrorCode::READ_TIMEOUT);
                assert_eq!(source.message(), b"native final failure");
                assert!(source.source().is_none());
            } else {
                assert_cause(&error, &last);
            }
            drop(error);
            assert_eq!(last.drops.load(Ordering::SeqCst), usize::from(!native_last));
            assert_eq!(first.diagnostics.load(Ordering::SeqCst), 0);
            assert!(!pool.full());
            pool.drain_and_join().await.unwrap();
        });
    }
}

#[test]
fn attached_failure_fanout_shares_selected_cause() {
    run_case(|context, _runtime| async move {
        let probe = CauseProbe::default();
        let (future, gate) = gated_cause(probe.failure(false));
        let future = RefCell::new(Some(future));
        let pool = cause_pool(
            &context,
            params(1),
            cause_factory(move |_, _, _| Box::pin(future.borrow_mut().take().unwrap())),
        );
        let mut first = pool.get_session_with_reservation(CancellationToken::never());
        pending(first.as_mut()).await;
        gate.entered.await.unwrap();
        let mut second = pool.get_session_with_reservation(CancellationToken::never());
        let mut third = pool.get_session_with_reservation(CancellationToken::never());
        pending(second.as_mut()).await;
        pending(third.as_mut()).await;
        gate.release.send(()).unwrap();
        let errors = [
            acquisition_error(first.await),
            acquisition_error(second.await),
            acquisition_error(third.await),
        ];
        gate.destroyed.await.unwrap();
        for error in &errors {
            let failure = assert_cause(error, &probe);
            assert!(Arc::ptr_eq(
                failure.external_cause().unwrap(),
                assert_cause(&errors[0], &probe).external_cause().unwrap(),
            ));
        }
        pool.drain_and_join().await.unwrap();
        drop(pool);
        assert_eq!(probe.drops.load(Ordering::SeqCst), 0);
        drop(errors);
        assert_eq!(probe.drops.load(Ordering::SeqCst), 1);
    });
}

#[test]
fn attached_failure_parallel_tasks_follow_native_selection() {
    for reverse in [false, true] {
        run_case(move |context, _runtime| async move {
            let first_probe = CauseProbe::default();
            let second_probe = CauseProbe::default();
            let replacement_probe = CauseProbe::default();
            let (first, first_gate) = gated_cause(first_probe.failure(false));
            let (second, second_gate) = gated_cause(second_probe.failure(false));
            let replacement = replacement_probe.failure(false);
            let plans = RefCell::new(VecDeque::from([
                Box::pin(first) as LocalClientFuture<Result<ClientSession, AttachedConnectFailure>>,
                Box::pin(second),
                Box::pin(std::future::ready(Err(replacement))),
            ]));
            let calls = Rc::new(Cell::new(0));
            let factory_calls = Rc::clone(&calls);
            let pool = cause_pool(
                &context,
                params(2),
                cause_factory(move |_, _, _| {
                    factory_calls.set(factory_calls.get() + 1);
                    plans.borrow_mut().pop_front().unwrap()
                }),
            );
            let progress = Rc::new(CausePending::default());
            pool.set_observer(Some(progress.clone())).unwrap();
            let mut first_waiter = pool.get_session_with_reservation(CancellationToken::never());
            pending(first_waiter.as_mut()).await;
            first_gate.entered.await.unwrap();
            let mut second_waiter = pool.get_session_with_reservation(CancellationToken::never());
            pending(second_waiter.as_mut()).await;
            second_gate.entered.await.unwrap();
            let mut third_waiter = pool.get_session_with_reservation(CancellationToken::never());
            pending(third_waiter.as_mut()).await;
            let (early_release, late_release, early_probe, late_probe) = if reverse {
                (
                    second_gate.release,
                    first_gate.release,
                    &second_probe,
                    &first_probe,
                )
            } else {
                (
                    first_gate.release,
                    second_gate.release,
                    &first_probe,
                    &second_probe,
                )
            };
            early_release.send(()).unwrap();
            progress.wait_for(1).await;
            assert_eq!(early_probe.drops.load(Ordering::SeqCst), 1);
            pending(second_waiter.as_mut()).await;
            pending(third_waiter.as_mut()).await;
            late_release.send(()).unwrap();
            progress.wait_for(0).await;
            let second_error = acquisition_error(second_waiter.await);
            let third_error = acquisition_error(third_waiter.await);
            assert_cause(&second_error, late_probe);
            assert_cause(&third_error, late_probe);
            assert_eq!(calls.get(), 2);
            let replacement_error = acquisition_error(first_waiter.await);
            assert_cause(&replacement_error, &replacement_probe);
            assert_eq!(calls.get(), 3);
            first_gate.destroyed.await.unwrap();
            second_gate.destroyed.await.unwrap();
            drop((second_error, third_error, replacement_error));
            assert_eq!(late_probe.drops.load(Ordering::SeqCst), 1);
            assert_eq!(replacement_probe.drops.load(Ordering::SeqCst), 1);
            pool.drain_and_join().await.unwrap();
        });
    }
    run_case(|context, _runtime| async move {
        let mut pools = Vec::new();
        let mut acquisitions = Vec::new();
        let mut gates = Vec::new();
        let mut probes = Vec::new();
        for _ in 0..2 {
            let probe = CauseProbe::default();
            let (future, gate) = gated_cause(probe.failure(false));
            let future = RefCell::new(Some(future));
            let pool = cause_pool(
                &context,
                params(1),
                cause_factory(move |_, _, _| Box::pin(future.borrow_mut().take().unwrap())),
            );
            let mut acquired = pool.get_session_with_reservation(CancellationToken::never());
            pending(acquired.as_mut()).await;
            gate.entered.await.unwrap();
            pools.push(pool);
            acquisitions.push(acquired);
            gates.push((gate.release, gate.destroyed));
            probes.push(probe);
        }
        for (release, destroyed) in gates.into_iter().rev() {
            release.send(()).unwrap();
            destroyed.await.unwrap();
        }
        for (acquired, probe) in acquisitions.into_iter().zip(&probes) {
            let error = acquisition_error(acquired.await);
            assert_cause(&error, probe);
            drop(error);
            assert_eq!(probe.drops.load(Ordering::SeqCst), 1);
        }
        for pool in pools {
            pool.drain_and_join().await.unwrap();
        }
    });
}

#[test]
fn attached_failure_abandoned_waiter_and_late_completion() {
    for abandonment in 0..3 {
        for replacement in [false, true] {
            run_case(move |context, _runtime| async move {
                let original = CauseProbe::default();
                let fresh = CauseProbe::default();
                let (future, gate) = gated_cause(original.failure(false));
                let plans = RefCell::new(VecDeque::from([
                    Box::pin(future)
                        as LocalClientFuture<Result<ClientSession, AttachedConnectFailure>>,
                    Box::pin(std::future::ready(Err(fresh.failure(false)))),
                ]));
                let calls = Rc::new(Cell::new(0));
                let factory_calls = Rc::clone(&calls);
                let mut options = params(1);
                if abandonment == 2 {
                    options.connect_timeout = Duration::from_millis(100);
                }
                let pool = cause_pool(
                    &context,
                    options,
                    cause_factory(move |_, _, token| {
                        factory_calls.set(factory_calls.get() + 1);
                        let future = plans.borrow_mut().pop_front().unwrap();
                        Box::pin(async move {
                            let result = future.await;
                            assert!(!token.is_cancellation_requested());
                            result
                        })
                    }),
                );
                let progress = Rc::new(CausePending::default());
                pool.set_observer(Some(progress.clone())).unwrap();
                let cancellation = CancellationSource::new();
                let mut abandoned = pool.get_session_with_reservation(cancellation.token());
                pending(abandoned.as_mut()).await;
                gate.entered.await.unwrap();
                match abandonment {
                    0 => drop(abandoned),
                    1 => {
                        cancellation.request_cancellation();
                        assert!(matches!(abandoned.await, Err(PoolError::Cancelled)));
                    }
                    _ => assert!(matches!(abandoned.await, Err(PoolError::Timeout))),
                }
                let mut next = replacement
                    .then(|| pool.get_session_with_reservation(CancellationToken::never()));
                if let Some(next) = next.as_mut() {
                    pending(next.as_mut()).await;
                }
                assert_eq!(calls.get(), 1);
                gate.release.send(()).unwrap();
                if let Some(next) = next {
                    let error = acquisition_error(next.await);
                    assert_cause(&error, &original);
                    drop(error);
                }
                progress.wait_for(0).await;
                gate.destroyed.await.unwrap();
                assert_eq!(original.drops.load(Ordering::SeqCst), 1);
                assert_eq!(
                    calls.get(),
                    1,
                    "no waiter means no additional factory retry"
                );
                let error = acquisition_error(
                    pool.get_session_with_reservation(CancellationToken::never())
                        .await,
                );
                assert_cause(&error, &fresh);
                assert_eq!(calls.get(), 2);
                drop(error);
                assert_eq!(fresh.drops.load(Ordering::SeqCst), 1);
                pool.drain_and_join().await.unwrap();
                assert_eq!(pool.task_count_for_test(), 0);
            });
        }
    }
}

#[test]
fn attached_failure_drain_joins_late_error() {
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let producer = fixture.clone();
        let first_probe = CauseProbe::default();
        let second_probe = CauseProbe::default();
        let mut plans = VecDeque::new();
        let mut entries = Vec::new();
        let mut cancellations = Vec::new();
        let mut releases = VecDeque::new();
        for (probe, panic_on_drop) in [(&first_probe, true), (&second_probe, false)] {
            let (entered, entry_seen) = oneshot::channel();
            let (cancelled, cancellation_seen) = oneshot::channel();
            let (release, released) = oneshot::channel();
            plans.push_back((probe.failure(panic_on_drop), entered, cancelled, released));
            entries.push(entry_seen);
            cancellations.push(cancellation_seen);
            releases.push_back(release);
        }
        let plans = RefCell::new(plans);
        let destroyed = Rc::new(Cell::new(0));
        let factory_destroyed = Rc::clone(&destroyed);
        let make = cause_factory(move |context, _, cancellation| {
            producer.calls.set(producer.calls.get() + 1);
            if producer.calls.get() == 1 {
                let session = producer.attach(&context, CodecProtocol::Http11);
                return Box::pin(std::future::ready(Ok(session)));
            }
            let (failure, entered, cancelled, release) = plans.borrow_mut().pop_front().unwrap();
            let probe = FactoryJoinDropProbe {
                owner: current().id(),
                drops: Rc::clone(&factory_destroyed),
            };
            Box::pin(async move {
                let _probe = probe;
                entered.send(()).unwrap();
                let baton = CancellableBaton::new();
                assert_eq!(baton.wait(cancellation).await, WaitStatus::Cancelled);
                cancelled.send(()).unwrap();
                release.await.unwrap();
                Err(failure)
            })
        });
        let pool = cause_pool(&context, params(3), make);
        let lease = pool
            .get_session_with_reservation(CancellationToken::never())
            .await
            .unwrap();
        let mut acquisitions = Vec::new();
        for entry in entries {
            let mut acquisition = pool.get_session_with_reservation(CancellationToken::never());
            pending(acquisition.as_mut()).await;
            entry.await.unwrap();
            acquisitions.push(acquisition);
        }
        let weak = Rc::downgrade(&pool);
        let reentered = Rc::new(Cell::new(false));
        let drop_reentered = Rc::clone(&reentered);
        CAUSE_DROP_HOOK.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                let pool = weak.upgrade().unwrap();
                assert!(!pool.drain());
                let _ = pool.describe();
                drop_reentered.set(true);
            }));
        });
        let mut joined = pool.drain_and_join();
        for acquisition in acquisitions {
            assert!(matches!(acquisition.await, Err(PoolError::Draining)));
        }
        for cancellation in cancellations {
            cancellation.await.unwrap();
        }
        pending(joined.as_mut()).await;
        assert_eq!(destroyed.get(), 0);
        releases.pop_front().unwrap().send(()).unwrap();
        first_probe.wait_dropped().await;
        assert!(
            reentered.get(),
            "late cause disposal must not hold a pool borrow"
        );
        assert_eq!(first_probe.primary.load(Ordering::SeqCst), 1);
        assert_eq!(first_probe.secondary.load(Ordering::SeqCst), 1);
        assert_eq!(destroyed.get(), 1);
        assert_eq!(second_probe.drops.load(Ordering::SeqCst), 0);
        pending(joined.as_mut()).await;
        assert!(!lease.session.is_finished());
        releases.pop_front().unwrap().send(()).unwrap();
        second_probe.wait_dropped().await;
        pending(joined.as_mut()).await;
        assert!(
            !lease.session.is_finished(),
            "join still owns the live reservation"
        );
        drop(lease.reservation);
        joined.await.unwrap();
        assert!(lease.session.is_finished());
        assert_eq!(destroyed.get(), 2);
        assert_eq!(fixture.calls.get(), 3);
        assert_eq!(first_probe.diagnostics.load(Ordering::SeqCst), 0);
        assert_eq!(second_probe.diagnostics.load(Ordering::SeqCst), 0);
        assert!(!pool.full());
        assert_eq!(pool.task_count_for_test(), 0);
    });
}

#[test]
fn attached_failure_clones_outlive_pool_without_retention() {
    run_case(|context, _runtime| async move {
        let probe = CauseProbe::default();
        let failure = probe.failure(true);
        let cause_weak = Arc::downgrade(failure.external_cause().unwrap());
        let failure = RefCell::new(Some(failure));
        let pool = cause_pool(
            &context,
            params(1),
            cause_factory(move |_, _, _| {
                Box::pin(std::future::ready(Err(failure
                    .borrow_mut()
                    .take()
                    .unwrap())))
            }),
        );
        let pool_weak = Rc::downgrade(&pool);
        let error = acquisition_error(
            pool.get_session_with_reservation(CancellationToken::never())
                .await,
        );
        let retained = error.clone();
        pool.drain_and_join().await.unwrap();
        assert_eq!(pool.task_count_for_test(), 0);
        drop(pool);
        assert!(pool_weak.upgrade().is_none());
        assert_cause(&error, &probe);
        assert_cause(&retained, &probe);
        drop(error);
        assert!(cause_weak.upgrade().is_some());
        assert_eq!(probe.drops.load(Ordering::SeqCst), 0);
        let _ = format!("{retained}: {retained:?}");
        assert_eq!(probe.diagnostics.load(Ordering::SeqCst), 0);
        drop(retained);
        assert!(cause_weak.upgrade().is_none());
        assert_eq!(probe.drops.load(Ordering::SeqCst), 1);
        assert_eq!(probe.primary.load(Ordering::SeqCst), 1);
        assert_eq!(probe.secondary.load(Ordering::SeqCst), 1);
    });
}

struct CountedEcho(Rc<Cell<usize>>);

impl HttpHandler for CountedEcho {
    fn handle_request(
        &self,
        context: HandlerSessionContext,
        request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        self.0.set(self.0.get() + 1);
        Echo.handle_request(context, request)
    }
}

#[test]
fn attached_failure_creation_retry_does_not_replay_request() {
    run_case(|context, _runtime| async move {
        let probe = CauseProbe::default();
        let failure = RefCell::new(Some(probe.failure(false)));
        let requests = Rc::new(Cell::new(0));
        let handler_requests = Rc::clone(&requests);
        let calls = Rc::new(Cell::new(0));
        let factory_calls = Rc::clone(&calls);
        let peer = Rc::new(RefCell::new(None));
        let server_peer = Rc::clone(&peer);
        let make = cause_factory(move |context, _, _| {
            factory_calls.set(factory_calls.get() + 1);
            if let Some(failure) = failure.borrow_mut().take() {
                return Box::pin(std::future::ready(Err(failure)));
            }
            assert_eq!(factory_calls.get(), 2);
            let (client, server) = tokio::io::duplex(16 * 1024);
            let server = adopt_local(
                &context,
                EstablishedIo::new(
                    server,
                    TransportEndpoint::Unknown,
                    TransportEndpoint::Unknown,
                    None,
                ),
                limits(),
            )
            .unwrap();
            let connection = serve_transport(
                &context,
                server,
                Rc::new(CountedEcho(Rc::clone(&handler_requests))),
                ServeTransportOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext),
            )
            .unwrap();
            server_peer.borrow_mut().replace(connection);
            let client = adopt_local(
                &context,
                EstablishedIo::new(
                    client,
                    TransportEndpoint::Unknown,
                    TransportEndpoint::Unknown,
                    None,
                ),
                limits(),
            )
            .unwrap();
            let session = ClientSession::attach_transport(
                &context,
                client,
                ClientAttachOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext),
            )
            .unwrap();
            Box::pin(std::future::ready(Ok(session)))
        });
        let mut options = params(1);
        options.max_connection_attempts = 2;
        let pool = cause_pool(&context, options, make);
        let acquired = pool
            .get_session_with_reservation(CancellationToken::never())
            .await
            .unwrap();
        assert_eq!(calls.get(), 2);
        assert_eq!(
            requests.get(),
            0,
            "factory retries cannot send application requests"
        );
        assert_eq!(probe.drops.load(Ordering::SeqCst), 1);
        let session = exchange(acquired).await;
        assert_eq!(requests.get(), 1);
        assert_eq!(calls.get(), 2);
        pool.drain_and_join().await.unwrap();
        assert!(session.is_finished());
        let peer = peer.borrow_mut().take().unwrap();
        peer.wait_closed().await.unwrap();
        assert_eq!(requests.get(), 1);
        assert_eq!(pool.task_count_for_test(), 0);
    });
}

fn params(capacity: u32) -> AttachedPoolParams {
    AttachedPoolParams {
        max_connections: capacity,
        max_connection_attempts: 1,
        connect_timeout: TIMEOUT,
        max_age: Duration::ZERO,
        max_waiters: 1,
        max_retiring: None,
        pending_connect_credit: 1,
    }
}

/// The `adapters.connect` accounting: retiring sessions leave `max_connections`.
fn retiring_params(capacity: u32) -> AttachedPoolParams {
    AttachedPoolParams {
        max_retiring: Some(capacity),
        pending_connect_credit: 2,
        ..params(capacity)
    }
}

fn limits() -> ByteStreamLimits {
    ByteStreamLimits::new(4096, 4096, 8).unwrap()
}

struct Echo;

impl HttpHandler for Echo {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(async move {
            let head = request.read_header_event().await?;
            let mut body = BufQueue::new(128);
            if !head.eom() {
                loop {
                    let event = request.read_body_event(128).await?;
                    if let Some(bytes) = event.as_body() {
                        body.append(&mut bytes.clone_shared()).unwrap();
                    }
                    if event.eom() {
                        break;
                    }
                }
            }
            Ok(HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_response(200, Some(body)).unwrap(),
            )))
        })
    }
}

struct IdleNotice(Rc<Notify>);

impl IdleSessionObserver for IdleNotice {
    fn on_idle_sessions_changed(&self, _pool: &HttpSessionPool) {
        self.0.notify_one();
    }
}

#[derive(Clone, Default)]
struct Fixture {
    peers: Rc<RefCell<Vec<ServerConnection>>>,
    calls: Rc<Cell<u32>>,
    idle: Rc<Notify>,
}

impl Fixture {
    fn attach(&self, context: &WorkerContext, protocol: CodecProtocol) -> ClientSession {
        let (client, server) = tokio::io::duplex(16 * 1024);
        self.attach_io(context, protocol, (client, server))
    }

    fn attach_io<Stream: AsyncRead + AsyncWrite + Unpin + 'static>(
        &self,
        context: &WorkerContext,
        protocol: CodecProtocol,
        streams: (Stream, DuplexStream),
    ) -> ClientSession {
        self.attach_configured_io(
            context,
            ClientAttachOptions::new(protocol, TransportSecurity::Plaintext),
            streams,
        )
    }

    fn attach_options(
        &self,
        context: &WorkerContext,
        options: ClientAttachOptions,
    ) -> ClientSession {
        self.attach_configured_io(context, options, tokio::io::duplex(16 * 1024))
    }

    fn attach_configured_io<Stream: AsyncRead + AsyncWrite + Unpin + 'static>(
        &self,
        context: &WorkerContext,
        options: ClientAttachOptions,
        streams: (Stream, DuplexStream),
    ) -> ClientSession {
        let served = ServeTransportOptions::new(options.protocol, TransportSecurity::Plaintext);
        self.attach_served_io(context, options, served, streams)
    }

    fn attach_served(
        &self,
        context: &WorkerContext,
        options: ClientAttachOptions,
        served: ServeTransportOptions,
    ) -> ClientSession {
        self.attach_served_io(context, options, served, tokio::io::duplex(16 * 1024))
    }

    fn attach_served_io<Stream: AsyncRead + AsyncWrite + Unpin + 'static>(
        &self,
        context: &WorkerContext,
        options: ClientAttachOptions,
        served: ServeTransportOptions,
        streams: (Stream, DuplexStream),
    ) -> ClientSession {
        let (client, server) = streams;
        let server = adopt_local(
            context,
            EstablishedIo::new(
                server,
                TransportEndpoint::Unknown,
                TransportEndpoint::Unknown,
                None,
            ),
            limits(),
        )
        .unwrap();
        self.peers
            .borrow_mut()
            .push(serve_transport(context, server, Rc::new(Echo), served).unwrap());
        let client = adopt_local(
            context,
            EstablishedIo::new(
                client,
                TransportEndpoint::Unknown,
                TransportEndpoint::Unknown,
                None,
            ),
            limits(),
        )
        .unwrap();
        ClientSession::attach_transport(context, client, options).unwrap()
    }

    fn factory(&self, protocol: CodecProtocol) -> Rc<dyn AttachedSessionFactory> {
        let fixture = self.clone();
        factory(move |context, _cancellation| {
            fixture.calls.set(fixture.calls.get() + 1);
            let session = fixture.attach(&context, protocol);
            Box::pin(async move { Ok(session) })
        })
    }

    /// Attaches sessions that admit at most `streams` concurrent requests.
    fn limited_factory(
        &self,
        protocol: CodecProtocol,
        streams: u32,
    ) -> Rc<dyn AttachedSessionFactory> {
        let fixture = self.clone();
        factory(move |context, _cancellation| {
            fixture.calls.set(fixture.calls.get() + 1);
            let mut options = ClientAttachOptions::new(protocol, TransportSecurity::Plaintext);
            options.session.max_concurrent_outgoing_streams = Some(streams);
            let session = fixture.attach_options(&context, options);
            Box::pin(async move { Ok(session) })
        })
    }

    /// Attaches sessions whose handoff waits until `release` holds true.
    fn gated_factory(
        &self,
        protocol: CodecProtocol,
        release: &watch::Sender<bool>,
    ) -> Rc<dyn AttachedSessionFactory> {
        let fixture = self.clone();
        let release = release.clone();
        factory(move |context, _cancellation| {
            fixture.calls.set(fixture.calls.get() + 1);
            let session = fixture.attach(&context, protocol);
            let mut released = release.subscribe();
            Box::pin(async move {
                released.wait_for(|released| *released).await.unwrap();
                Ok(session)
            })
        })
    }

    /// Attaches one-stream HTTP/2 sessions; after the first `immediate`
    /// calls, each handoff waits until `release` holds true.
    fn one_stream_gated_factory(
        &self,
        immediate: u32,
        release: &watch::Sender<bool>,
    ) -> Rc<dyn AttachedSessionFactory> {
        let fixture = self.clone();
        let release = release.clone();
        factory(move |context, _cancellation| {
            fixture.calls.set(fixture.calls.get() + 1);
            let mut options =
                ClientAttachOptions::new(CodecProtocol::Http2, TransportSecurity::Plaintext);
            options.session.max_concurrent_outgoing_streams = Some(1);
            let session = fixture.attach_options(&context, options);
            let mut released = (fixture.calls.get() > immediate).then(|| release.subscribe());
            Box::pin(async move {
                if let Some(released) = released.as_mut() {
                    released.wait_for(|released| *released).await.unwrap();
                }
                Ok(session)
            })
        })
    }

    /// Scripts each call: `None` attaches an HTTP/2 session admitting
    /// `streams` requests, and `Some(release)` fails with `CONNECT_ERROR` once
    /// `release` fires.
    fn scripted_factory(
        &self,
        streams: u32,
        plans: Vec<Option<oneshot::Receiver<()>>>,
    ) -> Rc<dyn AttachedSessionFactory> {
        let fixture = self.clone();
        let plans = RefCell::new(VecDeque::from(plans));
        factory(move |context, _cancellation| {
            fixture.calls.set(fixture.calls.get() + 1);
            match plans.borrow_mut().pop_front().expect("a scripted connect") {
                None => {
                    let mut options = ClientAttachOptions::new(
                        CodecProtocol::Http2,
                        TransportSecurity::Plaintext,
                    );
                    options.session.max_concurrent_outgoing_streams = Some(streams);
                    let session = fixture.attach_options(&context, options);
                    Box::pin(async move { Ok(session) })
                }
                Some(release) => Box::pin(async move {
                    release.await.unwrap();
                    Err(HttpError::new(HttpErrorCode::CONNECT_ERROR))
                }),
            }
        })
    }

    /// Attaches an HTTP/2 session whose peer advertises `streams` concurrent
    /// streams; the future resolves once those SETTINGS arrive.
    fn attach_peer_limited(
        &self,
        context: &WorkerContext,
        streams: u32,
    ) -> LocalClientFuture<ClientSession> {
        let settings = Rc::new(CancellableBaton::new());
        let mut options =
            ClientAttachOptions::new(CodecProtocol::Http2, TransportSecurity::Plaintext);
        options.session.lifecycle_observer = Some(Rc::new(PeerSettingsSeen(Rc::clone(&settings))));
        let mut served =
            ServeTransportOptions::new(CodecProtocol::Http2, TransportSecurity::Plaintext);
        served
            .session
            .settings
            .retain(|setting| setting.identifier != 3);
        served.session.settings.push(CodecSetting {
            identifier: 3,
            value: u64::from(streams),
        });
        let session = self.attach_served(context, options, served);
        Box::pin(async move {
            settings.wait(CancellationToken::never()).await;
            session
        })
    }

    fn pool(
        &self,
        context: &WorkerContext,
        capacity: u32,
        factory: Rc<dyn AttachedSessionFactory>,
    ) -> HttpSessionPool {
        self.pool_with(context, params(capacity), factory)
    }

    fn pool_with(
        &self,
        context: &WorkerContext,
        options: AttachedPoolParams,
        factory: Rc<dyn AttachedSessionFactory>,
    ) -> HttpSessionPool {
        let pool = HttpSessionPool::try_with_attached_factory(
            context.clone(),
            options,
            TransportSecurity::Plaintext,
            factory,
        )
        .unwrap();
        pool.set_idle_session_observer(Some(Rc::new(IdleNotice(Rc::clone(&self.idle)))))
            .unwrap();
        pool
    }

    async fn idle(&self, session: &ClientSession) {
        loop {
            let changed = self.idle.notified();
            if session.is_idle() {
                return;
            }
            changed.await;
        }
    }
}

fn payload_request() -> HttpFixedSource {
    let body = BufQueue::from_bytes(Bytes::from_static(b"payload"), 7).unwrap();
    HttpFixedSource::make_fixed_request("/", HttpMethod::Post, Some(body)).unwrap()
}

async fn exchange(acquired: GetSessionResult) -> ClientSession {
    let request = HttpSourceHolder::new(Box::new(payload_request()));
    let mut response = acquired
        .session
        .send_reserved_request(request, acquired.reservation, CancellationToken::never())
        .await
        .unwrap();
    read_payload(&mut response).await;
    acquired.session
}

async fn read_payload(response: &mut UniplexResponse) {
    let mut header = response.source.read_header_event().await.unwrap();
    assert_eq!(header.headers_mut().status_code().unwrap(), 200);
    let mut bytes = BufQueue::new(128);
    if !header.eom() {
        loop {
            let event = response.source.read_body_event(128).await.unwrap();
            if let Some(data) = event.as_body() {
                bytes.append(&mut data.clone_shared()).unwrap();
            }
            if event.eom() {
                break;
            }
        }
    }
    assert_eq!(bytes.coalesce(128).unwrap().as_ref(), b"payload");
}

/// Holds its request body until released, so the request's stream stays open.
struct HeldUpload {
    inner: HttpFixedSource,
    release: Rc<CancellableBaton>,
}

impl HttpSource for HeldUpload {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        self.inner.read_header_event()
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let release = Rc::clone(&self.release);
        let body = self.inner.read_body_event(maximum);
        Box::pin(async move {
            release.wait(CancellationToken::never()).await;
            body.await
        })
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.inner.stop_reading(error);
    }
}

struct HeldStream {
    session: ClientSession,
    release: Rc<CancellableBaton>,
    response: UniplexResponse,
}

impl HeldStream {
    async fn open(acquired: GetSessionResult) -> Self {
        let release = Rc::new(CancellableBaton::new());
        let request = HttpSourceHolder::new(Box::new(HeldUpload {
            inner: payload_request(),
            release: Rc::clone(&release),
        }));
        let response = acquired
            .session
            .send_reserved_request(request, acquired.reservation, CancellationToken::never())
            .await
            .unwrap();
        Self {
            session: acquired.session,
            release,
            response,
        }
    }

    /// Completes the upload and reads the echoed response.
    async fn finish(mut self) -> ClientSession {
        self.release.signal();
        read_payload(&mut self.response).await;
        self.session
    }
}

#[test]
fn attached_pool_zero_timeout_waits_for_factory_and_capacity() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        run_case(move |context, _runtime| async move {
            let fixture = Fixture::default();
            let producer = fixture.clone();
            let drops = Rc::new(Cell::new(0));
            let factory_drops = Rc::clone(&drops);
            let (started, ready) = oneshot::channel();
            let (release, released) = oneshot::channel();
            let gates = RefCell::new(Some((started, released)));
            let make = timeout_factory(move |context, timeout, cancellation| {
                assert_eq!(timeout, Duration::ZERO);
                producer.calls.set(producer.calls.get() + 1);
                let mut options = ClientAttachOptions::new(protocol, TransportSecurity::Plaintext);
                options.session.max_concurrent_outgoing_streams = Some(1);
                let session = producer.attach_options(&context, options);
                let (started, released) = gates.borrow_mut().take().expect("one factory");
                let probe = FactoryJoinDropProbe {
                    owner: current().id(),
                    drops: Rc::clone(&factory_drops),
                };
                Box::pin(async move {
                    let _probe = probe;
                    started.send(()).expect("factory activation observed");
                    released.await.expect("explicit handoff release");
                    assert!(!cancellation.is_cancellation_requested());
                    Ok(session)
                })
            });
            let pool = HttpSessionPool::try_with_attached_factory(
                context,
                AttachedPoolParams {
                    connect_timeout: Duration::ZERO,
                    ..params(1)
                },
                TransportSecurity::Plaintext,
                make,
            )
            .expect("attached pool registers before activation");
            let mut first = pool.get_session_with_reservation(CancellationToken::never());
            pending(first.as_mut()).await;
            ready.await.expect("factory reached the handoff gate");
            pending(first.as_mut()).await;
            assert!(pool.full());
            assert_eq!(pool.session_count(), 0);
            assert_eq!(drops.get(), 0);
            let cancellation = CancellationSource::new();
            let mut extra = pool.get_session_with_reservation(cancellation.token());
            pending(extra.as_mut()).await;
            cancellation.request_cancellation();
            assert!(matches!(extra.await, Err(PoolError::Cancelled)));
            assert_eq!(fixture.calls.get(), 1);
            release.send(()).expect("release the only factory");
            let first = first.await.expect("zero permits the gated handoff");
            assert_eq!(drops.get(), 1);

            let mut queued = pool.get_session_with_reservation(CancellationToken::never());
            pending(queued.as_mut()).await;
            assert!(matches!(
                pool.get_session_with_reservation(CancellationToken::never())
                    .await,
                Err(PoolError::MaxWaiters)
            ));
            drop(first.reservation);
            let second = queued.await.expect("released capacity wakes the waiter");
            assert!(first.session.is_same_session(&second.session));
            assert_eq!(fixture.calls.get(), 1);

            pool.flush();
            assert_eq!(pool.session_count(), 0);
            assert!(pool.full(), "retiring ownership still occupies capacity");
            let mut queued = pool.get_session_with_reservation(CancellationToken::never());
            pending(queued.as_mut()).await;
            assert!(matches!(
                pool.get_session_with_reservation(CancellationToken::never())
                    .await,
                Err(PoolError::MaxWaiters)
            ));
            assert_eq!(fixture.calls.get(), 1);
            drop(queued);
            let mut joined = pool.drain_and_join();
            pending(joined.as_mut()).await;
            drop(second.reservation);
            joined
                .await
                .expect("leased and retiring session actually joins");
            assert!(second.session.is_finished());
            assert!(!pool.full());
            assert_eq!(pool.task_count_for_test(), 0);
        });
    }
}

#[test]
fn attached_pool_zero_timeout_cancellation_joins_late_handoff() {
    for drop_requester in [false, true] {
        run_case(move |context, _runtime| async move {
            let fixture = Fixture::default();
            let producer = fixture.clone();
            let (started, ready) = oneshot::channel();
            let (cancelled, cancellation_seen) = oneshot::channel();
            let (finish, finished) = oneshot::channel();
            let gates = RefCell::new(Some((started, cancelled, finished)));
            let returned = Rc::new(RefCell::new(None));
            let returned_session = Rc::clone(&returned);
            let factory_token = Rc::new(RefCell::new(None));
            let observed_token = Rc::clone(&factory_token);
            let drops = Rc::new(Cell::new(0));
            let factory_drops = Rc::clone(&drops);
            let make = timeout_factory(move |context, timeout, cancellation| {
                assert_eq!(timeout, Duration::ZERO);
                producer.calls.set(producer.calls.get() + 1);
                let session = producer.attach(&context, CodecProtocol::Http11);
                *returned_session.borrow_mut() = Some(session.clone());
                *observed_token.borrow_mut() = Some(cancellation.clone());
                let (started, cancelled, finished) =
                    gates.borrow_mut().take().expect("one shared attempt");
                let probe = FactoryJoinDropProbe {
                    owner: current().id(),
                    drops: Rc::clone(&factory_drops),
                };
                Box::pin(async move {
                    let _probe = probe;
                    started.send(()).expect("factory activation observed");
                    assert_eq!(
                        CancellableBaton::new().wait(cancellation).await,
                        WaitStatus::Cancelled
                    );
                    cancelled.send(()).expect("pool cancellation observed");
                    finished.await.expect("late handoff explicitly released");
                    Ok(session)
                })
            });
            let pool = HttpSessionPool::try_with_attached_factory(
                context,
                AttachedPoolParams {
                    connect_timeout: Duration::ZERO,
                    ..params(1)
                },
                TransportSecurity::Plaintext,
                make,
            )
            .expect("attached pool registers before activation");
            let cancellation = CancellationSource::new();
            let mut acquisition = pool.get_session_with_reservation(cancellation.token());
            pending(acquisition.as_mut()).await;
            ready.await.expect("factory is genuinely pending");
            if drop_requester {
                drop(acquisition);
            } else {
                cancellation.request_cancellation();
                assert!(matches!(acquisition.await, Err(PoolError::Cancelled)));
            }
            assert!(
                !factory_token
                    .borrow()
                    .as_ref()
                    .expect("factory received its cancellation token")
                    .is_cancellation_requested()
            );
            let mut replacement = pool.get_session_with_reservation(CancellationToken::never());
            pending(replacement.as_mut()).await;
            assert_eq!(fixture.calls.get(), 1);
            let mut joined = pool.drain_and_join();
            assert!(matches!(replacement.await, Err(PoolError::Draining)));
            cancellation_seen
                .await
                .expect("drain cancels the shared attempt");
            pending(joined.as_mut()).await;
            assert_eq!(drops.get(), 0);
            finish
                .send(())
                .expect("allow factory cleanup and late handoff");
            joined.await.expect("drain joins the actual late session");
            assert_eq!(drops.get(), 1);
            assert!(
                returned
                    .borrow()
                    .as_ref()
                    .expect("late session")
                    .is_finished()
            );
            assert!(!pool.full());
            assert_eq!(pool.task_count_for_test(), 0);
        });
    }
}

#[test]
fn attached_pool_zero_timeout_preserves_factory_timeout_cause() {
    run_case(|context, _runtime| async move {
        let first_probe = CauseProbe::default();
        let final_probe = CauseProbe::default();
        let probes = RefCell::new(VecDeque::from([first_probe.clone(), final_probe.clone()]));
        let calls = Rc::new(Cell::new(0));
        let factory_calls = Rc::clone(&calls);
        let drops = Rc::new(Cell::new(0));
        let factory_drops = Rc::clone(&drops);
        let (started, ready) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let first_gate = RefCell::new(Some((started, released)));
        let pool = cause_pool(
            &context,
            AttachedPoolParams {
                connect_timeout: Duration::ZERO,
                max_connection_attempts: 2,
                ..params(1)
            },
            cause_factory(move |_, timeout, cancellation| {
                assert_eq!(timeout, Duration::ZERO);
                factory_calls.set(factory_calls.get() + 1);
                let probe = probes
                    .borrow_mut()
                    .pop_front()
                    .expect("exactly two attempts");
                let gate = first_gate.borrow_mut().take();
                let drop_probe = FactoryJoinDropProbe {
                    owner: current().id(),
                    drops: Rc::clone(&factory_drops),
                };
                Box::pin(async move {
                    let _drop_probe = drop_probe;
                    if let Some((started, released)) = gate {
                        started.send(()).expect("first attempt reached gate");
                        released.await.expect("waiters admitted before timeout");
                    }
                    let elapsed = tokio::time::timeout(
                        Duration::from_millis(20),
                        std::future::pending::<()>(),
                    )
                    .await
                    .expect_err("underlying connector's own finite deadline expires");
                    assert!(!cancellation.is_cancellation_requested());
                    Err(AttachedConnectFailure::with_cause(
                        HttpError::with_message(
                            HttpErrorCode::CONNECT_ERROR,
                            b"attached connect failed",
                        ),
                        Arc::new(OriginalConnectCause {
                            probe,
                            nested: io::Error::new(io::ErrorKind::TimedOut, elapsed),
                            panic_on_drop: false,
                        }),
                    ))
                })
            }),
        );
        let mut first = pool.get_session_with_reservation(CancellationToken::never());
        pending(first.as_mut()).await;
        ready.await.expect("first connector attempt is parked");
        let mut second = pool.get_session_with_reservation(CancellationToken::never());
        pending(second.as_mut()).await;
        release
            .send(())
            .expect("let the actual connector deadlines expire");
        let errors = [
            acquisition_error(first.await),
            acquisition_error(second.await),
        ];
        let selected = assert_cause(&errors[0], &final_probe);
        assert!(Arc::ptr_eq(
            selected.external_cause().expect("selected original cause"),
            assert_cause(&errors[1], &final_probe)
                .external_cause()
                .expect("same cause for the second waiter"),
        ));
        let original = selected
            .external_cause()
            .expect("original factory timeout")
            .downcast_ref::<OriginalConnectCause>()
            .expect("concrete factory cause is retained");
        assert!(
            original
                .nested
                .get_ref()
                .expect("original timer error remains nested")
                .downcast_ref::<tokio::time::error::Elapsed>()
                .is_some()
        );
        assert_eq!(calls.get(), 2);
        assert_eq!(drops.get(), 2);
        assert_eq!(first_probe.drops.load(Ordering::SeqCst), 1);
        assert_eq!(final_probe.drops.load(Ordering::SeqCst), 0);
        assert!(!pool.full());
        pool.drain_and_join()
            .await
            .expect("both attempts actually join");
        assert_eq!(pool.task_count_for_test(), 0);
        drop(errors);
        assert_eq!(final_probe.drops.load(Ordering::SeqCst), 1);
        assert_eq!(first_probe.diagnostics.load(Ordering::SeqCst), 0);
        assert_eq!(final_probe.diagnostics.load(Ordering::SeqCst), 0);
    });
}

#[test]
fn attached_pool_zero_extension_preserves_native_and_finite_deadlines() {
    run_case(|context, _runtime| async move {
        let calls = Rc::new(Cell::new(0));
        let connector_calls = Rc::clone(&calls);
        let native = HttpSessionPool::with_connector(
            context.clone(),
            SocketAddress::new("127.0.0.1", 0),
            PoolParams {
                connect_timeout: Duration::ZERO,
                ..PoolParams::default()
            },
            ConnectionParams::default(),
            SessionParams::default(),
            timeout_factory(move |_, _, _| {
                connector_calls.set(connector_calls.get() + 1);
                Box::pin(async { Err(HttpError::new(HttpErrorCode::CONNECT_ERROR)) })
            }),
        );
        assert!(matches!(
            native
                .get_session_with_reservation(CancellationToken::never())
                .await,
            Err(PoolError::Timeout)
        ));
        assert_eq!(calls.get(), 0, "native zero retains its admission expiry");
        native
            .drain_and_join()
            .await
            .expect("native zero leaves no attempt");
        assert_eq!(native.task_count_for_test(), 0);

        for attached in [false, true] {
            let timeout = Duration::from_millis(20);
            let calls = Rc::new(Cell::new(0));
            let factory_calls = Rc::clone(&calls);
            let drops = Rc::new(Cell::new(0));
            let factory_drops = Rc::clone(&drops);
            let token = Rc::new(RefCell::new(None));
            let factory_token = Rc::clone(&token);
            let (started, ready) = oneshot::channel();
            let started = RefCell::new(Some(started));
            let make = timeout_factory(move |_, actual_timeout, cancellation| {
                assert_eq!(actual_timeout, timeout);
                factory_calls.set(factory_calls.get() + 1);
                *factory_token.borrow_mut() = Some(cancellation.clone());
                let started = started.borrow_mut().take().expect("one pending attempt");
                let probe = FactoryJoinDropProbe {
                    owner: current().id(),
                    drops: Rc::clone(&factory_drops),
                };
                Box::pin(async move {
                    let _probe = probe;
                    started.send(()).expect("attempt activation observed");
                    assert_eq!(
                        CancellableBaton::new().wait(cancellation).await,
                        WaitStatus::Cancelled
                    );
                    Err(HttpError::new(HttpErrorCode::CORO_CANCELLED))
                })
            });
            let pool = if attached {
                HttpSessionPool::try_with_attached_factory(
                    context.clone(),
                    AttachedPoolParams {
                        connect_timeout: timeout,
                        ..params(1)
                    },
                    TransportSecurity::Plaintext,
                    make,
                )
                .expect("finite attached pool")
            } else {
                HttpSessionPool::with_connector(
                    context.clone(),
                    SocketAddress::new("127.0.0.1", 0),
                    PoolParams {
                        max_connections: 1,
                        max_connection_attempts: 1,
                        connect_timeout: timeout,
                        ..PoolParams::default()
                    },
                    ConnectionParams::default(),
                    SessionParams::default(),
                    make,
                )
            };
            let mut acquisition = pool.get_session_with_reservation(CancellationToken::never());
            pending(acquisition.as_mut()).await;
            ready.await.expect("finite factory is genuinely pending");
            assert!(matches!(acquisition.await, Err(PoolError::Timeout)));
            assert_eq!(calls.get(), 1);
            assert_eq!(drops.get(), 0, "waiter expiry does not destroy the attempt");
            assert!(
                !token
                    .borrow()
                    .as_ref()
                    .expect("attempt received its cancellation token")
                    .is_cancellation_requested()
            );
            pool.drain_and_join()
                .await
                .expect("finite attempt cancels and joins");
            assert_eq!(drops.get(), 1);
            assert!(!pool.full());
            assert_eq!(pool.task_count_for_test(), 0);
        }
    });
}

#[test]
fn attached_pool_h1_h2_reuses_native_scheduler() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        run_case(move |context, _runtime| async move {
            let fixture = Fixture::default();
            let pool = fixture.pool(&context, 1, fixture.factory(protocol));
            let first = pool
                .get_session_with_reservation(CancellationToken::never())
                .await
                .unwrap();
            assert_eq!(
                first.session.connection_context().peer_endpoint(),
                &TransportEndpoint::Unknown
            );
            let session = exchange(first).await;
            fixture.idle(&session).await;
            let second = pool
                .get_session_with_reservation(CancellationToken::never())
                .await
                .unwrap();
            assert!(session.is_same_session(&second.session));
            exchange(second).await;
            assert_eq!(fixture.calls.get(), 1);
            assert!(pool.describe().contains("attached"));
            assert!(!pool.describe().contains("peer="));
            pool.drain_and_join().await.unwrap();
        });
    }
}

#[test]
fn attached_pool_reuses_protocol_capacity_with_unread_response_sources() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        run_case(move |context, _runtime| async move {
            let fixture = Fixture::default();
            let producer = fixture.clone();
            let make = factory(move |context, _cancellation| {
                producer.calls.set(producer.calls.get() + 1);
                let mut options = ClientAttachOptions::new(protocol, TransportSecurity::Plaintext);
                options.session.max_concurrent_outgoing_streams = Some(1);
                let session = producer.attach_options(&context, options);
                Box::pin(async move { Ok(session) })
            });
            let pool = fixture.pool(&context, 1, make);
            let first = pool
                .get_session_with_reservation(CancellationToken::never())
                .await
                .unwrap();
            let session = first.session;
            let request = HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_request(
                    "/",
                    HttpMethod::Post,
                    Some(BufQueue::from_bytes(Bytes::from_static(b"payload"), 7).unwrap()),
                )
                .unwrap(),
            ));
            let response = session
                .send_reserved_request(request, first.reservation, CancellationToken::never())
                .await
                .unwrap();
            response
                .source
                .ingress_completion()
                .unwrap()
                .wait()
                .await
                .unwrap();
            assert!(response.source.readable());
            assert_eq!(session.active_transactions(), 1);
            assert!(!session.is_idle());
            assert!(session.supports_more_transactions());
            let second = tokio::time::timeout(
                Duration::from_secs(2),
                pool.get_session_with_reservation(CancellationToken::never()),
            )
            .await
            .expect("completed unread response wakes pool availability")
            .unwrap();
            assert!(session.is_same_session(&second.session));
            exchange(second).await;
            assert_eq!(fixture.calls.get(), 1);
            drop(response);
            pool.drain_and_join().await.unwrap();
        });
    }
}

#[test]
fn attached_pool_factory_errors_retry_and_release_capacity() {
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let calls = Rc::clone(&fixture.calls);
        let make = factory(move |_context, _cancellation| {
            calls.set(calls.get() + 1);
            Box::pin(async {
                Err(HttpError::with_message(
                    HttpErrorCode::CONNECT_ERROR,
                    b"factory failure",
                ))
            })
        });
        let mut options = params(1);
        options.max_connection_attempts = 3;
        let pool = HttpSessionPool::try_with_attached_factory(
            context,
            options,
            TransportSecurity::Plaintext,
            make,
        )
        .unwrap();
        for expected in [3, 6] {
            let Err(PoolError::ConnectFailed { source }) = pool
                .get_session_with_reservation(CancellationToken::never())
                .await
            else {
                panic!("factory failure must remain typed")
            };
            assert_eq!(source.code(), HttpErrorCode::CONNECT_ERROR);
            assert_eq!(fixture.calls.get(), expected);
            assert!(!pool.full());
        }
        pool.drain_and_join().await.unwrap();
        assert_eq!(pool.task_count_for_test(), 0);
    });
}

#[test]
fn attached_pool_requester_drop_preserves_pool_owned_factory() {
    for cancel_waiter in [false, true] {
        run_case(move |context, _runtime| async move {
            let fixture = Fixture::default();
            let producer = fixture.clone();
            let (started, ready) = oneshot::channel();
            let (release, released) = oneshot::channel();
            let gates = RefCell::new(Some((started, released)));
            let make = factory(move |context, cancellation| {
                producer.calls.set(producer.calls.get() + 1);
                let session = producer.attach(&context, CodecProtocol::Http11);
                let (started, released) = gates.borrow_mut().take().unwrap();
                Box::pin(async move {
                    started.send(()).unwrap();
                    released.await.unwrap();
                    assert!(!cancellation.is_cancellation_requested());
                    Ok(session)
                })
            });
            let pool = fixture.pool(&context, 1, make);
            let cancellation = CancellationSource::new();
            let mut abandoned = Box::pin(pool.get_session_with_reservation(cancellation.token()));
            pending(abandoned.as_mut()).await;
            ready.await.unwrap();
            if cancel_waiter {
                cancellation.request_cancellation();
                assert!(matches!(abandoned.await, Err(PoolError::Cancelled)));
            } else {
                drop(abandoned);
            }
            let mut replacement =
                Box::pin(pool.get_session_with_reservation(CancellationToken::never()));
            pending(replacement.as_mut()).await;
            release.send(()).unwrap();
            let acquired = replacement.await.unwrap();
            assert_eq!(fixture.calls.get(), 1);
            drop(acquired.reservation);
            pool.drain_and_join().await.unwrap();
        });
    }
}

#[test]
fn attached_pool_rejects_invalid_handoffs_and_native_configuration() {
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let owner = fixture.pool(&context, 1, fixture.factory(CodecProtocol::Http11));
        let held = owner
            .get_session_with_reservation(CancellationToken::never())
            .await
            .unwrap();
        let foreign = held.session.clone();
        let rejecting = fixture.pool(
            &context,
            1,
            factory(move |_, _| {
                let session = foreign.clone();
                Box::pin(async move { Ok(session) })
            }),
        );
        assert!(matches!(
            rejecting
                .get_session_with_reservation(CancellationToken::never())
                .await,
            Err(PoolError::InvalidSessionState)
        ));
        assert!(
            !held.session.is_draining(),
            "rejecting another pool's session must not drain it"
        );
        assert!(matches!(
            rejecting.set_connection_params(ConnectionParams::default()),
            Err(PoolError::NativeConfigurationOnly)
        ));
        assert!(matches!(
            rejecting.set_session_params(SessionParams::default()),
            Err(PoolError::NativeConfigurationOnly)
        ));
        rejecting.drain_and_join().await.unwrap();
        drop(held.reservation);
        owner.drain_and_join().await.unwrap();

        let active = fixture.attach(&context, CodecProtocol::Http11);
        let reservation = active.reserve_request().unwrap();
        let returned = active.clone();
        let pool = fixture.pool(
            &context,
            1,
            factory(move |_, _| {
                let session = returned.clone();
                Box::pin(async move { Ok(session) })
            }),
        );
        assert!(matches!(
            pool.get_session_with_reservation(CancellationToken::never())
                .await,
            Err(PoolError::InvalidSessionState)
        ));
        assert!(!active.is_draining());
        drop(reservation);
        active.initiate_drain().unwrap();
        active.wait_closed().await.unwrap();
        pool.drain_and_join().await.unwrap();

        let pool = HttpSessionPool::try_with_attached_factory(
            context,
            params(1),
            TransportSecurity::Tls,
            fixture.factory(CodecProtocol::Http11),
        )
        .unwrap();
        assert!(pool.is_secure(), "query reports declared immediate-hop TLS");
        assert!(matches!(
            pool.get_session_with_reservation(CancellationToken::never())
                .await,
            Err(PoolError::SecurityMismatch)
        ));
        pool.drain_and_join().await.unwrap();
    });
}

#[test]
fn attached_pool_constructor_rejects_quiescing_admission() {
    run_case(|context, runtime| async move {
        let fixture = Fixture::default();
        context.handle().submit(|_| {}).unwrap();
        context.handle().submit(|_| {}).unwrap();
        assert_eq!(
            context.handle().submit(|_| {}),
            Err(SubmitError::Overloaded)
        );
        drop(runtime.quiesce(Duration::from_secs(60)));
        assert_eq!(context.handle().state(), WorkerState::Open);
        assert!(!context.handle().is_accepting());
        assert!(matches!(
            HttpSessionPool::try_with_attached_factory(
                context,
                params(1),
                TransportSecurity::Plaintext,
                fixture.factory(CodecProtocol::Http11)
            ),
            Err(PoolError::Draining)
        ));
        assert_eq!(
            fixture.calls.get(),
            0,
            "failed construction never starts the factory"
        );
    });
}

#[test]
fn attached_pool_drain_joins_cancelled_factory_and_late_session() {
    for late_session in [false, true] {
        run_case(move |context, _runtime| async move {
            let fixture = Fixture::default();
            let producer = fixture.clone();
            let (started, ready) = oneshot::channel();
            let (cancelled, cancellation_seen) = oneshot::channel();
            let (finish, finished) = oneshot::channel();
            let gates = RefCell::new(Some((started, cancelled, finished)));
            let returned = Rc::new(RefCell::new(None));
            let returned_session = Rc::clone(&returned);
            let cleanup = Rc::new(Cell::new(false));
            let cleanup_done = Rc::clone(&cleanup);
            let make = factory(move |context, cancellation| {
                let session =
                    late_session.then(|| producer.attach(&context, CodecProtocol::Http11));
                *returned_session.borrow_mut() = session.clone();
                let (started, cancelled, finished) = gates.borrow_mut().take().unwrap();
                let cleanup_done = Rc::clone(&cleanup_done);
                Box::pin(async move {
                    started.send(()).unwrap();
                    let baton = CancellableBaton::new();
                    assert_eq!(baton.wait(cancellation).await, WaitStatus::Cancelled);
                    cancelled.send(()).unwrap();
                    finished.await.unwrap();
                    cleanup_done.set(true);
                    session.ok_or_else(|| HttpError::new(HttpErrorCode::CORO_CANCELLED))
                })
            });
            let pool = fixture.pool(&context, 1, make);
            let mut acquisition =
                Box::pin(pool.get_session_with_reservation(CancellationToken::never()));
            pending(acquisition.as_mut()).await;
            ready.await.unwrap();
            let mut joined = pool.drain_and_join();
            assert!(matches!(acquisition.await, Err(PoolError::Draining)));
            cancellation_seen.await.unwrap();
            pending(joined.as_mut()).await;
            assert!(!cleanup.get());
            assert_eq!(pool.session_count(), 0);
            finish.send(()).unwrap();
            joined.await.unwrap();
            assert!(cleanup.get());
            if let Some(session) = returned.borrow().as_ref() {
                assert!(session.is_finished());
            }
            assert!(!pool.full());
            assert_eq!(pool.task_count_for_test(), 0);
        });
    }
}

#[test]
fn attached_pool_drain_joins_leased_and_retired_sessions() {
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let pool = fixture.pool(&context, 2, fixture.factory(CodecProtocol::Http11));
        let first = pool
            .get_session_with_reservation(CancellationToken::never())
            .await
            .unwrap();
        let second = pool
            .get_session_with_reservation(CancellationToken::never())
            .await
            .unwrap();
        assert_eq!(fixture.calls.get(), 2);
        pool.flush();
        assert_eq!(
            pool.session_count(),
            0,
            "both sessions left the scheduler but remain owned"
        );
        let mut joined = pool.drain_and_join();
        pending(joined.as_mut()).await;
        drop(first.reservation);
        first.session.wait_closed().await.unwrap();
        pending(joined.as_mut()).await;
        drop(second.reservation);
        joined.await.unwrap();
        assert!(first.session.is_finished());
        assert!(second.session.is_finished());
        assert_eq!(pool.task_count_for_test(), 0);
    });
}

#[test]
fn attached_pool_retiring_sessions_remain_bounded() {
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let pool = fixture.pool(&context, 1, fixture.factory(CodecProtocol::Http11));
        for attempt in 1..=8 {
            let held = pool
                .get_session_with_reservation(CancellationToken::never())
                .await
                .unwrap();
            assert_eq!(fixture.calls.get(), attempt);
            pool.flush();
            pool.flush();
            assert!(pool.full(), "retirement retains its ownership slot");
            let mut queued =
                Box::pin(pool.get_session_with_reservation(CancellationToken::never()));
            pending(queued.as_mut()).await;
            assert!(matches!(
                pool.get_session_with_reservation(CancellationToken::never())
                    .await,
                Err(PoolError::MaxWaiters)
            ));
            assert_eq!(fixture.calls.get(), attempt);
            assert!(pool.task_count_for_test() <= 1);
            drop(queued);
            drop(held.reservation);
            held.session.wait_closed().await.unwrap();
        }
        pool.drain_and_join().await.unwrap();
        assert_eq!(pool.task_count_for_test(), 0);
    });
}

#[test]
fn attached_pool_acquisition_after_drain_dials_a_replacement() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        run_case(move |context, _runtime| async move {
            let fixture = Fixture::default();
            let pool = fixture.pool_with(
                &context,
                retiring_params(1),
                fixture.limited_factory(protocol, 1),
            );
            let first = exchange(
                pool.get_session_with_reservation(CancellationToken::never())
                    .await
                    .unwrap(),
            )
            .await;
            fixture.idle(&first).await;
            let leased = pool
                .get_session_with_reservation(CancellationToken::never())
                .await
                .unwrap();
            assert!(leased.session.is_same_session(&first));
            let held = HeldStream::open(leased).await;
            first.initiate_drain().unwrap();
            let replacement = pool
                .get_session_with_reservation(CancellationToken::never())
                .await
                .expect("the draining session no longer holds the only slot");
            assert!(!replacement.session.is_same_session(&first));
            assert_eq!(fixture.calls.get(), 2);
            assert!(!first.is_finished(), "the in-flight request keeps it open");
            held.finish().await;
            first.wait_closed().await.unwrap();
            drop(replacement.reservation);
            pool.drain_and_join().await.unwrap();
            assert_eq!(pool.task_count_for_test(), 0);
        });
    }
}

#[test]
fn attached_pool_expired_idle_session_is_replaced() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        run_case(move |context, _runtime| async move {
            let fixture = Fixture::default();
            let pool = fixture.pool_with(&context, retiring_params(1), fixture.factory(protocol));
            let progress = Rc::new(CausePending::default());
            pool.set_observer(Some(progress.clone())).unwrap();
            let first = exchange(
                pool.get_session_with_reservation(CancellationToken::never())
                    .await
                    .unwrap(),
            )
            .await;
            fixture.idle(&first).await;
            pool.expire_sessions_for_test();
            let mut replacement = pool.get_session_with_reservation(CancellationToken::never());
            pending(replacement.as_mut()).await;
            assert!(first.is_draining());
            assert_eq!(
                progress.count.get(),
                1,
                "the expired session's slot is redialed in the same step"
            );
            let replacement = replacement.await.unwrap();
            assert!(!replacement.session.is_same_session(&first));
            assert_eq!(fixture.calls.get(), 2);
            first.wait_closed().await.unwrap();
            drop(replacement.reservation);
            pool.drain_and_join().await.unwrap();
        });
    }
}

#[test]
fn attached_pool_two_full_sessions_release_one_waiter_each() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        run_case(move |context, _runtime| async move {
            let fixture = Fixture::default();
            let pool = fixture.pool_with(
                &context,
                AttachedPoolParams {
                    max_waiters: 2,
                    ..retiring_params(2)
                },
                fixture.limited_factory(protocol, 1),
            );
            let first = HeldStream::open(
                pool.get_session_with_reservation(CancellationToken::never())
                    .await
                    .unwrap(),
            )
            .await;
            let second = HeldStream::open(
                pool.get_session_with_reservation(CancellationToken::never())
                    .await
                    .unwrap(),
            )
            .await;
            assert!(!first.session.is_same_session(&second.session));
            let mut third = pool.get_session_with_reservation(CancellationToken::never());
            let mut fourth = pool.get_session_with_reservation(CancellationToken::never());
            pending(third.as_mut()).await;
            pending(fourth.as_mut()).await;
            assert_eq!(
                fixture.calls.get(),
                2,
                "waiters behind two full sessions start no connect"
            );
            let first_session = first.finish().await;
            let third = third.await.unwrap();
            assert!(third.session.is_same_session(&first_session));
            pending(fourth.as_mut()).await;
            let third = HeldStream::open(third).await;
            let second_session = second.finish().await;
            let fourth = fourth.await.unwrap();
            assert!(fourth.session.is_same_session(&second_session));
            assert_eq!(fixture.calls.get(), 2);
            third.finish().await;
            drop(fourth.reservation);
            pool.drain_and_join().await.unwrap();
        });
    }
}

#[test]
fn attached_pool_last_failed_connect_fails_waiters() {
    run_case(|context, _runtime| async move {
        let calls = Rc::new(Cell::new(0));
        let factory_calls = Rc::clone(&calls);
        let pool = HttpSessionPool::try_with_attached_factory(
            context,
            retiring_params(1),
            TransportSecurity::Plaintext,
            factory(move |_, _| {
                factory_calls.set(factory_calls.get() + 1);
                Box::pin(async {
                    Err(HttpError::with_message(
                        HttpErrorCode::CONNECT_ERROR,
                        b"connect refused",
                    ))
                })
            }),
        )
        .unwrap();
        let mut first = pool.get_session_with_reservation(CancellationToken::never());
        let mut second = pool.get_session_with_reservation(CancellationToken::never());
        pending(first.as_mut()).await;
        pending(second.as_mut()).await;
        for result in [first.await, second.await] {
            let Err(PoolError::ConnectFailed { source }) = result else {
                panic!("the last failed connect fails every waiter with its error");
            };
            assert_eq!(source.code(), HttpErrorCode::CONNECT_ERROR);
        }
        assert_eq!(calls.get(), 1);
        let Err(PoolError::ConnectFailed { source }) = pool
            .get_session_with_reservation(CancellationToken::never())
            .await
        else {
            panic!("a later acquisition dials again instead of seeing a cancellation");
        };
        assert_eq!(source.code(), HttpErrorCode::CONNECT_ERROR);
        assert_eq!(calls.get(), 2);
        pool.drain_and_join().await.unwrap();
    });
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let (fail, failure_release) = oneshot::channel::<()>();
        let pool = fixture.pool_with(
            &context,
            AttachedPoolParams {
                max_waiters: 2,
                ..retiring_params(1)
            },
            fixture.scripted_factory(1, vec![None, Some(failure_release)]),
        );
        let progress = Rc::new(CausePending::default());
        pool.set_observer(Some(progress.clone())).unwrap();
        let held = HeldStream::open(
            pool.get_session_with_reservation(CancellationToken::never())
                .await
                .unwrap(),
        )
        .await;
        let mut first = pool.get_session_with_reservation(CancellationToken::never());
        let mut second = pool.get_session_with_reservation(CancellationToken::never());
        pending(first.as_mut()).await;
        pending(second.as_mut()).await;
        held.session.initiate_drain().unwrap();
        pending(first.as_mut()).await;
        pending(second.as_mut()).await;
        assert_eq!(progress.count.get(), 1, "the drain lets one waiter dial");
        fail.send(()).unwrap();
        for result in [first.await, second.await] {
            let Err(PoolError::ConnectFailed { source }) = result else {
                panic!(
                    "with only a retiring session left, the last failed connect fails every waiter"
                );
            };
            assert_eq!(source.code(), HttpErrorCode::CONNECT_ERROR);
        }
        assert_eq!(fixture.calls.get(), 2);
        held.finish().await.wait_closed().await.unwrap();
        pool.drain_and_join().await.unwrap();
    });
}

#[test]
fn attached_pool_last_failed_connect_keeps_waiters_while_a_session_exists() {
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let producer = fixture.clone();
        let plans = RefCell::new(VecDeque::from([false, true, false]));
        let make = factory(move |context, _| {
            producer.calls.set(producer.calls.get() + 1);
            if plans
                .borrow_mut()
                .pop_front()
                .expect("three scripted connects")
            {
                let session = producer.attach(&context, CodecProtocol::Http11);
                Box::pin(async move { Ok(session) })
            } else {
                Box::pin(async { Err(HttpError::new(HttpErrorCode::CONNECT_ERROR)) })
            }
        });
        let pool = fixture.pool_with(
            &context,
            AttachedPoolParams {
                max_waiters: 2,
                ..retiring_params(1)
            },
            make,
        );
        let progress = Rc::new(CausePending::default());
        pool.set_observer(Some(progress.clone())).unwrap();
        assert!(matches!(
            pool.get_session_with_reservation(CancellationToken::never())
                .await,
            Err(PoolError::ConnectFailed { .. })
        ));
        let first = pool
            .get_session_with_reservation(CancellationToken::never())
            .await
            .unwrap();
        assert!(pool.full());
        let mut second = pool.get_session_with_reservation(CancellationToken::never());
        pending(second.as_mut()).await;
        assert_eq!(fixture.calls.get(), 2, "a full pool waits without dialing");
        pool.set_max_connections(2).unwrap();
        assert!(!pool.full());
        let mut third = pool.get_session_with_reservation(CancellationToken::never());
        pending(third.as_mut()).await;
        progress.wait_for(0).await;
        assert_eq!(fixture.calls.get(), 3);
        pending(second.as_mut()).await;
        pending(third.as_mut()).await;
        assert_eq!(progress.count.get(), 0, "no queued waiter redials");
        drop(first.reservation);
        let second = second.await.unwrap();
        assert!(second.session.is_same_session(&first.session));
        pending(third.as_mut()).await;
        drop(second.reservation);
        let third = third.await.unwrap();
        assert!(third.session.is_same_session(&first.session));
        assert_eq!(fixture.calls.get(), 3);
        drop(third.reservation);
        pool.drain_and_join().await.unwrap();
    });
}

#[test]
fn attached_pool_flush_cancels_queued_waiters() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        run_case(move |context, _runtime| async move {
            let fixture = Fixture::default();
            let pool = fixture.pool_with(
                &context,
                AttachedPoolParams {
                    max_waiters: 2,
                    ..retiring_params(2)
                },
                fixture.limited_factory(protocol, 1),
            );
            let (first, second) = tokio::join!(
                pool.get_session_with_reservation(CancellationToken::never()),
                pool.get_session_with_reservation(CancellationToken::never()),
            );
            let (first, second) = (first.unwrap(), second.unwrap());
            assert!(!first.session.is_same_session(&second.session));
            assert!(pool.full());
            let mut third = pool.get_session_with_reservation(CancellationToken::never());
            let mut fourth = pool.get_session_with_reservation(CancellationToken::never());
            pending(third.as_mut()).await;
            pending(fourth.as_mut()).await;
            pool.flush();
            assert!(matches!(third.await, Err(PoolError::Draining)));
            assert!(matches!(fourth.await, Err(PoolError::Draining)));
            assert_eq!(fixture.calls.get(), 2);
            drop(first.reservation);
            drop(second.reservation);
            pool.drain_and_join().await.unwrap();
        });
    }
}

#[test]
fn attached_pool_peer_goaway_frees_a_slot_while_streams_finish() {
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let pool = fixture.pool_with(
            &context,
            retiring_params(1),
            fixture.limited_factory(CodecProtocol::Http2, 1),
        );
        let held = HeldStream::open(
            pool.get_session_with_reservation(CancellationToken::never())
                .await
                .unwrap(),
        )
        .await;
        let mut queued = pool.get_session_with_reservation(CancellationToken::never());
        pending(queued.as_mut()).await;
        assert!(pool.full());
        fixture.peers.borrow()[0].initiate_drain().unwrap();
        let replacement = queued
            .await
            .expect("the peer GOAWAY frees the slot for the queued waiter");
        assert!(!replacement.session.is_same_session(&held.session));
        assert!(held.session.is_draining());
        assert!(!held.session.is_finished());
        assert_eq!(fixture.calls.get(), 2);
        let first = held.finish().await;
        first.wait_closed().await.unwrap();
        drop(replacement.reservation);
        pool.drain_and_join().await.unwrap();
    });
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let (release, _) = watch::channel(false);
        let pool = fixture.pool_with(
            &context,
            AttachedPoolParams {
                max_waiters: 3,
                ..retiring_params(2)
            },
            fixture.one_stream_gated_factory(1, &release),
        );
        let progress = Rc::new(CausePending::default());
        pool.set_observer(Some(progress.clone())).unwrap();
        let held = HeldStream::open(
            pool.get_session_with_reservation(CancellationToken::never())
                .await
                .unwrap(),
        )
        .await;
        let mut queued: Vec<_> = (0..3)
            .map(|_| pool.get_session_with_reservation(CancellationToken::never()))
            .collect();
        for acquisition in &mut queued {
            pending(acquisition.as_mut()).await;
        }
        assert_eq!(
            progress.count.get(),
            1,
            "the first waiter's connect closes the gate to the others"
        );
        fixture.peers.borrow()[0].initiate_drain().unwrap();
        until("the peer GOAWAY reaches the held session", || {
            held.session.is_draining()
        })
        .await;
        for acquisition in &mut queued {
            pending(acquisition.as_mut()).await;
        }
        assert_eq!(
            progress.count.get(),
            2,
            "the GOAWAY's slot reaches a waiter that the pending connect does not cover"
        );
        release.send_replace(true);
        while !queued.is_empty() {
            for acquired in complete_some(&mut queued).await {
                drop(acquired.expect("each waiter gets a stream within its deadline"));
            }
        }
        assert_eq!(fixture.calls.get(), 3);
        held.finish().await.wait_closed().await.unwrap();
        pool.drain_and_join().await.unwrap();
    });
}

#[test]
fn attached_pool_explicit_drain_frees_a_slot_while_streams_finish() {
    for close_when_idle in [false, true] {
        for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
            run_case(move |context, _runtime| async move {
                let fixture = Fixture::default();
                let pool = fixture.pool_with(
                    &context,
                    retiring_params(1),
                    fixture.limited_factory(protocol, 1),
                );
                let held = HeldStream::open(
                    pool.get_session_with_reservation(CancellationToken::never())
                        .await
                        .unwrap(),
                )
                .await;
                let mut queued = pool.get_session_with_reservation(CancellationToken::never());
                pending(queued.as_mut()).await;
                if close_when_idle {
                    held.session.close_when_idle().unwrap();
                } else {
                    held.session.initiate_drain().unwrap();
                }
                let replacement = queued
                    .await
                    .expect("an explicit drain frees the slot for the queued waiter");
                assert!(!replacement.session.is_same_session(&held.session));
                assert!(!held.session.is_finished());
                assert_eq!(fixture.calls.get(), 2);
                let first = held.finish().await;
                first.wait_closed().await.unwrap();
                drop(replacement.reservation);
                pool.drain_and_join().await.unwrap();
            });
        }
    }
}

#[test]
fn attached_pool_expired_session_with_open_stream_frees_a_slot() {
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let pool = fixture.pool_with(
            &context,
            retiring_params(1),
            fixture.factory(CodecProtocol::Http2),
        );
        let held = HeldStream::open(
            pool.get_session_with_reservation(CancellationToken::never())
                .await
                .unwrap(),
        )
        .await;
        assert!(held.session.supports_more_transactions());
        pool.expire_sessions_for_test();
        let replacement = pool
            .get_session_with_reservation(CancellationToken::never())
            .await
            .expect("the expired session frees its slot at acquisition");
        assert!(held.session.is_draining());
        assert!(!held.session.is_finished());
        assert!(!replacement.session.is_same_session(&held.session));
        assert_eq!(fixture.calls.get(), 2);
        let first = held.finish().await;
        first.wait_closed().await.unwrap();
        drop(replacement.reservation);
        pool.drain_and_join().await.unwrap();
    });
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let producer = fixture.clone();
        let deadline = Duration::from_secs(2);
        let make = timeout_factory(move |context, timeout, _| {
            assert_eq!(timeout, deadline);
            producer.calls.set(producer.calls.get() + 1);
            let session = producer.attach(&context, CodecProtocol::Http2);
            Box::pin(async move { Ok(session) })
        });
        let pool = fixture.pool_with(
            &context,
            AttachedPoolParams {
                connect_timeout: deadline,
                ..retiring_params(1)
            },
            make,
        );
        let retiring = HeldStream::open(
            pool.get_session_with_reservation(CancellationToken::never())
                .await
                .unwrap(),
        )
        .await;
        retiring.session.initiate_drain().unwrap();
        let aged = HeldStream::open(
            pool.get_session_with_reservation(CancellationToken::never())
                .await
                .unwrap(),
        )
        .await;
        assert!(
            pool.full(),
            "a retiring and an active session own max_connections + max_retiring"
        );
        pool.expire_sessions_for_test();
        let shared = pool
            .get_session_with_reservation(CancellationToken::never())
            .await
            .expect("an aged session the bound cannot replace keeps serving");
        assert!(shared.session.is_same_session(&aged.session));
        assert!(!aged.session.is_draining());
        assert_eq!(fixture.calls.get(), 2);
        drop(shared.reservation);
        retiring.finish().await.wait_closed().await.unwrap();
        aged.finish().await;
        pool.drain_and_join().await.unwrap();
    });
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let pool = fixture.pool_with(
            &context,
            AttachedPoolParams {
                max_retiring: Some(1),
                ..retiring_params(2)
            },
            fixture.limited_factory(CodecProtocol::Http2, 1),
        );
        let retiring = HeldStream::open(
            pool.get_session_with_reservation(CancellationToken::never())
                .await
                .unwrap(),
        )
        .await;
        retiring.session.initiate_drain().unwrap();
        let first = HeldStream::open(
            pool.get_session_with_reservation(CancellationToken::never())
                .await
                .unwrap(),
        )
        .await;
        let second = HeldStream::open(
            pool.get_session_with_reservation(CancellationToken::never())
                .await
                .unwrap(),
        )
        .await;
        let first = first.finish().await;
        let second = second.finish().await;
        fixture.idle(&first).await;
        fixture.idle(&second).await;
        assert!(
            pool.full(),
            "a retiring and two active sessions own max_connections + max_retiring"
        );
        pool.expire_sessions_for_test();
        let acquired = pool
            .get_session_with_reservation(CancellationToken::never())
            .await
            .expect("the last selectable aged session keeps serving");
        let (expired, kept) = if first.is_draining() {
            (first, second)
        } else {
            (second, first)
        };
        assert!(
            expired.is_draining(),
            "an aged session retires while another selectable session remains"
        );
        assert!(!kept.is_draining());
        assert!(acquired.session.is_same_session(&kept));
        assert_eq!(fixture.calls.get(), 3);
        drop(acquired.reservation);
        expired.wait_closed().await.unwrap();
        retiring.finish().await.wait_closed().await.unwrap();
        pool.drain_and_join().await.unwrap();
    });
}

#[test]
fn attached_pool_flush_fails_waiters_and_frees_slots_for_later_acquisitions() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        run_case(move |context, _runtime| async move {
            let fixture = Fixture::default();
            let pool = fixture.pool_with(
                &context,
                retiring_params(2),
                fixture.limited_factory(protocol, 1),
            );
            let mut flushed = Vec::new();
            for _ in 0..2 {
                flushed.push(
                    HeldStream::open(
                        pool.get_session_with_reservation(CancellationToken::never())
                            .await
                            .unwrap(),
                    )
                    .await,
                );
            }
            let mut queued = pool.get_session_with_reservation(CancellationToken::never());
            pending(queued.as_mut()).await;
            pool.flush();
            assert!(matches!(queued.await, Err(PoolError::Draining)));
            assert!(
                !pool.full(),
                "flushed sessions retire outside max_connections"
            );
            let third = pool
                .get_session_with_reservation(CancellationToken::never())
                .await
                .unwrap();
            let fourth = pool
                .get_session_with_reservation(CancellationToken::never())
                .await
                .unwrap();
            assert!(!third.session.is_same_session(&fourth.session));
            assert!(flushed.iter().all(|held| {
                !held.session.is_same_session(&third.session)
                    && !held.session.is_same_session(&fourth.session)
                    && !held.session.is_finished()
            }));
            assert_eq!(fixture.calls.get(), 4);
            assert!(pool.full());
            assert!(
                !pool.overflowed(),
                "replacements reach max_connections and ownership reaches max_connections + max_retiring"
            );
            pool.set_max_connections(1).unwrap();
            assert!(
                pool.overflowed(),
                "two non-retiring sessions exceed the lowered max_connections"
            );
            third.session.initiate_drain().unwrap();
            assert!(
                pool.overflowed(),
                "four owned sessions exceed the lowered max_connections + max_retiring"
            );
            for held in flushed {
                held.finish().await.wait_closed().await.unwrap();
            }
            until(
                "the closed flushed sessions release their ownership",
                || !pool.overflowed(),
            )
            .await;
            drop(third.reservation);
            drop(fourth.reservation);
            pool.drain_and_join().await.unwrap();
        });
    }
}

#[test]
fn attached_pool_drain_and_join_waits_for_retiring_sessions() {
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let pool = fixture.pool_with(
            &context,
            retiring_params(1),
            fixture.limited_factory(CodecProtocol::Http2, 1),
        );
        let retiring = HeldStream::open(
            pool.get_session_with_reservation(CancellationToken::never())
                .await
                .unwrap(),
        )
        .await;
        retiring.session.initiate_drain().unwrap();
        let leased = pool
            .get_session_with_reservation(CancellationToken::never())
            .await
            .unwrap();
        assert_eq!(fixture.calls.get(), 2);
        let mut joined = pool.drain_and_join();
        pending(joined.as_mut()).await;
        drop(leased.reservation);
        leased.session.wait_closed().await.unwrap();
        pending(joined.as_mut()).await;
        assert!(!retiring.session.is_finished());
        let retired = retiring.session.clone();
        retired.cancel();
        drop(retiring);
        let Err(PoolError::SessionJoin { source }) = joined.await else {
            panic!("the join reports the failure of the retiring session");
        };
        assert_eq!(source.code(), HttpErrorCode::CORO_CANCELLED);
        assert!(retired.is_finished());
        assert_eq!(pool.task_count_for_test(), 0);
    });
}

#[test]
fn attached_pool_retiring_bound_caps_owned_connections() {
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let (release, _) = watch::channel(false);
        let pool = fixture.pool_with(
            &context,
            AttachedPoolParams {
                max_retiring: Some(1),
                max_waiters: 2,
                ..params(2)
            },
            fixture.one_stream_gated_factory(2, &release),
        );
        let first = HeldStream::open(
            pool.get_session_with_reservation(CancellationToken::never())
                .await
                .unwrap(),
        )
        .await;
        let second = HeldStream::open(
            pool.get_session_with_reservation(CancellationToken::never())
                .await
                .unwrap(),
        )
        .await;
        pool.flush();
        assert!(!pool.full(), "retiring sessions leave max_connections");
        let mut third = pool.get_session_with_reservation(CancellationToken::never());
        pending(third.as_mut()).await;
        assert!(
            pool.full(),
            "two retiring sessions and a pending connect own max_connections + max_retiring"
        );
        let mut fourth = pool.get_session_with_reservation(CancellationToken::never());
        let mut fifth = pool.get_session_with_reservation(CancellationToken::never());
        pending(fourth.as_mut()).await;
        pending(fifth.as_mut()).await;
        release.send_replace(true);
        let third = third.await.unwrap();
        assert_eq!(fixture.calls.get(), 3);
        assert!(
            matches!(
                pool.get_session_with_reservation(CancellationToken::never())
                    .await,
                Err(PoolError::MaxWaiters)
            ),
            "established and retiring sessions alone close the gate"
        );
        let donor_fixture = Fixture::default();
        let donor = donor_fixture.pool(&context, 1, donor_fixture.factory(CodecProtocol::Http2));
        let idle = exchange(
            donor
                .get_session_with_reservation(CancellationToken::never())
                .await
                .unwrap(),
        )
        .await;
        donor_fixture.idle(&idle).await;
        let detached = donor
            .detach_idle_session()
            .expect("idle session transfers explicitly");
        assert!(
            matches!(
                pool.insert_idle_session(detached),
                Err(PoolError::OwnershipCapacity)
            ),
            "the ownership bound refuses an inserted session below max_connections"
        );
        donor.drain_and_join().await.unwrap();
        third.session.initiate_drain().unwrap();
        pending(fourth.as_mut()).await;
        pending(fifth.as_mut()).await;
        assert!(pool.full(), "retiring adds no connection");
        assert_eq!(fixture.calls.get(), 3);
        drop(fourth);
        first.finish().await.wait_closed().await.unwrap();
        let fifth = fifth
            .await
            .expect("a closed retiring session returns its ownership");
        assert_eq!(fixture.calls.get(), 4);
        assert!(pool.full());
        second.finish().await;
        drop(third.reservation);
        drop(fifth.reservation);
        pool.drain_and_join().await.unwrap();
    });
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let pool = fixture.pool_with(
            &context,
            AttachedPoolParams {
                max_retiring: Some(1),
                max_waiters: 2,
                ..retiring_params(2)
            },
            fixture.limited_factory(CodecProtocol::Http2, 1),
        );
        let progress = Rc::new(CausePending::default());
        pool.set_observer(Some(progress.clone())).unwrap();
        let first = HeldStream::open(
            pool.get_session_with_reservation(CancellationToken::never())
                .await
                .unwrap(),
        )
        .await;
        let second = HeldStream::open(
            pool.get_session_with_reservation(CancellationToken::never())
                .await
                .unwrap(),
        )
        .await;
        let mut third = pool.get_session_with_reservation(CancellationToken::never());
        let mut fourth = pool.get_session_with_reservation(CancellationToken::never());
        pending(third.as_mut()).await;
        pending(fourth.as_mut()).await;
        assert!(
            matches!(
                pool.get_session_with_reservation(CancellationToken::never())
                    .await,
                Err(PoolError::MaxWaiters)
            ),
            "non-retiring sessions at max_connections apply max_waiters"
        );
        for peer in fixture.peers.borrow().iter() {
            peer.initiate_drain().unwrap();
        }
        until("both GOAWAYs reach their sessions", || {
            first.session.is_draining() && second.session.is_draining()
        })
        .await;
        pending(third.as_mut()).await;
        pending(fourth.as_mut()).await;
        assert_eq!(
            progress.count.get(),
            1,
            "two GOAWAYs in one turn admit one dial within max_connections + max_retiring"
        );
        assert!(pool.full());
        let third = third.await.unwrap();
        pending(fourth.as_mut()).await;
        assert_eq!(fixture.calls.get(), 3);
        first.finish().await.wait_closed().await.unwrap();
        let fourth = fourth
            .await
            .expect("a closed retiring session lets the second waiter dial");
        assert_eq!(fixture.calls.get(), 4);
        second.finish().await;
        drop(third.reservation);
        drop(fourth.reservation);
        pool.drain_and_join().await.unwrap();
    });
}

#[test]
fn attached_pool_retiring_bound_waits_within_the_deadline() {
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let producer = fixture.clone();
        let deadline = Duration::from_millis(500);
        let make = timeout_factory(move |context, timeout, _| {
            assert_eq!(timeout, deadline);
            producer.calls.set(producer.calls.get() + 1);
            let mut options =
                ClientAttachOptions::new(CodecProtocol::Http2, TransportSecurity::Plaintext);
            options.session.max_concurrent_outgoing_streams = Some(1);
            let session = producer.attach_options(&context, options);
            Box::pin(async move { Ok(session) })
        });
        let pool = fixture.pool_with(
            &context,
            AttachedPoolParams {
                max_retiring: Some(1),
                connect_timeout: deadline,
                ..params(1)
            },
            make,
        );
        let first = HeldStream::open(
            pool.get_session_with_reservation(CancellationToken::never())
                .await
                .unwrap(),
        )
        .await;
        first.session.initiate_drain().unwrap();
        let second = HeldStream::open(
            pool.get_session_with_reservation(CancellationToken::never())
                .await
                .expect("the only session's drain permits a replacement"),
        )
        .await;
        assert_eq!(fixture.calls.get(), 2);
        second.session.initiate_drain().unwrap();
        assert!(pool.full());
        let started = Instant::now();
        assert!(matches!(
            pool.get_session_with_reservation(CancellationToken::never())
                .await,
            Err(PoolError::Timeout)
        ));
        assert!(started.elapsed() >= deadline);
        let mut queued = pool.get_session_with_reservation(CancellationToken::never());
        pending(queued.as_mut()).await;
        assert!(
            matches!(
                pool.get_session_with_reservation(CancellationToken::never())
                    .await,
                Err(PoolError::MaxWaiters)
            ),
            "retiring sessions alone close the gate"
        );
        drop(queued);
        assert_eq!(fixture.calls.get(), 2);
        first.finish().await.wait_closed().await.unwrap();
        second.finish().await.wait_closed().await.unwrap();
        assert!(!pool.full());
        pool.drain_and_join().await.unwrap();
    });
}

#[test]
fn attached_pool_closed_retiring_session_wakes_a_waiter() {
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let pool = fixture.pool_with(
            &context,
            AttachedPoolParams {
                max_retiring: Some(1),
                ..params(1)
            },
            fixture.limited_factory(CodecProtocol::Http2, 1),
        );
        let first = HeldStream::open(
            pool.get_session_with_reservation(CancellationToken::never())
                .await
                .unwrap(),
        )
        .await;
        first.session.initiate_drain().unwrap();
        let second = HeldStream::open(
            pool.get_session_with_reservation(CancellationToken::never())
                .await
                .unwrap(),
        )
        .await;
        second.session.initiate_drain().unwrap();
        let mut waiter = pool.get_session_with_reservation(CancellationToken::never());
        pending(waiter.as_mut()).await;
        assert_eq!(fixture.calls.get(), 2);
        first.finish().await.wait_closed().await.unwrap();
        let acquired = waiter
            .await
            .expect("the closed session's ownership wakes the waiter, which dials");
        assert_eq!(fixture.calls.get(), 3);
        assert!(!acquired.session.is_same_session(&second.session));
        second.finish().await;
        drop(acquired.reservation);
        pool.drain_and_join().await.unwrap();
    });
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let (release, _) = watch::channel(false);
        let pool = fixture.pool_with(
            &context,
            AttachedPoolParams {
                max_retiring: Some(1),
                pending_connect_credit: 4,
                max_waiters: 5,
                ..retiring_params(2)
            },
            fixture.one_stream_gated_factory(2, &release),
        );
        let progress = Rc::new(CausePending::default());
        pool.set_observer(Some(progress.clone())).unwrap();
        let mut retiring = VecDeque::new();
        for _ in 0..2 {
            retiring.push_back(
                HeldStream::open(
                    pool.get_session_with_reservation(CancellationToken::never())
                        .await
                        .unwrap(),
                )
                .await,
            );
        }
        pool.flush();
        let mut queued: Vec<_> = (0..5)
            .map(|_| pool.get_session_with_reservation(CancellationToken::never()))
            .collect();
        for acquisition in &mut queued {
            pending(acquisition.as_mut()).await;
        }
        assert!(pool.full());
        assert_eq!(progress.count.get(), 1);
        let closed = retiring.pop_front().unwrap().finish().await;
        closed.wait_closed().await.unwrap();
        until("the closed retiring session frees the gate", || {
            !pool.full()
        })
        .await;
        for acquisition in &mut queued {
            pending(acquisition.as_mut()).await;
        }
        assert_eq!(
            progress.count.get(),
            2,
            "the closed session's slot reaches a waiter that the pending connect does not cover"
        );
        release.send_replace(true);
        while !queued.is_empty() {
            for acquired in complete_some(&mut queued).await {
                drop(acquired.expect("each waiter gets a stream within its deadline"));
            }
        }
        assert_eq!(fixture.calls.get(), 4);
        for held in retiring {
            held.finish().await;
        }
        pool.drain_and_join().await.unwrap();
    });
}

#[test]
fn attached_pool_burst_within_credit_dials_once() {
    let defaults = AttachedPoolParams::default();
    assert_eq!(
        (defaults.max_retiring, defaults.pending_connect_credit),
        (None, 1),
        "the defaults keep the shared bound and the native scheduler"
    );
    for (credit, expected_dials) in [(0, 3), (1, 3), (3, 1)] {
        run_case(move |context, _runtime| async move {
            let fixture = Fixture::default();
            let (release, _) = watch::channel(false);
            let pool = fixture.pool_with(
                &context,
                AttachedPoolParams {
                    pending_connect_credit: credit,
                    ..params(3)
                },
                fixture.gated_factory(CodecProtocol::Http2, &release),
            );
            let progress = Rc::new(CausePending::default());
            pool.set_observer(Some(progress.clone())).unwrap();
            let mut burst: Vec<_> = (0..3)
                .map(|_| pool.get_session_with_reservation(CancellationToken::never()))
                .collect();
            for acquisition in &mut burst {
                pending(acquisition.as_mut()).await;
            }
            assert_eq!(progress.count.get(), i32::try_from(expected_dials).unwrap());
            until("every credited connect reaches the factory", || {
                fixture.calls.get() == expected_dials
            })
            .await;
            release.send_replace(true);
            let mut acquired = Vec::new();
            for acquisition in burst {
                acquired.push(acquisition.await.unwrap());
            }
            assert_eq!(fixture.calls.get(), expected_dials);
            if expected_dials == 1 {
                assert!(
                    acquired
                        .iter()
                        .all(|result| result.session.is_same_session(&acquired[0].session))
                );
            }
            drop(acquired);
            pool.drain_and_join().await.unwrap();
        });
    }
}

#[test]
fn attached_pool_burst_beyond_credit_dials_up_to_max_connections() {
    for (capacity, expected_dials) in [(2, 2), (4, 3)] {
        run_case(move |context, _runtime| async move {
            let fixture = Fixture::default();
            let (release, _) = watch::channel(false);
            let pool = fixture.pool_with(
                &context,
                AttachedPoolParams {
                    pending_connect_credit: 2,
                    ..params(capacity)
                },
                fixture.gated_factory(CodecProtocol::Http2, &release),
            );
            let progress = Rc::new(CausePending::default());
            pool.set_observer(Some(progress.clone())).unwrap();
            let mut burst: Vec<_> = (0..5)
                .map(|_| pool.get_session_with_reservation(CancellationToken::never()))
                .collect();
            for acquisition in &mut burst {
                pending(acquisition.as_mut()).await;
            }
            assert_eq!(
                progress.count.get(),
                i32::try_from(expected_dials).unwrap(),
                "one connect per two waiters, within max_connections"
            );
            until("every gated connect reaches the factory", || {
                fixture.calls.get() == expected_dials
            })
            .await;
            release.send_replace(true);
            let mut acquired = Vec::new();
            for acquisition in burst {
                acquired.push(acquisition.await.unwrap());
            }
            assert_eq!(fixture.calls.get(), expected_dials);
            drop(acquired);
            pool.drain_and_join().await.unwrap();
        });
    }
}

#[test]
fn attached_pool_abandoned_credited_waiter_returns_its_credit() {
    for drop_requester in [false, true] {
        run_case(move |context, _runtime| async move {
            let fixture = Fixture::default();
            let (release, _) = watch::channel(false);
            let pool = fixture.pool_with(
                &context,
                retiring_params(3),
                fixture.gated_factory(CodecProtocol::Http2, &release),
            );
            let progress = Rc::new(CausePending::default());
            pool.set_observer(Some(progress.clone())).unwrap();
            let cancellation = CancellationSource::new();
            let mut originator = pool.get_session_with_reservation(cancellation.token());
            let mut credited = pool.get_session_with_reservation(CancellationToken::never());
            pending(originator.as_mut()).await;
            pending(credited.as_mut()).await;
            assert_eq!(progress.count.get(), 1, "both waiters share one connect");
            until("the shared connect reaches the factory", || {
                fixture.calls.get() == 1
            })
            .await;

            if drop_requester {
                drop(originator);
            } else {
                cancellation.request_cancellation();
                assert!(matches!(originator.await, Err(PoolError::Cancelled)));
            }
            assert_eq!(pool.waiter_count_for_test(), 1, "the abandoned waiter left");

            let mut next = pool.get_session_with_reservation(CancellationToken::never());
            pending(next.as_mut()).await;
            assert_eq!(
                progress.count.get(),
                1,
                "the returned credit absorbs the next waiter instead of a second connect"
            );
            assert_eq!(pool.waiter_count_for_test(), 2);

            release.send_replace(true);
            let credited = credited.await.unwrap();
            let next = next.await.unwrap();
            assert!(credited.session.is_same_session(&next.session));
            assert_eq!(
                fixture.calls.get(),
                1,
                "the abandoned waiter's connect serves both"
            );
            assert_eq!(pool.waiter_count_for_test(), 0, "no waiter is stranded");
            progress.wait_for(0).await;
            drop((credited, next));
            pool.drain_and_join().await.unwrap();
            assert_eq!(pool.task_count_for_test(), 0);
        });
    }
}

struct PeerSettingsSeen(Rc<CancellableBaton>);

impl LifecycleObserver for PeerSettingsSeen {
    fn on_event(&self, event: LifecycleEvent) {
        if matches!(event, LifecycleEvent::Http2Settings { .. }) {
            self.0.signal();
        }
    }
}

#[test]
fn attached_pool_short_peer_stream_limit_starts_another_connect() {
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let producer = fixture.clone();
        let deadline = Duration::from_secs(2);
        let make = timeout_factory(move |context, timeout, _| {
            assert_eq!(timeout, deadline);
            producer.calls.set(producer.calls.get() + 1);
            let attached = producer.attach_peer_limited(&context, 1);
            Box::pin(async move {
                let session = attached.await;
                assert_eq!(session.available_transactions(), 1);
                Ok(session)
            })
        });
        let pool = fixture.pool_with(
            &context,
            AttachedPoolParams {
                pending_connect_credit: 2,
                connect_timeout: deadline,
                ..params(2)
            },
            make,
        );
        let progress = Rc::new(CausePending::default());
        pool.set_observer(Some(progress.clone())).unwrap();
        let mut first = pool.get_session_with_reservation(CancellationToken::never());
        let mut second = pool.get_session_with_reservation(CancellationToken::never());
        pending(first.as_mut()).await;
        pending(second.as_mut()).await;
        assert_eq!(progress.count.get(), 1, "one connect absorbs both waiters");
        let first = first.await.unwrap();
        let second = second
            .await
            .expect("the uncovered waiter dials instead of waiting for a stream");
        assert!(!first.session.is_same_session(&second.session));
        assert_eq!(fixture.calls.get(), 2);
        drop((first, second));
        pool.drain_and_join().await.unwrap();
    });
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let producer = fixture.clone();
        let deadline = Duration::from_secs(2);
        let (first_release, first_gate) = oneshot::channel::<()>();
        let (second_release, second_gate) = oneshot::channel::<()>();
        let gates = RefCell::new(VecDeque::from([first_gate, second_gate]));
        let make = timeout_factory(move |context, timeout, _| {
            assert_eq!(timeout, deadline);
            producer.calls.set(producer.calls.get() + 1);
            let mut options =
                ClientAttachOptions::new(CodecProtocol::Http2, TransportSecurity::Plaintext);
            options.session.max_concurrent_outgoing_streams = Some(1);
            let session = producer.attach_options(&context, options);
            let gate = gates.borrow_mut().pop_front();
            Box::pin(async move {
                if let Some(gate) = gate {
                    gate.await.unwrap();
                }
                Ok(session)
            })
        });
        let pool = fixture.pool_with(
            &context,
            AttachedPoolParams {
                connect_timeout: deadline,
                ..retiring_params(4)
            },
            make,
        );
        let progress = Rc::new(CausePending::default());
        pool.set_observer(Some(progress.clone())).unwrap();
        let mut burst: Vec<_> = (0..4)
            .map(|_| pool.get_session_with_reservation(CancellationToken::never()))
            .collect();
        for acquisition in &mut burst {
            pending(acquisition.as_mut()).await;
        }
        assert_eq!(progress.count.get(), 2, "each connect absorbs two waiters");
        second_release.send(()).unwrap();
        let mut acquired = complete_some(&mut burst).await;
        first_release.send(()).unwrap();
        while !burst.is_empty() {
            acquired.extend(complete_some(&mut burst).await);
        }
        let acquired: Vec<_> = acquired
            .into_iter()
            .map(|result| result.expect("no waiter waits past its deadline because of the credit"))
            .collect();
        assert!(acquired.iter().enumerate().all(|(index, result)| {
            acquired[..index]
                .iter()
                .all(|earlier| !earlier.session.is_same_session(&result.session))
        }));
        assert_eq!(
            fixture.calls.get(),
            4,
            "four one-stream acquisitions need four sessions"
        );
        drop(acquired);
        pool.drain_and_join().await.unwrap();
    });
}

#[test]
fn attached_pool_observed_stream_limit_caps_credit() {
    // The peer admits two streams per session. With four connections the
    // burst needs fewer connects than the gate allows; with three, the gate
    // holds the waiter beyond six streams.
    for (capacity, burst_size, dials) in [(4_u32, 5_usize, 3_u32), (3, 7, 3)] {
        run_case(move |context, _runtime| async move {
            let fixture = Fixture::default();
            let producer = fixture.clone();
            let (first_release, first_gate) = oneshot::channel::<()>();
            let first_gate = RefCell::new(Some(first_gate));
            let (release, _) = watch::channel(false);
            let later = release.clone();
            let make = factory(move |context, _| {
                producer.calls.set(producer.calls.get() + 1);
                let attached = producer.attach_peer_limited(&context, 2);
                let first = first_gate.borrow_mut().take();
                let mut released = later.subscribe();
                Box::pin(async move {
                    let session = attached.await;
                    match first {
                        Some(gate) => gate.await.unwrap(),
                        None => {
                            released.wait_for(|released| *released).await.unwrap();
                        }
                    }
                    Ok(session)
                })
            });
            let pool = fixture.pool_with(
                &context,
                AttachedPoolParams {
                    pending_connect_credit: 100,
                    ..retiring_params(capacity)
                },
                make,
            );
            let progress = Rc::new(CausePending::default());
            pool.set_observer(Some(progress.clone())).unwrap();
            let mut burst: Vec<_> = (0..burst_size)
                .map(|_| pool.get_session_with_reservation(CancellationToken::never()))
                .collect();
            for acquisition in &mut burst {
                pending(acquisition.as_mut()).await;
            }
            assert_eq!(progress.count.get(), 1, "one connect absorbs the burst");
            let served = |results: Vec<Result<GetSessionResult, PoolError>>| {
                results
                    .into_iter()
                    .map(|result| result.expect("each waiter gets a stream"))
                    .collect::<Vec<_>>()
            };
            first_release.send(()).unwrap();
            let mut acquired = served(complete_some(&mut burst).await);
            assert_eq!(acquired.len(), 2, "the first session serves two streams");
            assert_eq!(
                progress.count.get(),
                i32::try_from(dials - 1).unwrap(),
                "the uncovered waiters dial in parallel, two per connect, within max_connections"
            );
            until("the parallel connects reach the factory", || {
                fixture.calls.get() == dials
            })
            .await;
            release.send_replace(true);
            let streams = usize::try_from(dials * 2).unwrap();
            while acquired.len() < burst_size.min(streams) {
                acquired.extend(served(complete_some(&mut burst).await));
            }
            assert_eq!(fixture.calls.get(), dials, "no connect beyond the need");
            assert_eq!(progress.count.get(), 0);
            if !burst.is_empty() {
                assert!(pool.full());
                pending(burst[0].as_mut()).await;
                drop(acquired.pop());
                acquired.extend(served(complete_some(&mut burst).await));
            }
            assert!(burst.is_empty());
            assert_eq!(fixture.calls.get(), dials);
            drop(acquired);
            pool.drain_and_join().await.unwrap();
        });
    }
}

/// Polls every unfinished acquisition and resolves once at least one finished.
async fn complete_some(
    burst: &mut Vec<LocalClientFuture<Result<GetSessionResult, PoolError>>>,
) -> Vec<Result<GetSessionResult, PoolError>> {
    poll_fn(|context| {
        let mut finished = Vec::new();
        let mut index = 0;
        while index < burst.len() {
            if let Poll::Ready(result) = burst[index].as_mut().poll(context) {
                finished.push(result);
                drop(burst.remove(index));
            } else {
                index += 1;
            }
        }
        if finished.is_empty() {
            Poll::Pending
        } else {
            Poll::Ready(finished)
        }
    })
    .await
}

#[test]
fn attached_pool_failed_credited_connect_fails_waiters_only_as_the_last_connect() {
    run_case(|context, _runtime| async move {
        let failed = CauseProbe::default();
        let last = CauseProbe::default();
        let later = CauseProbe::default();
        let (failed_connect, failed_gate) = gated_cause(failed.failure(false));
        let (last_connect, last_gate) = gated_cause(last.failure(false));
        let plans = RefCell::new(VecDeque::from([
            Box::pin(failed_connect)
                as LocalClientFuture<Result<ClientSession, AttachedConnectFailure>>,
            Box::pin(last_connect),
            Box::pin(std::future::ready(Err(later.failure(false)))),
        ]));
        let calls = Rc::new(Cell::new(0));
        let factory_calls = Rc::clone(&calls);
        let pool = cause_pool(
            &context,
            AttachedPoolParams {
                pending_connect_credit: 2,
                max_waiters: 4,
                ..params(2)
            },
            cause_factory(move |_, _, _| {
                factory_calls.set(factory_calls.get() + 1);
                plans.borrow_mut().pop_front().unwrap()
            }),
        );
        let progress = Rc::new(CausePending::default());
        pool.set_observer(Some(progress.clone())).unwrap();
        let mut waiters: Vec<_> = (0..4)
            .map(|_| pool.get_session_with_reservation(CancellationToken::never()))
            .collect();
        for waiter in &mut waiters {
            pending(waiter.as_mut()).await;
        }
        assert_eq!(progress.count.get(), 2);
        failed_gate.entered.await.unwrap();
        last_gate.entered.await.unwrap();
        failed_gate.release.send(()).unwrap();
        progress.wait_for(1).await;
        assert_eq!(failed.drops.load(Ordering::SeqCst), 1);
        for waiter in &mut waiters {
            pending(waiter.as_mut()).await;
        }
        assert_eq!(
            progress.count.get(),
            1,
            "waiters of a failed connect stay queued without redialing"
        );
        assert_eq!(calls.get(), 2);
        last_gate.release.send(()).unwrap();
        progress.wait_for(0).await;
        let mut errors = Vec::new();
        for waiter in waiters {
            let error = acquisition_error(waiter.await);
            assert_cause(&error, &last);
            errors.push(error);
        }
        assert_eq!(calls.get(), 2);
        let error = acquisition_error(
            pool.get_session_with_reservation(CancellationToken::never())
                .await,
        );
        assert_cause(&error, &later);
        assert_eq!(calls.get(), 3);
        failed_gate.destroyed.await.unwrap();
        last_gate.destroyed.await.unwrap();
        drop((errors, error));
        pool.drain_and_join().await.unwrap();
    });
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let (fail, failure_release) = oneshot::channel::<()>();
        let pool = fixture.pool_with(
            &context,
            AttachedPoolParams {
                pending_connect_credit: 2,
                ..params(3)
            },
            fixture.scripted_factory(2, vec![None, Some(failure_release)]),
        );
        let progress = Rc::new(CausePending::default());
        pool.set_observer(Some(progress.clone())).unwrap();
        let held = pool
            .get_session_with_reservation(CancellationToken::never())
            .await
            .unwrap();
        let other = pool
            .get_session_with_reservation(CancellationToken::never())
            .await
            .unwrap();
        assert!(other.session.is_same_session(&held.session));
        let mut first = pool.get_session_with_reservation(CancellationToken::never());
        let mut second = pool.get_session_with_reservation(CancellationToken::never());
        pending(first.as_mut()).await;
        pending(second.as_mut()).await;
        assert_eq!(progress.count.get(), 1, "the second waiter is credited");
        fail.send(()).unwrap();
        progress.wait_for(0).await;
        pending(first.as_mut()).await;
        pending(second.as_mut()).await;
        assert_eq!(
            progress.count.get(),
            0,
            "a session exists, so the waiters neither fail nor redial"
        );
        assert_eq!(fixture.calls.get(), 2);
        drop(held.reservation);
        let first = first.await.unwrap();
        assert!(first.session.is_same_session(&held.session));
        pending(second.as_mut()).await;
        drop(first.reservation);
        let second = second.await.unwrap();
        assert!(second.session.is_same_session(&held.session));
        assert_eq!(fixture.calls.get(), 2);
        drop((second.reservation, other.reservation));
        pool.drain_and_join().await.unwrap();
    });
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let (fail, failure_release) = oneshot::channel::<()>();
        let pool = fixture.pool_with(
            &context,
            AttachedPoolParams {
                max_retiring: Some(1),
                pending_connect_credit: 2,
                ..params(3)
            },
            fixture.scripted_factory(1, vec![None, None, None, Some(failure_release)]),
        );
        let progress = Rc::new(CausePending::default());
        pool.set_observer(Some(progress.clone())).unwrap();
        let mut held = VecDeque::new();
        for _ in 0..3 {
            held.push_back(
                HeldStream::open(
                    pool.get_session_with_reservation(CancellationToken::never())
                        .await
                        .unwrap(),
                )
                .await,
            );
        }
        held[1].session.initiate_drain().unwrap();
        held[2].session.initiate_drain().unwrap();
        let mut originator = pool.get_session_with_reservation(CancellationToken::never());
        pending(originator.as_mut()).await;
        let mut blocked = pool.get_session_with_reservation(CancellationToken::never());
        pending(blocked.as_mut()).await;
        assert!(pool.full());
        assert_eq!(
            progress.count.get(),
            1,
            "only the retiring bound holds the second waiter back"
        );
        fail.send(()).unwrap();
        progress.wait_for(0).await;
        pending(originator.as_mut()).await;
        pending(blocked.as_mut()).await;
        assert_eq!(
            progress.count.get(),
            0,
            "neither the failed connect's originator nor the waiter the retiring bound held redials"
        );
        assert_eq!(fixture.calls.get(), 4);
        let active = held.pop_front().unwrap().finish().await;
        let originator = originator.await.unwrap();
        assert!(originator.session.is_same_session(&active));
        pending(blocked.as_mut()).await;
        drop(originator.reservation);
        let blocked = blocked.await.unwrap();
        assert!(blocked.session.is_same_session(&active));
        assert_eq!(fixture.calls.get(), 4);
        drop(blocked.reservation);
        for stream in held {
            stream.finish().await;
        }
        pool.drain_and_join().await.unwrap();
    });
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let (fail, failure_release) = oneshot::channel::<()>();
        let (redial_fail, redial_release) = oneshot::channel::<()>();
        let pool = fixture.pool_with(
            &context,
            AttachedPoolParams {
                max_retiring: Some(1),
                max_waiters: 2,
                ..params(2)
            },
            fixture.scripted_factory(1, vec![None, Some(failure_release), Some(redial_release)]),
        );
        let progress = Rc::new(CausePending::default());
        pool.set_observer(Some(progress.clone())).unwrap();
        let held = pool
            .get_session_with_reservation(CancellationToken::never())
            .await
            .unwrap();
        let mut originator = pool.get_session_with_reservation(CancellationToken::never());
        let mut queued = pool.get_session_with_reservation(CancellationToken::never());
        pending(originator.as_mut()).await;
        pending(queued.as_mut()).await;
        assert_eq!(
            progress.count.get(),
            1,
            "credit 1 credits no waiter, and the pending connect closes the gate"
        );
        fail.send(()).unwrap();
        progress.wait_for(0).await;
        pending(originator.as_mut()).await;
        pending(queued.as_mut()).await;
        assert_eq!(
            progress.count.get(),
            1,
            "with credit 1 the failed connect's slot wakes one waiter, which redials"
        );
        until("the redial reaches the factory", || {
            fixture.calls.get() == 3
        })
        .await;
        drop(held.reservation);
        let served = queued
            .await
            .expect("the session serves the waiter that did not redial");
        assert!(served.session.is_same_session(&held.session));
        drop(served.reservation);
        let redialed = originator.await.unwrap();
        assert!(redialed.session.is_same_session(&held.session));
        drop(redialed.reservation);
        redial_fail.send(()).unwrap();
        progress.wait_for(0).await;
        assert_eq!(fixture.calls.get(), 3);
        pool.drain_and_join().await.unwrap();
    });
}

#[test]
fn attached_pool_detach_transfers_join_responsibility() {
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let first = fixture.pool(&context, 1, fixture.factory(CodecProtocol::Http2));
        let session = exchange(
            first
                .get_session_with_reservation(CancellationToken::never())
                .await
                .unwrap(),
        )
        .await;
        fixture.idle(&session).await;
        let detached = first
            .detach_idle_session()
            .expect("idle session transfers explicitly");
        let second = fixture.pool(&context, 1, fixture.factory(CodecProtocol::Http2));
        second.insert_idle_session(detached).unwrap();
        let leased = second
            .get_session_with_reservation(CancellationToken::never())
            .await
            .unwrap();
        assert!(leased.session.is_same_session(&session));
        first
            .drain_and_join()
            .await
            .expect("former owner does not wait for the transferred lease");
        assert!(!session.is_draining());
        let mut joined = second.drain_and_join();
        pending(joined.as_mut()).await;
        drop(leased.reservation);
        joined.await.unwrap();
        assert!(session.is_finished());
        assert_eq!(fixture.calls.get(), 1);
    });
}

#[test]
fn attached_pool_dropped_join_observer_does_not_abandon_cleanup() {
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let pool = fixture.pool(&context, 1, fixture.factory(CodecProtocol::Http11));
        let held = pool
            .get_session_with_reservation(CancellationToken::never())
            .await
            .unwrap();
        let mut receipt = pool.drain_and_join();
        drop(pool.drain_and_join());
        assert!(matches!(
            pool.get_session_with_reservation(CancellationToken::never())
                .await,
            Err(PoolError::Draining)
        ));
        pending(receipt.as_mut()).await;
        drop(pool);
        drop(held.reservation);
        receipt.await.unwrap();
        assert!(held.session.is_finished());
    });
}

struct CloseGate {
    started: Option<oneshot::Sender<()>>,
    released: oneshot::Receiver<()>,
    done: bool,
}

impl CloseGate {
    fn poll(&mut self, context: &mut Context<'_>) -> Poll<()> {
        if self.done {
            return Poll::Ready(());
        }
        if let Some(started) = self.started.take() {
            let _ = started.send(());
        }
        if Pin::new(&mut self.released).poll(context).is_pending() {
            return Poll::Pending;
        }
        self.done = true;
        Poll::Ready(())
    }
}

struct CloseIo {
    io: DuplexStream,
    enabled: Rc<Cell<bool>>,
    flush: CloseGate,
    shutdown: CloseGate,
    owner: ThreadId,
    drops: Arc<AtomicUsize>,
}

impl AsyncRead for CloseIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(context, buffer)
    }
}

impl AsyncWrite for CloseIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(context, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.enabled.get() && self.flush.poll(context).is_pending() {
            return Poll::Pending;
        }
        Pin::new(&mut self.io).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.enabled.get() && self.shutdown.poll(context).is_pending() {
            return Poll::Pending;
        }
        Pin::new(&mut self.io).poll_shutdown(context)
    }
}

impl Drop for CloseIo {
    fn drop(&mut self) {
        assert_eq!(std::thread::current().id(), self.owner);
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn attached_pool_session_join_precedes_transport_quiescence() {
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 2,
    })
    .unwrap();
    let runtime = owner.handle();
    let worker = runtime.worker(0).unwrap();
    let (flush_started, flush_observed) = oneshot::channel();
    let (flush_release, flush_released) = oneshot::channel();
    let (shutdown_started, shutdown_observed) = oneshot::channel();
    let (shutdown_release, shutdown_released) = oneshot::channel();
    let drops = Arc::new(AtomicUsize::new(0));
    let owner_drops = Arc::clone(&drops);
    let initialized = worker
        .call_local(move |context| async move {
            let fixture = Fixture::default();
            let (client, server) = tokio::io::duplex(16 * 1024);
            let enabled = Rc::new(Cell::new(false));
            let io = CloseIo {
                io: client,
                enabled: Rc::clone(&enabled),
                flush: CloseGate {
                    started: Some(flush_started),
                    released: flush_released,
                    done: false,
                },
                shutdown: CloseGate {
                    started: Some(shutdown_started),
                    released: shutdown_released,
                    done: false,
                },
                owner: std::thread::current().id(),
                drops: owner_drops,
            };
            let session = fixture.attach_io(&context, CodecProtocol::Http11, (io, server));
            let returned = RefCell::new(Some(session));
            let pool = fixture.pool(
                &context,
                1,
                factory(move |_, _| {
                    let session = returned.borrow_mut().take().unwrap();
                    Box::pin(async move { Ok(session) })
                }),
            );
            let session = exchange(
                pool.get_session_with_reservation(CancellationToken::never())
                    .await
                    .unwrap(),
            )
            .await;
            fixture.idle(&session).await;
            enabled.set(true);
            pool.drain_and_join()
                .await
                .expect("session/task receipt does not promise transport flush");
            assert!(session.is_finished());
        })
        .unwrap();
    wait(async {
        initialized
            .await
            .expect("pool initializer and drain observer finish before transport cleanup");
        flush_observed.await.unwrap();
        let mut quiesced = runtime.quiesce(Duration::from_secs(60));
        pending(Pin::new(&mut quiesced)).await;
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(worker.submit(|_| {}), Err(SubmitError::Closed));
        flush_release.send(()).unwrap();
        shutdown_observed.await.unwrap();
        pending(Pin::new(&mut quiesced)).await;
        shutdown_release.send(()).unwrap();
        quiesced.await.unwrap();
    });
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(owner.join().unwrap().workers_forced, 0);
}

#[test]
fn attached_pool_expiration_retires_before_acquisition_error() {
    for timed_out in [false, true] {
        run_case(move |context, _runtime| async move {
            let fixture = Fixture::default();
            let mut options = params(1);
            options.max_waiters = 0;
            let pool = HttpSessionPool::try_with_attached_factory(
                context,
                options,
                TransportSecurity::Plaintext,
                fixture.factory(CodecProtocol::Http11),
            )
            .unwrap();
            pool.set_idle_session_observer(Some(Rc::new(IdleNotice(Rc::clone(&fixture.idle)))))
                .unwrap();
            let session = exchange(
                pool.get_session_with_reservation(CancellationToken::never())
                    .await
                    .unwrap(),
            )
            .await;
            fixture.idle(&session).await;
            pool.expire_sessions_for_test();
            let result = if timed_out {
                pool.acquisition_step_for_test(Instant::now() - TIMEOUT - TIMEOUT)
            } else {
                pool.get_session_with_reservation(CancellationToken::never())
                    .await
                    .map(|_| ())
            };
            assert!(matches!(
                (timed_out, result),
                (false, Err(PoolError::MaxWaiters)) | (true, Err(PoolError::Timeout))
            ));
            assert_eq!(pool.session_count(), 0);
            assert!(
                session.is_draining(),
                "retained expired handles must be explicitly retired on errors"
            );
            assert!(
                pool.full(),
                "retirement retains ownership until the session joins"
            );
            pool.drain_and_join().await.unwrap();
            assert!(session.is_finished());
            assert!(!pool.full());
            assert_eq!(pool.task_count_for_test(), 0);
            assert_eq!(fixture.calls.get(), 1);
        });
    }
}

struct ReadySessionDropPanic {
    session: Option<ClientSession>,
    disposals: Rc<Cell<usize>>,
}

impl Future for ReadySessionDropPanic {
    type Output = Result<ClientSession, HttpError>;

    fn poll(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(self
            .session
            .take()
            .expect("one successful factory handoff")))
    }
}

impl Drop for ReadySessionDropPanic {
    fn drop(&mut self) {
        self.disposals.set(self.disposals.get() + 1);
        panic!("completed factory future destructor panicked");
    }
}

#[test]
fn attached_pool_completed_factory_drop_preserves_handoff() {
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let session = RefCell::new(Some(fixture.attach(&context, CodecProtocol::Http11)));
        let disposals = Rc::new(Cell::new(0));
        let factory_disposals = Rc::clone(&disposals);
        let make = factory(
            move |_, _| -> LocalClientFuture<Result<ClientSession, HttpError>> {
                if let Some(session) = session.borrow_mut().take() {
                    Box::pin(ReadySessionDropPanic {
                        session: Some(session),
                        disposals: Rc::clone(&factory_disposals),
                    })
                } else {
                    Box::pin(async { Err(HttpError::new(HttpErrorCode::CONNECT_ERROR)) })
                }
            },
        );
        let pool = fixture.pool(&context, 1, make);
        let leased = pool
            .get_session_with_reservation(CancellationToken::never())
            .await
            .unwrap();
        assert_eq!(disposals.get(), 1);
        let mut joined = pool.drain_and_join();
        pending(joined.as_mut()).await;
        assert!(
            !leased.session.is_finished(),
            "failure cannot certify an outstanding session lease"
        );
        drop(leased.reservation);
        assert!(matches!(joined.await, Err(PoolError::CleanupPanicked)));
        assert!(leased.session.is_finished());
        assert!(!pool.full());
        assert_eq!(pool.task_count_for_test(), 0);
        assert!(matches!(
            pool.drain_and_join().await,
            Err(PoolError::CleanupPanicked)
        ));
    });
}

struct SecondaryPanicPayload(Arc<AtomicUsize>);

impl Drop for SecondaryPanicPayload {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct DroppingPanicPayload {
    primary: Arc<AtomicUsize>,
    secondary: Arc<AtomicUsize>,
}

impl Drop for DroppingPanicPayload {
    fn drop(&mut self) {
        self.primary.fetch_add(1, Ordering::SeqCst);
        panic_any(SecondaryPanicPayload(Arc::clone(&self.secondary)));
    }
}

struct JoinPanicPayload {
    primary: Arc<AtomicUsize>,
    secondary: Arc<AtomicUsize>,
    disposed: Option<oneshot::Sender<ThreadId>>,
    owner: ThreadId,
}

impl Drop for JoinPanicPayload {
    fn drop(&mut self) {
        assert_eq!(current().id(), self.owner);
        self.primary.fetch_add(1, Ordering::SeqCst);
        if let Some(disposed) = self.disposed.take() {
            let _ = disposed.send(self.owner);
        }
        panic_any(SecondaryPanicPayload(Arc::clone(&self.secondary)));
    }
}

struct FactoryJoinDropProbe {
    owner: ThreadId,
    drops: Rc<Cell<usize>>,
}

impl Drop for FactoryJoinDropProbe {
    fn drop(&mut self) {
        assert_eq!(current().id(), self.owner);
        self.drops.set(self.drops.get() + 1);
    }
}

struct JoinedSessionProbe {
    owner: ThreadId,
    destroyed: Rc<Cell<usize>>,
}

impl LifecycleObserver for JoinedSessionProbe {
    fn on_event(&self, event: LifecycleEvent) {
        assert_eq!(current().id(), self.owner);
        if matches!(event, LifecycleEvent::Destroy) {
            self.destroyed.set(self.destroyed.get() + 1);
        }
    }
}

enum PayloadJoinPlan {
    Session,
    Gated {
        started: oneshot::Sender<()>,
        cancelled: oneshot::Sender<()>,
        finish: oneshot::Receiver<()>,
        completed: oneshot::Sender<()>,
    },
    Panic {
        started: oneshot::Sender<()>,
        throw: oneshot::Receiver<()>,
        disposed: oneshot::Sender<ThreadId>,
    },
}

fn factory_payload_join_case(during_drain: bool) {
    run_case(move |context, _runtime| async move {
        let fixture = Fixture::default();
        let producer = fixture.clone();
        let owner = context.owner_thread();
        let (gated_started, gated_ready) = oneshot::channel();
        let (cancelled, cancellation_seen) = oneshot::channel();
        let (finish, finished) = oneshot::channel();
        let (completed, completion_seen) = oneshot::channel();
        let (panic_started, panic_ready) = oneshot::channel();
        let (throw, thrown) = oneshot::channel();
        let (disposed, disposal_seen) = oneshot::channel();
        let plans = RefCell::new(VecDeque::from([
            PayloadJoinPlan::Session,
            PayloadJoinPlan::Gated {
                started: gated_started,
                cancelled,
                finish: finished,
                completed,
            },
            PayloadJoinPlan::Panic {
                started: panic_started,
                throw: thrown,
                disposed,
            },
        ]));
        let primary = Arc::new(AtomicUsize::new(0));
        let secondary = Arc::new(AtomicUsize::new(0));
        let factory_primary = Arc::clone(&primary);
        let factory_secondary = Arc::clone(&secondary);
        let drops = Rc::new(Cell::new(0));
        let factory_drops = Rc::clone(&drops);
        let destroyed = Rc::new(Cell::new(0));
        let session_destroyed = Rc::clone(&destroyed);
        let make = factory(
            move |context, cancellation| -> LocalClientFuture<Result<ClientSession, HttpError>> {
                producer.calls.set(producer.calls.get() + 1);
                match plans
                    .borrow_mut()
                    .pop_front()
                    .expect("three admitted owners")
                {
                    PayloadJoinPlan::Session => {
                        let mut options = ClientAttachOptions::new(
                            CodecProtocol::Http11,
                            TransportSecurity::Plaintext,
                        );
                        options.session.lifecycle_observer = Some(Rc::new(JoinedSessionProbe {
                            owner,
                            destroyed: Rc::clone(&session_destroyed),
                        }));
                        let session = producer.attach_options(&context, options);
                        Box::pin(async move { Ok(session) })
                    }
                    PayloadJoinPlan::Gated {
                        started,
                        cancelled,
                        finish,
                        completed,
                    } => {
                        let cleanup = FactoryJoinDropProbe {
                            owner,
                            drops: Rc::clone(&factory_drops),
                        };
                        Box::pin(async move {
                            let _cleanup = cleanup;
                            started.send(()).unwrap();
                            let baton = CancellableBaton::new();
                            assert_eq!(baton.wait(cancellation).await, WaitStatus::Cancelled);
                            cancelled.send(()).unwrap();
                            finish.await.unwrap();
                            completed.send(()).unwrap();
                            Err(HttpError::new(HttpErrorCode::CORO_CANCELLED))
                        })
                    }
                    PayloadJoinPlan::Panic {
                        started,
                        throw,
                        disposed,
                    } => {
                        let cleanup = FactoryJoinDropProbe {
                            owner,
                            drops: Rc::clone(&factory_drops),
                        };
                        let primary = Arc::clone(&factory_primary);
                        let secondary = Arc::clone(&factory_secondary);
                        Box::pin(async move {
                            let _cleanup = cleanup;
                            started.send(()).unwrap();
                            throw.await.unwrap();
                            panic_any(JoinPanicPayload {
                                primary,
                                secondary,
                                disposed: Some(disposed),
                                owner,
                            });
                        })
                    }
                }
            },
        );
        let pool = fixture.pool(&context, 3, make);
        let leased = pool
            .get_session_with_reservation(CancellationToken::never())
            .await
            .unwrap();
        let mut gated = pool.get_session_with_reservation(CancellationToken::never());
        pending(gated.as_mut()).await;
        gated_ready.await.unwrap();
        let mut panicking = pool.get_session_with_reservation(CancellationToken::never());
        pending(panicking.as_mut()).await;
        panic_ready.await.unwrap();
        let mut joined = if during_drain {
            let joined = pool.drain_and_join();
            tokio::time::timeout(TIMEOUT, async {
                while pool.task_count_for_test() != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("supervisor takes the still-gated group for its join loop");
            throw.send(()).unwrap();
            assert_eq!(disposal_seen.await.unwrap(), owner);
            joined
        } else {
            throw.send(()).unwrap();
            assert_eq!(disposal_seen.await.unwrap(), owner);
            assert!(
                !leased.session.is_draining(),
                "the panic was reaped while the pool remained open"
            );
            assert_eq!(
                pool.task_count_for_test(),
                2,
                "open reaping retained both remaining owners"
            );
            pool.set_max_connection_attempts(1)
                .expect("pool admission is still open after reaping");
            pool.drain_and_join()
        };
        cancellation_seen.await.unwrap();
        assert!(matches!(gated.await, Err(PoolError::Draining)));
        assert!(matches!(panicking.await, Err(PoolError::Draining)));
        assert_eq!(primary.load(Ordering::SeqCst), 1);
        assert_eq!(secondary.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.calls.get(), 3);
        assert_eq!(
            drops.get(),
            1,
            "the other factory's cleanup is still held at its gate"
        );
        assert_eq!(destroyed.get(), 0);
        pending(joined.as_mut()).await;
        finish.send(()).unwrap();
        completion_seen.await.unwrap();
        assert_eq!(drops.get(), 2);
        pending(joined.as_mut()).await;
        assert_eq!(
            destroyed.get(),
            0,
            "session ownership still waits for the outstanding lease"
        );
        drop(leased.reservation);
        assert!(matches!(
            joined.await,
            Err(PoolError::TaskJoin {
                source: TaskGroupError::TaskPanicked
            })
        ));
        assert!(leased.session.is_finished());
        assert_eq!(destroyed.get(), 1);
        assert_eq!(drops.get(), 2);
        assert!(!pool.full());
        assert_eq!(pool.task_count_for_test(), 0);
        assert!(matches!(
            pool.drain_and_join().await,
            Err(PoolError::TaskJoin {
                source: TaskGroupError::TaskPanicked
            })
        ));
    });
}

#[test]
fn attached_pool_factory_panic_payload_open_reaping_joins_remaining_owners() {
    factory_payload_join_case(false);
}

#[test]
fn attached_pool_factory_panic_payload_during_join_joins_remaining_owners() {
    factory_payload_join_case(true);
}

struct PayloadPanicWake {
    primary: Arc<AtomicUsize>,
    secondary: Arc<AtomicUsize>,
}

impl Wake for PayloadPanicWake {
    fn wake(self: Arc<Self>) {
        panic_any(DroppingPanicPayload {
            primary: Arc::clone(&self.primary),
            secondary: Arc::clone(&self.secondary),
        });
    }
}

#[test]
fn attached_pool_panicking_cancellation_payload_still_joins_all_owners() {
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let producer = fixture.clone();
        let (started, ready) = oneshot::channel();
        let (cancelled, cancellation_seen) = oneshot::channel();
        let (finish, finished) = oneshot::channel();
        let (completed, completion_seen) = oneshot::channel();
        let gates = RefCell::new(Some((started, cancelled, finished, completed)));
        let primary = Arc::new(AtomicUsize::new(0));
        let secondary = Arc::new(AtomicUsize::new(0));
        let factory_primary = Arc::clone(&primary);
        let factory_secondary = Arc::clone(&secondary);
        let cleanup = Rc::new(Cell::new(false));
        let cleanup_done = Rc::clone(&cleanup);
        let make = factory(
            move |context, cancellation| -> LocalClientFuture<Result<ClientSession, HttpError>> {
                producer.calls.set(producer.calls.get() + 1);
                if producer.calls.get() == 1 {
                    let session = producer.attach(&context, CodecProtocol::Http11);
                    return Box::pin(async move { Ok(session) });
                }
                let (started, cancelled, finished, completed) = gates.borrow_mut().take().unwrap();
                let primary = Arc::clone(&factory_primary);
                let secondary = Arc::clone(&factory_secondary);
                let cleanup_done = Rc::clone(&cleanup_done);
                Box::pin(async move {
                    let panicking_baton = CancellableBaton::new();
                    let mut panicking_wait = Box::pin(panicking_baton.wait(cancellation.clone()));
                    let waker = Waker::from(Arc::new(PayloadPanicWake { primary, secondary }));
                    assert!(
                        panicking_wait
                            .as_mut()
                            .poll(&mut Context::from_waker(&waker))
                            .is_pending()
                    );
                    started.send(()).unwrap();
                    let notified_baton = CancellableBaton::new();
                    assert_eq!(
                        notified_baton.wait(cancellation).await,
                        WaitStatus::Cancelled
                    );
                    cancelled.send(()).unwrap();
                    finished.await.unwrap();
                    cleanup_done.set(true);
                    completed.send(()).unwrap();
                    Err(HttpError::new(HttpErrorCode::CORO_CANCELLED))
                })
            },
        );
        let pool = fixture.pool(&context, 2, make);
        let leased = pool
            .get_session_with_reservation(CancellationToken::never())
            .await
            .unwrap();
        let mut second = pool.get_session_with_reservation(CancellationToken::never());
        pending(second.as_mut()).await;
        ready.await.unwrap();
        let mut joined = pool.drain_and_join();
        assert_eq!(primary.load(Ordering::SeqCst), 1);
        assert_eq!(
            secondary.load(Ordering::SeqCst),
            1,
            "secondary panic payload is retired"
        );
        assert!(
            leased.session.is_draining(),
            "payload disposal cannot skip established-session drain"
        );
        assert!(matches!(second.await, Err(PoolError::Draining)));
        cancellation_seen.await.unwrap();
        pending(joined.as_mut()).await;
        assert!(!cleanup.get());
        finish.send(()).unwrap();
        completion_seen.await.unwrap();
        assert!(cleanup.get());
        pending(joined.as_mut()).await;
        drop(leased.reservation);
        assert!(matches!(joined.await, Err(PoolError::CleanupPanicked)));
        assert!(leased.session.is_finished());
        assert!(!pool.full());
        assert_eq!(pool.task_count_for_test(), 0);
        assert!(matches!(
            pool.drain_and_join().await,
            Err(PoolError::CleanupPanicked)
        ));
    });
}

struct ExpirationPanic(Rc<Cell<usize>>);

impl LifecycleObserver for ExpirationPanic {
    fn on_event(&self, event: LifecycleEvent) {
        if matches!(event, LifecycleEvent::DrainStarted(_)) {
            self.0.set(self.0.get() + 1);
            panic!("expired-session drain callback panicked");
        }
    }
}

#[test]
fn attached_pool_expiration_panic_rolls_back_unstarted_attempt() {
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let producer = fixture.clone();
        let callbacks = Rc::new(Cell::new(0));
        let factory_callbacks = Rc::clone(&callbacks);
        let make = factory(move |context, _| {
            producer.calls.set(producer.calls.get() + 1);
            let mut options =
                ClientAttachOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext);
            if producer.calls.get() == 1 {
                options.session.lifecycle_observer =
                    Some(Rc::new(ExpirationPanic(Rc::clone(&factory_callbacks))));
            }
            let session = producer.attach_options(&context, options);
            Box::pin(async move { Ok(session) })
        });
        let pool = fixture.pool(&context, 2, make);
        let session = exchange(
            pool.get_session_with_reservation(CancellationToken::never())
                .await
                .unwrap(),
        )
        .await;
        fixture.idle(&session).await;
        pool.expire_sessions_for_test();
        let mut replacement = pool.get_session_with_reservation(CancellationToken::never());
        let panicked = poll_fn(|context| {
            Poll::Ready(catch_unwind(AssertUnwindSafe(|| {
                replacement.as_mut().poll(context)
            })))
        })
        .await;
        assert!(
            panicked.is_err(),
            "native expiry callback panic still propagates"
        );
        drop(panicked);
        drop(replacement);
        assert_eq!(callbacks.get(), 1);
        assert_eq!(
            fixture.calls.get(),
            1,
            "replacement reserved capacity but never started its factory"
        );
        assert!(session.is_draining());
        assert!(
            !pool.full(),
            "the unstarted replacement rolled its slot back synchronously"
        );
        session.wait_closed().await.unwrap();
        pool.set_max_connections(1).unwrap();
        let replacement = pool
            .get_session_with_reservation(CancellationToken::never())
            .await
            .unwrap();
        assert_eq!(fixture.calls.get(), 2);
        drop(replacement.reservation);
        pool.drain_and_join().await.unwrap();
        assert!(!pool.full());
        assert_eq!(pool.task_count_for_test(), 0);
    });
}

#[test]
fn attached_pool_late_invalid_failure_survives_pool_drop() {
    for already_finished in [false, true] {
        run_case(move |context, _runtime| async move {
            let fixture = Fixture::default();
            let producer = fixture.clone();
            let (started, ready) = oneshot::channel();
            let (cancelled, cancellation_seen) = oneshot::channel();
            let (finish, finished) = oneshot::channel();
            let gates = RefCell::new(Some((started, cancelled, finished)));
            let handed = Rc::new(RefCell::new(None));
            let returned = Rc::clone(&handed);
            let make = factory(move |context, cancellation| {
                let session = producer.attach(&context, CodecProtocol::Http11);
                returned.borrow_mut().replace(session.clone());
                let (started, cancelled, finished) = gates.borrow_mut().take().unwrap();
                Box::pin(async move {
                    started.send(()).unwrap();
                    let baton = CancellableBaton::new();
                    assert_eq!(baton.wait(cancellation).await, WaitStatus::Cancelled);
                    cancelled.send(()).unwrap();
                    finished.await.unwrap();
                    session.cancel();
                    if already_finished {
                        assert_eq!(
                            session
                                .wait_closed()
                                .await
                                .expect_err("cancelled handoff closes with failure")
                                .code(),
                            HttpErrorCode::CORO_CANCELLED
                        );
                        assert!(session.is_finished());
                    } else {
                        assert!(session.is_draining());
                        assert!(!session.is_finished());
                    }
                    Ok(session)
                })
            });
            let pool = fixture.pool(&context, 1, make);
            let mut acquisition = pool.get_session_with_reservation(CancellationToken::never());
            pending(acquisition.as_mut()).await;
            ready.await.unwrap();
            let mut joined = pool.drain_and_join();
            drop(acquisition);
            drop(pool);
            cancellation_seen.await.unwrap();
            pending(joined.as_mut()).await;
            finish.send(()).unwrap();
            let Err(PoolError::SessionJoin { source }) = joined.await else {
                panic!(
                    "rooted completion must retain the late invalid session failure without PoolState"
                );
            };
            assert_eq!(source.code(), HttpErrorCode::CORO_CANCELLED);
            assert!(handed.borrow().as_ref().unwrap().is_finished());
        });
    }
}

struct PanickingObserver;

impl PoolObserver for PanickingObserver {
    fn on_pending_connections_changed(&self, _delta: i32) {
        panic!("injected observer panic");
    }
}

struct PanickingWake;

impl Wake for PanickingWake {
    fn wake(self: Arc<Self>) {
        panic!("injected cancellation wake panic");
    }
}

enum PanicPlan {
    Block {
        started: oneshot::Sender<()>,
        cancelled: oneshot::Sender<()>,
        finish: oneshot::Receiver<()>,
    },
    Panic(oneshot::Sender<()>),
}

#[test]
fn attached_pool_panics_do_not_skip_remaining_joins() {
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let (first_started, first_ready) = oneshot::channel();
        let (panic_started, panic_ready) = oneshot::channel();
        let (cancelled, cancellation_seen) = oneshot::channel();
        let (finish, finished) = oneshot::channel();
        let plans = RefCell::new(VecDeque::from([
            PanicPlan::Block {
                started: first_started,
                cancelled,
                finish: finished,
            },
            PanicPlan::Panic(panic_started),
        ]));
        let cleanup = Rc::new(Cell::new(false));
        let cleanup_done = Rc::clone(&cleanup);
        let make = factory(
            move |_context, cancellation| -> LocalClientFuture<Result<ClientSession, HttpError>> {
                match plans
                    .borrow_mut()
                    .pop_front()
                    .expect("two admitted factories")
                {
                    PanicPlan::Block {
                        started,
                        cancelled,
                        finish,
                    } => {
                        let cleanup_done = Rc::clone(&cleanup_done);
                        Box::pin(async move {
                            started.send(()).unwrap();
                            let baton = CancellableBaton::new();
                            assert_eq!(baton.wait(cancellation).await, WaitStatus::Cancelled);
                            cancelled.send(()).unwrap();
                            finish.await.unwrap();
                            cleanup_done.set(true);
                            Err(HttpError::new(HttpErrorCode::CORO_CANCELLED))
                        })
                    }
                    PanicPlan::Panic(started) => Box::pin(async move {
                        started.send(()).unwrap();
                        panic!("injected factory panic");
                    }),
                }
            },
        );
        let pool = fixture.pool(&context, 2, make);
        pool.set_observer(Some(Rc::new(PanickingObserver))).unwrap();
        let mut first = Box::pin(pool.get_session_with_reservation(CancellationToken::never()));
        pending(first.as_mut()).await;
        first_ready.await.unwrap();
        let mut second = Box::pin(pool.get_session_with_reservation(CancellationToken::never()));
        pending(second.as_mut()).await;
        panic_ready.await.unwrap();
        let mut joined = pool.drain_and_join();
        assert!(matches!(first.await, Err(PoolError::Draining)));
        assert!(matches!(second.await, Err(PoolError::Draining)));
        cancellation_seen.await.unwrap();
        pending(joined.as_mut()).await;
        assert!(
            !cleanup.get(),
            "a task failure must not skip the blocked task's cleanup"
        );
        finish.send(()).unwrap();
        assert!(matches!(
            joined.await,
            Err(PoolError::TaskJoin {
                source: TaskGroupError::TaskPanicked
            })
        ));
        assert!(cleanup.get());
        assert_eq!(pool.task_count_for_test(), 0);
        assert!(matches!(
            pool.drain_and_join().await,
            Err(PoolError::TaskJoin {
                source: TaskGroupError::TaskPanicked
            })
        ));
    });
    run_case(|context, _runtime| async move {
        let fixture = Fixture::default();
        let (started, ready) = oneshot::channel();
        let (cancelled, cancellation_seen) = oneshot::channel();
        let (finish, finished) = oneshot::channel();
        let gates = RefCell::new(Some((started, cancelled, finished)));
        let cleanup = Rc::new(Cell::new(false));
        let cleanup_done = Rc::clone(&cleanup);
        let make = factory(move |_context, cancellation| {
            let (started, cancelled, finished) = gates.borrow_mut().take().unwrap();
            let cleanup_done = Rc::clone(&cleanup_done);
            Box::pin(async move {
                let panicking_baton = CancellableBaton::new();
                let mut panicking_wait = Box::pin(panicking_baton.wait(cancellation.clone()));
                let waker = Waker::from(Arc::new(PanickingWake));
                assert!(
                    panicking_wait
                        .as_mut()
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending()
                );
                started.send(()).unwrap();
                let notified_baton = CancellableBaton::new();
                assert_eq!(
                    notified_baton.wait(cancellation).await,
                    WaitStatus::Cancelled
                );
                cancelled.send(()).unwrap();
                finished.await.unwrap();
                cleanup_done.set(true);
                Err(HttpError::new(HttpErrorCode::CORO_CANCELLED))
            })
        });
        let pool = fixture.pool(&context, 1, make);
        let mut acquisition = pool.get_session_with_reservation(CancellationToken::never());
        pending(acquisition.as_mut()).await;
        ready.await.unwrap();
        let mut joined = pool.drain_and_join();
        assert!(matches!(acquisition.await, Err(PoolError::Draining)));
        cancellation_seen.await.unwrap();
        pending(joined.as_mut()).await;
        assert!(!cleanup.get());
        finish.send(()).unwrap();
        assert!(matches!(joined.await, Err(PoolError::CleanupPanicked)));
        assert!(cleanup.get());
        assert_eq!(pool.task_count_for_test(), 0);
        assert!(matches!(
            pool.drain_and_join().await,
            Err(PoolError::CleanupPanicked)
        ));
    });
}
