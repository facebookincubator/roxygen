/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Shared, cancellation-safe startup of a CONNECT runtime on first use.

use std::hash::Hash;
use std::io;
use std::mem;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use roxygen_runtime::panic::discard_panic;
use tokio::sync::watch;
use tokio::time::Instant;

use crate::error::ConnectError;
use crate::options::ConnectOptions;
use crate::runtime::ConnectClient;
use crate::runtime::ConnectRuntime;

/// A shared startup or shutdown failure of [`LazyConnectRuntime`].
#[derive(Clone, Debug, thiserror::Error)]
pub enum LazyConnectError {
    #[error("failed to spawn the CONNECT runtime startup thread: {0}")]
    Spawn(#[source] Arc<io::Error>),
    #[error("failed to start the CONNECT runtime: {0}")]
    Start(#[source] Arc<ConnectError>),
    #[error("the CONNECT runtime did not start within {0:?}")]
    TimedOut(Duration),
    #[error("the CONNECT runtime has been shut down")]
    Closed,
    #[error("the CONNECT runtime startup thread panicked")]
    Panicked,
    #[error("failed to shut down the CONNECT runtime: {0}")]
    Shutdown(#[source] Arc<ConnectError>),
}

type ShutdownResult = Result<(), LazyConnectError>;
type Started<K> = Result<(ConnectRuntime, ConnectClient<K>), ConnectError>;
type StartThread = thread::JoinHandle<ShutdownResult>;
type Spawn =
    dyn Fn(Box<dyn FnOnce() -> ShutdownResult + Send>) -> io::Result<StartThread> + Send + Sync;

struct Starter<K> {
    spawn: Arc<Spawn>,
    build: Arc<dyn Fn(ConnectOptions) -> Started<K> + Send + Sync>,
}

enum Phase<K> {
    New,
    Starting {
        deadline: Instant,
    },
    Ready {
        runtime: ConnectRuntime,
        client: ConnectClient<K>,
    },
    Failed(LazyConnectError),
    Closing,
    Closed(ShutdownResult),
}

struct State<K> {
    phase: Phase<K>,
    start_thread: Option<StartThread>,
    worker_thread: Option<thread::ThreadId>,
}

struct Shared<K> {
    options: ConnectOptions,
    startup_timeout: Duration,
    starter: Starter<K>,
    state: Mutex<State<K>>,
    changed: watch::Sender<()>,
    closed: Condvar,
}

struct Owner<K> {
    shared: Arc<Shared<K>>,
}

impl<K> Drop for Owner<K> {
    fn drop(&mut self) {
        let _ = self.shared.shutdown();
    }
}

/// Starts one CONNECT runtime on the first call to [`Self::client`].
///
/// Clones share the start and its outcome. Cancelling a caller does not cancel or restart the
/// worker's construction. A failed or timed-out start is never retried; a runtime that finishes
/// after the deadline is shut down on the startup thread.
///
/// Shutdown and the last owner's drop block while joining owned work, so run them outside
/// asynchronous Tokio workers. Like [`ConnectRuntime`], shutdown on the protocol worker itself
/// starts its shutdown without joining that worker; later shutdown calls return that result
/// without joining it either. Client handles do not own this runtime.
pub struct LazyConnectRuntime<K> {
    owner: Arc<Owner<K>>,
}

impl<K> Clone for LazyConnectRuntime<K> {
    fn clone(&self) -> Self {
        Self {
            owner: Arc::clone(&self.owner),
        }
    }
}

impl<K> LazyConnectRuntime<K>
where
    K: Eq + Hash + Clone + Send + 'static,
{
    /// Creates an unstarted owner. Options are validated when the first caller starts it.
    pub fn new(options: ConnectOptions, startup_timeout: Duration) -> Self {
        Self::with_starter(
            options,
            startup_timeout,
            Starter {
                spawn: Arc::new(|start| {
                    thread::Builder::new()
                        .name("roxygen-connect-start".to_owned())
                        .spawn(start)
                }),
                build: Arc::new(ConnectRuntime::new::<K>),
            },
        )
    }

    fn with_starter(
        options: ConnectOptions,
        startup_timeout: Duration,
        starter: Starter<K>,
    ) -> Self {
        Self {
            owner: Arc::new(Owner {
                shared: Arc::new(Shared {
                    options,
                    startup_timeout,
                    starter,
                    state: Mutex::new(State {
                        phase: Phase::New,
                        start_thread: None,
                        worker_thread: None,
                    }),
                    changed: watch::channel(()).0,
                    closed: Condvar::new(),
                }),
            }),
        }
    }

    /// Waits for the shared start, whose deadline begins on the first call.
    ///
    /// The startup thread is spawned from the thread polling this call, inheriting its process
    /// context, including its network namespace. A returned client may race shutdown and then
    /// fail its CONNECTs with [`crate::ConnectErrorKind::Closed`].
    pub async fn client(&self) -> Result<ConnectClient<K>, LazyConnectError> {
        let shared = &self.owner.shared;
        let mut changed = shared.changed.subscribe();
        loop {
            let deadline = {
                let mut state = shared
                    .state
                    .lock()
                    .expect("should not poison runtime state");
                if matches!(state.phase, Phase::New) {
                    state.phase = Phase::Starting {
                        deadline: Instant::now() + shared.startup_timeout,
                    };
                    let owned = Arc::clone(shared);
                    match (shared.starter.spawn)(Box::new(move || owned.start())) {
                        Ok(start_thread) => state.start_thread = Some(start_thread),
                        Err(error) => {
                            state.phase = Phase::Failed(LazyConnectError::Spawn(Arc::new(error)));
                        }
                    }
                }
                match &state.phase {
                    Phase::Starting { deadline } => *deadline,
                    Phase::Ready { client, .. } => return Ok(client.clone()),
                    Phase::Failed(error) => return Err(error.clone()),
                    Phase::Closing | Phase::Closed(_) => return Err(LazyConnectError::Closed),
                    Phase::New => unreachable!("should have started the runtime"),
                }
            };
            if tokio::time::timeout_at(deadline, changed.changed())
                .await
                .is_err()
            {
                let mut state = shared
                    .state
                    .lock()
                    .expect("should not poison runtime state");
                if matches!(state.phase, Phase::Starting { .. }) {
                    state.phase = Phase::Failed(LazyConnectError::TimedOut(shared.startup_timeout));
                }
                drop(state);
                shared.changed.send_replace(());
            }
        }
    }
}

impl<K> LazyConnectRuntime<K> {
    /// Permanently prevents startup, drains a started runtime, and joins its startup thread.
    /// Concurrent shutdown callers share the completed result.
    pub fn shutdown(&self) -> ShutdownResult {
        self.owner.shared.shutdown()
    }
}

impl<K> Shared<K> {
    fn start(&self) -> ShutdownResult {
        let built = catch_unwind(AssertUnwindSafe(|| {
            (self.starter.build)(self.options.clone())
        }))
        .map_err(|panic| {
            discard_panic(panic);
            LazyConnectError::Panicked
        })
        .and_then(|built| built.map_err(|error| LazyConnectError::Start(Arc::new(error))));
        let mut state = self.state.lock().expect("should not poison runtime state");
        if let Phase::Starting { deadline } = state.phase {
            if Instant::now() < deadline {
                state.phase = match built {
                    Ok((runtime, client)) => {
                        state.worker_thread = runtime.worker_thread_id();
                        Phase::Ready { runtime, client }
                    }
                    Err(error) => Phase::Failed(error),
                };
                drop(state);
                self.changed.send_replace(());
                return Ok(());
            }
            state.phase = Phase::Failed(LazyConnectError::TimedOut(self.startup_timeout));
        }
        drop(state);
        self.changed.send_replace(());
        match built {
            Ok((runtime, _)) => runtime
                .shutdown()
                .map_err(|error| LazyConnectError::Shutdown(Arc::new(error))),
            Err(_) => Ok(()),
        }
    }

    fn shutdown(&self) -> ShutdownResult {
        let mut state = self.state.lock().expect("should not poison runtime state");
        loop {
            match &state.phase {
                Phase::Closed(result) => return result.clone(),
                Phase::Closing if state.worker_thread == Some(thread::current().id()) => {
                    return Ok(());
                }
                Phase::Closing => {
                    state = self
                        .closed
                        .wait(state)
                        .expect("should not poison runtime state");
                }
                _ => break,
            }
        }
        let runtime = match mem::replace(&mut state.phase, Phase::Closing) {
            Phase::Ready { runtime, .. } => Some(runtime),
            _ => None,
        };
        let start_thread = state.start_thread.take();
        drop(state);
        self.changed.send_replace(());

        let stopped = runtime.map_or(Ok(()), |runtime| {
            runtime
                .shutdown()
                .map_err(|error| LazyConnectError::Shutdown(Arc::new(error)))
        });
        let joined = start_thread.map_or(Ok(()), |thread| {
            thread.join().unwrap_or(Err(LazyConnectError::Panicked))
        });
        let result = stopped.and(joined);
        self.state
            .lock()
            .expect("should not poison runtime state")
            .phase = Phase::Closed(result.clone());
        self.closed.notify_all();
        self.changed.send_replace(());
        result
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::future::pending;
    use std::pin::Pin;
    use std::sync::Barrier;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;

    use http::Request;
    use tokio::sync::oneshot;

    use super::*;
    use crate::error::ConnectErrorKind;
    use crate::runtime::ConnectedIo;
    use crate::runtime::Connector;
    use crate::runtime::DialError;

    const TEST_TIMEOUT: Duration = Duration::from_secs(10);

    struct NeverDials;

    impl Connector for NeverDials {
        fn connect(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<ConnectedIo, DialError>> + Send + 'static>>
        {
            Box::pin(pending())
        }
    }

    struct HeldStart {
        release: mpsc::Sender<()>,
        spawned: Arc<AtomicUsize>,
        client: Arc<Mutex<Option<ConnectClient<()>>>>,
    }

    impl HeldStart {
        fn new(timeout: Duration) -> (Self, LazyConnectRuntime<()>) {
            let (release, wait) = mpsc::channel();
            let wait = Mutex::new(wait);
            let spawned = Arc::new(AtomicUsize::new(0));
            let client = Arc::new(Mutex::new(None));
            let starter = Starter {
                spawn: Arc::new({
                    let spawned = Arc::clone(&spawned);
                    move |start| {
                        spawned.fetch_add(1, Ordering::SeqCst);
                        thread::Builder::new().spawn(start)
                    }
                }),
                build: Arc::new({
                    let client = Arc::clone(&client);
                    move |options| {
                        let _ = wait.lock().expect("start gate").recv_timeout(TEST_TIMEOUT);
                        let (runtime, started) = ConnectRuntime::new(options)?;
                        *client.lock().expect("client slot") = Some(started.clone());
                        Ok((runtime, started))
                    }
                }),
            };
            (
                Self {
                    release,
                    spawned,
                    client,
                },
                LazyConnectRuntime::with_starter(ConnectOptions::default(), timeout, starter),
            )
        }
    }

    fn executor() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime")
    }

    async fn poll_start(owner: &LazyConnectRuntime<()>) {
        assert!(
            tokio::time::timeout(Duration::ZERO, owner.client())
                .await
                .is_err()
        );
    }

    async fn assert_closed(client: &ConnectClient<()>) {
        let error = client
            .connect(
                (),
                Request::builder()
                    .method("CONNECT")
                    .uri("example.com:443")
                    .body(())
                    .expect("CONNECT"),
                Arc::new(NeverDials),
            )
            .await
            .expect_err("closed runtime");
        assert_eq!(error.kind(), ConnectErrorKind::Closed);
    }

    fn shutdown_on_thread(
        owner: &LazyConnectRuntime<()>,
    ) -> (thread::JoinHandle<()>, oneshot::Receiver<ShutdownResult>) {
        let owner = owner.clone();
        let (send, receive) = oneshot::channel();
        let joined = thread::spawn(move || {
            let _ = send.send(owner.shutdown());
        });
        (joined, receive)
    }

    async fn shut_down(owner: &LazyConnectRuntime<()>) {
        let (joined, result) = shutdown_on_thread(owner);
        tokio::time::timeout(TEST_TIMEOUT, result)
            .await
            .expect("shutdown returns")
            .expect("shutdown reply")
            .expect("shutdown succeeds");
        joined.join().expect("shutdown thread");
    }

    #[test]
    fn completed_start_survives_cancellation_of_its_waiter() {
        let (held, owner) = HeldStart::new(TEST_TIMEOUT);
        executor().block_on(async {
            let mut client = Box::pin(owner.client());
            assert!(
                tokio::time::timeout(Duration::ZERO, &mut client)
                    .await
                    .is_err()
            );
            held.release.send(()).expect("release start");
            let mut changed = owner.owner.shared.changed.subscribe();
            tokio::time::timeout(TEST_TIMEOUT, async {
                loop {
                    if matches!(
                        owner.owner.shared.state.lock().expect("state").phase,
                        Phase::Ready { .. }
                    ) {
                        break;
                    }
                    changed.changed().await.expect("owner alive");
                }
            })
            .await
            .expect("start finishes");
            drop(client);
            let client = owner.client().await.expect("keep the completed start");
            assert_eq!(held.spawned.load(Ordering::SeqCst), 1);
            shut_down(&owner).await;
            assert_closed(&client).await;
        });
    }

    #[test]
    fn cancelled_waiters_share_one_start_and_shutdown_joins_it() {
        let (held, owner) = HeldStart::new(TEST_TIMEOUT);
        executor().block_on(async {
            poll_start(&owner).await;
            poll_start(&owner).await;
            assert_eq!(held.spawned.load(Ordering::SeqCst), 1);
            let (joined, mut shutdown) = shutdown_on_thread(&owner);
            let mut changed = owner.owner.shared.changed.subscribe();
            tokio::time::timeout(TEST_TIMEOUT, async {
                loop {
                    if matches!(
                        owner.owner.shared.state.lock().expect("state").phase,
                        Phase::Closing
                    ) {
                        break;
                    }
                    changed.changed().await.expect("owner alive");
                }
            })
            .await
            .expect("shutdown starts");
            assert!(matches!(
                shutdown.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
            assert!(matches!(
                owner.client().await,
                Err(LazyConnectError::Closed)
            ));
            held.release.send(()).expect("release start");
            tokio::time::timeout(TEST_TIMEOUT, shutdown)
                .await
                .expect("shutdown returns")
                .expect("shutdown reply")
                .expect("shutdown succeeds");
            joined.join().expect("shutdown thread");
            let client = held
                .client
                .lock()
                .expect("client slot")
                .clone()
                .expect("late client");
            assert_closed(&client).await;
        });
    }

    #[test]
    fn shutdown_before_start_is_terminal() {
        let (held, owner) = HeldStart::new(TEST_TIMEOUT);
        owner.shutdown().expect("nothing to stop");
        executor().block_on(async {
            assert!(matches!(
                owner.client().await,
                Err(LazyConnectError::Closed)
            ));
        });
        assert_eq!(held.spawned.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn timeout_is_cached_and_the_late_runtime_is_closed() {
        let (held, owner) = HeldStart::new(Duration::from_millis(10));
        executor().block_on(async {
            for _ in 0..2 {
                assert!(matches!(
                    owner.client().await,
                    Err(LazyConnectError::TimedOut(_))
                ));
            }
            assert_eq!(held.spawned.load(Ordering::SeqCst), 1);
            held.release.send(()).expect("release start");
            shut_down(&owner).await;
            let client = held
                .client
                .lock()
                .expect("client slot")
                .clone()
                .expect("late client");
            assert_closed(&client).await;
        });
    }

    #[test]
    fn spawn_failure_is_cached() {
        let spawns = Arc::new(AtomicUsize::new(0));
        let owner = LazyConnectRuntime::<()>::with_starter(
            ConnectOptions::default(),
            TEST_TIMEOUT,
            Starter {
                spawn: Arc::new({
                    let spawns = Arc::clone(&spawns);
                    move |_| {
                        spawns.fetch_add(1, Ordering::SeqCst);
                        Err(io::ErrorKind::WouldBlock.into())
                    }
                }),
                build: Arc::new(|_| panic!("a failed spawn cannot build")),
            },
        );
        executor().block_on(async {
            for _ in 0..2 {
                assert!(matches!(
                    owner.client().await,
                    Err(LazyConnectError::Spawn(_))
                ));
            }
        });
        assert_eq!(spawns.load(Ordering::SeqCst), 1);
        owner.shutdown().expect("nothing to stop");
    }

    #[test]
    fn concurrent_clients_and_shutdown_leave_no_live_runtime() {
        for _ in 0..8 {
            let owner = LazyConnectRuntime::<()>::new(ConnectOptions::default(), TEST_TIMEOUT);
            let barrier = Arc::new(Barrier::new(3));
            let clients: Vec<_> = (0..2)
                .map(|_| {
                    let owner = owner.clone();
                    let barrier = Arc::clone(&barrier);
                    thread::spawn(move || {
                        barrier.wait();
                        executor().block_on(owner.client())
                    })
                })
                .collect();
            barrier.wait();
            owner.shutdown().expect("shutdown succeeds");
            for client in clients {
                match client.join().expect("client caller") {
                    Ok(client) => executor().block_on(assert_closed(&client)),
                    Err(error) => assert!(matches!(error, LazyConnectError::Closed)),
                }
            }
            owner.shutdown().expect("repeat shutdown succeeds");
        }
    }

    #[test]
    fn last_owner_drop_joins_a_held_start() {
        let (held, owner) = HeldStart::new(TEST_TIMEOUT);
        executor().block_on(async {
            poll_start(&owner).await;
            let shared = Arc::clone(&owner.owner.shared);
            let mut changed = shared.changed.subscribe();
            let (dropped, done) = oneshot::channel();
            let joined = thread::spawn(move || {
                drop(owner);
                let _ = dropped.send(());
            });
            tokio::time::timeout(TEST_TIMEOUT, async {
                loop {
                    if matches!(shared.state.lock().expect("state").phase, Phase::Closing) {
                        break;
                    }
                    changed.changed().await.expect("shared state alive");
                }
            })
            .await
            .expect("last owner starts shutdown");
            held.release.send(()).expect("release start");
            tokio::time::timeout(TEST_TIMEOUT, done)
                .await
                .expect("drop joins the start")
                .expect("drop reply");
            joined.join().expect("drop thread");
            let client = held
                .client
                .lock()
                .expect("client slot")
                .clone()
                .expect("late client");
            assert_closed(&client).await;
        });
    }

    struct OnConnect(Box<dyn Fn() + Send + Sync>);

    impl Connector for OnConnect {
        fn connect(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<ConnectedIo, DialError>> + Send + 'static>>
        {
            self.0();
            Box::pin(pending())
        }
    }

    #[test]
    fn worker_shutdown_does_not_wait_for_a_thread_joining_the_worker() {
        let owner = LazyConnectRuntime::<()>::new(ConnectOptions::default(), TEST_TIMEOUT);
        executor().block_on(async {
            let client = owner.client().await.expect("start");
            let (entered, entering) = oneshot::channel();
            let entered = Mutex::new(Some(entered));
            let (release, wait) = mpsc::channel();
            let wait = Mutex::new(wait);
            let (stopped, stopping) = oneshot::channel();
            let stopped = Mutex::new(Some(stopped));
            let connector = Arc::new(OnConnect(Box::new({
                let owner = owner.clone();
                move || {
                    entered
                        .lock()
                        .expect("entered slot")
                        .take()
                        .expect("one dial")
                        .send(())
                        .expect("waiter");
                    wait.lock()
                        .expect("worker gate")
                        .recv_timeout(TEST_TIMEOUT)
                        .expect("release worker");
                    let result = owner.shutdown();
                    let _ = stopped
                        .lock()
                        .expect("stopped slot")
                        .take()
                        .expect("one dial")
                        .send(result);
                }
            })));
            let mut connect = Box::pin(
                client.connect(
                    (),
                    Request::builder()
                        .method("CONNECT")
                        .uri("example.com:443")
                        .body(())
                        .expect("CONNECT"),
                    connector,
                ),
            );
            assert!(
                tokio::time::timeout(Duration::ZERO, &mut connect)
                    .await
                    .is_err()
            );
            tokio::time::timeout(TEST_TIMEOUT, entering)
                .await
                .expect("worker enters connector")
                .expect("entered reply");
            let (joined, shutdown) = shutdown_on_thread(&owner);
            let mut changed = owner.owner.shared.changed.subscribe();
            tokio::time::timeout(TEST_TIMEOUT, async {
                loop {
                    if matches!(
                        owner.owner.shared.state.lock().expect("state").phase,
                        Phase::Closing
                    ) {
                        break;
                    }
                    changed.changed().await.expect("owner alive");
                }
            })
            .await
            .expect("external shutdown begins");
            release.send(()).expect("release worker");
            tokio::time::timeout(TEST_TIMEOUT, stopping)
                .await
                .expect("worker-side shutdown returns")
                .expect("worker reply")
                .expect("worker-side shutdown succeeds");
            drop(connect);
            tokio::time::timeout(TEST_TIMEOUT, shutdown)
                .await
                .expect("external shutdown returns")
                .expect("shutdown reply")
                .expect("shutdown succeeds");
            joined.join().expect("shutdown thread");
            assert_closed(&client).await;
        });
    }
}
