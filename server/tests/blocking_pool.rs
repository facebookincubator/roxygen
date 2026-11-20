/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Blocking-pool limits on the workers that `HttpServer` starts.

use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerHandle;

use crate::server::BlockingPoolLimits;
use crate::server::HttpServer;
use crate::server::HttpServerState;
use crate::server::ServerConfig;
use crate::server::ServerObserver;

/// Bounds every wait in these tests.
const TIMEOUT: Duration = Duration::from_secs(10);

/// The handler of a server that these tests send no request.
struct NoRequests;

impl HttpHandler for NoRequests {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        unreachable!("these tests send no request")
    }
}

/// Records which workers reported their stop.
#[derive(Default)]
struct StoppedWorkers(Mutex<Vec<usize>>);

impl ServerObserver for StoppedWorkers {
    fn on_thread_stop(&self, worker_index: usize) {
        self.0.lock().unwrap().push(worker_index);
    }
}

/// Runs three blocking tasks of 50 ms each on `worker`'s runtime and returns
/// the most that ran at once.
fn peak_blocking_concurrency(worker: &WorkerHandle) -> usize {
    let probe = worker
        .call_local(|_| async {
            let running = Arc::new(AtomicUsize::new(0));
            let peak = Arc::new(AtomicUsize::new(0));
            let joins = (0..3)
                .map(|_| {
                    let running = Arc::clone(&running);
                    let peak = Arc::clone(&peak);
                    tokio::task::spawn_blocking(move || {
                        let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(now, Ordering::SeqCst);
                        std::thread::sleep(Duration::from_millis(50));
                        running.fetch_sub(1, Ordering::SeqCst);
                    })
                })
                .collect::<Vec<_>>();
            for join in joins {
                join.await.expect("blocking task completes");
            }
            peak.load(Ordering::SeqCst)
        })
        .expect("the worker admits the probe");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async { tokio::time::timeout(TIMEOUT, probe).await })
        .expect("the probe completes in time")
        .expect("the probe runs")
}

#[test]
fn server_config_blocking_limits_reach_every_worker() {
    let handles = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&handles);
    let stopped = Arc::new(StoppedWorkers::default());
    let config = ServerConfig {
        num_io_threads: 2,
        shutdown_on_signals: Vec::new(),
        worker_blocking_pool: BlockingPoolLimits::new(NonZeroUsize::new(1), None),
        ..ServerConfig::default()
    };
    let server = HttpServer::start_with_worker_factory(
        config,
        Arc::new(move |context: WorkerContext, index| {
            captured
                .lock()
                .unwrap()
                .push((index, context.handle().clone()));
            Rc::new(NoRequests) as HttpHandlerRef
        }),
        Some(Arc::clone(&stopped) as Arc<dyn ServerObserver>),
    )
    .expect("server starts");
    let mut handles = std::mem::take(&mut *handles.lock().unwrap());
    handles.sort_by_key(|(index, _)| *index);
    assert_eq!(handles.len(), 2);

    for (index, worker) in &handles {
        assert_eq!(
            peak_blocking_concurrency(worker),
            1,
            "worker {index} runs one blocking task at a time"
        );
    }
    for (index, stats) in server.worker_poll_stats().iter().enumerate() {
        assert_eq!(
            stats.max_blocking_threads_live, 1,
            "worker {index}: {stats:?}"
        );
        assert!(stats.blocking_cap_hits >= 1, "worker {index}: {stats:?}");
    }

    assert!(server.drain());
    assert!(server.wait_for_stop().expect("workers join"));
    assert_eq!(server.state(), HttpServerState::Stopped);
    let mut stopped = stopped.0.lock().unwrap().clone();
    stopped.sort_unstable();
    assert_eq!(stopped, [0, 1], "each worker's exit callback runs");
}
