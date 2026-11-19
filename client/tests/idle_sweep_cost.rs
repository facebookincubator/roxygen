/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A manual measurement for `DIV-032`: what the interval idle sweep, which runs
//! once per pool, costs an idle worker with one origin pool and with 128 (and with
//! none, for the worker's own idle cost), under the router's pool parameters. Run
//! it in an optimized build.

use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::tokio_host::HostedWorker;
use roxygen_runtime::tokio_host::HostedWorkerOptions;
use roxygen_runtime::worker::WorkerContext;
use roxygen_server::scoped_server::ScopedHttpServer;
use roxygen_server::server::ServerConfig;

use crate::connector::ConnectionParams;
use crate::connector::SessionParams;
use crate::pool::GetSessionResult;
use crate::pool::HttpSessionPool;
use crate::pool::IdleSweep;
use crate::pool::PoolParams;
use crate::pool::SessionReuseOrder;

/// The router's idle timeout (`backend_pool_idle_timeout_secs` unset), which is
/// also the sweep period.
const IDLE_TIMEOUT: Duration = Duration::from_secs(4);
/// Sweep periods in the measured window.
const PERIODS: u32 = 5;
const REPETITIONS: usize = 3;
const POOL_COUNTS: [usize; 3] = [0, 1, 128];

struct OkHandler;

impl HttpHandler for OkHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(async move {
            request.read_header_event().await?;
            let body = BufQueue::from_bytes(Bytes::from_static(b"ok"), 2)
                .expect("test response is bounded");
            Ok(HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_response(200, Some(body))
                    .expect("test response is valid"),
            )))
        })
    }
}

/// The router's pool to one engine (connect timeout aside).
fn router_pool(context: WorkerContext, engine: SocketAddr) -> HttpSessionPool {
    HttpSessionPool::new(
        context,
        SocketAddress::new(engine.ip().to_string(), engine.port()),
        PoolParams {
            max_connections: u32::MAX,
            max_connection_attempts: 1,
            connect_timeout: Duration::from_secs(5),
            max_age: Duration::ZERO,
            max_waiters: u16::MAX,
            idle_timeout: IDLE_TIMEOUT,
            reuse_order: SessionReuseOrder::MostRecentlyUsed,
            idle_sweep: IdleSweep::Interval,
            ..PoolParams::default()
        },
        ConnectionParams::default(),
        SessionParams {
            stream_read_timeout: Some(Duration::ZERO),
            connection_read_timeout: Some(Duration::ZERO),
            write_timeout: Some(Duration::ZERO),
            ..SessionParams::default()
        },
    )
}

/// One GET on the pool's next session, read to its end; the session then idles
/// with no waiter, which starts the pool's sweep.
async fn one_exchange(pool: &HttpSessionPool) {
    let GetSessionResult {
        reservation,
        session,
    } = pool
        .get_session_with_reservation(CancellationToken::never())
        .await
        .expect("acquisition succeeds");
    let request = HttpFixedSource::make_get_request(b"/", None).expect("test request is valid");
    let mut response = session
        .send_reserved_request(
            HttpSourceHolder::new(Box::new(request)),
            reservation,
            CancellationToken::never(),
        )
        .await
        .expect("request is sent");
    let head = response
        .source
        .read_header_event()
        .await
        .expect("response head");
    let mut eom = head.eom();
    while !eom {
        eom = response
            .source
            .read_body_event(u32::MAX)
            .await
            .expect("response body")
            .eom();
    }
    while session.active_transactions() != 0 {
        tokio::task::yield_now().await;
    }
}

fn thread_cpu_time() -> Duration {
    let mut now = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `now` is a valid, writable timespec for the call's duration.
    let result = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut now) };
    assert_eq!(result, 0, "the thread CPU clock is readable");
    Duration::new(
        u64::try_from(now.tv_sec).expect("thread CPU time is not negative"),
        u32::try_from(now.tv_nsec).expect("nanoseconds fit"),
    )
}

#[derive(Debug)]
struct IdleSweepCost {
    pools: usize,
    window: Duration,
    worker_cpu: Duration,
    sweeps: u64,
    pool_tasks: usize,
    sessions: usize,
}

/// Starts `pools` router pools to `engine` on a fresh `HostedWorker`, their
/// sweeps a period apart in all, waits until every session has retired, then
/// measures the worker over `PERIODS` sweep periods.
fn measure(engine: SocketAddr, pools: usize) -> IdleSweepCost {
    let worker = HostedWorker::spawn(HostedWorkerOptions {
        command_capacity: NonZeroUsize::new(4).expect("nonzero capacity"),
        thread_name: "rox-sweep".to_owned(),
        on_owner_thread_stop: None,
    })
    .expect("worker starts");
    let call = worker
        .handle()
        .call_local(move |context| async move {
            let count = u32::try_from(pools).expect("pool count fits");
            let pool_list: Vec<_> = (0..pools)
                .map(|_| router_pool(context.clone(), engine))
                .collect();
            let start = Instant::now();
            for (index, pool) in (0..count).zip(&pool_list) {
                // Pools to many engines start their sweeps at unrelated times.
                tokio::time::sleep_until((start + IDLE_TIMEOUT * index / count).into()).await;
                one_exchange(pool).await;
            }
            // Each session retires at its pool's second sweep at the latest.
            tokio::time::sleep_until(
                (start + IDLE_TIMEOUT * 3 + Duration::from_millis(500)).into(),
            )
            .await;
            let passes = |pools: &[HttpSessionPool]| -> u64 {
                pools
                    .iter()
                    .map(HttpSessionPool::idle_retirement_passes_for_test)
                    .sum()
            };
            let sessions = pool_list.iter().map(HttpSessionPool::session_count).sum();
            let pool_tasks = pool_list
                .iter()
                .map(HttpSessionPool::task_count_for_test)
                .sum();
            let passes_before = passes(&pool_list);
            let cpu_before = thread_cpu_time();
            let window_start = Instant::now();
            tokio::time::sleep(IDLE_TIMEOUT * PERIODS).await;
            let worker_cpu = thread_cpu_time() - cpu_before;
            let window = window_start.elapsed();
            // A sweep is two passes: the sweep, then the pass that re-arms its timer.
            let sweeps = (passes(&pool_list) - passes_before) / 2;
            IdleSweepCost {
                pools,
                window,
                worker_cpu,
                sweeps,
                pool_tasks,
                sessions,
            }
        })
        .expect("measurement admitted");
    let cost = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
        .block_on(call)
        .expect("measurement completes");
    drop(worker);
    cost
}

#[test]
#[ignore = "manual idle-sweep cost measurement (about 5 minutes; run optimized)"]
fn measure_idle_sweep_cost_of_one_and_many_origin_pools() {
    let server = ScopedHttpServer::start(
        ServerConfig::default(),
        Arc::new(|_| Rc::new(OkHandler) as HttpHandlerRef),
        None,
    )
    .expect("test server starts");
    let engine = server.address().expect("server has an address");
    let costs: Vec<_> = (0..REPETITIONS)
        .flat_map(|_| POOL_COUNTS)
        .map(|pools| measure(engine, pools))
        .collect();
    for cost in &costs {
        let pools = u64::try_from(cost.pools).expect("pool count fits");
        eprintln!(
            "pools {:>3}: window {:?}, worker CPU {:?} ({:.1} us/s), sweeps {}, pool tasks {}, \
             sessions left {}",
            cost.pools,
            cost.window,
            cost.worker_cpu,
            cost.worker_cpu.as_secs_f64() * 1e6 / cost.window.as_secs_f64(),
            cost.sweeps,
            cost.pool_tasks,
            cost.sessions,
        );
        assert_eq!(
            cost.sessions, 0,
            "every idle session retired before the window"
        );
        assert_eq!(
            cost.pool_tasks, cost.pools,
            "one sweep task per pool is left, each waiting on its timer"
        );
        let expected = pools * u64::from(PERIODS);
        assert!(
            cost.sweeps.abs_diff(expected) <= pools,
            "each pool swept once per period: {} for {expected}",
            cost.sweeps
        );
    }
    let [none, one, many] = POOL_COUNTS.map(|pools| {
        let mut cpu: Vec<_> = costs
            .iter()
            .filter(|cost| cost.pools == pools)
            .map(|cost| cost.worker_cpu)
            .collect();
        cpu.sort_unstable();
        let sweeps: u64 = costs
            .iter()
            .filter(|cost| cost.pools == pools)
            .map(|cost| cost.sweeps)
            .sum();
        (cpu[cpu.len() / 2], sweeps)
    });
    let runs = u32::try_from(REPETITIONS).expect("repetitions fit");
    eprintln!(
        "median worker CPU per {:?} window: no pool {:?}, 1 pool {:?}, 128 pools {:?}; per \
         extra sweep {:?}",
        IDLE_TIMEOUT * PERIODS,
        none.0,
        one.0,
        many.0,
        (many.0.saturating_sub(one.0) * runs)
            / u32::try_from((many.1 - one.1).max(1)).expect("sweep count fits"),
    );
}
