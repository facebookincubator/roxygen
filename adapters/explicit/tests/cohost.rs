/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::future::Future;
use std::future::pending;
use std::future::poll_fn;
use std::pin::Pin;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread;
use std::thread::ThreadId;
use std::time::Duration;
use std::time::Instant;

use crate::ErrorKind;
use crate::ExplicitError;
use crate::ExplicitRuntime;
use crate::Phase;
use crate::Response;
use crate::SlotState;
use crate::exchange::TestPause;
use crate::exchange_tests::support::EXPIRY_BUDGET;
use crate::exchange_tests::support::HANG_GUARD;
use crate::exchange_tests::support::ReleaseOnDrop;
use crate::exchange_tests::support::TestServer;
use crate::exchange_tests::support::block_on;
use crate::exchange_tests::support::config;
use crate::exchange_tests::support::plain_target;
use crate::exchange_tests::support::request;
use crate::exchange_tests::support::responding;
use crate::exchange_tests::support::start;
use crate::exchange_tests::support::wait_until;
use crate::runtime::RuntimeHooks;

fn spawn<F, T>(runtime: &ExplicitRuntime, work: F) -> tokio::task::JoinHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let executor = block_on(runtime.cohost_executor().unwrap()).unwrap();
    let local = executor.clone();
    let _entered = executor.enter();
    tokio::spawn(async move { local.scope(work).await })
}

struct DropThread(mpsc::Sender<ThreadId>);

impl Drop for DropThread {
    fn drop(&mut self) {
        let _ = self.0.send(thread::current().id());
    }
}

#[test]
fn cohosted_clients_and_exchanges_stay_on_the_probe_thread() {
    let runtime = start(2, "cohost-affinity");
    let server = responding("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
    let target = plain_target(server.address);
    let mut slots = Vec::new();
    for _ in 0..4 {
        let local = runtime.clone();
        let target = Arc::clone(&target);
        slots.push(
            block_on(spawn(&runtime, async move {
                let owner = thread::current().id();
                let mut client = local.client(target);
                let slot = client.slot();
                assert_eq!(owner, local.worker_handle(slot).unwrap().owner_thread());
                let response = client
                    .send(request("localhost", "/", HANG_GUARD))
                    .await
                    .unwrap();
                assert_eq!(response.status, 200);
                assert_eq!(thread::current().id(), owner);
                slot
            }))
            .unwrap(),
        );
    }
    assert_eq!(slots, [0, 1, 0, 1]);
    assert_eq!(runtime.stats().sends, 4);
}

#[test]
fn cohost_affinity_does_not_apply_to_another_runtime() {
    let first = start(1, "cohost-first");
    let second = start(2, "cohost-second");
    let local = second.clone();
    block_on(spawn(&first, async move {
        let target = plain_target("127.0.0.1:9".parse().unwrap());
        let first = local.client(Arc::clone(&target));
        let second = local.client(target);
        assert_eq!([first.slot(), second.slot()], [0, 1]);
    }))
    .unwrap();
}

#[test]
fn abort_and_join_destroys_the_cohosted_task_on_its_worker() {
    let runtime = start(1, "cohost-cancel");
    let (started_tx, started) = mpsc::channel();
    let (dropped_tx, dropped) = mpsc::channel();
    let task = spawn(&runtime, async move {
        let _drop = DropThread(dropped_tx);
        started_tx.send(thread::current().id()).unwrap();
        pending::<()>().await;
    });
    let owner = started.recv_timeout(HANG_GUARD).unwrap();
    task.abort();
    assert!(block_on(task).unwrap_err().is_cancelled());
    assert_eq!(dropped.try_recv().unwrap(), owner);
    assert!(runtime.is_healthy());
}

#[test]
fn runtime_shutdown_destroys_cohosted_tasks_and_completes_their_joins() {
    let runtime = start(1, "cohost-stop");
    let (started_tx, started) = mpsc::channel();
    let (dropped_tx, dropped) = mpsc::channel();
    let task = spawn(&runtime, async move {
        let _drop = DropThread(dropped_tx);
        started_tx.send(thread::current().id()).unwrap();
        pending::<()>().await;
    });
    let owner = started.recv_timeout(HANG_GUARD).unwrap();
    drop(runtime);
    assert_eq!(dropped.try_recv().unwrap(), owner);
    assert!(block_on(task).unwrap_err().is_cancelled());
}

#[test]
fn a_cohost_panic_is_reported_without_closing_the_worker() {
    let runtime = start(1, "cohost-panic");
    let task = spawn(&runtime, async { panic!("probe loop panic") });
    assert!(block_on(task).unwrap_err().is_panic());
    assert!(runtime.is_healthy());
    assert_eq!(block_on(spawn(&runtime, async { 17 })).unwrap(), 17);
}

#[test]
fn a_full_queue_refuses_executor_lookup_before_any_probe_starts() {
    let runtime = ExplicitRuntime::start(config(1, 1, "cohost-full")).unwrap();
    let (release, wait) = mpsc::channel::<()>();
    let (started_tx, started) = mpsc::channel();
    let handle = runtime.worker_handle(0).unwrap();
    handle
        .submit(move |_| {
            started_tx.send(()).unwrap();
            let _ = wait.recv_timeout(HANG_GUARD);
        })
        .unwrap();
    started.recv_timeout(HANG_GUARD).unwrap();
    handle.submit(|_| {}).unwrap();
    match runtime.cohost_executor() {
        Ok(_) => panic!("the queue should be full"),
        Err(error) => assert_eq!(error.kind, ErrorKind::Overloaded),
    }
    drop(release);
}

/// A send boxed to move between threads.
type MovedSend = Pin<Box<dyn Future<Output = Result<Response, ExplicitError>> + Send>>;

/// Polls `send` once with the calling task's waker.
async fn poll_once<F: Future + ?Sized>(mut send: Pin<&mut F>) -> Poll<F::Output> {
    poll_fn(|context| Poll::Ready(send.as_mut().poll(context))).await
}

/// Blocks the calling thread until `deadline` has passed.
fn sleep_past(deadline: Instant) {
    while Instant::now() <= deadline {
        thread::sleep(deadline.saturating_duration_since(Instant::now()));
    }
}

/// Records that it was woken.
#[derive(Default)]
struct WakeFlag(AtomicBool);

impl Wake for WakeFlag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// Keeps slot 0's worker from finishing its shutdown, and so its co-hosted tasks running, until
/// `hold` is released.
fn hold_shutdown(runtime: &ExplicitRuntime, hold: &TestPause) {
    let hold = hold.clone();
    let call = runtime
        .worker_handle(0)
        .unwrap()
        .call(move |context| {
            context
                .register_cleanup_task(async move { hold.pause().await })
                .unwrap()
                .activate()
                .unwrap()
                .detach();
        })
        .unwrap();
    block_on(call).unwrap();
}

#[test]
fn cohosted_send_dropped_by_a_closing_worker_after_its_deadline_times_out_in_submit() {
    let server = TestServer::plain(|mut conn| {
        conn.read_request();
    });
    let runtime = start(1, "cohost-drop");
    let hold = TestPause::new();
    let _release = ReleaseOnDrop(hold.clone());
    hold_shutdown(&runtime, &hold);
    let worker = runtime.worker_handle(0).unwrap();
    let local = runtime.clone();
    let target = plain_target(server.address);
    let task = spawn(&runtime, async move {
        let mut client = local.client(target);
        let late = request("localhost", "/", EXPIRY_BUDGET);
        let deadline = late.deadline;
        let mut send = pin!(client.send(late));
        let woken = Arc::new(WakeFlag::default());
        let waker = Waker::from(Arc::clone(&woken));
        let first = send.as_mut().poll(&mut Context::from_waker(&waker));
        assert!(first.is_pending());
        assert_eq!(worker.queued_commands(), 1);
        worker.begin_shutdown();
        while !woken.0.load(Ordering::SeqCst) && Instant::now() < deadline {
            tokio::task::yield_now().await;
        }
        assert!(
            Instant::now() < deadline,
            "the closing worker dropped the command and woke the send"
        );
        assert_eq!(worker.queued_commands(), 0);
        sleep_past(deadline);
        send.await
    });
    let error = block_on(task)
        .unwrap()
        .expect_err("expired with its command dropped");
    assert_eq!(
        (error.phase, error.kind, error.code),
        (Phase::Submit, ErrorKind::Timeout, None)
    );
    assert!(!error.reached_wire);
    assert_eq!(server.accepted(), 0);
}

#[test]
fn cohosted_send_whose_deadline_passes_before_its_job_starts_times_out_without_dialing() {
    let server = TestServer::plain(|mut conn| {
        conn.read_request();
    });
    let runtime = start(1, "cohost-late");
    let local = runtime.clone();
    let target = plain_target(server.address);
    let error = block_on(spawn(&runtime, async move {
        let hooks = Arc::clone(local.hooks());
        let job_finished = || !hooks.job_errors.lock().unwrap().is_empty();
        let mut client = local.client(target);
        let late = request("localhost", "/", Duration::from_millis(200));
        let deadline = late.deadline;
        let mut send = pin!(client.send(late));
        assert!(poll_once(send.as_mut()).await.is_pending());
        // The worker's thread stays in this task, so its driver cannot start the job before the
        // deadline passes.
        sleep_past(deadline);
        // The driver then starts the job, which finds the deadline passed before it dials.
        while !job_finished() {
            tokio::task::yield_now().await;
        }
        send.await
    }))
    .unwrap()
    .expect_err("expired before its job started");
    assert_eq!(
        (error.phase, error.kind, error.code),
        (Phase::Submit, ErrorKind::Timeout, None)
    );
    assert!(!error.reached_wire);
    assert_eq!(runtime.slot_state(0), SlotState::Open);
    assert_eq!(server.accepted(), 0);
}

#[test]
fn send_moved_off_a_blocked_worker_times_out_by_its_deadline() {
    let server = TestServer::plain(|mut conn| {
        conn.read_request();
    });
    let runtime = start(1, "cohost-move");
    let local = runtime.clone();
    let target = plain_target(server.address);
    let (moved_tx, moved) = mpsc::channel::<(MovedSend, Instant)>();
    let (release, released) = mpsc::channel::<()>();
    let hold = spawn(&runtime, async move {
        let mut client = local.client(target);
        let late = request("localhost", "/", EXPIRY_BUDGET);
        let deadline = late.deadline;
        let mut send: MovedSend = Box::pin(async move { client.send(late).await });
        assert!(poll_once(send.as_mut()).await.is_pending());
        moved_tx.send((send, deadline)).unwrap();
        // Holds the worker's thread, and with it the worker's driver and timer.
        let _ = released.recv_timeout(HANG_GUARD);
    });
    let (send, deadline) = moved.recv_timeout(HANG_GUARD).unwrap();
    let caller = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    // The guard's expiry wins a tie and never polls the send, so only the send's own wake can
    // resolve it.
    let result = caller.block_on(async {
        tokio::select! {
            biased;
            () = tokio::time::sleep(HANG_GUARD / 2) => None,
            result = send => Some(result),
        }
    });
    let resolved = Instant::now();
    drop(release);
    block_on(hold).unwrap();
    let error = result
        .expect("the send resolved while its worker was held")
        .expect_err("expired before its job started");
    assert_eq!(
        (error.phase, error.kind, error.code),
        (Phase::Submit, ErrorKind::Timeout, None)
    );
    assert!(!error.reached_wire);
    assert!(resolved >= deadline);
    wait_until("the released job finishes", || {
        !runtime.hooks().job_errors.lock().unwrap().is_empty()
    });
    assert_eq!(server.accepted(), 0);
}

#[test]
fn caller_deadline_sleep_arms_only_off_the_worker_thread() {
    let server = responding("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
    let hooks = Arc::new(RuntimeHooks::default());
    let runtime =
        ExplicitRuntime::start_with_hooks(config(1, 64, "cohost-arm"), Arc::clone(&hooks)).unwrap();
    let armed = || hooks.caller_sleeps.load(Ordering::SeqCst);
    let target = plain_target(server.address);

    let local = runtime.clone();
    let worker_target = Arc::clone(&target);
    let status = block_on(spawn(&runtime, async move {
        let mut client = local.client(worker_target);
        let response = client.send(request("localhost", "/", HANG_GUARD)).await;
        response.map(|response| response.status)
    }))
    .unwrap()
    .unwrap();
    assert_eq!(status, 200);
    assert_eq!(armed(), 0, "a send polled only on its worker's thread");

    let mut client = runtime.client(Arc::clone(&target));
    let response = block_on(client.send(request("localhost", "/", HANG_GUARD))).unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(armed(), 1, "a send polled on a test thread");

    let local = runtime.clone();
    let (moved_tx, moved) = mpsc::channel::<MovedSend>();
    block_on(spawn(&runtime, async move {
        let mut client = local.client(target);
        let mut send: MovedSend =
            Box::pin(async move { client.send(request("localhost", "/", HANG_GUARD)).await });
        assert!(poll_once(send.as_mut()).await.is_pending());
        moved_tx.send(send).unwrap();
    }))
    .unwrap();
    let response = block_on(moved.recv_timeout(HANG_GUARD).unwrap()).unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(
        armed(),
        2,
        "a send first polled on its worker and then on a test thread"
    );
}
