/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Driving a worker's wheel timer: the `LocalHost::wheel_timer` capability,
//! the Tokio host's worker wheel, and its wakeup in the supervisor of hosted
//! and managed workers (`run_managed_driver`).

use std::cell::RefCell;
use std::future::pending;
use std::future::ready;
use std::panic::panic_any;
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::task::Wake;
use std::time::Instant;

use futures::FutureExt;
use futures::task::noop_waker;

use super::*;
use crate::cancellation::CancellationSource;
use crate::cancellation::CancellationToken;
use crate::host::LocalFuture;
use crate::host::LocalHost;
use crate::host::timer::Expiry;
use crate::host::timer::Timeout;
use crate::host::timer::tests::count_allocations;
use crate::wait::CancellableBaton;
use crate::wait::TimedBaton;
use crate::wait::WaitStatus;
use crate::worker::QuiesceDeadline;
use crate::worker::WorkerContext;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Blocks on `future` on a separate test runtime, failing the test if it
/// does not finish within `TEST_TIMEOUT`.
fn wait<Work: Future>(future: Work) -> Work::Output {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime starts")
        .block_on(async { tokio::time::timeout(TEST_TIMEOUT, future).await })
        .expect("the worker progresses within the test timeout")
}

fn ms(milliseconds: u64) -> Duration {
    Duration::from_millis(milliseconds)
}

/// Runs `test` as a command on a worker that `run_managed_driver` drives, as
/// a `HostedWorker`'s owner thread does, with Tokio's clock paused; then shuts
/// the worker down and returns the command's output. The worker runs on its
/// own thread, which must finish within `TEST_TIMEOUT` of wall time.
fn on_paused_worker<Test, Work, Output>(test: Test) -> Output
where
    Test: FnOnce(WorkerContext) -> Work + Send + 'static,
    Work: Future<Output = Output> + 'static,
    Output: Send + 'static,
{
    let (output_tx, output_rx) = std::sync::mpsc::channel();
    let owner = std::thread::spawn(move || {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .expect("paused test runtime starts");
        let (handle, driver) =
            WorkerDriver::with_host(4, Host::tokio()).expect("worker driver is created");
        let output = LocalSet::new().block_on(&runtime, async move {
            let call = handle.call_local(test).expect("command admitted");
            let command = async {
                let output = call.await;
                handle.begin_shutdown();
                output
            };
            let (output, driven) =
                futures::future::join(command, run_managed_driver(handle.clone(), driver, None))
                    .await;
            driven.expect("the worker shuts down cleanly");
            output.expect("command returns")
        });
        let _ = output_tx.send(output);
    });
    let output = output_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("the paused worker finishes within the test timeout");
    owner.join().expect("the paused worker's thread exits");
    output
}

/// Runs `test` on this thread beside a worker supervisor
/// (`run_managed_driver`) that drives a wheel timer for the thread, as a
/// hosted worker's owner thread does, then shuts the supervisor down. It must
/// run inside a `LocalSet` on a runtime with time enabled. The supervisor
/// installs the wheel before `test` is first polled, so the Tokio host
/// reports the wheel to everything `test` makes. A panic of `test` is
/// re-raised once the supervisor has shut down.
pub(crate) async fn run_with_worker_wheel<Work: Future>(test: Work) -> Work::Output {
    let (handle, driver) =
        WorkerDriver::with_host(1, Host::tokio()).expect("worker driver is created");
    let shutdown = handle.clone();
    let test = async move {
        let output = AssertUnwindSafe(async {
            assert!(
                Host::tokio().wheel_timer().is_some(),
                "the supervisor installed its wheel"
            );
            test.await
        })
        .catch_unwind()
        .await;
        shutdown.begin_shutdown();
        output
    };
    let (driven, output) =
        futures::future::join(run_managed_driver(handle, driver, None), test).await;
    let output = output.unwrap_or_else(|payload| resume_unwind(payload));
    driven.expect("the worker shuts down cleanly");
    output
}

fn hosted_worker(thread_name: &str) -> HostedWorker {
    HostedWorker::spawn(HostedWorkerOptions {
        command_capacity: NonZeroUsize::new(4).expect("nonzero capacity"),
        thread_name: thread_name.to_owned(),
        on_owner_thread_stop: None,
    })
    .expect("hosted worker starts")
}

/// A host that implements only the required capabilities.
struct PlainHost;

impl LocalHost for PlainHost {
    fn spawn_local(&self, _future: LocalFuture) {
        unreachable!("nothing is spawned on this host");
    }

    fn now(&self) -> Instant {
        Instant::now()
    }

    fn sleep_until(&self, _deadline: Instant) -> LocalFuture {
        Box::pin(pending())
    }

    fn yield_to_io(&self) -> LocalFuture {
        Box::pin(ready(()))
    }
}

/// Outside a hosted or managed worker, the Tokio host has no wheel: with no
/// runtime, on a bare runtime and `LocalSet`, and in a caller-driven worker.
/// A custom host keeps the trait's default.
#[test]
fn a_bare_tokio_host_reports_no_wheel() {
    assert!(Host::tokio().wheel_timer().is_none(), "no runtime");

    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime starts");
    LocalSet::new().block_on(&runtime, async {
        assert!(
            Host::tokio().wheel_timer().is_none(),
            "a bare runtime and LocalSet"
        );
    });
    let mut guard = CallerDrivenGuard::new(1).expect("caller-driven worker starts");
    runtime
        .block_on(guard.run_until(async {
            assert!(
                Host::tokio().wheel_timer().is_none(),
                "a caller-driven worker"
            );
        }))
        .expect("polled on its owner thread");
    runtime
        .block_on(guard.shutdown())
        .expect("caller-driven worker shuts down");

    assert!(
        Host::new(Rc::new(PlainHost)).wheel_timer().is_none(),
        "a custom host keeps the default"
    );
}

/// Hosted and managed workers give their owner thread one wheel: a command's
/// context host and the thread's Tokio host report the same wheel on every
/// call.
#[test]
fn hosted_and_managed_workers_report_one_wheel() {
    let hosted = hosted_worker("rox-wheel-reports");
    let managed = ManagedWorker::new(4).expect("managed worker starts");
    for handle in [hosted.handle(), managed.handle()] {
        let shared = wait(
            handle
                .call(|context| {
                    let first = context
                        .host()
                        .wheel_timer()
                        .expect("the worker's host has a wheel");
                    let second = Host::tokio()
                        .wheel_timer()
                        .expect("so does its thread's Tokio host");
                    Rc::ptr_eq(&first, &second)
                })
                .expect("command admitted"),
        )
        .expect("command returns");
        assert!(shared, "one wheel per worker");
    }
}

/// On a worker, a `Timeout` made from the worker's host takes a slot on the
/// worker's wheel at its first schedule, and nothing before it.
#[test]
fn a_timeout_on_a_worker_host_takes_a_slot_on_its_wheel() {
    let worker = hosted_worker("rox-wheel-binds");
    let observed = wait(
        worker
            .handle()
            .call(|context| {
                let host = context.host();
                let wheel = host.wheel_timer().expect("the worker's host has a wheel");
                let mut timeout = Timeout::new(host);
                let unscheduled = wheel.stats().live_slots;
                timeout.schedule_at(host.now() + Duration::from_secs(60), &noop_waker());
                let stats = wheel.stats();
                (unscheduled, stats.live_slots, stats.armed, stats.scheduled)
            })
            .expect("command admitted"),
    )
    .expect("command returns");
    assert_eq!(
        observed,
        (0, 1, 1, 1),
        "(slots before the schedule, slots, armed, schedules) on the worker's wheel"
    );
}

/// What a command observed of one timeout it armed.
#[derive(Debug, PartialEq)]
struct Fire {
    expiry: Expiry,
    thread: ThreadId,
    early: bool,
    fired: u64,
}

/// Arms one timeout `after` from now from inside a command and waits for its
/// expiry.
async fn fire_after(context: WorkerContext, after: Duration) -> Fire {
    let host = context.host().clone();
    let wheel = host.wheel_timer().expect("the worker's host has a wheel");
    let deadline = host.now() + after;
    let mut timeout = Timeout::new(&host);
    let mut scheduled = false;
    let expiry = poll_fn(|task| {
        if !scheduled {
            timeout.schedule_at(deadline, task.waker());
            scheduled = true;
        }
        timeout.poll_expired(task)
    })
    .await;
    Fire {
        expiry,
        thread: std::thread::current().id(),
        early: host.now() < deadline,
        fired: wheel.stats().fired,
    }
}

/// A timeout a command arms fires on the worker's owner thread, not before
/// its deadline, on hosted and managed workers.
#[test]
fn a_fire_lands_on_the_owner_thread() {
    let hosted = hosted_worker("rox-wheel-fires");
    let managed = ManagedWorker::new(4).expect("managed worker starts");
    for handle in [hosted.handle(), managed.handle()] {
        let fire = wait(
            handle
                .call_local(|context| fire_after(context, Duration::from_millis(20)))
                .expect("command admitted"),
        )
        .expect("command returns");
        assert_eq!(
            fire,
            Fire {
                expiry: Expiry::Elapsed,
                thread: handle.owner_thread(),
                early: false,
                fired: 1,
            }
        );
    }
}

/// Arms earlier and later from a task, under a paused clock: moving a
/// deadline earlier than the wheel's armed wakeup fires at the new deadline,
/// and extending one fires at the later deadline, not the first.
#[test]
fn arming_earlier_and_later_from_a_task() {
    let observed = on_paused_worker(|context| async move {
        let host = context.host().clone();
        let start = host.now();
        let mut timeouts: Vec<Timeout> = (0..3).map(|_| Timeout::new(&host)).collect();
        let mut fired = [None; 3];
        let mut step = 0;
        poll_fn(|task| {
            match step {
                0 => {
                    timeouts[0].schedule_at(start + ms(50), task.waker());
                    timeouts[1].schedule_at(start + ms(30), task.waker());
                }
                1 => {
                    timeouts[0].schedule_at(start + ms(10), task.waker());
                    timeouts[1].extend_to(start + ms(40));
                    timeouts[2].schedule_at(start + ms(100), task.waker());
                }
                _ => {}
            }
            if step < 2 {
                step += 1;
                // Let the supervisor take the first wakeup before re-arming.
                task.waker().wake_by_ref();
                return Poll::Pending;
            }
            for (timeout, fired) in timeouts.iter_mut().zip(&mut fired) {
                if fired.is_none() && timeout.poll_expired(task).is_ready() {
                    *fired = Some(host.now() - start);
                }
            }
            match fired {
                [Some(first), Some(second), Some(third)] => Poll::Ready([first, second, third]),
                _ => Poll::Pending,
            }
        })
        .await
    });
    assert_eq!(
        observed,
        [ms(10), ms(40), ms(100)],
        "moved earlier from 50 ms to 10 ms; extended from 30 ms to 40 ms; armed for 100 ms"
    );
}

/// Under a paused clock, a timeout is observed at the instant a Tokio
/// `sleep_until` with the same deadline is: on level 0, below the tick, and
/// after cascades from levels 1 and 2.
#[test]
fn fire_instants_equal_tokio_sleep_until_under_paused_time() {
    const OFFSETS: [Duration; 9] = [
        Duration::from_micros(1),
        Duration::from_millis(1),
        Duration::from_micros(7_500),
        Duration::from_millis(255),
        Duration::from_millis(256),
        Duration::from_millis(300),
        Duration::from_millis(1_000),
        Duration::from_millis(70_000),
        Duration::from_secs(3_600),
    ];
    let (wheel, tokio) = on_paused_worker(|context| async move {
        let host = context.host().clone();
        let start = host.now();
        let mut timeouts: Vec<Timeout> = OFFSETS.iter().map(|_| Timeout::new(&host)).collect();
        let mut sleeps: Vec<Pin<Box<Sleep>>> = OFFSETS
            .iter()
            .map(|offset| Box::pin(tokio::time::sleep_until((start + *offset).into())))
            .collect();
        let mut wheel = [None; OFFSETS.len()];
        let mut tokio = [None; OFFSETS.len()];
        let mut scheduled = false;
        poll_fn(|task| {
            if !scheduled {
                for (timeout, offset) in timeouts.iter_mut().zip(OFFSETS) {
                    timeout.schedule_at(start + offset, task.waker());
                }
                scheduled = true;
            }
            let now = host.now() - start;
            for index in 0..OFFSETS.len() {
                if wheel[index].is_none() && timeouts[index].poll_expired(task).is_ready() {
                    wheel[index] = Some(now);
                }
                if tokio[index].is_none() && sleeps[index].as_mut().poll(task).is_ready() {
                    tokio[index] = Some(now);
                }
            }
            if wheel.iter().chain(&tokio).all(Option::is_some) {
                Poll::Ready((wheel, tokio))
            } else {
                Poll::Pending
            }
        })
        .await
    });
    assert_eq!(wheel, tokio, "observed at the instants Tokio's sleeps are");
    for (fired, offset) in wheel.iter().zip(OFFSETS) {
        assert!(
            fired.is_some_and(|fired| fired >= offset),
            "never early: {fired:?} for {offset:?}"
        );
    }
}

thread_local! {
    /// Timeouts kept armed on a worker's thread across commands.
    static KEPT: RefCell<Vec<Timeout>> = const { RefCell::new(Vec::new()) };
    /// The wheel of the worker on this thread, kept past its teardown.
    static KEPT_WHEEL: RefCell<Option<Rc<WheelTimer>>> = const { RefCell::new(None) };
}

/// The wakeup counts of `handle`'s worker thread.
fn counts(handle: &WorkerHandle) -> WakeupCounts {
    wait(handle.call(|_| wakeup_counts()).expect("command admitted")).expect("command returns")
}

/// Runs commands on `handle`'s worker, one after another, until its
/// supervisor has run at least `polls` more times. A command's completion
/// wakes the supervisor through its wake gate, but commands that complete
/// before the supervisor runs share that run, so how many commands it takes
/// varies.
fn run_commands(handle: &WorkerHandle, polls: usize) {
    let since = counts(handle).supervisor_polls;
    let deadline = Instant::now() + TEST_TIMEOUT;
    while counts(handle).supervisor_polls - since < polls {
        assert!(
            Instant::now() < deadline,
            "the supervisor runs {polls} times within the test timeout"
        );
    }
}

/// Runs commands on `handle`'s worker until its supervisor has been polled
/// at least `polls` times since `since` was taken, and returns the counts
/// then. Commands that the worker runs within one `LocalSet` tick share the
/// supervisor poll after it, so how many commands that takes varies.
fn poll_supervisor(handle: &WorkerHandle, since: WakeupCounts, polls: usize) -> WakeupCounts {
    let deadline = Instant::now() + TEST_TIMEOUT;
    loop {
        let now = counts(handle);
        if now.supervisor_polls - since.supervisor_polls >= polls {
            return now;
        }
        assert!(
            Instant::now() < deadline,
            "the supervisor is polled {polls} times within the test timeout: {since:?} {now:?}"
        );
    }
}

/// The supervisor polls the wheel's sleep only when Tokio may have fired it,
/// or when Tokio must take the supervisor's waker for it. Its other polls,
/// after the wheel has emptied and while a far timeout is armed, poll no
/// `Sleep`.
#[test]
fn a_supervisor_poll_with_no_wheel_work_polls_no_sleep() {
    let worker = hosted_worker("rox-wheel-quiet");
    let handle = worker.handle();
    let fire = wait(
        handle
            .call_local(|context| fire_after(context, ms(5)))
            .expect("command admitted"),
    )
    .expect("command returns");
    assert_eq!(fire.expiry, Expiry::Elapsed);

    let emptied = counts(&handle);
    run_commands(&handle, 200);
    let after_emptied = counts(&handle);

    wait(
        handle
            .call(|context| {
                let mut timeout = Timeout::new(context.host());
                timeout.schedule_at(
                    context.host().now() + Duration::from_secs(600),
                    &noop_waker(),
                );
                KEPT.with_borrow_mut(|kept| kept.push(timeout));
            })
            .expect("command admitted"),
    )
    .expect("command returns");
    let armed = counts(&handle);
    run_commands(&handle, 200);
    let after_armed = counts(&handle);
    wait(
        handle
            .call(|_| drop(KEPT.take()))
            .expect("command admitted"),
    )
    .expect("command returns");

    assert!(
        after_emptied.supervisor_polls - emptied.supervisor_polls >= 200
            && after_armed.supervisor_polls - armed.supervisor_polls >= 200,
        "every command polled the supervisor: {emptied:?} {after_emptied:?} {armed:?} {after_armed:?}"
    );
    assert_eq!(
        after_emptied.sleep_polls - emptied.sleep_polls,
        0,
        "no sleep polls after the wheel emptied"
    );
    assert_eq!(
        after_armed.sleep_polls - armed.sleep_polls,
        0,
        "no sleep polls while a far timeout is armed"
    );
}

/// Arms `offsets` from now, earliest last, on timeouts kept on the worker's
/// thread. Returns the wheel's host arms and supervisor wakes they made.
fn arm_kept(handle: &WorkerHandle, offsets: &'static [u64]) -> (u64, usize) {
    wait(
        handle
            .call(move |context| {
                let host = context.host();
                let wheel = host.wheel_timer().expect("the worker's host has a wheel");
                let (arms, wakes) = (wheel.stats().host_arms, wakeup_counts().supervisor_wakes);
                for seconds in offsets {
                    let mut timeout = Timeout::new(host);
                    timeout.schedule_at(host.now() + Duration::from_secs(*seconds), &noop_waker());
                    KEPT.with_borrow_mut(|kept| kept.push(timeout));
                }
                (
                    wheel.stats().host_arms - arms,
                    wakeup_counts().supervisor_wakes - wakes,
                )
            })
            .expect("command admitted"),
    )
    .expect("command returns")
}

/// Arming wakes the supervisor exactly once while the wheel's sleep needs a
/// poll (it is new, or it fired), however many arms come before that poll.
/// Arms while Tokio holds the supervisor's waker for the sleep, and the
/// re-arm the supervisor's own `expire` makes, wake nothing.
#[test]
fn an_empty_wheel_wakes_the_supervisor_exactly_once_on_arm() {
    let worker = hosted_worker("rox-wheel-arm-wakes");
    let handle = worker.handle();
    assert_eq!(
        arm_kept(&handle, &[600, 500, 400]),
        (3, 1),
        "a new sleep: three earlier arms, one supervisor wake"
    );
    assert_eq!(
        arm_kept(&handle, &[300]),
        (1, 0),
        "a registered sleep moved earlier wakes nothing"
    );
    let wakes = counts(&handle).supervisor_wakes;
    let fire = wait(
        handle
            .call_local(|context| fire_after(context, ms(5)))
            .expect("command admitted"),
    )
    .expect("command returns");
    assert_eq!(fire.expiry, Expiry::Elapsed);
    assert_eq!(
        counts(&handle).supervisor_wakes - wakes,
        0,
        "neither the 5 ms arm nor the re-arm for 300 s after its fire wakes the supervisor"
    );
    wait(
        handle
            .call(|_| drop(KEPT.take()))
            .expect("command admitted"),
    )
    .expect("command returns");

    let worker = hosted_worker("rox-wheel-arm-wakes-2");
    let handle = worker.handle();
    for round in 0..2 {
        let wakes = counts(&handle).supervisor_wakes;
        let fire = wait(
            handle
                .call_local(|context| fire_after(context, ms(5)))
                .expect("command admitted"),
        )
        .expect("command returns");
        assert_eq!(fire.expiry, Expiry::Elapsed);
        assert_eq!(
            counts(&handle).supervisor_wakes - wakes,
            1,
            "round {round}: arming the empty wheel wakes the supervisor once"
        );
    }
}

/// The zero-config guard on workers: commands that use no timers, including
/// one that makes a `Timeout` and never schedules it, create no wheel and no
/// `Sleep`, poll none and request no supervisor wake, on hosted and managed
/// workers.
#[test]
fn a_worker_with_no_timers_allocates_no_sleep() {
    let hosted = hosted_worker("rox-wheel-untimed");
    let managed = ManagedWorker::new(4).expect("managed worker starts");
    for handle in [hosted.handle(), managed.handle()] {
        run_commands(&handle, 50);
        wait(
            handle
                .call(|context| {
                    let mut timeout = Timeout::new(context.host());
                    timeout.extend_to(context.host().now() + ms(1));
                    timeout.cancel();
                    assert_eq!(timeout.take_expired(), None);
                })
                .expect("command admitted"),
        )
        .expect("command returns");
        let counts = counts(&handle);
        assert!(counts.supervisor_polls >= 50, "{counts:?}");
        assert_eq!(
            (
                counts.wheels,
                counts.sleeps,
                counts.sleep_polls,
                counts.supervisor_wakes,
                counts.wheel_steps
            ),
            (0, 0, 0, 0, 0),
            "(wheels, sleeps, sleep polls, supervisor wakes, supervisor polls with wheel work)"
        );
    }
}

/// The zero-config guard for batons: untimed waits on timed and cancellable
/// batons, signalled or cancelled, create no wheel and no `Sleep`, on hosted
/// and managed workers.
#[test]
fn untimed_baton_waits_on_a_worker_create_no_wheel() {
    let hosted = hosted_worker("rox-wheel-untimed-batons");
    let managed = ManagedWorker::new(4).expect("managed worker starts");
    for handle in [hosted.handle(), managed.handle()] {
        wait(
            handle
                .call_local(|_| async {
                    for baton in [TimedBaton::new(None), TimedBaton::new(Some(Duration::ZERO))] {
                        let mut wait = pin!(baton.wait(CancellationToken::never()));
                        assert!(futures::poll!(wait.as_mut()).is_pending());
                        baton.signal();
                        assert_eq!(wait.await, WaitStatus::Signalled);
                    }
                    let baton = CancellableBaton::new();
                    let mut wait =
                        pin!(baton.timed_wait(Duration::ZERO, CancellationToken::never()));
                    assert!(futures::poll!(wait.as_mut()).is_pending());
                    baton.signal();
                    assert_eq!(wait.await, WaitStatus::Signalled);
                    baton.reset();
                    let source = CancellationSource::new();
                    let mut wait = pin!(baton.wait(source.token()));
                    assert!(futures::poll!(wait.as_mut()).is_pending());
                    source.request_cancellation();
                    assert_eq!(wait.await, WaitStatus::Cancelled);
                })
                .expect("command admitted"),
        )
        .expect("command returns");
        let counts = counts(&handle);
        assert_eq!(
            (counts.wheels, counts.sleeps, counts.sleep_polls),
            (0, 0, 0),
            "(wheels, sleeps, sleep polls)"
        );
    }
}

/// On a worker, the Tokio host's `sleep_until` is a timeout on the worker's
/// wheel instead of a Tokio `Sleep`: it takes a slot at its first poll,
/// resolves at the instant a Tokio sleep with the same deadline does, never
/// earlier, frees its slot when it resolves, and stays resolved.
#[test]
fn tokio_host_sleeps_use_the_worker_wheel() {
    let (armed, slept, tokio_slept, after) = on_paused_worker(|_| async move {
        let host = Host::tokio();
        let wheel = host.wheel_timer().expect("the worker has a wheel");
        let start = host.now();
        let deadline = start + Duration::from_micros(7_500);
        let mut sleep = host.sleep_until(deadline);
        let mut tokio = Box::pin(tokio::time::sleep_until(deadline.into()));
        assert!(futures::poll!(sleep.as_mut()).is_pending());
        assert!(futures::poll!(tokio.as_mut()).is_pending());
        let armed = wheel.stats();
        let (mut slept, mut tokio_slept) = (None, None);
        poll_fn(|task| {
            let now = host.now() - start;
            if slept.is_none() && sleep.as_mut().poll(task).is_ready() {
                slept = Some(now);
            }
            if tokio_slept.is_none() && tokio.as_mut().poll(task).is_ready() {
                tokio_slept = Some(now);
            }
            if slept.is_some() && tokio_slept.is_some() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        assert!(
            futures::poll!(sleep.as_mut()).is_ready(),
            "a resolved sleep stays resolved"
        );
        (armed, slept, tokio_slept, wheel.stats())
    });
    assert_eq!(
        (armed.scheduled, armed.armed, armed.live_slots),
        (1, 1, 1),
        "(schedules, armed, slots) after the first poll: {armed:?}"
    );
    assert_eq!(slept, tokio_slept, "resolved when the Tokio sleep did");
    assert!(
        slept.is_some_and(|slept| slept >= Duration::from_micros(7_500)),
        "never early: {slept:?}"
    );
    assert_eq!(
        (after.fired, after.armed, after.live_slots),
        (1, 0, 0),
        "(fires, armed, slots) once resolved: {after:?}"
    );
}

/// A Tokio host sleep still pending when its worker's wheel stops resolves at
/// its deadline, as a Tokio sleep, never earlier; and so does one made after.
#[test]
fn a_host_sleep_outliving_its_wheel_resolves_at_its_deadline() {
    let (pending, made_after) = on_paused_worker(|_| async move {
        let host = Host::tokio();
        let start = host.now();
        let mut sleep = host.sleep_until(start + ms(10));
        assert!(futures::poll!(sleep.as_mut()).is_pending());
        host.wheel_timer().expect("the worker has a wheel").stop();
        sleep.await;
        let pending = host.now() - start;
        let start = host.now();
        host.sleep_until(start + ms(5)).await;
        (pending, host.now() - start)
    });
    assert_eq!((pending, made_after), (ms(10), ms(5)));
}

/// On a worker, a Tokio host sleep whose deadline is due by its first poll is
/// ready at that poll exactly when a Tokio sleep made then is, so it wins an
/// arbitration that polls it first against ready work when that Tokio sleep
/// would (Uniplex makes its flow, buffer and GOAWAY sleeps anew at each
/// arbitration). Tokio rounds a deadline up to its next millisecond tick: at a
/// deadline on a tick, and after one, both are ready; half a tick before the
/// rounded deadline neither is. None schedules on the wheel.
#[test]
fn a_due_host_sleep_is_ready_at_its_first_poll_when_a_tokio_sleep_is() {
    let (picks, scheduled) = on_paused_worker(|_| async move {
        let host = Host::tokio();
        let wheel = host.wheel_timer().expect("the worker has a wheel");
        let start = host.now();
        let on_tick = start + ms(5);
        let off_tick = start + Duration::from_micros(6_500);
        let half_tick = Duration::from_micros(500);
        let mut picks = Vec::new();
        for (advance, deadline) in [
            (ms(5), on_tick),
            (ms(1), on_tick),
            (half_tick, off_tick),
            (half_tick, off_tick),
        ] {
            tokio::time::advance(advance).await;
            let host_pick = tokio::select! {
                biased;
                () = host.sleep_until(deadline) => "timer",
                () = ready(()) => "work",
            };
            let tokio_pick = tokio::select! {
                biased;
                () = tokio::time::sleep_until(deadline.into()) => "timer",
                () = ready(()) => "work",
            };
            picks.push((host_pick, tokio_pick));
        }
        (picks, wheel.stats().scheduled)
    });
    assert_eq!(
        picks,
        [
            ("timer", "timer"),
            ("timer", "timer"),
            ("work", "work"),
            ("timer", "timer"),
        ],
        "(host sleep, Tokio sleep): at a deadline on a tick, 1 ms after it, at a deadline \
         half a tick before its rounded deadline, and at that rounded deadline"
    );
    assert_eq!(
        scheduled, 0,
        "a due host sleep schedules nothing on the wheel"
    );
}

/// Until a timer creates the wheel, a supervisor poll only checks that it
/// does not exist, and never calls the wheel step, on hosted and managed
/// workers.
#[test]
fn a_supervisor_poll_with_no_wheel_calls_no_wheel_step() {
    let hosted = hosted_worker("rox-wheel-absent");
    let managed = ManagedWorker::new(4).expect("managed worker starts");
    for handle in [hosted.handle(), managed.handle()] {
        run_commands(&handle, 50);
        let counts = counts(&handle);
        assert!(counts.supervisor_polls >= 50, "{counts:?}");
        assert_eq!(counts.wheel_drives, 0, "wheel step calls: {counts:?}");
    }
}

/// Once a timer has created the wheel, a supervisor poll runs the wheel step
/// only while the wheel's wakeup is due: never with nothing armed, and once
/// for an arm, the poll that has Tokio take the supervisor's waker for the
/// new sleep, on hosted and managed workers.
#[test]
fn a_supervisor_poll_steps_the_wheel_only_while_its_wakeup_is_due() {
    let hosted = hosted_worker("rox-wheel-present");
    let managed = ManagedWorker::new(4).expect("managed worker starts");
    for handle in [hosted.handle(), managed.handle()] {
        wait(
            handle
                .call(|context| {
                    context
                        .host()
                        .wheel_timer()
                        .expect("the worker's host has a wheel");
                })
                .expect("command admitted"),
        )
        .expect("command returns");
        let created = counts(&handle);
        let idle = poll_supervisor(&handle, created, 50);
        assert_eq!(
            (
                idle.wheel_drives - created.wheel_drives,
                idle.wheel_steps - created.wheel_steps
            ),
            (0, 0),
            "(wheel step calls, wheel steps) over 50 supervisor polls with nothing armed: {created:?} {idle:?}"
        );

        wait(
            handle
                .call(|context| {
                    let mut timeout = Timeout::new(context.host());
                    timeout.schedule_at(
                        context.host().now() + Duration::from_secs(600),
                        &noop_waker(),
                    );
                    KEPT.with_borrow_mut(|kept| kept.push(timeout));
                })
                .expect("command admitted"),
        )
        .expect("command returns");
        let armed = poll_supervisor(&handle, idle, 50);
        wait(
            handle
                .call(|_| drop(KEPT.take()))
                .expect("command admitted"),
        )
        .expect("command returns");
        assert_eq!(
            (
                armed.wheel_drives - idle.wheel_drives,
                armed.wheel_steps - idle.wheel_steps,
                armed.sleep_polls - idle.sleep_polls
            ),
            (1, 1, 1),
            "(wheel step calls, wheel steps, sleep polls) for one far arm: {idle:?} {armed:?}"
        );
    }
}

/// The zero-config residue guard (LR-P39): with session timers off, a
/// worker's only timed wait (an engine dial's connect bound) creates the
/// wheel and arms its wakeup. Once that wait completes and its baton drops,
/// the wheel has withdrawn the wakeup, dropping its Tokio sleep, so Tokio's
/// parks are untimed again, and later supervisor polls leave the wheel alone,
/// on hosted and managed workers.
#[test]
fn a_one_shot_wait_leaves_no_wakeup_behind() {
    let hosted = hosted_worker("rox-wheel-one-shot");
    let managed = ManagedWorker::new(4).expect("managed worker starts");
    for handle in [hosted.handle(), managed.handle()] {
        let before = counts(&handle);
        let stats = wait(
            handle
                .call_local(|context| async move {
                    {
                        let baton = TimedBaton::new(Some(Duration::from_secs(30)));
                        let mut wait = pin!(baton.wait(CancellationToken::never()));
                        assert!(futures::poll!(wait.as_mut()).is_pending());
                        tokio::task::yield_now().await;
                        baton.signal();
                        assert_eq!(wait.await, WaitStatus::Signalled);
                    }
                    context
                        .host()
                        .wheel_timer()
                        .expect("the worker's host has a wheel")
                        .stats()
                })
                .expect("command admitted"),
        )
        .expect("command returns");
        let withdrawn = counts(&handle);
        let after = poll_supervisor(&handle, withdrawn, 100);

        assert_eq!(
            (stats.scheduled, stats.armed, stats.live_slots),
            (1, 0, 0),
            "(schedules, armed, slots) once the baton dropped: {stats:?}"
        );
        assert_eq!(
            (
                withdrawn.wheels - before.wheels,
                withdrawn.withdrawals - before.withdrawals
            ),
            (1, 1),
            "(wheels created, wakeups withdrawn): {before:?} {withdrawn:?}"
        );
        assert_eq!(
            (
                after.wheel_drives - withdrawn.wheel_drives,
                after.sleep_polls - withdrawn.sleep_polls
            ),
            (0, 0),
            "(wheel step calls, sleep polls) over 100 supervisor polls after the withdrawal: {withdrawn:?} {after:?}"
        );
    }
}

/// A wakeup the wheel withdrew is re-armed by its next arm: the sleep is made
/// anew in its box and the supervisor is woken once to have Tokio take its
/// waker (design §2.5: arming never polls the sleep), and the timeout fires
/// at its deadline, round after round, under a paused clock.
#[test]
fn a_re_arm_after_a_withdrawal_wakes_the_supervisor_once_and_fires_at_its_deadline() {
    let rounds = on_paused_worker(|context| async move {
        let host = context.host().clone();
        let mut rounds = Vec::new();
        for _ in 0..3 {
            let mut withdrawn = Timeout::new(&host);
            withdrawn.schedule_at(host.now() + Duration::from_secs(30), &noop_waker());
            tokio::task::yield_now().await;
            let freed = wakeup_counts();
            drop(withdrawn);
            let before = wakeup_counts();
            let start = host.now();
            let expiry = wait_timeout(host.clone(), ms(5)).await;
            let after = wakeup_counts();
            rounds.push((
                expiry,
                host.now() - start,
                before.withdrawals - freed.withdrawals,
                after.supervisor_wakes - before.supervisor_wakes,
                after.sleeps - before.sleeps,
            ));
        }
        rounds
    });
    assert_eq!(
        rounds,
        vec![(Expiry::Elapsed, ms(5), 1, 1, 1); 3],
        "(expiry, fired after, withdrawals at the free, supervisor wakes, sleeps made) per round"
    );
}

/// A cancelled slot stays linked, so a later arm no earlier than its link is
/// covered by the wakeup without a new request: neither the cancel nor
/// freeing another slot while it is linked withdraws the wakeup, and the
/// re-armed slot fires at its deadline, under a paused clock.
#[test]
fn a_cancelled_slot_re_armed_while_linked_still_fires() {
    async fn re_arm(timeout: &mut Timeout, deadline: Instant) -> Expiry {
        let mut scheduled = false;
        poll_fn(|task| {
            if !scheduled {
                timeout.schedule_at(deadline, task.waker());
                scheduled = true;
            }
            timeout.poll_expired(task)
        })
        .await
    }

    let (lone, beside_freed) = on_paused_worker(|context| async move {
        let host = context.host().clone();
        let withdrawals = wakeup_counts().withdrawals;

        let start = host.now();
        let mut lone = Timeout::new(&host);
        lone.schedule_at(start + ms(20), &noop_waker());
        tokio::task::yield_now().await;
        lone.cancel();
        tokio::task::yield_now().await;
        let expiry = re_arm(&mut lone, start + ms(20)).await;
        let lone_fire = (
            expiry,
            host.now() - start,
            wakeup_counts().withdrawals - withdrawals,
        );
        drop(lone);

        let start = host.now();
        let mut freed = Timeout::new(&host);
        let mut cancelled = Timeout::new(&host);
        freed.schedule_at(start + ms(10), &noop_waker());
        cancelled.schedule_at(start + ms(20), &noop_waker());
        tokio::task::yield_now().await;
        cancelled.cancel();
        drop(freed);
        let expiry = re_arm(&mut cancelled, start + ms(20)).await;
        let beside_freed = (
            expiry,
            host.now() - start,
            wakeup_counts().withdrawals - withdrawals,
        );
        (lone_fire, beside_freed)
    });
    assert_eq!(
        lone,
        (Expiry::Elapsed, ms(20), 0),
        "(expiry, fired after, withdrawals): a lone slot cancelled and re-armed"
    );
    assert_eq!(
        beside_freed,
        (Expiry::Elapsed, ms(20), 0),
        "(expiry, fired after, withdrawals): a cancelled slot re-armed after another was freed"
    );
}

/// Re-arming a withdrawn wakeup allocates nothing: the Tokio sleep is made
/// anew in the box the first arm allocated. Observable only where std's
/// allocations reach this crate's counting allocator (opt builds); the
/// withdrawals and sleeps made are counted everywhere.
#[test]
fn a_re_arm_after_a_withdrawal_allocates_nothing() {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime starts");
    let (allocations, withdrawals, sleeps) = LocalSet::new().block_on(
        &runtime,
        run_with_worker_wheel(async {
            let host = Host::tokio();
            let waker = noop_waker();
            let arm_and_free = || {
                let mut timeout = Timeout::new(&host);
                timeout.schedule_at(host.now() + Duration::from_secs(30), &waker);
            };
            arm_and_free();
            let before = wakeup_counts();
            let allocations = count_allocations(|| (0..10).for_each(|_| arm_and_free()));
            let after = wakeup_counts();
            (
                allocations,
                after.withdrawals - before.withdrawals,
                after.sleeps - before.sleeps,
            )
        }),
    );
    assert_eq!(
        (withdrawals, sleeps),
        (10, 10),
        "(withdrawals, sleeps made) for 10 arms, each of the wheel's only slot"
    );
    if let Some(allocations) = allocations {
        assert_eq!(
            allocations, 0,
            "allocations for 10 re-arms after a withdrawal"
        );
    }
}

#[derive(Default)]
struct CountingWake(AtomicUsize);

impl Wake for CountingWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// What a worker's exit callback, which runs on its owner thread after
/// `run_managed_driver` returns, observed of a timeout kept armed across the
/// teardown and of the worker's wheel.
#[derive(Debug, PartialEq)]
struct Teardown {
    expiry: Option<Expiry>,
    owner_wakes: usize,
    rescheduled: Option<Expiry>,
    fired: u64,
    armed: usize,
    live_slots: usize,
    host_has_wheel: bool,
}

fn observe_teardown(owner: Arc<CountingWake>, report: mpsc::Sender<Teardown>) {
    let wheel = KEPT_WHEEL.take().expect("a command kept the wheel");
    let mut kept = KEPT.take();
    let expiry = kept[0].take_expired();
    kept[0].schedule_at(Instant::now() + Duration::from_secs(1), &noop_waker());
    let rescheduled = kept[0].take_expired();
    wheel.expire(Instant::now() + Duration::from_secs(7_200));
    let stats = wheel.stats();
    report
        .send(Teardown {
            expiry,
            owner_wakes: owner.0.load(Ordering::SeqCst),
            rescheduled,
            fired: stats.fired,
            armed: stats.armed,
            live_slots: stats.live_slots,
            host_has_wheel: Host::tokio().wheel_timer().is_some(),
        })
        .expect("the test observes the teardown");
}

/// Teardown stops the wheel after the final join: a timeout kept armed past
/// it completes as stopped and its owner is woken, a later schedule
/// completes as stopped at once, nothing fires, and the thread's Tokio host
/// reports no wheel. Shutdown does not wait for armed timers. Both through
/// `shutdown` and through a forced quiesce, whose running command (and the
/// timeout it holds) is dropped.
#[test]
fn teardown_stops_the_wheel_and_nothing_fires_after_it() {
    for forced in [false, true] {
        let owner = Arc::new(CountingWake::default());
        let (report_tx, report_rx) = mpsc::channel();
        let probe_owner = Arc::clone(&owner);
        let worker = ManagedWorker::new_with_exit_callback(4, move || {
            observe_teardown(probe_owner, report_tx);
        })
        .expect("managed worker starts");
        let handle = worker.handle();
        let waker = Waker::from(Arc::clone(&owner));
        wait(
            handle
                .call(move |context| {
                    let host = context.host();
                    KEPT_WHEEL.set(host.wheel_timer());
                    let mut timeout = Timeout::new(host);
                    timeout.schedule_at(host.now() + Duration::from_secs(5), &waker);
                    KEPT.with_borrow_mut(|kept| kept.push(timeout));
                })
                .expect("command admitted"),
        )
        .expect("command returns");
        let started = Instant::now();
        if forced {
            let (running_tx, running_rx) = mpsc::channel();
            let _running = handle
                .call_local(move |context| async move {
                    let mut timeout = Timeout::new(context.host());
                    timeout.schedule_at(
                        context.host().now() + Duration::from_secs(600),
                        &noop_waker(),
                    );
                    running_tx.send(()).expect("the test observes the command");
                    pending::<()>().await;
                })
                .expect("command admitted");
            running_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("the command runs");
            handle.begin_quiesce(QuiesceDeadline::new(ms(20)));
            let mut completion = worker.completion();
            wait(completion.wait_for(Option::is_some)).expect("the worker publishes completion");
        }
        assert_eq!(worker.shutdown(), Ok(()));
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "teardown does not wait for armed timers"
        );
        assert_eq!(forced, handle.was_forced());
        assert_eq!(
            report_rx.recv_timeout(TEST_TIMEOUT),
            Ok(Teardown {
                expiry: Some(Expiry::Stopped),
                owner_wakes: 1,
                rescheduled: Some(Expiry::Stopped),
                fired: 0,
                armed: 0,
                live_slots: 1,
                host_has_wheel: false,
            }),
            "forced: {forced}"
        );
    }
}

/// A timeout waker whose wake panics, with a string payload or with a
/// payload whose destructor panics too.
struct PanickingWake {
    payload_drop_panics: bool,
}

impl PanickingWake {
    fn waker(payload_drop_panics: bool) -> Waker {
        Waker::from(Arc::new(Self {
            payload_drop_panics,
        }))
    }
}

impl Wake for PanickingWake {
    fn wake(self: Arc<Self>) {
        if self.payload_drop_panics {
            panic_any(PanickingPayload { inner_panics: 1 });
        }
        panic!("injected timeout waker panic");
    }
}

/// A panic payload whose destructor panics with such a payload,
/// `inner_panics` deep, and then with a string.
struct PanickingPayload {
    inner_panics: usize,
}

impl Drop for PanickingPayload {
    fn drop(&mut self) {
        if self.inner_panics != 0 {
            panic_any(Self {
                inner_panics: self.inner_panics - 1,
            });
        }
        panic!("injected panic payload destructor panic");
    }
}

/// Arms a timeout on `host` for `after` from now and waits for its expiry.
async fn wait_timeout(host: Host, after: Duration) -> Expiry {
    let deadline = host.now() + after;
    let mut timeout = Timeout::new(&host);
    let mut scheduled = false;
    poll_fn(|task| {
        if !scheduled {
            timeout.schedule_at(deadline, task.waker());
            scheduled = true;
        }
        timeout.poll_expired(task)
    })
    .await
}

/// A timeout waker that panics while the supervisor expires the wheel does
/// not unwind the worker: the supervisor begins shutdown, the wheel keeps
/// timing (a registered cleanup waits on a timeout), every task is joined
/// and cleanup runs to completion, and the panic is then reported as the
/// owner thread's, also when the payload's destructor panics.
#[test]
fn a_waker_panic_during_expiry_shuts_the_worker_down_after_joining() {
    for payload_drop_panics in [false, true] {
        expiry_panic_shuts_the_worker_down(payload_drop_panics);
    }
}

fn expiry_panic_shuts_the_worker_down(payload_drop_panics: bool) {
    let worker = hosted_worker("rox-wheel-waker-panic");
    let handle = worker.handle();
    let (finished_tx, finished_rx) = mpsc::channel();
    let (parked_tx, parked_rx) = mpsc::channel();
    handle
        .spawn_local(move |context| async move {
            let host = context.host().clone();
            let _cleanup = context
                .register_cleanup_task(async move {
                    // Arm once the shutdown has gone quiet, so that only the
                    // arm itself can bring the supervisor back.
                    tokio::time::sleep(ms(20)).await;
                    let expiry = wait_timeout(host, ms(5)).await;
                    finished_tx
                        .send(expiry)
                        .expect("the test observes the cleanup");
                })
                .expect("cleanup registered while open");
            let mut timeout = Timeout::new(context.host());
            timeout.schedule_at(
                context.host().now() + ms(5),
                &PanickingWake::waker(payload_drop_panics),
            );
            KEPT.with_borrow_mut(|kept| kept.push(timeout));
            parked_tx.send(()).expect("the test observes the command");
            pending::<()>().await;
        })
        .expect("command admitted");
    parked_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("the command parks");

    let mut completion = worker.completion();
    let completed = wait(async move {
        completion
            .wait_for(Option::is_some)
            .await
            .expect("the owner thread publishes completion")
            .clone()
            .expect("completion is published")
    });
    assert_eq!(
        completed,
        Err(ManagedWorkerError::OwnerThreadPanicked),
        "payload_drop_panics: {payload_drop_panics}"
    );
    assert_eq!(
        finished_rx.try_recv(),
        Ok(Expiry::Elapsed),
        "the cleanup ran to completion, its own timeout firing during the shutdown"
    );
    assert_eq!(handle.state(), WorkerState::Closed);
}

/// A timeout waker that panics when the teardown stops the wheel is reported
/// as the owner thread's panic once the teardown finishes, alone or after a
/// waker panic during expiry, with payloads whose destructors panic: the
/// owner thread publishes completion and the process does not abort.
#[test]
fn a_waker_panic_during_teardown_is_reported_after_it() {
    for after_expiry_panic in [false, true] {
        let worker = hosted_worker("rox-wheel-stop-panic");
        let handle = worker.handle();
        let (parked_tx, parked_rx) = mpsc::channel();
        handle
            .spawn_local(move |context| async move {
                let host = context.host();
                let mut kept = Timeout::new(host);
                kept.schedule_at(
                    host.now() + Duration::from_secs(600),
                    &PanickingWake::waker(true),
                );
                KEPT.with_borrow_mut(|timeouts| timeouts.push(kept));
                if after_expiry_panic {
                    let mut expiring = Timeout::new(host);
                    expiring.schedule_at(host.now() + ms(5), &PanickingWake::waker(true));
                    KEPT.with_borrow_mut(|timeouts| timeouts.push(expiring));
                }
                parked_tx.send(()).expect("the test observes the command");
                pending::<()>().await;
            })
            .expect("command admitted");
        parked_rx
            .recv_timeout(TEST_TIMEOUT)
            .expect("the command parks");
        if !after_expiry_panic {
            worker.shutdown();
        }

        let mut completion = worker.completion();
        let completed = wait(async move {
            completion
                .wait_for(Option::is_some)
                .await
                .expect("the owner thread publishes completion")
                .clone()
                .expect("completion is published")
        });
        assert_eq!(
            completed,
            Err(ManagedWorkerError::OwnerThreadPanicked),
            "after_expiry_panic: {after_expiry_panic}"
        );
        assert_eq!(handle.state(), WorkerState::Closed);
    }
}

/// A bounded teardown's deadline (`HostedWorker::shutdown_by`, `DIV-043`).
const BOUNDED_TEARDOWN: Duration = Duration::from_millis(300);

/// Reports, as it is dropped, whether the thread's Tokio host still reported
/// the worker's wheel and what the first timeout kept on the thread had
/// completed with by then.
struct WheelAtDrop(mpsc::Sender<(bool, Option<Expiry>)>);

impl Drop for WheelAtDrop {
    fn drop(&mut self) {
        let kept = KEPT.with_borrow_mut(|kept| kept.first_mut().and_then(Timeout::take_expired));
        let _ = self.0.send((Host::tokio().wheel_timer().is_some(), kept));
    }
}

/// Starts a hosted worker whose command keeps a timeout armed on the wheel for
/// ten minutes with `waker`, then registers a cleanup that arms a timeout of
/// its own and holds it, never finishing, with a `WheelAtDrop` that reports
/// to the returned receiver. `observe` runs as the owner thread's stop hook.
fn park_a_cleanup_holding_a_wheel_timeout(
    thread_name: &str,
    waker: Waker,
    observe: Option<Box<dyn FnOnce() + Send + 'static>>,
) -> (HostedWorker, mpsc::Receiver<(bool, Option<Expiry>)>) {
    let worker = HostedWorker::spawn(HostedWorkerOptions {
        command_capacity: NonZeroUsize::new(4).expect("nonzero capacity"),
        thread_name: thread_name.to_owned(),
        on_owner_thread_stop: observe,
    })
    .expect("hosted worker starts");
    let (dropped_tx, dropped_rx) = mpsc::channel();
    let (parked_tx, parked_rx) = mpsc::channel();
    worker
        .handle()
        .spawn_local(move |context| async move {
            let host = context.host().clone();
            KEPT_WHEEL.set(host.wheel_timer());
            let mut kept = Timeout::new(&host);
            kept.schedule_at(host.now() + Duration::from_secs(600), &waker);
            KEPT.with_borrow_mut(|timeouts| timeouts.push(kept));
            let _cleanup = context
                .register_cleanup_task(async move {
                    let _probe = WheelAtDrop(dropped_tx);
                    let mut held = Timeout::new(&host);
                    held.schedule_at(host.now() + Duration::from_secs(600), &noop_waker());
                    pending::<()>().await;
                })
                .expect("cleanup registered while open");
            parked_tx.send(()).expect("the test observes the command");
            pending::<()>().await;
        })
        .expect("command admitted");
    parked_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("the command parks");
    (worker, dropped_rx)
}

fn published(worker: &HostedWorker) -> Result<(), ManagedWorkerError> {
    let mut completion = worker.completion();
    wait(async move {
        completion
            .wait_for(Option::is_some)
            .await
            .expect("the owner thread publishes completion")
            .clone()
            .expect("completion is published")
    })
}

/// A bounded teardown whose deadline passes while a cleanup holds an armed
/// wheel timeout ends with `TeardownDeadlineExceeded` and stops the wheel only
/// after the final join: the cleanup, dropped there, still sees the wheel
/// installed and a timeout kept armed across the teardown not yet completed.
/// After the teardown that timeout has completed as stopped and its owner was
/// woken once, a later schedule completes as stopped at once, nothing fires,
/// and the thread's Tokio host reports no wheel (the owner thread's stop hook
/// observes it); completion is then published.
#[test]
fn a_bounded_teardown_past_its_deadline_stops_the_wheel_after_the_final_join() {
    let owner = Arc::new(CountingWake::default());
    let (report_tx, report_rx) = mpsc::channel();
    let probe_owner = Arc::clone(&owner);
    let (worker, dropped_rx) = park_a_cleanup_holding_a_wheel_timeout(
        "rox-wheel-bounded-teardown",
        Waker::from(Arc::clone(&owner)),
        Some(Box::new(move || observe_teardown(probe_owner, report_tx))),
    );
    let handle = worker.handle();
    let deadline = Instant::now() + BOUNDED_TEARDOWN;
    assert!(worker.shutdown_by(deadline));

    assert_eq!(
        published(&worker),
        Err(ManagedWorkerError::TeardownDeadlineExceeded)
    );
    assert!(
        Instant::now() >= deadline,
        "the cleanup ran until the deadline"
    );
    assert_eq!(
        dropped_rx.try_recv(),
        Ok((true, None)),
        "(wheel installed, kept timeout's expiry) as the final join dropped the cleanup"
    );
    assert_eq!(
        report_rx.try_recv(),
        Ok(Teardown {
            expiry: Some(Expiry::Stopped),
            owner_wakes: 1,
            rescheduled: Some(Expiry::Stopped),
            fired: 0,
            armed: 0,
            live_slots: 1,
            host_has_wheel: false,
        })
    );
    assert_eq!(handle.state(), WorkerState::Closed);
}

/// A timeout waker that panics when a bounded teardown, past its deadline,
/// stops the wheel after the final join is reported as the owner thread's
/// panic, over `TeardownDeadlineExceeded`, also when the payload's destructor
/// panics: the owner thread publishes completion and the process does not
/// abort.
#[test]
fn a_waker_panic_as_a_bounded_teardown_stops_the_wheel_is_reported_over_its_deadline() {
    for payload_drop_panics in [false, true] {
        let (worker, dropped_rx) = park_a_cleanup_holding_a_wheel_timeout(
            "rox-wheel-bounded-stop-panic",
            PanickingWake::waker(payload_drop_panics),
            None,
        );
        let handle = worker.handle();
        assert!(worker.shutdown_by(Instant::now() + BOUNDED_TEARDOWN));

        assert_eq!(
            published(&worker),
            Err(ManagedWorkerError::OwnerThreadPanicked),
            "payload_drop_panics: {payload_drop_panics}"
        );
        assert_eq!(
            dropped_rx.try_recv(),
            Ok((true, None)),
            "the final join dropped the cleanup before the wheel stopped, payload_drop_panics: \
             {payload_drop_panics}"
        );
        assert_eq!(handle.state(), WorkerState::Closed);
    }
}

/// What a cleanup waiting on the wheel observed.
#[derive(Debug, Eq, PartialEq)]
struct WokenFire {
    expiry: Option<Expiry>,
    early: bool,
    fired: u64,
    polls: usize,
}

/// The inline driver polls a cleanup only when
/// the cleanup's own waker fires, and the supervisor runs only when its wake
/// gate's waker fires. A cleanup that a cross-thread wake (no command, so no
/// work completion wakes the supervisor) makes arm a timeout is still woken at
/// its deadline: the arm wakes the supervisor through the gate, the
/// supervisor's expiry wakes the cleanup, and the cleanup is polled three
/// times in all.
#[test]
fn a_wheel_expiry_wakes_its_cleanup_under_woken_polling() {
    let worker = ManagedWorker::new(4).expect("managed worker starts");
    let handle = worker.handle();
    let parked = Arc::new(Mutex::new(None::<Waker>));
    let (fired_tx, fired_rx) = mpsc::channel();
    handle
        .submit({
            let parked = Arc::clone(&parked);
            move |context| {
                let host = context.host().clone();
                let worker = context.handle().clone();
                let mut timeout = Timeout::new(&host);
                let mut deadline = None;
                let mut polls = 0;
                context
                    .register_cleanup_task(poll_fn(move |task| {
                        polls += 1;
                        // Lets a failing test's shutdown finish the cleanup.
                        if worker.is_cancelled() {
                            return Poll::Ready(());
                        }
                        let Some(deadline) = deadline else {
                            if polls == 1 {
                                *parked.lock().expect("parked waker") = Some(task.waker().clone());
                            } else {
                                let at = host.now() + ms(20);
                                timeout.schedule_at(at, task.waker());
                                deadline = Some(at);
                            }
                            return Poll::Pending;
                        };
                        let expiry = timeout.take_expired();
                        if expiry.is_none() {
                            return Poll::Pending;
                        }
                        let wheel = host.wheel_timer().expect("the worker's host has a wheel");
                        let _ = fired_tx.send(WokenFire {
                            expiry,
                            early: host.now() < deadline,
                            fired: wheel.stats().fired,
                            polls,
                        });
                        Poll::Ready(())
                    }))
                    .expect("cleanup reserved")
                    .activate()
                    .expect("cleanup activated")
                    .detach();
            }
        })
        .expect("command admitted");
    let started = Instant::now();
    let waker = loop {
        if let Some(waker) = parked.lock().expect("parked waker").take() {
            break waker;
        }
        assert!(
            started.elapsed() < TEST_TIMEOUT,
            "the cleanup's first poll parks its waker"
        );
        std::thread::yield_now();
    };
    // Lets the worker finish the pass and the supervisor run that the command's
    // completion woke, so that only the arm can wake the supervisor next.
    std::thread::sleep(ms(50));
    waker.wake();
    let fire = fired_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("the wheel's expiry wakes the cleanup");
    assert_eq!(
        fire,
        WokenFire {
            expiry: Some(Expiry::Elapsed),
            early: false,
            fired: 1,
            polls: 3,
        }
    );
    worker.shutdown().expect("worker shuts down");
}
