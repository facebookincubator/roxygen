//! Driving a worker's wheel timer: the `LocalHost::wheel_timer` capability,
//! the Tokio host's worker wheel, and its wakeup in the supervisor of hosted
//! and managed workers (`run_managed_driver`).

use std::cell::RefCell;
use std::future::pending;
use std::future::ready;
use std::panic::panic_any;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::task::Wake;
use std::time::Instant;

use futures::task::noop_waker;

use super::*;
use crate::host::LocalFuture;
use crate::host::LocalHost;
use crate::host::timer::Expiry;
use crate::host::timer::Timeout;
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
                futures::future::join(command, run_managed_driver(handle.clone(), driver)).await;
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

fn hosted_worker(thread_name: &str) -> HostedWorker {
    HostedWorker::spawn(HostedWorkerOptions {
        command_capacity: NonZeroUsize::new(4).expect("nonzero capacity"),
        thread_name: thread_name.to_owned(),
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

/// Once a timer has created the wheel, every supervisor poll runs the wheel
/// step (refreshing the supervisor's waker and checking the sleep), on hosted
/// and managed workers, even with no timeout armed.
#[test]
fn once_the_wheel_exists_every_supervisor_poll_steps_it() {
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
        let before = counts(&handle);
        run_commands(&handle, 50);
        let after = counts(&handle);
        let polls = after.supervisor_polls - before.supervisor_polls;
        assert!(polls >= 50, "{before:?} {after:?}");
        assert_eq!(
            (
                after.wheel_drives - before.wheel_drives,
                after.wheel_steps - before.wheel_steps
            ),
            (polls, polls),
            "(wheel step calls, wheel steps) per supervisor poll: {before:?} {after:?}"
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
