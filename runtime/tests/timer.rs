//! Tests of the owner-local wheel timer, including ports of folly's
//! `HHWheelTimerTest` (folly/io/async/test/HHWheelTimerTest.cpp).
//!
//! The manual host fires the wheel's wakeup at exactly the instant the wheel
//! armed, so fire instants are asserted exactly, which is stricter than the
//! folly tests' `T_CHECK_NOT_EARLY` bounds.

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use super::*;

fn ms(milliseconds: u64) -> Duration {
    Duration::from_millis(milliseconds)
}

#[derive(Default)]
struct WakeupLog {
    armed: Cell<Option<Instant>>,
    arms: RefCell<Vec<Instant>>,
    disarms: Cell<usize>,
}

/// The wheel's one host wakeup on the manual host: it records each request.
struct ManualWakeup(Rc<WakeupLog>);

impl WheelWakeup for ManualWakeup {
    fn arm(&self, deadline: Instant) {
        self.0.armed.set(Some(deadline));
        self.0.arms.borrow_mut().push(deadline);
    }

    fn disarm(&self) {
        self.0.armed.set(None);
        self.0.disarms.set(self.0.disarms.get() + 1);
    }
}

/// A waker that queues its owner's id, so the harness observes owners in wake
/// order, as a FIFO executor polls woken tasks.
struct QueueWaker {
    id: usize,
    queue: Arc<Mutex<VecDeque<usize>>>,
}

impl Wake for QueueWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.queue.lock().expect("queue lock").push_back(self.id);
    }
}

type OnExpiry = Box<dyn FnMut(&Harness)>;

/// The owner of one `Timeout`, the analog of folly's `TestTimeout`. It
/// observes its slot when woken, as an owner's poll would, and records the
/// observation instant.
struct TestTimeout {
    id: usize,
    timeout: RefCell<Timeout>,
    waker: Waker,
    timestamps: RefCell<Vec<Instant>>,
    stopped: RefCell<Vec<Instant>>,
    on_expiry: RefCell<Option<OnExpiry>>,
}

impl TestTimeout {
    fn timestamps(&self) -> Vec<Instant> {
        self.timestamps.borrow().clone()
    }

    fn set_on_expiry(&self, on_expiry: impl FnMut(&Harness) + 'static) {
        *self.on_expiry.borrow_mut() = Some(Box::new(on_expiry));
    }

    fn cancel(&self) {
        self.timeout.borrow_mut().cancel();
    }

    fn deadline(&self) -> Option<Instant> {
        self.timeout.borrow().deadline()
    }
}

struct Harness {
    now: Cell<Instant>,
    wakeup: Rc<WakeupLog>,
    wheel: Rc<WheelTimer>,
    queue: Arc<Mutex<VecDeque<usize>>>,
    owners: RefCell<Vec<Rc<TestTimeout>>>,
    observed: RefCell<Vec<usize>>,
}

impl Harness {
    fn new(tick: Duration) -> Self {
        let now = Instant::now();
        let wakeup = Rc::new(WakeupLog::default());
        Self {
            now: Cell::new(now),
            wheel: WheelTimer::new(tick, now, Box::new(ManualWakeup(Rc::clone(&wakeup)))),
            wakeup,
            queue: Arc::default(),
            owners: RefCell::default(),
            observed: RefCell::default(),
        }
    }

    fn now(&self) -> Instant {
        self.now.get()
    }

    fn timeout(&self) -> Rc<TestTimeout> {
        let id = self.owners.borrow().len();
        let owner = Rc::new(TestTimeout {
            id,
            timeout: RefCell::new(Timeout::with_wheel(Rc::clone(&self.wheel))),
            waker: Waker::from(Arc::new(QueueWaker {
                id,
                queue: Arc::clone(&self.queue),
            })),
            timestamps: RefCell::default(),
            stopped: RefCell::default(),
            on_expiry: RefCell::default(),
        });
        self.owners.borrow_mut().push(Rc::clone(&owner));
        owner
    }

    /// `HHWheelTimer::scheduleTimeout(callback, timeout)`.
    fn schedule(&self, owner: &TestTimeout, timeout: Duration) {
        self.schedule_at(owner, self.now() + timeout);
    }

    fn schedule_at(&self, owner: &TestTimeout, deadline: Instant) {
        owner
            .timeout
            .borrow_mut()
            .schedule_at(deadline, &owner.waker);
    }

    fn armed(&self) -> usize {
        self.wheel.stats().armed
    }

    /// The host's wakeup fires at `now`.
    fn expire_at(&self, now: Instant) {
        self.now.set(now);
        self.wakeup.armed.set(None);
        self.wheel.expire(now);
        self.observe();
    }

    /// Owners woken by the wheel observe their slots, in wake order.
    fn observe(&self) {
        loop {
            let next = self.queue.lock().expect("queue lock").pop_front();
            let Some(id) = next else {
                return;
            };
            let owner = Rc::clone(&self.owners.borrow()[id]);
            let expiry = owner.timeout.borrow_mut().take_expired();
            match expiry {
                Some(Expiry::Elapsed) => owner.timestamps.borrow_mut().push(self.now()),
                Some(Expiry::Stopped) => owner.stopped.borrow_mut().push(self.now()),
                None => continue,
            }
            self.observed.borrow_mut().push(id);
            let on_expiry = owner.on_expiry.borrow_mut().take();
            if let Some(mut on_expiry) = on_expiry {
                on_expiry(self);
                owner.on_expiry.borrow_mut().get_or_insert(on_expiry);
            }
        }
    }

    /// `EventBase::loop()`: fires the wakeup at each armed instant until none
    /// is armed.
    fn run(&self) {
        for _ in 0..100_000 {
            let Some(armed) = self.wakeup.armed.get() else {
                return;
            };
            self.expire_at(armed.max(self.now()));
        }
        panic!("the wheel keeps its wakeup armed");
    }

    /// Fires the wakeup at each armed instant at or before `until`, then
    /// moves the clock to `until`.
    fn run_until(&self, until: Instant) {
        for _ in 0..100_000 {
            match self.wakeup.armed.get() {
                Some(armed) if armed <= until => self.expire_at(armed.max(self.now())),
                _ => {
                    self.now.set(until.max(self.now()));
                    return;
                }
            }
        }
        panic!("the wheel keeps its wakeup armed");
    }

    fn observed(&self) -> Vec<usize> {
        self.observed.borrow().clone()
    }
}

impl Drop for Harness {
    /// Breaks the cycles of expiry callbacks that capture their own owner.
    fn drop(&mut self) {
        let callbacks: Vec<_> = self
            .owners
            .borrow()
            .iter()
            .map(|owner| owner.on_expiry.borrow_mut().take())
            .collect();
        drop(callbacks);
    }
}

/// Port of `HHWheelTimerTest.FireOnce`: simple timeouts fire once, a second
/// schedule replaces the first, and equal deadlines fire in schedule order.
#[test]
fn fire_once() {
    let harness = Harness::new(ms(1));
    let t1 = harness.timeout();
    let t2 = harness.timeout();
    let t3 = harness.timeout();
    assert_eq!(harness.armed(), 0);

    harness.schedule(&t1, ms(20));
    harness.schedule(&t2, ms(20));
    // Scheduling twice replaces the first schedule: one expiry only.
    harness.schedule(&t2, ms(20));
    harness.schedule(&t3, ms(40));
    assert_eq!(harness.armed(), 3);

    let start = harness.now();
    harness.run();

    assert_eq!(t1.timestamps(), vec![start + ms(20)]);
    assert_eq!(t2.timestamps(), vec![start + ms(20)]);
    assert_eq!(t3.timestamps(), vec![start + ms(40)]);
    assert_eq!(harness.armed(), 0);
    assert_eq!(harness.observed(), vec![t1.id, t2.id, t3.id]);
}

/// Port of `HHWheelTimerTest.SlowFast`: a timeout scheduled after a later one
/// still fires first, on time.
#[test]
fn slow_fast() {
    let harness = Harness::new(ms(1));
    let t1 = harness.timeout();
    let t2 = harness.timeout();

    harness.schedule(&t1, ms(10));
    harness.schedule(&t2, ms(5));
    assert_eq!(harness.armed(), 2);

    let start = harness.now();
    harness.run();

    assert_eq!(t1.timestamps(), vec![start + ms(10)]);
    assert_eq!(t2.timestamps(), vec![start + ms(5)]);
    assert_eq!(harness.armed(), 0);
    assert_eq!(harness.observed(), vec![t2.id, t1.id]);
}

/// Port of `HHWheelTimerTest.GetTimeRemaining`, as `Timeout::deadline`: none
/// before scheduling, the exact deadline while armed, none after expiry.
#[test]
fn get_time_remaining() {
    let harness = Harness::new(ms(1));
    let t1 = harness.timeout();
    assert_eq!(t1.deadline(), None);
    assert_eq!(harness.armed(), 0);

    let start = harness.now();
    harness.schedule(&t1, ms(10));
    assert_eq!(t1.deadline(), Some(start + ms(10)));

    harness.run();

    assert_eq!(t1.deadline(), None);
    assert_eq!(harness.armed(), 0);
    assert_eq!(t1.timestamps(), vec![start + ms(10)]);
}

/// Port of `HHWheelTimerTest.NegativeTimeout`: a deadline already in the
/// past, even before the wheel's start, fires at the next expiry, before an
/// earlier-scheduled future timeout.
#[test]
fn negative_timeout() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    harness.run_until(start + ms(10));
    let tt1 = harness.timeout();
    let tt2 = harness.timeout();

    // Make sure a wakeup is armed.
    harness.schedule(&tt1, ms(1));
    // A deadline that appears earlier than the armed one.
    let past = start.checked_sub(ms(500)).unwrap_or(start);
    harness.schedule_at(&tt2, past);

    let now = harness.now();
    harness.run();

    assert_eq!(tt2.timestamps(), vec![now]);
    assert_eq!(tt1.timestamps(), vec![now + ms(1)]);
    assert_eq!(harness.observed(), vec![tt2.id, tt1.id]);
}

/// Port of `HHWheelTimerTest.prematureTimeout` as the never-early property.
/// The host is late past the first tick before a 2555 ms timeout is
/// scheduled on a 10 ms wheel; folly's tick arithmetic could fire it up to a
/// tick early, and Roxygen's wheel never fires before the deadline.
#[test]
fn premature_timeout_never_fires_early() {
    let harness = Harness::new(ms(10));
    let start = harness.now();
    let t1 = harness.timeout();
    let t2 = harness.timeout();

    harness.schedule(&t1, ms(1));
    // The host runs late: the clock passes the first tick without an expiry.
    harness.now.set(start + ms(10) + Duration::from_micros(100));
    let scheduled = harness.now();
    harness.schedule(&t2, ms(2555));

    harness.run();

    assert_eq!(t1.timestamps(), vec![scheduled]);
    assert_eq!(t2.timestamps(), vec![scheduled + ms(2555)]);
}

/// The wheel arms its wakeup at the exact deadline, not at a tick boundary,
/// and a host fire before it (an EventBase timer may fire up to a tick
/// early) fires nothing and re-arms the same deadline.
#[test]
fn early_host_fire_rearms_without_firing() {
    let harness = Harness::new(ms(10));
    let start = harness.now();
    let t1 = harness.timeout();

    harness.schedule(&t1, ms(15));
    assert_eq!(harness.wakeup.armed.get(), Some(start + ms(15)));

    harness.expire_at(start + ms(15) - Duration::from_nanos(1));
    assert!(t1.timestamps().is_empty());
    assert_eq!(harness.wakeup.armed.get(), Some(start + ms(15)));
    assert_eq!(harness.wheel.stats().empty_expiries, 1);

    harness.run();
    assert_eq!(t1.timestamps(), vec![start + ms(15)]);
}

/// Port of `HHWheelTimerTest.Level1`: a timeout in the next epoch fires on
/// time. As in folly, the wakeup first stops at the epoch boundary (tick
/// 256), where the level-1 bucket cascades, then at the deadline.
#[test]
fn level1() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let tt = harness.timeout();

    harness.schedule(&tt, ms(500));
    harness.run();

    assert_eq!(tt.timestamps(), vec![start + ms(500)]);
    assert_eq!(
        *harness.wakeup.arms.borrow(),
        vec![start + ms(256), start + ms(500)]
    );
}

/// Port of `HHWheelTimerTest.ReschedTest`: a timeout scheduled from an
/// expiry for `WHEEL_SIZE - 1` ticks fires on time.
#[test]
fn resched_test() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let t1 = harness.timeout();
    let t2 = Rc::new(harness.timeout());
    assert_eq!(harness.armed(), 0);

    harness.schedule(&t1, ms(128));
    let rescheduled = Rc::new(Cell::new(None));
    t1.set_on_expiry({
        let t2 = Rc::clone(&t2);
        let rescheduled = Rc::clone(&rescheduled);
        move |harness| {
            harness.schedule(&t2, ms(255));
            rescheduled.set(Some(harness.now()));
            assert_eq!(harness.armed(), 1);
        }
    });
    assert_eq!(harness.armed(), 1);

    harness.run();

    assert_eq!(t1.timestamps(), vec![start + ms(128)]);
    let rescheduled = rescheduled.get().expect("t1's expiry rescheduled t2");
    assert_eq!(t2.timestamps(), vec![rescheduled + ms(255)]);
    assert_eq!(harness.armed(), 0);
}

/// A level-2 timeout (65536 ticks or more ahead) cascades at its level-2
/// boundary (tick 65536), then at its level-1 boundary (tick 70000 rounded
/// down to 256, 69888), then fires at its exact deadline.
#[test]
fn level2_cascades_at_each_level_boundary() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let tt = harness.timeout();

    harness.schedule(&tt, ms(70_000));
    harness.run();

    assert_eq!(tt.timestamps(), vec![start + ms(70_000)]);
    assert_eq!(
        *harness.wakeup.arms.borrow(),
        vec![start + ms(65_536), start + ms(69_888), start + ms(70_000)]
    );
}

/// A timeout beyond folly's `LARGEST_SLOT` (2^32 - 1 ticks) is clamped into
/// the level-3 bucket of tick 2^32 - 1, which cascades at tick 0xFF000000,
/// and re-cascades from its exact deadline until it fires on time.
#[test]
fn largest_slot_clamp_recascades() {
    let tick = Duration::from_micros(1);
    let harness = Harness::new(tick);
    let start = harness.now();
    let tt = harness.timeout();
    let timeout = Duration::from_micros(5_000_000_000);

    harness.schedule(&tt, timeout);
    assert_eq!(
        harness.wakeup.armed.get(),
        Some(start + Duration::from_micros(0xFF00_0000))
    );
    harness.run();

    assert_eq!(tt.timestamps(), vec![start + timeout]);
    assert!(
        harness
            .wakeup
            .arms
            .borrow()
            .iter()
            .all(|armed| *armed <= start + timeout),
        "no wakeup is requested after the deadline"
    );
}

/// A long timeout on a busy wheel (another slot fires every tick) moves once,
/// when its level-1 bucket cascades at tick 9984, instead of once per epoch
/// (39 times in 10000 ticks) as a single-level wheel would relink it.
#[test]
fn long_timeout_on_a_busy_wheel_is_reslotted_once() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let long = harness.timeout();
    let busy = Rc::new(harness.timeout());

    harness.schedule(&long, ms(10_000));
    harness.schedule(&busy, ms(1));
    busy.set_on_expiry({
        let busy = Rc::clone(&busy);
        move |harness| {
            if harness.now() < start + ms(10_100) {
                harness.schedule(&busy, ms(1));
            }
        }
    });
    harness.run();

    assert_eq!(long.timestamps(), vec![start + ms(10_000)]);
    assert_eq!(busy.timestamps().len(), 10_100);
    assert_eq!(harness.wheel.stats().reslotted, 1);
}

/// Port of `HHWheelTimerTest.CancelTimeout`. Cancelling a fired slot before
/// its owner observes it discards the expiry (a timeout cancelled while
/// "ready to be fired"), at the head, in the middle and at the end of a
/// tick; cancelling armed slots of later ticks keeps them from firing; and a
/// slot rescheduled from its own expiry fires again.
#[test]
fn cancel_timeout() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let t5: Vec<_> = (0..5).map(|_| Rc::new(harness.timeout())).collect();
    t5.iter().for_each(|owner| harness.schedule(owner, ms(5)));
    let t10: Vec<_> = (0..3).map(|_| Rc::new(harness.timeout())).collect();
    t10.iter().for_each(|owner| harness.schedule(owner, ms(10)));
    let t20: Vec<_> = (0..2).map(|_| Rc::new(harness.timeout())).collect();
    t20.iter().for_each(|owner| harness.schedule(owner, ms(20)));

    // t5_1 cancels t5_2 (the head of the remaining due list) and t5_4 (its
    // middle).
    t5[0].set_on_expiry({
        let (t5_2, t5_4) = (Rc::clone(&t5[1]), Rc::clone(&t5[3]));
        move |_| {
            t5_2.cancel();
            t5_4.cancel();
        }
    });
    // t5_3 cancels t5_5 (the last due slot), reschedules itself once, and
    // cancels timeouts of ticks not yet due.
    t5[2].set_on_expiry({
        let t5_3 = Rc::clone(&t5[2]);
        let t5_5 = Rc::clone(&t5[4]);
        let t10_2 = Rc::clone(&t10[1]);
        let t20 = t20.clone();
        let mut rescheduled = false;
        move |harness| {
            t5_5.cancel();
            if !rescheduled {
                rescheduled = true;
                harness.schedule(&t5_3, ms(5));
            }
            t10_2.cancel();
            t20.iter().for_each(|owner| owner.cancel());
        }
    });

    harness.run();

    assert_eq!(t5[0].timestamps(), vec![start + ms(5)]);
    assert_eq!(t5[2].timestamps(), vec![start + ms(5), start + ms(10)]);
    assert_eq!(t10[0].timestamps(), vec![start + ms(10)]);
    assert_eq!(t10[2].timestamps(), vec![start + ms(10)]);
    for cancelled in [&t5[1], &t5[3], &t5[4], &t10[1], &t20[0], &t20[1]] {
        assert!(
            cancelled.timestamps().is_empty(),
            "a cancelled timeout never expires"
        );
    }
    assert_eq!(harness.armed(), 0);
    // Cancels are stores: the cancelled 20 ms bucket still wakes the host
    // once, and that expiry fires nothing.
    assert_eq!(harness.now(), start + ms(20));
    assert_eq!(harness.wheel.stats().empty_expiries, 1);
}

/// A later deadline is a store: 1000 extends of an armed slot neither relink
/// it nor re-arm the wakeup. The slot moves once, when its original bucket is
/// processed, and fires at its last deadline.
#[test]
fn later_extends_store_without_relinking() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let tt = harness.timeout();

    harness.schedule(&tt, ms(10));
    for extension in 1..=1000 {
        tt.timeout
            .borrow_mut()
            .extend_to(start + ms(10 + extension));
    }
    let stats = harness.wheel.stats();
    assert_eq!(stats.relinked, 1, "only the first schedule links");
    assert_eq!(stats.host_arms, 1, "only the first schedule arms the host");
    assert_eq!(tt.deadline(), Some(start + ms(1010)));

    harness.run();

    assert_eq!(tt.timestamps(), vec![start + ms(1010)]);
    let stats = harness.wheel.stats();
    assert_eq!(stats.relinked, 1);
    assert!(
        stats.reslotted <= 2,
        "the stale bucket relinks the slot, at most once more at a cascade: {stats:?}"
    );
}

/// An earlier deadline, by `extend_to` or `schedule_at`, relinks at once and
/// moves the wakeup earlier.
#[test]
fn earlier_deadline_relinks_and_rearms_immediately() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let t1 = harness.timeout();
    let t2 = harness.timeout();

    harness.schedule(&t1, ms(100));
    t1.timeout.borrow_mut().extend_to(start + ms(50));
    assert_eq!(harness.wakeup.armed.get(), Some(start + ms(50)));
    harness.schedule(&t2, ms(100));
    harness.schedule(&t2, ms(30));
    assert_eq!(harness.wakeup.armed.get(), Some(start + ms(30)));
    assert_eq!(harness.wheel.stats().relinked, 4);

    harness.run();

    assert_eq!(t1.timestamps(), vec![start + ms(50)]);
    assert_eq!(t2.timestamps(), vec![start + ms(30)]);
}

/// A cancel is a store: the slot stays linked until its bucket is processed,
/// which fires nothing. Re-arming it later than its link is a store too.
#[test]
fn cancel_and_later_rearm_are_stores() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let t1 = harness.timeout();
    let t2 = harness.timeout();

    harness.schedule(&t1, ms(10));
    t1.cancel();
    assert_eq!(t1.deadline(), None);
    assert_eq!(harness.armed(), 0);
    harness.schedule(&t2, ms(10));
    t2.cancel();
    harness.schedule(&t2, ms(20));
    assert_eq!(harness.wheel.stats().relinked, 2);
    assert_eq!(harness.wheel.stats().host_arms, 1);

    harness.run();

    assert!(t1.timestamps().is_empty());
    assert_eq!(t2.timestamps(), vec![start + ms(20)]);
    assert_eq!(harness.wakeup.armed.get(), None);
}

/// Freeing the wheel's last slot withdraws the wakeup, once: folly withdraws
/// its `AsyncTimeout` when `count_` reaches 0. A cancel is a store and
/// withdraws nothing, and neither does freeing a slot while another,
/// cancelled but still linked, remains: re-armed no earlier than its link,
/// that slot is covered by the wakeup and fires. A wakeup that fired with
/// nothing left to cover is not withdrawn, and an arm after a withdrawal
/// requests a new one.
#[test]
fn freeing_the_last_slot_withdraws_the_wakeup() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let waker = futures::task::noop_waker();
    let wakeup = || (harness.wakeup.disarms.get(), harness.wakeup.armed.get());
    let mut freed = Timeout::with_wheel(Rc::clone(&harness.wheel));
    let mut cancelled = Timeout::with_wheel(Rc::clone(&harness.wheel));

    freed.schedule_at(start + ms(10), &waker);
    cancelled.schedule_at(start + ms(20), &waker);
    cancelled.cancel();
    assert_eq!(wakeup(), (0, Some(start + ms(10))), "a cancel is a store");
    drop(freed);
    assert_eq!(
        wakeup(),
        (0, Some(start + ms(10))),
        "a cancelled slot still linked keeps the wakeup"
    );
    cancelled.schedule_at(start + ms(20), &waker);
    assert_eq!(
        harness.wheel.stats().host_arms,
        1,
        "re-armed at its link: covered, no new request"
    );
    harness.run();
    assert_eq!(cancelled.take_expired(), Some(Expiry::Elapsed));
    assert_eq!(harness.now(), start + ms(20));
    drop(cancelled);
    assert_eq!(
        wakeup(),
        (0, None),
        "a fired wakeup with nothing left to cover is not withdrawn"
    );

    let mut last = Timeout::with_wheel(Rc::clone(&harness.wheel));
    last.schedule_at(start + ms(100), &waker);
    assert_eq!(wakeup(), (0, Some(start + ms(100))));
    drop(last);
    assert_eq!(
        wakeup(),
        (1, None),
        "freeing the last slot while its wakeup is armed withdraws it"
    );
    let mut next = Timeout::with_wheel(Rc::clone(&harness.wheel));
    next.schedule_at(start + ms(200), &waker);
    assert_eq!(
        wakeup(),
        (1, Some(start + ms(200))),
        "an arm after a withdrawal requests a wakeup"
    );
    harness.run();
    assert_eq!(next.take_expired(), Some(Expiry::Elapsed));
    assert_eq!(harness.now(), start + ms(200));
}

/// `extend_to` changes only an armed timeout: it does nothing before the
/// first schedule or after a cancel.
#[test]
fn extend_to_needs_an_armed_timeout() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let tt = harness.timeout();

    tt.timeout.borrow_mut().extend_to(start + ms(5));
    assert_eq!(tt.deadline(), None);
    harness.schedule(&tt, ms(10));
    tt.cancel();
    tt.timeout.borrow_mut().extend_to(start + ms(5));
    assert_eq!(tt.deadline(), None);

    harness.run();

    assert!(tt.timestamps().is_empty());
    assert_eq!(harness.wheel.stats().relinked, 1);
}

/// One late expiry wakes due slots bucket by bucket in tick order, and within
/// a bucket in link order (HHWheelTimer's `timeoutsToRunNow_` order), not by
/// exact deadline.
#[test]
fn one_expiry_wakes_in_tick_order_then_fifo() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let a = harness.timeout();
    let b = harness.timeout();
    let c = harness.timeout();
    let d = harness.timeout();

    harness.schedule(&a, ms(5));
    harness.schedule(&b, ms(3) + Duration::from_micros(600));
    harness.schedule(&c, ms(3) + Duration::from_micros(100));
    harness.schedule(&d, ms(3) + Duration::from_micros(500));
    harness.expire_at(start + ms(10));

    assert_eq!(harness.observed(), vec![b.id, c.id, d.id, a.id]);
}

/// A slot extended lazily stays linked in its old bucket. When one late
/// expiry processes that bucket, the slot is relinked into its deadline's
/// bucket instead of firing early in the order, so wakes stay in tick order.
#[test]
fn lazily_extended_slots_fire_in_tick_order() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let a = harness.timeout();
    let b = harness.timeout();

    harness.schedule(&a, ms(3));
    a.timeout.borrow_mut().extend_to(start + ms(8));
    harness.schedule(&b, ms(5));
    harness.expire_at(start + ms(10));

    assert_eq!(harness.observed(), vec![b.id, a.id]);
    assert_eq!(a.timestamps(), vec![start + ms(10)]);
    assert_eq!(b.timestamps(), vec![start + ms(10)]);
}

std::thread_local! {
    static INLINE_WAKES: RefCell<Vec<Option<Box<dyn FnMut()>>>> = RefCell::default();
}

/// A waker that runs code inline when woken, inside the wheel's wake phase.
/// The code lives in a thread-local table because a `Waker` must be `Send`.
struct InlineWaker(usize);

impl Wake for InlineWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let wake = INLINE_WAKES.with(|wakes| wakes.borrow_mut()[self.0].take());
        if let Some(mut wake) = wake {
            wake();
            INLINE_WAKES.with(|wakes| wakes.borrow_mut()[self.0] = Some(wake));
        }
    }
}

fn inline_waker(wake: impl FnMut() + 'static) -> Waker {
    let id = INLINE_WAKES.with(|wakes| {
        let mut wakes = wakes.borrow_mut();
        wakes.push(Some(Box::new(wake)));
        wakes.len() - 1
    });
    Waker::from(Arc::new(InlineWaker(id)))
}

/// Port of `HHWheelTimerTest.TestSchedulingWithinCallback`: a timeout
/// scheduled from an expiry fires on time and leaves the wheel consistent.
#[test]
fn test_scheduling_within_callback() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let t1 = harness.timeout();
    let t2 = Rc::new(harness.timeout());

    harness.schedule(&t1, ms(500));
    t1.set_on_expiry({
        let t2 = Rc::clone(&t2);
        move |harness| harness.schedule(&t2, ms(1))
    });
    assert_eq!(harness.armed(), 1);

    harness.run();

    assert_eq!(harness.armed(), 0);
    assert_eq!(t1.timestamps(), vec![start + ms(500)]);
    assert_eq!(t2.timestamps(), vec![start + ms(501)]);
}

/// Wakers run after the wheel's state is released and after the wakeup is
/// re-armed, so a waker may schedule, cancel and re-arm slots: a schedule
/// earlier than the re-armed wakeup moves it earlier, and a cancel discards a
/// fired slot's expiry.
#[test]
fn reentrant_schedule_and_cancel_during_expire() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let b = harness.timeout();
    let d = Rc::new(harness.timeout());
    let e = harness.timeout();
    let a = Rc::new(RefCell::new(Timeout::with_wheel(Rc::clone(&harness.wheel))));
    let a_woken = Rc::new(Cell::new(0));
    let a_waker = inline_waker({
        let a = Rc::clone(&a);
        let b = Rc::clone(&harness.owners.borrow()[b.id]);
        let d = Rc::clone(&d);
        let a_woken = Rc::clone(&a_woken);
        move || {
            a_woken.set(a_woken.get() + 1);
            if a_woken.get() == 1 {
                b.cancel();
                let waker = d.waker.clone();
                d.timeout.borrow_mut().schedule_at(start + ms(11), &waker);
                a.borrow_mut().cancel();
            }
        }
    });

    a.borrow_mut().schedule_at(start + ms(10), &a_waker);
    harness.schedule(&b, ms(10));
    harness.schedule(&e, ms(20));
    harness.expire_at(start + ms(10));

    assert_eq!(a_woken.get(), 1);
    assert_eq!(
        a.borrow_mut().take_expired(),
        None,
        "a's own cancel discarded its expiry"
    );
    assert!(
        b.timestamps().is_empty(),
        "a's waker cancelled b before b was observed"
    );
    assert_eq!(harness.wakeup.armed.get(), Some(start + ms(11)));

    harness.run();

    assert_eq!(d.timestamps(), vec![start + ms(11)]);
    assert_eq!(e.timestamps(), vec![start + ms(20)]);
    assert_eq!(harness.armed(), 0);
}

/// Port of `HHWheelTimerTest.DeleteWheelInTimeout`, as a `Timeout` dropped
/// during expiry (an owner cannot destroy the wheel from inside `expire`,
/// whose caller holds it): t1's waker cancels t2 and drops t3. The dropped
/// slot is unlinked and freed, and the wheel stays consistent.
#[test]
fn timeout_dropped_during_expiry() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let t2 = harness.timeout();
    let t3 = Rc::new(RefCell::new(Some(Timeout::with_wheel(Rc::clone(
        &harness.wheel,
    )))));
    let t1 = Rc::new(RefCell::new(Timeout::with_wheel(Rc::clone(&harness.wheel))));
    let t1_waker = inline_waker({
        let t2 = Rc::clone(&harness.owners.borrow()[t2.id]);
        let t3 = Rc::clone(&t3);
        move || {
            t2.cancel();
            drop(t3.borrow_mut().take());
        }
    });

    t1.borrow_mut().schedule_at(start + ms(128), &t1_waker);
    harness.schedule(&t2, ms(128));
    t3.borrow_mut()
        .as_mut()
        .expect("t3 exists")
        .schedule_at(start + ms(128), &futures::task::noop_waker());
    assert_eq!(harness.armed(), 3);
    assert_eq!(harness.wheel.stats().live_slots, 3);

    harness.expire_at(start + ms(128));

    assert_eq!(t1.borrow_mut().take_expired(), Some(Expiry::Elapsed));
    assert!(t2.timestamps().is_empty());
    assert!(t3.borrow().is_none());
    assert_eq!(harness.wheel.stats().live_slots, 2);
    assert_eq!(harness.armed(), 0);

    let t4 = harness.timeout();
    harness.schedule(&t4, ms(1));
    assert_eq!(
        harness.wheel.stats().live_slots,
        3,
        "t4 reuses t3's freed slot"
    );
    harness.run();
    assert_eq!(t4.timestamps(), vec![start + ms(129)]);
}

/// A waker that panics with its payload.
struct PanickingWaker(&'static str);

impl Wake for PanickingWaker {
    fn wake(self: Arc<Self>) {
        std::panic::panic_any(self.0);
    }
}

/// Wakers that panic during `expire` do not stop the others: every fired
/// slot is marked and every waker is woken, the wakeup is already re-armed,
/// and the first panic is re-raised after the last wake.
#[test]
fn waker_panics_leave_the_wheel_consistent() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let a = harness.timeout();
    let b = Timeout::with_wheel(Rc::clone(&harness.wheel));
    let c = harness.timeout();
    let d = Timeout::with_wheel(Rc::clone(&harness.wheel));
    let e = harness.timeout();
    let (mut b, mut d) = (b, d);

    harness.schedule(&a, ms(10));
    b.schedule_at(
        start + ms(10),
        &Waker::from(Arc::new(PanickingWaker("b's waker"))),
    );
    harness.schedule(&c, ms(10));
    d.schedule_at(
        start + ms(10),
        &Waker::from(Arc::new(PanickingWaker("d's waker"))),
    );
    harness.schedule(&e, ms(20));

    let payload = std::panic::catch_unwind(AssertUnwindSafe(|| harness.expire_at(start + ms(10))))
        .expect_err("a waker's panic is re-raised");
    assert_eq!(payload.downcast_ref::<&str>(), Some(&"b's waker"));

    // a and c were woken although b's and d's wakers panicked between them.
    harness.observe();
    assert_eq!(a.timestamps(), vec![start + ms(10)]);
    assert_eq!(c.timestamps(), vec![start + ms(10)]);
    assert_eq!(b.take_expired(), Some(Expiry::Elapsed));
    assert_eq!(d.take_expired(), Some(Expiry::Elapsed));
    assert_eq!(harness.armed(), 1);
    assert_eq!(harness.wakeup.armed.get(), Some(start + ms(20)));

    harness.run();
    assert_eq!(e.timestamps(), vec![start + ms(20)]);
}

std::thread_local! {
    static CLONE_HOOKS: RefCell<Vec<Option<Box<dyn FnOnce()>>>> = RefCell::default();
    static HOOKED_WAKES: RefCell<Vec<usize>> = RefCell::default();
    static HOOKED_LIVE: RefCell<Vec<usize>> = RefCell::default();
}

/// A waker whose next clone runs the hook armed by `arm_clone_hook`, as a
/// waker's `clone` is arbitrary code. It counts its wakes and its live
/// clones. Returns its id with it.
fn clone_hook_waker() -> (usize, Waker) {
    fn raw(id: *const ()) -> RawWaker {
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop_waker);
        RawWaker::new(id, &VTABLE)
    }
    fn clone(id: *const ()) -> RawWaker {
        let hook = CLONE_HOOKS.with(|hooks| hooks.borrow_mut()[id.addr()].take());
        if let Some(hook) = hook {
            hook();
        }
        HOOKED_LIVE.with(|live| live.borrow_mut()[id.addr()] += 1);
        raw(id)
    }
    fn wake(id: *const ()) {
        wake_by_ref(id);
        drop_waker(id);
    }
    fn wake_by_ref(id: *const ()) {
        HOOKED_WAKES.with(|wakes| wakes.borrow_mut()[id.addr()] += 1);
    }
    fn drop_waker(id: *const ()) {
        HOOKED_LIVE.with(|live| live.borrow_mut()[id.addr()] -= 1);
    }

    let id = CLONE_HOOKS.with(|hooks| {
        let mut hooks = hooks.borrow_mut();
        hooks.push(None);
        hooks.len() - 1
    });
    HOOKED_WAKES.with(|wakes| wakes.borrow_mut().push(0));
    HOOKED_LIVE.with(|live| live.borrow_mut().push(1));
    // SAFETY: `RawWaker`'s contract holds: the data is an index carried as an
    // address that no callback dereferences, every clone is valid on its own,
    // nothing is owned, and the callbacks are safe code over thread-locals, so
    // calling them from any thread is sound.
    let waker = unsafe { Waker::from_raw(raw(std::ptr::without_provenance(id))) };
    (id, waker)
}

fn arm_clone_hook(id: usize, hook: impl FnOnce() + 'static) {
    CLONE_HOOKS.with(|hooks| hooks.borrow_mut()[id] = Some(Box::new(hook)));
}

fn hooked_wakes(id: usize) -> usize {
    HOOKED_WAKES.with(|wakes| wakes.borrow()[id])
}

fn hooked_live(id: usize) -> usize {
    HOOKED_LIVE.with(|live| live.borrow()[id])
}

/// A waker clone that panics during `expire` does not stop the expiry: every
/// due slot is marked and the other owners are woken, the wakeup is re-armed,
/// and the panic is re-raised after the last wake.
#[test]
fn waker_clone_panic_during_expire_leaves_the_wheel_consistent() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let a = harness.timeout();
    let mut b = Timeout::with_wheel(Rc::clone(&harness.wheel));
    let (b_id, b_waker) = clone_hook_waker();
    let c = harness.timeout();
    let e = harness.timeout();

    harness.schedule(&a, ms(10));
    b.schedule_at(start + ms(10), &b_waker);
    harness.schedule(&c, ms(10));
    harness.schedule(&e, ms(20));
    arm_clone_hook(b_id, || std::panic::panic_any("b's clone"));

    let payload = std::panic::catch_unwind(AssertUnwindSafe(|| harness.expire_at(start + ms(10))))
        .expect_err("the clone's panic is re-raised");
    assert_eq!(payload.downcast_ref::<&str>(), Some(&"b's clone"));

    harness.observe();
    assert_eq!(a.timestamps(), vec![start + ms(10)]);
    assert_eq!(c.timestamps(), vec![start + ms(10)]);
    assert_eq!(b.take_expired(), Some(Expiry::Elapsed));
    assert_eq!(harness.armed(), 1);
    assert_eq!(harness.wakeup.armed.get(), Some(start + ms(20)));

    harness.run();
    assert_eq!(e.timestamps(), vec![start + ms(20)]);
}

/// A waker clone may re-enter the wheel during `expire`: b's clone cancels c
/// before c is observed and schedules d earlier than the re-armed wakeup.
#[test]
fn waker_clone_reentering_the_wheel_during_expire() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let mut b = Timeout::with_wheel(Rc::clone(&harness.wheel));
    let (b_id, b_waker) = clone_hook_waker();
    let c = harness.timeout();
    let d = harness.timeout();
    let e = harness.timeout();

    b.schedule_at(start + ms(10), &b_waker);
    harness.schedule(&c, ms(10));
    harness.schedule(&e, ms(20));
    arm_clone_hook(b_id, {
        let c = Rc::clone(&c);
        let d = Rc::clone(&d);
        move || {
            c.cancel();
            d.timeout.borrow_mut().schedule_at(start + ms(15), &d.waker);
        }
    });

    harness.expire_at(start + ms(10));

    assert_eq!(hooked_wakes(b_id), 1);
    assert_eq!(b.take_expired(), Some(Expiry::Elapsed));
    assert!(c.timestamps().is_empty(), "b's clone cancelled c");
    assert_eq!(harness.wakeup.armed.get(), Some(start + ms(15)));

    harness.run();
    assert_eq!(d.timestamps(), vec![start + ms(15)]);
    assert_eq!(e.timestamps(), vec![start + ms(20)]);
    assert_eq!(harness.armed(), 0);
}

/// A waker clone that reschedules its own timeout with another waker during
/// `expire` keeps that waker: the next fire wakes it, not the cloned one.
#[test]
fn waker_clone_rescheduling_its_own_timeout_keeps_the_new_waker() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let timeout = Rc::new(RefCell::new(Timeout::with_wheel(Rc::clone(&harness.wheel))));
    let (first_id, first) = clone_hook_waker();
    let (second_id, second) = clone_hook_waker();

    timeout.borrow_mut().schedule_at(start + ms(10), &first);
    arm_clone_hook(first_id, {
        let timeout = Rc::clone(&timeout);
        move || timeout.borrow_mut().schedule_at(start + ms(20), &second)
    });
    harness.expire_at(start + ms(10));

    assert_eq!(hooked_wakes(first_id), 1);
    assert_eq!(timeout.borrow().deadline(), Some(start + ms(20)));
    harness.run();
    assert_eq!(
        (hooked_wakes(first_id), hooked_wakes(second_id)),
        (1, 1),
        "the 20 ms fire wakes the waker of the latest schedule"
    );
    assert_eq!(timeout.borrow_mut().take_expired(), Some(Expiry::Elapsed));
    assert_eq!(
        hooked_live(first_id),
        1,
        "the slot no longer holds the first waker"
    );
}

/// A waker clone that drops its own timeout during `expire` frees the slot
/// with no waker left in it.
#[test]
fn waker_clone_dropping_its_own_timeout_leaves_no_waker_behind() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let timeout = Rc::new(RefCell::new(Some(Timeout::with_wheel(Rc::clone(
        &harness.wheel,
    )))));
    let (id, waker) = clone_hook_waker();

    timeout
        .borrow_mut()
        .as_mut()
        .expect("the timeout exists")
        .schedule_at(start + ms(10), &waker);
    assert_eq!(hooked_live(id), 2, "the slot holds a clone");
    arm_clone_hook(id, {
        let timeout = Rc::clone(&timeout);
        move || drop(timeout.borrow_mut().take())
    });
    harness.expire_at(start + ms(10));

    assert_eq!(hooked_wakes(id), 1);
    assert_eq!(harness.wheel.stats().live_slots, 0);
    assert_eq!(hooked_live(id), 1, "only the test's own waker is left");
}

/// A waker clone that panics during `stop` does not stop it: every armed
/// timeout completes as stopped and the other owners are woken, the wakeup is
/// withdrawn, and the panic is re-raised.
#[test]
fn waker_clone_panic_during_stop_leaves_the_wheel_consistent() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let a = harness.timeout();
    let mut b = Timeout::with_wheel(Rc::clone(&harness.wheel));
    let (b_id, b_waker) = clone_hook_waker();
    let c = harness.timeout();

    harness.schedule(&a, ms(10));
    b.schedule_at(start + ms(50), &b_waker);
    harness.schedule(&c, ms(100_000));
    arm_clone_hook(b_id, || std::panic::panic_any("b's clone"));

    let payload = std::panic::catch_unwind(AssertUnwindSafe(|| harness.wheel.stop()))
        .expect_err("the clone's panic is re-raised");
    assert_eq!(payload.downcast_ref::<&str>(), Some(&"b's clone"));

    assert_eq!(harness.wakeup.disarms.get(), 1);
    harness.observe();
    assert_eq!(*a.stopped.borrow(), vec![start]);
    assert_eq!(*c.stopped.borrow(), vec![start]);
    assert_eq!(b.take_expired(), Some(Expiry::Stopped));
    assert_eq!(harness.armed(), 0);
    assert_eq!(harness.wheel.stop(), 0);
}

/// A waker clone may re-enter the wheel during `stop`: a schedule made from
/// it completes as stopped at once.
#[test]
fn waker_clone_reentering_the_wheel_during_stop() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let mut b = Timeout::with_wheel(Rc::clone(&harness.wheel));
    let (b_id, b_waker) = clone_hook_waker();
    let d = harness.timeout();

    b.schedule_at(start + ms(10), &b_waker);
    arm_clone_hook(b_id, {
        let d = Rc::clone(&d);
        move || {
            d.timeout.borrow_mut().schedule_at(start + ms(5), &d.waker);
        }
    });

    assert_eq!(harness.wheel.stop(), 1);

    assert_eq!(hooked_wakes(b_id), 1);
    assert_eq!(b.take_expired(), Some(Expiry::Stopped));
    harness.observe();
    assert_eq!(*d.stopped.borrow(), vec![start]);
    assert_eq!(d.deadline(), None);
}

/// `schedule_at` clones its waker with the wheel's state released, so the
/// clone may re-enter the wheel: here it schedules another timeout.
#[test]
fn waker_clone_reentering_the_wheel_during_schedule() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let other = harness.timeout();
    let mut timeout = Timeout::with_wheel(Rc::clone(&harness.wheel));
    let (id, waker) = clone_hook_waker();

    arm_clone_hook(id, {
        let other = Rc::clone(&other);
        move || {
            other
                .timeout
                .borrow_mut()
                .schedule_at(start + ms(5), &other.waker);
        }
    });
    timeout.schedule_at(start + ms(10), &waker);

    assert_eq!(timeout.deadline(), Some(start + ms(10)));
    assert_eq!(other.deadline(), Some(start + ms(5)));
    harness.run();
    assert_eq!(other.timestamps(), vec![start + ms(5)]);
    assert_eq!(hooked_wakes(id), 1);
    assert_eq!(timeout.take_expired(), Some(Expiry::Elapsed));
}

/// `poll_expired` clones a new waker with the wheel's state released, so the
/// clone may re-enter the wheel: here it cancels another timeout. The fire
/// then wakes the polled waker.
#[test]
fn waker_clone_reentering_the_wheel_during_poll_expired() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let other = harness.timeout();
    let mut timeout = Timeout::with_wheel(Rc::clone(&harness.wheel));
    let (first_id, first) = clone_hook_waker();
    let (second_id, second) = clone_hook_waker();

    harness.schedule(&other, ms(5));
    timeout.schedule_at(start + ms(10), &first);
    arm_clone_hook(second_id, {
        let other = Rc::clone(&other);
        move || other.cancel()
    });
    assert_eq!(
        timeout.poll_expired(&mut Context::from_waker(&second)),
        Poll::Pending
    );

    assert_eq!(other.deadline(), None);
    harness.run();
    assert!(other.timestamps().is_empty());
    assert_eq!((hooked_wakes(first_id), hooked_wakes(second_id)), (0, 1));
    assert_eq!(timeout.take_expired(), Some(Expiry::Elapsed));
}

/// `extend_to` on a stopped wheel wakes the slot's waker through a clone made
/// with the wheel's state released, so the clone may re-enter the wheel: a
/// schedule from it completes as stopped.
#[test]
fn waker_clone_reentering_the_wheel_during_extend_on_a_stopped_wheel() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let other = harness.timeout();
    let mut timeout = Timeout::with_wheel(Rc::clone(&harness.wheel));
    let (id, waker) = clone_hook_waker();

    timeout.schedule_at(start + ms(10), &waker);
    harness.wheel.expire(start + ms(10));
    assert_eq!(hooked_wakes(id), 1);
    assert_eq!(
        harness.wheel.stop(),
        1,
        "the unobserved fire completes as stopped"
    );
    arm_clone_hook(id, {
        let other = Rc::clone(&other);
        move || {
            other
                .timeout
                .borrow_mut()
                .schedule_at(start + ms(20), &other.waker);
        }
    });
    timeout.extend_to(start + ms(30));

    assert_eq!(hooked_wakes(id), 2);
    assert_eq!(timeout.take_expired(), Some(Expiry::Stopped));
    harness.observe();
    assert_eq!(*other.stopped.borrow(), vec![start]);
}

/// A fire is discarded by activity before the owner observes it: a later
/// `extend_to` or `schedule_at` on a fired slot re-arms it, as a read
/// completing in the same poll makes its timeout moot.
#[test]
fn stale_fire_is_discarded_by_later_activity() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let extended = harness.timeout();
    let rescheduled = harness.timeout();

    harness.schedule(&extended, ms(10));
    harness.schedule(&rescheduled, ms(10));
    harness.wheel.expire(start + ms(10));
    harness.now.set(start + ms(10));
    // Both slots fired; their owners have not observed them yet.
    extended.timeout.borrow_mut().extend_to(start + ms(30));
    harness.schedule_at(&rescheduled, start + ms(20));
    harness.observe();

    assert!(extended.timestamps().is_empty());
    assert!(rescheduled.timestamps().is_empty());
    assert_eq!(extended.deadline(), Some(start + ms(30)));
    assert_eq!(harness.armed(), 2);

    harness.run();

    assert_eq!(rescheduled.timestamps(), vec![start + ms(20)]);
    assert_eq!(extended.timestamps(), vec![start + ms(30)]);
}

/// `poll_expired` keeps the latest waker: a fire wakes the waker of the last
/// poll, the next poll takes the expiry, and an idle timeout stays pending.
#[test]
fn poll_expired_wakes_the_latest_waker() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let first = harness.timeout();
    let second = harness.timeout();
    let mut timeout = Timeout::with_wheel(Rc::clone(&harness.wheel));
    let mut first_context = Context::from_waker(&first.waker);
    let mut second_context = Context::from_waker(&second.waker);

    timeout.schedule_at(start + ms(5), &first.waker);
    assert_eq!(timeout.poll_expired(&mut second_context), Poll::Pending);
    harness.wheel.expire(start + ms(5));

    assert_eq!(
        harness
            .queue
            .lock()
            .expect("queue lock")
            .drain(..)
            .collect::<Vec<_>>(),
        vec![second.id]
    );
    assert_eq!(
        timeout.poll_expired(&mut first_context),
        Poll::Ready(Expiry::Elapsed)
    );
    assert_eq!(timeout.poll_expired(&mut first_context), Poll::Pending);
}

/// Port of `HHWheelTimerTest.cancelAll`, as `stop`: two timeouts expire in
/// one expiry and each owner stops the wheel when it observes its expiry.
/// The first stop completes the other, still unobserved, timeout as stopped
/// (folly cancels `timeoutsToRunNow_`), so exactly one is stopped.
#[test]
fn cancel_all_as_stop() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let t1 = harness.timeout();
    let t2 = harness.timeout();
    let stopped = Rc::new(Cell::new(0));
    for owner in [&t1, &t2] {
        harness.schedule(owner, ms(1));
        owner.set_on_expiry({
            let stopped = Rc::clone(&stopped);
            move |harness| stopped.set(stopped.get() + harness.wheel.stop())
        });
    }

    harness.run();

    assert_eq!(t1.timestamps(), vec![start + ms(1)]);
    assert_eq!(*t2.stopped.borrow(), vec![start + ms(1)]);
    assert!(t2.timestamps().is_empty());
    assert_eq!(stopped.get(), 1);
}

/// `stop` is `~HHWheelTimer`'s `cancelAll`: armed slots, on any level,
/// complete as stopped and are woken; a cancelled slot stays silent; the
/// wakeup is withdrawn; later expiries do nothing; and later schedules
/// complete as stopped at once.
#[test]
fn stop_completes_armed_slots_and_later_schedules_as_stopped() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let near = harness.timeout();
    let far = harness.timeout();
    let cancelled = harness.timeout();

    harness.schedule(&near, ms(10));
    harness.schedule(&far, ms(100_000));
    harness.schedule(&cancelled, ms(5));
    cancelled.cancel();

    assert_eq!(harness.wheel.stop(), 2);
    assert_eq!(harness.wakeup.disarms.get(), 1);
    harness.observe();
    assert_eq!(*near.stopped.borrow(), vec![start]);
    assert_eq!(*far.stopped.borrow(), vec![start]);
    assert!(cancelled.stopped.borrow().is_empty());
    assert_eq!(harness.armed(), 0);

    harness.wakeup.armed.set(None);
    harness.wheel.expire(start + ms(200_000));
    assert_eq!(harness.wakeup.armed.get(), None);

    harness.schedule(&near, ms(10));
    assert_eq!(near.deadline(), None);
    harness.observe();
    assert_eq!(*near.stopped.borrow(), vec![start, start]);
    assert_eq!(harness.wheel.stop(), 0, "a second stop finds nothing armed");
    assert!(near.timestamps().is_empty() && far.timestamps().is_empty());
}

/// SplitMix64: a small deterministic generator for the model test.
struct SplitMix(u64);

impl SplitMix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

/// The reference for one owner: its armed deadline, and the tick the wheel
/// had processed when the owner armed it. A deadline of an already processed
/// tick is linked into that tick's bucket (folly's `diff < 0` case), so that
/// tick is the slot's place in the wake order.
#[derive(Clone, Copy)]
struct ModelTimeout {
    deadline: Instant,
    floor_tick: u64,
}

/// A random deadline around `now`: past, inside the current tick, on an
/// exact tick boundary, or up to `span_ticks` ahead.
fn model_deadline(
    rng: &mut SplitMix,
    start: Instant,
    now: Instant,
    tick: Duration,
    span_ticks: u64,
) -> Instant {
    let tick_nanos = tick.as_nanos() as u64;
    match rng.below(10) {
        0 => now
            .checked_sub(Duration::from_nanos(rng.below(3 * tick_nanos)))
            .unwrap_or(now),
        1 => now + Duration::from_nanos(rng.below(tick_nanos)),
        2 => {
            let elapsed = now.duration_since(start).as_nanos() as u64 / tick_nanos;
            start + Duration::from_nanos((elapsed + rng.below(span_ticks) + 1) * tick_nanos)
        }
        _ => now + Duration::from_nanos(rng.below(span_ticks.saturating_mul(tick_nanos))),
    }
}

/// Random schedule, extend, cancel, drop and advance against a `BTreeMap`
/// reference. The host fires the wakeup whenever the clock reaches it, and
/// sometimes spuriously. Each expiry must wake exactly the reference's due
/// set (never early, and never late at the host's resolution, which is
/// stricter than folly's one-tick bound), in tick order; and the wakeup must
/// never be armed after a due deadline.
fn run_model(seed: u64, tick: Duration, span_ticks: u64, steps: usize) {
    const OWNERS: usize = 12;
    let mut rng = SplitMix(seed);
    let harness = Harness::new(tick);
    let start = harness.now();
    let tick_of = |instant: Instant| {
        (instant.saturating_duration_since(start).as_nanos() / tick.as_nanos()) as u64
    };
    let owners: Vec<_> = (0..OWNERS).map(|_| harness.timeout()).collect();
    let mut model: [Option<ModelTimeout>; OWNERS] = [None; OWNERS];
    let mut processed_tick = 0;

    for step in 0..steps {
        let id = rng.below(OWNERS as u64) as usize;
        let owner = &owners[id];
        let now = harness.now();
        match rng.below(100) {
            0..=29 => {
                let deadline = model_deadline(&mut rng, start, now, tick, span_ticks);
                harness.schedule_at(owner, deadline);
                model[id] = Some(ModelTimeout {
                    deadline,
                    floor_tick: processed_tick,
                });
            }
            30..=49 => {
                let deadline = model_deadline(&mut rng, start, now, tick, span_ticks);
                owner.timeout.borrow_mut().extend_to(deadline);
                if model[id].is_some() {
                    model[id] = Some(ModelTimeout {
                        deadline,
                        floor_tick: processed_tick,
                    });
                }
            }
            50..=57 => {
                owner.cancel();
                model[id] = None;
            }
            58..=61 => {
                *owner.timeout.borrow_mut() = Timeout::with_wheel(Rc::clone(&harness.wheel));
                model[id] = None;
            }
            choice => {
                let armed = harness.wakeup.armed.get();
                let now = match (choice % 4, armed) {
                    (0, Some(armed)) if armed > now => armed,
                    (1, _) => now,
                    (2, _) => now + Duration::from_nanos(rng.below(4 * tick.as_nanos() as u64)),
                    _ => {
                        now + Duration::from_nanos(
                            rng.below(span_ticks.saturating_mul(tick.as_nanos() as u64)),
                        )
                    }
                };
                harness.now.set(now);
                let host_fires = armed.is_some_and(|armed| armed <= now);
                if host_fires || rng.below(20) == 0 {
                    harness.wakeup.armed.set(None);
                    harness.wheel.expire(now);
                    let woken: Vec<usize> = harness
                        .queue
                        .lock()
                        .expect("queue lock")
                        .drain(..)
                        .collect();
                    let due: BTreeMap<(Instant, usize), u64> = model
                        .iter()
                        .enumerate()
                        .filter_map(|(id, timeout)| {
                            timeout
                                .filter(|timeout| timeout.deadline <= now)
                                .map(|timeout| {
                                    (
                                        (timeout.deadline, id),
                                        tick_of(timeout.deadline).max(timeout.floor_tick),
                                    )
                                })
                        })
                        .collect();
                    let mut woken_sorted = woken.clone();
                    woken_sorted.sort_unstable();
                    let mut due_ids: Vec<usize> = due.keys().map(|(_, id)| *id).collect();
                    due_ids.sort_unstable();
                    assert_eq!(
                        woken_sorted, due_ids,
                        "seed {seed} step {step}: the woken set is the due set"
                    );
                    let order: Vec<u64> = woken
                        .iter()
                        .map(|id| {
                            let timeout = model[*id].expect("woken timeouts were armed");
                            tick_of(timeout.deadline).max(timeout.floor_tick)
                        })
                        .collect();
                    assert!(
                        order.windows(2).all(|pair| pair[0] <= pair[1]),
                        "seed {seed} step {step}: wakes in tick order: {order:?}"
                    );
                    for id in woken {
                        assert_eq!(
                            owners[id].timeout.borrow_mut().take_expired(),
                            Some(Expiry::Elapsed)
                        );
                        model[id] = None;
                    }
                    processed_tick = processed_tick.max(tick_of(now));
                }
                assert!(
                    model.iter().flatten().all(|timeout| timeout.deadline > now),
                    "seed {seed} step {step}: a due timeout is left without an expiry (wakeup {:?})",
                    harness.wakeup.armed.get()
                );
            }
        }
        assert_eq!(
            harness.armed(),
            model.iter().flatten().count(),
            "seed {seed} step {step}"
        );
        assert!(owners
            .iter()
            .zip(&model)
            .all(|(owner, timeout)| owner.deadline() == timeout.map(|timeout| timeout.deadline)));
    }
}

#[test]
fn model_one_millisecond_tick_two_levels() {
    (1..=4).for_each(|seed| run_model(seed, ms(1), 600, 20_000));
}

#[test]
fn model_one_millisecond_tick_three_levels() {
    (11..=14).for_each(|seed| run_model(seed, ms(1), 200_000, 20_000));
}

#[test]
fn model_ten_millisecond_tick() {
    (21..=24).for_each(|seed| run_model(seed, ms(10), 3_000, 20_000));
}

#[test]
fn model_one_microsecond_tick_past_the_largest_slot() {
    (31..=34).for_each(|seed| run_model(seed, Duration::from_micros(1), 1 << 33, 20_000));
}

/// Counts heap allocations made on a thread while its counter is enabled
/// (the pattern of `benchmarks/src/runtime_task_wrapping.rs`).
struct CountingAllocator;

std::thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

fn record_allocation() {
    if COUNTING.with(Cell::get) {
        ALLOCATIONS.with(|count| count.set(count.get() + 1));
    }
}

// SAFETY: every method passes its arguments unchanged to the same `System`
// method, so `System` upholds the `GlobalAlloc` contract. Counting touches only
// const-initialized thread-local cells, which neither allocate nor register
// destructors.
unsafe impl std::alloc::GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        record_allocation();
        // SAFETY: this call's caller upheld `alloc`'s layout precondition.
        unsafe { std::alloc::System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: std::alloc::Layout) -> *mut u8 {
        record_allocation();
        // SAFETY: this call's caller upheld `alloc_zeroed`'s layout precondition.
        unsafe { std::alloc::System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(
        &self,
        pointer: *mut u8,
        layout: std::alloc::Layout,
        new_size: usize,
    ) -> *mut u8 {
        record_allocation();
        // SAFETY: `pointer` came from this allocator, which is `System`, with
        // `layout`, and this call's caller upheld `realloc`'s size precondition.
        unsafe { std::alloc::System.realloc(pointer, layout, new_size) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: std::alloc::Layout) {
        // SAFETY: `pointer` came from this allocator, which is `System`, with
        // `layout`.
        unsafe { std::alloc::System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// The allocations `operation` makes on this thread, or `None` where they are
/// not observable: dev builds link a shared std whose internal allocations
/// bypass this crate's `#[global_allocator]`, so counts are complete only when
/// an allocation made inside std is observed.
pub(crate) fn count_allocations(operation: impl FnOnce()) -> Option<usize> {
    ALLOCATIONS.with(|count| count.set(0));
    COUNTING.with(|counting| counting.set(true));
    operation();
    COUNTING.with(|counting| counting.set(false));
    let count = ALLOCATIONS.with(Cell::get);

    ALLOCATIONS.with(|count| count.set(0));
    COUNTING.with(|counting| counting.set(true));
    drop(std::hint::black_box(Vec::<u8>::with_capacity(4_096)));
    COUNTING.with(|counting| counting.set(false));
    (ALLOCATIONS.with(Cell::get) == 1).then_some(count)
}

/// A wakeup that only records the armed instant.
struct LatestWakeup(Rc<Cell<Option<Instant>>>);

impl WheelWakeup for LatestWakeup {
    fn arm(&self, deadline: Instant) {
        self.0.set(Some(deadline));
    }

    fn disarm(&self) {
        self.0.set(None);
    }
}

/// After warm-up, schedule, extend (later and earlier), cancel, expiry with
/// fires, cascades and re-slots, observation, and replacing a dropped
/// timeout allocate nothing: slots come from the slab's free list and the
/// wake list keeps its capacity.
#[test]
fn timer_operations_allocate_nothing_after_warm_up() {
    let start = Instant::now();
    let armed = Rc::new(Cell::new(None));
    let wheel = WheelTimer::new(ms(1), start, Box::new(LatestWakeup(Rc::clone(&armed))));
    let waker = futures::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut timeouts: Vec<_> = (0..64)
        .map(|_| Timeout::with_wheel(Rc::clone(&wheel)))
        .collect();
    let mut now = start;
    let mut round = |now: &mut Instant, timeouts: &mut Vec<Timeout>| {
        let base = *now;
        for (index, timeout) in timeouts.iter_mut().enumerate() {
            let offset = ms(1 + (index as u64 * 37) % 700);
            timeout.schedule_at(base + offset, &waker);
            timeout.extend_to(base + offset + ms(3));
            match index % 4 {
                0 => timeout.cancel(),
                1 => timeout.extend_to(base + offset / 2),
                _ => {}
            }
        }
        let replaced = std::mem::replace(&mut timeouts[5], Timeout::with_wheel(Rc::clone(&wheel)));
        drop(replaced);
        timeouts[5].schedule_at(base + ms(2), &waker);
        while let Some(at) = armed.take() {
            *now = at;
            wheel.expire(at);
            for timeout in timeouts.iter_mut() {
                let _ = timeout.poll_expired(&mut context);
                let _ = timeout.take_expired();
            }
        }
    };

    round(&mut now, &mut timeouts);
    round(&mut now, &mut timeouts);
    let allocations = count_allocations(|| {
        for _ in 0..3 {
            round(&mut now, &mut timeouts);
        }
    });

    if let Some(allocations) = allocations {
        assert_eq!(
            allocations, 0,
            "timer operations after warm-up allocate nothing"
        );
    }
    let stats = wheel.stats();
    assert!(
        stats.reslotted > 0 && stats.relinked > 0,
        "the rounds exercise relinks and re-slots: {stats:?}"
    );
}

/// The clock and sleeps of `SleepHost`.
struct SleepClock {
    now: Cell<Instant>,
    /// How long before its deadline a sleep resolves. An early sleep never
    /// resolves at the instant it was made, as an EventBase timer fires at a
    /// later tick.
    early: Duration,
    now_calls: Cell<usize>,
    sleeps_made: Cell<usize>,
    sleeps: RefCell<Vec<Rc<SleepEntry>>>,
}

struct SleepEntry {
    resolves_at: Instant,
    made_at: Instant,
    waker: RefCell<Option<Waker>>,
}

impl SleepClock {
    fn now(&self) -> Instant {
        self.now.get()
    }

    fn resolved(&self, entry: &SleepEntry) -> bool {
        let now = self.now.get();
        now >= entry.resolves_at && (self.early.is_zero() || now > entry.made_at)
    }

    fn advance_to(&self, now: Instant) {
        self.now.set(now);
        let ready: Vec<Waker> = self
            .sleeps
            .borrow()
            .iter()
            .filter(|entry| self.resolved(entry))
            .filter_map(|entry| entry.waker.borrow_mut().take())
            .collect();
        ready.into_iter().for_each(Waker::wake);
    }
}

struct HostSleep {
    entry: Rc<SleepEntry>,
    clock: Rc<SleepClock>,
}

impl std::future::Future for HostSleep {
    type Output = ();

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.clock.resolved(&self.entry) {
            return Poll::Ready(());
        }
        *self.entry.waker.borrow_mut() = Some(cx.waker().clone());
        Poll::Pending
    }
}

/// A host without a wheel: a manual clock, and sleeps it counts.
struct SleepHost(Rc<SleepClock>);

impl crate::host::LocalHost for SleepHost {
    fn spawn_local(&self, _future: crate::host::LocalFuture) {
        unreachable!("timeouts spawn nothing");
    }

    fn now(&self) -> Instant {
        self.0.now_calls.set(self.0.now_calls.get() + 1);
        self.0.now.get()
    }

    fn sleep_until(&self, deadline: Instant) -> crate::host::LocalFuture {
        self.0.sleeps_made.set(self.0.sleeps_made.get() + 1);
        let entry = Rc::new(SleepEntry {
            resolves_at: deadline.checked_sub(self.0.early).unwrap_or(deadline),
            made_at: self.0.now.get(),
            waker: RefCell::new(None),
        });
        self.0.sleeps.borrow_mut().push(Rc::clone(&entry));
        Box::pin(HostSleep {
            entry,
            clock: Rc::clone(&self.0),
        })
    }

    fn yield_to_io(&self) -> crate::host::LocalFuture {
        unreachable!("timeouts never yield");
    }
}

fn sleep_host(early: Duration) -> (crate::host::Host, Rc<SleepClock>) {
    let clock = Rc::new(SleepClock {
        now: Cell::new(Instant::now()),
        early,
        now_calls: Cell::new(0),
        sleeps_made: Cell::new(0),
        sleeps: RefCell::default(),
    });
    (
        crate::host::Host::new(Rc::new(SleepHost(Rc::clone(&clock)))),
        clock,
    )
}

/// A waker that counts its wakes.
#[derive(Default)]
struct CountingWaker(std::sync::atomic::AtomicUsize);

impl CountingWaker {
    fn wakes(&self) -> usize {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

fn counting_waker() -> (Arc<CountingWaker>, Waker) {
    let wakes = Arc::new(CountingWaker::default());
    (Arc::clone(&wakes), Waker::from(wakes))
}

/// On a host without a wheel, a timeout fires once at its deadline through
/// one host sleep.
#[test]
fn fallback_fires_once_at_the_deadline() {
    let (host, clock) = sleep_host(Duration::ZERO);
    let start = clock.now();
    let (wakes, waker) = counting_waker();
    let mut timeout = Timeout::new(&host);

    timeout.schedule_at(start + ms(10), &waker);
    assert_eq!(clock.sleeps_made.get(), 1);
    assert_eq!(timeout.deadline(), Some(start + ms(10)));
    clock.advance_to(start + ms(10) - Duration::from_nanos(1));
    assert_eq!(wakes.wakes(), 0);
    assert_eq!(timeout.take_expired(), None);

    clock.advance_to(start + ms(10));
    assert_eq!(wakes.wakes(), 1);
    assert_eq!(timeout.take_expired(), Some(Expiry::Elapsed));
    assert_eq!(timeout.take_expired(), None);
    assert_eq!(timeout.deadline(), None);
    assert_eq!(clock.sleeps_made.get(), 1);
}

/// A later deadline is a store on the fallback too: 1000 extends keep the one
/// host sleep, which re-arms for the remainder when it fires early for the
/// extended deadline.
#[test]
fn fallback_makes_one_host_sleep_for_1000_later_extends() {
    let (host, clock) = sleep_host(Duration::ZERO);
    let start = clock.now();
    let (wakes, waker) = counting_waker();
    let mut timeout = Timeout::new(&host);

    timeout.schedule_at(start + ms(10), &waker);
    for extension in 1..=1000 {
        timeout.extend_to(start + ms(10 + extension));
    }
    assert_eq!(clock.sleeps_made.get(), 1);
    assert_eq!(timeout.deadline(), Some(start + ms(1010)));

    clock.advance_to(start + ms(10));
    assert_eq!(wakes.wakes(), 1);
    assert_eq!(timeout.take_expired(), None);
    assert_eq!(
        clock.sleeps_made.get(),
        2,
        "the remainder takes one more sleep"
    );

    clock.advance_to(start + ms(1010));
    assert_eq!(timeout.take_expired(), Some(Expiry::Elapsed));
    assert_eq!(clock.sleeps_made.get(), 2);
}

/// An earlier deadline replaces the fallback's host sleep at once.
#[test]
fn fallback_earlier_deadline_rearms() {
    let (host, clock) = sleep_host(Duration::ZERO);
    let start = clock.now();
    let (wakes, waker) = counting_waker();
    let mut timeout = Timeout::new(&host);

    timeout.schedule_at(start + ms(100), &waker);
    timeout.extend_to(start + ms(50));
    assert_eq!(clock.sleeps_made.get(), 2);
    timeout.schedule_at(start + ms(30), &waker);
    assert_eq!(clock.sleeps_made.get(), 3);

    clock.advance_to(start + ms(30));
    assert_eq!(wakes.wakes(), 1);
    assert_eq!(timeout.take_expired(), Some(Expiry::Elapsed));
}

/// A host sleep that resolves before the deadline (an EventBase timer may
/// fire up to a tick early) is a hint: the fallback re-arms for the
/// remainder and never reports an early expiry.
#[test]
fn fallback_early_host_fire_rearms_the_remainder() {
    let (host, clock) = sleep_host(ms(2));
    let start = clock.now();
    let (wakes, waker) = counting_waker();
    let mut timeout = Timeout::new(&host);

    timeout.schedule_at(start + ms(10), &waker);
    clock.advance_to(start + ms(8));
    assert_eq!(wakes.wakes(), 1, "the host fired early");
    assert_eq!(timeout.take_expired(), None);
    assert_eq!(timeout.deadline(), Some(start + ms(10)));
    assert_eq!(clock.sleeps_made.get(), 2);

    clock.advance_to(start + ms(9));
    assert_eq!(timeout.take_expired(), None);
    clock.advance_to(start + ms(10));
    assert_eq!(timeout.take_expired(), Some(Expiry::Elapsed));
}

/// A cancel is a store on the fallback: the host sleep is kept, a later
/// re-arm reuses it, and its stale resolution reports nothing.
#[test]
fn fallback_cancel_is_a_store_and_a_later_rearm_reuses_the_sleep() {
    let (host, clock) = sleep_host(Duration::ZERO);
    let start = clock.now();
    let (_wakes, waker) = counting_waker();
    let mut timeout = Timeout::new(&host);

    timeout.schedule_at(start + ms(10), &waker);
    timeout.cancel();
    assert_eq!(timeout.deadline(), None);
    timeout.schedule_at(start + ms(20), &waker);
    assert_eq!(clock.sleeps_made.get(), 1);

    clock.advance_to(start + ms(10));
    assert_eq!(timeout.take_expired(), None);
    clock.advance_to(start + ms(20));
    assert_eq!(timeout.take_expired(), Some(Expiry::Elapsed));
    assert_eq!(clock.sleeps_made.get(), 2);
}

/// Activity before the owner observes a fallback fire discards it.
#[test]
fn fallback_stale_fire_is_discarded_by_later_activity() {
    let (host, clock) = sleep_host(Duration::ZERO);
    let start = clock.now();
    let (wakes, waker) = counting_waker();
    let mut timeout = Timeout::new(&host);
    let mut context = Context::from_waker(&waker);

    timeout.schedule_at(start + ms(10), &waker);
    clock.advance_to(start + ms(10));
    assert_eq!(wakes.wakes(), 1);
    timeout.extend_to(start + ms(30));
    assert_eq!(timeout.poll_expired(&mut context), Poll::Pending);
    assert_eq!(timeout.deadline(), Some(start + ms(30)));

    clock.advance_to(start + ms(30));
    assert_eq!(
        timeout.poll_expired(&mut context),
        Poll::Ready(Expiry::Elapsed)
    );
}

/// An owner woken by its fallback's host sleep may extend the deadline and
/// park without polling the timeout. The resolved sleep must not absorb the
/// new deadline: the owner is woken at it.
#[test]
fn fallback_extend_after_an_unobserved_resolution_wakes_at_the_new_deadline() {
    let (host, clock) = sleep_host(Duration::ZERO);
    let start = clock.now();
    let (wakes, waker) = counting_waker();
    let mut timeout = Timeout::new(&host);

    timeout.schedule_at(start + ms(10), &waker);
    clock.advance_to(start + ms(10));
    assert_eq!(wakes.wakes(), 1, "the host sleep resolved");
    timeout.extend_to(start + ms(30));
    clock.advance_to(start + ms(29));
    assert_eq!(wakes.wakes(), 1, "never early");
    clock.advance_to(start + ms(30));

    assert_eq!(wakes.wakes(), 2, "the extended deadline wakes the owner");
    assert_eq!(timeout.take_expired(), Some(Expiry::Elapsed));
}

/// As above, for a reschedule with the same waker.
#[test]
fn fallback_reschedule_after_an_unobserved_resolution_wakes_at_the_new_deadline() {
    let (host, clock) = sleep_host(Duration::ZERO);
    let start = clock.now();
    let (wakes, waker) = counting_waker();
    let mut timeout = Timeout::new(&host);

    timeout.schedule_at(start + ms(10), &waker);
    clock.advance_to(start + ms(10));
    assert_eq!(wakes.wakes(), 1, "the host sleep resolved");
    timeout.schedule_at(start + ms(30), &waker);
    clock.advance_to(start + ms(29));
    assert_eq!(wakes.wakes(), 1, "never early");
    clock.advance_to(start + ms(30));

    assert_eq!(wakes.wakes(), 2, "the new deadline wakes the owner");
    assert_eq!(timeout.take_expired(), Some(Expiry::Elapsed));
}

/// `poll_expired` on the fallback registers the latest waker with the host
/// sleep.
#[test]
fn fallback_poll_expired_wakes_the_latest_waker() {
    let (host, clock) = sleep_host(Duration::ZERO);
    let start = clock.now();
    let (first_wakes, first) = counting_waker();
    let (second_wakes, second) = counting_waker();
    let mut timeout = Timeout::new(&host);

    timeout.schedule_at(start + ms(5), &first);
    assert_eq!(
        timeout.poll_expired(&mut Context::from_waker(&second)),
        Poll::Pending
    );
    clock.advance_to(start + ms(5));

    assert_eq!((first_wakes.wakes(), second_wakes.wakes()), (0, 1));
    assert_eq!(
        timeout.poll_expired(&mut Context::from_waker(&second)),
        Poll::Ready(Expiry::Elapsed)
    );
}

/// A deadline already past on the fallback completes at once and wakes the
/// owner, as the wheel's wakeup would fire at once.
#[test]
fn fallback_past_deadline_wakes_at_once() {
    let (host, clock) = sleep_host(Duration::ZERO);
    let start = clock.now();
    let (wakes, waker) = counting_waker();
    let mut timeout = Timeout::new(&host);

    clock.advance_to(start + ms(5));
    timeout.schedule_at(start + ms(1), &waker);

    assert_eq!(wakes.wakes(), 1);
    assert_eq!(timeout.take_expired(), Some(Expiry::Elapsed));
}

/// The zero-config guard: a timeout that is never scheduled takes no slot,
/// makes no host sleep, reads no clock and allocates nothing, on either
/// backend.
#[test]
fn an_unscheduled_timeout_takes_nothing() {
    let (host, clock) = sleep_host(Duration::ZERO);
    let harness = Harness::new(ms(1));
    let waker = futures::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    let start = clock.now();
    let mut exercise = |timeout: &mut Timeout| {
        timeout.extend_to(start + ms(1));
        timeout.cancel();
        assert_eq!(timeout.deadline(), None);
        assert_eq!(timeout.take_expired(), None);
        assert_eq!(timeout.poll_expired(&mut context), Poll::Pending);
    };
    let mut live_slots = None;

    let allocations = count_allocations(|| {
        let mut on_host = Timeout::new(&host);
        let mut on_wheel = Timeout::with_wheel(Rc::clone(&harness.wheel));
        exercise(&mut on_host);
        exercise(&mut on_wheel);
        live_slots = Some(harness.wheel.stats().live_slots);
    });

    if let Some(allocations) = allocations {
        assert_eq!(allocations, 0);
    }
    assert_eq!(live_slots, Some(0), "an unscheduled timeout holds no slot");
    assert_eq!(clock.now_calls.get(), 0);
    assert_eq!(clock.sleeps_made.get(), 0);
    assert_eq!(harness.wheel.stats().live_slots, 0);
    assert!(harness.wakeup.arms.borrow().is_empty());
}

/// `WheelStats` counts `schedule_at` calls and fired slots: a replaced
/// schedule counts as a schedule, an extend does not, and a cancelled or
/// stopped slot does not fire.
#[test]
fn stats_count_schedules_and_fires() {
    let harness = Harness::new(ms(1));
    let start = harness.now();
    let t1 = harness.timeout();
    let t2 = harness.timeout();
    let t3 = harness.timeout();

    harness.schedule(&t1, ms(5));
    harness.schedule(&t2, ms(5));
    harness.schedule(&t2, ms(6));
    harness.schedule(&t3, ms(7));
    t1.timeout.borrow_mut().extend_to(start + ms(8));
    t3.cancel();
    harness.run();

    let stats = harness.wheel.stats();
    assert_eq!(stats.scheduled, 4);
    assert_eq!(stats.fired, 2);
    harness.schedule(&t3, ms(1));
    harness.wheel.stop();
    assert_eq!(harness.wheel.stats().fired, 2);
}

/// A sleep kept across a cancel can resolve while the timeout is idle, and
/// its owner need not look. Re-arming later than that sleep must not store
/// onto the resolved sleep, which would never wake again.
#[test]
fn fallback_rearm_after_an_unobserved_stale_resolution_fires() {
    let (host, clock) = sleep_host(Duration::ZERO);
    let start = clock.now();
    let (wakes, waker) = counting_waker();
    let mut timeout = Timeout::new(&host);

    timeout.schedule_at(start + ms(10), &waker);
    timeout.cancel();
    clock.advance_to(start + ms(10));
    assert_eq!(wakes.wakes(), 1, "the stale sleep resolved");
    timeout.schedule_at(start + ms(20), &waker);
    clock.advance_to(start + ms(20));

    assert_eq!(wakes.wakes(), 2);
    assert_eq!(timeout.take_expired(), Some(Expiry::Elapsed));
}
