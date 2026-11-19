/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! An owner-local hashed hierarchical wheel timer.
//!
//! [`WheelTimer`] ports folly's `HHWheelTimer`. A host drives it through one
//! wakeup ([`WheelWakeup`]) and calls [`WheelTimer::expire`] when that wakeup
//! fires. A [`Timeout`] is one reusable slot per owner object. On a host
//! without a wheel, a `Timeout` falls back to at most one host sleep at a
//! time.
//!
//! The wheel follows these rules:
//! - Never early. A slot fires only when its exact deadline is at or before
//!   the `now` passed to `expire`. The wakeup is armed at the earliest exact
//!   deadline, not at a tick boundary.
//! - Expiry marks and wakes. Due slots are marked fired, their wakers are
//!   cloned and woken after the wheel's state is released, and the owner
//!   observes the expiry in its own poll. Owner code, including a waker's
//!   clone, never runs inside the wheel.
//! - Lazy later, eager earlier. A later deadline and a cancel only update the
//!   slot. The wheel relinks or unlinks the slot when it processes its bucket.
//!   An earlier deadline, or arming an unlinked slot, links immediately.
//! - [`WheelTimer::stop`] completes armed timeouts as [`Expiry::Stopped`], and
//!   later schedules complete the same way.

use std::cell::RefCell;
use std::mem;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::rc::Rc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use crate::host::Host;
use crate::host::LocalFuture;
use crate::worker::retain_first_panic;

const WHEEL_LEVELS: usize = 4;
const WHEEL_BITS: u32 = 8;
const WHEEL_SIZE: usize = 1 << WHEEL_BITS;
const WHEEL_MASK: u64 = WHEEL_SIZE as u64 - 1;
const BITMAP_WORDS: usize = WHEEL_SIZE / 64;
/// The largest tick distance a slot is linked at, the same cap folly's wheel
/// uses. Farther deadlines wait in the last level-3 bucket and cascade again
/// from their exact deadline.
const LARGEST_SLOT: u64 = 0xffff_ffff;
const NIL: u32 = u32::MAX;
const UNLINKED: u16 = u16::MAX;

/// The host's single wakeup for a wheel.
pub trait WheelWakeup: 'static {
    /// Requests one call of [`WheelTimer::expire`] on the owner thread,
    /// outside any task poll, at or after `deadline`. Replaces any earlier
    /// request.
    fn arm(&self, deadline: Instant);

    /// Withdraws the request. A wakeup that cannot be withdrawn may still
    /// fire, and `expire` then fires no slots.
    fn disarm(&self);
}

/// How an armed timeout completed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Expiry {
    /// The deadline passed.
    Elapsed,
    /// The wheel stopped first.
    Stopped,
}

/// Owner-local counters of one wheel.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WheelStats {
    /// Armed timeouts.
    pub armed: usize,
    /// `schedule_at` calls.
    pub scheduled: u64,
    /// Slots that fired with [`Expiry::Elapsed`].
    pub fired: u64,
    /// `expire` calls that fired no slots.
    pub empty_expiries: u64,
    /// Links and relinks made immediately by `schedule_at` and `extend_to`.
    pub relinked: u64,
    /// Slots the wheel moved to another bucket because they were not yet due
    /// when their bucket was processed.
    pub reslotted: u64,
    /// Requests made to the host wakeup.
    pub host_arms: u64,
    /// Slots held by timeouts.
    pub live_slots: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SlotState {
    Idle,
    Armed,
    Fired(Expiry),
}

#[derive(Clone, Copy)]
struct Bucket {
    head: u32,
    tail: u32,
}

const EMPTY_BUCKET: Bucket = Bucket {
    head: NIL,
    tail: NIL,
};

struct Slot {
    prev: u32,
    next: u32,
    /// The bucket the slot is linked in, `level << WHEEL_BITS | index`, or
    /// `UNLINKED`.
    bucket: u16,
    state: SlotState,
    deadline: Instant,
    /// The deadline the slot was linked with. A later deadline only updates
    /// `deadline`, and processing the bucket relinks the slot.
    linked_deadline: Instant,
    waker: Option<Waker>,
    /// Set while the waker is out of the slot to be cloned with the state
    /// released. Storing another waker or freeing the slot clears this, so the
    /// taken waker is dropped instead of put back.
    lent: bool,
}

struct WheelState {
    start: Instant,
    tick_nanos: u64,
    /// The tick being processed. Earlier ticks are processed; slots of this
    /// tick whose deadline is after the last `expire` are still linked.
    current: u64,
    buckets: Box<[[Bucket; WHEEL_SIZE]; WHEEL_LEVELS]>,
    /// Per level-0 bucket, a lower bound of the deadlines linked there.
    /// Unlinking leaves it stale and early, which costs at most a spurious
    /// wakeup.
    earliest: Box<[Option<Instant>; WHEEL_SIZE]>,
    bitmaps: [[u64; BITMAP_WORDS]; WHEEL_LEVELS],
    slots: Vec<Slot>,
    free: u32,
    armed_wakeup: Option<Instant>,
    stopped: bool,
    /// Slots to wake once the state is released.
    wakes: Vec<u32>,
    stats: WheelStats,
}

/// What arming a slot requires of the host.
enum Armed {
    /// The slot is linked, and the wakeup already covers it.
    Covered,
    /// The wakeup must move earlier, to this instant.
    Wakeup(Instant),
    /// The wheel is stopped: the slot completed as stopped, and its owner
    /// must be woken.
    Stopped,
}

/// A hashed hierarchical wheel timer. It is owner-local (`!Send`) and shared
/// through `Rc`.
pub struct WheelTimer {
    state: RefCell<WheelState>,
    wakeup: Box<dyn WheelWakeup>,
}

impl WheelTimer {
    /// A wheel with `tick` granularity whose tick 0 starts at `now`.
    pub fn new(tick: Duration, now: Instant, wakeup: Box<dyn WheelWakeup>) -> Rc<Self> {
        let tick_nanos = u64::try_from(tick.as_nanos()).unwrap_or(u64::MAX);
        assert!(tick_nanos != 0, "a wheel tick must be positive");
        Rc::new(Self {
            state: RefCell::new(WheelState {
                start: now,
                tick_nanos,
                current: 0,
                buckets: Box::new([[EMPTY_BUCKET; WHEEL_SIZE]; WHEEL_LEVELS]),
                earliest: Box::new([None; WHEEL_SIZE]),
                bitmaps: [[0; BITMAP_WORDS]; WHEEL_LEVELS],
                slots: Vec::new(),
                free: NIL,
                armed_wakeup: None,
                stopped: false,
                wakes: Vec::new(),
                stats: WheelStats::default(),
            }),
            wakeup,
        })
    }

    /// Marks every armed slot whose deadline is at or before `now` as fired,
    /// in tick order and then FIFO, re-arms the wakeup, and then wakes the
    /// fired slots' owners. If a waker's clone or wake panics, the others are
    /// still woken and the first panic is re-raised.
    pub fn expire(&self, now: Instant) {
        let (rearm, wakes) = {
            let mut state = self.state.borrow_mut();
            if state.stopped {
                return;
            }
            state.armed_wakeup = None;
            if state.advance(now) == 0 {
                state.stats.empty_expiries += 1;
            }
            let rearm = state.next_wakeup();
            state.armed_wakeup = rearm;
            if rearm.is_some() {
                state.stats.host_arms += 1;
            }
            (rearm, mem::take(&mut state.wakes))
        };
        if let Some(deadline) = rearm {
            self.wakeup.arm(deadline);
        }
        self.wake_slots(wakes);
    }

    /// Stops the wheel and returns how many timeouts completed as stopped.
    ///
    /// Every armed timeout, and every fired one that its owner has not yet
    /// observed, completes as [`Expiry::Stopped`]. The armed ones' owners are
    /// woken and the wakeup is withdrawn. Later schedules complete as stopped
    /// immediately, and later `expire` calls have no effect.
    pub fn stop(&self) -> usize {
        let (stopped, wakes) = {
            let mut state = self.state.borrow_mut();
            if state.stopped {
                return 0;
            }
            state.stopped = true;
            state.armed_wakeup = None;
            let stopped = state.stop_slots();
            (stopped, mem::take(&mut state.wakes))
        };
        self.wakeup.disarm();
        self.wake_slots(wakes);
        stopped
    }

    /// Wakes the owners of the slots in `wakes` after the state is released.
    /// If a waker's clone or wake panics, the others are still woken and the
    /// first panic is re-raised.
    fn wake_slots(&self, mut wakes: Vec<u32>) {
        let mut first_panic = None;
        for index in wakes.drain(..) {
            retain_first_panic(
                &mut first_panic,
                catch_unwind(AssertUnwindSafe(|| self.wake_slot(index))).err(),
            );
        }
        {
            let mut state = self.state.borrow_mut();
            if state.wakes.capacity() < wakes.capacity() {
                state.wakes = wakes;
            }
        }
        if let Some(payload) = first_panic {
            resume_unwind(payload);
        }
    }

    /// Wakes a slot's owner through a clone of its waker. A waker's clone is
    /// arbitrary code, so the waker is taken out of the slot and cloned with
    /// the state released.
    fn wake_slot(&self, index: u32) {
        let lent = self.state.borrow_mut().lend_waker(index);
        let Some(waker) = lent else {
            return;
        };
        let cloned = catch_unwind(AssertUnwindSafe(|| waker.clone()));
        let unused = self.state.borrow_mut().return_waker(index, waker);
        drop(unused);
        match cloned {
            Ok(cloned) => cloned.wake(),
            Err(payload) => resume_unwind(payload),
        }
    }

    /// The wheel's counters.
    pub fn stats(&self) -> WheelStats {
        self.state.borrow().stats
    }

    fn allocate(&self) -> u32 {
        self.state.borrow_mut().allocate()
    }

    /// Arms a slot. A new waker is cloned with the state released, as a
    /// waker's clone is arbitrary code.
    fn schedule(&self, index: u32, deadline: Instant, waker: &Waker) {
        let needs_waker = self.state.borrow().needs_waker(index, waker);
        let stored = needs_waker.then(|| waker.clone());
        let (rearm, _replaced) = {
            let mut state = self.state.borrow_mut();
            state.stats.scheduled += 1;
            let replaced = stored.and_then(|waker| state.store_waker(index, waker));
            (state.arm(index, deadline), replaced)
        };
        match rearm {
            Armed::Covered => {}
            Armed::Wakeup(deadline) => self.wakeup.arm(deadline),
            Armed::Stopped => waker.wake_by_ref(),
        }
    }

    fn extend(&self, index: u32, deadline: Instant) {
        let rearm = {
            let mut state = self.state.borrow_mut();
            match state.slots[index as usize].state {
                SlotState::Armed | SlotState::Fired(_) => state.arm(index, deadline),
                SlotState::Idle => Armed::Covered,
            }
        };
        match rearm {
            Armed::Covered => {}
            Armed::Wakeup(deadline) => self.wakeup.arm(deadline),
            Armed::Stopped => self.wake_slot(index),
        }
    }

    fn cancel(&self, index: u32) {
        let mut state = self.state.borrow_mut();
        let state = &mut *state;
        let slot = &mut state.slots[index as usize];
        if slot.state == SlotState::Armed {
            state.stats.armed -= 1;
        }
        slot.state = SlotState::Idle;
    }

    fn deadline(&self, index: u32) -> Option<Instant> {
        let state = self.state.borrow();
        let slot = &state.slots[index as usize];
        (slot.state == SlotState::Armed).then_some(slot.deadline)
    }

    /// Takes a fired slot's expiry, or keeps `waker` for an armed one. A new
    /// waker is cloned with the state released.
    fn poll_expired(&self, index: u32, waker: &Waker) -> Poll<Expiry> {
        let needs_waker = {
            let mut state = self.state.borrow_mut();
            match state.slots[index as usize].state {
                SlotState::Fired(expiry) => {
                    state.slots[index as usize].state = SlotState::Idle;
                    return Poll::Ready(expiry);
                }
                SlotState::Armed => state.needs_waker(index, waker),
                SlotState::Idle => false,
            }
        };
        if needs_waker {
            let stored = waker.clone();
            let _replaced = self.state.borrow_mut().store_waker(index, stored);
        }
        Poll::Pending
    }

    fn take_expired(&self, index: u32) -> Option<Expiry> {
        let mut state = self.state.borrow_mut();
        let slot = &mut state.slots[index as usize];
        match slot.state {
            SlotState::Fired(expiry) => {
                slot.state = SlotState::Idle;
                Some(expiry)
            }
            SlotState::Idle | SlotState::Armed => None,
        }
    }

    /// Unlinks and frees a slot. Freeing the wheel's last slot withdraws the
    /// wakeup. Cancelling does not, because a cancelled slot stays linked and a
    /// later arm of it relies on the wakeup. The waker is dropped, and the
    /// wakeup withdrawn, after the state is released.
    fn release(&self, index: u32) {
        let (_waker, withdraw) = {
            let mut state = self.state.borrow_mut();
            let state = &mut *state;
            if state.slots[index as usize].bucket != UNLINKED {
                state.unlink(index);
            }
            let free = state.free;
            let slot = &mut state.slots[index as usize];
            if slot.state == SlotState::Armed {
                state.stats.armed -= 1;
            }
            slot.state = SlotState::Idle;
            slot.next = free;
            slot.lent = false;
            let waker = slot.waker.take();
            state.free = index;
            state.stats.live_slots -= 1;
            let withdraw = state.stats.live_slots == 0 && state.armed_wakeup.take().is_some();
            (waker, withdraw)
        };
        if withdraw {
            self.wakeup.disarm();
        }
    }
}

impl WheelState {
    fn tick_of(&self, instant: Instant) -> u64 {
        let nanos = instant.saturating_duration_since(self.start).as_nanos();
        u64::try_from(nanos).unwrap_or(u64::MAX) / self.tick_nanos
    }

    fn allocate(&mut self) -> u32 {
        self.stats.live_slots += 1;
        if self.free != NIL {
            let index = self.free;
            self.free = mem::replace(&mut self.slots[index as usize].next, NIL);
            return index;
        }
        let index = u32::try_from(self.slots.len())
            .ok()
            .filter(|index| *index != NIL)
            .expect("a wheel holds fewer than u32::MAX timeouts");
        self.slots.push(Slot {
            prev: NIL,
            next: NIL,
            bucket: UNLINKED,
            state: SlotState::Idle,
            deadline: self.start,
            linked_deadline: self.start,
            waker: None,
            lent: false,
        });
        index
    }

    /// Whether a slot must store `waker`: its stored waker does not wake the
    /// same task.
    fn needs_waker(&self, index: u32, waker: &Waker) -> bool {
        !self.slots[index as usize]
            .waker
            .as_ref()
            .is_some_and(|stored| stored.will_wake(waker))
    }

    /// Stores a slot's waker. Returns the replaced one, to drop with the
    /// state released.
    fn store_waker(&mut self, index: u32, waker: Waker) -> Option<Waker> {
        let slot = &mut self.slots[index as usize];
        slot.lent = false;
        slot.waker.replace(waker)
    }

    /// Takes a slot's waker out, to clone with the state released.
    fn lend_waker(&mut self, index: u32) -> Option<Waker> {
        let slot = &mut self.slots[index as usize];
        let waker = slot.waker.take()?;
        slot.lent = true;
        Some(waker)
    }

    /// Puts back a waker that `lend_waker` took out. Returns it instead, to
    /// drop with the state released, if the slot stored another waker or was
    /// freed meanwhile.
    fn return_waker(&mut self, index: u32, waker: Waker) -> Option<Waker> {
        let slot = &mut self.slots[index as usize];
        if !mem::take(&mut slot.lent) {
            return Some(waker);
        }
        slot.waker = Some(waker);
        None
    }

    /// Arms a slot for `deadline`. A slot linked at or before `deadline`
    /// stays where it is. Otherwise it is linked immediately and the wakeup
    /// moved earlier if needed. Returns the wakeup request to make.
    fn arm(&mut self, index: u32, deadline: Instant) -> Armed {
        let slot = &mut self.slots[index as usize];
        if self.stopped {
            slot.state = SlotState::Fired(Expiry::Stopped);
            return Armed::Stopped;
        }
        if slot.state != SlotState::Armed {
            self.stats.armed += 1;
        }
        slot.state = SlotState::Armed;
        slot.deadline = deadline;
        if slot.bucket != UNLINKED {
            if deadline >= slot.linked_deadline {
                return Armed::Covered;
            }
            self.unlink(index);
        }
        let wakeup = self.link(index);
        self.stats.relinked += 1;
        match self.request_wakeup(wakeup) {
            Some(wakeup) => Armed::Wakeup(wakeup),
            None => Armed::Covered,
        }
    }

    /// Unlinks every slot and completes armed and unobserved fired slots as
    /// stopped, collecting the armed ones to wake. Returns how many
    /// completed.
    fn stop_slots(&mut self) -> usize {
        for level in 0..WHEEL_LEVELS {
            for bucket in 0..WHEEL_SIZE {
                let mut cursor = self.clear_bucket(level, bucket);
                while cursor != NIL {
                    let slot = &mut self.slots[cursor as usize];
                    cursor = mem::replace(&mut slot.next, NIL);
                    slot.prev = NIL;
                    slot.bucket = UNLINKED;
                }
            }
        }
        let mut stopped = 0;
        for (index, slot) in (0..).zip(&mut self.slots) {
            match slot.state {
                SlotState::Armed => {
                    if slot.waker.is_some() {
                        self.wakes.push(index);
                    }
                    stopped += 1;
                }
                SlotState::Fired(Expiry::Elapsed) => stopped += 1,
                SlotState::Idle | SlotState::Fired(Expiry::Stopped) => continue,
            }
            slot.state = SlotState::Fired(Expiry::Stopped);
        }
        self.stats.armed = 0;
        stopped
    }

    fn tick_start(&self, tick: u64) -> Instant {
        self.start + Duration::from_nanos(tick.saturating_mul(self.tick_nanos))
    }

    /// Links a slot at the tail of the bucket of its deadline's tick, or of
    /// the current tick if that tick is already processed. The slot goes on
    /// level 0 within `WHEEL_SIZE` ticks of the current tick, and otherwise on
    /// the level of that distance. Returns when the wakeup must fire for it:
    /// its deadline on level 0, or its bucket's cascade boundary above.
    fn link(&mut self, index: u32) -> Instant {
        let deadline = self.slots[index as usize].deadline;
        let mut due = self.tick_of(deadline).max(self.current);
        let distance = due - self.current;
        let level = match distance {
            distance if distance < 1 << WHEEL_BITS => 0,
            distance if distance < 1 << (2 * WHEEL_BITS) => 1,
            distance if distance < 1 << (3 * WHEEL_BITS) => 2,
            distance => {
                if distance > LARGEST_SLOT {
                    due = self.current + LARGEST_SLOT;
                }
                3
            }
        };
        let shift = level as u32 * WHEEL_BITS;
        let bucket = ((due >> shift) & WHEEL_MASK) as usize;
        let tail = self.buckets[level][bucket].tail;
        let slot = &mut self.slots[index as usize];
        slot.prev = tail;
        slot.next = NIL;
        slot.bucket = (level << WHEEL_BITS | bucket) as u16;
        slot.linked_deadline = deadline;
        match tail {
            NIL => self.buckets[level][bucket].head = index,
            tail => self.slots[tail as usize].next = index,
        }
        self.buckets[level][bucket].tail = index;
        self.bitmaps[level][bucket / 64] |= 1 << (bucket % 64);
        if level != 0 {
            return self.tick_start(due >> shift << shift);
        }
        let earliest = &mut self.earliest[bucket];
        *earliest = Some(earliest.map_or(deadline, |earliest| earliest.min(deadline)));
        deadline
    }

    fn unlink(&mut self, index: u32) {
        let slot = &mut self.slots[index as usize];
        let location = mem::replace(&mut slot.bucket, UNLINKED) as usize;
        let (level, bucket) = (location >> WHEEL_BITS, location & WHEEL_MASK as usize);
        let prev = mem::replace(&mut slot.prev, NIL);
        let next = mem::replace(&mut slot.next, NIL);
        match prev {
            NIL => self.buckets[level][bucket].head = next,
            prev => self.slots[prev as usize].next = next,
        }
        match next {
            NIL => self.buckets[level][bucket].tail = prev,
            next => self.slots[next as usize].prev = prev,
        }
        if self.buckets[level][bucket].head == NIL {
            self.clear_bucket(level, bucket);
        }
    }

    fn clear_bucket(&mut self, level: usize, bucket: usize) -> u32 {
        let head = mem::replace(&mut self.buckets[level][bucket], EMPTY_BUCKET).head;
        self.bitmaps[level][bucket / 64] &= !(1 << (bucket % 64));
        if level == 0 {
            self.earliest[bucket] = None;
        }
        head
    }

    /// Processes every tick up to `now`'s, firing due slots. Returns how many
    /// fired. Ticks with no work are skipped: the next one is the next
    /// non-empty level-0 bucket or the next boundary that cascades a
    /// non-empty bucket.
    fn advance(&mut self, now: Instant) -> usize {
        let now_tick = self.tick_of(now);
        let mut fired = 0;
        loop {
            fired += self.drain_current(now);
            if self.current >= now_tick {
                return fired;
            }
            let next = match (self.next_bucket_tick(), self.next_cascade_tick()) {
                (Some(bucket), Some(cascade)) => Some(bucket.min(cascade)),
                (bucket, cascade) => bucket.or(cascade),
            };
            match next {
                Some(tick) if tick <= now_tick => {
                    self.current = tick;
                    if tick & WHEEL_MASK == 0 {
                        self.cascade_boundary();
                    }
                }
                _ => {
                    self.current = now_tick;
                    return fired;
                }
            }
        }
    }

    /// Cascades at an epoch boundary: level 1, then level 2 when level 1
    /// wraps, then level 3 when level 2 wraps.
    fn cascade_boundary(&mut self) {
        for level in 1..WHEEL_LEVELS {
            let bucket = ((self.current >> (level as u32 * WHEEL_BITS)) & WHEEL_MASK) as usize;
            self.cascade(level, bucket);
            if bucket != 0 {
                return;
            }
        }
    }

    /// Relinks a bucket's armed slots from their deadlines, relative to the
    /// current tick.
    fn cascade(&mut self, level: usize, bucket: usize) {
        let mut cursor = self.clear_bucket(level, bucket);
        while cursor != NIL {
            let index = cursor;
            let slot = &mut self.slots[index as usize];
            cursor = mem::replace(&mut slot.next, NIL);
            slot.prev = NIL;
            slot.bucket = UNLINKED;
            if slot.state == SlotState::Armed {
                self.link(index);
                self.stats.reslotted += 1;
            }
        }
    }

    /// Fires the current bucket's due slots, FIFO, and relinks the rest: a
    /// slot extended past this tick moves to its deadline's bucket even if
    /// that deadline has passed, so that one expiry fires in tick order.
    fn drain_current(&mut self, now: Instant) -> usize {
        let mut cursor = self.clear_bucket(0, (self.current & WHEEL_MASK) as usize);
        if cursor == NIL {
            return 0;
        }
        let next_tick = self.tick_start(self.current + 1);
        let mut fired = 0;
        while cursor != NIL {
            let index = cursor;
            let slot = &mut self.slots[index as usize];
            let due = slot.deadline <= now && slot.deadline < next_tick;
            cursor = mem::replace(&mut slot.next, NIL);
            slot.prev = NIL;
            slot.bucket = UNLINKED;
            match slot.state {
                SlotState::Armed if due => {
                    slot.state = SlotState::Fired(Expiry::Elapsed);
                    if slot.waker.is_some() {
                        self.wakes.push(index);
                    }
                    self.stats.armed -= 1;
                    self.stats.fired += 1;
                    fired += 1;
                }
                SlotState::Armed => {
                    self.link(index);
                    self.stats.reslotted += 1;
                }
                SlotState::Idle | SlotState::Fired(_) => {}
            }
        }
        fired
    }

    /// The tick of the next non-empty level-0 bucket after the current one.
    fn next_bucket_tick(&self) -> Option<u64> {
        let from = ((self.current + 1) & WHEEL_MASK) as usize;
        next_set(&self.bitmaps[0], from)
            .filter(|distance| *distance != WHEEL_SIZE - 1)
            .map(|distance| self.current + 1 + distance as u64)
    }

    /// The next boundary after the current tick whose cascade moves a
    /// non-empty bucket.
    fn next_cascade_tick(&self) -> Option<u64> {
        (1..WHEEL_LEVELS)
            .filter_map(|level| {
                let shift = level as u32 * WHEEL_BITS;
                let epoch = (self.current >> shift) + 1;
                next_set(&self.bitmaps[level], (epoch & WHEEL_MASK) as usize)
                    .map(|distance| (epoch + distance as u64) << shift)
            })
            .min()
    }

    /// The earliest instant the wakeup must cover after an expiry.
    fn next_wakeup(&self) -> Option<Instant> {
        let current = self.earliest[(self.current & WHEEL_MASK) as usize];
        let bucket = self
            .next_bucket_tick()
            .and_then(|tick| self.earliest[(tick & WHEEL_MASK) as usize]);
        let cascade = self.next_cascade_tick().map(|tick| self.tick_start(tick));
        [current, bucket, cascade].into_iter().flatten().min()
    }

    /// Moves the wakeup earlier, to `at`, if it does not already cover it.
    fn request_wakeup(&mut self, at: Instant) -> Option<Instant> {
        if self.armed_wakeup.is_some_and(|armed| armed <= at) {
            return None;
        }
        self.armed_wakeup = Some(at);
        self.stats.host_arms += 1;
        Some(at)
    }
}

/// The distance from `from` to the first set bit at or after it, wrapping.
fn next_set(bitmap: &[u64; BITMAP_WORDS], from: usize) -> Option<usize> {
    let first_word = from / 64;
    let low_bits = (1u64 << (from % 64)) - 1;
    (0..=BITMAP_WORDS).find_map(|step| {
        let word = (first_word + step) % BITMAP_WORDS;
        let bits = match step {
            0 => bitmap[word] & !low_bits,
            BITMAP_WORDS => bitmap[word] & low_bits,
            _ => bitmap[word],
        };
        (bits != 0).then(|| {
            let position = word * 64 + bits.trailing_zeros() as usize;
            (position + WHEEL_SIZE - from) % WHEEL_SIZE
        })
    })
}

/// A reusable timeout slot owned by one object. It is `!Send` and not
/// `Clone`.
///
/// An expiry is only marked; the owner observes it with
/// [`poll_expired`](Self::poll_expired) or [`take_expired`](Self::take_expired)
/// in its own poll. `schedule_at`, `extend_to` and `cancel` discard an expiry
/// the owner has not observed, so activity seen earlier in the same poll
/// beats the timeout.
pub struct Timeout {
    backend: Backend,
}

enum Backend {
    /// Not scheduled yet: no slot and no sleep.
    Unbound(Host),
    /// A slot on a wheel, taken at the first schedule.
    Wheel { wheel: Rc<WheelTimer>, slot: u32 },
    /// At most one host sleep, for a host without a wheel.
    Fallback(LazySleep),
}

impl Timeout {
    /// A timeout for `host`. It holds no slot or sleep until its first
    /// schedule, which takes a slot on the host's wheel timer
    /// ([`Host::wheel_timer`](crate::host::Host::wheel_timer)) or, on a host
    /// without one, binds it to at most one host sleep at a time.
    pub fn new(host: &Host) -> Self {
        Self {
            backend: Backend::Unbound(host.clone()),
        }
    }

    /// A timeout on `wheel`. It takes a slot at its first schedule.
    pub fn with_wheel(wheel: Rc<WheelTimer>) -> Self {
        Self {
            backend: Backend::Wheel { wheel, slot: NIL },
        }
    }

    /// Arms the timeout for `deadline`, replacing any earlier schedule, and
    /// discards an expiry not yet observed. An expiry wakes `waker`.
    pub fn schedule_at(&mut self, deadline: Instant, waker: &Waker) {
        if let Backend::Unbound(host) = &self.backend
            && let Some(wheel) = host.wheel_timer()
        {
            self.backend = Backend::Wheel { wheel, slot: NIL };
        }
        match &mut self.backend {
            Backend::Unbound(host) => {
                let mut sleep = LazySleep::new(host.clone());
                sleep.schedule_at(deadline, waker);
                self.backend = Backend::Fallback(sleep);
            }
            Backend::Wheel { wheel, slot } => {
                if *slot == NIL {
                    *slot = wheel.allocate();
                }
                wheel.schedule(*slot, deadline, waker);
            }
            Backend::Fallback(sleep) => sleep.schedule_at(deadline, waker),
        }
    }

    /// Moves an armed timeout's deadline, keeping its waker. On a fired
    /// timeout whose expiry is not yet observed, discards the expiry and
    /// re-arms. An idle timeout stays idle.
    pub fn extend_to(&mut self, deadline: Instant) {
        match &mut self.backend {
            Backend::Wheel { wheel, slot } if *slot != NIL => wheel.extend(*slot, deadline),
            Backend::Fallback(sleep) => sleep.extend_to(deadline),
            Backend::Unbound(_) | Backend::Wheel { .. } => {}
        }
    }

    /// Disarms the timeout and discards an expiry not yet observed.
    pub fn cancel(&mut self) {
        match &mut self.backend {
            Backend::Wheel { wheel, slot } if *slot != NIL => wheel.cancel(*slot),
            Backend::Fallback(sleep) => sleep.cancel(),
            Backend::Unbound(_) | Backend::Wheel { .. } => {}
        }
    }

    /// The armed deadline, if the timeout is armed.
    pub fn deadline(&self) -> Option<Instant> {
        match &self.backend {
            Backend::Wheel { wheel, slot } if *slot != NIL => wheel.deadline(*slot),
            Backend::Fallback(sleep) => sleep.deadline,
            Backend::Unbound(_) | Backend::Wheel { .. } => None,
        }
    }

    /// Takes an expiry not yet observed. While armed, keeps `cx`'s waker for
    /// the expiry.
    pub fn poll_expired(&mut self, cx: &mut Context<'_>) -> Poll<Expiry> {
        match &mut self.backend {
            Backend::Wheel { wheel, slot } if *slot != NIL => wheel.poll_expired(*slot, cx.waker()),
            Backend::Fallback(sleep) => sleep.poll_expired(cx.waker()),
            Backend::Unbound(_) | Backend::Wheel { .. } => Poll::Pending,
        }
    }

    /// Takes an expiry not yet observed, for owners that keep a stable waker
    /// and call this after that waker fired. On the fallback it polls the
    /// host sleep.
    pub fn take_expired(&mut self) -> Option<Expiry> {
        match &mut self.backend {
            Backend::Wheel { wheel, slot } if *slot != NIL => wheel.take_expired(*slot),
            Backend::Fallback(sleep) => sleep.take_expired(),
            Backend::Unbound(_) | Backend::Wheel { .. } => None,
        }
    }
}

impl Drop for Timeout {
    fn drop(&mut self) {
        if let Backend::Wheel { wheel, slot } = &self.backend
            && *slot != NIL
        {
            wheel.release(*slot);
        }
    }
}

/// A host sleep and the deadline it was made for.
struct HostSleep {
    future: LocalFuture,
    deadline: Instant,
}

/// The fallback backend: at most one host sleep at a time.
///
/// A later deadline, or a cancel, only updates the stored deadline. The sleep
/// is replaced when the deadline moves earlier. When it resolves before the
/// deadline (an extended deadline, or a host that fires early), it is re-armed
/// for the remainder. Every arm polls the sleep, since it may have resolved and
/// woken the owner unobserved. The clock is read only when the sleep resolves.
struct LazySleep {
    host: Host,
    sleep: Option<HostSleep>,
    deadline: Option<Instant>,
    fired: bool,
    waker: Option<Waker>,
}

impl LazySleep {
    fn new(host: Host) -> Self {
        Self {
            host,
            sleep: None,
            deadline: None,
            fired: false,
            waker: None,
        }
    }

    fn schedule_at(&mut self, deadline: Instant, waker: &Waker) {
        self.refresh_waker(waker);
        self.fired = false;
        self.arm(deadline);
    }

    fn extend_to(&mut self, deadline: Instant) {
        if self.fired || self.deadline.is_some() {
            self.fired = false;
            self.arm(deadline);
        }
    }

    fn cancel(&mut self) {
        self.deadline = None;
        self.fired = false;
    }

    fn poll_expired(&mut self, waker: &Waker) -> Poll<Expiry> {
        if !self.fired && self.deadline.is_some() {
            self.refresh_waker(waker);
            self.poll_sleep();
        }
        match mem::take(&mut self.fired) {
            true => Poll::Ready(Expiry::Elapsed),
            false => Poll::Pending,
        }
    }

    fn take_expired(&mut self) -> Option<Expiry> {
        if !self.fired && self.deadline.is_some() {
            self.poll_sleep();
        }
        mem::take(&mut self.fired).then_some(Expiry::Elapsed)
    }

    /// Stores `waker` unless the stored one wakes the same task.
    fn refresh_waker(&mut self, waker: &Waker) {
        if self
            .waker
            .as_ref()
            .is_some_and(|current| current.will_wake(waker))
        {
            return;
        }
        self.waker = Some(waker.clone());
    }

    /// Arms for `deadline`. The current sleep is kept if it resolves no later,
    /// and replaced otherwise. The sleep is polled either way, so that the host
    /// holds the current waker and a sleep that resolved unobserved re-arms for
    /// the remainder. A deadline already past completes immediately and wakes
    /// the owner.
    fn arm(&mut self, deadline: Instant) {
        self.deadline = Some(deadline);
        let renew = self
            .sleep
            .as_ref()
            .is_none_or(|sleep| deadline < sleep.deadline);
        if renew {
            self.sleep = Some(HostSleep {
                future: self.host.sleep_until(deadline),
                deadline,
            });
        }
        if self.poll_sleep()
            && let Some(waker) = &self.waker
        {
            waker.wake_by_ref();
        }
    }

    /// Polls the sleep with the owner's waker. A resolution at or after the
    /// deadline fires. An earlier one re-arms for the remainder and polls the
    /// new sleep once. If that one resolves immediately too, the owner is woken
    /// to retry. A resolved sleep of a cancelled timeout is dropped. Returns
    /// whether the timeout fired.
    fn poll_sleep(&mut self) -> bool {
        let Some(waker) = &self.waker else {
            return false;
        };
        let mut context = Context::from_waker(waker);
        for _ in 0..2 {
            let Some(sleep) = &mut self.sleep else {
                return false;
            };
            if sleep.future.as_mut().poll(&mut context).is_pending() {
                return false;
            }
            self.sleep = None;
            let Some(deadline) = self.deadline else {
                return false;
            };
            if deadline <= self.host.now() {
                self.deadline = None;
                self.fired = true;
                return true;
            }
            self.sleep = Some(HostSleep {
                future: self.host.sleep_until(deadline),
                deadline,
            });
        }
        waker.wake_by_ref();
        false
    }
}

#[cfg(test)]
#[path = "../../tests/timer.rs"]
pub(crate) mod tests;
