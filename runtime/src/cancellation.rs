use std::any::Any;
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Waker;

/// Registered wakers by registration slot. A registration made while the
/// registry holds `One` has slot 0, which promotion to `Many` keeps.
#[derive(Default)]
enum Wakers {
    #[default]
    Empty,
    One(u64, Waker),
    Many(WakerSlab),
}

impl fmt::Debug for Wakers {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut entries = formatter.debug_map();
        match self {
            Self::Empty => {}
            Self::One(id, waker) => {
                entries.entry(id, waker);
            }
            Self::Many(wakers) => {
                let mut occupied: Vec<_> = wakers
                    .slots
                    .iter()
                    .filter_map(|slot| match slot {
                        WakerSlot::Occupied(id, waker) => Some((id, waker)),
                        WakerSlot::Vacant(_) => None,
                    })
                    .collect();
                occupied.sort_unstable_by_key(|&(id, _)| *id);
                entries.entries(occupied);
            }
        }
        entries.finish()
    }
}

impl Wakers {
    fn get_mut(&mut self, slot: usize, id: u64) -> Option<&mut Waker> {
        match self {
            Self::One(registered_id, waker) if *registered_id == id => Some(waker),
            Self::Many(wakers) => wakers.get_mut(slot, id),
            _ => None,
        }
    }

    /// Stores a waker already cloned for the registry, and returns its slot.
    fn insert(&mut self, id: u64, waker: Waker) -> usize {
        match self {
            Self::Empty => {
                *self = Self::One(id, waker);
                0
            }
            Self::One(_, _) => {
                let Self::One(registered_id, registered_waker) = mem::take(self) else {
                    unreachable!("a single cancellation waker is owned");
                };
                *self = Self::Many(WakerSlab {
                    slots: vec![
                        WakerSlot::Occupied(registered_id, registered_waker),
                        WakerSlot::Occupied(id, waker),
                    ],
                    free: NO_SLOT,
                    live: 2,
                });
                1
            }
            Self::Many(wakers) => wakers.insert(id, waker),
        }
    }

    fn remove(&mut self, slot: usize, id: u64) -> Option<Waker> {
        match self {
            Self::One(registered_id, _) if *registered_id == id => {
                let Self::One(_, waker) = mem::take(self) else {
                    unreachable!("a single cancellation waker is owned");
                };
                Some(waker)
            }
            Self::Many(wakers) => wakers.remove(slot, id),
            _ => None,
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::One(_, _) => 1,
            Self::Many(wakers) => wakers.live,
        }
    }

    #[cfg(test)]
    fn slots(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::One(_, _) => 1,
            Self::Many(wakers) => wakers.slots.len(),
        }
    }

    /// The wakers in ascending registration id order.
    fn into_iter(self) -> impl Iterator<Item = Waker> {
        let (one, mut many) = match self {
            Self::Empty => (None, Vec::new()),
            Self::One(_, waker) => (Some(waker), Vec::new()),
            Self::Many(wakers) => (None, wakers.slots),
        };
        many.retain(|slot| matches!(slot, WakerSlot::Occupied(..)));
        many.sort_unstable_by_key(|slot| match slot {
            WakerSlot::Occupied(id, _) => *id,
            WakerSlot::Vacant(_) => u64::MAX,
        });
        one.into_iter()
            .chain(many.into_iter().filter_map(|slot| match slot {
                WakerSlot::Occupied(_, waker) => Some(waker),
                WakerSlot::Vacant(_) => None,
            }))
    }
}

const NO_SLOT: usize = usize::MAX;

/// Wakers addressed by slot, each stored with its registration id. Lookup and
/// removal check both, and freed slots are reused last-freed first, so
/// storage stays at the peak registration count.
struct WakerSlab {
    slots: Vec<WakerSlot>,
    free: usize,
    live: usize,
}

enum WakerSlot {
    Occupied(u64, Waker),
    Vacant(usize),
}

impl WakerSlab {
    fn get_mut(&mut self, slot: usize, id: u64) -> Option<&mut Waker> {
        match self.slots.get_mut(slot)? {
            WakerSlot::Occupied(registered_id, waker) if *registered_id == id => Some(waker),
            _ => None,
        }
    }

    fn insert(&mut self, id: u64, waker: Waker) -> usize {
        let entry = WakerSlot::Occupied(id, waker);
        let slot = match self.slots.get_mut(self.free) {
            Some(vacant) => {
                let WakerSlot::Vacant(next) = *vacant else {
                    unreachable!("the free list holds only vacant slots");
                };
                *vacant = entry;
                mem::replace(&mut self.free, next)
            }
            None => {
                self.slots.push(entry);
                self.slots.len() - 1
            }
        };
        self.live += 1;
        slot
    }

    /// Vacates the slot only if it holds `id`, and returns the waker for the
    /// caller to drop once the bookkeeping is complete.
    fn remove(&mut self, slot: usize, id: u64) -> Option<Waker> {
        let entry = self.slots.get_mut(slot)?;
        if !matches!(entry, WakerSlot::Occupied(registered_id, _) if *registered_id == id) {
            return None;
        }
        let WakerSlot::Occupied(_, waker) = mem::replace(entry, WakerSlot::Vacant(self.free))
        else {
            unreachable!("the slot was checked to be occupied");
        };
        self.free = slot;
        self.live -= 1;
        Some(waker)
    }
}

#[derive(Debug)]
struct Registry {
    next_id: u64,
    wakers: Wakers,
}

impl Registry {
    fn new() -> Self {
        Self {
            next_id: 1,
            wakers: Wakers::Empty,
        }
    }
}

#[derive(Debug)]
struct CancellationState {
    requested: AtomicBool,
    registry: Mutex<Registry>,
    #[cfg(test)]
    registry_locks: AtomicUsize,
}

impl CancellationState {
    fn new() -> Self {
        Self {
            requested: AtomicBool::new(false),
            registry: Mutex::new(Registry::new()),
            #[cfg(test)]
            registry_locks: AtomicUsize::new(0),
        }
    }

    fn registry(&self) -> std::sync::MutexGuard<'_, Registry> {
        #[cfg(test)]
        self.registry_locks.fetch_add(1, Ordering::Relaxed);
        self.registry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }
}

/// The request side of cooperative cancellation.
///
/// Requesting cancellation is thread-safe. It only marks the token and wakes
/// registered tasks; wait-state mutation and completion remain on the task's
/// owner thread.
#[derive(Debug)]
pub struct CancellationSource {
    state: Arc<CancellationState>,
}

impl CancellationSource {
    pub fn new() -> Self {
        Self {
            state: Arc::new(CancellationState::new()),
        }
    }

    pub fn token(&self) -> CancellationToken {
        CancellationToken {
            state: Some(Arc::clone(&self.state)),
        }
    }

    /// Same answer as `self.token().is_cancellation_requested()`.
    #[inline]
    pub fn is_cancellation_requested(&self) -> bool {
        self.state.requested.load(Ordering::Acquire)
    }

    pub fn request_cancellation(&self) -> bool {
        if self.state.requested.load(Ordering::Acquire) {
            return false;
        }
        let wakers = {
            let mut registry = self.state.registry();
            if self.state.requested.swap(true, Ordering::AcqRel) {
                return false;
            }
            mem::take(&mut registry.wakers)
        };
        let mut first_panic = None;
        for waker in wakers.into_iter() {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| waker.wake())) {
                if first_panic.is_none() {
                    first_panic = Some(payload);
                } else if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
                    crate::panic::discard_panic(secondary);
                }
            }
        }
        if let Some(payload) = first_panic {
            resume_unwind(payload);
        }
        true
    }
}

impl Default for CancellationSource {
    fn default() -> Self {
        Self::new()
    }
}

/// An explicitly passed cancellation capability.
#[derive(Clone, Debug, Default)]
pub struct CancellationToken {
    state: Option<Arc<CancellationState>>,
}

impl CancellationToken {
    pub fn never() -> Self {
        Self::default()
    }

    /// Whether this token is connected to a source that can request cancellation.
    pub fn can_be_cancelled(&self) -> bool {
        self.state.is_some()
    }

    #[inline]
    pub fn is_cancellation_requested(&self) -> bool {
        self.state
            .as_ref()
            .is_some_and(|state| state.requested.load(Ordering::Acquire))
    }

    /// Creates an owner-local wake registration permanently bound to this token.
    /// No listener is installed until its waker is updated.
    pub fn wake_registration(&self) -> CancellationWakeRegistration {
        CancellationWakeRegistration {
            token: self.clone(),
            registration: None,
            waker: None,
            owner: PhantomData,
        }
    }

    pub(crate) fn register(
        &self,
        registration: &mut Option<CancellationRegistration>,
        waker: &Waker,
    ) -> bool {
        let Some(state) = &self.state else {
            return false;
        };

        // Waker callbacks can re-enter this token, so they run outside the lock.
        let replacement = waker.clone();
        let mut registry = state.registry();
        if state.requested.load(Ordering::Acquire) {
            drop(registry);
            retire_registration_and_waker(registration.take(), Some(replacement));
            return true;
        }

        if let Some(existing) = registration.as_ref()
            && let Some(registered_waker) = existing
                .id
                .and_then(|id| registry.wakers.get_mut(existing.slot, id))
        {
            let retired = if registered_waker.will_wake(waker) {
                replacement
            } else {
                mem::replace(registered_waker, replacement)
            };
            drop(registry);
            drop(retired);
            return false;
        }

        let id = registry.next_id;
        registry.next_id = registry.next_id.wrapping_add(1);
        let slot = registry.wakers.insert(id, replacement);
        let retired = registration.replace(CancellationRegistration {
            state: Arc::clone(state),
            id: Some(id),
            slot,
        });
        drop(registry);
        drop(retired);
        false
    }

    #[cfg(test)]
    pub(crate) fn registration_count(&self) -> usize {
        self.state
            .as_ref()
            .map_or(0, |state| state.registry().wakers.len())
    }

    /// How many times the registry lock has been taken.
    #[cfg(test)]
    pub(crate) fn registry_locks(&self) -> usize {
        self.state
            .as_ref()
            .map_or(0, |state| state.registry_locks.load(Ordering::Relaxed))
    }

    /// Every slot the registry stores, occupied or vacant.
    #[cfg(test)]
    pub(crate) fn registration_slots(&self) -> usize {
        self.state
            .as_ref()
            .map_or(0, |state| state.registry().wakers.slots())
    }
}

/// An independent cancellation listener updated and retired on its owner thread.
///
/// Dropping this guard unregisters its listener, but cancellation may already
/// have extracted a waker that can still run after an update or drop.
#[derive(Debug)]
pub struct CancellationWakeRegistration {
    token: CancellationToken,
    registration: Option<CancellationRegistration>,
    waker: Option<Waker>,
    owner: PhantomData<Rc<()>>,
}

impl CancellationWakeRegistration {
    /// Updates the wake target and reports whether cancellation was observed.
    /// A `true` result does not promise a fresh wake of the supplied target; a
    /// `false` result does not prevent cancellation immediately afterward.
    /// Never-cancellable tokens neither retain nor clone the waker.
    ///
    /// A waker callback panic may leave the new target installed. The retained
    /// guard can be retried with either target or dropped to retire its listener.
    #[inline]
    pub fn update_waker(&mut self, waker: &Waker) -> bool {
        if !self.token.can_be_cancelled() {
            return false;
        }
        if self.token.is_cancellation_requested() {
            self.clear();
            return true;
        }
        if self
            .waker
            .as_ref()
            .is_some_and(|cached| cached.will_wake(waker))
        {
            return false;
        }
        self.replace_waker(waker)
    }

    #[inline(never)]
    fn replace_waker(&mut self, waker: &Waker) -> bool {
        // Publish the cache only after registry work succeeds, so panic retries synchronize.
        drop(self.waker.take());
        if self.token.register(&mut self.registration, waker)
            || self.token.is_cancellation_requested()
        {
            self.clear();
            return true;
        }
        self.waker = Some(waker.clone());
        if self.token.is_cancellation_requested() {
            self.clear();
            return true;
        }
        false
    }

    fn clear(&mut self) {
        let waker = self.waker.take();
        retire_registration_and_waker(self.registration.take(), waker);
    }
}

impl Drop for CancellationWakeRegistration {
    fn drop(&mut self) {
        self.clear();
    }
}

pub(crate) struct CancellationRegistration {
    state: Arc<CancellationState>,
    id: Option<u64>,
    slot: usize,
}

impl fmt::Debug for CancellationRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CancellationRegistration")
            .field("state", &self.state)
            .field("id", &self.id)
            .finish()
    }
}

impl CancellationRegistration {
    /// Reports whether cancellation was requested before the removed waker
    /// is dropped.
    #[inline]
    pub(crate) fn unregister(mut self) -> bool {
        let mut registry = self.state.registry();
        let retired = self
            .id
            .take()
            .and_then(|id| registry.wakers.remove(self.slot, id));
        let requested = self.state.requested.load(Ordering::Acquire);
        drop(registry);
        drop(retired);
        requested
    }
}

impl Drop for CancellationRegistration {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let retired = self.state.registry().wakers.remove(self.slot, id);
            drop(retired);
        }
    }
}

/// Drops a retired registration and then its waker, each inside its own unwind
/// boundary, so one panicking drop cannot skip the other. The first panic
/// resumes unless the thread is already unwinding; every other one is
/// discarded.
fn retire_registration_and_waker(
    registration: Option<CancellationRegistration>,
    waker: Option<Waker>,
) {
    let mut first = catch_unwind(AssertUnwindSafe(|| drop(registration))).err();
    if let Err(next) = catch_unwind(AssertUnwindSafe(|| drop(waker))) {
        if first.is_none() {
            first = Some(next);
        } else {
            crate::panic::discard_panic(next);
        }
    }
    if let Some(payload) = first {
        finish_retirement_panic(payload);
    }
}

#[cold]
#[inline(never)]
fn finish_retirement_panic(payload: Box<dyn Any + Send>) {
    if std::thread::panicking() {
        crate::panic::discard_panic(payload);
    } else {
        resume_unwind(payload);
    }
}

#[cfg(test)]
mod callback_safety_tests {
    use std::cell::Cell;
    use std::panic::panic_any;
    use std::ptr;
    use std::sync::atomic::AtomicUsize;
    use std::sync::mpsc;
    use std::task::RawWaker;
    use std::task::RawWakerVTable;
    use std::task::Wake;
    use std::time::Duration;

    use super::*;

    fn assert_unlocked(token: &CancellationToken) {
        assert!(
            token.state.as_ref().unwrap().registry.try_lock().is_ok(),
            "a waker callback must run outside the cancellation registry lock",
        );
    }

    struct CountWake(Arc<AtomicUsize>);

    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn counting_waker() -> (Waker, Arc<AtomicUsize>) {
        let wakes = Arc::new(AtomicUsize::new(0));
        (Waker::from(Arc::new(CountWake(Arc::clone(&wakes)))), wakes)
    }

    /// A waker whose last drop re-enters its token's source.
    struct ReentrantDrop {
        source: Arc<CancellationSource>,
        dropped: Arc<AtomicUsize>,
        cancel: bool,
        panic: bool,
    }

    #[expect(
        clippy::manual_noop_waker,
        reason = "Distinct owners exercise last-drop reentry"
    )]
    impl Wake for ReentrantDrop {
        fn wake(self: Arc<Self>) {}
    }

    impl Drop for ReentrantDrop {
        fn drop(&mut self) {
            let token = self.source.token();
            assert_unlocked(&token);
            self.dropped.fetch_add(1, Ordering::SeqCst);
            if self.cancel {
                self.source.request_cancellation();
            } else {
                let mut probe = None;
                assert!(!token.register(&mut probe, Waker::noop()));
                drop(probe);
            }
            assert!(!self.panic, "retired cancellation target");
        }
    }

    fn reentrant_waker(
        source: &Arc<CancellationSource>,
        dropped: &Arc<AtomicUsize>,
        cancel: bool,
        panic: bool,
    ) -> Waker {
        Waker::from(Arc::new(ReentrantDrop {
            source: Arc::clone(source),
            dropped: Arc::clone(dropped),
            cancel,
            panic,
        }))
    }

    thread_local! {
        static CANCEL_ON_CLONE: Cell<Option<Arc<CancellationSource>>> = const { Cell::new(None) };
        static PANICKING_DROPS: Cell<u8> = const { Cell::new(0) };
        static DROPS: Cell<u8> = const { Cell::new(0) };
    }

    /// A waker whose clone can request cancellation and whose drops can panic,
    /// each driven by this thread's test state.
    fn callback_waker() -> Waker {
        fn clone(_: *const ()) -> RawWaker {
            if let Some(source) = CANCEL_ON_CLONE.take() {
                assert_unlocked(&source.token());
                source.request_cancellation();
            }
            raw()
        }
        fn wake(_: *const ()) {}
        fn drop_waker(_: *const ()) {
            let remaining = PANICKING_DROPS.get();
            if remaining != 0 {
                PANICKING_DROPS.set(remaining - 1);
                let ordinal = DROPS.get() + 1;
                DROPS.set(ordinal);
                panic_any(ordinal);
            }
        }
        fn raw() -> RawWaker {
            static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake, drop_waker);
            RawWaker::new(ptr::null(), &VTABLE)
        }
        // SAFETY: The vtable owns no resources and never dereferences its null
        // data. Each clone is independently valid and callbacks use only TLS.
        unsafe { Waker::from_raw(raw()) }
    }

    #[test]
    fn registration_clone_can_cancel_before_insertion_or_replacement() {
        for replace in [false, true] {
            let source = Arc::new(CancellationSource::new());
            let token = source.token();
            let (old, wakes) = counting_waker();
            let mut registration = None;
            if replace {
                assert!(!token.register(&mut registration, &old));
            }
            CANCEL_ON_CLONE.set(Some(Arc::clone(&source)));
            assert!(token.register(&mut registration, &callback_waker()));
            assert!(CANCEL_ON_CLONE.take().is_none());
            assert!(registration.is_none());
            assert_eq!(token.registration_count(), 0);
            assert_eq!(wakes.load(Ordering::SeqCst), usize::from(replace));
        }
    }

    #[test]
    fn replacement_publishes_the_new_target_before_retiring_the_old_target() {
        let source = Arc::new(CancellationSource::new());
        let dropped = Arc::new(AtomicUsize::new(0));
        let mut listener = source.token().wake_registration();
        let old = reentrant_waker(&source, &dropped, true, false);
        assert!(!listener.update_waker(&old));
        drop(old);
        let (current, wakes) = counting_waker();
        assert!(listener.update_waker(&current));
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert_eq!(source.token().registration_count(), 0);
    }

    /// Requests cancellation on its last drop, without checking the lock first.
    struct CancelOnDrop(Arc<CancellationSource>);

    #[expect(
        clippy::manual_noop_waker,
        reason = "Distinct owners exercise last-drop reentry"
    )]
    impl Wake for CancelOnDrop {
        fn wake(self: Arc<Self>) {}
    }

    impl Drop for CancelOnDrop {
        fn drop(&mut self) {
            self.0.request_cancellation();
        }
    }

    /// A displaced target whose last drop requests cancellation on its own
    /// source must not wait on the registry lock that displaced it. The
    /// registration runs on another thread so a hang reports as a failure.
    #[test]
    fn reentrant_cancellation_from_a_displaced_target_does_not_deadlock() {
        let (done_tx, done_rx) = mpsc::channel();
        std::thread::spawn(move || {
            let source = Arc::new(CancellationSource::new());
            let mut listener = source.token().wake_registration();
            let old = Waker::from(Arc::new(CancelOnDrop(Arc::clone(&source))));
            assert!(!listener.update_waker(&old));
            drop(old);
            let (current, _wakes) = counting_waker();
            let observed = listener.update_waker(&current);
            let _ = done_tx.send((observed, source.is_cancellation_requested()));
        });
        match done_rx.recv_timeout(Duration::from_secs(3)) {
            Ok(outcome) => assert_eq!(
                outcome,
                (true, true),
                "the displaced target's cancellation is observed"
            ),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                panic!("the displaced target's drop re-locked the cancellation registry")
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => panic!("the registering thread panicked"),
        }
    }

    #[test]
    fn registration_removal_allows_reentry_and_preserves_its_cancellation_snapshot() {
        for explicit in [false, true] {
            for cancel in [false, true] {
                let source = Arc::new(CancellationSource::new());
                let token = source.token();
                let dropped = Arc::new(AtomicUsize::new(0));
                let old = reentrant_waker(&source, &dropped, cancel, false);
                let mut registration = None;
                assert!(!token.register(&mut registration, &old));
                drop(old);
                let registration = registration.unwrap();
                if explicit {
                    assert!(
                        !registration.unregister(),
                        "removal precedes destructor cancellation"
                    );
                } else {
                    drop(registration);
                }
                assert_eq!(dropped.load(Ordering::SeqCst), 1);
                assert_eq!(token.registration_count(), 0);
                assert_eq!(token.is_cancellation_requested(), cancel);
            }
        }
    }

    #[test]
    fn replacement_remains_registered_when_the_retired_target_panics() {
        let source = Arc::new(CancellationSource::new());
        let token = source.token();
        let dropped = Arc::new(AtomicUsize::new(0));
        let old = reentrant_waker(&source, &dropped, false, true);
        let mut listener = token.wake_registration();
        assert!(!listener.update_waker(&old));
        drop(old);
        let (current, wakes) = counting_waker();
        let panic = catch_unwind(AssertUnwindSafe(|| listener.update_waker(&current)))
            .expect_err("the displaced last target panics after replacement");
        assert_eq!(
            panic.downcast_ref::<&str>(),
            Some(&"retired cancellation target")
        );
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(token.registration_count(), 1);
        assert!(!listener.update_waker(&current));
        assert!(source.request_cancellation());
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert_eq!(token.registration_count(), 0);
    }

    #[test]
    fn listener_cleanup_retires_both_wakers_and_preserves_the_first_panic() {
        let source = CancellationSource::new();
        let token = source.token();
        let mut listener = token.wake_registration();
        let waker = callback_waker();
        assert!(!listener.update_waker(&waker));
        DROPS.set(0);
        PANICKING_DROPS.set(2);
        let panic = catch_unwind(AssertUnwindSafe(|| drop(listener)))
            .expect_err("both independently retained targets panic on retirement");
        PANICKING_DROPS.set(0);
        assert_eq!(panic.downcast_ref::<u8>(), Some(&1));
        assert_eq!(DROPS.get(), 2);
        assert_eq!(token.registration_count(), 0);
        assert_unlocked(&token);
    }

    #[test]
    fn listener_cleanup_during_unwind_preserves_the_outer_panic() {
        let source = CancellationSource::new();
        let token = source.token();
        let mut listener = token.wake_registration();
        let waker = callback_waker();
        assert!(!listener.update_waker(&waker));
        DROPS.set(0);
        PANICKING_DROPS.set(2);
        let panic = catch_unwind(AssertUnwindSafe(move || {
            let _listener = listener;
            panic_any(99_u8);
        }))
        .expect_err("listener cleanup must preserve the active outer unwind");
        PANICKING_DROPS.set(0);
        assert_eq!(panic.downcast_ref::<u8>(), Some(&99));
        assert_eq!(DROPS.get(), 2);
        assert_eq!(token.registration_count(), 0);
        assert_unlocked(&token);
    }

    /// A wake registration's first update takes the registry lock once. An
    /// unchanged waker takes none (the `will_wake` cache), a changed one takes
    /// one more, and dropping the registration takes one to retire its
    /// listener.
    #[test]
    fn a_wake_registrations_first_update_takes_one_registry_lock() {
        let source = CancellationSource::new();
        let token = source.token();
        let (first, _) = counting_waker();
        let (second, _) = counting_waker();
        let mut registration = token.wake_registration();
        let locks = token.registry_locks();
        assert!(!registration.update_waker(&first));
        assert_eq!(token.registry_locks(), locks + 1, "the first update");
        assert!(!registration.update_waker(&first.clone()));
        assert_eq!(token.registry_locks(), locks + 1, "an unchanged waker");
        assert!(!registration.update_waker(&second));
        assert_eq!(token.registry_locks(), locks + 2, "a changed waker");
        drop(registration);
        assert_eq!(token.registry_locks(), locks + 3, "the registration's drop");
    }

    /// Every registration takes the registry lock once, whether its target is
    /// new, unchanged or changed, and the token keeps the latest target.
    #[test]
    fn registration_takes_one_registry_lock_per_call() {
        let source = CancellationSource::new();
        let token = source.token();
        let (first, first_wakes) = counting_waker();
        let (second, second_wakes) = counting_waker();
        let mut registration = None;
        let locks = token.registry_locks();
        assert!(!token.register(&mut registration, &first));
        assert_eq!(token.registry_locks(), locks + 1, "an initial target");
        assert!(!token.register(&mut registration, &first.clone()));
        assert_eq!(token.registry_locks(), locks + 2, "an unchanged target");
        assert!(!token.register(&mut registration, &second));
        assert_eq!(token.registry_locks(), locks + 3, "a changed target");
        assert_eq!(token.registration_count(), 1);
        assert!(source.request_cancellation());
        assert_eq!(first_wakes.load(Ordering::SeqCst), 0);
        assert_eq!(second_wakes.load(Ordering::SeqCst), 1);
        drop(registration);
    }
}
