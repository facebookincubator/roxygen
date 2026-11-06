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

        let mut registry = state.registry();
        if state.requested.load(Ordering::Acquire) {
            drop(registry);
            registration.take();
            return true;
        }

        if let Some(existing) = registration {
            if let Some(registered_waker) = existing
                .id
                .and_then(|id| registry.wakers.get_mut(existing.slot, id))
            {
                if !registered_waker.will_wake(waker) {
                    *registered_waker = waker.clone();
                }
                return false;
            }
        }

        let id = registry.next_id;
        registry.next_id = registry.next_id.wrapping_add(1);
        let slot = registry.wakers.insert(id, waker.clone());
        *registration = Some(CancellationRegistration {
            state: Arc::clone(state),
            id: Some(id),
            slot,
        });
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
        drop(self.registration.take());
        drop(waker);
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
    pub(crate) fn unregister(mut self) -> bool {
        let mut registry = self.state.registry();
        if let Some(id) = self.id.take() {
            registry.wakers.remove(self.slot, id);
        }
        self.state.requested.load(Ordering::Acquire)
    }
}

impl Drop for CancellationRegistration {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            self.state.registry().wakers.remove(self.slot, id);
        }
    }
}
