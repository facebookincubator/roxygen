/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The exchanges of a connection's request streams, kept by stream.

#[cfg(test)]
use std::cell::Cell;
use std::collections::VecDeque;
use std::mem;
use std::ops::Deref;
use std::ops::DerefMut;
#[cfg(test)]
use std::ptr;

use mvfstr_proto::Dir;
use mvfstr_proto::Side;
use mvfstr_proto::StreamId;

use super::Exchange;

/// The most slots the window holds: 8 KiB of pointers, above every default stream limit.
const WINDOW_SLOTS: usize = 1024;

/// The exchanges of a connection's request streams, the client-initiated bidirectional streams,
/// keyed by stream index; a lookup of a stream of any other kind does not find an exchange. Client
/// request stream IDs are dense and their streams close nearly in order, so the exchanges sit in a
/// window of slots indexed by stream: a lookup probes one slot, a removal at the front pops its
/// slot and the empty slots behind it, and a removal elsewhere leaves its slot empty. The window
/// holds a bounded number of slots. An exchange that would hold its front open past the bound, such
/// as a long-lived CONNECT, moves to a side map sorted by stream, as does a stream below the window
/// that cannot take a slot in front of it. Every stream in the side map is below every slot.
pub(super) struct ExchangeTable<E> {
    /// The slot of the stream with index `base + i` at `i`. The first slot is occupied unless the
    /// window is empty.
    window: VecDeque<Option<Box<Exchange<E>>>>,
    base: u64,
    /// The most slots the window holds.
    bound: usize,
    /// Exchanges below the window, by stream index.
    side: Vec<(u64, Box<Exchange<E>>)>,
    /// The exchanges present, and those of them not retired.
    occupied: usize,
    live: usize,
    #[cfg(test)]
    census: Cell<Census>,
}

/// Where a present exchange is.
enum Slot {
    Window(usize),
    Side(usize),
}

impl<E> ExchangeTable<E> {
    pub(super) fn new() -> Self {
        Self {
            window: VecDeque::new(),
            base: 0,
            bound: WINDOW_SLOTS,
            side: Vec::new(),
            occupied: 0,
            live: 0,
            #[cfg(test)]
            census: Cell::default(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.occupied == 0
    }

    /// Returns how many exchanges present have not retired.
    pub(super) fn live(&self) -> usize {
        self.live
    }

    /// Adds the exchange of request stream `id`. The stream must not have one already.
    pub(super) fn insert(&mut self, id: StreamId, exchange: Box<Exchange<E>>) {
        debug_assert!(is_request(id) && !exchange.retired);
        self.occupied += 1;
        self.live += 1;
        let index = id.index();
        let above_side = self.side.last().is_none_or(|&(last, _)| last < index);
        if self.window.is_empty() && above_side {
            self.base = index;
        }
        if index < self.base {
            let grow = self.base - index;
            if above_side && self.window.len() as u64 + grow <= self.bound as u64 {
                for _ in 1..grow {
                    self.window.push_front(None);
                }
                self.window.push_front(Some(exchange));
                self.base = index;
            } else {
                let at = self.side.partition_point(|&(other, _)| other < index);
                self.side.insert(at, (index, exchange));
            }
            return;
        }
        // The exchanges that hold the window's front open past the bound move to the side map.
        while index - self.base >= self.bound as u64 {
            if let Some(Some(exchange)) = self.window.pop_front() {
                self.side.push((self.base, exchange));
            }
            self.base += 1;
            self.pop_empty_front();
            if self.window.is_empty() {
                self.base = index;
            }
        }
        let offset = (index - self.base) as usize;
        if offset < self.window.len() {
            debug_assert!(self.window[offset].is_none(), "a stream has one exchange");
            self.window[offset] = Some(exchange);
        } else {
            self.window.resize_with(offset, || None);
            self.window.push_back(Some(exchange));
        }
    }

    pub(super) fn find(&self, id: StreamId) -> Option<&Exchange<E>> {
        Some(match self.locate(id)? {
            Slot::Window(offset) => self.window[offset].as_deref()?,
            Slot::Side(at) => &self.side[at].1,
        })
    }

    pub(super) fn find_mut(&mut self, id: StreamId) -> Option<ExchangeMut<'_, E>> {
        let exchange = match self.locate(id)? {
            Slot::Window(offset) => self.window[offset].as_deref_mut()?,
            Slot::Side(at) => &mut *self.side[at].1,
        };
        Some(ExchangeMut {
            was_live: !exchange.retired,
            exchange,
            live: &mut self.live,
        })
    }

    pub(super) fn remove(&mut self, id: StreamId) -> Option<Box<Exchange<E>>> {
        let exchange = match self.locate(id)? {
            Slot::Window(offset) => {
                let exchange = self.window[offset].take()?;
                if offset == 0 {
                    self.pop_empty_front();
                }
                exchange
            }
            Slot::Side(at) => {
                #[cfg(test)]
                {
                    let moved = self.side.len() - at - 1;
                    self.count(|census| census.moved += moved);
                }
                self.side.remove(at).1
            }
        };
        self.occupied -= 1;
        if !exchange.retired {
            self.live -= 1;
        }
        Some(exchange)
    }

    /// Takes every exchange out, in ascending stream order.
    pub(super) fn drain(&mut self) -> impl Iterator<Item = Box<Exchange<E>>> {
        self.occupied = 0;
        self.live = 0;
        let side = mem::take(&mut self.side);
        let window = mem::take(&mut self.window);
        side.into_iter()
            .map(|(_, exchange)| exchange)
            .chain(window.into_iter().flatten())
    }

    /// Pops the empty slots at the front of the window.
    fn pop_empty_front(&mut self) {
        while let Some(None) = self.window.front() {
            self.window.pop_front();
            self.base += 1;
        }
    }

    /// Returns where the exchange of stream `id` is, if present.
    fn locate(&self, id: StreamId) -> Option<Slot> {
        #[cfg(test)]
        self.count(|census| census.finds += 1);
        if !is_request(id) {
            return None;
        }
        let index = id.index();
        if index >= self.base {
            let offset = index - self.base;
            if offset >= self.window.len() as u64 {
                return None;
            }
            #[cfg(test)]
            self.count(|census| census.probes += 1);
            let offset = offset as usize;
            return self.window[offset]
                .is_some()
                .then_some(Slot::Window(offset));
        }
        self.side
            .binary_search_by(|&(other, _)| {
                #[cfg(test)]
                self.count(|census| census.probes += 1);
                other.cmp(&index)
            })
            .ok()
            .map(Slot::Side)
    }
}

/// Returns whether `id` is a request stream, the only kind with an exchange.
fn is_request(id: StreamId) -> bool {
    id.initiator() == Side::Client && id.dir() == Dir::Bi
}

/// An exchange borrowed from the table. Dropping it takes the exchange out of the live count if it
/// retired while borrowed.
pub(super) struct ExchangeMut<'a, E> {
    exchange: &'a mut Exchange<E>,
    live: &'a mut usize,
    was_live: bool,
}

impl<E> Deref for ExchangeMut<'_, E> {
    type Target = Exchange<E>;

    fn deref(&self) -> &Exchange<E> {
        self.exchange
    }
}

impl<E> DerefMut for ExchangeMut<'_, E> {
    fn deref_mut(&mut self) -> &mut Exchange<E> {
        self.exchange
    }
}

impl<E> Drop for ExchangeMut<'_, E> {
    fn drop(&mut self) {
        if self.was_live && self.exchange.retired {
            *self.live -= 1;
        }
    }
}

/// The work of the table's lookups, counted for the tests.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct Census {
    /// Exchanges that admission visited to count the live ones.
    pub(crate) visited: usize,
    /// Exchanges that removals moved.
    pub(crate) moved: usize,
    /// Lookups of a stream, and the slots or keys they probed.
    pub(crate) finds: usize,
    pub(crate) probes: usize,
}

/// The layout of the table, for the tests.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Shape {
    /// The stream index of the window's first slot, and the window's slots.
    pub(crate) base: u64,
    pub(crate) slots: usize,
    /// The exchanges in the side map.
    pub(crate) side: usize,
    /// The exchanges present, and those of them not retired.
    pub(crate) occupied: usize,
    pub(crate) live: usize,
}

#[cfg(test)]
impl<E> ExchangeTable<E> {
    pub(super) fn census(&self) -> Census {
        self.census.get()
    }

    pub(super) fn set_bound(&mut self, slots: usize) {
        debug_assert!(slots > 0 && self.window.len() <= slots);
        self.bound = slots;
    }

    pub(super) fn shape(&self) -> Shape {
        Shape {
            base: self.base,
            slots: self.window.len(),
            side: self.side.len(),
            occupied: self.occupied,
            live: self.live,
        }
    }

    /// Checks the counts, emptiness and lookups against a walk of the exchanges, and the layout's
    /// invariants.
    pub(super) fn check(&self) {
        let present = || {
            let side = self.side.iter().map(|(_, exchange)| exchange);
            side.chain(self.window.iter().flatten())
        };
        let live = present().filter(|exchange| !exchange.retired).count();
        assert_eq!(
            (self.occupied, self.live),
            (present().count(), live),
            "the counts of present and live exchanges match a walk"
        );
        assert_eq!(
            self.is_empty(),
            present().next().is_none(),
            "the table is empty when no exchange is present"
        );
        // The census counts the lookups of the code under test, not these.
        let census = self.census.get();
        let side = self
            .side
            .iter()
            .map(|(index, exchange)| (*index, Some(exchange)));
        let window = (self.base..).zip(self.window.iter().map(Option::as_ref));
        for (index, at) in side.chain(window) {
            let id = StreamId::new(Side::Client, Dir::Bi, index).expect("a stream index fits");
            assert_eq!(
                self.find(id).map(ptr::from_ref),
                at.map(|exchange| ptr::from_ref(&**exchange)),
                "a lookup of stream index {index} finds the exchange at its position"
            );
        }
        self.census.set(census);
        assert!(
            self.window.len() <= self.bound,
            "the window holds its bound"
        );
        assert!(
            !matches!(self.window.front(), Some(None)),
            "the window starts at an exchange"
        );
        assert!(
            self.side.windows(2).all(|pair| pair[0].0 < pair[1].0),
            "the side map is sorted"
        );
        assert!(
            self.side.last().is_none_or(|&(last, _)| last < self.base),
            "the side map is below the window"
        );
    }

    fn count(&self, update: impl FnOnce(&mut Census)) {
        let mut census = self.census.get();
        update(&mut census);
        self.census.set(census);
    }
}
