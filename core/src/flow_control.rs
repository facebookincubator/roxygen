/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

const MAX_WINDOW_SIZE: u32 = i32::MAX as u32;

/// A signed HTTP/2 flow-control window with an unsigned configured capacity.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Window {
    outstanding: i32,
    capacity: i32,
}

impl Window {
    /// Constructs a window, or returns `None` if `capacity` exceeds `i32::MAX`.
    pub fn new(capacity: u32) -> Option<Self> {
        let mut window = Self::default();
        window.set_capacity(capacity).then_some(window)
    }

    /// Returns currently available credit, which may be negative.
    pub const fn size(&self) -> i32 {
        self.capacity - self.outstanding
    }

    /// Returns currently available credit clamped to zero.
    pub const fn non_negative_size(&self) -> u32 {
        let size = self.size();
        if size > 0 { size as u32 } else { 0 }
    }

    /// Returns the configured capacity.
    pub const fn capacity(&self) -> u32 {
        self.capacity as u32
    }

    /// Returns reserved bytes, clamped to zero after excess credit is freed.
    pub const fn outstanding(&self) -> u32 {
        if self.outstanding < 0 {
            0
        } else {
            self.outstanding as u32
        }
    }

    /// Reserves `amount`, optionally rejecting reservations beyond capacity.
    ///
    /// Every failure leaves the window unchanged.
    pub fn reserve(&mut self, amount: u32, strict: bool) -> bool {
        if amount > MAX_WINDOW_SIZE {
            return false;
        }
        let Some(new_outstanding) = self.outstanding.checked_add(amount as i32) else {
            return false;
        };
        if strict && new_outstanding > self.capacity {
            return false;
        }
        self.outstanding = new_outstanding;
        true
    }

    /// Returns `amount` of credit to the window.
    ///
    /// Every failure leaves the window unchanged.
    pub fn free(&mut self, amount: u32) -> bool {
        if amount > MAX_WINDOW_SIZE {
            return false;
        }
        let Some(new_outstanding) = self.outstanding.checked_sub(amount as i32) else {
            return false;
        };
        if new_outstanding < self.capacity - i32::MAX {
            return false;
        }
        self.outstanding = new_outstanding;
        true
    }

    /// Changes the capacity while preserving the outstanding byte count.
    ///
    /// Every failure leaves the window unchanged.
    pub fn set_capacity(&mut self, capacity: u32) -> bool {
        if capacity > MAX_WINDOW_SIZE {
            return false;
        }
        let capacity = capacity as i32;
        let difference = capacity - self.capacity;
        if difference > 0 {
            let size = self.size();
            if size > 0 && difference > i32::MAX - size {
                return false;
            }
        }
        self.capacity = capacity;
        true
    }
}

/// Receive-window accounting with coalesced credit acknowledgements.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WindowContainer {
    receive_window: Window,
    received_to_acknowledge: usize,
}

impl WindowContainer {
    const UPDATE_THRESHOLD: u32 = 2;
    const MINIMUM_UPDATE: usize = 128 * 1024;

    /// Constructs a container, or returns `None` if `capacity` exceeds `i32::MAX`.
    pub fn new(capacity: u32) -> Option<Self> {
        Some(Self {
            receive_window: Window::new(capacity)?,
            received_to_acknowledge: 0,
        })
    }

    /// Reserves body plus padding credit, then immediately returns the padding.
    ///
    /// Every failure leaves both window and acknowledgement state unchanged.
    pub fn reserve(&mut self, length: usize, padding: u16, strict: bool) -> bool {
        let total = length.wrapping_add(usize::from(padding));
        if total > u32::MAX as usize {
            return false;
        }

        let mut updated_window = self.receive_window;
        if !updated_window.reserve(total as u32, strict) {
            return false;
        }
        let padding = u32::from(padding);
        let freed = updated_window.free(padding);
        debug_assert!(
            freed,
            "a successful reservation must make its padding freeable"
        );
        if !freed {
            return false;
        }
        self.receive_window = updated_window;
        self.received_to_acknowledge = self.received_to_acknowledge.wrapping_add(padding as usize);
        true
    }

    /// Returns processed credit when either update threshold is reached.
    ///
    /// Invalid or unrepresentable amounts return `None` without mutation.
    pub fn processed(&mut self, amount: usize) -> Option<usize> {
        if amount > u32::MAX as usize {
            return None;
        }
        let mut updated_window = self.receive_window;
        if !updated_window.free(amount as u32) {
            return None;
        }
        let received_to_acknowledge = self.received_to_acknowledge.wrapping_add(amount);
        self.receive_window = updated_window;
        self.received_to_acknowledge = received_to_acknowledge;

        if self.received_to_acknowledge >= Self::MINIMUM_UPDATE
            || self.received_to_acknowledge
                >= (self.receive_window.capacity() / Self::UPDATE_THRESHOLD) as usize
        {
            let update = self.received_to_acknowledge;
            self.received_to_acknowledge = 0;
            Some(update)
        } else {
            Some(0)
        }
    }

    /// Grows the capacity and returns its delta; shrink attempts return zero.
    ///
    /// An unrepresentable capacity or window-size overflow returns `None`
    /// without mutation.
    pub fn set_capacity(&mut self, capacity: u32) -> Option<u32> {
        if capacity < self.receive_window.capacity() {
            return Some(0);
        }
        let delta = capacity - self.receive_window.capacity();
        let mut updated_window = self.receive_window;
        if !updated_window.set_capacity(capacity) {
            return None;
        }
        self.receive_window = updated_window;
        Some(delta)
    }

    /// Returns currently available receive credit, which may be negative.
    pub const fn size(&self) -> i32 {
        self.receive_window.size()
    }

    /// Returns the underlying receive-window state.
    pub const fn window(&self) -> &Window {
        &self.receive_window
    }
}
