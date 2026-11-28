/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The test build's global allocator, which counts a thread's heap
//! allocations inside [`count_allocations`].

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::cell::Cell;
use std::hint::black_box;

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
// method, so `System` upholds the `GlobalAlloc` contract. Counting touches
// only const-initialized thread-local cells, which neither allocate nor
// register destructors.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        // SAFETY: this call's caller upheld `alloc`'s layout precondition.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        // SAFETY: this call's caller upheld `alloc_zeroed`'s layout
        // precondition.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record_allocation();
        // SAFETY: `pointer` came from this allocator, which is `System`,
        // with `layout`, and this call's caller upheld `realloc`'s size
        // precondition.
        unsafe { System.realloc(pointer, layout, new_size) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: `pointer` came from this allocator, which is `System`,
        // with `layout`.
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// Runs `run` and returns its value and the allocations it made on this
/// thread, reallocations included.
pub(crate) fn count_allocations<T>(run: impl FnOnce() -> T) -> (T, usize) {
    ALLOCATIONS.with(|count| count.set(0));
    COUNTING.with(|counting| counting.set(true));
    let value = run();
    COUNTING.with(|counting| counting.set(false));
    (value, ALLOCATIONS.with(Cell::get))
}

/// Whether this build's std allocates through [`ALLOCATOR`]. With a
/// dynamically linked std, std's own allocations, including `Vec` growth,
/// bypass it.
pub(crate) fn std_allocations_observable() -> bool {
    count_allocations(|| drop(black_box(Vec::<u8>::with_capacity(4_096)))).1 == 1
}
