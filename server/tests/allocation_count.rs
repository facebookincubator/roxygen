//! Counts the heap allocations a thread makes while its counter is enabled; the
//! server crate's test build uses it as its global allocator.

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::cell::Cell;

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
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        // SAFETY: this call's caller upheld `alloc`'s layout precondition.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        // SAFETY: this call's caller upheld `alloc_zeroed`'s layout precondition.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record_allocation();
        // SAFETY: `pointer` came from this allocator, which is `System`, with
        // `layout`, and this call's caller upheld `realloc`'s size precondition.
        unsafe { System.realloc(pointer, layout, new_size) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: `pointer` came from this allocator, which is `System`, with
        // `layout`.
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// Whether the counts cover every crate's allocations. Buck's fbcode test
/// builds link each Rust crate, `std` and `alloc` included, as a shared library
/// whose allocations do not reach this binary's global allocator, so there a
/// count covers only code compiled into the test crate, and tests skip their
/// allocation assertions.
pub(crate) const COUNTS_EVERY_CRATE: bool = cfg!(not(fbcode_build));

/// Starts counting this thread's allocations from zero.
pub(crate) fn start_counting() {
    ALLOCATIONS.with(|count| count.set(0));
    COUNTING.with(|counting| counting.set(true));
}

/// Stops counting and returns this thread's allocations since `start_counting`.
pub(crate) fn stop_counting() -> usize {
    COUNTING.with(|counting| counting.set(false));
    ALLOCATIONS.with(Cell::get)
}
