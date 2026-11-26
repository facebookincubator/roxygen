/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! QPACK field compression for HTTP/3, using the static table and literals only.
//!
//! [`Encoder`] encodes a field section with static table references and literals, Huffman-coding
//! each string when that makes it shorter. It never uses the dynamic table, so its field sections
//! never block the peer's decoder, and it does not send encoder stream instructions.
//!
//! [`Decoder`] decodes every field line representation of RFC 9204 for an endpoint that advertises
//! a maximum dynamic table capacity of 0, the default of SETTINGS_QPACK_MAX_TABLE_CAPACITY. Any
//! reference to the dynamic table is then a decompression error.
//!
//! The peer may still open an encoder and a decoder stream. [`Decoder::feed_encoder_stream`]
//! accepts only Set Dynamic Table Capacity with a capacity of 0, and
//! [`Encoder::feed_decoder_stream`] accepts only Stream Cancellation. The [`instructions`] module
//! parses and encodes every instruction of both streams.
//!
//! Field lines are name and value bytes. The HTTP/3 layer applies the HTTP/3 message rules
//! (lowercase names, pseudo-header fields first, the ban on connection-specific fields and the
//! field section size limit). A message that breaks them is malformed or too large, which that
//! layer handles on the request stream. Every error here means that a field section or instruction
//! cannot be decoded.

mod decoder;
mod encoder;
mod error;
mod huffman;
pub mod instructions;
mod integer;
mod static_table;
mod string;

pub use decoder::Decoder;
pub use encoder::Encoder;
pub use error::DecodeError;
pub use error::DecoderStreamError;
pub use error::EncoderStreamError;
pub use error::QPACK_DECODER_STREAM_ERROR;
pub use error::QPACK_DECOMPRESSION_FAILED;
pub use error::QPACK_ENCODER_STREAM_ERROR;
pub use error::WireError;
pub use static_table::STATIC_TABLE;
pub use string::StringLiteral;

/// One line of a field section.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FieldLine<'a> {
    /// The field name.
    pub name: &'a [u8],
    /// The field value.
    pub value: &'a [u8],
    /// Whether the value is sensitive, so every hop must send the line as a literal (the N bit).
    pub never_indexed: bool,
}

impl<'a> FieldLine<'a> {
    /// Creates a line that intermediaries may index.
    pub const fn new(name: &'a [u8], value: &'a [u8]) -> Self {
        Self {
            name,
            value,
            never_indexed: false,
        }
    }

    /// Creates a line that intermediaries must not index.
    pub const fn never_indexed(name: &'a [u8], value: &'a [u8]) -> Self {
        Self {
            name,
            value,
            never_indexed: true,
        }
    }
}

/// What the static table holds for a field line.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaticRef {
    /// The table was not consulted. [`Decoder::decode_resolved_section`] passes this for a line
    /// with a literal name, and [`Encoder::encode_resolved_section`] looks such a line up.
    Unresolved,
    /// The table does not have the line's name, so the encoder writes the name as a literal.
    Unlisted,
    /// The entry at this index has the line's name. The decoder passes this for a line whose name
    /// references the entry and whose value is a literal.
    Name(u8),
    /// The entry at this index has the line's name and value.
    Entry(u8),
}

#[cfg(test)]
mod test_util {
    use std::alloc::GlobalAlloc;
    use std::alloc::Layout;
    use std::alloc::System;
    use std::cell::Cell;

    /// The global allocator of this crate's tests, which counts the heap allocations a thread
    /// makes inside [`count_allocations`].
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

    // SAFETY: every method passes its arguments unchanged to the same `System` method, so
    // `System` upholds the `GlobalAlloc` contract. Counting touches only const-initialized
    // thread-local cells, which neither allocate nor register destructors.
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
            // SAFETY: `pointer` came from this allocator, which is `System`, with `layout`, and
            // this call's caller upheld `realloc`'s size precondition.
            unsafe { System.realloc(pointer, layout, new_size) }
        }

        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            // SAFETY: `pointer` came from this allocator, which is `System`, with `layout`.
            unsafe { System.dealloc(pointer, layout) }
        }
    }

    #[global_allocator]
    static ALLOCATOR: CountingAllocator = CountingAllocator;

    /// Returns the number of allocations `operation` makes on this thread, or `None` where they
    /// are not observable. When std is linked as a shared library, its internal allocations
    /// bypass this crate's `#[global_allocator]`, so counts are complete only when an allocation
    /// made inside std is observed.
    pub(crate) fn count_allocations(operation: impl FnOnce()) -> Option<usize> {
        let allocations = counted(operation);
        let observable = counted(|| drop(std::hint::black_box(vec![0_u8; 4096]))) == 1;
        observable.then_some(allocations)
    }

    fn counted(operation: impl FnOnce()) -> usize {
        ALLOCATIONS.with(|count| count.set(0));
        COUNTING.with(|counting| counting.set(true));
        operation();
        COUNTING.with(|counting| counting.set(false));
        ALLOCATIONS.with(Cell::get)
    }

    /// Parses hex digits, ignoring whitespace.
    pub(crate) fn hex(digits: &str) -> Vec<u8> {
        let digits: Vec<u8> = digits
            .bytes()
            .filter(|b| !b.is_ascii_whitespace())
            .collect();
        digits
            .chunks(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }
}
