/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::borrow::Cow;
#[cfg(test)]
use std::cell::Cell;
use std::vec::IntoIter;

use roxygen_core::buf_queue::BufQueue;
use roxygen_core::buf_queue::BufQueueSegments;

use super::HpackError;
use super::Instruction;
use super::NBIT_MASKS;
use super::huffman;

/// The decoder scratch size: it holds any Huffman literal of at most 160 encoded bytes.
pub(super) const LITERAL_SCRATCH: usize = huffman::max_decoded_len(160);

#[cfg(test)]
thread_local! {
    static OWNED_LITERAL_COPIES: Cell<usize> = const { Cell::new(0) };
    static BLOCK_ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    static HUFFMAN_HEAP_DECODES: Cell<usize> = const { Cell::new(0) };
    static SLOW_PATH_ENTRIES: Cell<usize> = const { Cell::new(0) };
}

/// Counts one header block buffer allocation (an initial allocation, or growth during an encode).
#[cfg(test)]
fn count_block_allocation() {
    BLOCK_ALLOCATIONS.with(|count| count.set(count.get() + 1));
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HpackEncodeBuffer {
    bytes: Vec<u8>,
    headroom: usize,
    huffman_enabled: bool,
    huffman_min: usize,
    huffman_max: usize,
    #[cfg(test)]
    start_capacity: usize,
}

impl HpackEncodeBuffer {
    /// Returns and resets this thread's count of header block buffer allocations. A block buffer
    /// counts when it starts with new storage, grows while encoding, or copies a memoized block.
    #[cfg(test)]
    pub(crate) fn take_block_allocations() -> usize {
        BLOCK_ALLOCATIONS.with(|count| count.replace(0))
    }

    pub fn new(huffman_enabled: bool) -> Self {
        Self {
            bytes: Vec::new(),
            headroom: 0,
            huffman_enabled,
            huffman_min: 0,
            huffman_max: usize::MAX,
            #[cfg(test)]
            start_capacity: 0,
        }
    }

    pub fn with_headroom(huffman_enabled: bool, headroom: usize) -> Self {
        Self::with_headroom_and_capacity(huffman_enabled, headroom, 0)
    }

    pub(crate) fn with_headroom_and_capacity(
        huffman_enabled: bool,
        headroom: usize,
        initial_capacity: usize,
    ) -> Self {
        Self::with_storage(huffman_enabled, headroom, initial_capacity, Vec::new())
    }

    /// Like [`with_headroom_and_capacity`](Self::with_headroom_and_capacity), but encodes into
    /// `storage`, discarding its contents and reusing its capacity.
    pub(crate) fn with_storage(
        huffman_enabled: bool,
        headroom: usize,
        initial_capacity: usize,
        mut storage: Vec<u8>,
    ) -> Self {
        #[cfg(test)]
        let reused_capacity = storage.capacity();
        storage.clear();
        storage.reserve(initial_capacity);
        storage.resize(headroom, 0);
        let mut buffer = Self::new(huffman_enabled);
        buffer.bytes = storage;
        buffer.headroom = headroom;
        #[cfg(test)]
        {
            if buffer.bytes.capacity() != reused_capacity {
                count_block_allocation();
            }
            buffer.start_capacity = buffer.bytes.capacity();
        }
        buffer
    }

    pub fn set_huffman_limits(&mut self, minimum: usize, maximum: usize) {
        self.huffman_min = minimum;
        self.huffman_max = maximum;
    }

    #[inline]
    pub fn encode_integer(&mut self, value: u64, instruction: Instruction) -> usize {
        self.encode_integer_parts(value, instruction.code, instruction.prefix_length)
    }

    /// Always inlined, so that a constant instruction folds its checks and a value that fits the
    /// prefix is one push; a larger value continues in `encode_integer_continuation`.
    #[inline(always)]
    pub fn encode_integer_parts(&mut self, value: u64, instruction: u8, nbits: u8) -> usize {
        assert!(
            (1..=8).contains(&nbits),
            "HPACK integer prefix must be 1..=8"
        );
        let mask = NBIT_MASKS[usize::from(nbits)];
        assert_eq!(
            instruction & mask,
            0,
            "instruction overlaps its integer prefix"
        );
        if value < u64::from(mask) {
            self.bytes.push(instruction | value as u8);
            return 1;
        }
        self.encode_integer_continuation(value - u64::from(mask), instruction | mask)
    }

    /// Writes `first`, a full prefix, then `value`, the rest of the integer, in 7-bit groups.
    fn encode_integer_continuation(&mut self, mut value: u64, first: u8) -> usize {
        self.bytes.push(first);
        let start = self.bytes.len();
        while value >= 128 {
            self.bytes.push(0x80 | (value as u8 & 0x7f));
            value >>= 7;
        }
        self.bytes.push(value as u8);
        self.bytes.len() - start + 1
    }

    pub fn encode_literal(&mut self, literal: &[u8]) -> usize {
        self.encode_literal_parts(0, 7, literal)
    }

    pub fn encode_literal_parts(&mut self, instruction: u8, nbits: u8, literal: &[u8]) -> usize {
        assert!(
            (1..=7).contains(&nbits),
            "HPACK literal prefix must be 1..=7"
        );
        if self.huffman_enabled && (self.huffman_min..=self.huffman_max).contains(&literal.len()) {
            let huffman_bit = 1_u8 << nbits;
            assert_eq!(
                instruction & huffman_bit,
                0,
                "instruction sets the Huffman bit"
            );
            // Preserve the emitted length when the bit-count estimate can overflow.
            if literal.len() > usize::MAX / 30 {
                let encoded = huffman::encode(literal);
                let count = self.encode_integer_parts(
                    encoded.len() as u64,
                    instruction | huffman_bit,
                    nbits,
                );
                self.bytes.extend_from_slice(&encoded);
                return count + encoded.len();
            }
            let encoded_size = huffman::encoded_size(literal);
            let count =
                self.encode_integer_parts(encoded_size as u64, instruction | huffman_bit, nbits);
            huffman::append_encoded_sized(literal, encoded_size, &mut self.bytes);
            return count + encoded_size;
        }

        let count = self.encode_integer_parts(literal.len() as u64, instruction, nbits);
        self.bytes.extend_from_slice(literal);
        count + literal.len()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[self.headroom..]
    }

    pub fn into_bytes(self) -> Vec<u8> {
        if self.headroom == 0 {
            return self.bytes;
        }
        self.bytes[self.headroom..].to_vec()
    }

    pub fn release(self) -> EncodedHeaderBlock {
        #[cfg(test)]
        if self.bytes.capacity() != self.start_capacity {
            count_block_allocation();
        }
        EncodedHeaderBlock {
            storage: self.bytes,
            start: self.headroom,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedHeaderBlock {
    storage: Vec<u8>,
    start: usize,
}

impl EncodedHeaderBlock {
    /// Copies `bytes` after `headroom` into `storage`, discarding its contents and reusing its
    /// capacity.
    pub(super) fn copy_with_headroom_into(
        mut storage: Vec<u8>,
        bytes: &[u8],
        headroom: usize,
    ) -> Self {
        #[cfg(test)]
        let reused_capacity = storage.capacity();
        storage.clear();
        storage.reserve_exact(headroom + bytes.len());
        #[cfg(test)]
        if storage.capacity() != reused_capacity {
            count_block_allocation();
        }
        storage.resize(headroom, 0);
        storage.extend_from_slice(bytes);
        Self {
            storage,
            start: headroom,
        }
    }

    pub const fn headroom(&self) -> usize {
        self.start
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.storage[self.start..]
    }

    pub fn prepend(&mut self, prefix: &[u8]) -> bool {
        if prefix.len() > self.start {
            return false;
        }
        self.start -= prefix.len();
        self.storage[self.start..self.start + prefix.len()].copy_from_slice(prefix);
        true
    }

    pub fn into_bytes(self) -> Vec<u8> {
        if self.start == 0 {
            return self.storage;
        }
        self.storage[self.start..].to_vec()
    }

    /// Returns the backing storage, headroom included, for reuse by a later block.
    pub(crate) fn into_storage(self) -> Vec<u8> {
        self.storage
    }
}

#[derive(Clone, Debug)]
enum DecodeSegments<'a> {
    Queue(BufQueueSegments<'a>),
    Collected(IntoIter<&'a [u8]>),
}

impl<'a> DecodeSegments<'a> {
    fn next(&mut self) -> Option<&'a [u8]> {
        match self {
            Self::Queue(segments) => segments.next().map(|segment| segment.as_ref()),
            Self::Collected(segments) => segments.next(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct HpackDecodeBuffer<'a> {
    segments: DecodeSegments<'a>,
    segment: &'a [u8],
    offset: usize,
    remaining: usize,
    total: usize,
    max_literal_size: usize,
}

impl<'a> HpackDecodeBuffer<'a> {
    pub fn new(bytes: &'a [u8], max_literal_size: usize) -> Self {
        Self {
            segments: DecodeSegments::Collected(Vec::new().into_iter()),
            segment: bytes,
            offset: 0,
            remaining: bytes.len(),
            total: bytes.len(),
            max_literal_size,
        }
    }

    pub fn from_buf_queue(
        queue: &'a BufQueue,
        total_bytes: usize,
        max_literal_size: usize,
    ) -> Self {
        Self {
            segments: DecodeSegments::Queue(queue.segments()),
            segment: &[],
            offset: 0,
            remaining: total_bytes,
            total: total_bytes,
            max_literal_size,
        }
    }

    /// Eagerly collects segment references before decoding any bytes.
    pub fn from_segments(
        segments: impl IntoIterator<Item = &'a [u8]>,
        total_bytes: usize,
        max_literal_size: usize,
    ) -> Self {
        let segments: Vec<_> = segments.into_iter().collect();
        Self {
            segments: DecodeSegments::Collected(segments.into_iter()),
            segment: &[],
            offset: 0,
            remaining: total_bytes,
            total: total_bytes,
            max_literal_size,
        }
    }

    pub const fn consumed_bytes(&self) -> usize {
        self.total - self.remaining
    }

    pub const fn remaining_bytes(&self) -> usize {
        self.remaining
    }

    pub const fn is_empty(&self) -> bool {
        self.remaining == 0
    }

    /// Returns the next byte without consuming it. A byte left in the current segment is returned
    /// inline; every other state takes `peek_slow`.
    #[inline]
    pub fn peek(&mut self) -> Result<u8, HpackError> {
        if self.remaining != 0 {
            if let Some(&byte) = self.segment.get(self.offset) {
                return Ok(byte);
            }
        }
        self.peek_slow()
    }

    /// `peek` when no bytes remain or the current segment is used up.
    #[cold]
    #[inline(never)]
    fn peek_slow(&mut self) -> Result<u8, HpackError> {
        self.skip_empty_segments();
        if self.remaining == 0 {
            return Err(HpackError::BufferUnderflow);
        }
        self.segment
            .get(self.offset)
            .copied()
            .ok_or(HpackError::BufferUnderflow)
    }

    #[inline]
    pub(crate) fn read_byte(&mut self) -> Result<u8, HpackError> {
        let byte = self.peek()?;
        self.offset += 1;
        self.remaining -= 1;
        Ok(byte)
    }

    #[inline]
    pub fn decode_integer(&mut self, nbits: u8) -> Result<u64, HpackError> {
        if !(1..=8).contains(&nbits) {
            return Err(HpackError::InvalidEncoding);
        }
        let first = self.read_byte()?;
        self.decode_integer_from(first, NBIT_MASKS[usize::from(nbits)])
    }

    /// Finishes an integer after its first byte, `first`, has been read. A prefix below `mask` is
    /// the whole value, and a full prefix continues out of line.
    #[inline]
    fn decode_integer_from(&mut self, first: u8, mask: u8) -> Result<u64, HpackError> {
        let prefix = first & mask;
        if prefix != mask {
            return Ok(u64::from(prefix));
        }
        self.decode_integer_continuation(u64::from(prefix))
    }

    /// Reads the 7-bit groups that follow a full prefix and adds them to `integer`, the prefix's
    /// value.
    #[inline(never)]
    fn decode_integer_continuation(&mut self, mut integer: u64) -> Result<u64, HpackError> {
        let mut shift = 0_u32;
        loop {
            let byte = self.read_byte()?;
            if shift >= 64 {
                return Err(HpackError::IntegerOverflow);
            }
            let factor = u64::from(byte & 0x7f);
            if shift == 63 && factor > 1 {
                return Err(HpackError::IntegerOverflow);
            }
            let add = factor
                .checked_shl(shift)
                .ok_or(HpackError::IntegerOverflow)?;
            if u64::MAX - integer <= add {
                return Err(HpackError::IntegerOverflow);
            }
            integer += add;
            if byte & 0x80 == 0 {
                return Ok(integer);
            }
            shift += 7;
        }
    }

    pub fn decode_literal(&mut self) -> Result<Vec<u8>, HpackError> {
        self.decode_literal_with_prefix(7)
    }

    pub fn decode_literal_with_prefix(&mut self, nbits: u8) -> Result<Vec<u8>, HpackError> {
        let (huffman, encoded) = self.take_literal(nbits)?;
        if huffman {
            huffman::decode(&encoded)
        } else {
            Ok(encoded.into_owned())
        }
    }

    /// Decodes a literal without copying where it can: a raw literal inside one input segment
    /// borrows the input, and a Huffman literal whose decoded bound fits `scratch` decodes into it.
    pub(super) fn decode_literal_with_scratch<'s>(
        &mut self,
        scratch: &'s mut [u8],
    ) -> Result<Cow<'s, [u8]>, HpackError>
    where
        'a: 's,
    {
        let (huffman, encoded) = self.take_literal(7)?;
        if !huffman {
            #[cfg(test)]
            if matches!(encoded, Cow::Owned(_)) {
                OWNED_LITERAL_COPIES.set(OWNED_LITERAL_COPIES.get() + 1);
            }
            return Ok(encoded);
        }
        if huffman::max_decoded_len(encoded.len()) > scratch.len() {
            #[cfg(test)]
            HUFFMAN_HEAP_DECODES.set(HUFFMAN_HEAP_DECODES.get() + 1);
            return huffman::decode(&encoded).map(Cow::Owned);
        }
        let length = huffman::decode_to_slice(&encoded, scratch)?;
        Ok(Cow::Borrowed(&scratch[..length]))
    }

    /// Returns how many raw literals, names or values, this thread has copied into an owned buffer
    /// because they span input segments, since the last call.
    #[cfg(test)]
    pub(crate) fn take_owned_literal_copies() -> usize {
        OWNED_LITERAL_COPIES.take()
    }

    /// Returns how many Huffman literals this thread has decoded into a heap buffer because they do
    /// not fit the decoder's scratch, since the last call.
    #[cfg(test)]
    pub(crate) fn take_huffman_heap_decodes() -> usize {
        HUFFMAN_HEAP_DECODES.take()
    }

    /// Returns how many times this thread has entered the segment-advancing slow path since the
    /// last call.
    #[cfg(test)]
    pub(crate) fn take_slow_path_entries() -> usize {
        SLOW_PATH_ENTRIES.take()
    }

    fn take_literal(&mut self, nbits: u8) -> Result<(bool, Cow<'a, [u8]>), HpackError> {
        if !(1..=7).contains(&nbits) {
            return Err(HpackError::InvalidEncoding);
        }
        let first = self.read_byte()?;
        let huffman = first & (1 << nbits) != 0;
        let size =
            usize::try_from(self.decode_integer_from(first, NBIT_MASKS[usize::from(nbits)])?)
                .map_err(|_| HpackError::IntegerOverflow)?;
        if size > self.remaining {
            return Err(HpackError::BufferUnderflow);
        }
        if size > self.max_literal_size {
            return Err(HpackError::LiteralTooLarge);
        }
        let encoded = self.take(size)?;
        Ok((huffman, encoded))
    }

    fn take(&mut self, length: usize) -> Result<Cow<'a, [u8]>, HpackError> {
        if length > self.remaining {
            return Err(HpackError::BufferUnderflow);
        }
        if length == 0 {
            return Ok(Cow::Borrowed(&[]));
        }
        if self.offset == self.segment.len() {
            self.skip_empty_segments();
        }
        if length <= self.segment.len() - self.offset {
            let bytes = &self.segment[self.offset..self.offset + length];
            self.offset += length;
            self.remaining -= length;
            return Ok(Cow::Borrowed(bytes));
        }
        (0..length)
            .map(|_| self.read_byte())
            .collect::<Result<Vec<_>, _>>()
            .map(Cow::Owned)
    }

    #[cold]
    #[inline(never)]
    fn skip_empty_segments(&mut self) {
        #[cfg(test)]
        SLOW_PATH_ENTRIES.set(SLOW_PATH_ENTRIES.get() + 1);
        while self.offset == self.segment.len() {
            let Some(segment) = self.segments.next() else {
                break;
            };
            self.segment = segment;
            self.offset = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::HpackDecodeBuffer;
    use super::LITERAL_SCRATCH;

    #[test]
    fn raw_literal_in_one_segment_borrows_the_input() {
        let input = [0x05, b'h', b'e', b'l', b'l', b'o'];
        let mut buffer = HpackDecodeBuffer::new(&input, 4096);
        let mut scratch = [0; LITERAL_SCRATCH];

        let literal = buffer.decode_literal_with_scratch(&mut scratch).unwrap();

        let Cow::Borrowed(value) = literal else {
            panic!("a raw literal inside one segment was copied: {literal:?}");
        };
        assert_eq!(value, b"hello");
        assert_eq!(value.as_ptr(), input[1..].as_ptr());
        assert!(buffer.is_empty());
    }
}
