/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::VecDeque;

use bytes::BufMut;

use crate::DecodeError;
use crate::EncoderStreamError;
use crate::FieldLine;
use crate::StaticRef;
use crate::WireError;
use crate::instructions::DecoderInstruction;
use crate::integer;
use crate::static_table::STATIC_TABLE;
use crate::string::StringLiteral;

/// A field section decoder. [`Decoder::new`] is for an endpoint that advertises a maximum dynamic
/// table capacity of 0, so every dynamic table reference is an error. [`Decoder::with_max_capacity`]
/// is for one that advertises a larger maximum and no blocked streams: it keeps the dynamic table
/// that the peer's encoder stream builds and collects the decoder stream instructions that
/// acknowledge it. Field sections reuse its Huffman scratch buffers, and an insert decodes its
/// strings straight into its entry.
#[derive(Debug, Default)]
pub struct Decoder {
    name: Vec<u8>,
    value: Vec<u8>,
    max: u64,
    capacity: u64,
    size: u64,
    entries: VecDeque<Entry>,
    inserts: u64,
    /// The Known Received Count that the decoder stream instructions establish.
    known_received: u64,
    /// The encoded Section Acknowledgments and Stream Cancellations not yet written.
    owed: Vec<u8>,
}

/// A dynamic table entry: its name and value in one allocation, and the static entry its name
/// came from. The allocation may end in room that a Huffman decoding did not use.
#[derive(Debug)]
struct Entry {
    bytes: Box<[u8]>,
    name_len: usize,
    len: usize,
    static_name: Option<u8>,
}

impl Decoder {
    /// Creates a decoder for an endpoint that advertises a maximum dynamic table capacity of 0.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a decoder for an endpoint that advertises `max` as SETTINGS_QPACK_MAX_TABLE_CAPACITY
    /// and 0 as SETTINGS_QPACK_BLOCKED_STREAMS. The table's capacity starts at 0, and the peer's
    /// Set Dynamic Table Capacity may raise it up to `max`. With `max` 0 it is [`Self::new`].
    pub fn with_max_capacity(max: u64) -> Self {
        Self {
            max,
            ..Self::default()
        }
    }

    /// Decodes the complete encoded field section `block` of stream `stream_id`, passing each
    /// line and its [`StaticRef`] to `on_field` as [`Self::decode_resolved_section`] does. A line
    /// from the dynamic table passes [`StaticRef::Name`] if its entry's name came from the static
    /// table, and otherwise [`StaticRef::Unresolved`]. A section with a non-zero Required Insert
    /// Count adds a Section Acknowledgment to the instructions that [`Self::write_owed`] writes.
    /// A section whose Required Insert Count exceeds the entries received would block, which is
    /// [`DecodeError::Blocked`].
    pub fn decode_section(
        &mut self,
        stream_id: u64,
        block: &[u8],
        on_field: impl FnMut(FieldLine<'_>, StaticRef),
    ) -> Result<(), DecodeError> {
        let required = self.decode(block, true, on_field)?;
        if required != 0 {
            DecoderInstruction::SectionAcknowledgment(stream_id).encode(&mut self.owed);
            self.known_received = self.known_received.max(required);
        }
        Ok(())
    }

    /// Records that stream `stream_id` was reset, or its reading abandoned, before all its field
    /// sections were decoded. That adds a Stream Cancellation to the instructions that
    /// [`Self::write_owed`] writes, unless the advertised maximum capacity is 0, which leaves the
    /// encoder no references to release.
    pub fn cancel_stream(&mut self, stream_id: u64) {
        if self.max > 0 {
            DecoderInstruction::StreamCancellation(stream_id).encode(&mut self.owed);
        }
    }

    /// Returns whether [`Self::write_owed`] would write anything.
    pub fn has_owed(&self) -> bool {
        !self.owed.is_empty() || self.known_received < self.inserts
    }

    /// Returns the number of bytes that [`Self::write_owed`] would write.
    pub fn owed_len(&self) -> usize {
        let increment = self.inserts - self.known_received;
        let increment_len = if increment == 0 {
            0
        } else {
            integer::encoded_len(6, increment)
        };
        self.owed.len() + increment_len
    }

    /// Writes the decoder stream instructions not yet written: the Section Acknowledgments and
    /// Stream Cancellations in the order they arose, then one Insert Count Increment for the
    /// inserts that the acknowledgments do not cover.
    pub fn write_owed(&mut self, dst: &mut impl BufMut) {
        dst.put_slice(&self.owed);
        self.owed.clear();
        if self.known_received < self.inserts {
            let increment = self.inserts - self.known_received;
            DecoderInstruction::InsertCountIncrement(increment).encode(dst);
            self.known_received = self.inserts;
        }
    }

    /// Returns the size of the dynamic table: each entry's name and value lengths plus 32.
    pub fn table_size(&self) -> u64 {
        self.size
    }

    /// Returns the number of entries in the dynamic table.
    pub fn table_len(&self) -> usize {
        self.entries.len()
    }

    /// Returns the number of entries inserted into the dynamic table, including evicted ones.
    pub fn insert_count(&self) -> u64 {
        self.inserts
    }

    /// Decodes the complete encoded field section `block`, passing each field line to `on_field`
    /// in order. On error, the lines already passed belong to an invalid section. The section may
    /// not use the dynamic table: since this method does not acknowledge sections, a non-zero
    /// Required Insert Count is [`DecodeError::RequiredInsertCount`].
    pub fn decode_field_section(
        &mut self,
        block: &[u8],
        mut on_field: impl FnMut(FieldLine<'_>),
    ) -> Result<(), DecodeError> {
        self.decode_resolved_section(block, |line, _| on_field(line))
    }

    /// Decodes like [`Self::decode_field_section`], and also passes the static table entry that
    /// each line's name, or name and value, came from.
    pub fn decode_resolved_section(
        &mut self,
        block: &[u8],
        on_field: impl FnMut(FieldLine<'_>, StaticRef),
    ) -> Result<(), DecodeError> {
        self.decode(block, false, on_field).map(drop)
    }

    /// Decodes a field section, which may use the dynamic table only if `dynamic`, and returns
    /// its Required Insert Count.
    fn decode(
        &mut self,
        block: &[u8],
        dynamic: bool,
        mut on_field: impl FnMut(FieldLine<'_>, StaticRef),
    ) -> Result<u64, DecodeError> {
        let mut input = Input(block);
        let encoded = input.integer(8)?;
        let required = match encoded {
            0 => 0,
            _ if dynamic => required_insert_count(encoded, self.max / ENTRY_OVERHEAD, self.inserts)
                .ok_or(DecodeError::RequiredInsertCount(encoded))?,
            _ => return Err(DecodeError::RequiredInsertCount(encoded)),
        };
        // With no blocked streams allowed, a section must not wait for an insert.
        if required > self.inserts {
            return Err(DecodeError::Blocked {
                required,
                inserted: self.inserts,
            });
        }
        // A set sign bit means Base = Required Insert Count - Delta Base - 1.
        let sign = input.first().is_some_and(|b| b & 0b1000_0000 != 0);
        let delta = input.integer(7)?;
        let base = if sign {
            required
                .checked_sub(delta + 1)
                .ok_or(DecodeError::NegativeBase)?
        } else {
            required + delta
        };
        // A line may use the entries below the Required Insert Count that are still in the table.
        let dropped = self.inserts - self.entries.len() as u64;
        let entries = &self.entries;
        let entry = |absolute: Option<u64>| {
            absolute
                .filter(|&a| a < required)
                .and_then(|a| a.checked_sub(dropped))
                .and_then(|i| entries.get(usize::try_from(i).ok()?))
                .ok_or(DecodeError::DynamicTableReference)
        };
        while let Some(first) = input.first() {
            // Without a Required Insert Count, a dynamic reference fails before its index is read.
            let dynamic = first & 0b1100_0000 == 0b1000_0000
                || first & 0b1101_0000 == 0b0100_0000
                || first & 0b1110_0000 == 0;
            if dynamic && required == 0 {
                return Err(DecodeError::DynamicTableReference);
            }
            let (line, static_ref) = if first & 0b1000_0000 != 0 {
                // Indexed Field Line: 1, T, then a 6-bit prefix index.
                let index = input.integer(6)?;
                if first & 0b0100_0000 == 0 {
                    entry(base.checked_sub(index + 1))?.line()
                } else {
                    let index = static_index(index)?;
                    let (name, value) = STATIC_TABLE[usize::from(index)];
                    let line = FieldLine {
                        name,
                        value,
                        never_indexed: false,
                    };
                    (line, StaticRef::Entry(index))
                }
            } else if first & 0b0100_0000 != 0 {
                // Literal Field Line with Name Reference: 01, N, T, then a 4-bit prefix index.
                let index = input.integer(4)?;
                let (name, static_ref) = if first & 0b0001_0000 == 0 {
                    let (line, static_ref) = entry(base.checked_sub(index + 1))?.line();
                    (line.name, static_ref)
                } else {
                    let index = static_index(index)?;
                    (STATIC_TABLE[usize::from(index)].0, StaticRef::Name(index))
                };
                let line = FieldLine {
                    name,
                    value: text(input.string(8)?, &mut self.value)?,
                    never_indexed: first & 0b0010_0000 != 0,
                };
                (line, static_ref)
            } else if first & 0b0010_0000 != 0 {
                // Literal Field Line with Literal Name: 001, N, then a 4-bit prefix name string.
                let name = input.string(4)?;
                let value = input.string(8)?;
                let line = FieldLine {
                    name: text(name, &mut self.name)?,
                    value: text(value, &mut self.value)?,
                    never_indexed: first & 0b0001_0000 != 0,
                };
                (line, StaticRef::Unresolved)
            } else if first & 0b0001_0000 != 0 {
                // Indexed Field Line with Post-Base Index: 0001, then a 4-bit prefix index.
                entry(base.checked_add(input.integer(4)?))?.line()
            } else {
                // Literal Field Line with Post-Base Name Reference: 0000, N, a 3-bit prefix index.
                let (line, static_ref) = entry(base.checked_add(input.integer(3)?))?.line();
                let line = FieldLine {
                    name: line.name,
                    value: text(input.string(8)?, &mut self.value)?,
                    never_indexed: first & 0b0000_1000 != 0,
                };
                (line, static_ref)
            };
            on_field(line, static_ref);
        }
        Ok(required)
    }

    /// Processes bytes from the peer's encoder stream and returns how many were consumed. The
    /// caller keeps the rest, a partial instruction, and passes it again with the next bytes. An
    /// insert fails as soon as its length prefixes show that its entry cannot fit the capacity,
    /// so a partial instruction holds at most about four capacities of bytes. While the capacity
    /// is below 32 bytes, every insert fails on its first byte.
    pub fn feed_encoder_stream(&mut self, buf: &[u8]) -> Result<usize, EncoderStreamError> {
        let mut used = 0;
        while used < buf.len() {
            match self.encoder_instruction(&buf[used..])? {
                Some(len) => used += len,
                None => break,
            }
        }
        Ok(used)
    }

    /// Applies the encoder stream instruction at the start of `buf` and returns its length, or
    /// `None` if `buf` holds only part of it.
    fn encoder_instruction(&mut self, buf: &[u8]) -> Result<Option<usize>, EncoderStreamError> {
        let first = buf[0];
        if first & 0b1100_0000 != 0 {
            // While the capacity is below the smallest entry size, an insert fails on its first
            // byte.
            if self.capacity < ENTRY_OVERHEAD {
                return Err(EncoderStreamError::Insert);
            }
            return if first & 0b1000_0000 != 0 {
                self.insert_with_name_reference(buf)
            } else {
                self.insert_with_literal_name(buf)
            };
        }
        if first & 0b0010_0000 != 0 {
            let Some((capacity, len)) = stream_integer(buf, 5)? else {
                return Ok(None);
            };
            if capacity > self.max {
                return Err(EncoderStreamError::CapacityExceeded(capacity));
            }
            self.set_capacity(capacity);
            return Ok(Some(len));
        }
        if self.entries.is_empty() {
            return Err(EncoderStreamError::Duplicate);
        }
        let Some((relative, len)) = stream_integer(buf, 5)? else {
            return Ok(None);
        };
        let copy = relative_entry(&self.entries, relative)?.copy();
        self.insert(copy);
        Ok(Some(len))
    }

    /// Insert with Name Reference: 1, T, a 6-bit prefix name index, then the value.
    fn insert_with_name_reference(
        &mut self,
        buf: &[u8],
    ) -> Result<Option<usize>, EncoderStreamError> {
        let Some((index, used)) = stream_integer(buf, 6)? else {
            return Ok(None);
        };
        let (name, static_name) = if buf[0] & 0b0100_0000 != 0 {
            let (name, _) = usize::try_from(index)
                .ok()
                .and_then(|i| STATIC_TABLE.get(i))
                .ok_or(EncoderStreamError::InvalidStaticIndex(index))?;
            (*name, Some(index as u8))
        } else {
            let entry = relative_entry(&self.entries, index)?;
            (entry.name(), entry.static_name)
        };
        let name = StringLiteral {
            huffman: false,
            data: name,
        };
        let capacity = self.capacity;
        let Some((value, len)) = insert_string(&buf[used..], 8, name.data.len() as u64, capacity)?
        else {
            return Ok(None);
        };
        let entry = Entry::decode(name, value, static_name, capacity)?;
        self.insert(entry);
        Ok(Some(used + len))
    }

    /// Insert with Literal Name: 01, then a 6-bit prefix name string and the value.
    fn insert_with_literal_name(
        &mut self,
        buf: &[u8],
    ) -> Result<Option<usize>, EncoderStreamError> {
        let capacity = self.capacity;
        let Some((name, used)) = insert_string(buf, 6, 0, capacity)? else {
            return Ok(None);
        };
        // The name is decoded only with the value, so until then it counts at its fewest
        // decoded bytes.
        let least = least_decoded_len(name.huffman, name.data.len() as u64);
        let Some((value, len)) = insert_string(&buf[used..], 8, least, capacity)? else {
            return Ok(None);
        };
        let entry = Entry::decode(name, value, None, capacity)?;
        self.insert(entry);
        Ok(Some(used + len))
    }

    fn set_capacity(&mut self, capacity: u64) {
        self.capacity = capacity;
        self.evict_to(capacity);
        // The table never holds more than capacity / 32 entries, so inserts do not grow it.
        let most = usize::try_from(capacity / ENTRY_OVERHEAD).unwrap_or(usize::MAX);
        if let Some(more) = most.checked_sub(self.entries.len())
            && self.entries.capacity() < most
        {
            self.entries.reserve_exact(more);
        }
    }

    /// Adds an entry no larger than the capacity, evicting the oldest entries to make room.
    fn insert(&mut self, entry: Entry) {
        let size = entry.size();
        self.evict_to(self.capacity - size);
        self.size += size;
        self.entries.push_back(entry);
        self.inserts += 1;
    }

    fn evict_to(&mut self, size: u64) {
        while self.size > size {
            let evicted = self
                .entries
                .pop_front()
                .expect("a table with a positive size has entries");
            self.size -= evicted.size();
        }
    }
}

/// The bytes an entry takes beyond its name and value.
const ENTRY_OVERHEAD: u64 = 32;

/// Returns the Required Insert Count that the non-zero `encoded` stands for, given the table's
/// MaxEntries and the inserts received, or `None` if no count encodes to it (RFC 9204).
fn required_insert_count(encoded: u64, max_entries: u64, inserts: u64) -> Option<u64> {
    let full_range = 2 * max_entries;
    if encoded > full_range {
        return None;
    }
    let max_value = inserts + max_entries;
    let max_wrapped = max_value / full_range * full_range;
    let mut required = max_wrapped + encoded - 1;
    if required > max_value {
        // The encoder's count wrapped one time fewer.
        required = required.checked_sub(full_range).filter(|&r| r > 0)?;
    }
    (required != 0).then_some(required)
}

impl Entry {
    /// Builds the entry of an insert in one allocation, with room for the longest decoding of
    /// each string, and decodes Huffman-coded strings straight into it. Fails if the entry does
    /// not fit `capacity`.
    fn decode(
        name: StringLiteral<'_>,
        value: StringLiteral<'_>,
        static_name: Option<u8>,
        capacity: u64,
    ) -> Result<Self, EncoderStreamError> {
        let mut bytes = vec![0; room(name) + room(value)].into_boxed_slice();
        let name_len = put(name, &mut bytes)?;
        let len = name_len + put(value, &mut bytes[name_len..])?;
        let size = len as u64 + ENTRY_OVERHEAD;
        if size > capacity {
            return Err(EncoderStreamError::EntryTooLarge { size, capacity });
        }
        Ok(Self {
            bytes,
            name_len,
            len,
            static_name,
        })
    }

    /// Returns a copy of the entry, without the room its strings did not use.
    fn copy(&self) -> Self {
        Self {
            bytes: self.bytes[..self.len].into(),
            name_len: self.name_len,
            len: self.len,
            static_name: self.static_name,
        }
    }

    fn name(&self) -> &[u8] {
        &self.bytes[..self.name_len]
    }

    fn value(&self) -> &[u8] {
        &self.bytes[self.name_len..self.len]
    }

    fn size(&self) -> u64 {
        self.len as u64 + ENTRY_OVERHEAD
    }

    /// Returns the entry as an indexed field line. A line from the dynamic table reports the
    /// static entry its name came from, but never a static entry for its name and value, so that
    /// the caller checks its value as a literal's.
    fn line(&self) -> (FieldLine<'_>, StaticRef) {
        let line = FieldLine {
            name: self.name(),
            value: self.value(),
            never_indexed: false,
        };
        (
            line,
            self.static_name
                .map_or(StaticRef::Unresolved, StaticRef::Name),
        )
    }
}

/// Returns the entry `relative` places before the newest.
fn relative_entry(entries: &VecDeque<Entry>, relative: u64) -> Result<&Entry, EncoderStreamError> {
    usize::try_from(relative)
        .ok()
        .and_then(|r| entries.len().checked_sub(r.checked_add(1)?))
        .map(|i| &entries[i])
        .ok_or(EncoderStreamError::InvalidRelativeIndex(relative))
}

/// Reads an encoder stream integer, or `None` if `buf` ends inside it.
fn stream_integer(buf: &[u8], prefix_bits: u8) -> Result<Option<(u64, usize)>, EncoderStreamError> {
    match integer::decode(buf, prefix_bits) {
        Ok(decoded) => Ok(Some(decoded)),
        Err(WireError::Truncated) => Ok(None),
        Err(_) => Err(EncoderStreamError::IntegerTooLarge),
    }
}

/// Reads a string literal of an insert whose other strings take `before` bytes, failing as soon
/// as its length shows that the entry cannot fit `capacity`. Returns `None` if `buf` ends inside
/// the literal.
fn insert_string(
    buf: &[u8],
    prefix_bits: u8,
    before: u64,
    capacity: u64,
) -> Result<Option<(StringLiteral<'_>, usize)>, EncoderStreamError> {
    let Some(&first) = buf.first() else {
        return Ok(None);
    };
    let Some((len, used)) = stream_integer(buf, prefix_bits - 1)? else {
        return Ok(None);
    };
    let huffman = first & (1 << (prefix_bits - 1)) != 0;
    let least = least_decoded_len(huffman, len);
    let size = before.saturating_add(least).saturating_add(ENTRY_OVERHEAD);
    if size > capacity {
        return Err(EncoderStreamError::EntryTooLarge { size, capacity });
    }
    let end = used + len as usize;
    if buf.len() < end {
        return Ok(None);
    }
    let literal = StringLiteral {
        huffman,
        data: &buf[used..end],
    };
    Ok(Some((literal, end)))
}

/// Returns the fewest bytes a string literal of `len` bytes decodes to. The longest Huffman code
/// is 30 bits and padding is shorter than 8, so n coded bytes hold at least (8n - 7) / 30
/// symbols, rounded up.
fn least_decoded_len(huffman: bool, len: u64) -> u64 {
    if huffman {
        len.saturating_mul(8).saturating_sub(7).div_ceil(30)
    } else {
        len
    }
}

/// Returns the most bytes that decoding `literal` writes.
fn room(literal: StringLiteral<'_>) -> usize {
    if literal.huffman {
        roxygen_huffman::max_decoded_len(literal.data.len())
    } else {
        literal.data.len()
    }
}

/// Writes the bytes of `literal` at the front of `out`, which holds at least [`room`] bytes, and
/// returns their length.
fn put(literal: StringLiteral<'_>, out: &mut [u8]) -> Result<usize, EncoderStreamError> {
    if !literal.huffman {
        out[..literal.data.len()].copy_from_slice(literal.data);
        return Ok(literal.data.len());
    }
    roxygen_huffman::decode_to_slice(literal.data, out)
        .map_err(|_| EncoderStreamError::InvalidHuffman)
}

/// The unread part of a field section.
struct Input<'a>(&'a [u8]);

impl<'a> Input<'a> {
    fn first(&self) -> Option<u8> {
        self.0.first().copied()
    }

    fn integer(&mut self, prefix_bits: u8) -> Result<u64, WireError> {
        let (value, used) = integer::decode(self.0, prefix_bits)?;
        self.0 = &self.0[used..];
        Ok(value)
    }

    fn string(&mut self, prefix_bits: u8) -> Result<StringLiteral<'a>, WireError> {
        let (literal, used) = StringLiteral::parse(self.0, prefix_bits)?;
        self.0 = &self.0[used..];
        Ok(literal)
    }
}

/// Returns `index` if it names a static table entry.
fn static_index(index: u64) -> Result<u8, DecodeError> {
    u8::try_from(index)
        .ok()
        .filter(|&i| usize::from(i) < STATIC_TABLE.len())
        .ok_or(DecodeError::InvalidStaticIndex(index))
}

/// Returns the bytes of `literal`: its own data when raw, else its decoding in `scratch`, which
/// keeps its length between calls so that it is zeroed only when it grows.
fn text<'a>(literal: StringLiteral<'a>, scratch: &'a mut Vec<u8>) -> Result<&'a [u8], DecodeError> {
    if !literal.huffman {
        return Ok(literal.data);
    }
    #[cfg(test)]
    HUFFMAN_INPUT.with(|count| count.set(count.get() + literal.data.len()));
    let bound = roxygen_huffman::max_decoded_len(literal.data.len());
    if scratch.len() < bound {
        scratch.resize(bound, 0);
    }
    let len = roxygen_huffman::decode_to_slice(literal.data, scratch).map_err(WireError::from)?;
    Ok(&scratch[..len])
}

#[cfg(test)]
std::thread_local! {
    /// The Huffman-coded bytes that the text path decoded on this thread.
    static HUFFMAN_INPUT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Encoder;
    use crate::instructions::EncoderInstruction;
    use crate::test_util::count_allocations;
    use crate::test_util::hex;

    /// Returns the dynamic table's entries, oldest first.
    fn table(decoder: &Decoder) -> Vec<(Vec<u8>, Vec<u8>)> {
        decoder
            .entries
            .iter()
            .map(|e| (e.name().to_vec(), e.value().to_vec()))
            .collect()
    }

    fn raw(data: &[u8]) -> StringLiteral<'_> {
        StringLiteral {
            huffman: false,
            data,
        }
    }

    /// Returns the encoder stream bytes of `instructions`.
    fn stream(instructions: &[EncoderInstruction<'_>]) -> Vec<u8> {
        let mut out = Vec::new();
        for instruction in instructions {
            instruction.encode(&mut out);
        }
        out
    }

    fn capacity(capacity: u64) -> Vec<u8> {
        stream(&[EncoderInstruction::SetDynamicTableCapacity(capacity)])
    }

    fn literal_insert(name: &[u8], value: &[u8]) -> Vec<u8> {
        stream(&[EncoderInstruction::InsertWithLiteralName {
            name: raw(name),
            value: raw(value),
        }])
    }

    /// Feeds `bytes` as the session does: an incomplete instruction stays for the next feed.
    fn feed_all(decoder: &mut Decoder, bytes: &[u8]) -> Result<(), EncoderStreamError> {
        let used = decoder.feed_encoder_stream(bytes)?;
        assert_eq!(used, bytes.len(), "the stream holds whole instructions");
        Ok(())
    }

    #[test]
    fn processes_the_encoder_stream() {
        use EncoderStreamError::*;
        // The capacity may rise to the advertised maximum and no further.
        let mut decoder = Decoder::with_max_capacity(4096);
        assert_eq!(feed_all(&mut decoder, &capacity(4096)), Ok(()));
        assert_eq!(
            decoder.feed_encoder_stream(&capacity(4097)),
            Err(CapacityExceeded(4097))
        );

        // An entry is its name and value plus 32 bytes, and must fit the capacity.
        let mut decoder = Decoder::with_max_capacity(4096);
        feed_all(&mut decoder, &capacity(100)).unwrap();
        let (name, value) = ([b'n'; 30], [b'v'; 38]);
        assert_eq!(
            feed_all(&mut decoder, &literal_insert(&name, &value)),
            Ok(())
        );
        assert_eq!(decoder.table_size(), 100);
        assert_eq!(
            decoder.feed_encoder_stream(&literal_insert(&name, &[b'v'; 39])),
            Err(EntryTooLarge {
                size: 101,
                capacity: 100
            })
        );

        // Inserts evict the oldest entries; then a Duplicate or name reference of an evicted entry
        // is an error, as is one past the table.
        let mut decoder = Decoder::with_max_capacity(4096);
        feed_all(&mut decoder, &capacity(100)).unwrap();
        for (name, value) in [
            (b"aaaaaaaa", b"1"),
            (b"bbbbbbbb", b"2"),
            (b"cccccccc", b"3"),
        ] {
            feed_all(&mut decoder, &literal_insert(name, value)).unwrap();
        }
        assert_eq!(
            table(&decoder),
            vec![
                (b"bbbbbbbb".to_vec(), b"2".to_vec()),
                (b"cccccccc".to_vec(), b"3".to_vec())
            ]
        );
        assert_eq!((decoder.table_size(), decoder.insert_count()), (82, 3));
        for relative in [2, 9] {
            assert_eq!(
                decoder.feed_encoder_stream(&stream(&[EncoderInstruction::Duplicate(relative)])),
                Err(InvalidRelativeIndex(relative))
            );
            let insert = EncoderInstruction::InsertWithNameReference {
                static_table: false,
                name_index: relative,
                value: raw(b"x"),
            };
            assert_eq!(
                decoder.feed_encoder_stream(&stream(&[insert])),
                Err(InvalidRelativeIndex(relative))
            );
        }
        let insert = EncoderInstruction::InsertWithNameReference {
            static_table: true,
            name_index: 99,
            value: raw(b"x"),
        };
        assert_eq!(
            decoder.feed_encoder_stream(&stream(&[insert])),
            Err(InvalidStaticIndex(99))
        );
        // Lowering the capacity evicts too.
        feed_all(&mut decoder, &capacity(41)).unwrap();
        assert_eq!(table(&decoder), vec![(b"cccccccc".to_vec(), b"3".to_vec())]);
        assert_eq!(decoder.table_size(), 41);

        // Every instruction kind, split at every byte boundary, builds the same table.
        let huffman_value = {
            let mut coded = Vec::new();
            crate::string::encode(&mut coded, 0, 8, b"localhost:8443", true);
            coded
        };
        let mut whole = capacity(4096);
        whole.extend(stream(&[EncoderInstruction::InsertWithNameReference {
            static_table: true,
            name_index: 0,
            value: StringLiteral::parse(&huffman_value, 8).unwrap().0,
        }]));
        whole.extend(literal_insert(b"custom-key", b"custom-value"));
        whole.extend(stream(&[
            EncoderInstruction::Duplicate(1),
            EncoderInstruction::InsertWithNameReference {
                static_table: false,
                name_index: 1,
                value: raw(b"custom-value2"),
            },
        ]));
        let mut reference = Decoder::with_max_capacity(4096);
        feed_all(&mut reference, &whole).unwrap();
        let expected = vec![
            (b":authority".to_vec(), b"localhost:8443".to_vec()),
            (b"custom-key".to_vec(), b"custom-value".to_vec()),
            (b":authority".to_vec(), b"localhost:8443".to_vec()),
            (b"custom-key".to_vec(), b"custom-value2".to_vec()),
        ];
        assert_eq!(table(&reference), expected);
        for cut in 0..=whole.len() {
            let mut decoder = Decoder::with_max_capacity(4096);
            let mut pending = whole[..cut].to_vec();
            let used = decoder.feed_encoder_stream(&pending).unwrap();
            pending.drain(..used);
            pending.extend_from_slice(&whole[cut..]);
            assert_eq!(decoder.feed_encoder_stream(&pending), Ok(pending.len()));
            assert_eq!(table(&decoder), expected, "cut at {cut}");
            assert_eq!(decoder.insert_count(), 4, "cut at {cut}");
        }

        // An insert fails once its length prefixes prove that it cannot fit, before its bytes
        // arrive. A Huffman string of n bytes decodes to at least (8n - 7) / 30 bytes, rounded up.
        let mut decoder = Decoder::with_max_capacity(4096);
        feed_all(&mut decoder, &capacity(100)).unwrap();
        // A literal name "x", then a Huffman value whose length allows 68 or 67 bytes.
        assert_eq!(
            decoder.feed_encoder_stream(&hex("41 78 ff 7e")),
            Err(EntryTooLarge {
                size: 101,
                capacity: 100
            })
        );
        assert_eq!(decoder.feed_encoder_stream(&hex("41 78 ff 7d")), Ok(0));
        // A Huffman name whose length alone allows 69 or 68 bytes.
        assert_eq!(
            decoder.feed_encoder_stream(&hex("7f e1 01")),
            Err(EntryTooLarge {
                size: 101,
                capacity: 100
            })
        );
        assert_eq!(decoder.feed_encoder_stream(&hex("7f e0 01")), Ok(0));
        // Four Huffman bytes may hold one 30-bit code: a line feed fits a 33-byte capacity.
        let mut decoder = Decoder::with_max_capacity(4096);
        feed_all(&mut decoder, &capacity(33)).unwrap();
        let mut line_feed = hex("40 84");
        crate::huffman::encode(b"\n", &mut line_feed);
        assert_eq!(line_feed.len(), 6);
        assert_eq!(feed_all(&mut decoder, &line_feed), Ok(()));
        assert_eq!(table(&decoder), vec![(Vec::new(), b"\n".to_vec())]);

        // Malformed integers and Huffman strings.
        assert_eq!(
            decoder.feed_encoder_stream(&hex("3fff ffff ffff ffff ffff 01")),
            Err(IntegerTooLarge)
        );
        assert_eq!(
            decoder.feed_encoder_stream(&hex("40 81 ff")),
            Err(InvalidHuffman)
        );

        // Until the capacity reaches 32, inserts fail on their first byte; on an empty table a
        // Duplicate does too.
        let mut decoder = Decoder::with_max_capacity(4096);
        for first in [0xc0, 0x80, 0x4a, 0x60] {
            assert_eq!(
                decoder.feed_encoder_stream(&[first]),
                Err(Insert),
                "{first:x}"
            );
        }
        feed_all(&mut decoder, &capacity(31)).unwrap();
        assert_eq!(decoder.feed_encoder_stream(&[0x40]), Err(Insert));
        assert_eq!(decoder.feed_encoder_stream(&[0x00]), Err(Duplicate));
        feed_all(&mut decoder, &capacity(32)).unwrap();
        assert_eq!(decoder.feed_encoder_stream(&[0x40]), Ok(0));
        assert_eq!(decoder.feed_encoder_stream(&[0x00]), Err(Duplicate));
    }

    type Lines = Vec<(Vec<u8>, Vec<u8>, bool)>;

    fn decode(block: &[u8]) -> Result<Lines, DecodeError> {
        let mut lines = Vec::new();
        Decoder::new().decode_field_section(block, |line| {
            lines.push((line.name.to_vec(), line.value.to_vec(), line.never_indexed));
        })?;
        Ok(lines)
    }

    fn line(name: &str, value: &str, never_indexed: bool) -> (Vec<u8>, Vec<u8>, bool) {
        (name.into(), value.into(), never_indexed)
    }

    #[test]
    fn decodes_rfc9204_b1() {
        let block = hex("0000 510b 2f69 6e64 6578 2e68 746d 6c");
        assert_eq!(
            decode(&block),
            Ok(vec![line(":path", "/index.html", false)])
        );
    }

    #[test]
    fn decodes_indexed_static_lines() {
        let expected = vec![
            line(":method", "GET", false),
            line(":status", "200", false),
            line("x-frame-options", "sameorigin", false),
            line(":authority", "", false),
        ];
        assert_eq!(decode(&hex("0000 d1d9 ff23 c0")), Ok(expected));
    }

    #[test]
    fn decodes_literal_lines_in_both_codings() {
        let huffman = hex(
            "0000 5188 60d5 485f 2bce 9a68 2f01 25a8 49e9 5ba9 7d7f 8925 a849 e95b b8e8 b4bf 2261 6200",
        );
        let expected = vec![
            line(":path", "/index.html", false),
            line("custom-key", "custom-value", false),
            line("ab", "", false),
        ];
        assert_eq!(decode(&huffman), Ok(expected));
        let never_indexed = [
            &hex("0000 7f45 06")[..],
            b"secret",
            &hex("7f02 03"),
            b"GET",
            &hex("3701"),
            b"x-secret",
            &[0x01],
            b"v",
        ]
        .concat();
        let expected = vec![
            line("authorization", "secret", true),
            line(":method", "GET", true),
            line("x-secret", "v", true),
        ];
        assert_eq!(decode(&never_indexed), Ok(expected));
    }

    #[test]
    fn passes_the_static_entry_of_each_line() {
        for index in 0..99u8 {
            let (name, value) = STATIC_TABLE[usize::from(index)];
            // An Indexed Field Line; Literal Field Lines with a Name Reference, a raw value and a
            // Huffman-coded "a", the second with N; then a Literal Field Line with Literal Name.
            let mut block = hex("0000");
            integer::encode(&mut block, 0b1100_0000, 6, index.into());
            integer::encode(&mut block, 0b0101_0000, 4, index.into());
            block.extend([0x01, b'v']);
            integer::encode(&mut block, 0b0111_0000, 4, index.into());
            block.extend([0x81, 0x1f]);
            block.extend(b"\x25:path\x01/");
            let mut lines = Vec::new();
            Decoder::new()
                .decode_resolved_section(&block, |line, static_ref| {
                    lines.push((line.name.to_vec(), line.value.to_vec(), static_ref));
                })
                .unwrap();
            let expected = vec![
                (name.to_vec(), value.to_vec(), StaticRef::Entry(index)),
                (name.to_vec(), b"v".to_vec(), StaticRef::Name(index)),
                (name.to_vec(), b"a".to_vec(), StaticRef::Name(index)),
                (b":path".to_vec(), b"/".to_vec(), StaticRef::Unresolved),
            ];
            assert_eq!(lines, expected, "{index}");
        }
    }

    #[test]
    fn rejects_dynamic_table_references() {
        for block in [
            // Indexed Field Line with T=0, then with a post-Base index.
            "0000 80",
            "0000 10",
            // Literal Field Line with a dynamic Name Reference, without and with N.
            "0000 4000",
            "0000 6000",
            // Literal Field Line with a post-Base Name Reference, without and with N.
            "0000 0000",
            "0000 0800",
            // A dynamic reference after a valid line.
            "0000 d1 81",
        ] {
            assert_eq!(
                decode(&hex(block)),
                Err(DecodeError::DynamicTableReference),
                "{block}"
            );
        }
    }

    #[test]
    fn rejects_a_nonzero_required_insert_count() {
        // The field sections of RFC 9204 B.2 and B.4, then static lines under a non-zero count.
        assert_eq!(
            decode(&hex("0381 1011")),
            Err(DecodeError::RequiredInsertCount(3))
        );
        assert_eq!(
            decode(&hex("0500 80c1 81")),
            Err(DecodeError::RequiredInsertCount(5))
        );
        assert_eq!(
            decode(&hex("0100 d1")),
            Err(DecodeError::RequiredInsertCount(1))
        );
    }

    #[test]
    fn rejects_static_indices_past_the_table() {
        // Index 99 in an Indexed Field Line and in a Name Reference, then a 40-bit index.
        assert_eq!(
            decode(&hex("0000 ff24")),
            Err(DecodeError::InvalidStaticIndex(99))
        );
        assert_eq!(
            decode(&hex("0000 5f54 00")),
            Err(DecodeError::InvalidStaticIndex(99))
        );
        let mut block = hex("0000");
        integer::encode(&mut block, 0b1100_0000, 6, 1 << 40);
        assert_eq!(
            decode(&block),
            Err(DecodeError::InvalidStaticIndex(1 << 40))
        );
        assert_eq!(
            decode(&hex("0000 5f53 00")),
            Ok(vec![line("x-frame-options", "", false)])
        );
    }

    #[test]
    fn rejects_malformed_primitives() {
        let wire = |e| Err(DecodeError::Wire(e));
        for block in [
            "",
            "00",
            "0000 51",
            "0000 510b 2f69",
            "0000 2f",
            "0000 2f01 25a8",
            "0000 ff",
            "0000 5f",
        ] {
            assert_eq!(decode(&hex(block)), wire(WireError::Truncated), "{block}");
        }
        // Ten continuation bytes after a full prefix, in an index, a Required Insert Count and a
        // Delta Base.
        for block in [
            "0000 ffff ffff ffff ffff ffff 01",
            "ffff ffff ffff ffff ffff 01",
            "00ff ffff ffff ffff ffff ff01",
        ] {
            assert_eq!(
                decode(&hex(block)),
                wire(WireError::IntegerTooLarge),
                "{block}"
            );
        }
        assert_eq!(
            decode(&hex("0000 5181 ff")),
            wire(WireError::HuffmanPadding)
        );
        assert_eq!(
            decode(&hex("0000 5184 ffff ffff")),
            wire(WireError::HuffmanEos)
        );
        assert_eq!(
            decode(&hex("0000 29ff 00")),
            wire(WireError::HuffmanPadding)
        );
    }

    /// Returns field sections that cover every representation the encoder emits.
    fn sample_sections() -> Vec<Vec<(Vec<u8>, Vec<u8>, bool)>> {
        let request = vec![
            line(":method", "GET", false),
            line(":scheme", "https", false),
            line(":authority", "www.example.com", false),
            line(":path", "/index.html?q=1", false),
            line("user-agent", "h2load nghttp2/1.64.0", false),
            line("accept", "*/*", false),
            line("cookie", "a=b", true),
        ];
        let response = vec![
            line(":status", "200", false),
            line("content-type", "text/plain", false),
            line("content-length", "1234", false),
            line("server", "roxygen", false),
            line("x-trace", "", false),
            line("set-cookie", "id=42; Secure", true),
        ];
        let full_matches = STATIC_TABLE
            .iter()
            .map(|&(n, v)| (n.to_vec(), v.to_vec(), false))
            .collect();
        let name_matches = STATIC_TABLE
            .iter()
            .map(|&(n, _)| (n.to_vec(), b"\x7f\x80".to_vec(), true))
            .collect();
        let all_bytes: Vec<u8> = (0..=255).collect();
        let unusual = vec![
            (all_bytes.clone(), all_bytes.clone(), false),
            (Vec::new(), Vec::new(), false),
            (b"x-long".to_vec(), vec![b'a'; 20_000], false),
            (vec![b'n'; 300], vec![0xff; 300], true),
        ];
        vec![
            request,
            response,
            full_matches,
            name_matches,
            unusual,
            Vec::new(),
        ]
    }

    #[test]
    fn round_trips_static_field_sections() {
        let mut decoder = Decoder::new();
        for huffman in [true, false] {
            let mut encoder = Encoder::new();
            encoder.set_huffman(huffman);
            for section in sample_sections() {
                let mut block = Vec::new();
                let lines = section.iter().map(|(n, v, never)| FieldLine {
                    name: n,
                    value: v,
                    never_indexed: *never,
                });
                encoder.encode_field_section(lines, &mut block);
                let mut decoded = Vec::new();
                decoder
                    .decode_field_section(&block, |l| {
                        decoded.push((l.name.to_vec(), l.value.to_vec(), l.never_indexed))
                    })
                    .unwrap();
                assert_eq!(decoded, section, "huffman {huffman}");
            }
        }
    }

    #[test]
    fn truncated_sections_fail_or_decode_a_prefix() {
        for section in sample_sections().into_iter().take(2) {
            let mut block = Vec::new();
            let lines = section.iter().map(|(n, v, never)| FieldLine {
                name: n,
                value: v,
                never_indexed: *never,
            });
            Encoder::new().encode_field_section(lines, &mut block);
            for cut in 0..block.len() {
                match decode(&block[..cut]) {
                    Ok(lines) => assert_eq!(lines[..], section[..lines.len()], "cut at {cut}"),
                    Err(e) => {
                        assert_eq!(e, DecodeError::Wire(WireError::Truncated), "cut at {cut}")
                    }
                }
            }
        }
    }

    #[test]
    fn checks_the_base() {
        // With a Required Insert Count of 0, a set sign bit means a negative Base.
        assert_eq!(decode(&hex("0080 d1")), Err(DecodeError::NegativeBase));
        assert_eq!(decode(&hex("00ff 05 d1")), Err(DecodeError::NegativeBase));
        // Any positive Delta Base is valid when the section does not use the dynamic table.
        assert_eq!(
            decode(&hex("007f 05 d1")),
            Ok(vec![line(":method", "GET", false)])
        );
    }

    #[test]
    fn accepts_only_a_zero_capacity_on_the_encoder_stream() {
        let feed = |buf: &[u8]| Decoder::new().feed_encoder_stream(buf);
        assert_eq!(feed(&[]), Ok(0));
        assert_eq!(feed(&[0x20]), Ok(1));
        assert_eq!(feed(&[0x20, 0x20]), Ok(2));
        // A capacity that needs continuation bytes stays unconsumed until they arrive.
        assert_eq!(feed(&[0x20, 0x3f]), Ok(1));
        assert_eq!(
            feed(&hex("3fbd01")),
            Err(EncoderStreamError::CapacityExceeded(220))
        );
        assert_eq!(
            feed(&hex("3fff ffff ffff ffff ffff 01")),
            Err(EncoderStreamError::IntegerTooLarge)
        );
        // Inserts and duplicates fail on their first byte.
        for first in [0xc0, 0x80, 0x4a, 0x60] {
            assert_eq!(
                feed(&[0x20, first]),
                Err(EncoderStreamError::Insert),
                "{first:x}"
            );
        }
        assert_eq!(feed(&[0x20, 0x02]), Err(EncoderStreamError::Duplicate));
        assert_eq!(feed(&[0x1f]), Err(EncoderStreamError::Duplicate));
    }

    /// The request h2load sends once nghttp3 has indexed its `:authority` and `user-agent`,
    /// in nghttp3's field order.
    const H2LOAD_REQUEST: [(&[u8], &[u8]); 5] = [
        (b":path", b"/baseline2?a=1&b=1"),
        (b":scheme", b"https"),
        (b":authority", b"localhost:8443"),
        (b":method", b"GET"),
        (b"user-agent", b"h2load nghttp2/1.69.0"),
    ];

    /// Appends a Literal Field Line with a static Name Reference and a Huffman-coded value.
    fn static_name_literal(block: &mut Vec<u8>, index: u8, value: &[u8]) {
        integer::encode(block, 0b0101_0000, 4, index.into());
        crate::string::encode(block, 0, 8, value, true);
    }

    /// Returns the encoder stream nghttp3 sends after the server's SETTINGS: the capacity, then
    /// `:authority` and `user-agent` with static name references.
    fn h2load_encoder_stream() -> Vec<u8> {
        let mut out = capacity(4096);
        for (index, value) in [(0, H2LOAD_REQUEST[2].1), (95, H2LOAD_REQUEST[4].1)] {
            integer::encode(&mut out, 0b1100_0000, 6, index);
            crate::string::encode(&mut out, 0, 8, value, true);
        }
        out
    }

    /// Returns the request's field section once both entries are acknowledged, and in its
    /// literal form.
    fn h2load_sections() -> (Vec<u8>, Vec<u8>) {
        // Required Insert Count 2 (encoded 3 under a 4,096-byte maximum) and Base 2; `:path` is a
        // literal, `:scheme` and `:method` static, and the other two relative indices 1 and 0.
        let mut dynamic = hex("0300");
        static_name_literal(&mut dynamic, 1, H2LOAD_REQUEST[0].1);
        dynamic.extend(hex("d7 81 d1 80"));
        let mut literal = hex("0000");
        static_name_literal(&mut literal, 1, H2LOAD_REQUEST[0].1);
        literal.push(0xd7);
        static_name_literal(&mut literal, 0, H2LOAD_REQUEST[2].1);
        literal.push(0xd1);
        static_name_literal(&mut literal, 95, H2LOAD_REQUEST[4].1);
        (dynamic, literal)
    }

    #[test]
    fn a_steady_state_request_section_allocates_nothing_and_huffman_decodes_fourteen_bytes() {
        let (dynamic, literal) = h2load_sections();
        assert_eq!((dynamic.len(), literal.len()), (22, 50));
        let mut decoder = Decoder::with_max_capacity(4096);
        feed_all(&mut decoder, &h2load_encoder_stream()).unwrap();
        // Returns the outcome, the lines that matched the request and the Huffman bytes decoded.
        let decode = |decoder: &mut Decoder, block: &[u8]| {
            let mut lines = 0;
            HUFFMAN_INPUT.set(0);
            let decoded = decoder.decode_section(0, block, |line, _| {
                assert_eq!((line.name, line.value), H2LOAD_REQUEST[lines]);
                lines += 1;
            });
            (decoded, lines, HUFFMAN_INPUT.get())
        };
        let whole = (Ok(()), 5);
        let (decoded, lines, literal_huffman) = decode(&mut decoder, &literal);
        // A warm-up decode sizes the scratch buffers.
        let (warm, warm_lines, _) = decode(&mut decoder, &dynamic);
        assert_eq!(
            (decoded, lines, literal_huffman, warm, warm_lines),
            (Ok(()), 5, 39, whole.0, whole.1)
        );
        let mut outcome = None;
        let allocations = count_allocations(|| outcome = Some(decode(&mut decoder, &dynamic)));
        assert_eq!(outcome, Some((Ok(()), 5, 14)));
        if let Some(allocations) = allocations {
            assert_eq!(allocations, 0);
        }
        // An insert allocates only its entry.
        let insert = literal_insert(b"x-custom", b"value");
        let allocations = count_allocations(|| feed_all(&mut decoder, &insert).unwrap());
        if let Some(allocations) = allocations {
            assert_eq!(allocations, 1);
        }
    }

    /// Returns `text` as a Huffman-coded string literal's bytes.
    fn huffman(text: &[u8]) -> Vec<u8> {
        let mut coded = Vec::new();
        crate::huffman::encode(text, &mut coded);
        coded
    }

    #[test]
    fn an_insert_allocates_once_whatever_strings_came_before() {
        let mut decoder = Decoder::with_max_capacity(4096);
        feed_all(&mut decoder, &capacity(4096)).unwrap();
        // Every kind of insert, each with Huffman-coded strings longer than any before it, on a
        // decoder that has not yet decoded a Huffman string.
        let texts = [
            b"x-custom-name".repeat(2),
            b"a custom value".repeat(4),
            b"x-longer-custom-name".repeat(3),
            b"a longer custom value".repeat(8),
            b"an even longer custom value".repeat(10),
            b"the longest custom value of all".repeat(12),
        ];
        let coded = texts.each_ref().map(|text| huffman(text));
        let literal = |index: usize| StringLiteral {
            huffman: true,
            data: &coded[index],
        };
        let inserts = [
            EncoderInstruction::InsertWithLiteralName {
                name: literal(0),
                value: literal(1),
            },
            EncoderInstruction::InsertWithLiteralName {
                name: literal(2),
                value: literal(3),
            },
            EncoderInstruction::InsertWithNameReference {
                static_table: true,
                name_index: 95,
                value: literal(4),
            },
            EncoderInstruction::InsertWithNameReference {
                static_table: false,
                name_index: 1,
                value: literal(5),
            },
            EncoderInstruction::Duplicate(0),
        ]
        .map(|insert| stream(&[insert]));
        for (index, insert) in inserts.iter().enumerate() {
            let allocations = count_allocations(|| feed_all(&mut decoder, insert).unwrap());
            if let Some(allocations) = allocations {
                assert_eq!(allocations, 1, "insert {index}");
            }
        }
        let entry = |name: &[u8], value: usize| (name.to_vec(), texts[value].clone());
        assert_eq!(
            table(&decoder),
            vec![
                entry(&texts[0], 1),
                entry(&texts[2], 3),
                entry(b"user-agent", 4),
                entry(&texts[2], 5),
                entry(&texts[2], 5),
            ]
        );
    }

    #[test]
    fn the_owed_instructions_reuse_their_buffer_across_phases() {
        let (dynamic, _) = h2load_sections();
        let mut decoder = Decoder::with_max_capacity(4096);
        feed_all(&mut decoder, &h2load_encoder_stream()).unwrap();
        // A phase of 64 sections on stream IDs whose Section Acknowledgments take 3 bytes each.
        let decode_phase = |decoder: &mut Decoder, first: u64| {
            for stream in (0..64).map(|index| first + 4 * index) {
                decoder.decode_section(stream, &dynamic, |_, _| {}).unwrap();
            }
        };
        decode_phase(&mut decoder, 1000);
        let mut written = Vec::new();
        decoder.write_owed(&mut written);
        assert_eq!(written.len(), 64 * 3);
        // Once a phase has sized the buffer of owed instructions and the Huffman scratch, the
        // next phase's sections do not allocate.
        let allocations = count_allocations(|| decode_phase(&mut decoder, 2000));
        assert_eq!(decoder.owed_len(), 64 * 3);
        if let Some(allocations) = allocations {
            assert_eq!(allocations, 0);
        }
    }

    /// Inserts entries with an empty name and the decimal insert count as value, from `from`.
    fn numbered_inserts(decoder: &mut Decoder, from: u64, to: u64) {
        for i in from..to {
            feed_all(decoder, &literal_insert(b"", i.to_string().as_bytes())).unwrap();
        }
    }

    /// Decodes `block` on stream 0 into (value, StaticRef) pairs.
    fn values(decoder: &mut Decoder, block: &[u8]) -> Result<Vec<Vec<u8>>, DecodeError> {
        let mut lines = Vec::new();
        decoder.decode_section(0, block, |line, _| lines.push(line.value.to_vec()))?;
        Ok(lines)
    }

    /// Returns a section prefix: the encoded Required Insert Count, then the sign and Delta Base.
    fn prefix(encoded: u64, sign: bool, delta: u64) -> Vec<u8> {
        let mut block = Vec::new();
        integer::encode(&mut block, 0, 8, encoded);
        integer::encode(&mut block, if sign { 0x80 } else { 0 }, 7, delta);
        block
    }

    #[test]
    fn decodes_the_required_insert_count_and_base() {
        // The example of RFC 9204: under a 100-byte maximum, MaxEntries is 3, and with 10 inserts
        // an encoded count of 4 is 9. Base 9 makes relative index 0 entry 8.
        let mut decoder = Decoder::with_max_capacity(100);
        feed_all(&mut decoder, &capacity(100)).unwrap();
        numbered_inserts(&mut decoder, 0, 10);
        let mut block = prefix(4, false, 0);
        block.push(0x80);
        assert_eq!(values(&mut decoder, &block), Ok(vec![b"8".to_vec()]));

        // Under 4,096 bytes the encoded count wraps at 256: after 300 inserts, 45 is 300 and 251
        // is 250.
        let mut decoder = Decoder::with_max_capacity(4096);
        feed_all(&mut decoder, &capacity(4096)).unwrap();
        numbered_inserts(&mut decoder, 0, 300);
        for (encoded, newest) in [(45, b"299"), (251, b"249")] {
            let mut block = prefix(encoded, false, 0);
            block.push(0x80);
            assert_eq!(values(&mut decoder, &block), Ok(vec![newest.to_vec()]));
        }

        // An encoded count above 2 * MaxEntries, one that would wrap below 0, and one that
        // decodes to 0 are invalid.
        let mut decoder = Decoder::with_max_capacity(4096);
        feed_all(&mut decoder, &capacity(4096)).unwrap();
        numbered_inserts(&mut decoder, 0, 2);
        for encoded in [257, 200, 1] {
            assert_eq!(
                values(&mut decoder, &prefix(encoded, false, 0)),
                Err(DecodeError::RequiredInsertCount(encoded)),
                "{encoded}"
            );
        }

        // With the sign bit, Base is the count minus Delta Base minus 1, and must not be negative.
        let mut decoder = Decoder::with_max_capacity(4096);
        feed_all(&mut decoder, &capacity(4096)).unwrap();
        numbered_inserts(&mut decoder, 0, 2);
        let mut block = prefix(3, true, 0);
        block.push(0x80);
        assert_eq!(values(&mut decoder, &block), Ok(vec![b"0".to_vec()]));

        // A count larger than the section needs is accepted: 2 where entry 0 alone needs 1, and 2
        // where only the static table is referenced.
        let mut block = prefix(3, false, 0);
        block.push(0x81);
        assert_eq!(values(&mut decoder, &block), Ok(vec![b"0".to_vec()]));
        let mut block = prefix(3, false, 0);
        block.push(0xd1);
        assert_eq!(values(&mut decoder, &block), Ok(vec![b"GET".to_vec()]));
        for delta in [2, 3, 1000] {
            assert_eq!(
                values(&mut decoder, &prefix(3, true, delta)),
                Err(DecodeError::NegativeBase),
                "{delta}"
            );
        }
    }

    /// Decodes `block` on `stream` into (name, value) pairs.
    fn lines(
        decoder: &mut Decoder,
        stream: u64,
        block: &[u8],
    ) -> Result<Vec<(String, String)>, DecodeError> {
        let mut lines = Vec::new();
        decoder.decode_section(stream, block, |line, _| {
            let text = |bytes: &[u8]| String::from_utf8(bytes.to_vec()).unwrap();
            lines.push((text(line.name), text(line.value)));
        })?;
        Ok(lines)
    }

    fn owed(decoder: &mut Decoder) -> Vec<u8> {
        let mut out = Vec::new();
        decoder.write_owed(&mut out);
        out
    }

    fn pairs(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn decodes_the_rfc9204_dynamic_table_examples() {
        let mut decoder = Decoder::with_max_capacity(4096);
        // B.1: a static name reference.
        let b1 = hex("0000 510b 2f69 6e64 6578 2e68 746d 6c");
        assert_eq!(
            lines(&mut decoder, 0, &b1),
            Ok(pairs(&[(":path", "/index.html")]))
        );
        assert!(!decoder.has_owed());

        // B.2: two inserts, then a section that references both after its Base.
        feed_all(
            &mut decoder,
            &hex("3fbd01 c00f 7777 772e 6578 616d 706c 652e 636f 6d
                 c10c 2f73 616d 706c 652f 7061 7468"),
        )
        .unwrap();
        assert_eq!(decoder.table_size(), 106);
        assert_eq!(
            lines(&mut decoder, 4, &hex("0381 1011")),
            Ok(pairs(&[
                (":authority", "www.example.com"),
                (":path", "/sample/path")
            ]))
        );
        assert!(decoder.has_owed());
        assert_eq!(owed(&mut decoder), hex("84"));
        assert!(!decoder.has_owed());

        // B.3: an insert with a literal name, which an Insert Count Increment acknowledges.
        feed_all(
            &mut decoder,
            &hex("4a63 7573 746f 6d2d 6b65 790c 6375 7374 6f6d 2d76 616c 7565"),
        )
        .unwrap();
        assert_eq!(decoder.table_size(), 160);
        assert_eq!(owed(&mut decoder), hex("01"));

        // B.4: a Duplicate. Stream 8's section would block, so the stream is abandoned before it
        // is decoded; the same section then decodes on stream 12.
        feed_all(&mut decoder, &hex("02")).unwrap();
        assert_eq!(decoder.table_size(), 217);
        decoder.cancel_stream(8);
        assert_eq!(owed(&mut decoder), hex("48 01"));
        assert_eq!(
            lines(&mut decoder, 12, &hex("0500 80c1 81")),
            Ok(pairs(&[
                (":authority", "www.example.com"),
                (":path", "/"),
                ("custom-key", "custom-value")
            ]))
        );
        assert_eq!(owed(&mut decoder), hex("8c"));

        // B.5: an insert with a dynamic name reference evicts the oldest entry.
        feed_all(&mut decoder, &hex("810d 6375 7374 6f6d 2d76 616c 7565 32")).unwrap();
        assert_eq!(decoder.table_size(), 215);
        assert_eq!(owed(&mut decoder), hex("01"));
        assert_eq!(
            table(&decoder),
            vec![
                (b":path".to_vec(), b"/sample/path".to_vec()),
                (b"custom-key".to_vec(), b"custom-value".to_vec()),
                (b":authority".to_vec(), b"www.example.com".to_vec()),
                (b"custom-key".to_vec(), b"custom-value2".to_vec()),
            ]
        );
        assert_eq!(
            lines(&mut decoder, 16, &b1),
            Ok(pairs(&[(":path", "/index.html")]))
        );
        assert!(!decoder.has_owed());
    }

    #[test]
    fn rejects_invalid_dynamic_references() {
        // A 100-byte table of 33-byte entries holds entries 2 to 4 of the 5 inserted.
        let mut decoder = Decoder::with_max_capacity(4096);
        feed_all(&mut decoder, &capacity(100)).unwrap();
        numbered_inserts(&mut decoder, 0, 5);
        // Required Insert Count 4 (encoded 5) and Base 5: relative index 1 is entry 3.
        assert_eq!(
            values(&mut decoder, &hex("0501 81")),
            Ok(vec![b"3".to_vec()])
        );
        let cases = [
            // Relative index 4 under Base 5 is entry 0, which was evicted.
            ("0600 84", DecodeError::DynamicTableReference),
            // Relative index 0 under Base 5 is entry 4, at the Required Insert Count of 4.
            ("0501 80", DecodeError::DynamicTableReference),
            // Post-Base index 1 under Base 3 is entry 4 too.
            ("0580 11", DecodeError::DynamicTableReference),
            // Relative index 5 under Base 5 is before entry 0.
            ("0600 85", DecodeError::DynamicTableReference),
            // A Required Insert Count of 6 would wait for an entry not yet received.
            (
                "0700",
                DecodeError::Blocked {
                    required: 6,
                    inserted: 5,
                },
            ),
        ];
        for (block, error) in cases {
            assert_eq!(values(&mut decoder, &hex(block)), Err(error), "{block}");
            assert!(error.is_connection_error(), "{block}");
        }
        // Only the valid section is acknowledged.
        assert_eq!(owed(&mut decoder), hex("80 01"));

        // Under a Required Insert Count of 0, a dynamic reference fails before its index is read,
        // as on a decoder that advertised a capacity of 0, so even an index too large to decode
        // is a connection error.
        for first in [0xbf, 0x4f, 0x6f, 0x1f, 0x07, 0x0f] {
            let block = [&hex("0000")[..], &[first], &[0xff; 9], &[0x01]].concat();
            let reference = DecodeError::DynamicTableReference;
            assert_eq!(values(&mut decoder, &block), Err(reference), "{first:x}");
            assert_eq!(
                Decoder::new().decode_field_section(&block, |_| {}),
                Err(reference),
                "{first:x}"
            );
        }
    }

    /// Writes the due instructions after checking that `owed_len` gives their length.
    fn checked_owed(decoder: &mut Decoder) -> Vec<u8> {
        let len = decoder.owed_len();
        let written = owed(decoder);
        assert_eq!(written.len(), len);
        assert!(!decoder.has_owed());
        written
    }

    #[test]
    fn owed_instructions_are_acks_then_one_increment() {
        let mut decoder = Decoder::with_max_capacity(4096);
        feed_all(&mut decoder, &capacity(4096)).unwrap();
        numbered_inserts(&mut decoder, 0, 3);
        // A section without dynamic references is not acknowledged. Acknowledgments and
        // cancellations go in the order they arose, then one increment for the inserts that
        // neither acknowledges.
        values(&mut decoder, &hex("0000 d1")).unwrap();
        for (stream, block) in [(4, "0200 80"), (8, "0300 80")] {
            decoder
                .decode_section(stream, &hex(block), |_, _| {})
                .unwrap();
        }
        decoder.cancel_stream(12);
        decoder
            .decode_section(16, &hex("0200 80"), |_, _| {})
            .unwrap();
        assert_eq!(checked_owed(&mut decoder), hex("84 88 4c 90 01"));

        // An acknowledgment that covers every insert leaves no increment.
        numbered_inserts(&mut decoder, 3, 5);
        decoder
            .decode_section(20, &hex("0600 80"), |_, _| {})
            .unwrap();
        assert_eq!(checked_owed(&mut decoder), hex("94"));

        // The increment counts exactly the inserts after the largest acknowledged count.
        numbered_inserts(&mut decoder, 5, 70);
        decoder
            .decode_section(400, &hex("0700 80"), |_, _| {})
            .unwrap();
        assert_eq!(checked_owed(&mut decoder), hex("ff 9102 3f01"));
        assert_eq!(decoder.owed_len(), 0);
        assert_eq!(checked_owed(&mut decoder), hex(""));

        // A decoder that advertised a capacity of 0 writes no cancellations.
        let mut zero = Decoder::new();
        zero.cancel_stream(4);
        assert!(!zero.has_owed());
        assert_eq!(checked_owed(&mut zero), hex(""));
    }

    #[test]
    fn dynamic_lines_report_their_static_name_and_never_a_static_entry() {
        let mut decoder = Decoder::with_max_capacity(4096);
        feed_all(&mut decoder, &capacity(4096)).unwrap();
        let insert =
            |static_table, name_index, value| EncoderInstruction::InsertWithNameReference {
                static_table,
                name_index,
                value: raw(value),
            };
        // Entries 0 to 5: `user-agent` from the static table; the same name as a literal; a
        // Duplicate of entry 0; dynamic name references to entries 0 and 1; and `:method: GET`,
        // which equals static entry 17.
        let mut encoder_stream = stream(&[insert(true, 95, b"a")]);
        encoder_stream.extend(literal_insert(b"user-agent", b"b"));
        encoder_stream.extend(stream(&[
            EncoderInstruction::Duplicate(1),
            insert(false, 2, b"c"),
            insert(false, 2, b"d"),
            insert(true, 17, b"GET"),
        ]));
        feed_all(&mut decoder, &encoder_stream).unwrap();

        let name = StaticRef::Name;
        let unresolved = StaticRef::Unresolved;
        let refs: [(&[u8], StaticRef); 6] = [
            (b"a", name(95)),
            (b"b", unresolved),
            (b"a", name(95)),
            (b"c", name(95)),
            (b"d", unresolved),
            (b"GET", name(17)),
        ];
        let name_of = |absolute: usize| -> &[u8] {
            if absolute == 5 {
                b":method"
            } else {
                b"user-agent"
            }
        };
        // Required Insert Count 6 and Base 6: each entry by relative index, then literal values
        // with relative name references to entries 0 and 1, the second with N.
        let first = hex("0700 85 84 83 82 81 80 45 01 78 64 01 79");
        let mut expected: Vec<_> = (0..6)
            .map(|i| (name_of(i).to_vec(), refs[i].0.to_vec(), false, refs[i].1))
            .collect();
        expected.push((b"user-agent".to_vec(), b"x".to_vec(), false, name(95)));
        expected.push((b"user-agent".to_vec(), b"y".to_vec(), true, unresolved));
        // Base 2: entries 2 to 5 by post-Base index, then literal values with post-Base name
        // references to entries 4 and 5, the second with N.
        let second = hex("0783 10 11 12 13 02 01 78 0b 01 79");
        let mut expected_second: Vec<_> = (2..6)
            .map(|i| (name_of(i).to_vec(), refs[i].0.to_vec(), false, refs[i].1))
            .collect();
        expected_second.push((b"user-agent".to_vec(), b"x".to_vec(), false, unresolved));
        expected_second.push((b":method".to_vec(), b"y".to_vec(), true, name(17)));

        for (section, expected) in [(first, expected), (second, expected_second)] {
            let mut lines = Vec::new();
            decoder
                .decode_section(0, &section, |line, static_ref| {
                    lines.push((
                        line.name.to_vec(),
                        line.value.to_vec(),
                        line.never_indexed,
                        static_ref,
                    ));
                })
                .unwrap();
            assert_eq!(lines, expected);
        }
    }
}
