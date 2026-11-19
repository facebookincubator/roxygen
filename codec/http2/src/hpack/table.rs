/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::borrow::Borrow;
use std::cell::Cell;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::collections::hash_map::Entry;
use std::fmt;
use std::hash::BuildHasher;
use std::hash::BuildHasherDefault;
use std::hash::Hash;
use std::hash::Hasher;
use std::hash::RandomState;
use std::iter;
use std::panic::RefUnwindSafe;
use std::sync::OnceLock;

use foldhash::SharedSeed;
use foldhash::fast::SeedableRandomState;

use super::HeaderName;
use super::HpackHeader;
use crate::util::has_ascii_uppercase;

#[cfg(test)]
thread_local! {
    static EXAMINED: Cell<usize> = const { Cell::new(0) };
    static HEADER_HASHES: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
fn record_examined(entries: usize) {
    EXAMINED.set(EXAMINED.get() + entries);
}

#[cfg(test)]
fn record_header_hash() {
    HEADER_HASHES.set(HEADER_HASHES.get() + 1);
}

/// A table's foldhash state for header names and values. Each table draws its own per-hasher seed
/// from std's `RandomState`, so a peer cannot aim collisions at it, and a collision only sends a
/// lookup to the bounded same-name scan.
#[derive(Clone)]
struct TableHash(SeedableRandomState);

/// The seed is secret, so `Debug` leaves it out, as std's `RandomState` does.
impl fmt::Debug for TableHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("TableHash").finish_non_exhaustive()
    }
}

impl TableHash {
    fn new() -> Self {
        Self(SeedableRandomState::with_seed(
            RandomState::new().hash_one("roxygen hpack table"),
            SharedSeed::global_random(),
        ))
    }

    /// Hashes each byte string once: the name of a name without a common code, then the pair
    /// from the name part (the name hash, or the code) and the value. Lengths are hashed
    /// explicitly, so where the name ends and the value starts is part of the hash.
    fn header(&self, key: NameKey<'_>, value: &[u8]) -> HeaderHashes {
        let mut pair = self.0.build_hasher();
        let name = match key {
            NameKey::Common(code) => {
                pair.write_u8(code);
                0
            }
            NameKey::Other(bytes) => {
                let name = self.name(bytes);
                pair.write_u64(name);
                name
            }
        };
        pair.write_usize(value.len());
        pair.write(value);
        HeaderHashes {
            name,
            pair: pair.finish(),
        }
    }

    /// The hash of a name without a common code.
    fn name(&self, bytes: &[u8]) -> u64 {
        let mut name = self.0.build_hasher();
        name.write_usize(bytes.len());
        name.write(bytes);
        name.finish()
    }
}

/// One header's hashes under its table's `TableHash`: `name` over the name key and `pair` over the
/// name key and the value. `name` is 0 for a common name, which the names index keys by code.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct HeaderHashes {
    name: u64,
    pair: u64,
}

/// A header name as the indexes key it: common names by code, other names by lowercase bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NameKey<'a> {
    Common(u8),
    Other(&'a [u8]),
}

impl<'a> NameKey<'a> {
    fn of(name: &'a HeaderName) -> Self {
        if name.is_common_header() {
            Self::Common(name.code().as_u8())
        } else {
            Self::Other(name.as_bytes())
        }
    }

    /// How many name bytes the key holds: none for a common name, which it keys by code.
    fn byte_len(self) -> usize {
        match self {
            Self::Common(_) => 0,
            Self::Other(name) => name.len(),
        }
    }
}

/// A name as the names index keys it: a common name by code, any other name by its name hash.
#[derive(Clone, Copy, Debug)]
enum IndexedName {
    Common(u8),
    Other(u64),
}

impl IndexedName {
    fn new(key: NameKey<'_>, name_hash: u64) -> Self {
        match key {
            NameKey::Common(code) => Self::Common(code),
            NameKey::Other(_) => Self::Other(name_hash),
        }
    }
}

/// Port of proxygen `HeaderTable::names_map` (`HeaderTable.h:27-30`): for each live name, the
/// insertion ordinal of its newest entry. Each entry links to the next older entry under the same
/// key (`TableEntry::older`), so the links from the newest entry visit a name's entries newest
/// first, as proxygen's per-name index list does, without a list per name. Common names are keyed
/// by their one-byte code, as proxygen's `HPACKHeaderName` hash does (`HPACKHeaderName.h:208-216`),
/// and other names by their precomputed name hash.
#[derive(Clone, Debug, Default)]
struct NamesIndex {
    common: HashMap<u8, u32, BuildHasherDefault<CodeHasher>>,
    other: HashMap<u64, u32, BuildHasherDefault<PrehashedHasher>>,
    /// Gives every name without a common code the same hash, so that tests can force collisions.
    #[cfg(test)]
    colliding: bool,
}

/// Proxygen hashes a common name by its code (`HPACKHeaderName.h:215`). Multiplying by the
/// Fibonacci constant spreads the at most 256 codes across the hash's high bits.
#[derive(Default)]
struct CodeHasher(u64);

impl Hasher for CodeHasher {
    fn write(&mut self, bytes: &[u8]) {
        bytes.iter().for_each(|&byte| self.write_u8(byte));
    }

    fn write_u8(&mut self, code: u8) {
        self.0 = u64::from(code).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

/// Keys a map by values that are already table hashes, passing them through unchanged.
#[derive(Default)]
struct PrehashedHasher(u64);

impl Hasher for PrehashedHasher {
    fn write(&mut self, bytes: &[u8]) {
        self.0 = bytes
            .iter()
            .fold(self.0, |hash, &byte| hash.rotate_left(8) ^ u64::from(byte));
    }

    fn write_u64(&mut self, hash: u64) {
        self.0 = hash;
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

impl NamesIndex {
    fn other_key(&self, name_hash: u64) -> u64 {
        #[cfg(test)]
        if self.colliding {
            return 0;
        }
        name_hash
    }

    /// The insertion ordinal of the newest live entry under `name`'s key.
    fn get(&self, name: IndexedName) -> Option<u32> {
        match name {
            IndexedName::Common(code) => self.common.get(&code).copied(),
            IndexedName::Other(hash) => self.other.get(&self.other_key(hash)).copied(),
        }
    }

    /// Makes `ordinal` the newest entry under `name`'s key, and returns the entry it supersedes.
    fn push(&mut self, name: IndexedName, ordinal: u32) -> Option<u32> {
        match name {
            IndexedName::Common(code) => self.common.insert(code, ordinal),
            IndexedName::Other(hash) => {
                let key = self.other_key(hash);
                self.other.insert(key, ordinal)
            }
        }
    }

    /// Forgets `name`'s key when the evicted entry `ordinal`, the oldest live entry, was the newest
    /// under it.
    fn pop_oldest(&mut self, name: IndexedName, ordinal: u32) {
        match name {
            IndexedName::Common(code) => forget_if_newest(&mut self.common, &code, ordinal),
            IndexedName::Other(hash) => {
                let key = self.other_key(hash);
                forget_if_newest(&mut self.other, &key, ordinal);
            }
        }
    }

    fn clear(&mut self) {
        self.common.clear();
        self.other.clear();
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.common.len() + self.other.len()
    }
}

/// Mirrors the names-index half of proxygen `HeaderTable::removeLast` (`HeaderTable.cpp:120-131`):
/// the key goes when its last entry does. Evictions take the oldest entry, so an evicted entry
/// that is not the newest under its key leaves newer entries there.
fn forget_if_newest<K, Q, S>(names: &mut HashMap<K, u32, S>, key: &Q, ordinal: u32)
where
    K: Borrow<Q> + Eq + Hash,
    Q: Eq + Hash + ?Sized,
    S: BuildHasher,
{
    let newest = *names
        .get(key)
        .expect("should index the evicted entry's name");
    if newest == ordinal {
        names.remove(key);
    }
}

/// Roxygen only, beyond proxygen: for each live `(name, value)` pair, the insertion ordinal of its
/// newest entry, keyed by the pair hash.
#[derive(Clone, Debug, Default)]
struct ExactIndex {
    newest: HashMap<u64, u32, BuildHasherDefault<PrehashedHasher>>,
    /// Gives every pair the same hash, so that tests can force collisions.
    #[cfg(test)]
    colliding: bool,
}

impl ExactIndex {
    fn key(&self, pair_hash: u64) -> u64 {
        #[cfg(test)]
        if self.colliding {
            return 0;
        }
        pair_hash
    }

    fn get(&self, pair_hash: u64) -> Option<u32> {
        self.newest.get(&self.key(pair_hash)).copied()
    }

    fn insert(&mut self, pair_hash: u64, ordinal: u32) {
        let key = self.key(pair_hash);
        self.newest.insert(key, ordinal);
    }

    /// Forgets an evicted entry, unless a newer entry has taken over its hash.
    fn remove(&mut self, pair_hash: u64, ordinal: u32) {
        if let Entry::Occupied(newest) = self.newest.entry(self.key(pair_hash))
            && *newest.get() == ordinal
        {
            newest.remove();
        }
    }

    fn clear(&mut self) {
        self.newest.clear();
    }
}

/// The hashes of the header the last lookup found no exact match for, when that header would fit
/// the table, so that the encoder's `add_missed` of that header does not hash it again. Every
/// lookup overwrites it. Lookups take `&self`, hence the `Cell`.
#[derive(Clone, Debug, Default)]
struct LastMiss(Cell<Option<HeaderHashes>>);

/// Each lookup replaces the whole value with one store, so a lookup that unwinds leaves either
/// the previous lookup's hashes or its own, and the next lookup replaces those.
impl RefUnwindSafe for LastMiss {}

#[derive(Clone, Debug)]
struct TableEntry {
    header: HpackHeader,
    /// Zero while the names index is disabled.
    hashes: HeaderHashes,
    /// The insertion ordinal of the next older entry under the same names-index key, which was live
    /// when this entry was added; `None` while the names index is disabled. Once that entry is
    /// evicted the link is stale, and `same_name` stops at it.
    older: Option<u32>,
}

#[derive(Clone, Debug)]
pub struct HeaderTable {
    capacity: u32,
    bytes: u32,
    insert_count: u32,
    slot_len: usize,
    entries: VecDeque<TableEntry>,
    index_names: bool,
    /// Insertion ordinal of the next entry. It wraps; the live entries hold the `size()` ordinals
    /// just below it.
    next_ordinal: u32,
    state: TableHash,
    names: NamesIndex,
    exact: ExactIndex,
    last_miss: LastMiss,
}

impl HeaderTable {
    pub fn new(capacity: u32) -> Self {
        Self {
            capacity,
            bytes: 0,
            insert_count: 0,
            slot_len: initial_table_length(capacity),
            entries: VecDeque::new(),
            index_names: true,
            next_ordinal: 0,
            state: TableHash::new(),
            names: NamesIndex::default(),
            exact: ExactIndex::default(),
            last_miss: LastMiss::default(),
        }
    }

    /// Returns an empty table whose next insertion ordinal is `start`.
    #[cfg(test)]
    pub(crate) fn with_next_ordinal(capacity: u32, start: u32) -> Self {
        Self {
            next_ordinal: start,
            ..Self::new(capacity)
        }
    }

    /// Hashes a whole byte string under the table's `TableHash`, length first, as a name is hashed.
    pub(crate) fn hash_bytes(&self, bytes: &[u8]) -> u64 {
        self.state.name(bytes)
    }

    pub fn disable_names_index(&mut self) {
        self.index_names = false;
    }

    /// Returns how many table entries lookups on this thread have examined for a value match since
    /// the last call. Checking only an entry's name does not count.
    #[cfg(test)]
    pub(crate) fn take_examined() -> usize {
        EXAMINED.take()
    }

    /// Returns how many header and name hashes (`hash_header` and `indexed_name` computations)
    /// tables on this thread have made since the last call.
    #[cfg(test)]
    pub(crate) fn take_header_hashes() -> usize {
        HEADER_HASHES.take()
    }

    /// Returns how many distinct names the names index holds (proxygen `names().size()`).
    #[cfg(test)]
    pub(crate) fn names_len(&self) -> usize {
        self.names.len()
    }

    /// Returns how many live entries the names index holds for `name`.
    #[cfg(test)]
    pub(crate) fn name_entry_count(&self, name: &HeaderName) -> usize {
        let key = NameKey::of(name);
        self.names
            .get(self.indexed_name(key))
            .map_or(0, |newest| self.same_name(newest, key).count())
    }

    pub fn add(&mut self, header: HpackHeader) -> bool {
        self.add_hashed(header, None)
    }

    /// Adds `header`, which the last lookup on this table found no exact match for, with that
    /// lookup's hashes. The encoder calls it right after looking the same header up.
    pub(super) fn add_missed(&mut self, header: HpackHeader) -> bool {
        let missed = self.last_miss.0.take();
        self.add_hashed(header, missed)
    }

    fn add_hashed(&mut self, header: HpackHeader, missed: Option<HeaderHashes>) -> bool {
        let header_bytes = header.bytes();
        if header_bytes > self.capacity {
            self.clear();
            return false;
        }

        while self.bytes.saturating_add(header_bytes) > self.capacity {
            if self.remove_last().is_none() {
                return false;
            }
        }

        if self.entries.len() == self.slot_len {
            let grown = self.entries.len().saturating_mul(3).div_ceil(2);
            self.slot_len = grown.min(self.max_table_length() as usize);
        }
        self.bytes += header_bytes;
        let (hashes, older) = if self.index_names {
            let key = NameKey::of(&header.name);
            let hashes = match missed {
                Some(hashes) => {
                    debug_assert_eq!(
                        hashes,
                        self.state.header(key, &header.value),
                        "add_missed adds the header its table's last lookup missed"
                    );
                    hashes
                }
                None => self.hash_header(key, &header.value),
            };
            let older = self
                .names
                .push(IndexedName::new(key, hashes.name), self.next_ordinal);
            self.exact.insert(hashes.pair, self.next_ordinal);
            (hashes, older)
        } else {
            (HeaderHashes::default(), None)
        };
        self.entries.push_front(TableEntry {
            header,
            hashes,
            older,
        });
        self.next_ordinal = self.next_ordinal.wrapping_add(1);
        self.insert_count = self.insert_count.saturating_add(1);
        true
    }

    /// Mirrors proxygen `HeaderTable::removeLast` (`HeaderTable.cpp:117-139`); returns the evicted
    /// entry's bytes, or `None` when the table is empty.
    fn remove_last(&mut self) -> Option<u32> {
        let oldest = self.next_ordinal.wrapping_sub(self.size());
        let evicted = self.entries.pop_back()?;
        if self.index_names {
            let key = NameKey::of(&evicted.header.name);
            self.names
                .pop_oldest(IndexedName::new(key, evicted.hashes.name), oldest);
            self.exact.remove(evicted.hashes.pair, oldest);
        }
        let evicted_bytes = evicted.header.bytes();
        self.bytes -= evicted_bytes;
        Some(evicted_bytes)
    }

    pub fn get_index(&self, header: &HpackHeader) -> (u32, u32) {
        self.get_index_parts(&header.name, &header.value)
    }

    pub fn get_index_parts(&self, name: &HeaderName, value: &[u8]) -> (u32, u32) {
        assert!(self.index_names, "name lookup requires a names index");
        self.lookup(NameKey::of(name), value)
    }

    /// The newest exact match, else the newest same-name index, else `(0, 0)`. An exact-index
    /// mapping is checked against its entry; one that holds another pair (a hash collision) falls
    /// back to the same-name scan. The hashes of a header without an exact match that would fit the
    /// table are remembered for `add_missed`.
    fn lookup(&self, key: NameKey<'_>, value: &[u8]) -> (u32, u32) {
        let hashes = self.hash_header(key, value);
        let mapped = self
            .exact
            .get(hashes.pair)
            .map(|ordinal| self.external_index(ordinal));
        if let Some(index) = mapped {
            #[cfg(test)]
            record_examined(1);
            let entry = self.indexed_entry(index);
            if NameKey::of(&entry.name) == key && entry.value.as_ref() == value {
                self.last_miss.0.set(None);
                return (index, 0);
            }
        }
        let fits = HpackHeader::bytes_for(key.byte_len(), value.len()) <= self.capacity;
        self.last_miss.0.set(fits.then_some(hashes));
        let Some(newest) = self.names.get(IndexedName::new(key, hashes.name)) else {
            return (0, 0);
        };
        if mapped.is_some() {
            return self.scan(newest, key, value);
        }
        (0, self.newest_index(newest, key))
    }

    /// Mirrors proxygen `HeaderTable::getIndexImpl` (`HeaderTable.cpp:76-93`): compare values of
    /// same-name entries newest first; on no match, return the newest same-name index.
    fn scan(&self, newest_ordinal: u32, key: NameKey<'_>, value: &[u8]) -> (u32, u32) {
        let mut same_name = self.same_name(newest_ordinal, key).peekable();
        let newest = same_name.peek().copied().unwrap_or(0);
        let hit = same_name.find(|&index| {
            #[cfg(test)]
            record_examined(1);
            self.indexed_entry(index).value.as_ref() == value
        });
        hit.map_or((0, newest), |index| (index, 0))
    }

    /// The external indexes of the entries named `key`, newest first, from `newest`, the ordinal
    /// its names-index key maps to, along the `older` links while they reach live entries. A
    /// common name's key holds only that name; another name's key can also hold names that share
    /// its hash, so those entries' names are compared.
    ///
    /// A stale link names an evicted ordinal, which is older than every live entry, so its external
    /// index exceeds `size()`. It cannot wrap back into the live range: every entry added after a
    /// live entry is live too, and a table holds at most `u32::MAX / 32` entries, so the linking
    /// entry is under 2^27 insertions old, and the linked one, live when the linking entry was
    /// added, under 2^28.
    fn same_name<'a>(&'a self, newest: u32, key: NameKey<'a>) -> impl Iterator<Item = u32> + 'a {
        iter::successors(Some(self.external_index(newest)), move |&index| {
            let older = self.entry(index).older?;
            let older_index = self.external_index(older);
            (older_index <= self.size()).then_some(older_index)
        })
        .filter(move |&index| {
            matches!(key, NameKey::Common(_)) || NameKey::of(&self.indexed_entry(index).name) == key
        })
    }

    /// Hashes a header's name key and value under the table's `TableHash`.
    fn hash_header(&self, key: NameKey<'_>, value: &[u8]) -> HeaderHashes {
        #[cfg(test)]
        record_header_hash();
        self.state.header(key, value)
    }

    /// The names-index key of a name. Only a name without a common code is hashed, to the `name`
    /// that `hash_header` gives it.
    fn indexed_name(&self, key: NameKey<'_>) -> IndexedName {
        match key {
            NameKey::Common(code) => IndexedName::Common(code),
            NameKey::Other(name) => {
                #[cfg(test)]
                record_header_hash();
                IndexedName::Other(self.state.name(name))
            }
        }
    }

    /// The external index of a live entry's insertion ordinal; 1 is the newest entry.
    fn external_index(&self, ordinal: u32) -> u32 {
        self.next_ordinal.wrapping_sub(ordinal)
    }

    /// The newest index named `key` from `newest`, its names-index key's newest ordinal, or 0.
    fn newest_index(&self, newest: u32, key: NameKey<'_>) -> u32 {
        self.same_name(newest, key).next().unwrap_or(0)
    }

    fn indexed_entry(&self, index: u32) -> &HpackHeader {
        &self.entry(index).header
    }

    fn entry(&self, index: u32) -> &TableEntry {
        usize::try_from(index)
            .ok()
            .and_then(|index| index.checked_sub(1))
            .and_then(|index| self.entries.get(index))
            .expect("should hold a live entry for every indexed ordinal")
    }

    /// Looks up a name the caller did not map to a common code. Only `Other` entries match, and
    /// their names compare ASCII-case-insensitively (stored `Other` names are ASCII lowercase), so a
    /// common-name entry with the same spelling never matches. Names up to 64 bytes that need
    /// lowercasing are lowercased on the stack.
    pub(super) fn get_unknown_index_parts(&self, name: &[u8], value: &[u8]) -> (u32, u32) {
        assert!(self.index_names, "name lookup requires a names index");
        if !has_ascii_uppercase(name) {
            return self.lookup(NameKey::Other(name), value);
        }
        let mut buffer = [0; 64];
        let Some(lowercase) = buffer.get_mut(..name.len()) else {
            return self.lookup(NameKey::Other(&name.to_ascii_lowercase()), value);
        };
        lowercase.copy_from_slice(name);
        lowercase.make_ascii_lowercase();
        self.lookup(NameKey::Other(lowercase), value)
    }

    pub fn name_index(&self, name: &HeaderName) -> u32 {
        assert!(self.index_names, "name lookup requires a names index");
        let key = NameKey::of(name);
        self.names
            .get(self.indexed_name(key))
            .map_or(0, |newest| self.newest_index(newest, key))
    }

    pub fn has_name(&self, name: &HeaderName) -> bool {
        self.name_index(name) != 0
    }

    pub fn get_header(&self, index: u32) -> Option<&HpackHeader> {
        let index = usize::try_from(index).ok()?.checked_sub(1)?;
        self.entries.get(index).map(|entry| &entry.header)
    }

    pub fn is_valid(&self, index: u32) -> bool {
        index != 0 && index <= self.size()
    }

    pub const fn capacity(&self) -> u32 {
        self.capacity
    }

    pub const fn bytes(&self) -> u32 {
        self.bytes
    }

    pub fn size(&self) -> u32 {
        u32::try_from(self.entries.len()).expect("HPACK table size fits in u32")
    }

    pub const fn insert_count(&self) -> u32 {
        self.insert_count
    }

    pub const fn allocated_slots(&self) -> usize {
        self.slot_len
    }

    pub const fn max_table_length(&self) -> u32 {
        self.capacity >> 5
    }

    pub fn set_capacity(&mut self, capacity: u32) -> bool {
        if capacity < self.capacity {
            while self.bytes > capacity {
                if self.remove_last().is_none() {
                    return false;
                }
            }
        } else if capacity > self.capacity {
            self.slot_len = self.slot_len.max(initial_table_length(capacity));
        }
        self.capacity = capacity;
        true
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.bytes = 0;
        self.names.clear();
        self.exact.clear();
    }

    pub fn entries(&self) -> impl ExactSizeIterator<Item = &HpackHeader> {
        self.entries.iter().map(|entry| &entry.header)
    }

    pub const fn to_external(head: u32, length: u32, internal_index: u32) -> u32 {
        ((head + length - internal_index) % length) + 1
    }

    pub const fn to_internal(head: u32, length: u32, external_index: u32) -> u32 {
        (head + length - (external_index - 1)) % length
    }
}

impl PartialEq for HeaderTable {
    fn eq(&self, other: &Self) -> bool {
        self.size() == other.size() && self.bytes == other.bytes
    }
}

impl Eq for HeaderTable {}

impl fmt::Display for HeaderTable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter)?;
        for (index, header) in self.entries().enumerate() {
            writeln!(formatter, "[{}] (s={}) {header}", index + 1, header.bytes())?;
        }
        writeln!(formatter, "total size: {}", self.bytes)
    }
}

const fn initial_table_length(capacity: u32) -> usize {
    let maximum = capacity >> 5;
    if maximum == 1 {
        1
    } else {
        (maximum / 2) as usize
    }
}

fn names_equal(left: &HeaderName, right: &HeaderName) -> bool {
    left.code() == right.code() && (left.is_common_header() || left.as_bytes() == right.as_bytes())
}

struct StaticTableData {
    entries: Vec<HpackHeader>,
    first_indices: [u8; 256],
}

pub struct StaticHeaderTable;

impl StaticHeaderTable {
    pub fn get() -> &'static [HpackHeader] {
        &Self::data().entries
    }

    pub fn get_header(index: u32) -> Option<&'static HpackHeader> {
        Self::get().get(usize::try_from(index).ok()?.checked_sub(1)?)
    }

    pub fn get_index(name: &HeaderName, value: &[u8]) -> (u32, u32) {
        let table = Self::data();
        if name.is_common_header() {
            let first = table.first_indices[usize::from(name.code().as_u8())];
            if first == 0 {
                return (0, 0);
            }
            let full = table.entries[usize::from(first) - 1..]
                .iter()
                .take_while(|header| names_equal(&header.name, name))
                .position(|header| header.value.as_ref() == value);
            return full.map_or((0, u32::from(first)), |offset| {
                (
                    u32::try_from(usize::from(first) + offset)
                        .expect("static table index fits in u32"),
                    0,
                )
            });
        }

        let mut name_index = 0;
        for (offset, header) in table.entries.iter().enumerate() {
            if names_equal(&header.name, name) {
                let index = u32::try_from(offset + 1).expect("static table index fits in u32");
                if header.value.as_ref() == value {
                    return (index, 0);
                }
                if name_index == 0 {
                    name_index = index;
                }
            }
        }
        (0, name_index)
    }

    pub fn name_index(name: &HeaderName) -> u32 {
        let table = Self::data();
        if name.is_common_header() {
            return u32::from(table.first_indices[usize::from(name.code().as_u8())]);
        }
        table
            .entries
            .iter()
            .position(|header| names_equal(&header.name, name))
            .and_then(|index| u32::try_from(index + 1).ok())
            .unwrap_or(0)
    }

    pub fn is_valid(index: u32) -> bool {
        index != 0 && usize::try_from(index).is_ok_and(|index| index <= Self::get().len())
    }

    pub fn is_header_code_in_table_with_non_empty_value(
        code: roxygen_core::headers::HttpHeaderCode,
    ) -> bool {
        use roxygen_core::headers::HttpHeaderCode;

        matches!(
            code,
            HttpHeaderCode::ColonMethod
                | HttpHeaderCode::ColonPath
                | HttpHeaderCode::ColonScheme
                | HttpHeaderCode::ColonStatus
                | HttpHeaderCode::AcceptEncoding
        )
    }

    fn data() -> &'static StaticTableData {
        static TABLE: OnceLock<StaticTableData> = OnceLock::new();
        TABLE.get_or_init(|| {
            let entries: Vec<_> = STATIC_ENTRIES
                .iter()
                .map(|(name, value)| HpackHeader::new(name, value))
                .collect();
            let mut first_indices = [0; 256];
            for (offset, header) in entries.iter().enumerate() {
                if header.name.is_common_header() {
                    let first = &mut first_indices[usize::from(header.name.code().as_u8())];
                    if *first == 0 {
                        *first = u8::try_from(offset + 1)
                            .expect("61-entry static table indices fit in u8");
                    }
                }
            }
            StaticTableData {
                entries,
                first_indices,
            }
        })
    }
}

const STATIC_ENTRIES: &[(&[u8], &[u8]); 61] = &[
    (b":authority", b""),
    (b":method", b"GET"),
    (b":method", b"POST"),
    (b":path", b"/"),
    (b":path", b"/index.html"),
    (b":scheme", b"http"),
    (b":scheme", b"https"),
    (b":status", b"200"),
    (b":status", b"204"),
    (b":status", b"206"),
    (b":status", b"304"),
    (b":status", b"400"),
    (b":status", b"404"),
    (b":status", b"500"),
    (b"accept-charset", b""),
    (b"accept-encoding", b"gzip, deflate"),
    (b"accept-language", b""),
    (b"accept-ranges", b""),
    (b"accept", b""),
    (b"access-control-allow-origin", b""),
    (b"age", b""),
    (b"allow", b""),
    (b"authorization", b""),
    (b"cache-control", b""),
    (b"content-disposition", b""),
    (b"content-encoding", b""),
    (b"content-language", b""),
    (b"content-length", b""),
    (b"content-location", b""),
    (b"content-range", b""),
    (b"content-type", b""),
    (b"cookie", b""),
    (b"date", b""),
    (b"etag", b""),
    (b"expect", b""),
    (b"expires", b""),
    (b"from", b""),
    (b"host", b""),
    (b"if-match", b""),
    (b"if-modified-since", b""),
    (b"if-none-match", b""),
    (b"if-range", b""),
    (b"if-unmodified-since", b""),
    (b"last-modified", b""),
    (b"link", b""),
    (b"location", b""),
    (b"max-forwards", b""),
    (b"proxy-authenticate", b""),
    (b"proxy-authorization", b""),
    (b"range", b""),
    (b"referer", b""),
    (b"refresh", b""),
    (b"retry-after", b""),
    (b"server", b""),
    (b"set-cookie", b""),
    (b"strict-transport-security", b""),
    (b"transfer-encoding", b""),
    (b"user-agent", b""),
    (b"vary", b""),
    (b"via", b""),
    (b"www-authenticate", b""),
];

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::VecDeque;

    use foldhash::SharedSeed;
    use foldhash::fast::SeedableRandomState;
    use roxygen_core::headers::HttpHeaderCode;

    use super::HeaderHashes;
    use super::HeaderName;
    use super::HeaderTable;
    use super::HpackHeader;
    use super::LastMiss;
    use super::NameKey;
    use super::TableHash;

    /// The linear-scan `HeaderTable` lookups and mutations as of `0045fe7b35df`
    /// (`table.rs:10-162`, `195-206`), frozen as the oracle for later lookup
    /// indexes. Do not change.
    #[derive(Clone, Debug)]
    struct LinearTable {
        capacity: u32,
        bytes: u32,
        insert_count: u32,
        slot_len: usize,
        entries: VecDeque<HpackHeader>,
        index_names: bool,
    }

    impl LinearTable {
        fn new(capacity: u32) -> Self {
            Self {
                capacity,
                bytes: 0,
                insert_count: 0,
                slot_len: initial_table_length(capacity),
                entries: VecDeque::new(),
                index_names: true,
            }
        }

        fn disable_names_index(&mut self) {
            self.index_names = false;
        }

        fn add(&mut self, header: HpackHeader) -> bool {
            let header_bytes = header.bytes();
            if header_bytes > self.capacity {
                self.clear();
                return false;
            }

            while self.bytes.saturating_add(header_bytes) > self.capacity {
                let Some(evicted) = self.entries.pop_back() else {
                    return false;
                };
                self.bytes -= evicted.bytes();
            }

            if self.entries.len() == self.slot_len {
                let grown = self.entries.len().saturating_mul(3).div_ceil(2);
                self.slot_len = grown.min(self.max_table_length() as usize);
            }
            self.bytes += header_bytes;
            self.entries.push_front(header);
            self.insert_count = self.insert_count.saturating_add(1);
            true
        }

        fn get_index_parts(&self, name: &HeaderName, value: &[u8]) -> (u32, u32) {
            self.get_index_matching(value, |stored| names_equal(stored, name))
        }

        fn get_unknown_index_parts(&self, name: &[u8], value: &[u8]) -> (u32, u32) {
            self.get_index_matching(value, |stored| {
                stored.code() == HttpHeaderCode::Other
                    && stored.as_bytes().eq_ignore_ascii_case(name)
            })
        }

        fn get_index_matching(
            &self,
            value: &[u8],
            matches_name: impl Fn(&HeaderName) -> bool,
        ) -> (u32, u32) {
            assert!(self.index_names, "name lookup requires a names index");
            let mut name_index = 0;
            for (offset, header) in self.entries.iter().enumerate() {
                if matches_name(&header.name) {
                    let index = u32::try_from(offset + 1).expect("table index fits in u32");
                    if header.value.as_ref() == value {
                        return (index, 0);
                    }
                    if name_index == 0 {
                        name_index = index;
                    }
                }
            }
            (0, name_index)
        }

        fn name_index(&self, name: &HeaderName) -> u32 {
            assert!(self.index_names, "name lookup requires a names index");
            self.entries
                .iter()
                .position(|header| names_equal(&header.name, name))
                .and_then(|index| u32::try_from(index + 1).ok())
                .unwrap_or(0)
        }

        fn has_name(&self, name: &HeaderName) -> bool {
            assert!(self.index_names, "name lookup requires a names index");
            self.entries
                .iter()
                .any(|header| names_equal(&header.name, name))
        }

        const fn max_table_length(&self) -> u32 {
            self.capacity >> 5
        }

        fn set_capacity(&mut self, capacity: u32) -> bool {
            if capacity < self.capacity {
                while self.bytes > capacity {
                    let Some(evicted) = self.entries.pop_back() else {
                        return false;
                    };
                    self.bytes -= evicted.bytes();
                }
            } else if capacity > self.capacity {
                self.slot_len = self.slot_len.max(initial_table_length(capacity));
            }
            self.capacity = capacity;
            true
        }

        fn clear(&mut self) {
            self.entries.clear();
            self.bytes = 0;
        }
    }

    const fn initial_table_length(capacity: u32) -> usize {
        let maximum = capacity >> 5;
        if maximum == 1 {
            1
        } else {
            (maximum / 2) as usize
        }
    }

    fn names_equal(left: &HeaderName, right: &HeaderName) -> bool {
        left.code() == right.code()
            && (left.is_common_header() || left.as_bytes() == right.as_bytes())
    }

    struct XorShift64(u64);

    impl XorShift64 {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, bound: usize) -> usize {
            usize::try_from(self.next() % u64::try_from(bound).expect("bound fits in u64"))
                .expect("a value below a usize bound fits in usize")
        }
    }

    const NAMES: [&[u8]; 16] = [
        b":authority",
        b":path",
        b"accept-encoding",
        b"cache-control",
        b"content-type",
        b"cookie",
        b"date",
        b"user-agent",
        b"x-request-id",
        b"x-response-id",
        b"x-\x80",
        b"allow",
        b"x-y",
        b"x-lookup-\xc3\xa4",
        b"x-custom-header-sixteen-plus",
        b"x-long-custom-header-name-beyond-any-stack-buffer-0123456789-abcdefghij",
    ];
    const VALUES: [&[u8]; 5] = [b"", b"v", b"gzip", b"0123456789abcdef", b"\0\xff"];
    const MISSING_VALUE: &[u8] = b"never-added";

    enum Operation {
        Add(HpackHeader),
        SetCapacity(u32),
        Clear,
    }

    fn random_case(rng: &mut XorShift64, name: &[u8]) -> Vec<u8> {
        name.iter()
            .map(|&byte| {
                if rng.next() & 1 == 0 {
                    byte.to_ascii_uppercase()
                } else {
                    byte
                }
            })
            .collect()
    }

    fn next_operation(
        rng: &mut XorShift64,
        base: u32,
        capacity: u32,
        unique: &mut u64,
    ) -> Operation {
        let pooled = NAMES[rng.below(NAMES.len())];
        let name = random_case(rng, pooled);
        match rng.below(1000) {
            0..=9 => Operation::SetCapacity([0, 64, 256, base, 65536][rng.below(5)]),
            10..=12 => Operation::Clear,
            13..=17 => Operation::Add(HpackHeader::new(name, vec![b'o'; capacity as usize])),
            _ if rng.next() & 1 == 0 => {
                Operation::Add(HpackHeader::new(name, VALUES[rng.below(VALUES.len())]))
            }
            _ => {
                *unique += 1;
                Operation::Add(HpackHeader::new(name, format!("u{unique}")))
            }
        }
    }

    fn apply(operation: &Operation, table: &mut HeaderTable, linear: &mut LinearTable) {
        match operation {
            Operation::Add(header) => {
                assert_eq!(table.add(header.clone()), linear.add(header.clone()));
            }
            Operation::SetCapacity(capacity) => {
                assert_eq!(
                    table.set_capacity(*capacity),
                    linear.set_capacity(*capacity)
                );
            }
            Operation::Clear => {
                table.clear();
                linear.clear();
            }
        }
    }

    fn assert_same_state(table: &HeaderTable, linear: &LinearTable, base: u32, step: usize) {
        assert!(
            table.entries().eq(linear.entries.iter()),
            "entries differ at base {base} step {step}"
        );
        assert_eq!(
            (
                table.size(),
                table.bytes(),
                table.insert_count(),
                table.allocated_slots(),
                table.capacity(),
            ),
            (
                u32::try_from(linear.entries.len()).expect("table size fits in u32"),
                linear.bytes,
                linear.insert_count,
                linear.slot_len,
                linear.capacity,
            ),
            "state differs at base {base} step {step}"
        );
    }

    fn assert_same_lookups(
        table: &HeaderTable,
        linear: &LinearTable,
        rng: &mut XorShift64,
        name: &[u8],
        base: u32,
        step: usize,
    ) {
        let query = random_case(rng, name);
        let header_name = HeaderName::new(&query);
        let stored: Vec<&[u8]> = linear
            .entries
            .iter()
            .filter(|header| names_equal(&header.name, &header_name))
            .map(|header| header.value.as_ref())
            .collect();
        let stored_value = if stored.is_empty() {
            VALUES[1]
        } else {
            stored[rng.below(stored.len())]
        };
        let shown = String::from_utf8_lossy(&query);
        for value in [stored_value, MISSING_VALUE, b""] {
            assert_eq!(
                table.get_index_parts(&header_name, value),
                linear.get_index_parts(&header_name, value),
                "get_index_parts({shown}, {}) at base {base} step {step}",
                String::from_utf8_lossy(value)
            );
            assert_eq!(
                table.get_unknown_index_parts(&query, value),
                linear.get_unknown_index_parts(&query, value),
                "get_unknown_index_parts({shown}, {}) at base {base} step {step}",
                String::from_utf8_lossy(value)
            );
        }
        assert_eq!(
            table.name_index(&header_name),
            linear.name_index(&header_name),
            "name_index({shown}) at base {base} step {step}"
        );
        assert_eq!(
            table.has_name(&header_name),
            linear.has_name(&header_name),
            "has_name({shown}) at base {base} step {step}"
        );
    }

    fn run_against_linear_scan(base: u32, operations: usize, seed: u64, names_index: bool) {
        let mut rng = XorShift64(seed);
        let mut table = HeaderTable::new(base);
        let mut linear = LinearTable::new(base);
        if !names_index {
            table.disable_names_index();
            linear.disable_names_index();
        }
        let mut unique = 0;
        for step in 0..operations {
            let operation = next_operation(&mut rng, base, table.capacity(), &mut unique);
            apply(&operation, &mut table, &mut linear);
            assert_same_state(&table, &linear, base, step);
            if !names_index {
                continue;
            }
            if step % 64 == 63 || step + 1 == operations {
                for name in NAMES {
                    assert_same_lookups(&table, &linear, &mut rng, name, base, step);
                }
            } else {
                let name = NAMES[step % NAMES.len()];
                assert_same_lookups(&table, &linear, &mut rng, name, base, step);
            }
        }
    }

    fn assert_indices_match_positions(table: &HeaderTable, step: usize) {
        for position in 1..=table.size() {
            let stored = table.get_header(position).expect("position is live");
            let newest = (1..=position)
                .find(|&earlier| table.get_header(earlier) == Some(stored))
                .expect("the position itself holds the entry");
            assert_eq!(
                table.get_index_parts(&stored.name, &stored.value),
                (newest, 0),
                "lookup of {stored} at position {position}, step {step}"
            );
        }
    }

    #[test]
    fn lookups_stay_exact_across_ordinal_wraparound() {
        fn small(name: &str, value: &str) -> Operation {
            Operation::Add(HpackHeader::new(name, value))
        }
        fn big(name: &str, fill: u8) -> Operation {
            Operation::Add(HpackHeader::new(name, [fill; 1000]))
        }

        let start = u32::MAX - 8;
        let mut table = HeaderTable::with_next_ordinal(4096, start);
        let mut linear = LinearTable::new(4096);
        let mut rng = XorShift64(0x0DD5_EED5_0F0E_D1A7);
        let operations = [
            small(":path", "/a"),
            small("x-request-id", "1"),
            small(":path", "/b"),
            small("X-Request-Id", "2"),
            small("accept-encoding", "gzip"),
            small("x-request-id", "1"),
            Operation::SetCapacity(160),
            Operation::SetCapacity(4096),
            small("x-y", "v"),
            small(":path", "/a"),
            small("x-request-id", "3"),
            small(":authority", "c1"),
            big("x-request-id", b'a'),
            big(":path", b'b'),
            big("x-request-id", b'c'),
            big("x-y", b'd'),
            small("x-request-id", "1"),
            Operation::SetCapacity(1100),
            Operation::SetCapacity(4096),
            Operation::Clear,
            small(":path", "/a"),
            big("x-request-id", b'e'),
            big("x-request-id", b'f'),
            big("x-request-id", b'g'),
            big("x-request-id", b'h'),
            small("x-request-id", "1"),
            Operation::SetCapacity(0),
            Operation::SetCapacity(4096),
            small("x-request-id", "2"),
            small(":path", "/b"),
        ];
        for (step, operation) in operations.iter().enumerate() {
            apply(operation, &mut table, &mut linear);
            assert_same_state(&table, &linear, start, step);
            for name in NAMES {
                assert_same_lookups(&table, &linear, &mut rng, name, start, step);
            }
            assert_indices_match_positions(&table, step);
        }
        assert!(
            table.next_ordinal < start,
            "the ordinal wrapped: next_ordinal is {}",
            table.next_ordinal
        );
    }

    /// The consumer-shaped request table in steady state: `:authority c1`, `:path /echo`, then 66
    /// `x-request-id` values.
    fn consumer_steady_state() -> HeaderTable {
        let mut table = HeaderTable::new(4096);
        assert!(table.add(HpackHeader::new(":authority", "c1")));
        assert!(table.add(HpackHeader::new(":path", "/echo")));
        for id in 0..66_u64 {
            assert!(table.add(HpackHeader::new("x-request-id", format!("{id:016x}"))));
        }
        assert_eq!(table.size(), 68, "66 * 60 + 44 + 42 = 4046 bytes fit");
        table
    }

    #[test]
    fn unknown_lookup_examines_only_same_name_entries() {
        let table = consumer_steady_state();
        HeaderTable::take_examined();

        for name in [&b"x-request-id"[..], b"X-Request-Id"] {
            let shown = String::from_utf8_lossy(name);
            assert_eq!(
                table.get_unknown_index_parts(name, b"00000000000000ff"),
                (0, 1),
                "a new {shown} value names the newest x-request-id entry"
            );
            let examined = HeaderTable::take_examined();
            assert!(
                examined <= 66,
                "a new {shown} value reads at most the 66 x-request-id entries, read {examined}"
            );
        }
    }

    #[test]
    fn unknown_unique_value_miss_examines_at_most_one_entry() {
        let table = consumer_steady_state();
        HeaderTable::take_examined();

        for name in [&b"x-request-id"[..], b"X-Request-Id"] {
            let shown = String::from_utf8_lossy(name);
            assert_eq!(
                table.get_unknown_index_parts(name, b"00000000000000ff"),
                (0, 1),
                "a new {shown} value names the newest x-request-id entry"
            );
            let examined = HeaderTable::take_examined();
            assert!(
                examined <= 1,
                "a new {shown} value reads at most one entry, read {examined}"
            );

            for (id, index) in [(0_u64, 66_u32), (40, 26), (65, 1)] {
                let value = format!("{id:016x}");
                assert_eq!(
                    table.get_unknown_index_parts(name, value.as_bytes()),
                    (index, 0),
                    "{shown} {value} is the {index}th newest entry (id 65 is the newest)"
                );
                let examined = HeaderTable::take_examined();
                assert!(
                    examined <= 1,
                    "an existing {shown} value reads at most one entry, read {examined}"
                );
            }
        }
    }

    #[test]
    fn encoding_a_unique_header_hashes_it_once() {
        let mut table = consumer_steady_state();
        // The table holds 4046 of 4096 bytes, so each 60-byte add evicts one entry: first
        // `:authority c1` (44 bytes), then `:path /echo` (42), then the oldest `x-request-id` entries.
        let evicted_names = [":authority", ":path", "x-request-id", "x-request-id"];
        for (id, evicted_name) in (0x100_u64..).zip(evicted_names) {
            let value = format!("{id:016x}");
            let oldest = table
                .get_header(table.size())
                .expect("the table is not empty")
                .clone();
            assert_eq!(
                oldest.name.as_bytes(),
                evicted_name.as_bytes(),
                "the oldest entry is next to go"
            );
            HeaderTable::take_header_hashes();

            assert_eq!(
                table.get_unknown_index_parts(b"x-request-id", value.as_bytes()),
                (0, 1),
                "a new x-request-id value names the newest x-request-id entry"
            );
            assert!(table.add_missed(HpackHeader::new("x-request-id", value.as_str())));
            let hashes = HeaderTable::take_header_hashes();
            assert_eq!(
                table.get_unknown_index_parts(b"x-request-id", value.as_bytes()),
                (1, 0),
                "the added header is the newest exact match"
            );

            assert_eq!(table.size(), 68, "the add evicts exactly one entry");
            assert!(
                !table.entries().any(|header| *header == oldest),
                "the add evicts {oldest}"
            );
            assert_eq!(
                hashes, 1,
                "the lookup, add and eviction of x-request-id {value} hash it once, hashed {hashes} times"
            );
        }
    }

    #[test]
    fn table_lookups_compute_no_siphash() {
        let mut table = consumer_steady_state();
        let value = b"0000000000000100";
        HeaderTable::take_header_hashes();

        assert_eq!(
            table.get_unknown_index_parts(b"x-request-id", value),
            (0, 1),
            "a new x-request-id value names the newest x-request-id entry"
        );
        assert!(table.add_missed(HpackHeader::new("x-request-id", &value[..])));
        assert_eq!(
            HeaderTable::take_header_hashes(),
            1,
            "the lookup and add of a new x-request-id hash it once"
        );

        assert_eq!(
            table.get_unknown_index_parts(b"x-request-id", value),
            (1, 0),
            "the added header is the newest exact match"
        );
        assert_eq!(
            HeaderTable::take_header_hashes(),
            1,
            "the second lookup hashes the header once"
        );
    }

    #[test]
    fn encoder_add_reuses_the_miss_hashes_without_a_copy_of_the_header() {
        let mut table = consumer_steady_state();
        let value = b"0000000000000100";
        HeaderTable::take_header_hashes();

        assert_eq!(
            table.get_unknown_index_parts(b"x-request-id", value),
            (0, 1),
            "a new x-request-id value names the newest x-request-id entry"
        );
        assert!(table.add_missed(HpackHeader::new("x-request-id", &value[..])));
        assert_eq!(
            HeaderTable::take_header_hashes(),
            1,
            "the encoder's lookup and add of a new x-request-id hash it once"
        );
        assert_eq!(
            table.get_unknown_index_parts(b"x-request-id", value),
            (1, 0),
            "the added header is the newest exact match"
        );
        assert_eq!(
            size_of::<LastMiss>(),
            size_of::<Cell<Option<HeaderHashes>>>(),
            "a miss keeps the header's hashes and no copy of its name or value"
        );
    }

    #[test]
    fn tables_draw_independent_seeds() {
        let key = NameKey::Other(b"x-request-id");
        let first = HeaderTable::new(4096).hash_header(key, b"v");
        let second = HeaderTable::new(4096).hash_header(key, b"v");
        assert_ne!(
            first.pair, second.pair,
            "each table draws its own seed, so two tables hash a header apart (a false failure has \
             probability 2^-64)"
        );

        let seeded = || HeaderTable {
            state: TableHash(SeedableRandomState::with_seed(
                0x5EED,
                SharedSeed::global_fixed(),
            )),
            ..HeaderTable::new(4096)
        };
        assert_eq!(
            seeded().hash_header(key, b"v").pair,
            seeded().hash_header(key, b"v").pair,
            "control: tables that share a seed hash a header alike, so the seed alone separates them"
        );
    }

    #[test]
    fn hash_separates_name_and_value_boundaries() {
        let table = HeaderTable::new(4096);
        let pair = |name: &[u8], value: &[u8]| table.hash_header(NameKey::Other(name), value).pair;
        assert_ne!(
            pair(b"x-ab", b"c"),
            pair(b"x-a", b"bc"),
            "where the name ends and the value starts is part of the hash"
        );

        let values: Vec<Vec<u8>> = [7, 8, 9, 15, 16, 17]
            .into_iter()
            .flat_map(|length| {
                let ending = |last| {
                    let mut value = vec![b'v'; length];
                    value[length - 1] = last;
                    value
                };
                [ending(b'a'), ending(b'b'), [ending(b'a'), vec![0]].concat()]
            })
            .collect();
        let mut pairs: Vec<u64> = values
            .iter()
            .map(|value| pair(b"x-request-id", value))
            .collect();
        pairs.sort_unstable();
        pairs.dedup();
        assert_eq!(
            pairs.len(),
            values.len(),
            "values of length 7, 8, 9, 15, 16 and 17 that differ only in their last byte, or by a \
             trailing zero byte, hash apart"
        );
    }

    #[test]
    fn a_lookup_keeps_no_copy_of_a_header_too_big_to_add() {
        let table = consumer_steady_state();
        let oversized_name = vec![b'n'; 4096];
        let oversized_value = vec![b'o'; 4096];
        for (name, value) in [
            (&b"x-request-id"[..], &oversized_value[..]),
            (&oversized_name[..], b"v"),
        ] {
            assert_eq!(table.get_unknown_index_parts(name, value).0, 0);
            assert_eq!(
                table.last_miss.0.get(),
                None,
                "a {}-byte header cannot be added to the 4096-byte table, so the lookup keeps nothing of it",
                name.len() + value.len()
            );
        }
    }

    #[test]
    fn unknown_lookup_ignores_common_entries_with_same_spelling() {
        let mut table = HeaderTable::new(4096);
        let mut linear = LinearTable::new(4096);
        for header in [
            HpackHeader::new("content-length", ""),
            HpackHeader::new("allow", ""),
        ] {
            assert!(table.add(header.clone()));
            assert!(linear.add(header));
        }
        let common: Vec<bool> = table
            .entries()
            .map(|stored| stored.name.is_common_header())
            .collect();
        assert_eq!(
            common,
            [false, true],
            "content-length has a common code; allow has none (static_table_header_names_are_common)"
        );
        HeaderTable::take_examined();

        for name in [&b"Content-Length"[..], b"content-length"] {
            let shown = String::from_utf8_lossy(name);
            assert_eq!(linear.get_unknown_index_parts(name, b""), (0, 0));
            assert_eq!(
                table.get_unknown_index_parts(name, b""),
                (0, 0),
                "{shown} passed as an unknown name misses the common content-length entry"
            );
            assert_eq!(
                HeaderTable::take_examined(),
                0,
                "{shown} passed as an unknown name reads no entry"
            );
        }

        assert_eq!(linear.get_unknown_index_parts(b"Allow", b""), (1, 0));
        assert_eq!(
            table.get_unknown_index_parts(b"Allow", b""),
            (1, 0),
            "allow is an Other name, so the unknown path finds it"
        );
        assert_eq!(
            HeaderTable::take_examined(),
            1,
            "the allow hit reads only the allow entry"
        );
    }

    #[test]
    fn mixed_case_unknown_lookup_matches_the_oracle_around_the_stack_buffer() {
        for length in [63_u8, 64, 65, 100] {
            let mut stored: Vec<u8> = (0..length).map(|offset| b'a' + offset % 26).collect();
            stored[3] = 0xc4;
            let mixed = random_case(&mut XorShift64(0x5EED ^ u64::from(length)), &stored);
            let mut folded_beyond_ascii = mixed.clone();
            folded_beyond_ascii[3] = 0xe4;
            assert!(
                mixed.iter().any(u8::is_ascii_uppercase),
                "the {length}-byte query is mixed case"
            );
            let mut table = HeaderTable::new(4096);
            let mut linear = LinearTable::new(4096);
            for header in [
                HpackHeader::new(&stored, "v"),
                HpackHeader::new("x-request-id", "v"),
                HpackHeader::new(&stored, "w"),
            ] {
                assert!(table.add(header.clone()));
                assert!(linear.add(header));
            }

            let cases = [
                (&stored, &b"v"[..], (3, 0)),
                (&stored, b"w", (1, 0)),
                (&stored, b"x", (0, 1)),
                (&mixed, b"v", (3, 0)),
                (&mixed, b"w", (1, 0)),
                (&mixed, b"x", (0, 1)),
                (&folded_beyond_ascii, b"v", (0, 0)),
            ];
            for (query, value, expected) in cases {
                let shown = format!(
                    "{}: {}",
                    String::from_utf8_lossy(query),
                    String::from_utf8_lossy(value)
                );
                assert_eq!(
                    linear.get_unknown_index_parts(query, value),
                    expected,
                    "the oracle for the {length}-byte {shown}"
                );
                assert_eq!(
                    table.get_unknown_index_parts(query, value),
                    expected,
                    "the {length}-byte {shown}"
                );
            }
        }
    }

    #[test]
    fn exact_index_tracks_the_newest_duplicate_across_eviction() {
        fn add(name: &str, value: &str) -> Operation {
            Operation::Add(HpackHeader::new(name, value))
        }
        const PROBES: [(&str, &str); 6] = [
            ("a", "1"),
            ("a", "9"),
            ("b", "2"),
            ("c", "3"),
            ("d", "4"),
            ("e", "5"),
        ];

        // Every entry is 32 + 1 + 1 = 34 bytes, so 102 bytes hold exactly three. Comments show the
        // entries newest first after each step.
        let steps = [
            (add("a", "1"), vec![(("a", "1"), (1, 0))]),
            (add("b", "2"), vec![(("a", "1"), (2, 0))]),
            // a:1 b:2 a:1
            (
                add("a", "1"),
                vec![(("a", "1"), (1, 0)), (("b", "2"), (2, 0))],
            ),
            // a:9 a:1 b:2, the oldest a:1 evicted: the duplicate still matches.
            (
                add("a", "9"),
                vec![(("a", "1"), (2, 0)), (("a", "9"), (1, 0))],
            ),
            // c:3 a:9 a:1
            (
                add("c", "3"),
                vec![(("a", "1"), (3, 0)), (("b", "2"), (0, 0))],
            ),
            // d:4 c:3 a:9, the duplicate evicted: a:9 still names a.
            (
                add("d", "4"),
                vec![(("a", "1"), (0, 3)), (("a", "9"), (3, 0))],
            ),
            // e:5 d:4 c:3, no a left.
            (
                add("e", "5"),
                vec![(("a", "1"), (0, 0)), (("a", "9"), (0, 0))],
            ),
            (add("a", "1"), vec![(("a", "1"), (1, 0))]),
            // a:1 a:1 e:5
            (
                add("a", "1"),
                vec![(("a", "1"), (1, 0)), (("e", "5"), (3, 0))],
            ),
            // a:1 a:1
            (
                Operation::SetCapacity(68),
                vec![(("a", "1"), (1, 0)), (("e", "5"), (0, 0))],
            ),
            // a:1, the older duplicate evicted.
            (Operation::SetCapacity(34), vec![(("a", "1"), (1, 0))]),
            (Operation::SetCapacity(0), vec![(("a", "1"), (0, 0))]),
            (Operation::SetCapacity(102), vec![(("a", "1"), (0, 0))]),
            (add("a", "1"), vec![(("a", "1"), (1, 0))]),
            // b:2 a:1
            (
                add("b", "2"),
                vec![(("a", "1"), (2, 0)), (("b", "2"), (1, 0))],
            ),
            (
                Operation::Clear,
                vec![(("a", "1"), (0, 0)), (("b", "2"), (0, 0))],
            ),
            // b:2
            (
                add("b", "2"),
                vec![(("a", "1"), (0, 0)), (("b", "2"), (1, 0))],
            ),
        ];

        let mut table = HeaderTable::new(102);
        let mut linear = LinearTable::new(102);
        for (step, (operation, expected)) in steps.iter().enumerate() {
            apply(operation, &mut table, &mut linear);
            assert_same_state(&table, &linear, 102, step);
            for &((name, value), indices) in expected {
                assert_eq!(
                    table.get_index_parts(&HeaderName::new(name), value.as_bytes()),
                    indices,
                    "{name}: {value} at step {step}"
                );
            }
            for (name, value) in PROBES {
                let header_name = HeaderName::new(name);
                assert_eq!(
                    table.get_index_parts(&header_name, value.as_bytes()),
                    linear.get_index_parts(&header_name, value.as_bytes()),
                    "get_index_parts({name}, {value}) at step {step}"
                );
                assert_eq!(
                    table.get_unknown_index_parts(name.as_bytes(), value.as_bytes()),
                    linear.get_unknown_index_parts(name.as_bytes(), value.as_bytes()),
                    "get_unknown_index_parts({name}, {value}) at step {step}"
                );
            }
        }
    }

    #[test]
    fn exact_index_falls_back_on_hash_collision() {
        for base in [256, 4096] {
            let mut rng = XorShift64(0xC011_1DED_5EED_0001 ^ u64::from(base));
            let mut table = HeaderTable::new(base);
            table.exact.colliding = true;
            let mut linear = LinearTable::new(base);
            let mut unique = 0;
            for step in 0..2_000 {
                let operation = next_operation(&mut rng, base, table.capacity(), &mut unique);
                apply(&operation, &mut table, &mut linear);
                assert_same_state(&table, &linear, base, step);
                assert!(
                    table.exact.newest.len() <= 1,
                    "every pair shares one hash at base {base} step {step}"
                );
                for name in NAMES {
                    assert_same_lookups(&table, &linear, &mut rng, name, base, step);
                }
            }
        }
    }

    #[test]
    fn lookups_match_the_oracle_when_names_collide() {
        // With pair hashes colliding too, every exact-index mapping that holds another pair sends the
        // lookup to the same-name scan, so the scan's name check is exercised as well.
        for (base, pairs_collide) in [(256, false), (4096, false), (256, true), (4096, true)] {
            let mut rng = XorShift64(0xC011_1DED_5EED_0002 ^ u64::from(base));
            let mut table = HeaderTable::new(base);
            table.names.colliding = true;
            table.exact.colliding = pairs_collide;
            let mut linear = LinearTable::new(base);
            let mut unique = 0;
            for step in 0..2_000 {
                let operation = next_operation(&mut rng, base, table.capacity(), &mut unique);
                apply(&operation, &mut table, &mut linear);
                assert_same_state(&table, &linear, base, step);
                assert!(
                    table.names.other.len() <= 1,
                    "every name without a common code shares one hash at base {base} step {step}"
                );
                for name in NAMES {
                    assert_same_lookups(&table, &linear, &mut rng, name, base, step);
                }
            }
        }
    }

    #[test]
    fn lookup_matches_frozen_linear_scan_over_random_operations() {
        for (base, operations) in [(256, 20_000), (4096, 20_000), (65536, 3_000)] {
            let seed = 0x9E37_79B9_7F4A_7C15 ^ u64::from(base);
            run_against_linear_scan(base, operations, seed, true);
            run_against_linear_scan(base, operations, seed, false);
        }
    }
}
