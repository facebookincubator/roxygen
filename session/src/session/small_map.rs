//! Ordered maps that keep up to two entries inline before using a `BTreeMap`.
//!
//! Observable behavior matches `BTreeMap` and `BTreeSet`: ascending key order
//! for every iterator and for drops, `insert` keeps an existing key and returns
//! the replaced value, and destruction continues past a panicking drop. A map
//! that has grown into a tree stays a tree until it is replaced.

use std::collections::BTreeMap;
use std::collections::btree_map;
use std::iter::Flatten;
use std::mem;
use std::ops::Index;

const INLINE_CAPACITY: usize = 2;

pub(crate) struct SmallMap<K, V> {
    repr: Repr<K, V>,
}

enum Repr<K, V> {
    // A sorted contiguous prefix: `slots[1]` is occupied only when `slots[0]` is.
    Inline([Option<(K, V)>; INLINE_CAPACITY]),
    Tree(BTreeMap<K, V>),
}

impl<K, V> Default for SmallMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> SmallMap<K, V> {
    pub(crate) const fn new() -> Self {
        Self {
            repr: Repr::Inline([None, None]),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match &self.repr {
            Repr::Inline(slots) => slots.iter().filter(|slot| slot.is_some()).count(),
            Repr::Tree(tree) => tree.len(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match &self.repr {
            Repr::Inline(slots) => slots[0].is_none(),
            Repr::Tree(tree) => tree.is_empty(),
        }
    }

    #[cfg(test)]
    pub(crate) fn is_inline_for_test(&self) -> bool {
        matches!(self.repr, Repr::Inline(_))
    }

    pub(crate) fn iter(&self) -> Iter<'_, K, V> {
        match &self.repr {
            Repr::Inline(slots) => Iter::Inline(slots.iter().flatten()),
            Repr::Tree(tree) => Iter::Tree(tree.iter()),
        }
    }

    pub(crate) fn keys(&self) -> Keys<'_, K, V> {
        Keys(self.iter())
    }

    pub(crate) fn values(&self) -> Values<'_, K, V> {
        Values(self.iter())
    }

    pub(crate) fn into_values(self) -> IntoValues<K, V> {
        IntoValues(self.into_iter())
    }
}

impl<K: Ord, V> SmallMap<K, V> {
    fn inline_position(slots: &[Option<(K, V)>; INLINE_CAPACITY], key: &K) -> Result<usize, usize> {
        for (index, slot) in slots.iter().enumerate() {
            match slot {
                Some((existing, _)) => match existing.cmp(key) {
                    std::cmp::Ordering::Less => {}
                    std::cmp::Ordering::Equal => return Ok(index),
                    std::cmp::Ordering::Greater => return Err(index),
                },
                None => return Err(index),
            }
        }
        Err(INLINE_CAPACITY)
    }

    pub(crate) fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    pub(crate) fn get(&self, key: &K) -> Option<&V> {
        match &self.repr {
            Repr::Inline(slots) => Self::inline_position(slots, key)
                .ok()
                .and_then(|index| slots[index].as_ref())
                .map(|(_, value)| value),
            Repr::Tree(tree) => tree.get(key),
        }
    }

    pub(crate) fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        match &mut self.repr {
            Repr::Inline(slots) => match Self::inline_position(slots, key) {
                Ok(index) => slots[index].as_mut().map(|(_, value)| value),
                Err(_) => None,
            },
            Repr::Tree(tree) => tree.get_mut(key),
        }
    }

    pub(crate) fn insert(&mut self, key: K, value: V) -> Option<V> {
        match self.entry(key) {
            Entry::Occupied(mut entry) => Some(entry.insert(value)),
            Entry::Vacant(entry) => {
                entry.insert(value);
                None
            }
        }
    }

    pub(crate) fn remove(&mut self, key: &K) -> Option<V> {
        match &mut self.repr {
            Repr::Inline(slots) => match Self::inline_position(slots, key) {
                Ok(index) => Some(remove_inline(slots, index).1),
                Err(_) => None,
            },
            Repr::Tree(tree) => tree.remove(key),
        }
    }

    pub(crate) fn entry(&mut self, key: K) -> Entry<'_, K, V> {
        let found = match &self.repr {
            Repr::Inline(slots) => Self::inline_position(slots, &key).is_ok(),
            Repr::Tree(tree) => tree.contains_key(&key),
        };
        if found {
            Entry::Occupied(OccupiedEntry { map: self, key })
        } else {
            Entry::Vacant(VacantEntry { map: self, key })
        }
    }

    /// Visits entries in ascending order and drops each rejected entry before
    /// the next visit, leaving the map consistent if the predicate or a drop
    /// panics.
    pub(crate) fn retain(&mut self, mut keep: impl FnMut(&K, &mut V) -> bool) {
        match &mut self.repr {
            Repr::Inline(slots) => {
                let mut index = 0;
                while let Some((key, value)) = slots.get_mut(index).and_then(Option::as_mut) {
                    if keep(key, value) {
                        index += 1;
                    } else {
                        drop(remove_inline(slots, index));
                    }
                }
            }
            Repr::Tree(tree) => tree.retain(keep),
        }
    }
}

fn remove_inline<K, V>(slots: &mut [Option<(K, V)>; INLINE_CAPACITY], index: usize) -> (K, V) {
    let removed = slots[index]
        .take()
        .expect("an inline position holds an entry");
    if index == 0 {
        slots.swap(0, 1);
    }
    removed
}

impl<K: Ord, V> Index<&K> for SmallMap<K, V> {
    type Output = V;

    fn index(&self, key: &K) -> &V {
        self.get(key).expect("no entry found for key")
    }
}

impl<K, V> IntoIterator for SmallMap<K, V> {
    type Item = (K, V);
    type IntoIter = IntoIter<K, V>;

    fn into_iter(self) -> IntoIter<K, V> {
        match self.repr {
            Repr::Inline(slots) => IntoIter::Inline { slots, next: 0 },
            Repr::Tree(tree) => IntoIter::Tree(tree.into_iter()),
        }
    }
}

impl<'a, K, V> IntoIterator for &'a SmallMap<K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = Iter<'a, K, V>;

    fn into_iter(self) -> Iter<'a, K, V> {
        self.iter()
    }
}

pub(crate) enum Entry<'a, K: Ord, V> {
    Occupied(OccupiedEntry<'a, K, V>),
    Vacant(VacantEntry<'a, K, V>),
}

impl<'a, K: Ord, V> Entry<'a, K, V> {
    pub(crate) fn or_insert_with(self, default: impl FnOnce() -> V) -> &'a mut V {
        match self {
            Self::Occupied(entry) => entry.into_mut(),
            Self::Vacant(entry) => entry.insert(default()),
        }
    }

    pub(crate) fn or_default(self) -> &'a mut V
    where
        V: Default,
    {
        self.or_insert_with(V::default)
    }
}

pub(crate) struct OccupiedEntry<'a, K: Ord, V> {
    map: &'a mut SmallMap<K, V>,
    key: K,
}

impl<'a, K: Ord, V> OccupiedEntry<'a, K, V> {
    pub(crate) fn get(&self) -> &V {
        self.map
            .get(&self.key)
            .expect("an occupied entry has a value")
    }

    pub(crate) fn get_mut(&mut self) -> &mut V {
        self.map
            .get_mut(&self.key)
            .expect("an occupied entry has a value")
    }

    pub(crate) fn into_mut(self) -> &'a mut V {
        self.map
            .get_mut(&self.key)
            .expect("an occupied entry has a value")
    }

    pub(crate) fn insert(&mut self, value: V) -> V {
        mem::replace(self.get_mut(), value)
    }

    pub(crate) fn remove(self) -> V {
        self.map
            .remove(&self.key)
            .expect("an occupied entry has a value")
    }
}

pub(crate) struct VacantEntry<'a, K: Ord, V> {
    map: &'a mut SmallMap<K, V>,
    key: K,
}

impl<'a, K: Ord, V> VacantEntry<'a, K, V> {
    pub(crate) fn insert(self, value: V) -> &'a mut V {
        let Self { map, key } = self;
        if let Repr::Inline(slots) = &mut map.repr
            && slots[INLINE_CAPACITY - 1].is_some()
        {
            let [first, second] = mem::take(slots);
            map.repr = Repr::Tree(first.into_iter().chain(second).collect());
        }
        match &mut map.repr {
            Repr::Inline(slots) => {
                let index = SmallMap::inline_position(slots, &key)
                    .expect_err("a vacant entry has no inline value");
                if index == 0 {
                    slots.swap(0, 1);
                }
                &mut slots[index].insert((key, value)).1
            }
            Repr::Tree(tree) => tree.entry(key).or_insert(value),
        }
    }
}

pub(crate) enum Iter<'a, K, V> {
    Inline(Flatten<std::slice::Iter<'a, Option<(K, V)>>>),
    Tree(btree_map::Iter<'a, K, V>),
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Inline(entries) => entries.next().map(|(key, value)| (key, value)),
            Self::Tree(entries) => entries.next(),
        }
    }
}

impl<K, V> DoubleEndedIterator for Iter<'_, K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Inline(entries) => entries.next_back().map(|(key, value)| (key, value)),
            Self::Tree(entries) => entries.next_back(),
        }
    }
}

pub(crate) struct Keys<'a, K, V>(Iter<'a, K, V>);

impl<'a, K, V> Iterator for Keys<'a, K, V> {
    type Item = &'a K;

    fn next(&mut self) -> Option<&'a K> {
        self.0.next().map(|(key, _)| key)
    }
}

impl<K, V> DoubleEndedIterator for Keys<'_, K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back().map(|(key, _)| key)
    }
}

pub(crate) struct Values<'a, K, V>(Iter<'a, K, V>);

impl<'a, K, V> Iterator for Values<'a, K, V> {
    type Item = &'a V;

    fn next(&mut self) -> Option<&'a V> {
        self.0.next().map(|(_, value)| value)
    }
}

impl<K, V> DoubleEndedIterator for Values<'_, K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back().map(|(_, value)| value)
    }
}

/// Remaining entries drop in ascending order, continuing past a panicking drop,
/// when the iterator is dropped.
pub(crate) enum IntoIter<K, V> {
    Inline {
        slots: [Option<(K, V)>; INLINE_CAPACITY],
        next: usize,
    },
    Tree(btree_map::IntoIter<K, V>),
}

impl<K, V> Iterator for IntoIter<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<(K, V)> {
        match self {
            Self::Inline { slots, next } => {
                let entry = slots.get_mut(*next)?.take()?;
                *next += 1;
                Some(entry)
            }
            Self::Tree(entries) => entries.next(),
        }
    }
}

pub(crate) struct IntoValues<K, V>(IntoIter<K, V>);

impl<K, V> Iterator for IntoValues<K, V> {
    type Item = V;

    fn next(&mut self) -> Option<V> {
        self.0.next().map(|(_, value)| value)
    }
}

/// An ordered set with the same inline storage as [`SmallMap`].
pub(crate) struct SmallSet<T> {
    map: SmallMap<T, ()>,
}

impl<T> Default for SmallSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> SmallSet<T> {
    pub(crate) const fn new() -> Self {
        Self {
            map: SmallMap::new(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }
}

impl<T: Ord> SmallSet<T> {
    pub(crate) fn contains(&self, value: &T) -> bool {
        self.map.contains_key(value)
    }

    /// Returns whether the value was newly inserted; an equal stored value is kept.
    pub(crate) fn insert(&mut self, value: T) -> bool {
        match self.map.entry(value) {
            Entry::Occupied(_) => false,
            Entry::Vacant(entry) => {
                entry.insert(());
                true
            }
        }
    }

    pub(crate) fn remove(&mut self, value: &T) -> bool {
        self.map.remove(value).is_some()
    }
}
