//! The ordered list behind a pool's sessions.
//!
//! A slab of nodes doubly linked in the pool's order, where a `VecDeque` searched for a
//! session and shifted the others to move or remove it: an item whose slot is known is
//! found, removed or moved to the back in constant time. A node may also be linked into one
//! of two class lists, which keep the main order: an owned HTTP/1.1 pool's selectable
//! sessions, busy or idle (S6.9). Freed nodes are reused before the slab grows, so the list
//! allocates only when it holds more items than it ever has.

/// An item that records the slot the list gives it.
pub(super) trait Linked {
    fn set_slot(&self, slot: Option<Slot>);
}

/// Where an item is linked.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Slot(u32);

/// One of the two class lists a node may also be linked into.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Class {
    Busy,
    Idle,
}

impl Class {
    const fn index(self) -> usize {
        match self {
            Self::Busy => 0,
            Self::Idle => 1,
        }
    }
}

/// The first and last node of a list.
#[derive(Clone, Copy, Default)]
struct Ends {
    head: Option<u32>,
    tail: Option<u32>,
}

/// Links of one list through a node.
#[derive(Clone, Copy, Default)]
struct Links {
    prev: Option<u32>,
    next: Option<u32>,
}

struct Node<T> {
    /// `None` while the node is free; a free node's `order.next` links the free list.
    item: Option<T>,
    order: Links,
    class: Option<Class>,
    class_links: Links,
}

pub(super) struct SessionList<T> {
    nodes: Vec<Node<T>>,
    free: Option<u32>,
    order: Ends,
    classes: [Ends; 2],
    len: usize,
}

impl<T: Linked> SessionList<T> {
    pub(super) const fn new() -> Self {
        Self {
            nodes: Vec::new(),
            free: None,
            order: Ends {
                head: None,
                tail: None,
            },
            classes: [Ends {
                head: None,
                tail: None,
            }; 2],
            len: 0,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Whether `slot` holds an item.
    pub(super) fn holds(&self, slot: Slot) -> bool {
        self.nodes
            .get(slot.0 as usize)
            .is_some_and(|node| node.item.is_some())
    }

    fn node(&self, index: u32) -> &Node<T> {
        &self.nodes[index as usize]
    }

    fn node_mut(&mut self, index: u32) -> &mut Node<T> {
        &mut self.nodes[index as usize]
    }

    pub(super) fn get(&self, slot: Slot) -> &T {
        self.node(slot.0)
            .item
            .as_ref()
            .expect("a linked slot holds its item")
    }

    pub(super) fn get_mut(&mut self, slot: Slot) -> &mut T {
        self.node_mut(slot.0)
            .item
            .as_mut()
            .expect("a linked slot holds its item")
    }

    pub(super) fn front(&self) -> Option<Slot> {
        self.order.head.map(Slot)
    }

    pub(super) fn next(&self, slot: Slot) -> Option<Slot> {
        self.node(slot.0).order.next.map(Slot)
    }

    pub(super) fn push_back(&mut self, item: T) -> Slot {
        self.insert_before(None, item)
    }

    /// Inserts `item` before `next`, or at the back.
    pub(super) fn insert_before(&mut self, next: Option<Slot>, item: T) -> Slot {
        let index = match self.free {
            Some(index) => {
                let node = self.node_mut(index);
                let free = node.order.next;
                node.item = Some(item);
                node.order = Links::default();
                self.free = free;
                index
            }
            None => {
                let index =
                    u32::try_from(self.nodes.len()).expect("a pool holds fewer than 2^32 sessions");
                self.nodes.push(Node {
                    item: Some(item),
                    order: Links::default(),
                    class: None,
                    class_links: Links::default(),
                });
                index
            }
        };
        self.link_before(index, next.map(|slot| slot.0));
        self.len += 1;
        let slot = Slot(index);
        self.get(slot).set_slot(Some(slot));
        slot
    }

    fn link_before(&mut self, index: u32, next: Option<u32>) {
        let prev = match next {
            Some(next) => self.node(next).order.prev,
            None => self.order.tail,
        };
        self.node_mut(index).order = Links { prev, next };
        match prev {
            Some(prev) => self.node_mut(prev).order.next = Some(index),
            None => self.order.head = Some(index),
        }
        match next {
            Some(next) => self.node_mut(next).order.prev = Some(index),
            None => self.order.tail = Some(index),
        }
    }

    fn unlink(&mut self, index: u32) {
        let Links { prev, next } = std::mem::take(&mut self.node_mut(index).order);
        match prev {
            Some(prev) => self.node_mut(prev).order.next = next,
            None => self.order.head = next,
        }
        match next {
            Some(next) => self.node_mut(next).order.prev = prev,
            None => self.order.tail = prev,
        }
    }

    /// Unlinks the node and frees it, keeping its item.
    fn detach(&mut self, index: u32) -> T {
        self.set_class(Slot(index), None);
        self.unlink(index);
        self.len -= 1;
        let free = self.free;
        let node = self.node_mut(index);
        let item = node.item.take().expect("a linked slot holds its item");
        node.order.next = free;
        self.free = Some(index);
        item.set_slot(None);
        item
    }

    pub(super) fn remove(&mut self, slot: Slot) -> T {
        self.detach(slot.0)
    }

    /// `VecDeque::pop_front`, which the list's differential test against a `VecDeque` uses.
    #[cfg(test)]
    pub(super) fn pop_front(&mut self) -> Option<T> {
        self.order.head.map(|index| self.detach(index))
    }

    /// Moves the item to the back of the list, and of its class list.
    pub(super) fn move_to_back(&mut self, slot: Slot) {
        if self.order.tail != Some(slot.0) {
            self.unlink(slot.0);
            self.link_before(slot.0, None);
        }
        if let Some(class) = self.node(slot.0).class {
            self.class_unlink(slot.0, class);
            self.class_link_back(slot.0, class);
        }
    }

    /// The first item, in order, that `matches`.
    pub(super) fn find(&self, mut matches: impl FnMut(&T) -> bool) -> Option<Slot> {
        let mut cursor = self.order.head;
        while let Some(index) = cursor {
            let node = self.node(index);
            if matches(node.item.as_ref().expect("a linked slot holds its item")) {
                return Some(Slot(index));
            }
            cursor = node.order.next;
        }
        None
    }

    /// Removes the items that `keep` refuses, testing each in order, and drops them, in
    /// order, once every item is tested, as `VecDeque::retain` drops after its pass.
    pub(super) fn retain(&mut self, mut keep: impl FnMut(&T) -> bool) {
        let mut cursor = self.order.head;
        let mut removed = Ends::default();
        while let Some(index) = cursor {
            cursor = self.node(index).order.next;
            if keep(self.get(Slot(index))) {
                continue;
            }
            // Park the node, still holding its item, on a list of removed nodes.
            self.set_class(Slot(index), None);
            self.unlink(index);
            self.len -= 1;
            self.node_mut(index).order.prev = removed.tail;
            match removed.tail {
                Some(tail) => self.node_mut(tail).order.next = Some(index),
                None => removed.head = Some(index),
            }
            removed.tail = Some(index);
        }
        while let Some(index) = removed.head {
            let free = self.free;
            let node = self.node_mut(index);
            removed.head = node.order.next;
            let item = node.item.take().expect("a removed node holds its item");
            node.order = Links {
                prev: None,
                next: free,
            };
            self.free = Some(index);
            item.set_slot(None);
            drop(item);
        }
    }

    pub(super) fn for_each_mut(&mut self, mut update: impl FnMut(&mut T)) {
        let mut cursor = self.order.head;
        while let Some(index) = cursor {
            let node = self.node_mut(index);
            cursor = node.order.next;
            update(node.item.as_mut().expect("a linked slot holds its item"));
        }
    }

    pub(super) fn iter(&self) -> Iter<'_, T> {
        Iter {
            list: self,
            cursor: self.order.head,
        }
    }

    /// Each item with its slot, in order.
    pub(super) fn entries(&self) -> impl Iterator<Item = (Slot, &T)> {
        let mut cursor = self.order.head;
        std::iter::from_fn(move || {
            let index = cursor?;
            let node = self.node(index);
            cursor = node.order.next;
            node.item.as_ref().map(|item| (Slot(index), item))
        })
    }

    pub(super) fn class(&self, slot: Slot) -> Option<Class> {
        self.node(slot.0).class
    }

    /// Unlinks the node from its class list, then links it at the back of `class`'s.
    pub(super) fn set_class(&mut self, slot: Slot, class: Option<Class>) {
        if let Some(current) = self.node(slot.0).class {
            self.class_unlink(slot.0, current);
        }
        if let Some(class) = class {
            self.class_link_back(slot.0, class);
        }
    }

    fn class_link_back(&mut self, index: u32, class: Class) {
        let ends = &mut self.classes[class.index()];
        let prev = ends.tail;
        ends.tail = Some(index);
        if prev.is_none() {
            ends.head = Some(index);
        }
        let node = self.node_mut(index);
        node.class = Some(class);
        node.class_links = Links { prev, next: None };
        if let Some(prev) = prev {
            self.node_mut(prev).class_links.next = Some(index);
        }
    }

    fn class_unlink(&mut self, index: u32, class: Class) {
        let node = self.node_mut(index);
        node.class = None;
        let Links { prev, next } = std::mem::take(&mut node.class_links);
        match prev {
            Some(prev) => self.node_mut(prev).class_links.next = next,
            None => self.classes[class.index()].head = next,
        }
        match next {
            Some(next) => self.node_mut(next).class_links.prev = prev,
            None => self.classes[class.index()].tail = prev,
        }
    }

    pub(super) fn class_front(&self, class: Class) -> Option<Slot> {
        self.classes[class.index()].head.map(Slot)
    }

    pub(super) fn class_back(&self, class: Class) -> Option<Slot> {
        self.classes[class.index()].tail.map(Slot)
    }

    pub(super) fn class_next(&self, slot: Slot) -> Option<Slot> {
        self.node(slot.0).class_links.next.map(Slot)
    }

    pub(super) fn class_prev(&self, slot: Slot) -> Option<Slot> {
        self.node(slot.0).class_links.prev.map(Slot)
    }

    /// Unlinks every node from its class list.
    pub(super) fn clear_classes(&mut self) {
        for class in [Class::Busy, Class::Idle] {
            while let Some(index) = self.classes[class.index()].head {
                self.class_unlink(index, class);
            }
        }
    }

    #[cfg(test)]
    fn allocated_slots(&self) -> usize {
        self.nodes.len()
    }
}

pub(super) struct Iter<'list, T> {
    list: &'list SessionList<T>,
    cursor: Option<u32>,
}

impl<'list, T> Iterator for Iter<'list, T> {
    type Item = &'list T;

    fn next(&mut self) -> Option<Self::Item> {
        let node = &self.list.nodes[self.cursor? as usize];
        self.cursor = node.order.next;
        node.item.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::rc::Rc;

    use super::*;

    /// An item that records its slot, and its drop in a shared log.
    struct Item {
        id: u32,
        slot: Cell<Option<Slot>>,
        drops: Rc<RefCell<Vec<u32>>>,
    }

    impl Linked for Item {
        fn set_slot(&self, slot: Option<Slot>) {
            self.slot.set(slot);
        }
    }

    impl Drop for Item {
        fn drop(&mut self) {
            self.drops.borrow_mut().push(self.id);
        }
    }

    struct Fixture {
        list: SessionList<Item>,
        drops: Rc<RefCell<Vec<u32>>>,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                list: SessionList::new(),
                drops: Rc::default(),
            }
        }

        fn item(&self, id: u32) -> Item {
            Item {
                id,
                slot: Cell::new(None),
                drops: Rc::clone(&self.drops),
            }
        }

        fn push(&mut self, id: u32) -> Slot {
            let item = self.item(id);
            self.list.push_back(item)
        }

        fn ids(&self) -> Vec<u32> {
            self.list.iter().map(|item| item.id).collect()
        }

        fn class_ids(&self, class: Class) -> Vec<u32> {
            let mut ids = Vec::new();
            let mut cursor = self.list.class_front(class);
            while let Some(slot) = cursor {
                ids.push(self.list.get(slot).id);
                cursor = self.list.class_next(slot);
            }
            ids
        }

        /// Every linked item records its own slot.
        fn assert_slots_recorded(&self) {
            let mut cursor = self.list.front();
            while let Some(slot) = cursor {
                assert_eq!(self.list.get(slot).slot.get(), Some(slot));
                cursor = self.list.next(slot);
            }
        }
    }

    #[test]
    fn items_keep_their_insertion_order_and_record_their_slots() {
        let mut fixture = Fixture::new();
        assert!(fixture.list.is_empty());
        let slots = [fixture.push(1), fixture.push(2), fixture.push(3)];
        assert_eq!(fixture.ids(), [1, 2, 3]);
        assert_eq!(fixture.list.len(), 3);
        assert!(!fixture.list.is_empty());
        for (slot, id) in slots.into_iter().zip([1, 2, 3]) {
            assert!(fixture.list.holds(slot));
            assert_eq!(fixture.list.get(slot).id, id);
            assert_eq!(fixture.list.get(slot).slot.get(), Some(slot));
        }
        fixture.list.get_mut(slots[1]).id = 20;
        assert_eq!(fixture.ids(), [1, 20, 3]);
    }

    #[test]
    fn removal_unlinks_an_item_from_any_position_and_clears_its_slot() {
        let mut fixture = Fixture::new();
        let slots = [1, 2, 3, 4].map(|id| fixture.push(id));
        let middle = fixture.list.remove(slots[1]);
        assert_eq!(middle.id, 2);
        assert_eq!(middle.slot.get(), None);
        assert!(!fixture.list.holds(slots[1]));
        assert_eq!(fixture.ids(), [1, 3, 4]);
        assert_eq!(fixture.list.remove(slots[3]).id, 4);
        assert_eq!(fixture.ids(), [1, 3]);
        assert_eq!(fixture.list.pop_front().map(|item| item.id), Some(1));
        assert_eq!(fixture.ids(), [3]);
        assert_eq!(fixture.list.remove(slots[2]).id, 3);
        assert!(fixture.list.is_empty());
        assert_eq!(fixture.list.front(), None);
        assert!(fixture.list.pop_front().is_none());
    }

    #[test]
    fn moving_an_item_to_the_back_keeps_the_others_in_order() {
        let mut fixture = Fixture::new();
        let slots = [1, 2, 3].map(|id| fixture.push(id));
        fixture.list.move_to_back(slots[0]);
        assert_eq!(fixture.ids(), [2, 3, 1]);
        fixture.list.move_to_back(slots[0]);
        assert_eq!(fixture.ids(), [2, 3, 1]);
        fixture.list.move_to_back(slots[2]);
        assert_eq!(fixture.ids(), [2, 1, 3]);
        fixture.assert_slots_recorded();
    }

    #[test]
    fn freed_slots_are_reused_before_the_list_grows() {
        let mut fixture = Fixture::new();
        let slots = [1, 2, 3].map(|id| fixture.push(id));
        drop(fixture.list.remove(slots[0]));
        drop(fixture.list.remove(slots[2]));
        fixture.push(4);
        fixture.push(5);
        assert_eq!(fixture.list.allocated_slots(), 3);
        assert_eq!(fixture.ids(), [2, 4, 5]);
        fixture.push(6);
        assert_eq!(fixture.list.allocated_slots(), 4);
        fixture.assert_slots_recorded();
    }

    #[test]
    fn retain_drops_the_removed_items_after_every_test_in_order() {
        let mut fixture = Fixture::new();
        [1, 2, 3, 4, 5].into_iter().for_each(|id| {
            fixture.push(id);
        });
        let drops = Rc::clone(&fixture.drops);
        let mut tested = Vec::new();
        fixture.list.retain(|item| {
            // No removed item is dropped until every item is tested.
            assert!(drops.borrow().is_empty());
            tested.push(item.id);
            item.id % 2 == 0
        });
        assert_eq!(tested, [1, 2, 3, 4, 5]);
        assert_eq!(*fixture.drops.borrow(), [1, 3, 5]);
        assert_eq!(fixture.ids(), [2, 4]);
        fixture.assert_slots_recorded();
    }

    #[test]
    fn find_and_for_each_mut_visit_items_in_order() {
        let mut fixture = Fixture::new();
        [5, 6, 7].into_iter().for_each(|id| {
            fixture.push(id);
        });
        let mut visited = Vec::new();
        let found = fixture.list.find(|item| {
            visited.push(item.id);
            item.id >= 6
        });
        assert_eq!(found.map(|slot| fixture.list.get(slot).id), Some(6));
        assert_eq!(visited, [5, 6]);
        assert_eq!(fixture.list.find(|item| item.id > 7), None);
        fixture.list.for_each_mut(|item| item.id *= 10);
        assert_eq!(fixture.ids(), [50, 60, 70]);
    }

    #[test]
    fn entries_pair_each_item_with_its_slot_in_order() {
        let mut fixture = Fixture::new();
        let slots = [7, 8, 9].map(|id| fixture.push(id));
        fixture.list.move_to_back(slots[0]);
        let entries: Vec<_> = fixture
            .list
            .entries()
            .map(|(slot, item)| (slot, item.id))
            .collect();
        assert_eq!(entries, [(slots[1], 8), (slots[2], 9), (slots[0], 7)]);
    }

    /// A deterministic xorshift generator.
    struct Random(u64);

    impl Random {
        fn below(&mut self, bound: usize) -> usize {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            usize::try_from(self.0 % u64::try_from(bound).unwrap()).unwrap()
        }
    }

    /// The slot at `position` in the list's order.
    fn slot_at(list: &SessionList<Item>, position: usize) -> Slot {
        let mut slot = list.front().expect("a position within the list");
        for _ in 0..position {
            slot = list.next(slot).expect("a position within the list");
        }
        slot
    }

    /// Every operation the pool used on its `VecDeque` leaves the list in the
    /// same order as the `VecDeque`.
    #[test]
    fn operations_leave_the_same_order_as_a_vec_deque() {
        let mut fixture = Fixture::new();
        let mut model: VecDeque<u32> = VecDeque::new();
        let mut random = Random(0x9e37_79b9_7f4a_7c15);
        let mut next_id = 0;
        for _ in 0..4000 {
            let len = model.len();
            match random.below(7) {
                0 | 1 => {
                    next_id += 1;
                    fixture.push(next_id);
                    model.push_back(next_id);
                }
                2 if len > 0 => {
                    let position = random.below(len);
                    let slot = slot_at(&fixture.list, position);
                    assert_eq!(
                        fixture.list.remove(slot).id,
                        model.remove(position).unwrap()
                    );
                }
                3 if len > 0 => {
                    let position = random.below(len);
                    let slot = slot_at(&fixture.list, position);
                    fixture.list.move_to_back(slot);
                    let id = model.remove(position).unwrap();
                    model.push_back(id);
                }
                4 => {
                    next_id += 1;
                    let position = random.below(len + 1);
                    let next = (position < len).then(|| slot_at(&fixture.list, position));
                    let item = fixture.item(next_id);
                    fixture.list.insert_before(next, item);
                    model.insert(position, next_id);
                }
                5 => {
                    assert_eq!(
                        fixture.list.pop_front().map(|item| item.id),
                        model.pop_front()
                    );
                }
                6 => {
                    let modulus = u32::try_from(random.below(5) + 2).unwrap();
                    fixture.list.retain(|item| item.id % modulus != 0);
                    model.retain(|id| id % modulus != 0);
                }
                _ => {}
            }
            assert_eq!(fixture.ids(), Vec::from(model.clone()));
            assert_eq!(fixture.list.len(), model.len());
        }
        fixture.assert_slots_recorded();
    }

    #[test]
    fn class_lists_keep_the_main_order_through_moves_and_removals() {
        let mut fixture = Fixture::new();
        let slots = [1, 2, 3, 4, 5].map(|id| fixture.push(id));
        fixture.list.set_class(slots[0], Some(Class::Busy));
        fixture.list.set_class(slots[1], Some(Class::Idle));
        fixture.list.set_class(slots[3], Some(Class::Idle));
        fixture.list.set_class(slots[4], Some(Class::Idle));
        assert_eq!(fixture.class_ids(Class::Busy), [1]);
        assert_eq!(fixture.class_ids(Class::Idle), [2, 4, 5]);
        assert_eq!(fixture.list.class(slots[2]), None);
        assert_eq!(fixture.list.class(slots[3]), Some(Class::Idle));
        assert_eq!(
            fixture
                .list
                .class_back(Class::Idle)
                .map(|slot| fixture.list.get(slot).id),
            Some(5)
        );
        // Moving to the back moves to the back of its class list too.
        fixture.list.move_to_back(slots[1]);
        assert_eq!(fixture.ids(), [1, 3, 4, 5, 2]);
        assert_eq!(fixture.class_ids(Class::Idle), [4, 5, 2]);
        // An unclassified move leaves the class lists alone.
        fixture.list.move_to_back(slots[2]);
        assert_eq!(fixture.class_ids(Class::Idle), [4, 5, 2]);
        // Reclassifying links at the back of the new class.
        fixture.list.set_class(slots[3], Some(Class::Busy));
        assert_eq!(fixture.class_ids(Class::Busy), [1, 4]);
        assert_eq!(fixture.class_ids(Class::Idle), [5, 2]);
        // Removal, a pop and a retain unlink from the class list.
        drop(fixture.list.remove(slots[4]));
        assert_eq!(fixture.class_ids(Class::Idle), [2]);
        drop(fixture.list.pop_front());
        assert_eq!(fixture.class_ids(Class::Busy), [4]);
        fixture.list.retain(|item| item.id != 4);
        assert_eq!(fixture.class_ids(Class::Busy), Vec::<u32>::new());
        assert_eq!(fixture.list.class_front(Class::Busy), None);
        // Unclassifying, and clearing every class.
        fixture.list.set_class(slots[1], None);
        assert_eq!(fixture.class_ids(Class::Idle), Vec::<u32>::new());
        fixture.list.set_class(slots[1], Some(Class::Idle));
        fixture.list.set_class(slots[2], Some(Class::Busy));
        fixture.list.clear_classes();
        assert_eq!(fixture.class_ids(Class::Idle), Vec::<u32>::new());
        assert_eq!(fixture.class_ids(Class::Busy), Vec::<u32>::new());
        assert_eq!(fixture.list.class(slots[1]), None);
        // [1, 2, 3, 4, 5] -> moves of 2 and 3 -> [1, 4, 5, 2, 3] -> less 5, 1 and 4.
        assert_eq!(fixture.ids(), [2, 3]);
    }

    #[test]
    fn a_reused_slot_starts_unclassified() {
        let mut fixture = Fixture::new();
        let first = fixture.push(1);
        fixture.list.set_class(first, Some(Class::Idle));
        drop(fixture.list.remove(first));
        let second = fixture.push(2);
        assert_eq!(second, first);
        assert_eq!(fixture.list.class(second), None);
        assert_eq!(fixture.list.class_front(Class::Idle), None);
    }
}
