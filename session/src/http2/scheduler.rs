use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::collections::btree_map::Entry as MapEntry;

use roxygen_core::ids::StreamId;
use roxygen_core::priority::HttpPriority;

const URGENCY_LEVELS: usize = 8;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct OrderedStream {
    order_id: u32,
    stream: StreamId,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PriorityEntry {
    priority: HttpPriority,
    enqueued: bool,
}

#[derive(Default)]
pub(crate) struct PriorityQueue {
    ordered: [BTreeSet<OrderedStream>; URGENCY_LEVELS],
    incremental: [VecDeque<StreamId>; URGENCY_LEVELS],
    // Paused entries retain enqueue intent without occupying either queue.
    nonempty_urgencies: u8,
}

/// RFC 9218 scheduler used by an HTTP/2 session.
///
/// Non-incremental work is ordered before incremental work at the same
/// urgency. Incremental work is round-robin and rotates only after bytes were
/// consumed for the selected stream.
#[derive(Default)]
pub struct Http2PriorityScheduler {
    entries: BTreeMap<StreamId, PriorityEntry>,
    queues: PriorityQueue,
}

impl Http2PriorityScheduler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, stream: StreamId, priority: HttpPriority) -> bool {
        let MapEntry::Vacant(entry) = self.entries.entry(stream) else {
            return false;
        };
        entry.insert(PriorityEntry {
            priority,
            enqueued: false,
        });
        true
    }

    pub fn update(&mut self, stream: StreamId, priority: HttpPriority) -> bool {
        let Some(entry) = self.entries.get_mut(&stream) else {
            return false;
        };
        self.queues.update(stream, entry, priority);
        true
    }

    pub fn enqueue(&mut self, stream: StreamId) -> bool {
        let Some(entry) = self.entries.get_mut(&stream) else {
            return false;
        };
        self.queues.enqueue(stream, entry)
    }

    pub fn pause(&mut self, stream: StreamId, paused: bool) -> bool {
        let Some(mut priority) = self.priority(stream) else {
            return false;
        };
        priority.paused = paused;
        self.update(stream, priority)
    }

    pub fn next(&self) -> Option<StreamId> {
        self.queues.head()
    }

    pub fn next_matching(&self, eligible: impl FnMut(StreamId) -> bool) -> Option<StreamId> {
        self.queues.next_matching(eligible)
    }

    pub fn consume(&mut self, stream: StreamId, bytes: usize) -> bool {
        let Some(entry) = self.entries.get(&stream) else {
            return false;
        };
        self.queues.consume(stream, entry, bytes)
    }

    pub fn dequeue(&mut self, stream: StreamId) -> bool {
        let Some(entry) = self.entries.get_mut(&stream) else {
            return false;
        };
        self.queues.dequeue(stream, entry)
    }

    pub fn remove(&mut self, stream: StreamId) -> bool {
        let Some(mut entry) = self.entries.remove(&stream) else {
            return false;
        };
        self.queues.dequeue(stream, &mut entry);
        true
    }

    pub fn priority(&self, stream: StreamId) -> Option<HttpPriority> {
        self.entries.get(&stream).map(|entry| entry.priority)
    }

    pub fn contains(&self, stream: StreamId) -> bool {
        self.entries.contains_key(&stream)
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl PriorityQueue {
    pub(crate) fn update(
        &mut self,
        stream: StreamId,
        entry: &mut PriorityEntry,
        priority: HttpPriority,
    ) {
        let previous = *entry;
        if previous.enqueued {
            self.remove(stream, previous.priority);
        }
        entry.priority = priority;
        if previous.enqueued && !priority.paused {
            self.insert(stream, priority);
            entry.enqueued = true;
        }
    }

    pub(crate) fn enqueue(&mut self, stream: StreamId, entry: &mut PriorityEntry) -> bool {
        if entry.enqueued || entry.priority.paused {
            return false;
        }
        self.insert(stream, entry.priority);
        entry.enqueued = true;
        true
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.nonempty_urgencies == 0
    }

    /// Equivalent to `next_matching(|_| true)`.
    pub(crate) fn head(&self) -> Option<StreamId> {
        let mut urgencies = self.nonempty_urgencies;
        while urgencies != 0 {
            let urgency = urgencies.trailing_zeros() as usize;
            urgencies &= urgencies - 1;
            if let Some(entry) = self.ordered[urgency].first() {
                return Some(entry.stream);
            }
            if let Some(stream) = self.incremental[urgency].front() {
                return Some(*stream);
            }
        }
        None
    }

    pub(crate) fn next_matching(
        &self,
        mut eligible: impl FnMut(StreamId) -> bool,
    ) -> Option<StreamId> {
        let mut urgencies = self.nonempty_urgencies;
        while urgencies != 0 {
            let urgency = urgencies.trailing_zeros() as usize;
            urgencies &= urgencies - 1;
            if let Some(stream) = self.ordered[urgency]
                .iter()
                .map(|entry| entry.stream)
                .find(|stream| eligible(*stream))
            {
                return Some(stream);
            }
            if let Some(stream) = self.incremental[urgency]
                .iter()
                .copied()
                .find(|stream| eligible(*stream))
            {
                return Some(stream);
            }
        }
        None
    }

    pub(crate) fn consume(
        &mut self,
        stream: StreamId,
        entry: &PriorityEntry,
        bytes: usize,
    ) -> bool {
        if !entry.enqueued || bytes == 0 || !entry.priority.incremental {
            return entry.enqueued;
        }
        let queue = &mut self.incremental[usize::from(entry.priority.urgency.min(7))];
        if let Some(position) = queue.iter().position(|queued| *queued == stream) {
            queue.remove(position);
            queue.push_back(stream);
        }
        true
    }

    pub(crate) fn dequeue(&mut self, stream: StreamId, entry: &mut PriorityEntry) -> bool {
        if !entry.enqueued {
            return false;
        }
        self.remove(stream, entry.priority);
        entry.enqueued = false;
        true
    }

    fn insert(&mut self, stream: StreamId, priority: HttpPriority) {
        let urgency = usize::from(priority.urgency.min(7));
        if priority.incremental {
            self.incremental[urgency].push_back(stream);
        } else {
            self.ordered[urgency].insert(OrderedStream {
                order_id: priority.order_id,
                stream,
            });
        }
        self.nonempty_urgencies |= 1 << urgency;
    }

    fn remove(&mut self, stream: StreamId, priority: HttpPriority) {
        let urgency = usize::from(priority.urgency.min(7));
        if priority.incremental {
            let queue = &mut self.incremental[urgency];
            if queue.front() == Some(&stream) {
                queue.pop_front();
            } else if let Some(index) = queue.iter().position(|queued| *queued == stream) {
                queue.remove(index);
            }
        } else {
            self.ordered[urgency].remove(&OrderedStream {
                order_id: priority.order_id,
                stream,
            });
        }
        if self.ordered[urgency].is_empty() && self.incremental[urgency].is_empty() {
            self.nonempty_urgencies &= !(1 << urgency);
        }
    }
}
