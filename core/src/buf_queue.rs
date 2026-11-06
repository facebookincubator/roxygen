use std::collections::VecDeque;
use std::collections::vec_deque::Iter;
use std::io::IoSlice;

use bytes::Buf;
use bytes::Bytes;
use thiserror::Error;

/// Maximum visible length and requested allocation for compact wire segments.
pub const COMPACT_WRITE_LIMIT: usize = 2048;

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum BufQueueError {
    #[error("buffer length overflow: {current} existing bytes plus {incoming} incoming bytes")]
    LengthOverflow { current: usize, incoming: usize },
    #[error(
        "buffer limit {limit} exceeded: {current} existing bytes plus {incoming} incoming bytes"
    )]
    LimitExceeded {
        limit: usize,
        current: usize,
        incoming: usize,
    },
    #[error("cannot trim {requested} bytes from a buffer containing {available} bytes")]
    TrimUnderflow { requested: usize, available: usize },
    #[error("cannot coalesce {length} bytes with an explicit maximum of {limit}")]
    CoalesceLimitExceeded { limit: usize, length: usize },
}

/// A queue of immutable byte segments bounded by their visible logical length.
#[derive(Debug)]
pub struct BufQueue {
    segments: Segments,
    length: usize,
    limit: usize,
    backing_capacity: Option<usize>,
}

/// The smallest capacity `VecDeque` growth allocates for `Bytes` segments.
const MIN_SEGMENT_CAPACITY: usize = 4;

/// The largest refill hint, bounding a hinted container at 2 KiB.
const MAX_SEGMENT_HINT: u16 = 64;

#[derive(Clone, Debug)]
enum Segments {
    /// At most one segment, plus the refill hint: the capacity of the last
    /// container that left this queue, capped at [`MAX_SEGMENT_HINT`]. Both
    /// variants carry the hint in the tag word's padding, adding no size.
    Inline(Bytes, u16),
    /// A container allocated at the smallest capacity, as without a hint. Only
    /// its first growth past that jumps to the hint, so fills of at most
    /// [`MIN_SEGMENT_CAPACITY`] segments allocate exactly as unhinted queues
    /// do, and such a fill leaves a small hint behind.
    Many(VecDeque<Bytes>, u16),
}

fn segment_hint(capacity: usize) -> u16 {
    capacity.min(usize::from(MAX_SEGMENT_HINT)) as u16
}

/// Makes room for `incoming` more segments. Growth reaches at least `hint`,
/// and otherwise follows `VecDeque`'s own doubling.
fn reserve_hinted(segments: &mut VecDeque<Bytes>, incoming: usize, hint: usize) {
    let needed = segments.len() + incoming;
    if needed > segments.capacity() {
        segments.reserve(needed.max(hint) - segments.len());
    }
}

impl Segments {
    fn push_back(&mut self, bytes: Bytes) {
        match self {
            Self::Inline(segment, _) if segment.is_empty() => *segment = bytes,
            Self::Inline(segment, hint) => {
                let mut segments = VecDeque::with_capacity(MIN_SEGMENT_CAPACITY);
                segments.push_back(std::mem::take(segment));
                segments.push_back(bytes);
                *self = Self::Many(segments, *hint);
            }
            Self::Many(segments, hint) => {
                reserve_hinted(segments, 1, usize::from(*hint));
                segments.push_back(bytes);
            }
        }
    }

    fn pop_front(&mut self) -> Option<Bytes> {
        match self {
            Self::Inline(segment, _) => (!segment.is_empty()).then(|| std::mem::take(segment)),
            Self::Many(segments, _) => segments.pop_front(),
        }
    }

    /// Moves every segment out and leaves this queue unallocated. A departing
    /// container leaves its capacity behind as the refill hint.
    fn take(&mut self) -> Self {
        match self {
            Self::Inline(segment, _) => Self::Inline(std::mem::take(segment), 0),
            Self::Many(segments, _) => {
                let hint = segment_hint(segments.capacity());
                let mut taken = std::mem::replace(self, Self::Inline(Bytes::new(), hint));
                taken.set_hint(0);
                taken
            }
        }
    }

    fn departure_hint(&self) -> u16 {
        match self {
            Self::Inline(_, hint) => *hint,
            Self::Many(segments, _) => segment_hint(segments.capacity()),
        }
    }

    fn set_hint(&mut self, hint: u16) {
        match self {
            Self::Inline(_, own) | Self::Many(_, own) => *own = hint,
        }
    }

    /// Swaps `other`'s segments into this unallocated queue. Each queue keeps
    /// its own hint, except that a departing container sets `other`'s.
    fn adopt(&mut self, other: &mut Self) {
        let hint = self.departure_hint();
        let departing = other.departure_hint();
        std::mem::swap(self, other);
        self.set_hint(hint);
        other.set_hint(departing);
    }

    /// Moves the segments that hold the first `amount` bytes, splitting a
    /// boundary segment. `amount` must be nonzero and below the visible length.
    ///
    /// A multi-segment result takes one container sized for its segments, or
    /// takes this container when at most one segment remains here.
    fn split_to(&mut self, amount: usize) -> Self {
        let segments = match self {
            Self::Inline(segment, _) => return Self::Inline(segment.split_to(amount), 0),
            Self::Many(segments, _) => segments,
        };
        let mut remaining = amount;
        let whole = segments
            .iter()
            .position(|segment| {
                let covered = remaining != 0 && segment.len() <= remaining;
                if covered {
                    remaining -= segment.len();
                }
                !covered
            })
            .expect("a split below the visible length ends before the last segment");
        let partial = remaining != 0;
        let taken = whole + usize::from(partial);
        let boundary = "a split below the visible length leaves a segment";
        if taken == 1 {
            let segment = if partial {
                segments.front_mut().expect(boundary).split_to(remaining)
            } else {
                segments.pop_front().expect(boundary)
            };
            return Self::Inline(segment, 0);
        }
        if segments.len() - whole == 1 {
            let hint = segment_hint(segments.capacity());
            let mut prefix = std::mem::take(segments);
            let last = prefix.back_mut().expect(boundary);
            let rest = if partial {
                let head = last.split_to(remaining);
                std::mem::replace(last, head)
            } else {
                prefix.pop_back().expect(boundary)
            };
            *self = Self::Inline(rest, hint);
            return Self::Many(prefix, 0);
        }
        let mut prefix = VecDeque::with_capacity(taken.max(MIN_SEGMENT_CAPACITY));
        prefix.extend(segments.drain(..whole));
        if partial {
            prefix.push_back(segments.front_mut().expect(boundary).split_to(remaining));
        }
        Self::Many(prefix, 0)
    }

    /// Before `source` is appended, gives a container that the append would
    /// allocate or grow room for at least `preferred` segments. Appends that
    /// allocate nothing keep their shape: an unallocated queue adopts the
    /// source container, and an inline segment moves into a source container
    /// with room.
    fn grow_for(&mut self, source: &Self, preferred: usize) {
        let incoming = match source {
            Self::Inline(..) => 1,
            Self::Many(segments, _) => segments.len(),
        };
        match (&mut *self, source) {
            (Self::Inline(segment, _), _) if segment.is_empty() => {}
            (Self::Inline(..), Self::Many(source, _)) if source.len() < source.capacity() => {}
            (Self::Inline(segment, hint), _) => {
                let capacity = (1 + incoming).max(preferred).max(MIN_SEGMENT_CAPACITY);
                let mut segments = VecDeque::with_capacity(capacity);
                segments.push_back(std::mem::take(segment));
                *self = Self::Many(segments, *hint);
            }
            (Self::Many(segments, hint), _) if segments.capacity() != 0 => {
                reserve_hinted(segments, incoming, preferred.max(usize::from(*hint)));
            }
            (Self::Many(..), _) => {}
        }
    }

    /// Moves the segments that hold the first `amount` bytes to the back of
    /// `destination`, splitting a boundary segment. `amount` must be nonzero
    /// and below the visible length.
    fn move_prefix_to(&mut self, amount: usize, destination: &mut Self) {
        let segments = match self {
            Self::Inline(segment, _) => return destination.push_back(segment.split_to(amount)),
            Self::Many(segments, _) => segments,
        };
        let boundary = "a prefix below the visible length ends before the last segment";
        let mut remaining = amount;
        loop {
            let front = segments.front_mut().expect(boundary);
            if front.len() > remaining {
                destination.push_back(front.split_to(remaining));
                return;
            }
            remaining -= front.len();
            destination.push_back(segments.pop_front().expect(boundary));
            if remaining == 0 {
                return;
            }
        }
    }
}

#[derive(Clone, Debug)]
enum SegmentRefs<'a> {
    Inline(std::option::IntoIter<&'a Bytes>),
    Many(Iter<'a, Bytes>),
}

/// A borrowed iterator over a queue's immutable byte segments.
///
/// Cloning preserves the current position without copying payloads.
#[derive(Clone, Debug)]
pub struct BufQueueSegments<'a> {
    inner: SegmentRefs<'a>,
}

impl<'a> Iterator for BufQueueSegments<'a> {
    type Item = &'a Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            SegmentRefs::Inline(segments) => segments.next(),
            SegmentRefs::Many(segments) => segments.next(),
        }
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        match &mut self.inner {
            SegmentRefs::Inline(segments) => segments.nth(n),
            SegmentRefs::Many(segments) => segments.nth(n),
        }
    }

    fn count(self) -> usize {
        match self.inner {
            SegmentRefs::Inline(segments) => segments.count(),
            SegmentRefs::Many(segments) => segments.count(),
        }
    }

    fn last(self) -> Option<Self::Item> {
        match self.inner {
            SegmentRefs::Inline(segments) => segments.last(),
            SegmentRefs::Many(segments) => segments.last(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let length = self.len();
        (length, Some(length))
    }
}

impl ExactSizeIterator for BufQueueSegments<'_> {
    fn len(&self) -> usize {
        match &self.inner {
            SegmentRefs::Inline(segments) => segments.len(),
            SegmentRefs::Many(segments) => segments.len(),
        }
    }
}

enum IntoSegments {
    Inline(std::option::IntoIter<Bytes>),
    Many(std::collections::vec_deque::IntoIter<Bytes>),
}

impl Iterator for IntoSegments {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Inline(segments) => segments.next(),
            Self::Many(segments) => segments.next(),
        }
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        match self {
            Self::Inline(segments) => segments.nth(n),
            Self::Many(segments) => segments.nth(n),
        }
    }

    fn count(self) -> usize {
        match self {
            Self::Inline(segments) => segments.count(),
            Self::Many(segments) => segments.count(),
        }
    }

    fn last(self) -> Option<Self::Item> {
        match self {
            Self::Inline(segments) => segments.last(),
            Self::Many(segments) => segments.last(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let length = self.len();
        (length, Some(length))
    }
}

impl ExactSizeIterator for IntoSegments {
    fn len(&self) -> usize {
        match self {
            Self::Inline(segments) => segments.len(),
            Self::Many(segments) => segments.len(),
        }
    }
}

impl BufQueue {
    pub fn new(limit: usize) -> Self {
        Self {
            segments: Segments::Inline(Bytes::new(), 0),
            length: 0,
            limit,
            backing_capacity: Some(0),
        }
    }

    /// Creates an empty queue whose segment container holds `segments`
    /// segments before it grows.
    pub fn with_segment_capacity(limit: usize, segments: usize) -> Self {
        Self {
            segments: if segments > 1 {
                Segments::Many(VecDeque::with_capacity(segments), 0)
            } else {
                Segments::Inline(Bytes::new(), 0)
            },
            length: 0,
            limit,
            backing_capacity: Some(0),
        }
    }

    /// Moves `bytes` into a queue while preserving its shared backing storage.
    pub fn from_bytes(bytes: Bytes, limit: usize) -> Result<Self, BufQueueError> {
        let mut queue = Self::new(limit);
        queue.push_bytes(bytes)?;
        Ok(queue)
    }

    /// Moves a vector without copying and records its full backing capacity.
    pub fn from_vec(bytes: Vec<u8>, limit: usize) -> Result<Self, BufQueueError> {
        let mut queue = Self::new(limit);
        queue.push_vec(bytes)?;
        Ok(queue)
    }

    /// Conservative backing-capacity bound, separate from logical admission.
    ///
    /// Arbitrary `Bytes` may hide a larger allocation and yield `None`. Shared
    /// splits and trims retain the original bound until the queue is empty.
    pub fn backing_capacity_upper_bound(&self) -> Option<usize> {
        self.backing_capacity
    }

    /// Copies `bytes` into a new immutable backing allocation.
    pub fn copy_from_slice(bytes: &[u8], limit: usize) -> Result<Self, BufQueueError> {
        let mut queue = Self::new(limit);
        queue.push_copy(bytes)?;
        Ok(queue)
    }

    pub fn chain_length(&self) -> usize {
        self.length
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn remaining_capacity(&self) -> usize {
        self.limit - self.length
    }

    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub fn segment_count(&self) -> usize {
        match &self.segments {
            Segments::Inline(segment, _) => usize::from(!segment.is_empty()),
            Segments::Many(segments, _) => segments.len(),
        }
    }

    pub fn front(&self) -> Option<&Bytes> {
        match &self.segments {
            Segments::Inline(segment, _) => (!segment.is_empty()).then_some(segment),
            Segments::Many(segments, _) => segments.front(),
        }
    }

    /// Borrows each segment in queue order without copying or coalescing.
    pub fn segments(&self) -> BufQueueSegments<'_> {
        BufQueueSegments {
            inner: match &self.segments {
                Segments::Inline(segment, _) => {
                    SegmentRefs::Inline((!segment.is_empty()).then_some(segment).into_iter())
                }
                Segments::Many(segments, _) => SegmentRefs::Many(segments.iter()),
            },
        }
    }

    pub fn iovecs(&self) -> impl ExactSizeIterator<Item = IoSlice<'_>> {
        self.segments()
            .map(|segment| IoSlice::new(segment.as_ref()))
    }

    /// Appends `bytes` while preserving its shared backing storage.
    pub fn push_bytes(&mut self, bytes: Bytes) -> Result<(), BufQueueError> {
        let new_length = self.checked_new_length(bytes.len())?;
        self.push_checked(bytes, new_length, None);
        Ok(())
    }

    /// Appends `bytes` and records `capacity` as its backing bound, as
    /// [`push_vec`](Self::push_vec) records a vector's capacity.
    pub fn push_bytes_with_backing(
        &mut self,
        bytes: Bytes,
        capacity: usize,
    ) -> Result<(), BufQueueError> {
        let new_length = self.checked_new_length(bytes.len())?;
        self.push_checked(bytes, new_length, Some(capacity));
        Ok(())
    }

    /// Appends static bytes, which retain no heap allocation.
    pub fn push_static(&mut self, bytes: &'static [u8]) -> Result<(), BufQueueError> {
        let new_length = self.checked_new_length(bytes.len())?;
        self.push_checked(Bytes::from_static(bytes), new_length, Some(0));
        Ok(())
    }

    /// Appends a vector without copying and records its full backing capacity.
    pub fn push_vec(&mut self, bytes: Vec<u8>) -> Result<(), BufQueueError> {
        let new_length = self.checked_new_length(bytes.len())?;
        let capacity = bytes.capacity();
        self.push_checked(Bytes::from(bytes), new_length, Some(capacity));
        Ok(())
    }

    /// Copies `bytes` into a new immutable backing allocation and appends it.
    pub fn push_copy(&mut self, bytes: &[u8]) -> Result<(), BufQueueError> {
        let new_length = self.checked_new_length(bytes.len())?;
        self.push_checked(
            Bytes::from(bytes.to_vec().into_boxed_slice()),
            new_length,
            Some(bytes.len()),
        );
        Ok(())
    }

    /// Copies into the existing tail only when its backing is unique and has
    /// enough spare capacity. This does not grow the backing or add a segment.
    ///
    /// Returns `false` without changing visible bytes, pointers or bounds when
    /// the tail cannot accept the copy. Recovering a unique tail may replace
    /// ownership metadata even when it lacks capacity. Empty input succeeds.
    pub fn try_append_copy_to_tail(&mut self, bytes: &[u8]) -> Result<bool, BufQueueError> {
        let new_length = self.checked_new_length(bytes.len())?;
        if bytes.is_empty() {
            return Ok(true);
        }
        let tail = match &mut self.segments {
            Segments::Inline(segment, _) => segment,
            Segments::Many(segments, _) => {
                let Some(segment) = segments.back_mut() else {
                    return Ok(false);
                };
                segment
            }
        };
        if tail.is_empty() {
            return Ok(false);
        }
        match std::mem::take(tail).try_into_mut() {
            Ok(mut writable) => {
                let appended = bytes.len() <= writable.capacity() - writable.len();
                if appended {
                    writable.extend_from_slice(bytes);
                }
                *tail = writable.freeze();
                if appended {
                    self.length = new_length;
                }
                Ok(appended)
            }
            Err(original) => {
                *tail = original;
                Ok(false)
            }
        }
    }

    /// Copies a prefix and body into a unique small tail, using spare capacity
    /// or replacing the tail with an exactly sized copy.
    ///
    /// Reuse requires a known backing bound within [`COMPACT_WRITE_LIMIT`].
    /// Returns `false` without changing visible bytes when reuse is unavailable.
    /// A recovered offset view may move within its allocation. The body is
    /// borrowed and remains intact, including when output capacity is exceeded.
    pub fn try_append_compact_to_tail(
        &mut self,
        prefix: &[u8],
        body: &Self,
    ) -> Result<bool, BufQueueError> {
        let incoming =
            prefix
                .len()
                .checked_add(body.length)
                .ok_or(BufQueueError::LengthOverflow {
                    current: prefix.len(),
                    incoming: body.length,
                })?;
        let new_length = self.checked_new_length(incoming)?;
        let Some(bound) = self
            .backing_capacity
            .filter(|bound| *bound <= COMPACT_WRITE_LIMIT)
        else {
            return Ok(false);
        };
        let tail = match &mut self.segments {
            Segments::Inline(segment, _) => segment,
            Segments::Many(segments, _) => {
                let Some(segment) = segments.back_mut() else {
                    return Ok(false);
                };
                segment
            }
        };
        if tail.is_empty() || incoming > COMPACT_WRITE_LIMIT.saturating_sub(tail.len()) {
            return Ok(false);
        }
        let writable = match std::mem::take(tail).try_into_mut() {
            Ok(writable) => writable,
            Err(original) => {
                *tail = original;
                return Ok(false);
            }
        };
        let mut bytes = Vec::<u8>::from(writable);
        let old_capacity = bytes.capacity();
        if old_capacity - bytes.len() < incoming {
            // At these sizes a fresh allocation plus copy is cheaper than
            // jemalloc's realloc path, which usually moves the block anyway.
            let mut grown = Vec::with_capacity(bytes.len() + incoming);
            grown.extend_from_slice(&bytes);
            bytes = grown;
        }
        bytes.extend_from_slice(prefix);
        body.segments()
            .for_each(|segment| bytes.extend_from_slice(segment));
        self.backing_capacity = bound
            .checked_sub(old_capacity)
            .and_then(|rest| rest.checked_add(bytes.capacity()));
        *tail = Bytes::from(bytes);
        self.length = new_length;
        Ok(true)
    }

    /// Moves every segment from `other` on success and leaves both queues
    /// unchanged when this queue lacks capacity.
    pub fn append(&mut self, other: &mut Self) -> Result<(), BufQueueError> {
        let new_length = self.checked_new_length(other.length)?;
        self.backing_capacity = self
            .backing_capacity
            .zip(other.backing_capacity)
            .and_then(|(left, right)| left.checked_add(right));
        if !other.is_empty() {
            let unallocated = match &self.segments {
                Segments::Inline(segment, _) => segment.is_empty(),
                Segments::Many(segments, _) => segments.capacity() == 0,
            };
            if unallocated {
                self.segments.adopt(&mut other.segments);
            } else {
                match (&mut self.segments, &mut other.segments) {
                    (destination, Segments::Inline(segment, _)) => {
                        destination.push_back(std::mem::take(segment));
                    }
                    (Segments::Inline(segment, own), Segments::Many(source, _))
                        if source.len() < source.capacity() =>
                    {
                        let own = *own;
                        source.push_front(std::mem::take(segment));
                        let departing = segment_hint(source.capacity());
                        std::mem::swap(&mut self.segments, &mut other.segments);
                        self.segments.set_hint(own);
                        other.segments.set_hint(departing);
                    }
                    (Segments::Inline(segment, own), Segments::Many(source, _)) => {
                        let mut segments = VecDeque::with_capacity(source.len() + 1);
                        segments.push_back(std::mem::take(segment));
                        segments.append(source);
                        self.segments = Segments::Many(segments, *own);
                    }
                    (Segments::Many(destination, hint), Segments::Many(source, _)) => {
                        reserve_hinted(destination, source.len(), usize::from(*hint));
                        destination.append(source);
                    }
                }
            }
        }
        self.length = new_length;
        other.length = 0;
        other.backing_capacity = Some(0);
        Ok(())
    }

    /// Appends like [`append`](Self::append). When the move has to allocate or
    /// grow this queue's segment container, the container gets room for at
    /// least `segments` segments, so a queue that keeps coalescing grows once.
    pub fn append_with_growth_hint(
        &mut self,
        other: &mut Self,
        segments: u16,
    ) -> Result<(), BufQueueError> {
        if !other.is_empty() && self.checked_new_length(other.length).is_ok() {
            self.segments
                .grow_for(&other.segments, usize::from(segments));
        }
        self.append(other)
    }

    /// The segment container's capacity, capped at 64, for sizing a later
    /// queue that fills the same way. A queue without a container reports 0.
    pub fn segment_growth_hint(&self) -> u16 {
        match &self.segments {
            Segments::Inline(..) => 0,
            Segments::Many(segments, _) => segment_hint(segments.capacity()),
        }
    }

    /// Moves at most `amount` front bytes of `other` onto this queue, sharing
    /// a split boundary segment's backing, and returns how many moved.
    ///
    /// Both queues end as `other.split_at_most(amount)` followed by appending
    /// the result leaves them, including the conservative backing bounds. When
    /// this queue lacks capacity, neither queue changes.
    pub fn append_prefix_from(
        &mut self,
        other: &mut Self,
        amount: usize,
    ) -> Result<usize, BufQueueError> {
        let amount = amount.min(other.length);
        if amount == 0 {
            return Ok(0);
        }
        if amount == other.length {
            self.append(other)?;
            return Ok(amount);
        }
        let new_length = self.checked_new_length(amount)?;
        other.segments.move_prefix_to(amount, &mut self.segments);
        self.length = new_length;
        other.length -= amount;
        self.backing_capacity = self
            .backing_capacity
            .zip(other.backing_capacity)
            .and_then(|(left, right)| left.checked_add(right));
        Ok(amount)
    }

    /// Moves every segment out, leaving this queue empty. When a segment
    /// container leaves, the queue keeps its capacity as a hint, so a refill
    /// that outgrows the smallest container grows once instead of doubling.
    pub fn take(&mut self) -> Self {
        let taken = Self {
            segments: self.segments.take(),
            length: self.length,
            limit: self.limit,
            backing_capacity: self.backing_capacity,
        };
        self.length = 0;
        self.backing_capacity = Some(0);
        taken
    }

    pub fn clone_shared(&self) -> Self {
        Self {
            segments: self.segments.clone(),
            length: self.length,
            limit: self.limit,
            backing_capacity: self.backing_capacity,
        }
    }

    pub fn split_at_most(&mut self, amount: usize) -> Self {
        if amount == 0 || self.is_empty() {
            return Self::new(self.limit);
        }
        if amount >= self.length {
            return self.take();
        }

        let segments = self.segments.split_to(amount);
        self.length -= amount;
        Self {
            segments,
            length: amount,
            limit: self.limit,
            backing_capacity: self.backing_capacity,
        }
    }

    pub fn trim_start_at_most(&mut self, amount: usize) -> usize {
        let trim_length = amount.min(self.length);
        if trim_length == 0 {
            return 0;
        }

        let mut remaining = trim_length;
        while remaining != 0 {
            let mut segment = self
                .segments
                .pop_front()
                .expect("cached nonzero length requires a front segment");
            let consumed = remaining.min(segment.len());
            remaining -= consumed;
            self.length -= consumed;
            if self.is_empty() {
                self.backing_capacity = Some(0);
            }
            if consumed < segment.len() {
                segment.advance(consumed);
                match &mut self.segments {
                    Segments::Inline(front, _) => *front = segment,
                    Segments::Many(segments, _) => segments.push_front(segment),
                }
            }
        }

        trim_length
    }

    pub fn trim_start(&mut self, amount: usize) -> Result<(), BufQueueError> {
        if amount > self.length {
            return Err(BufQueueError::TrimUnderflow {
                requested: amount,
                available: self.length,
            });
        }
        let trimmed = self.trim_start_at_most(amount);
        debug_assert_eq!(trimmed, amount);
        Ok(())
    }

    pub fn coalesce(&self, maximum: usize) -> Result<Bytes, BufQueueError> {
        if self.length > maximum {
            return Err(BufQueueError::CoalesceLimitExceeded {
                limit: maximum,
                length: self.length,
            });
        }
        if self.segment_count() == 1 {
            if let Some(segment) = self.front() {
                return Ok(segment.clone());
            }
        }

        let mut bytes = Vec::with_capacity(self.length);
        for segment in self.segments() {
            bytes.extend_from_slice(segment.as_ref());
        }
        Ok(Bytes::from(bytes.into_boxed_slice()))
    }

    /// Replaces multiple segments with one contiguous buffer.
    pub fn coalesce_in_place(&mut self) -> Result<(), BufQueueError> {
        if self.segment_count() <= 1 {
            return Ok(());
        }
        let bytes = self.coalesce(self.length)?;
        match &mut self.segments {
            Segments::Inline(segment, _) => *segment = bytes,
            Segments::Many(segments, _) => {
                segments.clear();
                segments.push_back(bytes);
            }
        }
        self.backing_capacity = Some(self.length);
        Ok(())
    }

    pub fn into_bytes_segments(self) -> VecDeque<Bytes> {
        match self.segments {
            Segments::Inline(segment, _) => (!segment.is_empty())
                .then_some(segment)
                .into_iter()
                .collect(),
            Segments::Many(segments, _) => segments,
        }
    }

    /// Moves each segment in queue order without copying payloads or allocating
    /// a segment container. Dropping the iterator releases unconsumed segments.
    pub fn into_segment_iter(self) -> impl ExactSizeIterator<Item = Bytes> {
        match self.segments {
            Segments::Inline(segment, _) => {
                IntoSegments::Inline((!segment.is_empty()).then_some(segment).into_iter())
            }
            Segments::Many(segments, _) => IntoSegments::Many(segments.into_iter()),
        }
    }

    fn checked_new_length(&self, incoming: usize) -> Result<usize, BufQueueError> {
        let new_length =
            self.length
                .checked_add(incoming)
                .ok_or(BufQueueError::LengthOverflow {
                    current: self.length,
                    incoming,
                })?;
        if new_length > self.limit {
            return Err(BufQueueError::LimitExceeded {
                limit: self.limit,
                current: self.length,
                incoming,
            });
        }
        Ok(new_length)
    }

    fn push_checked(&mut self, bytes: Bytes, new_length: usize, capacity: Option<usize>) {
        if bytes.is_empty() {
            return;
        }
        self.segments.push_back(bytes);
        self.length = new_length;
        self.backing_capacity = self
            .backing_capacity
            .zip(capacity)
            .and_then(|(left, right)| left.checked_add(right));
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use bytes::Bytes;

    use super::BufQueue;
    use super::Segments;

    static STATIC_BYTES: [u8; 20_000] = [0x5a; 20_000];

    /// The take this module used before queues kept a refill hint.
    fn reference_take(queue: &mut BufQueue) -> BufQueue {
        let limit = queue.limit;
        std::mem::replace(queue, BufQueue::new(limit))
    }

    /// The segment-by-segment split this module used before bulk splitting.
    fn reference_split_at_most(queue: &mut BufQueue, amount: usize) -> BufQueue {
        if amount == 0 || queue.is_empty() {
            return BufQueue::new(queue.limit);
        }
        if amount >= queue.length {
            return reference_take(queue);
        }

        let split_length = amount;
        let mut result = BufQueue::new(queue.limit);
        result.backing_capacity = queue.backing_capacity;
        let mut remaining = amount;
        while remaining != 0 {
            let mut segment = queue
                .segments
                .pop_front()
                .expect("cached nonzero length requires a front segment");
            if segment.len() <= remaining {
                remaining -= segment.len();
                result.segments.push_back(segment);
            } else {
                let prefix = segment.split_to(remaining);
                result.segments.push_back(prefix);
                match &mut queue.segments {
                    Segments::Inline(front, _) => *front = segment,
                    Segments::Many(segments, _) => segments.push_front(segment),
                }
                remaining = 0;
            }
        }

        queue.length -= split_length;
        result.length = split_length;
        result
    }

    /// The append this module used before moving an inline destination into
    /// the source container.
    fn reference_append(
        queue: &mut BufQueue,
        other: &mut BufQueue,
    ) -> Result<(), super::BufQueueError> {
        let new_length = queue.checked_new_length(other.length)?;
        queue.backing_capacity = queue
            .backing_capacity
            .zip(other.backing_capacity)
            .and_then(|(left, right)| left.checked_add(right));
        if !other.is_empty() {
            let unallocated = match &queue.segments {
                Segments::Inline(segment, _) => segment.is_empty(),
                Segments::Many(segments, _) => segments.capacity() == 0,
            };
            if unallocated {
                std::mem::swap(&mut queue.segments, &mut other.segments);
            } else {
                match (&mut queue.segments, &mut other.segments) {
                    (destination, Segments::Inline(segment, _)) => {
                        destination.push_back(std::mem::take(segment));
                    }
                    (Segments::Inline(segment, _), Segments::Many(source, _)) => {
                        let mut segments = VecDeque::with_capacity(source.len() + 1);
                        segments.push_back(std::mem::take(segment));
                        segments.append(source);
                        queue.segments = Segments::Many(segments, 0);
                    }
                    (Segments::Many(destination, _), Segments::Many(source, _)) => {
                        destination.append(source);
                    }
                }
            }
        }
        queue.length = new_length;
        other.length = 0;
        other.backing_capacity = Some(0);
        Ok(())
    }

    #[derive(Debug, Eq, PartialEq)]
    struct Observed {
        segments: Vec<(usize, usize)>,
        length: usize,
        limit: usize,
        backing_capacity: Option<usize>,
        segment_count: usize,
        front: Option<(usize, usize)>,
        iovecs: Vec<(usize, usize)>,
    }

    fn observe(queue: &BufQueue) -> Observed {
        let view = |bytes: &Bytes| (bytes.as_ptr() as usize, bytes.len());
        Observed {
            segments: queue.segments().map(view).collect(),
            length: queue.chain_length(),
            limit: queue.limit(),
            backing_capacity: queue.backing_capacity_upper_bound(),
            segment_count: queue.segment_count(),
            front: queue.front().map(view),
            iovecs: queue
                .iovecs()
                .map(|slice| (slice.as_ptr() as usize, slice.len()))
                .collect(),
        }
    }

    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, bound: usize) -> usize {
            (self.next() % bound as u64) as usize
        }

        fn range(&mut self, low: usize, high: usize) -> usize {
            low + self.below(high - low + 1)
        }
    }

    struct Owner(Vec<u8>);

    impl AsRef<[u8]> for Owner {
        fn as_ref(&self) -> &[u8] {
            &self.0
        }
    }

    fn push_random_segment(rng: &mut Rng, queue: &mut BufQueue, max_length: usize) {
        let length = rng.range(1, max_length);
        let result = match rng.below(4) {
            0 => queue.push_static(&STATIC_BYTES[..length]),
            1 => {
                let mut bytes = Vec::with_capacity(length + rng.below(64));
                bytes.resize(length, 0xa5);
                queue.push_vec(bytes)
            }
            2 => queue.push_bytes(Bytes::from_owner(Owner(vec![0x3c; length]))),
            _ => queue.push_copy(&STATIC_BYTES[..length]),
        };
        result.expect("random segment must fit the test limit");
    }

    fn random_queue(rng: &mut Rng, max_segments: usize, max_length: usize) -> BufQueue {
        let mut queue = BufQueue::new(usize::MAX / 2);
        (0..rng.range(1, max_segments))
            .for_each(|_| push_random_segment(rng, &mut queue, max_length));
        queue
    }

    /// Amounts at, and one byte either side of, every segment boundary, plus
    /// zero, the full length and past it.
    fn split_amounts(queue: &BufQueue) -> Vec<usize> {
        let mut amounts = vec![0, 1, queue.chain_length(), queue.chain_length() + 1];
        let mut offset = 0;
        for segment in queue.segments() {
            offset += segment.len();
            amounts.extend([offset.saturating_sub(1), offset, offset + 1]);
        }
        amounts
    }

    fn assert_split_matches_reference(queue: &BufQueue, amount: usize) -> (BufQueue, BufQueue) {
        let mut actual = queue.clone_shared();
        let mut expected = queue.clone_shared();
        let actual_prefix = actual.split_at_most(amount);
        let expected_prefix = reference_split_at_most(&mut expected, amount);
        assert_eq!(
            observe(&actual_prefix),
            observe(&expected_prefix),
            "prefix of split at {amount}"
        );
        assert_eq!(
            observe(&actual),
            observe(&expected),
            "remainder of split at {amount}"
        );
        (actual, actual_prefix)
    }

    #[test]
    fn bulk_split_matches_reference_at_every_boundary() {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        for _ in 0..300 {
            let queue = random_queue(&mut rng, 40, 20_000);
            for amount in split_amounts(&queue) {
                assert_split_matches_reference(&queue, amount);
            }
        }
    }

    #[test]
    fn split_and_append_sequences_match_reference() {
        let mut rng = Rng(0x2545_f491_4f6c_dd1d);
        for _ in 0..200 {
            let mut actual = random_queue(&mut rng, 40, 2_000);
            let mut expected = actual.clone_shared();
            for step in 0..60 {
                match rng.below(6) {
                    0 => {
                        let amount = rng.below(actual.chain_length() + 2);
                        let mut actual_prefix = actual.split_at_most(amount);
                        let mut expected_prefix = reference_split_at_most(&mut expected, amount);
                        assert_eq!(
                            observe(&actual_prefix),
                            observe(&expected_prefix),
                            "step {step}"
                        );
                        // Refill the split-off prefix so later operations see
                        // both representations again.
                        let mut extra = random_queue(&mut rng, 4, 200);
                        let mut extra_twin = extra.clone_shared();
                        actual_prefix.append(&mut extra).unwrap();
                        reference_append(&mut expected_prefix, &mut extra_twin).unwrap();
                        assert_eq!(
                            observe(&actual_prefix),
                            observe(&expected_prefix),
                            "step {step}"
                        );
                        assert_eq!(observe(&extra), observe(&extra_twin), "step {step}");
                        if rng.below(2) == 0 {
                            actual = actual_prefix;
                            expected = expected_prefix;
                        }
                    }
                    1 => {
                        let max_segments = if rng.below(2) == 0 { 1 } else { 12 };
                        let mut source = random_queue(&mut rng, max_segments, 2_000);
                        let mut source_twin = source.clone_shared();
                        if rng.below(2) == 0 {
                            // Append into the other queue, so an inline or
                            // single-segment destination meets a long source.
                            source.append(&mut actual).unwrap();
                            reference_append(&mut source_twin, &mut expected).unwrap();
                            assert_eq!(observe(&actual), observe(&expected), "step {step}");
                            actual = source;
                            expected = source_twin;
                        } else {
                            actual.append(&mut source).unwrap();
                            reference_append(&mut expected, &mut source_twin).unwrap();
                            assert_eq!(observe(&source), observe(&source_twin), "step {step}");
                        }
                    }
                    2 => {
                        let amount = rng.below(actual.chain_length() + 2);
                        assert_eq!(
                            actual.trim_start_at_most(amount),
                            expected.trim_start_at_most(amount)
                        );
                    }
                    3 => {
                        let taken = actual.take();
                        let expected_taken = expected.take();
                        assert_eq!(observe(&taken), observe(&expected_taken), "step {step}");
                        if rng.below(2) == 0 {
                            actual = taken;
                            expected = expected_taken;
                        }
                    }
                    4 => {
                        let mut rejected = BufQueue::new(usize::MAX / 2);
                        push_random_segment(&mut rng, &mut rejected, 20);
                        let mut rejected_twin = rejected.clone_shared();
                        let mut small = BufQueue::new(actual.chain_length());
                        let mut small_twin = BufQueue::new(expected.chain_length());
                        assert_eq!(
                            small
                                .append(&mut actual)
                                .and_then(|()| small.append(&mut rejected)),
                            reference_append(&mut small_twin, &mut expected).and_then(|()| {
                                reference_append(&mut small_twin, &mut rejected_twin)
                            }),
                        );
                        assert_eq!(observe(&small), observe(&small_twin), "step {step}");
                        assert_eq!(observe(&rejected), observe(&rejected_twin), "step {step}");
                        actual.append(&mut small).unwrap();
                        reference_append(&mut expected, &mut small_twin).unwrap();
                    }
                    _ => {
                        let length = rng.range(1, 500);
                        if rng.below(2) == 0 {
                            actual.push_static(&STATIC_BYTES[..length]).unwrap();
                            expected.push_static(&STATIC_BYTES[..length]).unwrap();
                        } else {
                            let pushed = Bytes::from(vec![0x7e; length]);
                            actual.push_bytes(pushed.clone()).unwrap();
                            expected.push_bytes(pushed).unwrap();
                        }
                    }
                }
                assert_eq!(observe(&actual), observe(&expected), "step {step}");
            }
        }
    }

    fn many_capacity(queue: &BufQueue) -> Option<usize> {
        match &queue.segments {
            Segments::Inline(..) => None,
            Segments::Many(segments, _) => Some(segments.capacity()),
        }
    }

    fn four_byte_segments(count: usize) -> BufQueue {
        let mut queue = BufQueue::new(64);
        (0..count).for_each(|index| queue.push_static(&STATIC_BYTES[index * 4..][..4]).unwrap());
        queue
    }

    fn five_segment_queue() -> BufQueue {
        four_byte_segments(5)
    }

    #[test]
    fn multi_segment_split_allocates_exactly_once() {
        // Results of up to four segments get VecDeque's smallest capacity;
        // longer results get exactly their segment count.
        for (count, amount, capacity) in
            [(5, 9, 4), (5, 12, 4), (5, 13, 4), (10, 21, 6), (10, 28, 7)]
        {
            let (remainder, prefix) =
                assert_split_matches_reference(&four_byte_segments(count), amount);
            assert_eq!(
                many_capacity(&prefix),
                Some(capacity),
                "split {count} at {amount}"
            );
            assert!(remainder.segment_count() >= 2, "split {count} at {amount}");
        }
    }

    #[test]
    fn split_leaving_one_segment_moves_the_source_container() {
        for amount in [16, 17, 19] {
            let mut queue = five_segment_queue();
            assert_split_matches_reference(&queue, amount);
            let capacity = many_capacity(&queue);
            let prefix = queue.split_at_most(amount);
            assert_eq!(many_capacity(&prefix), capacity, "split at {amount}");
            assert_eq!(many_capacity(&queue), None, "split at {amount}");
            assert_eq!(queue.segment_count(), 1, "split at {amount}");
        }
    }

    #[test]
    fn single_segment_split_does_not_allocate() {
        for amount in [1, 3, 4] {
            let (remainder, prefix) = assert_split_matches_reference(&five_segment_queue(), amount);
            assert_eq!(many_capacity(&prefix), None, "split at {amount}");
            assert!(remainder.segment_count() >= 4, "split at {amount}");
        }
    }

    #[test]
    fn inline_destination_append_reuses_the_source_container() {
        let mut destination = BufQueue::from_vec(b"head".to_vec(), 64).unwrap();
        let bound = destination.backing_capacity_upper_bound().unwrap();
        let mut source = five_segment_queue();
        let capacity = many_capacity(&source);
        let mut pointers: Vec<_> = destination
            .segments()
            .map(|segment| segment.as_ptr())
            .collect();
        pointers.extend(source.segments().map(|segment| segment.as_ptr()));

        destination.append(&mut source).unwrap();

        assert_eq!(many_capacity(&destination), capacity);
        assert_eq!(
            destination
                .segments()
                .map(|segment| segment.as_ptr())
                .collect::<Vec<_>>(),
            pointers,
        );
        assert_eq!(destination.chain_length(), 24);
        assert_eq!(destination.backing_capacity_upper_bound(), Some(bound));
        assert!(source.is_empty());
        assert_eq!(source.segment_count(), 0);
        assert_eq!(source.backing_capacity_upper_bound(), Some(0));
        assert_eq!(source.limit(), 64);
    }

    #[test]
    fn inline_destination_append_to_a_full_source_allocates_once() {
        let mut destination = BufQueue::from_vec(b"head".to_vec(), 64).unwrap();
        let mut source = four_byte_segments(4);
        assert_eq!(many_capacity(&source), Some(4));
        let mut pointers: Vec<_> = destination
            .segments()
            .map(|segment| segment.as_ptr())
            .collect();
        pointers.extend(source.segments().map(|segment| segment.as_ptr()));

        destination.append(&mut source).unwrap();

        assert_eq!(
            many_capacity(&destination),
            Some(5),
            "one exactly sized container, no regrowth"
        );
        assert_eq!(
            destination
                .segments()
                .map(|segment| segment.as_ptr())
                .collect::<Vec<_>>(),
            pointers,
        );
        assert_eq!(destination.chain_length(), 20);
        assert!(source.is_empty());
        assert_eq!(source.backing_capacity_upper_bound(), Some(0));
    }

    /// Moves a prefix with `append_prefix_from` and with `split_at_most` plus
    /// `append`, and checks both queues end in the same observable state. On
    /// error the moving side must leave its source untouched.
    fn assert_prefix_move_matches_split_and_append(
        destination: &mut BufQueue,
        source: &mut BufQueue,
        amount: usize,
    ) -> Result<usize, super::BufQueueError> {
        let original_destination = observe(destination);
        let original_source = observe(source);
        let mut expected = destination.clone_shared();
        let mut expected_source = source.clone_shared();
        let moved = destination.append_prefix_from(source, amount);
        let mut chunk = expected_source.split_at_most(amount);
        let expected_moved = chunk.chain_length();
        let expected_result = expected.append(&mut chunk).map(|()| expected_moved);
        assert_eq!(moved, expected_result, "move of {amount}");
        if moved.is_ok() {
            assert_eq!(observe(destination), observe(&expected), "move of {amount}");
            assert_eq!(
                observe(source),
                observe(&expected_source),
                "move of {amount}"
            );
        } else {
            assert_eq!(
                observe(destination),
                original_destination,
                "move of {amount}"
            );
            assert_eq!(observe(source), original_source, "move of {amount}");
        }
        moved
    }

    #[test]
    fn prefix_move_matches_split_and_append_at_every_boundary() {
        let mut rng = Rng(0x6a09_e667_f3bc_c909);
        for _ in 0..200 {
            let source = random_queue(&mut rng, 40, 20_000);
            for amount in split_amounts(&source) {
                for destination_segments in [0, 1, 3] {
                    let mut destination = if rng.below(2) == 0 {
                        BufQueue::new(usize::MAX / 2)
                    } else {
                        BufQueue::with_segment_capacity(usize::MAX / 2, 64)
                    };
                    (0..destination_segments)
                        .for_each(|_| push_random_segment(&mut rng, &mut destination, 100));
                    let mut source = source.clone_shared();
                    assert_prefix_move_matches_split_and_append(
                        &mut destination,
                        &mut source,
                        amount,
                    )
                    .unwrap();
                }
            }
        }
    }

    #[test]
    fn prefix_moves_that_exceed_capacity_move_nothing() {
        let mut rng = Rng(0xbb67_ae85_84ca_a73b);
        for _ in 0..200 {
            let source = random_queue(&mut rng, 12, 2_000);
            for amount in split_amounts(&source) {
                let moved = amount.min(source.chain_length());
                if moved == 0 {
                    continue;
                }
                let mut destination = BufQueue::new(moved + 3);
                destination.push_static(&STATIC_BYTES[..4]).unwrap();
                let mut source = source.clone_shared();
                let error = assert_prefix_move_matches_split_and_append(
                    &mut destination,
                    &mut source,
                    amount,
                )
                .unwrap_err();
                assert_eq!(
                    error,
                    super::BufQueueError::LimitExceeded {
                        limit: moved + 3,
                        current: 4,
                        incoming: moved,
                    }
                );
            }
        }
    }

    #[test]
    fn frame_sized_prefix_moves_match_split_and_append_until_exhausted() {
        let mut rng = Rng(0x3c6e_f372_fe94_f82b);
        for _ in 0..100 {
            let mut source = random_queue(&mut rng, 40, 20_000);
            let mut expected_source = source.clone_shared();
            let frame = rng.range(1, 20_000);
            let mut destination = BufQueue::with_segment_capacity(usize::MAX / 2, 8);
            let mut expected = BufQueue::new(usize::MAX / 2);
            while !source.is_empty() {
                let header = Bytes::from(vec![0x11; 9]);
                destination.push_bytes(header.clone()).unwrap();
                expected.push_bytes(header).unwrap();
                let moved = destination.append_prefix_from(&mut source, frame).unwrap();
                let mut chunk = expected_source.split_at_most(frame);
                assert_eq!(moved, chunk.chain_length());
                expected.append(&mut chunk).unwrap();
                assert_eq!(observe(&destination), observe(&expected));
                assert_eq!(observe(&source), observe(&expected_source));
            }
        }
    }

    #[test]
    fn presized_queue_is_observably_empty_and_does_not_regrow() {
        for segments in [0, 1] {
            let queue = BufQueue::with_segment_capacity(64, segments);
            assert_eq!(many_capacity(&queue), None);
            assert_eq!(observe(&queue), observe(&BufQueue::new(64)));
        }
        // One header plus eight 4-byte segments moved 6 bytes at a time: five
        // moves split a boundary and the last takes the remaining suffix.
        let mut queue = BufQueue::with_segment_capacity(64, 12);
        assert_eq!(many_capacity(&queue), Some(12));
        assert_eq!(observe(&queue), observe(&BufQueue::new(64)));
        assert!(!queue.try_append_copy_to_tail(b"x").unwrap());
        let mut source = four_byte_segments(8);
        queue.push_static(b"head").unwrap();
        while !source.is_empty() {
            queue.append_prefix_from(&mut source, 6).unwrap();
        }
        assert_eq!(queue.segment_count(), 12);
        assert_eq!(
            many_capacity(&queue),
            Some(12),
            "moves fill without regrowth"
        );
        assert_eq!(queue.chain_length(), 36);
    }

    fn inline_hint(queue: &BufQueue) -> Option<u16> {
        match &queue.segments {
            Segments::Inline(_, hint) => Some(*hint),
            Segments::Many(..) => None,
        }
    }

    fn many_hint(queue: &BufQueue) -> Option<u16> {
        match &queue.segments {
            Segments::Inline(..) => None,
            Segments::Many(_, hint) => Some(*hint),
        }
    }

    /// Pushes `count` 4-byte segments one at a time and returns the container
    /// capacity after each push.
    fn refill(queue: &mut BufQueue, count: usize) -> Vec<Option<usize>> {
        (0..count)
            .map(|index| {
                queue.push_static(&STATIC_BYTES[index * 4..][..4]).unwrap();
                many_capacity(queue)
            })
            .collect()
    }

    #[test]
    fn refill_cycles_match_reference() {
        // Long-lived queues drain by take or split and refill one segment at a
        // time, as transport ingress does. Only container capacity may differ.
        let mut rng = Rng(0x510e_527f_ade6_82d1);
        for _ in 0..100 {
            let mut actual = BufQueue::new(usize::MAX / 2);
            let mut expected = BufQueue::new(usize::MAX / 2);
            for step in 0..30 {
                let fill = if rng.below(4) == 0 {
                    rng.range(1, 3)
                } else {
                    rng.range(1, 90)
                };
                for _ in 0..fill {
                    let length = rng.range(1, 300);
                    if rng.below(2) == 0 {
                        actual.push_static(&STATIC_BYTES[..length]).unwrap();
                        expected.push_static(&STATIC_BYTES[..length]).unwrap();
                    } else {
                        let segment = Bytes::from(vec![0x42; length]);
                        actual.push_bytes(segment.clone()).unwrap();
                        expected.push_bytes(segment).unwrap();
                    }
                }
                let (mut drained, mut expected_drained) = if rng.below(3) == 0 {
                    (actual.take(), reference_take(&mut expected))
                } else {
                    let amount = rng.below(actual.chain_length() + 2);
                    (
                        actual.split_at_most(amount),
                        reference_split_at_most(&mut expected, amount),
                    )
                };
                assert_eq!(observe(&drained), observe(&expected_drained), "step {step}");
                assert_eq!(observe(&actual), observe(&expected), "step {step}");
                // The reader appends what it drained into its own queue.
                let mut sink = BufQueue::new(usize::MAX / 2);
                let mut expected_sink = BufQueue::new(usize::MAX / 2);
                if rng.below(2) == 0 {
                    sink.push_static(&STATIC_BYTES[..7]).unwrap();
                    expected_sink.push_static(&STATIC_BYTES[..7]).unwrap();
                }
                sink.append(&mut drained).unwrap();
                reference_append(&mut expected_sink, &mut expected_drained).unwrap();
                assert_eq!(observe(&sink), observe(&expected_sink), "step {step}");
                assert_eq!(observe(&drained), observe(&expected_drained), "step {step}");
                for queue in [&actual, &drained, &sink] {
                    assert!(
                        inline_hint(queue).is_none_or(|hint| hint <= 64),
                        "step {step}"
                    );
                }
            }
        }
    }

    /// Empties `queue` with `take` and returns the departed container's
    /// capacity.
    fn take_container(queue: &mut BufQueue) -> usize {
        many_capacity(&queue.take()).expect("a multi-segment queue has a container")
    }

    /// The capacities `refill` reports for `count` pushes into an empty queue
    /// with refill hint `hint`: the smallest container up to four segments,
    /// then one growth to the hint, then doubling.
    fn hinted_refill_capacities(hint: usize, count: usize) -> Vec<Option<usize>> {
        let mut capacity = 0;
        (1..=count)
            .map(|segments| {
                if segments < 2 {
                    return None;
                }
                capacity = match capacity {
                    0 => super::MIN_SEGMENT_CAPACITY,
                    full if segments > full => hint.max(2 * full),
                    room => room,
                };
                Some(capacity)
            })
            .collect()
    }

    #[test]
    fn taken_queue_refills_small_first_then_grows_once_to_its_last_container() {
        let mut queue = four_byte_segments(10);
        let departed = take_container(&mut queue);
        assert!(departed >= 10);
        assert_eq!(inline_hint(&queue), Some(departed as u16));
        assert_eq!(observe(&queue), observe(&BufQueue::new(64)));
        let capacities = refill(&mut queue, 10);
        assert_eq!(capacities, hinted_refill_capacities(departed, 10));
        assert_eq!(capacities[4], Some(departed), "one growth to the hint");
        assert_eq!(observe(&queue), observe(&four_byte_segments(10)));
        // A refill that fits the smallest container allocates only that and
        // leaves it behind as the hint.
        drop(queue.take());
        assert_eq!(refill(&mut queue, 3), [None, Some(4), Some(4)]);
        assert_eq!(take_container(&mut queue), 4);
        assert_eq!(inline_hint(&queue), Some(4));
        // Taking a single inline segment leaves no container behind, so the
        // hint of the last container stays.
        refill(&mut queue, 1);
        drop(queue.take());
        assert_eq!(inline_hint(&queue), Some(4));
    }

    #[test]
    fn small_refills_after_a_burst_allocate_as_unhinted_queues_do() {
        // After one 100-segment burst the hint is 64, but every later refill
        // of at most four segments allocates the same 4-segment container as
        // a queue that never had a hint.
        let mut hinted = BufQueue::new(1024);
        refill(&mut hinted, 100);
        drop(hinted.take());
        assert_eq!(inline_hint(&hinted), Some(64));
        for fill in 1..=4 {
            let mut plain = BufQueue::new(1024);
            assert_eq!(
                refill(&mut hinted, fill),
                refill(&mut plain, fill),
                "fill {fill}"
            );
            drop(hinted.take());
        }
        assert_eq!(inline_hint(&hinted), Some(4));
    }

    #[test]
    fn split_leaving_one_segment_refills_for_the_moved_container() {
        for amount in [36, 38] {
            let mut queue = four_byte_segments(10);
            let prefix = queue.split_at_most(amount);
            let moved = many_capacity(&prefix).expect("the prefix takes the container");
            assert_eq!(queue.segment_count(), 1, "split at {amount}");
            assert_eq!(inline_hint(&queue), Some(moved as u16), "split at {amount}");
            let capacities = refill(&mut queue, 9);
            assert_eq!(
                capacities,
                hinted_refill_capacities(moved, 10)[1..],
                "split at {amount}"
            );
        }
    }

    #[test]
    fn refill_hint_is_capped_at_sixty_four_segments() {
        let mut queue = BufQueue::new(1024);
        refill(&mut queue, 100);
        drop(queue.take());
        assert_eq!(inline_hint(&queue), Some(64));
        let capacities = refill(&mut queue, 65);
        assert_eq!(capacities, hinted_refill_capacities(64, 65));
        assert_eq!(capacities[63], Some(64));
        assert_eq!(capacities[64], Some(128));
    }

    #[test]
    fn new_split_and_taken_inline_queues_start_at_the_smallest_container() {
        let mut fresh = BufQueue::new(64);
        assert_eq!(inline_hint(&fresh), Some(0));
        assert_eq!(refill(&mut fresh, 2)[1], Some(4));
        let mut hinted = four_byte_segments(10);
        let departed = take_container(&mut hinted) as u16;
        refill(&mut hinted, 1);
        let mut prefix = hinted.split_at_most(2);
        assert_eq!(inline_hint(&prefix), Some(0));
        assert_eq!(inline_hint(&hinted), Some(departed));
        assert_eq!(refill(&mut prefix, 1)[0], Some(4));
        let mut taken = hinted.take();
        assert_eq!(inline_hint(&taken), Some(0));
        assert_eq!(inline_hint(&hinted), Some(departed));
        assert_eq!(refill(&mut taken, 1)[0], Some(4));
        let mut many = four_byte_segments(5);
        let mut single = many.split_at_most(4);
        assert_eq!(inline_hint(&single), Some(0));
        assert_eq!(refill(&mut single, 1)[0], Some(4));
    }

    #[test]
    fn append_keeps_each_queue_s_own_hint() {
        let mut destination = four_byte_segments(10);
        let departed = take_container(&mut destination);
        let mut single = BufQueue::from_vec(b"head".to_vec(), 64).unwrap();
        destination.append(&mut single).unwrap();
        assert_eq!(inline_hint(&destination), Some(departed as u16));
        assert_eq!(inline_hint(&single), Some(0));
        assert_eq!(
            refill(&mut destination, 9),
            hinted_refill_capacities(departed, 10)[1..]
        );
        // A source whose container moves keeps that container's capacity.
        let mut source = four_byte_segments(12);
        let capacity = many_capacity(&source).unwrap();
        let mut empty = BufQueue::new(64);
        empty.append(&mut source).unwrap();
        assert_eq!(inline_hint(&source), Some(capacity as u16));
        assert_eq!(empty.segment_count(), 12);
        let mut head = BufQueue::from_vec(b"head".to_vec(), 64).unwrap();
        let mut spare = five_segment_queue();
        let capacity = many_capacity(&spare).unwrap();
        head.append(&mut spare).unwrap();
        assert_eq!(inline_hint(&spare), Some(capacity as u16));
        assert_eq!(head.segment_count(), 6);
    }

    /// A queue holding two 4-byte segments in a 4-segment container whose
    /// refill hint is 64, from one earlier 100-segment burst.
    fn burst_hinted_pair() -> BufQueue {
        let mut queue = BufQueue::new(1024);
        refill(&mut queue, 100);
        drop(queue.take());
        refill(&mut queue, 2);
        assert_eq!(many_capacity(&queue), Some(4));
        assert_eq!(many_hint(&queue), Some(64));
        queue
    }

    #[test]
    fn appends_that_grow_a_hinted_container_grow_it_once_to_the_hint() {
        let mut destination = burst_hinted_pair();
        destination.append(&mut four_byte_segments(5)).unwrap();
        assert_eq!(many_capacity(&destination), Some(64));
        assert_eq!(destination.segment_count(), 7);
        // Appends that fit, or a hint the doubling already covers, grow as
        // without a hint.
        let mut fits = burst_hinted_pair();
        fits.append(&mut four_byte_segments(2)).unwrap();
        assert_eq!(many_capacity(&fits), Some(4));
        let mut plain = four_byte_segments(2);
        plain.append(&mut four_byte_segments(5)).unwrap();
        assert_eq!(many_capacity(&plain), Some(8));
        // An inline queue that moves into the source container keeps its own
        // hint for that container's growth.
        let mut head = BufQueue::new(1024);
        refill(&mut head, 100);
        drop(head.take());
        refill(&mut head, 1);
        let mut spare = five_segment_queue();
        assert_eq!(many_capacity(&spare), Some(8));
        head.append(&mut spare).unwrap();
        assert_eq!(many_capacity(&head), Some(8));
        assert_eq!(many_hint(&head), Some(64));
        assert_eq!(inline_hint(&spare), Some(8));
        refill(&mut head, 3);
        assert_eq!(many_capacity(&head), Some(64));
        // A taken container leaves its capacity as this queue's hint and
        // carries no hint of its own.
        let mut queue = burst_hinted_pair();
        let taken = queue.take();
        assert_eq!(many_hint(&taken), Some(0));
        assert_eq!(inline_hint(&queue), Some(4));
    }

    #[test]
    fn the_refill_hint_does_not_grow_the_queue() {
        use std::mem::size_of;
        assert_eq!(
            size_of::<Segments>(),
            size_of::<VecDeque<Bytes>>() + size_of::<usize>()
        );
        assert_eq!(
            size_of::<BufQueue>(),
            size_of::<Segments>() + 4 * size_of::<usize>()
        );
    }

    /// A random queue whose container, when it has one, may have spare room.
    fn random_append_side(rng: &mut Rng) -> BufQueue {
        match rng.below(6) {
            0 => BufQueue::new(usize::MAX / 2),
            1 => {
                let mut queue = BufQueue::new(usize::MAX / 2);
                push_random_segment(rng, &mut queue, 300);
                queue
            }
            2 => {
                let segments = rng.range(1, 12);
                let mut queue =
                    BufQueue::with_segment_capacity(usize::MAX / 2, segments + rng.below(8));
                (0..segments).for_each(|_| push_random_segment(rng, &mut queue, 300));
                queue
            }
            _ => random_queue(rng, 40, 300),
        }
    }

    #[test]
    fn append_with_growth_hint_matches_append() {
        let mut rng = Rng(0x3c6e_f372_fe94_f82b);
        for case in 0..2_000 {
            let destination = random_append_side(&mut rng);
            let source = random_append_side(&mut rng);
            let hint = match rng.below(3) {
                0 => 0,
                1 => rng.range(1, 64) as u16,
                _ => rng.range(65, 300) as u16,
            };
            let (mut actual, mut actual_source) =
                (destination.clone_shared(), source.clone_shared());
            let (mut expected, mut expected_source) =
                (destination.clone_shared(), source.clone_shared());
            actual
                .append_with_growth_hint(&mut actual_source, hint)
                .unwrap();
            reference_append(&mut expected, &mut expected_source).unwrap();
            assert_eq!(observe(&actual), observe(&expected), "case {case}");
            assert_eq!(
                observe(&actual_source),
                observe(&expected_source),
                "case {case}"
            );
        }
    }

    #[test]
    fn append_with_growth_hint_changes_nothing_when_the_limit_is_exceeded() {
        let mut destination = BufQueue::from_vec(vec![1; 8], 10).unwrap();
        let mut source = four_byte_segments(3);
        let (before, source_before) = (observe(&destination), observe(&source));
        let error = destination
            .append_with_growth_hint(&mut source, 32)
            .unwrap_err();
        assert!(matches!(
            error,
            super::BufQueueError::LimitExceeded { limit: 10, .. }
        ));
        assert_eq!(observe(&destination), before);
        assert_eq!(observe(&source), source_before);
        assert_eq!(many_capacity(&destination), None, "no container was grown");
    }

    #[test]
    fn growth_hint_sizes_a_growing_container_once() {
        // A 4-segment container that keeps receiving two-segment sources
        // grows once to the hint and then takes 32 segments without growing;
        // without a hint it doubles 4 -> 8 -> 16 -> 32.
        let filled = |count| {
            let mut queue = BufQueue::new(1024);
            refill(&mut queue, count);
            queue
        };
        let pair = || four_byte_segments(2);
        let mut hinted = filled(4);
        let mut plain = filled(4);
        assert_eq!(many_capacity(&hinted), Some(4));
        let (mut hinted_capacities, mut plain_capacities) = (Vec::new(), Vec::new());
        while hinted.segment_count() < 32 {
            hinted.append_with_growth_hint(&mut pair(), 32).unwrap();
            plain.append(&mut pair()).unwrap();
            hinted_capacities.push(many_capacity(&hinted).unwrap());
            plain_capacities.push(many_capacity(&plain).unwrap());
        }
        assert!(hinted_capacities.iter().all(|capacity| *capacity == 32));
        assert_eq!(
            plain_capacities,
            [8, 8, 16, 16, 16, 16, 32, 32, 32, 32, 32, 32, 32, 32]
        );
        assert_eq!(observe(&hinted).length, observe(&plain).length);
        // Past the hint, growth doubles as usual.
        hinted.append_with_growth_hint(&mut pair(), 32).unwrap();
        assert_eq!(many_capacity(&hinted), Some(64));
    }

    #[test]
    fn growth_hint_allocates_only_where_append_would() {
        // An unallocated destination adopts the source container.
        let mut empty = BufQueue::new(1024);
        let mut source = four_byte_segments(5);
        let capacity = many_capacity(&source);
        empty.append_with_growth_hint(&mut source, 64).unwrap();
        assert_eq!(many_capacity(&empty), capacity);
        // An inline segment moves into a source container with room.
        let mut single = BufQueue::from_vec(b"head".to_vec(), 1024).unwrap();
        let mut roomy = four_byte_segments(5);
        let capacity = many_capacity(&roomy);
        assert!(capacity > Some(5));
        single.append_with_growth_hint(&mut roomy, 64).unwrap();
        assert_eq!(many_capacity(&single), capacity);
        // A container with room keeps its capacity.
        let mut destination = four_byte_segments(5);
        let capacity = many_capacity(&destination);
        destination
            .append_with_growth_hint(&mut four_byte_segments(2), 64)
            .unwrap();
        assert_eq!(many_capacity(&destination), capacity);
        // An inline destination that must allocate gets the hinted size, for
        // an inline source and for a full source container alike.
        let mut single = BufQueue::from_vec(b"head".to_vec(), 1024).unwrap();
        single
            .append_with_growth_hint(&mut BufQueue::from_vec(b"tail".to_vec(), 4).unwrap(), 16)
            .unwrap();
        assert_eq!(many_capacity(&single), Some(16));
        let mut single = BufQueue::from_vec(b"head".to_vec(), 1024).unwrap();
        let mut full = four_byte_segments(4);
        assert_eq!(many_capacity(&full), Some(4));
        single.append_with_growth_hint(&mut full, 16).unwrap();
        assert_eq!(many_capacity(&single), Some(16));
        assert_eq!(single.segment_count(), 5);
    }

    #[test]
    fn segment_growth_hint_is_the_capped_container_capacity() {
        assert_eq!(BufQueue::new(8).segment_growth_hint(), 0);
        assert_eq!(
            BufQueue::from_vec(b"one".to_vec(), 8)
                .unwrap()
                .segment_growth_hint(),
            0
        );
        let ten = BufQueue::with_segment_capacity(1024, 10);
        assert_eq!(ten.segment_growth_hint(), 10);
        let mut many = BufQueue::new(1024);
        refill(&mut many, 100);
        assert!(many_capacity(&many).unwrap() > 64);
        assert_eq!(many.segment_growth_hint(), 64);
    }
}
