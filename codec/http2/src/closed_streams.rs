/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A bounded record of streams that closed other than after the peer ended them.

use std::collections::BTreeMap;

/// The most streams the record keeps. It needs a stream only while the peer's frames on it can
/// still be in flight, and a peer that keeps to a concurrency limit of 100 streams has at most
/// that many streams with frames in flight.
const MAX_CLOSED_STREAMS: usize = 128;

/// How a stream closed, when not after the peer ended it. A later variant overrides an earlier
/// one for the same stream.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum ClosedBy {
    /// The session dropped the stream before the peer ended it, and neither side reset it.
    Abandoned,
    /// The peer reset the stream.
    PeerReset,
    /// This endpoint reset the stream, or raised a stream error that the session answers with a
    /// reset. The peer may have sent frames before the reset reached it.
    LocalReset,
}

/// What the record knows about a stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Recorded {
    Closed(ClosedBy),
    /// The stream is at or below one that the record evicted, so how it closed is unknown.
    Forgotten,
    /// The stream closed after the peer ended it, or never opened.
    Unrecorded,
}

#[derive(Debug, Default)]
pub(crate) struct ClosedStreams {
    streams: BTreeMap<u32, ClosedBy>,
    /// The highest stream evicted from `streams`. How a stream at or below it closed is unknown.
    forgotten_through: u32,
}

impl ClosedStreams {
    #[cold]
    #[inline(never)]
    pub(crate) fn record(&mut self, stream: u32, by: ClosedBy) {
        let recorded = self.streams.entry(stream).or_insert(by);
        *recorded = (*recorded).max(by);
        if self.streams.len() > MAX_CLOSED_STREAMS
            && let Some((lowest, _)) = self.streams.pop_first()
        {
            self.forgotten_through = self.forgotten_through.max(lowest);
        }
    }

    pub(crate) fn lookup(&self, stream: u32) -> Recorded {
        match self.streams.get(&stream) {
            Some(by) => Recorded::Closed(*by),
            None if stream <= self.forgotten_through => Recorded::Forgotten,
            None => Recorded::Unrecorded,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reset_overrides_an_abandoned_stream_and_a_local_reset_wins() {
        let mut closed = ClosedStreams::default();
        closed.record(1, ClosedBy::Abandoned);
        closed.record(1, ClosedBy::PeerReset);
        assert_eq!(closed.lookup(1), Recorded::Closed(ClosedBy::PeerReset));
        closed.record(1, ClosedBy::LocalReset);
        closed.record(1, ClosedBy::PeerReset);
        assert_eq!(closed.lookup(1), Recorded::Closed(ClosedBy::LocalReset));
        assert_eq!(closed.lookup(3), Recorded::Unrecorded);
    }

    #[test]
    fn eviction_forgets_the_lowest_streams() {
        let mut closed = ClosedStreams::default();
        for index in 0..=MAX_CLOSED_STREAMS as u32 {
            closed.record(2 * index + 3, ClosedBy::LocalReset);
        }
        // Stream 3 was evicted, so stream 1, which was never recorded, is unknown too.
        assert_eq!(closed.lookup(1), Recorded::Forgotten);
        assert_eq!(closed.lookup(3), Recorded::Forgotten);
        assert_eq!(closed.lookup(5), Recorded::Closed(ClosedBy::LocalReset));
        let highest = 2 * MAX_CLOSED_STREAMS as u32 + 3;
        assert_eq!(
            closed.lookup(highest),
            Recorded::Closed(ClosedBy::LocalReset)
        );
        assert_eq!(closed.lookup(highest + 2), Recorded::Unrecorded);
        // A full record evicts the lowest stream, even the one just recorded.
        closed.record(1, ClosedBy::PeerReset);
        assert_eq!(closed.lookup(1), Recorded::Forgotten);
        assert_eq!(closed.lookup(5), Recorded::Closed(ClosedBy::LocalReset));
        closed.record(highest + 2, ClosedBy::Abandoned);
        assert_eq!(closed.lookup(5), Recorded::Forgotten);
        assert_eq!(
            closed.lookup(highest + 2),
            Recorded::Closed(ClosedBy::Abandoned)
        );
    }
}
