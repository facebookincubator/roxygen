/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::io::IoSlice;

use bytes::Bytes;

use crate::BufQueue;
use crate::BufQueueError;
use crate::COMPACT_WRITE_LIMIT;

fn queue_with_segments(limit: usize, segments: &[&[u8]]) -> BufQueue {
    let mut queue = BufQueue::new(limit);
    for segment in segments {
        queue
            .push_bytes(Bytes::copy_from_slice(segment))
            .expect("test segment must fit");
    }
    queue
}

fn contents(queue: &BufQueue) -> Vec<u8> {
    queue
        .segments()
        .flat_map(|segment| segment.iter().copied())
        .collect()
}

#[test]
fn partial_trim_keeps_unique_backing() {
    let mut queue = BufQueue::from_bytes(Bytes::from(b"prefix-body".to_vec()), 32).unwrap();
    let pointer = queue.front().unwrap().as_ptr();
    assert!(queue.front().unwrap().is_unique());
    queue.trim_start(7).unwrap();
    assert_eq!(contents(&queue), b"body");
    assert_eq!(queue.front().unwrap().as_ptr(), pointer.wrapping_add(7));
    assert!(queue.front().unwrap().is_unique());
}

#[test]
fn trim_owner_panic_leaves_canonical_suffix() {
    struct PanickingOwner;

    impl AsRef<[u8]> for PanickingOwner {
        fn as_ref(&self) -> &[u8] {
            b"prefix"
        }
    }

    impl Drop for PanickingOwner {
        fn drop(&mut self) {
            panic!("trim owner panic");
        }
    }

    let mut queue = BufQueue::new(32);
    queue.push_bytes(Bytes::from_owner(PanickingOwner)).unwrap();
    queue.push_static(b"suffix").unwrap();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        queue.trim_start(8).unwrap();
    }))
    .unwrap_err();
    assert_eq!(panic.downcast_ref::<&str>(), Some(&"trim owner panic"));
    assert_eq!(queue.chain_length(), 6);
    assert_eq!(queue.segment_count(), 1);
    assert_eq!(contents(&queue), b"suffix");
    queue.trim_start(6).unwrap();
    assert!(queue.is_empty());
    assert_eq!(queue.backing_capacity_upper_bound(), Some(0));
}

#[test]
fn compact_tail_growth_copies_fragmented_body_and_tracks_capacity() {
    for trimmed in [false, true] {
        let mut allocation = Vec::with_capacity(8);
        if trimmed {
            allocation.extend_from_slice(b"xx");
        }
        allocation.extend_from_slice(b"prefix");
        let mut output = BufQueue::from_vec(allocation, 14).unwrap();
        if trimmed {
            output.trim_start(2).unwrap();
        }
        let body = queue_with_segments(7, &[b"pay", b"load"]);
        assert!(output.try_append_compact_to_tail(b"h", &body).unwrap());
        assert_eq!(contents(&output), b"prefixhpayload");
        assert_eq!(contents(&body), b"payload");
        assert_eq!(output.segment_count(), 1);
        assert_eq!(output.remaining_capacity(), 0);
        let bound = output.backing_capacity_upper_bound().unwrap();
        let allocation = Vec::<u8>::from(
            output
                .into_segment_iter()
                .next()
                .unwrap()
                .try_into_mut()
                .unwrap(),
        );
        assert_eq!(bound, allocation.capacity());
        assert!(bound <= COMPACT_WRITE_LIMIT);
    }
}

#[test]
fn compact_tail_spare_capacity_appends_in_place() {
    let mut allocation = Vec::with_capacity(32);
    allocation.extend_from_slice(b"prefix");
    let pointer = allocation.as_ptr();
    let mut output = BufQueue::from_vec(allocation, 64).unwrap();
    let body = queue_with_segments(7, &[b"pay", b"load"]);

    assert!(output.try_append_compact_to_tail(b"h", &body).unwrap());
    assert_eq!(contents(&output), b"prefixhpayload");
    assert_eq!(output.segment_count(), 1);
    assert_eq!(
        output.front().unwrap().as_ptr(),
        pointer,
        "spare capacity should be reused without moving the tail"
    );
    assert_eq!(output.backing_capacity_upper_bound(), Some(32));
    assert_eq!(output.remaining_capacity(), 64 - 14);
}

#[test]
fn compact_tail_growth_leaves_room_for_later_appends() {
    let mut output = BufQueue::new(64 * 1024);
    output.push_copy(b"headers").unwrap();
    let body = queue_with_segments(16, &[b"0123456789", b"abcdef"]);
    let mut expected = b"headers".to_vec();
    let mut tails = vec![(
        output.backing_capacity_upper_bound().unwrap(),
        output.front().unwrap().as_ptr(),
    )];
    while output.chain_length() + 17 <= COMPACT_WRITE_LIMIT {
        assert!(output.try_append_compact_to_tail(b"h", &body).unwrap());
        expected.extend_from_slice(b"h0123456789abcdef");
        assert_eq!(output.segment_count(), 1);
        let tail = (
            output.backing_capacity_upper_bound().unwrap(),
            output.front().unwrap().as_ptr(),
        );
        if tails.last().map(|last| last.0) != Some(tail.0) {
            tails.push(tail);
        } else {
            assert_eq!(
                tails.last(),
                Some(&tail),
                "an append with room stays in place"
            );
        }
    }
    assert_eq!(contents(&output), expected);
    // Exact copies while the tail holds at most 128 bytes, then tails of 512, 1024 and 2048.
    let capacities = tails.iter().map(|tail| tail.0).collect::<Vec<_>>();
    let exact = (0..8).map(|appends| 7 + 17 * appends);
    assert_eq!(
        capacities,
        exact
            .chain([512, 1024, COMPACT_WRITE_LIMIT])
            .collect::<Vec<_>>()
    );
    assert!(!output.try_append_compact_to_tail(b"h", &body).unwrap());
    assert_eq!(contents(&output), expected);
}

#[test]
fn compact_tail_growth_counts_the_backing_of_earlier_segments() {
    let mut output = BufQueue::new(64 * 1024);
    output.push_vec(vec![b'x'; 1500]).unwrap();
    output.push_vec(b"tail".to_vec()).unwrap();
    let body = queue_with_segments(16, &[b"0123456789abcdef"]);
    let mut bounds = Vec::new();
    while output.try_append_compact_to_tail(b"", &body).unwrap() {
        bounds.push(output.backing_capacity_upper_bound().unwrap());
    }
    // Past 128 bytes the tail grows to 512 bytes, then to the 548 bytes that the limit leaves
    // beside the first segment. The append after that grows it to an exact size past the limit,
    // which ends reuse.
    assert_eq!(bounds.len(), 35);
    assert_eq!(
        (bounds[6], bounds[7], bounds[31]),
        (1500 + 116, 1500 + 512, COMPACT_WRITE_LIMIT)
    );
    assert!(
        bounds[..34]
            .iter()
            .all(|bound| *bound <= COMPACT_WRITE_LIMIT)
    );
    assert_eq!(bounds[34], 1500 + 4 + 35 * 16);
    assert_eq!(output.chain_length(), 1500 + 4 + 35 * 16);
    assert_eq!(output.segment_count(), 2);
}

#[test]
fn compact_tail_rejects_aliases_hidden_capacity_and_excess_output() {
    let body = queue_with_segments(4, &[b"body"]);
    let mut shared = BufQueue::from_vec(b"prefix".to_vec(), 16).unwrap();
    let alias = shared.clone_shared();
    assert!(!shared.try_append_compact_to_tail(b"h", &body).unwrap());
    assert_eq!(contents(&shared), b"prefix");
    assert_eq!(contents(&alias), b"prefix");

    let mut large =
        BufQueue::from_vec(vec![b'x'; COMPACT_WRITE_LIMIT * 2], COMPACT_WRITE_LIMIT * 2).unwrap();
    large.trim_start(COMPACT_WRITE_LIMIT * 2 - 1).unwrap();
    let pointer = large.front().unwrap().as_ptr();
    assert!(!large.try_append_compact_to_tail(b"h", &body).unwrap());
    assert_eq!(large.front().unwrap().as_ptr(), pointer);
    assert_eq!(contents(&large), b"x");

    let mut opaque = BufQueue::from_bytes(Bytes::from(b"prefix".to_vec()), 16).unwrap();
    assert!(!opaque.try_append_compact_to_tail(b"h", &body).unwrap());
    assert_eq!(contents(&opaque), b"prefix");
    let mut limited = BufQueue::from_vec(b"prefix".to_vec(), 10).unwrap();
    assert_eq!(
        limited.try_append_compact_to_tail(b"h", &body),
        Err(BufQueueError::LimitExceeded {
            limit: 10,
            current: 6,
            incoming: 5,
        })
    );
    assert_eq!(contents(&limited), b"prefix");
    assert_eq!(contents(&body), b"body");
}

#[test]
fn borrowed_segment_iterator_preserves_position_and_length() {
    let backing = Bytes::from((0_u8..32).collect::<Vec<_>>());
    let mut queue = BufQueue::new(32);
    for index in 0..16 {
        queue.push_bytes(backing.slice(index..index + 1)).unwrap();
    }
    queue.trim_start(13).unwrap();
    for index in 16..29 {
        queue.push_bytes(backing.slice(index..index + 1)).unwrap();
    }

    let mut segments: crate::BufQueueSegments<'_> = queue.segments();
    assert_eq!(segments.len(), 16);
    assert_eq!(segments.size_hint(), (16, Some(16)));
    assert_eq!(segments.next().unwrap().as_ref(), &[13]);
    let mut cloned = segments.clone();
    for index in 14..29 {
        assert_eq!(segments.len(), 29 - index);
        assert_eq!(segments.size_hint(), (29 - index, Some(29 - index)));
        let segment = segments.next().unwrap();
        assert_eq!(segment.as_ref(), &[index as u8]);
        assert_eq!(segment.as_ptr(), backing.slice(index..index + 1).as_ptr());
    }
    assert_eq!(segments.len(), 0);
    assert_eq!(segments.size_hint(), (0, Some(0)));
    assert!(segments.next().is_none());
    assert_eq!(cloned.len(), 15);
    assert_eq!(cloned.next().unwrap().as_ref(), &[14]);
    assert_eq!(cloned.len(), 14);
    assert_eq!(
        cloned
            .flat_map(|segment| segment.iter().copied())
            .collect::<Vec<_>>(),
        (15_u8..29).collect::<Vec<_>>(),
    );
    assert_eq!(contents(&queue), (13_u8..29).collect::<Vec<_>>());
    assert_eq!(BufQueue::new(0).segments().len(), 0);
}

#[test]
fn append() {
    let mut queue = queue_with_segments(32, &[b"Hello"]);
    let mut queue2 = queue_with_segments(32, &[b", ", b"World"]);
    let expected_pointers: Vec<_> = queue
        .segments()
        .chain(queue2.segments())
        .map(|segment| segment.as_ptr())
        .collect();

    queue.append(&mut queue2).unwrap();

    assert_eq!(queue.chain_length(), 12);
    assert_eq!(queue.segment_count(), 3);
    assert_eq!(contents(&queue), b"Hello, World");
    assert_eq!(
        queue
            .segments()
            .map(|segment| segment.as_ptr())
            .collect::<Vec<_>>(),
        expected_pointers,
    );
    assert!(queue2.is_empty());
    assert!(queue2.front().is_none());
}

#[test]
fn append2() {
    let queue = queue_with_segments(32, &[b"Hello"]);
    let queue2 = queue_with_segments(32, &[b", ", b"World"]);

    assert_eq!(queue.chain_length(), 5);
    assert_eq!(queue.segment_count(), 1);
    assert_eq!(queue2.chain_length(), 7);
    assert_eq!(queue2.segment_count(), 2);
}

#[test]
fn append_string_piece() {
    let value = String::from("Hello, World");
    let queue = queue_with_segments(32, &[value.as_bytes()]);
    let queue2 = BufQueue::from_bytes(Bytes::copy_from_slice(value.as_bytes()), 32).unwrap();

    assert_eq!(queue.chain_length(), value.len());
    assert_eq!(queue2.chain_length(), value.len());
    assert_eq!(contents(&queue), contents(&queue2));
}

#[test]
fn split() {
    let mut queue = queue_with_segments(64, &[b"Hello", b",", b" ", b"", b"World"]);
    assert_eq!(queue.chain_length(), 12);

    let prefix = queue.split_at_most(1);
    assert_eq!(contents(&prefix), b"H");
    assert_eq!(queue.chain_length(), 11);

    let prefix = queue.split_at_most(2);
    assert_eq!(contents(&prefix), b"el");
    assert_eq!(queue.chain_length(), 9);

    let prefix = queue.split_at_most(3);
    assert_eq!(contents(&prefix), b"lo,");
    assert_eq!(queue.chain_length(), 6);

    let prefix = queue.split_at_most(1);
    assert_eq!(contents(&prefix), b" ");
    assert_eq!(queue.chain_length(), 5);

    let prefix = queue.split_at_most(5);
    assert_eq!(contents(&prefix), b"World");
    assert!(queue.is_empty());
    assert!(queue.front().is_none());

    queue.push_bytes(Bytes::from_static(b"Hello,")).unwrap();
    let prefix = queue.split_at_most(3);
    assert_eq!(contents(&prefix), b"Hel");
    assert_eq!(queue.chain_length(), 3);

    queue.push_bytes(Bytes::from_static(b" World")).unwrap();
    let prefix = queue.split_at_most(13);
    assert_eq!(contents(&prefix), b"lo, World");
    assert_eq!(prefix.chain_length(), 9);
    assert!(queue.is_empty());
}

#[test]
fn split_zero() {
    let mut queue = queue_with_segments(32, &[b"Hello world"]);
    let original = queue.front().unwrap().as_ptr();

    let split = queue.split_at_most(0);

    assert!(split.is_empty());
    assert_eq!(queue.chain_length(), 11);
    assert_eq!(queue.front().unwrap().as_ptr(), original);
}

#[test]
fn split_empty() {
    let mut queue = BufQueue::new(32);
    assert!(queue.split_at_most(0).is_empty());
}

#[test]
fn split_emptt() {
    let mut queue = BufQueue::new(32);
    assert!(queue.split_at_most(1).is_empty());
}

#[test]
fn trim_start_at_most() {
    let mut queue = queue_with_segments(64, &[b"Hello"]);
    assert_eq!(queue.trim_start_at_most(3), 3);
    assert_eq!(contents(&queue), b"lo");
    assert_eq!(queue.trim_start_at_most(2), 2);
    assert!(queue.is_empty());

    queue = queue_with_segments(64, &[b"Hello", b"World"]);
    assert_eq!(queue.trim_start_at_most(7), 7);
    assert_eq!(contents(&queue), b"rld");
    assert_eq!(queue.trim_start_at_most(10), 3);
    assert!(queue.is_empty());

    queue = queue_with_segments(64, &[b"Hello", b"World"]);
    assert_eq!(queue.trim_start_at_most(12), 10);
    assert!(queue.is_empty());

    queue = queue_with_segments(64, &[b"Hello", b"World", b"Hello"]);
    assert_eq!(queue.trim_start_at_most(12), 12);
    assert_eq!(contents(&queue), b"llo");
    assert_eq!(queue.chain_length(), 3);
}

#[test]
fn trim_start_one_byte() {
    let mut queue = queue_with_segments(1, &[b"H"]);
    queue.trim_start(1).unwrap();

    assert!(queue.is_empty());
    assert!(queue.front().is_none());
}

#[test]
fn trim_start_clear_chain() {
    let alphabet = b"abcdefghijklmnopqrstuvwxyz";
    let mut queue = queue_with_segments(alphabet.len() * 2, &[alphabet, alphabet]);

    assert_eq!(queue.trim_start_at_most(alphabet.len() * 2 + 1), 52);
    assert!(queue.is_empty());
    assert!(queue.front().is_none());
}

#[test]
fn clone_buf_null() {
    let queue = BufQueue::new(32);
    let clone = queue.clone_shared();

    assert!(clone.is_empty());
    assert!(clone.front().is_none());
}

#[test]
fn clone_buf() {
    let value = b"Hello, World";
    let mut queue = queue_with_segments(64, &[value]);
    let clone = queue.clone_shared();

    assert_eq!(contents(&queue), value);
    assert_eq!(contents(&clone), value);
    queue.push_bytes(Bytes::copy_from_slice(value)).unwrap();
    assert_eq!(queue.chain_length(), value.len() * 2);
    assert_eq!(clone.chain_length(), value.len());

    let clone = queue.clone_shared();
    assert_eq!(clone.chain_length(), value.len() * 2);
}

#[test]
fn clone_shares_segments_without_aliasing_mutation() {
    let mut queue = queue_with_segments(64, &[b"first", b"second"]);
    let clone = queue.clone_shared();
    let original_pointers: Vec<_> = queue.segments().map(|segment| segment.as_ptr()).collect();
    let clone_pointers: Vec<_> = clone.segments().map(|segment| segment.as_ptr()).collect();

    assert_eq!(original_pointers, clone_pointers);
    assert_eq!(queue.trim_start_at_most(3), 3);
    queue.push_bytes(Bytes::from_static(b"third")).unwrap();
    assert_eq!(contents(&clone), b"firstsecond");
    assert_eq!(contents(&queue), b"stsecondthird");
}

#[test]
fn partial_view_shares_backing() {
    let input = Bytes::from_static(b"abcdefgh");
    let mut queue = BufQueue::from_bytes(input.clone(), 64).unwrap();

    let prefix = queue.split_at_most(3);

    assert_eq!(contents(&prefix), b"abc");
    assert_eq!(contents(&queue), b"defgh");
    assert_eq!(prefix.front().unwrap().as_ptr(), input.as_ptr());
    assert_eq!(queue.front().unwrap().as_ptr(), input.slice(3..).as_ptr());
}

#[test]
fn progressive_split_trim_shares_backing() {
    let first = Bytes::from_static(b"abcdef");
    let second = Bytes::from_static(b"ghijkl");
    let mut queue = BufQueue::new(64);
    queue.push_bytes(first.clone()).unwrap();
    queue.push_bytes(second.clone()).unwrap();

    let prefix = queue.split_at_most(2);
    assert_eq!(contents(&prefix), b"ab");
    assert_eq!(contents(&queue), b"cdefghijkl");
    assert_eq!(prefix.front().unwrap().as_ptr(), first.as_ptr());
    assert_eq!(queue.front().unwrap().as_ptr(), first.slice(2..).as_ptr());

    assert_eq!(queue.trim_start_at_most(1), 1);
    assert_eq!(contents(&queue), b"defghijkl");
    assert_eq!(queue.front().unwrap().as_ptr(), first.slice(3..).as_ptr());
    assert_eq!(queue.segments().nth(1).unwrap().as_ptr(), second.as_ptr());
}

#[test]
fn public_adapter_bytes_share_backing() {
    fn adapter_admit(queue: &mut BufQueue, bytes: Bytes) -> Result<(), BufQueueError> {
        queue.push_bytes(bytes)
    }

    let input = Bytes::from_static(b"adapter bytes");
    let input_pointer = input.as_ptr();
    let mut queue = BufQueue::new(input.len());

    adapter_admit(&mut queue, input.clone()).unwrap();

    assert_eq!(contents(&queue), input.as_ref());
    assert_eq!(queue.front().unwrap().as_ptr(), input_pointer);
    assert_eq!(queue.remaining_capacity(), 0);
}

#[test]
fn vectored_iteration_preserves_segments_without_coalescing() {
    let pieces: Vec<Vec<u8>> = (0_u8..20).map(|value| vec![value; 2]).collect();
    let mut queue = BufQueue::new(40);
    for piece in &pieces {
        queue.push_bytes(Bytes::copy_from_slice(piece)).unwrap();
    }
    let pointers_before: Vec<_> = queue.segments().map(|segment| segment.as_ptr()).collect();

    let iovecs: Vec<IoSlice<'_>> = queue.iovecs().collect();

    assert_eq!(iovecs.len(), 20);
    for (iovec, expected) in iovecs.iter().zip(&pieces) {
        assert_eq!(iovec.as_ref(), expected);
    }
    assert_eq!(queue.segment_count(), 20);
    assert_eq!(
        queue
            .segments()
            .map(|segment| segment.as_ptr())
            .collect::<Vec<_>>(),
        pointers_before,
    );
    assert_eq!(iovecs[..16].len(), 16);
    assert_eq!(iovecs[16..].len(), 4);
}

#[test]
fn take_empties_source_and_preserves_chain_length() {
    let mut queue = queue_with_segments(32, &[b"Hello", b", ", b"World"]);
    let pointers: Vec<_> = queue.segments().map(|segment| segment.as_ptr()).collect();

    let moved = queue.take();

    assert!(queue.is_empty());
    assert_eq!(queue.limit(), 32);
    assert_eq!(moved.chain_length(), 12);
    assert_eq!(contents(&moved), b"Hello, World");
    assert_eq!(
        moved
            .segments()
            .map(|segment| segment.as_ptr())
            .collect::<Vec<_>>(),
        pointers,
    );
}

#[test]
fn trim_start_at_most_zero_is_noop() {
    let mut queue = queue_with_segments(32, &[b"Hello", b"World"]);
    let pointers: Vec<_> = queue.segments().map(|segment| segment.as_ptr()).collect();

    assert_eq!(queue.trim_start_at_most(0), 0);

    assert_eq!(contents(&queue), b"HelloWorld");
    assert_eq!(
        queue
            .segments()
            .map(|segment| segment.as_ptr())
            .collect::<Vec<_>>(),
        pointers,
    );
}

#[test]
fn checked_trim_underflow_is_error() {
    let mut queue = queue_with_segments(32, &[b"Hello", b"World"]);
    let pointers: Vec<_> = queue.segments().map(|segment| segment.as_ptr()).collect();

    assert_eq!(
        queue.trim_start(11),
        Err(BufQueueError::TrimUnderflow {
            requested: 11,
            available: 10,
        }),
    );
    assert_eq!(contents(&queue), b"HelloWorld");
    assert_eq!(
        queue
            .segments()
            .map(|segment| segment.as_ptr())
            .collect::<Vec<_>>(),
        pointers,
    );
}

#[test]
fn from_bytes_shares_backing() {
    let input = Bytes::from_static(b"public");
    let pointer = input.as_ptr();

    let queue = BufQueue::from_bytes(input.clone(), input.len()).unwrap();

    assert_eq!(contents(&queue), input.as_ref());
    assert_eq!(queue.front().unwrap().as_ptr(), pointer);
    assert_eq!(queue.chain_length(), queue.limit());
}

#[test]
fn push_bytes_shares_backing() {
    let input = Bytes::from_static(b"public");
    let pointer = input.as_ptr();
    let mut queue = BufQueue::new(input.len());

    queue.push_bytes(input.clone()).unwrap();

    assert_eq!(contents(&queue), input.as_ref());
    assert_eq!(queue.front().unwrap().as_ptr(), pointer);
    assert_eq!(queue.remaining_capacity(), 0);
}

#[test]
fn copy_from_slice_uses_distinct_backing() {
    let mut input = b"copy me".to_vec();
    let input_pointer = input.as_ptr();

    let queue = BufQueue::copy_from_slice(&input, input.len()).unwrap();
    input.fill(b'x');

    assert_eq!(contents(&queue), b"copy me");
    assert_ne!(queue.front().unwrap().as_ptr(), input_pointer);
    assert_eq!(queue.remaining_capacity(), 0);
}

#[test]
fn push_copy_uses_distinct_backing() {
    let mut input = b"copy me".to_vec();
    let input_pointer = input.as_ptr();
    let mut queue = BufQueue::new(input.len());

    queue.push_copy(&input).unwrap();
    input.fill(b'x');

    assert_eq!(contents(&queue), b"copy me");
    assert_ne!(queue.front().unwrap().as_ptr(), input_pointer);
    assert_eq!(queue.remaining_capacity(), 0);
}

#[test]
fn push_copy_over_limit_leaves_queue_unchanged() {
    let existing = Bytes::from_static(b"kept");
    let pointer = existing.as_ptr();
    let mut queue = BufQueue::from_bytes(existing, 5).unwrap();

    assert_eq!(
        queue.push_copy(b"too large"),
        Err(BufQueueError::LimitExceeded {
            limit: 5,
            current: 4,
            incoming: 9,
        }),
    );
    assert_eq!(contents(&queue), b"kept");
    assert_eq!(queue.front().unwrap().as_ptr(), pointer);
    assert_eq!(queue.remaining_capacity(), 1);
}

#[test]
fn partial_slice_shares_backing_and_charges_visible_length() {
    let backing = Bytes::from(vec![b'x'; 4096]);
    let visible = backing.slice(2048..2055);
    let pointer = visible.as_ptr();

    let queue = BufQueue::from_bytes(visible.clone(), 8).unwrap();

    assert_eq!(contents(&queue), vec![b'x'; 7]);
    assert_eq!(queue.front().unwrap().as_ptr(), pointer);
    assert_eq!(queue.chain_length(), 7);
    assert_eq!(queue.remaining_capacity(), 1);
}

#[test]
fn into_bytes_segments_is_o1_and_zero_copy() {
    let queue = queue_with_segments(32, &[b"one", b"two", b"three"]);
    let pointers: Vec<_> = queue.segments().map(|segment| segment.as_ptr()).collect();

    let segments = queue.into_bytes_segments();

    assert_eq!(segments.len(), 3);
    assert_eq!(
        segments
            .iter()
            .map(|segment| segment.as_ptr())
            .collect::<Vec<_>>(),
        pointers,
    );
    assert_eq!(
        segments
            .iter()
            .flat_map(|segment| segment.iter().copied())
            .collect::<Vec<_>>(),
        b"onetwothree",
    );
}

#[test]
fn adapter_boundaries_make_copying_explicit() {
    let public = Bytes::from_static(b"public");
    let public_pointer = public.as_ptr();
    let mut queue = BufQueue::new(public.len());
    queue.push_bytes(public.clone()).unwrap();
    assert_eq!(queue.front().unwrap().as_ptr(), public_pointer);

    let rejected = Bytes::from_static(b"!");
    assert_eq!(
        queue.push_bytes(rejected),
        Err(BufQueueError::LimitExceeded {
            limit: 6,
            current: 6,
            incoming: 1,
        }),
    );
    assert_eq!(contents(&queue), b"public");

    let mut destination = queue_with_segments(5, &[b"one"]);
    let mut source = queue_with_segments(5, &[b"two"]);
    assert_eq!(
        destination.append(&mut source),
        Err(BufQueueError::LimitExceeded {
            limit: 5,
            current: 3,
            incoming: 3,
        }),
    );
    assert_eq!(contents(&destination), b"one");
    assert_eq!(contents(&source), b"two");

    let copied = b"inside".to_vec();
    let copied_pointer = copied.as_ptr();
    let copied_queue = BufQueue::copy_from_slice(&copied, copied.len()).unwrap();
    assert_ne!(copied_queue.front().unwrap().as_ptr(), copied_pointer);
}

#[test]
fn coalescing_requires_a_sufficient_explicit_maximum() {
    let queue = queue_with_segments(16, &[b"one", b"two"]);
    let pointers: Vec<_> = queue.segments().map(|segment| segment.as_ptr()).collect();

    assert_eq!(
        queue.coalesce(5),
        Err(BufQueueError::CoalesceLimitExceeded {
            limit: 5,
            length: 6,
        }),
    );
    assert_eq!(queue.coalesce(6).unwrap(), Bytes::from_static(b"onetwo"));
    assert_eq!(queue.segment_count(), 2);
    assert_eq!(
        queue
            .segments()
            .map(|segment| segment.as_ptr())
            .collect::<Vec<_>>(),
        pointers,
    );
}

#[test]
fn coalesce_in_place_reuses_one_contiguous_segment() {
    let mut queue = queue_with_segments(16, &[b"one", b"two"]);

    queue.coalesce_in_place().unwrap();

    assert_eq!(queue.segment_count(), 1);
    assert_eq!(queue.front().unwrap(), &b"onetwo"[..]);
    let pointer = queue.front().unwrap().as_ptr();
    queue.coalesce_in_place().unwrap();
    assert_eq!(queue.front().unwrap().as_ptr(), pointer);
}

#[test]
fn backing_bound_preserves_spare_capacity_through_shared_views() {
    let mut bytes = Vec::with_capacity(4096);
    bytes.extend_from_slice(b"payload");
    let capacity = bytes.capacity();
    let pointer = bytes.as_ptr();
    let mut queue = BufQueue::from_vec(bytes, 7).unwrap();
    assert_eq!(queue.backing_capacity_upper_bound(), Some(capacity));
    assert_eq!(queue.remaining_capacity(), 0);
    assert_eq!(queue.front().unwrap().as_ptr(), pointer);
    let cloned = queue.clone_shared();
    let mut prefix = queue.split_at_most(2);
    queue.trim_start(3).unwrap();
    for view in [&queue, &prefix, &cloned] {
        assert_eq!(view.backing_capacity_upper_bound(), Some(capacity));
    }
    assert_eq!(prefix.front().unwrap().as_ptr(), pointer);
    assert_eq!(queue.front().unwrap().as_ptr(), pointer.wrapping_add(5));
    prefix.append(&mut queue).unwrap();
    assert_eq!(prefix.backing_capacity_upper_bound(), Some(2 * capacity));
    assert_eq!(queue.backing_capacity_upper_bound(), Some(0));
    prefix.trim_start_at_most(usize::MAX);
    assert_eq!(prefix.backing_capacity_upper_bound(), Some(0));
}

#[test]
fn opaque_backing_fails_closed_until_replaced_or_empty() {
    let bytes = Bytes::from(vec![b'x'; 4096]).slice(2048..2049);
    let pointer = bytes.as_ptr();
    let mut queue = BufQueue::from_bytes(bytes, 2).unwrap();
    assert_eq!(queue.backing_capacity_upper_bound(), None);
    assert_eq!(queue.front().unwrap().as_ptr(), pointer);
    queue.push_bytes(Bytes::new()).unwrap();
    queue.coalesce_in_place().unwrap();
    assert_eq!(queue.backing_capacity_upper_bound(), None);
    assert!(queue.push_vec(vec![1, 2]).is_err());
    assert_eq!(queue.backing_capacity_upper_bound(), None);
    queue.push_copy(b"y").unwrap();
    assert_eq!(queue.backing_capacity_upper_bound(), None);
    queue.coalesce_in_place().unwrap();
    assert_eq!(queue.backing_capacity_upper_bound(), Some(2));
    let moved = queue.take();
    assert_eq!(queue.backing_capacity_upper_bound(), Some(0));
    assert_eq!(moved.backing_capacity_upper_bound(), Some(2));
    queue.push_bytes(Bytes::new()).unwrap();
    assert_eq!(queue.backing_capacity_upper_bound(), Some(0));
}

#[test]
fn inline_queue_transitions_preserve_shared_payloads_and_bounds() {
    let mut queue = BufQueue::new(6);
    queue.push_bytes(Bytes::new()).unwrap();
    assert_eq!(queue.segments().len(), 0);
    assert_eq!(queue.backing_capacity_upper_bound(), Some(0));
    let mut backing = Vec::with_capacity(16);
    backing.extend_from_slice(b"abcd");
    let capacity = backing.capacity();
    let pointer = backing.as_ptr();
    queue.push_vec(backing).unwrap();
    let shared = queue.clone_shared();
    let mut borrowed = queue.segments();
    let mut cloned = borrowed.clone();
    assert_eq!(borrowed.next().unwrap().as_ptr(), pointer);
    assert_eq!(borrowed.len(), 0);
    assert_eq!(cloned.size_hint(), (1, Some(1)));
    assert_eq!(cloned.next().unwrap().as_ptr(), pointer);
    assert!(cloned.next().is_none());

    let mut prefix = queue.split_at_most(1);
    queue.push_static(b"ef").unwrap();
    assert_eq!(contents(&prefix), b"a");
    assert_eq!(prefix.front().unwrap().as_ptr(), pointer);
    assert_eq!(contents(&queue), b"bcdef");
    assert_eq!(queue.front().unwrap().as_ptr(), pointer.wrapping_add(1));
    assert_eq!(queue.segments().size_hint(), (2, Some(2)));
    assert_eq!(prefix.backing_capacity_upper_bound(), Some(capacity));
    assert_eq!(queue.backing_capacity_upper_bound(), Some(capacity));

    let mut rejected = BufQueue::from_bytes(Bytes::from_static(b"XY"), 2).unwrap();
    let rejected_pointer = rejected.front().unwrap().as_ptr();
    assert_eq!(
        queue.append(&mut rejected),
        Err(BufQueueError::LimitExceeded {
            limit: 6,
            current: 5,
            incoming: 2,
        }),
    );
    assert_eq!(contents(&queue), b"bcdef");
    assert_eq!(queue.front().unwrap().as_ptr(), pointer.wrapping_add(1));
    assert_eq!(queue.remaining_capacity(), 1);
    assert_eq!(queue.backing_capacity_upper_bound(), Some(capacity));
    assert_eq!(contents(&rejected), b"XY");
    assert_eq!(rejected.front().unwrap().as_ptr(), rejected_pointer);
    assert_eq!(rejected.remaining_capacity(), 0);
    assert_eq!(rejected.backing_capacity_upper_bound(), None);

    queue.trim_start(3).unwrap();
    assert_eq!(contents(&queue), b"ef");
    assert_eq!(queue.segments().len(), 1);
    queue.trim_start(2).unwrap();
    assert!(queue.front().is_none());
    assert_eq!(queue.backing_capacity_upper_bound(), Some(0));
    queue.push_static(b"xy").unwrap();
    let refill_pointer = queue.front().unwrap().as_ptr();
    queue.append(&mut prefix).unwrap();
    assert!(prefix.is_empty());
    assert_eq!(prefix.backing_capacity_upper_bound(), Some(0));
    assert_eq!(contents(&queue), b"xya");
    assert_eq!(queue.backing_capacity_upper_bound(), Some(capacity));

    let mut moved = BufQueue::new(6);
    moved.append(&mut queue).unwrap();
    assert!(queue.is_empty());
    assert_eq!(queue.limit(), 6);
    assert_eq!(queue.backing_capacity_upper_bound(), Some(0));
    let segments: std::collections::VecDeque<Bytes> = moved.into_bytes_segments();
    assert_eq!(segments.len(), 2);
    assert_eq!(segments[0].as_ref(), b"xy");
    assert_eq!(segments[0].as_ptr(), refill_pointer);
    assert_eq!(segments[1].as_ref(), b"a");
    assert_eq!(segments[1].as_ptr(), pointer);
    let singleton: std::collections::VecDeque<Bytes> = shared.into_bytes_segments();
    assert_eq!(singleton.len(), 1);
    assert_eq!(singleton[0].as_ref(), b"abcd");
    assert_eq!(singleton[0].as_ptr(), pointer);
}

#[test]
fn consuming_segment_iterator_preserves_order_and_owner_lifetimes() {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    struct TrackedBytes {
        bytes: Vec<u8>,
        drops: Arc<AtomicUsize>,
    }

    impl AsRef<[u8]> for TrackedBytes {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    impl Drop for TrackedBytes {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    let mut empty = BufQueue::new(0).into_segment_iter();
    assert_eq!(empty.size_hint(), (0, Some(0)));
    assert!(empty.next().is_none());
    for (count, take_count) in [(1_usize, 0), (1, 1), (3, 2)] {
        let drops: Vec<_> = (0..count).map(|_| Arc::new(AtomicUsize::new(0))).collect();
        let mut queue = BufQueue::new(count);
        let mut pointers = Vec::new();
        for (index, dropped) in drops.iter().enumerate() {
            let bytes = Bytes::from_owner(TrackedBytes {
                bytes: vec![index as u8],
                drops: dropped.clone(),
            });
            pointers.push(bytes.as_ptr());
            queue.push_bytes(bytes).unwrap();
        }
        let alias = queue.front().unwrap().clone();
        let mut segments = queue.into_segment_iter();
        assert_eq!(segments.size_hint(), (count, Some(count)));
        let mut yielded = Vec::new();
        for (index, pointer) in pointers.iter().enumerate().take(take_count) {
            assert_eq!(segments.len(), count - index);
            assert_eq!(segments.size_hint(), (count - index, Some(count - index)));
            let bytes = segments.next().unwrap();
            assert_eq!(bytes.as_ref(), &[index as u8]);
            assert_eq!(bytes.as_ptr(), *pointer);
            yielded.push(bytes);
        }
        assert_eq!(segments.len(), count - yielded.len());
        drop(segments);
        for (index, dropped) in drops.iter().enumerate() {
            assert_eq!(
                dropped.load(Ordering::SeqCst),
                usize::from(index != 0 && index >= yielded.len()),
            );
        }
        drop(yielded);
        assert_eq!(drops[0].load(Ordering::SeqCst), 0);
        assert_eq!(alias.as_ref(), &[0]);
        assert_eq!(alias.as_ptr(), pointers[0]);
        drop(alias);
        for dropped in drops {
            assert_eq!(dropped.load(Ordering::SeqCst), 1);
        }
    }
}

#[test]
fn bulk_split_and_inline_append_move_views_without_dropping_owners() {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    struct TrackedBytes {
        bytes: Vec<u8>,
        drops: Arc<AtomicUsize>,
    }

    impl AsRef<[u8]> for TrackedBytes {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    impl Drop for TrackedBytes {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    // Splits at 9 (bulk, partial boundary), 16 (leaves one whole segment)
    // and 17 (leaves one suffix) over five 4-byte owner-backed segments.
    for amount in [9, 16, 17] {
        let drops: Vec<_> = (0..5).map(|_| Arc::new(AtomicUsize::new(0))).collect();
        let mut queue = BufQueue::new(64);
        let pointers: Vec<_> = drops
            .iter()
            .enumerate()
            .map(|(index, dropped)| {
                let bytes = Bytes::from_owner(TrackedBytes {
                    bytes: vec![b'a' + index as u8; 4],
                    drops: dropped.clone(),
                });
                let pointer = bytes.as_ptr();
                queue.push_bytes(bytes).unwrap();
                pointer
            })
            .collect();

        let mut prefix = queue.split_at_most(amount);
        let boundary = amount / 4;
        let offset = amount % 4;
        assert_eq!(prefix.chain_length(), amount);
        assert_eq!(queue.chain_length(), 20 - amount);
        let prefix_pointers: Vec<_> = prefix.segments().map(|segment| segment.as_ptr()).collect();
        assert_eq!(prefix_pointers[..boundary], pointers[..boundary]);
        assert_eq!(prefix_pointers.len(), boundary + usize::from(offset != 0));
        assert_eq!(
            queue.front().unwrap().as_ptr(),
            pointers[boundary].wrapping_add(offset),
            "the boundary suffix shares its segment's backing",
        );
        assert!(
            drops
                .iter()
                .all(|dropped| dropped.load(Ordering::SeqCst) == 0)
        );

        let mut head = BufQueue::new(64);
        head.push_static(b"head").unwrap();
        head.append(&mut prefix).unwrap();
        assert!(prefix.is_empty());
        assert_eq!(head.chain_length(), 4 + amount);
        assert_eq!(head.backing_capacity_upper_bound(), None);
        assert_eq!(
            head.segments()
                .skip(1)
                .map(|segment| segment.as_ptr())
                .collect::<Vec<_>>(),
            prefix_pointers,
        );
        assert!(
            drops
                .iter()
                .all(|dropped| dropped.load(Ordering::SeqCst) == 0)
        );

        drop(head);
        for (index, dropped) in drops.iter().enumerate() {
            let shared_with_remainder = index >= boundary;
            assert_eq!(
                dropped.load(Ordering::SeqCst),
                usize::from(!shared_with_remainder)
            );
        }
        drop(queue);
        assert!(
            drops
                .iter()
                .all(|dropped| dropped.load(Ordering::SeqCst) == 1)
        );
    }
}

#[test]
fn prefix_moves_share_backing_and_drop_no_owner() {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    struct TrackedBytes {
        bytes: Vec<u8>,
        drops: Arc<AtomicUsize>,
    }

    impl AsRef<[u8]> for TrackedBytes {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    impl Drop for TrackedBytes {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    let drops: Vec<_> = (0..5).map(|_| Arc::new(AtomicUsize::new(0))).collect();
    let mut source = BufQueue::new(64);
    let pointers: Vec<_> = drops
        .iter()
        .enumerate()
        .map(|(index, dropped)| {
            let bytes = Bytes::from_owner(TrackedBytes {
                bytes: vec![b'a' + index as u8; 4],
                drops: dropped.clone(),
            });
            let pointer = bytes.as_ptr();
            source.push_bytes(bytes).unwrap();
            pointer
        })
        .collect();

    let mut short = BufQueue::new(8);
    short.push_static(b"head").unwrap();
    assert_eq!(
        short.append_prefix_from(&mut source, 9),
        Err(BufQueueError::LimitExceeded {
            limit: 8,
            current: 4,
            incoming: 9,
        })
    );
    assert_eq!(contents(&short), b"head");
    assert_eq!(source.chain_length(), 20);
    assert_eq!(
        source
            .segments()
            .map(|segment| segment.as_ptr())
            .collect::<Vec<_>>(),
        pointers,
    );

    let mut destination = BufQueue::with_segment_capacity(64, 8);
    destination.push_static(b"head").unwrap();
    assert_eq!(destination.append_prefix_from(&mut source, 0), Ok(0));
    assert_eq!(destination.append_prefix_from(&mut source, 9), Ok(9));
    assert_eq!(source.chain_length(), 11);
    assert_eq!(
        source.front().unwrap().as_ptr(),
        pointers[2].wrapping_add(1),
        "the boundary suffix shares its segment's backing",
    );
    assert_eq!(destination.append_prefix_from(&mut source, 64), Ok(11));
    assert!(source.is_empty());
    assert_eq!(source.backing_capacity_upper_bound(), Some(0));
    assert_eq!(destination.chain_length(), 24);
    assert_eq!(destination.backing_capacity_upper_bound(), None);
    assert_eq!(
        destination
            .segments()
            .skip(1)
            .map(|segment| segment.as_ptr())
            .collect::<Vec<_>>(),
        [
            pointers[0],
            pointers[1],
            pointers[2],
            pointers[2].wrapping_add(1),
            pointers[3],
            pointers[4],
        ],
    );
    assert!(
        drops
            .iter()
            .all(|dropped| dropped.load(Ordering::SeqCst) == 0)
    );
    drop(source);
    assert!(
        drops
            .iter()
            .all(|dropped| dropped.load(Ordering::SeqCst) == 0)
    );
    drop(destination);
    assert!(
        drops
            .iter()
            .all(|dropped| dropped.load(Ordering::SeqCst) == 1)
    );
}
