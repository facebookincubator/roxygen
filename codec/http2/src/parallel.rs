/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Common stream-id and graceful-drain state for multiplexed codecs.

use roxygen_core::http::TransportDirection;
use roxygen_core::ids::StreamId;

use crate::constants::MAX_STREAM_ID;

/// HTTP/2 connection closing progression.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClosingState {
    Open,
    OpenWithGracefulDrainEnabled,
    FirstGoawaySent,
    Closed,
}

/// Runtime-neutral common state for a parallel stream codec.
#[derive(Clone, Debug)]
pub struct ParallelCodecState {
    direction: TransportDirection,
    next_egress_stream: u32,
    last_incoming_stream: u32,
    ingress_goaway_ack: u32,
    ingress_goaway_received: bool,
    egress_goaway_ack: u32,
    closing: ClosingState,
}

impl ParallelCodecState {
    pub fn new(direction: TransportDirection) -> Self {
        Self {
            direction,
            next_egress_stream: if direction.is_upstream() { 1 } else { 2 },
            last_incoming_stream: 0,
            ingress_goaway_ack: MAX_STREAM_ID,
            ingress_goaway_received: false,
            egress_goaway_ack: MAX_STREAM_ID,
            closing: ClosingState::Open,
        }
    }

    pub const fn direction(&self) -> TransportDirection {
        self.direction
    }

    pub fn create_stream(&mut self) -> StreamId {
        let stream = self.next_egress_stream;
        self.next_egress_stream = self.next_egress_stream.saturating_add(2);
        StreamId::new(u64::from(stream))
    }

    pub fn set_next_egress_stream_id(&mut self, stream: u32) {
        if stream > self.next_egress_stream
            && stream & 1 == self.next_egress_stream & 1
            && self.next_egress_stream < MAX_STREAM_ID
        {
            self.next_egress_stream = stream;
        }
    }

    pub const fn next_egress_stream_id(&self) -> u32 {
        self.next_egress_stream
    }

    pub const fn is_initiated_stream(&self, stream: u32) -> bool {
        Self::is_initiated_stream_for(self.direction, stream)
    }

    pub const fn is_initiated_stream_for(direction: TransportDirection, stream: u32) -> bool {
        (stream & 1 == 1) == direction.is_upstream()
    }

    pub const fn stream_allowed(&self, stream: u32) -> bool {
        if self.is_initiated_stream(stream) {
            stream <= self.ingress_goaway_ack
        } else {
            stream <= self.egress_goaway_ack
        }
    }

    pub const fn is_waiting_to_drain(&self) -> bool {
        matches!(
            self.closing,
            ClosingState::Open | ClosingState::FirstGoawaySent
        )
    }

    pub const fn is_reusable(&self) -> bool {
        let closing_allows_reuse = matches!(
            self.closing,
            ClosingState::Open | ClosingState::OpenWithGracefulDrainEnabled
        ) || (self.direction.is_downstream()
            && self.is_waiting_to_drain());
        closing_allows_reuse
            && !self.ingress_goaway_received
            && self.ingress_goaway_ack == MAX_STREAM_ID
            && self.next_egress_stream <= MAX_STREAM_ID - 2
    }

    pub fn enable_double_goaway_drain(&mut self) {
        if self.closing == ClosingState::Open {
            self.closing = ClosingState::OpenWithGracefulDrainEnabled;
        }
    }

    pub fn disable_double_goaway_drain(&mut self) {
        if self.closing == ClosingState::OpenWithGracefulDrainEnabled {
            self.closing = ClosingState::Open;
        }
    }

    pub const fn closing_state(&self) -> ClosingState {
        self.closing
    }

    pub const fn last_incoming_stream_id(&self) -> u32 {
        self.last_incoming_stream
    }

    pub fn note_incoming_stream(&mut self, stream: u32) {
        self.last_incoming_stream = self.last_incoming_stream.max(stream);
    }

    pub fn receive_goaway(&mut self, last_stream: u32) -> bool {
        if self.ingress_goaway_received && last_stream >= self.ingress_goaway_ack {
            return false;
        }
        self.ingress_goaway_received = true;
        self.ingress_goaway_ack = self.ingress_goaway_ack.min(last_stream);
        true
    }

    pub const fn ingress_goaway_ack(&self) -> u32 {
        self.ingress_goaway_ack
    }

    pub const fn egress_goaway_ack(&self) -> u32 {
        self.egress_goaway_ack
    }

    pub fn send_goaway(&mut self, requested_last_stream: u32, has_error: bool) -> Option<u32> {
        if self.closing == ClosingState::Closed {
            return None;
        }
        let last_stream = self.egress_goaway_ack.min(requested_last_stream);
        self.egress_goaway_ack = last_stream;
        self.closing = match self.closing {
            ClosingState::Open | ClosingState::OpenWithGracefulDrainEnabled
                if last_stream == MAX_STREAM_ID && !has_error =>
            {
                ClosingState::FirstGoawaySent
            }
            ClosingState::Open
            | ClosingState::OpenWithGracefulDrainEnabled
            | ClosingState::FirstGoawaySent => ClosingState::Closed,
            ClosingState::Closed => ClosingState::Closed,
        };
        Some(last_stream)
    }
}
