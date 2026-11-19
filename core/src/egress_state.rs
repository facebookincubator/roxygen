/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

/// Non-datagram states in the HTTP transaction egress state machine.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum EgressState {
    Start = 0,
    HeadersSent = 1,
    RegularBodySent = 3,
    ChunkHeaderSent = 4,
    ChunkBodySent = 5,
    ChunkTerminatorSent = 6,
    TrailersSent = 7,
    EomQueued = 8,
    SendingDone = 9,
}

/// Non-datagram events accepted by the HTTP transaction egress state machine.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum EgressEvent {
    SendHeaders = 0,
    SendBody = 2,
    SendChunkHeader = 3,
    SendChunkTerminator = 4,
    SendTrailers = 5,
    SendEom = 6,
    EomFlushed = 7,
}

/// The immutable transition table definition.
#[derive(Debug)]
pub struct HttpTransactionEgressStateMachineData;

impl HttpTransactionEgressStateMachineData {
    pub const NAME: &'static str = "HTTPTransactionEgress";

    pub const fn initial_state() -> EgressState {
        EgressState::Start
    }

    pub const fn find(state: EgressState, event: EgressEvent) -> (EgressState, bool) {
        use EgressEvent as Event;
        use EgressState as State;

        match (state, event) {
            (State::Start, Event::SendHeaders) => (State::HeadersSent, true),
            (State::HeadersSent, Event::SendHeaders) => (State::HeadersSent, true),
            (State::HeadersSent, Event::SendBody) => (State::RegularBodySent, true),
            (State::HeadersSent, Event::SendTrailers) => (State::TrailersSent, true),
            (State::HeadersSent, Event::SendChunkHeader) => (State::ChunkHeaderSent, true),
            (State::HeadersSent, Event::SendEom) => (State::EomQueued, true),
            (State::RegularBodySent, Event::SendBody) => (State::RegularBodySent, true),
            (State::RegularBodySent, Event::SendTrailers) => (State::TrailersSent, true),
            (State::RegularBodySent, Event::SendEom) => (State::EomQueued, true),
            (State::ChunkHeaderSent, Event::SendBody) => (State::ChunkBodySent, true),
            (State::ChunkBodySent, Event::SendBody) => (State::ChunkBodySent, true),
            (State::ChunkBodySent, Event::SendChunkTerminator) => {
                (State::ChunkTerminatorSent, true)
            }
            (State::ChunkTerminatorSent, Event::SendChunkHeader) => (State::ChunkHeaderSent, true),
            (State::ChunkTerminatorSent, Event::SendTrailers) => (State::TrailersSent, true),
            (State::ChunkTerminatorSent, Event::SendEom) => (State::EomQueued, true),
            (State::TrailersSent, Event::SendEom) => (State::EomQueued, true),
            (State::EomQueued, Event::EomFlushed) => (State::SendingDone, true),
            _ => (state, false),
        }
    }
}

/// Stateless transition helpers; callers retain ownership of the current state.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HttpTransactionEgressStateMachine;

impl HttpTransactionEgressStateMachine {
    pub const fn get_new_instance() -> EgressState {
        HttpTransactionEgressStateMachineData::initial_state()
    }

    pub const fn can_transit(state: EgressState, event: EgressEvent) -> bool {
        HttpTransactionEgressStateMachineData::find(state, event).1
    }

    pub fn transit(state: &mut EgressState, event: EgressEvent) -> bool {
        let (next, valid) = HttpTransactionEgressStateMachineData::find(*state, event);
        if valid {
            *state = next;
        }
        valid
    }
}
