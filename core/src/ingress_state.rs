/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

/// States in the HTTP transaction ingress state machine.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum IngressState {
    Start = 0,
    NonFinalHeadersReceived = 1,
    FinalHeadersReceived = 2,
    RegularBodyReceived = 3,
    ChunkHeaderReceived = 4,
    ChunkBodyReceived = 5,
    ChunkCompleted = 6,
    TrailersReceived = 7,
    UpgradeComplete = 8,
    EomQueued = 9,
    ReceivingDone = 10,
}

/// Non-datagram events accepted by the HTTP transaction ingress state machine.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum IngressEvent {
    NonFinalHeaders = 0,
    FinalHeaders = 1,
    Body = 3,
    ChunkHeader = 4,
    ChunkComplete = 5,
    Trailers = 6,
    Upgrade = 7,
    Eom = 8,
    EomFlushed = 9,
}

/// The immutable transition table definition.
#[derive(Debug)]
pub struct HttpTransactionIngressStateMachineData;

impl HttpTransactionIngressStateMachineData {
    pub const NAME: &'static str = "HTTPTransactionIngress";

    pub const fn initial_state() -> IngressState {
        IngressState::Start
    }

    pub const fn find(state: IngressState, event: IngressEvent) -> (IngressState, bool) {
        use IngressEvent as Event;
        use IngressState as State;

        match (state, event) {
            (State::Start, Event::FinalHeaders) => (State::FinalHeadersReceived, true),
            (State::Start, Event::NonFinalHeaders) => (State::NonFinalHeadersReceived, true),
            (State::NonFinalHeadersReceived, Event::NonFinalHeaders) => {
                (State::NonFinalHeadersReceived, true)
            }
            (State::NonFinalHeadersReceived, Event::FinalHeaders) => {
                (State::FinalHeadersReceived, true)
            }
            (State::NonFinalHeadersReceived, Event::Upgrade) => (State::UpgradeComplete, true),
            (State::FinalHeadersReceived, Event::Body) => (State::RegularBodyReceived, true),
            (State::FinalHeadersReceived, Event::ChunkHeader) => (State::ChunkHeaderReceived, true),
            (State::FinalHeadersReceived, Event::Trailers) => (State::TrailersReceived, true),
            (State::FinalHeadersReceived, Event::Upgrade) => (State::UpgradeComplete, true),
            (State::FinalHeadersReceived, Event::Eom) => (State::EomQueued, true),
            (State::RegularBodyReceived, Event::Body) => (State::RegularBodyReceived, true),
            (State::RegularBodyReceived, Event::Trailers) => (State::TrailersReceived, true),
            (State::RegularBodyReceived, Event::Eom) => (State::EomQueued, true),
            (State::ChunkHeaderReceived, Event::Body) => (State::ChunkBodyReceived, true),
            (State::ChunkBodyReceived, Event::Body) => (State::ChunkBodyReceived, true),
            (State::ChunkBodyReceived, Event::ChunkComplete) => (State::ChunkCompleted, true),
            (State::ChunkCompleted, Event::ChunkHeader) => (State::ChunkHeaderReceived, true),
            (State::ChunkCompleted, Event::Trailers) => (State::TrailersReceived, true),
            (State::ChunkCompleted, Event::Eom) => (State::EomQueued, true),
            (State::TrailersReceived, Event::Eom) => (State::EomQueued, true),
            (State::UpgradeComplete, Event::Body) => (State::UpgradeComplete, true),
            (State::UpgradeComplete, Event::Eom) => (State::EomQueued, true),
            (State::EomQueued, Event::EomFlushed) => (State::ReceivingDone, true),
            _ => (state, false),
        }
    }
}

/// Stateless transition helpers; callers retain ownership of the current state.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HttpTransactionIngressStateMachine;

impl HttpTransactionIngressStateMachine {
    pub const fn get_new_instance() -> IngressState {
        HttpTransactionIngressStateMachineData::initial_state()
    }

    pub const fn can_transit(state: IngressState, event: IngressEvent) -> bool {
        HttpTransactionIngressStateMachineData::find(state, event).1
    }

    pub fn transit(state: &mut IngressState, event: IngressEvent) -> bool {
        let (next, valid) = HttpTransactionIngressStateMachineData::find(*state, event);
        if valid {
            *state = next;
        }
        valid
    }
}
