/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

mod egress_state {
    use crate::egress_state::EgressEvent;
    use crate::egress_state::EgressState;
    use crate::egress_state::HttpTransactionEgressStateMachine;
    use crate::egress_state::HttpTransactionEgressStateMachineData;

    fn follow(state: &mut EgressState, event: EgressEvent, expected: EgressState) {
        assert!(HttpTransactionEgressStateMachine::transit(state, event));
        assert_eq!(*state, expected);
    }

    fn fail(state: &mut EgressState, event: EgressEvent) {
        let original = *state;
        assert!(!HttpTransactionEgressStateMachine::can_transit(
            *state, event
        ));
        assert!(!HttpTransactionEgressStateMachine::transit(state, event));
        assert_eq!(*state, original);
    }

    #[test]
    fn reject_body_before_headers() {
        let mut machine = HttpTransactionEgressStateMachine::get_new_instance();
        follow(
            &mut machine,
            EgressEvent::SendHeaders,
            EgressState::HeadersSent,
        );
        follow(
            &mut machine,
            EgressEvent::SendChunkHeader,
            EgressState::ChunkHeaderSent,
        );
        follow(
            &mut machine,
            EgressEvent::SendBody,
            EgressState::ChunkBodySent,
        );
        fail(&mut machine, EgressEvent::SendEom);
    }

    #[test]
    fn reject_second_headers() {
        let mut machine = HttpTransactionEgressStateMachine::get_new_instance();
        fail(&mut machine, EgressEvent::SendBody);
    }

    #[test]
    fn reject_body_after_eom() {
        let mut machine = HttpTransactionEgressStateMachine::get_new_instance();
        follow(
            &mut machine,
            EgressEvent::SendHeaders,
            EgressState::HeadersSent,
        );
        for _ in 0..2 {
            follow(
                &mut machine,
                EgressEvent::SendBody,
                EgressState::RegularBodySent,
            );
        }
        follow(&mut machine, EgressEvent::SendEom, EgressState::EomQueued);
        fail(&mut machine, EgressEvent::SendEom);
    }

    #[test]
    fn reject_invalid_chunk_sequence() {
        let mut machine = HttpTransactionEgressStateMachine::get_new_instance();
        follow(
            &mut machine,
            EgressEvent::SendHeaders,
            EgressState::HeadersSent,
        );
        follow(
            &mut machine,
            EgressEvent::SendChunkHeader,
            EgressState::ChunkHeaderSent,
        );
        follow(
            &mut machine,
            EgressEvent::SendBody,
            EgressState::ChunkBodySent,
        );
        follow(
            &mut machine,
            EgressEvent::SendChunkTerminator,
            EgressState::ChunkTerminatorSent,
        );
        follow(
            &mut machine,
            EgressEvent::SendChunkHeader,
            EgressState::ChunkHeaderSent,
        );
        fail(&mut machine, EgressEvent::SendChunkTerminator);
    }

    #[test]
    fn chunked_transition_sequence() {
        let mut machine = HttpTransactionEgressStateMachine::get_new_instance();
        follow(
            &mut machine,
            EgressEvent::SendHeaders,
            EgressState::HeadersSent,
        );
        for _ in 0..3 {
            follow(
                &mut machine,
                EgressEvent::SendChunkHeader,
                EgressState::ChunkHeaderSent,
            );
            follow(
                &mut machine,
                EgressEvent::SendBody,
                EgressState::ChunkBodySent,
            );
            follow(
                &mut machine,
                EgressEvent::SendChunkTerminator,
                EgressState::ChunkTerminatorSent,
            );
        }
        follow(
            &mut machine,
            EgressEvent::SendTrailers,
            EgressState::TrailersSent,
        );
        follow(&mut machine, EgressEvent::SendEom, EgressState::EomQueued);
        follow(
            &mut machine,
            EgressEvent::EomFlushed,
            EgressState::SendingDone,
        );
    }

    #[test]
    fn normal_transition_sequence() {
        let mut machine = HttpTransactionEgressStateMachine::get_new_instance();
        follow(
            &mut machine,
            EgressEvent::SendHeaders,
            EgressState::HeadersSent,
        );
        for _ in 0..4 {
            follow(
                &mut machine,
                EgressEvent::SendBody,
                EgressState::RegularBodySent,
            );
        }
        follow(&mut machine, EgressEvent::SendEom, EgressState::EomQueued);
    }

    #[test]
    fn normal_transition_sequence_with_trailers() {
        let mut machine = HttpTransactionEgressStateMachine::get_new_instance();
        follow(
            &mut machine,
            EgressEvent::SendHeaders,
            EgressState::HeadersSent,
        );
        for _ in 0..4 {
            follow(
                &mut machine,
                EgressEvent::SendBody,
                EgressState::RegularBodySent,
            );
        }
        follow(
            &mut machine,
            EgressEvent::SendTrailers,
            EgressState::TrailersSent,
        );
        follow(&mut machine, EgressEvent::SendEom, EgressState::EomQueued);
        follow(
            &mut machine,
            EgressEvent::EomFlushed,
            EgressState::SendingDone,
        );
    }

    #[test]
    fn informational_headers_sequence() {
        let mut machine = HttpTransactionEgressStateMachine::get_new_instance();
        follow(
            &mut machine,
            EgressEvent::SendHeaders,
            EgressState::HeadersSent,
        );
        follow(
            &mut machine,
            EgressEvent::SendTrailers,
            EgressState::TrailersSent,
        );
        follow(&mut machine, EgressEvent::SendEom, EgressState::EomQueued);
        follow(
            &mut machine,
            EgressEvent::EomFlushed,
            EgressState::SendingDone,
        );
    }

    #[test]
    fn every_non_datagram_event_matches_the_transition_table() {
        const STATES: [EgressState; 9] = [
            EgressState::Start,
            EgressState::HeadersSent,
            EgressState::RegularBodySent,
            EgressState::ChunkHeaderSent,
            EgressState::ChunkBodySent,
            EgressState::ChunkTerminatorSent,
            EgressState::TrailersSent,
            EgressState::EomQueued,
            EgressState::SendingDone,
        ];
        const EVENTS: [EgressEvent; 7] = [
            EgressEvent::SendHeaders,
            EgressEvent::SendBody,
            EgressEvent::SendChunkHeader,
            EgressEvent::SendChunkTerminator,
            EgressEvent::SendTrailers,
            EgressEvent::SendEom,
            EgressEvent::EomFlushed,
        ];
        const LEGAL: [(EgressState, EgressEvent, EgressState); 17] = [
            (
                EgressState::Start,
                EgressEvent::SendHeaders,
                EgressState::HeadersSent,
            ),
            (
                EgressState::HeadersSent,
                EgressEvent::SendHeaders,
                EgressState::HeadersSent,
            ),
            (
                EgressState::HeadersSent,
                EgressEvent::SendBody,
                EgressState::RegularBodySent,
            ),
            (
                EgressState::HeadersSent,
                EgressEvent::SendTrailers,
                EgressState::TrailersSent,
            ),
            (
                EgressState::HeadersSent,
                EgressEvent::SendChunkHeader,
                EgressState::ChunkHeaderSent,
            ),
            (
                EgressState::HeadersSent,
                EgressEvent::SendEom,
                EgressState::EomQueued,
            ),
            (
                EgressState::RegularBodySent,
                EgressEvent::SendBody,
                EgressState::RegularBodySent,
            ),
            (
                EgressState::RegularBodySent,
                EgressEvent::SendTrailers,
                EgressState::TrailersSent,
            ),
            (
                EgressState::RegularBodySent,
                EgressEvent::SendEom,
                EgressState::EomQueued,
            ),
            (
                EgressState::ChunkHeaderSent,
                EgressEvent::SendBody,
                EgressState::ChunkBodySent,
            ),
            (
                EgressState::ChunkBodySent,
                EgressEvent::SendBody,
                EgressState::ChunkBodySent,
            ),
            (
                EgressState::ChunkBodySent,
                EgressEvent::SendChunkTerminator,
                EgressState::ChunkTerminatorSent,
            ),
            (
                EgressState::ChunkTerminatorSent,
                EgressEvent::SendChunkHeader,
                EgressState::ChunkHeaderSent,
            ),
            (
                EgressState::ChunkTerminatorSent,
                EgressEvent::SendTrailers,
                EgressState::TrailersSent,
            ),
            (
                EgressState::ChunkTerminatorSent,
                EgressEvent::SendEom,
                EgressState::EomQueued,
            ),
            (
                EgressState::TrailersSent,
                EgressEvent::SendEom,
                EgressState::EomQueued,
            ),
            (
                EgressState::EomQueued,
                EgressEvent::EomFlushed,
                EgressState::SendingDone,
            ),
        ];

        assert_eq!(
            HttpTransactionEgressStateMachineData::NAME,
            "HTTPTransactionEgress"
        );
        assert_eq!(
            HttpTransactionEgressStateMachine::get_new_instance(),
            HttpTransactionEgressStateMachineData::initial_state()
        );
        for state in STATES {
            for event in EVENTS {
                let expected = LEGAL.iter().find_map(|(from, candidate, to)| {
                    (*from == state && *candidate == event).then_some(*to)
                });
                assert_eq!(
                    HttpTransactionEgressStateMachineData::find(state, event),
                    expected.map_or((state, false), |next| (next, true)),
                    "mismatch for {state:?} on {event:?}"
                );
                let mut actual = state;
                assert_eq!(
                    HttpTransactionEgressStateMachine::can_transit(actual, event),
                    expected.is_some()
                );
                match expected {
                    Some(next) => {
                        assert!(HttpTransactionEgressStateMachine::transit(
                            &mut actual,
                            event
                        ));
                        assert_eq!(actual, next);
                    }
                    None => {
                        assert!(!HttpTransactionEgressStateMachine::transit(
                            &mut actual,
                            event
                        ));
                        assert_eq!(actual, state);
                    }
                }
            }
        }
    }

    #[test]
    fn discriminants_reserve_the_excluded_datagram_slots() {
        assert_eq!(EgressState::HeadersSent as u8, 1);
        assert_eq!(EgressState::RegularBodySent as u8, 3);
        assert_eq!(EgressEvent::SendHeaders as u8, 0);
        assert_eq!(EgressEvent::SendBody as u8, 2);
    }
}
