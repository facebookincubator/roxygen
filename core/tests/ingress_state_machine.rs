/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

mod ingress_state {
    use crate::ingress_state::HttpTransactionIngressStateMachine;
    use crate::ingress_state::HttpTransactionIngressStateMachineData;
    use crate::ingress_state::IngressEvent;
    use crate::ingress_state::IngressState;

    fn follow(state: &mut IngressState, event: IngressEvent, expected: IngressState) {
        assert!(HttpTransactionIngressStateMachine::transit(state, event));
        assert_eq!(*state, expected);
    }

    fn fail(state: &mut IngressState, event: IngressEvent) {
        let original = *state;
        assert!(!HttpTransactionIngressStateMachine::can_transit(
            *state, event
        ));
        assert!(!HttpTransactionIngressStateMachine::transit(state, event));
        assert_eq!(*state, original);
    }

    #[test]
    fn reject_eom_inside_open_chunk() {
        let mut machine = HttpTransactionIngressStateMachine::get_new_instance();
        follow(
            &mut machine,
            IngressEvent::FinalHeaders,
            IngressState::FinalHeadersReceived,
        );
        follow(
            &mut machine,
            IngressEvent::ChunkHeader,
            IngressState::ChunkHeaderReceived,
        );
        follow(
            &mut machine,
            IngressEvent::Body,
            IngressState::ChunkBodyReceived,
        );
        fail(&mut machine, IngressEvent::Eom);
    }

    #[test]
    fn reject_body_before_headers() {
        let mut machine = HttpTransactionIngressStateMachine::get_new_instance();
        fail(&mut machine, IngressEvent::Body);
    }

    #[test]
    fn reject_second_eom() {
        let mut machine = HttpTransactionIngressStateMachine::get_new_instance();
        follow(
            &mut machine,
            IngressEvent::FinalHeaders,
            IngressState::FinalHeadersReceived,
        );
        follow(
            &mut machine,
            IngressEvent::Body,
            IngressState::RegularBodyReceived,
        );
        follow(
            &mut machine,
            IngressEvent::Body,
            IngressState::RegularBodyReceived,
        );
        follow(&mut machine, IngressEvent::Eom, IngressState::EomQueued);
        fail(&mut machine, IngressEvent::Eom);
    }

    #[test]
    fn reject_chunk_complete_without_body() {
        let mut machine = HttpTransactionIngressStateMachine::get_new_instance();
        follow(
            &mut machine,
            IngressEvent::FinalHeaders,
            IngressState::FinalHeadersReceived,
        );
        follow(
            &mut machine,
            IngressEvent::ChunkHeader,
            IngressState::ChunkHeaderReceived,
        );
        follow(
            &mut machine,
            IngressEvent::Body,
            IngressState::ChunkBodyReceived,
        );
        follow(
            &mut machine,
            IngressEvent::ChunkComplete,
            IngressState::ChunkCompleted,
        );
        follow(
            &mut machine,
            IngressEvent::ChunkHeader,
            IngressState::ChunkHeaderReceived,
        );
        fail(&mut machine, IngressEvent::ChunkComplete);
    }

    #[test]
    fn chunked_transition_sequence() {
        let mut machine = HttpTransactionIngressStateMachine::get_new_instance();
        follow(
            &mut machine,
            IngressEvent::FinalHeaders,
            IngressState::FinalHeadersReceived,
        );
        for _ in 0..3 {
            follow(
                &mut machine,
                IngressEvent::ChunkHeader,
                IngressState::ChunkHeaderReceived,
            );
            follow(
                &mut machine,
                IngressEvent::Body,
                IngressState::ChunkBodyReceived,
            );
            follow(
                &mut machine,
                IngressEvent::ChunkComplete,
                IngressState::ChunkCompleted,
            );
        }
        follow(
            &mut machine,
            IngressEvent::Trailers,
            IngressState::TrailersReceived,
        );
        follow(&mut machine, IngressEvent::Eom, IngressState::EomQueued);
    }

    #[test]
    fn normal_transition_sequence() {
        let mut machine = HttpTransactionIngressStateMachine::get_new_instance();
        follow(
            &mut machine,
            IngressEvent::FinalHeaders,
            IngressState::FinalHeadersReceived,
        );
        for _ in 0..4 {
            follow(
                &mut machine,
                IngressEvent::Body,
                IngressState::RegularBodyReceived,
            );
        }
        follow(&mut machine, IngressEvent::Eom, IngressState::EomQueued);
    }

    #[test]
    fn informational_headers_then_trailers() {
        let mut machine = HttpTransactionIngressStateMachine::get_new_instance();
        for _ in 0..3 {
            follow(
                &mut machine,
                IngressEvent::NonFinalHeaders,
                IngressState::NonFinalHeadersReceived,
            );
        }
        follow(
            &mut machine,
            IngressEvent::FinalHeaders,
            IngressState::FinalHeadersReceived,
        );
        follow(
            &mut machine,
            IngressEvent::Trailers,
            IngressState::TrailersReceived,
        );
        follow(&mut machine, IngressEvent::Eom, IngressState::EomQueued);
    }

    #[test]
    fn every_non_datagram_event_matches_the_transition_table() {
        const STATES: [IngressState; 11] = [
            IngressState::Start,
            IngressState::NonFinalHeadersReceived,
            IngressState::FinalHeadersReceived,
            IngressState::RegularBodyReceived,
            IngressState::ChunkHeaderReceived,
            IngressState::ChunkBodyReceived,
            IngressState::ChunkCompleted,
            IngressState::TrailersReceived,
            IngressState::UpgradeComplete,
            IngressState::EomQueued,
            IngressState::ReceivingDone,
        ];
        const EVENTS: [IngressEvent; 9] = [
            IngressEvent::NonFinalHeaders,
            IngressEvent::FinalHeaders,
            IngressEvent::Body,
            IngressEvent::ChunkHeader,
            IngressEvent::ChunkComplete,
            IngressEvent::Trailers,
            IngressEvent::Upgrade,
            IngressEvent::Eom,
            IngressEvent::EomFlushed,
        ];
        const LEGAL: [(IngressState, IngressEvent, IngressState); 23] = [
            (
                IngressState::Start,
                IngressEvent::FinalHeaders,
                IngressState::FinalHeadersReceived,
            ),
            (
                IngressState::Start,
                IngressEvent::NonFinalHeaders,
                IngressState::NonFinalHeadersReceived,
            ),
            (
                IngressState::NonFinalHeadersReceived,
                IngressEvent::NonFinalHeaders,
                IngressState::NonFinalHeadersReceived,
            ),
            (
                IngressState::NonFinalHeadersReceived,
                IngressEvent::FinalHeaders,
                IngressState::FinalHeadersReceived,
            ),
            (
                IngressState::NonFinalHeadersReceived,
                IngressEvent::Upgrade,
                IngressState::UpgradeComplete,
            ),
            (
                IngressState::FinalHeadersReceived,
                IngressEvent::Body,
                IngressState::RegularBodyReceived,
            ),
            (
                IngressState::FinalHeadersReceived,
                IngressEvent::ChunkHeader,
                IngressState::ChunkHeaderReceived,
            ),
            (
                IngressState::FinalHeadersReceived,
                IngressEvent::Trailers,
                IngressState::TrailersReceived,
            ),
            (
                IngressState::FinalHeadersReceived,
                IngressEvent::Upgrade,
                IngressState::UpgradeComplete,
            ),
            (
                IngressState::FinalHeadersReceived,
                IngressEvent::Eom,
                IngressState::EomQueued,
            ),
            (
                IngressState::RegularBodyReceived,
                IngressEvent::Body,
                IngressState::RegularBodyReceived,
            ),
            (
                IngressState::RegularBodyReceived,
                IngressEvent::Trailers,
                IngressState::TrailersReceived,
            ),
            (
                IngressState::RegularBodyReceived,
                IngressEvent::Eom,
                IngressState::EomQueued,
            ),
            (
                IngressState::ChunkHeaderReceived,
                IngressEvent::Body,
                IngressState::ChunkBodyReceived,
            ),
            (
                IngressState::ChunkBodyReceived,
                IngressEvent::Body,
                IngressState::ChunkBodyReceived,
            ),
            (
                IngressState::ChunkBodyReceived,
                IngressEvent::ChunkComplete,
                IngressState::ChunkCompleted,
            ),
            (
                IngressState::ChunkCompleted,
                IngressEvent::ChunkHeader,
                IngressState::ChunkHeaderReceived,
            ),
            (
                IngressState::ChunkCompleted,
                IngressEvent::Trailers,
                IngressState::TrailersReceived,
            ),
            (
                IngressState::ChunkCompleted,
                IngressEvent::Eom,
                IngressState::EomQueued,
            ),
            (
                IngressState::TrailersReceived,
                IngressEvent::Eom,
                IngressState::EomQueued,
            ),
            (
                IngressState::UpgradeComplete,
                IngressEvent::Body,
                IngressState::UpgradeComplete,
            ),
            (
                IngressState::UpgradeComplete,
                IngressEvent::Eom,
                IngressState::EomQueued,
            ),
            (
                IngressState::EomQueued,
                IngressEvent::EomFlushed,
                IngressState::ReceivingDone,
            ),
        ];

        assert_eq!(
            HttpTransactionIngressStateMachineData::NAME,
            "HTTPTransactionIngress"
        );
        assert_eq!(
            HttpTransactionIngressStateMachine::get_new_instance(),
            HttpTransactionIngressStateMachineData::initial_state()
        );
        for state in STATES {
            for event in EVENTS {
                let expected = LEGAL.iter().find_map(|(from, candidate, to)| {
                    (*from == state && *candidate == event).then_some(*to)
                });
                assert_eq!(
                    HttpTransactionIngressStateMachineData::find(state, event),
                    expected.map_or((state, false), |next| (next, true)),
                    "mismatch for {state:?} on {event:?}"
                );
                let mut actual = state;
                assert_eq!(
                    HttpTransactionIngressStateMachine::can_transit(actual, event),
                    expected.is_some()
                );
                match expected {
                    Some(next) => {
                        assert!(HttpTransactionIngressStateMachine::transit(
                            &mut actual,
                            event
                        ));
                        assert_eq!(actual, next);
                    }
                    None => {
                        assert!(!HttpTransactionIngressStateMachine::transit(
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
    fn terminal_states_reject_external_events_and_eom_flushes_once() {
        let mut machine = HttpTransactionIngressStateMachine::get_new_instance();
        follow(
            &mut machine,
            IngressEvent::FinalHeaders,
            IngressState::FinalHeadersReceived,
        );
        follow(&mut machine, IngressEvent::Eom, IngressState::EomQueued);
        fail(&mut machine, IngressEvent::Body);
        follow(
            &mut machine,
            IngressEvent::EomFlushed,
            IngressState::ReceivingDone,
        );
        fail(&mut machine, IngressEvent::EomFlushed);
        fail(&mut machine, IngressEvent::Eom);
    }

    #[test]
    fn discriminants_preserve_the_excluded_datagram_gap() {
        assert_eq!(IngressEvent::NonFinalHeaders as u8, 0);
        assert_eq!(IngressEvent::FinalHeaders as u8, 1);
        assert_eq!(IngressEvent::Body as u8, 3);
        assert_eq!(IngressEvent::EomFlushed as u8, 9);
    }
}
