/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

mod byte_event {
    use std::any::Any;
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::panic::panic_any;
    use std::rc::Rc;
    use std::rc::Weak;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::thread::ThreadId;

    use crate::byte_event::ByteEventType;
    use crate::byte_event::FieldSectionInfo;
    use crate::byte_event::FieldSectionType;
    use crate::byte_event::HttpByteEvent;
    use crate::byte_event::HttpByteEventCallback;
    use crate::byte_event::HttpByteEventRegistration;
    use crate::error::HttpError;
    use crate::error::HttpErrorCode;
    use crate::http::HttpHeaderSize;
    use crate::ids::StreamId;

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum CallbackAction {
        Fired(HttpByteEvent),
        Cancelled {
            event: HttpByteEvent,
            code: HttpErrorCode,
            message: Box<[u8]>,
        },
    }

    #[derive(Default)]
    struct RecordingCallback {
        actions: RefCell<Vec<CallbackAction>>,
    }

    impl RecordingCallback {
        fn actions(&self) -> Vec<CallbackAction> {
            self.actions.borrow().clone()
        }
    }

    impl HttpByteEventCallback for RecordingCallback {
        fn on_byte_event(&self, event: HttpByteEvent) {
            self.actions.borrow_mut().push(CallbackAction::Fired(event));
        }

        fn on_byte_event_cancelled(&self, event: HttpByteEvent, error: HttpError) {
            self.actions.borrow_mut().push(CallbackAction::Cancelled {
                event,
                code: error.code(),
                message: error.message().into(),
            });
        }
    }

    struct PanickingCancellationCallback {
        attempted: RefCell<Vec<ByteEventType>>,
        panic_on_next: Cell<bool>,
    }

    impl PanickingCancellationCallback {
        fn new() -> Self {
            Self {
                attempted: RefCell::new(Vec::new()),
                panic_on_next: Cell::new(true),
            }
        }

        fn attempted(&self) -> Vec<ByteEventType> {
            self.attempted.borrow().clone()
        }
    }

    impl HttpByteEventCallback for PanickingCancellationCallback {
        fn on_byte_event(&self, _event: HttpByteEvent) {
            panic!("panicking cancellation callback received a fired event");
        }

        fn on_byte_event_cancelled(&self, event: HttpByteEvent, _error: HttpError) {
            self.attempted.borrow_mut().push(event.event_type);
            if self.panic_on_next.replace(false) {
                panic!("first cancellation callback panic");
            }
        }
    }

    const ALL_EVENTS: u8 = ByteEventType::TransportWrite.bit()
        | ByteEventType::KernelWrite.bit()
        | ByteEventType::NicTx.bit()
        | ByteEventType::CumulativeAck.bit();

    fn populated_event(event_type: ByteEventType, body_offset: u64) -> HttpByteEvent {
        HttpByteEvent {
            stream_id: Some(StreamId::new(91)),
            field_section_info: Some(FieldSectionInfo {
                section_type: FieldSectionType::Trailers,
                final_headers: true,
                size: HttpHeaderSize {
                    compressed: 13,
                    uncompressed: 29,
                    compressed_block: 11,
                },
            }),
            body_offset,
            transport_offset: 307,
            stream_offset: 211,
            eom: true,
            event_type,
        }
    }

    #[test]
    fn registration_drop_cancels_each_requested_kind_once() {
        assert_eq!(ByteEventType::TransportWrite.bit(), 0x01);
        assert_eq!(ByteEventType::KernelWrite.bit(), 0x02);
        assert_eq!(ByteEventType::NicTx.bit(), 0x04);
        assert_eq!(ByteEventType::CumulativeAck.bit(), 0x08);

        let callback = Rc::new(RecordingCallback::default());
        {
            let registration =
                HttpByteEventRegistration::new(Some(StreamId::new(17)), ALL_EVENTS, &callback);
            assert_eq!(registration.stream_id(), Some(StreamId::new(17)));
            assert_eq!(registration.pending_events(), ALL_EVENTS);
            assert!(registration.callback_is_alive());
        }

        let actions = callback.actions();
        assert_eq!(actions.len(), 4);
        for (action, event_type) in actions.iter().zip(ByteEventType::ALL) {
            let CallbackAction::Cancelled {
                event,
                code,
                message,
            } = action
            else {
                panic!("registration drop must cancel every requested kind");
            };
            assert_eq!(event.stream_id, Some(StreamId::new(17)));
            assert_eq!(event.field_section_info, None);
            assert_eq!(event.body_offset, 0);
            assert_eq!(event.transport_offset, 0);
            assert_eq!(event.stream_offset, 0);
            assert!(!event.eom);
            assert_eq!(event.event_type, event_type);
            assert_eq!(*code, HttpErrorCode::CANCEL);
            assert_eq!(message.as_ref(), b"ByteEvent registration cancelled");
        }

        let explicit = Rc::new(RecordingCallback::default());
        let mut registration =
            HttpByteEventRegistration::new(Some(StreamId::new(17)), ALL_EVENTS, &explicit);
        let fired = populated_event(ByteEventType::KernelWrite, 53);
        assert!(registration.fire(fired.clone()));
        assert!(!registration.fire(fired));
        let error = HttpError::with_message(HttpErrorCode::TRANSPORT_WRITE_ERROR, b"write failed");
        let cancelled = populated_event(ByteEventType::TransportWrite, 53);
        registration.cancel(&error, Some(&cancelled));
        registration.cancel(&error, Some(&cancelled));
        drop(registration);

        let actions = explicit.actions();
        assert_eq!(actions.len(), 4);
        assert_eq!(
            actions[0],
            CallbackAction::Fired(populated_event(ByteEventType::KernelWrite, 53))
        );
        let cancelled_types: Vec<_> = actions[1..]
            .iter()
            .map(|action| match action {
                CallbackAction::Cancelled {
                    event,
                    code,
                    message,
                } => {
                    assert_eq!(*code, HttpErrorCode::TRANSPORT_WRITE_ERROR);
                    assert_eq!(message.as_ref(), b"write failed");
                    assert_eq!(event.body_offset, 53);
                    assert_eq!(event.stream_id, Some(StreamId::new(91)));
                    assert_eq!(event.transport_offset, 307);
                    assert_eq!(event.stream_offset, 211);
                    assert!(event.eom);
                    assert_eq!(
                        event.field_section_info,
                        populated_event(ByteEventType::TransportWrite, 53).field_section_info,
                    );
                    event.event_type
                }
                CallbackAction::Fired(_) => panic!("only the fired kind may succeed"),
            })
            .collect();
        assert_eq!(
            cancelled_types,
            vec![
                ByteEventType::TransportWrite,
                ByteEventType::NicTx,
                ByteEventType::CumulativeAck,
            ],
        );

        let untouched = Rc::new(RecordingCallback::default());
        drop(HttpByteEventRegistration::new(None, 0x80, &untouched));
        assert!(untouched.actions().is_empty());
        let dead = Rc::new(RecordingCallback::default());
        let registration = HttpByteEventRegistration::new(None, ALL_EVENTS, &dead);
        drop(dead);
        assert!(!registration.callback_is_alive());
        drop(registration);
    }

    #[test]
    fn split_registrations_resolve_each_kind_independently() {
        let callback = Rc::new(RecordingCallback::default());
        let registration =
            HttpByteEventRegistration::new(Some(StreamId::new(43)), ALL_EVENTS, &callback);
        let mut registrations = registration.into_event_registrations();
        assert_eq!(registrations.len(), ByteEventType::ALL.len());
        for (registration, event_type) in registrations.iter().zip(ByteEventType::ALL) {
            assert_eq!(registration.stream_id(), Some(StreamId::new(43)));
            assert_eq!(registration.pending_events(), event_type.bit());
            assert!(registration.callback_is_alive());
        }

        let mut transport_write = registrations.remove(0);
        let kernel_write = registrations.remove(0);
        let mut nic_tx = registrations.remove(0);
        let cumulative_ack = registrations.remove(0);

        let transport_event = populated_event(ByteEventType::TransportWrite, 7);
        assert!(transport_write.fire(transport_event.clone()));
        nic_tx.cancel(
            &HttpError::new(HttpErrorCode::READ_TIMEOUT),
            Some(&populated_event(ByteEventType::NicTx, 11)),
        );
        drop(kernel_write);
        drop(cumulative_ack);

        assert_eq!(
            callback.actions(),
            vec![
                CallbackAction::Fired(transport_event),
                CallbackAction::Cancelled {
                    event: populated_event(ByteEventType::NicTx, 11),
                    code: HttpErrorCode::READ_TIMEOUT,
                    message: b"READ_TIMEOUT".to_vec().into_boxed_slice(),
                },
                CallbackAction::Cancelled {
                    event: HttpByteEvent {
                        stream_id: Some(StreamId::new(43)),
                        event_type: ByteEventType::KernelWrite,
                        ..HttpByteEvent::new(ByteEventType::KernelWrite)
                    },
                    code: HttpErrorCode::CANCEL,
                    message: b"ByteEvent registration cancelled"
                        .to_vec()
                        .into_boxed_slice(),
                },
                CallbackAction::Cancelled {
                    event: HttpByteEvent {
                        stream_id: Some(StreamId::new(43)),
                        event_type: ByteEventType::CumulativeAck,
                        ..HttpByteEvent::new(ByteEventType::CumulativeAck)
                    },
                    code: HttpErrorCode::CANCEL,
                    message: b"ByteEvent registration cancelled"
                        .to_vec()
                        .into_boxed_slice(),
                },
            ],
        );
    }

    #[test]
    fn explicit_cancel_attempts_all_kinds_before_resuming_first_panic() {
        let callback = Rc::new(PanickingCancellationCallback::new());
        let mut registration =
            HttpByteEventRegistration::new(Some(StreamId::new(29)), ALL_EVENTS, &callback);
        let error = HttpError::new(HttpErrorCode::CANCEL);

        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            registration.cancel(&error, None);
        }));
        assert!(result.is_err());
        assert_eq!(callback.attempted(), ByteEventType::ALL);
        assert_eq!(registration.pending_events(), 0);
        assert!(!registration.callback_is_alive());

        registration.cancel(&error, None);
        drop(registration);
        assert_eq!(callback.attempted(), ByteEventType::ALL);
    }

    #[test]
    fn registration_drop_attempts_all_kinds_before_resuming_first_panic() {
        let callback = Rc::new(PanickingCancellationCallback::new());
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            drop(HttpByteEventRegistration::new(
                Some(StreamId::new(31)),
                ALL_EVENTS,
                &callback,
            ));
        }));
        assert!(result.is_err());
        assert_eq!(callback.attempted(), ByteEventType::ALL);
    }

    #[test]
    fn registration_drop_suppresses_callback_panic_during_unwind() {
        struct OuterDrop {
            registration: Option<HttpByteEventRegistration>,
        }

        impl Drop for OuterDrop {
            fn drop(&mut self) {
                let _registration = self
                    .registration
                    .take()
                    .expect("outer drop owns its registration");
                panic!("outer drop panic");
            }
        }

        let callback = Rc::new(PanickingCancellationCallback::new());
        let outer = OuterDrop {
            registration: Some(HttpByteEventRegistration::new(
                Some(StreamId::new(37)),
                ALL_EVENTS,
                &callback,
            )),
        };
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| drop(outer)));
        let payload = result.expect_err("outer drop panic is preserved");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"outer drop panic"));
        assert_eq!(callback.attempted(), ByteEventType::ALL);
    }

    #[derive(Clone, Default)]
    struct PayloadRetirementProbe {
        drops: Arc<AtomicUsize>,
        secondary_drops: Arc<AtomicUsize>,
    }

    struct CallbackRetirementPayload {
        label: &'static str,
        probe: PayloadRetirementProbe,
    }

    impl Drop for CallbackRetirementPayload {
        fn drop(&mut self) {
            self.probe.drops.fetch_add(1, Ordering::SeqCst);
            panic_any(SecondaryRetirementPayload(Arc::clone(
                &self.probe.secondary_drops,
            )));
        }
    }

    struct SecondaryRetirementPayload(Arc<AtomicUsize>);

    impl Drop for SecondaryRetirementPayload {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("secondary retirement payload destructor");
        }
    }

    #[derive(Default)]
    enum RetirementFailure {
        #[default]
        None,
        Message(&'static str),
        Payload(&'static str, PayloadRetirementProbe),
    }

    impl RetirementFailure {
        fn trigger(&self) {
            match self {
                Self::None => {}
                Self::Message(message) => panic_any(*message),
                Self::Payload(label, probe) => panic_any(CallbackRetirementPayload {
                    label,
                    probe: probe.clone(),
                }),
            }
        }
    }

    #[derive(Default)]
    struct CallbackFailures {
        first: RetirementFailure,
        later: RetirementFailure,
        retirement: RetirementFailure,
    }

    struct RetiringCallback {
        owner: ThreadId,
        external: Weak<RefCell<Option<Rc<RetiringCallback>>>>,
        actions: Rc<RefCell<Vec<CallbackAction>>>,
        drops: Rc<Cell<usize>>,
        release_on: ByteEventType,
        failures: CallbackFailures,
    }

    impl RetiringCallback {
        fn invoke(&self, action: CallbackAction) {
            assert_eq!(std::thread::current().id(), self.owner);
            let event_type = match &action {
                CallbackAction::Fired(event) | CallbackAction::Cancelled { event, .. } => {
                    event.event_type
                }
            };
            let first = self.actions.borrow().is_empty();
            self.actions.borrow_mut().push(action);
            if event_type == self.release_on {
                let external = self
                    .external
                    .upgrade()
                    .expect("the external owner slot remains live");
                let last_external = external.borrow_mut().take();
                assert!(last_external.is_some());
                drop(last_external);
                assert_eq!(
                    self.drops.get(),
                    0,
                    "the framework still owns the upgraded Rc"
                );
            }
            if first {
                self.failures.first.trigger();
            } else {
                self.failures.later.trigger();
            }
        }
    }

    impl HttpByteEventCallback for RetiringCallback {
        fn on_byte_event(&self, event: HttpByteEvent) {
            self.invoke(CallbackAction::Fired(event));
        }

        fn on_byte_event_cancelled(&self, event: HttpByteEvent, error: HttpError) {
            self.invoke(CallbackAction::Cancelled {
                event,
                code: error.code(),
                message: error.message().into(),
            });
        }
    }

    impl Drop for RetiringCallback {
        fn drop(&mut self) {
            assert_eq!(std::thread::current().id(), self.owner);
            assert_eq!(self.drops.replace(self.drops.get() + 1), 0);
            self.failures.retirement.trigger();
        }
    }

    struct RetirementFixture {
        registration: HttpByteEventRegistration,
        external: Rc<RefCell<Option<Rc<RetiringCallback>>>>,
        actions: Rc<RefCell<Vec<CallbackAction>>>,
        drops: Rc<Cell<usize>>,
    }

    impl RetirementFixture {
        fn new(events: u8, release_on: ByteEventType, failures: CallbackFailures) -> Self {
            let external = Rc::new(RefCell::new(None));
            let actions = Rc::new(RefCell::new(Vec::new()));
            let drops = Rc::new(Cell::new(0));
            let callback = Rc::new(RetiringCallback {
                owner: std::thread::current().id(),
                external: Rc::downgrade(&external),
                actions: Rc::clone(&actions),
                drops: Rc::clone(&drops),
                release_on,
                failures,
            });
            let registration =
                HttpByteEventRegistration::new(Some(StreamId::new(47)), events, &callback);
            *external.borrow_mut() = Some(callback);
            Self {
                registration,
                external,
                actions,
                drops,
            }
        }
    }

    fn assert_first_retirement_failure(payload: Box<dyn Any + Send>, expected: &'static str) {
        if let Some(message) = payload.downcast_ref::<&str>() {
            assert_eq!(*message, expected);
        } else {
            assert_eq!(
                payload
                    .downcast_ref::<CallbackRetirementPayload>()
                    .unwrap()
                    .label,
                expected
            );
        }
        if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
            roxygen_runtime::panic::discard_panic(secondary);
        }
    }

    #[test]
    fn cancellation_preserves_callback_panic_across_last_owner_drop() {
        for throwing_payload in [false, true] {
            let primary = PayloadRetirementProbe::default();
            let retirement = PayloadRetirementProbe::default();
            let mut fixture = RetirementFixture::new(
                ALL_EVENTS,
                ByteEventType::TransportWrite,
                CallbackFailures {
                    first: if throwing_payload {
                        RetirementFailure::Payload("cancel invocation first", primary.clone())
                    } else {
                        RetirementFailure::Message("cancel invocation first")
                    },
                    retirement: RetirementFailure::Payload(
                        "cancel Rc retirement second",
                        retirement.clone(),
                    ),
                    ..CallbackFailures::default()
                },
            );
            let error = HttpError::new(HttpErrorCode::CANCEL);
            let payload = catch_unwind(AssertUnwindSafe(|| {
                fixture.registration.cancel(&error, None)
            }))
            .expect_err("the callback invocation remains the first failure");
            assert!(fixture.external.borrow().is_none());
            assert_eq!(fixture.drops.get(), 1);
            assert_eq!(fixture.registration.pending_events(), 0);
            assert!(!fixture.registration.callback_is_alive());
            assert_eq!(
                fixture.actions.borrow().len(),
                1,
                "later kinds recheck the dead weak callback"
            );
            assert_eq!(primary.drops.load(Ordering::SeqCst), 0);
            assert_eq!(retirement.drops.load(Ordering::SeqCst), 1);
            assert_eq!(
                retirement.secondary_drops.load(Ordering::SeqCst),
                retirement.drops.load(Ordering::SeqCst)
            );
            assert_first_retirement_failure(payload, "cancel invocation first");
            assert_eq!(
                primary.drops.load(Ordering::SeqCst),
                usize::from(throwing_payload)
            );
            assert_eq!(
                primary.secondary_drops.load(Ordering::SeqCst),
                primary.drops.load(Ordering::SeqCst)
            );
            fixture.registration.cancel(&error, None);
            drop(fixture.registration);
            assert_eq!(fixture.actions.borrow().len(), 1);
        }
    }

    #[test]
    fn cancellation_disposes_secondary_panic_payloads_without_replacing_first() {
        let later = PayloadRetirementProbe::default();
        let retirement = PayloadRetirementProbe::default();
        let mut fixture = RetirementFixture::new(
            ALL_EVENTS,
            ByteEventType::CumulativeAck,
            CallbackFailures {
                first: RetirementFailure::Message("first ordered cancellation"),
                later: RetirementFailure::Payload("later cancellation", later.clone()),
                retirement: RetirementFailure::Payload("final Rc retirement", retirement.clone()),
            },
        );
        let payload = catch_unwind(AssertUnwindSafe(|| {
            fixture
                .registration
                .cancel(&HttpError::new(HttpErrorCode::CANCEL), None);
        }))
        .expect_err("all live kinds are attempted before the first panic resumes");
        let kinds: Vec<_> = fixture
            .actions
            .borrow()
            .iter()
            .map(|action| match action {
                CallbackAction::Cancelled { event, .. } => event.event_type,
                CallbackAction::Fired(_) => panic!("cancellation must not fire an event"),
            })
            .collect();
        assert_eq!(kinds, ByteEventType::ALL);
        assert!(fixture.external.borrow().is_none());
        assert_eq!(fixture.drops.get(), 1);
        assert_eq!(fixture.registration.pending_events(), 0);
        assert!(!fixture.registration.callback_is_alive());
        assert_eq!(later.drops.load(Ordering::SeqCst), 3);
        assert_eq!(
            later.secondary_drops.load(Ordering::SeqCst),
            later.drops.load(Ordering::SeqCst)
        );
        assert_eq!(retirement.drops.load(Ordering::SeqCst), 1);
        assert_eq!(
            retirement.secondary_drops.load(Ordering::SeqCst),
            retirement.drops.load(Ordering::SeqCst)
        );
        assert_first_retirement_failure(payload, "first ordered cancellation");
        drop(fixture.registration);
        assert_eq!(fixture.actions.borrow().len(), 4);
    }

    #[test]
    fn registration_drop_preserves_outer_panic_when_callback_retirement_panics() {
        struct DuringUnwind {
            registration: Option<HttpByteEventRegistration>,
            fire: bool,
            retired: Rc<Cell<bool>>,
        }

        impl Drop for DuringUnwind {
            fn drop(&mut self) {
                assert!(std::thread::panicking());
                let mut registration = self.registration.take().unwrap();
                if self.fire {
                    assert!(registration.fire(populated_event(ByteEventType::TransportWrite, 5)));
                }
                drop(registration);
                self.retired.set(true);
            }
        }

        for fire in [false, true] {
            let invocation = PayloadRetirementProbe::default();
            let retirement = PayloadRetirementProbe::default();
            let fixture = RetirementFixture::new(
                ALL_EVENTS,
                ByteEventType::TransportWrite,
                CallbackFailures {
                    first: RetirementFailure::Payload("nested invocation", invocation.clone()),
                    retirement: RetirementFailure::Payload(
                        "nested Rc retirement",
                        retirement.clone(),
                    ),
                    ..CallbackFailures::default()
                },
            );
            let retired = Rc::new(Cell::new(false));
            let registration = fixture.registration;
            let payload = catch_unwind(AssertUnwindSafe(|| {
                let _cleanup = DuringUnwind {
                    registration: Some(registration),
                    fire,
                    retired: Rc::clone(&retired),
                };
                panic!("original outer unwind");
            }))
            .expect_err("only the original outer panic escapes");
            assert_eq!(
                payload.downcast_ref::<&str>(),
                Some(&"original outer unwind")
            );
            assert!(retired.get());
            assert!(fixture.external.borrow().is_none());
            assert_eq!(fixture.actions.borrow().len(), 1);
            assert_eq!(fixture.drops.get(), 1);
            assert_eq!(invocation.drops.load(Ordering::SeqCst), 1);
            assert_eq!(retirement.drops.load(Ordering::SeqCst), 1);
            assert_eq!(
                invocation.secondary_drops.load(Ordering::SeqCst),
                invocation.drops.load(Ordering::SeqCst)
            );
            assert_eq!(
                retirement.secondary_drops.load(Ordering::SeqCst),
                retirement.drops.load(Ordering::SeqCst)
            );
        }
    }

    #[test]
    fn firing_preserves_callback_panic_across_last_owner_drop() {
        for events in [ByteEventType::TransportWrite.bit(), ALL_EVENTS] {
            for invocation_panics in [false, true] {
                let invocation = PayloadRetirementProbe::default();
                let retirement = PayloadRetirementProbe::default();
                let mut fixture = RetirementFixture::new(
                    events,
                    ByteEventType::TransportWrite,
                    CallbackFailures {
                        first: if invocation_panics {
                            RetirementFailure::Payload("fire invocation first", invocation.clone())
                        } else {
                            RetirementFailure::None
                        },
                        retirement: RetirementFailure::Payload(
                            "fire Rc retirement",
                            retirement.clone(),
                        ),
                        ..CallbackFailures::default()
                    },
                );
                let event = populated_event(ByteEventType::TransportWrite, 23);
                let payload = catch_unwind(AssertUnwindSafe(|| {
                    fixture.registration.fire(event.clone())
                }))
                .expect_err("firing retains the invocation or retirement failure");
                assert_eq!(
                    fixture.registration.pending_events(),
                    events & !event.event_type.bit()
                );
                assert!(!fixture.registration.callback_is_alive());
                assert!(fixture.external.borrow().is_none());
                assert_eq!(fixture.drops.get(), 1);
                assert_eq!(
                    *fixture.actions.borrow(),
                    vec![CallbackAction::Fired(event.clone())]
                );
                assert_eq!(invocation.drops.load(Ordering::SeqCst), 0);
                assert_eq!(
                    retirement.drops.load(Ordering::SeqCst),
                    usize::from(invocation_panics)
                );
                assert_first_retirement_failure(
                    payload,
                    if invocation_panics {
                        "fire invocation first"
                    } else {
                        "fire Rc retirement"
                    },
                );
                assert_eq!(
                    invocation.drops.load(Ordering::SeqCst),
                    usize::from(invocation_panics)
                );
                assert_eq!(retirement.drops.load(Ordering::SeqCst), 1);
                assert_eq!(
                    invocation.secondary_drops.load(Ordering::SeqCst),
                    invocation.drops.load(Ordering::SeqCst)
                );
                assert_eq!(
                    retirement.secondary_drops.load(Ordering::SeqCst),
                    retirement.drops.load(Ordering::SeqCst)
                );
                assert!(!fixture.registration.fire(event));
                fixture
                    .registration
                    .cancel(&HttpError::new(HttpErrorCode::CANCEL), None);
                assert_eq!(fixture.registration.pending_events(), 0);
                assert_eq!(fixture.actions.borrow().len(), 1);
            }
        }
    }

    struct BodyPiece {
        bytes: Vec<u8>,
        registrations: Vec<HttpByteEventRegistration>,
    }

    fn split_body(mut body: BodyPiece, prefix_len: usize) -> (BodyPiece, BodyPiece) {
        let suffix = body.bytes.split_off(prefix_len);
        let registrations = std::mem::take(&mut body.registrations);
        (
            body,
            BodyPiece {
                bytes: suffix,
                registrations,
            },
        )
    }

    #[test]
    fn registration_survives_body_split() {
        let callback = Rc::new(RecordingCallback::default());
        let registration = HttpByteEventRegistration::new(
            Some(StreamId::new(3)),
            ByteEventType::TransportWrite.bit(),
            &callback,
        );
        let body = BodyPiece {
            bytes: b"abcdef".to_vec(),
            registrations: vec![registration],
        };

        let (prefix, remainder) = split_body(body, 2);
        assert_eq!(prefix.bytes, b"ab");
        assert!(prefix.registrations.is_empty());
        assert!(callback.actions().is_empty());
        let (middle, mut suffix) = split_body(remainder, 3);
        assert_eq!(middle.bytes, b"cde");
        assert!(middle.registrations.is_empty());
        assert_eq!(suffix.bytes, b"f");
        assert_eq!(suffix.registrations.len(), 1);
        assert!(callback.actions().is_empty());

        assert!(suffix.registrations[0].fire(HttpByteEvent {
            stream_id: Some(StreamId::new(3)),
            field_section_info: None,
            body_offset: 5,
            transport_offset: 40,
            stream_offset: 23,
            eom: false,
            event_type: ByteEventType::TransportWrite,
        }));
        drop(suffix);
        assert_eq!(callback.actions().len(), 1);
    }

    struct PositionedRegistration {
        end_offset: usize,
        registration: HttpByteEventRegistration,
    }

    struct CoalescedBody {
        bytes: Vec<u8>,
        registrations: Vec<PositionedRegistration>,
    }

    fn coalesce(mut pieces: Vec<BodyPiece>) -> CoalescedBody {
        let mut bytes = Vec::new();
        let mut registrations = Vec::new();
        for mut piece in pieces.drain(..) {
            bytes.extend_from_slice(&piece.bytes);
            let end_offset = bytes.len().saturating_sub(1);
            registrations.extend(piece.registrations.drain(..).map(|registration| {
                PositionedRegistration {
                    end_offset,
                    registration,
                }
            }));
        }
        CoalescedBody {
            bytes,
            registrations,
        }
    }

    #[test]
    fn registration_survives_body_coalesce() {
        let callback = Rc::new(RecordingCallback::default());
        let first = HttpByteEventRegistration::new(
            Some(StreamId::new(8)),
            ByteEventType::NicTx.bit(),
            &callback,
        );
        let second = HttpByteEventRegistration::new(
            Some(StreamId::new(8)),
            ByteEventType::CumulativeAck.bit(),
            &callback,
        );
        let zero_byte_eom = HttpByteEventRegistration::new(
            Some(StreamId::new(8)),
            ByteEventType::TransportWrite.bit(),
            &callback,
        );
        let mut body = coalesce(vec![
            BodyPiece {
                bytes: b"ab".to_vec(),
                registrations: vec![first],
            },
            BodyPiece {
                bytes: b"cde".to_vec(),
                registrations: vec![second],
            },
        ]);

        assert_eq!(body.bytes, b"abcde");
        assert_eq!(body.registrations.len(), 2);
        assert_eq!(body.registrations[0].end_offset, 1);
        assert_eq!(body.registrations[1].end_offset, 4);
        for positioned in &mut body.registrations {
            let event_type = if positioned.end_offset == 1 {
                ByteEventType::NicTx
            } else {
                ByteEventType::CumulativeAck
            };
            assert!(positioned.registration.fire(HttpByteEvent {
                stream_id: Some(StreamId::new(8)),
                field_section_info: None,
                body_offset: positioned.end_offset as u64,
                transport_offset: 100 + positioned.end_offset as u64,
                stream_offset: 70 + positioned.end_offset as u64,
                eom: false,
                event_type,
            }));
        }
        assert!(
            callback
                .actions()
                .iter()
                .all(|action| matches!(action, CallbackAction::Fired(_)))
        );

        let mut zero_byte_eom = zero_byte_eom;
        assert!(zero_byte_eom.fire(HttpByteEvent {
            stream_id: Some(StreamId::new(8)),
            field_section_info: None,
            body_offset: 4,
            transport_offset: 105,
            stream_offset: 75,
            eom: true,
            event_type: ByteEventType::TransportWrite,
        }));
        drop(zero_byte_eom);
        let actions = callback.actions();
        assert_eq!(actions.len(), 3);
        let CallbackAction::Fired(eom) = &actions[2] else {
            panic!("zero-byte EOM registration must fire");
        };
        assert!(eom.eom);
        assert_eq!(eom.body_offset, 4);
    }
}
