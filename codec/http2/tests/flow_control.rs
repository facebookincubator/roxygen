mod flow_control_filter {
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::panic::AssertUnwindSafe;
    use std::rc::Rc;

    use bytes::Bytes;
    use roxygen_codec::CodecCallbackAction;
    use roxygen_codec::CodecCallbackActions;
    use roxygen_codec::CodecCallbackResult;
    use roxygen_codec::CodecCapabilities;
    use roxygen_codec::CodecEvent;
    use roxygen_codec::CodecIngressResult;
    use roxygen_codec::EgressFrame;
    use roxygen_codec::GeneratedFrame;
    use roxygen_codec::H2ErrorCode;
    use roxygen_codec::HttpCodec;
    use roxygen_codec::HttpCodecCallback;
    use roxygen_codec::HttpCodecFilter;
    use roxygen_codec::HttpCodecFilterChain;
    use roxygen_codec::HttpExceptionDirection;
    use roxygen_codec::OwnedEgressFrame;
    use roxygen_core::buf_queue::BufQueue;
    use roxygen_core::error::HttpError;
    use roxygen_core::error::HttpErrorCode;
    use roxygen_core::http::CodecProtocol;
    use roxygen_core::http::TransportDirection;
    use roxygen_core::ids::StreamId;

    use crate::flow_control::ConnectionFlowControlCallback;
    use crate::flow_control::FlowControlledCodec;

    const INITIAL_CAPACITY: u32 = 12_345;
    const OUTPUT_LIMIT: usize = 4 * 1024 * 1024;

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum Record {
        OuterError,
        UserError,
        Open,
        Closed,
        Body {
            stream: StreamId,
            length: usize,
            padding: u16,
        },
        WindowUpdate {
            stream: StreamId,
            amount: u32,
        },
        Error {
            stream: StreamId,
            direction: HttpExceptionDirection,
            code: Option<H2ErrorCode>,
            has_http_status: bool,
            has_proxygen_error: bool,
            message: Vec<u8>,
            new_stream: bool,
        },
    }

    #[derive(Clone, Copy, Default)]
    enum CallbackActionMode {
        #[default]
        None,
        ErrorStreamWindowUpdate,
        BodyConnectionWindowUpdate,
        BodyWindowUpdateThenClosingBody,
    }

    #[derive(Default)]
    struct TestCallbacks {
        records: RefCell<Vec<Record>>,
        action_mode: Cell<CallbackActionMode>,
        opts_out_of_frame_headers: Cell<bool>,
    }

    impl TestCallbacks {
        fn take(&self) -> Vec<Record> {
            std::mem::take(&mut *self.records.borrow_mut())
        }

        fn record_event(&self, event: CodecEvent<'_>) {
            match event {
                CodecEvent::Body {
                    stream,
                    body,
                    padding,
                } => self.records.borrow_mut().push(Record::Body {
                    stream,
                    length: body.chain_length(),
                    padding,
                }),
                CodecEvent::WindowUpdate { stream, amount } => self
                    .records
                    .borrow_mut()
                    .push(Record::WindowUpdate { stream, amount }),
                CodecEvent::Error {
                    stream,
                    error,
                    new_stream,
                } => self.records.borrow_mut().push(Record::Error {
                    stream,
                    direction: error.direction(),
                    code: error.codec_status_code(),
                    has_http_status: error.has_http_status_code(),
                    has_proxygen_error: error.has_proxygen_error(),
                    message: error.message().to_vec(),
                    new_stream,
                }),
                _ => {}
            }
        }
    }

    impl ConnectionFlowControlCallback for TestCallbacks {
        fn on_connection_send_window_open(&self) {
            self.records.borrow_mut().push(Record::Open);
        }

        fn on_connection_send_window_closed(&self) {
            self.records.borrow_mut().push(Record::Closed);
        }
    }

    struct CallbackProxy(Rc<TestCallbacks>);

    impl HttpCodecCallback for CallbackProxy {
        fn on_event(&mut self, event: CodecEvent<'_>) {
            self.0.record_event(event);
        }

        fn on_ingress_event(&mut self, event: CodecEvent<'_>) -> CodecCallbackActions {
            let error = matches!(&event, CodecEvent::Error { .. });
            let body_event = matches!(&event, CodecEvent::Body { .. });
            self.0.record_event(event);
            match (self.0.action_mode.get(), error, body_event) {
                (CallbackActionMode::ErrorStreamWindowUpdate, true, _) => {
                    self.0.records.borrow_mut().push(Record::UserError);
                    vec![CodecCallbackAction::Generate(
                        OwnedEgressFrame::WindowUpdate {
                            stream: StreamId::new(7),
                            amount: 9,
                        },
                    )]
                    .into()
                }
                (CallbackActionMode::BodyConnectionWindowUpdate, _, true) => {
                    vec![CodecCallbackAction::Generate(
                        OwnedEgressFrame::WindowUpdate {
                            stream: StreamId::UNSPECIFIED,
                            amount: 9,
                        },
                    )]
                    .into()
                }
                (CallbackActionMode::BodyWindowUpdateThenClosingBody, _, true) => vec![
                    CodecCallbackAction::Generate(OwnedEgressFrame::WindowUpdate {
                        stream: StreamId::new(7),
                        amount: 9,
                    }),
                    CodecCallbackAction::Generate(OwnedEgressFrame::Body {
                        stream: StreamId::new(1),
                        body: body(INITIAL_CAPACITY as usize),
                        padding: None,
                        end_stream: false,
                    }),
                ]
                .into(),
                _ => CodecCallbackActions::new(),
            }
        }

        fn observes_frame_headers(&self) -> bool {
            !self.0.opts_out_of_frame_headers.get()
        }
    }

    enum PendingIngress {
        Body { length: usize, padding: u16 },
        WindowUpdate { stream: StreamId, amount: u32 },
    }

    struct TestCodecControl {
        reusable: Cell<bool>,
        fail_body: Cell<bool>,
        fail_window_update: Cell<bool>,
        pending_ingress: RefCell<VecDeque<PendingIngress>>,
        observed_frame_headers: Cell<Option<bool>>,
    }

    impl Default for TestCodecControl {
        fn default() -> Self {
            Self {
                reusable: Cell::new(true),
                fail_body: Cell::new(false),
                fail_window_update: Cell::new(false),
                pending_ingress: RefCell::new(VecDeque::new()),
                observed_frame_headers: Cell::new(None),
            }
        }
    }

    struct TestCodec {
        records: Rc<TestCallbacks>,
        control: Rc<TestCodecControl>,
    }

    impl HttpCodec for TestCodec {
        fn protocol(&self) -> CodecProtocol {
            CodecProtocol::Http2
        }

        fn transport_direction(&self) -> TransportDirection {
            TransportDirection::Downstream
        }

        fn capabilities(&self) -> CodecCapabilities {
            CodecCapabilities {
                stream_flow_control: true,
                session_flow_control: true,
                parallel_requests: true,
                push_transactions: true,
                default_window_size: INITIAL_CAPACITY,
            }
        }

        fn create_stream(&mut self) -> StreamId {
            StreamId::new(1)
        }

        fn is_busy(&self) -> bool {
            false
        }

        fn set_parser_paused(&mut self, _paused: bool) {}

        fn is_parser_paused(&self) -> bool {
            false
        }

        fn on_ingress(
            &mut self,
            input: &BufQueue,
            callback: &mut dyn HttpCodecCallback,
            callback_egress: &mut BufQueue,
        ) -> CodecIngressResult {
            self.control
                .observed_frame_headers
                .set(Some(callback.observes_frame_headers()));
            let mut result = CodecIngressResult::new(input.chain_length());
            let pending = self.control.pending_ingress.borrow_mut().pop_front();
            if let Some(pending) = pending {
                let callback_result = match pending {
                    PendingIngress::Body { length, padding } => self.dispatch_ingress_callback(
                        callback,
                        callback_egress,
                        CodecEvent::Body {
                            stream: StreamId::new(1),
                            body: body(length),
                            padding,
                        },
                    ),
                    PendingIngress::WindowUpdate { stream, amount } => self
                        .dispatch_ingress_callback(
                            callback,
                            callback_egress,
                            CodecEvent::WindowUpdate { stream, amount },
                        ),
                };
                result.append_callback_result(callback_result);
            }
            result
        }

        fn on_ingress_eof(
            &mut self,
            _callback: &mut dyn HttpCodecCallback,
            _callback_egress: &mut BufQueue,
        ) -> CodecCallbackResult {
            CodecCallbackResult::default()
        }

        fn is_reusable(&self) -> bool {
            self.control.reusable.get()
        }

        fn is_waiting_to_drain(&self) -> bool {
            false
        }

        fn close_on_egress_complete(&self) -> bool {
            false
        }

        fn last_incoming_stream_id(&self) -> StreamId {
            StreamId::UNSPECIFIED
        }

        fn generate(
            &mut self,
            output: &mut BufQueue,
            frame: EgressFrame<'_>,
            _callback: &mut dyn HttpCodecCallback,
        ) -> Result<GeneratedFrame, HttpError> {
            let (bytes_written, fail) = match frame {
                EgressFrame::Body {
                    stream,
                    body,
                    padding,
                    ..
                } => {
                    self.records.records.borrow_mut().push(Record::Body {
                        stream,
                        length: body.chain_length(),
                        padding: u16::from(padding.unwrap_or(0)),
                    });
                    (body.chain_length() + 4, self.control.fail_body.get())
                }
                EgressFrame::WindowUpdate { stream, amount } => {
                    self.records
                        .records
                        .borrow_mut()
                        .push(Record::WindowUpdate { stream, amount });
                    (10, self.control.fail_window_update.get())
                }
                _ => (0, false),
            };
            if fail {
                return Err(HttpError::new(HttpErrorCode::INTERNAL_ERROR));
            }
            if bytes_written != 0 {
                output
                    .push_bytes(Bytes::from(vec![0; bytes_written]))
                    .expect("test output has sufficient capacity");
            }
            Ok(GeneratedFrame {
                bytes_written,
                ..GeneratedFrame::default()
            })
        }
    }

    struct Fixture {
        codec: FlowControlledCodec,
        callback: CallbackProxy,
        output: BufQueue,
        records: Rc<TestCallbacks>,
        control: Rc<TestCodecControl>,
    }

    static ZERO_CHUNK: [u8; 64 * 1024] = [0; 64 * 1024];

    fn body(length: usize) -> BufQueue {
        let mut queue = BufQueue::new(length);
        let mut remaining = length;
        while remaining != 0 {
            let chunk = remaining.min(ZERO_CHUNK.len());
            queue
                .push_bytes(Bytes::from_static(&ZERO_CHUNK[..chunk]))
                .expect("test body exactly fits its queue");
            remaining -= chunk;
        }
        queue
    }

    fn fixture(receive_capacity: u32) -> Fixture {
        let records = Rc::new(TestCallbacks::default());
        let control = Rc::new(TestCodecControl::default());
        let inner = TestCodec {
            records: Rc::clone(&records),
            control: Rc::clone(&control),
        };
        let mut callback = CallbackProxy(Rc::clone(&records));
        let mut output = BufQueue::new(OUTPUT_LIMIT);
        let codec = FlowControlledCodec::new(
            Box::new(inner),
            records.clone(),
            &mut output,
            &mut callback,
            receive_capacity,
        )
        .expect("test wrapper construction succeeds");
        Fixture {
            codec,
            callback,
            output,
            records,
            control,
        }
    }

    fn ingress(fixture: &mut Fixture, pending: PendingIngress) -> CodecIngressResult {
        fixture
            .control
            .pending_ingress
            .borrow_mut()
            .push_back(pending);
        let mut callback_egress = BufQueue::new(OUTPUT_LIMIT);
        HttpCodec::on_ingress(
            &mut fixture.codec,
            &BufQueue::from_bytes(Bytes::from_static(b"x"), 1).expect("one byte fits"),
            &mut fixture.callback,
            &mut callback_egress,
        )
    }

    fn generate_body(fixture: &mut Fixture, length: usize, padding: Option<u8>) {
        HttpCodec::generate(
            &mut fixture.codec,
            &mut fixture.output,
            EgressFrame::Body {
                stream: StreamId::new(1),
                body: body(length),
                padding,
                end_stream: false,
            },
            &mut fixture.callback,
        )
        .expect("test body generation succeeds");
    }

    #[test]
    fn flow_control_forwards_the_opt_out() {
        for opts_out in [true, false] {
            let mut fixture = fixture(INITIAL_CAPACITY);
            fixture.records.opts_out_of_frame_headers.set(opts_out);
            let result = ingress(
                &mut fixture,
                PendingIngress::WindowUpdate {
                    stream: StreamId::new(1),
                    amount: 1,
                },
            );
            assert_eq!(result.consumed, 1);
            assert_eq!(
                fixture.control.observed_frame_headers.get(),
                Some(!opts_out),
                "the wrapped codec sees the inner callback's choice (opts out: {opts_out})"
            );
        }
    }

    #[test]
    fn default_flow_control_flow_control_construct() {
        let mut fixture = fixture(0);
        assert!(fixture.output.is_empty());

        generate_body(&mut fixture, INITIAL_CAPACITY as usize - 1, None);
        let _ = ingress(
            &mut fixture,
            PendingIngress::WindowUpdate {
                stream: StreamId::UNSPECIFIED,
                amount: 1,
            },
        );
        generate_body(&mut fixture, 2, None);
        let _ = ingress(
            &mut fixture,
            PendingIngress::WindowUpdate {
                stream: StreamId::UNSPECIFIED,
                amount: 1,
            },
        );

        assert_eq!(
            fixture.records.take(),
            vec![
                Record::Body {
                    stream: StreamId::new(1),
                    length: INITIAL_CAPACITY as usize - 1,
                    padding: 0,
                },
                Record::Closed,
                Record::Body {
                    stream: StreamId::new(1),
                    length: 2,
                    padding: 0,
                },
                Record::Open,
            ]
        );
        assert_eq!(fixture.codec.available_send(), 1);

        let panic = std::panic::catch_unwind(AssertUnwindSafe(|| {
            generate_body(&mut fixture, 2, None);
        }));
        assert!(panic.is_err());
        assert!(fixture.records.take().is_empty());
    }

    #[test]
    fn default_flow_control_send_update() {
        let mut fixture = fixture(0);
        let _ = ingress(
            &mut fixture,
            PendingIngress::Body {
                length: INITIAL_CAPACITY as usize / 2 + 1,
                padding: 0,
            },
        );
        assert!(
            !fixture
                .codec
                .ingress_bytes_processed(
                    &mut fixture.output,
                    INITIAL_CAPACITY / 2,
                    &mut fixture.callback,
                )
                .unwrap()
        );
        assert!(
            fixture
                .codec
                .ingress_bytes_processed(&mut fixture.output, 1, &mut fixture.callback)
                .unwrap()
        );
        assert_eq!(
            fixture.records.take(),
            vec![
                Record::Body {
                    stream: StreamId::new(1),
                    length: INITIAL_CAPACITY as usize / 2 + 1,
                    padding: 0,
                },
                Record::WindowUpdate {
                    stream: StreamId::UNSPECIFIED,
                    amount: INITIAL_CAPACITY / 2 + 1,
                },
            ]
        );
    }

    #[test]
    fn big_window_recv_too_much() {
        let mut fixture = fixture(1_000_000);
        assert!(!fixture.output.is_empty());
        assert_eq!(
            fixture.records.take(),
            vec![Record::WindowUpdate {
                stream: StreamId::UNSPECIFIED,
                amount: 1_000_000 - INITIAL_CAPACITY,
            }]
        );

        let _ = ingress(
            &mut fixture,
            PendingIngress::Body {
                length: 1_000_000,
                padding: 0,
            },
        );
        assert!(HttpCodec::is_reusable(&fixture.codec));
        let _ = ingress(
            &mut fixture,
            PendingIngress::Body {
                length: 1,
                padding: 0,
            },
        );
        assert_eq!(
            fixture.records.take(),
            vec![
                Record::Body {
                    stream: StreamId::new(1),
                    length: 1_000_000,
                    padding: 0,
                },
                Record::Error {
                    stream: StreamId::UNSPECIFIED,
                    direction: HttpExceptionDirection::IngressAndEgress,
                    code: Some(H2ErrorCode::FLOW_CONTROL_ERROR),
                    has_http_status: false,
                    has_proxygen_error: false,
                    message: b"Failed to reserve receive window, window size=0, amount=1".to_vec(),
                    new_stream: false,
                },
            ]
        );
        assert!(!HttpCodec::is_reusable(&fixture.codec));
    }

    #[test]
    fn big_window_remote_increase() {
        let mut fixture = fixture(1_000_000);
        fixture.records.take();
        assert_eq!(fixture.codec.available_send(), INITIAL_CAPACITY);
        let _ = ingress(
            &mut fixture,
            PendingIngress::WindowUpdate {
                stream: StreamId::UNSPECIFIED,
                amount: 10,
            },
        );
        assert_eq!(fixture.codec.available_send(), INITIAL_CAPACITY + 10);
        generate_body(&mut fixture, (INITIAL_CAPACITY + 10) as usize, None);
        assert_eq!(fixture.codec.available_send(), 0);
        let _ = ingress(
            &mut fixture,
            PendingIngress::WindowUpdate {
                stream: StreamId::UNSPECIFIED,
                amount: i32::MAX as u32,
            },
        );
        assert_eq!(fixture.codec.available_send(), i32::MAX as u32);
        let _ = ingress(
            &mut fixture,
            PendingIngress::WindowUpdate {
                stream: StreamId::UNSPECIFIED,
                amount: 1,
            },
        );
        assert_eq!(
            fixture.records.take(),
            vec![
                Record::Closed,
                Record::Body {
                    stream: StreamId::new(1),
                    length: (INITIAL_CAPACITY + 10) as usize,
                    padding: 0,
                },
                Record::Open,
                Record::Error {
                    stream: StreamId::UNSPECIFIED,
                    direction: HttpExceptionDirection::IngressAndEgress,
                    code: Some(H2ErrorCode::FLOW_CONTROL_ERROR),
                    has_http_status: false,
                    has_proxygen_error: false,
                    message: b"Failed to update send window, outstanding=0, amount=1".to_vec(),
                    new_stream: false,
                },
            ]
        );
        assert!(!HttpCodec::is_reusable(&fixture.codec));
    }

    #[test]
    fn set_receive_window_size_only_grows_and_emits_exact_delta() {
        let mut fixture = fixture(0);
        fixture
            .codec
            .set_receive_window_size(&mut fixture.output, 20_000, &mut fixture.callback)
            .unwrap();
        fixture
            .codec
            .set_receive_window_size(&mut fixture.output, INITIAL_CAPACITY, &mut fixture.callback)
            .unwrap();
        fixture
            .codec
            .set_receive_window_size(&mut fixture.output, u32::MAX, &mut fixture.callback)
            .unwrap();
        assert_eq!(fixture.codec.receive_window().capacity(), 20_000);
        assert_eq!(
            fixture.records.take(),
            vec![Record::WindowUpdate {
                stream: StreamId::UNSPECIFIED,
                amount: 20_000 - INITIAL_CAPACITY,
            }]
        );
    }

    #[test]
    fn constructor_ignores_capacity_below_codec_default() {
        let fixture = fixture(100);
        assert_eq!(fixture.codec.receive_window().capacity(), INITIAL_CAPACITY);
        assert!(fixture.output.is_empty());
        assert!(fixture.records.take().is_empty());
    }

    #[test]
    fn ingress_padding_credit_is_returned_with_the_connection_window_update() {
        let mut fixture = fixture(0);
        let _ = ingress(
            &mut fixture,
            PendingIngress::Body {
                length: INITIAL_CAPACITY as usize - 10,
                padding: 10,
            },
        );
        assert_eq!(fixture.codec.receive_window().size(), 0);
        assert!(
            !fixture
                .codec
                .ingress_bytes_processed(
                    &mut fixture.output,
                    INITIAL_CAPACITY / 2 - 10,
                    &mut fixture.callback,
                )
                .unwrap()
        );
        assert!(
            fixture
                .codec
                .ingress_bytes_processed(&mut fixture.output, 1, &mut fixture.callback)
                .unwrap()
        );
        assert_eq!(
            fixture.records.take(),
            vec![
                Record::Body {
                    stream: StreamId::new(1),
                    length: INITIAL_CAPACITY as usize - 10,
                    padding: 10,
                },
                Record::WindowUpdate {
                    stream: StreamId::UNSPECIFIED,
                    amount: INITIAL_CAPACITY / 2 + 1,
                },
            ]
        );
        assert_eq!(
            fixture.codec.receive_window().size(),
            (INITIAL_CAPACITY / 2 + 1) as i32
        );
    }

    #[test]
    fn nonzero_stream_window_update_is_forwarded_unchanged() {
        let mut fixture = fixture(0);
        let _ = ingress(
            &mut fixture,
            PendingIngress::WindowUpdate {
                stream: StreamId::new(7),
                amount: 91,
            },
        );
        assert_eq!(
            fixture.records.take(),
            vec![Record::WindowUpdate {
                stream: StreamId::new(7),
                amount: 91,
            }]
        );
        assert_eq!(fixture.codec.available_send(), INITIAL_CAPACITY);
    }

    #[test]
    fn connection_window_update_is_never_forwarded() {
        let mut fixture = fixture(0);
        let _ = ingress(
            &mut fixture,
            PendingIngress::WindowUpdate {
                stream: StreamId::UNSPECIFIED,
                amount: 1,
            },
        );
        assert!(fixture.records.take().is_empty());
        assert_eq!(fixture.codec.available_send(), INITIAL_CAPACITY + 1);
    }

    #[test]
    fn manual_connection_window_update_is_rejected() {
        let mut fixture = fixture(0);
        let panic = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _ = HttpCodec::generate(
                &mut fixture.codec,
                &mut fixture.output,
                EgressFrame::WindowUpdate {
                    stream: StreamId::UNSPECIFIED,
                    amount: 1,
                },
                &mut fixture.callback,
            );
        }));
        assert!(panic.is_err());
        assert!(fixture.records.take().is_empty());
    }

    #[test]
    fn callback_generated_connection_window_update_is_rejected() {
        let mut fixture = fixture(0);
        fixture
            .records
            .action_mode
            .set(CallbackActionMode::BodyConnectionWindowUpdate);
        let panic = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _ = ingress(
                &mut fixture,
                PendingIngress::Body {
                    length: 1,
                    padding: 0,
                },
            );
        }));

        assert!(panic.is_err());
        assert_eq!(fixture.codec.available_send(), INITIAL_CAPACITY);
        assert_eq!(
            fixture.records.take(),
            vec![Record::Body {
                stream: StreamId::new(1),
                length: 1,
                padding: 0,
            }]
        );
    }

    #[test]
    fn callback_generated_actions_preserve_window_update_then_close_then_body_order() {
        let mut fixture = fixture(0);
        fixture
            .records
            .action_mode
            .set(CallbackActionMode::BodyWindowUpdateThenClosingBody);

        let result = ingress(
            &mut fixture,
            PendingIngress::Body {
                length: 1,
                padding: 0,
            },
        );

        assert_eq!(result.egress_results.len(), 2);
        assert!(result.egress_results.iter().all(Result::is_ok));
        assert_eq!(fixture.codec.available_send(), 0);
        assert_eq!(
            fixture.records.take(),
            vec![
                Record::Body {
                    stream: StreamId::new(1),
                    length: 1,
                    padding: 0,
                },
                Record::WindowUpdate {
                    stream: StreamId::new(7),
                    amount: 9,
                },
                Record::Closed,
                Record::Body {
                    stream: StreamId::new(1),
                    length: INITIAL_CAPACITY as usize,
                    padding: 0,
                },
            ]
        );
    }

    #[test]
    fn failed_callback_generated_body_consumes_credit_and_is_sticky() {
        let mut fixture = fixture(0);
        fixture.control.fail_body.set(true);
        fixture
            .records
            .action_mode
            .set(CallbackActionMode::BodyWindowUpdateThenClosingBody);

        let result = ingress(
            &mut fixture,
            PendingIngress::Body {
                length: 1,
                padding: 0,
            },
        );

        assert_eq!(result.egress_results.len(), 2);
        assert!(result.egress_results[0].is_ok());
        assert!(result.egress_results[1].is_err());
        assert_eq!(fixture.codec.available_send(), 0);
        assert!(fixture.codec.has_error());
        assert!(!HttpCodec::is_reusable(&fixture.codec));
        assert_eq!(
            fixture.records.take(),
            vec![
                Record::Body {
                    stream: StreamId::new(1),
                    length: 1,
                    padding: 0,
                },
                Record::WindowUpdate {
                    stream: StreamId::new(7),
                    amount: 9,
                },
                Record::Closed,
                Record::Body {
                    stream: StreamId::new(1),
                    length: INITIAL_CAPACITY as usize,
                    padding: 0,
                },
            ]
        );
    }

    #[test]
    fn delegate_reusability_is_preserved_until_sticky_filter_error() {
        let mut fixture = fixture(0);
        fixture.control.reusable.set(false);
        assert!(!HttpCodec::is_reusable(&fixture.codec));
        fixture.control.reusable.set(true);
        assert!(HttpCodec::is_reusable(&fixture.codec));
        let _ = ingress(
            &mut fixture,
            PendingIngress::Body {
                length: INITIAL_CAPACITY as usize + 1,
                padding: 0,
            },
        );
        assert!(fixture.codec.has_error());
        assert!(!HttpCodec::is_reusable(&fixture.codec));
        fixture.control.reusable.set(false);
        assert!(!HttpCodec::is_reusable(&fixture.codec));
    }

    struct OuterErrorFilter(Rc<TestCallbacks>);

    impl HttpCodecFilter for OuterErrorFilter {
        fn on_ingress_callback<'event>(
            &self,
            event: CodecEvent<'event>,
            next: &mut dyn FnMut(CodecEvent<'event>) -> CodecCallbackActions,
        ) -> CodecCallbackActions {
            if matches!(&event, CodecEvent::Error { .. }) {
                self.0.records.borrow_mut().push(Record::OuterError);
            }
            next(event)
        }
    }

    #[test]
    fn synthesized_error_traverses_outer_filters_and_preserves_actions() {
        let mut fixture = fixture(0);
        fixture
            .records
            .action_mode
            .set(CallbackActionMode::ErrorStreamWindowUpdate);
        fixture
            .control
            .pending_ingress
            .borrow_mut()
            .push_back(PendingIngress::Body {
                length: INITIAL_CAPACITY as usize + 1,
                padding: 0,
            });
        let mut chain = HttpCodecFilterChain::new(fixture.codec);
        chain.add_filter(OuterErrorFilter(Rc::clone(&fixture.records)));
        let mut callback_egress = BufQueue::new(64);
        let result = HttpCodec::on_ingress(
            &mut chain,
            &BufQueue::from_bytes(Bytes::from_static(b"x"), 1).expect("one byte fits"),
            &mut fixture.callback,
            &mut callback_egress,
        );
        assert_eq!(result.egress_results.len(), 1);
        assert!(result.egress_results[0].is_ok());
        assert_eq!(
            fixture.records.take(),
            vec![
                Record::OuterError,
                Record::Error {
                    stream: StreamId::UNSPECIFIED,
                    direction: HttpExceptionDirection::IngressAndEgress,
                    code: Some(H2ErrorCode::FLOW_CONTROL_ERROR),
                    has_http_status: false,
                    has_proxygen_error: false,
                    message: b"Failed to reserve receive window, window size=12345, amount=12346"
                        .to_vec(),
                    new_stream: false,
                },
                Record::UserError,
                Record::WindowUpdate {
                    stream: StreamId::new(7),
                    amount: 9,
                },
            ]
        );
        assert!(!HttpCodec::is_reusable(&chain));
    }

    #[test]
    fn failed_receive_ack_is_sticky_after_committed_credit() {
        let mut fixture = fixture(0);
        let _ = ingress(
            &mut fixture,
            PendingIngress::Body {
                length: INITIAL_CAPACITY as usize / 2 + 1,
                padding: 0,
            },
        );
        fixture.control.fail_window_update.set(true);
        let result = fixture.codec.ingress_bytes_processed(
            &mut fixture.output,
            INITIAL_CAPACITY / 2 + 1,
            &mut fixture.callback,
        );
        assert!(result.is_err());
        assert!(fixture.codec.has_error());
        assert!(!HttpCodec::is_reusable(&fixture.codec));
        assert_eq!(
            fixture.codec.receive_window().size(),
            INITIAL_CAPACITY as i32
        );
    }

    #[test]
    fn failed_body_generation_consumes_credit_and_is_sticky() {
        let mut fixture = fixture(0);
        fixture.control.fail_body.set(true);
        let result = HttpCodec::generate(
            &mut fixture.codec,
            &mut fixture.output,
            EgressFrame::Body {
                stream: StreamId::new(1),
                body: body(INITIAL_CAPACITY as usize),
                padding: None,
                end_stream: false,
            },
            &mut fixture.callback,
        );
        assert!(result.is_err());
        assert_eq!(fixture.codec.available_send(), 0);
        assert!(fixture.codec.has_error());
        assert!(!HttpCodec::is_reusable(&fixture.codec));
        assert_eq!(
            fixture.records.take(),
            vec![
                Record::Closed,
                Record::Body {
                    stream: StreamId::new(1),
                    length: INITIAL_CAPACITY as usize,
                    padding: 0,
                },
            ]
        );
    }

    fn process(fixture: &mut Fixture, amount: u32) -> bool {
        fixture
            .codec
            .ingress_bytes_processed(&mut fixture.output, amount, &mut fixture.callback)
            .expect("processed credit is returned")
    }

    fn connection_updates(records: &[Record]) -> u64 {
        records
            .iter()
            .map(|record| match record {
                Record::WindowUpdate { stream, amount } if stream.is_unspecified() => {
                    u64::from(*amount)
                }
                _ => 0,
            })
            .sum()
    }

    #[test]
    fn padded_data_credit_is_freed_once_at_maximum_window() {
        let capacity = i32::MAX as u32;
        let mut fixture = fixture(capacity);
        fixture.records.take();
        let _ = ingress(
            &mut fixture,
            PendingIngress::Body {
                length: 5,
                padding: 10,
            },
        );
        let bulk = 1usize << 30;
        let _ = ingress(
            &mut fixture,
            PendingIngress::Body {
                length: bulk,
                padding: 0,
            },
        );
        assert!(process(&mut fixture, 5 + bulk as u32));
        assert_eq!(fixture.codec.receive_window().size(), capacity as i32);
        assert_eq!(
            fixture.records.take().last(),
            Some(&Record::WindowUpdate {
                stream: StreamId::UNSPECIFIED,
                amount: bulk as u32 + 15,
            })
        );
        assert!(HttpCodec::is_reusable(&fixture.codec));
    }

    #[test]
    fn unreserved_connection_credit_at_maximum_window_is_a_flow_control_error() {
        let capacity = i32::MAX as u32;
        let mut fixture = fixture(capacity);
        fixture.records.take();
        let written = fixture.output.chain_length();
        let error = fixture
            .codec
            .ingress_bytes_processed(&mut fixture.output, capacity / 2 + 1, &mut fixture.callback)
            .expect_err("credit that was never reserved cannot be returned");
        assert_eq!(error.code(), HttpErrorCode::FLOW_CONTROL_ERROR);
        assert_eq!(
            fixture.output.chain_length(),
            written,
            "no WINDOW_UPDATE is serialized"
        );
        assert_eq!(connection_updates(&fixture.records.take()), 0);
        assert_eq!(fixture.codec.receive_window().size(), capacity as i32);
        assert!(!HttpCodec::is_reusable(&fixture.codec));
    }

    #[test]
    fn padded_data_does_not_inflate_the_connection_window() {
        let mut fixture = fixture(0);
        let mut received = 0u64;
        for _ in 0..100 {
            let _ = ingress(
                &mut fixture,
                PendingIngress::Body {
                    length: 50,
                    padding: 50,
                },
            );
            received += 100;
            process(&mut fixture, 50);
        }
        let acknowledged = connection_updates(&fixture.records.take());
        assert!(acknowledged > 0);
        assert_eq!(
            i64::from(fixture.codec.receive_window().size()),
            i64::from(INITIAL_CAPACITY) - (received as i64 - acknowledged as i64)
        );
    }

    #[test]
    fn padding_overrun_of_the_true_window_is_a_flow_control_error() {
        let mut fixture = fixture(0);
        let _ = ingress(
            &mut fixture,
            PendingIngress::Body {
                length: 6000,
                padding: 200,
            },
        );
        assert!(process(&mut fixture, 6000));
        let _ = ingress(
            &mut fixture,
            PendingIngress::Body {
                length: INITIAL_CAPACITY as usize,
                padding: 0,
            },
        );
        assert!(HttpCodec::is_reusable(&fixture.codec));
        fixture.records.take();
        let _ = ingress(
            &mut fixture,
            PendingIngress::Body {
                length: 1,
                padding: 0,
            },
        );
        assert_eq!(
            fixture.records.take(),
            vec![Record::Error {
                stream: StreamId::UNSPECIFIED,
                direction: HttpExceptionDirection::IngressAndEgress,
                code: Some(H2ErrorCode::FLOW_CONTROL_ERROR),
                has_http_status: false,
                has_proxygen_error: false,
                message: b"Failed to reserve receive window, window size=0, amount=1".to_vec(),
                new_stream: false,
            }]
        );
        assert!(!HttpCodec::is_reusable(&fixture.codec));
    }

    #[test]
    fn padded_window_updates_are_unchanged() {
        let mut fixture = fixture(0);
        let frames: [(usize, u16); 6] = [
            (1000, 256),
            (3000, 1),
            (2500, 100),
            (4000, 0),
            (100, 255),
            (2000, 20),
        ];
        let mut expected = Vec::new();
        for (length, padding) in frames {
            let _ = ingress(&mut fixture, PendingIngress::Body { length, padding });
            process(&mut fixture, length as u32);
            expected.push(Record::Body {
                stream: StreamId::new(1),
                length,
                padding,
            });
            match length {
                2500 => expected.push(Record::WindowUpdate {
                    stream: StreamId::UNSPECIFIED,
                    amount: 6857,
                }),
                2000 => expected.push(Record::WindowUpdate {
                    stream: StreamId::UNSPECIFIED,
                    amount: 6375,
                }),
                _ => {}
            }
        }
        assert_eq!(fixture.records.take(), expected);
        assert_eq!(fixture.output.chain_length(), 20);
        assert!(HttpCodec::is_reusable(&fixture.codec));
    }
}
