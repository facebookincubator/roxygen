use std::cell::Cell;
use std::cell::RefCell;
use std::rc::Rc;

use roxygen_core::buf_queue::BufQueue;
use roxygen_core::error::HttpError;
use roxygen_core::http::CodecProtocol;
use roxygen_core::http::TransportDirection;
use roxygen_core::ids::StreamId;
use roxygen_core::message::HttpMessage;

use crate::CodecCallbackAction;
use crate::CodecCallbackActions;
use crate::CodecCallbackResult;
use crate::CodecCapabilities;
use crate::CodecEvent;
use crate::CodecIngressResult;
use crate::EgressFrame;
use crate::GeneratedFrame;
use crate::H2ErrorCode;
use crate::HttpCodec;
use crate::HttpCodecCall;
use crate::HttpCodecCallback;
use crate::HttpCodecFilter;
use crate::HttpCodecFilterChain;
use crate::HttpCodecQuery;
use crate::HttpException;
use crate::HttpExceptionDirection;
use crate::MultiplexedSessionTransport;
use crate::OwnedEgressFrame;

fn queue(bytes: &[u8]) -> BufQueue {
    BufQueue::copy_from_slice(bytes, bytes.len()).expect("test input fits its exact queue limit")
}

#[derive(Debug, Eq, PartialEq)]
struct RequestStream(u64);

#[derive(Debug, Eq, PartialEq)]
struct ControlStream(u64);

#[derive(Default)]
struct MockMultiplexedTransport {
    next_request: u64,
    next_control: u64,
}

impl MultiplexedSessionTransport for MockMultiplexedTransport {
    type RequestStream = RequestStream;
    type ControlStream = ControlStream;

    fn open_request_stream(&mut self) -> Result<Self::RequestStream, HttpError> {
        self.next_request += 1;
        Ok(RequestStream(self.next_request))
    }

    fn open_unidirectional_control_stream(&mut self) -> Result<Self::ControlStream, HttpError> {
        self.next_control += 1;
        Ok(ControlStream(self.next_control))
    }
}

#[derive(Default)]
struct SpikeCodec {
    frame: Vec<u8>,
    expected_payload: Option<usize>,
    paused: bool,
    next_stream: u64,
    pipelined: bool,
    action_log: Option<Rc<RefCell<Vec<String>>>>,
    emit_generated_callback: bool,
}

impl SpikeCodec {
    fn emit_frame(
        &mut self,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
    ) -> CodecCallbackResult {
        let stream = StreamId::new(1);
        let mut message = HttpMessage::new();
        let mut result = HttpCodec::dispatch_ingress_callback(
            self,
            callback,
            callback_egress,
            CodecEvent::MessageBegin {
                stream,
                message: &mut message,
            },
        );
        result.append(HttpCodec::dispatch_ingress_callback(
            self,
            callback,
            callback_egress,
            CodecEvent::HeadersComplete {
                stream,
                message: Box::new(message),
            },
        ));
        result.append(HttpCodec::dispatch_ingress_callback(
            self,
            callback,
            callback_egress,
            CodecEvent::Body {
                stream,
                body: BufQueue::new(0),
                padding: 0,
            },
        ));
        result.append(HttpCodec::dispatch_ingress_callback(
            self,
            callback,
            callback_egress,
            CodecEvent::MessageComplete {
                stream,
                upgraded: false,
            },
        ));
        self.frame.clear();
        self.expected_payload = None;
        result
    }

    fn emit_error(
        &mut self,
        direction: HttpExceptionDirection,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
    ) -> CodecCallbackResult {
        let mut error = HttpException::new(direction, b"invalid spike frame");
        error.set_codec_status_code(H2ErrorCode::PROTOCOL_ERROR);
        HttpCodec::dispatch_ingress_callback(
            self,
            callback,
            callback_egress,
            CodecEvent::Error {
                stream: StreamId::UNSPECIFIED,
                error: &error,
                new_stream: true,
            },
        )
    }
}

impl HttpCodec for SpikeCodec {
    fn protocol(&self) -> CodecProtocol {
        CodecProtocol::Http11
    }

    fn transport_direction(&self) -> TransportDirection {
        TransportDirection::Downstream
    }

    fn capabilities(&self) -> CodecCapabilities {
        CodecCapabilities::default()
    }

    fn create_stream(&mut self) -> StreamId {
        self.next_stream += 1;
        StreamId::new(self.next_stream)
    }

    fn is_busy(&self) -> bool {
        self.expected_payload.is_some()
    }

    fn set_parser_paused(&mut self, paused: bool) {
        if let Some(log) = &self.action_log {
            log.borrow_mut().push(format!("pause:{paused}"));
        }
        self.paused = paused;
    }

    fn is_parser_paused(&self) -> bool {
        self.paused
    }

    fn on_ingress(
        &mut self,
        input: &BufQueue,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
    ) -> CodecIngressResult {
        if self.paused || input.is_empty() {
            return CodecIngressResult::new(0);
        }

        let mut consumed = 0;
        let mut result = CodecIngressResult::default();
        for byte in input.segments().flat_map(|segment| segment.iter().copied()) {
            if self.paused {
                break;
            }
            if self.expected_payload.is_none() {
                let direction = match byte {
                    u8::MAX => Some(HttpExceptionDirection::Ingress),
                    0xfe => Some(HttpExceptionDirection::Egress),
                    0xfd => Some(HttpExceptionDirection::IngressAndEgress),
                    _ => None,
                };
                if let Some(direction) = direction {
                    consumed += 1;
                    result.append_callback_result(self.emit_error(
                        direction,
                        callback,
                        callback_egress,
                    ));
                    break;
                }
                self.expected_payload = Some(usize::from(byte));
                self.frame.push(byte);
                consumed += 1;
                if self.frame.len()
                    == self
                        .expected_payload
                        .expect("a parsed length prefix sets the expected payload")
                        + 1
                {
                    result.append_callback_result(self.emit_frame(callback, callback_egress));
                    if !self.pipelined {
                        break;
                    }
                }
                continue;
            }

            self.frame.push(byte);
            consumed += 1;

            let expected_frame_len = self
                .expected_payload
                .expect("a parsed length prefix sets the expected payload")
                + 1;
            if self.frame.len() != expected_frame_len {
                continue;
            }

            result.append_callback_result(self.emit_frame(callback, callback_egress));
            if !self.pipelined {
                break;
            }
        }
        result.consumed = consumed;
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
        true
    }

    fn is_waiting_to_drain(&self) -> bool {
        false
    }

    fn close_on_egress_complete(&self) -> bool {
        false
    }

    fn last_incoming_stream_id(&self) -> StreamId {
        StreamId::new(1)
    }

    fn generate(
        &mut self,
        _output: &mut BufQueue,
        frame: EgressFrame<'_>,
        callback: &mut dyn HttpCodecCallback,
    ) -> Result<GeneratedFrame, HttpError> {
        if self.emit_generated_callback {
            callback.on_event(CodecEvent::GeneratedFrameHeader {
                stream: StreamId::new(1),
                frame_type: 1,
                length: 0,
                version: 0,
            });
        }
        let payload_offset = match frame {
            EgressFrame::Headers { stream, .. } => {
                if let Some(log) = &self.action_log {
                    log.borrow_mut()
                        .push(format!("generate:headers:{}", stream.get()));
                }
                Some(stream.get())
            }
            EgressFrame::EndOfMessage { stream } => {
                if let Some(log) = &self.action_log {
                    log.borrow_mut()
                        .push(format!("generate:eom:{}", stream.get()));
                }
                Some(stream.get() + 100)
            }
            _ => None,
        };
        Ok(GeneratedFrame {
            bytes_written: 0,
            header_size: None,
            payload_offset,
        })
    }
}

#[derive(Default)]
struct EventNames {
    names: Vec<&'static str>,
}

impl HttpCodecCallback for EventNames {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        self.names.push(event_name(&event));
    }
}

fn event_name(event: &CodecEvent<'_>) -> &'static str {
    match event {
        CodecEvent::MessageBegin { .. } => "begin",
        CodecEvent::HeadersComplete { .. } => "headers",
        CodecEvent::Body { .. } => "body",
        CodecEvent::MessageComplete { upgraded: true, .. } => "complete-upgraded",
        CodecEvent::MessageComplete {
            upgraded: false, ..
        } => "complete",
        CodecEvent::Error { .. } => "error",
        _ => "other",
    }
}

struct RecordingFilter {
    name: &'static str,
    log: Rc<RefCell<Vec<String>>>,
}

impl HttpCodecFilter for RecordingFilter {
    fn on_ingress(
        &self,
        input: &BufQueue,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        next: &mut dyn HttpCodecCall,
    ) -> CodecIngressResult {
        self.log.borrow_mut().push(format!("call:{}", self.name));
        next.on_ingress(input, callback, callback_egress)
    }

    fn on_callback<'event>(
        &self,
        event: CodecEvent<'event>,
        next: &mut dyn FnMut(CodecEvent<'event>),
    ) {
        self.log
            .borrow_mut()
            .push(format!("{}:{}", self.name, event_name(&event)));
        next(event);
    }
}

struct CallControlFilter;

impl HttpCodecFilter for CallControlFilter {
    fn create_stream(&self, _next: &mut dyn HttpCodecCall) -> StreamId {
        StreamId::new(99)
    }

    fn set_parser_paused(&self, paused: bool, next: &mut dyn HttpCodecCall) {
        next.set_parser_paused(!paused);
    }

    fn on_ingress(
        &self,
        input: &BufQueue,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        next: &mut dyn HttpCodecCall,
    ) -> CodecIngressResult {
        if input
            .segments()
            .flat_map(|segment| segment.iter().copied())
            .eq([0xaa])
        {
            CodecIngressResult::new(0)
        } else {
            next.on_ingress(&queue(&[0]), callback, callback_egress)
        }
    }

    fn generate(
        &self,
        _output: &mut BufQueue,
        _frame: EgressFrame<'_>,
        _callback: &mut dyn HttpCodecCallback,
        _next: &mut dyn HttpCodecCall,
    ) -> Result<GeneratedFrame, HttpError> {
        Ok(GeneratedFrame {
            bytes_written: 42,
            header_size: None,
            payload_offset: Some(7),
        })
    }
}

struct CallbackControlFilter;

impl HttpCodecFilter for CallbackControlFilter {
    fn on_callback<'event>(
        &self,
        event: CodecEvent<'event>,
        next: &mut dyn FnMut(CodecEvent<'event>),
    ) {
        match event {
            CodecEvent::Body { .. } => {}
            CodecEvent::MessageComplete { stream, .. } => next(CodecEvent::MessageComplete {
                stream,
                upgraded: true,
            }),
            event => next(event),
        }
    }
}

struct SynthesizingInnerFilter {
    log: Rc<RefCell<Vec<String>>>,
}

impl HttpCodecFilter for SynthesizingInnerFilter {
    fn on_ingress(
        &self,
        _input: &BufQueue,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        next: &mut dyn HttpCodecCall,
    ) -> CodecIngressResult {
        self.log.borrow_mut().push("call:second".to_owned());
        let mut message = HttpMessage::new();
        let action_result = next.dispatch_ingress_callback(
            callback,
            callback_egress,
            CodecEvent::MessageBegin {
                stream: StreamId::new(7),
                message: &mut message,
            },
        );
        CodecIngressResult {
            consumed: 0,
            egress_results: action_result.egress_results,
        }
    }

    fn on_callback<'event>(
        &self,
        event: CodecEvent<'event>,
        next: &mut dyn FnMut(CodecEvent<'event>),
    ) {
        self.log
            .borrow_mut()
            .push(format!("second:{}", event_name(&event)));
        next(event);
    }
}

struct QueryTransformFilter;

impl HttpCodecFilter for QueryTransformFilter {
    fn protocol(&self, next: &dyn HttpCodecQuery) -> CodecProtocol {
        assert_eq!(next.protocol(), CodecProtocol::Http11);
        CodecProtocol::Http2
    }

    fn transport_direction(&self, next: &dyn HttpCodecQuery) -> TransportDirection {
        !next.transport_direction()
    }

    fn capabilities(&self, next: &dyn HttpCodecQuery) -> CodecCapabilities {
        let mut capabilities = next.capabilities();
        capabilities.stream_flow_control = true;
        capabilities.default_window_size = 99;
        capabilities
    }

    fn is_busy(&self, next: &dyn HttpCodecQuery) -> bool {
        !next.is_busy()
    }

    fn is_parser_paused(&self, next: &dyn HttpCodecQuery) -> bool {
        !next.is_parser_paused()
    }

    fn is_reusable(&self, next: &dyn HttpCodecQuery) -> bool {
        !next.is_reusable()
    }

    fn is_waiting_to_drain(&self, next: &dyn HttpCodecQuery) -> bool {
        !next.is_waiting_to_drain()
    }

    fn close_on_egress_complete(&self, next: &dyn HttpCodecQuery) -> bool {
        !next.close_on_egress_complete()
    }

    fn last_incoming_stream_id(&self, next: &dyn HttpCodecQuery) -> StreamId {
        StreamId::new(next.last_incoming_stream_id().get() + 10)
    }
}

struct RecordingCallback {
    log: Rc<RefCell<Vec<String>>>,
}

impl HttpCodecCallback for RecordingCallback {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        self.log
            .borrow_mut()
            .push(format!("callback:{}", event_name(&event)));
    }
}

struct ErrorCapture {
    seen: Option<(HttpExceptionDirection, Option<H2ErrorCode>, bool)>,
}

impl HttpCodecCallback for ErrorCapture {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        if let CodecEvent::Error {
            error, new_stream, ..
        } = event
        {
            self.seen = Some((error.direction(), error.codec_status_code(), new_stream));
        }
    }
}

struct PauseAfterFirstComplete {
    completions: usize,
}

impl HttpCodecCallback for PauseAfterFirstComplete {
    fn on_event(&mut self, _event: CodecEvent<'_>) {}

    fn on_ingress_event(&mut self, event: CodecEvent<'_>) -> CodecCallbackActions {
        if matches!(event, CodecEvent::MessageComplete { .. }) {
            self.completions += 1;
            if self.completions == 1 {
                return vec![CodecCallbackAction::SetParserPaused(true)].into();
            }
        }
        CodecCallbackActions::new()
    }
}

struct GenerateOnHeaders {
    log: Rc<RefCell<Vec<String>>>,
}

impl HttpCodecCallback for GenerateOnHeaders {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        self.log
            .borrow_mut()
            .push(format!("callback:{}", event_name(&event)));
    }

    fn on_ingress_event(&mut self, event: CodecEvent<'_>) -> CodecCallbackActions {
        self.log
            .borrow_mut()
            .push(format!("callback:{}", event_name(&event)));
        if matches!(event, CodecEvent::HeadersComplete { .. }) {
            return vec![
                CodecCallbackAction::SetParserPaused(true),
                CodecCallbackAction::Generate(OwnedEgressFrame::Headers {
                    stream: StreamId::new(1),
                    message: Box::new(HttpMessage::new()),
                    end_stream: false,
                    extra_headers: None,
                }),
                CodecCallbackAction::Generate(OwnedEgressFrame::EndOfMessage {
                    stream: StreamId::new(1),
                }),
                CodecCallbackAction::SetParserPaused(false),
            ]
            .into();
        }
        CodecCallbackActions::new()
    }
}

#[derive(Clone, Copy)]
enum ActionEdit {
    TransformHeaders(StreamId),
    SuppressEndOfMessage,
}

struct ActionEditingFilter {
    name: &'static str,
    edit: ActionEdit,
    log: Rc<RefCell<Vec<String>>>,
}

impl HttpCodecFilter for ActionEditingFilter {
    fn generate(
        &self,
        output: &mut BufQueue,
        frame: EgressFrame<'_>,
        callback: &mut dyn HttpCodecCallback,
        next: &mut dyn HttpCodecCall,
    ) -> Result<GeneratedFrame, HttpError> {
        match (self.edit, frame) {
            (
                ActionEdit::TransformHeaders(stream),
                EgressFrame::Headers {
                    message,
                    end_stream,
                    extra_headers,
                    ..
                },
            ) => next.generate(
                output,
                EgressFrame::Headers {
                    stream,
                    message,
                    end_stream,
                    extra_headers,
                },
                callback,
            ),
            (ActionEdit::SuppressEndOfMessage, EgressFrame::EndOfMessage { .. }) => {
                Ok(GeneratedFrame::default())
            }
            (_, frame) => next.generate(output, frame, callback),
        }
    }

    fn on_ingress_callback<'event>(
        &self,
        event: CodecEvent<'event>,
        next: &mut dyn FnMut(CodecEvent<'event>) -> CodecCallbackActions,
    ) -> CodecCallbackActions {
        self.log
            .borrow_mut()
            .push(format!("{}:callback:{}", self.name, event_name(&event)));
        let mut actions = next(event);
        if actions.is_empty() {
            return actions;
        }
        self.log.borrow_mut().push(format!("{}:actions", self.name));
        match self.edit {
            ActionEdit::TransformHeaders(transformed_stream) => {
                actions.iter_mut().for_each(|action| {
                    if let CodecCallbackAction::Generate(OwnedEgressFrame::Headers {
                        stream, ..
                    }) = action
                    {
                        *stream = transformed_stream;
                    }
                });
            }
            ActionEdit::SuppressEndOfMessage => actions.retain(|action| {
                !matches!(
                    action,
                    CodecCallbackAction::Generate(OwnedEgressFrame::EndOfMessage { .. })
                )
            }),
        }
        actions
    }
}

struct GenerateOnComplete {
    log: Rc<RefCell<Vec<String>>>,
}

impl HttpCodecCallback for GenerateOnComplete {
    fn on_event(&mut self, _event: CodecEvent<'_>) {
        self.log.borrow_mut().push("callback".to_owned());
    }

    fn on_ingress_event(&mut self, event: CodecEvent<'_>) -> CodecCallbackActions {
        self.log.borrow_mut().push("callback".to_owned());
        if matches!(event, CodecEvent::MessageComplete { .. }) {
            return vec![
                CodecCallbackAction::Generate(OwnedEgressFrame::Headers {
                    stream: StreamId::new(1),
                    message: Box::new(HttpMessage::new()),
                    end_stream: false,
                    extra_headers: None,
                }),
                CodecCallbackAction::Generate(OwnedEgressFrame::EndOfMessage {
                    stream: StreamId::new(1),
                }),
            ]
            .into();
        }
        CodecCallbackActions::new()
    }
}

struct GenerateOnBegin;

impl HttpCodecCallback for GenerateOnBegin {
    fn on_event(&mut self, _event: CodecEvent<'_>) {}

    fn on_ingress_event(&mut self, event: CodecEvent<'_>) -> CodecCallbackActions {
        if matches!(event, CodecEvent::MessageBegin { .. }) {
            vec![CodecCallbackAction::Generate(OwnedEgressFrame::Headers {
                stream: StreamId::new(1),
                message: Box::new(HttpMessage::new()),
                end_stream: false,
                extra_headers: None,
            })]
            .into()
        } else {
            CodecCallbackActions::new()
        }
    }
}

#[derive(Default)]
struct FramingObserver {
    framing_events: usize,
    ingress_events: usize,
}

impl HttpCodecCallback for FramingObserver {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        if matches!(event, CodecEvent::GeneratedFrameHeader { .. }) {
            self.framing_events += 1;
        }
    }

    fn on_ingress_event(&mut self, _event: CodecEvent<'_>) -> CodecCallbackActions {
        self.ingress_events += 1;
        vec![CodecCallbackAction::SetParserPaused(true)].into()
    }
}

struct DropProbe(Rc<Cell<u32>>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

#[test]
fn future_multiplexed_stream_and_control_seam() {
    let mut transport = MockMultiplexedTransport::default();

    assert_eq!(
        transport.open_request_stream().expect("request stream"),
        RequestStream(1),
    );
    assert_eq!(
        transport.open_request_stream().expect("request stream"),
        RequestStream(2),
    );
    assert_eq!(
        transport
            .open_unidirectional_control_stream()
            .expect("control stream"),
        ControlStream(1),
    );
}

#[test]
fn spike_reports_exact_consumed_bytes() {
    let mut codec = SpikeCodec::default();
    let mut callback = EventNames::default();
    let mut callback_egress = BufQueue::new(0);

    assert_eq!(
        HttpCodec::on_ingress(
            &mut codec,
            &queue(&[3, b'a']),
            &mut callback,
            &mut callback_egress,
        )
        .consumed,
        2,
    );
    assert!(HttpCodec::is_busy(&codec));
    assert!(callback.names.is_empty());
    assert_eq!(
        HttpCodec::on_ingress(
            &mut codec,
            &queue(b"bc!"),
            &mut callback,
            &mut callback_egress,
        )
        .consumed,
        2,
    );
    assert!(!HttpCodec::is_busy(&codec));
    assert_eq!(callback.names, ["begin", "headers", "body", "complete"]);
}

#[test]
fn callback_pause_preserves_the_exact_pipelined_remainder() {
    let mut codec = SpikeCodec {
        pipelined: true,
        ..SpikeCodec::default()
    };
    let mut callback = PauseAfterFirstComplete { completions: 0 };
    let mut callback_egress = BufQueue::new(0);
    let mut input = queue(&[0, 0]);

    let first = HttpCodec::on_ingress(&mut codec, &input, &mut callback, &mut callback_egress);

    assert_eq!(first.consumed, 1);
    assert!(first.egress_results.is_empty());
    assert!(HttpCodec::is_parser_paused(&codec));
    assert_eq!(callback.completions, 1);

    HttpCodec::set_parser_paused(&mut codec, false);
    input
        .trim_start(first.consumed)
        .expect("the reported prefix belongs to the input");
    let second = HttpCodec::on_ingress(&mut codec, &input, &mut callback, &mut callback_egress);

    assert_eq!(second.consumed, 1);
    assert!(second.egress_results.is_empty());
    assert_eq!(callback.completions, 2);
    assert!(!HttpCodec::is_busy(&codec));
}

#[test]
fn segmented_ingress_reports_exact_consumption() {
    let mut codec = SpikeCodec {
        pipelined: true,
        ..SpikeCodec::default()
    };
    let mut callback = PauseAfterFirstComplete { completions: 0 };
    let mut callback_egress = BufQueue::new(0);
    let mut input = BufQueue::new(2);
    input.push_copy(&[0]).expect("first segment fits");
    input.push_copy(&[0]).expect("second segment fits");

    let result = HttpCodec::on_ingress(&mut codec, &input, &mut callback, &mut callback_egress);

    assert_eq!(result.consumed, 1);
    assert_eq!(callback.completions, 1);
    assert_eq!(input.chain_length(), 2);
    assert_eq!(input.segment_count(), 2);
}

#[test]
fn callback_egress_actions_run_in_order_before_the_next_event() {
    let log = Rc::new(RefCell::new(Vec::new()));
    let mut codec = SpikeCodec {
        action_log: Some(Rc::clone(&log)),
        ..SpikeCodec::default()
    };
    let mut callback = GenerateOnHeaders {
        log: Rc::clone(&log),
    };
    let mut callback_egress = BufQueue::new(0);

    let result = HttpCodec::on_ingress(
        &mut codec,
        &queue(&[0]),
        &mut callback,
        &mut callback_egress,
    );

    assert_eq!(result.consumed, 1);
    assert_eq!(result.egress_results.len(), 2);
    assert_eq!(
        result
            .egress_results
            .into_iter()
            .map(|result| result.expect("spike generation succeeds").payload_offset)
            .collect::<Vec<_>>(),
        [Some(1), Some(101)],
    );
    assert_eq!(
        *log.borrow(),
        [
            "callback:begin",
            "callback:headers",
            "pause:true",
            "generate:headers:1",
            "generate:eom:1",
            "pause:false",
            "callback:body",
            "callback:complete",
        ],
    );
}

#[test]
fn framing_callbacks_are_observation_only() {
    let mut codec = SpikeCodec {
        emit_generated_callback: true,
        ..SpikeCodec::default()
    };
    let mut callback = FramingObserver::default();
    let mut output = BufQueue::new(0);

    HttpCodec::generate(
        &mut codec,
        &mut output,
        EgressFrame::ConnectionPreface,
        &mut callback,
    )
    .expect("spike framing succeeds");

    assert_eq!(callback.framing_events, 1);
    assert_eq!(callback.ingress_events, 0);
    assert!(!HttpCodec::is_parser_paused(&codec));
}

#[test]
fn two_filters_apply_equivalent_direct_and_callback_egress_policy() {
    let log = Rc::new(RefCell::new(Vec::new()));
    let codec = SpikeCodec {
        action_log: Some(Rc::clone(&log)),
        ..SpikeCodec::default()
    };
    let mut chain = HttpCodecFilterChain::new(codec);
    chain.add_filter(ActionEditingFilter {
        name: "first",
        edit: ActionEdit::TransformHeaders(StreamId::new(9)),
        log: Rc::clone(&log),
    });
    chain.add_filter(ActionEditingFilter {
        name: "second",
        edit: ActionEdit::SuppressEndOfMessage,
        log: Rc::clone(&log),
    });
    let mut callback = GenerateOnComplete {
        log: Rc::clone(&log),
    };
    let mut callback_egress = BufQueue::new(0);
    let direct_message = HttpMessage::new();

    let direct_header = HttpCodec::generate(
        &mut chain,
        &mut callback_egress,
        EgressFrame::Headers {
            stream: StreamId::new(1),
            message: &direct_message,
            end_stream: false,
            extra_headers: None,
        },
        &mut callback,
    )
    .expect("transformed direct header generation succeeds");
    let direct_eom = HttpCodec::generate(
        &mut chain,
        &mut callback_egress,
        EgressFrame::EndOfMessage {
            stream: StreamId::new(1),
        },
        &mut callback,
    )
    .expect("suppressed direct EOM generation succeeds");

    assert_eq!(direct_header.payload_offset, Some(9));
    assert_eq!(direct_eom, GeneratedFrame::default());
    log.borrow_mut().clear();

    let result = HttpCodec::on_ingress(
        &mut chain,
        &queue(&[0]),
        &mut callback,
        &mut callback_egress,
    );

    assert_eq!(result.consumed, 1);
    assert_eq!(result.egress_results.len(), 1);
    assert_eq!(
        result.egress_results[0]
            .as_ref()
            .expect("transformed header generation succeeds")
            .payload_offset,
        Some(9),
    );
    assert_eq!(
        &log.borrow()[9..],
        [
            "second:callback:complete",
            "first:callback:complete",
            "callback",
            "first:actions",
            "second:actions",
            "generate:headers:9",
        ],
    );
}

#[test]
fn synthesized_callback_actions_traverse_only_the_upstream_prefix() {
    let log = Rc::new(RefCell::new(Vec::new()));
    let codec = SpikeCodec {
        action_log: Some(Rc::clone(&log)),
        ..SpikeCodec::default()
    };
    let mut chain = HttpCodecFilterChain::new(codec);
    chain.add_filter(ActionEditingFilter {
        name: "first",
        edit: ActionEdit::TransformHeaders(StreamId::new(9)),
        log: Rc::clone(&log),
    });
    chain.add_filter(SynthesizingInnerFilter {
        log: Rc::clone(&log),
    });
    let mut callback = GenerateOnBegin;
    let mut callback_egress = BufQueue::new(0);

    let result = HttpCodec::on_ingress(
        &mut chain,
        &queue(&[0]),
        &mut callback,
        &mut callback_egress,
    );

    assert_eq!(result.consumed, 0);
    assert_eq!(result.egress_results.len(), 1);
    assert_eq!(
        *log.borrow(),
        [
            "call:second",
            "first:callback:begin",
            "first:actions",
            "generate:headers:9",
        ],
    );
}

#[test]
fn spike_preserves_callback_order() {
    let log = Rc::new(RefCell::new(Vec::new()));
    let mut chain = HttpCodecFilterChain::new(SpikeCodec::default());
    chain.add_filter(RecordingFilter {
        name: "first",
        log: Rc::clone(&log),
    });
    chain.add_filter(RecordingFilter {
        name: "second",
        log: Rc::clone(&log),
    });
    let mut callback = RecordingCallback {
        log: Rc::clone(&log),
    };
    let mut callback_egress = BufQueue::new(0);

    assert_eq!(
        HttpCodec::on_ingress(
            &mut chain,
            &queue(&[0]),
            &mut callback,
            &mut callback_egress,
        )
        .consumed,
        1,
    );
    assert_eq!(
        *log.borrow(),
        [
            "call:first",
            "call:second",
            "second:begin",
            "first:begin",
            "callback:begin",
            "second:headers",
            "first:headers",
            "callback:headers",
            "second:body",
            "first:body",
            "callback:body",
            "second:complete",
            "first:complete",
            "callback:complete",
        ],
    );
}

#[test]
fn filters_can_suppress_and_transform_codec_calls() {
    let mut chain = HttpCodecFilterChain::new(SpikeCodec::default());
    chain.add_filter(CallControlFilter);
    let mut callback = EventNames::default();
    let mut callback_egress = BufQueue::new(0);

    assert_eq!(HttpCodec::create_stream(&mut chain), StreamId::new(99));
    assert_eq!(
        HttpCodec::create_stream(chain.destination_mut()),
        StreamId::new(1)
    );

    HttpCodec::set_parser_paused(&mut chain, true);
    assert!(!HttpCodec::is_parser_paused(chain.destination()));

    assert_eq!(
        HttpCodec::on_ingress(
            &mut chain,
            &queue(&[0xaa]),
            &mut callback,
            &mut callback_egress,
        )
        .consumed,
        0,
    );
    assert!(callback.names.is_empty());
    assert_eq!(
        HttpCodec::on_ingress(
            &mut chain,
            &queue(&[8, 9]),
            &mut callback,
            &mut callback_egress,
        )
        .consumed,
        1,
    );
    assert_eq!(callback.names, ["begin", "headers", "body", "complete"]);

    let generated = HttpCodec::generate(
        &mut chain,
        &mut BufQueue::new(0),
        EgressFrame::ConnectionPreface,
        &mut callback,
    )
    .expect("filter-generated result");
    assert_eq!(
        generated,
        GeneratedFrame {
            bytes_written: 42,
            header_size: None,
            payload_offset: Some(7),
        },
    );
}

#[test]
fn filters_can_suppress_and_transform_callbacks() {
    let mut chain = HttpCodecFilterChain::new(SpikeCodec::default());
    chain.add_filter(CallbackControlFilter);
    let mut callback = EventNames::default();
    let mut callback_egress = BufQueue::new(0);

    assert_eq!(
        HttpCodec::on_ingress(
            &mut chain,
            &queue(&[0]),
            &mut callback,
            &mut callback_egress,
        )
        .consumed,
        1,
    );
    assert_eq!(callback.names, ["begin", "headers", "complete-upgraded"]);
}

#[test]
fn inner_synthesized_callback_traverses_only_upstream_prefix() {
    let log = Rc::new(RefCell::new(Vec::new()));
    let mut chain = HttpCodecFilterChain::new(SpikeCodec::default());
    chain.add_filter(RecordingFilter {
        name: "first",
        log: Rc::clone(&log),
    });
    chain.add_filter(SynthesizingInnerFilter {
        log: Rc::clone(&log),
    });
    let mut callback = RecordingCallback {
        log: Rc::clone(&log),
    };
    let mut callback_egress = BufQueue::new(0);

    assert_eq!(
        HttpCodec::on_ingress(
            &mut chain,
            &queue(&[3, b'a']),
            &mut callback,
            &mut callback_egress,
        )
        .consumed,
        0
    );
    assert!(!HttpCodec::is_busy(chain.destination()));
    assert_eq!(
        *log.borrow(),
        ["call:first", "call:second", "first:begin", "callback:begin",],
    );
}

#[test]
fn filters_intercept_and_transform_every_codec_query() {
    let mut chain = HttpCodecFilterChain::new(SpikeCodec::default());
    chain.add_filter(QueryTransformFilter);

    assert_eq!(HttpCodec::protocol(&chain), CodecProtocol::Http2);
    assert_eq!(
        HttpCodec::transport_direction(&chain),
        TransportDirection::Upstream,
    );
    assert_eq!(
        HttpCodec::capabilities(&chain),
        CodecCapabilities {
            stream_flow_control: true,
            default_window_size: 99,
            ..CodecCapabilities::default()
        },
    );
    assert!(HttpCodec::is_busy(&chain));
    assert!(HttpCodec::is_parser_paused(&chain));
    assert!(!HttpCodec::is_reusable(&chain));
    assert!(HttpCodec::is_waiting_to_drain(&chain));
    assert!(HttpCodec::close_on_egress_complete(&chain));
    assert_eq!(
        HttpCodec::last_incoming_stream_id(&chain),
        StreamId::new(11)
    );
}

#[test]
fn spike_preserves_error_direction_and_code() {
    let mut codec = SpikeCodec::default();
    let mut callback = ErrorCapture { seen: None };
    let mut callback_egress = BufQueue::new(0);

    for (marker, direction) in [
        (u8::MAX, HttpExceptionDirection::Ingress),
        (0xfe, HttpExceptionDirection::Egress),
        (0xfd, HttpExceptionDirection::IngressAndEgress),
    ] {
        assert_eq!(
            HttpCodec::on_ingress(
                &mut codec,
                &queue(&[marker, b'!']),
                &mut callback,
                &mut callback_egress,
            )
            .consumed,
            1,
        );
        assert_eq!(
            callback.seen.take(),
            Some((direction, Some(H2ErrorCode::PROTOCOL_ERROR), true)),
        );
    }
}

#[test]
fn spike_cannot_own_session_or_source() {
    let session_drops = Rc::new(Cell::new(0));
    let source_drops = Rc::new(Cell::new(0));
    let session = Rc::new(DropProbe(Rc::clone(&session_drops)));
    let source = Rc::new(DropProbe(Rc::clone(&source_drops)));
    let session_weak = Rc::downgrade(&session);
    let source_weak = Rc::downgrade(&source);
    let mut codec: Box<dyn HttpCodec> = Box::new(SpikeCodec::default());

    drop(session);
    drop(source);

    assert!(session_weak.upgrade().is_none());
    assert!(source_weak.upgrade().is_none());
    assert_eq!(session_drops.get(), 1);
    assert_eq!(source_drops.get(), 1);
    assert_eq!(HttpCodec::create_stream(codec.as_mut()), StreamId::new(1));
}

#[test]
fn an_emptied_action_list_yields_a_result_without_a_buffer() {
    let mut codec = SpikeCodec::default();
    let mut callback = EventNames::default();
    let mut callback_egress = BufQueue::new(0);
    // A filter that rejects every action leaves an empty list that keeps its
    // buffer.
    let mut actions = CodecCallbackActions::from(vec![
        CodecCallbackAction::SetParserPaused(true),
        CodecCallbackAction::Generate(OwnedEgressFrame::ConnectionPreface),
    ]);
    actions.retain(|_| false);

    let result =
        HttpCodec::apply_callback_actions(&mut codec, actions, &mut callback, &mut callback_egress);

    assert!(result.egress_results.is_empty());
    assert_eq!(
        result.egress_results.capacity(),
        0,
        "an empty action list yields a result without a heap buffer"
    );
    assert!(!HttpCodec::is_parser_paused(&codec));
    assert!(callback_egress.is_empty());
    assert!(callback.names.is_empty());
}

fn written(bytes_written: usize) -> Result<GeneratedFrame, HttpError> {
    Ok(GeneratedFrame {
        bytes_written,
        ..GeneratedFrame::default()
    })
}

fn bytes_written(result: &CodecIngressResult) -> Vec<usize> {
    result
        .egress_results
        .iter()
        .map(|generated| generated.as_ref().expect("written").bytes_written)
        .collect()
}

#[test]
fn empty_callback_results_leave_the_ingress_results_unchanged() {
    let mut result = CodecIngressResult::new(3);

    result.append_callback_result(CodecCallbackResult::default());
    assert!(result.egress_results.is_empty());
    assert_eq!(result.egress_results.capacity(), 0);

    result.append_callback_result(CodecCallbackResult {
        egress_results: vec![written(1)],
    });
    let capacity = result.egress_results.capacity();
    result.append_callback_result(CodecCallbackResult::default());
    result.append_callback_result(CodecCallbackResult {
        egress_results: Vec::with_capacity(4),
    });
    assert_eq!(bytes_written(&result), [1]);
    assert_eq!(result.egress_results.capacity(), capacity);

    result.append_callback_result(CodecCallbackResult {
        egress_results: vec![written(2), written(3)],
    });
    assert_eq!(result.consumed, 3);
    assert_eq!(
        bytes_written(&result),
        [1, 2, 3],
        "later results follow earlier ones"
    );
}
