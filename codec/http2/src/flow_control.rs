/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::RefCell;
use std::rc::Rc;

use roxygen_codec::CodecCallbackActions;
use roxygen_codec::CodecCallbackGenerateInterceptor;
use roxygen_codec::CodecCallbackResult;
use roxygen_codec::CodecCapabilities;
use roxygen_codec::CodecEvent;
use roxygen_codec::CodecIngressResult;
use roxygen_codec::EgressFrame;
use roxygen_codec::GeneratedFrame;
use roxygen_codec::H2ErrorCode;
use roxygen_codec::HttpCodec;
use roxygen_codec::HttpCodecCallback;
use roxygen_codec::HttpException;
use roxygen_codec::HttpExceptionDirection;
use roxygen_codec::OwnedEgressFrame;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::flow_control::Window;
use roxygen_core::http::CodecProtocol;
use roxygen_core::http::TransportDirection;
use roxygen_core::ids::StreamId;

/// Notifications emitted when the connection send window changes usability.
pub trait ConnectionFlowControlCallback {
    /// The connection send window changed from blocked to usable.
    fn on_connection_send_window_open(&self);

    /// The connection send window became blocked.
    fn on_connection_send_window_closed(&self);
}

#[derive(Clone, Copy, Debug)]
struct FlowControlState {
    receive_window: Window,
    send_window: Window,
    to_acknowledge: i32,
    error: bool,
    sends_blocked: bool,
}

/// An owning HTTP/2 codec wrapper with connection-level flow control.
///
/// Window updates, ingress callbacks and egress frames all go through the
/// wrapped codec. An outer [`roxygen_codec::HttpCodecFilterChain`] can wrap
/// this value so that synthesized errors take the same callback path and keep
/// their callback actions.
pub struct FlowControlledCodec {
    codec: Box<dyn HttpCodec>,
    state: Rc<RefCell<FlowControlState>>,
    notification: Rc<dyn ConnectionFlowControlCallback>,
}

impl FlowControlledCodec {
    /// Wraps `codec` and emits the initial connection window update when
    /// `receive_capacity` exceeds the codec default.
    pub fn new(
        codec: Box<dyn HttpCodec>,
        notification: Rc<dyn ConnectionFlowControlCallback>,
        output: &mut BufQueue,
        callback: &mut dyn HttpCodecCallback,
        receive_capacity: u32,
    ) -> Result<Self, HttpError> {
        let default_window = HttpCodec::capabilities(codec.as_ref()).default_window_size;
        let receive_window = Window::new(default_window)
            .expect("codec default connection window must fit the signed HTTP/2 range");
        let send_window = Window::new(default_window)
            .expect("codec default connection window must fit the signed HTTP/2 range");
        let mut controlled = Self {
            codec,
            state: Rc::new(RefCell::new(FlowControlState {
                receive_window,
                send_window,
                to_acknowledge: 0,
                error: false,
                sends_blocked: false,
            })),
            notification,
        };

        if receive_capacity > default_window {
            let delta = receive_capacity - default_window;
            assert!(
                controlled
                    .state
                    .borrow_mut()
                    .receive_window
                    .set_capacity(receive_capacity),
                "configured connection receive window must fit the signed HTTP/2 range"
            );
            controlled.generate_connection_window_update(output, callback, delta)?;
        }
        Ok(controlled)
    }

    /// Grows the advertised connection receive window.
    ///
    /// Shrinks and values outside the signed HTTP/2 range are ignored. A
    /// failure to serialize the update leaves the committed accounting intact
    /// and makes the wrapper non-reusable.
    pub fn set_receive_window_size(
        &mut self,
        output: &mut BufQueue,
        capacity: u32,
        callback: &mut dyn HttpCodecCallback,
    ) -> Result<(), HttpError> {
        let mut state = self.state.borrow_mut();
        if capacity < state.receive_window.capacity() {
            return Ok(());
        }

        let delta = capacity.wrapping_sub(state.receive_window.capacity()) as i32;
        if delta < 0 || !state.receive_window.set_capacity(capacity) {
            return Ok(());
        }
        state.to_acknowledge = state.to_acknowledge.wrapping_add(delta);
        if state.to_acknowledge <= 0 {
            return Ok(());
        }
        state.to_acknowledge = 0;
        drop(state);

        if let Err(error) = self.generate_connection_window_update(output, callback, delta as u32) {
            self.state.borrow_mut().error = true;
            return Err(error);
        }
        Ok(())
    }

    /// Returns processed connection credit and emits a coalesced update only
    /// after the pending amount is strictly greater than half the capacity.
    /// A serialization failure is terminal for connection reuse.
    pub fn ingress_bytes_processed(
        &mut self,
        output: &mut BufQueue,
        delta: u32,
        callback: &mut dyn HttpCodecCallback,
    ) -> Result<bool, HttpError> {
        let mut state = self.state.borrow_mut();
        state.to_acknowledge = state.to_acknowledge.wrapping_add(delta as i32);
        let will_ack = state.to_acknowledge > 0
            && state.to_acknowledge as u32 > state.receive_window.capacity() / 2;
        if !will_ack {
            return Ok(false);
        }

        let update = state.to_acknowledge as u32;
        if !state.receive_window.free(update) {
            state.error = true;
            return Err(Self::unrepresentable_credit(update));
        }
        state.to_acknowledge = 0;
        drop(state);
        if let Err(error) = self.generate_connection_window_update(output, callback, update) {
            self.state.borrow_mut().error = true;
            return Err(error);
        }
        Ok(true)
    }

    /// How much more credit can be returned without emitting a connection
    /// update: `ingress_bytes_processed(delta)` emits iff `delta` exceeds this
    /// value. Negative once received padding alone exceeds half the capacity.
    pub fn ingress_credit_headroom(&self) -> i64 {
        let state = self.state.borrow();
        i64::from(state.receive_window.capacity() / 2) - i64::from(state.to_acknowledge)
    }

    /// Currently available connection send credit, clamped to zero.
    pub fn available_send(&self) -> u32 {
        self.state.borrow().send_window.non_negative_size()
    }

    /// Current connection send-window accounting.
    pub fn send_window(&self) -> Window {
        self.state.borrow().send_window
    }

    /// Current connection receive-window accounting.
    pub fn receive_window(&self) -> Window {
        self.state.borrow().receive_window
    }

    /// Whether this wrapper has observed a terminal flow-control or downstream
    /// generation failure.
    pub fn has_error(&self) -> bool {
        self.state.borrow().error
    }

    fn generate_connection_window_update(
        &mut self,
        output: &mut BufQueue,
        callback: &mut dyn HttpCodecCallback,
        amount: u32,
    ) -> Result<GeneratedFrame, HttpError> {
        HttpCodec::generate(
            self.codec.as_mut(),
            output,
            EgressFrame::WindowUpdate {
                stream: StreamId::UNSPECIFIED,
                amount,
            },
            callback,
        )
    }

    /// Returned credit beyond what the window can represent means some
    /// caller returned bytes that were never reserved.
    #[cold]
    #[inline(never)]
    fn unrepresentable_credit(update: u32) -> HttpError {
        HttpError::with_message(
            HttpErrorCode::FLOW_CONTROL_ERROR,
            format!("processed connection credit {update} exceeds the receive window"),
        )
    }

    fn flow_control_error(message: String) -> HttpException {
        let mut error = HttpException::new(HttpExceptionDirection::IngressAndEgress, message);
        error.set_codec_status_code(H2ErrorCode::FLOW_CONTROL_ERROR);
        error
    }

    fn reserve_egress_body(
        state: &Rc<RefCell<FlowControlState>>,
        notification: &dyn ConnectionFlowControlCallback,
        amount: u32,
    ) {
        let closed = {
            let mut state = state.borrow_mut();
            assert!(
                state.send_window.reserve(amount, true),
                "session-level send window underflowed"
            );
            let closed = state.send_window.non_negative_size() == 0;
            if closed {
                state.sends_blocked = true;
            }
            closed
        };
        if closed {
            notification.on_connection_send_window_closed();
        }
    }
}

struct FlowControlGenerateInterceptor {
    state: Rc<RefCell<FlowControlState>>,
    notification: Rc<dyn ConnectionFlowControlCallback>,
    body_actions: RefCell<Vec<bool>>,
}

impl CodecCallbackGenerateInterceptor for FlowControlGenerateInterceptor {
    fn before_generate(&self, frame: &OwnedEgressFrame) -> Result<(), HttpError> {
        let is_body = match frame {
            OwnedEgressFrame::Body { body, padding, .. } => {
                let amount =
                    (body.chain_length() as u32).wrapping_add(u32::from(padding.unwrap_or(0)));
                FlowControlledCodec::reserve_egress_body(
                    &self.state,
                    self.notification.as_ref(),
                    amount,
                );
                true
            }
            OwnedEgressFrame::WindowUpdate { stream, .. } => {
                assert!(
                    !stream.is_unspecified(),
                    "connection receive window is managed by FlowControlledCodec"
                );
                false
            }
            _ => false,
        };
        self.body_actions.borrow_mut().push(is_body);
        Ok(())
    }

    fn after_generate(&self, result: &Result<GeneratedFrame, HttpError>) {
        let is_body = self.body_actions.borrow_mut().pop().unwrap_or(false);
        if is_body && result.is_err() {
            self.state.borrow_mut().error = true;
        }
    }
}

struct FlowControlledCallback<'a> {
    state: Rc<RefCell<FlowControlState>>,
    notification: Rc<dyn ConnectionFlowControlCallback>,
    callback: &'a mut dyn HttpCodecCallback,
}

impl FlowControlledCallback<'_> {
    #[inline(always)]
    fn intercept_actions(&self, actions: &mut CodecCallbackActions) {
        if actions.is_empty() {
            return;
        }
        self.intercept_nonempty_actions(actions);
    }

    #[cold]
    #[inline(never)]
    fn intercept_nonempty_actions(&self, actions: &mut CodecCallbackActions) {
        actions.intercept_generate(Rc::new(FlowControlGenerateInterceptor {
            state: Rc::clone(&self.state),
            notification: Rc::clone(&self.notification),
            body_actions: RefCell::new(Vec::new()),
        }));
    }

    fn process_body(
        &mut self,
        stream: StreamId,
        body: BufQueue,
        padding: u16,
        ingress: bool,
    ) -> CodecCallbackActions {
        let amount = body.chain_length();
        let total = (amount as u32).wrapping_add(u32::from(padding));
        let reserve_error = {
            let mut state = self.state.borrow_mut();
            if state.receive_window.reserve(total, true) {
                // Padding is freed with the window update that returns it to the
                // peer; freeing it here too would credit it twice.
                state.to_acknowledge = state.to_acknowledge.wrapping_add(i32::from(padding));
                None
            } else {
                state.error = true;
                Some(state.receive_window.size())
            }
        };
        if let Some(window_size) = reserve_error {
            let error = FlowControlledCodec::flow_control_error(format!(
                "Failed to reserve receive window, window size={}, amount={amount}",
                window_size
            ));
            if ingress {
                let mut actions = self.callback.on_ingress_event(CodecEvent::Error {
                    stream: StreamId::UNSPECIFIED,
                    error: &error,
                    new_stream: false,
                });
                self.intercept_actions(&mut actions);
                return actions;
            }
            self.callback.on_event(CodecEvent::Error {
                stream: StreamId::UNSPECIFIED,
                error: &error,
                new_stream: false,
            });
            return CodecCallbackActions::new();
        }

        if ingress {
            let mut actions = self.callback.on_ingress_event(CodecEvent::Body {
                stream,
                body,
                padding,
            });
            self.intercept_actions(&mut actions);
            actions
        } else {
            self.callback.on_event(CodecEvent::Body {
                stream,
                body,
                padding,
            });
            CodecCallbackActions::new()
        }
    }

    fn process_window_update(
        &mut self,
        stream: StreamId,
        amount: u32,
        ingress: bool,
    ) -> CodecCallbackActions {
        if !stream.is_unspecified() {
            if ingress {
                let mut actions = self
                    .callback
                    .on_ingress_event(CodecEvent::WindowUpdate { stream, amount });
                self.intercept_actions(&mut actions);
                return actions;
            }
            self.callback
                .on_event(CodecEvent::WindowUpdate { stream, amount });
            return CodecCallbackActions::new();
        }

        let (error, opened) = {
            let mut state = self.state.borrow_mut();
            let error = if state.send_window.free(amount) {
                None
            } else {
                state.error = true;
                Some(FlowControlledCodec::flow_control_error(format!(
                    "Failed to update send window, outstanding={}, amount={amount}",
                    state.send_window.outstanding()
                )))
            };
            let opened = state.sends_blocked && state.send_window.non_negative_size() != 0;
            if opened {
                state.sends_blocked = false;
            }
            (error, opened)
        };

        let mut actions = if let Some(error) = error {
            if ingress {
                self.callback.on_ingress_event(CodecEvent::Error {
                    stream,
                    error: &error,
                    new_stream: false,
                })
            } else {
                self.callback.on_event(CodecEvent::Error {
                    stream,
                    error: &error,
                    new_stream: false,
                });
                CodecCallbackActions::new()
            }
        } else {
            CodecCallbackActions::new()
        };
        if opened {
            self.notification.on_connection_send_window_open();
        }
        self.intercept_actions(&mut actions);
        actions
    }
}

impl HttpCodecCallback for FlowControlledCallback<'_> {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        match event {
            CodecEvent::Body {
                stream,
                body,
                padding,
            } => {
                let _ = self.process_body(stream, body, padding, false);
            }
            CodecEvent::WindowUpdate { stream, amount } => {
                let _ = self.process_window_update(stream, amount, false);
            }
            other => self.callback.on_event(other),
        }
    }

    fn on_ingress_event(&mut self, event: CodecEvent<'_>) -> CodecCallbackActions {
        match event {
            CodecEvent::Body {
                stream,
                body,
                padding,
            } => self.process_body(stream, body, padding, true),
            CodecEvent::WindowUpdate { stream, amount } => {
                self.process_window_update(stream, amount, true)
            }
            other => {
                let mut actions = self.callback.on_ingress_event(other);
                self.intercept_actions(&mut actions);
                actions
            }
        }
    }

    fn num_outgoing_streams(&self) -> u32 {
        self.callback.num_outgoing_streams()
    }

    fn num_incoming_streams(&self) -> u32 {
        self.callback.num_incoming_streams()
    }

    fn observes_frame_headers(&self) -> bool {
        self.callback.observes_frame_headers()
    }
}

impl HttpCodec for FlowControlledCodec {
    fn protocol(&self) -> CodecProtocol {
        HttpCodec::protocol(self.codec.as_ref())
    }

    fn transport_direction(&self) -> TransportDirection {
        HttpCodec::transport_direction(self.codec.as_ref())
    }

    fn capabilities(&self) -> CodecCapabilities {
        HttpCodec::capabilities(self.codec.as_ref())
    }

    fn create_stream(&mut self) -> StreamId {
        HttpCodec::create_stream(self.codec.as_mut())
    }

    fn is_busy(&self) -> bool {
        HttpCodec::is_busy(self.codec.as_ref())
    }

    fn has_partial_request_head(&self) -> bool {
        HttpCodec::has_partial_request_head(self.codec.as_ref())
    }

    fn set_parser_paused(&mut self, paused: bool) {
        HttpCodec::set_parser_paused(self.codec.as_mut(), paused);
    }

    fn is_parser_paused(&self) -> bool {
        HttpCodec::is_parser_paused(self.codec.as_ref())
    }

    fn on_ingress(
        &mut self,
        input: &BufQueue,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
    ) -> CodecIngressResult {
        let mut controlled_callback = FlowControlledCallback {
            state: Rc::clone(&self.state),
            notification: Rc::clone(&self.notification),
            callback,
        };
        HttpCodec::on_ingress(
            self.codec.as_mut(),
            input,
            &mut controlled_callback,
            callback_egress,
        )
    }

    fn on_ingress_eof(
        &mut self,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
    ) -> CodecCallbackResult {
        let mut controlled_callback = FlowControlledCallback {
            state: Rc::clone(&self.state),
            notification: Rc::clone(&self.notification),
            callback,
        };
        HttpCodec::on_ingress_eof(
            self.codec.as_mut(),
            &mut controlled_callback,
            callback_egress,
        )
    }

    fn is_reusable(&self) -> bool {
        !self.state.borrow().error && HttpCodec::is_reusable(self.codec.as_ref())
    }

    fn is_waiting_to_drain(&self) -> bool {
        HttpCodec::is_waiting_to_drain(self.codec.as_ref())
    }

    fn close_on_egress_complete(&self) -> bool {
        HttpCodec::close_on_egress_complete(self.codec.as_ref())
    }

    fn last_incoming_stream_id(&self) -> StreamId {
        HttpCodec::last_incoming_stream_id(self.codec.as_ref())
    }

    fn generate(
        &mut self,
        output: &mut BufQueue,
        frame: EgressFrame<'_>,
        callback: &mut dyn HttpCodecCallback,
    ) -> Result<GeneratedFrame, HttpError> {
        match frame {
            EgressFrame::Body {
                stream,
                body,
                padding,
                end_stream,
            } => {
                let amount =
                    (body.chain_length() as u32).wrapping_add(u32::from(padding.unwrap_or(0)));
                Self::reserve_egress_body(&self.state, self.notification.as_ref(), amount);
                let result = HttpCodec::generate(
                    self.codec.as_mut(),
                    output,
                    EgressFrame::Body {
                        stream,
                        body,
                        padding,
                        end_stream,
                    },
                    callback,
                );
                if result.is_err() {
                    self.state.borrow_mut().error = true;
                }
                result
            }
            EgressFrame::WindowUpdate { stream, amount } => {
                assert!(
                    !stream.is_unspecified(),
                    "connection receive window is managed by FlowControlledCodec"
                );
                HttpCodec::generate(
                    self.codec.as_mut(),
                    output,
                    EgressFrame::WindowUpdate { stream, amount },
                    callback,
                )
            }
            other => HttpCodec::generate(self.codec.as_mut(), output, other, callback),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;

    use bytes::Bytes;

    use super::*;

    const CAPACITY: u32 = 65_535;

    struct Quiet;

    impl ConnectionFlowControlCallback for Quiet {
        fn on_connection_send_window_open(&self) {}

        fn on_connection_send_window_closed(&self) {}
    }

    struct Ignore;

    impl HttpCodecCallback for Ignore {
        fn on_event(&mut self, _event: CodecEvent<'_>) {}
    }

    /// Delivers one DATA event of the scripted `(length, padding)` per `on_ingress`.
    struct DataCodec {
        next: Rc<Cell<(usize, u16)>>,
    }

    impl HttpCodec for DataCodec {
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
                push_transactions: false,
                default_window_size: CAPACITY,
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
            _callback_egress: &mut BufQueue,
        ) -> CodecIngressResult {
            let (length, padding) = self.next.get();
            let body = BufQueue::from_bytes(Bytes::from(vec![0; length]), length)
                .expect("scripted DATA fits its queue");
            drop(callback.on_ingress_event(CodecEvent::Body {
                stream: StreamId::new(1),
                body,
                padding,
            }));
            CodecIngressResult::new(input.chain_length())
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
            StreamId::UNSPECIFIED
        }

        fn generate(
            &mut self,
            _output: &mut BufQueue,
            _frame: EgressFrame<'_>,
            _callback: &mut dyn HttpCodecCallback,
        ) -> Result<GeneratedFrame, HttpError> {
            Ok(GeneratedFrame::default())
        }
    }

    /// Receives one DATA frame of `processed` body bytes and `padding` (the codec's callback
    /// value, which counts the pad-length byte), returns the body credit, then returns
    /// `delta` more. Yields the headroom read just before `delta` and whether `delta` emitted.
    fn headroom_then_emit(processed: usize, padding: u16, delta: u32) -> (i64, bool) {
        let next = Rc::new(Cell::new((processed, padding)));
        let mut output = BufQueue::new(1 << 16);
        let mut codec = FlowControlledCodec::new(
            Box::new(DataCodec {
                next: Rc::clone(&next),
            }),
            Rc::new(Quiet),
            &mut output,
            &mut Ignore,
            CAPACITY,
        )
        .expect("the wrapper builds");
        let input = BufQueue::from_bytes(Bytes::from_static(b"x"), 1).expect("one byte fits");
        let mut callback_egress = BufQueue::new(1 << 16);
        let _ = HttpCodec::on_ingress(&mut codec, &input, &mut Ignore, &mut callback_egress);
        assert!(
            !codec
                .ingress_bytes_processed(&mut output, processed as u32, &mut Ignore)
                .expect("body credit is representable"),
            "the priming credit stays below the threshold"
        );
        let headroom = codec.ingress_credit_headroom();
        let emitted = codec
            .ingress_bytes_processed(&mut output, delta, &mut Ignore)
            .expect("returned credit is representable");
        (headroom, emitted)
    }

    #[test]
    fn ingress_credit_headroom_predicts_emission() {
        // Half of 65,535 is 32,767. Pending credit is the processed body plus the padding
        // charged when the DATA frame arrived (255 bytes of padding + the pad-length byte).
        for (processed, padding, headroom) in [
            (0, 0, 32_767),
            (1_000, 0, 31_767),
            (0, 256, 32_511),
            (1_000, 256, 31_511),
            (0, 1, 32_766),
        ] {
            let delta_at = |delta: i64| u32::try_from(delta).expect("deltas stay non-negative");
            for delta in [0, 1, headroom - 1, headroom, headroom + 1, headroom + 2] {
                let (observed, emitted) = headroom_then_emit(processed, padding, delta_at(delta));
                assert_eq!(
                    observed, headroom,
                    "headroom after {processed} processed bytes and {padding} padding"
                );
                assert_eq!(
                    emitted,
                    delta > headroom,
                    "delta {delta} with headroom {headroom} (processed {processed}, padding {padding})"
                );
            }
        }
    }
}
