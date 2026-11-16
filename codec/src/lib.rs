use std::rc::Rc;

use roxygen_core::buf_queue::BufQueue;
use roxygen_core::error::HttpError;
use roxygen_core::headers::HttpHeaders;
use roxygen_core::http::CodecProtocol;
use roxygen_core::http::HttpHeaderSize;
use roxygen_core::http::TransportDirection;
use roxygen_core::ids::StreamId;
use roxygen_core::message::HttpMessage;
use roxygen_core::priority::HttpPriority;

pub mod error_adapter;

pub use error_adapter::H2ErrorCode;
pub use error_adapter::HttpException;
pub use error_adapter::HttpExceptionDirection;
pub use error_adapter::ProxygenError;

/// One protocol setting represented without exposing a parser dependency.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CodecSetting {
    /// Protocol-defined setting identifier.
    pub identifier: u16,
    /// Protocol-defined setting value.
    pub value: u64,
}

/// Static capabilities of an HTTP codec.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CodecCapabilities {
    /// Whether each stream has an independent flow-control window.
    pub stream_flow_control: bool,
    /// Whether the connection has a flow-control window.
    pub session_flow_control: bool,
    /// Whether more than one request can be active concurrently.
    pub parallel_requests: bool,
    /// Whether the peer can initiate pushed transactions.
    pub push_transactions: bool,
    /// Default receive-window size, or zero when flow control is unsupported.
    pub default_window_size: u32,
}

/// Metadata returned after one egress operation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GeneratedFrame {
    /// Exact number of bytes appended to the output queue.
    pub bytes_written: usize,
    /// Header compression accounting when the operation emitted headers.
    pub header_size: Option<HttpHeaderSize>,
    /// Offset of the first payload byte relative to this operation's output.
    pub payload_offset: Option<u64>,
}

/// A protocol-neutral egress operation understood by an HTTP/1.1 or HTTP/2
/// codec.
pub enum EgressFrame<'a> {
    /// Emit any connection preface required by the selected protocol.
    ConnectionPreface,
    /// Emit a request or response header block.
    Headers {
        /// Codec-local stream identifier.
        stream: StreamId,
        /// Header metadata to serialize.
        message: &'a HttpMessage,
        /// Whether this header block also ends the message.
        end_stream: bool,
        /// Additional header fields supplied by the caller.
        extra_headers: Option<&'a HttpHeaders>,
    },
    /// Emit a pushed response header block.
    PushPromise {
        /// Codec-local pushed-stream identifier.
        stream: StreamId,
        /// Stream that caused the push.
        associated_stream: StreamId,
        /// Header metadata to serialize.
        message: &'a HttpMessage,
        /// Whether the pushed message also ends with this block.
        end_stream: bool,
    },
    /// Emit body bytes, transferring the queue to the codec.
    Body {
        /// Codec-local stream identifier.
        stream: StreamId,
        /// Body segments in wire order.
        body: BufQueue,
        /// Optional protocol padding amount.
        padding: Option<u8>,
        /// Whether this body operation also ends the message.
        end_stream: bool,
    },
    /// Emit an HTTP/1.1 chunk header when applicable.
    ChunkHeader {
        /// Codec-local stream identifier.
        stream: StreamId,
        /// Announced chunk length.
        length: usize,
    },
    /// Emit an HTTP/1.1 chunk terminator when applicable.
    ChunkTerminator {
        /// Codec-local stream identifier.
        stream: StreamId,
    },
    /// Emit message trailers.
    Trailers {
        /// Codec-local stream identifier.
        stream: StreamId,
        /// Trailer fields to serialize.
        trailers: &'a HttpHeaders,
    },
    /// Emit protocol padding independent of a body operation.
    Padding {
        /// Codec-local stream identifier.
        stream: StreamId,
        /// Number of padding bytes.
        length: u16,
    },
    /// Emit protocol framing that completes the message.
    EndOfMessage {
        /// Codec-local stream identifier.
        stream: StreamId,
    },
    /// Abort one stream with its HTTP/2 wire code.
    Reset {
        /// Codec-local stream identifier.
        stream: StreamId,
        /// Wire reset code.
        code: H2ErrorCode,
    },
    /// Gracefully drain or abort the connection.
    Goaway {
        /// Last stream accepted from the peer.
        last_stream: StreamId,
        /// Wire connection error code.
        code: H2ErrorCode,
        /// Optional diagnostic bytes.
        debug_data: Option<BufQueue>,
    },
    /// Emit a ping request.
    PingRequest {
        /// Optional caller-provided ping payload.
        data: Option<u64>,
    },
    /// Emit a ping reply.
    PingReply {
        /// Ping payload received from the peer.
        data: u64,
    },
    /// Emit a settings frame.
    Settings {
        /// Settings in their original order.
        settings: &'a [CodecSetting],
    },
    /// Acknowledge peer settings.
    SettingsAck,
    /// Return receive-window credit.
    WindowUpdate {
        /// Stream to update, or the unspecified stream for connection credit.
        stream: StreamId,
        /// Positive window increment.
        amount: u32,
    },
    /// Emit a priority update for a request stream.
    Priority {
        /// Codec-local stream identifier.
        stream: StreamId,
        /// Priority fields to serialize.
        priority: HttpPriority,
    },
    /// Emit a priority update for a pushed stream.
    PushPriority {
        /// Codec-local pushed-stream identifier.
        stream: StreamId,
        /// Priority fields to serialize.
        priority: HttpPriority,
    },
}

/// An owned egress operation that a synchronous ingress callback can request.
///
/// Owned fields let the callback return before the codec applies the operation,
/// so applying it never requires reborrowing a codec captured by the callback.
pub enum OwnedEgressFrame {
    /// Emit any connection preface required by the selected protocol.
    ConnectionPreface,
    /// Emit a request or response header block.
    Headers {
        /// Codec-local stream identifier.
        stream: StreamId,
        /// Header metadata to serialize.
        message: Box<HttpMessage>,
        /// Whether this header block also ends the message.
        end_stream: bool,
        /// Additional header fields supplied by the caller.
        extra_headers: Option<Box<HttpHeaders>>,
    },
    /// Emit a pushed response header block.
    PushPromise {
        /// Codec-local pushed-stream identifier.
        stream: StreamId,
        /// Stream that caused the push.
        associated_stream: StreamId,
        /// Header metadata to serialize.
        message: Box<HttpMessage>,
        /// Whether the pushed message also ends with this block.
        end_stream: bool,
    },
    /// Emit body bytes, transferring the queue to the codec.
    Body {
        /// Codec-local stream identifier.
        stream: StreamId,
        /// Body segments in wire order.
        body: BufQueue,
        /// Optional protocol padding amount.
        padding: Option<u8>,
        /// Whether this body operation also ends the message.
        end_stream: bool,
    },
    /// Emit an HTTP/1.1 chunk header when applicable.
    ChunkHeader {
        /// Codec-local stream identifier.
        stream: StreamId,
        /// Announced chunk length.
        length: usize,
    },
    /// Emit an HTTP/1.1 chunk terminator when applicable.
    ChunkTerminator {
        /// Codec-local stream identifier.
        stream: StreamId,
    },
    /// Emit message trailers.
    Trailers {
        /// Codec-local stream identifier.
        stream: StreamId,
        /// Trailer fields to serialize.
        trailers: Box<HttpHeaders>,
    },
    /// Emit protocol padding independent of a body operation.
    Padding {
        /// Codec-local stream identifier.
        stream: StreamId,
        /// Number of padding bytes.
        length: u16,
    },
    /// Emit protocol framing that completes the message.
    EndOfMessage {
        /// Codec-local stream identifier.
        stream: StreamId,
    },
    /// Abort one stream with its HTTP/2 wire code.
    Reset {
        /// Codec-local stream identifier.
        stream: StreamId,
        /// Wire reset code.
        code: H2ErrorCode,
    },
    /// Gracefully drain or abort the connection.
    Goaway {
        /// Last stream accepted from the peer.
        last_stream: StreamId,
        /// Wire connection error code.
        code: H2ErrorCode,
        /// Optional diagnostic bytes.
        debug_data: Option<BufQueue>,
    },
    /// Emit a ping request.
    PingRequest {
        /// Optional caller-provided ping payload.
        data: Option<u64>,
    },
    /// Emit a ping reply.
    PingReply {
        /// Ping payload received from the peer.
        data: u64,
    },
    /// Emit a settings frame.
    Settings {
        /// Settings in their original order.
        settings: Box<[CodecSetting]>,
    },
    /// Acknowledge peer settings.
    SettingsAck,
    /// Return receive-window credit.
    WindowUpdate {
        /// Stream to update, or the unspecified stream for connection credit.
        stream: StreamId,
        /// Positive window increment.
        amount: u32,
    },
    /// Emit a priority update for a request stream.
    Priority {
        /// Codec-local stream identifier.
        stream: StreamId,
        /// Priority fields to serialize.
        priority: HttpPriority,
    },
    /// Emit a priority update for a pushed stream.
    PushPriority {
        /// Codec-local pushed-stream identifier.
        stream: StreamId,
        /// Priority fields to serialize.
        priority: HttpPriority,
    },
}

/// Hooks run immediately around one callback-requested generation operation.
///
/// This is a worker-local interception seam for filters whose state transition
/// must remain ordered with the generated frame. It deliberately exposes only
/// Roxygen-owned framing types.
pub trait CodecCallbackGenerateInterceptor {
    /// Runs immediately before the owned frame is generated.
    fn before_generate(&self, frame: &OwnedEgressFrame) -> Result<(), HttpError>;

    /// Runs immediately after generation, including when preparation or
    /// generation failed.
    fn after_generate(&self, result: &Result<GeneratedFrame, HttpError>);
}

/// One control request returned by a synchronous ingress callback.
pub enum CodecCallbackAction {
    /// Pause or resume ingress parsing before the parser continues.
    SetParserPaused(bool),
    /// Append one framed operation to the callback-egress queue.
    Generate(OwnedEgressFrame),
    /// Generate through worker-local interceptors installed by codec filters.
    #[doc(hidden)]
    InterceptedGenerate {
        frame: OwnedEgressFrame,
        interceptors: Vec<Rc<dyn CodecCallbackGenerateInterceptor>>,
    },
}

/// Ordered controls requested by one synchronous ingress callback.
///
/// The empty sequence is the allocation-free default for observation-only
/// callbacks.
#[derive(Default)]
#[must_use]
pub struct CodecCallbackActions(Vec<CodecCallbackAction>);

impl CodecCallbackActions {
    /// Creates an empty action sequence without allocating.
    pub const fn new() -> Self {
        Self(Vec::new())
    }

    /// Returns whether the callback requested no controls.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterates mutably so a filter can transform actions while they unwind.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut CodecCallbackAction> {
        self.0.iter_mut()
    }

    /// Removes actions rejected by a filter while preserving remaining order.
    pub fn retain(&mut self, keep: impl FnMut(&CodecCallbackAction) -> bool) {
        self.0.retain(keep);
    }

    /// Appends a later action sequence without changing action order.
    pub fn append(&mut self, mut later: Self) {
        self.0.append(&mut later.0);
    }

    /// Installs a hook around every generated action while preserving action
    /// order. Multiple filters are invoked in installation order before
    /// generation and reverse order afterward.
    pub fn intercept_generate(&mut self, interceptor: Rc<dyn CodecCallbackGenerateInterceptor>) {
        self.0.iter_mut().for_each(|action| match action {
            CodecCallbackAction::Generate(_) => {
                let previous =
                    std::mem::replace(action, CodecCallbackAction::SetParserPaused(false));
                let CodecCallbackAction::Generate(frame) = previous else {
                    unreachable!("matched generation action must remain a generation action");
                };
                *action = CodecCallbackAction::InterceptedGenerate {
                    frame,
                    interceptors: vec![Rc::clone(&interceptor)],
                };
            }
            CodecCallbackAction::InterceptedGenerate { interceptors, .. } => {
                interceptors.push(Rc::clone(&interceptor));
            }
            CodecCallbackAction::SetParserPaused(_) => {}
        });
    }
}

impl From<Vec<CodecCallbackAction>> for CodecCallbackActions {
    fn from(actions: Vec<CodecCallbackAction>) -> Self {
        Self(actions)
    }
}

impl IntoIterator for CodecCallbackActions {
    type Item = CodecCallbackAction;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Observable results of egress actions returned by one or more callbacks.
#[derive(Debug, Default)]
#[must_use]
pub struct CodecCallbackResult {
    /// Generation results in the same order as their callback actions.
    pub egress_results: Vec<Result<GeneratedFrame, HttpError>>,
}

impl CodecCallbackResult {
    /// Appends later callback results without changing their order.
    pub fn append(&mut self, mut later: Self) {
        self.egress_results.append(&mut later.egress_results);
    }
}

/// Result of parsing one ingress prefix and applying its callback actions.
#[derive(Debug, Default)]
#[must_use]
pub struct CodecIngressResult {
    /// Exact number of bytes consumed from the supplied ingress slice.
    pub consumed: usize,
    /// Generation results in callback and action order.
    pub egress_results: Vec<Result<GeneratedFrame, HttpError>>,
}

impl CodecIngressResult {
    /// Creates a result with no callback-triggered egress.
    pub fn new(consumed: usize) -> Self {
        Self {
            consumed,
            egress_results: Vec::new(),
        }
    }

    /// Appends results from the next callback emitted by this parse call.
    pub fn append_callback_result(&mut self, result: CodecCallbackResult) {
        if result.egress_results.is_empty() {
            return;
        }
        self.egress_results.extend(result.egress_results);
    }
}

/// One synchronous callback emitted while parsing or framing.
pub enum CodecEvent<'a> {
    /// Parsing for a new message began.
    MessageBegin {
        stream: StreamId,
        message: &'a mut HttpMessage,
    },
    /// Parsing for a pushed message began.
    PushMessageBegin {
        stream: StreamId,
        associated_stream: StreamId,
        /// Decoded request for an unsplit promise, or `None` while a
        /// CONTINUATION sequence is still incomplete.
        message: Option<&'a mut HttpMessage>,
    },
    /// A complete request or response header block was parsed.
    HeadersComplete {
        stream: StreamId,
        message: Box<HttpMessage>,
    },
    /// Body data was parsed after protocol framing was removed.
    Body {
        stream: StreamId,
        body: BufQueue,
        padding: u16,
    },
    /// An HTTP/1.1 chunk header was parsed.
    ChunkHeader { stream: StreamId, length: usize },
    /// An HTTP/1.1 chunk terminator was parsed.
    ChunkComplete { stream: StreamId },
    /// A non-empty trailer block was parsed.
    TrailersComplete {
        stream: StreamId,
        trailers: Box<HttpHeaders>,
    },
    /// Parsing for one message completed.
    MessageComplete { stream: StreamId, upgraded: bool },
    /// A parse or protocol error occurred.
    Error {
        stream: StreamId,
        error: &'a HttpException,
        new_stream: bool,
    },
    /// The peer immediately aborted one stream.
    Abort { stream: StreamId, code: H2ErrorCode },
    /// A framed protocol header was received.
    FrameHeader {
        stream: StreamId,
        flags: u8,
        length: u64,
        frame_type: u64,
        version: u16,
    },
    /// The peer initiated connection drain.
    Goaway {
        last_good_stream: StreamId,
        code: H2ErrorCode,
        debug_data: Option<BufQueue>,
    },
    /// The peer sent a ping request.
    PingRequest { data: u64 },
    /// The peer replied to a ping.
    PingReply { data: u64 },
    /// The peer increased a receive window.
    WindowUpdate { stream: StreamId, amount: u32 },
    /// The peer sent protocol settings.
    Settings { settings: Box<[CodecSetting]> },
    /// The peer acknowledged local settings.
    SettingsAck,
    /// The peer updated request-stream priority.
    Priority {
        stream: StreamId,
        priority: HttpPriority,
    },
    /// The peer updated pushed-stream priority.
    PushPriority {
        stream: StreamId,
        priority: HttpPriority,
    },
    /// A framed codec generated one frame header.
    GeneratedFrameHeader {
        stream: StreamId,
        frame_type: u8,
        length: u64,
        version: u16,
    },
}

/// Synchronous receiver for codec ingress and framing callbacks.
pub trait HttpCodecCallback {
    /// Receives one observation-only parsing or framing callback.
    fn on_event(&mut self, event: CodecEvent<'_>);

    /// Receives one ingress callback and returns controls to apply before
    /// parsing continues.
    ///
    /// Framing callbacks use [`HttpCodecCallback::on_event`] and cannot request
    /// actions because `generate` has no callback-action result channel.
    fn on_ingress_event(&mut self, event: CodecEvent<'_>) -> CodecCallbackActions {
        self.on_event(event);
        CodecCallbackActions::new()
    }

    /// Number of locally initiated streams currently open.
    fn num_outgoing_streams(&self) -> u32 {
        0
    }

    /// Number of peer-initiated streams currently open.
    fn num_incoming_streams(&self) -> u32 {
        0
    }

    /// The most body bytes the codec may deliver for `stream` before it asks again, or `None`
    /// for no bound. The codec reads zero as one, so that parsing progresses; a callback that
    /// can take no more body bytes pauses parsing instead.
    ///
    /// The HTTP/1.1 codec asks before it parses each run of a message body, before each body
    /// event of an upgraded tunnel or HTTP/0.9 response, and after each message head, bodiless
    /// or not, that more than one byte of its run follows. It counts a chunked run's framing
    /// against the limit too. A callback that pauses parsing at a buffered-body limit answers
    /// this to bound how far one body event can carry a stream past that limit (`DIV-058`);
    /// otherwise one event can carry a whole read buffer past it. Codec filters do not
    /// intercept the query. Other codecs do not ask. Roxygen-only.
    fn ingress_body_limit(&self, _stream: StreamId) -> Option<usize> {
        None
    }

    /// Whether this callback wants [`CodecEvent::FrameHeader`] events.
    ///
    /// A codec may skip delivering that observation-only event to a callback
    /// that returns `false`. Every other event, and all codec state, is
    /// unchanged. Roxygen-only; see `DIV-026` in
    /// `roxygen/migration/DIVERGENCES.md`.
    fn observes_frame_headers(&self) -> bool {
        true
    }
}

/// Runtime-neutral HTTP/1.1 or HTTP/2 parser and framer.
///
/// The trait object is intentionally not `Send`. Its methods accept no source,
/// session, task, or runtime ownership, leaving those responsibilities with the
/// worker-local session.
pub trait HttpCodec {
    /// Selected wire protocol.
    fn protocol(&self) -> CodecProtocol;

    /// Whether this codec consumes requests or responses on ingress.
    fn transport_direction(&self) -> TransportDirection;

    /// Static framing and flow-control capabilities.
    fn capabilities(&self) -> CodecCapabilities;

    /// Reserve the next codec-local stream identifier.
    fn create_stream(&mut self) -> StreamId;

    /// Whether the codec is tracking any incomplete messages.
    fn is_busy(&self) -> bool;

    /// Pause or resume ingress parsing.
    fn set_parser_paused(&mut self, paused: bool);

    /// Whether ingress parsing is paused.
    fn is_parser_paused(&self) -> bool;

    /// Parse a prefix of segmented `input` and return its exact consumed byte
    /// count and callback-triggered egress results.
    ///
    /// Every callback caused by the consumed prefix is delivered synchronously
    /// before this method returns. Its actions are applied to `callback_egress`
    /// immediately after that callback returns and before parsing continues. A
    /// codec must neither mutate nor retain `input`, and must never report more
    /// than `input.chain_length()`. Body callbacks own shared views of input
    /// storage, so their payloads remain valid after the caller trims its queue.
    fn on_ingress(
        &mut self,
        input: &BufQueue,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
    ) -> CodecIngressResult;

    /// Finish parsing after the ordered ingress byte stream reaches EOF and
    /// return any callback-triggered egress results.
    fn on_ingress_eof(
        &mut self,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
    ) -> CodecCallbackResult;

    /// Whether the underlying connection can carry a later request.
    fn is_reusable(&self) -> bool;

    /// Whether graceful connection drain has begun.
    fn is_waiting_to_drain(&self) -> bool;

    /// Whether completing egress requires a transport half-close.
    fn close_on_egress_complete(&self) -> bool;

    /// Last stream identifier initiated by the peer.
    fn last_incoming_stream_id(&self) -> StreamId;

    /// Whether the codec holds part of a request head that opens a stream: a downstream
    /// HTTP/2 HEADERS frame without END_HEADERS whose CONTINUATION frames have not yet ended
    /// the block, so no callback has seen the stream. A block refused for concurrency, or on
    /// a stream above the last stream of a GOAWAY the codec sent, opens none. Codecs that
    /// keep no partial head return false.
    fn has_partial_request_head(&self) -> bool {
        false
    }

    /// Serialize one operation and return exact wire-size and offset metadata.
    fn generate(
        &mut self,
        output: &mut BufQueue,
        frame: EgressFrame<'_>,
        callback: &mut dyn HttpCodecCallback,
    ) -> Result<GeneratedFrame, HttpError>;

    /// Applies one callback's owned controls in order after that callback has
    /// returned.
    ///
    /// Codec implementations normally call
    /// [`HttpCodec::dispatch_ingress_callback`] rather than invoking this
    /// method directly.
    #[inline]
    fn apply_callback_actions(
        &mut self,
        actions: CodecCallbackActions,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
    ) -> CodecCallbackResult {
        if actions.is_empty() {
            return CodecCallbackResult::default();
        }
        let egress_results = actions
            .into_iter()
            .filter_map(|action| match action {
                CodecCallbackAction::SetParserPaused(paused) => {
                    HttpCodec::set_parser_paused(self, paused);
                    None
                }
                CodecCallbackAction::Generate(frame) => {
                    Some(generate_owned_frame(self, callback_egress, frame, callback))
                }
                CodecCallbackAction::InterceptedGenerate {
                    frame,
                    interceptors,
                } => {
                    let result = interceptors
                        .iter()
                        .try_for_each(|interceptor| interceptor.before_generate(&frame))
                        .and_then(|()| {
                            generate_owned_frame(self, callback_egress, frame, callback)
                        });
                    interceptors
                        .iter()
                        .rev()
                        .for_each(|interceptor| interceptor.after_generate(&result));
                    Some(result)
                }
            })
            .collect();
        CodecCallbackResult { egress_results }
    }

    /// Delivers one callback and applies all controls it returns before
    /// returning to the parser.
    fn dispatch_ingress_callback(
        &mut self,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        event: CodecEvent<'_>,
    ) -> CodecCallbackResult {
        let actions = callback.on_ingress_event(event);
        self.apply_callback_actions(actions, callback, callback_egress)
    }
}

fn generate_owned_frame<T: HttpCodec + ?Sized>(
    codec: &mut T,
    output: &mut BufQueue,
    frame: OwnedEgressFrame,
    callback: &mut dyn HttpCodecCallback,
) -> Result<GeneratedFrame, HttpError> {
    match frame {
        OwnedEgressFrame::ConnectionPreface => {
            HttpCodec::generate(codec, output, EgressFrame::ConnectionPreface, callback)
        }
        OwnedEgressFrame::Headers {
            stream,
            message,
            end_stream,
            extra_headers,
        } => HttpCodec::generate(
            codec,
            output,
            EgressFrame::Headers {
                stream,
                message: &message,
                end_stream,
                extra_headers: extra_headers.as_deref(),
            },
            callback,
        ),
        OwnedEgressFrame::PushPromise {
            stream,
            associated_stream,
            message,
            end_stream,
        } => HttpCodec::generate(
            codec,
            output,
            EgressFrame::PushPromise {
                stream,
                associated_stream,
                message: &message,
                end_stream,
            },
            callback,
        ),
        OwnedEgressFrame::Body {
            stream,
            body,
            padding,
            end_stream,
        } => HttpCodec::generate(
            codec,
            output,
            EgressFrame::Body {
                stream,
                body,
                padding,
                end_stream,
            },
            callback,
        ),
        OwnedEgressFrame::ChunkHeader { stream, length } => HttpCodec::generate(
            codec,
            output,
            EgressFrame::ChunkHeader { stream, length },
            callback,
        ),
        OwnedEgressFrame::ChunkTerminator { stream } => HttpCodec::generate(
            codec,
            output,
            EgressFrame::ChunkTerminator { stream },
            callback,
        ),
        OwnedEgressFrame::Trailers { stream, trailers } => HttpCodec::generate(
            codec,
            output,
            EgressFrame::Trailers {
                stream,
                trailers: &trailers,
            },
            callback,
        ),
        OwnedEgressFrame::Padding { stream, length } => HttpCodec::generate(
            codec,
            output,
            EgressFrame::Padding { stream, length },
            callback,
        ),
        OwnedEgressFrame::EndOfMessage { stream } => HttpCodec::generate(
            codec,
            output,
            EgressFrame::EndOfMessage { stream },
            callback,
        ),
        OwnedEgressFrame::Reset { stream, code } => {
            HttpCodec::generate(codec, output, EgressFrame::Reset { stream, code }, callback)
        }
        OwnedEgressFrame::Goaway {
            last_stream,
            code,
            debug_data,
        } => HttpCodec::generate(
            codec,
            output,
            EgressFrame::Goaway {
                last_stream,
                code,
                debug_data,
            },
            callback,
        ),
        OwnedEgressFrame::PingRequest { data } => {
            HttpCodec::generate(codec, output, EgressFrame::PingRequest { data }, callback)
        }
        OwnedEgressFrame::PingReply { data } => {
            HttpCodec::generate(codec, output, EgressFrame::PingReply { data }, callback)
        }
        OwnedEgressFrame::Settings { settings } => HttpCodec::generate(
            codec,
            output,
            EgressFrame::Settings {
                settings: &settings,
            },
            callback,
        ),
        OwnedEgressFrame::SettingsAck => {
            HttpCodec::generate(codec, output, EgressFrame::SettingsAck, callback)
        }
        OwnedEgressFrame::WindowUpdate { stream, amount } => HttpCodec::generate(
            codec,
            output,
            EgressFrame::WindowUpdate { stream, amount },
            callback,
        ),
        OwnedEgressFrame::Priority { stream, priority } => HttpCodec::generate(
            codec,
            output,
            EgressFrame::Priority { stream, priority },
            callback,
        ),
        OwnedEgressFrame::PushPriority { stream, priority } => HttpCodec::generate(
            codec,
            output,
            EgressFrame::PushPriority { stream, priority },
            callback,
        ),
    }
}

/// Read-only continuation exposed to codec filters.
pub trait HttpCodecQuery {
    /// Selected wire protocol.
    fn protocol(&self) -> CodecProtocol;
    /// Whether this codec consumes requests or responses on ingress.
    fn transport_direction(&self) -> TransportDirection;
    /// Static framing and flow-control capabilities.
    fn capabilities(&self) -> CodecCapabilities;
    /// Whether the codec is tracking any incomplete messages.
    fn is_busy(&self) -> bool;
    /// Whether ingress parsing is paused.
    fn is_parser_paused(&self) -> bool;
    /// Whether the underlying connection can carry a later request.
    fn is_reusable(&self) -> bool;
    /// Whether graceful connection drain has begun.
    fn is_waiting_to_drain(&self) -> bool;
    /// Whether completing egress requires a transport half-close.
    fn close_on_egress_complete(&self) -> bool;
    /// Last stream identifier initiated by the peer.
    fn last_incoming_stream_id(&self) -> StreamId;
}

impl<T: HttpCodec + ?Sized> HttpCodecQuery for T {
    fn protocol(&self) -> CodecProtocol {
        HttpCodec::protocol(self)
    }

    fn transport_direction(&self) -> TransportDirection {
        HttpCodec::transport_direction(self)
    }

    fn capabilities(&self) -> CodecCapabilities {
        HttpCodec::capabilities(self)
    }

    fn is_busy(&self) -> bool {
        HttpCodec::is_busy(self)
    }

    fn is_parser_paused(&self) -> bool {
        HttpCodec::is_parser_paused(self)
    }

    fn is_reusable(&self) -> bool {
        HttpCodec::is_reusable(self)
    }

    fn is_waiting_to_drain(&self) -> bool {
        HttpCodec::is_waiting_to_drain(self)
    }

    fn close_on_egress_complete(&self) -> bool {
        HttpCodec::close_on_egress_complete(self)
    }

    fn last_incoming_stream_id(&self) -> StreamId {
        HttpCodec::last_incoming_stream_id(self)
    }
}

/// Mutable continuation exposed to codec filters.
pub trait HttpCodecCall: HttpCodecQuery {
    /// Reserve the next codec-local stream identifier.
    fn create_stream(&mut self) -> StreamId;
    /// Pause or resume ingress parsing.
    fn set_parser_paused(&mut self, paused: bool);
    /// Parse a prefix of `input` and apply callback controls.
    fn on_ingress(
        &mut self,
        input: &BufQueue,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
    ) -> CodecIngressResult;
    /// Finish parsing after ingress reaches EOF and apply callback controls.
    fn on_ingress_eof(
        &mut self,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
    ) -> CodecCallbackResult;
    /// Serialize one egress operation.
    fn generate(
        &mut self,
        output: &mut BufQueue,
        frame: EgressFrame<'_>,
        callback: &mut dyn HttpCodecCallback,
    ) -> Result<GeneratedFrame, HttpError>;
    /// Delivers an ingress callback synthesized by a filter and applies its
    /// actions.
    fn dispatch_ingress_callback(
        &mut self,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        event: CodecEvent<'_>,
    ) -> CodecCallbackResult;
}

impl<T: HttpCodec + ?Sized> HttpCodecCall for T {
    fn create_stream(&mut self) -> StreamId {
        HttpCodec::create_stream(self)
    }

    fn set_parser_paused(&mut self, paused: bool) {
        HttpCodec::set_parser_paused(self, paused);
    }

    fn on_ingress(
        &mut self,
        input: &BufQueue,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
    ) -> CodecIngressResult {
        HttpCodec::on_ingress(self, input, callback, callback_egress)
    }

    fn on_ingress_eof(
        &mut self,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
    ) -> CodecCallbackResult {
        HttpCodec::on_ingress_eof(self, callback, callback_egress)
    }

    fn generate(
        &mut self,
        output: &mut BufQueue,
        frame: EgressFrame<'_>,
        callback: &mut dyn HttpCodecCallback,
    ) -> Result<GeneratedFrame, HttpError> {
        HttpCodec::generate(self, output, frame, callback)
    }

    fn dispatch_ingress_callback(
        &mut self,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        event: CodecEvent<'_>,
    ) -> CodecCallbackResult {
        HttpCodec::dispatch_ingress_callback(self, callback, callback_egress, event)
    }
}

/// Two-way interception points for every codec operation and callback.
///
/// Each default implementation delegates exactly once. An implementation may
/// change arguments before delegation, transform the returned value, or
/// suppress an operation by not invoking `next`. Methods take `&self` so the
/// same filter can participate in a synchronous callback while its downstream
/// call remains active; stateful filters use explicit interior mutability.
/// Callback actions return through the already-traversed filters in the
/// opposite direction, allowing each filter to transform or suppress them
/// before the destination applies them.
pub trait HttpCodecFilter {
    /// Intercepts the selected protocol query.
    fn protocol(&self, next: &dyn HttpCodecQuery) -> CodecProtocol {
        next.protocol()
    }

    /// Intercepts the transport-direction query.
    fn transport_direction(&self, next: &dyn HttpCodecQuery) -> TransportDirection {
        next.transport_direction()
    }

    /// Intercepts the capabilities query.
    fn capabilities(&self, next: &dyn HttpCodecQuery) -> CodecCapabilities {
        next.capabilities()
    }

    /// Intercepts stream reservation.
    fn create_stream(&self, next: &mut dyn HttpCodecCall) -> StreamId {
        next.create_stream()
    }

    /// Intercepts the busy-state query.
    fn is_busy(&self, next: &dyn HttpCodecQuery) -> bool {
        next.is_busy()
    }

    /// Intercepts parser pause changes.
    fn set_parser_paused(&self, paused: bool, next: &mut dyn HttpCodecCall) {
        next.set_parser_paused(paused);
    }

    /// Intercepts the parser-pause query.
    fn is_parser_paused(&self, next: &dyn HttpCodecQuery) -> bool {
        next.is_parser_paused()
    }

    /// Intercepts ingress parsing.
    fn on_ingress(
        &self,
        input: &BufQueue,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        next: &mut dyn HttpCodecCall,
    ) -> CodecIngressResult {
        next.on_ingress(input, callback, callback_egress)
    }

    /// Intercepts ingress EOF processing.
    fn on_ingress_eof(
        &self,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        next: &mut dyn HttpCodecCall,
    ) -> CodecCallbackResult {
        next.on_ingress_eof(callback, callback_egress)
    }

    /// Intercepts the reusability query.
    fn is_reusable(&self, next: &dyn HttpCodecQuery) -> bool {
        next.is_reusable()
    }

    /// Intercepts the drain-state query.
    fn is_waiting_to_drain(&self, next: &dyn HttpCodecQuery) -> bool {
        next.is_waiting_to_drain()
    }

    /// Intercepts the egress-close query.
    fn close_on_egress_complete(&self, next: &dyn HttpCodecQuery) -> bool {
        next.close_on_egress_complete()
    }

    /// Intercepts the last-peer-stream query.
    fn last_incoming_stream_id(&self, next: &dyn HttpCodecQuery) -> StreamId {
        next.last_incoming_stream_id()
    }

    /// Intercepts egress framing.
    fn generate(
        &self,
        output: &mut BufQueue,
        frame: EgressFrame<'_>,
        callback: &mut dyn HttpCodecCallback,
        next: &mut dyn HttpCodecCall,
    ) -> Result<GeneratedFrame, HttpError> {
        next.generate(output, frame, callback)
    }

    /// Intercepts observation-only callbacks in reverse insertion order.
    fn on_callback<'event>(
        &self,
        event: CodecEvent<'event>,
        next: &mut dyn FnMut(CodecEvent<'event>),
    ) {
        next(event);
    }

    /// Intercepts ingress callbacks in reverse insertion order and returns the
    /// action sequence toward the codec in forward order.
    ///
    /// The default preserves any event transformation or suppression performed
    /// by [`HttpCodecFilter::on_callback`]. A filter that transforms or
    /// suppresses direct [`HttpCodecFilter::generate`] calls must apply the
    /// equivalent owned-frame policy here; invoking `generate` itself would
    /// reborrow the destination while its parser call is active.
    fn on_ingress_callback<'event>(
        &self,
        event: CodecEvent<'event>,
        next: &mut dyn FnMut(CodecEvent<'event>) -> CodecCallbackActions,
    ) -> CodecCallbackActions {
        let mut actions = CodecCallbackActions::new();
        self.on_callback(event, &mut |event| actions.append(next(event)));
        actions
    }

    /// Intercepts the outgoing-stream count callback query.
    fn num_outgoing_streams(&self, next: &dyn Fn() -> u32) -> u32 {
        next()
    }

    /// Intercepts the incoming-stream count callback query.
    fn num_incoming_streams(&self, next: &dyn Fn() -> u32) -> u32 {
        next()
    }
}

struct FilteredCallback<'a> {
    filter: &'a dyn HttpCodecFilter,
    callback: &'a mut dyn HttpCodecCallback,
}

impl HttpCodecCallback for FilteredCallback<'_> {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        let callback = &mut *self.callback;
        let mut next = |event| callback.on_event(event);
        self.filter.on_callback(event, &mut next);
    }

    fn on_ingress_event(&mut self, event: CodecEvent<'_>) -> CodecCallbackActions {
        let callback = &mut *self.callback;
        let mut next = |event| callback.on_ingress_event(event);
        self.filter.on_ingress_callback(event, &mut next)
    }

    fn num_outgoing_streams(&self) -> u32 {
        let callback = &*self.callback;
        self.filter
            .num_outgoing_streams(&|| callback.num_outgoing_streams())
    }

    fn num_incoming_streams(&self) -> u32 {
        let callback = &*self.callback;
        self.filter
            .num_incoming_streams(&|| callback.num_incoming_streams())
    }

    fn ingress_body_limit(&self, stream: StreamId) -> Option<usize> {
        self.callback.ingress_body_limit(stream)
    }
}

struct QueryTail<'a> {
    filters: &'a [Box<dyn HttpCodecFilter>],
    destination: &'a dyn HttpCodec,
}

impl HttpCodecQuery for QueryTail<'_> {
    fn protocol(&self) -> CodecProtocol {
        if let Some((filter, remaining)) = self.filters.split_first() {
            filter.protocol(&Self {
                filters: remaining,
                destination: self.destination,
            })
        } else {
            HttpCodec::protocol(self.destination)
        }
    }

    fn transport_direction(&self) -> TransportDirection {
        if let Some((filter, remaining)) = self.filters.split_first() {
            filter.transport_direction(&Self {
                filters: remaining,
                destination: self.destination,
            })
        } else {
            HttpCodec::transport_direction(self.destination)
        }
    }

    fn capabilities(&self) -> CodecCapabilities {
        if let Some((filter, remaining)) = self.filters.split_first() {
            filter.capabilities(&Self {
                filters: remaining,
                destination: self.destination,
            })
        } else {
            HttpCodec::capabilities(self.destination)
        }
    }

    fn is_busy(&self) -> bool {
        if let Some((filter, remaining)) = self.filters.split_first() {
            filter.is_busy(&Self {
                filters: remaining,
                destination: self.destination,
            })
        } else {
            HttpCodec::is_busy(self.destination)
        }
    }

    fn is_parser_paused(&self) -> bool {
        if let Some((filter, remaining)) = self.filters.split_first() {
            filter.is_parser_paused(&Self {
                filters: remaining,
                destination: self.destination,
            })
        } else {
            HttpCodec::is_parser_paused(self.destination)
        }
    }

    fn is_reusable(&self) -> bool {
        if let Some((filter, remaining)) = self.filters.split_first() {
            filter.is_reusable(&Self {
                filters: remaining,
                destination: self.destination,
            })
        } else {
            HttpCodec::is_reusable(self.destination)
        }
    }

    fn is_waiting_to_drain(&self) -> bool {
        if let Some((filter, remaining)) = self.filters.split_first() {
            filter.is_waiting_to_drain(&Self {
                filters: remaining,
                destination: self.destination,
            })
        } else {
            HttpCodec::is_waiting_to_drain(self.destination)
        }
    }

    fn close_on_egress_complete(&self) -> bool {
        if let Some((filter, remaining)) = self.filters.split_first() {
            filter.close_on_egress_complete(&Self {
                filters: remaining,
                destination: self.destination,
            })
        } else {
            HttpCodec::close_on_egress_complete(self.destination)
        }
    }

    fn last_incoming_stream_id(&self) -> StreamId {
        if let Some((filter, remaining)) = self.filters.split_first() {
            filter.last_incoming_stream_id(&Self {
                filters: remaining,
                destination: self.destination,
            })
        } else {
            HttpCodec::last_incoming_stream_id(self.destination)
        }
    }
}

struct CallTail<'a> {
    filters: &'a [Box<dyn HttpCodecFilter>],
    callback_filter: Option<&'a dyn HttpCodecFilter>,
    destination: &'a mut dyn HttpCodec,
}

impl HttpCodecQuery for CallTail<'_> {
    fn protocol(&self) -> CodecProtocol {
        QueryTail {
            filters: self.filters,
            destination: &*self.destination,
        }
        .protocol()
    }

    fn transport_direction(&self) -> TransportDirection {
        QueryTail {
            filters: self.filters,
            destination: &*self.destination,
        }
        .transport_direction()
    }

    fn capabilities(&self) -> CodecCapabilities {
        QueryTail {
            filters: self.filters,
            destination: &*self.destination,
        }
        .capabilities()
    }

    fn is_busy(&self) -> bool {
        QueryTail {
            filters: self.filters,
            destination: &*self.destination,
        }
        .is_busy()
    }

    fn is_parser_paused(&self) -> bool {
        QueryTail {
            filters: self.filters,
            destination: &*self.destination,
        }
        .is_parser_paused()
    }

    fn is_reusable(&self) -> bool {
        QueryTail {
            filters: self.filters,
            destination: &*self.destination,
        }
        .is_reusable()
    }

    fn is_waiting_to_drain(&self) -> bool {
        QueryTail {
            filters: self.filters,
            destination: &*self.destination,
        }
        .is_waiting_to_drain()
    }

    fn close_on_egress_complete(&self) -> bool {
        QueryTail {
            filters: self.filters,
            destination: &*self.destination,
        }
        .close_on_egress_complete()
    }

    fn last_incoming_stream_id(&self) -> StreamId {
        QueryTail {
            filters: self.filters,
            destination: &*self.destination,
        }
        .last_incoming_stream_id()
    }
}

impl CallTail<'_> {
    fn on_ingress_after_callback(
        &mut self,
        input: &BufQueue,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
    ) -> CodecIngressResult {
        if let Some((filter, remaining)) = self.filters.split_first() {
            let mut next = CallTail {
                filters: remaining,
                callback_filter: Some(filter.as_ref()),
                destination: &mut *self.destination,
            };
            filter.on_ingress(input, callback, callback_egress, &mut next)
        } else {
            HttpCodec::on_ingress(self.destination, input, callback, callback_egress)
        }
    }

    fn on_ingress_eof_after_callback(
        &mut self,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
    ) -> CodecCallbackResult {
        if let Some((filter, remaining)) = self.filters.split_first() {
            let mut next = CallTail {
                filters: remaining,
                callback_filter: Some(filter.as_ref()),
                destination: &mut *self.destination,
            };
            filter.on_ingress_eof(callback, callback_egress, &mut next)
        } else {
            HttpCodec::on_ingress_eof(self.destination, callback, callback_egress)
        }
    }

    fn generate_after_callback(
        &mut self,
        output: &mut BufQueue,
        frame: EgressFrame<'_>,
        callback: &mut dyn HttpCodecCallback,
    ) -> Result<GeneratedFrame, HttpError> {
        if let Some((filter, remaining)) = self.filters.split_first() {
            let mut next = CallTail {
                filters: remaining,
                callback_filter: Some(filter.as_ref()),
                destination: &mut *self.destination,
            };
            filter.generate(output, frame, callback, &mut next)
        } else {
            HttpCodec::generate(self.destination, output, frame, callback)
        }
    }
}

impl HttpCodecCall for CallTail<'_> {
    fn create_stream(&mut self) -> StreamId {
        if let Some((filter, remaining)) = self.filters.split_first() {
            let mut next = CallTail {
                filters: remaining,
                callback_filter: Some(filter.as_ref()),
                destination: &mut *self.destination,
            };
            filter.create_stream(&mut next)
        } else {
            HttpCodec::create_stream(self.destination)
        }
    }

    fn set_parser_paused(&mut self, paused: bool) {
        if let Some((filter, remaining)) = self.filters.split_first() {
            let mut next = CallTail {
                filters: remaining,
                callback_filter: Some(filter.as_ref()),
                destination: &mut *self.destination,
            };
            filter.set_parser_paused(paused, &mut next);
        } else {
            HttpCodec::set_parser_paused(self.destination, paused);
        }
    }

    fn on_ingress(
        &mut self,
        input: &BufQueue,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
    ) -> CodecIngressResult {
        if let Some(filter) = self.callback_filter {
            let mut callback = FilteredCallback { filter, callback };
            self.on_ingress_after_callback(input, &mut callback, callback_egress)
        } else {
            self.on_ingress_after_callback(input, callback, callback_egress)
        }
    }

    fn on_ingress_eof(
        &mut self,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
    ) -> CodecCallbackResult {
        if let Some(filter) = self.callback_filter {
            let mut callback = FilteredCallback { filter, callback };
            self.on_ingress_eof_after_callback(&mut callback, callback_egress)
        } else {
            self.on_ingress_eof_after_callback(callback, callback_egress)
        }
    }

    fn generate(
        &mut self,
        output: &mut BufQueue,
        frame: EgressFrame<'_>,
        callback: &mut dyn HttpCodecCallback,
    ) -> Result<GeneratedFrame, HttpError> {
        if let Some(filter) = self.callback_filter {
            let mut callback = FilteredCallback { filter, callback };
            self.generate_after_callback(output, frame, &mut callback)
        } else {
            self.generate_after_callback(output, frame, callback)
        }
    }

    fn dispatch_ingress_callback(
        &mut self,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        event: CodecEvent<'_>,
    ) -> CodecCallbackResult {
        let actions = callback.on_ingress_event(event);
        HttpCodec::apply_callback_actions(self.destination, actions, callback, callback_egress)
    }
}

/// Owning, worker-local chain for a codec and its two-way filters.
pub struct HttpCodecFilterChain {
    destination: Box<dyn HttpCodec>,
    filters: Vec<Box<dyn HttpCodecFilter>>,
}

impl HttpCodecFilterChain {
    /// Creates a chain whose destination is `codec`.
    pub fn new(codec: impl HttpCodec + 'static) -> Self {
        Self::from_boxed(Box::new(codec))
    }

    /// Creates a chain from an already erased worker-local codec.
    pub fn from_boxed(codec: Box<dyn HttpCodec>) -> Self {
        Self {
            destination: codec,
            filters: Vec::new(),
        }
    }

    /// Adds a filter at the destination end of the current chain.
    pub fn add_filter(&mut self, filter: impl HttpCodecFilter + 'static) {
        self.filters.push(Box::new(filter));
    }

    /// Returns the concrete destination through its codec interface.
    pub fn destination(&self) -> &dyn HttpCodec {
        self.destination.as_ref()
    }

    /// Returns mutable access to the concrete destination interface.
    pub fn destination_mut(&mut self) -> &mut dyn HttpCodec {
        self.destination.as_mut()
    }

    /// Number of installed filters.
    pub fn filter_count(&self) -> usize {
        self.filters.len()
    }

    fn query_tail(&self) -> QueryTail<'_> {
        QueryTail {
            filters: &self.filters,
            destination: self.destination.as_ref(),
        }
    }

    fn call_tail(&mut self) -> CallTail<'_> {
        CallTail {
            filters: &self.filters,
            callback_filter: None,
            destination: self.destination.as_mut(),
        }
    }
}

impl HttpCodec for HttpCodecFilterChain {
    fn protocol(&self) -> CodecProtocol {
        self.query_tail().protocol()
    }

    fn transport_direction(&self) -> TransportDirection {
        self.query_tail().transport_direction()
    }

    fn capabilities(&self) -> CodecCapabilities {
        self.query_tail().capabilities()
    }

    fn create_stream(&mut self) -> StreamId {
        self.call_tail().create_stream()
    }

    fn is_busy(&self) -> bool {
        self.query_tail().is_busy()
    }

    fn has_partial_request_head(&self) -> bool {
        self.destination.has_partial_request_head()
    }

    fn set_parser_paused(&mut self, paused: bool) {
        self.call_tail().set_parser_paused(paused);
    }

    fn is_parser_paused(&self) -> bool {
        self.query_tail().is_parser_paused()
    }

    fn on_ingress(
        &mut self,
        input: &BufQueue,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
    ) -> CodecIngressResult {
        self.call_tail()
            .on_ingress(input, callback, callback_egress)
    }

    fn on_ingress_eof(
        &mut self,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
    ) -> CodecCallbackResult {
        self.call_tail().on_ingress_eof(callback, callback_egress)
    }

    fn is_reusable(&self) -> bool {
        self.query_tail().is_reusable()
    }

    fn is_waiting_to_drain(&self) -> bool {
        self.query_tail().is_waiting_to_drain()
    }

    fn close_on_egress_complete(&self) -> bool {
        self.query_tail().close_on_egress_complete()
    }

    fn last_incoming_stream_id(&self) -> StreamId {
        self.query_tail().last_incoming_stream_id()
    }

    fn generate(
        &mut self,
        output: &mut BufQueue,
        frame: EgressFrame<'_>,
        callback: &mut dyn HttpCodecCallback,
    ) -> Result<GeneratedFrame, HttpError> {
        self.call_tail().generate(output, frame, callback)
    }
}

/// Protocol-neutral transport seam reserved for a future multiplexed session.
///
/// Request streams and unidirectional control streams remain distinct without
/// exposing QUIC, QPACK, datagram, or runtime types in the current API.
pub trait MultiplexedSessionTransport {
    /// Bidirectional stream used for one request/response exchange.
    type RequestStream;
    /// Locally initiated unidirectional control stream.
    type ControlStream;

    /// Opens a new independent request stream.
    fn open_request_stream(&mut self) -> Result<Self::RequestStream, HttpError>;

    /// Opens a new independent unidirectional control stream.
    fn open_unidirectional_control_stream(&mut self) -> Result<Self::ControlStream, HttpError>;
}

#[cfg(test)]
#[path = "../tests/contract.rs"]
mod codec_contract;

#[cfg(test)]
#[path = "../tests/error_adapter.rs"]
mod error_adapter_tests;
