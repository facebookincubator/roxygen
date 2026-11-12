//! Source-faithful HTTP/2 codec built from the framing and HPACK layers.

use std::collections::HashSet;
use std::rc::Rc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Instant;

use roxygen_codec::CodecCapabilities;
use roxygen_codec::CodecEvent;
use roxygen_codec::CodecIngressResult;
use roxygen_codec::CodecSetting;
use roxygen_codec::EgressFrame;
use roxygen_codec::GeneratedFrame;
use roxygen_codec::H2ErrorCode;
use roxygen_codec::HttpCodec;
use roxygen_codec::HttpCodecCallback;
use roxygen_codec::HttpException;
use roxygen_codec::HttpExceptionDirection;
use roxygen_codec::ProxygenError;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::headers::HttpHeaders;
use roxygen_core::http::CodecProtocol;
use roxygen_core::http::HttpHeaderSize;
use roxygen_core::http::HttpMethod;
use roxygen_core::http::TransportDirection;
use roxygen_core::ids::StreamId;
use roxygen_core::message::HttpMessage;
use roxygen_core::priority::HttpPriority;
use roxygen_core::priority::http_priority_from_string;
use roxygen_core::priority::http_priority_to_string;

use crate::constants::CONNECTION_PREFACE;
use crate::constants::FRAME_HEADER_SIZE;
use crate::constants::INITIAL_WINDOW;
use crate::constants::MAX_FRAME_PAYLOAD_LENGTH;
use crate::constants::MAX_FRAME_PAYLOAD_LENGTH_MIN;
use crate::constants::MAX_HEADER_TABLE_SIZE;
use crate::constants::MAX_STREAM_ID;
use crate::constants::MAX_WINDOW_UPDATE_SIZE;
use crate::frame;
use crate::frame::DataHeaderArena;
use crate::frame::Flags;
use crate::frame::FrameError;
use crate::frame::FrameHeader;
use crate::frame::FrameType;
use crate::frame::SegmentCursor;
use crate::frame::Setting;
use crate::header_decode::FieldDecodeOptions;
use crate::header_decode::HeaderDecodeInfo;
use crate::hpack::CompressionInfo;
use crate::hpack::DecodeMemoState;
use crate::hpack::DecodeMemoToken;
use crate::hpack::EMPTY_BLOCK_BOUND;
use crate::hpack::EncodeHeader;
use crate::hpack::Header;
use crate::hpack::HeaderCodecStats;
use crate::hpack::HeaderIndexingStrategy;
use crate::hpack::HeaderInputs;
use crate::hpack::HeaderName;
use crate::hpack::HpackCodec;
use crate::hpack::HpackError;
use crate::hpack::MEMO_MAX_COMPRESSED;
use crate::hpack::MEMO_MAX_UNCOMPRESSED;
use crate::hpack::StreamingCallback;
use crate::hpack::add_input_bound;
use crate::parallel::ClosingState;
use crate::parallel::ParallelCodecState;
use crate::settings::HttpSettings;
use crate::settings::SettingsId;
use crate::util::for_each_modern_header;
use crate::util::is_modern_header;

const MAX_CONTINUATION_FRAMES: u8 = 100;
const DEFAULT_MAX_HEADER_LIST_SIZE: u64 = 1 << 17;
/// The capacity `HttpHeaders` allocates on its first insert.
const HEADER_MAP_INITIAL_CAPACITY: usize = 16;
/// The largest capacity a decoded head's header map is pre-sized to.
const MAX_HEADER_PRESIZE: usize = 64;
static NEXT_PING_TOKEN: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IngressState {
    ConnectionPreface,
    FirstFrameHeader,
    FrameHeader,
    FramePayload(FrameHeader),
    Data(DataState),
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DataState {
    header: FrameHeader,
    remaining: usize,
    trailing_padding: usize,
    prefix_pending: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HeaderBlockKind {
    Headers {
        stream: u32,
        request: bool,
        trailers: bool,
        end_stream: bool,
    },
    PushPromise {
        associated_stream: u32,
        promised_stream: u32,
    },
}

#[derive(Debug)]
struct HeaderBlockState {
    kind: HeaderBlockKind,
    bytes: Vec<u8>,
    continuations_left: u8,
    rejected: bool,
    push_begin_emitted: bool,
}

enum MessagePseudoHeaders<'a> {
    Request {
        method: &'a [u8],
        scheme_and_path: Option<(&'a [u8], &'a [u8])>,
        authority: &'a [u8],
        protocol: Option<&'a [u8]>,
    },
    Response {
        bytes: [u8; 5],
        start: u8,
    },
}

struct PreparedMessageHeaders<'a> {
    pseudo: MessagePseudoHeaders<'a>,
    headers: &'a HttpHeaders,
    extra: Option<&'a HttpHeaders>,
    date: Option<String>,
    /// `DIV-041`: values are encoded as stored instead of trimmed.
    verbatim_values: bool,
}

#[cfg(test)]
thread_local! {
    static HEADER_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static HEADER_FRAME_ALLOCATIONS: std::cell::Cell<(usize, usize)> =
        const { std::cell::Cell::new((0, 0)) };
    static MEMO_COPIES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// The capacity the last uncached HEADERS decode reserved, 0 when none.
    static LAST_HEADER_PRESIZE: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static UPGRADED_STREAM_PROBES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Returns and resets the number of fields `PreparedMessageHeaders::for_each` has visited on
/// this thread.
#[cfg(test)]
pub(crate) fn take_header_visits() -> usize {
    HEADER_VISITS.replace(0)
}

impl PreparedMessageHeaders<'_> {
    /// Visits the pseudo-headers, then each header and the date. With `MESSAGE_FIELDS_ONLY`, it
    /// skips headers that HTTP/2 does not carry and trims their values unless `VERBATIM`, which
    /// `for_each` sets from `verbatim_values`; without it, it visits every header untrimmed, which
    /// only suits length bounds.
    fn visit<'a, const MESSAGE_FIELDS_ONLY: bool, const VERBATIM: bool>(
        &'a self,
        mut visitor: impl FnMut(Header<'a>),
    ) {
        match &self.pseudo {
            MessagePseudoHeaders::Request {
                method,
                scheme_and_path,
                authority,
                protocol,
            } => {
                visitor(Header::new(HttpHeaderCode::ColonMethod.into(), method));
                if let Some((scheme, _)) = scheme_and_path {
                    visitor(Header::new(HttpHeaderCode::ColonScheme.into(), scheme));
                }
                if !authority.is_empty() {
                    visitor(Header::new(
                        HttpHeaderCode::ColonAuthority.into(),
                        authority,
                    ));
                }
                if let Some((_, path)) = scheme_and_path {
                    visitor(Header::new(HttpHeaderCode::ColonPath.into(), path));
                }
                if let Some(protocol) = protocol {
                    visitor(Header::new(HttpHeaderCode::ColonProtocol.into(), protocol));
                }
            }
            MessagePseudoHeaders::Response { bytes, start } => {
                visitor(Header::new(
                    HttpHeaderCode::ColonStatus.into(),
                    &bytes[usize::from(*start)..],
                ));
            }
        }
        let mut visit_headers = |headers: &'a HttpHeaders| {
            for (code, name, value) in headers.iter() {
                if !MESSAGE_FIELDS_ONLY {
                    visitor(Header { code, name, value });
                } else if is_modern_header(code, name) {
                    visitor(Header {
                        code,
                        name,
                        value: if VERBATIM { value } else { trim_lws(value) },
                    });
                }
            }
        };
        visit_headers(self.headers);
        if let Some(extra) = self.extra {
            visit_headers(extra);
        }
        if let Some(date) = &self.date {
            visitor(Header::new(HttpHeaderCode::Date.into(), date.as_bytes()));
        }
    }
}

impl HeaderInputs for PreparedMessageHeaders<'_> {
    fn for_each<'a>(&'a self, visitor: impl FnMut(Header<'a>)) {
        #[cfg(test)]
        let visitor = {
            let mut visitor = visitor;
            move |header: Header<'a>| {
                HEADER_VISITS.set(HEADER_VISITS.get() + 1);
                visitor(header);
            }
        };
        if self.verbatim_values {
            self.visit::<true, true>(visitor);
        } else {
            self.visit::<true, false>(visitor);
        }
    }

    fn length_bound(&self) -> Option<usize> {
        let mut bound = Some(EMPTY_BLOCK_BOUND);
        self.visit::<false, false>(|header| {
            bound = add_input_bound(bound, &header);
        });
        bound
    }
}

struct DecodedTemplate {
    message: Box<HttpMessage>,
    static_refs: u32,
}

struct DecoderMemo {
    token: DecodeMemoToken,
    request: bool,
    validate: bool,
    strict: bool,
    options: FieldDecodeOptions,
    /// The length and seeded hash of the last eligible block. Only a block that matches them is
    /// copied, so a block seen once costs no allocation.
    len: usize,
    hash: u64,
    /// The block `ready` was decoded from. It is empty while `ready` is `None` and keeps its
    /// capacity for the next copy.
    wire: Vec<u8>,
    ready: Option<DecodedTemplate>,
}

/// A runtime-neutral HTTP/2 parser and framer.
pub struct Http2Codec {
    parallel: ParallelCodecState,
    ingress_settings: HttpSettings,
    egress_settings: HttpSettings,
    hpack: HpackCodec,
    ingress_state: IngressState,
    header_block: Option<HeaderBlockState>,
    pending_end_stream: Option<u32>,
    upgraded_streams: HashSet<u32>,
    pending_table_max_size: Option<u32>,
    fatal_error: Option<H2ErrorCode>,
    add_date_to_response: bool,
    validate_headers: bool,
    strict_validation: bool,
    field_options: FieldDecodeOptions,
    reuse_payload_headroom: bool,
    compact_data: bool,
    data_headers: DataHeaderArena,
    user_agent: Box<[u8]>,
    decoder_memo: Option<DecoderMemo>,
    /// The header count of the last head decoded without the memo.
    last_head_header_count: usize,
    #[cfg(test)]
    decoder_template_copies: usize,
    #[cfg(test)]
    equal_memo_fingerprints: bool,
}

impl Http2Codec {
    pub fn new(direction: TransportDirection) -> Self {
        let ingress_state = if direction.is_downstream() {
            IngressState::ConnectionPreface
        } else {
            IngressState::FirstFrameHeader
        };
        let egress_settings = HttpSettings::from_pairs([
            (SettingsId::HeaderTableSize, 4096),
            (SettingsId::EnablePush, 0),
            (
                SettingsId::MaxFrameSize,
                u64::from(MAX_FRAME_PAYLOAD_LENGTH_MIN),
            ),
            (SettingsId::MaxHeaderListSize, DEFAULT_MAX_HEADER_LIST_SIZE),
        ]);
        let mut hpack = HpackCodec::new();
        hpack.set_max_uncompressed(DEFAULT_MAX_HEADER_LIST_SIZE);
        Self {
            parallel: ParallelCodecState::new(direction),
            ingress_settings: HttpSettings::new(),
            egress_settings,
            hpack,
            ingress_state,
            header_block: None,
            pending_end_stream: None,
            upgraded_streams: HashSet::new(),
            pending_table_max_size: None,
            fatal_error: None,
            add_date_to_response: true,
            validate_headers: true,
            strict_validation: false,
            field_options: FieldDecodeOptions::default(),
            reuse_payload_headroom: true,
            compact_data: false,
            data_headers: DataHeaderArena::default(),
            user_agent: Box::default(),
            decoder_memo: None,
            last_head_header_count: 0,
            #[cfg(test)]
            decoder_template_copies: 0,
            #[cfg(test)]
            equal_memo_fingerprints: false,
        }
    }

    pub const fn ingress_settings(&self) -> &HttpSettings {
        &self.ingress_settings
    }

    pub fn egress_settings_mut(&mut self) -> &mut HttpSettings {
        &mut self.egress_settings
    }

    pub fn set_next_egress_stream_id(&mut self, stream: u32) {
        self.parallel.set_next_egress_stream_id(stream);
    }

    pub fn enable_double_goaway_drain(&mut self) {
        self.parallel.enable_double_goaway_drain();
    }

    pub fn disable_double_goaway_drain(&mut self) {
        self.parallel.disable_double_goaway_drain();
    }

    /// Retains Proxygen's compatibility setting.
    ///
    /// Both values preserve payload backing because [`Bytes`](bytes::Bytes) has no safe
    /// mutable-headroom operation; frame metadata is allocated separately.
    pub fn set_reuse_payload_headroom(&mut self, enabled: bool) {
        self.reuse_payload_headroom = enabled;
    }

    /// Copies small unpadded DATA into bounded compact wire segments.
    ///
    /// Large and padded frames retain payload backing. Copied body owners may
    /// be released during generation, before physical write completion.
    pub fn set_compact_data(&mut self, enabled: bool) {
        self.compact_data = enabled;
    }

    pub fn set_add_date_header_to_response(&mut self, enabled: bool) {
        self.add_date_to_response = enabled;
    }

    pub fn set_validate_headers(&mut self, enabled: bool) {
        self.decoder_memo = None;
        self.validate_headers = enabled;
    }

    pub fn set_strict_validation(&mut self, enabled: bool) {
        self.decoder_memo = None;
        self.strict_validation = enabled;
    }

    /// Roxygen-only (`DIV-040`): a request's `:method` is read as RFC 9110 section 9.1 defines
    /// methods, and as h2 (`http::Method::from_bytes`) reads them: any token, case-sensitive and
    /// kept byte for byte (`Method::from_exact_token`), so `post` and `Post` are extension
    /// methods and only `POST` is `HttpMethod::Post`, instead of the reference decoder's run of
    /// letters and `-`, whose recognized methods match without regard to case and whose
    /// extension methods are uppercased. A `:method` that is not a token, the empty one
    /// included, makes the request malformed. Off by default.
    pub fn set_exact_methods(&mut self, enabled: bool) {
        self.decoder_memo = None;
        self.field_options.exact_methods = enabled;
    }

    /// Roxygen-only (`DIV-041`): every decoded field section (request and response heads,
    /// trailers, push promises) stores its regular field values exactly as decoded, surrounding
    /// spaces and tabs included, as h2 (and hyper over it) does, instead of trimming them as the
    /// reference decoder does for every field; and every generated message head encodes its
    /// regular field values as stored (as h2 writes a `HeaderValue`), instead of trimming them
    /// (trailers already go out as stored). Values are then checked as h2 checks them, so the
    /// decoder never accepts one h2 refuses: a byte below 0x20 other than HTAB, or DEL, makes the
    /// message malformed (no obsolete line folding), as does a `content-length` that is not at
    /// most 19 digits or a `te` other than `trailers`. An empty `content-length`, which h2 reads
    /// as zero, is kept as an empty field (a uniplex session with
    /// `verbatim_http2_header_values` checks the body against zero). `cookie` crumbs are still
    /// combined into one value, untrimmed. Pseudo-headers are unchanged. Off by default.
    pub fn set_verbatim_field_values(&mut self, enabled: bool) {
        self.decoder_memo = None;
        self.field_options.verbatim_values = enabled;
    }

    pub fn set_header_indexing_strategy(
        &mut self,
        strategy: std::rc::Rc<dyn HeaderIndexingStrategy>,
    ) {
        self.hpack.set_header_indexing_strategy(strategy);
    }

    pub fn set_header_codec_stats(&mut self, stats: Option<Rc<dyn HeaderCodecStats>>) {
        self.decoder_memo = None;
        self.hpack.set_stats(stats);
    }

    pub fn compression_info(&self) -> CompressionInfo {
        self.hpack.compression_info()
    }

    pub fn user_agent(&self) -> &[u8] {
        &self.user_agent
    }

    pub fn supports_push_transactions(&self) -> bool {
        if self.parallel.direction().is_downstream() {
            self.ingress_settings.value_or(SettingsId::EnablePush, 1) != 0
        } else {
            self.egress_settings.value_or(SettingsId::EnablePush, 1) != 0
        }
    }

    /// Whether stream-scoped ingress and egress remain eligible after GOAWAY.
    pub fn is_stream_ingress_egress_allowed(&self, stream: StreamId) -> bool {
        u32::try_from(stream.get())
            .ok()
            .filter(|stream| *stream <= MAX_STREAM_ID)
            .is_some_and(|stream| self.parallel.stream_allowed(stream))
    }

    /// Serializes the codec's configured egress settings in insertion order.
    pub fn generate_configured_settings(
        &mut self,
        output: &mut BufQueue,
        callback: &mut dyn HttpCodecCallback,
    ) -> Result<GeneratedFrame, HttpError> {
        let settings = self.egress_settings.codec_settings();
        HttpCodec::generate(
            self,
            output,
            EgressFrame::Settings {
                settings: &settings,
            },
            callback,
        )
    }

    fn max_send_frame_size(&self) -> usize {
        usize::try_from(self.ingress_settings.value_or(
            SettingsId::MaxFrameSize,
            u64::from(MAX_FRAME_PAYLOAD_LENGTH_MIN),
        ))
        .unwrap_or(MAX_FRAME_PAYLOAD_LENGTH_MIN as usize)
    }

    fn max_receive_frame_size(&self) -> u32 {
        u32::try_from(self.egress_settings.value_or(
            SettingsId::MaxFrameSize,
            u64::from(MAX_FRAME_PAYLOAD_LENGTH_MIN),
        ))
        .unwrap_or(MAX_FRAME_PAYLOAD_LENGTH_MIN)
    }

    fn dispatch(
        &mut self,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
        event: CodecEvent<'_>,
    ) {
        let stream = match &event {
            CodecEvent::MessageBegin { stream, .. }
            | CodecEvent::PushMessageBegin { stream, .. }
            | CodecEvent::HeadersComplete { stream, .. }
            | CodecEvent::Body { stream, .. }
            | CodecEvent::ChunkHeader { stream, .. }
            | CodecEvent::ChunkComplete { stream }
            | CodecEvent::TrailersComplete { stream, .. }
            | CodecEvent::MessageComplete { stream, .. }
            | CodecEvent::Error { stream, .. }
            | CodecEvent::Abort { stream, .. }
            | CodecEvent::WindowUpdate { stream, .. }
            | CodecEvent::Priority { stream, .. }
            | CodecEvent::PushPriority { stream, .. } => Some(stream.get()),
            CodecEvent::FrameHeader { .. }
            | CodecEvent::Goaway { .. }
            | CodecEvent::PingRequest { .. }
            | CodecEvent::PingReply { .. }
            | CodecEvent::Settings { .. }
            | CodecEvent::SettingsAck
            | CodecEvent::GeneratedFrameHeader { .. } => None,
        };
        if stream.is_some_and(|stream| {
            u32::try_from(stream)
                .ok()
                .is_none_or(|stream| !self.parallel.stream_allowed(stream))
        }) {
            return;
        }
        let actions = callback.on_ingress_event(event);
        if actions.is_empty() {
            return;
        }
        let callback_result =
            HttpCodec::apply_callback_actions(self, actions, callback, callback_egress);
        result.append_callback_result(callback_result);
    }

    #[cold]
    #[inline(never)]
    fn connection_error(
        &mut self,
        code: H2ErrorCode,
        message: impl AsRef<[u8]>,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) {
        if self.fatal_error.is_some() {
            return;
        }
        self.fatal_error = Some(code);
        self.ingress_state = IngressState::Failed;
        let mut error = HttpException::new(HttpExceptionDirection::IngressAndEgress, message);
        error.set_codec_status_code(code);
        self.dispatch(
            callback,
            callback_egress,
            result,
            CodecEvent::Error {
                stream: StreamId::UNSPECIFIED,
                error: &error,
                new_stream: false,
            },
        );
    }

    #[cold]
    #[inline(never)]
    fn stream_error(
        &mut self,
        stream: u32,
        code: H2ErrorCode,
        message: impl AsRef<[u8]>,
        new_stream: bool,
        partial_message: Option<HttpMessage>,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) {
        let mut error = HttpException::new(HttpExceptionDirection::IngressAndEgress, message);
        error.set_codec_status_code(code);
        error.set_partial_message(partial_message);
        self.dispatch(
            callback,
            callback_egress,
            result,
            CodecEvent::Error {
                stream: StreamId::new(u64::from(stream)),
                error: &error,
                new_stream,
            },
        );
    }

    #[cold]
    #[inline(never)]
    fn consume_connection_preface(
        &mut self,
        remaining: &mut SegmentCursor<'_>,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) -> bool {
        let mut prefix = remaining
            .take(CONNECTION_PREFACE.len())
            .expect("the connection preface length was checked");
        let mut matches = true;
        let mut matched = 0;
        while let Some(chunk) = prefix.next_chunk() {
            matches &= chunk == &CONNECTION_PREFACE[matched..matched + chunk.len()];
            matched += chunk.len();
        }
        if !matches {
            self.connection_error(
                H2ErrorCode::PROTOCOL_ERROR,
                b"missing connection preface",
                callback,
                callback_egress,
                result,
            );
            return false;
        }
        self.ingress_state = IngressState::FirstFrameHeader;
        true
    }

    // Fatal errors report protocol consumption independently of cursor progress;
    // the ingress loop stops before using that cursor again.
    fn process_data(
        &mut self,
        input: &mut SegmentCursor<'_>,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) -> usize {
        let IngressState::Data(mut state) = self.ingress_state else {
            return 0;
        };
        if state.header.stream == 0 {
            self.connection_error(
                H2ErrorCode::PROTOCOL_ERROR,
                b"DATA frame requires a nonzero stream",
                callback,
                callback_egress,
                result,
            );
            return 0;
        }
        if state.remaining == 0 {
            if state.prefix_pending {
                self.connection_error(
                    H2ErrorCode::FRAME_SIZE_ERROR,
                    b"padded DATA frame has no padding length",
                    callback,
                    callback_egress,
                    result,
                );
            } else {
                self.finish_data(state.header, callback, callback_egress, result);
            }
            return 0;
        }

        if state.remaining == state.header.length as usize && input.remaining() >= state.remaining {
            let consumed = state.remaining;
            match frame::parse_data_cursor(input, &state.header) {
                Ok(data) => {
                    if data.padding != 0 || !data.data.is_empty() {
                        self.dispatch(
                            callback,
                            callback_egress,
                            result,
                            CodecEvent::Body {
                                stream: StreamId::new(u64::from(state.header.stream)),
                                body: data.data,
                                padding: data.padding,
                            },
                        );
                    }
                    self.finish_data(state.header, callback, callback_egress, result);
                }
                Err(error) => {
                    let (code, message) = frame_error(error);
                    self.connection_error(code, message, callback, callback_egress, result);
                }
            }
            return consumed;
        }

        let mut consumed = 0;
        let mut padding_for_callback = 0;
        if state.prefix_pending {
            if input.remaining() == 1 {
                return 0;
            }
            if input.remaining() == 0 {
                return 0;
            }
            let padding = usize::from(input.read_u8().expect("the padding prefix is available"));
            if padding >= state.remaining {
                self.connection_error(
                    H2ErrorCode::PROTOCOL_ERROR,
                    b"DATA padding exceeds frame payload",
                    callback,
                    callback_egress,
                    result,
                );
                return 0;
            }
            state.prefix_pending = false;
            state.trailing_padding = padding;
            state.remaining -= 1;
            padding_for_callback = u16::try_from(padding + 1).unwrap_or(u16::MAX);
            consumed = 1;
        }

        let available = input.remaining();
        let application_remaining = state.remaining.saturating_sub(state.trailing_padding);
        let body_length = application_remaining.min(available);
        let body = input
            .read_queue(body_length)
            .expect("the DATA body length is bounded by available input");
        let padding_offset = consumed + body_length;
        let padding_length = state.trailing_padding.min(input.remaining());
        if frame::parse_data_end_cursor(input, padding_length, padding_length).is_err() {
            self.connection_error(
                H2ErrorCode::PROTOCOL_ERROR,
                b"DATA padding contains a nonzero byte",
                callback,
                callback_egress,
                result,
            );
            return padding_offset;
        }
        consumed = padding_offset + padding_length;
        state.remaining -= body_length + padding_length;
        state.trailing_padding -= padding_length;
        if body_length != 0 || padding_for_callback != 0 {
            self.dispatch(
                callback,
                callback_egress,
                result,
                CodecEvent::Body {
                    stream: StreamId::new(u64::from(state.header.stream)),
                    body,
                    padding: padding_for_callback,
                },
            );
        }
        if state.remaining == 0 {
            self.finish_data(state.header, callback, callback_egress, result);
        } else {
            self.ingress_state = IngressState::Data(state);
        }
        consumed
    }

    fn finish_data(
        &mut self,
        header: FrameHeader,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) {
        self.ingress_state = IngressState::FrameHeader;
        if header.flags & Flags::END_STREAM != 0
            && self.pending_end_stream.take() == Some(header.stream)
        {
            self.dispatch(
                callback,
                callback_egress,
                result,
                CodecEvent::MessageComplete {
                    stream: StreamId::new(u64::from(header.stream)),
                    upgraded: false,
                },
            );
        }
    }

    fn process_frame_payload(
        &mut self,
        header: FrameHeader,
        payload: SegmentCursor<'_>,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) -> Result<(), (H2ErrorCode, &'static [u8])> {
        if frame::frame_affects_compression(header.frame_type) {
            let buffered = self
                .header_block
                .as_ref()
                .map_or(0, |block| block.bytes.len());
            if buffered.saturating_add(header.length as usize) > self.maximum_header_block_size() {
                return Err((H2ErrorCode::PROTOCOL_ERROR, b"header block is too large"));
            }
        }
        if let Some(block) = &self.header_block {
            if header.frame_type != FrameType::CONTINUATION
                || header.stream != header_block_stream(block.kind)
            {
                return Err((
                    H2ErrorCode::PROTOCOL_ERROR,
                    b"expected CONTINUATION for the active header block",
                ));
            }
        } else if header.frame_type == FrameType::CONTINUATION {
            return Err((
                H2ErrorCode::PROTOCOL_ERROR,
                b"unexpected CONTINUATION frame",
            ));
        }

        match header.frame_type {
            FrameType::HEADERS => {
                let fragment =
                    frame::parse_headers_cursor(payload, &header).map_err(frame_error)?;
                self.process_headers_start(header, fragment, callback, callback_egress, result)?;
            }
            FrameType::WINDOW_UPDATE => {
                let amount =
                    frame::parse_window_update_cursor(payload, &header).map_err(frame_error)?;
                if amount == 0 {
                    if header.stream != 0 {
                        self.stream_error(
                            header.stream,
                            H2ErrorCode::PROTOCOL_ERROR,
                            b"WINDOW_UPDATE increment is zero",
                            false,
                            None,
                            callback,
                            callback_egress,
                            result,
                        );
                    }
                    return Err((
                        H2ErrorCode::PROTOCOL_ERROR,
                        b"WINDOW_UPDATE increment is zero",
                    ));
                }
                self.dispatch(
                    callback,
                    callback_egress,
                    result,
                    CodecEvent::WindowUpdate {
                        stream: StreamId::new(u64::from(header.stream)),
                        amount,
                    },
                );
            }
            _ => self.process_uncommon_frame_payload(
                header,
                payload,
                callback,
                callback_egress,
                result,
            )?,
        }
        Ok(())
    }

    #[cold]
    #[inline(never)]
    fn process_uncommon_frame_payload(
        &mut self,
        header: FrameHeader,
        payload: SegmentCursor<'_>,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) -> Result<(), (H2ErrorCode, &'static [u8])> {
        match header.frame_type {
            FrameType::CONTINUATION => {
                let fragment =
                    frame::parse_continuation_cursor(payload, &header).map_err(frame_error)?;
                self.process_continuation(header, fragment, callback, callback_egress, result)?;
            }
            FrameType::RST_STREAM => {
                let code = frame::parse_rst_stream_cursor(payload, &header).map_err(frame_error)?;
                self.remove_upgraded_stream(header.stream);
                self.dispatch(
                    callback,
                    callback_egress,
                    result,
                    CodecEvent::Abort {
                        stream: StreamId::new(u64::from(header.stream)),
                        code,
                    },
                );
            }
            FrameType::SETTINGS => {
                let settings =
                    frame::parse_settings_cursor(payload, &header).map_err(frame_error)?;
                if header.flags & Flags::ACK != 0 {
                    if let Some(size) = self.pending_table_max_size.take() {
                        self.hpack.set_decoder_header_table_max_size(size);
                    }
                    self.dispatch(callback, callback_egress, result, CodecEvent::SettingsAck);
                } else {
                    self.handle_settings(settings, callback, callback_egress, result)?;
                }
            }
            FrameType::PUSH_PROMISE => {
                if self.parallel.direction().is_downstream() {
                    return Err((
                        H2ErrorCode::PROTOCOL_ERROR,
                        b"PUSH_PROMISE received by downstream codec",
                    ));
                }
                if self.egress_settings.value_or(SettingsId::EnablePush, 1) != 1 {
                    return Err((
                        H2ErrorCode::PROTOCOL_ERROR,
                        b"PUSH_PROMISE received while push is disabled",
                    ));
                }
                let (promised, fragment) =
                    frame::parse_push_promise_cursor(payload, &header).map_err(frame_error)?;
                self.check_new_stream(promised, false)?;
                let push_begin_emitted = if header.flags & Flags::END_HEADERS == 0 {
                    self.dispatch_push_begin(
                        promised,
                        header.stream,
                        None,
                        callback,
                        callback_egress,
                        result,
                    )
                } else {
                    false
                };
                self.start_header_block(
                    HeaderBlockKind::PushPromise {
                        associated_stream: header.stream,
                        promised_stream: promised,
                    },
                    fragment,
                    header.flags & Flags::END_HEADERS != 0,
                    false,
                    push_begin_emitted,
                    callback,
                    callback_egress,
                    result,
                )?;
            }
            FrameType::PING => {
                let data = frame::parse_ping_cursor(payload, &header).map_err(frame_error)?;
                let event = if header.flags & Flags::ACK != 0 {
                    CodecEvent::PingReply { data }
                } else {
                    CodecEvent::PingRequest { data }
                };
                self.dispatch(callback, callback_egress, result, event);
            }
            FrameType::GOAWAY => {
                let (last_stream, code, debug_data) =
                    frame::parse_goaway_cursor(payload, &header).map_err(frame_error)?;
                if self.parallel.receive_goaway(last_stream) {
                    self.dispatch(
                        callback,
                        callback_egress,
                        result,
                        CodecEvent::Goaway {
                            last_good_stream: StreamId::new(u64::from(last_stream)),
                            code,
                            debug_data: (!debug_data.is_empty()).then_some(debug_data),
                        },
                    );
                }
            }
            FrameType::RFC9218_PRIORITY => {
                let (stream, priority) =
                    frame::parse_rfc9218_priority_cursor(payload, &header).map_err(frame_error)?;
                let priority = http_priority_from_string(priority).unwrap_or_default();
                self.dispatch(
                    callback,
                    callback_egress,
                    result,
                    CodecEvent::Priority {
                        stream: StreamId::new(u64::from(stream)),
                        priority,
                    },
                );
            }
            _ => {}
        }
        Ok(())
    }

    fn handle_settings(
        &mut self,
        settings: Vec<Setting>,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) -> Result<(), (H2ErrorCode, &'static [u8])> {
        let mut accepted = Vec::new();
        for setting in settings {
            let Some(id) = SettingsId::from_u16(setting.identifier) else {
                continue;
            };
            match id {
                SettingsId::HeaderTableSize => {
                    self.hpack
                        .set_encoder_header_table_size(setting.value.min(MAX_HEADER_TABLE_SIZE));
                }
                SettingsId::EnablePush => {
                    if setting.value > 1
                        || (setting.value == 1 && self.parallel.direction().is_upstream())
                    {
                        return Err((H2ErrorCode::PROTOCOL_ERROR, b"invalid ENABLE_PUSH"));
                    }
                }
                SettingsId::InitialWindowSize => {
                    if setting.value > MAX_WINDOW_UPDATE_SIZE {
                        return Err((
                            H2ErrorCode::FLOW_CONTROL_ERROR,
                            b"invalid INITIAL_WINDOW_SIZE",
                        ));
                    }
                }
                SettingsId::MaxFrameSize => {
                    if !(MAX_FRAME_PAYLOAD_LENGTH_MIN..=MAX_FRAME_PAYLOAD_LENGTH)
                        .contains(&setting.value)
                    {
                        return Err((H2ErrorCode::PROTOCOL_ERROR, b"invalid MAX_FRAME_SIZE"));
                    }
                }
                SettingsId::EnableConnectProtocol if setting.value > 1 => {
                    return Err((
                        H2ErrorCode::PROTOCOL_ERROR,
                        b"invalid ENABLE_CONNECT_PROTOCOL",
                    ));
                }
                SettingsId::MaxConcurrentStreams
                | SettingsId::MaxHeaderListSize
                | SettingsId::EnableConnectProtocol => {}
            }
            self.ingress_settings.set(id, u64::from(setting.value));
            accepted.push(CodecSetting {
                identifier: setting.identifier,
                value: u64::from(setting.value),
            });
        }
        self.dispatch(
            callback,
            callback_egress,
            result,
            CodecEvent::Settings {
                settings: accepted.into_boxed_slice(),
            },
        );
        Ok(())
    }

    fn process_headers_start(
        &mut self,
        header: FrameHeader,
        fragment: SegmentCursor<'_>,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) -> Result<(), (H2ErrorCode, &'static [u8])> {
        let trailers = if self.parallel.direction().is_downstream() {
            self.check_new_stream(header.stream, true)?
        } else {
            false
        };
        let kind = HeaderBlockKind::Headers {
            stream: header.stream,
            request: self.parallel.direction().is_downstream(),
            trailers,
            end_stream: header.flags & Flags::END_STREAM != 0,
        };
        let rejected = !self.parallel.is_initiated_stream(header.stream)
            && u64::from(callback.num_incoming_streams())
                >= self
                    .egress_settings
                    .value_or(SettingsId::MaxConcurrentStreams, i32::MAX as u64);
        if rejected {
            self.stream_error(
                header.stream,
                H2ErrorCode::REFUSED_STREAM,
                b"exceeded max_concurrent_streams",
                true,
                None,
                callback,
                callback_egress,
                result,
            );
        }
        self.start_header_block(
            kind,
            fragment,
            header.flags & Flags::END_HEADERS != 0,
            rejected,
            false,
            callback,
            callback_egress,
            result,
        )
    }

    fn start_header_block(
        &mut self,
        kind: HeaderBlockKind,
        mut fragment: SegmentCursor<'_>,
        end_headers: bool,
        rejected: bool,
        push_begin_emitted: bool,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) -> Result<(), (H2ErrorCode, &'static [u8])> {
        if fragment.remaining() > self.maximum_header_block_size() {
            return Err((H2ErrorCode::PROTOCOL_ERROR, b"header block is too large"));
        }
        let borrowed_bytes = if end_headers {
            fragment.contiguous()
        } else {
            None
        };
        let bytes = if borrowed_bytes.is_some() {
            Vec::new()
        } else {
            let mut bytes = Vec::with_capacity(fragment.remaining());
            while let Some(segment) = fragment.next_chunk() {
                bytes.extend_from_slice(segment);
            }
            bytes
        };
        self.header_block = Some(HeaderBlockState {
            kind,
            bytes,
            continuations_left: MAX_CONTINUATION_FRAMES,
            rejected,
            push_begin_emitted,
        });
        if end_headers {
            self.finish_header_block(borrowed_bytes, callback, callback_egress, result)?;
        }
        Ok(())
    }

    fn process_continuation(
        &mut self,
        header: FrameHeader,
        mut fragment: SegmentCursor<'_>,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) -> Result<(), (H2ErrorCode, &'static [u8])> {
        let maximum = self.maximum_header_block_size();
        let Some(block) = self.header_block.as_mut() else {
            return Err((H2ErrorCode::PROTOCOL_ERROR, b"unexpected CONTINUATION"));
        };
        if block.continuations_left == 0 {
            return Err((
                H2ErrorCode::ENHANCE_YOUR_CALM,
                b"too many CONTINUATION frames",
            ));
        }
        block.continuations_left -= 1;
        while let Some(segment) = fragment.next_chunk() {
            if block.bytes.len().saturating_add(segment.len()) > maximum {
                return Err((H2ErrorCode::PROTOCOL_ERROR, b"header block is too large"));
            }
            block.bytes.extend_from_slice(segment);
        }
        if header.flags & Flags::END_HEADERS != 0 {
            self.finish_header_block(None, callback, callback_egress, result)?;
        }
        Ok(())
    }

    fn finish_header_block(
        &mut self,
        borrowed_bytes: Option<&[u8]>,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) -> Result<(), (H2ErrorCode, &'static [u8])> {
        let block = self
            .header_block
            .take()
            .expect("completed header block must exist");
        let bytes = borrowed_bytes.unwrap_or(&block.bytes);
        let compression_error = |_| {
            (
                H2ErrorCode::COMPRESSION_ERROR,
                b"failed decoding HPACK header block" as &'static [u8],
            )
        };
        if block.rejected {
            self.hpack.decode(bytes).map_err(compression_error)?;
            self.pending_end_stream = None;
            return Ok(());
        }
        let (request, mut trailers) = match block.kind {
            HeaderBlockKind::Headers {
                request, trailers, ..
            } => (request, trailers),
            HeaderBlockKind::PushPromise { .. } => (true, false),
        };
        let memo_before = if !trailers
            && matches!(block.kind, HeaderBlockKind::Headers { .. })
            && bytes.len() <= MEMO_MAX_COMPRESSED
        {
            self.hpack.decode_memo_state()
        } else {
            None
        };
        let cached = memo_before.and_then(|state| self.reuse_decoded_header(bytes, request, state));
        let mut decoded_head_count = None;
        let mut message = if let Some(message) = cached {
            message
        } else {
            let mut decode = HeaderDecodeInfo::new(
                request,
                request && trailers,
                self.validate_headers,
                self.strict_validation,
                false,
            );
            decode.set_options(self.field_options);
            let head = !trailers && matches!(block.kind, HeaderBlockKind::Headers { .. });
            let mut presizing = PresizingDecode {
                decode: &mut decode,
                pending: if head {
                    presized_header_capacity(self.last_head_header_count)
                } else {
                    0
                },
                reserved: 0,
            };
            if self.parallel.direction().is_downstream() {
                presizing.reserve();
            }
            let decoded = self.hpack.decode_streaming(bytes, &mut presizing);
            #[cfg(test)]
            LAST_HEADER_PRESIZE.set(presizing.reserved);
            decoded.map_err(compression_error)?;
            if decode.parsing_error.is_some() || decode.decode_error.is_some() {
                self.finish_malformed_header_block(
                    block.kind,
                    block.push_begin_emitted,
                    trailers,
                    decode,
                    callback,
                    callback_egress,
                    result,
                );
                return Ok(());
            }
            if head {
                decoded_head_count = Some(decode.message.headers().len());
            }
            self.remember_decoded_header(bytes, request, memo_before, &mut decode.message);
            decode.message
        };
        if self.parallel.direction().is_upstream()
            && matches!(block.kind, HeaderBlockKind::Headers { .. })
            && !message.is_response()
        {
            trailers = true;
        }
        if let Some(count) = decoded_head_count.filter(|_| !trailers) {
            self.last_head_header_count = count;
        }
        let message_stream = match block.kind {
            HeaderBlockKind::Headers { stream, .. } => stream,
            HeaderBlockKind::PushPromise {
                promised_stream, ..
            } => promised_stream,
        };
        self.apply_header_completion_metadata(&mut message, message_stream);

        match block.kind {
            HeaderBlockKind::Headers {
                stream, end_stream, ..
            } => {
                if self.parallel.direction().is_upstream()
                    && stream & 1 == 1
                    && stream >= self.parallel.next_egress_stream_id()
                {
                    return Err((
                        H2ErrorCode::PROTOCOL_ERROR,
                        b"HEADERS received on idle upstream stream",
                    ));
                }
                if trailers {
                    let trailers = Box::new(message.extract_headers());
                    self.dispatch(
                        callback,
                        callback_egress,
                        result,
                        CodecEvent::TrailersComplete {
                            stream: StreamId::new(u64::from(stream)),
                            trailers,
                        },
                    );
                } else {
                    self.dispatch(
                        callback,
                        callback_egress,
                        result,
                        CodecEvent::MessageBegin {
                            stream: StreamId::new(u64::from(stream)),
                            message: &mut message,
                        },
                    );
                    if !end_stream {
                        message.set_chunked(true);
                    }
                    self.dispatch(
                        callback,
                        callback_egress,
                        result,
                        CodecEvent::HeadersComplete {
                            stream: StreamId::new(u64::from(stream)),
                            message,
                        },
                    );
                }
                if end_stream && self.pending_end_stream.take() == Some(stream) {
                    self.dispatch(
                        callback,
                        callback_egress,
                        result,
                        CodecEvent::MessageComplete {
                            stream: StreamId::new(u64::from(stream)),
                            upgraded: false,
                        },
                    );
                }
            }
            HeaderBlockKind::PushPromise {
                associated_stream,
                promised_stream,
            } => {
                if !block.push_begin_emitted {
                    self.dispatch_push_begin(
                        promised_stream,
                        associated_stream,
                        Some(&mut message),
                        callback,
                        callback_egress,
                        result,
                    );
                }
                message.set_chunked(true);
                self.dispatch(
                    callback,
                    callback_egress,
                    result,
                    CodecEvent::HeadersComplete {
                        stream: StreamId::new(u64::from(promised_stream)),
                        message,
                    },
                );
            }
        }
        Ok(())
    }

    #[cold]
    #[inline(never)]
    fn finish_malformed_header_block(
        &mut self,
        kind: HeaderBlockKind,
        push_begin_emitted: bool,
        trailers: bool,
        mut info: HeaderDecodeInfo,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) {
        let (stream, downstream_headers) = match kind {
            HeaderBlockKind::Headers { stream, .. } => (
                stream,
                self.parallel.direction().is_downstream() && !trailers,
            ),
            HeaderBlockKind::PushPromise {
                promised_stream, ..
            } => (promised_stream, false),
        };
        self.apply_header_completion_metadata(&mut info.message, stream);
        if downstream_headers {
            self.dispatch(
                callback,
                callback_egress,
                result,
                CodecEvent::MessageBegin {
                    stream: StreamId::new(u64::from(stream)),
                    message: &mut info.message,
                },
            );
            let mut error = HttpException::new(
                HttpExceptionDirection::Ingress,
                info.parsing_error
                    .as_deref()
                    .unwrap_or("malformed HTTP/2 request fields"),
            );
            error.set_http_status_code(400);
            error.set_proxygen_error(info.proxygen_error.unwrap_or(ProxygenError::PARSE_HEADER));
            error.set_partial_message(Some(*info.message));
            self.dispatch(
                callback,
                callback_egress,
                result,
                CodecEvent::Error {
                    stream: StreamId::new(u64::from(stream)),
                    error: &error,
                    new_stream: true,
                },
            );
            self.pending_end_stream = None;
            return;
        }
        if let HeaderBlockKind::PushPromise {
            associated_stream,
            promised_stream,
        } = kind
            && !push_begin_emitted
        {
            self.dispatch_push_begin(
                promised_stream,
                associated_stream,
                Some(&mut info.message),
                callback,
                callback_egress,
                result,
            );
        }
        if self.parallel.direction().is_upstream()
            && matches!(kind, HeaderBlockKind::Headers { .. })
        {
            self.dispatch(
                callback,
                callback_egress,
                result,
                CodecEvent::MessageBegin {
                    stream: StreamId::new(u64::from(stream)),
                    message: &mut info.message,
                },
            );
        }
        let (error_stream, header_error) = match kind {
            HeaderBlockKind::Headers { stream, .. } => (stream, true),
            HeaderBlockKind::PushPromise {
                associated_stream, ..
            } => (associated_stream, false),
        };
        let description = info
            .parsing_error
            .clone()
            .unwrap_or_else(|| "malformed HTTP/2 field section".to_owned());
        let partial_message = *info.message;
        self.stream_error(
            error_stream,
            H2ErrorCode::PROTOCOL_ERROR,
            description,
            header_error,
            Some(partial_message),
            callback,
            callback_egress,
            result,
        );
        if let HeaderBlockKind::PushPromise {
            promised_stream, ..
        } = kind
        {
            self.stream_error(
                promised_stream,
                H2ErrorCode::REFUSED_STREAM,
                b"malformed PUSH_PROMISE",
                false,
                None,
                callback,
                callback_egress,
                result,
            );
        }
        self.pending_end_stream = None;
    }

    fn reuse_decoded_header(
        &mut self,
        bytes: &[u8],
        request: bool,
        state: DecodeMemoState,
    ) -> Option<Box<HttpMessage>> {
        let memo = self.decoder_memo.as_ref()?;
        let ready = memo.ready.as_ref()?;
        if memo.token != state.token
            || memo.request != request
            || memo.validate != self.validate_headers
            || memo.strict != self.strict_validation
            || memo.options != self.field_options
            || memo.wire != bytes
        {
            return None;
        }
        let started = Instant::now();
        let mut message = ready.message.clone();
        message.set_start_time(started);
        assert!(
            self.hpack
                .replay_decode_memo(state.token, ready.static_refs)
        );
        Some(message)
    }

    fn remember_decoded_header(
        &mut self,
        bytes: &[u8],
        request: bool,
        before: Option<DecodeMemoState>,
        message: &mut HttpMessage,
    ) {
        let eligible = before.and_then(|state| {
            self.hpack
                .decode_memo_delta(state)
                .map(|delta| (state.token, delta))
        });
        let Some((token, static_refs)) = eligible.filter(|_| {
            message.ingress_header_size().uncompressed <= MEMO_MAX_UNCOMPRESSED
                && (request
                    || message
                        .response_status()
                        .is_some_and(|status| status >= 200))
        }) else {
            self.decoder_memo = None;
            return;
        };
        let hash = self.memo_fingerprint_hash(bytes);
        let (validate, strict, options) = (
            self.validate_headers,
            self.strict_validation,
            self.field_options,
        );
        match &mut self.decoder_memo {
            Some(memo)
                if memo.token == token
                    && memo.request == request
                    && memo.validate == validate
                    && memo.strict == strict
                    && memo.options == options
                    && memo.len == bytes.len()
                    && memo.hash == hash =>
            {
                #[cfg(test)]
                {
                    MEMO_COPIES.set(MEMO_COPIES.get() + 1);
                    self.decoder_template_copies += 1;
                }
                memo.wire.clear();
                memo.wire.extend_from_slice(bytes);
                message.headers_mut().share_entries();
                memo.ready = Some(DecodedTemplate {
                    message: Box::new(message.clone()),
                    static_refs,
                });
            }
            Some(memo) => {
                memo.token = token;
                memo.request = request;
                memo.validate = validate;
                memo.strict = strict;
                memo.options = options;
                memo.len = bytes.len();
                memo.hash = hash;
                memo.wire.clear();
                memo.ready = None;
            }
            None => {
                self.decoder_memo = Some(DecoderMemo {
                    token,
                    request,
                    validate,
                    strict,
                    options,
                    len: bytes.len(),
                    hash,
                    wire: Vec::new(),
                    ready: None,
                });
            }
        }
    }

    fn memo_fingerprint_hash(&self, bytes: &[u8]) -> u64 {
        #[cfg(test)]
        if self.equal_memo_fingerprints {
            return 0;
        }
        self.hpack.decode_block_hash(bytes)
    }

    /// Gives every header block the same decoder-memo fingerprint hash.
    #[cfg(test)]
    pub(crate) const fn force_equal_memo_fingerprints(&mut self) {
        self.equal_memo_fingerprints = true;
    }

    #[cfg(test)]
    pub(crate) const fn header_template_copies(&self) -> usize {
        self.decoder_template_copies
    }

    /// Returns and resets the number of header blocks copied into the decoder memo on this thread.
    #[cfg(test)]
    pub(crate) fn take_memo_copies() -> usize {
        MEMO_COPIES.replace(0)
    }

    /// Returns and resets how many stream lookups in the upgrade set this thread has made.
    #[cfg(test)]
    pub(crate) fn take_upgraded_stream_probes() -> usize {
        UPGRADED_STREAM_PROBES.replace(0)
    }

    /// Returns and resets `(temporary queue pushes, fallback boxes)` made while framing HEADERS and
    /// PUSH_PROMISE blocks on this thread.
    #[cfg(test)]
    pub(crate) fn take_header_frame_allocations() -> (usize, usize) {
        HEADER_FRAME_ALLOCATIONS.replace((0, 0))
    }

    #[cfg(test)]
    pub(crate) fn header_loop_counts(&self) -> (usize, usize) {
        self.hpack.header_loop_counts()
    }

    /// `(length_bound, bound)` for the fields `generate` would encode for `message`, where `bound`
    /// is the encoded-size bound of the fields `for_each` visits.
    #[cfg(test)]
    pub(crate) fn header_length_bounds(
        &self,
        message: &HttpMessage,
        extra: Option<&HttpHeaders>,
    ) -> (Option<usize>, Option<usize>) {
        let fields = self.prepare_message_headers(message, extra).unwrap();
        let mut bound = Some(EMPTY_BLOCK_BOUND);
        fields.for_each(|header| {
            bound = add_input_bound(bound, &header);
        });
        (fields.length_bound(), bound)
    }

    fn dispatch_push_begin(
        &mut self,
        promised_stream: u32,
        associated_stream: u32,
        message: Option<&mut HttpMessage>,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) -> bool {
        if !self.parallel.stream_allowed(promised_stream) {
            return false;
        }
        self.dispatch(
            callback,
            callback_egress,
            result,
            CodecEvent::PushMessageBegin {
                stream: StreamId::new(u64::from(promised_stream)),
                associated_stream: StreamId::new(u64::from(associated_stream)),
                message,
            },
        );
        true
    }

    /// Forgets `stream`'s pending WebSocket upgrade and returns whether it had one. Most
    /// connections never have an upgrade pending, so an empty set is not hashed into.
    fn remove_upgraded_stream(&mut self, stream: u32) -> bool {
        if self.upgraded_streams.is_empty() {
            return false;
        }
        #[cfg(test)]
        UPGRADED_STREAM_PROBES.set(UPGRADED_STREAM_PROBES.get() + 1);
        self.upgraded_streams.remove(&stream)
    }

    fn apply_header_completion_metadata(&mut self, message: &mut HttpMessage, stream: u32) {
        message.set_advanced_protocol_string(b"h2");
        let websocket_request = self.parallel.direction().is_downstream()
            && message.method().ok().flatten() == Some(HttpMethod::Connect)
            && message.upgrade_protocol() == Some(b"websocket".as_slice());
        let websocket_response = self.parallel.direction().is_upstream()
            && self.remove_upgraded_stream(stream)
            && message.status_code().ok() == Some(200);
        if websocket_request || websocket_response {
            message.set_ingress_websocket_upgrade();
        }
        if self.user_agent.is_empty() {
            let user_agent = message
                .headers()
                .get_single_or_empty(HttpHeaderCode::UserAgent);
            if !user_agent.is_empty() {
                self.user_agent = user_agent.to_vec().into_boxed_slice();
            }
        }
    }

    fn check_new_stream(
        &mut self,
        stream: u32,
        trailers_allowed: bool,
    ) -> Result<bool, (H2ErrorCode, &'static [u8])> {
        let existing = stream <= self.parallel.last_incoming_stream_id();
        if stream == 0 || (!trailers_allowed && existing) {
            return Err((
                H2ErrorCode::PROTOCOL_ERROR,
                b"invalid new stream identifier",
            ));
        }
        if !existing && self.parallel.closing_state() != ClosingState::Closed {
            self.parallel.note_incoming_stream(stream);
        }
        if self.parallel.is_initiated_stream(stream) {
            return Err((
                H2ErrorCode::PROTOCOL_ERROR,
                b"peer used a locally initiated stream identifier",
            ));
        }
        Ok(trailers_allowed && existing)
    }

    fn maximum_header_block_size(&self) -> usize {
        usize::try_from(
            self.egress_settings
                .value_or(SettingsId::MaxHeaderListSize, 0),
        )
        .unwrap_or(usize::MAX)
    }

    fn generated_frame(
        callback: &mut dyn HttpCodecCallback,
        stream: u32,
        frame_type: FrameType,
        length: usize,
    ) {
        callback.on_event(CodecEvent::GeneratedFrameHeader {
            stream: StreamId::new(u64::from(stream)),
            frame_type: frame_type.as_u8(),
            length: u64::try_from(length).unwrap_or(u64::MAX),
            version: 0,
        });
    }

    fn prepare_message_headers<'a>(
        &self,
        message: &'a HttpMessage,
        extra: Option<&'a HttpHeaders>,
    ) -> Result<PreparedMessageHeaders<'a>, HttpError> {
        let pseudo = if message.is_request() {
            let method = message.request_method().unwrap_or_default();
            let connect = method == HttpMethod::Connect.as_str();
            let protocol = message.upgrade_protocol();
            let authority = message.headers().get_single_or_empty(HttpHeaderCode::Host);
            let scheme_and_path = if !connect || protocol.is_some() {
                let path = message
                    .request_url()
                    .ok_or_else(|| local_error("request has no URL state"))?;
                Some((message.scheme(), if path.is_empty() { b"/" } else { path }))
            } else {
                None
            };
            MessagePseudoHeaders::Request {
                method: method.as_bytes(),
                scheme_and_path,
                authority,
                protocol,
            }
        } else if message.is_response() {
            let mut status = message
                .response_status()
                .ok_or_else(|| local_error("response has no status state"))?;
            let mut bytes = [0; 5];
            let mut start = bytes.len() as u8;
            loop {
                start -= 1;
                bytes[usize::from(start)] = b'0' + (status % 10) as u8;
                status /= 10;
                if status == 0 {
                    break;
                }
            }
            MessagePseudoHeaders::Response { bytes, start }
        } else {
            return Err(local_error("cannot encode an empty HTTP message"));
        };
        let date = (message.is_response()
            && self.add_date_to_response
            && !message.headers().exists(HttpHeaderCode::Date)
            && extra.is_none_or(|headers| !headers.exists(HttpHeaderCode::Date)))
        .then(HttpMessage::format_date_header);
        Ok(PreparedMessageHeaders {
            pseudo,
            headers: message.headers(),
            extra,
            date,
            verbatim_values: self.field_options.verbatim_values,
        })
    }

    fn generate_headers(
        &mut self,
        output: &mut BufQueue,
        stream: u32,
        associated_stream: Option<u32>,
        message: &HttpMessage,
        end_stream: bool,
        extra: Option<&HttpHeaders>,
        callback: &mut dyn HttpCodecCallback,
    ) -> Result<GeneratedFrame, HttpError> {
        if !self.parallel.stream_allowed(stream) {
            return Ok(GeneratedFrame::default());
        }
        let max_frame = self.max_send_frame_size();
        let first_prefix = if associated_stream.is_some() { 4 } else { 0 };
        let capacity = output.remaining_capacity();
        let payload_budget = maximum_header_payload(capacity, max_frame, first_prefix)
            .ok_or_else(|| buffer_capacity_error(capacity, FRAME_HEADER_SIZE + first_prefix))?;
        let headroom = FRAME_HEADER_SIZE + first_prefix;
        let mut block = {
            let fields = self.prepare_message_headers(message, extra)?;
            if let Some(associated) = associated_stream {
                frame::validate_push_promise_streams(associated, stream)
                    .map_err(local_frame_error)?;
            } else {
                frame::validate_stream(stream, false).map_err(local_frame_error)?;
            }
            self.hpack
                .encode_inputs_block(&fields, payload_budget, headroom)
                .map_err(buffer_error)?
        };
        let header_size = self.hpack.encoded_size();
        let encoded_length = block.as_bytes().len();
        let first_capacity = max_frame.saturating_sub(first_prefix);
        let first_length = encoded_length.min(first_capacity);
        let first_done = first_length == encoded_length;
        let first_type = if associated_stream.is_some() {
            FrameType::PUSH_PROMISE
        } else {
            FrameType::HEADERS
        };
        let continuation_stream = associated_stream.unwrap_or(stream);
        let mut flags = if first_done { Flags::END_HEADERS } else { 0 };
        if associated_stream.is_none() && end_stream {
            flags |= Flags::END_STREAM;
        }
        let prefix = frame::frame_header_prefix(
            first_length + first_prefix,
            first_type,
            flags,
            continuation_stream,
        )
        .map_err(local_frame_error)?;
        if associated_stream.is_some() {
            assert!(block.prepend(&stream.to_be_bytes()));
        }
        assert!(block.prepend(&prefix));
        let written = headroom + first_length;
        let mut bytes_written = written;
        let generated = if first_done {
            if !output
                .try_append_compact_to_tail(block.as_bytes(), &BufQueue::new(0))
                .map_err(buffer_error)?
            {
                #[cfg(test)]
                HEADER_FRAME_ALLOCATIONS.with(|counts| {
                    let (pushes, boxes) = counts.get();
                    counts.set((pushes, boxes + 1));
                });
                output.push_copy(block.as_bytes()).map_err(buffer_error)?;
            }
            self.hpack.recycle_block(block);
            Vec::new()
        } else {
            let mut encoded = BufQueue::new(capacity);
            #[cfg(test)]
            HEADER_FRAME_ALLOCATIONS.with(|counts| {
                let (pushes, boxes) = counts.get();
                counts.set((pushes + 1, boxes));
            });
            encoded.push_vec(block.into_bytes()).map_err(buffer_error)?;
            let mut staged = encoded.split_at_most(written);
            let generated = write_continuation_frames(
                &mut encoded,
                &mut staged,
                continuation_stream,
                first_length,
                encoded_length,
                max_frame,
                &mut bytes_written,
            )?;
            output.append(&mut staged).map_err(buffer_error)?;
            generated
        };
        if associated_stream.is_none()
            && message.is_request()
            && message.is_egress_websocket_upgrade()
        {
            self.upgraded_streams.insert(stream);
        }
        Self::generated_frame(callback, stream, first_type, written);
        for (stream, frame_type, length) in generated {
            Self::generated_frame(callback, stream, frame_type, length);
        }
        Ok(GeneratedFrame {
            bytes_written,
            header_size: Some(header_size),
            payload_offset: None,
        })
    }

    fn generate_trailers(
        &mut self,
        output: &mut BufQueue,
        stream: u32,
        trailers: &HttpHeaders,
        callback: &mut dyn HttpCodecCallback,
    ) -> Result<GeneratedFrame, HttpError> {
        let mut fields = Vec::new();
        for_each_modern_header(trailers, HttpHeaderCode::None, |_, name, value| {
            fields.push(EncodeHeader::borrowed(HeaderName::new(name), value));
        });
        if fields.is_empty() {
            return self.generate_eom(output, stream, callback);
        }
        frame::validate_stream(stream, false).map_err(local_frame_error)?;
        let max_frame = self.max_send_frame_size();
        let capacity = output.remaining_capacity();
        let payload_budget = maximum_header_payload(capacity, max_frame, 0)
            .ok_or_else(|| buffer_capacity_error(capacity, FRAME_HEADER_SIZE))?;
        let mut encoded = BufQueue::new(payload_budget);
        self.hpack
            .encode_inputs_to_buf_queue(fields, &mut encoded)
            .map_err(buffer_error)?;
        let header_size = self.hpack.encoded_size();
        let encoded_length = encoded.chain_length();
        let first_length = encoded_length.min(max_frame);
        let first = encoded.split_at_most(first_length);
        let mut staged = BufQueue::new(capacity);
        let written = frame::write_headers(
            &mut staged,
            first,
            stream,
            None,
            true,
            first_length == encoded_length,
        )
        .map_err(local_frame_error)?;
        let mut bytes_written = written;
        let generated = if first_length < encoded_length {
            write_continuation_frames(
                &mut encoded,
                &mut staged,
                stream,
                first_length,
                encoded_length,
                max_frame,
                &mut bytes_written,
            )?
        } else {
            Vec::new()
        };
        output.append(&mut staged).map_err(buffer_error)?;
        Self::generated_frame(callback, stream, FrameType::HEADERS, written);
        for (stream, frame_type, length) in generated {
            Self::generated_frame(callback, stream, frame_type, length);
        }
        Ok(GeneratedFrame {
            bytes_written,
            header_size: Some(header_size),
            payload_offset: None,
        })
    }

    fn generate_eom(
        &mut self,
        output: &mut BufQueue,
        stream: u32,
        callback: &mut dyn HttpCodecCallback,
    ) -> Result<GeneratedFrame, HttpError> {
        if !self.parallel.stream_allowed(stream) {
            return Ok(GeneratedFrame::default());
        }
        let written = if self.compact_data {
            frame::write_compact_data(output, BufQueue::new(0), stream, true)
                .map_err(local_frame_error)?
        } else {
            let mut staged = BufQueue::new(output.remaining_capacity());
            let written = frame::write_data(
                &mut staged,
                BufQueue::new(0),
                stream,
                None,
                true,
                self.reuse_payload_headroom,
            )
            .map_err(local_frame_error)?;
            output.append(&mut staged).map_err(buffer_error)?;
            written
        };
        self.remove_upgraded_stream(stream);
        Self::generated_frame(callback, stream, FrameType::DATA, written);
        Ok(GeneratedFrame {
            bytes_written: written,
            header_size: None,
            payload_offset: Some(FRAME_HEADER_SIZE as u64),
        })
    }

    #[cold]
    #[inline(never)]
    fn generate_connection_preface(
        &self,
        output: &mut BufQueue,
    ) -> Result<GeneratedFrame, HttpError> {
        if self.parallel.direction().is_downstream() {
            return Ok(GeneratedFrame::default());
        }
        output.push_copy(CONNECTION_PREFACE).map_err(buffer_error)?;
        Ok(GeneratedFrame {
            bytes_written: CONNECTION_PREFACE.len(),
            header_size: None,
            payload_offset: None,
        })
    }

    #[cold]
    #[inline(never)]
    fn generate_push_promise(
        &mut self,
        output: &mut BufQueue,
        stream: StreamId,
        associated_stream: StreamId,
        message: &HttpMessage,
        callback: &mut dyn HttpCodecCallback,
    ) -> Result<GeneratedFrame, HttpError> {
        self.generate_headers(
            output,
            wire_stream(stream)?,
            Some(wire_stream(associated_stream)?),
            message,
            false,
            None,
            callback,
        )
    }

    #[cold]
    #[inline(never)]
    fn generate_padding(
        &mut self,
        output: &mut BufQueue,
        stream: StreamId,
        length: u16,
        callback: &mut dyn HttpCodecCallback,
    ) -> Result<GeneratedFrame, HttpError> {
        let stream = wire_stream(stream)?;
        let max = self.max_send_frame_size().min(usize::from(u16::MAX));
        let mut staged = BufQueue::new(output.remaining_capacity());
        let mut bytes_written = 0;
        let mut generated_lengths = Vec::new();
        while bytes_written < usize::from(length) {
            let amount = max.min(usize::from(length));
            let written = frame::write_padding(
                &mut staged,
                stream,
                u16::try_from(amount).map_err(|_| local_error("padding is too large"))?,
            )
            .map_err(local_frame_error)?;
            generated_lengths.push(written);
            bytes_written += written;
        }
        output.append(&mut staged).map_err(buffer_error)?;
        for length in generated_lengths {
            Self::generated_frame(callback, stream, FrameType::PADDING, length);
        }
        Ok(GeneratedFrame {
            bytes_written,
            header_size: None,
            payload_offset: None,
        })
    }

    #[cold]
    #[inline(never)]
    fn generate_reset(
        &mut self,
        output: &mut BufQueue,
        stream: StreamId,
        code: H2ErrorCode,
        callback: &mut dyn HttpCodecCallback,
    ) -> Result<GeneratedFrame, HttpError> {
        let stream = wire_stream(stream)?;
        if !self.parallel.stream_allowed(stream) {
            return Ok(GeneratedFrame::default());
        }
        let mut staged = BufQueue::new(output.remaining_capacity());
        let written =
            frame::write_rst_stream(&mut staged, stream, code).map_err(local_frame_error)?;
        output.append(&mut staged).map_err(buffer_error)?;
        if self.pending_end_stream == Some(stream) {
            self.pending_end_stream = None;
        }
        self.remove_upgraded_stream(stream);
        Self::generated_frame(callback, stream, FrameType::RST_STREAM, written);
        Ok(simple_generated(written))
    }

    #[cold]
    #[inline(never)]
    fn generate_goaway(
        &mut self,
        output: &mut BufQueue,
        last_stream: StreamId,
        code: H2ErrorCode,
        debug_data: Option<BufQueue>,
        callback: &mut dyn HttpCodecCallback,
    ) -> Result<GeneratedFrame, HttpError> {
        if self.parallel.closing_state() == ClosingState::Closed {
            return Ok(GeneratedFrame::default());
        }
        let requested = if last_stream == StreamId::MAX {
            if code != H2ErrorCode::NO_ERROR
                || !self.parallel.is_reusable()
                || self.parallel.is_waiting_to_drain()
            {
                self.parallel.last_incoming_stream_id()
            } else {
                MAX_STREAM_ID
            }
        } else {
            wire_stream_allow_zero(last_stream)?
        };
        let mut next_parallel = self.parallel.clone();
        let Some(last_stream) = next_parallel.send_goaway(requested, code != H2ErrorCode::NO_ERROR)
        else {
            return Ok(GeneratedFrame::default());
        };
        let mut staged = BufQueue::new(output.remaining_capacity());
        let written = frame::write_goaway(&mut staged, last_stream, code, debug_data)
            .map_err(local_frame_error)?;
        output.append(&mut staged).map_err(buffer_error)?;
        self.parallel = next_parallel;
        Self::generated_frame(callback, 0, FrameType::GOAWAY, written);
        Ok(simple_generated(written))
    }

    #[cold]
    #[inline(never)]
    fn generate_ping_request(
        output: &mut BufQueue,
        data: Option<u64>,
        callback: &mut dyn HttpCodecCallback,
    ) -> Result<GeneratedFrame, HttpError> {
        let mut staged = BufQueue::new(output.remaining_capacity());
        let written = frame::write_ping(&mut staged, data.unwrap_or_else(fresh_ping_token), false)
            .map_err(local_frame_error)?;
        output.append(&mut staged).map_err(buffer_error)?;
        Self::generated_frame(callback, 0, FrameType::PING, written);
        Ok(simple_generated(written))
    }

    #[cold]
    #[inline(never)]
    fn generate_ping_reply(
        output: &mut BufQueue,
        data: u64,
        callback: &mut dyn HttpCodecCallback,
    ) -> Result<GeneratedFrame, HttpError> {
        let mut staged = BufQueue::new(output.remaining_capacity());
        let written = frame::write_ping(&mut staged, data, true).map_err(local_frame_error)?;
        output.append(&mut staged).map_err(buffer_error)?;
        Self::generated_frame(callback, 0, FrameType::PING, written);
        Ok(simple_generated(written))
    }

    #[cold]
    #[inline(never)]
    fn generate_settings(
        &mut self,
        output: &mut BufQueue,
        settings: &[CodecSetting],
        callback: &mut dyn HttpCodecCallback,
    ) -> Result<GeneratedFrame, HttpError> {
        let mut wire_settings = Vec::new();
        let mut next_pending_table_max_size = self.pending_table_max_size;
        let mut next_max_uncompressed = None;
        for setting in settings {
            let Some(id) = SettingsId::from_u16(setting.identifier) else {
                continue;
            };
            if id == SettingsId::EnablePush && self.parallel.direction().is_downstream() {
                continue;
            }
            if id == SettingsId::EnableConnectProtocol && setting.value == 0 {
                continue;
            }
            let value = u32::try_from(setting.value)
                .map_err(|_| local_error("setting exceeds HTTP/2 wire width"))?;
            if id == SettingsId::HeaderTableSize {
                if next_pending_table_max_size.is_some() {
                    continue;
                }
                next_pending_table_max_size = Some(value);
            }
            if id == SettingsId::MaxHeaderListSize {
                next_max_uncompressed = Some(setting.value);
            }
            wire_settings.push(Setting {
                identifier: setting.identifier,
                value,
            });
        }
        let mut staged = BufQueue::new(output.remaining_capacity());
        let written =
            frame::write_settings(&mut staged, &wire_settings).map_err(local_frame_error)?;
        output.append(&mut staged).map_err(buffer_error)?;
        self.pending_table_max_size = next_pending_table_max_size;
        if let Some(maximum) = next_max_uncompressed {
            self.hpack.set_max_uncompressed(maximum);
        }
        Self::generated_frame(callback, 0, FrameType::SETTINGS, written);
        Ok(simple_generated(written))
    }

    #[cold]
    #[inline(never)]
    fn generate_settings_ack(
        output: &mut BufQueue,
        callback: &mut dyn HttpCodecCallback,
    ) -> Result<GeneratedFrame, HttpError> {
        let mut staged = BufQueue::new(output.remaining_capacity());
        let written = frame::write_settings_ack(&mut staged).map_err(local_frame_error)?;
        output.append(&mut staged).map_err(buffer_error)?;
        Self::generated_frame(callback, 0, FrameType::SETTINGS, written);
        Ok(simple_generated(written))
    }

    #[cold]
    #[inline(never)]
    fn generate_priority(
        &mut self,
        output: &mut BufQueue,
        stream: StreamId,
        priority: HttpPriority,
        callback: &mut dyn HttpCodecCallback,
    ) -> Result<GeneratedFrame, HttpError> {
        let stream = wire_stream(stream)?;
        if !self.parallel.stream_allowed(stream) {
            return Ok(GeneratedFrame::default());
        }
        let mut staged = BufQueue::new(output.remaining_capacity());
        let written =
            frame::write_rfc9218_priority(&mut staged, stream, http_priority_to_string(priority))
                .map_err(local_frame_error)?;
        output.append(&mut staged).map_err(buffer_error)?;
        Self::generated_frame(callback, stream, FrameType::RFC9218_PRIORITY, written);
        Ok(simple_generated(written))
    }
}

impl HttpCodec for Http2Codec {
    fn protocol(&self) -> CodecProtocol {
        CodecProtocol::Http2
    }

    fn transport_direction(&self) -> TransportDirection {
        self.parallel.direction()
    }

    fn capabilities(&self) -> CodecCapabilities {
        CodecCapabilities {
            stream_flow_control: true,
            session_flow_control: true,
            parallel_requests: true,
            push_transactions: self.supports_push_transactions(),
            default_window_size: INITIAL_WINDOW,
        }
    }

    fn create_stream(&mut self) -> StreamId {
        self.parallel.create_stream()
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
        let mut result = CodecIngressResult::default();
        if self.fatal_error.is_some() {
            return result;
        }
        let observes_frame_headers = callback.observes_frame_headers();
        let input_length = input.chain_length();
        let mut remaining = SegmentCursor::from_queue(input);
        let mut offset = 0;
        while offset <= input_length && self.fatal_error.is_none() {
            match self.ingress_state {
                IngressState::ConnectionPreface => {
                    if remaining.remaining() < CONNECTION_PREFACE.len() {
                        break;
                    }
                    offset += CONNECTION_PREFACE.len();
                    if !self.consume_connection_preface(
                        &mut remaining,
                        callback,
                        callback_egress,
                        &mut result,
                    ) {
                        break;
                    }
                }
                IngressState::FirstFrameHeader | IngressState::FrameHeader => {
                    if remaining.remaining() < FRAME_HEADER_SIZE {
                        break;
                    }
                    let first = self.ingress_state == IngressState::FirstFrameHeader;
                    let header = frame::parse_frame_header_cursor(&mut remaining)
                        .expect("a complete fixed-size frame header must parse");
                    offset += FRAME_HEADER_SIZE;
                    if matches!(header.frame_type, FrameType::HEADERS | FrameType::DATA)
                        && header.flags & Flags::END_STREAM != 0
                    {
                        self.pending_end_stream = Some(header.stream);
                    }
                    if observes_frame_headers {
                        self.dispatch(
                            callback,
                            callback_egress,
                            &mut result,
                            CodecEvent::FrameHeader {
                                stream: StreamId::new(u64::from(header.stream)),
                                flags: header.flags,
                                length: u64::from(header.length),
                                frame_type: u64::from(header.frame_type.as_u8()),
                                version: 0,
                            },
                        );
                    }
                    if first && header.frame_type != FrameType::SETTINGS {
                        self.connection_error(
                            H2ErrorCode::PROTOCOL_ERROR,
                            b"first frame after preface is not SETTINGS",
                            callback,
                            callback_egress,
                            &mut result,
                        );
                        break;
                    }
                    if header.length > self.max_receive_frame_size() {
                        self.connection_error(
                            H2ErrorCode::FRAME_SIZE_ERROR,
                            b"frame exceeds configured maximum size",
                            callback,
                            callback_egress,
                            &mut result,
                        );
                        break;
                    }
                    self.ingress_state = if header.frame_type == FrameType::DATA {
                        IngressState::Data(DataState {
                            header,
                            remaining: header.length as usize,
                            trailing_padding: 0,
                            prefix_pending: header.flags & Flags::PADDED != 0,
                        })
                    } else {
                        IngressState::FramePayload(header)
                    };
                }
                IngressState::FramePayload(header) => {
                    let length = header.length as usize;
                    if remaining.remaining() < length {
                        break;
                    }
                    let payload = remaining
                        .take(length)
                        .expect("the frame payload length was checked");
                    offset += length;
                    self.ingress_state = IngressState::FrameHeader;
                    if let Err((code, message)) = self.process_frame_payload(
                        header,
                        payload,
                        callback,
                        callback_egress,
                        &mut result,
                    ) {
                        self.connection_error(
                            code,
                            message,
                            callback,
                            callback_egress,
                            &mut result,
                        );
                    }
                }
                IngressState::Data(_) => {
                    let consumed =
                        self.process_data(&mut remaining, callback, callback_egress, &mut result);
                    offset += consumed;
                    if consumed == 0 && matches!(self.ingress_state, IngressState::Data(_)) {
                        break;
                    }
                }
                IngressState::Failed => break,
            }
        }
        result.consumed = offset;
        result
    }

    fn on_ingress_eof(
        &mut self,
        _callback: &mut dyn HttpCodecCallback,
        _callback_egress: &mut BufQueue,
    ) -> roxygen_codec::CodecCallbackResult {
        roxygen_codec::CodecCallbackResult::default()
    }

    fn is_reusable(&self) -> bool {
        self.fatal_error.is_none() && self.parallel.is_reusable()
    }

    fn is_waiting_to_drain(&self) -> bool {
        self.parallel.is_waiting_to_drain()
    }

    fn close_on_egress_complete(&self) -> bool {
        false
    }

    fn last_incoming_stream_id(&self) -> StreamId {
        StreamId::new(u64::from(self.parallel.last_incoming_stream_id()))
    }

    fn has_partial_request_head(&self) -> bool {
        self.header_block.as_ref().is_some_and(|block| {
            !block.rejected
                && matches!(
                    block.kind,
                    HeaderBlockKind::Headers {
                        stream,
                        request: true,
                        trailers: false,
                        ..
                    } if self.parallel.stream_allowed(stream)
                )
        })
    }

    fn generate(
        &mut self,
        output: &mut BufQueue,
        frame: EgressFrame<'_>,
        callback: &mut dyn HttpCodecCallback,
    ) -> Result<GeneratedFrame, HttpError> {
        match frame {
            EgressFrame::ConnectionPreface => self.generate_connection_preface(output),
            EgressFrame::Headers {
                stream,
                message,
                end_stream,
                extra_headers,
            } => {
                let stream = wire_stream(stream)?;
                self.generate_headers(
                    output,
                    stream,
                    None,
                    message,
                    end_stream,
                    extra_headers,
                    callback,
                )
            }
            EgressFrame::PushPromise {
                stream,
                associated_stream,
                message,
                end_stream: _,
            } => self.generate_push_promise(output, stream, associated_stream, message, callback),
            EgressFrame::Body {
                stream,
                mut body,
                padding,
                end_stream,
            } => {
                let stream = wire_stream(stream)?;
                if !self.parallel.stream_allowed(stream) {
                    return Ok(GeneratedFrame::default());
                }
                let max_frame = self.max_send_frame_size();
                let direct = body.chain_length() <= max_frame
                    && body
                        .chain_length()
                        .checked_add(
                            FRAME_HEADER_SIZE + padding.map_or(0, |amount| usize::from(amount) + 1),
                        )
                        .is_some_and(|length| length <= output.remaining_capacity());
                let mut staged = if body.chain_length() > max_frame {
                    BufQueue::with_segment_capacity(
                        output.remaining_capacity(),
                        frame::data_frames_segment_bound(
                            &body,
                            max_frame,
                            padding,
                            output.remaining_capacity(),
                        ),
                    )
                } else {
                    BufQueue::new(output.remaining_capacity())
                };
                let destination = if direct { &mut *output } else { &mut staged };
                let mut bytes_written = 0;
                let mut first_payload_offset = None;
                // Every frame before the last carries exactly `max_frame`
                // bytes, so all of them have the same length.
                let mut full_frames = 0;
                let mut full_frame_length = 0;
                while body.chain_length() > max_frame {
                    let written = frame::write_data_from_with(
                        destination,
                        &mut body,
                        max_frame,
                        stream,
                        padding,
                        Some(&mut self.data_headers),
                    )
                    .map_err(local_frame_error)?;
                    first_payload_offset
                        .get_or_insert((FRAME_HEADER_SIZE + usize::from(padding.is_some())) as u64);
                    full_frames += 1;
                    full_frame_length = written;
                    bytes_written += written;
                }
                let written = if self.compact_data && padding.is_none() {
                    frame::write_compact_data_with(
                        destination,
                        body,
                        stream,
                        end_stream,
                        Some(&mut self.data_headers),
                    )
                } else {
                    frame::write_data_with(
                        destination,
                        body,
                        stream,
                        padding,
                        end_stream,
                        Some(&mut self.data_headers),
                    )
                }
                .map_err(local_frame_error)?;
                first_payload_offset
                    .get_or_insert((FRAME_HEADER_SIZE + usize::from(padding.is_some())) as u64);
                bytes_written += written;
                if !direct {
                    output.append(&mut staged).map_err(buffer_error)?;
                }
                for _ in 0..full_frames {
                    Self::generated_frame(callback, stream, FrameType::DATA, full_frame_length);
                }
                Self::generated_frame(callback, stream, FrameType::DATA, written);
                Ok(GeneratedFrame {
                    bytes_written,
                    header_size: None,
                    payload_offset: first_payload_offset,
                })
            }
            EgressFrame::ChunkHeader { .. } | EgressFrame::ChunkTerminator { .. } => {
                Ok(GeneratedFrame::default())
            }
            EgressFrame::Trailers { stream, trailers } => {
                self.generate_trailers(output, wire_stream(stream)?, trailers, callback)
            }
            EgressFrame::Padding { stream, length } => {
                self.generate_padding(output, stream, length, callback)
            }
            EgressFrame::EndOfMessage { stream } => {
                self.generate_eom(output, wire_stream(stream)?, callback)
            }
            EgressFrame::Reset { stream, code } => {
                self.generate_reset(output, stream, code, callback)
            }
            EgressFrame::Goaway {
                last_stream,
                code,
                debug_data,
            } => self.generate_goaway(output, last_stream, code, debug_data, callback),
            EgressFrame::PingRequest { data } => {
                Self::generate_ping_request(output, data, callback)
            }
            EgressFrame::PingReply { data } => Self::generate_ping_reply(output, data, callback),
            EgressFrame::Settings { settings } => {
                self.generate_settings(output, settings, callback)
            }
            EgressFrame::SettingsAck => Self::generate_settings_ack(output, callback),
            EgressFrame::WindowUpdate { stream, amount } => {
                let stream = wire_stream_allow_zero(stream)?;
                if !self.parallel.stream_allowed(stream) {
                    return Ok(GeneratedFrame::default());
                }
                let mut staged = BufQueue::new(output.remaining_capacity());
                let written = frame::write_window_update(&mut staged, stream, amount)
                    .map_err(local_frame_error)?;
                output.append(&mut staged).map_err(buffer_error)?;
                Self::generated_frame(callback, stream, FrameType::WINDOW_UPDATE, written);
                Ok(simple_generated(written))
            }
            EgressFrame::Priority { stream, priority }
            | EgressFrame::PushPriority { stream, priority } => {
                self.generate_priority(output, stream, priority, callback)
            }
        }
    }
}

/// Writes the remaining header block bytes in `encoded` as CONTINUATION
/// frames, the last carrying END_HEADERS.
#[cold]
#[inline(never)]
fn write_continuation_frames(
    encoded: &mut BufQueue,
    staged: &mut BufQueue,
    stream: u32,
    first_length: usize,
    encoded_length: usize,
    max_frame: usize,
    bytes_written: &mut usize,
) -> Result<Vec<(u32, FrameType, usize)>, HttpError> {
    let mut generated = Vec::new();
    let mut offset = first_length;
    while offset < encoded_length {
        let length = (encoded_length - offset).min(max_frame);
        let chunk = encoded.split_at_most(length);
        offset += length;
        let written = frame::write_continuation(staged, stream, offset == encoded_length, chunk)
            .map_err(local_frame_error)?;
        generated.push((stream, FrameType::CONTINUATION, written));
        *bytes_written += written;
    }
    Ok(generated)
}

fn header_block_stream(kind: HeaderBlockKind) -> u32 {
    match kind {
        HeaderBlockKind::Headers { stream, .. } => stream,
        HeaderBlockKind::PushPromise {
            associated_stream, ..
        } => associated_stream,
    }
}

#[cold]
#[inline(never)]
fn frame_error(error: FrameError) -> (H2ErrorCode, &'static [u8]) {
    match error.peer_code() {
        Some(code) => (code, b"malformed HTTP/2 frame"),
        None => (H2ErrorCode::PROTOCOL_ERROR, b"invalid HTTP/2 frame input"),
    }
}

#[cold]
#[inline(never)]
fn local_error(message: impl AsRef<[u8]>) -> HttpError {
    HttpError::with_message(HttpErrorCode::PROTOCOL_ERROR, message)
}

#[cold]
#[inline(never)]
fn local_frame_error(error: FrameError) -> HttpError {
    let code = error
        .peer_code()
        .map_or(HttpErrorCode::PROTOCOL_ERROR, |code| {
            HttpErrorCode::from_u16(code.as_u32() as u16)
        });
    HttpError::with_message(code, error.to_string())
}

#[cold]
#[inline(never)]
fn buffer_error(error: roxygen_core::buf_queue::BufQueueError) -> HttpError {
    HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string())
}

fn maximum_header_payload(capacity: usize, max_frame: usize, first_prefix: usize) -> Option<usize> {
    if capacity < FRAME_HEADER_SIZE + first_prefix || max_frame <= first_prefix {
        return None;
    }
    if capacity == usize::MAX {
        return Some(capacity);
    }
    let remaining = capacity - FRAME_HEADER_SIZE - first_prefix;
    let first_capacity = max_frame - first_prefix;
    if remaining <= first_capacity {
        return Some(remaining);
    }
    let leftover = remaining - first_capacity;
    let continuation_size = max_frame + FRAME_HEADER_SIZE;
    Some(
        first_capacity
            + (leftover / continuation_size) * max_frame
            + (leftover % continuation_size).saturating_sub(FRAME_HEADER_SIZE),
    )
}

#[cold]
#[inline(never)]
fn buffer_capacity_error(available: usize, needed: usize) -> HttpError {
    HttpError::with_message(
        HttpErrorCode::INTERNAL_ERROR,
        format!("output buffer has {available} bytes available but needs at least {needed}"),
    )
}

fn trim_lws(mut value: &[u8]) -> &[u8] {
    while value
        .first()
        .is_some_and(|byte| matches!(*byte, b' ' | b'\t'))
    {
        value = &value[1..];
    }
    while value
        .last()
        .is_some_and(|byte| matches!(*byte, b' ' | b'\t'))
    {
        value = &value[..value.len() - 1];
    }
    value
}

fn wire_stream(stream: StreamId) -> Result<u32, HttpError> {
    let stream = wire_stream_allow_zero(stream)?;
    if stream == 0 {
        Err(local_error("frame requires a nonzero stream identifier"))
    } else {
        Ok(stream)
    }
}

fn wire_stream_allow_zero(stream: StreamId) -> Result<u32, HttpError> {
    u32::try_from(stream.get())
        .ok()
        .filter(|stream| *stream <= MAX_STREAM_ID)
        .ok_or_else(|| local_error("stream identifier exceeds HTTP/2 range"))
}

/// The capacity a decoded head's header map grows to when it holds `previous` headers, capped at
/// `MAX_HEADER_PRESIZE`, or 0 when the map's first allocation already holds them.
const fn presized_header_capacity(previous: usize) -> usize {
    if previous <= HEADER_MAP_INITIAL_CAPACITY {
        0
    } else if previous >= MAX_HEADER_PRESIZE {
        MAX_HEADER_PRESIZE
    } else {
        previous.next_power_of_two()
    }
}

/// Decodes a head's fields into a header map pre-sized to `pending` entries. Downstream, trailers
/// are known before decoding, so a head reserves up front. Upstream, a block is a response head
/// only when it carries `:status`, which precedes every regular field, so the reservation waits
/// for the first regular field and is dropped for trailers.
struct PresizingDecode<'a> {
    decode: &'a mut HeaderDecodeInfo,
    pending: usize,
    reserved: usize,
}

impl PresizingDecode<'_> {
    fn reserve(&mut self) {
        if self.pending != 0 {
            self.decode.message.headers_mut().reserve(self.pending);
            self.reserved = self.pending;
            self.pending = 0;
        }
    }
}

impl StreamingCallback for PresizingDecode<'_> {
    fn on_header(&mut self, name: &HeaderName, value: &[u8]) {
        if self.pending != 0 && !name.as_bytes().starts_with(b":") {
            if self.decode.has_status() {
                self.reserve();
            } else {
                self.pending = 0;
            }
        }
        StreamingCallback::on_header(self.decode, name, value);
    }

    fn on_headers_complete(&mut self, decoded_size: HttpHeaderSize, acknowledge: bool) {
        StreamingCallback::on_headers_complete(self.decode, decoded_size, acknowledge);
    }

    fn on_decode_error(&mut self, error: HpackError) {
        StreamingCallback::on_decode_error(self.decode, error);
    }
}

const fn simple_generated(bytes_written: usize) -> GeneratedFrame {
    GeneratedFrame {
        bytes_written,
        header_size: None,
        payload_offset: None,
    }
}

fn fresh_ping_token() -> u64 {
    loop {
        let token = NEXT_PING_TOKEN.fetch_add(1, Ordering::Relaxed);
        if token != 0 {
            return token;
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use roxygen_codec::CodecEvent;
    use roxygen_codec::HttpCodec;
    use roxygen_codec::HttpCodecCallback;
    use roxygen_core::buf_queue::BufQueue;
    use roxygen_core::headers::HttpHeaders;
    use roxygen_core::http::TransportDirection;

    use super::Http2Codec;
    use super::LAST_HEADER_PRESIZE;
    use crate::codec_contract::consumer_request_fields;
    use crate::codec_contract::consumer_response_fields;
    use crate::codec_contract::encoded_headers;
    use crate::constants::CONNECTION_PREFACE;
    use crate::frame;
    use crate::hpack::HpackHeader;

    type Fields = Vec<(Vec<u8>, Vec<u8>)>;

    /// The fields of every delivered head and trailer block, in order, and the delivered maps.
    #[derive(Default)]
    struct Delivered(Vec<Fields>, Vec<HttpHeaders>);

    impl HttpCodecCallback for Delivered {
        fn on_event(&mut self, event: CodecEvent<'_>) {
            let headers = match event {
                CodecEvent::HeadersComplete { mut message, .. } => message.extract_headers(),
                CodecEvent::TrailersComplete { trailers, .. } => *trailers,
                _ => return,
            };
            self.0.push(
                headers
                    .iter()
                    .map(|(_, name, value)| (name.to_vec(), value.to_vec()))
                    .collect(),
            );
            self.1.push(headers);
        }
    }

    /// Whether `headers` holds `capacity` entries without growing. A shared map that already has
    /// room is left sharing by `reserve`; one without room stops sharing, which moves its entries.
    fn has_room_for(headers: &mut HttpHeaders, capacity: usize) -> bool {
        let first_value = |headers: &HttpHeaders| headers.iter().next().unwrap().2.as_ptr();
        headers.share_entries();
        let mut probe = headers.clone();
        probe.reserve(capacity - headers.len());
        first_value(&probe) == first_value(headers)
    }

    fn queue(bytes: &[u8]) -> BufQueue {
        BufQueue::from_bytes(Bytes::copy_from_slice(bytes), bytes.len()).unwrap()
    }

    fn empty_settings() -> Vec<u8> {
        let mut settings = BufQueue::new(1024);
        frame::write_settings(&mut settings, &[]).unwrap();
        settings
            .segments()
            .flat_map(|segment| segment.iter().copied())
            .collect()
    }

    fn opened_downstream() -> Http2Codec {
        let mut codec = Http2Codec::new(TransportDirection::Downstream);
        let mut ingress = CONNECTION_PREFACE.to_vec();
        ingress.extend(empty_settings());
        ingest(&mut codec, &ingress, &mut Delivered::default());
        codec
    }

    fn ingest(codec: &mut Http2Codec, wire: &[u8], delivered: &mut Delivered) {
        let result = codec.on_ingress(&queue(wire), delivered, &mut BufQueue::new(64 * 1024));
        assert_eq!(result.consumed, wire.len());
    }

    /// Ingests one header block and returns the capacity its decode reserved.
    fn presize_for(codec: &mut Http2Codec, wire: &[u8], delivered: &mut Delivered) -> usize {
        LAST_HEADER_PRESIZE.set(usize::MAX);
        ingest(codec, wire, delivered);
        LAST_HEADER_PRESIZE.get()
    }

    #[test]
    fn second_large_head_is_presized_to_its_growth_capacity() {
        let mut codec = opened_downstream();
        let mut delivered = Delivered::default();
        let first = encoded_headers(1, &consumer_request_fields(0), true);
        let second = encoded_headers(3, &consumer_request_fields(1), true);
        assert_eq!(presize_for(&mut codec, &first, &mut delivered), 0);
        assert_eq!(presize_for(&mut codec, &second, &mut delivered), 32);
        assert_eq!(delivered.0.len(), 2);
    }

    /// A request with the four pseudo-headers and `regular` regular headers.
    fn request_with(regular: usize) -> Vec<HpackHeader> {
        let mut fields = vec![
            HpackHeader::new(b":method", b"GET"),
            HpackHeader::new(b":scheme", b"https"),
            HpackHeader::new(b":path", b"/"),
            HpackHeader::new(b":authority", b"example.com"),
        ];
        fields.extend(
            (0..regular).map(|index| HpackHeader::new(format!("x-field-{index}"), b"value")),
        );
        fields
    }

    /// An upstream codec that has opened `streams` streams and received SETTINGS.
    fn opened_upstream(streams: usize) -> Http2Codec {
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        for _ in 0..streams {
            codec.create_stream();
        }
        ingest(&mut codec, &empty_settings(), &mut Delivered::default());
        codec
    }

    /// A response with `:status` and `regular` regular headers.
    fn response_with(regular: usize) -> Vec<HpackHeader> {
        let mut fields = vec![HpackHeader::new(b":status", b"200")];
        fields.extend(request_with(regular).split_off(4));
        fields
    }

    /// The fields of `wire`'s one head, decoded by a fresh `codec`, which never pre-sizes it.
    fn unpresized_fields(mut codec: Http2Codec, wire: &[u8]) -> Fields {
        let mut delivered = Delivered::default();
        assert_eq!(presize_for(&mut codec, wire, &mut delivered), 0);
        delivered.0.pop().unwrap()
    }

    #[test]
    fn small_heads_are_never_presized() {
        let mut codec = opened_downstream();
        let mut delivered = Delivered::default();
        for index in 0..10_u32 {
            let wire = encoded_headers(2 * index + 1, &request_with(4), true);
            assert_eq!(presize_for(&mut codec, &wire, &mut delivered), 0);
        }
        assert_eq!(delivered.0.len(), 10);
    }

    #[test]
    fn trailers_neither_use_nor_update_the_hint() {
        let mut codec = opened_downstream();
        let mut delivered = Delivered::default();
        let head = encoded_headers(1, &consumer_request_fields(0), false);
        let trailers = encoded_headers(
            1,
            &[
                HpackHeader::new(b"x-checksum", b"0123abcd"),
                HpackHeader::new(b"x-status", b"ok"),
            ],
            true,
        );
        let next = encoded_headers(3, &consumer_request_fields(1), true);
        assert_eq!(presize_for(&mut codec, &head, &mut delivered), 0);
        assert_eq!(presize_for(&mut codec, &trailers, &mut delivered), 0);
        assert_eq!(delivered.0[1].len(), 2);
        assert_eq!(presize_for(&mut codec, &next, &mut delivered), 32);
    }

    #[test]
    fn presize_is_capped() {
        let mut codec = opened_downstream();
        let mut delivered = Delivered::default();
        let first = encoded_headers(1, &request_with(99), true);
        let second = encoded_headers(3, &request_with(99), true);
        assert_eq!(presize_for(&mut codec, &first, &mut delivered), 0);
        assert_eq!(presize_for(&mut codec, &second, &mut delivered), 64);
        assert_eq!(delivered.0[1].len(), 100);
        assert_eq!(
            delivered.0[0],
            unpresized_fields(opened_downstream(), &first)
        );
        assert_eq!(
            delivered.0[1],
            unpresized_fields(opened_downstream(), &second)
        );
    }

    #[test]
    fn small_head_after_large_is_bounded() {
        let mut codec = opened_downstream();
        let mut delivered = Delivered::default();
        let large = encoded_headers(1, &request_with(59), true);
        let small = encoded_headers(3, &request_with(4), true);
        let third = encoded_headers(5, &request_with(4), true);
        assert_eq!(presize_for(&mut codec, &large, &mut delivered), 0);
        assert_eq!(delivered.0[0].len(), 60);
        assert_eq!(presize_for(&mut codec, &small, &mut delivered), 64);
        assert!(has_room_for(&mut delivered.1[1], 64));
        assert!(!has_room_for(&mut delivered.1[1], 65));
        assert_eq!(
            delivered.0[1],
            unpresized_fields(opened_downstream(), &small)
        );
        assert_eq!(presize_for(&mut codec, &third, &mut delivered), 0);
    }

    #[test]
    fn upstream_trailers_neither_use_nor_update_the_hint() {
        let mut codec = opened_upstream(2);
        let mut delivered = Delivered::default();
        let head = encoded_headers(1, &consumer_response_fields(0), false);
        let trailers = encoded_headers(
            1,
            &[
                HpackHeader::new(b"x-checksum", b"0123abcd"),
                HpackHeader::new(b"x-status", b"ok"),
            ],
            true,
        );
        let next = encoded_headers(3, &consumer_response_fields(1), false);
        assert_eq!(presize_for(&mut codec, &head, &mut delivered), 0);
        assert_eq!(presize_for(&mut codec, &trailers, &mut delivered), 0);
        assert_eq!(delivered.0[1].len(), 2);
        assert_eq!(presize_for(&mut codec, &next, &mut delivered), 32);
    }

    #[test]
    fn bodiless_upstream_heads_use_and_update_the_hint() {
        let mut codec = opened_upstream(3);
        let mut delivered = Delivered::default();
        let first = encoded_headers(1, &consumer_response_fields(0), false);
        let bodiless = encoded_headers(3, &response_with(39), true);
        let next = encoded_headers(5, &consumer_response_fields(1), false);
        assert_eq!(presize_for(&mut codec, &first, &mut delivered), 0);
        assert_eq!(presize_for(&mut codec, &bodiless, &mut delivered), 32);
        assert_eq!(delivered.0[1].len(), 39);
        assert_eq!(presize_for(&mut codec, &next, &mut delivered), 64);
    }

    #[test]
    fn upstream_small_head_after_large_is_presized_before_its_first_field() {
        let mut codec = opened_upstream(3);
        let mut delivered = Delivered::default();
        let large = encoded_headers(1, &response_with(60), false);
        let small = encoded_headers(3, &response_with(4), true);
        let third = encoded_headers(5, &response_with(4), false);
        assert_eq!(presize_for(&mut codec, &large, &mut delivered), 0);
        assert_eq!(delivered.0[0].len(), 60);
        assert_eq!(presize_for(&mut codec, &small, &mut delivered), 64);
        assert!(has_room_for(&mut delivered.1[1], 64));
        assert!(
            !has_room_for(&mut delivered.1[1], 65),
            "a reservation made before the first field allocates exactly 64 entries"
        );
        assert_eq!(
            delivered.0[1],
            unpresized_fields(opened_upstream(2), &small)
        );
        assert_eq!(presize_for(&mut codec, &third, &mut delivered), 0);
    }
}
