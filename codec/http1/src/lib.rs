//! Incremental HTTP/1.0 and HTTP/1.1 parsing and framing.

use std::borrow::Cow;
#[cfg(test)]
use std::cell::Cell;
use std::marker::PhantomData;
use std::rc::Rc;

use base64::Engine as _;
use bytes::Bytes;
use rand::Rng as _;
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
use roxygen_codec::ProxygenError;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::buf_queue::BufQueueError;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::headers::HttpHeaders;
use roxygen_core::http::CodecProtocol;
use roxygen_core::http::HttpHeaderSize;
use roxygen_core::http::HttpMethod;
use roxygen_core::http::Method;
use roxygen_core::http::TransportDirection;
use roxygen_core::ids::StreamId;
use roxygen_core::message::HttpMessage;
use roxygen_core::rfc2616;
use sha1::Digest as _;
use sha1::Sha1;

const MAX_HEADER_BYTES: usize = 80 * 1024;
const WEBSOCKET_GUID: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
/// The longest request line besides its method and target.
const MAX_REQUEST_LINE_SUFFIX: usize = b"  HTTP/255.255\r\n".len();
/// The longest status line besides its reason.
const MAX_STATUS_LINE_SUFFIX: usize = b"HTTP/255.255 65535 \r\n".len();
/// The lines `generate_headers` may add after the message's headers: chunked
/// framing, Date, Connection with its keep-alive token, and the final CRLF.
const GENERATED_LINES: usize = b"Transfer-Encoding: chunked\r\n".len()
    + b"Date: Tue, 06 Oct 2026 00:00:00 GMT\r\n".len()
    + b"Connection: , keep-alive\r\n".len()
    + b"\r\n".len();
/// The generated websocket lines and Upgrade token, besides the stored accept
/// key.
const MAX_WEBSOCKET_LINES: usize = b"Upgrade: websocket\r\n".len()
    + b"Sec-WebSocket-Accept: \r\n".len()
    + b"Sec-WebSocket-Key: 0123456789012345678901==\r\n".len()
    + b", Upgrade".len();
const FROZEN_HTTP_METHODS: [&[u8]; 24] = [
    b"DELETE",
    b"GET",
    b"HEAD",
    b"POST",
    b"PUT",
    b"CONNECT",
    b"OPTIONS",
    b"TRACE",
    b"COPY",
    b"LOCK",
    b"MKCOL",
    b"MOVE",
    b"PROPFIND",
    b"PROPPATCH",
    b"UNLOCK",
    b"REPORT",
    b"MKACTIVITY",
    b"CHECKOUT",
    b"MERGE",
    b"M-SEARCH",
    b"NOTIFY",
    b"SUBSCRIBE",
    b"UNSUBSCRIBE",
    b"PATCH",
];
/// `valid_initial_prefix` can fail only within this many head bytes: an
/// upstream head that passes at five bytes starts with `HTTP/`, and a
/// downstream head that passes at one byte more than the longest frozen method
/// holds the delimiter that ends its method token.
const INITIAL_PREFIX_LIMIT: usize = longest_frozen_method() + 1;
/// The upstream share of `INITIAL_PREFIX_LIMIT`: an upstream head that passes
/// at `HTTP/`'s five bytes passes at every longer length, so its prefix can
/// change only while the head has at most this many bytes.
const UPSTREAM_PREFIX_LIMIT: usize = b"HTTP/".len();
/// The most header entries `parse_head` reserves before it adds a head's
/// headers. The reservation is a hint: peer bytes never size it beyond this
/// constant, and a head with more header lines grows its entries as it adds
/// them.
const HEAD_RESERVE_CAP: usize = 64;

/// One bit per entry of `FROZEN_HTTP_METHODS`.
const ALL_FROZEN_HTTP_METHODS: u32 = (1 << FROZEN_HTTP_METHODS.len()) - 1;

const _: () = assert!(
    UPSTREAM_PREFIX_LIMIT < INITIAL_PREFIX_LIMIT && INITIAL_PREFIX_LIMIT < MAX_HEADER_BYTES
);
const _: () = assert!(FROZEN_HTTP_METHODS.len() < u32::BITS as usize);

const fn longest_frozen_method() -> usize {
    let mut longest = 0;
    let mut index = 0;
    while index < FROZEN_HTTP_METHODS.len() {
        if FROZEN_HTTP_METHODS[index].len() > longest {
            longest = FROZEN_HTTP_METHODS[index].len();
        }
        index += 1;
    }
    longest
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum KeepaliveRequested {
    #[default]
    Unset,
    Enabled,
    Disabled,
}

#[derive(Clone, Debug, Default)]
enum ParseState {
    #[default]
    Idle,
    Head(Vec<u8>),
    FixedBody(u64),
    ChunkSize(ChunkSizeLine),
    ChunkBody(u64),
    /// Whether the chunk terminator's CR has been read.
    ChunkDelimiter(bool),
    Trailers(Vec<u8>),
    UntilEof,
    Upgraded,
    MessageComplete(bool),
}

/// The longest chunk-size line, its LF included.
const MAX_CHUNK_HEADER_BYTES: u8 = 128;

/// A chunk-size line read so far, kept as what its parse needs once its LF
/// arrives.
#[derive(Clone, Copy, Debug, Default)]
struct ChunkSizeLine {
    /// Bytes read, the LF included.
    length: u8,
    size: ChunkSizeToken,
    /// Whether the last byte read was a CR.
    after_cr: bool,
    /// Whether a CR was followed by a byte other than the line's LF, so that the
    /// line holds a CR besides one just before its LF.
    stray_cr: bool,
}

/// Why [`ChunkSizeLine::read`] stopped before the end of its input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ChunkSizeLineEnd {
    /// The line's LF was read.
    Lf,
    /// The last byte read took the line past `MAX_CHUNK_HEADER_BYTES`.
    TooLarge,
}

impl ChunkSizeLine {
    /// Reads `input` up to and including the line's LF, or up to the byte that
    /// takes the line past `MAX_CHUNK_HEADER_BYTES`, LF or not. Returns the
    /// bytes read and why reading stopped, or `None` when the input ran out.
    fn read(&mut self, input: &[u8]) -> (usize, Option<ChunkSizeLineEnd>) {
        for (index, byte) in input.iter().enumerate() {
            self.length += 1;
            if self.length > MAX_CHUNK_HEADER_BYTES {
                return (index + 1, Some(ChunkSizeLineEnd::TooLarge));
            }
            if *byte == b'\n' {
                return (index + 1, Some(ChunkSizeLineEnd::Lf));
            }
            self.stray_cr |= self.after_cr;
            self.after_cr = *byte == b'\r';
            self.size = self.size.push(*byte);
        }
        (input.len(), None)
    }
}

#[cfg(test)]
std::thread_local! {
    static CHUNK_DELIMITER_TURNS: Cell<usize> = const { Cell::new(0) };
}

/// Turns of the ingress parse loop in `ParseState::ChunkDelimiter` on this
/// thread, read by the count oracles.
#[cfg(test)]
fn chunk_delimiter_turns() -> usize {
    CHUNK_DELIMITER_TURNS.with(Cell::get)
}

/// The chunk size read so far, from the line's bytes before its first `;`
/// with HTTP whitespace trimmed from both ends: nonempty hex digits whose value
/// fits a `u64`, or invalid. A CR counts as whitespace, as the trim treats it;
/// the line is rejected for any CR but one just before its LF.
#[derive(Clone, Copy, Debug, Default)]
enum ChunkSizeToken {
    /// Whitespace only.
    #[default]
    Leading,
    /// Hex digits after any leading whitespace.
    Digits(u64),
    /// Hex digits followed by whitespace.
    Trailing(u64),
    /// Not a hex number that fits a `u64`.
    Invalid,
    /// The size as it stood at the first `;`.
    Ended(Option<u64>),
}

impl ChunkSizeToken {
    fn push(self, byte: u8) -> Self {
        let whitespace = matches!(byte, b' ' | b'\t' | b'\r' | b'\n');
        match self {
            Self::Ended(_) => self,
            _ if byte == b';' => Self::Ended(self.value()),
            Self::Digits(size) if whitespace => Self::Trailing(size),
            Self::Leading | Self::Trailing(_) | Self::Invalid if whitespace => self,
            Self::Leading => hex_digit(byte).map_or(Self::Invalid, Self::Digits),
            Self::Digits(size) => hex_digit(byte)
                .and_then(|digit| size.checked_mul(16)?.checked_add(digit))
                .map_or(Self::Invalid, Self::Digits),
            Self::Trailing(_) | Self::Invalid => Self::Invalid,
        }
    }

    fn value(self) -> Option<u64> {
        match self {
            Self::Digits(size) | Self::Trailing(size) => Some(size),
            Self::Ended(size) => size,
            Self::Leading | Self::Invalid => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ParseErrorKind {
    Header,
    HeaderContent,
    Body,
}

/// Why a head scan stopped before the end of its segment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HeadScanStop {
    Complete,
    TooLarge,
    InvalidStartLine,
    InvalidHeaderByte,
}

/// The first `end` bytes of a scanned segment belong to the head, the stopping
/// byte included.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HeadScan {
    end: usize,
    stop: Option<HeadScanStop>,
}

/// The most LF positions a head scan records.
const RECORDED_LINE_FEEDS: usize = 32;

const _: () = assert!(MAX_HEADER_BYTES <= u32::MAX as usize);

/// The positions of the LFs a head scan reads, the first
/// `RECORDED_LINE_FEEDS` of them, so that a head parsed in place is split into
/// lines without another search.
struct LineFeedPositions {
    positions: [u32; RECORDED_LINE_FEEDS],
    count: usize,
}

impl LineFeedPositions {
    fn new() -> Self {
        Self {
            positions: [0; RECORDED_LINE_FEEDS],
            count: 0,
        }
    }

    /// Records the next LF, at `index`, which is inside a head and so below
    /// `MAX_HEADER_BYTES`.
    #[inline]
    fn record(&mut self, index: usize) {
        if let Some(position) = self.positions.get_mut(self.count) {
            *position = index as u32;
        }
        self.count += 1;
    }

    /// The recorded LFs of a head of `length` bytes whose LFs this recorded,
    /// each of them in order.
    fn known(&self, length: usize) -> KnownLineFeeds<'_> {
        match self.positions.get(..self.count) {
            Some(positions) => KnownLineFeeds {
                positions,
                searched_to: length,
            },
            None => KnownLineFeeds {
                positions: &self.positions,
                searched_to: self.positions[RECORDED_LINE_FEEDS - 1] as usize + 1,
            },
        }
    }
}

/// LF positions in a head: every LF before `searched_to`, in order.
#[derive(Clone, Copy, Debug)]
struct KnownLineFeeds<'a> {
    positions: &'a [u32],
    searched_to: usize,
}

/// The slot of `byte` in a [`METHOD_BYTE_MASKS`] row. Every byte of a frozen
/// method name is an uppercase letter or `-`.
const fn method_byte_slot(byte: u8) -> Option<usize> {
    match byte {
        b'A'..=b'Z' => Some((byte - b'A') as usize),
        b'-' => Some(26),
        _ => None,
    }
}

/// For each index into a frozen method name and each byte slot, the methods,
/// one bit per entry of `FROZEN_HTTP_METHODS`, whose name holds that byte at
/// that index.
static METHOD_BYTE_MASKS: [[u32; 27]; INITIAL_PREFIX_LIMIT - 1] = {
    let mut masks = [[0; 27]; INITIAL_PREFIX_LIMIT - 1];
    let mut method = 0;
    while method < FROZEN_HTTP_METHODS.len() {
        let name = FROZEN_HTTP_METHODS[method];
        let mut index = 0;
        while index < name.len() {
            let Some(slot) = method_byte_slot(name[index]) else {
                panic!("a frozen method name holds a byte without a slot");
            };
            masks[index][slot] |= 1 << method;
            index += 1;
        }
        method += 1;
    }
    masks
};

/// `valid_initial_prefix` of a head that grows one byte at a time, with each
/// byte read once. Downstream it keeps the frozen methods that the method token
/// still prefixes and whether a delimiter has ended the token; upstream, which
/// byte of `HTTP/` comes next. A prefix that fails stays failed: every longer
/// head holds the same mismatch.
struct PrefixMatch {
    upstream: bool,
    length: usize,
    candidates: u32,
    delimited: bool,
    valid: bool,
}

impl PrefixMatch {
    fn new(upstream: bool) -> Self {
        Self {
            upstream,
            length: 0,
            candidates: ALL_FROZEN_HTTP_METHODS,
            delimited: false,
            valid: true,
        }
    }

    /// Whether every longer head passes as well: the head passes, and
    /// downstream a delimiter has ended the method token, or upstream the head
    /// holds all of `HTTP/`.
    #[inline]
    fn settled(&self) -> bool {
        self.valid
            && if self.upstream {
                self.length >= b"HTTP/".len()
            } else {
                self.delimited
            }
    }

    /// Appends `byte` to the head, and returns whether the head passes
    /// `valid_initial_prefix`.
    #[inline]
    fn push(&mut self, byte: u8) -> bool {
        let index = self.length;
        self.length += 1;
        if !self.valid {
            return false;
        }
        if self.upstream {
            self.valid = b"HTTP/".get(index).is_none_or(|expected| *expected == byte);
        } else if !self.delimited {
            if matches!(byte, b' ' | b'\t' | b'\r' | b'\n') {
                self.delimited = true;
            } else {
                #[cfg(test)]
                count_helper_work(|work| &work.prefix_method_steps, 1);
                let matching = match (METHOD_BYTE_MASKS.get(index), method_byte_slot(byte)) {
                    (Some(row), Some(slot)) => row[slot],
                    _ => 0,
                };
                self.candidates &= matching;
                self.valid = self.candidates != 0;
            }
        }
        self.valid
    }
}

/// Work counts of the head path, read by the count oracles.
#[cfg(test)]
#[derive(Debug, Default)]
struct HeadWork {
    /// Head bytes the start-line prefix check reads.
    prefix_reads: Cell<usize>,
    /// Head bytes the start-line prefix check reads from outside the scan's
    /// `buffered` and `segment`, that is, from a copy.
    prefix_copies: Cell<usize>,
    /// `word_scan::count_lf` passes over a parsed head.
    line_feed_counts: Cell<usize>,
    /// Steps of the head scan's main loop: a byte checked on its own, or a
    /// search for the next control byte.
    scan_steps: Cell<usize>,
    /// Owned copies of a parsed request's method name.
    method_copies: Cell<usize>,
    /// Nonempty Upgrade values that `parse_head` reads from anywhere but the
    /// message's header, that is, from a copy.
    upgrade_copies: Cell<usize>,
    /// Owned copies of a nonempty User-Agent value.
    user_agent_copies: Cell<usize>,
}

/// Work counts of the egress path, read by the count oracles.
#[cfg(test)]
#[derive(Debug, Default)]
struct EgressWork {
    /// Vectors with a heap capacity that `push_output` queues. Strings built
    /// by `format!`, queued or not, are counted in `FORMATTED_STRINGS`.
    byte_buffers: Cell<usize>,
    /// Whole queues that `append_output` moves onto another queue.
    appended_queues: Cell<usize>,
}

#[cfg(test)]
std::thread_local! {
    /// Strings built by `format!` in this file after the counting macro below.
    static FORMATTED_STRINGS: Cell<usize> = const { Cell::new(0) };
}

/// Strings built by `format!` on this thread, read by the count oracles.
#[cfg(test)]
fn formatted_strings() -> usize {
    FORMATTED_STRINGS.with(Cell::get)
}

#[cfg(test)]
fn count_work(counter: &Cell<usize>, amount: usize) {
    counter.set(counter.get() + amount);
}

/// Work counts of the head-path helpers that run without a codec, read by the
/// count oracles.
#[cfg(test)]
#[derive(Debug, Default)]
struct HelperWork {
    /// Frozen methods that `PrefixMatch::push` matches a byte against.
    prefix_method_steps: Cell<usize>,
    /// Request-line bytes that `request_line_is_http_09` checks for a
    /// delimiter.
    http_09_delimiter_reads: Cell<usize>,
    /// Bytes that `HeaderLines` searches for an LF.
    line_search_bytes: Cell<usize>,
    /// Header-value bytes that `valid_header_value` checks one at a time.
    value_byte_checks: Cell<usize>,
    /// Version numbers that `parse_version` parses as decimal digits.
    version_number_parses: Cell<usize>,
    /// Loads that `generate_headers` makes to check header values for a CR or
    /// an LF: a byte, or a word of eight.
    egress_cr_lf_loads: Cell<usize>,
}

#[cfg(test)]
std::thread_local! {
    static HELPER_WORK: HelperWork = HelperWork::default();
}

/// The count `counter` selects from this thread's `HelperWork`.
#[cfg(test)]
fn helper_work(counter: fn(&HelperWork) -> &Cell<usize>) -> usize {
    HELPER_WORK.with(|work| counter(work).get())
}

#[cfg(test)]
fn count_helper_work(counter: fn(&HelperWork) -> &Cell<usize>, amount: usize) {
    HELPER_WORK.with(|work| count_work(counter(work), amount));
}

/// The bytes that end an egress message: none, or for chunked egress the final
/// CRLF, preceded by the last chunk when it was not written yet.
#[derive(Clone, Copy, Debug)]
enum MessageEnd {
    Nothing,
    Crlf,
    LastChunk,
}

impl MessageEnd {
    fn bytes(self) -> &'static [u8] {
        match self {
            Self::Nothing => b"",
            Self::Crlf => b"\r\n",
            Self::LastChunk => b"0\r\n\r\n",
        }
    }

    /// The bytes after a body that opened a chunk: the chunk's CRLF, then
    /// [`bytes`](Self::bytes), as one static string.
    fn after_chunk(self) -> &'static [u8] {
        match self {
            Self::Nothing => b"\r\n",
            Self::Crlf => b"\r\n\r\n",
            Self::LastChunk => b"\r\n0\r\n\r\n",
        }
    }
}

#[derive(Debug)]
struct ParsedHead {
    message: Box<HttpMessage>,
    body: IngressBody,
    informational: bool,
    upgrade: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IngressBody {
    None,
    Fixed(u64),
    Chunked,
    UntilEof,
}

/// A worker-local, incremental HTTP/1 codec.
pub struct Http1xCodec {
    direction: TransportDirection,
    force_1_1: bool,
    strict_validation: bool,
    downstream_upgrade_as_ordinary: bool,
    /// Roxygen-only (`DIV-036`): a request head that ends its message gets no body framing.
    unframed_bodiless_requests: bool,
    /// The last request stream parsed under `downstream_upgrade_as_ordinary`, or unspecified.
    last_ordinary_request: StreamId,
    upstream_upgrade_as_ordinary: bool,
    /// The last request an upstream codec generated was generated under
    /// `upstream_upgrade_as_ordinary`, so its response is read as an ordinary response.
    ordinary_request_sent: bool,
    release_egress_after_request: bool,
    state: ParseState,
    current_message: Option<HttpMessage>,
    ingress_stream: StreamId,
    egress_stream: StreamId,
    parser_paused: bool,
    parser_error: bool,
    request_pending: bool,
    response_pending: bool,
    egress_chunked: bool,
    in_chunk: bool,
    last_chunk_written: bool,
    keepalive: bool,
    disable_keepalive_pending: bool,
    keepalive_requested: KeepaliveRequested,
    connect_request: bool,
    head_request: bool,
    expect_no_response_body: bool,
    may_chunk_egress: bool,
    informational_response: bool,
    ingress_upgrade: bool,
    ingress_upgrade_complete: bool,
    egress_upgrade: bool,
    upgrade_header: Vec<u8>,
    websocket_accept_key: Vec<u8>,
    user_agent: Vec<u8>,
    expect_http_09_response: bool,
    /// The last head reservation: the entries reserved, and the entries the
    /// message already held.
    #[cfg(test)]
    head_reserve: Option<(usize, usize)>,
    #[cfg(test)]
    head_work: HeadWork,
    #[cfg(test)]
    egress_work: EgressWork,
    _worker_local: PhantomData<Rc<()>>,
}

#[derive(Clone)]
struct EgressRollback {
    egress_stream: StreamId,
    request_pending: bool,
    response_pending: bool,
    egress_chunked: bool,
    in_chunk: bool,
    last_chunk_written: bool,
    keepalive: bool,
    disable_keepalive_pending: bool,
    connect_request: bool,
    ordinary_request_sent: bool,
    head_request: bool,
    expect_no_response_body: bool,
    may_chunk_egress: bool,
    informational_response: bool,
    ingress_upgrade: bool,
    egress_upgrade: bool,
    upgrade_header: Vec<u8>,
    websocket_accept_key: Vec<u8>,
    expect_http_09_response: bool,
}

impl Http1xCodec {
    /// Creates a codec using the reference codec's compatibility validation.
    pub fn new(direction: TransportDirection) -> Self {
        Self::with_options(direction, false, false)
    }

    /// Creates a codec with explicit HTTP/1.1 forcing and strict validation.
    pub fn with_options(
        direction: TransportDirection,
        force_1_1: bool,
        strict_validation: bool,
    ) -> Self {
        Self {
            direction,
            force_1_1,
            strict_validation,
            downstream_upgrade_as_ordinary: false,
            unframed_bodiless_requests: false,
            last_ordinary_request: StreamId::UNSPECIFIED,
            upstream_upgrade_as_ordinary: false,
            ordinary_request_sent: false,
            release_egress_after_request: false,
            state: ParseState::Idle,
            current_message: None,
            ingress_stream: StreamId::UNSPECIFIED,
            egress_stream: StreamId::UNSPECIFIED,
            parser_paused: false,
            parser_error: false,
            request_pending: false,
            response_pending: false,
            egress_chunked: false,
            in_chunk: false,
            last_chunk_written: false,
            keepalive: true,
            disable_keepalive_pending: false,
            keepalive_requested: KeepaliveRequested::Unset,
            connect_request: false,
            head_request: false,
            expect_no_response_body: false,
            may_chunk_egress: false,
            informational_response: false,
            ingress_upgrade: false,
            ingress_upgrade_complete: false,
            egress_upgrade: false,
            upgrade_header: Vec::new(),
            websocket_accept_key: Vec::new(),
            user_agent: Vec::new(),
            expect_http_09_response: false,
            #[cfg(test)]
            head_reserve: None,
            #[cfg(test)]
            head_work: HeadWork::default(),
            #[cfg(test)]
            egress_work: EgressWork::default(),
            _worker_local: PhantomData,
        }
    }

    /// Creates a response-only codec with an explicitly known chunking policy.
    pub fn make_response_codec(may_chunk_egress: bool) -> Self {
        let mut codec = Self::new(TransportDirection::Downstream);
        codec.may_chunk_egress = may_chunk_egress;
        codec
    }

    /// Changes header and URL validation for subsequent messages.
    pub fn set_strict_validation(&mut self, strict: bool) {
        self.strict_validation = strict;
    }

    /// Roxygen-only (`DIV-030`): parses each downstream request whose head is read after this
    /// call, including one with an `Upgrade` header or a `CONNECT` method, as an ordinary
    /// request, so parsing continues behind it. Off by default, which is the reference
    /// codec's upgrade handling. It has no effect on an upstream codec.
    pub fn set_downstream_upgrade_as_ordinary(&mut self, enabled: bool) {
        self.downstream_upgrade_as_ordinary = enabled;
    }

    /// Roxygen-only (`DIV-031`): sends each request an upstream codec generates after this call,
    /// including one with an `Upgrade` header or a `CONNECT` method, as an ordinary request, and
    /// reads its response as an ordinary response. Off by default, which is the reference
    /// codec's upgrade handling. It has no effect on a downstream codec.
    pub fn set_upstream_upgrade_as_ordinary(&mut self, enabled: bool) {
        self.upstream_upgrade_as_ordinary = enabled;
    }

    /// Roxygen-only (`DIV-036`): an upstream request whose head ends the message (it has no
    /// body) and that carries neither `Content-Length` nor a chunked `Transfer-Encoding` is sent
    /// with no body framing at all, as Hyper sends a request with an empty body, instead of the
    /// reference codec's `Transfer-Encoding: chunked` and empty last chunk for a `POST` (or,
    /// where chunking is not allowed, `Connection: close`). A head with framing of the caller's
    /// is generated exactly as without the option (a caller's chunked `Transfer-Encoding` with no
    /// body still closes the connection), and a request that has a body is framed as before.
    /// Off by default. It has no effect on a downstream codec.
    pub fn set_unframed_bodiless_requests(&mut self, enabled: bool) {
        self.unframed_bodiless_requests = enabled;
    }

    /// Releases upstream egress ownership as soon as the request completes.
    pub fn set_release_egress_after_request(&mut self, release: bool) {
        self.release_egress_after_request = release;
    }

    /// Resolves a downstream upgrade paused after request headers.
    pub fn resolve_downstream_upgrade(&mut self, response: &HttpMessage) -> Option<bool> {
        if !self.direction.is_downstream() {
            return None;
        }
        // `DIV-030`: as `generate_headers` refuses the switch, so does this resolution.
        let accepted = if self.is_at_or_before_an_ordinary_request(self.ingress_stream) {
            false
        } else if self.connect_request {
            let mut response = response.clone();
            response
                .status_code()
                .is_ok_and(|status| (200..300).contains(&status))
        } else {
            response.is_egress_websocket_upgrade()
        };
        self.ingress_upgrade = accepted;
        self.ingress_upgrade_complete = false;
        match self.state {
            ParseState::MessageComplete(_) => {
                self.state = ParseState::MessageComplete(accepted);
                Some(accepted)
            }
            ParseState::Upgraded if accepted => Some(true),
            ParseState::Upgraded => {
                self.state = ParseState::Idle;
                Some(false)
            }
            _ => None,
        }
    }

    /// Whether `stream` is a request parsed under `downstream_upgrade_as_ordinary`, or one before
    /// such a request.
    fn is_at_or_before_an_ordinary_request(&self, stream: StreamId) -> bool {
        !self.last_ordinary_request.is_unspecified() && stream <= self.last_ordinary_request
    }

    /// First user-agent value observed on this connection.
    pub fn user_agent(&self) -> &[u8] {
        &self.user_agent
    }

    /// Whether an ALPN value names an HTTP/1 protocol supported by this codec.
    pub fn supports_next_protocol(protocol: &[u8]) -> bool {
        matches!(protocol, b"http/1.0" | b"http/1.1")
    }

    /// Immediately makes the connection non-reusable without emitting bytes.
    pub fn generate_immediate_goaway(&mut self) -> GeneratedFrame {
        self.keepalive = false;
        GeneratedFrame::default()
    }

    fn dispatch(
        &mut self,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        event: CodecEvent<'_>,
        result: &mut CodecIngressResult,
    ) {
        result.append_callback_result(HttpCodec::dispatch_ingress_callback(
            self,
            callback,
            callback_egress,
            event,
        ));
    }

    fn dispatch_eof(
        &mut self,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        event: CodecEvent<'_>,
        result: &mut CodecCallbackResult,
    ) {
        result.append(HttpCodec::dispatch_ingress_callback(
            self,
            callback,
            callback_egress,
            event,
        ));
    }

    fn begin_message(
        &mut self,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) {
        if self.direction.is_downstream() || !self.informational_response {
            self.ingress_stream = StreamId::new(self.ingress_stream.get().saturating_add(1));
        }
        if self.direction.is_upstream() {
            self.informational_response = false;
        } else {
            self.request_pending = true;
            self.response_pending = true;
        }
        let mut message = HttpMessage::new();
        self.dispatch(
            callback,
            callback_egress,
            CodecEvent::MessageBegin {
                stream: self.ingress_stream,
                message: &mut message,
            },
            result,
        );
        self.current_message = Some(message);
    }

    fn begin_http_09_response(
        &mut self,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) {
        self.begin_message(callback, callback_egress, result);
        if self.parser_paused {
            return;
        }
        let mut message = self.current_message.take().unwrap_or_default();
        message.set_http_version(0, 9);
        let _ = message.set_status_code(200);
        message.set_ingress_header_size(HttpHeaderSize::default());
        self.dispatch(
            callback,
            callback_egress,
            CodecEvent::HeadersComplete {
                stream: self.ingress_stream,
                message: Box::new(message),
            },
            result,
        );
        self.state = ParseState::UntilEof;
    }

    fn complete_message(
        &mut self,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
        upgraded: bool,
    ) {
        if self.direction.is_downstream() {
            self.request_pending = false;
        } else {
            self.response_pending = self.informational_response;
            if self.informational_response && !self.ingress_upgrade {
                self.state = ParseState::Idle;
                return;
            }
        }
        self.dispatch(
            callback,
            callback_egress,
            CodecEvent::MessageComplete {
                stream: self.ingress_stream,
                upgraded,
            },
            result,
        );
        if upgraded {
            self.ingress_upgrade_complete = true;
            self.parser_paused = true;
            self.state = ParseState::Upgraded;
        } else {
            self.state = ParseState::Idle;
        }
    }

    fn complete_message_eof(
        &mut self,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        result: &mut CodecCallbackResult,
    ) {
        if self.direction.is_downstream() {
            self.request_pending = false;
        } else {
            self.response_pending = false;
        }
        self.dispatch_eof(
            callback,
            callback_egress,
            CodecEvent::MessageComplete {
                stream: self.ingress_stream,
                upgraded: self.ingress_upgrade,
            },
            result,
        );
        self.state = ParseState::Idle;
    }

    fn fail(
        &mut self,
        kind: ParseErrorKind,
        message: &'static [u8],
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) {
        if self.parser_error {
            return;
        }
        self.parser_error = true;
        self.parser_paused = true;
        self.backfill_partial_request();
        let mut error = HttpException::new(HttpExceptionDirection::Ingress, message);
        error.set_proxygen_error(match kind {
            ParseErrorKind::Header => ProxygenError::PARSE_HEADER,
            ParseErrorKind::HeaderContent => ProxygenError::HEADER_CONTENT_VALIDATION,
            ParseErrorKind::Body => ProxygenError::PARSE_BODY,
        });
        if self.direction.is_downstream() && self.egress_stream < self.ingress_stream {
            error.set_http_status_code(400);
        }
        error.set_partial_message(self.current_message.take());
        self.dispatch(
            callback,
            callback_egress,
            CodecEvent::Error {
                stream: self.ingress_stream,
                error: &error,
                new_stream: false,
            },
            result,
        );
    }

    fn fail_eof(
        &mut self,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        result: &mut CodecCallbackResult,
    ) {
        if self.parser_error {
            return;
        }
        self.parser_error = true;
        self.parser_paused = true;
        self.backfill_partial_request();
        let mut error = HttpException::new(HttpExceptionDirection::Ingress, b"unexpected EOF");
        error.set_proxygen_error(ProxygenError::EOF);
        if self.direction.is_downstream() && self.egress_stream < self.ingress_stream {
            error.set_http_status_code(400);
        }
        error.set_partial_message(self.current_message.take());
        self.dispatch_eof(
            callback,
            callback_egress,
            CodecEvent::Error {
                stream: self.ingress_stream,
                error: &error,
                new_stream: false,
            },
            result,
        );
    }

    fn headers_terminated(bytes: &[u8]) -> bool {
        bytes.ends_with(b"\r\n\r\n") || bytes.ends_with(b"\n\n")
    }

    fn backfill_partial_request(&mut self) {
        if self.direction.is_upstream() {
            return;
        }
        let Some(message) = self.current_message.as_mut() else {
            return;
        };
        if message.is_request() {
            return;
        }
        let ParseState::Head(head) = &self.state else {
            return;
        };
        let Some(newline) = head.iter().position(|byte| *byte == b'\n') else {
            return;
        };
        let line = trim_trailing_cr(&head[..newline]);
        let mut words = line.split(|byte| matches!(*byte, b' ' | b'\t'));
        let (Some(method), Some(url)) = (words.next(), words.next()) else {
            return;
        };
        let version = match words.next() {
            None => (0, 9),
            Some(version) => match parse_version(version) {
                Some(version) => version,
                None => return,
            },
        };
        if words.next().is_some() || !is_frozen_http_method(method) {
            return;
        }
        let parsed =
            roxygen_core::url::ParseUrl::parse_url_maybe_invalid(url, self.strict_validation);
        if !parsed.valid() {
            return;
        }
        let _ = message.set_method(Method::from_name(
            String::from_utf8_lossy(method).into_owned(),
        ));
        let _ = message.set_url(url, self.strict_validation);
        message.set_http_version(version.0, version.1);
    }

    #[inline]
    fn valid_initial_prefix(&self, head: &[u8]) -> bool {
        if self.direction.is_upstream() {
            #[cfg(test)]
            count_work(&self.head_work.prefix_reads, head.len().min(5));
            return b"HTTP/".starts_with(head) || head.starts_with(b"HTTP/");
        }
        let token_end = head
            .iter()
            .position(|byte| matches!(*byte, b' ' | b'\t' | b'\r' | b'\n'))
            .unwrap_or(head.len());
        #[cfg(test)]
        count_work(
            &self.head_work.prefix_reads,
            (token_end + 1).min(head.len()),
        );
        let token = &head[..token_end];
        FROZEN_HTTP_METHODS
            .iter()
            .any(|method| method.starts_with(token))
    }

    /// The scan of `scan_head_counting_line_feeds`, without its count.
    #[cfg(test)]
    fn scan_head(&self, buffered: &[u8], segment: &[u8], from: usize) -> HeadScan {
        self.scan_head_counting_line_feeds(buffered, segment, from, &mut LineFeedPositions::new())
            .0
    }

    /// Scans `segment[from..]` as the continuation of a head whose first
    /// bytes, at least one, are `buffered` and then `segment[..from]`. Each
    /// byte gets the `ParseState::Head` checks in order: the size limit, the
    /// start-line prefix, the terminator at an LF, and then control bytes. The
    /// terminator is CRLFCRLF or LFLF, or downstream an HTTP/0.9 request line,
    /// which can first hold at the head's first LF.
    ///
    /// The prefix check reads each head byte inside its window once per call,
    /// the bytes before `from` included. The scan also returns the number of
    /// LFs in `segment[from..end]`, each of which it visits, and records their
    /// positions in `segment` in `line_feed_positions`.
    fn scan_head_counting_line_feeds(
        &self,
        buffered: &[u8],
        segment: &[u8],
        from: usize,
        line_feed_positions: &mut LineFeedPositions,
    ) -> (HeadScan, usize) {
        let base = buffered.len();
        let head_byte = |index: usize| {
            if index < base {
                buffered[index]
            } else {
                segment[index - base]
            }
        };
        let limit = segment.len().min(MAX_HEADER_BYTES.saturating_sub(base));
        let prefix_limit = if self.direction.is_upstream() {
            UPSTREAM_PREFIX_LIMIT
        } else {
            INITIAL_PREFIX_LIMIT
        };
        let push_prefix = |prefix: &mut PrefixMatch, byte: &u8| {
            #[cfg(test)]
            {
                count_work(&self.head_work.prefix_reads, 1);
                let address = std::ptr::from_ref(byte);
                let in_place = buffered.as_ptr_range().contains(&address)
                    || segment.as_ptr_range().contains(&address);
                count_work(&self.head_work.prefix_copies, usize::from(!in_place));
            }
            prefix.push(*byte)
        };
        // The position of the first byte this call reaches that fails the
        // start-line prefix check. Bytes past it are not scanned.
        let mut prefix_stop = None;
        if base + from < prefix_limit {
            let mut prefix = PrefixMatch::new(self.direction.is_upstream());
            for byte in buffered {
                push_prefix(&mut prefix, byte);
            }
            for byte in &segment[..from] {
                push_prefix(&mut prefix, byte);
            }
            let window = segment
                .get(from..limit.min(prefix_limit - base))
                .unwrap_or_default();
            for (offset, byte) in window.iter().enumerate() {
                if !push_prefix(&mut prefix, byte) {
                    prefix_stop = Some(from + offset);
                    break;
                }
                if prefix.settled() {
                    break;
                }
            }
        }
        let search_end = prefix_stop.unwrap_or(limit);
        let mut line_feeds = 0;
        let mut earlier_newline = None;
        let mut position = from;
        while position < search_end {
            #[cfg(test)]
            count_work(&self.head_work.scan_steps, 1);
            let Some(offset) = word_scan::find_control(&segment[position..search_end]) else {
                position = search_end;
                break;
            };
            let mut index = position + offset;
            // A CR passes every check, so the LF of a CRLF is checked at once.
            if segment[index] == b'\r' && index + 1 < search_end && segment[index + 1] == b'\n' {
                index += 1;
            }
            position = index + 1;
            let stop = match segment[index] {
                b'\n' => {
                    line_feeds += 1;
                    line_feed_positions.record(index);
                    let length = base + position;
                    let terminated = if index >= 3 {
                        let end = &segment[index - 3..index];
                        end[2] == b'\n' || *end == *b"\r\n\r"
                    } else {
                        (length >= 2 && head_byte(length - 2) == b'\n')
                            || (length >= 4
                                && head_byte(length - 4) == b'\r'
                                && head_byte(length - 3) == b'\n'
                                && head_byte(length - 2) == b'\r')
                    };
                    let http_09 = self.direction.is_downstream()
                        && !earlier_newline.replace(true).unwrap_or_else(|| {
                            buffered.contains(&b'\n') || segment[..from].contains(&b'\n')
                        })
                        && request_line_is_http_09(buffered, &segment[..index]);
                    (terminated || http_09).then_some(HeadScanStop::Complete)
                }
                b'\r' | b'\t' => None,
                byte if byte < b' ' => Some(HeadScanStop::InvalidHeaderByte),
                _ => None,
            };
            if stop.is_some() {
                let scan = HeadScan {
                    end: position,
                    stop,
                };
                return (scan, line_feeds);
            }
        }
        if let Some(stop) = prefix_stop {
            let scan = HeadScan {
                end: stop + 1,
                stop: Some(HeadScanStop::InvalidStartLine),
            };
            return (scan, line_feeds);
        }
        if position < segment.len() {
            let scan = HeadScan {
                end: position + 1,
                stop: Some(HeadScanStop::TooLarge),
            };
            return (scan, line_feeds);
        }
        let scan = HeadScan {
            end: segment.len(),
            stop: None,
        };
        (scan, line_feeds)
    }

    /// Continues the head that `buffered` holds, or that begins at
    /// `input[head_start]` when `buffered` is empty, through the rest of
    /// `input`. The head's bytes before `result.consumed` are already taken. A
    /// head that lies wholly in `input` is parsed in place.
    fn ingress_head(
        &mut self,
        mut buffered: Vec<u8>,
        input: &[u8],
        head_start: usize,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) {
        let segment = &input[head_start..];
        let from = result.consumed - head_start;
        let mut line_feed_positions = LineFeedPositions::new();
        let (scan, scanned_line_feeds) =
            self.scan_head_counting_line_feeds(&buffered, segment, from, &mut line_feed_positions);
        result.consumed = head_start + scan.end;
        let accepted = &segment[..scan.end];
        let message: &'static [u8] = match scan.stop {
            None => {
                buffered.extend_from_slice(accepted);
                self.state = ParseState::Head(buffered);
                return;
            }
            Some(HeadScanStop::Complete) => {
                let parsed = if buffered.is_empty() {
                    let earlier_line_feeds = segment[..from]
                        .iter()
                        .filter(|byte| **byte == b'\n')
                        .count();
                    let known = (earlier_line_feeds == 0)
                        .then(|| line_feed_positions.known(accepted.len()));
                    self.parse_head(
                        accepted,
                        Some(scanned_line_feeds + earlier_line_feeds),
                        known,
                    )
                } else {
                    buffered.extend_from_slice(accepted);
                    self.parse_head(&buffered, None, None)
                };
                match parsed {
                    Ok(parsed) => {
                        self.install_parsed_head(parsed, callback, callback_egress, result);
                    }
                    Err((kind, message)) => {
                        if buffered.is_empty() {
                            buffered = accepted.to_vec();
                        }
                        self.state = ParseState::Head(buffered);
                        if self.current_message.is_none() {
                            self.current_message = Some(HttpMessage::new());
                        }
                        self.fail(kind, message, callback, callback_egress, result);
                    }
                }
                return;
            }
            Some(HeadScanStop::TooLarge) => b"HTTP header block is too large",
            Some(HeadScanStop::InvalidStartLine) => b"invalid HTTP start line",
            Some(HeadScanStop::InvalidHeaderByte) => b"invalid HTTP header byte",
        };
        buffered.extend_from_slice(accepted);
        self.state = ParseState::Head(buffered);
        self.fail(
            ParseErrorKind::Header,
            message,
            callback,
            callback_egress,
            result,
        );
    }

    /// Parses the complete head `raw`, which holds `line_feeds` LFs when the
    /// scan counted them, at `known` when the scan recorded them.
    fn parse_head(
        &mut self,
        raw: &[u8],
        line_feeds: Option<usize>,
        known: Option<KnownLineFeeds<'_>>,
    ) -> Result<ParsedHead, (ParseErrorKind, &'static [u8])> {
        let mut message = self.current_message.take().unwrap_or_default();
        let line_feeds = line_feeds.unwrap_or_else(|| {
            #[cfg(test)]
            count_work(&self.head_work.line_feed_counts, 1);
            word_scan::count_lf(raw)
        });
        // A head with N header lines has N + 2 LFs, so this leaves room for its
        // headers and an inferred Host. Folds and escaped newlines only
        // over-count, up to the cap.
        let reserve = line_feeds.saturating_sub(1).min(HEAD_RESERVE_CAP);
        #[cfg(test)]
        {
            self.head_reserve = Some((reserve, message.headers().len()));
        }
        message.headers_mut().reserve(reserve);
        let mut lines = match known {
            Some(known) => HeaderLines::with_known(raw, known),
            None => header_lines(raw),
        };
        let start_line = trim_trailing_cr(lines.next().unwrap_or_default());
        let mut inferred_host = None;
        let (version, informational) = if self.direction.is_downstream() {
            let mut words = start_line.split(|byte| matches!(*byte, b' ' | b'\t'));
            let method = words.next().filter(|word| !word.is_empty()).ok_or((
                ParseErrorKind::Header,
                b"invalid HTTP request line".as_slice(),
            ))?;
            let url = words.next().filter(|word| !word.is_empty()).ok_or((
                ParseErrorKind::Header,
                b"invalid HTTP request line".as_slice(),
            ))?;
            let version = match words.next() {
                None => (0, 9),
                Some(version) => parse_version(version).ok_or((
                    ParseErrorKind::Header,
                    b"invalid HTTP request version".as_slice(),
                ))?,
            };
            if words.next().is_some() || !is_frozen_http_method(method) {
                self.current_message = Some(message);
                return Err((ParseErrorKind::Header, b"invalid HTTP request line"));
            }
            let method = match HttpMethod::from_bytes(method) {
                Some(method) => Method::from(method),
                None => {
                    #[cfg(test)]
                    count_work(&self.head_work.method_copies, 1);
                    Method::from_name(String::from_utf8_lossy(method).into_owned())
                }
            };
            message.set_method(method).map_err(|_| {
                (
                    ParseErrorKind::Header,
                    b"invalid HTTP request method".as_slice(),
                )
            })?;
            let parsed = message.set_url(url, self.strict_validation).map_err(|_| {
                (
                    ParseErrorKind::Header,
                    b"invalid HTTP request URL".as_slice(),
                )
            })?;
            if self.strict_validation && !parsed.valid() {
                self.current_message = Some(HttpMessage::new());
                return Err((ParseErrorKind::Header, b"invalid HTTP request URL"));
            }
            if parsed.has_host() {
                inferred_host = Some(parsed.host_and_port());
            }
            (version, false)
        } else {
            let mut words = start_line.splitn(3, |byte| *byte == b' ');
            let version = parse_version(words.next().unwrap_or_default()).ok_or((
                ParseErrorKind::Header,
                b"invalid HTTP response version".as_slice(),
            ))?;
            let status = parse_decimal_u64(words.next().unwrap_or_default())
                .filter(|status| *status <= 999)
                .ok_or((
                    ParseErrorKind::Header,
                    b"invalid HTTP response status".as_slice(),
                ))? as u16;
            message
                .set_status_code(status)
                .map_err(|_| (ParseErrorKind::Header, b"invalid HTTP response".as_slice()))?;
            message
                .set_status_message(words.next().unwrap_or_default())
                .map_err(|_| (ParseErrorKind::Header, b"invalid HTTP response".as_slice()))?;
            (version, status / 100 == 1)
        };
        message.set_http_version(version.0, version.1);

        let mut previous: Option<(&[u8], Cow<'_, [u8]>)> = None;
        for raw_line in lines {
            let line = trim_trailing_cr(raw_line);
            if line.is_empty() {
                break;
            }
            if matches!(line.first(), Some(b' ' | b'\t')) {
                let Some((_, value)) = previous.as_mut() else {
                    self.current_message = Some(message);
                    return Err((ParseErrorKind::Header, b"invalid folded header"));
                };
                let value = value.to_mut();
                value.push(b' ');
                value.extend_from_slice(trim_http_whitespace(line));
                continue;
            }
            if let Some((name, value)) = previous.take() {
                self.add_validated_header(&mut message, name, &value)?;
            }
            let Some((colon, valid_name)) = header_name_end(line) else {
                self.current_message = Some(message);
                return Err((ParseErrorKind::Header, b"invalid HTTP header"));
            };
            if !valid_name {
                self.current_message = Some(message);
                return Err((ParseErrorKind::Header, b"invalid HTTP header name"));
            }
            previous = Some((
                &line[..colon],
                Cow::Borrowed(trim_http_whitespace(&line[colon + 1..])),
            ));
        }
        if let Some((name, value)) = previous {
            self.add_validated_header(&mut message, name, &value)?;
        }

        let mut first: Option<&[u8]> = None;
        let mut valid = true;
        if message.headers().exists(HttpHeaderCode::ContentLength) {
            for (code, _, value) in message.headers().iter() {
                if code != HttpHeaderCode::ContentLength {
                    continue;
                }
                if first.is_some_and(|existing| existing != value) {
                    valid = false;
                    break;
                }
                first = Some(value);
            }
        }
        if !valid {
            self.current_message = Some(message);
            return Err((
                ParseErrorKind::Header,
                b"conflicting Content-Length headers",
            ));
        }
        let content_length = first
            .map(|value| {
                parse_decimal_u64(value)
                    .ok_or((ParseErrorKind::Header, b"invalid Content-Length".as_slice()))
            })
            .transpose()?;
        let mut chunked = false;
        let mut valid_transfer_encoding = true;
        message
            .headers()
            .for_each_value(HttpHeaderCode::TransferEncoding, |value| {
                if value.eq_ignore_ascii_case(b"chunked") {
                    chunked = true;
                    false
                } else {
                    valid_transfer_encoding = false;
                    true
                }
            });
        if !valid_transfer_encoding || (chunked && content_length.is_some()) {
            self.current_message = Some(message);
            return Err((ParseErrorKind::Header, b"invalid Transfer-Encoding"));
        }
        message.set_chunked(chunked);
        // As frozen `onHeadersComplete`: after the last wire header and the
        // Content-Length and Transfer-Encoding checks, replacing every wire Host.
        if let Some(host) = inferred_host {
            message.headers_mut().set(HttpHeaderCode::Host, host);
        }

        let status = if self.direction.is_upstream() {
            message.status_code().unwrap_or_default()
        } else {
            0
        };
        // `DIV-030`: under the option a downstream CONNECT is an ordinary request, with no
        // tunnel and no keep-alive change.
        let ordinary = self.downstream_upgrade_as_ordinary && self.direction.is_downstream();
        if ordinary {
            self.last_ordinary_request = self.ingress_stream;
        }
        self.connect_request = if self.direction.is_downstream() {
            !ordinary && message.method().ok().flatten() == Some(HttpMethod::Connect)
        } else {
            self.connect_request
        };
        self.head_request = if self.direction.is_downstream() {
            message.method().ok().flatten() == Some(HttpMethod::Head)
        } else {
            self.head_request
        };
        if self.direction.is_downstream() {
            self.may_chunk_egress = version.0 == 1 && version.1 >= 1;
        }

        let upgrade = message
            .headers()
            .get_single_or_empty(HttpHeaderCode::Upgrade);
        #[cfg(test)]
        count_work(
            &self.head_work.upgrade_copies,
            usize::from(
                !upgrade.is_empty()
                    && upgrade.as_ptr()
                        != message
                            .headers()
                            .get_single_or_empty(HttpHeaderCode::Upgrade)
                            .as_ptr(),
            ),
        );
        // `DIV-031`: a response to a request an upstream codec sent under the option is read as
        // an ordinary response.
        let ordinary_response = self.direction.is_upstream() && self.ordinary_request_sent;
        // `DIV-030`, `DIV-031`: so is its `Upgrade` header an ordinary header.
        let websocket =
            !(ordinary || ordinary_response) && upgrade.eq_ignore_ascii_case(b"websocket");
        self.keepalive &= ordinary || ordinary_response || upgrade.is_empty();
        self.keepalive &= !self.connect_request;
        if websocket {
            message.set_ingress_websocket_upgrade();
            if self.direction.is_upstream() {
                let accept = message
                    .headers()
                    .get_single_or_empty(HttpHeaderCode::SecWebSocketAccept);
                if accept != self.websocket_accept_key.as_slice() {
                    self.current_message = Some(message);
                    return Err((ParseErrorKind::Header, b"invalid websocket accept key"));
                }
            } else {
                if !self.websocket_accept_key.is_empty() {
                    self.current_message = Some(message);
                    return Err((ParseErrorKind::Header, b"duplicate websocket upgrade"));
                }
                let key = message
                    .headers()
                    .get_single_or_empty(HttpHeaderCode::SecWebSocketKey);
                self.websocket_accept_key = websocket_accept(key);
            }
        }

        if self.direction.is_upstream() {
            if self.connect_request && (200..300).contains(&status) {
                self.ingress_upgrade = true;
            } else if status == 101 {
                // `DIV-031`: the request was sent as ordinary, so nothing can switch.
                if ordinary_response {
                    self.current_message = Some(message);
                    return Err((
                        ParseErrorKind::Header,
                        b"switching protocols refused for an ordinary request",
                    ));
                }
                let server_upgrade = message
                    .headers()
                    .get_single_or_empty(HttpHeaderCode::Upgrade);
                if !server_accepted_upgrade(&self.upgrade_header, server_upgrade) {
                    self.current_message = Some(message);
                    return Err((ParseErrorKind::Header, b"upgrade protocol mismatch"));
                }
                self.ingress_upgrade = true;
                self.egress_upgrade = true;
            }
            if status != 100 {
                self.upgrade_header.clear();
            }
        } else if self.connect_request {
            self.ingress_upgrade = true;
        }
        message.set_upgraded(self.ingress_upgrade);

        let message_keepalive = message.compute_keepalive();
        message.set_wants_keepalive(message_keepalive);
        self.keepalive &= message_keepalive;
        if self.direction.is_downstream() {
            self.keepalive_requested = if message_keepalive
                && version == (1, 0)
                && matches!(
                    self.keepalive_requested,
                    KeepaliveRequested::Unset | KeepaliveRequested::Enabled
                ) {
                KeepaliveRequested::Enabled
            } else {
                KeepaliveRequested::Disabled
            };
        }
        if self.user_agent.is_empty() {
            self.user_agent = message
                .headers()
                .get_single_or_empty(HttpHeaderCode::UserAgent)
                .to_vec();
            #[cfg(test)]
            count_work(
                &self.head_work.user_agent_copies,
                usize::from(!self.user_agent.is_empty()),
            );
        }
        message.set_ingress_header_size(HttpHeaderSize {
            compressed: u32::try_from(raw.len()).unwrap_or(u32::MAX),
            uncompressed: u32::try_from(raw.len()).unwrap_or(u32::MAX),
            compressed_block: 0,
        });

        self.informational_response = informational;
        let ignore_body = self.direction.is_upstream()
            && (self.expect_no_response_body
                || rfc2616::response_body_must_be_empty(u32::from(status)));
        let body = if self.ingress_upgrade || ignore_body {
            IngressBody::None
        } else if chunked {
            IngressBody::Chunked
        } else if let Some(length) = content_length {
            if length == 0 {
                IngressBody::None
            } else {
                IngressBody::Fixed(length)
            }
        } else if self.direction.is_upstream() {
            IngressBody::UntilEof
        } else {
            IngressBody::None
        };
        Ok(ParsedHead {
            message: Box::new(message),
            body,
            informational,
            upgrade: self.ingress_upgrade,
        })
    }

    fn add_validated_header(
        &mut self,
        message: &mut HttpMessage,
        name: &[u8],
        value: &[u8],
    ) -> Result<(), (ParseErrorKind, &'static [u8])> {
        if !valid_header_value(value, self.strict_validation) {
            self.current_message = Some(message.clone());
            return Err((
                ParseErrorKind::HeaderContent,
                b"invalid HTTP header content",
            ));
        }
        message.headers_mut().add(name, value);
        Ok(())
    }

    fn install_parsed_head(
        &mut self,
        parsed: ParsedHead,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) {
        let stream = self.ingress_stream;
        self.informational_response = parsed.informational;
        self.dispatch(
            callback,
            callback_egress,
            CodecEvent::HeadersComplete {
                stream,
                message: parsed.message,
            },
            result,
        );
        self.state = match parsed.body {
            IngressBody::None => ParseState::MessageComplete(parsed.upgrade),
            IngressBody::Fixed(length) => ParseState::FixedBody(length),
            IngressBody::Chunked => ParseState::ChunkSize(ChunkSizeLine::default()),
            IngressBody::UntilEof => ParseState::UntilEof,
        };
        if parsed.body == IngressBody::None && !self.parser_paused {
            self.complete_message(callback, callback_egress, result, parsed.upgrade);
        }
    }

    fn emit_body(
        &mut self,
        body: Bytes,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) {
        if body.is_empty() {
            return;
        }
        let body_length = body.len();
        let queue = BufQueue::from_bytes(body, body_length)
            .expect("an exactly sized ingress queue must accept its payload");
        self.dispatch(
            callback,
            callback_egress,
            CodecEvent::Body {
                stream: self.ingress_stream,
                body: queue,
                padding: 0,
            },
            result,
        );
    }

    fn parse_trailers(raw: &[u8], strict: bool) -> Result<HttpHeaders, ParseErrorKind> {
        let mut trailers = HttpHeaders::new();
        for raw_line in header_lines(raw) {
            let line = trim_trailing_cr(raw_line);
            if line.is_empty() {
                continue;
            }
            let Some((colon, valid_name)) = header_name_end(line) else {
                return Err(ParseErrorKind::Body);
            };
            let name = &line[..colon];
            let value = trim_http_whitespace(&line[colon + 1..]);
            if !valid_name || !valid_header_value(value, strict) {
                return Err(if strict {
                    ParseErrorKind::HeaderContent
                } else {
                    ParseErrorKind::Body
                });
            }
            trailers.add(name, value);
        }
        Ok(trailers)
    }

    fn is_egress_busy(&self) -> bool {
        (self.direction.is_downstream() && self.response_pending)
            || (self.direction.is_upstream()
                && (self.request_pending
                    || (!self.egress_upgrade
                        && self.response_pending
                        && !self.release_egress_after_request)))
    }

    fn egress_rollback(&self) -> EgressRollback {
        EgressRollback {
            egress_stream: self.egress_stream,
            request_pending: self.request_pending,
            response_pending: self.response_pending,
            egress_chunked: self.egress_chunked,
            in_chunk: self.in_chunk,
            last_chunk_written: self.last_chunk_written,
            keepalive: self.keepalive,
            disable_keepalive_pending: self.disable_keepalive_pending,
            connect_request: self.connect_request,
            ordinary_request_sent: self.ordinary_request_sent,
            head_request: self.head_request,
            expect_no_response_body: self.expect_no_response_body,
            may_chunk_egress: self.may_chunk_egress,
            informational_response: self.informational_response,
            ingress_upgrade: self.ingress_upgrade,
            egress_upgrade: self.egress_upgrade,
            upgrade_header: self.upgrade_header.clone(),
            websocket_accept_key: self.websocket_accept_key.clone(),
            expect_http_09_response: self.expect_http_09_response,
        }
    }

    fn restore_egress(&mut self, rollback: EgressRollback) {
        self.egress_stream = rollback.egress_stream;
        self.request_pending = rollback.request_pending;
        self.response_pending = rollback.response_pending;
        self.egress_chunked = rollback.egress_chunked;
        self.in_chunk = rollback.in_chunk;
        self.last_chunk_written = rollback.last_chunk_written;
        self.keepalive = rollback.keepalive;
        self.disable_keepalive_pending = rollback.disable_keepalive_pending;
        self.connect_request = rollback.connect_request;
        self.ordinary_request_sent = rollback.ordinary_request_sent;
        self.head_request = rollback.head_request;
        self.expect_no_response_body = rollback.expect_no_response_body;
        self.may_chunk_egress = rollback.may_chunk_egress;
        self.informational_response = rollback.informational_response;
        self.ingress_upgrade = rollback.ingress_upgrade;
        self.egress_upgrade = rollback.egress_upgrade;
        self.upgrade_header = rollback.upgrade_header;
        self.websocket_accept_key = rollback.websocket_accept_key;
        self.expect_http_09_response = rollback.expect_http_09_response;
    }

    #[inline(never)]
    fn generate_headers(
        &mut self,
        message: &HttpMessage,
        stream: StreamId,
        end_stream: bool,
        extra_headers: Option<&HttpHeaders>,
    ) -> Result<(Vec<u8>, HttpHeaderSize), HttpError> {
        if self.keepalive && self.disable_keepalive_pending {
            self.keepalive = false;
        }
        let upstream = self.direction.is_upstream();
        let previous_informational = self.informational_response;
        let mut request = None;
        let mut method = None;
        let mut response = (0, b"".as_slice());
        if upstream {
            if stream != self.egress_stream {
                return Err(invalid_state(b"HTTP/1 request stream is out of order"));
            }
            // `DIV-031`: the flag asks for a handshake whose 101 an ordinary request refuses.
            if self.upstream_upgrade_as_ordinary && message.is_egress_websocket_upgrade() {
                return Err(invalid_state(
                    b"HTTP/1 egress websocket upgrade on a request sent as ordinary",
                ));
            }
            if self.upstream_upgrade_as_ordinary
                && !self.frames_ordinary_request(message, extra_headers, end_stream)
            {
                return Err(invalid_state(
                    b"HTTP/1 request sent as ordinary has ambiguous framing",
                ));
            }
            self.request_pending = true;
            self.response_pending = true;
            request = request_fields(message);
            method = request.and_then(|(name, _)| HttpMethod::from_bytes(name.as_bytes()));
            // `DIV-031`: under the option a CONNECT is an ordinary request, with no egress
            // upgrade, no tunnel and a response read by its own framing.
            self.ordinary_request_sent = self.upstream_upgrade_as_ordinary;
            self.connect_request =
                method == Some(HttpMethod::Connect) && !self.ordinary_request_sent;
            self.head_request = method == Some(HttpMethod::Head);
            self.expect_no_response_body = self.connect_request || self.head_request;
        } else {
            let next = StreamId::new(self.egress_stream.get().saturating_add(1));
            if (stream != next && !(stream == self.egress_stream && previous_informational))
                || stream > self.ingress_stream
            {
                return Err(invalid_state(b"HTTP/1 response stream is out of order"));
            }
            if !previous_informational {
                self.egress_stream = next;
            }
            response = response_fields(message)
                .ok_or_else(|| invalid_state(b"HTTP/1 response requires a status"))?;
            // `DIV-030`: the bytes behind a request parsed as ordinary may already be parsed as
            // HTTP/1.1, so the connection cannot switch protocols in a response to it or to any
            // request before it.
            if self.is_at_or_before_an_ordinary_request(stream)
                && (response.0 == 101 || message.is_egress_websocket_upgrade())
            {
                return Err(invalid_state(
                    b"HTTP/1 upgrade response after a request parsed as ordinary",
                ));
            }
            self.informational_response =
                response.0 / 100 == 1 || message.is_egress_websocket_upgrade();
            self.expect_no_response_body = self.connect_request
                || self.head_request
                || rfc2616::response_body_must_be_empty(u32::from(response.0));
        }

        let version = self.egress_version(message);
        let egress_websocket = message.is_egress_websocket_upgrade();
        let status = if upstream {
            0
        } else if egress_websocket {
            101
        } else {
            response.0
        };
        if !upstream {
            if self.connect_request && (200..300).contains(&status) {
                self.egress_upgrade = true;
            } else if status == 101 {
                self.ingress_upgrade = true;
                self.egress_upgrade = true;
            } else if self.connect_request && self.ingress_upgrade {
                self.ingress_upgrade = false;
                self.keepalive = false;
            }
        } else if self.connect_request || egress_websocket {
            self.egress_upgrade = true;
        }

        self.egress_chunked = message.is_chunked() && !self.egress_upgrade;
        self.last_chunk_written = false;
        let mut output;
        if upstream {
            let method_name = if egress_websocket {
                "GET"
            } else {
                request
                    .ok_or_else(|| invalid_state(b"HTTP/1 request requires a method"))?
                    .0
            };
            if method_name.is_empty() {
                return Err(invalid_state(b"HTTP/1 request requires a method"));
            }
            let url = request
                .ok_or_else(|| invalid_state(b"HTTP/1 request requires a URL"))?
                .1;
            let target = if method == Some(HttpMethod::Connect) && url.is_empty() {
                message.headers().get_single_or_empty(HttpHeaderCode::Host)
            } else {
                url
            };
            let start_line = method_name.len() + target.len() + MAX_REQUEST_LINE_SUFFIX;
            output = Vec::with_capacity(
                start_line + self.header_capacity(message, extra_headers, end_stream),
            );
            output.extend_from_slice(method_name.as_bytes());
            output.push(b' ');
            output.extend_from_slice(target);
            if version != (0, 9) {
                output.extend_from_slice(b" HTTP/");
                push_decimal(&mut output, u16::from(version.0));
                output.push(b'.');
                push_decimal(&mut output, u16::from(version.1));
            }
            output.extend_from_slice(b"\r\n");
            self.may_chunk_egress = version.0 == 1 && version.1 >= 1;
            if !self.upgrade_header.is_empty() {
                self.upgrade_header.clear();
            }
        } else {
            if status == 0 {
                return Err(invalid_state(b"HTTP/1 response requires a status"));
            }
            if version == (0, 9) {
                if end_stream {
                    self.finish_egress_message();
                }
                return Ok((Vec::new(), HttpHeaderSize::default()));
            }
            let reason = if egress_websocket {
                HttpMessage::default_reason(101)
            } else {
                response.1
            };
            let start_line = reason.len() + MAX_STATUS_LINE_SUFFIX;
            output = Vec::with_capacity(
                start_line + self.header_capacity(message, extra_headers, end_stream),
            );
            output.extend_from_slice(b"HTTP/");
            push_decimal(&mut output, u16::from(version.0));
            output.push(b'.');
            push_decimal(&mut output, u16::from(version.1));
            output.push(b' ');
            push_decimal(&mut output, status);
            output.push(b' ');
            output.extend_from_slice(reason);
            output.extend_from_slice(b"\r\n");
        }
        if self.keepalive
            && (!message.wants_keepalive()
                || version.0 < 1
                || (!upstream
                    && version == (1, 0)
                    && self.keepalive_requested != KeepaliveRequested::Enabled))
        {
            self.keepalive = false;
        }
        self.egress_chunked &= self.may_chunk_egress;
        if version == (0, 9) {
            self.expect_http_09_response = upstream;
            if end_stream {
                self.finish_egress_message();
            }
            let size = header_size(output.len());
            return Ok((output, size));
        }

        let mut deferred_content_length = None;
        let mut transfer_encoding_written = false;
        let mut has_date = false;
        let mut has_upgrade = false;
        let mut connection_header_seen = false;
        let mut explicit_keepalive = false;
        let mut connection_tokens = Vec::<&[u8]>::new();
        let mut has_upgrade_connection_token = false;
        let extra = extra_headers.into_iter().flat_map(HttpHeaders::iter);
        for (code, name, value) in message.headers().iter().chain(extra) {
            if code == HttpHeaderCode::ContentLength {
                deferred_content_length = Some(value);
                continue;
            }
            if code == HttpHeaderCode::Connection
                && (!self.informational_response || egress_websocket)
            {
                connection_header_seen = true;
                for token in value.split(|byte| *byte == b',') {
                    let token = trim_http_whitespace(token);
                    if token.eq_ignore_ascii_case(b"upgrade") {
                        has_upgrade_connection_token = true;
                    }
                    if token.eq_ignore_ascii_case(b"close") {
                        self.keepalive = false;
                    } else if token.eq_ignore_ascii_case(b"keep-alive") {
                        explicit_keepalive = true;
                    } else if !token.is_empty() {
                        connection_tokens.push(token);
                    }
                }
                continue;
            }
            if code == HttpHeaderCode::Upgrade {
                has_upgrade = true;
                if upstream {
                    self.upgrade_header = value.to_vec();
                }
            } else if code == HttpHeaderCode::TransferEncoding && !transfer_encoding_written {
                if !value.eq_ignore_ascii_case(b"chunked") {
                    continue;
                }
                transfer_encoding_written = true;
                if !self.may_chunk_egress {
                    continue;
                }
            } else if code == HttpHeaderCode::Date {
                has_date = true;
            } else if egress_websocket
                && matches!(
                    code,
                    HttpHeaderCode::SecWebSocketKey | HttpHeaderCode::SecWebSocketAccept
                )
            {
                continue;
            }
            if holds_cr_or_lf(value) {
                continue;
            }
            output.extend_from_slice(name);
            output.extend_from_slice(b": ");
            output.extend_from_slice(value);
            output.extend_from_slice(b"\r\n");
        }

        // `DIV-036`: only a head that ends its message with no framing of the
        // caller's goes out unframed.
        let unframed = end_stream
            && self.unframed_bodiless_requests
            && !transfer_encoding_written
            && deferred_content_length.is_none();
        let body_check = (!upstream && !self.expect_no_response_body && !self.egress_upgrade)
            || (upstream && !unframed && (method == Some(HttpMethod::Post) || self.egress_chunked));
        self.egress_chunked &= transfer_encoding_written;
        if body_check && !self.egress_chunked && deferred_content_length.is_none() {
            if !transfer_encoding_written && self.may_chunk_egress {
                output.extend_from_slice(b"Transfer-Encoding: chunked\r\n");
                self.egress_chunked = true;
            } else {
                self.keepalive = false;
            }
        }
        if !upstream && !has_date {
            output.extend_from_slice(b"Date: ");
            output.extend_from_slice(HttpMessage::format_date_header().as_bytes());
            output.extend_from_slice(b"\r\n");
        }
        if egress_websocket && !has_upgrade {
            output.extend_from_slice(b"Upgrade: websocket\r\n");
            if upstream {
                self.upgrade_header = b"websocket".to_vec();
                let key = websocket_key(stream);
                output.extend_from_slice(b"Sec-WebSocket-Key: ");
                output.extend_from_slice(&key);
                output.extend_from_slice(b"\r\n");
                self.websocket_accept_key = websocket_accept(&key);
            } else {
                output.extend_from_slice(b"Sec-WebSocket-Accept: ");
                output.extend_from_slice(&self.websocket_accept_key);
                output.extend_from_slice(b"\r\n");
            }
            if !has_upgrade_connection_token {
                connection_tokens.push(b"Upgrade");
            }
        }
        let omit_implicit_http11_keepalive = upstream
            && version == (1, 1)
            && self.keepalive
            && connection_header_seen
            && !explicit_keepalive
            && connection_tokens.is_empty();
        if (!self.informational_response || upstream || !connection_tokens.is_empty())
            && !omit_implicit_http11_keepalive
        {
            output.extend_from_slice(b"Connection: ");
            for (index, token) in connection_tokens.iter().enumerate() {
                if index != 0 {
                    output.extend_from_slice(b", ");
                }
                output.extend_from_slice(token);
            }
            if !self.informational_response {
                if !connection_tokens.is_empty() {
                    output.extend_from_slice(b", ");
                }
                output.extend_from_slice(if self.keepalive {
                    b"keep-alive"
                } else {
                    b"close"
                });
            }
            output.extend_from_slice(b"\r\n");
        }
        if let Some(content_length) = deferred_content_length {
            output.extend_from_slice(b"Content-Length: ");
            output.extend_from_slice(content_length);
            output.extend_from_slice(b"\r\n");
        }
        output.extend_from_slice(b"\r\n");
        if end_stream {
            self.append_eom(&mut output);
            self.finish_egress_message();
        }
        let size = header_size(output.len());
        Ok((output, size))
    }

    /// The HTTP version `generate_headers` writes for `message`.
    fn egress_version(&self, message: &HttpMessage) -> (u8, u8) {
        let mut version = message.http_version();
        if version > (1, 1) {
            version = (1, 1);
        }
        if self.force_1_1
            && version < (1, 1)
            && !(self.direction.is_downstream() && version == (0, 9))
        {
            version = (1, 1);
        }
        version
    }

    /// Roxygen-only (`DIV-031`): whether the head of a request that the upstream option sends
    /// as ordinary, read as `generate_headers` writes it (`message`'s headers, then
    /// `extra_headers`), tells the origin where the request ends exactly as this codec frames
    /// it: at most one `Content-Length`, a decimal length that is 0 on a head that ends the
    /// message; at most one written `Transfer-Encoding` line, never beside a `Content-Length`
    /// and only over this codec's chunk framing; and, behind a head that does not end the
    /// message, a body framed by that length or that chunking. Under the option, generating a
    /// request for which this is false fails. It is true for every other request, which is
    /// written as without the option.
    pub fn frames_ordinary_request(
        &self,
        message: &HttpMessage,
        extra_headers: Option<&HttpHeaders>,
        end_stream: bool,
    ) -> bool {
        let method =
            request_fields(message).and_then(|(name, _)| HttpMethod::from_bytes(name.as_bytes()));
        let headers = || {
            message
                .headers()
                .iter()
                .chain(extra_headers.into_iter().flat_map(HttpHeaders::iter))
        };
        if method != Some(HttpMethod::Connect)
            && !headers().any(|(code, _, _)| code == HttpHeaderCode::Upgrade)
        {
            return true;
        }
        let version = self.egress_version(message);
        if version == (0, 9) {
            // No header line is written, so nothing frames a body.
            return end_stream;
        }
        let may_chunk = version.0 == 1 && version.1 >= 1;
        let mut lengths = Vec::new();
        let mut chunked_seen = false;
        let mut transfer_encodings = 0_usize;
        // As the header loop of `generate_headers`: the last `Content-Length` is written; a
        // `Transfer-Encoding` line before the first chunked one is dropped, as is that line
        // on HTTP/1.0, and every later line without CR or LF is written verbatim.
        for (code, _, value) in headers() {
            if code == HttpHeaderCode::ContentLength {
                lengths.push(value);
            } else if code == HttpHeaderCode::TransferEncoding {
                if !chunked_seen {
                    if !value.eq_ignore_ascii_case(b"chunked") {
                        continue;
                    }
                    chunked_seen = true;
                    if !may_chunk {
                        continue;
                    }
                }
                if !value.iter().any(|byte| matches!(*byte, b'\r' | b'\n')) {
                    transfer_encodings += 1;
                }
            }
        }
        // As `generate_headers`: a message marked chunked is chunked under its chunked line,
        // and a POST or such a message with neither framing header gets a generated one.
        let marked = message.is_chunked() && may_chunk;
        let generated = (method == Some(HttpMethod::Post) || marked)
            && !chunked_seen
            && lengths.is_empty()
            && may_chunk;
        let chunk_framed = (marked && chunked_seen) || generated;
        transfer_encodings += usize::from(generated);
        let length = match lengths.as_slice() {
            [] => None,
            [value] => match parse_decimal_u64(value) {
                Some(length) => Some(length),
                None => return false,
            },
            _ => return false,
        };
        if transfer_encodings > 1
            || (transfer_encodings != 0 && (length.is_some() || !chunk_framed))
        {
            return false;
        }
        if end_stream {
            length.is_none_or(|length| length == 0)
        } else {
            length.is_some() || chunk_framed
        }
    }

    /// Bytes for every header line and generated line that can follow a start
    /// line. Connection tokens are rejoined with `", "`, so each comma in a
    /// Connection value counts one more byte.
    fn header_capacity(
        &self,
        message: &HttpMessage,
        extra_headers: Option<&HttpHeaders>,
        end_stream: bool,
    ) -> usize {
        let extra = extra_headers.into_iter().flat_map(HttpHeaders::iter);
        let headers: usize = message
            .headers()
            .iter()
            .chain(extra)
            .map(|(code, name, value)| {
                let separators = if code == HttpHeaderCode::Connection {
                    value.iter().filter(|byte| **byte == b',').count()
                } else {
                    0
                };
                name.len() + value.len() + b": \r\n".len() + separators
            })
            .sum();
        let websocket = if message.is_egress_websocket_upgrade() {
            MAX_WEBSOCKET_LINES + self.websocket_accept_key.len()
        } else {
            0
        };
        headers + GENERATED_LINES + websocket + if end_stream { b"0\r\n\r\n".len() } else { 0 }
    }

    fn append_eom(&mut self, output: &mut Vec<u8>) {
        output.extend_from_slice(self.message_end().bytes());
    }

    /// Ends the egress message's framing and returns the bytes that end it.
    fn message_end(&mut self) -> MessageEnd {
        if !self.egress_chunked {
            return MessageEnd::Nothing;
        }
        if self.head_request && self.direction.is_downstream() {
            self.last_chunk_written = true;
            return MessageEnd::Nothing;
        }
        if self.last_chunk_written {
            MessageEnd::Crlf
        } else {
            self.last_chunk_written = true;
            MessageEnd::LastChunk
        }
    }

    fn finish_egress_message(&mut self) {
        if self.direction.is_downstream() {
            self.response_pending = false;
        } else {
            self.request_pending = false;
        }
    }

    fn push_static_output(output: &mut BufQueue, bytes: &'static [u8]) -> Result<(), HttpError> {
        output.push_static(bytes).map_err(|error| {
            HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string())
        })
    }

    fn push_output(&self, output: &mut BufQueue, bytes: Vec<u8>) -> Result<(), HttpError> {
        #[cfg(test)]
        if bytes.capacity() != 0 {
            count_work(&self.egress_work.byte_buffers, 1);
        }
        output.push_vec(bytes).map_err(|error| {
            HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string())
        })
    }

    fn append_output(&self, output: &mut BufQueue, other: &mut BufQueue) -> Result<(), HttpError> {
        #[cfg(test)]
        count_work(&self.egress_work.appended_queues, 1);
        output.append(other).map_err(|error| {
            HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string())
        })
    }

    fn push_chunk_line(
        &self,
        output: &mut BufQueue,
        line: Cow<'static, [u8]>,
    ) -> Result<(), HttpError> {
        match line {
            Cow::Borrowed(line) => Self::push_static_output(output, line),
            Cow::Owned(line) => self.push_output(output, line),
        }
    }

    /// Fails as appending `incoming` bytes to `output` would, so that a frame
    /// queued in parts is queued whole or not at all.
    fn check_output_room(output: &BufQueue, incoming: usize) -> Result<(), HttpError> {
        let current = output.chain_length();
        let error = match current.checked_add(incoming) {
            Some(length) if length <= output.limit() => return Ok(()),
            Some(_) => BufQueueError::LimitExceeded {
                limit: output.limit(),
                current,
                incoming,
            },
            None => BufQueueError::LengthOverflow { current, incoming },
        };
        Err(HttpError::with_message(
            HttpErrorCode::INTERNAL_ERROR,
            error.to_string(),
        ))
    }

    /// Returns the length of a frame of `prefix`, `body` and `suffix` bytes,
    /// or fails as staging the frame in one queue and appending that queue to
    /// `output` would, including when the frame's length overflows.
    fn check_frame_room(
        output: &BufQueue,
        prefix: usize,
        body: usize,
        suffix: usize,
    ) -> Result<usize, HttpError> {
        let overflow = |current, incoming| {
            HttpError::with_message(
                HttpErrorCode::INTERNAL_ERROR,
                BufQueueError::LengthOverflow { current, incoming }.to_string(),
            )
        };
        let framed = prefix
            .checked_add(body)
            .ok_or_else(|| overflow(prefix, body))?;
        let written = framed
            .checked_add(suffix)
            .ok_or_else(|| overflow(framed, suffix))?;
        Self::check_output_room(output, written)?;
        Ok(written)
    }
}

/// Counts every string built by `format!` from here to the end of this file
/// (the `generate` arms and the mounted test modules), so the count oracles see
/// any chunk prefix formatted through `core::fmt`.
#[cfg(test)]
macro_rules! format {
    ($($argument:tt)*) => {{
        $crate::FORMATTED_STRINGS.with(|count| count.set(count.get() + 1));
        ::std::format!($($argument)*)
    }};
}

impl HttpCodec for Http1xCodec {
    fn protocol(&self) -> CodecProtocol {
        CodecProtocol::Http11
    }

    fn transport_direction(&self) -> TransportDirection {
        self.direction
    }

    fn capabilities(&self) -> CodecCapabilities {
        CodecCapabilities::default()
    }

    fn create_stream(&mut self) -> StreamId {
        if self.direction.is_downstream() {
            self.ingress_stream = StreamId::new(self.ingress_stream.get().saturating_add(1));
            self.ingress_stream
        } else {
            self.egress_stream = StreamId::new(self.egress_stream.get().saturating_add(1));
            self.egress_stream
        }
    }

    fn is_busy(&self) -> bool {
        self.request_pending || self.response_pending
    }

    fn set_parser_paused(&mut self, paused: bool) {
        if !self.parser_error {
            self.parser_paused = paused;
        }
    }

    fn is_parser_paused(&self) -> bool {
        self.parser_paused
    }

    fn on_ingress(
        &mut self,
        input: &BufQueue,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
    ) -> CodecIngressResult {
        let mut parse_segment = |input: &Bytes| {
            let mut result = CodecIngressResult::new(0);
            if self.parser_error {
                return result;
            }
            if self.ingress_upgrade_complete {
                if !input.is_empty() {
                    result.consumed = input.len();
                    self.state = ParseState::Upgraded;
                    self.emit_body(input.clone(), callback, callback_egress, &mut result);
                }
                return result;
            }
            if self.parser_paused {
                return result;
            }
            if input.is_empty() {
                if let ParseState::MessageComplete(upgraded) = &self.state {
                    let upgraded = *upgraded;
                    self.complete_message(callback, callback_egress, &mut result, upgraded);
                }
                return result;
            }
            if self.direction.is_upstream()
                && self.expect_http_09_response
                && matches!(&self.state, ParseState::Idle)
            {
                self.begin_http_09_response(callback, callback_egress, &mut result);
                if self.parser_paused {
                    return result;
                }
            }

            while result.consumed < input.len() && !self.parser_paused && !self.parser_error {
                let state = std::mem::take(&mut self.state);
                match state {
                    ParseState::Idle => {
                        let Some(skipped) = input[result.consumed..]
                            .iter()
                            .position(|byte| !matches!(*byte, b'\r' | b'\n'))
                        else {
                            result.consumed = input.len();
                            continue;
                        };
                        let head_start = result.consumed + skipped;
                        let byte = input[head_start];
                        result.consumed = head_start + 1;
                        self.begin_message(callback, callback_egress, &mut result);
                        if !self.valid_initial_prefix(&[byte]) {
                            self.state = ParseState::Head(vec![byte]);
                            self.fail(
                                ParseErrorKind::Header,
                                b"invalid HTTP start line",
                                callback,
                                callback_egress,
                                &mut result,
                            );
                        } else if self.parser_paused {
                            self.state = ParseState::Head(vec![byte]);
                        } else {
                            self.ingress_head(
                                Vec::new(),
                                input,
                                head_start,
                                callback,
                                callback_egress,
                                &mut result,
                            );
                        }
                    }
                    ParseState::Head(head) => {
                        let head_start = result.consumed;
                        self.ingress_head(
                            head,
                            input,
                            head_start,
                            callback,
                            callback_egress,
                            &mut result,
                        );
                    }
                    ParseState::FixedBody(mut remaining) => {
                        let available = input.len() - result.consumed;
                        let amount =
                            usize::try_from(remaining.min(available as u64)).unwrap_or(available);
                        let start = result.consumed;
                        result.consumed += amount;
                        remaining -= amount as u64;
                        self.state = if remaining == 0 {
                            ParseState::MessageComplete(self.ingress_upgrade)
                        } else {
                            ParseState::FixedBody(remaining)
                        };
                        self.emit_body(
                            input.slice(start..start + amount),
                            callback,
                            callback_egress,
                            &mut result,
                        );
                        if remaining == 0 && !self.parser_paused {
                            self.complete_message(
                                callback,
                                callback_egress,
                                &mut result,
                                self.ingress_upgrade,
                            );
                        }
                    }
                    ParseState::ChunkSize(mut line) => {
                        let (read, end) = line.read(&input[result.consumed..]);
                        result.consumed += read;
                        let Some(end) = end else {
                            self.state = ParseState::ChunkSize(line);
                            continue;
                        };
                        if end == ChunkSizeLineEnd::TooLarge {
                            self.state = ParseState::ChunkSize(line);
                            self.fail(
                                ParseErrorKind::Body,
                                b"HTTP chunk header is too large",
                                callback,
                                callback_egress,
                                &mut result,
                            );
                            continue;
                        }
                        if line.stray_cr {
                            self.state = ParseState::ChunkSize(line);
                            self.fail(
                                ParseErrorKind::Body,
                                b"invalid HTTP chunk delimiter",
                                callback,
                                callback_egress,
                                &mut result,
                            );
                            continue;
                        }
                        let Some(size) = line.size.value() else {
                            self.state = ParseState::ChunkSize(line);
                            self.fail(
                                ParseErrorKind::Body,
                                b"invalid HTTP chunk length",
                                callback,
                                callback_egress,
                                &mut result,
                            );
                            continue;
                        };
                        if size > i64::MAX as u64 {
                            self.state = ParseState::ChunkSize(line);
                            self.fail(
                                ParseErrorKind::Body,
                                b"HTTP chunk length overflow",
                                callback,
                                callback_egress,
                                &mut result,
                            );
                        } else if size == 0 {
                            self.state = ParseState::Trailers(Vec::new());
                        } else {
                            self.state = ParseState::ChunkBody(size);
                            self.dispatch(
                                callback,
                                callback_egress,
                                CodecEvent::ChunkHeader {
                                    stream: self.ingress_stream,
                                    length: size as usize,
                                },
                                &mut result,
                            );
                        }
                    }
                    ParseState::ChunkBody(mut remaining) => {
                        let available = input.len() - result.consumed;
                        let amount =
                            usize::try_from(remaining.min(available as u64)).unwrap_or(available);
                        let start = result.consumed;
                        result.consumed += amount;
                        remaining -= amount as u64;
                        self.state = if remaining == 0 {
                            ParseState::ChunkDelimiter(false)
                        } else {
                            ParseState::ChunkBody(remaining)
                        };
                        self.emit_body(
                            input.slice(start..start + amount),
                            callback,
                            callback_egress,
                            &mut result,
                        );
                    }
                    ParseState::ChunkDelimiter(after_cr) => {
                        #[cfg(test)]
                        CHUNK_DELIMITER_TURNS.with(|turns| count_work(turns, 1));
                        let mut byte = input[result.consumed];
                        result.consumed += 1;
                        if byte == b'\r' && !after_cr && input.get(result.consumed) == Some(&b'\n')
                        {
                            byte = b'\n';
                            result.consumed += 1;
                        }
                        if byte == b'\n' {
                            self.state = ParseState::ChunkSize(ChunkSizeLine::default());
                            self.dispatch(
                                callback,
                                callback_egress,
                                CodecEvent::ChunkComplete {
                                    stream: self.ingress_stream,
                                },
                                &mut result,
                            );
                        } else if byte == b'\r' && !after_cr {
                            self.state = ParseState::ChunkDelimiter(true);
                        } else {
                            self.state = ParseState::ChunkDelimiter(after_cr);
                            self.fail(
                                ParseErrorKind::Body,
                                b"invalid HTTP chunk terminator",
                                callback,
                                callback_egress,
                                &mut result,
                            );
                        }
                    }
                    ParseState::Trailers(mut trailers) => {
                        let byte = input[result.consumed];
                        result.consumed += 1;
                        trailers.push(byte);
                        if trailers.len() > MAX_HEADER_BYTES {
                            self.state = ParseState::Trailers(trailers);
                            self.fail(
                                ParseErrorKind::Body,
                                b"HTTP trailers are too large",
                                callback,
                                callback_egress,
                                &mut result,
                            );
                        } else if Self::headers_terminated(&trailers)
                            || trailers == b"\r\n"
                            || trailers == b"\n"
                        {
                            match Self::parse_trailers(&trailers, self.strict_validation) {
                                Ok(parsed) => {
                                    self.state = ParseState::MessageComplete(self.ingress_upgrade);
                                    if !parsed.is_empty() {
                                        self.dispatch(
                                            callback,
                                            callback_egress,
                                            CodecEvent::TrailersComplete {
                                                stream: self.ingress_stream,
                                                trailers: Box::new(parsed),
                                            },
                                            &mut result,
                                        );
                                    }
                                    if !self.parser_paused {
                                        self.complete_message(
                                            callback,
                                            callback_egress,
                                            &mut result,
                                            self.ingress_upgrade,
                                        );
                                    }
                                }
                                Err(kind) => {
                                    self.state = ParseState::Trailers(trailers);
                                    self.fail(
                                        kind,
                                        b"invalid HTTP trailers",
                                        callback,
                                        callback_egress,
                                        &mut result,
                                    );
                                }
                            }
                        } else {
                            self.state = ParseState::Trailers(trailers);
                        }
                    }
                    ParseState::UntilEof => {
                        let start = result.consumed;
                        result.consumed = input.len();
                        self.state = ParseState::UntilEof;
                        self.emit_body(
                            input.slice(start..),
                            callback,
                            callback_egress,
                            &mut result,
                        );
                    }
                    ParseState::Upgraded => {
                        let start = result.consumed;
                        result.consumed = input.len();
                        self.state = ParseState::Upgraded;
                        self.emit_body(
                            input.slice(start..),
                            callback,
                            callback_egress,
                            &mut result,
                        );
                    }
                    ParseState::MessageComplete(upgraded) => {
                        self.state = ParseState::Idle;
                        self.complete_message(callback, callback_egress, &mut result, upgraded);
                    }
                }
            }
            result
        };

        if input.is_empty() {
            return parse_segment(&Bytes::new());
        }

        let mut result = CodecIngressResult::default();
        for segment in input.segments() {
            let segment_result = parse_segment(segment);
            let consumed = segment_result.consumed;
            result.consumed += consumed;
            result.egress_results.extend(segment_result.egress_results);
            if consumed != segment.len() {
                break;
            }
        }
        result
    }

    fn on_ingress_eof(
        &mut self,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
    ) -> CodecCallbackResult {
        let mut result = CodecCallbackResult::default();
        if self.parser_error {
            return result;
        }
        if self.parser_paused && !self.ingress_upgrade_complete {
            return result;
        }
        let state = std::mem::take(&mut self.state);
        match state {
            ParseState::Idle => {}
            ParseState::Upgraded => {
                self.dispatch_eof(
                    callback,
                    callback_egress,
                    CodecEvent::MessageComplete {
                        stream: self.ingress_stream,
                        upgraded: false,
                    },
                    &mut result,
                );
                self.state = ParseState::Upgraded;
            }
            ParseState::UntilEof => {
                self.complete_message_eof(callback, callback_egress, &mut result);
            }
            ParseState::MessageComplete(upgraded) => {
                if self.direction.is_upstream() && self.informational_response {
                    self.response_pending = true;
                    self.state = ParseState::Idle;
                } else {
                    if self.direction.is_downstream() {
                        self.request_pending = false;
                    } else {
                        self.response_pending = false;
                    }
                    self.dispatch_eof(
                        callback,
                        callback_egress,
                        CodecEvent::MessageComplete {
                            stream: self.ingress_stream,
                            upgraded,
                        },
                        &mut result,
                    );
                    self.state = if upgraded {
                        ParseState::Upgraded
                    } else {
                        ParseState::Idle
                    };
                }
            }
            unfinished => {
                self.state = unfinished;
                self.fail_eof(callback, callback_egress, &mut result);
            }
        }
        result
    }

    fn is_reusable(&self) -> bool {
        self.keepalive
            && !self.egress_upgrade
            && !self.ingress_upgrade
            && !self.parser_error
            && self.websocket_accept_key.is_empty()
    }

    fn is_waiting_to_drain(&self) -> bool {
        self.disable_keepalive_pending && self.keepalive
    }

    fn close_on_egress_complete(&self) -> bool {
        !self.is_egress_busy() && !self.is_reusable()
    }

    fn last_incoming_stream_id(&self) -> StreamId {
        self.ingress_stream
    }

    fn generate(
        &mut self,
        output: &mut BufQueue,
        frame: EgressFrame<'_>,
        _callback: &mut dyn HttpCodecCallback,
    ) -> Result<GeneratedFrame, HttpError> {
        let rollback = self.egress_rollback();
        let result = (|| -> Result<GeneratedFrame, HttpError> {
            match frame {
                EgressFrame::ConnectionPreface => Ok(GeneratedFrame::default()),
                EgressFrame::Headers {
                    stream,
                    message,
                    end_stream,
                    extra_headers,
                } => {
                    let (bytes, size) =
                        self.generate_headers(message, stream, end_stream, extra_headers)?;
                    let written = bytes.len();
                    self.push_output(output, bytes)?;
                    Ok(GeneratedFrame {
                        bytes_written: written,
                        header_size: Some(size),
                        payload_offset: None,
                    })
                }
                EgressFrame::Body {
                    stream,
                    mut body,
                    end_stream,
                    ..
                } => {
                    if stream != self.egress_stream {
                        return Err(invalid_state(b"HTTP/1 body stream is out of order"));
                    }
                    let body_len = body.chain_length();
                    let opens_chunk = body_len != 0 && self.egress_chunked && !self.in_chunk;
                    let (prefix, payload_offset) = if opens_chunk {
                        let prefix = chunk_prefix(body_len);
                        let offset = prefix.len() as u64;
                        (prefix, Some(offset))
                    } else {
                        (Cow::Borrowed(&[][..]), (body_len != 0).then_some(0))
                    };
                    let end = if end_stream {
                        let end = self.message_end();
                        self.finish_egress_message();
                        end
                    } else {
                        MessageEnd::Nothing
                    };
                    let suffix = if opens_chunk {
                        end.after_chunk()
                    } else {
                        end.bytes()
                    };
                    let written =
                        Self::check_frame_room(output, prefix.len(), body_len, suffix.len())?;
                    self.push_chunk_line(output, prefix)?;
                    self.append_output(output, &mut body)?;
                    Self::push_static_output(output, suffix)?;
                    Ok(GeneratedFrame {
                        bytes_written: written,
                        header_size: None,
                        payload_offset,
                    })
                }
                EgressFrame::ChunkHeader { stream, length } => {
                    if stream != self.egress_stream || length == 0 {
                        return Err(invalid_state(b"invalid HTTP/1 chunk header"));
                    }
                    let bytes = if self.egress_chunked {
                        if self.in_chunk {
                            return Err(invalid_state(b"HTTP/1 chunk already open"));
                        }
                        self.in_chunk = true;
                        chunk_prefix(length)
                    } else {
                        Cow::Borrowed(&[][..])
                    };
                    let written = bytes.len();
                    self.push_chunk_line(output, bytes)?;
                    Ok(GeneratedFrame {
                        bytes_written: written,
                        header_size: None,
                        payload_offset: None,
                    })
                }
                EgressFrame::ChunkTerminator { stream } => {
                    if stream != self.egress_stream {
                        return Err(invalid_state(b"HTTP/1 chunk stream is out of order"));
                    }
                    let bytes: &'static [u8] = if self.egress_chunked && self.in_chunk {
                        self.in_chunk = false;
                        b"\r\n"
                    } else {
                        b""
                    };
                    let written = bytes.len();
                    Self::push_static_output(output, bytes)?;
                    Ok(GeneratedFrame {
                        bytes_written: written,
                        header_size: None,
                        payload_offset: None,
                    })
                }
                EgressFrame::Trailers { stream, trailers } => {
                    if stream != self.egress_stream {
                        return Err(invalid_state(b"HTTP/1 trailer stream is out of order"));
                    }
                    let mut bytes = Vec::new();
                    if self.egress_chunked {
                        if self.in_chunk {
                            return Err(invalid_state(b"HTTP/1 trailers inside a chunk"));
                        }
                        bytes.extend_from_slice(b"0\r\n");
                        self.last_chunk_written = true;
                        trailers.for_each(|name, value| {
                            bytes.extend_from_slice(name);
                            bytes.extend_from_slice(b": ");
                            bytes.extend_from_slice(value);
                            bytes.extend_from_slice(b"\r\n");
                        });
                        self.append_eom(&mut bytes);
                        self.finish_egress_message();
                    }
                    let written = bytes.len();
                    self.push_output(output, bytes)?;
                    Ok(GeneratedFrame {
                        bytes_written: written,
                        header_size: None,
                        payload_offset: None,
                    })
                }
                EgressFrame::EndOfMessage { stream } => {
                    if stream != self.egress_stream {
                        return Err(invalid_state(b"HTTP/1 EOM stream is out of order"));
                    }
                    let bytes = self.message_end().bytes();
                    self.finish_egress_message();
                    let written = bytes.len();
                    Self::push_static_output(output, bytes)?;
                    Ok(GeneratedFrame {
                        bytes_written: written,
                        header_size: None,
                        payload_offset: None,
                    })
                }
                EgressFrame::Reset { .. } => {
                    self.keepalive = false;
                    Ok(GeneratedFrame::default())
                }
                EgressFrame::Goaway {
                    last_stream, code, ..
                } => {
                    if self.direction.is_upstream()
                        || self.disable_keepalive_pending
                        || last_stream != StreamId::MAX
                        || code != H2ErrorCode::NO_ERROR
                    {
                        self.keepalive = false;
                        self.disable_keepalive_pending = false;
                    } else {
                        self.disable_keepalive_pending = true;
                    }
                    Ok(GeneratedFrame::default())
                }
                EgressFrame::Padding { .. } => Ok(GeneratedFrame::default()),
                EgressFrame::PushPromise { .. }
                | EgressFrame::PingRequest { .. }
                | EgressFrame::PingReply { .. }
                | EgressFrame::Settings { .. }
                | EgressFrame::SettingsAck
                | EgressFrame::WindowUpdate { .. }
                | EgressFrame::Priority { .. }
                | EgressFrame::PushPriority { .. } => Err(HttpError::with_message(
                    HttpErrorCode::PROTOCOL_ERROR,
                    b"operation is unsupported by HTTP/1",
                )),
            }
        })();
        if result.is_err() {
            self.restore_egress(rollback);
        }
        result
    }
}

/// Returns true when the server selected any protocol advertised by the client.
pub fn server_accepted_upgrade(client: &[u8], server: &[u8]) -> bool {
    client
        .split(|byte| *byte == b',')
        .map(trim_http_whitespace)
        .filter(|token| !token.is_empty())
        .any(|client_token| {
            server
                .split(|byte| *byte == b',')
                .map(trim_http_whitespace)
                .filter(|token| !token.is_empty())
                .any(|server_token| client_token.eq_ignore_ascii_case(server_token))
        })
}

fn is_frozen_http_method(method: &[u8]) -> bool {
    FROZEN_HTTP_METHODS.contains(&method)
}

fn parse_version(value: &[u8]) -> Option<(u8, u8)> {
    const HTTP_1_1: u64 = u64::from_le_bytes(*b"HTTP/1.1");
    const HTTP_1_0: u64 = u64::from_le_bytes(*b"HTTP/1.0");
    if let Ok(word) = <[u8; 8]>::try_from(value) {
        match u64::from_le_bytes(word) {
            HTTP_1_1 => return Some((1, 1)),
            HTTP_1_0 => return Some((1, 0)),
            _ => {}
        }
    }
    let value = value.strip_prefix(b"HTTP/")?;
    let separator = value.iter().position(|byte| *byte == b'.')?;
    let (major, minor_with_separator) = value.split_at(separator);
    let minor = minor_with_separator.get(1..)?;
    #[cfg(test)]
    count_helper_work(|work| &work.version_number_parses, 2);
    let major = parse_decimal_u64(major)?;
    let minor = parse_decimal_u64(minor)?;
    Some((u8::try_from(major).ok()?, u8::try_from(minor).ok()?))
}

fn parse_decimal_u64(value: &[u8]) -> Option<u64> {
    if value.is_empty() {
        return None;
    }
    value.iter().try_fold(0_u64, |current, byte| {
        current.checked_mul(10)?.checked_add(u64::from(
            byte.checked_sub(b'0').filter(|digit| *digit <= 9)?,
        ))
    })
}

fn hex_digit(byte: u8) -> Option<u64> {
    match byte {
        b'0'..=b'9' => Some(u64::from(byte - b'0')),
        b'a'..=b'f' => Some(u64::from(byte - b'a' + 10)),
        b'A'..=b'F' => Some(u64::from(byte - b'A' + 10)),
        _ => None,
    }
}

fn trim_trailing_cr(value: &[u8]) -> &[u8] {
    value.strip_suffix(b"\r").unwrap_or(value)
}

/// Splits a header block at each LF except an escaped newline, an LF after
/// backslash, CR, backslash, which stays in its line. It yields at least one
/// line, and an empty line after a final LF.
fn header_lines(value: &[u8]) -> HeaderLines<'_> {
    HeaderLines::with_known(
        value,
        KnownLineFeeds {
            positions: &[],
            searched_to: 0,
        },
    )
}

struct HeaderLines<'a> {
    value: &'a [u8],
    start: usize,
    finished: bool,
    /// LF positions not yet split at or skipped, which hold every LF from
    /// `start` to `searched_to`.
    known: &'a [u32],
    searched_to: usize,
}

impl<'a> HeaderLines<'a> {
    /// The lines of `value`, whose LFs before `known.searched_to` are
    /// `known.positions`.
    fn with_known(value: &'a [u8], known: KnownLineFeeds<'a>) -> Self {
        Self {
            value,
            start: 0,
            finished: false,
            known: known.positions,
            searched_to: known.searched_to,
        }
    }

    /// Whether the LF at `index` ends a line rather than an escaped newline.
    #[inline]
    fn ends_line(&self, index: usize) -> bool {
        !(index >= 3
            && self.value[index - 1] == b'\\'
            && self.value[index - 2] == b'\r'
            && self.value[index - 3] == b'\\')
    }

    /// The offset of the first LF in `value[search..]`.
    #[inline]
    fn find_lf_from(&self, search: usize) -> Option<usize> {
        #[cfg(test)]
        count_helper_work(|work| &work.line_search_bytes, self.value.len() - search);
        word_scan::find_lf(&self.value[search..])
    }

    /// The next line, past the known LFs.
    fn search(&mut self) -> Option<&'a [u8]> {
        let value = self.value;
        let mut search = self.start.max(self.searched_to);
        while let Some(offset) = self.find_lf_from(search) {
            let index = search + offset;
            search = index + 1;
            if self.ends_line(index) {
                let line = &value[self.start..index];
                self.start = search;
                return Some(line);
            }
        }
        self.finished = true;
        Some(&value[self.start..])
    }
}

impl<'a> Iterator for HeaderLines<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<&'a [u8]> {
        if self.finished {
            return None;
        }
        while let Some((&index, known)) = self.known.split_first() {
            self.known = known;
            let index = index as usize;
            if self.ends_line(index) {
                let line = &self.value[self.start..index];
                self.start = index + 1;
                return Some(line);
            }
        }
        self.search()
    }
}

/// Whether the request line that ends a head at its first LF, `first` followed
/// by `second`, is HTTP/0.9. Split at each SP and TAB after one trailing CR is
/// trimmed, it holds two words, the first nonempty, or three, the last
/// `HTTP/0.9`. A last word equal to `HTTP/0.9` holds no SP or TAB, so the
/// split stops at the second delimiter.
fn request_line_is_http_09(first: &[u8], second: &[u8]) -> bool {
    if first.is_empty() {
        return line_is_http_09(second);
    }
    let byte = |index: usize| {
        if index < first.len() {
            first[index]
        } else {
            second[index - first.len()]
        }
    };
    let mut length = first.len() + second.len();
    if length > 0 && byte(length - 1) == b'\r' {
        length -= 1;
    }
    let mut delimiters = (0..length).filter(|index| {
        #[cfg(test)]
        count_helper_work(|work| &work.http_09_delimiter_reads, 1);
        matches!(byte(*index), b' ' | b'\t')
    });
    let Some(method_end) = delimiters.next() else {
        return false;
    };
    let Some(url_end) = delimiters.next() else {
        return method_end > 0;
    };
    let version = url_end + 1..length;
    version.len() == b"HTTP/0.9".len()
        && version
            .zip(b"HTTP/0.9")
            .all(|(index, expected)| byte(index) == *expected)
}

/// [`request_line_is_http_09`] of a request line in one slice.
fn line_is_http_09(line: &[u8]) -> bool {
    let line = trim_trailing_cr(line);
    let delimiter = |byte: &u8| {
        #[cfg(test)]
        count_helper_work(|work| &work.http_09_delimiter_reads, 1);
        matches!(*byte, b' ' | b'\t')
    };
    let Some(method_end) = line.iter().position(delimiter) else {
        return false;
    };
    let words = &line[method_end + 1..];
    let Some(url_length) = words.iter().position(delimiter) else {
        return method_end > 0;
    };
    words[url_length + 1..] == *b"HTTP/0.9"
}

fn trim_http_whitespace(value: &[u8]) -> &[u8] {
    let start = value
        .iter()
        .position(|byte| !matches!(*byte, b' ' | b'\t' | b'\r' | b'\n'))
        .unwrap_or(value.len());
    let end = value
        .iter()
        .rposition(|byte| !matches!(*byte, b' ' | b'\t' | b'\r' | b'\n'))
        .map_or(start, |index| index + 1);
    &value[start..end]
}

/// Finds the colon that ends a header line's name, and whether the name before
/// it is a nonempty token. The search continues past a byte that is not a
/// token byte, so a line with no colon is told apart from an invalid name.
#[inline]
fn header_name_end(line: &[u8]) -> Option<(usize, bool)> {
    let token_end = line
        .iter()
        .position(|byte| !header_name_byte(*byte))
        .unwrap_or(line.len());
    if line.get(token_end) == Some(&b':') {
        return Some((token_end, token_end != 0));
    }
    let colon = token_end + line[token_end..].iter().position(|byte| *byte == b':')?;
    Some((colon, false))
}

static HEADER_NAME_BYTE: [bool; 256] = {
    let mut table = [false; 256];
    let mut byte = 0;
    while byte < table.len() {
        table[byte] = is_header_name_byte(byte as u8);
        byte += 1;
    }
    table
};

#[inline]
fn header_name_byte(byte: u8) -> bool {
    HEADER_NAME_BYTE[usize::from(byte)]
}

const fn is_header_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

/// Whether `value` passes `valid_header_value_exact`. A value of only visible
/// ASCII and spaces passes in both modes without the per-byte check.
#[inline]
fn valid_header_value(value: &[u8], strict: bool) -> bool {
    let visible = word_scan::all_words(value, |word| {
        (word_scan::has_less(word, b' ')
            | (word & word_scan::HIGH)
            | word_scan::has_zero(word ^ word_scan::repeat(0x7f)))
            == 0
    });
    visible || valid_header_value_exact(value, strict)
}

/// Whether `value` holds a CR or an LF.
#[inline]
fn holds_cr_or_lf(value: &[u8]) -> bool {
    !word_scan::all_words(value, |word| {
        #[cfg(test)]
        count_helper_work(|work| &work.egress_cr_lf_loads, 1);
        // CR and LF are below 0x0e: most words hold no such byte.
        word_scan::has_less(word, 0x0e) == 0
            || (word_scan::has_zero(word ^ word_scan::repeat(b'\r'))
                | word_scan::has_zero(word ^ word_scan::repeat(b'\n')))
                == 0
    })
}

fn valid_header_value_exact(value: &[u8], strict: bool) -> bool {
    #[cfg(test)]
    count_helper_work(|work| &work.value_byte_checks, value.len());
    value.iter().enumerate().all(|(index, byte)| {
        if *byte >= 0x80 {
            return !strict;
        }
        match *byte {
            b'\t' | b' '..=b'~' => true,
            b'\r' if !strict => {
                index != 0 && value[index - 1] == b'\\' && value.get(index + 1) == Some(&b'\\')
            }
            b'\n' if !strict => index != 0 && value[index - 1] == b'\\',
            _ => false,
        }
    })
}

fn websocket_key(_stream: StreamId) -> Vec<u8> {
    let mut raw = [0_u8; 16];
    rand::rng().fill_bytes(&mut raw);
    base64::engine::general_purpose::STANDARD
        .encode(raw)
        .into_bytes()
}

fn websocket_accept(key: &[u8]) -> Vec<u8> {
    let mut hasher = Sha1::new();
    hasher.update(key);
    hasher.update(WEBSOCKET_GUID);
    base64::engine::general_purpose::STANDARD
        .encode(hasher.finalize())
        .into_bytes()
}

fn invalid_state(message: &'static [u8]) -> HttpError {
    HttpError::with_message(HttpErrorCode::INVALID_STATE_TRANSITION, message)
}

/// Reads the method and URL as `HttpMessage`'s `&mut` request accessors would:
/// a message with no kind reads as an empty method and URL, and a response
/// has neither.
fn request_fields(message: &HttpMessage) -> Option<(&str, &[u8])> {
    (!message.is_response()).then(|| {
        (
            message.request_method().unwrap_or_default(),
            message.request_url().unwrap_or_default(),
        )
    })
}

/// Reads the status and reason as `HttpMessage`'s `&mut` response accessors
/// would: a message with no kind reads as status 0 and an empty reason, and a
/// request has neither.
fn response_fields(message: &HttpMessage) -> Option<(u16, &[u8])> {
    (!message.is_request()).then(|| {
        (
            message.response_status().unwrap_or_default(),
            message.response_status_message().unwrap_or_default(),
        )
    })
}

/// Chunks shorter than this open with a line from [`CHUNK_LINES`].
const STATIC_CHUNK_LINES: usize = 0x1000;

/// The width of a [`CHUNK_LINES`] entry: three hex digits, then CRLF.
const CHUNK_LINE_WIDTH: usize = 5;

/// The line that opens each chunk shorter than [`STATIC_CHUNK_LINES`], right
/// aligned in its entry, so that queueing it allocates nothing.
static CHUNK_LINES: [u8; STATIC_CHUNK_LINES * CHUNK_LINE_WIDTH] = {
    let mut lines = [0; STATIC_CHUNK_LINES * CHUNK_LINE_WIDTH];
    let mut length = 0;
    while length < STATIC_CHUNK_LINES {
        write_chunk_line(&mut lines, (length + 1) * CHUNK_LINE_WIDTH, length);
        length += 1;
    }
    lines
};

/// Writes the line that opens a chunk, `length` in lowercase hex digits and
/// then CRLF, so that it ends at `end` in `line`, and returns its start. The
/// digits are written as [`push_decimal`] writes decimal ones, not through
/// `core::fmt`.
const fn write_chunk_line(line: &mut [u8], end: usize, mut length: usize) -> usize {
    const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut start = end - 2;
    line[start] = b'\r';
    line[start + 1] = b'\n';
    loop {
        start -= 1;
        line[start] = HEX_DIGITS[length % 16];
        length /= 16;
        if length == 0 {
            return start;
        }
    }
}

/// The line that opens a chunk: a static line from [`CHUNK_LINES`], or for a
/// chunk of at least [`STATIC_CHUNK_LINES`] bytes one buffer whose capacity is
/// the line's length.
fn chunk_prefix(length: usize) -> Cow<'static, [u8]> {
    if length < STATIC_CHUNK_LINES {
        let digits = (usize::BITS - (length | 1).leading_zeros()).div_ceil(4) as usize;
        let end = (length + 1) * CHUNK_LINE_WIDTH;
        return Cow::Borrowed(&CHUNK_LINES[end - 2 - digits..end]);
    }
    let mut line = [0_u8; usize::BITS as usize / 4 + 2];
    let end = line.len();
    let start = write_chunk_line(&mut line, end, length);
    Cow::Owned(line[start..].to_vec())
}

fn push_decimal(output: &mut Vec<u8>, mut value: u16) {
    let mut digits = [0_u8; 5];
    let mut start = digits.len();
    loop {
        start -= 1;
        digits[start] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    output.extend_from_slice(&digits[start..]);
}

fn header_size(length: usize) -> HttpHeaderSize {
    let length = u32::try_from(length).unwrap_or(u32::MAX);
    HttpHeaderSize {
        compressed: length,
        uncompressed: length,
        compressed_block: 0,
    }
}

/// Word-at-a-time byte classes over little-endian `u64` loads. A class mask
/// marks each matching byte with its high bit and is nonzero exactly when some
/// byte of the word matches, but only its lowest mark is exact: a borrow out
/// of a matching byte can also mark the bytes above it.
mod word_scan {
    pub(super) const HIGH: u64 = u64::from_ne_bytes([0x80; 8]);
    const ONES: u64 = u64::from_ne_bytes([0x01; 8]);
    const LOW: u64 = u64::from_ne_bytes([0x7f; 8]);

    pub(super) const fn repeat(byte: u8) -> u64 {
        ONES * byte as u64
    }

    /// Marks the zero bytes of `word`.
    pub(super) const fn has_zero(word: u64) -> u64 {
        word.wrapping_sub(ONES) & !word & HIGH
    }

    /// Marks the bytes of `word` below `bound`, which is at most 0x80.
    pub(super) const fn has_less(word: u64, bound: u8) -> u64 {
        word.wrapping_sub(repeat(bound)) & !word & HIGH
    }

    /// The index of the first byte of `bytes` whose word `class` marks at its
    /// lowest mark, or, past the last whole word, that `matches` accepts.
    #[inline]
    fn position(
        bytes: &[u8],
        class: impl Fn(u64) -> u64,
        matches: impl Fn(u8) -> bool,
    ) -> Option<usize> {
        let (words, tail) = bytes.as_chunks::<8>();
        for (index, word) in words.iter().enumerate() {
            let marks = class(u64::from_le_bytes(*word));
            if marks != 0 {
                return Some(index * 8 + (marks.trailing_zeros() / 8) as usize);
            }
        }
        tail.iter()
            .position(|byte| matches(*byte))
            .map(|offset| words.len() * 8 + offset)
    }

    /// Whether `test` holds for each of a set of words that covers every
    /// byte of `bytes`: its whole words and its last eight bytes, or for
    /// fewer than eight bytes one word built from them. A byte class that
    /// `test` checks in every byte of a word is thus checked in every byte of
    /// `bytes`, with no byte checked on its own.
    #[inline]
    pub(super) fn all_words(bytes: &[u8], test: impl Fn(u64) -> bool) -> bool {
        if let Some(last) = bytes.last_chunk::<8>() {
            let (words, _) = bytes.as_chunks::<8>();
            return words.iter().all(|word| test(u64::from_le_bytes(*word)))
                && test(u64::from_le_bytes(*last));
        }
        if let (Some(first), Some(last)) = (bytes.first_chunk::<4>(), bytes.last_chunk::<4>()) {
            let word = u64::from(u32::from_le_bytes(*first))
                | (u64::from(u32::from_le_bytes(*last)) << 32);
            return test(word);
        }
        match bytes {
            [] => true,
            [first, ..] => {
                let middle = bytes[bytes.len() / 2];
                let last = bytes[bytes.len() - 1];
                test(u64::from_le_bytes([
                    *first, middle, last, *first, *first, *first, *first, *first,
                ]))
            }
        }
    }

    /// The index of the first LF in `bytes`.
    #[inline]
    pub(super) fn find_lf(bytes: &[u8]) -> Option<usize> {
        position(
            bytes,
            |word| has_zero(word ^ repeat(b'\n')),
            |byte| byte == b'\n',
        )
    }

    /// The index of the first byte below 0x20 in `bytes`.
    #[inline]
    pub(super) fn find_control(bytes: &[u8]) -> Option<usize> {
        position(bytes, |word| has_less(word, b' '), |byte| byte < b' ')
    }

    /// The number of LFs in `bytes`. Each word's LFs are marked without a
    /// borrow between bytes, so every mark is exact.
    pub(super) fn count_lf(bytes: &[u8]) -> usize {
        let (words, tail) = bytes.as_chunks::<8>();
        let in_words: usize = words
            .iter()
            .map(|word| {
                let word = u64::from_le_bytes(*word) ^ repeat(b'\n');
                let marks = !(((word & LOW) + LOW) | word | LOW);
                ((marks >> 7).wrapping_mul(ONES) >> 56) as usize
            })
            .sum();
        in_words + tail.iter().filter(|byte| **byte == b'\n').count()
    }
}

#[cfg(test)]
#[path = "../tests/codec.rs"]
mod codec_http1;

#[cfg(test)]
#[path = "../tests/head_scan.rs"]
mod head_scan;

#[cfg(test)]
#[path = "../tests/generate_copies.rs"]
mod generate_copies;

#[cfg(test)]
#[path = "../tests/head_equivalence.rs"]
mod head_equivalence;
