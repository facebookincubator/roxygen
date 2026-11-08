use std::fmt;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::OnceLock;

use crate::owned_bytes::OwnedBytes;

macro_rules! define_common_headers {
    ($(($variant:ident, $code:literal, $camel:literal, $lower:literal)),+ $(,)?) => {
        /// Stable generated code for a common HTTP header name.
        ///
        /// The discriminants follow `HTTPCommonHeaders.txt` exactly, after the
        /// two reserved `None` and `Other` values.
        #[repr(u8)]
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub enum HttpHeaderCode {
            /// No valid header is present.
            None = 0,
            /// The header name is not in the generated common-name table.
            Other = 1,
            $($variant = $code),+
        }

        impl HttpHeaderCode {
            /// The first numeric code assigned to a common header.
            pub const COMMON_OFFSET: u8 = 2;

            /// Every valid code, in numeric order.
            pub const ALL: &'static [Self] = &[
                Self::None,
                Self::Other,
                $(Self::$variant),+
            ];

            /// Returns the code with the supplied generated numeric value.
            pub fn from_u8(value: u8) -> Option<Self> {
                Self::ALL.get(usize::from(value)).copied()
            }

            /// Returns the generated numeric value.
            pub const fn as_u8(self) -> u8 {
                self as u8
            }
        }

        const COMMON_HEADER_DEFINITIONS: &[(HttpHeaderCode, &str, &str)] = &[
            $((HttpHeaderCode::$variant, $camel, $lower)),+
        ];
    };
}

define_common_headers!(
    (ColonAuthority, 2, ":Authority", ":authority"),
    (ColonMethod, 3, ":Method", ":method"),
    (ColonPath, 4, ":Path", ":path"),
    (ColonScheme, 5, ":Scheme", ":scheme"),
    (ColonStatus, 6, ":Status", ":status"),
    (ColonProtocol, 7, ":Protocol", ":protocol"),
    (Accept, 8, "Accept", "accept"),
    (AcceptEncoding, 9, "Accept-Encoding", "accept-encoding"),
    (AcceptLanguage, 10, "Accept-Language", "accept-language"),
    (AcceptRanges, 11, "Accept-Ranges", "accept-ranges"),
    (
        AccessControlAllowCredentials,
        12,
        "Access-Control-Allow-Credentials",
        "access-control-allow-credentials"
    ),
    (
        AccessControlAllowMethods,
        13,
        "Access-Control-Allow-Methods",
        "access-control-allow-methods"
    ),
    (
        AccessControlAllowOrigin,
        14,
        "Access-Control-Allow-Origin",
        "access-control-allow-origin"
    ),
    (
        AccessControlExposeHeaders,
        15,
        "Access-Control-Expose-Headers",
        "access-control-expose-headers"
    ),
    (Age, 16, "Age", "age"),
    (AltSvc, 17, "Alt-Svc", "alt-svc"),
    (Authorization, 18, "Authorization", "authorization"),
    (CacheControl, 19, "Cache-Control", "cache-control"),
    (ClientTimeout, 20, "client_timeout", "client_timeout"),
    (Connection, 21, "Connection", "connection"),
    (ContentDigest, 22, "content-digest", "content-digest"),
    (
        ContentDisposition,
        23,
        "Content-Disposition",
        "content-disposition"
    ),
    (ContentEncoding, 24, "Content-Encoding", "content-encoding"),
    (ContentLanguage, 25, "Content-Language", "content-language"),
    (ContentLength, 26, "Content-Length", "content-length"),
    (ContentRange, 27, "Content-Range", "content-range"),
    (
        ContentSecurityPolicy,
        28,
        "content-security-policy",
        "content-security-policy"
    ),
    (ContentType, 29, "Content-Type", "content-type"),
    (Cookie, 30, "Cookie", "cookie"),
    (
        CrossOriginEmbedderPolicyReportOnly,
        31,
        "cross-origin-embedder-policy-report-only",
        "cross-origin-embedder-policy-report-only"
    ),
    (
        CrossOriginOpenerPolicy,
        32,
        "cross-origin-opener-policy",
        "cross-origin-opener-policy"
    ),
    (
        CrossOriginResourcePolicy,
        33,
        "cross-origin-resource-policy",
        "cross-origin-resource-policy"
    ),
    (Date, 34, "Date", "date"),
    (DocumentPolicy, 35, "document-policy", "document-policy"),
    (EdgeControl, 36, "Edge-Control", "edge-control"),
    (Etag, 37, "ETag", "etag"),
    (Expires, 38, "Expires", "expires"),
    (Host, 39, "Host", "host"),
    (
        IfModifiedSince,
        40,
        "If-Modified-Since",
        "if-modified-since"
    ),
    (IfNoneMatch, 41, "If-None-Match", "if-none-match"),
    (KeepAlive, 42, "Keep-Alive", "keep-alive"),
    (LastModified, 43, "Last-Modified", "last-modified"),
    (Location, 44, "Location", "location"),
    (NoVarySearch, 45, "no-vary-search", "no-vary-search"),
    (
        PermissionsPolicy,
        46,
        "permissions-policy",
        "permissions-policy"
    ),
    (Pragma, 47, "Pragma", "pragma"),
    (Priority, 48, "Priority", "priority"),
    (
        ProxyAuthenticate,
        49,
        "Proxy-Authenticate",
        "proxy-authenticate"
    ),
    (
        ProxyAuthorization,
        50,
        "Proxy-Authorization",
        "proxy-authorization"
    ),
    (ProxyConnection, 51, "Proxy-Connection", "proxy-connection"),
    (ProxyStatus, 52, "Proxy-Status", "proxy-status"),
    (QueueTimeout, 53, "queue_timeout", "queue_timeout"),
    (Range, 54, "Range", "range"),
    (Referer, 55, "Referer", "referer"),
    (ReportTo, 56, "report-to", "report-to"),
    (
        ReportingEndpoints,
        57,
        "reporting-endpoints",
        "reporting-endpoints"
    ),
    (RpcKind, 58, "rpckind", "rpckind"),
    (
        SecWebSocketAccept,
        59,
        "Sec-WebSocket-Accept",
        "sec-websocket-accept"
    ),
    (
        SecWebSocketKey,
        60,
        "Sec-WebSocket-Key",
        "sec-websocket-key"
    ),
    (Server, 61, "Server", "server"),
    (SetCookie, 62, "Set-Cookie", "set-cookie"),
    (
        StrictTransportSecurity,
        63,
        "Strict-Transport-Security",
        "strict-transport-security"
    ),
    (Te, 64, "TE", "te"),
    (
        TimingAllowOrigin,
        65,
        "timing-allow-origin",
        "timing-allow-origin"
    ),
    (Trailer, 66, "Trailer", "trailer"),
    (
        TransferEncoding,
        67,
        "Transfer-Encoding",
        "transfer-encoding"
    ),
    (Upgrade, 68, "Upgrade", "upgrade"),
    (UserAgent, 69, "User-Agent", "user-agent"),
    (Vary, 70, "Vary", "vary"),
    (Via, 71, "Via", "via"),
    (
        XContentTypeOptions,
        72,
        "X-Content-Type-Options",
        "x-content-type-options"
    ),
    (XForwardedFor, 73, "X-Forwarded-For", "x-forwarded-for"),
    (XFrameOptions, 74, "X-Frame-Options", "x-frame-options"),
    (XRobotsTag, 75, "x-robots-tag", "x-robots-tag"),
    (XXssProtection, 76, "X-XSS-Protection", "x-xss-protection"),
);

const MAX_COMMON_HEADER_NAME_LENGTH: usize = {
    let mut maximum = 0;
    let mut index = 0;
    while index < COMMON_HEADER_DEFINITIONS.len() {
        let length = COMMON_HEADER_DEFINITIONS[index].1.len();
        if length > maximum {
            maximum = length;
        }
        index += 1;
    }
    maximum
};

/// The common-header slot table has `1 << COMMON_HEADER_SLOT_BITS` slots.
const COMMON_HEADER_SLOT_BITS: u32 = 11;
const COMMON_HEADER_SLOTS: usize = 1 << COMMON_HEADER_SLOT_BITS;
/// A slot that holds no common name. It is not a valid entry index.
const EMPTY_COMMON_HEADER_SLOT: u8 = u8::MAX;
const _: () = assert!(COMMON_HEADER_DEFINITIONS.len() < EMPTY_COMMON_HEADER_SLOT as usize);
/// The most words a common name has between its first and last 8 bytes.
const COMMON_HEADER_MIDDLE_WORDS: usize = common_header_middle_words(MAX_COMMON_HEADER_NAME_LENGTH);

/// The number of 8-byte words that cover the bytes of a `length`-byte name
/// between its first and last 8 bytes.
const fn common_header_middle_words(length: usize) -> usize {
    length.saturating_sub(9) / 8
}

/// The little-endian 8-byte word at `offset` in `name`.
#[inline]
const fn common_header_word(name: &[u8], offset: usize) -> u64 {
    match name.split_at(offset).1.first_chunk::<8>() {
        Some(word) => u64::from_le_bytes(*word),
        None => panic!("a common-header word lies inside the name"),
    }
}

/// The two words that a name is hashed by and compared on first, as
/// little-endian integers.
///
/// A name of 8 or more bytes gives its first and last 8 bytes, which cover a
/// name of up to 16 bytes. A shorter name is packed whole into the first word
/// from its overlapping first and last halves (4 bytes each, or 2 for a name
/// of 2 or 3 bytes); the remaining bytes are zero.
#[inline]
const fn common_header_end_words(name: &[u8]) -> [u64; 2] {
    if let (Some(first), Some(last)) = (name.first_chunk::<8>(), name.last_chunk::<8>()) {
        return [u64::from_le_bytes(*first), u64::from_le_bytes(*last)];
    }
    if let (Some(first), Some(last)) = (name.first_chunk::<4>(), name.last_chunk::<4>()) {
        let first = u32::from_le_bytes(*first) as u64;
        let last = u32::from_le_bytes(*last) as u64;
        return [first | last << 32, 0];
    }
    if let (Some(first), Some(last)) = (name.first_chunk::<2>(), name.last_chunk::<2>()) {
        let first = u16::from_le_bytes(*first) as u64;
        let last = u16::from_le_bytes(*last) as u64;
        return [first | last << 16, 0];
    }
    match name.first() {
        Some(byte) => [*byte as u64, 0],
        None => [0, 0],
    }
}

/// The slot of a name of `length` bytes with end words `words` (see
/// `common_header_end_words`).
///
/// Each byte is ORed with 0x20 first, so names equal under ASCII case folding
/// share a slot.
#[inline]
const fn common_header_slot(seed: u64, length: usize, words: [u64; 2]) -> usize {
    const FOLD: u64 = u64::from_ne_bytes([0x20; 8]);
    let key = (words[0] | FOLD).wrapping_mul(seed) ^ (words[1] | FOLD) ^ length as u64;
    (key.wrapping_mul(seed) >> (u64::BITS - COMMON_HEADER_SLOT_BITS)) as usize
}

/// Places every common name in its own slot under `seed`, or returns `None`
/// when two names share a slot.
const fn place_common_headers(seed: u64) -> Option<[u8; COMMON_HEADER_SLOTS]> {
    let mut slots = [EMPTY_COMMON_HEADER_SLOT; COMMON_HEADER_SLOTS];
    let mut index = 0;
    while index < COMMON_HEADER_DEFINITIONS.len() {
        let name = COMMON_HEADER_DEFINITIONS[index].1.as_bytes();
        let slot = common_header_slot(seed, name.len(), common_header_end_words(name));
        if slots[slot] != EMPTY_COMMON_HEADER_SLOT {
            return None;
        }
        slots[slot] = index as u8;
        index += 1;
    }
    Some(slots)
}

/// The first seed of a fixed odd sequence (splitmix64) under which every
/// common name has its own slot.
const COMMON_HEADER_SLOT_SEED: u64 = {
    let mut attempt = 1_u64;
    loop {
        assert!(
            attempt <= 4096,
            "no seed within 4096 places every common header name in its own slot"
        );
        let mut seed = attempt.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        seed = (seed ^ (seed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        seed = (seed ^ (seed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        seed = (seed ^ (seed >> 31)) | 1;
        if place_common_headers(seed).is_some() {
            break seed;
        }
        attempt += 1;
    }
};

/// One 8-byte word of a common name, prepared for an ASCII case-insensitive
/// comparison with a word of a candidate name.
#[derive(Clone, Copy)]
struct CommonHeaderWord {
    /// The word of the name's ASCII-lowercase spelling.
    lower: u64,
    /// 0x20 in each byte that holds a letter in the name, zero elsewhere.
    letters: u64,
}

impl CommonHeaderWord {
    const ZERO: Self = Self {
        lower: 0,
        letters: 0,
    };

    /// Zero exactly when each byte of `word` equals this word's byte, or is
    /// its uppercase letter: ORing 0x20 lowercases a letter, and only a
    /// letter's two cases differ in that bit alone.
    #[inline]
    fn difference(self, word: u64) -> u64 {
        (word | self.letters) ^ self.lower
    }
}

#[derive(Clone, Copy)]
struct CommonHeaderEntry {
    code: HttpHeaderCode,
    length: u8,
    /// The words `common_header_end_words` gives for the name.
    ends: [CommonHeaderWord; 2],
}

/// A perfect hash of the common names: each name has its own slot, which
/// holds the index of its entry; every other slot is empty.
struct CommonHeaderLookup {
    slots: [u8; COMMON_HEADER_SLOTS],
    entries: [CommonHeaderEntry; COMMON_HEADER_DEFINITIONS.len()],
    /// Each name's words between its first and last 8 bytes, at offsets 8,
    /// 16, and so on.
    middles: [[CommonHeaderWord; COMMON_HEADER_MIDDLE_WORDS]; COMMON_HEADER_DEFINITIONS.len()],
}

static COMMON_HEADER_LOOKUP: CommonHeaderLookup = {
    let Some(slots) = place_common_headers(COMMON_HEADER_SLOT_SEED) else {
        panic!("the slot seed places every common header name in its own slot");
    };
    let mut lookup = CommonHeaderLookup {
        slots,
        entries: [CommonHeaderEntry {
            code: HttpHeaderCode::Other,
            length: 0,
            ends: [CommonHeaderWord::ZERO; 2],
        }; COMMON_HEADER_DEFINITIONS.len()],
        middles: [[CommonHeaderWord::ZERO; COMMON_HEADER_MIDDLE_WORDS];
            COMMON_HEADER_DEFINITIONS.len()],
    };
    let mut index = 0;
    while index < COMMON_HEADER_DEFINITIONS.len() {
        let (code, camel, _) = COMMON_HEADER_DEFINITIONS[index];
        let camel = camel.as_bytes();
        let mut lower = [0; MAX_COMMON_HEADER_NAME_LENGTH];
        let mut letters = [0; MAX_COMMON_HEADER_NAME_LENGTH];
        let mut byte = 0;
        while byte < camel.len() {
            lower[byte] = camel[byte].to_ascii_lowercase();
            if camel[byte].is_ascii_alphabetic() {
                letters[byte] = 0x20;
            }
            byte += 1;
        }
        let lower = lower.split_at(camel.len()).0;
        let letters = letters.split_at(camel.len()).0;
        let lower_ends = common_header_end_words(lower);
        let letter_ends = common_header_end_words(letters);
        lookup.entries[index] = CommonHeaderEntry {
            code,
            length: camel.len() as u8,
            ends: [
                CommonHeaderWord {
                    lower: lower_ends[0],
                    letters: letter_ends[0],
                },
                CommonHeaderWord {
                    lower: lower_ends[1],
                    letters: letter_ends[1],
                },
            ],
        };
        let mut middle = 0;
        while middle < common_header_middle_words(camel.len()) {
            lookup.middles[index][middle] = CommonHeaderWord {
                lower: common_header_word(lower, 8 + 8 * middle),
                letters: common_header_word(letters, 8 + 8 * middle),
            };
            middle += 1;
        }
        index += 1;
    }
    lookup
};

/// Test-only counts of the work `HttpCommonHeaders::hash` does.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct CommonHeaderLookupWork {
    /// Lookup-table positions examined.
    pub(crate) probes: usize,
    /// Loads from the looked-up name: one per byte, or one per word of up to
    /// 8 bytes.
    pub(crate) name_reads: usize,
}

#[cfg(test)]
thread_local! {
    static COMMON_HEADER_LOOKUP_WORK: std::cell::Cell<CommonHeaderLookupWork> =
        std::cell::Cell::new(CommonHeaderLookupWork::default());
}

#[cfg(test)]
fn count_common_header_lookup_work(probes: usize, name_reads: usize) {
    COMMON_HEADER_LOOKUP_WORK.with(|work| {
        let mut counted = work.get();
        counted.probes += probes;
        counted.name_reads += name_reads;
        work.set(counted);
    });
}

/// Returns and resets this thread's `HttpCommonHeaders::hash` work counts.
#[cfg(test)]
pub(crate) fn take_common_header_lookup_work() -> CommonHeaderLookupWork {
    COMMON_HEADER_LOOKUP_WORK.take()
}

/// Selects one of the generated common-header spelling tables.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum HttpCommonHeaderTable {
    /// Spellings from `HTTPCommonHeaders.txt`.
    CamelCase,
    /// ASCII-lowercase spellings.
    Lowercase,
}

/// Generated common HTTP-header lookup operations.
pub struct HttpCommonHeaders;

impl HttpCommonHeaders {
    /// Number of generated codes, including the two reserved codes.
    pub const NUM_CODES: usize = HttpHeaderCode::ALL.len();

    /// Returns the generated code for `name`, using ASCII-insensitive matching.
    pub fn hash(name: impl AsRef<[u8]>) -> HttpHeaderCode {
        let name = name.as_ref();
        if name.is_empty() || name.len() > MAX_COMMON_HEADER_NAME_LENGTH {
            return HttpHeaderCode::Other;
        }
        let words = common_header_end_words(name);
        #[cfg(test)]
        count_common_header_lookup_work(1, if name.len() == 1 { 1 } else { 2 });
        let lookup = &COMMON_HEADER_LOOKUP;
        let index = usize::from(
            lookup.slots[common_header_slot(COMMON_HEADER_SLOT_SEED, name.len(), words)],
        );
        let Some(entry) = lookup.entries.get(index) else {
            return HttpHeaderCode::Other;
        };
        if usize::from(entry.length) != name.len() {
            return HttpHeaderCode::Other;
        }
        let mut difference =
            entry.ends[0].difference(words[0]) | entry.ends[1].difference(words[1]);
        for (middle, word) in lookup.middles[index].iter().enumerate() {
            let offset = 8 + 8 * middle;
            if offset + 8 >= name.len() {
                break;
            }
            #[cfg(test)]
            count_common_header_lookup_work(0, 1);
            difference |= word.difference(common_header_word(name, offset));
        }
        if difference == 0 {
            entry.code
        } else {
            HttpHeaderCode::Other
        }
    }

    /// Returns the static spelling for `code` in the selected table.
    ///
    /// The two reserved codes map to an empty byte string, matching the generated
    /// C++ tables.
    pub fn name(code: HttpHeaderCode, table: HttpCommonHeaderTable) -> &'static [u8] {
        common_name_table(table)[usize::from(code.as_u8())].as_ref()
    }

    /// Returns whether `name` is a reference obtained from the selected table.
    pub fn is_name_from_table(name: Option<&[u8]>, table: HttpCommonHeaderTable) -> bool {
        Self::code_from_table_name(name, table).as_u8() >= HttpHeaderCode::COMMON_OFFSET
    }

    /// Reverse-maps a reference obtained from the selected name table.
    ///
    /// Content-equal byte strings from outside the table return [`HttpHeaderCode::Other`],
    /// preserving the pointer-identity contract of the generated C++ API. `None`
    /// represents a null pointer and returns [`HttpHeaderCode::None`].
    pub fn code_from_table_name(
        name: Option<&[u8]>,
        table: HttpCommonHeaderTable,
    ) -> HttpHeaderCode {
        let Some(name) = name else {
            return HttpHeaderCode::None;
        };
        common_name_table(table)
            .iter()
            .enumerate()
            .skip(usize::from(HttpHeaderCode::COMMON_OFFSET))
            .find_map(|(index, candidate)| {
                std::ptr::eq::<[u8]>(candidate.as_ref(), name).then(|| {
                    HttpHeaderCode::from_u8(index as u8)
                        .expect("the generated name table only contains valid header codes")
                })
            })
            .unwrap_or(HttpHeaderCode::Other)
    }
}

/// A borrowed byte-string header name or a generated common-header code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpHeaderName<'a> {
    /// A generated common-header code.
    Code(HttpHeaderCode),
    /// An arbitrary byte-string header name.
    Name(&'a [u8]),
}

#[derive(Clone, Copy)]
enum ResolvedHeaderName<'a> {
    Code(HttpHeaderCode),
    Other(&'a [u8]),
}

impl<'a> From<HttpHeaderCode> for HttpHeaderName<'a> {
    fn from(code: HttpHeaderCode) -> Self {
        Self::Code(code)
    }
}

impl<'a> From<&'a str> for HttpHeaderName<'a> {
    fn from(name: &'a str) -> Self {
        Self::Name(name.as_bytes())
    }
}

impl<'a> From<&'a String> for HttpHeaderName<'a> {
    fn from(name: &'a String) -> Self {
        Self::Name(name.as_bytes())
    }
}

impl<'a> From<&'a [u8]> for HttpHeaderName<'a> {
    fn from(name: &'a [u8]) -> Self {
        Self::Name(name)
    }
}

impl<'a, const LENGTH: usize> From<&'a [u8; LENGTH]> for HttpHeaderName<'a> {
    fn from(name: &'a [u8; LENGTH]) -> Self {
        Self::Name(name.as_slice())
    }
}

impl<'a> From<&'a Vec<u8>> for HttpHeaderName<'a> {
    fn from(name: &'a Vec<u8>) -> Self {
        Self::Name(name.as_slice())
    }
}

impl<'a> From<&'a Box<[u8]>> for HttpHeaderName<'a> {
    fn from(name: &'a Box<[u8]>) -> Self {
        Self::Name(name.as_ref())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum HeaderEntry {
    CommonInline {
        code: HttpHeaderCode,
        len: u8,
        bytes: [u8; 22],
    },
    CommonHeap {
        code: HttpHeaderCode,
        value: Box<[u8]>,
    },
    OtherInline {
        name_len: u8,
        len: u8,
        bytes: [u8; 22],
    },
    OtherHeap {
        name_len: usize,
        bytes: Box<[u8]>,
    },
}

impl HeaderEntry {
    fn new(name: ResolvedHeaderName<'_>, value: &[u8]) -> Self {
        match name {
            ResolvedHeaderName::Code(code) => {
                if value.len() <= 22 {
                    let mut bytes = [0; 22];
                    bytes[..value.len()].copy_from_slice(value);
                    Self::CommonInline {
                        code,
                        len: value.len() as u8,
                        bytes,
                    }
                } else {
                    Self::CommonHeap {
                        code,
                        value: value.to_vec().into_boxed_slice(),
                    }
                }
            }
            ResolvedHeaderName::Other(name) => {
                let len = name
                    .len()
                    .checked_add(value.len())
                    .expect("HTTP header name and value length overflow");
                if len <= 22 {
                    let mut bytes = [0; 22];
                    bytes[..name.len()].copy_from_slice(name);
                    bytes[name.len()..len].copy_from_slice(value);
                    Self::OtherInline {
                        name_len: name.len() as u8,
                        len: len as u8,
                        bytes,
                    }
                } else {
                    let mut bytes = Vec::with_capacity(len);
                    bytes.extend_from_slice(name);
                    bytes.extend_from_slice(value);
                    Self::OtherHeap {
                        name_len: name.len(),
                        bytes: bytes.into_boxed_slice(),
                    }
                }
            }
        }
    }

    #[inline]
    fn code(&self) -> HttpHeaderCode {
        match self {
            Self::CommonInline { code, .. } | Self::CommonHeap { code, .. } => *code,
            Self::OtherInline { .. } | Self::OtherHeap { .. } => HttpHeaderCode::Other,
        }
    }

    #[inline]
    fn name(&self) -> &[u8] {
        match self {
            Self::CommonInline { code, .. } | Self::CommonHeap { code, .. } => match code {
                HttpHeaderCode::None | HttpHeaderCode::Other => b"",
                code => {
                    // Entry names are not references into the public pointer-identity tables.
                    let index = usize::from(code.as_u8() - HttpHeaderCode::COMMON_OFFSET);
                    COMMON_HEADER_DEFINITIONS[index].1.as_bytes()
                }
            },
            Self::OtherInline {
                name_len, bytes, ..
            } => &bytes[..usize::from(*name_len)],
            Self::OtherHeap { name_len, bytes } => &bytes[..*name_len],
        }
    }

    #[inline]
    fn value(&self) -> &[u8] {
        match self {
            Self::CommonInline { len, bytes, .. } => &bytes[..usize::from(*len)],
            Self::CommonHeap { value, .. } => value,
            Self::OtherInline {
                name_len,
                len,
                bytes,
            } => &bytes[usize::from(*name_len)..usize::from(*len)],
            Self::OtherHeap { name_len, bytes } => &bytes[*name_len..],
        }
    }
}

/// Result of looking up a header that must have exactly one value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SingleHeaderValue<'a> {
    /// The value when exactly one instance exists; absent for zero or duplicates.
    pub value: Option<&'a [u8]>,
    /// Whether at least one instance exists.
    pub exists: bool,
}

impl<'a> SingleHeaderValue<'a> {
    /// Returns the unique value or an empty byte string for zero or duplicate values.
    pub fn value_or_empty(self) -> &'a [u8] {
        self.value.unwrap_or(b"")
    }
}

#[derive(Clone, Eq, PartialEq)]
enum HeaderEntries {
    Owned(Vec<HeaderEntry>),
    Shared(Arc<Vec<HeaderEntry>>),
}

impl HeaderEntries {
    fn make_mut(&mut self) -> &mut Vec<HeaderEntry> {
        if matches!(self, Self::Shared(_)) {
            let Self::Shared(entries) = std::mem::replace(self, Self::Owned(Vec::new())) else {
                unreachable!("shared entries were selected");
            };
            *self = Self::Owned(Arc::try_unwrap(entries).unwrap_or_else(|entries| {
                let mut owned = Vec::with_capacity(entries.capacity());
                owned.extend(entries.iter().cloned());
                owned
            }));
        }
        let Self::Owned(entries) = self else {
            unreachable!("shared entries were detached");
        };
        entries
    }
}

impl Deref for HeaderEntries {
    type Target = Vec<HeaderEntry>;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(entries) => entries,
            Self::Shared(entries) => entries,
        }
    }
}

/// An owned, insertion-ordered collection of HTTP header name/value pairs.
#[derive(Clone, Eq)]
pub struct HttpHeaders {
    entries: HeaderEntries,
    // Removals may leave false positives; a clear bit always proves absence.
    present_codes: [u64; HttpCommonHeaders::NUM_CODES.div_ceil(u64::BITS as usize)],
}

impl PartialEq for HttpHeaders {
    fn eq(&self, other: &Self) -> bool {
        *self.entries == *other.entries
    }
}

impl HttpHeaders {
    /// Separator used when combining duplicate header values.
    pub const COMBINE_SEPARATOR: &'static [u8] = b", ";

    const INITIAL_CAPACITY: usize = 16;

    /// Creates an empty collection with Proxygen's initial capacity.
    pub fn new() -> Self {
        Self {
            entries: HeaderEntries::Owned(Vec::with_capacity(Self::INITIAL_CAPACITY)),
            present_codes: [0; HttpCommonHeaders::NUM_CODES.div_ceil(u64::BITS as usize)],
        }
    }

    /// Creates an empty collection that allocates Proxygen's initial capacity
    /// on its first insert.
    pub(crate) fn unallocated() -> Self {
        Self {
            entries: HeaderEntries::Owned(Vec::new()),
            present_codes: [0; HttpCommonHeaders::NUM_CODES.div_ceil(u64::BITS as usize)],
        }
    }

    /// Reserves room for at least `additional` more headers, and for at least
    /// Proxygen's initial capacity.
    ///
    /// This is a performance hint with no observable effect. A collection that
    /// already has room is left as it is, even when its entries are shared. A
    /// reservation that cannot be made is ignored and leaves the headers and
    /// their order unchanged, though a collection without room may already
    /// have stopped sharing its entries.
    #[doc(hidden)]
    pub fn reserve(&mut self, additional: usize) {
        let target = Self::INITIAL_CAPACITY.max(self.entries.len().saturating_add(additional));
        if self.entries.capacity() >= target {
            return;
        }
        let entries = self.entries.make_mut();
        let _ = entries.try_reserve(target - entries.len());
    }

    /// Makes subsequent clones share immutable entries until either collection is mutated.
    ///
    /// Promotion retains the current vector and its capacity. Ordinary construction and
    /// append remain directly owned; promotion allocates one shared ownership block.
    pub fn share_entries(&mut self) {
        if let HeaderEntries::Owned(entries) = &mut self.entries {
            self.entries = HeaderEntries::Shared(Arc::new(std::mem::take(entries)));
        }
    }

    /// Adds a header after trimming surrounding space, LF, HT, and CR from its value.
    ///
    /// Existing values with the same name remain in their insertion positions.
    pub fn add<'a>(&mut self, name: impl Into<HttpHeaderName<'a>>, value: impl AsRef<[u8]>) {
        let name = Self::resolve_name(name.into());
        let value = trim_ascii_whitespace(value.as_ref());
        let entry = HeaderEntry::new(name, value);
        self.remember_code(entry.code());
        self.entries_for_push().push(entry);
    }

    /// Adds a header whose name the caller has already classified as uncommon
    /// (`HttpCommonHeaders::hash` returns `Other`), without classifying it again.
    ///
    /// Otherwise the same as `add`.
    #[doc(hidden)]
    pub fn add_uncommon(&mut self, name: &[u8], value: &[u8]) {
        assert!(!name.is_empty(), "HTTP header names must not be empty");
        debug_assert_eq!(
            HttpCommonHeaders::hash(name),
            HttpHeaderCode::Other,
            "add_uncommon requires a name without a common code"
        );
        let entry = HeaderEntry::new(
            ResolvedHeaderName::Other(name),
            trim_ascii_whitespace(value),
        );
        self.remember_code(entry.code());
        self.entries_for_push().push(entry);
    }

    /// Adds every header from an initializer-style sequence in sequence order.
    pub fn add_all<'a, I, V>(&mut self, headers: I)
    where
        I: IntoIterator<Item = (HttpHeaderName<'a>, V)>,
        V: AsRef<[u8]>,
    {
        for (name, value) in headers {
            self.add(name, value);
        }
    }

    /// Replaces every matching value with one new value at the end.
    pub fn set<'a>(&mut self, name: impl Into<HttpHeaderName<'a>>, value: impl AsRef<[u8]>) {
        let name = name.into();
        self.remove(name);
        self.add(name, value);
    }

    /// Removes hyphen/underscore variants before adding the supplied spelling.
    pub fn set_one_version(
        &mut self,
        name: impl AsRef<[u8]>,
        code: HttpHeaderCode,
        value: impl AsRef<[u8]>,
    ) {
        let name = name.as_ref();
        self.remove_all_versions(code, name);
        self.add(HttpHeaderName::Name(name), value);
    }

    /// Returns whether at least one matching header exists.
    pub fn exists<'a>(&self, name: impl Into<HttpHeaderName<'a>>) -> bool {
        let name = Self::resolve_query_name(name.into());
        if !self.may_contain(name) {
            return false;
        }
        self.entries
            .iter()
            .any(|entry| Self::entry_matches(entry, name))
    }

    /// Combines matching values in insertion order.
    pub fn combine<'a>(&self, name: impl Into<HttpHeaderName<'a>>, separator: &[u8]) -> Box<[u8]> {
        let name = Self::resolve_query_name(name.into());
        if !self.may_contain(name) {
            return Box::default();
        }
        let mut combined = Vec::new();
        for entry in self
            .entries
            .iter()
            .filter(|entry| Self::entry_matches(entry, name))
        {
            if !combined.is_empty() {
                combined.extend_from_slice(separator);
            }
            combined.extend_from_slice(entry.value());
        }
        combined.into_boxed_slice()
    }

    /// Combines matching values with [`Self::COMBINE_SEPARATOR`].
    pub fn combine_default<'a>(&self, name: impl Into<HttpHeaderName<'a>>) -> Box<[u8]> {
        self.combine(name, Self::COMBINE_SEPARATOR)
    }

    /// Visits every header in insertion order.
    pub fn for_each(&self, mut visitor: impl FnMut(&[u8], &[u8])) {
        for entry in self.entries.iter() {
            visitor(entry.name(), entry.value());
        }
    }

    /// Visits every header and its generated code in insertion order.
    pub fn for_each_with_code(&self, mut visitor: impl FnMut(HttpHeaderCode, &[u8], &[u8])) {
        for entry in self.entries.iter() {
            visitor(entry.code(), entry.name(), entry.value());
        }
    }

    /// Removes entries selected by `predicate`, preserving all remaining order.
    pub fn remove_by_predicate(
        &mut self,
        mut predicate: impl FnMut(HttpHeaderCode, &[u8], &[u8]) -> bool,
    ) -> bool {
        let original_len = self.entries.len();
        self.entries
            .make_mut()
            .retain(|entry| !predicate(entry.code(), entry.name(), entry.value()));
        self.entries.len() != original_len
    }

    /// Returns the unique-value result for a name or code.
    pub fn get_single<'a, 'name>(
        &'a self,
        name: impl Into<HttpHeaderName<'name>>,
    ) -> SingleHeaderValue<'a> {
        let name = Self::resolve_query_name(name.into());
        if !self.may_contain(name) {
            return SingleHeaderValue {
                value: None,
                exists: false,
            };
        }
        let mut matching = self
            .entries
            .iter()
            .filter(|entry| Self::entry_matches(entry, name));
        let Some(first) = matching.next() else {
            return SingleHeaderValue {
                value: None,
                exists: false,
            };
        };
        if matching.next().is_some() {
            SingleHeaderValue {
                value: None,
                exists: true,
            }
        } else {
            SingleHeaderValue {
                value: Some(first.value()),
                exists: true,
            }
        }
    }

    /// Returns the sole matching value, or an empty byte string otherwise.
    pub fn get_single_or_empty<'a, 'name>(
        &'a self,
        name: impl Into<HttpHeaderName<'name>>,
    ) -> &'a [u8] {
        self.get_single(name).value_or_empty()
    }

    /// Returns an owned copy of the sole matching value, or an empty byte string.
    pub fn raw_get<'a>(&self, name: impl Into<HttpHeaderName<'a>>) -> Box<[u8]> {
        self.get_single_or_empty(name).to_vec().into_boxed_slice()
    }

    /// Returns the number of matching values.
    pub fn number_of_values<'a>(&self, name: impl Into<HttpHeaderName<'a>>) -> usize {
        let name = Self::resolve_query_name(name.into());
        if !self.may_contain(name) {
            return 0;
        }
        self.entries
            .iter()
            .filter(|entry| Self::entry_matches(entry, name))
            .count()
    }

    /// Visits matching values in insertion order until `visitor` returns true.
    ///
    /// Each value is lent for as long as the headers are, as
    /// `forEachValueOfHeader` hands its function a reference into the headers.
    /// Returns true when visitation stopped early.
    pub fn for_each_value<'headers, 'a>(
        &'headers self,
        name: impl Into<HttpHeaderName<'a>>,
        mut visitor: impl FnMut(&'headers [u8]) -> bool,
    ) -> bool {
        let name = Self::resolve_query_name(name.into());
        if !self.may_contain(name) {
            return false;
        }
        for entry in self.entries.iter() {
            if Self::entry_matches(entry, name) && visitor(entry.value()) {
                return true;
            }
        }
        false
    }

    /// Removes every matching header and reports whether anything was removed.
    pub fn remove<'a>(&mut self, name: impl Into<HttpHeaderName<'a>>) -> bool {
        let name = Self::resolve_query_name(name.into());
        if !self.may_contain(name) {
            return false;
        }
        let original_len = self.entries.len();
        self.entries
            .make_mut()
            .retain(|entry| !Self::entry_matches(entry, name));
        self.entries.len() != original_len
    }

    /// Removes the common code plus uncommon names that differ only by `-`/`_`.
    pub fn remove_all_versions(&mut self, code: HttpHeaderCode, name: &[u8]) -> bool {
        let original_len = self.entries.len();
        self.entries.make_mut().retain(|entry| {
            let common_match = code != HttpHeaderCode::Other && entry.code() == code;
            let uncommon_match = entry.code() == HttpHeaderCode::Other
                && case_underscore_insensitive_equal(name, entry.name());
            !(common_match || uncommon_match)
        });
        self.entries.len() != original_len
    }

    /// Removes every header while retaining allocated collection capacity.
    pub fn clear(&mut self) {
        if let HeaderEntries::Shared(entries) = &self.entries
            && Arc::strong_count(entries) != 1
        {
            self.entries = HeaderEntries::Owned(Vec::with_capacity(entries.capacity()));
        } else {
            self.entries.make_mut().clear();
        }
        self.present_codes.fill(0);
    }

    /// Alias for [`Self::clear`] matching the source operation name.
    pub fn remove_all(&mut self) {
        self.clear();
    }

    /// Moves connection-named and fixed hop-by-hop headers into `stripped`.
    pub fn strip_per_hop_headers(
        &mut self,
        stripped: &mut Self,
        strip_priority: bool,
        custom_per_hop_headers: Option<&Self>,
    ) {
        let mut connection_index = 0;
        while let Some(value) = self
            .entries
            .iter()
            .filter(|entry| entry.code() == HttpHeaderCode::Connection)
            .nth(connection_index)
            .map(|entry| OwnedBytes::new(entry.value()))
        {
            for name in connection_header_tokens(value.bytes()) {
                self.transfer_header_if_present(name, stripped);
            }
            if !self.exists(HttpHeaderCode::Connection) {
                break;
            }
            connection_index += 1;
        }

        let mut retained = Vec::with_capacity(self.entries.capacity());
        for entry in self.entries.make_mut().drain(..) {
            let code = entry.code();
            let per_hop = if code == HttpHeaderCode::Other {
                custom_per_hop_headers.is_some_and(|headers| headers.exists(entry.name()))
            } else {
                is_standard_per_hop(code)
                    || (strip_priority && code == HttpHeaderCode::Priority)
                    || custom_per_hop_headers.is_some_and(|headers| headers.exists(code))
            };
            if per_hop {
                stripped.remember_code(entry.code());
                stripped.entries_for_push().push(entry);
            } else {
                retained.push(entry);
            }
        }
        self.entries = HeaderEntries::Owned(retained);
    }

    /// Returns the total number of name/value pairs, including duplicates.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns the total number of name/value pairs, including duplicates.
    pub fn size(&self) -> usize {
        self.len()
    }

    /// Returns true when the collection contains no headers.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Appends copies of every header to `destination` in insertion order.
    pub fn copy_to(&self, destination: &mut Self) {
        for (destination, source) in destination.present_codes.iter_mut().zip(self.present_codes) {
            *destination |= source;
        }
        destination.reserve(self.len());
        destination
            .entries_for_push()
            .extend(self.entries.iter().cloned());
    }

    /// Moves all headers out while leaving this collection reusable and empty.
    ///
    /// As after Proxygen's move constructor, the emptied collection holds no
    /// storage until its next insert allocates Proxygen's initial capacity.
    pub fn take(&mut self) -> Self {
        std::mem::replace(self, Self::unallocated())
    }

    /// Replaces this collection by moving all headers out of `source`.
    ///
    /// As Proxygen's move assignment does, `source` is left with this
    /// collection's storage, emptied.
    pub fn move_from(&mut self, source: &mut Self) {
        self.clear();
        std::mem::swap(self, source);
    }

    /// Iterates over code, name, and value in insertion order.
    #[inline]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (HttpHeaderCode, &[u8], &[u8])> {
        self.entries
            .iter()
            .map(|entry| (entry.code(), entry.name(), entry.value()))
    }

    fn resolve_name(name: HttpHeaderName<'_>) -> ResolvedHeaderName<'_> {
        match name {
            HttpHeaderName::Code(code) => ResolvedHeaderName::Code(code),
            HttpHeaderName::Name(name) => {
                assert!(!name.is_empty(), "HTTP header names must not be empty");
                let code = HttpCommonHeaders::hash(name);
                if code == HttpHeaderCode::Other {
                    ResolvedHeaderName::Other(name)
                } else {
                    ResolvedHeaderName::Code(code)
                }
            }
        }
    }

    fn resolve_query_name(name: HttpHeaderName<'_>) -> ResolvedHeaderName<'_> {
        match name {
            HttpHeaderName::Code(code) => ResolvedHeaderName::Code(code),
            HttpHeaderName::Name(name) => {
                let code = HttpCommonHeaders::hash(name);
                if code == HttpHeaderCode::Other {
                    ResolvedHeaderName::Other(name)
                } else {
                    ResolvedHeaderName::Code(code)
                }
            }
        }
    }

    /// The owned entries for an insert, with Proxygen's initial capacity
    /// allocated when they have none.
    fn entries_for_push(&mut self) -> &mut Vec<HeaderEntry> {
        let entries = self.entries.make_mut();
        if entries.capacity() == 0 {
            entries.reserve_exact(Self::INITIAL_CAPACITY);
        }
        entries
    }

    #[cfg(test)]
    pub(crate) fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    #[cfg(test)]
    pub(crate) fn entries_address(&self) -> usize {
        self.entries.as_ptr().addr()
    }

    fn remember_code(&mut self, code: HttpHeaderCode) {
        let code = usize::from(code.as_u8());
        self.present_codes[code / u64::BITS as usize] |= 1_u64 << (code % u64::BITS as usize);
    }

    fn may_contain(&self, name: ResolvedHeaderName<'_>) -> bool {
        let code = match name {
            ResolvedHeaderName::Code(code) => code,
            ResolvedHeaderName::Other(_) => HttpHeaderCode::Other,
        };
        let code = usize::from(code.as_u8());
        self.present_codes[code / u64::BITS as usize] & (1_u64 << (code % u64::BITS as usize)) != 0
    }

    #[inline]
    fn entry_matches(entry: &HeaderEntry, name: ResolvedHeaderName<'_>) -> bool {
        match name {
            ResolvedHeaderName::Code(code) => entry.code() == code,
            ResolvedHeaderName::Other(name) => Self::entry_name_matches(entry, name),
        }
    }

    #[inline(never)]
    fn entry_name_matches(entry: &HeaderEntry, name: &[u8]) -> bool {
        entry.code() == HttpHeaderCode::Other && entry.name().eq_ignore_ascii_case(name)
    }

    fn transfer_header_if_present(&mut self, name: &[u8], destination: &mut Self) -> bool {
        let name = Self::resolve_query_name(HttpHeaderName::Name(name));
        if !self.may_contain(name) {
            return false;
        }
        let original_len = self.entries.len();
        let mut retained = Vec::with_capacity(self.entries.capacity());
        for entry in self.entries.make_mut().drain(..) {
            if Self::entry_matches(&entry, name) {
                destination.remember_code(entry.code());
                destination.entries_for_push().push(entry);
            } else {
                retained.push(entry);
            }
        }
        self.entries = HeaderEntries::Owned(retained);
        self.entries.len() != original_len
    }
}

impl Default for HttpHeaders {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for HttpHeaders {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_list()
            .entries(
                self.entries
                    .iter()
                    .map(|entry| (entry.name(), entry.value())),
            )
            .finish()
    }
}

fn case_underscore_insensitive_equal(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .copied()
            .zip(right.iter().copied())
            .all(|(left, right)| {
                let normalize = |byte: u8| {
                    if byte == b'_' {
                        b'-'
                    } else {
                        byte.to_ascii_lowercase()
                    }
                };
                normalize(left) == normalize(right)
            })
}

fn common_name_table(table: HttpCommonHeaderTable) -> &'static [Box<[u8]>] {
    static CAMELCASE_NAMES: OnceLock<Box<[Box<[u8]>]>> = OnceLock::new();
    static LOWERCASE_NAMES: OnceLock<Box<[Box<[u8]>]>> = OnceLock::new();

    let names = match table {
        HttpCommonHeaderTable::CamelCase => &CAMELCASE_NAMES,
        HttpCommonHeaderTable::Lowercase => &LOWERCASE_NAMES,
    };
    names
        .get_or_init(|| {
            let mut names = Vec::with_capacity(HttpCommonHeaders::NUM_CODES);
            names.push(Box::<[u8]>::default());
            names.push(Box::<[u8]>::default());
            names.extend(
                COMMON_HEADER_DEFINITIONS
                    .iter()
                    .map(|(_, camel, lower)| match table {
                        HttpCommonHeaderTable::CamelCase => {
                            camel.as_bytes().to_vec().into_boxed_slice()
                        }
                        HttpCommonHeaderTable::Lowercase => {
                            lower.as_bytes().to_vec().into_boxed_slice()
                        }
                    }),
            );
            names.into_boxed_slice()
        })
        .as_ref()
}

#[inline]
fn is_lws(byte: u8) -> bool {
    matches!(byte, b' ' | b'\n' | b'\t' | b'\r')
}

#[inline]
fn trim_ascii_whitespace(value: &[u8]) -> &[u8] {
    let start = value
        .iter()
        .position(|byte| !is_lws(*byte))
        .unwrap_or(value.len());
    let end = value
        .iter()
        .rposition(|byte| !is_lws(*byte))
        .map_or(start, |index| index + 1);
    &value[start..end]
}

fn connection_header_tokens(value: &[u8]) -> Vec<&[u8]> {
    let prefix_end = value
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(value.len());
    let bytes = &value[..prefix_end];
    let mut tokens = Vec::new();
    let mut start = 0;
    while start < bytes.len() && is_lws(bytes[start]) {
        start += 1;
    }

    while start < bytes.len() {
        if let Some(relative_comma) = bytes[start..].iter().position(|byte| *byte == b',') {
            let comma = start + relative_comma;
            let mut end = comma;
            while end > start && is_lws(bytes[end - 1]) {
                end -= 1;
            }
            if end > start {
                tokens.push(&bytes[start..end]);
            }
            start = comma + 1;
            while start < bytes.len() && is_lws(bytes[start]) {
                start += 1;
            }
        } else {
            let mut end = start;
            while end < bytes.len() && !is_lws(bytes[end]) {
                end += 1;
            }
            if end > start {
                tokens.push(&bytes[start..end]);
            }
            break;
        }
    }
    tokens
}

fn is_standard_per_hop(code: HttpHeaderCode) -> bool {
    matches!(
        code,
        HttpHeaderCode::Connection
            | HttpHeaderCode::KeepAlive
            | HttpHeaderCode::ProxyAuthenticate
            | HttpHeaderCode::ProxyAuthorization
            | HttpHeaderCode::ProxyConnection
            | HttpHeaderCode::Te
            | HttpHeaderCode::Trailer
            | HttpHeaderCode::TransferEncoding
            | HttpHeaderCode::Upgrade
    )
}

#[cfg(test)]
mod shared_entries_tests {
    use super::*;
    use crate::message::HttpMessage;

    fn sample() -> HttpHeaders {
        let mut headers = HttpHeaders::new();
        headers.add(HttpHeaderCode::Host, b"example.test");
        headers.add(HttpHeaderCode::Date, b"Mon, 21 Sep 2026 12:00:00 GMT");
        headers.add(b"X-Dup", b"one");
        headers.add(b"x-dup", b"two");
        headers.add(b"x_version", b"old");
        headers
    }

    fn backing(headers: &HttpHeaders) -> (*const HeaderEntry, *const u8) {
        (
            headers.entries.as_ptr(),
            headers.get_single_or_empty(HttpHeaderCode::Date).as_ptr(),
        )
    }

    #[test]
    fn ordinary_headers_own_their_vector_until_explicit_promotion() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<HttpHeaders>();
        assert_send_sync::<HttpMessage>();
        let mut headers = sample();
        assert!(matches!(headers.entries, HeaderEntries::Owned(_)));
        assert_eq!(headers.entries.capacity(), HttpHeaders::INITIAL_CAPACITY);
        let original = backing(&headers);
        let owned_clone = headers.clone();
        assert_ne!(backing(&owned_clone).0, original.0);
        assert_ne!(backing(&owned_clone).1, original.1);

        headers.share_entries();
        headers.share_entries();
        assert_eq!(backing(&headers), original);
        assert_eq!(headers.entries.capacity(), HttpHeaders::INITIAL_CAPACITY);
        let clone = headers.clone();
        assert_eq!(backing(&clone), original);
        assert_eq!(clone.entries.capacity(), headers.entries.capacity());
        assert_eq!(clone, owned_clone);
        assert_eq!(clone.combine_default(b"X-DUP").as_ref(), b"one, two");
        let HeaderEntries::Shared(entries) = &headers.entries else {
            panic!("promotion installs shared entries");
        };
        assert_eq!(Arc::strong_count(entries), 2);
        assert!(!headers.remove(HttpHeaderCode::Server));
        assert_eq!(backing(&headers), original);
        assert!(matches!(headers.entries, HeaderEntries::Shared(_)));
    }

    #[test]
    fn header_mutations_detach_once_and_preserve_order_presence_and_capacity() {
        let mutations: [fn(&mut HttpHeaders); 8] = [
            |headers| headers.add(HttpHeaderCode::Server, b"new"),
            |headers| headers.add_all([(HttpHeaderName::from(b"x-more".as_slice()), b"new")]),
            |headers| headers.set(b"X-Dup", b"replacement"),
            |headers| headers.set_one_version(b"x-version", HttpHeaderCode::Other, b"new"),
            |headers| assert!(headers.remove(b"X-Dup")),
            |headers| assert!(headers.remove_all_versions(HttpHeaderCode::Other, b"x-version")),
            |headers| {
                let mut visited = Vec::new();
                assert!(headers.remove_by_predicate(|_, name, _| {
                    visited.push(name.to_vec());
                    name.eq_ignore_ascii_case(b"x-dup")
                }));
                assert_eq!(visited.len(), 5);
            },
            HttpHeaders::clear,
        ];
        for mutate in mutations {
            let original = sample();
            let mut expected = sample();
            let mut template = sample();
            template.share_entries();
            let retained = backing(&template);
            let capacity = template.entries.capacity();
            let mut actual = template.clone();
            mutate(&mut expected);
            mutate(&mut actual);
            assert_eq!(actual, expected);
            assert_eq!(template, original);
            assert_eq!(backing(&template), retained);
            assert!(matches!(actual.entries, HeaderEntries::Owned(_)));
            assert_eq!(actual.entries.capacity(), capacity);
            for &code in HttpHeaderCode::ALL {
                assert_eq!(actual.exists(code), expected.exists(code));
                assert_eq!(
                    actual.number_of_values(code),
                    expected.number_of_values(code)
                );
            }
            let detached = actual.entries.as_ptr();
            actual.add(HttpHeaderCode::ContentLength, b"0");
            assert_eq!(actual.entries.as_ptr(), detached);
        }
    }

    #[test]
    fn stripping_and_copying_detach_both_shared_destinations() {
        let mut template = sample();
        template.add(HttpHeaderCode::Connection, b"x-dup, date");
        template.add(HttpHeaderCode::Upgrade, b"websocket");
        let mut expected = template.clone();
        template.share_entries();
        let original = backing(&template);
        let mut actual = template.clone();
        let mut destination = sample();
        let mut expected_destination = destination.clone();
        destination.share_entries();
        let sibling = destination.clone();
        expected.strip_per_hop_headers(&mut expected_destination, false, None);
        actual.strip_per_hop_headers(&mut destination, false, None);
        assert_eq!(actual, expected);
        assert_eq!(destination, expected_destination);
        assert_eq!(backing(&template), original);
        assert_eq!(sibling, sample());
        assert_eq!(destination.number_of_values(b"X-DUP"), 4);
        assert!(!actual.exists(HttpHeaderCode::Date));
        assert!(destination.exists(HttpHeaderCode::Date));

        let mut copied = template.clone();
        let mut expected_copy = expected_destination;
        template.copy_to(&mut expected_copy);
        destination.share_entries();
        copied.clone_from(&destination);
        template.copy_to(&mut copied);
        assert_eq!(copied, expected_copy);
        assert_eq!(backing(&template), original);
    }

    #[test]
    fn moves_extraction_and_metadata_preserve_shared_backing() {
        let mut message = HttpMessage::new();
        *message.headers_mut() = sample();
        message.headers_mut().share_entries();
        let sibling = message.clone();
        let original = backing(message.headers());
        message.set_http_version(2, 0);
        message.set_chunked(true);
        message.set_upgraded(true);
        message.set_advanced_protocol_string(b"h2");
        message.set_start_time(std::time::Instant::now());
        assert_eq!(backing(message.headers_mut()), original);
        assert!(!sibling.is_chunked());
        assert!(!sibling.is_upgraded());
        assert_ne!(message.http_version(), sibling.http_version());
        let mut extracted = message.extract_headers();
        assert_eq!(backing(&extracted), original);
        assert!(message.headers().is_empty());
        assert!(matches!(message.headers().entries, HeaderEntries::Owned(_)));
        assert_eq!(message.headers().entries.capacity(), 0);
        let mut moved = HttpHeaders::new();
        moved.move_from(&mut extracted);
        assert_eq!(backing(&moved), original);
        assert_eq!(backing(sibling.headers()), original);
        assert!(extracted.is_empty());
        assert_eq!(extracted.entries.capacity(), HttpHeaders::INITIAL_CAPACITY);
    }

    #[test]
    fn last_shared_owner_recovers_vector_without_copying_entries() {
        let mut headers = sample();
        let original = backing(&headers);
        let capacity = headers.entries.capacity();
        headers.share_entries();
        headers.add(HttpHeaderCode::Server, b"new");
        assert!(matches!(headers.entries, HeaderEntries::Owned(_)));
        assert_eq!(backing(&headers), original);
        assert_eq!(headers.entries.capacity(), capacity);
    }
}
