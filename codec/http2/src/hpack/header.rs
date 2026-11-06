use std::borrow::Cow;
#[cfg(test)]
use std::cell::Cell;
use std::fmt;

use roxygen_core::headers::HttpCommonHeaders;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::headers::HttpHeaderName;

#[cfg(test)]
thread_local! {
    static NAME_BUFFERS: Cell<usize> = const { Cell::new(0) };
    static COMMON_NAME_HASHES: Cell<usize> = const { Cell::new(0) };
}

#[derive(Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HeaderName {
    bytes: Cow<'static, [u8]>,
    code: HttpHeaderCode,
}

impl Clone for HeaderName {
    fn clone(&self) -> Self {
        #[cfg(test)]
        if matches!(self.bytes, Cow::Owned(_)) {
            Self::record_name_buffer();
        }
        Self {
            bytes: self.bytes.clone(),
            code: self.code,
        }
    }
}

impl HeaderName {
    pub fn new(name: impl AsRef<[u8]>) -> Self {
        let name = name.as_ref();
        #[cfg(test)]
        Self::record_common_name_hash();
        let code = HttpCommonHeaders::hash(name);
        if code.as_u8() >= HttpHeaderCode::COMMON_OFFSET {
            Self::from_code(code)
        } else {
            Self::other_unchecked(name)
        }
    }

    /// Builds an uncommon name, lowercased into one buffer, for a name the caller has already
    /// classified as `HttpHeaderCode::Other`. It does not classify `name` again.
    pub(crate) fn other_unchecked(name: &[u8]) -> Self {
        #[cfg(test)]
        Self::record_name_buffer();
        Self {
            bytes: Cow::Owned(name.iter().map(u8::to_ascii_lowercase).collect()),
            code: HttpHeaderCode::Other,
        }
    }

    pub fn from_code(code: HttpHeaderCode) -> Self {
        assert!(
            code.as_u8() >= HttpHeaderCode::COMMON_OFFSET,
            "HPACK header codes must identify a generated common name"
        );
        Self {
            bytes: Cow::Borrowed(HttpCommonHeaders::name(
                code,
                roxygen_core::headers::HttpCommonHeaderTable::Lowercase,
            )),
            code,
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn code(&self) -> HttpHeaderCode {
        self.code
    }

    pub fn is_common_header(&self) -> bool {
        self.code.as_u8() >= HttpHeaderCode::COMMON_OFFSET
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Counts one heap buffer allocated to hold header-name bytes.
    #[cfg(test)]
    pub(crate) fn record_name_buffer() {
        NAME_BUFFERS.set(NAME_BUFFERS.get() + 1);
    }

    /// Counts one name-classifying (`HttpCommonHeaders::hash`) call made from this crate.
    #[cfg(test)]
    pub(crate) fn record_common_name_hash() {
        COMMON_NAME_HASHES.set(COMMON_NAME_HASHES.get() + 1);
    }

    /// Returns how many header-name buffers this thread has allocated since the last call.
    #[cfg(test)]
    pub(crate) fn take_name_buffers() -> usize {
        NAME_BUFFERS.take()
    }

    /// Returns how many name-classifying calls this thread has made since the last call.
    #[cfg(test)]
    pub(crate) fn take_common_name_hashes() -> usize {
        COMMON_NAME_HASHES.take()
    }
}

impl fmt::Display for HeaderName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", String::from_utf8_lossy(&self.bytes))
    }
}

impl From<HttpHeaderCode> for HeaderName {
    fn from(code: HttpHeaderCode) -> Self {
        Self::from_code(code)
    }
}

impl From<&str> for HeaderName {
    fn from(name: &str) -> Self {
        Self::new(name)
    }
}

impl From<&[u8]> for HeaderName {
    fn from(name: &[u8]) -> Self {
        Self::new(name)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub struct HpackHeader {
    pub name: HeaderName,
    pub value: Box<[u8]>,
}

impl HpackHeader {
    pub const ENTRY_OVERHEAD: u32 = 32;

    pub fn new(name: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Self {
        Self {
            name: HeaderName::new(name),
            value: value.as_ref().to_vec().into_boxed_slice(),
        }
    }

    pub fn from_name(name: HeaderName, value: impl AsRef<[u8]>) -> Self {
        Self {
            name,
            value: value.as_ref().to_vec().into_boxed_slice(),
        }
    }

    pub fn real_bytes(&self) -> u32 {
        Self::real_bytes_for(self.name.len(), self.value.len())
    }

    pub fn real_bytes_for(name_size: usize, value_size: usize) -> u32 {
        u32::try_from(name_size.saturating_add(value_size)).unwrap_or(u32::MAX)
    }

    pub fn bytes(&self) -> u32 {
        Self::bytes_for(self.name.len(), self.value.len())
    }

    pub fn bytes_for(name_size: usize, value_size: usize) -> u32 {
        Self::ENTRY_OVERHEAD.saturating_add(Self::real_bytes_for(name_size, value_size))
    }

    pub(crate) fn as_parts(&self) -> (&HeaderName, &[u8]) {
        (&self.name, &self.value)
    }

    pub fn has_value(&self) -> bool {
        !self.value.is_empty()
    }
}

impl fmt::Display for HpackHeader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}: {}",
            self.name,
            String::from_utf8_lossy(&self.value)
        )
    }
}

pub(crate) struct EncodeHeader<'a> {
    name: HeaderName,
    value: Cow<'a, [u8]>,
}

impl<'a> EncodeHeader<'a> {
    pub(crate) fn borrowed(name: HeaderName, value: &'a [u8]) -> Self {
        Self {
            name,
            value: Cow::Borrowed(value),
        }
    }

    pub(crate) fn as_parts(&self) -> (&HeaderName, &[u8]) {
        (&self.name, &self.value)
    }

    pub(crate) fn into_owned(self) -> HpackHeader {
        HpackHeader {
            name: self.name,
            value: self.value.into_owned().into_boxed_slice(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Header<'a> {
    pub code: HttpHeaderCode,
    pub name: &'a [u8],
    pub value: &'a [u8],
}

impl<'a> Header<'a> {
    pub fn new(name: HttpHeaderName<'a>, value: &'a [u8]) -> Self {
        match name {
            HttpHeaderName::Code(code) => Self {
                code,
                name: HttpCommonHeaders::name(
                    code,
                    roxygen_core::headers::HttpCommonHeaderTable::Lowercase,
                ),
                value,
            },
            HttpHeaderName::Name(name) => Self {
                code: HttpCommonHeaders::hash(name),
                name,
                value,
            },
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct HeaderPiece<'a> {
    bytes: Cow<'a, [u8]>,
    multi_valued: bool,
}

impl<'a> HeaderPiece<'a> {
    pub fn borrowed(bytes: &'a [u8], multi_valued: bool) -> Self {
        Self {
            bytes: Cow::Borrowed(bytes),
            multi_valued,
        }
    }

    pub fn owned(bytes: impl Into<Vec<u8>>, multi_valued: bool) -> Self {
        Self {
            bytes: Cow::Owned(bytes.into()),
            multi_valued,
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.bytes.as_ref()
    }

    pub fn is_owned(&self) -> bool {
        matches!(self.bytes, Cow::Owned(_))
    }

    pub fn is_multi_valued(&self) -> bool {
        self.multi_valued
    }
}
