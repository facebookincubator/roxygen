use std::borrow::Cow;
use std::mem::size_of;
use std::rc::Rc;

use roxygen_core::buf_queue::BufQueue;
use roxygen_core::buf_queue::BufQueueError;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::http::HttpHeaderSize;

use super::DEFAULT_MAX_UNCOMPRESSED;
use super::DEFAULT_TABLE_SIZE;
use super::DefaultIndexingStrategy;
use super::EncodeHeader;
use super::EncodedHeaderBlock;
use super::Header;
use super::HeaderIndexingStrategy;
use super::HeaderInputs;
use super::HeaderName;
use super::HpackContext;
use super::HpackDecodeBuffer;
use super::HpackEncodeBuffer;
use super::HpackError;
use super::HpackHeader;
use super::INDEX_REF;
use super::LITERAL;
use super::LITERAL_INC_INDEX;
use super::TABLE_SIZE_UPDATE;
use super::buffer::LITERAL_SCRATCH;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CompressionInfoPart {
    pub header_table_size: u32,
    pub bytes_stored: u32,
    pub headers_stored: u32,
    pub inserts: u32,
    pub blocked_inserts: u32,
    pub duplications: u32,
    pub static_refs: u32,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct CompressionInfo {
    pub egress: CompressionInfoPart,
    pub ingress: CompressionInfoPart,
}

impl PartialEq for CompressionInfo {
    fn eq(&self, other: &Self) -> bool {
        self.egress.header_table_size == other.egress.header_table_size
            && self.egress.bytes_stored == other.egress.bytes_stored
            && self.egress.headers_stored == other.egress.headers_stored
            && self.ingress.header_table_size == other.ingress.header_table_size
            && self.ingress.bytes_stored == other.ingress.bytes_stored
            && self.ingress.headers_stored == other.ingress.headers_stored
    }
}

impl Eq for CompressionInfo {}

pub fn prepare_headers(headers: &[Header<'_>]) -> (Vec<HpackHeader>, u32) {
    let prepared: Vec<_> = headers
        .iter()
        .map(|header| HpackHeader::new(header.name, header.value))
        .collect();
    let uncompressed = uncompressed_size(prepared.iter().map(HpackHeader::as_parts));
    (prepared, uncompressed)
}

fn prepare_encode_headers<'a>(headers: &[Header<'a>]) -> Vec<EncodeHeader<'a>> {
    headers
        .iter()
        .map(|header| EncodeHeader::borrowed(HeaderName::new(header.name), header.value))
        .collect()
}

fn uncompressed_size<'a>(headers: impl Iterator<Item = (&'a HeaderName, &'a [u8])>) -> u32 {
    headers.fold(0_u32, |total, (name, value)| {
        total.saturating_add(HpackHeader::real_bytes_for(name.len(), value.len()) + 2)
    })
}

fn input_name(header: &Header<'_>) -> HeaderName {
    if header.code.as_u8() >= HttpHeaderCode::COMMON_OFFSET {
        HeaderName::from_code(header.code)
    } else {
        HeaderName::new(header.name)
    }
}

/// The encoded-size bound of an empty block: two table size updates of at most 11 bytes each.
pub(crate) const EMPTY_BLOCK_BOUND: usize = 22;

/// Adds one field to an encoded-size bound: three integers of at most 11 bytes each, and at most
/// 30 Huffman bits per literal byte.
fn add_encoded_header_bound(total: usize, name_size: usize, value_size: usize) -> Option<usize> {
    let literal_bytes = name_size.checked_add(value_size)?;
    total.checked_add(33_usize.checked_add(literal_bytes.checked_mul(4)?)?)
}

/// Adds one input field to an encoded-size bound; `None` (overflow) stays `None`.
pub(crate) fn add_input_bound(bound: Option<usize>, header: &Header<'_>) -> Option<usize> {
    bound.and_then(|total| add_encoded_header_bound(total, header.name.len(), header.value.len()))
}

/// The field count and uncompressed size of an input block, accumulated while it is visited.
#[derive(Clone, Copy, Default)]
struct InputStats {
    fields: usize,
    uncompressed: u32,
}

impl InputStats {
    fn add(&mut self, header: &Header<'_>) {
        self.fields = self.fields.saturating_add(1);
        self.uncompressed = self
            .uncompressed
            .saturating_add(HpackHeader::real_bytes_for(header.name.len(), header.value.len()) + 2);
    }

    const fn memo_eligible(self) -> bool {
        self.fields <= MEMO_MAX_FIELDS && self.uncompressed <= MEMO_MAX_UNCOMPRESSED
    }
}

pub trait HeaderCodecStats: 'static {
    fn record_encode(&self, size: HttpHeaderSize);
    fn record_decode(&self, size: HttpHeaderSize);
    fn record_decode_error(&self);
    fn record_decode_too_large(&self);
}

pub trait StreamingCallback {
    fn on_header(&mut self, name: &HeaderName, value: &[u8]);
    fn on_headers_complete(&mut self, decoded_size: HttpHeaderSize, acknowledge: bool);
    fn on_decode_error(&mut self, error: HpackError);
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DecodedHeaderBlock {
    pub headers: Vec<HpackHeader>,
    pub bytes_consumed: usize,
    pub size: HttpHeaderSize,
}

pub struct HpackEncoder {
    context: HpackContext,
    huffman: bool,
    huffman_limits: (usize, usize),
    indexing_strategy: Option<Rc<dyn HeaderIndexingStrategy>>,
    pending_min_table_size: Option<u32>,
    pending_final_table_size: Option<u32>,
    encode_headroom: usize,
    #[cfg(test)]
    encode_inputs_calls: usize,
}

impl Clone for HpackEncoder {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
            huffman: self.huffman,
            huffman_limits: self.huffman_limits,
            indexing_strategy: self
                .indexing_strategy
                .as_ref()
                .map(|strategy| Rc::from(strategy.clone_box())),
            pending_min_table_size: self.pending_min_table_size,
            pending_final_table_size: self.pending_final_table_size,
            encode_headroom: self.encode_headroom,
            #[cfg(test)]
            encode_inputs_calls: self.encode_inputs_calls,
        }
    }
}

impl HpackEncoder {
    pub fn new(huffman: bool) -> Self {
        Self::with_table_size(huffman, DEFAULT_TABLE_SIZE)
    }

    pub fn with_table_size(huffman: bool, table_size: u32) -> Self {
        let huffman_limits = DefaultIndexingStrategy.huffman_limits();
        Self {
            context: HpackContext::new(table_size),
            huffman,
            huffman_limits,
            indexing_strategy: None,
            pending_min_table_size: None,
            pending_final_table_size: None,
            encode_headroom: 0,
            #[cfg(test)]
            encode_inputs_calls: 0,
        }
    }

    pub fn encode(&mut self, headers: &[HpackHeader]) -> Vec<u8> {
        self.encode_block(headers).into_bytes()
    }

    pub fn encode_block(&mut self, headers: &[HpackHeader]) -> EncodedHeaderBlock {
        self.encode_parts(
            headers.iter().map(HpackHeader::as_parts),
            self.encode_headroom,
        )
    }

    fn start_encode(
        &mut self,
        headroom: usize,
        initial_capacity: usize,
        storage: Vec<u8>,
    ) -> HpackEncodeBuffer {
        let mut output =
            HpackEncodeBuffer::with_storage(self.huffman, headroom, initial_capacity, storage);
        output.set_huffman_limits(self.huffman_limits.0, self.huffman_limits.1);
        if let Some(minimum) = self.pending_min_table_size.take() {
            let final_size = self
                .pending_final_table_size
                .take()
                .expect("a pending minimum table size requires a final size");
            output.encode_integer(u64::from(minimum), TABLE_SIZE_UPDATE);
            if final_size != minimum {
                output.encode_integer(u64::from(final_size), TABLE_SIZE_UPDATE);
            }
        }
        output
    }

    fn encode_parts<'a>(
        &mut self,
        headers: impl Iterator<Item = (&'a HeaderName, &'a [u8])>,
        headroom: usize,
    ) -> EncodedHeaderBlock {
        self.encode_parts_into(headers, headroom, Vec::new())
    }

    /// Encodes `headers` into `storage`, whose contents are discarded and whose capacity is reused.
    fn encode_parts_into<'a>(
        &mut self,
        headers: impl Iterator<Item = (&'a HeaderName, &'a [u8])>,
        headroom: usize,
        storage: Vec<u8>,
    ) -> EncodedHeaderBlock {
        let mut output = self.start_encode(headroom, 0, storage);
        for (name, value) in headers {
            self.encode_header(name, value, &mut output);
        }
        output.release()
    }

    #[cfg(test)]
    fn encode_inputs(
        &mut self,
        headers: &impl HeaderInputs,
        headroom: usize,
        stats: &mut InputStats,
    ) -> EncodedHeaderBlock {
        self.encode_inputs_into(headers, headroom, stats, Vec::new())
    }

    /// Encodes `headers` into `storage`, whose contents are discarded and whose capacity is reused.
    fn encode_inputs_into(
        &mut self,
        headers: &impl HeaderInputs,
        headroom: usize,
        stats: &mut InputStats,
        storage: Vec<u8>,
    ) -> EncodedHeaderBlock {
        #[cfg(test)]
        {
            self.encode_inputs_calls += 1;
        }
        // This path only receives nonempty message headers with bounded frame-prefix headroom.
        let mut output = self.start_encode(headroom, 32, storage);
        headers.for_each(|header| {
            stats.add(&header);
            if header.code.as_u8() >= HttpHeaderCode::COMMON_OFFSET {
                self.encode_header(&input_name(&header), header.value, &mut output);
                return;
            }
            let (index, name_index) = self
                .context
                .get_unknown_index_parts(header.name, header.value);
            if index != 0 {
                output.encode_integer(u64::from(index), INDEX_REF);
                return;
            }
            let name = input_name(&header);
            if name.code() == HttpHeaderCode::Other {
                self.encode_literal_header(Cow::Owned(name), header.value, name_index, &mut output);
            } else {
                self.encode_header(&name, header.value, &mut output);
            }
        });
        output.release()
    }

    pub fn encode_borrowed(&mut self, headers: &[Header<'_>]) -> (Vec<u8>, u32) {
        let prepared = prepare_encode_headers(headers);
        let uncompressed = uncompressed_size(prepared.iter().map(EncodeHeader::as_parts));
        if self.indexing_strategy.is_some() {
            let owned: Vec<_> = prepared.into_iter().map(EncodeHeader::into_owned).collect();
            return (self.encode(&owned), uncompressed);
        }
        (
            self.encode_parts(
                prepared.iter().map(EncodeHeader::as_parts),
                self.encode_headroom,
            )
            .into_bytes(),
            uncompressed,
        )
    }

    pub fn encode_to_buf_queue(
        &mut self,
        headers: &[HpackHeader],
        queue: &mut BufQueue,
    ) -> Result<usize, BufQueueError> {
        self.encode_parts_to_buf_queue(headers.iter().map(HpackHeader::as_parts), queue)
    }

    fn encode_inputs_to_buf_queue(
        &mut self,
        headers: Vec<EncodeHeader<'_>>,
        queue: &mut BufQueue,
    ) -> Result<usize, BufQueueError> {
        if self.indexing_strategy.is_some() {
            let owned: Vec<_> = headers.into_iter().map(EncodeHeader::into_owned).collect();
            return self.encode_to_buf_queue(&owned, queue);
        }
        self.encode_parts_to_buf_queue(headers.iter().map(EncodeHeader::as_parts), queue)
    }

    fn encode_parts_to_buf_queue<'a>(
        &mut self,
        headers: impl Iterator<Item = (&'a HeaderName, &'a [u8])> + Clone,
        queue: &mut BufQueue,
    ) -> Result<usize, BufQueueError> {
        if self.encode_headroom == 0
            && queue.is_empty()
            && self.can_encode_in_place(headers.clone(), queue.remaining_capacity(), 0)
        {
            let encoded = self
                .encode_parts(headers, self.encode_headroom)
                .into_bytes();
            let encoded_size = encoded.len();
            queue.push_vec(encoded)?;
            return Ok(encoded_size);
        }

        let mut updated = self.clone();
        let encoded = updated
            .encode_parts(headers, updated.encode_headroom)
            .into_bytes();
        let encoded_size = encoded.len();
        queue.push_vec(encoded)?;
        *self = updated;
        Ok(encoded_size)
    }

    /// Encodes `headers` into `storage`, accumulating their field count and uncompressed size into
    /// `stats`. `bound` is an upper bound on the encoded size, or `None` when it is unknown.
    fn encode_inputs_block(
        &mut self,
        headers: &impl HeaderInputs,
        bound: Option<usize>,
        maximum: usize,
        headroom: usize,
        stats: &mut InputStats,
        storage: Vec<u8>,
    ) -> Result<EncodedHeaderBlock, BufQueueError> {
        if self.indexing_strategy.is_some() {
            return self.encode_strategy_inputs_block(headers, maximum, headroom, stats, storage);
        }
        if self.context.table().capacity() != u32::MAX
            && self.bound_fits_in_place(bound, maximum, headroom)
        {
            return Ok(self.encode_inputs_into(headers, headroom, stats, storage));
        }
        self.encode_block_transaction(maximum, |encoder| {
            encoder.encode_inputs_into(headers, headroom, stats, storage)
        })
    }

    #[cold]
    #[inline(never)]
    fn encode_strategy_inputs_block(
        &mut self,
        headers: &impl HeaderInputs,
        maximum: usize,
        headroom: usize,
        stats: &mut InputStats,
        storage: Vec<u8>,
    ) -> Result<EncodedHeaderBlock, BufQueueError> {
        let mut owned = Vec::new();
        headers.for_each(|header| {
            stats.add(&header);
            owned.push(HpackHeader::from_name(input_name(&header), header.value));
        });
        self.encode_block_transaction(maximum, |encoder| {
            encoder.encode_parts_into(owned.iter().map(HpackHeader::as_parts), headroom, storage)
        })
    }

    fn encode_block_transaction(
        &mut self,
        maximum: usize,
        encode: impl FnOnce(&mut Self) -> EncodedHeaderBlock,
    ) -> Result<EncodedHeaderBlock, BufQueueError> {
        let mut updated = self.clone();
        let encoded = encode(&mut updated);
        let incoming = encoded.as_bytes().len();
        if incoming > maximum {
            return Err(BufQueueError::LimitExceeded {
                limit: maximum,
                current: 0,
                incoming,
            });
        }
        *self = updated;
        Ok(encoded)
    }

    fn can_encode_in_place<'a>(
        &self,
        mut headers: impl Iterator<Item = (&'a HeaderName, &'a [u8])>,
        maximum: usize,
        headroom: usize,
    ) -> bool {
        if self.indexing_strategy.is_some() || self.context.table().capacity() == u32::MAX {
            return false;
        }

        let bound = headers.try_fold(EMPTY_BLOCK_BOUND, |total, (name, value)| {
            add_encoded_header_bound(total, name.len(), value.len())
        });
        self.bound_fits_in_place(bound, maximum, headroom)
    }

    fn bound_fits_in_place(&self, bound: Option<usize>, maximum: usize, headroom: usize) -> bool {
        let Some(bound) = bound else {
            return false;
        };

        // The byte bound also limits Huffman's intermediate bit sum and
        // Vec growth. Table storage needs a margin for VecDeque doubling.
        bound <= maximum
            && bound
                .checked_add(headroom)
                .is_some_and(|storage| storage <= usize::MAX / 8)
            && (self.context.table().max_table_length() as usize)
                <= isize::MAX as usize / (2 * size_of::<HpackHeader>())
    }

    pub fn set_header_table_size(&mut self, size: u32) {
        if self.context.table().capacity() != size {
            assert!(self.context.table_mut().set_capacity(size));
            self.pending_min_table_size = Some(
                self.pending_min_table_size
                    .map_or(size, |minimum| minimum.min(size)),
            );
            self.pending_final_table_size = Some(size);
        }
    }

    pub fn set_header_indexing_strategy(&mut self, strategy: Rc<dyn HeaderIndexingStrategy>) {
        self.huffman_limits = strategy.huffman_limits();
        self.indexing_strategy = Some(strategy);
    }

    pub fn indexing_strategy(&self) -> &dyn HeaderIndexingStrategy {
        self.indexing_strategy
            .as_deref()
            .unwrap_or(&DefaultIndexingStrategy)
    }

    pub fn set_encode_headroom(&mut self, headroom: usize) {
        self.encode_headroom = headroom;
    }

    pub const fn encode_headroom(&self) -> usize {
        self.encode_headroom
    }

    pub fn context(&self) -> &HpackContext {
        &self.context
    }

    fn encode_header(&mut self, name: &HeaderName, value: &[u8], output: &mut HpackEncodeBuffer) {
        let (index, name_index) = self.context.get_index_parts(name, value, true);
        if index != 0 {
            output.encode_integer(u64::from(index), INDEX_REF);
            return;
        }

        self.encode_literal_header(Cow::Borrowed(name), value, name_index, output);
    }

    /// Encodes a literal field. An owned `name` moves into the table when the field is indexed.
    fn encode_literal_header(
        &mut self,
        name: Cow<'_, HeaderName>,
        value: &[u8],
        name_index: u32,
        output: &mut HpackEncodeBuffer,
    ) {
        let indexable = HpackHeader::bytes_for(name.len(), value.len())
            <= self.context.table().capacity()
            && self.indexing_strategy().index_header(
                &name,
                value,
                name_index != 0 && !HpackContext::is_static(name_index),
            );
        let instruction = if indexable {
            LITERAL_INC_INDEX
        } else {
            LITERAL
        };
        output.encode_integer(u64::from(name_index), instruction);
        if name_index == 0 {
            output.encode_literal(name.as_bytes());
        }
        output.encode_literal(value);
        if indexable {
            assert!(
                self.context
                    .table_mut()
                    .add_missed(HpackHeader::from_name(name.into_owned(), value))
            );
        }
    }
}

impl Default for HpackEncoder {
    fn default() -> Self {
        Self::new(true)
    }
}

pub struct HpackDecoder {
    context: HpackContext,
    maximum_table_size: u32,
    maximum_uncompressed: u64,
    error: Option<HpackError>,
    memo_reusable: bool,
    memo_fields: usize,
    scratch: [u8; LITERAL_SCRATCH],
    #[cfg(test)]
    decode_calls: usize,
    #[cfg(test)]
    field_start_reads: usize,
}

impl HpackDecoder {
    pub fn new() -> Self {
        Self::with_limits(DEFAULT_TABLE_SIZE, DEFAULT_MAX_UNCOMPRESSED)
    }

    pub fn with_limits(table_size: u32, maximum_uncompressed: u64) -> Self {
        let mut context = HpackContext::new(table_size);
        context.table_mut().disable_names_index();
        Self {
            context,
            maximum_table_size: table_size,
            maximum_uncompressed,
            error: None,
            memo_reusable: false,
            memo_fields: 0,
            scratch: [0; LITERAL_SCRATCH],
            #[cfg(test)]
            decode_calls: 0,
            #[cfg(test)]
            field_start_reads: 0,
        }
    }

    /// Returns and resets how many field-representation first bytes the decode loop has read.
    #[cfg(test)]
    pub(crate) fn take_field_start_reads(&mut self) -> usize {
        std::mem::take(&mut self.field_start_reads)
    }

    pub fn decode(&mut self, bytes: &[u8]) -> Result<DecodedHeaderBlock, HpackError> {
        let maximum_literal = usize::try_from(self.maximum_uncompressed).unwrap_or(usize::MAX);
        let mut input = HpackDecodeBuffer::new(bytes, maximum_literal);
        self.decode_buffer(&mut input)
    }

    pub fn decode_buf_queue(
        &mut self,
        queue: &BufQueue,
        total_bytes: usize,
    ) -> Result<DecodedHeaderBlock, HpackError> {
        let maximum_literal = usize::try_from(self.maximum_uncompressed).unwrap_or(usize::MAX);
        let mut input = HpackDecodeBuffer::from_buf_queue(queue, total_bytes, maximum_literal);
        self.decode_buffer(&mut input)
    }

    pub fn decode_segments<'a>(
        &mut self,
        segments: impl IntoIterator<Item = &'a [u8]>,
        total_bytes: usize,
    ) -> Result<DecodedHeaderBlock, HpackError> {
        let maximum_literal = usize::try_from(self.maximum_uncompressed).unwrap_or(usize::MAX);
        let mut input = HpackDecodeBuffer::from_segments(segments, total_bytes, maximum_literal);
        self.decode_buffer(&mut input)
    }

    fn decode_buffer(
        &mut self,
        input: &mut HpackDecodeBuffer<'_>,
    ) -> Result<DecodedHeaderBlock, HpackError> {
        let mut callback = CollectingCallback::default();
        let result = self.decode_streaming_buffer(input, &mut callback);
        match result {
            Ok(size) => Ok(DecodedHeaderBlock {
                headers: callback.headers,
                bytes_consumed: input.consumed_bytes(),
                size,
            }),
            Err(error) => Err(error),
        }
    }

    pub fn decode_streaming(
        &mut self,
        bytes: &[u8],
        callback: &mut dyn StreamingCallback,
    ) -> Result<HttpHeaderSize, HpackError> {
        if let Some(error) = self.error {
            callback.on_decode_error(error);
            return Err(error);
        }
        let maximum_literal = usize::try_from(self.maximum_uncompressed).unwrap_or(usize::MAX);
        let mut input = HpackDecodeBuffer::new(bytes, maximum_literal);
        self.decode_streaming_buffer(&mut input, callback)
    }

    pub fn decode_streaming_buf_queue(
        &mut self,
        queue: &BufQueue,
        total_bytes: usize,
        callback: &mut dyn StreamingCallback,
    ) -> Result<HttpHeaderSize, HpackError> {
        let maximum_literal = usize::try_from(self.maximum_uncompressed).unwrap_or(usize::MAX);
        let mut input = HpackDecodeBuffer::from_buf_queue(queue, total_bytes, maximum_literal);
        self.decode_streaming_buffer(&mut input, callback)
    }

    fn decode_streaming_buffer(
        &mut self,
        input: &mut HpackDecodeBuffer<'_>,
        callback: &mut dyn StreamingCallback,
    ) -> Result<HttpHeaderSize, HpackError> {
        if let Some(error) = self.error {
            callback.on_decode_error(error);
            return Err(error);
        }
        #[cfg(test)]
        {
            self.decode_calls += 1;
        }
        self.memo_reusable = true;
        self.memo_fields = 0;
        let mut emitted = 0_u64;
        let mut table_size_update_allowed = true;
        while !input.is_empty() {
            #[cfg(test)]
            {
                self.field_start_reads += 1;
            }
            let byte = match input.peek() {
                Ok(byte) => byte,
                Err(error) => {
                    self.error = Some(error);
                    callback.on_decode_error(error);
                    return Err(error);
                }
            };
            let table_size_update = byte & INDEX_REF.code == 0
                && byte & LITERAL_INC_INDEX.code == 0
                && byte & TABLE_SIZE_UPDATE.code != 0;
            if table_size_update && !table_size_update_allowed {
                let error = HpackError::InvalidTableSizeUpdate;
                self.error = Some(error);
                callback.on_decode_error(error);
                return Err(error);
            }
            self.memo_reusable &= byte & 0xf0 != super::LITERAL_NEV_INDEX.code;
            if !table_size_update {
                self.memo_fields = self.memo_fields.saturating_add(1);
            }
            let header_size = match self.decode_header(input, byte, callback) {
                Ok(size) => size,
                Err(error) => {
                    self.error = Some(error);
                    callback.on_decode_error(error);
                    return Err(error);
                }
            };
            emitted = emitted.saturating_add(u64::from(header_size));
            if emitted > self.maximum_uncompressed {
                self.error = Some(HpackError::HeadersTooLarge);
                callback.on_decode_error(HpackError::HeadersTooLarge);
                return Err(HpackError::HeadersTooLarge);
            }
            emitted = emitted.saturating_add(2);
            if !table_size_update {
                table_size_update_allowed = false;
            }
        }
        let compressed = u32::try_from(input.consumed_bytes()).unwrap_or(u32::MAX);
        let size = HttpHeaderSize {
            compressed,
            compressed_block: compressed,
            uncompressed: u32::try_from(emitted).unwrap_or(u32::MAX),
        };
        callback.on_headers_complete(size, false);
        Ok(size)
    }

    pub fn set_header_table_max_size(&mut self, size: u32) {
        self.maximum_table_size = size;
        if self.context.table().capacity() > size {
            assert!(self.context.table_mut().set_capacity(size));
        }
    }

    pub fn set_max_uncompressed(&mut self, maximum: u64) {
        self.maximum_uncompressed = maximum;
    }

    pub const fn max_uncompressed(&self) -> u64 {
        self.maximum_uncompressed
    }

    pub const fn error(&self) -> Option<HpackError> {
        self.error
    }

    pub fn context(&self) -> &HpackContext {
        &self.context
    }

    /// Decodes one field representation, whose first byte the caller has peeked as `byte`.
    fn decode_header(
        &mut self,
        input: &mut HpackDecodeBuffer<'_>,
        byte: u8,
        callback: &mut dyn StreamingCallback,
    ) -> Result<u32, HpackError> {
        if byte & INDEX_REF.code != 0 {
            let index = u32::try_from(input.decode_integer(INDEX_REF.prefix_length)?)
                .map_err(|_| HpackError::InvalidIndex)?;
            if index == 0 {
                return Err(HpackError::InvalidIndex);
            }
            let header = self
                .context
                .get_header(index)
                .ok_or(HpackError::InvalidIndex)?;
            let size = header.real_bytes();
            callback.on_header(&header.name, &header.value);
            return Ok(size);
        }

        let indexing = byte & LITERAL_INC_INDEX.code != 0;
        if !indexing && byte & TABLE_SIZE_UPDATE.code != 0 {
            let size = u32::try_from(input.decode_integer(TABLE_SIZE_UPDATE.prefix_length)?)
                .map_err(|_| HpackError::InvalidTableSize)?;
            if size > self.maximum_table_size {
                return Err(HpackError::InvalidTableSize);
            }
            self.context.table_mut().set_capacity(size);
            return Ok(0);
        }

        let prefix = if indexing {
            LITERAL_INC_INDEX.prefix_length
        } else {
            LITERAL.prefix_length
        };
        let mask = super::NBIT_MASKS[usize::from(prefix)];
        let name = if byte & mask != 0 {
            let index = u32::try_from(input.decode_integer(prefix)?)
                .map_err(|_| HpackError::InvalidIndex)?;
            self.context
                .get_header(index)
                .map(|header| header.name.clone())
                .ok_or(HpackError::InvalidIndex)?
        } else {
            input.read_byte()?;
            let literal = input.decode_literal_with_scratch(&mut self.scratch)?;
            #[cfg(test)]
            if matches!(literal, Cow::Owned(_)) {
                HeaderName::record_name_buffer();
            }
            HeaderName::new(literal)
        };
        let literal = input.decode_literal_with_scratch(&mut self.scratch)?;
        if !indexing {
            let size = HpackHeader::real_bytes_for(name.len(), literal.len());
            callback.on_header(&name, &literal);
            return Ok(size);
        }
        let value: Box<[u8]> = match literal {
            Cow::Borrowed(value) => Box::from(value),
            Cow::Owned(value) => value.into_boxed_slice(),
        };
        let header = HpackHeader { name, value };
        let size = header.real_bytes();
        callback.on_header(&header.name, &header.value);
        self.context.table_mut().add(header);
        Ok(size)
    }
}

impl Default for HpackDecoder {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) const MEMO_MAX_COMPRESSED: usize = 2048;
pub(crate) const MEMO_MAX_UNCOMPRESSED: u32 = 4096;
pub(crate) const MEMO_MAX_FIELDS: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DecodeMemoToken {
    access: u64,
    table: u64,
}

#[derive(Clone, Copy)]
pub(crate) struct DecodeMemoState {
    pub token: DecodeMemoToken,
    static_refs: u32,
}

struct EncodedMemo {
    bytes: Vec<u8>,
    static_refs: u32,
}

/// The cap on the encoder memo's back-off exponent: after `m` consecutive misses, a miss idles the
/// memo for `2^min(m, MEMO_BACKOFF_CAP) - 1` eligible blocks.
const MEMO_BACKOFF_CAP: u8 = 5;

struct EncoderMemo {
    epoch: u64,
    key: Vec<u8>,
    ready: Option<EncodedMemo>,
    /// Consecutive misses since the key last matched.
    misses: u8,
    /// Eligible blocks still to encode without a match pass after a miss; the block that brings it
    /// to 0 refills `key`.
    idle: u8,
}

pub struct HpackCodec {
    encoder: HpackEncoder,
    decoder: HpackDecoder,
    encoded_size: HttpHeaderSize,
    stats: Option<Rc<dyn HeaderCodecStats>>,
    encoder_memo: Option<EncoderMemo>,
    decoder_access_epoch: Option<u64>,
    /// Storage of the last framed header block, reused by the next one.
    block_storage: Vec<u8>,
    #[cfg(test)]
    encode_fields: usize,
    #[cfg(test)]
    memo_key_builds: usize,
    #[cfg(test)]
    memo_match_passes: usize,
}

impl HpackCodec {
    pub fn new() -> Self {
        Self {
            encoder: HpackEncoder::new(true),
            decoder: HpackDecoder::new(),
            encoded_size: HttpHeaderSize::default(),
            stats: None,
            encoder_memo: None,
            decoder_access_epoch: Some(0),
            block_storage: Vec::new(),
            #[cfg(test)]
            encode_fields: 0,
            #[cfg(test)]
            memo_key_builds: 0,
            #[cfg(test)]
            memo_match_passes: 0,
        }
    }

    pub fn encode(&mut self, headers: &[HpackHeader]) -> Vec<u8> {
        let uncompressed = uncompressed_size(headers.iter().map(HpackHeader::as_parts));
        self.encode_prepared(headers, uncompressed)
    }

    pub fn encode_borrowed(&mut self, headers: &[Header<'_>]) -> Vec<u8> {
        let (encoded, uncompressed) = self.encoder.encode_borrowed(headers);
        self.record_encode_size(encoded.len(), uncompressed);
        encoded
    }

    fn encode_prepared(&mut self, headers: &[HpackHeader], uncompressed: u32) -> Vec<u8> {
        let encoded = self.encoder.encode(headers);
        self.record_encode_size(encoded.len(), uncompressed);
        encoded
    }

    pub fn encode_to_buf_queue(
        &mut self,
        headers: &[HpackHeader],
        queue: &mut BufQueue,
    ) -> Result<usize, BufQueueError> {
        let uncompressed = uncompressed_size(headers.iter().map(HpackHeader::as_parts));
        let compressed = self.encoder.encode_to_buf_queue(headers, queue)?;
        self.record_encode_size(compressed, uncompressed);
        Ok(compressed)
    }

    pub fn encode_borrowed_to_buf_queue(
        &mut self,
        headers: &[Header<'_>],
        queue: &mut BufQueue,
    ) -> Result<usize, BufQueueError> {
        self.encode_inputs_to_buf_queue(prepare_encode_headers(headers), queue)
    }

    pub(crate) fn encode_inputs_to_buf_queue(
        &mut self,
        headers: Vec<EncodeHeader<'_>>,
        queue: &mut BufQueue,
    ) -> Result<usize, BufQueueError> {
        let uncompressed = uncompressed_size(headers.iter().map(EncodeHeader::as_parts));
        let compressed = self.encoder.encode_inputs_to_buf_queue(headers, queue)?;
        self.record_encode_size(compressed, uncompressed);
        Ok(compressed)
    }

    /// Keeps `block`'s storage for the next header block. Callers recycle only single-frame blocks, so the kept
    /// storage was sized for at most one frame (the egress maximum frame size plus its frame prefix).
    pub(crate) fn recycle_block(&mut self, block: EncodedHeaderBlock) {
        self.block_storage = block.into_storage();
    }

    pub(crate) fn encode_inputs_block(
        &mut self,
        headers: &impl HeaderInputs,
        maximum: usize,
        headroom: usize,
    ) -> Result<EncodedHeaderBlock, BufQueueError> {
        let epoch = self.encoder_memo_epoch();
        let memo = self
            .encoder_memo
            .as_ref()
            .filter(|memo| Some(memo.epoch) == epoch);
        let backing_off = memo.is_some_and(|memo| memo.idle != 0);
        // Only an armed memo for this epoch can hit, so only then does a matching pass precede the
        // encode. Otherwise (no memo, or one backing off after a miss) the encode pass alone counts
        // the fields.
        let (bound, matches) = if let Some(memo) = memo.filter(|memo| memo.idle == 0) {
            #[cfg(test)]
            {
                self.memo_match_passes += 1;
            }
            let mut key = memo.key.as_slice();
            let mut matches = true;
            let mut stats = InputStats::default();
            let mut bound = Some(EMPTY_BLOCK_BOUND);
            headers.for_each(|header| {
                stats.add(&header);
                bound = add_input_bound(bound, &header);
                if matches {
                    matches = match_field_key(&mut key, &header);
                }
            });
            matches &= key.is_empty();
            if stats.memo_eligible()
                && matches
                && let Some(ready) = memo.ready.as_ref()
                && headroom
                    .checked_add(ready.bytes.len())
                    .is_some_and(|size| size <= usize::MAX / 8)
            {
                let incoming = ready.bytes.len();
                if incoming > maximum {
                    return Err(BufQueueError::LimitExceeded {
                        limit: maximum,
                        current: 0,
                        incoming,
                    });
                }
                let encoded = EncodedHeaderBlock::copy_with_headroom_into(
                    std::mem::take(&mut self.block_storage),
                    &ready.bytes,
                    headroom,
                );
                self.encoder.context.replay_static_refs(ready.static_refs);
                #[cfg(test)]
                {
                    self.encode_fields = stats.fields;
                }
                self.record_encode_size(incoming, stats.uncompressed);
                return Ok(encoded);
            }
            (bound, matches)
        } else {
            (headers.length_bound(), false)
        };

        let before_refs = self.encoder.context.static_refs();
        let mut stats = InputStats::default();
        let encoded = self.encoder.encode_inputs_block(
            headers,
            bound,
            maximum,
            headroom,
            &mut stats,
            std::mem::take(&mut self.block_storage),
        )?;
        let after_refs = self.encoder.context.static_refs();
        if let Some(epoch) = epoch
            && stats.memo_eligible()
            && Some(epoch) == self.encoder_memo_epoch()
            && encoded.as_bytes().len() <= MEMO_MAX_COMPRESSED
            && after_refs != u32::MAX
        {
            match self
                .encoder_memo
                .as_mut()
                .filter(|memo| memo.epoch == epoch)
            {
                Some(memo) if matches => {
                    memo.misses = 0;
                    memo.ready = Some(EncodedMemo {
                        bytes: encoded.as_bytes().to_vec(),
                        static_refs: after_refs - before_refs,
                    });
                }
                Some(memo) => {
                    if backing_off {
                        memo.idle -= 1;
                    } else {
                        // A miss after `misses` consecutive misses idles the memo for
                        // `2^min(misses, cap) - 1` blocks; an isolated miss idles none.
                        memo.ready = None;
                        memo.idle = (1 << memo.misses.min(MEMO_BACKOFF_CAP)) - 1;
                        memo.misses = memo.misses.saturating_add(1);
                    }
                    if memo.idle == 0 {
                        #[cfg(test)]
                        {
                            self.memo_key_builds += 1;
                        }
                        fill_field_key(&mut memo.key, headers, stats.uncompressed, stats.fields);
                    }
                }
                None => {
                    #[cfg(test)]
                    {
                        self.memo_key_builds += 1;
                    }
                    self.encoder_memo = Some(EncoderMemo {
                        epoch,
                        key: encode_field_key(headers, stats.uncompressed, stats.fields),
                        ready: None,
                        misses: 0,
                        idle: 0,
                    });
                }
            }
        } else {
            self.encoder_memo = None;
        }
        #[cfg(test)]
        {
            self.encode_fields = stats.fields;
        }
        self.record_encode_size(encoded.as_bytes().len(), stats.uncompressed);
        Ok(encoded)
    }

    fn encoder_memo_epoch(&self) -> Option<u64> {
        if self.stats.is_some()
            || self.encoder.indexing_strategy.is_some()
            || self.encoder.pending_min_table_size.is_some()
            || self.encoder.pending_final_table_size.is_some()
        {
            return None;
        }
        self.encoder.context.mutation_epoch()
    }

    /// The decoder memo's fingerprint hash of a header block, under the decoder table's seeded hash.
    pub(crate) fn decode_block_hash(&self, block: &[u8]) -> u64 {
        self.decoder.context.table().hash_bytes(block)
    }

    pub(crate) fn decode_memo_state(&self) -> Option<DecodeMemoState> {
        if self.stats.is_some() || self.decoder.error.is_some() {
            return None;
        }
        Some(DecodeMemoState {
            token: DecodeMemoToken {
                access: self.decoder_access_epoch?,
                table: self.decoder.context.mutation_epoch()?,
            },
            static_refs: self.decoder.context.static_refs(),
        })
    }

    pub(crate) fn decode_memo_delta(&self, before: DecodeMemoState) -> Option<u32> {
        let after = self.decode_memo_state()?;
        (before.token == after.token
            && self.decoder.memo_reusable
            && self.decoder.memo_fields <= MEMO_MAX_FIELDS
            && after.static_refs != u32::MAX)
            .then(|| after.static_refs - before.static_refs)
    }

    pub(crate) fn replay_decode_memo(&mut self, token: DecodeMemoToken, static_refs: u32) -> bool {
        if self
            .decode_memo_state()
            .is_none_or(|state| state.token != token)
        {
            return false;
        }
        self.decoder.context.replay_static_refs(static_refs);
        true
    }

    fn invalidate_decoder_memo(&mut self) {
        self.decoder_access_epoch = self
            .decoder_access_epoch
            .and_then(|epoch| epoch.checked_add(1));
    }

    #[cfg(test)]
    pub(crate) fn header_loop_counts(&self) -> (usize, usize) {
        (self.encoder.encode_inputs_calls, self.decoder.decode_calls)
    }

    /// The field count that the last `encode_inputs_block` judged memo eligibility on.
    #[cfg(test)]
    pub(crate) const fn last_encode_fields(&self) -> usize {
        self.encode_fields
    }

    pub fn encode_block(&mut self, headers: &[HpackHeader]) -> EncodedHeaderBlock {
        let uncompressed = uncompressed_size(headers.iter().map(HpackHeader::as_parts));
        let encoded = self.encoder.encode_block(headers);
        self.record_encode_size(encoded.as_bytes().len(), uncompressed);
        encoded
    }

    fn record_encode_size(&mut self, compressed: usize, uncompressed: u32) {
        let compressed = u32::try_from(compressed).unwrap_or(u32::MAX);
        self.encoded_size = HttpHeaderSize {
            compressed,
            compressed_block: self
                .encoded_size
                .compressed_block
                .saturating_add(compressed),
            uncompressed,
        };
        if let Some(stats) = &self.stats {
            stats.record_encode(self.encoded_size);
        }
    }

    pub fn decode(&mut self, bytes: &[u8]) -> Result<DecodedHeaderBlock, HpackError> {
        let result = self.decoder.decode(bytes);
        self.record_decode_result(&result);
        result
    }

    pub fn decode_buf_queue(
        &mut self,
        queue: &BufQueue,
        total_bytes: usize,
    ) -> Result<DecodedHeaderBlock, HpackError> {
        let result = self.decoder.decode_buf_queue(queue, total_bytes);
        self.record_decode_result(&result);
        result
    }

    fn record_decode_result(&self, result: &Result<DecodedHeaderBlock, HpackError>) {
        match result {
            Ok(decoded) => {
                if let Some(stats) = &self.stats {
                    stats.record_decode(decoded.size);
                }
            }
            Err(HpackError::HeadersTooLarge | HpackError::LiteralTooLarge) => {
                if let Some(stats) = &self.stats {
                    stats.record_decode_too_large();
                }
            }
            Err(_) => {
                if let Some(stats) = &self.stats {
                    stats.record_decode_error();
                }
            }
        }
    }

    pub fn decode_streaming(
        &mut self,
        bytes: &[u8],
        callback: &mut dyn StreamingCallback,
    ) -> Result<HttpHeaderSize, HpackError> {
        if self.stats.is_none() {
            return self.decoder.decode_streaming(bytes, callback);
        }
        let mut callback = StatsStreamingCallback {
            stats: self.stats.as_deref(),
            callback,
        };
        self.decoder.decode_streaming(bytes, &mut callback)
    }

    pub fn decode_streaming_buf_queue(
        &mut self,
        queue: &BufQueue,
        total_bytes: usize,
        callback: &mut dyn StreamingCallback,
    ) -> Result<HttpHeaderSize, HpackError> {
        if self.stats.is_none() {
            return self
                .decoder
                .decode_streaming_buf_queue(queue, total_bytes, callback);
        }
        let mut callback = StatsStreamingCallback {
            stats: self.stats.as_deref(),
            callback,
        };
        self.decoder
            .decode_streaming_buf_queue(queue, total_bytes, &mut callback)
    }

    pub fn set_encoder_header_table_size(&mut self, size: u32) {
        self.encoder.set_header_table_size(size);
    }

    pub fn set_decoder_header_table_max_size(&mut self, size: u32) {
        self.invalidate_decoder_memo();
        self.decoder.set_header_table_max_size(size);
    }

    pub fn set_max_uncompressed(&mut self, maximum: u64) {
        self.invalidate_decoder_memo();
        self.decoder.set_max_uncompressed(maximum);
    }

    pub const fn max_uncompressed(&self) -> u64 {
        self.decoder.max_uncompressed()
    }

    pub fn set_encode_headroom(&mut self, headroom: usize) {
        self.encoder.set_encode_headroom(headroom);
    }

    pub fn set_header_indexing_strategy(&mut self, strategy: Rc<dyn HeaderIndexingStrategy>) {
        self.encoder.set_header_indexing_strategy(strategy);
    }

    pub fn set_stats(&mut self, stats: Option<Rc<dyn HeaderCodecStats>>) {
        self.encoder_memo = None;
        self.invalidate_decoder_memo();
        self.stats = stats;
    }

    pub const fn encoded_size(&self) -> HttpHeaderSize {
        self.encoded_size
    }

    pub fn compression_info(&self) -> CompressionInfo {
        CompressionInfo {
            egress: compression_part(&self.encoder.context),
            ingress: compression_part(&self.decoder.context),
        }
    }

    pub fn encoder(&self) -> &HpackEncoder {
        &self.encoder
    }

    pub fn encoder_mut(&mut self) -> &mut HpackEncoder {
        self.encoder_memo = None;
        &mut self.encoder
    }

    pub fn decoder(&self) -> &HpackDecoder {
        &self.decoder
    }

    pub fn decoder_mut(&mut self) -> &mut HpackDecoder {
        self.invalidate_decoder_memo();
        &mut self.decoder
    }
}

impl Default for HpackCodec {
    fn default() -> Self {
        Self::new()
    }
}

fn compression_part(context: &HpackContext) -> CompressionInfoPart {
    CompressionInfoPart {
        header_table_size: context.table().capacity(),
        bytes_stored: context.table().bytes(),
        headers_stored: context.table().size(),
        inserts: context.table().insert_count(),
        blocked_inserts: 0,
        duplications: 0,
        static_refs: context.static_refs(),
    }
}

struct StatsStreamingCallback<'a> {
    stats: Option<&'a dyn HeaderCodecStats>,
    callback: &'a mut dyn StreamingCallback,
}

impl StreamingCallback for StatsStreamingCallback<'_> {
    fn on_header(&mut self, name: &HeaderName, value: &[u8]) {
        self.callback.on_header(name, value);
    }

    fn on_headers_complete(&mut self, decoded_size: HttpHeaderSize, acknowledge: bool) {
        if let Some(stats) = self.stats {
            stats.record_decode(decoded_size);
        }
        self.callback.on_headers_complete(decoded_size, acknowledge);
    }

    fn on_decode_error(&mut self, error: HpackError) {
        if let Some(stats) = self.stats {
            if matches!(
                error,
                HpackError::HeadersTooLarge | HpackError::LiteralTooLarge
            ) {
                stats.record_decode_too_large();
            } else {
                stats.record_decode_error();
            }
        }
        self.callback.on_decode_error(error);
    }
}

#[derive(Default)]
struct CollectingCallback {
    headers: Vec<HpackHeader>,
}

impl StreamingCallback for CollectingCallback {
    fn on_header(&mut self, name: &HeaderName, value: &[u8]) {
        self.headers
            .push(HpackHeader::from_name(name.clone(), value));
    }

    fn on_headers_complete(&mut self, _decoded_size: HttpHeaderSize, _acknowledge: bool) {}

    fn on_decode_error(&mut self, _error: HpackError) {}
}

fn match_field_key(key: &mut &[u8], header: &Header<'_>) -> bool {
    let Some(prefix) = key.get(..5) else {
        return false;
    };
    let name_length = usize::from(u16::from_le_bytes([prefix[1], prefix[2]]));
    let value_length = usize::from(u16::from_le_bytes([prefix[3], prefix[4]]));
    let length = 5 + name_length + value_length;
    if prefix[0] != header.code.as_u8()
        || name_length != header.name.len()
        || value_length != header.value.len()
        || key.get(5..5 + name_length) != Some(header.name)
        || key.get(5 + name_length..length) != Some(header.value)
    {
        return false;
    }
    *key = &key[length..];
    true
}

fn encode_field_key(headers: &impl HeaderInputs, uncompressed: u32, fields: usize) -> Vec<u8> {
    let mut key = Vec::new();
    fill_field_key(&mut key, headers, uncompressed, fields);
    key
}

/// Replaces `key`'s contents with the memo key of `headers`, reusing its capacity.
fn fill_field_key(
    key: &mut Vec<u8>,
    headers: &impl HeaderInputs,
    uncompressed: u32,
    fields: usize,
) {
    key.clear();
    key.reserve(uncompressed as usize + 3 * fields);
    headers.for_each(|header| {
        key.push(header.code.as_u8());
        key.extend_from_slice(
            &u16::try_from(header.name.len())
                .expect("memo name is bounded")
                .to_le_bytes(),
        );
        key.extend_from_slice(
            &u16::try_from(header.value.len())
                .expect("memo value is bounded")
                .to_le_bytes(),
        );
        key.extend_from_slice(header.name);
        key.extend_from_slice(header.value);
    });
}

#[cfg(test)]
mod memo_tests {
    use super::*;

    struct Inputs(Vec<HpackHeader>);

    impl HeaderInputs for Inputs {
        fn for_each<'a>(&'a self, mut visitor: impl FnMut(Header<'a>)) {
            for header in &self.0 {
                visitor(Header {
                    code: header.name.code(),
                    name: header.name.as_bytes(),
                    value: &header.value,
                });
            }
        }
    }

    fn fields() -> Inputs {
        Inputs(vec![
            HpackHeader::new(":status", "200"),
            HpackHeader::new("content-length", "12345"),
        ])
    }

    fn warm(codec: &mut HpackCodec, fields: &Inputs) {
        for _ in 0..2 {
            codec.encode_inputs_block(fields, 4096, 9).unwrap();
        }
        assert!(codec.encoder_memo.as_ref().unwrap().ready.is_some());
    }

    #[test]
    fn memo_still_arms_and_hits() {
        let fields = fields();
        let mut codec = HpackCodec::new();
        let expected = codec
            .encoder
            .clone()
            .encode_inputs(&fields, 9, &mut InputStats::default());
        let mut blocks = Vec::new();
        blocks.push(codec.encode_inputs_block(&fields, 4096, 9).unwrap());
        assert!(
            codec
                .encoder_memo
                .as_ref()
                .is_some_and(|memo| memo.ready.is_none()),
            "the first block arms a memo key"
        );
        blocks.push(codec.encode_inputs_block(&fields, 4096, 9).unwrap());
        assert!(
            codec
                .encoder_memo
                .as_ref()
                .is_some_and(|memo| memo.ready.is_some()),
            "the second identical block records the memo's bytes"
        );
        let loops = codec.header_loop_counts();
        blocks.push(codec.encode_inputs_block(&fields, 4096, 9).unwrap());
        assert_eq!(
            codec.header_loop_counts(),
            loops,
            "the third identical block replays the memo without encoding"
        );
        for block in &blocks {
            assert_eq!(block.as_bytes(), expected.as_bytes());
        }
    }

    #[test]
    fn ready_encoder_memo_bypasses_loop_and_preserves_capacity_transaction() {
        let fields = fields();
        let mut codec = HpackCodec::new();
        warm(&mut codec, &fields);
        let expected = codec
            .encoder
            .clone()
            .encode_inputs(&fields, 9, &mut InputStats::default());
        let before_info = codec.compression_info().egress;
        let before_size = codec.encoded_size();
        let before_loops = codec.header_loop_counts();
        assert_eq!(
            codec.encode_inputs_block(&fields, expected.as_bytes().len() - 1, 9),
            Err(BufQueueError::LimitExceeded {
                limit: expected.as_bytes().len() - 1,
                current: 0,
                incoming: expected.as_bytes().len(),
            })
        );
        assert_eq!(codec.compression_info().egress, before_info);
        assert_eq!(codec.encoded_size(), before_size);
        assert_eq!(codec.header_loop_counts(), before_loops);
        let actual = codec
            .encode_inputs_block(&fields, expected.as_bytes().len(), 13)
            .unwrap();
        assert_eq!(actual.as_bytes(), expected.as_bytes());
        assert_eq!(actual.headroom(), 13);
        assert_eq!(codec.header_loop_counts(), before_loops);
        assert_eq!(
            codec.compression_info().egress.static_refs,
            before_info.static_refs + 2
        );
        assert_eq!(
            codec.encoded_size().compressed_block,
            before_size.compressed_block + expected.as_bytes().len() as u32
        );
    }

    #[test]
    fn mutable_encoder_replacement_discards_ready_and_candidate_memos() {
        for ready in [false, true] {
            let fields = fields();
            let mut codec = HpackCodec::new();
            codec.encode_inputs_block(&fields, 4096, 9).unwrap();
            if ready {
                codec.encode_inputs_block(&fields, 4096, 9).unwrap();
            }
            assert!(codec.encoder_memo.is_some());
            *codec.encoder_mut() = HpackEncoder::new(false);
            assert!(codec.encoder_memo.is_none());
            let expected =
                codec
                    .encoder
                    .clone()
                    .encode_inputs(&fields, 9, &mut InputStats::default());
            let actual = codec.encode_inputs_block(&fields, 4096, 9).unwrap();
            assert_eq!(actual, expected);
            assert_eq!(codec.header_loop_counts().0, 1);
        }
    }

    #[test]
    fn pending_table_updates_cannot_be_replayed_or_learned() {
        let fields = fields();
        let mut codec = HpackCodec::new();
        warm(&mut codec, &fields);
        codec.set_encoder_header_table_size(0);
        codec.set_encoder_header_table_size(DEFAULT_TABLE_SIZE);
        let expected = codec
            .encoder
            .clone()
            .encode_inputs(&fields, 9, &mut InputStats::default());
        let before = codec.header_loop_counts().0;
        let actual = codec.encode_inputs_block(&fields, 4096, 9).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(codec.header_loop_counts().0, before + 1);
        assert_eq!(actual.as_bytes()[0], TABLE_SIZE_UPDATE.code);
        assert!(codec.encoder_memo.is_none());
    }

    #[test]
    fn decoder_tokens_reject_replacement_limit_changes_and_sticky_errors() {
        let mut codec = HpackCodec::new();
        let first = codec.decode_memo_state().unwrap();
        codec.decode(&[0x82]).unwrap();
        assert_eq!(codec.decode_memo_delta(first), Some(1));
        *codec.decoder_mut() = HpackDecoder::new();
        assert_ne!(codec.decode_memo_state().unwrap().token, first.token);
        assert!(!codec.replay_decode_memo(first.token, 1));
        let replacement = codec.decode_memo_state().unwrap();
        codec.set_max_uncompressed(1);
        assert_ne!(codec.decode_memo_state().unwrap().token, replacement.token);
        assert!(codec.decode(&[0x82]).is_err());
        assert!(codec.decode_memo_state().is_none());

        let mut codec = HpackCodec::new();
        codec.decoder_access_epoch = Some(u64::MAX);
        let before = codec.decode_memo_state().unwrap();
        *codec.decoder_mut() = HpackDecoder::new();
        assert!(codec.decode_memo_state().is_none());
        assert!(!codec.replay_decode_memo(before.token, 1));
        *codec.decoder_mut() = HpackDecoder::new();
        assert!(codec.decode_memo_state().is_none());
    }

    #[test]
    fn decoder_memo_counts_static_literal_names_and_rejects_mutations_and_never_index() {
        let mut codec = HpackCodec::new();
        let before = codec.decode_memo_state().unwrap();
        codec
            .decode(&[0x82, 0x0f, 0x0d, 3, b'1', b'2', b'3'])
            .unwrap();
        assert_eq!(codec.decode_memo_delta(before), Some(2));
        let stable = codec.decode_memo_state().unwrap();
        codec.decode(&[0x1f, 0x0d, 3, b'1', b'2', b'3']).unwrap();
        assert_eq!(codec.decode_memo_delta(stable), None);
        let before_update = codec.decode_memo_state().unwrap();
        codec.decode(&[0x3f, 0xe1, 0x1f, 0x82]).unwrap();
        assert_eq!(codec.decode_memo_delta(before_update), None);
        codec.set_decoder_header_table_max_size(32);
        let before_clear = codec.decode_memo_state().unwrap();
        codec.decode(&[0x40, 1, b'x', 1, b'v']).unwrap();
        assert!(codec.decoder.context.table().entries().next().is_none());
        assert_eq!(codec.decode_memo_delta(before_clear), None);
    }

    #[test]
    fn static_reference_saturation_never_learns_an_understated_delta() {
        let mut codec = HpackCodec::new();
        let before = codec.decode_memo_state().unwrap();
        codec.decode(&[0x82]).unwrap();
        assert_eq!(codec.decode_memo_delta(before), Some(1));
        codec.decoder.context.replay_static_refs(u32::MAX - 2);
        let almost = codec.decode_memo_state().unwrap();
        codec.decode(&[0x82, 0x82]).unwrap();
        assert_eq!(codec.decode_memo_delta(almost), None);
        assert!(codec.replay_decode_memo(before.token, 1));
        assert_eq!(codec.decoder.context.static_refs(), u32::MAX);

        let fields = fields();
        let mut codec = HpackCodec::new();
        warm(&mut codec, &fields);
        codec.encoder.context.replay_static_refs(u32::MAX - 5);
        let loops = codec.header_loop_counts();
        codec.encode_inputs_block(&fields, 4096, 9).unwrap();
        assert_eq!(codec.encoder.context.static_refs(), u32::MAX);
        assert_eq!(codec.header_loop_counts(), loops);
    }

    #[test]
    fn encoder_memo_bounds_account_for_pseudo_fields_and_reject_churn_without_ready_copy() {
        let mut codec = HpackCodec::new();
        for length in [3, 4, 5, 6] {
            let fields = Inputs(vec![HpackHeader::new("content-length", vec![b'1'; length])]);
            codec.encode_inputs_block(&fields, 4096, 9).unwrap();
            assert!(codec.encoder_memo.as_ref().unwrap().ready.is_none());
        }
        let fields = Inputs(vec![HpackHeader::new(":path", vec![b'a'; 5000])]);
        codec.encode_inputs_block(&fields, 16 * 1024, 9).unwrap();
        assert!(codec.encoder_memo.is_none());
        let fields = Inputs(vec![
            HpackHeader::new(":status", "200");
            MEMO_MAX_FIELDS + 1
        ]);
        codec.encode_inputs_block(&fields, 4096, 9).unwrap();
        assert!(codec.encoder_memo.is_none());
    }

    /// A consumer-shaped gRPC response of five fields whose `x-upstream-time` is `digit`.
    fn grpc_response(digit: usize) -> Inputs {
        Inputs(vec![
            HpackHeader::new(":status", "200"),
            HpackHeader::new("content-type", "application/grpc"),
            HpackHeader::new("date", "Sun, 06 Nov 1994 08:49:37 GMT"),
            HpackHeader::new("server", "proxy"),
            HpackHeader::new("x-upstream-time", digit.to_string()),
        ])
    }

    /// A codec whose table holds every `grpc_response`, so that none of them mutates it again.
    fn indexed_digits() -> HpackCodec {
        let mut codec = HpackCodec::new();
        for digit in 0..10 {
            codec
                .encode_inputs_block(&grpc_response(digit), 4096, 9)
                .unwrap();
        }
        codec
    }

    /// Encodes `fields`, checks the block against a fresh copy of the encoder, and returns whether
    /// the block ran the encode loop (false: the memo replayed it).
    fn encode_checked(codec: &mut HpackCodec, fields: &Inputs) -> bool {
        let expected = codec
            .encoder
            .clone()
            .encode_inputs(fields, 9, &mut InputStats::default());
        let loops = codec.header_loop_counts().0;
        let actual = codec.encode_inputs_block(fields, 4096, 9).unwrap();
        assert_eq!(actual.as_bytes(), expected.as_bytes());
        codec.header_loop_counts().0 != loops
    }

    #[test]
    fn churning_indexed_value_backs_off_memo() {
        // Every block differs from the one before and refers only to indexed fields, so the memo
        // stays armed and every match pass misses. Block 1 finds no memo (the last warm-up block
        // inserted) and builds the key. A miss after m consecutive misses idles 2^min(m, 5) - 1
        // blocks, so match passes run on blocks 2, 3, 5, 9, 17 and 33 (the next on 65), and the key
        // is built on block 1 and refilled on blocks 2, 4, 8, 16, 32 and 64, the block before each
        // later pass (an isolated miss idles 0 blocks and refills at once): at most 7 key builds
        // and 6 match passes. Without a back-off every block builds a key and blocks 2-64 run a
        // match pass: 64 and 63.
        let mut codec = indexed_digits();
        let epoch = codec.encoder.context.mutation_epoch();
        let before = (codec.memo_key_builds, codec.memo_match_passes);
        for block in 0..64 {
            assert!(
                encode_checked(&mut codec, &grpc_response(block % 10)),
                "block {block} differs from the one before, so it cannot hit"
            );
        }
        assert_eq!(
            codec.encoder.context.mutation_epoch(),
            epoch,
            "no block mutates the table"
        );
        let builds = codec.memo_key_builds - before.0;
        let passes = codec.memo_match_passes - before.1;
        assert!(
            builds <= 7 && passes <= 6,
            "64 churning blocks made {builds} key builds and {passes} match passes"
        );
    }

    #[test]
    fn memo_hits_resume_within_the_back_off_cap_after_churn() {
        // The longest back-off idles 2^5 - 1 blocks. An identical run that starts on a miss idles
        // 31 blocks, refills the key on its 32nd, records on its 33rd and replays on its 34th.
        const LIMIT: usize = (1 << 5) + 2;
        for burst in 0..=40 {
            let mut codec = indexed_digits();
            for block in 0..burst {
                encode_checked(&mut codec, &grpc_response(block % 9));
            }
            let run = grpc_response(9);
            let mut first_hit = None;
            for position in 1..=LIMIT + 8 {
                let encoded = encode_checked(&mut codec, &run);
                match first_hit {
                    None if !encoded => first_hit = Some(position),
                    Some(hit) => assert!(
                        !encoded,
                        "burst {burst}: block {position} encodes after the hit on block {hit}"
                    ),
                    None => {}
                }
            }
            assert!(
                first_hit.is_some_and(|hit| hit <= LIMIT),
                "burst {burst}: the identical run's first hit is {first_hit:?}, not within {LIMIT}"
            );
        }
    }

    #[test]
    fn memo_hits_on_the_third_block_after_an_isolated_miss() {
        // The first run builds the key, records and replays. Every later run follows a hit, so
        // its first block is one isolated miss that refills the key; it records on its second
        // block and replays from its third, as without a back-off.
        let mut codec = indexed_digits();
        for digit in [1, 2, 1, 3, 2] {
            let encoded: Vec<bool> = (0..4)
                .map(|_| encode_checked(&mut codec, &grpc_response(digit)))
                .collect();
            assert_eq!(encoded, [true, true, false, false], "run of {digit}");
        }
    }
}
