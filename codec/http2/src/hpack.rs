/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! RFC 7541 HPACK header compression.

mod buffer;
mod codec;
mod context;
mod header;
mod huffman;
mod indexing;
mod logging;
mod table;

pub use buffer::EncodedHeaderBlock;
pub use buffer::HpackDecodeBuffer;
pub use buffer::HpackEncodeBuffer;
pub use codec::CompressionInfo;
pub use codec::CompressionInfoPart;
pub(crate) use codec::DecodeMemoState;
pub(crate) use codec::DecodeMemoToken;
pub use codec::DecodedHeaderBlock;
pub(crate) use codec::EMPTY_BLOCK_BOUND;
pub use codec::HeaderCodecStats;
pub use codec::HpackCodec;
pub use codec::HpackDecoder;
pub use codec::HpackEncoder;
pub(crate) use codec::MEMO_MAX_COMPRESSED;
pub(crate) use codec::MEMO_MAX_UNCOMPRESSED;
pub use codec::StreamingCallback;
pub(crate) use codec::add_input_bound;
pub use codec::prepare_headers;
pub use context::HpackContext;
pub(crate) use header::EncodeHeader;
pub use header::Header;
pub use header::HeaderName;
pub use header::HeaderPiece;
pub use header::HpackHeader;
pub use huffman::EOS_HPACK;
pub use huffman::HUFFMAN_TABLE_SIZE;
pub use huffman::HuffNode;
pub use huffman::HuffTree;
pub use huffman::SuperHuffNode;
pub use huffman::code as huffman_code;
pub use huffman::decode as decode_huffman;
pub use huffman::encode as encode_huffman;
pub use huffman::encoded_size as huffman_encoded_size;
pub use huffman::huff_tree;
pub use indexing::DefaultIndexingStrategy;
pub use indexing::HeaderIndexingStrategy;
pub use indexing::NoPathIndexingStrategy;
pub use logging::dump_binary;
pub use logging::dump_header_vector;
pub use logging::dump_refset;
pub use logging::print_delta;
pub use table::HeaderTable;
pub use table::StaticHeaderTable;
use thiserror::Error;

pub const DEFAULT_TABLE_SIZE: u32 = 4096;
pub const DEFAULT_MAX_UNCOMPRESSED: u64 = 128 * 1024;

pub(crate) trait HeaderInputs {
    fn for_each<'a>(&'a self, visitor: impl FnMut(Header<'a>));

    /// An upper bound on the size of the block that encodes these fields, from their lengths
    /// alone, or `None` on overflow.
    fn length_bound(&self) -> Option<usize> {
        let mut bound = Some(EMPTY_BLOCK_BOUND);
        self.for_each(|header| {
            bound = add_input_bound(bound, &header);
        });
        bound
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum HpackError {
    #[error("invalid header index")]
    InvalidIndex,
    #[error("invalid Huffman code")]
    InvalidHuffmanCode,
    #[error("invalid HPACK encoding")]
    InvalidEncoding,
    #[error("HPACK integer overflow")]
    IntegerOverflow,
    #[error("invalid dynamic table size")]
    InvalidTableSize,
    #[error("dynamic table size update appeared after a header representation")]
    InvalidTableSizeUpdate,
    #[error("decoded headers exceed the configured limit")]
    HeadersTooLarge,
    #[error("compressed header block ended early")]
    BufferUnderflow,
    #[error("literal exceeds the configured limit")]
    LiteralTooLarge,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Instruction {
    pub code: u8,
    pub prefix_length: u8,
}

pub const INDEX_REF: Instruction = Instruction {
    code: 0x80,
    prefix_length: 7,
};
pub const LITERAL_INC_INDEX: Instruction = Instruction {
    code: 0x40,
    prefix_length: 6,
};
pub const LITERAL: Instruction = Instruction {
    code: 0x00,
    prefix_length: 4,
};
pub const LITERAL_NEV_INDEX: Instruction = Instruction {
    code: 0x10,
    prefix_length: 4,
};
pub const TABLE_SIZE_UPDATE: Instruction = Instruction {
    code: 0x20,
    prefix_length: 5,
};

pub const NBIT_MASKS: [u8; 9] = [0x00, 0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0x7f, 0xff];
