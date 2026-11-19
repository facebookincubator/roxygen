/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use roxygen_core::headers::HttpHeaderCode;

use super::HeaderName;

pub trait HeaderIndexingStrategy: 'static {
    fn index_header(&self, name: &HeaderName, value: &[u8], name_exists: bool) -> bool;

    /// Creates an isolated strategy snapshot for transactional encoding.
    ///
    /// Decision-affecting mutable state must not be shared with `self`; the
    /// encoder commits the returned snapshot only after its output is accepted.
    fn clone_box(&self) -> Box<dyn HeaderIndexingStrategy>;

    fn huffman_limits(&self) -> (usize, usize) {
        (0, usize::MAX)
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultIndexingStrategy;

impl HeaderIndexingStrategy for DefaultIndexingStrategy {
    fn index_header(&self, name: &HeaderName, value: &[u8], _name_exists: bool) -> bool {
        match name.code() {
            HttpHeaderCode::ColonPath => {
                !value.contains(&b'=') && !value.windows(3).any(|window| window == b"jpg")
            }
            HttpHeaderCode::None
            | HttpHeaderCode::ContentLength
            | HttpHeaderCode::IfModifiedSince
            | HttpHeaderCode::LastModified => false,
            _ => true,
        }
    }

    fn clone_box(&self) -> Box<dyn HeaderIndexingStrategy> {
        Box::new(*self)
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoPathIndexingStrategy;

impl HeaderIndexingStrategy for NoPathIndexingStrategy {
    fn index_header(&self, name: &HeaderName, value: &[u8], name_exists: bool) -> bool {
        name.code() != HttpHeaderCode::ColonPath
            && DefaultIndexingStrategy.index_header(name, value, name_exists)
    }

    fn clone_box(&self) -> Box<dyn HeaderIndexingStrategy> {
        Box::new(*self)
    }
}
