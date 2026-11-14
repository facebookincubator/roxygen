// Copyright (c) Meta Platforms, Inc. and affiliates.
// Derived from llhttp, Copyright (c) 2018 Fedor Indutny.
// SPDX-License-Identifier: MIT

use std::ops::Range;

use super::Error;
use super::Event;
use super::ParserType;
use super::flags;
use super::generated::ROOT_STATE;
use super::generated::State;
use super::generated::finish;
use super::generated::methods;
use super::lenient_flags;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ExecuteResult {
    pub(crate) consumed: usize,
    pub(crate) error: Error,
}

/// Incremental llhttp state. Input ranges refer only to the current `execute`
/// call; no borrow or address into the input survives that call.
#[derive(Debug)]
pub(crate) struct Parser {
    pub(crate) content_length: u64,
    pub(crate) parser_type: u8,
    pub(crate) method: u8,
    pub(crate) http_major: u8,
    pub(crate) http_minor: u8,
    pub(super) header_state: u8,
    pub(crate) lenient_flags: u16,
    pub(crate) upgrade: u8,
    pub(super) finish_state: u8,
    pub(crate) flags: u16,
    pub(crate) status_code: u16,
    pub(super) initial_message_completed: u8,
    pub(crate) error: Error,
    pub(crate) reason: &'static str,
    pub(crate) error_pos: usize,
    pub(super) state: State,
    pub(super) sequence_index: usize,
    pub(super) span_start: usize,
    pub(super) span_callback: Option<Event>,
}

impl Parser {
    pub(crate) fn new(parser_type: ParserType) -> Self {
        Self {
            content_length: 0,
            parser_type: parser_type as u8,
            method: 0,
            http_major: 0,
            http_minor: 0,
            header_state: 0,
            lenient_flags: 0,
            upgrade: 0,
            finish_state: finish::SAFE,
            flags: 0,
            status_code: 0,
            initial_message_completed: 0,
            error: Error::OK,
            reason: "",
            error_pos: 0,
            state: ROOT_STATE,
            sequence_index: 0,
            span_start: 0,
            span_callback: None,
        }
    }

    /// Callbacks return llhttp callback codes. In particular, return
    /// `Error::PAUSED.0` to suspend execution at a callback boundary.
    pub(crate) fn execute<F>(&mut self, input: &[u8], mut callback: F) -> ExecuteResult
    where
        F: FnMut(&mut Parser, Event, Range<usize>) -> i32,
    {
        if self.error != Error::OK {
            return ExecuteResult {
                consumed: 0,
                error: self.error,
            };
        }
        if self.span_callback.is_some() {
            self.span_start = 0;
        }
        if let Err(consumed) = self.run(input, &mut callback) {
            self.error_pos = consumed;
            return ExecuteResult {
                consumed,
                error: self.error,
            };
        }
        if let Some(event) = self.span_callback {
            let error = self.span(&mut callback, event, self.span_start..input.len());
            if error != 0 {
                self.error = Error(error);
                self.error_pos = input.len();
                return ExecuteResult {
                    consumed: input.len(),
                    error: self.error,
                };
            }
        }
        ExecuteResult {
            consumed: input.len(),
            error: Error::OK,
        }
    }

    #[inline(always)]
    pub(super) fn span<F>(&mut self, callback: &mut F, event: Event, range: Range<usize>) -> i32
    where
        F: FnMut(&mut Parser, Event, Range<usize>) -> i32,
    {
        let result = callback(self, event, range);
        if result == -1 {
            self.reason = event.span_error_reason();
            Error::USER.0
        } else {
            result
        }
    }

    /// End-of-input is separate from executing an empty slice, which resumes
    /// transitions left pending by a callback pause at the end of a segment.
    pub(crate) fn finish<F>(&mut self, mut callback: F) -> Error
    where
        F: FnMut(&mut Parser, Event, Range<usize>) -> i32,
    {
        if self.error != Error::OK {
            return Error::OK;
        }
        match self.finish_state {
            finish::SAFE_WITH_CB => Error(callback(self, Event::MessageComplete, 0..0)),
            finish::SAFE => Error::OK,
            _ => {
                self.reason = "Invalid EOF state";
                Error::INVALID_EOF_STATE
            }
        }
    }

    #[allow(
        dead_code,
        reason = "Used by the standalone upstream comparison harness"
    )]
    pub(crate) fn reset(&mut self) {
        let parser_type = self.parser_type;
        let lenient_flags = self.lenient_flags;
        *self = Self::new(ParserType::Both);
        self.parser_type = parser_type;
        self.lenient_flags = lenient_flags;
    }

    pub(crate) fn resume(&mut self) {
        if self.error == Error::PAUSED {
            self.error = Error::OK;
        }
    }

    #[allow(
        dead_code,
        reason = "Used by the standalone upstream comparison harness"
    )]
    pub(crate) fn resume_after_upgrade(&mut self) {
        if self.error == Error::PAUSED_UPGRADE {
            self.error = Error::OK;
        }
    }

    pub(crate) fn reason(&self) -> &'static str {
        self.reason
    }

    pub(crate) fn skip_body(&mut self) {
        self.flags |= flags::SKIPBODY;
    }

    pub(crate) fn message_needs_eof(&self) -> bool {
        if self.parser_type == ParserType::Request as u8 {
            return false;
        }
        if self.status_code / 100 == 1
            || self.status_code == 204
            || self.status_code == 304
            || self.flags & flags::SKIPBODY != 0
        {
            return false;
        }
        if self.flags & flags::TRANSFER_ENCODING != 0 && self.flags & flags::CHUNKED == 0 {
            return true;
        }
        self.flags & (flags::CHUNKED | flags::CONTENT_LENGTH) == 0
    }

    pub(crate) fn should_keep_alive(&self) -> bool {
        if self.http_major > 0 && self.http_minor > 0 {
            if self.flags & flags::CONNECTION_CLOSE != 0 {
                return false;
            }
        } else if self.flags & flags::CONNECTION_KEEP_ALIVE == 0 {
            return false;
        }
        !self.message_needs_eof()
    }

    pub(super) fn before_headers_complete(&mut self) -> i32 {
        self.upgrade = u8::from(
            if self.flags & flags::UPGRADE != 0 && self.flags & flags::CONNECTION_UPGRADE != 0 {
                self.parser_type == ParserType::Request as u8 || self.status_code == 101
            } else {
                self.method == methods::CONNECT
            },
        );
        0
    }

    pub(super) fn after_headers_complete(&mut self) -> i32 {
        let has_body = self.flags & flags::CHUNKED != 0 || self.content_length > 0;
        let is_response = self.parser_type == ParserType::Response as u8;
        if (self.upgrade != 0
            && (self.method == methods::CONNECT || self.flags & flags::SKIPBODY != 0 || !has_body))
            || (is_response && self.status_code == 101)
        {
            return 1;
        }
        if is_response && self.status_code == 100 {
            return 0;
        }
        if self.flags & flags::SKIPBODY != 0
            || (is_response && matches!(self.status_code, 102 | 103 | 204 | 304))
        {
            0
        } else if self.flags & flags::CHUNKED != 0 {
            2
        } else if self.flags & flags::TRANSFER_ENCODING != 0 {
            if self.parser_type == ParserType::Request as u8
                && self.lenient_flags & lenient_flags::CHUNKED_LENGTH == 0
                && self.lenient_flags & lenient_flags::TRANSFER_ENCODING == 0
            {
                5
            } else {
                4
            }
        } else if self.flags & flags::CONTENT_LENGTH == 0 {
            if self.message_needs_eof() { 4 } else { 0 }
        } else if self.content_length == 0 {
            0
        } else {
            3
        }
    }

    pub(super) fn after_message_complete(&mut self) -> i32 {
        let keep_alive = self.should_keep_alive();
        self.finish_state = finish::SAFE;
        self.flags = 0;
        i32::from(keep_alive)
    }
}
