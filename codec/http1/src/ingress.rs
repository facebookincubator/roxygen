/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use roxygen_core::http::is_tchar;

use super::*;
use crate::llhttp::Callbacks;

#[derive(Default)]
struct HeaderSpan {
    borrowed: Option<Range<usize>>,
}

impl HeaderSpan {
    #[inline(always)]
    fn append(&mut self, owned: &mut Vec<u8>, input: &Bytes, range: Range<usize>) {
        if owned.is_empty() && self.borrowed.is_none() {
            self.borrowed = Some(range);
        } else {
            self.append_owned(owned, input, range);
        }
    }

    #[inline(never)]
    fn append_owned(&mut self, owned: &mut Vec<u8>, input: &Bytes, range: Range<usize>) {
        self.spill(owned, input);
        owned.extend_from_slice(&input[range]);
    }

    fn bytes<'a>(&self, input: &'a Bytes, owned: &'a [u8]) -> &'a [u8] {
        match &self.borrowed {
            Some(range) => &input[range.clone()],
            None => owned,
        }
    }

    fn spill(&mut self, owned: &mut Vec<u8>, input: &Bytes) {
        if let Some(range) = self.borrowed.take() {
            owned.extend_from_slice(&input[range]);
        }
    }
}

#[derive(Default)]
struct HeaderSpans {
    name: HeaderSpan,
    value: HeaderSpan,
}

/// The codec state that parser callbacks update while the parser runs over one
/// input slice, or at EOF.
struct Execution<'a, C: ?Sized> {
    codec: &'a mut Http1xCodec,
    input: &'a Bytes,
    metadata_cursor: usize,
    header_spans: HeaderSpans,
    callback: &'a mut C,
    callback_egress: &'a mut BufQueue,
    result: &'a mut CodecIngressResult,
}

impl<C: IngressDelivery + ?Sized> Callbacks for Execution<'_, C> {
    #[inline(always)]
    fn on_event(&mut self, parser: &mut Parser, event: ParserEvent, range: Range<usize>) -> i32 {
        self.event(parser, event, range)
    }
}

impl<C: IngressDelivery + ?Sized> Execution<'_, C> {
    /// Parses `input` until it is consumed or a callback pauses or fails, then
    /// moves unfinished header spans into owned storage because `input` is not
    /// retained after this call. Returns the consumed length and parser error.
    #[inline(always)]
    fn execute(&mut self, parser: &mut Parser) -> (usize, ParserError) {
        let input = self.input;
        let parsed = parser.execute_callbacks(input, self);
        self.header_spans
            .name
            .spill(&mut self.codec.header_name, input);
        self.header_spans
            .value
            .spill(&mut self.codec.header_value, input);
        (parsed.consumed, parsed.error)
    }

    fn finish(&mut self, parser: &mut Parser) -> ParserError {
        parser.finish_callbacks(self)
    }

    /// Routes each parser event to its handler.
    #[inline(always)]
    fn event(&mut self, parser: &mut Parser, event: ParserEvent, range: Range<usize>) -> i32 {
        match event {
            ParserEvent::HeaderField => {
                if let Err(error) = self.account(range.end) {
                    return self.fail(error);
                }
                self.header_spans
                    .name
                    .append(&mut self.codec.header_name, self.input, range);
                self.continuation()
            }
            ParserEvent::HeaderValue => {
                if let Err(error) = self.account(range.end) {
                    return self.fail(error);
                }
                self.header_spans
                    .value
                    .append(&mut self.codec.header_value, self.input, range);
                self.continuation()
            }
            ParserEvent::HeaderFieldComplete
            | ParserEvent::Method
            | ParserEvent::Version
            | ParserEvent::ProtocolComplete
            | ParserEvent::ChunkExtensionName
            | ParserEvent::ChunkExtensionNameComplete
            | ParserEvent::ChunkExtensionValue
            | ParserEvent::ChunkExtensionValueComplete
            | ParserEvent::Reset => {
                if let Err(error) = self.account(range.end) {
                    return self.fail(error);
                }
                self.continuation()
            }
            ParserEvent::HeaderValueComplete => self.header_value_complete(range.end),
            ParserEvent::Body => self.body(range),
            ParserEvent::MessageBegin
            | ParserEvent::Protocol
            | ParserEvent::Url
            | ParserEvent::MethodComplete
            | ParserEvent::UrlComplete
            | ParserEvent::VersionComplete
            | ParserEvent::Status
            | ParserEvent::StatusComplete
            | ParserEvent::HeadersComplete
            | ParserEvent::ChunkHeader
            | ParserEvent::ChunkComplete
            | ParserEvent::MessageComplete => self.parser_event(parser, event, range),
        }
    }

    #[inline(always)]
    fn account(&mut self, end: usize) -> Result<(), (ParseErrorKind, &'static [u8])> {
        if !self.codec.http09_synthetic
            && matches!(
                self.codec.phase,
                IngressPhase::Headers | IngressPhase::Trailers
            )
        {
            self.codec
                .track_header_bytes(end.saturating_sub(self.metadata_cursor))?;
        }
        self.metadata_cursor = end;
        Ok(())
    }

    #[inline(always)]
    fn continuation(&self) -> i32 {
        if self.codec.parser_paused {
            ParserError::PAUSED.0
        } else {
            0
        }
    }

    #[cold]
    #[inline(never)]
    fn fail(&mut self, (kind, message): (ParseErrorKind, &'static [u8])) -> i32 {
        self.codec.fail(
            kind,
            message,
            self.callback,
            self.callback_egress,
            self.result,
        );
        ParserError::USER.0
    }

    #[inline(never)]
    fn header_value_complete(&mut self, end: usize) -> i32 {
        let outcome = self
            .account(end)
            .and_then(|()| self.codec.finish_header(self.input, &mut self.header_spans));
        if let Err(error) = outcome {
            return self.fail(error);
        }
        self.continuation()
    }

    /// Handles the message, start-line, header-block and chunk events that
    /// `event` routes here.
    #[inline(never)]
    fn parser_event(
        &mut self,
        parser: &mut Parser,
        event: ParserEvent,
        range: Range<usize>,
    ) -> i32 {
        let event_end = range.end;
        let outcome = (|| -> Result<(), (ParseErrorKind, &'static [u8])> {
            self.account(range.end)?;
            match event {
                ParserEvent::MessageBegin => {
                    self.codec.phase = IngressPhase::Headers;
                    self.codec.header_bytes = 0;
                    self.codec.protocol_bytes = 0;
                    self.codec.http09_request = false;
                    self.codec.target.clear();
                    self.codec.target_complete = false;
                    self.codec.parsed_target = None;
                    self.codec.reason.clear();
                    self.codec.header_name.clear();
                    self.codec.header_value.clear();
                    self.header_spans = HeaderSpans::default();
                    self.codec.trailers = None;
                    self.codec
                        .begin_message(self.callback, self.callback_egress, self.result);
                    if self.codec.header_reserve_hint != 0
                        && let Some(message) = self.codec.current_message.as_mut()
                    {
                        message
                            .headers_mut()
                            .reserve(self.codec.header_reserve_hint);
                    }
                }
                ParserEvent::Protocol => {
                    let end = self.codec.protocol_bytes + range.len();
                    if b"HTTP".get(self.codec.protocol_bytes..end) != Some(&self.input[range]) {
                        return Err((ParseErrorKind::Header, b"invalid HTTP protocol"));
                    }
                    self.codec.protocol_bytes = end;
                }
                ParserEvent::Url => {
                    self.codec.http09_request |=
                        matches!(self.input.get(range.end), Some(b'\r' | b'\n'));
                    self.codec.target.extend_from_slice(&self.input[range]);
                }
                ParserEvent::MethodComplete => {
                    if let Some(message) = self.codec.current_message.as_mut() {
                        let method = Method::from_exact_token(parser.method_name().as_bytes())
                            .ok_or((
                                ParseErrorKind::Header,
                                b"invalid HTTP request method".as_slice(),
                            ))?;
                        message.set_method(method).map_err(|_| {
                            (
                                ParseErrorKind::Header,
                                b"invalid HTTP request method".as_slice(),
                            )
                        })?;
                    }
                }
                ParserEvent::UrlComplete => {
                    self.codec.target_complete = true;
                    if let Some(message) = self.codec.current_message.as_mut() {
                        self.codec.parsed_target =
                            message.set_url_components(&self.codec.target, true).ok();
                        message.set_http_version(parser.http_major, parser.http_minor);
                    }
                    self.codec.http09_request = parser.http_major == 0 && parser.http_minor == 9;
                }
                ParserEvent::VersionComplete => {
                    self.codec.http09_request = parser.http_major == 0 && parser.http_minor == 9;
                    if let Some(message) = self.codec.current_message.as_mut() {
                        message.set_http_version(parser.http_major, parser.http_minor);
                    }
                }
                ParserEvent::Status => {
                    self.codec.reason.extend_from_slice(&self.input[range]);
                }
                ParserEvent::StatusComplete => {
                    if let Some(message) = self.codec.current_message.as_mut() {
                        let _ = message.set_status_code(parser.status_code);
                        let _ = message.set_status_message(&self.codec.reason);
                    }
                }
                ParserEvent::HeadersComplete => {
                    let message = self.codec.finish_headers(parser)?;
                    self.codec.header_reserve_hint = message.headers().len().min(64);
                    self.codec.phase = IngressPhase::Body;
                    self.codec.dispatch(
                        self.callback,
                        self.callback_egress,
                        CodecEvent::HeadersComplete {
                            stream: self.codec.ingress_stream,
                            message,
                        },
                        self.result,
                    );
                    if self.codec.direction.is_downstream()
                        && (!self.codec.ingress_upgrade
                            || self.codec.downstream_upgrade_as_ordinary)
                        && self.codec.connect_request
                    {
                        parser.upgrade = 0;
                    }
                }
                ParserEvent::ChunkHeader => {
                    if parser.content_length > i64::MAX as u64 {
                        return Err((ParseErrorKind::Body, b"HTTP chunk length overflow"));
                    }
                    if parser.content_length == 0 {
                        self.codec.phase = IngressPhase::Trailers;
                        self.codec.header_bytes = 0;
                    } else {
                        self.codec.dispatch(
                            self.callback,
                            self.callback_egress,
                            CodecEvent::ChunkHeader {
                                stream: self.codec.ingress_stream,
                                length: usize::try_from(parser.content_length).map_err(|_| {
                                    (
                                        ParseErrorKind::Body,
                                        b"HTTP chunk length overflow".as_slice(),
                                    )
                                })?,
                            },
                            self.result,
                        );
                    }
                }
                ParserEvent::ChunkComplete => {
                    if self.codec.phase != IngressPhase::Trailers {
                        self.codec.dispatch(
                            self.callback,
                            self.callback_egress,
                            CodecEvent::ChunkComplete {
                                stream: self.codec.ingress_stream,
                            },
                            self.result,
                        );
                    }
                }
                ParserEvent::MessageComplete => {
                    if let Some(trailers) = self.codec.trailers.take() {
                        let trailers = Box::new(trailers);
                        self.codec.dispatch(
                            self.callback,
                            self.callback_egress,
                            CodecEvent::TrailersComplete {
                                stream: self.codec.ingress_stream,
                                trailers,
                            },
                            self.result,
                        );
                        if self.codec.parser_paused {
                            self.codec.pending_message_complete = true;
                            return Ok(());
                        }
                    }
                    self.codec.complete_message(
                        self.callback,
                        self.callback_egress,
                        self.result,
                        self.codec.ingress_upgrade,
                    );
                }
                _ => {}
            }
            Ok(())
        })();
        if let Err((kind, message)) = outcome {
            self.codec.fail(
                kind,
                message,
                self.callback,
                self.callback_egress,
                self.result,
            );
            return ParserError::USER.0;
        }
        let metadata_boundary = event == ParserEvent::MessageBegin
            || (event == ParserEvent::ChunkHeader && parser.content_length == 0);
        let rest = self.input.len().saturating_sub(event_end);
        // The pause re-slices the rest of the run, so a body that follows its head in one run
        // is parsed within the body limit too. The limit is at least one byte.
        let body_boundary = event == ParserEvent::HeadersComplete
            && self.codec.phase == IngressPhase::Body
            && rest > 1
            && rest > self.codec.body_limit(&*self.callback);
        if self.codec.parser_paused
            || (metadata_boundary && rest > MAX_HEADER_BYTES + 1)
            || body_boundary
            || (event == ParserEvent::UrlComplete
                && self.codec.http09_request
                && !self.codec.http09_synthetic)
        {
            ParserError::PAUSED.0
        } else {
            0
        }
    }

    #[inline(never)]
    fn body(&mut self, range: Range<usize>) -> i32 {
        if let Err(error) = self.account(range.end) {
            return self.fail(error);
        }
        self.codec.emit_body(
            self.input.slice(range),
            self.callback,
            self.callback_egress,
            self.result,
        );
        self.continuation()
    }
}

/// RFC 9112 section 6.1: codings the codec does not implement under one final chunked get 501.
/// Chunked that is not last (section 6.3) and a malformed coding stay 400.
fn unsupported_transfer_coding(headers: &HttpHeaders) -> (ParseErrorKind, &'static [u8]) {
    let mut codings = Vec::new();
    headers.for_each_value(HttpHeaderCode::TransferEncoding, |value| {
        codings.extend(
            value
                .split(|byte| *byte == b',')
                .map(trim_http_whitespace)
                .filter(|coding| !coding.is_empty()),
        );
        false
    });
    let not_implemented = codings.split_last().is_some_and(|(last, rest)| {
        last.eq_ignore_ascii_case(b"chunked")
            && !rest.is_empty()
            && rest
                .iter()
                .all(|coding| coding.iter().copied().all(is_tchar))
    });
    let kind = if not_implemented {
        ParseErrorKind::NotImplemented
    } else {
        ParseErrorKind::Header
    };
    (kind, b"unsupported Transfer-Encoding")
}

impl Http1xCodec {
    fn track_header_bytes(&mut self, amount: usize) -> Result<(), (ParseErrorKind, &'static [u8])> {
        self.header_bytes = self.header_bytes.saturating_add(amount);
        if self.header_bytes <= MAX_HEADER_BYTES {
            return Ok(());
        }
        Err(match self.phase {
            // Only the method and the spaces on either side of the target are counted with it, so
            // the target used up the head budget.
            IngressPhase::Headers if self.direction.is_downstream() && !self.target_complete => (
                ParseErrorKind::UriTooLong,
                b"HTTP request target is too long",
            ),
            IngressPhase::Headers => (
                ParseErrorKind::HeaderFieldsTooLarge,
                b"HTTP header block is too large",
            ),
            _ => (self.parse_error_kind(), b"HTTP header block is too large"),
        })
    }

    fn parse_error_kind(&self) -> ParseErrorKind {
        match self.phase {
            IngressPhase::Headers | IngressPhase::Idle => ParseErrorKind::Header,
            _ => ParseErrorKind::Body,
        }
    }

    #[inline]
    fn finish_header(
        &mut self,
        input: &Bytes,
        spans: &mut HeaderSpans,
    ) -> Result<(), (ParseErrorKind, &'static [u8])> {
        let name = spans.name.bytes(input, &self.header_name);
        let value = trim_http_whitespace(spans.value.bytes(input, &self.header_value));
        if self.strict_validation && value.iter().any(|byte| *byte >= 0x7f) {
            return Err((
                ParseErrorKind::HeaderContent,
                b"invalid HTTP header content",
            ));
        }
        if self.phase == IngressPhase::Trailers {
            self.trailers
                .get_or_insert_with(HttpHeaders::new)
                .add(name, value);
        } else if let Some(message) = self.current_message.as_mut() {
            message.headers_mut().add(name, value);
        }
        self.header_name.clear();
        self.header_value.clear();
        *spans = HeaderSpans::default();
        Ok(())
    }

    fn finish_headers(
        &mut self,
        parser: &mut Parser,
    ) -> Result<Box<HttpMessage>, (ParseErrorKind, &'static [u8])> {
        let mut message = self.current_message.take().unwrap_or_default();
        let result = self.prepare_headers(parser, &mut message);
        match result {
            Ok(()) => Ok(Box::new(message)),
            Err(error) => {
                self.current_message = Some(message);
                Err(error)
            }
        }
    }

    fn prepare_headers(
        &mut self,
        parser: &mut Parser,
        message: &mut HttpMessage,
    ) -> Result<(), (ParseErrorKind, &'static [u8])> {
        let header_error = |message: &'static [u8]| (ParseErrorKind::Header, message);
        if parser.content_length > i64::MAX as u64 {
            return Err(header_error(b"HTTP content length overflow"));
        }
        let mut supported_encoding = true;
        message
            .headers()
            .for_each_value(HttpHeaderCode::TransferEncoding, |value| {
                supported_encoding &= value.eq_ignore_ascii_case(b"chunked");
                !supported_encoding
            });
        if !supported_encoding {
            return Err(unsupported_transfer_coding(message.headers()));
        }
        let version = (parser.http_major, parser.http_minor);
        message.set_http_version(version.0, version.1);
        message.set_chunked(parser.flags & llhttp::flags::CHUNKED != 0);
        message.set_ingress_header_size(header_size(self.header_bytes));
        if self.direction.is_downstream() {
            if message.request_method() != Some(parser.method_name()) {
                let method = Method::from_exact_token(parser.method_name().as_bytes())
                    .ok_or_else(|| header_error(b"invalid HTTP request method"))?;
                message
                    .set_method(method)
                    .map_err(|_| header_error(b"invalid HTTP request method"))?;
            }
            let parsed = match self.parsed_target.take() {
                Some(parsed) => parsed,
                None => message
                    .set_url_components(&self.target, true)
                    .map_err(|_| header_error(b"invalid HTTP request URL"))?,
            };
            if !parsed.valid() || parsed.authority().contains(&b'@') {
                return Err(header_error(b"invalid HTTP request URL"));
            }
            if parsed.has_host() {
                let host = parsed.host_and_port();
                message.headers_mut().set(HttpHeaderCode::Host, host);
            }
            self.may_chunk_egress = version.0 == 1 && version.1 >= 1;
        } else {
            if message.response_status() != Some(parser.status_code) {
                message
                    .set_status_code(parser.status_code)
                    .map_err(|_| header_error(b"invalid HTTP response status"))?;
            }
            if message.response_status_message() != Some(self.reason.as_slice()) {
                message
                    .set_status_message(&self.reason)
                    .map_err(|_| header_error(b"invalid HTTP response reason"))?;
            }
        }
        let status = parser.status_code;
        let ordinary = self.direction.is_downstream() && self.downstream_upgrade_as_ordinary;
        let ordinary_response = self.direction.is_upstream() && self.ordinary_request_sent;
        if ordinary {
            self.last_ordinary_request = self.ingress_stream;
        }
        if self.direction.is_downstream() {
            self.connect_request =
                !ordinary && message.method().ok().flatten() == Some(HttpMethod::Connect);
            self.head_request = message.method().ok().flatten() == Some(HttpMethod::Head);
        }
        if ordinary || ordinary_response {
            parser.upgrade = 0;
        }
        let upgrade = message
            .headers()
            .get_single_or_empty(HttpHeaderCode::Upgrade);
        let websocket =
            !(ordinary || ordinary_response) && upgrade.eq_ignore_ascii_case(b"websocket");
        self.keepalive &= ordinary || ordinary_response || upgrade.is_empty();
        self.keepalive &= !self.connect_request;
        if websocket && (self.direction.is_upstream() || parser.upgrade != 0) {
            message.set_ingress_websocket_upgrade();
            if self.direction.is_upstream() {
                if message
                    .headers()
                    .get_single_or_empty(HttpHeaderCode::SecWebSocketAccept)
                    != self.websocket_accept_key
                {
                    return Err(header_error(b"invalid websocket accept key"));
                }
            } else {
                if !self.websocket_accept_key.is_empty() {
                    return Err(header_error(b"duplicate websocket upgrade"));
                }
                self.websocket_accept_key = websocket_accept(
                    message
                        .headers()
                        .get_single_or_empty(HttpHeaderCode::SecWebSocketKey),
                );
            }
        }
        if self.direction.is_upstream() {
            if self.connect_request && (200..300).contains(&status) {
                self.ingress_upgrade = true;
                parser.upgrade = 1;
            } else if status == 101 {
                if ordinary_response {
                    return Err(header_error(
                        b"switching protocols refused for an ordinary request",
                    ));
                }
                if !server_accepted_upgrade(
                    &self.upgrade_header,
                    message
                        .headers()
                        .get_single_or_empty(HttpHeaderCode::Upgrade),
                ) {
                    return Err(header_error(b"upgrade protocol mismatch"));
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
        self.informational_response = self.direction.is_upstream() && status / 100 == 1;
        if self.direction.is_upstream()
            && (self.head_request
                || self.ingress_upgrade
                || rfc2616::response_body_must_be_empty(u32::from(status)))
        {
            parser.skip_body();
        }
        let message_keepalive = parser.should_keep_alive();
        message.set_wants_keepalive(message_keepalive);
        self.keepalive &= message_keepalive;
        if self.direction.is_downstream() {
            self.keepalive_requested = if message_keepalive
                && version == (1, 0)
                && self.keepalive_requested != KeepaliveRequested::Disabled
            {
                KeepaliveRequested::Enabled
            } else {
                KeepaliveRequested::Disabled
            };
        }
        if self.user_agent.is_empty() {
            self.user_agent.extend_from_slice(
                message
                    .headers()
                    .get_single_or_empty(HttpHeaderCode::UserAgent),
            );
        }
        Ok(())
    }

    pub(super) fn parse_ingress(
        &mut self,
        input: &BufQueue,
        callback: &mut (impl IngressDelivery + ?Sized),
        callback_egress: &mut BufQueue,
    ) -> CodecIngressResult {
        let mut result = CodecIngressResult::default();
        if input.is_empty() {
            return self.parse_segment(&Bytes::new(), callback, callback_egress);
        }
        let raw_ingress = self.ingress_upgrade_complete
            || (self.phase == IngressPhase::Upgraded && self.ingress_upgrade);
        for segment in input.segments() {
            let parsed = self.parse_segment(segment, callback, callback_egress);
            let consumed = parsed.consumed;
            result.consumed += consumed;
            result.egress_results.extend(parsed.egress_results);
            if consumed != segment.len()
                || (self.parser_paused && (!raw_ingress || !self.upgrade_pause))
                || self.parser_error
            {
                break;
            }
        }
        result
    }

    fn parse_segment(
        &mut self,
        input: &Bytes,
        callback: &mut (impl IngressDelivery + ?Sized),
        callback_egress: &mut BufQueue,
    ) -> CodecIngressResult {
        let mut result = CodecIngressResult::default();
        if self.parser_error {
            return result;
        }
        if self.ingress_upgrade_complete
            || self.pending_message_complete
            || self.expect_http_09_response
            || matches!(self.phase, IngressPhase::Upgraded | IngressPhase::Http09)
        {
            if !self.segment_prelude(input, callback, callback_egress, &mut result) {
                return result;
            }
        } else if self.parser_paused {
            return result;
        }
        let mut parser = self
            .parser
            .take()
            .expect("parser is installed outside its callbacks");
        parser.resume();
        if self.direction.is_downstream()
            && self.phase == IngressPhase::Body
            && !self.ingress_upgrade
            && self.connect_request
        {
            parser.upgrade = 0;
        }
        let mut offset = 0;
        loop {
            parser.resume();
            let remaining = input.len() - offset;
            let length = match self.phase {
                IngressPhase::Idle => remaining.min(MAX_HEADER_BYTES + 1),
                IngressPhase::Headers | IngressPhase::Trailers => {
                    remaining.min(MAX_HEADER_BYTES.saturating_sub(self.header_bytes) + 1)
                }
                IngressPhase::Body => remaining.min(self.body_limit(&*callback)),
                _ => remaining,
            };
            let tail;
            let segment = if offset == 0 && length == input.len() {
                input
            } else {
                tail = input.slice(offset..offset + length);
                &tail
            };
            let mut execution = Execution {
                codec: self,
                input: segment,
                metadata_cursor: 0,
                header_spans: HeaderSpans::default(),
                callback,
                callback_egress,
                result: &mut result,
            };
            let (consumed, parse_error) = execution.execute(&mut parser);
            let metadata_cursor = execution.metadata_cursor;
            offset += consumed;
            result.consumed = offset;
            if !self.parser_error
                && matches!(self.phase, IngressPhase::Headers | IngressPhase::Trailers)
                && let Err((kind, message)) =
                    self.track_header_bytes(consumed.saturating_sub(metadata_cursor))
            {
                self.fail(kind, message, callback, callback_egress, &mut result);
            }
            let error = if self.http09_request
                && (parse_error == ParserError::PAUSED
                    || (parse_error == ParserError::OK && segment.last() == Some(&b'\n')))
                && !self.parser_error
            {
                self.end_http09_request_head(&mut parser, callback, callback_egress, &mut result)
            } else {
                parse_error
            };
            if error == ParserError::PAUSED_UPGRADE {
                self.upgrade_pause |= !self.parser_paused;
                self.parser_paused = true;
                self.phase = IngressPhase::Upgraded;
            } else if error != ParserError::OK && error != ParserError::PAUSED {
                self.fail(
                    self.parse_error_kind(),
                    parser.reason().as_bytes(),
                    callback,
                    callback_egress,
                    &mut result,
                );
            }
            if self.parser_paused
                || self.parser_error
                || offset == input.len()
                || (parse_error != ParserError::PAUSED && consumed != length)
            {
                break;
            }
        }
        self.parser = Some(parser);
        result
    }

    /// Handles the upgrade, pending completion and HTTP/0.9 states that precede
    /// parsing, in order, including forwarding upgraded tunnel bytes and HTTP/0.9
    /// response bodies. Returns whether the parser should run on `input`.
    #[inline(never)]
    fn segment_prelude(
        &mut self,
        input: &Bytes,
        callback: &mut (impl IngressDelivery + ?Sized),
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) -> bool {
        if self.phase == IngressPhase::Upgraded && self.ingress_upgrade {
            self.ingress_upgrade_complete = true;
        }
        if self.ingress_upgrade_complete {
            if self.parser_paused && !self.upgrade_pause {
                return false;
            }
            self.forward_body(input, callback, callback_egress, result);
            return false;
        }
        if self.parser_paused {
            return false;
        }
        if self.pending_message_complete {
            self.pending_message_complete = false;
            self.complete_message(callback, callback_egress, result, self.ingress_upgrade);
            if self.parser_paused {
                return false;
            }
        }
        if self.expect_http_09_response
            && self.direction.is_upstream()
            && self.phase == IngressPhase::Idle
            && !input.is_empty()
        {
            self.begin_http_09_response(callback, callback_egress, result);
        }
        if self.phase == IngressPhase::Http09 {
            if !self.parser_paused {
                self.forward_body(input, callback, callback_egress, result);
            }
            return false;
        }
        true
    }

    /// Supplies the empty header block that ends an HTTP/0.9 request line.
    #[cold]
    #[inline(never)]
    fn end_http09_request_head(
        &mut self,
        parser: &mut Parser,
        callback: &mut (impl IngressDelivery + ?Sized),
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) -> ParserError {
        self.http09_synthetic = true;
        parser.resume();
        let empty_headers = Bytes::from_static(b"\r\n");
        let (_, error) = Execution {
            codec: self,
            input: &empty_headers,
            metadata_cursor: 0,
            header_spans: HeaderSpans::default(),
            callback,
            callback_egress,
            result,
        }
        .execute(parser);
        self.http09_synthetic = false;
        self.http09_request = false;
        error
    }

    pub(super) fn finish_parser(
        &mut self,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
    ) -> CodecCallbackResult {
        let mut result = CodecIngressResult::default();
        if self.pending_message_complete {
            self.pending_message_complete = false;
            self.complete_message(callback, callback_egress, &mut result, self.ingress_upgrade);
        }
        if self.phase == IngressPhase::Idle || (self.parser_paused && !self.upgrade_pause) {
            return CodecCallbackResult {
                egress_results: result.egress_results,
            };
        }
        if self.phase == IngressPhase::Upgraded {
            self.dispatch(
                callback,
                callback_egress,
                CodecEvent::MessageComplete {
                    stream: self.ingress_stream,
                    upgraded: false,
                },
                &mut result,
            );
        } else if self.phase == IngressPhase::Http09 {
            self.complete_message(callback, callback_egress, &mut result, false);
        } else {
            let mut parser = self
                .parser
                .take()
                .expect("parser is installed outside its callbacks");
            parser.resume();
            let empty = Bytes::new();
            let mut execution = Execution {
                codec: self,
                input: &empty,
                metadata_cursor: 0,
                header_spans: HeaderSpans::default(),
                callback,
                callback_egress,
                result: &mut result,
            };
            let (_, mut error) = execution.execute(&mut parser);
            if error == ParserError::OK
                && execution.codec.phase != IngressPhase::Idle
                && !execution.codec.parser_paused
            {
                error = execution.finish(&mut parser);
            }
            self.parser = Some(parser);
            if error == ParserError::PAUSED_UPGRADE {
                self.upgrade_pause |= !self.parser_paused;
                self.parser_paused = true;
                self.phase = IngressPhase::Upgraded;
            }
            if self.phase == IngressPhase::Upgraded
                && self.ingress_upgrade_complete
                && (!self.parser_paused || self.upgrade_pause)
            {
                self.dispatch(
                    callback,
                    callback_egress,
                    CodecEvent::MessageComplete {
                        stream: self.ingress_stream,
                        upgraded: false,
                    },
                    &mut result,
                );
            }
            if error != ParserError::OK
                && error != ParserError::PAUSED
                && error != ParserError::PAUSED_UPGRADE
            {
                let mut eof = CodecCallbackResult::default();
                self.fail_eof(callback, callback_egress, &mut eof);
                result.egress_results.extend(eof.egress_results);
            }
        }
        CodecCallbackResult {
            egress_results: result.egress_results,
        }
    }
}
