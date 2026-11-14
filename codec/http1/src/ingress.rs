use super::*;

impl Http1xCodec {
    fn track_header_bytes(&mut self, amount: usize) -> Result<(), (ParseErrorKind, &'static [u8])> {
        self.header_bytes = self.header_bytes.saturating_add(amount);
        if self.header_bytes > MAX_HEADER_BYTES {
            return Err((self.parse_error_kind(), b"HTTP header block is too large"));
        }
        Ok(())
    }

    fn parse_error_kind(&self) -> ParseErrorKind {
        match self.phase {
            IngressPhase::Headers | IngressPhase::Idle => ParseErrorKind::Header,
            _ => ParseErrorKind::Body,
        }
    }

    fn finish_header(&mut self) -> Result<(), (ParseErrorKind, &'static [u8])> {
        let value = trim_http_whitespace(&self.header_value);
        if self.strict_validation && value.iter().any(|byte| *byte >= 0x7f) {
            return Err((
                ParseErrorKind::HeaderContent,
                b"invalid HTTP header content",
            ));
        }
        if self.phase == IngressPhase::Trailers {
            self.trailers.add(&self.header_name, value);
        } else if let Some(message) = self.current_message.as_mut() {
            message.headers_mut().add(&self.header_name, value);
        }
        self.header_name.clear();
        self.header_value.clear();
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
            return Err(header_error(b"unsupported Transfer-Encoding"));
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
                    .set_url(&self.target, true)
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

    fn parser_event(
        &mut self,
        parser: &mut Parser,
        event: ParserEvent,
        range: Range<usize>,
        input: &Bytes,
        metadata_cursor: &mut usize,
        callback: &mut (impl IngressDelivery + ?Sized),
        callback_egress: &mut BufQueue,
        result: &mut CodecIngressResult,
    ) -> i32 {
        let event_end = range.end;
        let outcome = (|| -> Result<(), (ParseErrorKind, &'static [u8])> {
            if !self.http09_synthetic
                && matches!(self.phase, IngressPhase::Headers | IngressPhase::Trailers)
            {
                self.track_header_bytes(range.end.saturating_sub(*metadata_cursor))?;
            }
            *metadata_cursor = range.end;
            match event {
                ParserEvent::MessageBegin => {
                    self.phase = IngressPhase::Headers;
                    self.header_bytes = 0;
                    self.protocol_bytes = 0;
                    self.http09_request = false;
                    self.target.clear();
                    self.parsed_target = None;
                    self.reason.clear();
                    self.header_name.clear();
                    self.header_value.clear();
                    self.trailers = HttpHeaders::new();
                    self.begin_message(callback, callback_egress, result);
                }
                ParserEvent::Protocol => {
                    let end = self.protocol_bytes + range.len();
                    if b"HTTP".get(self.protocol_bytes..end) != Some(&input[range]) {
                        return Err((ParseErrorKind::Header, b"invalid HTTP protocol"));
                    }
                    self.protocol_bytes = end;
                }
                ParserEvent::Url => {
                    self.http09_request |= matches!(input.get(range.end), Some(b'\r' | b'\n'));
                    self.target.extend_from_slice(&input[range]);
                }
                ParserEvent::MethodComplete => {
                    if let Some(message) = self.current_message.as_mut() {
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
                    if let Some(message) = self.current_message.as_mut() {
                        self.parsed_target = message.set_url(&self.target, true).ok();
                        message.set_http_version(parser.http_major, parser.http_minor);
                    }
                    self.http09_request = parser.http_major == 0 && parser.http_minor == 9;
                }
                ParserEvent::VersionComplete => {
                    self.http09_request = parser.http_major == 0 && parser.http_minor == 9;
                    if let Some(message) = self.current_message.as_mut() {
                        message.set_http_version(parser.http_major, parser.http_minor);
                    }
                }
                ParserEvent::Status => {
                    self.reason.extend_from_slice(&input[range]);
                }
                ParserEvent::StatusComplete => {
                    if let Some(message) = self.current_message.as_mut() {
                        let _ = message.set_status_code(parser.status_code);
                        let _ = message.set_status_message(&self.reason);
                    }
                }
                ParserEvent::HeaderField => {
                    self.header_name.extend_from_slice(&input[range]);
                }
                ParserEvent::HeaderValue => {
                    self.header_value.extend_from_slice(&input[range]);
                }
                ParserEvent::HeaderValueComplete => self.finish_header()?,
                ParserEvent::HeadersComplete => {
                    let message = self.finish_headers(parser)?;
                    self.phase = IngressPhase::Body;
                    self.dispatch(
                        callback,
                        callback_egress,
                        CodecEvent::HeadersComplete {
                            stream: self.ingress_stream,
                            message,
                        },
                        result,
                    );
                    if self.direction.is_downstream()
                        && (!self.ingress_upgrade || self.downstream_upgrade_as_ordinary)
                        && self.connect_request
                    {
                        parser.upgrade = 0;
                    }
                }
                ParserEvent::Body => {
                    self.emit_body(input.slice(range), callback, callback_egress, result)
                }
                ParserEvent::ChunkHeader => {
                    if parser.content_length > i64::MAX as u64 {
                        return Err((ParseErrorKind::Body, b"HTTP chunk length overflow"));
                    }
                    if parser.content_length == 0 {
                        self.phase = IngressPhase::Trailers;
                        self.header_bytes = 0;
                    } else {
                        self.dispatch(
                            callback,
                            callback_egress,
                            CodecEvent::ChunkHeader {
                                stream: self.ingress_stream,
                                length: usize::try_from(parser.content_length).map_err(|_| {
                                    (
                                        ParseErrorKind::Body,
                                        b"HTTP chunk length overflow".as_slice(),
                                    )
                                })?,
                            },
                            result,
                        );
                    }
                }
                ParserEvent::ChunkComplete => {
                    if self.phase != IngressPhase::Trailers {
                        self.dispatch(
                            callback,
                            callback_egress,
                            CodecEvent::ChunkComplete {
                                stream: self.ingress_stream,
                            },
                            result,
                        );
                    }
                }
                ParserEvent::MessageComplete => {
                    if !self.trailers.is_empty() {
                        let trailers = Box::new(std::mem::take(&mut self.trailers));
                        self.dispatch(
                            callback,
                            callback_egress,
                            CodecEvent::TrailersComplete {
                                stream: self.ingress_stream,
                                trailers,
                            },
                            result,
                        );
                        if self.parser_paused {
                            self.pending_message_complete = true;
                            return Ok(());
                        }
                    }
                    self.complete_message(callback, callback_egress, result, self.ingress_upgrade);
                }
                _ => {}
            }
            Ok(())
        })();
        if let Err((kind, message)) = outcome {
            self.fail(kind, message, callback, callback_egress, result);
            return ParserError::USER.0;
        }
        let metadata_boundary = event == ParserEvent::MessageBegin
            || (event == ParserEvent::ChunkHeader && parser.content_length == 0);
        if self.parser_paused
            || (metadata_boundary && input.len().saturating_sub(event_end) > MAX_HEADER_BYTES + 1)
            || (event == ParserEvent::UrlComplete && self.http09_request && !self.http09_synthetic)
        {
            ParserError::PAUSED.0
        } else {
            0
        }
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
        if self.phase == IngressPhase::Upgraded && self.ingress_upgrade {
            self.ingress_upgrade_complete = true;
        }
        if self.ingress_upgrade_complete {
            if self.parser_paused && !self.upgrade_pause {
                return result;
            }
            result.consumed = input.len();
            self.emit_body(input.clone(), callback, callback_egress, &mut result);
            return result;
        }
        if self.parser_paused {
            return result;
        }
        if self.pending_message_complete {
            self.pending_message_complete = false;
            self.complete_message(callback, callback_egress, &mut result, self.ingress_upgrade);
            if self.parser_paused {
                return result;
            }
        }
        if self.expect_http_09_response
            && self.direction.is_upstream()
            && self.phase == IngressPhase::Idle
            && !input.is_empty()
        {
            self.begin_http_09_response(callback, callback_egress, &mut result);
        }
        if self.phase == IngressPhase::Http09 {
            if !self.parser_paused {
                result.consumed = input.len();
                self.emit_body(input.clone(), callback, callback_egress, &mut result);
            }
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
                _ => remaining,
            };
            let tail;
            let segment = if offset == 0 && length == input.len() {
                input
            } else {
                tail = input.slice(offset..offset + length);
                &tail
            };
            let mut metadata_cursor = 0;
            let parsed = parser.execute(segment, |parser, event, range| {
                self.parser_event(
                    parser,
                    event,
                    range,
                    segment,
                    &mut metadata_cursor,
                    callback,
                    callback_egress,
                    &mut result,
                )
            });
            offset += parsed.consumed;
            result.consumed = offset;
            if !self.parser_error
                && matches!(self.phase, IngressPhase::Headers | IngressPhase::Trailers)
                && let Err((kind, message)) =
                    self.track_header_bytes(parsed.consumed.saturating_sub(metadata_cursor))
            {
                self.fail(kind, message, callback, callback_egress, &mut result);
            }
            let error = if self.http09_request
                && (parsed.error == ParserError::PAUSED
                    || (parsed.error == ParserError::OK && segment.last() == Some(&b'\n')))
                && !self.parser_error
            {
                self.http09_synthetic = true;
                parser.resume();
                let empty_headers = Bytes::from_static(b"\r\n");
                let mut metadata_cursor = 0;
                let error = parser
                    .execute(&empty_headers, |parser, event, range| {
                        self.parser_event(
                            parser,
                            event,
                            range,
                            &empty_headers,
                            &mut metadata_cursor,
                            callback,
                            callback_egress,
                            &mut result,
                        )
                    })
                    .error;
                self.http09_synthetic = false;
                self.http09_request = false;
                error
            } else {
                parsed.error
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
                || (parsed.error != ParserError::PAUSED && parsed.consumed != length)
            {
                break;
            }
        }
        self.parser = Some(parser);
        result
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
            let mut metadata_cursor = 0;
            let mut error = parser
                .execute(&[], |parser, event, range| {
                    self.parser_event(
                        parser,
                        event,
                        range,
                        &Bytes::new(),
                        &mut metadata_cursor,
                        callback,
                        callback_egress,
                        &mut result,
                    )
                })
                .error;
            if error == ParserError::OK && self.phase != IngressPhase::Idle && !self.parser_paused {
                error = parser.finish(|parser, event, range| {
                    self.parser_event(
                        parser,
                        event,
                        range,
                        &Bytes::new(),
                        &mut metadata_cursor,
                        callback,
                        callback_egress,
                        &mut result,
                    )
                });
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
