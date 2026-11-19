/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::VecDeque;
use std::task::Context;
use std::task::Poll;

use thiserror::Error;

use crate::buf_queue::BufQueue;
use crate::buf_queue::BufQueueError;
use crate::error::HttpError;
use crate::error::HttpErrorCode;
use crate::events::HeaderEventError;
use crate::events::HttpBodyEvent;
use crate::events::HttpHeaderEvent;
use crate::headers::HttpHeaderCode;
use crate::headers::HttpHeaders;
use crate::http::HttpMethod;
use crate::message::HttpMessage;
use crate::message::MessageTypeError;
use crate::source::HttpSource;
use crate::source::LocalSourceFuture;
use crate::url::ParseUrl;

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum FixedSourceError {
    #[error(transparent)]
    Buffer(#[from] BufQueueError),
    #[error(transparent)]
    Message(#[from] MessageTypeError),
    #[error("cannot construct a fixed request from an invalid URL")]
    InvalidUrl,
}

/// An in-memory source with one header event and a fixed body-event sequence.
pub struct HttpFixedSource {
    message: Option<Box<HttpMessage>>,
    pushes: VecDeque<HttpBodyEvent>,
    trailers: Option<HttpHeaders>,
    body: BufQueue,
}

impl HttpFixedSource {
    pub fn new(message: HttpMessage, body: Option<BufQueue>) -> Self {
        Self::with_boxed_message(Box::new(message), body)
    }

    fn with_boxed_message(mut message: Box<HttpMessage>, body: Option<BufQueue>) -> Self {
        let body = match body {
            Some(body) => {
                let mut digits = [0; DECIMAL_DIGITS];
                message.headers_mut().set(
                    HttpHeaderCode::ContentLength,
                    format_decimal(body.chain_length(), &mut digits),
                );
                body
            }
            None => BufQueue::new(0),
        };
        Self {
            message: Some(message),
            pushes: VecDeque::new(),
            trailers: None,
            body,
        }
    }

    pub fn make_fixed_body(body: BufQueue) -> Self {
        Self {
            message: None,
            pushes: VecDeque::new(),
            trailers: None,
            body,
        }
    }

    pub fn make_fixed_request(
        path: impl AsRef<[u8]>,
        method: HttpMethod,
        body: Option<BufQueue>,
    ) -> Result<Self, FixedSourceError> {
        let mut message = HttpMessage::new();
        message.set_url(path, false)?;
        message.set_method(method)?;
        message.set_http_version(1, 1);
        Ok(Self::new(message, body))
    }

    pub fn make_get_request(
        path: impl AsRef<[u8]>,
        body: Option<BufQueue>,
    ) -> Result<Self, FixedSourceError> {
        Self::make_fixed_request(path, HttpMethod::Get, body)
    }

    pub fn make_fixed_request_url(
        url: &ParseUrl,
        method: HttpMethod,
        body: Option<BufQueue>,
    ) -> Result<Self, FixedSourceError> {
        if !url.valid() {
            return Err(FixedSourceError::InvalidUrl);
        }
        let path = url.request_path();
        let query = url.request_query();
        let mut relative =
            Vec::with_capacity(path.len() + usize::from(!query.is_empty()) + query.len());
        relative.extend_from_slice(path);
        if !query.is_empty() {
            relative.push(b'?');
            relative.extend_from_slice(query);
        }
        let mut source = Self::make_fixed_request(relative, method, body)?;
        let message = source
            .message
            .as_mut()
            .expect("newly constructed fixed request retains its message");
        let secure = url.scheme().eq_ignore_ascii_case(b"https");
        message.set_secure(secure);
        if url.has_host() {
            let port = url.port();
            let omit_port = (secure && port == 443)
                || (url.scheme().eq_ignore_ascii_case(b"http") && port == 80);
            let host = if omit_port {
                url.host().to_vec().into_boxed_slice()
            } else {
                url.host_and_port()
            };
            message.headers_mut().set(HttpHeaderCode::Host, host);
        }
        Ok(source)
    }

    pub fn make_fixed_response(
        status_code: u16,
        body: Option<BufQueue>,
    ) -> Result<Self, FixedSourceError> {
        let mut message = HttpMessage::new();
        message.set_status_code(status_code)?;
        message.set_status_message(HttpMessage::default_reason(status_code))?;
        message.set_http_version(1, 1);
        Ok(Self::new(message, body))
    }

    /// Copies the message and body. Queued pushes and trailers are not copied.
    pub fn copy_fixed(&self) -> Self {
        Self::from_message_and_body(self.message.clone(), self.body.clone_shared())
    }

    /// Moves the message and body out. Queued pushes and trailers stay behind, and
    /// the moved-from value can still be inspected.
    pub fn take_fixed(&mut self) -> Self {
        Self::from_message_and_body(self.message.take(), self.body.take())
    }

    fn from_message_and_body(message: Option<Box<HttpMessage>>, body: BufQueue) -> Self {
        match message {
            Some(message) => {
                let body = (!body.is_empty()).then_some(body);
                Self::with_boxed_message(message, body)
            }
            None => Self::make_fixed_body(body),
        }
    }

    pub fn message(&self) -> Option<&HttpMessage> {
        self.message.as_deref()
    }

    pub fn message_mut(&mut self) -> Option<&mut HttpMessage> {
        self.message.as_deref_mut()
    }

    pub fn body(&self) -> &BufQueue {
        &self.body
    }

    pub fn pushes_len(&self) -> usize {
        self.pushes.len()
    }

    pub fn push_event(&mut self, event: HttpBodyEvent) {
        self.pushes.push_back(event);
    }

    pub fn set_trailers(&mut self, trailers: HttpHeaders) {
        self.trailers = Some(trailers);
    }

    pub fn has_trailers(&self) -> bool {
        self.trailers.is_some()
    }

    fn take_header_event(&mut self) -> Result<HttpHeaderEvent, HttpError> {
        let Some(message) = self.message.take() else {
            return Err(HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"fixed source header was already consumed or is body-only",
            ));
        };
        HttpHeaderEvent::from_boxed_headers(message, self.body.is_empty())
            .map_err(header_event_error)
    }

    fn next_body_event(&mut self, maximum: u32) -> HttpBodyEvent {
        if let Some(push) = self.pushes.pop_front() {
            return push;
        }
        if self.body.is_empty()
            && let Some(trailers) = self.trailers.take()
        {
            return HttpBodyEvent::trailers(trailers);
        }
        let body = self.body.split_at_most(maximum as usize);
        let eom = self.body.is_empty() && self.trailers.is_none();
        HttpBodyEvent::body(body, eom)
    }
}

impl Clone for HttpFixedSource {
    fn clone(&self) -> Self {
        self.copy_fixed()
    }
}

impl HttpSource for HttpFixedSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(self.take_header_event()))
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(std::future::ready(Ok(self.next_body_event(maximum))))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}

    fn polls_header_events(&self) -> bool {
        true
    }

    fn poll_header_event(
        &mut self,
        _context: &mut Context<'_>,
    ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
        Poll::Ready(self.take_header_event())
    }

    fn polls_body_events(&self) -> bool {
        true
    }

    fn poll_body_event(
        &mut self,
        _context: &mut Context<'_>,
        maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        Poll::Ready(Ok(self.next_body_event(maximum)))
    }
}

pub struct HttpErrorSource {
    error: Option<HttpError>,
}

impl HttpErrorSource {
    pub fn new(error: HttpError) -> Self {
        Self { error: Some(error) }
    }
}

impl HttpSource for HttpErrorSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let error = self.error.take().unwrap_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"error source was already consumed")
        });
        Box::pin(std::future::ready(Err(error)))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(std::future::ready(Err(HttpError::with_message(
            HttpErrorCode::INVALID_STATE_TRANSITION,
            b"error source cannot produce body events",
        ))))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

pub fn get_error_response(
    status_code: u16,
    body: impl AsRef<[u8]>,
) -> Result<HttpFixedSource, FixedSourceError> {
    let body = body.as_ref();
    let body = BufQueue::copy_from_slice(body, body.len())?;
    let mut response = HttpFixedSource::make_fixed_response(status_code, Some(body))?;
    response
        .message_mut()
        .expect("newly constructed fixed response retains its message")
        .set_wants_keepalive(false);
    Ok(response)
}

/// The most decimal digits a `usize` has.
pub(crate) const DECIMAL_DIGITS: usize = usize::MAX.ilog10() as usize + 1;

/// Writes `value` in decimal at the end of `digits` and returns the written part.
pub(crate) fn format_decimal(mut value: usize, digits: &mut [u8; DECIMAL_DIGITS]) -> &[u8] {
    let mut start = DECIMAL_DIGITS;
    loop {
        start -= 1;
        digits[start] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            return &digits[start..];
        }
    }
}

fn header_event_error(error: HeaderEventError) -> HttpError {
    HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string().as_bytes())
}
