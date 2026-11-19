/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::VecDeque;

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
    message: Option<HttpMessage>,
    pushes: VecDeque<HttpBodyEvent>,
    trailers: Option<HttpHeaders>,
    body: BufQueue,
}

impl HttpFixedSource {
    pub fn new(mut message: HttpMessage, body: Option<BufQueue>) -> Self {
        let body = match body {
            Some(body) => {
                message.headers_mut().set(
                    HttpHeaderCode::ContentLength,
                    body.chain_length().to_string().as_bytes(),
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

    /// Mirrors the source copy constructor: message and body are copied while
    /// queued pushes and trailers are not part of the copy.
    pub fn copy_fixed(&self) -> Self {
        Self::from_message_and_body(self.message.clone(), self.body.clone_shared())
    }

    /// Mirrors the source move constructor while keeping the moved-from value
    /// inspectable: only message and body move; pushes and trailers remain.
    pub fn take_fixed(&mut self) -> Self {
        Self::from_message_and_body(self.message.take(), self.body.take())
    }

    fn from_message_and_body(message: Option<HttpMessage>, body: BufQueue) -> Self {
        match message {
            Some(message) => {
                let body = (!body.is_empty()).then_some(body);
                Self::new(message, body)
            }
            None => Self::make_fixed_body(body),
        }
    }

    pub fn message(&self) -> Option<&HttpMessage> {
        self.message.as_ref()
    }

    pub fn message_mut(&mut self) -> Option<&mut HttpMessage> {
        self.message.as_mut()
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
}

impl Clone for HttpFixedSource {
    fn clone(&self) -> Self {
        self.copy_fixed()
    }
}

impl HttpSource for HttpFixedSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let result = self.message.take().map_or_else(
            || {
                Err(HttpError::with_message(
                    HttpErrorCode::INVALID_STATE_TRANSITION,
                    b"fixed source header was already consumed or is body-only",
                ))
            },
            |message| {
                HttpHeaderEvent::new(message, self.body.is_empty()).map_err(header_event_error)
            },
        );
        Box::pin(std::future::ready(result))
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let event = if let Some(push) = self.pushes.pop_front() {
            push
        } else if self.body.is_empty() && self.trailers.is_some() {
            HttpBodyEvent::trailers(
                self.trailers
                    .take()
                    .expect("checked trailer presence before transfer"),
            )
        } else {
            let body = self.body.split_at_most(maximum as usize);
            let eom = self.body.is_empty() && self.trailers.is_none();
            HttpBodyEvent::body(body, eom)
        };
        Box::pin(std::future::ready(Ok(event)))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
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

fn header_event_error(error: HeaderEventError) -> HttpError {
    HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string().as_bytes())
}
