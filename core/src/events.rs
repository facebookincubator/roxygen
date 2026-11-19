/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::future::Future;
use std::pin::Pin;

use thiserror::Error;

use crate::buf_queue::BufQueue;
use crate::byte_event::HttpByteEventRegistration;
use crate::headers::HttpHeaders;
use crate::http::HttpHeaderSize;
use crate::message::HttpMessage;
use crate::message::MessageTypeError;
use crate::source::HttpSource;
use crate::source_holder::HttpSourceHolder;

type HeaderEncodedSizeCallback = Box<dyn FnOnce(HttpHeaderSize) + 'static>;

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum HeaderEventError {
    #[error(transparent)]
    MessageType(#[from] MessageTypeError),
    #[error("an informational header event cannot carry end-of-message")]
    InformationalEom,
}

/// An HTTP header block and its common event metadata.
pub struct HttpHeaderEvent {
    headers: Box<HttpMessage>,
    eom: bool,
    byte_event_registrations: Vec<HttpByteEventRegistration>,
    egress_headers_callback: Option<HeaderEncodedSizeCallback>,
}

impl HttpHeaderEvent {
    pub fn new(headers: HttpMessage, eom: bool) -> Result<Self, HeaderEventError> {
        Self::from_boxed_headers(Box::new(headers), eom)
    }

    /// Takes ownership of an allocated head without moving its contents.
    pub fn from_boxed_headers(
        mut headers: Box<HttpMessage>,
        eom: bool,
    ) -> Result<Self, HeaderEventError> {
        if eom && !headers.is_final()? {
            return Err(HeaderEventError::InformationalEom);
        }
        Ok(Self {
            headers,
            eom,
            byte_event_registrations: Vec::new(),
            egress_headers_callback: None,
        })
    }

    pub fn headers(&self) -> &HttpMessage {
        &self.headers
    }

    pub fn headers_mut(&mut self) -> &mut HttpMessage {
        &mut self.headers
    }

    pub fn into_headers(self) -> HttpMessage {
        *self.headers
    }

    /// Transfers the head allocation and drops the remaining event metadata.
    pub fn into_boxed_headers(self) -> Box<HttpMessage> {
        self.headers
    }

    pub const fn eom(&self) -> bool {
        self.eom
    }

    /// Changes body framing while retaining the head and its registered callbacks.
    pub fn set_eom(&mut self, eom: bool) -> Result<(), HeaderEventError> {
        if eom && !self.headers.is_final()? {
            return Err(HeaderEventError::InformationalEom);
        }
        self.eom = eom;
        Ok(())
    }

    pub fn is_final(&mut self) -> Result<bool, MessageTypeError> {
        self.headers.is_final()
    }

    pub fn byte_event_registrations(&self) -> &[HttpByteEventRegistration] {
        &self.byte_event_registrations
    }

    pub fn byte_event_registrations_mut(&mut self) -> &mut Vec<HttpByteEventRegistration> {
        &mut self.byte_event_registrations
    }

    pub fn take_byte_event_registrations(&mut self) -> Vec<HttpByteEventRegistration> {
        std::mem::take(&mut self.byte_event_registrations)
    }

    pub fn set_egress_headers_callback(&mut self, callback: impl FnOnce(HttpHeaderSize) + 'static) {
        self.egress_headers_callback = Some(Box::new(callback));
    }

    pub fn has_egress_headers_callback(&self) -> bool {
        self.egress_headers_callback.is_some()
    }

    /// Invokes the one-shot callback with the size reported by serialization.
    ///
    /// The callback is taken before it runs, so it runs at most once even if it
    /// panics or this is called again.
    pub fn on_headers_encoded(&mut self, encoded_size: HttpHeaderSize) -> bool {
        let Some(callback) = self.egress_headers_callback.take() else {
            return false;
        };
        callback(encoded_size);
        true
    }

    pub fn cancel_egress_headers_callback(&mut self) -> bool {
        self.egress_headers_callback.take().is_some()
    }
}

/// A push promise owns its source until ownership is explicitly transferred.
pub struct HttpPushEvent {
    promise: Box<HttpMessage>,
    push_source: Option<HttpSourceHolder>,
}

impl HttpPushEvent {
    pub fn new(promise: HttpMessage, push_source: Option<Box<dyn HttpSource>>) -> Self {
        Self {
            promise: Box::new(promise),
            push_source: push_source.map(HttpSourceHolder::new),
        }
    }

    pub fn promise(&self) -> &HttpMessage {
        &self.promise
    }

    pub fn promise_mut(&mut self) -> &mut HttpMessage {
        &mut self.promise
    }

    pub fn move_push_source(&mut self) -> Option<HttpSourceHolder> {
        self.push_source.take()
    }

    pub fn has_push_source(&self) -> bool {
        self.push_source.is_some()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum HttpBodyEventType {
    Upgrade = 0,
    Body = 1,
    PushPromise = 3,
    Trailers = 4,
    Suspend = 5,
    Padding = 6,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ResumeStatus {
    NotReady = 0,
    Signalled = 1,
    TimedOut = 2,
    Cancelled = 3,
}

pub type ResumeFuture = Pin<Box<dyn Future<Output = ResumeStatus> + 'static>>;

pub enum HttpBodyEventKind {
    Upgrade,
    Body(BufQueue),
    PushPromise(HttpPushEvent),
    Trailers(HttpHeaders),
    Suspend(ResumeFuture),
    Padding(u16),
}

/// An HTTP event that follows the header block. EOM is a flag on every event, not a
/// separate kind.
pub struct HttpBodyEvent {
    kind: HttpBodyEventKind,
    eom: bool,
    byte_event_registrations: Vec<HttpByteEventRegistration>,
}

impl HttpBodyEvent {
    pub fn body(body: BufQueue, eom: bool) -> Self {
        Self::new(HttpBodyEventKind::Body(body), eom)
    }

    pub fn push_promise(
        promise: HttpMessage,
        push_source: Option<Box<dyn HttpSource>>,
        eom: bool,
    ) -> Self {
        Self::from_push_event(HttpPushEvent::new(promise, push_source), eom)
    }

    pub(crate) fn from_push_event(push: HttpPushEvent, eom: bool) -> Self {
        Self::new(HttpBodyEventKind::PushPromise(push), eom)
    }

    pub fn trailers(trailers: HttpHeaders) -> Self {
        Self::new(HttpBodyEventKind::Trailers(trailers), true)
    }

    pub fn suspend(resume: ResumeFuture) -> Self {
        Self::new(HttpBodyEventKind::Suspend(resume), false)
    }

    pub fn padding(size: u16, eom: bool) -> Self {
        Self::new(HttpBodyEventKind::Padding(size), eom)
    }

    pub fn upgrade(eom: bool) -> Self {
        Self::new(HttpBodyEventKind::Upgrade, eom)
    }

    fn new(kind: HttpBodyEventKind, eom: bool) -> Self {
        debug_assert!(
            !matches!(kind, HttpBodyEventKind::Trailers(_)) || eom,
            "trailers must carry end-of-message"
        );
        debug_assert!(
            !matches!(kind, HttpBodyEventKind::Suspend(_)) || !eom,
            "suspend cannot carry end-of-message"
        );
        Self {
            kind,
            eom,
            byte_event_registrations: Vec::new(),
        }
    }

    pub const fn eom(&self) -> bool {
        self.eom
    }

    pub const fn event_type(&self) -> HttpBodyEventType {
        match &self.kind {
            HttpBodyEventKind::Upgrade => HttpBodyEventType::Upgrade,
            HttpBodyEventKind::Body(_) => HttpBodyEventType::Body,
            HttpBodyEventKind::PushPromise(_) => HttpBodyEventType::PushPromise,
            HttpBodyEventKind::Trailers(_) => HttpBodyEventType::Trailers,
            HttpBodyEventKind::Suspend(_) => HttpBodyEventType::Suspend,
            HttpBodyEventKind::Padding(_) => HttpBodyEventType::Padding,
        }
    }

    pub fn kind(&self) -> &HttpBodyEventKind {
        &self.kind
    }

    pub fn push_promise_mut(&mut self) -> Option<&mut HttpPushEvent> {
        match &mut self.kind {
            HttpBodyEventKind::PushPromise(push) => Some(push),
            _ => None,
        }
    }

    pub fn into_kind(self) -> HttpBodyEventKind {
        let Self {
            kind,
            byte_event_registrations,
            ..
        } = self;
        if byte_event_registrations.capacity() == 0 {
            // A vector without capacity has no allocation and no registrations to drop.
            std::mem::forget(byte_event_registrations);
        } else {
            drop_byte_event_registrations(byte_event_registrations);
        }
        kind
    }

    pub fn as_body(&self) -> Option<&BufQueue> {
        match &self.kind {
            HttpBodyEventKind::Body(body) => Some(body),
            _ => None,
        }
    }

    pub fn as_body_mut(&mut self) -> Option<&mut BufQueue> {
        match &mut self.kind {
            HttpBodyEventKind::Body(body) => Some(body),
            _ => None,
        }
    }

    pub fn byte_event_registrations(&self) -> &[HttpByteEventRegistration] {
        &self.byte_event_registrations
    }

    pub fn byte_event_registrations_mut(&mut self) -> &mut Vec<HttpByteEventRegistration> {
        &mut self.byte_event_registrations
    }

    pub fn take_byte_event_registrations(&mut self) -> Vec<HttpByteEventRegistration> {
        std::mem::take(&mut self.byte_event_registrations)
    }
}

#[cfg(test)]
thread_local! {
    static REGISTRATION_DROPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn take_registration_drops_for_test() -> u64 {
    REGISTRATION_DROPS.with(|drops| drops.replace(0))
}

#[cold]
#[inline(never)]
fn drop_byte_event_registrations(registrations: Vec<HttpByteEventRegistration>) {
    #[cfg(test)]
    REGISTRATION_DROPS.with(|drops| drops.set(drops.get() + 1));
    drop(registrations);
}

/// Returns the mutable body buffer when `event` is a body event.
///
/// A free-function form of [`HttpBodyEvent::as_body_mut`].
pub fn as_body_ev(event: &mut HttpBodyEvent) -> Option<&mut BufQueue> {
    event.as_body_mut()
}
