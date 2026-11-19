/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::time::Duration;

use crate::error::HttpError;
use crate::error::HttpErrorCode;
use crate::events::HeaderEventError;
use crate::events::HttpBodyEvent;
use crate::events::HttpHeaderEvent;
use crate::ids::StreamId;
use crate::ingress_completion::IngressCompletion;
use crate::message::HttpMessage;
use crate::source::HttpSource;
use crate::source::LocalSourceFuture;
use crate::source_holder::HttpSourceHolder;

/// Supplies a fixed header event before forwarding to an owned upstream source.
pub struct HttpHybridSource {
    header_event: Option<HttpHeaderEvent>,
    upstream: Option<HttpSourceHolder>,
}

impl HttpHybridSource {
    pub fn new(
        headers: HttpMessage,
        upstream: Option<HttpSourceHolder>,
    ) -> Result<Self, HeaderEventError> {
        let upstream = upstream.unwrap_or_default();
        let header_event = HttpHeaderEvent::new(headers, !upstream.readable())?;
        Ok(Self::from_header_event(header_event, upstream))
    }

    /// Prepends an owned event without changing its framing or callbacks.
    pub fn from_header_event(header_event: HttpHeaderEvent, upstream: HttpSourceHolder) -> Self {
        Self {
            header_event: Some(header_event),
            upstream: Some(upstream),
        }
    }

    /// Supplies only `header_event`, as [`Self::from_header_event`] does with an empty
    /// upstream, without allocating that holder.
    pub fn from_header_event_only(header_event: HttpHeaderEvent) -> Self {
        Self {
            header_event: Some(header_event),
            upstream: None,
        }
    }

    pub const fn filter_name(&self) -> &'static str {
        "HybridSource"
    }

    pub fn has_upstream(&self) -> bool {
        self.upstream
            .as_ref()
            .is_some_and(HttpSourceHolder::readable)
    }
}

impl HttpSource for HttpHybridSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        if let Some(header_event) = self.header_event.take() {
            Box::pin(std::future::ready(Ok(header_event)))
        } else if let Some(upstream) = &mut self.upstream {
            upstream.read_header_event()
        } else {
            Box::pin(std::future::ready(Err(
                HttpSourceHolder::unavailable_error(),
            )))
        }
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        match &mut self.upstream {
            Some(upstream) => upstream.read_body_event(maximum),
            None => Box::pin(std::future::ready(Err(
                HttpSourceHolder::unavailable_error(),
            ))),
        }
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        if let Some(upstream) = self
            .upstream
            .as_mut()
            .filter(|upstream| upstream.readable())
        {
            upstream.stop_reading(error);
        }
    }

    fn get_stream_id(&self) -> Option<StreamId> {
        self.upstream.as_ref()?.get_stream_id()
    }

    fn ingress_completion(&self) -> Option<IngressCompletion> {
        self.upstream.as_ref()?.ingress_completion()
    }

    fn set_read_timeout(&mut self, timeout: Duration) {
        if let Some(upstream) = &mut self.upstream {
            upstream.set_read_timeout(timeout);
        }
    }
}
