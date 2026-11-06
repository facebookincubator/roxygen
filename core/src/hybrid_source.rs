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
    upstream: HttpSourceHolder,
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
            upstream,
        }
    }

    pub const fn filter_name(&self) -> &'static str {
        "HybridSource"
    }

    pub fn has_upstream(&self) -> bool {
        self.upstream.readable()
    }
}

impl HttpSource for HttpHybridSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        if let Some(header_event) = self.header_event.take() {
            Box::pin(std::future::ready(Ok(header_event)))
        } else {
            self.upstream.read_header_event()
        }
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.upstream.read_body_event(maximum)
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        if self.upstream.readable() {
            self.upstream.stop_reading(error);
        }
    }

    fn get_stream_id(&self) -> Option<StreamId> {
        self.upstream.get_stream_id()
    }

    fn ingress_completion(&self) -> Option<IngressCompletion> {
        self.upstream.ingress_completion()
    }

    fn set_read_timeout(&mut self, timeout: Duration) {
        self.upstream.set_read_timeout(timeout);
    }
}
