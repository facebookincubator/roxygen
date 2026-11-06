use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use crate::error::HttpError;
use crate::error::HttpErrorCode;
use crate::events::HttpBodyEvent;
use crate::events::HttpHeaderEvent;
use crate::ids::StreamId;
use crate::ingress_completion::IngressCompletion;

/// An owner-local future returned by an HTTP source.
///
/// The future is deliberately not `Send` and owns the state needed to finish
/// the operation. This permits owner-local timeout and stop handling while a
/// read is suspended without retaining a Rust borrow of the source object.
pub type LocalSourceFuture<T> = Pin<Box<dyn Future<Output = T> + 'static>>;

/// A worker-local, single-consumer producer of HTTP events.
pub trait HttpSource: 'static {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>>;

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>>;

    /// Stops an unfinished source. Implementations must complete this call on
    /// their owner and must not invoke it more than once.
    fn stop_reading(&mut self, error: Option<HttpErrorCode>);

    fn get_stream_id(&self) -> Option<StreamId> {
        None
    }

    /// Observes producer arrival independently of reads, when the source supports it.
    /// Obtain this before consuming the source; wrappers may retire it at EOM.
    fn ingress_completion(&self) -> Option<IngressCompletion> {
        None
    }

    fn set_read_timeout(&mut self, _timeout: Duration) {}
}
