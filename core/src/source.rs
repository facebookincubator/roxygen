/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
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

    /// Roxygen-only, default `false` (`DIV-039`): whether this source serves body
    /// reads through [`Self::poll_body_event`], polled in place, instead of the boxed
    /// future [`Self::read_body_event`] returns, as tokio's `is_write_vectored`
    /// selects `poll_write_vectored`. [`HttpSourceHolder`] asks once per read, when the
    /// read starts, and the sessions' exchange wrapper forwards it, so a response source
    /// that answers `true` costs no allocation per body read.
    ///
    /// [`HttpSourceHolder`]: crate::source_holder::HttpSourceHolder
    fn polls_body_events(&self) -> bool {
        false
    }

    /// Polls for the next body event of at most `maximum` bytes, with the meaning of a
    /// [`Self::read_body_event`] future's poll: `Pending` registers `context`'s waker,
    /// and a terminal event or an error ends the source's reads. Called only while
    /// [`Self::polls_body_events`] is `true`, from inside a read its consumer started;
    /// a read dropped while pending just stops polling, and the next read may ask for a
    /// different `maximum`. The holder's state is borrowed during the call, so the source
    /// must not use the holder reading it: a read started from inside the call fails with
    /// `INVALID_STATE_TRANSITION`, and a call that stops, clears, replaces, or releases
    /// its source panics. The default refuses with `INVALID_STATE_TRANSITION`.
    fn poll_body_event(
        &mut self,
        _context: &mut Context<'_>,
        _maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        Poll::Ready(Err(HttpError::with_message(
            HttpErrorCode::INVALID_STATE_TRANSITION,
            b"HTTP source does not poll body events",
        )))
    }
}
