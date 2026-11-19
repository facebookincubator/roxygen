/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! CONNECT response-head access and explicit stream abort.
//!
//! **Experimental.** The APIs in this module may change or be removed.
//!
//! [`ConnectResponseHeadExt`] exposes the final 2xx response head of an
//! established tunnel, and [`connect_rejection`] returns the
//! [`ConnectRejected`] cause of a connect that failed on a non-2xx head.
//! [`ConnectAbortExt`] resets a tunnel's stream with `CANCEL`, even after the
//! upload has ended. [`connect_with_reset_on_cancel`] and
//! [`connect_unique_with_reset_on_cancel`] reset the stream when a connect
//! ends before its final response head.
//!
//! The retained head is the `HttpMessage` the codec delivered, unchanged. A
//! common header name uses the case from the common-header table, and any
//! other name keeps the case the codec gave it. Match names case-insensitively.

use std::error::Error;
use std::panic::resume_unwind;
use std::time::Duration;

use roxygen_core::error::HttpError;
use roxygen_core::message::HttpMessage;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::worker::WorkerContext;
use roxygen_session::session::RequestReservation;

use crate::async_transport::HttpConnectAsyncTransport;
use crate::session::ConnectSession;
use crate::session::LocalConnectFuture;
use crate::stream::HttpConnectStream;
use crate::stream::RequestHeaderMap;

/// Access to the final 2xx CONNECT response head that an established
/// [`HttpConnectStream`] retains.
pub trait ConnectResponseHeadExt {
    /// The retained head, until it is taken.
    fn connect_response(&self) -> Option<&HttpMessage>;

    /// Takes the retained head. Later calls return `None`.
    fn take_connect_response(&mut self) -> Option<HttpMessage>;
}

impl ConnectResponseHeadExt for HttpConnectStream {
    fn connect_response(&self) -> Option<&HttpMessage> {
        self.connect_response.as_deref()
    }

    fn take_connect_response(&mut self) -> Option<HttpMessage> {
        self.connect_response.take().map(|head| *head)
    }
}

/// The final non-2xx CONNECT response that failed a connect.
///
/// It is the cause of the `CONNECT_ERROR` `HttpError` and displays the same
/// message.
#[derive(Debug, thiserror::Error)]
#[error("CONNECT to {authority} failed status={status}")]
pub struct ConnectRejected {
    authority: Box<str>,
    status: u16,
    response: Box<HttpMessage>,
}

impl ConnectRejected {
    pub(crate) fn new(authority: Box<str>, status: u16, response: Box<HttpMessage>) -> Self {
        Self {
            authority,
            status,
            response,
        }
    }

    /// The CONNECT authority.
    pub fn authority(&self) -> &str {
        &self.authority
    }

    /// The final response status.
    pub fn status_code(&self) -> u16 {
        self.status
    }

    /// The final response head. Its body was discarded.
    pub fn response(&self) -> &HttpMessage {
        &self.response
    }
}

/// The rejection that failed a connect, if `error` is one.
///
/// Only a final non-2xx head yields one. A peer reset, `READ_TIMEOUT`,
/// cancellation, session failure, or dial failure does not, even when its code
/// is also `CONNECT_ERROR`.
pub fn connect_rejection(error: &HttpError) -> Option<&ConnectRejected> {
    error.source()?.downcast_ref()
}

/// Resets the stream of an established CONNECT tunnel.
pub trait ConnectAbortExt {
    /// Resets the tunnel's stream with `CANCEL` and closes both directions.
    ///
    /// The reset is sent even after the upload has half-closed, and while the
    /// response is still streaming. Pending and later reads fail with
    /// `CANCEL`, deferred and later writes fail, and parked readers and
    /// writers wake. On HTTP/1.1 it starts the proxy session's drain, which
    /// ends the connection with a TCP reset, even after the upload has
    /// half-closed.
    ///
    /// The abort does not send a frame once the proxy's END_STREAM has arrived
    /// and the upload EOM was sent. It then discards the unread bytes. It also
    /// does not send a frame when either side already reset the stream, or
    /// when it is repeated. After a proxy reset, the transport's reads keep
    /// reporting the proxy's error code.
    ///
    /// Call it from owner-local code. Inside a session callout, such as an
    /// egress source callback, the session cannot reset the stream. The call
    /// then uses up the stream's one abort and only closes both directions
    /// locally. While the upload is open, the failed upload resets the
    /// stream. Otherwise the stream stays open until the proxy ends it.
    ///
    /// # Panics
    ///
    /// If a lifecycle observer or a waker that the abort runs panics, the
    /// panic resumes after both directions are closed.
    fn abort(&mut self);
}

impl ConnectAbortExt for HttpConnectStream {
    fn abort(&mut self) {
        if let Some(payload) = self.abort_with_reset() {
            resume_unwind(payload);
        }
    }
}

impl ConnectAbortExt for HttpConnectAsyncTransport {
    fn abort(&mut self) {
        self.abort_with_reset();
    }
}

/// Like [`HttpConnectStream::connect`], but resets the CONNECT stream with
/// `CANCEL` if the connect ends before the final response head.
///
/// The reset happens if the connect future is cancelled, fails, or is
/// dropped after the request is committed and before the final response head
/// is read. This holds even if the session has already marked the stream
/// upgraded.
///
/// The reset covers only the connect future. Once it returns the stream,
/// dropping the stream closes it gracefully. To discard an established stream
/// that was never handed on, reset it with [`ConnectAbortExt::abort`].
///
/// # Panics
///
/// If a lifecycle observer panics inside the reset, the teardown still
/// finishes and the panic resumes from the drop. If the drop runs while the
/// thread is already unwinding, the panic is discarded.
#[expect(
    clippy::too_many_arguments,
    reason = "mirrors HttpConnectStream::connect with the reset-on-cancel option"
)]
pub fn connect_with_reset_on_cancel<S: ConnectSession>(
    context: &WorkerContext,
    session: &S,
    reservation: RequestReservation,
    authority: &str,
    timeout: Duration,
    connect_headers: RequestHeaderMap,
    egress_buffer_size: u32,
    cancellation: CancellationToken,
) -> LocalConnectFuture<Result<HttpConnectStream, HttpError>> {
    HttpConnectStream::connect_inner(
        context,
        session,
        reservation,
        authority,
        timeout,
        connect_headers,
        egress_buffer_size,
        cancellation,
        true,
    )
}

/// Like [`HttpConnectStream::connect_unique`], with the reset-on-cancel
/// teardown of [`connect_with_reset_on_cancel`]. The session still drains.
#[expect(
    clippy::too_many_arguments,
    reason = "mirrors HttpConnectStream::connect_unique with the reset-on-cancel option"
)]
pub fn connect_unique_with_reset_on_cancel<S: ConnectSession>(
    context: &WorkerContext,
    session: S,
    reservation: RequestReservation,
    authority: &str,
    timeout: Duration,
    connect_headers: RequestHeaderMap,
    egress_buffer_size: u32,
    cancellation: CancellationToken,
) -> LocalConnectFuture<Result<HttpConnectStream, HttpError>> {
    HttpConnectStream::connect_unique_inner(
        context,
        session,
        reservation,
        authority,
        timeout,
        connect_headers,
        egress_buffer_size,
        cancellation,
        true,
    )
}
