/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Roxygen-only CONNECT response-head access (`DIV-011`) and explicit stream
//! abort (`DIV-023`).
//!
//! **Experimental.** This module prototypes proposed Proxygen APIs that frozen
//! Proxygen does not have: `HTTPConnectStream::getConnectResponse` and
//! `releaseConnectResponse` plus a typed `HTTPConnectError` (`DIV-011`), and an
//! `HTTPConnectAsyncTransport::closeWithReset` that resets the CONNECT stream
//! with `CANCEL` also after its egress EOM, through a stream-scoped reset on
//! `HTTPSessionContext`, plus an opt-in reset-on-cancel parameter of
//! `HTTPConnectStream::connect` and `connectUnique` (`DIV-023`). They may
//! change or be removed when Proxygen decides on those APIs; `DIVERGENCES.md`
//! `DIV-011` and `DIV-023` record the divergences and their retirement.
//!
//! The retained head is the `HttpMessage` the codec delivered, unchanged: a
//! common header name takes the common-header table's spelling, and any other
//! name keeps the codec's spelling. Match names case-insensitively.

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
    /// The retained head, until it is taken (`getConnectResponse`).
    fn connect_response(&self) -> Option<&HttpMessage>;

    /// Takes the retained head; later calls return `None`
    /// (`releaseConnectResponse`).
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

/// The final non-2xx CONNECT response that failed a connect
/// (`HTTPConnectError`). It rides as the cause of the `CONNECT_ERROR`
/// `HttpError`, whose message it displays.
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

    /// The CONNECT authority (`getAuthority`).
    pub fn authority(&self) -> &str {
        &self.authority
    }

    /// The final response status (`getStatusCode`).
    pub fn status_code(&self) -> u16 {
        self.status
    }

    /// The final response head; its body was discarded (`getResponse`).
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

/// Reset of an established CONNECT tunnel's stream (`DIV-023`).
pub trait ConnectAbortExt {
    /// Resets the tunnel's stream with `CANCEL` (`closeWithReset`), also after
    /// the upload half-closed and while the response is still streaming, and
    /// closes both directions: pending and later reads fail with `CANCEL`,
    /// deferred and later writes fail, and parked readers and writers wake.
    /// On HTTP/1.1 it starts the proxy session's drain, which ends the
    /// connection with a TCP reset, also after the upload half-closed.
    ///
    /// A tunnel whose proxy END_STREAM arrived and whose upload EOM was sent
    /// gets no frame; its unread bytes are discarded. A tunnel that either
    /// side already reset gets no frame, nor does a repeated abort; a proxy
    /// reset keeps its code for the transport's reads.
    ///
    /// Call it from owner-local code. From a session callout, such as an
    /// egress source callback, the session cannot reset the stream, and the
    /// call uses up the stream's one abort: it only closes both directions
    /// locally, which resets the stream through the failed upload while the
    /// upload is open, and otherwise leaves the stream open until the proxy
    /// ends it. A panic of a lifecycle observer or a waker that the abort
    /// runs resumes after both directions are closed.
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

/// [`HttpConnectStream::connect`] whose teardown resets the CONNECT stream
/// with `CANCEL` when it is cancelled, fails, or is dropped after the request
/// committed and before the final response head was read, also when the
/// session already marked the stream upgraded (`DIV-023`).
///
/// The reset covers only the connect future. Once it returns the stream,
/// dropping it closes gracefully; a caller that discards an established
/// stream it never handed on resets it with [`ConnectAbortExt::abort`].
///
/// If a lifecycle observer panics inside the reset, the teardown still
/// finishes and the panic resumes from the drop, unless the drop runs while
/// the thread unwinds, which discards it.
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

/// [`HttpConnectStream::connect_unique`] with the teardown of
/// [`connect_with_reset_on_cancel`]; the session still drains (`DIV-023`).
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
