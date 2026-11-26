/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The transport seam: exactly the connection, stream and pull-sink operations the session uses.
//!
//! The session is written against these traits; the driver's [`Conn`], [`StreamMut`] and
//! [`SendSink`] implement them, and the tests implement them in memory. Dispatch is static, except
//! for the session handle, which other threads hold as a trait object as they would a `Waker`.

use std::fmt::Debug;
use std::task::Waker;
use std::time::Instant;

use bytes::Bytes;
use mvfstr_driver::Conn;
use mvfstr_driver::ConnHandle;
use mvfstr_driver::SendSink;
use mvfstr_driver::SinkError;
use mvfstr_proto::AppCode;
use mvfstr_proto::BufQueue;
use mvfstr_proto::Connection;
use mvfstr_proto::CreditDue;
use mvfstr_proto::Dir;
use mvfstr_proto::OpenError;
use mvfstr_proto::SendError;
use mvfstr_proto::SendOrder;
use mvfstr_proto::StreamError;
use mvfstr_proto::StreamId;
use mvfstr_proto::StreamMut;

/// The connection operations of the session.
pub(crate) trait Transport {
    type Stream<'a>: TransportStream
    where
        Self: 'a;

    /// Returns the time the driver read when the current phase began.
    fn now(&self) -> Instant;

    /// Opens a unidirectional stream at `order` in the wire order.
    fn open_uni(&mut self, order: SendOrder) -> Result<StreamId, OpenError>;

    /// Gives mutable access to an open stream.
    fn stream(&mut self, id: StreamId) -> Result<Self::Stream<'_>, StreamError>;

    /// Returns a waker that puts stream `id` back on the pull-ready list.
    fn stream_waker(&self, id: StreamId) -> Waker;

    /// Returns a handle, usable from any thread, that makes the driver run the session's
    /// `poll_session`.
    fn session_handle(&self) -> Box<dyn SessionWake>;

    /// Closes the connection with an application CONNECTION_CLOSE.
    fn close(&mut self, code: AppCode, reason: Bytes);

    /// Stops accepting new streams and lets open ones finish.
    fn shutdown(&mut self);
}

/// A handle that brings the driver to the session's `poll_session`, from any thread.
pub(crate) trait SessionWake: Debug + Send + Sync + 'static {
    /// Requests the wake. Returns false if it was dropped, as a `ConnHandle` wake is when the
    /// driver's command queue is full.
    fn wake(&self) -> bool;
}

/// The stream operations of the session.
pub(crate) trait TransportStream {
    /// Returns the in-order received bytes not yet consumed.
    fn readable(&self) -> &BufQueue;

    /// Returns whether the peer's FIN arrived with every byte before it. It can be true before the
    /// stream's `Finished` edge is delivered.
    fn peer_finished(&self) -> bool;

    /// Consumes `n` readable bytes and returns their flow-control credit.
    fn consume(&mut self, n: usize) -> Result<CreditDue, StreamError>;

    /// Stages the longest prefix of `data` the allowance accepts.
    fn send(&mut self, data: &mut Bytes) -> Result<usize, SendError>;

    /// Abandons the send half with RESET_STREAM.
    fn reset(&mut self, code: AppCode) -> Result<(), SendError>;

    /// Sends STOP_SENDING to the peer and drops further ingress.
    fn stop_sending(&mut self, code: AppCode) -> Result<(), StreamError>;

    /// Moves the stream to a new place in the wire order.
    fn set_order(&mut self, order: SendOrder);
}

/// The pull sink `poll_send` writes a stream's data to.
pub(crate) trait PullSink {
    /// Returns how many bytes the stream accepts now.
    fn allowance(&self) -> usize;

    /// Stages all of `data`, or fails without staging any of it.
    fn push(&mut self, data: Bytes) -> Result<(), SinkError>;

    /// Stages a frame header and a shared payload without copying the payload. It stages both or
    /// fails without staging either.
    fn push_parts(&mut self, header: Bytes, shared: Bytes) -> Result<(), SinkError>;

    /// Ends the stream after the staged bytes.
    fn finish(&mut self) -> Result<(), SinkError>;
}

impl Transport for Conn<'_> {
    type Stream<'a>
        = StreamMut<'a>
    where
        Self: 'a;

    fn now(&self) -> Instant {
        Conn::now(self)
    }

    fn open_uni(&mut self, order: SendOrder) -> Result<StreamId, OpenError> {
        Connection::open_stream(self, Dir::Uni, order)
    }

    fn stream(&mut self, id: StreamId) -> Result<StreamMut<'_>, StreamError> {
        Connection::stream(self, id)
    }

    fn stream_waker(&self, id: StreamId) -> Waker {
        Conn::stream_waker(self, id)
    }

    fn session_handle(&self) -> Box<dyn SessionWake> {
        Box::new(self.handle())
    }

    fn close(&mut self, code: AppCode, reason: Bytes) {
        let now = Conn::now(self);
        Connection::close(self, now, code, reason);
    }

    fn shutdown(&mut self) {
        let now = Conn::now(self);
        Connection::shutdown(self, now);
    }
}

impl SessionWake for ConnHandle {
    fn wake(&self) -> bool {
        ConnHandle::wake(self).is_ok()
    }
}

impl TransportStream for StreamMut<'_> {
    fn readable(&self) -> &BufQueue {
        StreamMut::readable(self)
    }

    fn peer_finished(&self) -> bool {
        StreamMut::peer_finished(self)
    }

    fn consume(&mut self, n: usize) -> Result<CreditDue, StreamError> {
        StreamMut::consume(self, n)
    }

    fn send(&mut self, data: &mut Bytes) -> Result<usize, SendError> {
        StreamMut::send(self, data)
    }

    fn reset(&mut self, code: AppCode) -> Result<(), SendError> {
        StreamMut::reset(self, code)
    }

    fn stop_sending(&mut self, code: AppCode) -> Result<(), StreamError> {
        StreamMut::stop_sending(self, code)
    }

    fn set_order(&mut self, order: SendOrder) {
        StreamMut::set_order(self, order);
    }
}

impl PullSink for SendSink<'_> {
    fn allowance(&self) -> usize {
        SendSink::allowance(self)
    }

    fn push(&mut self, data: Bytes) -> Result<(), SinkError> {
        SendSink::push(self, data)
    }

    fn push_parts(&mut self, header: Bytes, shared: Bytes) -> Result<(), SinkError> {
        SendSink::push_parts(self, header, shared)
    }

    fn finish(&mut self) -> Result<(), SinkError> {
        SendSink::finish(self)
    }
}
