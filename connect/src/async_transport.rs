/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! `HttpConnectAsyncTransport`, the Tokio adapter over a CONNECT stream.
//!
//! One body read is stored across `poll_read` calls. A write that closes the
//! egress window holds back the next write, flush, or shutdown until its
//! bytes are processed.

use std::cell::Cell;
use std::cell::RefCell;
use std::io;
use std::io::IoSlice;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::rc::Weak;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

use roxygen_core::buf_queue::BufQueue;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpBodyEventKind;
use roxygen_core::ids::StreamId;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::stream_source::FlowControlState;
use roxygen_core::stream_source::HttpStreamSourceSharedCallback;
use roxygen_runtime::panic::discard_panic;
use roxygen_runtime::worker::WorkerContext;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;

use crate::stream::HttpConnectStream;

/// A tokio `AsyncRead` and `AsyncWrite` over a CONNECT tunnel. It is
/// owner-local and `!Send`.
pub struct HttpConnectAsyncTransport {
    stream: HttpConnectStream,
    writes: Rc<WriteState>,
    egress_offset: u64,
    read: ReadState,
}

#[derive(Default)]
struct ReadState {
    stored: Option<LocalSourceFuture<Result<HttpBodyEvent, HttpError>>>,
    remainder: Option<Remainder>,
    error: Option<HttpError>,
    eof: bool,
    waker: Option<Waker>,
}

struct Remainder {
    body: BufQueue,
    eom: bool,
}

/// The outstanding write that closed the egress window. There is at most one.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum DeferredWrite {
    #[default]
    None,
    /// Completes once the processed egress offset reaches `end`.
    Pending { end: u64 },
    /// Failed by `error_writes`. The next writer poll returns the error.
    Failed,
}

/// Write-side state that the egress source's callbacks update.
#[derive(Default)]
struct WriteState {
    flushed_offset: Cell<u64>,
    deferred: Cell<DeferredWrite>,
    waker: RefCell<Option<Waker>>,
}

impl WriteState {
    fn wake(&self) {
        let waker = self.waker.borrow_mut().take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Fails the deferred write, if any, and wakes its writer.
    fn error_writes(&self) {
        if let Some(waker) = self.fail_deferred() {
            waker.wake();
        }
    }

    /// Like `error_writes`, but returns the parked writer's waker instead of
    /// waking it.
    fn fail_deferred(&self) -> Option<Waker> {
        if !self.is_pending() {
            return None;
        }
        self.deferred.set(DeferredWrite::Failed);
        self.waker.borrow_mut().take()
    }

    fn is_pending(&self) -> bool {
        matches!(self.deferred.get(), DeferredWrite::Pending { .. })
    }

    /// Waits for the deferred write, if any, and reports its result once.
    fn poll_deferred(&self, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.deferred.get() {
            DeferredWrite::None => return Poll::Ready(Ok(())),
            DeferredWrite::Failed => {
                self.deferred.set(DeferredWrite::None);
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "Socket error",
                )));
            }
            DeferredWrite::Pending { .. } => {}
        }
        let keep = self
            .waker
            .borrow()
            .as_ref()
            .is_some_and(|stored| stored.will_wake(context.waker()));
        if keep {
            return Poll::Pending;
        }
        // Cloning or dropping a waker can run caller code that reaches
        // `bytes_processed` or `source_complete`, which borrow `waker`.
        let waker = context.waker().clone();
        drop(self.waker.replace(Some(waker)));
        if self.is_pending() {
            return Poll::Pending;
        }
        self.poll_deferred(context)
    }
}

impl HttpStreamSourceSharedCallback for WriteState {
    fn bytes_processed(&self, _id: StreamId, amount: usize, _to_acknowledge: usize) {
        let flushed = self.flushed_offset.get().saturating_add(amount as u64);
        self.flushed_offset.set(flushed);
        if let DeferredWrite::Pending { end } = self.deferred.get()
            && flushed >= end
        {
            self.deferred.set(DeferredWrite::None);
            self.wake();
        }
    }

    fn source_complete(&self, _id: StreamId, error: Option<HttpError>) {
        if error.is_some() {
            self.error_writes();
        }
    }
}

fn not_writable() -> io::Error {
    io::Error::new(
        io::ErrorKind::NotConnected,
        "write on a closed or failed transport",
    )
}

fn read_error(error: &HttpError) -> io::Error {
    io::Error::other(error.clone())
}

/// Moves up to `buf.remaining()` bytes from the front of `body` into `buf`.
fn fill(buf: &mut ReadBuf<'_>, body: &mut BufQueue) {
    let mut copied = 0;
    for segment in body.segments() {
        let length = segment.len().min(buf.remaining());
        if length == 0 {
            break;
        }
        buf.put_slice(&segment[..length]);
        copied += length;
    }
    body.trim_start_at_most(copied);
}

impl HttpConnectAsyncTransport {
    /// Takes the stream and installs the adapter as its egress callback.
    pub fn new(context: &WorkerContext, stream: HttpConnectStream) -> Self {
        assert_eq!(
            context.worker_id(),
            stream.worker_id(),
            "a CONNECT adapter must run on its stream's worker"
        );
        let writes = Rc::new(WriteState::default());
        let callback: Weak<WriteState> = Rc::downgrade(&writes);
        let callback: Weak<dyn HttpStreamSourceSharedCallback> = callback;
        stream.set_http_stream_source_callback(Some(callback));
        Self {
            stream,
            writes,
            egress_offset: 0,
            read: ReadState::default(),
        }
    }

    /// The proxy session's local address.
    pub fn local_address(&self) -> &SocketAddress {
        self.stream.local_address()
    }

    /// The tunnel's `X-Connected-To` peer, or the proxy session's peer address.
    pub fn peer_address(&self) -> &SocketAddress {
        self.stream.peer_address()
    }

    /// Ends reads with EOF and stops the CONNECT response source.
    pub fn shutdown_read(&mut self) {
        drop(self.read.stored.take());
        self.read.remainder = None;
        self.read.eof = true;
        self.stream.shutdown_read();
        if let Some(waker) = self.read.waker.take() {
            waker.wake();
        }
    }

    /// Ends the write side immediately. Without a deferred write it sends EOM
    /// if the stream is writable. Otherwise it fails the deferred write and
    /// aborts the egress with `CANCEL`.
    pub fn shutdown_write_now(&mut self) {
        if !self.writes.is_pending() {
            self.shutdown_write();
            return;
        }
        self.writes.error_writes();
        self.stream.egress_abort(HttpErrorCode::CANCEL);
    }

    /// Sends EOM if the stream is writable, then calls
    /// [`shutdown_read`](Self::shutdown_read).
    pub fn close(&mut self) {
        self.shutdown_write();
        self.shutdown_read();
    }

    /// Calls [`shutdown_write_now`](Self::shutdown_write_now), then
    /// [`shutdown_read`](Self::shutdown_read).
    pub fn close_now(&mut self) {
        self.shutdown_write_now();
        self.shutdown_read();
    }

    fn shutdown_write(&mut self) {
        if self.stream.can_write() {
            self.stream.egress_eom();
        }
    }

    /// Resets the stream with `CANCEL`, even after egress EOM. Reads keep a
    /// source error that arrived first, such as a proxy reset that a read has
    /// not reported yet. Deferred writes fail before the reset discards the
    /// egress bytes that would otherwise complete them. After the reset, the
    /// parked reader wakes, then the parked writer. The first panic from the
    /// reset or a wake resumes at the end.
    pub(crate) fn abort_with_reset(&mut self) {
        drop(self.read.stored.take());
        self.read.remainder = None;
        if self.read.error.is_none() {
            let terminal = self
                .stream
                .with_ingress_source(|ingress| ingress.ingress_completion())
                .and_then(|completion| completion.snapshot());
            self.read.error = Some(match terminal {
                Some(Err(error)) => error,
                _ => HttpError::with_message(HttpErrorCode::CANCEL, b"CONNECT tunnel aborted"),
            });
        }
        let writer = self.writes.fail_deferred();
        let mut panic = self.stream.abort_with_reset();
        for waker in [self.read.waker.take(), writer].into_iter().flatten() {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| waker.wake())) {
                match panic {
                    Some(_) => discard_panic(payload),
                    None => panic = Some(payload),
                }
            }
        }
        if let Some(payload) = panic {
            resume_unwind(payload);
        }
    }

    /// Submits one write. A write that closes the egress window becomes the
    /// deferred write.
    fn poll_submit(
        &mut self,
        context: &mut Context<'_>,
        length: usize,
        body: impl FnOnce() -> io::Result<BufQueue>,
    ) -> Poll<io::Result<usize>> {
        if !self.stream.can_write() {
            return Poll::Ready(Err(not_writable()));
        }
        if let Err(error) = std::task::ready!(self.writes.poll_deferred(context)) {
            return Poll::Ready(Err(error));
        }
        // The waker that `poll_deferred` replaced can complete the egress source
        // as it drops.
        if !self.stream.can_write() {
            return Poll::Ready(Err(not_writable()));
        }
        if length == 0 {
            return Poll::Ready(Ok(0));
        }
        let body = body()?;
        self.egress_offset = self.egress_offset.saturating_add(length as u64);
        if self.stream.egress_body(body) == FlowControlState::Closed {
            self.writes.deferred.set(DeferredWrite::Pending {
                end: self.egress_offset,
            });
        }
        Poll::Ready(Ok(length))
    }

    /// Polls the stored body read, starting one if needed, and copies its
    /// bytes into `buf`. Bytes that do not fit become the remainder.
    fn poll_read_body(
        &mut self,
        context: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            if self.read.eof || !self.stream.can_read() {
                self.read.eof = true;
                return Poll::Ready(Ok(()));
            }
            if self.read.stored.is_none() {
                let maximum = u32::try_from(buf.remaining()).unwrap_or(u32::MAX);
                self.read.stored = Some(self.stream.with_ingress_source(|ingress| {
                    ingress.set_read_timeout(Duration::ZERO);
                    ingress.read_body_event(maximum)
                }));
            }
            let stored = self
                .read
                .stored
                .as_mut()
                .expect("should hold the read it just stored");
            let Poll::Ready(result) = stored.as_mut().poll(context) else {
                self.read.waker = Some(context.waker().clone());
                return Poll::Pending;
            };
            self.read.stored = None;
            let event = match result {
                Ok(event) => event,
                Err(error) => {
                    let failure = read_error(&error);
                    self.read.error = Some(error);
                    return Poll::Ready(Err(failure));
                }
            };
            let eom = event.eom();
            match event.into_kind() {
                HttpBodyEventKind::Body(mut body) if !body.is_empty() => {
                    fill(buf, &mut body);
                    if !body.is_empty() {
                        self.read.remainder = Some(Remainder { body, eom });
                    } else if eom {
                        self.read.eof = true;
                    }
                    return Poll::Ready(Ok(()));
                }
                // CONNECT allows only body events. Other events, including the
                // suspend that precedes the first wait, are ignored.
                _ if eom => {
                    self.read.eof = true;
                    return Poll::Ready(Ok(()));
                }
                _ => {}
            }
        }
    }
}

impl AsyncRead for HttpConnectAsyncTransport {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if let Some(error) = &this.read.error {
            return Poll::Ready(Err(read_error(error)));
        }
        if let Some(remainder) = this.read.remainder.as_mut() {
            fill(buf, &mut remainder.body);
            if remainder.body.is_empty() {
                let eom = remainder.eom;
                this.read.remainder = None;
                this.read.eof |= eom;
            }
            return Poll::Ready(Ok(()));
        }
        this.poll_read_body(context, buf)
    }
}

impl AsyncWrite for HttpConnectAsyncTransport {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().poll_submit(context, buf.len(), || {
            BufQueue::copy_from_slice(buf, buf.len()).map_err(io::Error::other)
        })
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let length = bufs
            .iter()
            .fold(0_usize, |total, slice| total.saturating_add(slice.len()));
        self.get_mut().poll_submit(context, length, || {
            let mut body = BufQueue::with_segment_capacity(length, bufs.len());
            bufs.iter()
                .filter(|slice| !slice.is_empty())
                .try_for_each(|slice| body.push_copy(slice))
                .map_err(io::Error::other)?;
            Ok(body)
        })
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.writes.poll_deferred(context)
    }

    /// Sends EOM immediately, then waits for the deferred write, if any.
    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.shutdown_write();
        this.writes.poll_deferred(context)
    }
}

impl Drop for HttpConnectAsyncTransport {
    fn drop(&mut self) {
        // The stored read is released before the stream closes its source.
        drop(self.read.stored.take());
    }
}
