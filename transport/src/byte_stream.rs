/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::any::Any;
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::rc::Weak;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::task::ready;
use std::thread::ThreadId;
use std::time::Duration;
use std::time::Instant;

use roxygen_core::buf_queue::BufQueue;
use roxygen_core::byte_event::ByteEventType;
use roxygen_core::byte_event::HttpByteEvent;
use roxygen_core::byte_event::HttpByteEventRegistration;
pub use roxygen_core::connection::TransportEndpoint;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::message::SocketAddress;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::cancellation::CancellationWakeRegistration;
use roxygen_runtime::host::Host;
use roxygen_runtime::thread::current_id;
use roxygen_runtime::wait::TimedBaton;
use roxygen_runtime::wait::TimedWait;
use roxygen_runtime::wait::WaitStatus;
use roxygen_runtime::worker::WorkerCleanupRegistration;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerGuard;
use roxygen_runtime::worker::WorkerHandle;
use roxygen_runtime::worker::WorkerId;
use roxygen_runtime::worker::WorkerState;
use thiserror::Error;

use crate::byte_event::ByteEventWriteFlags;
use crate::established::AdoptedIoCompletion;
use crate::established::AdoptedIoCompletionError;

pub type LocalTransportFuture<T> = Pin<Box<dyn Future<Output = T> + 'static>>;

/// Error naming the transport retirement steps that could not be confirmed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
pub enum TransportRetirementError {
    #[error("transport I/O value destruction was not confirmed")]
    IoDestructionUnconfirmed,
    #[error("transport close-scope retirement was not confirmed")]
    DrainRetirementUnconfirmed,
    #[error("transport I/O value and close-scope retirement were not confirmed")]
    BothUnconfirmed,
}

/// Observes when a transport's I/O value and close future have been dropped.
///
/// Success means the complete I/O value, its owned fields, the registered close
/// future and its captures have all been dropped. It does not report whether
/// the I/O succeeded or reached the peer, and it does not cover the TLS
/// background task. With zero-copy enabled, the duplicate socket descriptor
/// that receives completions can stay open after success. The original worker
/// and reactor must keep running. This value does not own the I/O or the
/// worker, and dropping it neither starts nor cancels cleanup.
#[derive(Clone)]
pub struct TransportRetirement {
    completion: AdoptedIoCompletion,
}

impl TransportRetirement {
    pub(crate) fn new(completion: AdoptedIoCompletion) -> Self {
        Self { completion }
    }

    /// Waits for both the I/O value and the close future, even when one of them
    /// is unconfirmed.
    pub fn wait_retired(
        &self,
    ) -> impl Future<Output = Result<(), TransportRetirementError>> + Send + 'static {
        let completion = self.completion.wait_retired();
        async move {
            completion.await.map_err(|error| match error {
                AdoptedIoCompletionError::IoDestructionUnconfirmed => {
                    TransportRetirementError::IoDestructionUnconfirmed
                }
                AdoptedIoCompletionError::DrainRetirementUnconfirmed => {
                    TransportRetirementError::DrainRetirementUnconfirmed
                }
                AdoptedIoCompletionError::BothUnconfirmed => {
                    TransportRetirementError::BothUnconfirmed
                }
            })
        }
    }
}

pub(crate) type TransportPanic = Box<dyn Any + Send>;

pub(crate) fn retain_transport_panic(
    first: &mut Option<TransportPanic>,
    next: Option<TransportPanic>,
) {
    if first.is_none() {
        *first = next;
    } else if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(next))) {
        roxygen_runtime::panic::discard_panic(payload);
    }
}

pub(crate) fn finish_transport_panic(panic: Option<TransportPanic>, unwinding: bool) {
    if let Some(payload) = panic {
        if unwinding {
            if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
                roxygen_runtime::panic::discard_panic(secondary);
            }
        } else {
            resume_unwind(payload);
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerIdentity {
    encoded: Box<[u8]>,
}

impl PeerIdentity {
    pub fn new(encoded: impl AsRef<[u8]>) -> Self {
        Self {
            encoded: encoded.as_ref().to_vec().into_boxed_slice(),
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.encoded
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ByteStreamLimits {
    maximum_read_size: usize,
    maximum_write_size: usize,
    maximum_byte_event_registrations: usize,
}

impl ByteStreamLimits {
    pub fn new(
        maximum_read_size: usize,
        maximum_write_size: usize,
        maximum_byte_event_registrations: usize,
    ) -> Result<Self, HttpError> {
        if maximum_read_size == 0
            || maximum_write_size == 0
            || maximum_byte_event_registrations == 0
        {
            return Err(HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"byte-stream limits must be non-zero",
            ));
        }
        Ok(Self {
            maximum_read_size,
            maximum_write_size,
            maximum_byte_event_registrations,
        })
    }

    pub const fn maximum_read_size(self) -> usize {
        self.maximum_read_size
    }

    pub const fn maximum_write_size(self) -> usize {
        self.maximum_write_size
    }

    pub const fn maximum_byte_event_registrations(self) -> usize {
        self.maximum_byte_event_registrations
    }
}

#[derive(Debug)]
pub struct TransportRead {
    data: BufQueue,
    eof: bool,
    /// The read drained its stream ([`OwnedReader::is_drained`]).
    drained: bool,
}

impl TransportRead {
    /// Builds the result of an external `ByteStreamTransport::read`.
    ///
    /// `data` must hold at most the `maximum` passed to `read`. For a non-zero
    /// `maximum`, `data` may be empty only when `eof` is set. These rules are
    /// not checked. The session reads again immediately after an empty result
    /// without `eof`, so a transport that returns one while idle spins instead
    /// of reaching its read timeout.
    pub fn new(data: BufQueue, eof: bool) -> Self {
        Self::from_io(data, eof)
    }

    pub fn bytes_read(&self) -> usize {
        self.data.chain_length()
    }

    pub const fn is_eof(&self) -> bool {
        self.eof
    }

    pub fn data(&self) -> &BufQueue {
        &self.data
    }

    pub fn into_data(self) -> BufQueue {
        self.data
    }

    fn empty() -> Self {
        Self::from_io(BufQueue::new(0), false)
    }

    fn eof() -> Self {
        Self::from_io(BufQueue::new(0), true)
    }

    pub(crate) fn from_io(data: BufQueue, eof: bool) -> Self {
        Self {
            data,
            eof,
            drained: false,
        }
    }

    /// Marks a backend read that drained its stream (`PolledReader::poll_read`).
    pub(crate) fn with_drained(mut self, drained: bool) -> Self {
        self.drained = drained;
        self
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriteInfo {
    pub bytes_written: usize,
}

#[derive(Debug, Error)]
#[error("{error} after writing {bytes_written} bytes")]
pub struct TransportWriteError {
    error: HttpError,
    bytes_written: usize,
}

impl TransportWriteError {
    pub fn new(error: HttpError, bytes_written: usize) -> Self {
        Self {
            error,
            bytes_written,
        }
    }

    pub const fn code(&self) -> HttpErrorCode {
        self.error.code()
    }

    pub fn error(&self) -> &HttpError {
        &self.error
    }

    pub const fn bytes_written(&self) -> usize {
        self.bytes_written
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ByteEventCapability {
    Unsupported,
    TransportEvents,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ByteEventRegistrationError {
    #[error("transport does not support byte events")]
    Unsupported,
    #[error("transport is closed")]
    Closed,
    #[error("transport byte-event registration capacity is exhausted")]
    CapacityExhausted,
    #[error("transport was used from a different worker context")]
    WrongWorker,
}

pub trait ByteStreamTransport: 'static {
    fn read(
        &self,
        context: &WorkerContext,
        maximum: usize,
        allocation_size: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<TransportRead, HttpError>>;

    fn write(
        &self,
        context: &WorkerContext,
        data: BufQueue,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>>;

    fn write_with_byte_events(
        &self,
        context: &WorkerContext,
        data: BufQueue,
        _flags: ByteEventWriteFlags,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        self.write(context, data, timeout, cancellation)
    }

    fn shutdown_write(&self, context: &WorkerContext) -> Result<(), HttpError>;

    fn close(&self, context: &WorkerContext) -> Result<(), HttpError>;

    fn close_with_reset(&self, context: &WorkerContext) -> Result<(), HttpError>;

    fn local_address(&self, context: &WorkerContext) -> Result<SocketAddress, HttpError>;

    fn peer_address(&self, context: &WorkerContext) -> Result<SocketAddress, HttpError>;

    /// Returns the actual local endpoint, including Unix-domain addresses.
    fn local_endpoint(&self, context: &WorkerContext) -> Result<TransportEndpoint, HttpError> {
        self.local_address(context).map(TransportEndpoint::Socket)
    }

    /// Returns the actual peer endpoint, not the logical HTTP origin.
    fn peer_endpoint(&self, context: &WorkerContext) -> Result<TransportEndpoint, HttpError> {
        self.peer_address(context).map(TransportEndpoint::Socket)
    }

    fn peer_identity(&self, context: &WorkerContext) -> Result<Option<PeerIdentity>, HttpError>;

    fn byte_event_capability(
        &self,
        context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError>;

    fn register_byte_event(
        &self,
        context: &WorkerContext,
        registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError>;

    /// Acquires the transport's native write queue, which writes at most
    /// `maximum_write_size` bytes per submission and is untimed, so it is
    /// offered only for a zero `timeout`. `None` keeps the single-write API,
    /// including its timeout.
    ///
    /// The queue observes `cancellation` when it is polled. A cancellation
    /// requested from another task wakes the task that last polled the
    /// outstanding write.
    fn write_queue(
        &self,
        _context: &WorkerContext,
        _maximum_write_size: usize,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> Result<Option<OwnedWriteQueue>, HttpError> {
        Ok(None)
    }

    /// Acquires the transport's poll-based reader, which is untimed, so it is
    /// offered only for a zero `timeout`. `None` keeps the read API, including
    /// its timeout.
    fn reader(
        &self,
        _context: &WorkerContext,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> Result<Option<OwnedReader>, HttpError> {
        Ok(None)
    }
}

/// A poll-based reader for an [`OwnedByteStream`] ([`ByteStreamTransport::reader`]).
///
/// The owned uniplex drivers read through it instead of calling `read` for
/// each read. It starts one read at a time, and its reads reuse one read
/// operation. Each read has the same entry checks, clamps, buffer growth,
/// results, and close and cancellation outcomes as `read`, except for timing.
/// It has no deadline, its completion does not wake a task, and over a
/// Tokio-backed stream its I/O state lives in the backend, so it does not
/// allocate a future. Over TCP its first poll fills the tailroom of a buffer
/// that the transport keeps between reads, where a `read` starts a new buffer.
///
/// A cancellation wakes a pending read, as a timed read's wait does. The reader
/// registers that wake once for all its reads, so a cancellation requested
/// while no read is started wakes the task that last polled a pending read,
/// or that last received a drained read. Dropping the reader with a read
/// started, or drained, cancels that read, as dropping a read future does.
///
/// Over plaintext TCP, a `poll_start_read` read that completes on its first
/// poll is never registered with the transport. One that drains the socket
/// (`is_drained`) stays registered until the next read, so that the owner can
/// wait for the socket without polling a new read.
pub struct OwnedReader {
    worker: WorkerHandle,
    limits: ByteStreamLimits,
    context: WorkerContext,
    /// The started read, until `poll_read` returns its result.
    read: ReaderRead,
    /// The backend's reader when its reads can report a drained stream, which
    /// `poll_start_read` polls before registering a read.
    polled: Option<Weak<dyn PolledReader>>,
    /// The operation slot. Every read reuses it, with its transport handles.
    operation: ReadOperation,
    /// Drops after `operation`, because a waker it releases can start a
    /// replacement read.
    cancellation: ReaderCancellation,
}

/// The read an [`OwnedReader`] has started.
enum ReaderRead {
    Idle,
    /// The operation slot holds the read. `arm` is set when `poll_start_read`
    /// started the read, so the backend reports whether it drains the stream.
    Reading {
        arm: bool,
    },
    /// The read ended at entry with this result.
    Finished(Result<TransportRead, HttpError>),
    /// The last returned read drained the stream. No read is started, but the
    /// operation slot keeps that read registered with the transport under the
    /// waker of the poll that returned it, as for a pending read.
    Drained,
}

/// An [`OwnedReader`]'s cancellation: its token, and the one wake registration
/// that all its reads update while they pend.
struct ReaderCancellation {
    token: CancellationToken,
    listener: CancellationWakeRegistration,
}

impl ReaderCancellation {
    fn new(token: CancellationToken) -> Self {
        #[cfg(test)]
        record_reader_wake_registration();
        Self {
            listener: token.wake_registration(),
            token,
        }
    }
}

impl OwnedReader {
    /// Starts a read of at most `maximum` bytes that first offers
    /// `allocation_size` bytes, with `read`'s entry checks and clamps. Returns
    /// an error if the previously started read has not been returned yet.
    pub fn start_read(&mut self, maximum: usize, allocation_size: usize) -> Result<(), HttpError> {
        match self.read {
            ReaderRead::Idle => {}
            ReaderRead::Drained => self.end_registered(),
            ReaderRead::Reading { .. } | ReaderRead::Finished(_) => {
                return Err(HttpError::with_message(
                    HttpErrorCode::INVALID_STATE_TRANSITION,
                    b"owned reader already has a started read",
                ));
            }
        }
        let target = ReadTarget {
            io: &self.operation.io,
            state: &self.operation.state,
            worker: &self.worker,
            limits: self.limits,
        };
        match start_read_operation(
            target,
            &self.context,
            maximum,
            allocation_size,
            ReadKind::Reader {
                cancellation: &self.cancellation.token,
            },
        ) {
            ReadStart::Started { operation, maximum } => {
                self.operation.restart(operation, maximum);
                self.read = ReaderRead::Reading { arm: false };
            }
            ReadStart::Finished(result) => self.read = ReaderRead::Finished(result),
        }
        Ok(())
    }

    /// Polls the started read with `context`'s waker. Polling with no started
    /// read returns an error.
    pub fn poll_read(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<TransportRead, HttpError>> {
        match self.read {
            ReaderRead::Reading { arm } => {
                let result = ready!(self.operation.poll_with(
                    context,
                    Some(&mut self.cancellation),
                    arm
                ));
                if is_drained(&result) {
                    self.read = ReaderRead::Drained;
                    self.settle_drained(context);
                } else {
                    self.read = ReaderRead::Idle;
                    // A cancelled read's wake releases its waker clones only
                    // after the read has ended, because dropping them can start
                    // a replacement read.
                    if result.is_err() && self.cancellation.token.is_cancellation_requested() {
                        self.cancellation.listener.update_waker(context.waker());
                    }
                }
                Poll::Ready(result)
            }
            ReaderRead::Finished(_) => {
                let ReaderRead::Finished(result) =
                    std::mem::replace(&mut self.read, ReaderRead::Idle)
                else {
                    unreachable!("the started read finished at entry");
                };
                Poll::Ready(result)
            }
            ReaderRead::Idle | ReaderRead::Drained => Poll::Ready(Err(HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"owned reader has no started read",
            ))),
        }
    }

    /// Starts a read as `start_read` does and polls it immediately with
    /// `context`'s waker, as `poll_read` does. Starting one while a read is
    /// started returns an error.
    ///
    /// Over plaintext TCP, a read that completes immediately is never
    /// registered with the transport. After a drained read (`is_drained`), the
    /// next read reuses the drained read's registration, as the next poll of a
    /// pending read would. It ends at a close, reset or drop of the transport,
    /// observes the reader's cancellation if it pends, and otherwise polls the
    /// socket with the bounds given here, clamped as `read` clamps them.
    pub fn poll_start_read(
        &mut self,
        context: &mut Context<'_>,
        maximum: usize,
        allocation_size: usize,
    ) -> Poll<Result<TransportRead, HttpError>> {
        let polled = match self.read {
            ReaderRead::Idle => self.polled.as_ref().and_then(Weak::upgrade),
            ReaderRead::Drained => return self.poll_drained(context, maximum, allocation_size),
            ReaderRead::Reading { .. } | ReaderRead::Finished(_) => None,
        };
        let Some(polled) = polled else {
            if let Err(error) = self.start_read(maximum, allocation_size) {
                return Poll::Ready(Err(error));
            }
            if let ReaderRead::Reading { arm } = &mut self.read {
                *arm = true;
            }
            return self.poll_read(context);
        };
        let target = ReadTarget {
            io: &self.operation.io,
            state: &self.operation.state,
            worker: &self.worker,
            limits: self.limits,
        };
        let (maximum, allocation_size, ()) = match admit_read(
            target,
            &self.context,
            maximum,
            allocation_size,
            &self.cancellation.token,
            |_| (),
        ) {
            Ok(admitted) => admitted,
            Err(result) => return Poll::Ready(result),
        };
        match polled.try_read(context, maximum, allocation_size) {
            TryRead::Ready(result) => {
                let result = bounded_read(result, maximum);
                if is_drained(&result) {
                    self.register_read(polled, context, maximum);
                    self.read = ReaderRead::Drained;
                    self.settle_drained(context);
                }
                Poll::Ready(deliver_read(&self.operation.state, result))
            }
            TryRead::Pending => {
                self.register_read(polled, context, maximum);
                self.pend(context)
            }
            TryRead::Panicked(panic) => {
                self.register_read(polled, context, maximum);
                self.read = ReaderRead::Reading { arm: true };
                resume_unwind(panic)
            }
        }
    }

    /// Handles `poll_start_read` for a drained reader, whose read is still
    /// registered.
    fn poll_drained(
        &mut self,
        context: &mut Context<'_>,
        maximum: usize,
        allocation_size: usize,
    ) -> Poll<Result<TransportRead, HttpError>> {
        if let Some(result) = self.operation.closed() {
            self.read = ReaderRead::Idle;
            return Poll::Ready(result);
        }
        // A read with no bounds ends at entry, after the drained registration.
        let Some(polled) = self
            .polled
            .as_ref()
            .and_then(Weak::upgrade)
            .filter(|_| maximum != 0 && allocation_size != 0)
        else {
            self.end_registered();
            return self.poll_start_read(context, maximum, allocation_size);
        };
        let maximum = maximum.min(self.limits.maximum_read_size);
        let allocation_size = allocation_size.min(maximum);
        match polled.try_read(context, maximum, allocation_size) {
            TryRead::Ready(result) => {
                let result = bounded_read(result, maximum);
                if !is_drained(&result) {
                    self.operation.finish(None);
                    self.read = ReaderRead::Idle;
                } else if self.settle_drained(context) {
                    self.operation.rewake(context);
                }
                Poll::Ready(deliver_read(&self.operation.state, result))
            }
            TryRead::Pending => {
                self.operation.rebound(maximum);
                self.operation.rewake(context);
                self.pend(context)
            }
            TryRead::Panicked(panic) => {
                self.operation.rebound(maximum);
                self.operation.rewake(context);
                self.read = ReaderRead::Reading { arm: true };
                resume_unwind(panic)
            }
        }
    }

    /// Leaves the registered read pending, as a pending poll of a started read
    /// does, so the reader's cancellation wake follows `context`'s waker. The
    /// read is started before that update, which can unwind. If a cancellation
    /// was already requested, the read fails instead. It ends before the wake
    /// releases its wakers, so a release that unwinds leaves the reader idle.
    fn pend(&mut self, context: &Context<'_>) -> Poll<Result<TransportRead, HttpError>> {
        self.read = ReaderRead::Reading { arm: true };
        if self.follow_cancellation(context.waker()) {
            return Poll::Ready(Err(cancelled_error(b"byte-stream read cancelled")));
        }
        Poll::Pending
    }

    /// Keeps the drained read registered and returns whether it stays drained.
    /// The reader's cancellation wake then follows `context`'s waker, as for a
    /// pending read. If a cancellation was already requested, the registration
    /// ends as a cancelled read ends instead, which releases the waker that the
    /// read's readiness poll left with the socket.
    fn settle_drained(&mut self, context: &Context<'_>) -> bool {
        !self.follow_cancellation(context.waker())
    }

    /// Makes the reader's cancellation wake follow `waker`, or, once a
    /// cancellation is requested, ends the registered read as a cancelled read
    /// ends and returns `true`. The read ends before the wake releases its
    /// waker clones, because dropping them can start a replacement read.
    #[inline]
    fn follow_cancellation(&mut self, waker: &Waker) -> bool {
        if !self.cancellation.token.is_cancellation_requested()
            && !self.cancellation.listener.update_waker(waker)
        {
            return false;
        }
        self.end_registered();
        self.cancellation.listener.update_waker(waker);
        true
    }

    /// Registers a read that `try_read` started or drained, with `context`'s
    /// waker.
    fn register_read(
        &mut self,
        polled: Rc<dyn PolledReader>,
        context: &Context<'_>,
        maximum: usize,
    ) {
        let operation = {
            let mut state = self.operation.state.borrow_mut();
            debug_assert!(state.active_read.is_none(), "the read was admitted");
            let operation = state.next_operation();
            state.active_read = Some(ActiveRead {
                operation,
                drive: ReadDrive::Untimed {
                    io: ActiveReadIo::Polled(PolledReadIo(polled)),
                },
                waker: Some(context.waker().clone()),
            });
            operation
        };
        #[cfg(test)]
        record_read_registration();
        self.operation.restart(operation, maximum);
    }

    /// Whether the read that `poll_start_read` last returned drained the
    /// stream.
    ///
    /// A drained read gathered bytes without an error or EOF and left the
    /// socket unreadable. The waker of the poll that returned it is then
    /// registered for the socket's next readiness, and the read stays
    /// registered with the transport with that waker. A close, reset or drop of
    /// the transport, or a cancellation, wakes it as it wakes a pending read.
    /// Only plaintext TCP reads report it.
    pub fn is_drained(&self) -> bool {
        matches!(self.read, ReaderRead::Drained)
    }

    /// Whether the peer's FIN or RST has reached the socket, even behind
    /// unread bytes. This checks the kernel's current state, not Tokio's last
    /// readiness, and does not consume any bytes. Only plaintext TCP on Linux
    /// reports it.
    pub fn peer_closed(&self) -> bool {
        self.polled
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some_and(|polled| polled.peer_closed())
    }

    /// Ends the registered read, drained or pending, as a cancelled read ends,
    /// which releases the socket's waker.
    #[cold]
    fn end_registered(&mut self) {
        self.operation.finish(Some(HttpErrorCode::CORO_CANCELLED));
        self.read = ReaderRead::Idle;
    }
}

/// Unconsumed receipts an [`OwnedWriteQueue`] holds before it rejects submissions.
const WRITE_QUEUE_RECEIPTS: usize = 8;

/// One retired submission of an [`OwnedWriteQueue`].
#[derive(Debug)]
pub struct QueuedWriteCompletion {
    pub requested_bytes: usize,
    pub result: Result<WriteInfo, TransportWriteError>,
}

/// A submission whose receipt the queue has not yet delivered.
enum QueuedReceipt {
    /// A transport write of this many bytes is outstanding.
    Writing(usize),
    Done(QueuedWriteCompletion),
}

/// The native write queue of an [`OwnedByteStream`] over plaintext TCP
/// ([`ByteStreamTransport::write_queue`]).
///
/// It is a bounded writer with ordered completion receipts, which the owned
/// uniplex drivers use instead of one write future per write. The queue admits
/// a submission only while a transport write is not outstanding, and holds at
/// most `WRITE_QUEUE_RECEIPTS` unconsumed receipts. A submission is attempted
/// at entry, with no waker. A write finished by that attempt is never
/// registered with the transport or held by its backend. A write left
/// unfinished, because the socket took only part of it or would block, is
/// registered with the transport. The rest of its bytes are written as
/// [`OwnedWriteQueue::poll_complete`] polls it, with the transport's
/// single-write semantics except for timing. The write has no deadline, does
/// not allocate, and its completion does not wake a task. Either way the
/// submission holds the queue's capacity until a poll observes its completion.
///
/// The outstanding write observes a cancellation at its next poll, and the next
/// submission observes it at entry. A cancellation requested from another task
/// wakes the task that last polled the outstanding write, as a reset of the
/// transport does. Dropping the queue with a write outstanding resets the
/// transport, as dropping a write future does.
pub struct OwnedWriteQueue {
    worker: WorkerHandle,
    limits: ByteStreamLimits,
    byte_event_capability: ByteEventCapability,
    context: WorkerContext,
    maximum_write_size: usize,
    cancellation: CancellationToken,
    /// Wakes the task that last polled the outstanding write when another task
    /// requests cancellation. It does not install a listener until a write
    /// pends.
    cancellation_wake: CancellationWakeRegistration,
    /// The operation slot. Every write reuses it, with its transport handles
    /// and its progress cell, which counts each write's progress in turn.
    operation: WriteOperation,
    held: Held,
    receipts: VecDeque<QueuedReceipt>,
}

/// What withholds an [`OwnedWriteQueue`]'s capacity, from a submission's start
/// until a poll observes its completion.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Held {
    Free,
    /// The operation slot holds the outstanding write.
    Writing,
    /// The submission's first attempt finished it at entry, so its receipt is
    /// done.
    WrittenThrough,
}

impl OwnedWriteQueue {
    /// Whether the queue has capacity for a submission of any size up to the
    /// maximum.
    ///
    /// An outstanding write progresses only while `poll_complete` is polled,
    /// so capacity is withheld until a poll observes the submission's
    /// completion, as it is for a write finished at entry. Returning `false`
    /// here is what arms the owned HTTP/2 writer for every write the queue
    /// holds (`UniplexWriteQueue::can_write`).
    pub fn can_write(&self, _bytes: usize) -> bool {
        self.held == Held::Free && self.receipts.len() < WRITE_QUEUE_RECEIPTS
    }

    /// Submits `data`. The queue must have capacity and `data` must fit the
    /// maximum write size. Otherwise this returns an error and does not leave
    /// a receipt. A submission that the transport rejects at entry, or an empty
    /// one, gets its receipt and returns its capacity immediately.
    pub fn start_write(&mut self, data: BufQueue) -> Result<(), HttpError> {
        let requested_bytes = data.chain_length();
        if requested_bytes > self.maximum_write_size {
            return Err(HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"write queue submission exceeds its maximum write size",
            ));
        }
        if !self.can_write(requested_bytes) {
            return Err(HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"write queue has no capacity",
            ));
        }
        let target = WriteTarget {
            io: &self.operation.io,
            state: &self.operation.state,
            worker: &self.worker,
            limits: self.limits,
            byte_event_capability: self.byte_event_capability,
        };
        match start_write_operation(
            target,
            &self.context,
            data,
            ByteEventWriteFlags::default(),
            WriteKind::Queued {
                progress: &self.operation.progress,
                cancellation: &self.cancellation,
            },
        ) {
            WriteStart::Started {
                operation,
                expected,
                progress,
            } => {
                self.operation.restart(operation, expected, progress);
                self.held = Held::Writing;
                self.receipts
                    .push_back(QueuedReceipt::Writing(requested_bytes));
            }
            WriteStart::WrittenThrough(result) => {
                self.held = Held::WrittenThrough;
                self.receipts
                    .push_back(QueuedReceipt::Done(QueuedWriteCompletion {
                        requested_bytes,
                        result,
                    }));
            }
            WriteStart::Finished(result) => {
                self.receipts
                    .push_back(QueuedReceipt::Done(QueuedWriteCompletion {
                        requested_bytes,
                        result,
                    }));
            }
            WriteStart::Unwinding {
                operation,
                expected,
                progress,
                panic,
            } => {
                self.operation.restart(operation, expected, progress);
                self.held = Held::Writing;
                self.receipts
                    .push_back(QueuedReceipt::Writing(requested_bytes));
                resume_unwind(panic);
            }
        }
        Ok(())
    }

    /// Drives the outstanding write with `context`'s waker and yields the
    /// oldest receipt once its submission is complete. An idle queue does not
    /// register the waker.
    pub fn poll_complete(&mut self, context: &mut Context<'_>) -> Poll<QueuedWriteCompletion> {
        match self.held {
            Held::Writing => match self.operation.poll_with(context, Some(&self.cancellation)) {
                Poll::Ready(result) => {
                    self.held = Held::Free;
                    let receipt = self
                        .receipts
                        .back_mut()
                        .expect("an outstanding write keeps its receipt");
                    let QueuedReceipt::Writing(requested_bytes) = *receipt else {
                        unreachable!("only the newest receipt can be outstanding");
                    };
                    *receipt = QueuedReceipt::Done(QueuedWriteCompletion {
                        requested_bytes,
                        result,
                    });
                }
                Poll::Pending => {
                    if self.cancellation_wake.update_waker(context.waker()) {
                        context.waker().wake_by_ref();
                    }
                }
            },
            Held::WrittenThrough => self.held = Held::Free,
            Held::Free => {}
        }
        if !matches!(self.receipts.front(), Some(QueuedReceipt::Done(_))) {
            return Poll::Pending;
        }
        let Some(QueuedReceipt::Done(completion)) = self.receipts.pop_front() else {
            unreachable!("the oldest receipt is done");
        };
        Poll::Ready(completion)
    }

    /// Whether every receipt was consumed.
    pub fn is_idle(&self) -> bool {
        self.receipts.is_empty()
    }

    /// Whether bytes submitted now would wait behind bytes the transport has
    /// not yet accepted. That is the case while a write is outstanding, or once
    /// the write side is shut down or the transport is closing. A write
    /// finished at entry counts as outstanding until a poll observes its
    /// completion, because it withholds capacity until then.
    pub fn is_saturated(&self) -> bool {
        let state = self.operation.state.borrow();
        self.held != Held::Free || state.write_shutdown || state.close_state != CloseState::Open
    }
}

#[derive(Clone)]
pub(crate) struct WriteProgress {
    written: Rc<Cell<usize>>,
    maximum: usize,
}

impl WriteProgress {
    fn new(maximum: usize) -> Self {
        #[cfg(test)]
        WRITE_PROGRESS_CELLS.with(|count| count.set(count.get() + 1));
        Self {
            written: Rc::new(Cell::new(0)),
            maximum,
        }
    }

    pub(crate) fn record(&self, amount: usize) -> Result<(), HttpError> {
        let written = self
            .written
            .get()
            .checked_add(amount)
            .ok_or_else(|| write_error(b"byte-stream write progress overflow"))?;
        if written > self.maximum {
            return Err(write_error(
                b"byte-stream adapter reported excess write progress",
            ));
        }
        self.written.set(written);
        Ok(())
    }

    fn get(&self) -> usize {
        self.written.get()
    }

    /// Restarts this cell, which no other write is recording into, for a write of
    /// `maximum` bytes.
    fn restart(&self, maximum: usize) -> Self {
        self.written.set(0);
        Self {
            written: Rc::clone(&self.written),
            maximum,
        }
    }
}

#[derive(Clone)]
pub(crate) struct ByteEventWriteState {
    inner: Rc<RefCell<ByteEventWriteInner>>,
}

struct ByteEventWriteInner {
    registrations: Vec<HttpByteEventRegistration>,
}

impl ByteEventWriteState {
    fn new(registrations: Vec<HttpByteEventRegistration>) -> Option<Self> {
        (!registrations.is_empty()).then(|| Self {
            inner: Rc::new(RefCell::new(ByteEventWriteInner { registrations })),
        })
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "called by concrete socket adapters with native byte-event support"
        )
    )]
    pub(crate) fn fire(&self, event_type: ByteEventType, transport_offset: u64) {
        let registrations = {
            let mut inner = self.inner.borrow_mut();
            let mut ready = Vec::new();
            let mut retained = Vec::new();
            for registration in std::mem::take(&mut inner.registrations) {
                if registration.pending_events() == event_type.bit() {
                    ready.push(registration);
                } else {
                    retained.push(registration);
                }
            }
            inner.registrations = retained;
            ready
        };
        let mut first_panic = None;
        for mut registration in registrations {
            let mut event = HttpByteEvent::new(event_type);
            event.stream_id = registration.stream_id();
            event.transport_offset = transport_offset;
            let result = catch_unwind(AssertUnwindSafe(|| registration.fire(event)));
            if first_panic.is_none() {
                first_panic = result.err();
            }
        }
        if let Some(payload) = first_panic {
            resume_unwind(payload);
        }
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "called by concrete socket adapters with native byte-event support"
        )
    )]
    pub(crate) fn is_empty(&self) -> bool {
        self.inner.borrow().registrations.is_empty()
    }

    pub(crate) fn cancel(&self, error: &HttpError) {
        let registrations = std::mem::take(&mut self.inner.borrow_mut().registrations);
        let mut first_panic = None;
        for mut registration in registrations {
            let result = catch_unwind(AssertUnwindSafe(|| registration.cancel(error, None)));
            if first_panic.is_none() {
                first_panic = result.err();
            }
        }
        if let Some(payload) = first_panic {
            resume_unwind(payload);
        }
    }
}

/// Private seam implemented by worker-local I/O adapters in this crate.
pub(crate) trait ByteStreamIo: 'static {
    /// Reads at most `maximum` bytes, offering `allocation_size` bytes of
    /// capacity first. `begin_read` clamps both and keeps `allocation_size`
    /// no larger than `maximum`.
    fn read(
        &self,
        maximum: usize,
        allocation_size: usize,
    ) -> LocalTransportFuture<Result<TransportRead, HttpError>>;

    fn prepare_write_timeout(&self, _timeout: Duration) {}

    fn write(
        &self,
        data: BufQueue,
        progress: WriteProgress,
        flags: ByteEventWriteFlags,
        byte_events: Option<ByteEventWriteState>,
    ) -> LocalTransportFuture<Result<(), HttpError>>;

    fn cancel_read(&self, error: HttpErrorCode);

    fn cancel_write(&self, error: HttpErrorCode);

    fn shutdown_write(&self, context: &WorkerContext) -> Result<(), HttpError>;

    fn close(&self, context: &WorkerContext, drain_write: bool) -> Result<(), HttpError>;

    fn drop_transport(&self);

    fn close_with_reset(&self);

    /// The writer of an [`OwnedWriteQueue`] over this backend, or `None` if the
    /// backend does not offer a queue.
    fn queued_writer(&self) -> Option<Rc<dyn QueuedWriter>> {
        None
    }

    /// The reader for [`OwnedReader`] reads over this backend, or `None` to
    /// read through `read`'s future.
    fn polled_reader(&self) -> Option<Rc<dyn PolledReader>> {
        None
    }

    fn enable_zero_copy(
        &self,
        _context: &WorkerContext,
        _threshold: usize,
    ) -> Result<bool, HttpError> {
        Ok(false)
    }

    fn set_zero_copy_reenable_threshold(&self, _threshold: usize) -> Result<(), HttpError> {
        Ok(())
    }

    /// Makes `close_with_reset` close the socket without the linger-zero reset.
    /// Adapters without an abortive reset ignore it.
    fn enable_non_abortive_close(&self) {}

    #[cfg(test)]
    fn zero_copy_owner_for_test(&self) -> Option<usize> {
        None
    }

    #[cfg(test)]
    fn zero_copy_pending_for_test(&self) -> usize {
        0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CloseState {
    Open,
    Graceful,
    Reset,
    Dropped,
}

struct ActiveRead {
    operation: u64,
    drive: ReadDrive,
    waker: Option<Waker>,
}

/// What one read polls. Fields drop in declaration order.
enum ReadDrive {
    /// An ordinary read: `ByteStreamIo::read`'s boxed future, its deadline,
    /// then the deadline's wait.
    Timed {
        io_future: LocalTransportFuture<Result<TransportRead, HttpError>>,
        deadline: TimedBaton,
        wait: Pin<Box<TimedWait>>,
    },
    /// An [`OwnedReader`]'s read: no deadline. While it pends, its reader's
    /// cancellation registration wakes it, as a timed read's wait does.
    Untimed { io: ActiveReadIo },
}

/// The I/O of an [`OwnedReader`] read: `ByteStreamIo::read`'s boxed future,
/// or the backend that holds the read's state.
enum ActiveReadIo {
    Future(LocalTransportFuture<Result<TransportRead, HttpError>>),
    Polled(PolledReadIo),
}

impl ActiveReadIo {
    /// Polls the read, with `arm` as in `PolledReader::poll_read`.
    fn poll(
        &mut self,
        context: &mut Context<'_>,
        arm: bool,
    ) -> Poll<Result<TransportRead, HttpError>> {
        match self {
            Self::Future(future) => future.as_mut().poll(context),
            Self::Polled(polled) => polled.0.poll_read(context, arm),
        }
    }

    /// Ends the read with `cancel`, which releases the socket waker. For a
    /// polled read, the backend detaches the read's state and the read's guard
    /// drops before `cancel` runs. That way the old read's cleanup does not
    /// retire a replacement read that `cancel` admits.
    fn cancel(self, cancel: &mut dyn FnMut()) {
        match self {
            Self::Future(_future) => cancel(),
            Self::Polled(polled) => {
                let reader = Rc::clone(&polled.0);
                let mut guard = Some(polled);
                reader.cancel_read(&mut || {
                    drop(guard.take());
                    cancel();
                });
            }
        }
    }
}

/// A read that a [`PolledReader`] holds. Dropping it ends the read, as
/// dropping `ByteStreamIo::read`'s future does. After `Ready`, dropping it has
/// no effect.
struct PolledReadIo(Rc<dyn PolledReader>);

impl Drop for PolledReadIo {
    fn drop(&mut self) {
        self.0.retire_read();
    }
}

/// A backend that keeps one read's state itself, so that an [`OwnedReader`]
/// read does not allocate a future. Each read has the effects of
/// `ByteStreamIo::read`'s future.
pub(crate) trait PolledReader {
    fn start_read(&self, maximum: usize, allocation_size: usize);

    /// `Ready` ends the read. With `arm`, a read that gathered bytes without an
    /// error or EOF polls the stream's read readiness once more with
    /// `context`'s waker. If the stream is not readable, that poll registers
    /// the waker and the read is marked drained
    /// (`TransportRead::with_drained`).
    fn poll_read(
        &self,
        context: &mut Context<'_>,
        arm: bool,
    ) -> Poll<Result<TransportRead, HttpError>>;

    /// Starts a read and polls it immediately, armed as `poll_read` is.
    fn try_read(
        &self,
        context: &mut Context<'_>,
        maximum: usize,
        allocation_size: usize,
    ) -> TryRead;

    /// Whether its reads can report a drained stream.
    fn reports_drained(&self) -> bool;

    /// Backs `OwnedReader::peer_closed`.
    fn peer_closed(&self) -> bool;

    /// Detaches the unfinished read's state, then calls `cancel` once, keeping
    /// that state until `cancel` returns or unwinds. `cancel` can start a
    /// replacement read.
    fn cancel_read(&self, cancel: &mut dyn FnMut());

    /// Ends an unfinished read, as dropping `ByteStreamIo::read`'s future does.
    fn retire_read(&self);
}

#[cfg(test)]
std::thread_local! {
    static WRITE_DEADLINE_BATONS: Cell<usize> = const { Cell::new(0) };
    static CLOSE_PATH_ERRORS: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
std::thread_local! {
    static WRITE_PROGRESS_CELLS: Cell<usize> = const { Cell::new(0) };
    static WRITE_OPERATIONS: Cell<usize> = const { Cell::new(0) };
    static WRITE_REGISTRATIONS: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
std::thread_local! {
    static READ_DEADLINE_BATONS: Cell<usize> = const { Cell::new(0) };
    static READER_WAKE_REGISTRATIONS: Cell<usize> = const { Cell::new(0) };
    static READ_QUEUE_REBUILDS: Cell<usize> = const { Cell::new(0) };
    static READ_OPERATIONS: Cell<usize> = const { Cell::new(0) };
    static READ_REGISTRATIONS: Cell<usize> = const { Cell::new(0) };
}

/// `ReadOperation`s built on this thread.
#[cfg(test)]
pub(crate) fn read_operations_for_test() -> usize {
    READ_OPERATIONS.with(Cell::get)
}

#[cfg(test)]
fn record_read_registration() {
    READ_REGISTRATIONS.with(|count| count.set(count.get() + 1));
}

/// Reads registered with their transport (`TransportState::active_read`) on
/// this thread.
#[cfg(test)]
pub(crate) fn read_registrations_for_test() -> usize {
    READ_REGISTRATIONS.with(Cell::get)
}

/// Read-direction deadline batons built on this thread.
#[cfg(test)]
pub(crate) fn read_deadline_batons_for_test() -> usize {
    READ_DEADLINE_BATONS.with(Cell::get)
}

#[cfg(test)]
fn record_reader_wake_registration() {
    READER_WAKE_REGISTRATIONS.with(|count| count.set(count.get() + 1));
}

/// Cancellation wake registrations built for [`OwnedReader`] reads on this thread.
#[cfg(test)]
pub(crate) fn reader_wake_registrations_for_test() -> usize {
    READER_WAKE_REGISTRATIONS.with(Cell::get)
}

/// Completed reads whose bytes `normalize_read` moved into a new queue on this thread.
#[cfg(test)]
pub(crate) fn read_queue_rebuilds_for_test() -> usize {
    READ_QUEUE_REBUILDS.with(Cell::get)
}

/// Write progress cells built on this thread.
#[cfg(test)]
pub(crate) fn write_progress_cells_for_test() -> usize {
    WRITE_PROGRESS_CELLS.with(Cell::get)
}

/// `WriteOperation`s built on this thread.
#[cfg(test)]
pub(crate) fn write_operations_for_test() -> usize {
    WRITE_OPERATIONS.with(Cell::get)
}

/// Writes registered with their transport (`TransportState::active_write`) on this thread.
#[cfg(test)]
pub(crate) fn write_registrations_for_test() -> usize {
    WRITE_REGISTRATIONS.with(Cell::get)
}

#[cfg(test)]
fn record_write_deadline_baton() {
    WRITE_DEADLINE_BATONS.with(|count| count.set(count.get() + 1));
}

/// Write-direction deadline batons built on this thread.
#[cfg(test)]
pub(crate) fn write_deadline_batons_for_test() -> usize {
    WRITE_DEADLINE_BATONS.with(Cell::get)
}

#[cfg(test)]
pub(crate) fn record_close_path_error() {
    CLOSE_PATH_ERRORS.with(|count| count.set(count.get() + 1));
}

/// Byte-event cancellation errors that close paths built on this thread.
#[cfg(test)]
pub(crate) fn close_path_errors_for_test() -> usize {
    CLOSE_PATH_ERRORS.with(Cell::get)
}

fn transition_close_resources(
    io: &dyn ByteStreamIo,
    state: &Rc<RefCell<TransportState>>,
    close_state: CloseState,
    context: Option<&WorkerContext>,
) -> Result<(), HttpError> {
    let (mut active_read, mut active_write, drain_write, registrations) = {
        let mut state = state.borrow_mut();
        state.assert_owner();
        match (state.close_state, close_state) {
            (CloseState::Graceful, CloseState::Dropped) => {
                let byte_events = state
                    .active_write
                    .as_ref()
                    .and_then(|active| active.byte_events.clone());
                drop(state);
                let mut failure = catch_unwind(AssertUnwindSafe(|| {
                    if let Some(byte_events) = byte_events {
                        #[cfg(test)]
                        record_close_path_error();
                        byte_events.cancel(&dropped_error());
                    }
                }))
                .err();
                retain_transport_panic(
                    &mut failure,
                    catch_unwind(AssertUnwindSafe(|| io.drop_transport())).err(),
                );
                finish_transport_panic(failure, std::thread::panicking());
                return Ok(());
            }
            (CloseState::Open, _) | (CloseState::Graceful, CloseState::Reset) => {}
            _ => return Ok(()),
        }
        state.close_state = close_state;
        let drain_write = close_state == CloseState::Graceful && state.active_write.is_some();
        state.graceful_close_context = if drain_write { context.cloned() } else { None };
        (
            state.active_read.take(),
            if drain_write {
                None
            } else {
                state.active_write.take()
            },
            drain_write,
            state.take_all_registrations(),
        )
    };
    let read_waker = active_read
        .as_mut()
        .and_then(|operation| operation.waker.take());
    let write_waker = active_write
        .as_mut()
        .and_then(|operation| operation.waker.take());
    let mut failure = None;
    if active_read.is_some() {
        retain_transport_panic(
            &mut failure,
            catch_unwind(AssertUnwindSafe(|| {
                io.cancel_read(match close_state {
                    CloseState::Graceful => HttpErrorCode::TRANSPORT_EOF,
                    CloseState::Reset => HttpErrorCode::TRANSPORT_READ_ERROR,
                    CloseState::Dropped => HttpErrorCode::DROPPED,
                    CloseState::Open => unreachable!("a close transition must close the transport"),
                });
            }))
            .err(),
        );
    }
    if active_write.is_some() {
        retain_transport_panic(
            &mut failure,
            catch_unwind(AssertUnwindSafe(|| {
                io.cancel_write(match close_state {
                    CloseState::Reset => HttpErrorCode::TRANSPORT_WRITE_ERROR,
                    CloseState::Dropped => HttpErrorCode::DROPPED,
                    CloseState::Graceful => unreachable!("graceful close retains its active write"),
                    CloseState::Open => unreachable!("a close transition must close the transport"),
                });
            }))
            .err(),
        );
    }
    let error = (active_write
        .as_ref()
        .is_some_and(|active| active.byte_events.is_some())
        || !registrations.is_empty())
    .then(|| {
        #[cfg(test)]
        record_close_path_error();
        match close_state {
            CloseState::Reset => write_error(b"byte-stream reset cancelled byte event"),
            CloseState::Graceful => cancelled_error(b"byte-stream closed before byte event"),
            CloseState::Dropped => dropped_error(),
            CloseState::Open => unreachable!("a close transition must close the transport"),
        }
    });
    if let Some(byte_events) = active_write
        .as_ref()
        .and_then(|active| active.byte_events.as_ref())
        && let Some(error) = &error
    {
        retain_transport_panic(
            &mut failure,
            catch_unwind(AssertUnwindSafe(|| {
                byte_events.cancel(error);
            }))
            .err(),
        );
    }
    if let Some(active) = active_read {
        let ActiveRead { drive, waker, .. } = active;
        match drive {
            ReadDrive::Timed {
                io_future,
                deadline,
                wait,
            } => {
                retire_transport_value(wait, &mut failure);
                retire_transport_value(deadline, &mut failure);
                retire_transport_value(io_future, &mut failure);
            }
            ReadDrive::Untimed { io } => retire_transport_value(io, &mut failure),
        }
        retire_transport_value(waker, &mut failure);
    }
    if let Some(active) = active_write {
        let ActiveWrite {
            io,
            byte_events,
            _progress: progress,
            wait,
            waker,
            ..
        } = active;
        match wait {
            WriteWait::Armed { deadline, wait } => {
                retire_transport_value(wait, &mut failure);
                retire_transport_value(deadline, &mut failure);
            }
            wait => retire_transport_value(wait, &mut failure),
        }
        retire_transport_value(io, &mut failure);
        retire_transport_value(byte_events, &mut failure);
        retire_transport_value(progress, &mut failure);
        retire_transport_value(waker, &mut failure);
    }
    let mut close_result = Ok(());
    retain_transport_panic(
        &mut failure,
        catch_unwind(AssertUnwindSafe(|| {
            close_result = match close_state {
                CloseState::Graceful => io.close(
                    context.expect("a requested graceful close retains its worker context"),
                    drain_write,
                ),
                CloseState::Dropped => {
                    io.drop_transport();
                    Ok(())
                }
                CloseState::Reset => {
                    io.close_with_reset();
                    Ok(())
                }
                CloseState::Open => unreachable!("a close transition must close the transport"),
            };
        }))
        .err(),
    );
    if let Some(error) = &error {
        for mut registration in registrations {
            retain_transport_panic(
                &mut failure,
                catch_unwind(AssertUnwindSafe(|| {
                    registration.cancel(error, None);
                }))
                .err(),
            );
            retire_transport_value(registration, &mut failure);
        }
    }
    if let Some(waker) = read_waker {
        retain_transport_panic(
            &mut failure,
            catch_unwind(AssertUnwindSafe(|| waker.wake())).err(),
        );
    }
    if let Some(waker) = write_waker {
        retain_transport_panic(
            &mut failure,
            catch_unwind(AssertUnwindSafe(|| waker.wake())).err(),
        );
    }
    if close_result.is_err() && drain_write {
        retain_transport_panic(
            &mut failure,
            catch_unwind(AssertUnwindSafe(|| {
                transition_close_resources(io, state, CloseState::Reset, None)
                    .expect("resetting after failed graceful-close scheduling cannot fail");
            }))
            .err(),
        );
    }
    if failure.is_some() {
        retire_transport_value(close_result, &mut failure);
        resume_unwind(failure.expect("failed transition retains its first panic"));
    }
    close_result
}

fn retire_transport_value<Value>(value: Value, failure: &mut Option<TransportPanic>) {
    retain_transport_panic(
        failure,
        catch_unwind(AssertUnwindSafe(|| drop(value))).err(),
    );
}

struct ActiveWrite {
    operation: u64,
    io: ActiveIo,
    byte_events: Option<ByteEventWriteState>,
    _progress: WriteProgress,
    wait: WriteWait,
    waker: Option<Waker>,
}

/// The I/O of one transport write: an ordinary write's boxed future, or the
/// backend that holds an [`OwnedWriteQueue`] write's state.
enum ActiveIo {
    Future(LocalTransportFuture<Result<(), HttpError>>),
    Queued(QueuedIo),
}

impl ActiveIo {
    fn poll(&mut self, context: &mut Context<'_>) -> Poll<Result<(), HttpError>> {
        match self {
            Self::Future(future) => future.as_mut().poll(context),
            Self::Queued(queued) => queued.0.poll_write(context),
        }
    }
}

/// A write that a [`QueuedWriter`] holds. Dropping it ends the write, as
/// dropping an ordinary write's future does. After `Ready`, dropping it has no
/// effect.
struct QueuedIo(Rc<dyn QueuedWriter>);

impl Drop for QueuedIo {
    fn drop(&mut self) {
        self.0.retire_write();
    }
}

/// A backend that keeps one write's state itself, so that an
/// [`OwnedWriteQueue`] write does not allocate a future. Each write has the
/// effects of `ByteStreamIo::write`'s future.
pub(crate) trait QueuedWriter {
    fn start_write(
        &self,
        data: BufQueue,
        progress: WriteProgress,
        flags: ByteEventWriteFlags,
        byte_events: Option<ByteEventWriteState>,
    );

    /// Attempts a write of `data` without byte events once, with the effects
    /// of `start_write` followed by one `poll_write` with `Waker::noop()`,
    /// except that the backend never holds a write that the attempt finishes.
    fn write_through(&self, data: BufQueue, progress: &WriteProgress) -> WriteThrough;

    /// `Ready` ends the write.
    fn poll_write(&self, context: &mut Context<'_>) -> Poll<Result<(), HttpError>>;

    /// Ends an unfinished write, as dropping `ByteStreamIo::write`'s future does.
    fn retire_write(&self);
}

/// How a [`PolledReader::try_read`] attempt ended.
pub(crate) enum TryRead {
    /// The read finished with this result.
    Ready(Result<TransportRead, HttpError>),
    /// The read pends, and the backend holds it as after `start_read` and a
    /// pending `poll_read`.
    Pending,
    /// The poll panicked, and the caller resumes `panic`. The backend holds the
    /// read as for `Pending`, as after a `poll_read` that panicked.
    Panicked(TransportPanic),
}

/// How a [`QueuedWriter::write_through`] attempt ended.
pub(crate) enum WriteThrough {
    /// The attempt finished the write with this result.
    Finished(Result<(), HttpError>),
    /// The attempt left the write unfinished, and the backend holds it as
    /// after `start_write` and a pending `poll_write`.
    Held,
    /// The attempt panicked, and the caller resumes `panic`. The backend holds
    /// the write as for `Held`. If the panic came as the finished write ended,
    /// the backend is left as after a `poll_write` that panicked at that point.
    Panicked(TransportPanic),
}

/// A write's deadline, built only once a backend poll leaves the write
/// pending, so a write finished by its first poll never builds one. A first
/// backend poll that panics also builds it before the panic resumes, so that
/// the deadline and cancellation are registered when the write is polled again.
enum WriteWait {
    /// `started`, the first poll's time, anchors a nonzero timeout's deadline.
    /// A zero timeout does not read the clock.
    Unarmed {
        timeout: Duration,
        cancellation: CancellationToken,
        started: Option<Instant>,
    },
    /// `deadline` precedes `wait`, so that dropping the write runs
    /// `TimedBaton::drop`, which wakes the waiter, before `wait` unregisters it.
    Armed {
        deadline: TimedBaton,
        wait: Pin<Box<TimedWait>>,
    },
    /// An unarmed write that `WriteOperation::finish` retires, waking its task
    /// at the point where an armed write's `TimedBaton::drop` wakes it.
    Retired { _wake: CompletionWake },
    /// An [`OwnedWriteQueue`]'s write: no deadline, and the queue's
    /// cancellation is observed when the write is polled, which does not wake a
    /// task.
    Untimed,
}

struct CompletionWake(Option<Waker>);

impl Drop for CompletionWake {
    fn drop(&mut self) {
        if let Some(waker) = self.0.take() {
            waker.wake();
        }
    }
}

/// Arms an unarmed write's deadline at `started` plus its timeout on `host`
/// and polls the wait once to register it.
fn arm_write_deadline(
    wait: &mut WriteWait,
    started: Option<Instant>,
    host: &Host,
    context: &mut Context<'_>,
) {
    let WriteWait::Unarmed {
        timeout,
        cancellation,
        ..
    } = wait
    else {
        unreachable!("only an unarmed write arms its deadline");
    };
    let timeout = *timeout;
    // A zero remaining time would disable the deadline.
    let remaining = if timeout.is_zero() {
        Duration::ZERO
    } else {
        let started = started.expect("a timed write reads its first poll's time");
        timeout
            .saturating_sub(host.now().saturating_duration_since(started))
            .max(Duration::from_nanos(1))
    };
    #[cfg(test)]
    record_write_deadline_baton();
    let deadline = TimedBaton::with_host(Some(remaining), host.clone());
    let timed_wait = Box::pin(deadline.wait(std::mem::take(cancellation)));
    *wait = WriteWait::Armed {
        deadline,
        wait: timed_wait,
    };
    // A completed wait cannot be polled again. The next poll reads its outcome
    // from the baton.
    if let WriteWait::Armed { wait, .. } = wait
        && wait.as_mut().poll(context).is_ready()
    {
        context.waker().wake_by_ref();
    }
}

struct PendingByteEventRegistration {
    write_operation: Option<u64>,
    registration: HttpByteEventRegistration,
}

struct TransportState {
    owner_thread: ThreadId,
    /// The host of the worker context that built the transport, which every
    /// operation's context shares (`check_transport_context`). Writes time out
    /// on it.
    host: Host,
    next_operation: u64,
    active_read: Option<ActiveRead>,
    active_write: Option<ActiveWrite>,
    deferred_eof: bool,
    write_shutdown: bool,
    close_state: CloseState,
    graceful_close_context: Option<WorkerContext>,
    registrations: Vec<PendingByteEventRegistration>,
}

impl TransportState {
    fn new(owner_thread: ThreadId, host: Host) -> Self {
        Self {
            owner_thread,
            host,
            next_operation: 1,
            active_read: None,
            active_write: None,
            deferred_eof: false,
            write_shutdown: false,
            close_state: CloseState::Open,
            graceful_close_context: None,
            registrations: Vec::new(),
        }
    }

    fn assert_owner(&self) {
        assert_eq!(current_id(), self.owner_thread);
    }

    fn next_operation(&mut self) -> u64 {
        let operation = self.next_operation;
        self.next_operation = self.next_operation.wrapping_add(1);
        operation
    }

    fn take_registrations_for_write(&mut self, operation: u64) -> Vec<HttpByteEventRegistration> {
        let mut retained = Vec::with_capacity(self.registrations.len());
        let mut removed = Vec::new();
        for pending in std::mem::take(&mut self.registrations) {
            if pending.write_operation == Some(operation) {
                removed.push(pending.registration);
            } else {
                retained.push(pending);
            }
        }
        self.registrations = retained;
        removed
    }

    fn take_write_notifications(
        &mut self,
        operation: u64,
        flags: ByteEventWriteFlags,
    ) -> Vec<HttpByteEventRegistration> {
        let mut requested = 0;
        if flags.timestamp_tx {
            requested |= ByteEventType::NicTx.bit();
        }
        if flags.timestamp_ack {
            requested |= ByteEventType::CumulativeAck.bit();
        }
        if requested == 0 {
            return Vec::new();
        }
        let mut retained = Vec::with_capacity(self.registrations.len());
        let mut notifications = Vec::new();
        for pending in std::mem::take(&mut self.registrations) {
            if pending.write_operation != Some(operation) {
                retained.push(pending);
                continue;
            }
            for registration in pending.registration.into_event_registrations() {
                if registration.pending_events() & requested != 0 {
                    notifications.push(registration);
                } else {
                    retained.push(PendingByteEventRegistration {
                        write_operation: Some(operation),
                        registration,
                    });
                }
            }
        }
        self.registrations = retained;
        notifications
    }

    fn take_all_registrations(&mut self) -> Vec<HttpByteEventRegistration> {
        std::mem::take(&mut self.registrations)
            .into_iter()
            .map(|pending| pending.registration)
            .collect()
    }
}

/// An owned, fixed-worker H1/H2 byte-stream transport.
///
/// Concrete Tokio or socket values are confined to `ByteStreamIo`
/// implementations inside this crate. The public contract uses only Roxygen
/// buffers, addresses, errors, cancellation, and byte-event metadata.
pub struct OwnedByteStream {
    io: Weak<dyn ByteStreamIo>,
    state: Rc<RefCell<TransportState>>,
    worker: WorkerHandle,
    _worker_guard: WorkerGuard,
    cleanup_registration: Option<WorkerCleanupRegistration>,
    limits: ByteStreamLimits,
    local_endpoint: TransportEndpoint,
    peer_endpoint: TransportEndpoint,
    peer_identity: Option<PeerIdentity>,
    byte_event_capability: ByteEventCapability,
    retirement: Option<TransportRetirement>,
}

pub(crate) struct OwnedTransportReset {
    io: Rc<dyn ByteStreamIo>,
    state: Rc<RefCell<TransportState>>,
}

impl OwnedTransportReset {
    pub(crate) fn reset(&self) {
        let mut failure = catch_unwind(AssertUnwindSafe(|| {
            transition_close_resources(self.io.as_ref(), &self.state, CloseState::Reset, None)
                .expect("terminal reset cannot fail");
        }))
        .err();
        retain_transport_panic(
            &mut failure,
            catch_unwind(AssertUnwindSafe(|| self.io.close_with_reset())).err(),
        );
        finish_transport_panic(failure, false);
    }
}

impl OwnedByteStream {
    pub(crate) fn terminal_reset(&self) -> Result<OwnedTransportReset, HttpError> {
        self.assert_owner();
        Ok(OwnedTransportReset {
            io: self.io.upgrade().ok_or_else(dropped_error)?,
            state: Rc::clone(&self.state),
        })
    }

    /// The transport's worker cleanup becomes the only owner of `io`. An error
    /// drops `io` before returning.
    pub(crate) fn new<I: ByteStreamIo>(
        io: I,
        worker: WorkerHandle,
        context: &WorkerContext,
        limits: ByteStreamLimits,
        local_address: SocketAddress,
        peer_address: SocketAddress,
        peer_identity: Option<PeerIdentity>,
        byte_event_capability: ByteEventCapability,
    ) -> Result<Self, HttpError> {
        Self::from_rc(
            Rc::new(io),
            worker,
            context,
            limits,
            local_address,
            peer_address,
            peer_identity,
            byte_event_capability,
        )
    }

    fn from_rc(
        io: Rc<dyn ByteStreamIo>,
        worker: WorkerHandle,
        context: &WorkerContext,
        limits: ByteStreamLimits,
        local_address: SocketAddress,
        peer_address: SocketAddress,
        peer_identity: Option<PeerIdentity>,
        byte_event_capability: ByteEventCapability,
    ) -> Result<Self, HttpError> {
        let owner_thread = current_id();
        if !context.is_owner_thread() || !worker.same_worker(context.handle()) {
            return Err(HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"byte-stream transport requires its exact worker context",
            ));
        }
        if worker.state() != WorkerState::Open {
            return Err(dropped_error());
        }
        let state = Rc::new(RefCell::new(TransportState::new(
            owner_thread,
            context.host().clone(),
        )));
        let cleanup_io = Rc::clone(&io);
        let cleanup_state = Rc::clone(&state);
        let cleanup_registration = context
            .register_cleanup(move || {
                transition_close_resources(
                    cleanup_io.as_ref(),
                    &cleanup_state,
                    CloseState::Dropped,
                    None,
                )
                .expect("dropping a byte-stream backend cannot fail");
            })
            .map_err(|error| {
                HttpError::with_message(
                    HttpErrorCode::INVALID_STATE_TRANSITION,
                    error.to_string().as_bytes(),
                )
            })?;
        Ok(Self {
            io: Rc::downgrade(&io),
            state,
            worker,
            _worker_guard: context.acquire_guard(),
            cleanup_registration: Some(cleanup_registration),
            limits,
            local_endpoint: TransportEndpoint::Socket(local_address),
            peer_endpoint: TransportEndpoint::Socket(peer_address),
            peer_identity,
            byte_event_capability,
            retirement: None,
        })
    }

    pub(crate) fn with_retirement(mut self, retirement: TransportRetirement) -> Self {
        self.retirement = Some(retirement);
        self
    }

    /// Returns a cached, non-owning retirement observer when the backend supports it.
    ///
    /// `None` means the backend does not support the observation, not that cleanup
    /// has completed. The observer stays available after the backend has retired.
    pub fn retirement(&self) -> Option<TransportRetirement> {
        self.assert_owner();
        self.retirement.clone()
    }

    pub(crate) fn with_endpoints(
        mut self,
        local: TransportEndpoint,
        peer: TransportEndpoint,
    ) -> Self {
        self.local_endpoint = local;
        self.peer_endpoint = peer;
        self
    }

    pub fn worker_id(&self, context: &WorkerContext) -> Result<WorkerId, HttpError> {
        self.check_context(context)?;
        Ok(self.worker.id())
    }

    pub fn limits(&self, context: &WorkerContext) -> Result<ByteStreamLimits, HttpError> {
        self.check_context(context)?;
        Ok(self.limits)
    }

    pub fn enable_zero_copy(
        &self,
        context: &WorkerContext,
        threshold: usize,
    ) -> Result<bool, HttpError> {
        self.check_context(context)?;
        let Some(io) = self.io.upgrade() else {
            return Err(dropped_error());
        };
        io.enable_zero_copy(context, threshold)
    }

    /// Sets the interval after which zero-copy is re-enabled following `ENOBUFS`.
    ///
    /// A nonzero interval `N` permits zero-copy again on the `N`th later write
    /// attempt (and therefore skips `N - 1` attempts). Zero leaves zero-copy
    /// disabled until the caller explicitly enables it again.
    pub fn set_zero_copy_reenable_threshold(
        &self,
        context: &WorkerContext,
        threshold: usize,
    ) -> Result<(), HttpError> {
        self.check_context(context)?;
        let Some(io) = self.io.upgrade() else {
            return Err(dropped_error());
        };
        io.set_zero_copy_reenable_threshold(threshold)
    }

    /// Makes every later reset of this stream close the socket without `SO_LINGER`
    /// zero, as dropping a Tokio stream does.
    ///
    /// This covers `close_with_reset` and the resets that write failures and abandoned
    /// writes cause. The kernel still sends the bytes already written, then FIN, or RST
    /// only if received bytes are unread. The transport-side effects of a reset are
    /// unchanged. Pending reads and writes fail, unwritten bytes are discarded, and byte
    /// events are cancelled.
    pub fn enable_non_abortive_close(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.check_context(context)?;
        let Some(io) = self.io.upgrade() else {
            return Err(dropped_error());
        };
        io.enable_non_abortive_close();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn zero_copy_owner_for_test(
        &self,
        context: &WorkerContext,
    ) -> Result<Option<usize>, HttpError> {
        self.check_context(context)?;
        let Some(io) = self.io.upgrade() else {
            return Err(dropped_error());
        };
        Ok(io.zero_copy_owner_for_test())
    }

    #[cfg(test)]
    pub(crate) fn zero_copy_pending_for_test(
        &self,
        context: &WorkerContext,
    ) -> Result<usize, HttpError> {
        self.check_context(context)?;
        let Some(io) = self.io.upgrade() else {
            return Err(dropped_error());
        };
        Ok(io.zero_copy_pending_for_test())
    }

    pub fn has_pending_read(&self, context: &WorkerContext) -> Result<bool, HttpError> {
        self.check_context(context)?;
        Ok(self.state.borrow().active_read.is_some())
    }

    pub fn has_pending_write(&self, context: &WorkerContext) -> Result<bool, HttpError> {
        self.check_context(context)?;
        Ok(self.state.borrow().active_write.is_some())
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "consumed by the later byte-event delivery adapter"
        )
    )]
    pub(crate) fn take_byte_event_registrations(
        &self,
        context: &WorkerContext,
    ) -> Result<Vec<HttpByteEventRegistration>, HttpError> {
        self.check_context(context)?;
        let mut state = self.state.borrow_mut();
        state.assert_owner();
        Ok(state.take_all_registrations())
    }

    fn begin_read(
        &self,
        context: &WorkerContext,
        maximum: usize,
        allocation_size: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<TransportRead, HttpError>> {
        match start_read_operation(
            self.read_target(),
            context,
            maximum,
            allocation_size,
            ReadKind::Ordinary {
                timeout,
                cancellation,
            },
        ) {
            ReadStart::Started { operation, maximum } => Box::pin(ReadOperation::new(
                self.io.clone(),
                Rc::clone(&self.state),
                operation,
                maximum,
            )),
            ReadStart::Finished(result) => Box::pin(std::future::ready(result)),
        }
    }

    fn read_target(&self) -> ReadTarget<'_> {
        ReadTarget {
            io: &self.io,
            state: &self.state,
            worker: &self.worker,
            limits: self.limits,
        }
    }

    fn begin_write(
        &self,
        context: &WorkerContext,
        data: BufQueue,
        flags: ByteEventWriteFlags,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        match start_write_operation(
            self.write_target(),
            context,
            data,
            flags,
            WriteKind::Ordinary {
                timeout,
                cancellation,
            },
        ) {
            WriteStart::Started {
                operation,
                expected,
                progress,
            } => Box::pin(WriteOperation::new(
                self.io.clone(),
                Rc::clone(&self.state),
                operation,
                expected,
                progress,
            )),
            WriteStart::Finished(result) => Box::pin(std::future::ready(result)),
            WriteStart::WrittenThrough(_) | WriteStart::Unwinding { .. } => {
                unreachable!("only a queued write writes through")
            }
        }
    }

    fn write_target(&self) -> WriteTarget<'_> {
        WriteTarget {
            io: &self.io,
            state: &self.state,
            worker: &self.worker,
            limits: self.limits,
            byte_event_capability: self.byte_event_capability,
        }
    }

    fn transition_close(
        &self,
        close_state: CloseState,
        context: Option<&WorkerContext>,
    ) -> Result<(), HttpError> {
        self.assert_owner();
        if let Some(io) = self.io.upgrade() {
            transition_close_resources(io.as_ref(), &self.state, close_state, context)?;
        }
        Ok(())
    }

    fn assert_owner(&self) {
        assert_transport_owner(&self.state, &self.worker);
    }

    fn check_context(&self, context: &WorkerContext) -> Result<(), HttpError> {
        check_transport_context(&self.state, &self.worker, context)
    }

    fn check_identity(&self, context: &WorkerContext) -> Result<(), HttpError> {
        check_transport_identity(&self.worker, context)
    }
}

/// What a write needs from its transport: borrowed from an [`OwnedByteStream`], or from the
/// handles its [`OwnedWriteQueue`] holds.
#[derive(Clone, Copy)]
struct WriteTarget<'a> {
    io: &'a Weak<dyn ByteStreamIo>,
    state: &'a Rc<RefCell<TransportState>>,
    worker: &'a WorkerHandle,
    limits: ByteStreamLimits,
    byte_event_capability: ByteEventCapability,
}

impl WriteTarget<'_> {
    fn check_context(&self, context: &WorkerContext) -> Result<(), HttpError> {
        check_transport_context(self.state, self.worker, context)
    }

    fn transition_close(
        &self,
        close_state: CloseState,
        context: Option<&WorkerContext>,
    ) -> Result<(), HttpError> {
        assert_transport_owner(self.state, self.worker);
        if let Some(io) = self.io.upgrade() {
            transition_close_resources(io.as_ref(), self.state, close_state, context)?;
        }
        Ok(())
    }
}

/// What a read needs from its transport: borrowed from an [`OwnedByteStream`], or from the
/// handles its [`OwnedReader`] holds.
#[derive(Clone, Copy)]
struct ReadTarget<'a> {
    io: &'a Weak<dyn ByteStreamIo>,
    state: &'a Rc<RefCell<TransportState>>,
    worker: &'a WorkerHandle,
    limits: ByteStreamLimits,
}

impl ReadTarget<'_> {
    fn check_context(&self, context: &WorkerContext) -> Result<(), HttpError> {
        check_transport_context(self.state, self.worker, context)
    }
}

/// A read that [`start_read_operation`] began, or the result it ended with at entry.
enum ReadStart {
    /// The transport's read `operation`, with its clamped bounds, for the
    /// [`ReadOperation`] that the caller builds or restarts.
    Started {
        operation: u64,
        maximum: usize,
    },
    Finished(Result<TransportRead, HttpError>),
}

/// How [`start_read_operation`] reads.
enum ReadKind<'a> {
    /// An ordinary read, timed by `timeout` and observing `cancellation`.
    Ordinary {
        timeout: Duration,
        cancellation: CancellationToken,
    },
    /// An [`OwnedReader`]'s read: untimed, its I/O state kept by the backend's
    /// [`PolledReader`] when it has one, and observing the reader's
    /// `cancellation`.
    Reader { cancellation: &'a CancellationToken },
}

impl ReadKind<'_> {
    fn cancellation(&self) -> &CancellationToken {
        match self {
            Self::Ordinary { cancellation, .. } => cancellation,
            Self::Reader { cancellation } => cancellation,
        }
    }
}

/// Runs `read`'s entry checks and clamps. Returns the clamped bounds of an
/// admitted read with the result of `admit`, which runs under the same borrow
/// of the transport's state, or else the result that the read ends with at
/// entry. Inlined into each caller.
#[inline(always)]
fn admit_read<T>(
    target: ReadTarget<'_>,
    context: &WorkerContext,
    maximum: usize,
    allocation_size: usize,
    cancellation: &CancellationToken,
    admit: impl FnOnce(&mut TransportState) -> T,
) -> Result<(usize, usize, T), Result<TransportRead, HttpError>> {
    target.check_context(context).map_err(Err)?;
    if cancellation.is_cancellation_requested() {
        return Err(Err(cancelled_error(
            b"byte-stream read cancelled before entry",
        )));
    }
    if maximum == 0 {
        return Err(Ok(TransportRead::empty()));
    }
    if allocation_size == 0 {
        return Err(Err(HttpError::with_message(
            HttpErrorCode::INVALID_STATE_TRANSITION,
            b"byte-stream read allocation size must be non-zero",
        )));
    }

    let maximum = maximum.min(target.limits.maximum_read_size);
    let allocation_size = allocation_size.min(maximum);
    let mut state = target.state.borrow_mut();
    match state.close_state {
        CloseState::Open => {}
        CloseState::Graceful => return Err(Ok(TransportRead::eof())),
        CloseState::Reset => return Err(Err(read_error(b"byte-stream was reset"))),
        CloseState::Dropped => return Err(Err(dropped_error())),
    }
    if state.deferred_eof {
        state.deferred_eof = false;
        return Err(Ok(TransportRead::eof()));
    }
    if state.active_read.is_some() {
        return Err(Err(HttpError::with_message(
            HttpErrorCode::INVALID_STATE_TRANSITION,
            b"byte-stream already has a pending read",
        )));
    }
    Ok((maximum, allocation_size, admit(&mut state)))
}

/// Inlined into each caller, which then compiles for its own `kind`.
#[inline(always)]
fn start_read_operation(
    target: ReadTarget<'_>,
    context: &WorkerContext,
    maximum: usize,
    allocation_size: usize,
    kind: ReadKind<'_>,
) -> ReadStart {
    let (maximum, allocation_size, operation) = match admit_read(
        target,
        context,
        maximum,
        allocation_size,
        kind.cancellation(),
        TransportState::next_operation,
    ) {
        Ok(admitted) => admitted,
        Err(result) => return ReadStart::Finished(result),
    };

    let Some(io) = target.io.upgrade() else {
        return ReadStart::Finished(Err(dropped_error()));
    };
    let drive = match kind {
        ReadKind::Ordinary {
            timeout,
            cancellation,
        } => {
            let io_future = io.read(maximum, allocation_size);
            #[cfg(test)]
            READ_DEADLINE_BATONS.with(|count| count.set(count.get() + 1));
            let deadline = TimedBaton::with_host(Some(timeout), context.host().clone());
            let wait = Box::pin(deadline.wait(cancellation));
            ReadDrive::Timed {
                io_future,
                deadline,
                wait,
            }
        }
        ReadKind::Reader { .. } => {
            let io = match io.polled_reader() {
                Some(reader) => {
                    reader.start_read(maximum, allocation_size);
                    ActiveReadIo::Polled(PolledReadIo(reader))
                }
                None => ActiveReadIo::Future(io.read(maximum, allocation_size)),
            };
            ReadDrive::Untimed { io }
        }
    };
    #[cfg(test)]
    record_read_registration();
    target.state.borrow_mut().active_read = Some(ActiveRead {
        operation,
        drive,
        waker: None,
    });
    if Rc::strong_count(&io) == 1 {
        release_last_backend_owner(io, target.state, operation);
    } else {
        drop(io);
    }
    ReadStart::Started { operation, maximum }
}

/// Releases `io`, the backend's last owner, after [`start_read_operation`]
/// installed the read `operation`. If the backend's destruction panics, retires
/// that read and resumes the first panic. Cold and out of line, because a read
/// start whose backend has another owner only drops its count.
#[cold]
#[inline(never)]
fn release_last_backend_owner(
    io: Rc<dyn ByteStreamIo>,
    state: &RefCell<TransportState>,
    operation: u64,
) {
    let Err(panic) = catch_unwind(AssertUnwindSafe(|| drop(io))) else {
        return;
    };
    let active = {
        let mut state = state.borrow_mut();
        if state
            .active_read
            .as_ref()
            .is_some_and(|active| active.operation == operation)
        {
            state.active_read.take()
        } else {
            None
        }
    };
    let mut failure = Some(panic);
    retire_transport_value(active, &mut failure);
    resume_unwind(failure.expect("the release's panic is retained first"));
}

/// Drops `io` after the write `operation` was registered, through
/// [`release_last_write_backend_owner`] when it is the backend's last owner.
#[inline(always)]
fn release_write_backend_owner(
    io: Rc<dyn ByteStreamIo>,
    state: &RefCell<TransportState>,
    operation: u64,
) {
    if Rc::strong_count(&io) == 1 {
        release_last_write_backend_owner(io, state, operation);
    } else {
        drop(io);
    }
}

/// Releases `io`, the backend's last owner, after the write `operation` was
/// registered. If the backend's destruction panics, retires that write and
/// resumes the first panic, as [`release_last_backend_owner`] does for reads.
#[cold]
#[inline(never)]
fn release_last_write_backend_owner(
    io: Rc<dyn ByteStreamIo>,
    state: &RefCell<TransportState>,
    operation: u64,
) {
    let Err(panic) = catch_unwind(AssertUnwindSafe(|| drop(io))) else {
        return;
    };
    let active = {
        let mut state = state.borrow_mut();
        if state
            .active_write
            .as_ref()
            .is_some_and(|active| active.operation == operation)
        {
            state.active_write.take()
        } else {
            None
        }
    };
    let mut failure = Some(panic);
    retire_transport_value(active, &mut failure);
    resume_unwind(failure.expect("the release's panic is retained first"));
}

/// A write that [`start_write_operation`] began, or the result it ended with at entry.
enum WriteStart {
    /// The transport's write `operation` of `expected` bytes, and the progress
    /// handle of the [`WriteOperation`] that the caller builds or restarts.
    Started {
        operation: u64,
        expected: usize,
        progress: WriteProgress,
    },
    /// An [`OwnedWriteQueue`] write that its first attempt finished at entry
    /// ([`write_through`]).
    WrittenThrough(Result<WriteInfo, TransportWriteError>),
    Finished(Result<WriteInfo, TransportWriteError>),
    /// Like `Started`, for an [`OwnedWriteQueue`] write whose first attempt
    /// panicked. The caller holds the write as outstanding, then resumes
    /// `panic`.
    Unwinding {
        operation: u64,
        expected: usize,
        progress: WriteProgress,
        panic: TransportPanic,
    },
}

fn assert_transport_owner(state: &RefCell<TransportState>, worker: &WorkerHandle) {
    let state = state.borrow();
    state.assert_owner();
    assert_eq!(worker.owner_thread(), state.owner_thread);
}

fn check_transport_context(
    state: &RefCell<TransportState>,
    worker: &WorkerHandle,
    context: &WorkerContext,
) -> Result<(), HttpError> {
    assert_transport_owner(state, worker);
    check_transport_identity(worker, context)?;
    if worker.state() != WorkerState::Open {
        return Err(dropped_error());
    }
    Ok(())
}

fn check_transport_identity(
    worker: &WorkerHandle,
    context: &WorkerContext,
) -> Result<(), HttpError> {
    // Callers assert the transport owner before checking worker identity.
    if !worker.same_worker(context.handle()) {
        return Err(HttpError::with_message(
            HttpErrorCode::INVALID_STATE_TRANSITION,
            b"byte-stream transport used from a different worker context",
        ));
    }
    Ok(())
}

/// How [`start_write_operation`] writes.
enum WriteKind<'a> {
    /// An ordinary write, timed by `timeout` and observing `cancellation`.
    Ordinary {
        timeout: Duration,
        cancellation: CancellationToken,
    },
    /// An [`OwnedWriteQueue`]'s write: untimed, counting its progress in the
    /// queue's cell and observing the queue's `cancellation`.
    Queued {
        progress: &'a WriteProgress,
        cancellation: &'a CancellationToken,
    },
}

impl WriteKind<'_> {
    fn cancellation(&self) -> &CancellationToken {
        match self {
            Self::Ordinary { cancellation, .. } => cancellation,
            Self::Queued { cancellation, .. } => cancellation,
        }
    }
}

fn start_write_operation(
    target: WriteTarget<'_>,
    context: &WorkerContext,
    data: BufQueue,
    flags: ByteEventWriteFlags,
    kind: WriteKind<'_>,
) -> WriteStart {
    if let Err(error) = target.check_context(context) {
        return WriteStart::Finished(Err(TransportWriteError::new(error, 0)));
    }
    if flags != ByteEventWriteFlags::default()
        && target.byte_event_capability == ByteEventCapability::Unsupported
    {
        return WriteStart::Finished(Err(TransportWriteError::new(
            write_error(b"byte-event write flags are unsupported by this transport"),
            0,
        )));
    }
    if kind.cancellation().is_cancellation_requested() {
        let _ = target.transition_close(CloseState::Reset, Some(context));
        return WriteStart::Finished(Err(TransportWriteError::new(
            cancelled_error(b"byte-stream write cancelled before entry"),
            0,
        )));
    }
    let length = data.chain_length();
    if length > target.limits.maximum_write_size {
        return WriteStart::Finished(Err(TransportWriteError::new(
            write_error(b"byte-stream write exceeds the configured bound"),
            0,
        )));
    }
    if length == 0 && !flags.timestamp_write && !flags.timestamp_tx && !flags.timestamp_ack {
        return WriteStart::Finished(Ok(WriteInfo::default()));
    }

    let (operation, byte_events) = {
        let mut state = target.state.borrow_mut();
        if state.close_state != CloseState::Open || state.write_shutdown {
            return WriteStart::Finished(Err(TransportWriteError::new(
                write_error(b"byte-stream write side is closed"),
                0,
            )));
        }
        if state.active_write.is_some() {
            return WriteStart::Finished(Err(TransportWriteError::new(
                HttpError::with_message(
                    HttpErrorCode::INVALID_STATE_TRANSITION,
                    b"byte-stream already has a pending write",
                ),
                0,
            )));
        }
        if let WriteKind::Queued { progress, .. } = kind
            && target.byte_event_capability == ByteEventCapability::Unsupported
        {
            drop(state);
            return write_through(&target, data, length, progress);
        }
        let operation = state.next_operation();
        for pending in &mut state.registrations {
            if pending.write_operation.is_none() {
                pending.write_operation = Some(operation);
            }
        }
        let registrations = state.take_write_notifications(operation, flags);
        (operation, ByteEventWriteState::new(registrations))
    };

    let Some(io) = target.io.upgrade() else {
        return WriteStart::Finished(Err(TransportWriteError::new(dropped_error(), 0)));
    };
    let (progress, wait, queued_writer) = match kind {
        WriteKind::Ordinary {
            timeout,
            cancellation,
        } => {
            io.prepare_write_timeout(timeout);
            (
                WriteProgress::new(length),
                WriteWait::Unarmed {
                    timeout,
                    cancellation,
                    started: None,
                },
                None,
            )
        }
        WriteKind::Queued { progress, .. } => {
            io.prepare_write_timeout(Duration::ZERO);
            (
                progress.restart(length),
                WriteWait::Untimed,
                io.queued_writer(),
            )
        }
    };
    let active_io = match queued_writer {
        Some(writer) => {
            writer.start_write(data, progress.clone(), flags, byte_events.clone());
            ActiveIo::Queued(QueuedIo(writer))
        }
        None => ActiveIo::Future(io.write(data, progress.clone(), flags, byte_events.clone())),
    };
    #[cfg(test)]
    WRITE_REGISTRATIONS.with(|count| count.set(count.get() + 1));
    target.state.borrow_mut().active_write = Some(ActiveWrite {
        operation,
        io: active_io,
        byte_events: byte_events.clone(),
        _progress: progress.clone(),
        wait,
        waker: None,
    });
    release_write_backend_owner(io, target.state, operation);
    WriteStart::Started {
        operation,
        expected: length,
        progress,
    }
}

/// Writes an [`OwnedWriteQueue`] submission with one attempt at entry, polled
/// with no waker ([`QueuedWriter::write_through`]). Used only when the
/// transport does not offer byte events, so a byte-event registration never
/// waits for this write. A write that the attempt finishes is neither
/// registered with the transport nor held by its backend. If the attempt
/// leaves the write unfinished or panics, the write is registered as
/// [`start_write_operation`] registers a queued write, and the queue's next
/// poll continues it with the poller's waker.
fn write_through(
    target: &WriteTarget<'_>,
    data: BufQueue,
    length: usize,
    progress: &WriteProgress,
) -> WriteStart {
    let Some(io) = target.io.upgrade() else {
        return WriteStart::Finished(Err(TransportWriteError::new(dropped_error(), 0)));
    };
    let progress = progress.restart(length);
    let Some(writer) = io.queued_writer() else {
        io.prepare_write_timeout(Duration::ZERO);
        let active_io = ActiveIo::Future(io.write(
            data,
            progress.clone(),
            ByteEventWriteFlags::default(),
            None,
        ));
        let operation = register_queued_write(target, active_io, &progress);
        release_write_backend_owner(io, target.state, operation);
        return WriteStart::Started {
            operation,
            expected: length,
            progress,
        };
    };
    match writer.write_through(data, &progress) {
        WriteThrough::Finished(result) => {
            let written = progress.get();
            WriteStart::WrittenThrough(
                finished_write(result, written, length)
                    .map_err(|error| TransportWriteError::new(error, written)),
            )
        }
        WriteThrough::Held => {
            let operation =
                register_queued_write(target, ActiveIo::Queued(QueuedIo(writer)), &progress);
            release_write_backend_owner(io, target.state, operation);
            WriteStart::Started {
                operation,
                expected: length,
                progress,
            }
        }
        WriteThrough::Panicked(panic) => {
            let operation =
                register_queued_write(target, ActiveIo::Queued(QueuedIo(writer)), &progress);
            release_write_backend_owner(io, target.state, operation);
            WriteStart::Unwinding {
                operation,
                expected: length,
                progress,
                panic,
            }
        }
    }
}

/// Registers an unfinished [`write_through`] write with its transport.
fn register_queued_write(target: &WriteTarget<'_>, io: ActiveIo, progress: &WriteProgress) -> u64 {
    let mut state = target.state.borrow_mut();
    debug_assert!(
        state.registrations.is_empty(),
        "a transport without byte events holds no registration"
    );
    let operation = state.next_operation();
    #[cfg(test)]
    WRITE_REGISTRATIONS.with(|count| count.set(count.get() + 1));
    state.active_write = Some(ActiveWrite {
        operation,
        io,
        byte_events: None,
        _progress: progress.clone(),
        wait: WriteWait::Untimed,
        waker: None,
    });
    operation
}

/// A finished backend write's result: all `expected` bytes written, the
/// backend's error, or a partial write's error.
#[inline(always)]
fn finished_write(
    result: Result<(), HttpError>,
    written: usize,
    expected: usize,
) -> Result<WriteInfo, HttpError> {
    match result {
        Ok(()) if written == expected => Ok(WriteInfo {
            bytes_written: written,
        }),
        Ok(()) => Err(write_error(
            b"byte-stream adapter completed a partial write",
        )),
        Err(error) => Err(error),
    }
}

impl ByteStreamTransport for OwnedByteStream {
    fn read(
        &self,
        context: &WorkerContext,
        maximum: usize,
        allocation_size: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<TransportRead, HttpError>> {
        self.begin_read(context, maximum, allocation_size, timeout, cancellation)
    }

    fn write(
        &self,
        context: &WorkerContext,
        data: BufQueue,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        self.begin_write(
            context,
            data,
            ByteEventWriteFlags::default(),
            timeout,
            cancellation,
        )
    }

    fn write_with_byte_events(
        &self,
        context: &WorkerContext,
        data: BufQueue,
        flags: ByteEventWriteFlags,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        self.begin_write(context, data, flags, timeout, cancellation)
    }

    fn shutdown_write(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.check_context(context)?;
        let (should_shutdown, mut registrations) = {
            let mut state = self.state.borrow_mut();
            if state.close_state != CloseState::Open || state.write_shutdown {
                (false, Vec::new())
            } else {
                state.write_shutdown = true;
                (true, state.take_all_registrations())
            }
        };
        let shutdown_result = if should_shutdown {
            self.io
                .upgrade()
                .ok_or_else(dropped_error)?
                .shutdown_write(context)
        } else {
            Ok(())
        };
        if !registrations.is_empty() {
            #[cfg(test)]
            record_close_path_error();
            let error = write_error(b"byte-stream write side was shut down before byte event");
            for registration in &mut registrations {
                registration.cancel(&error, None);
            }
        }
        shutdown_result
    }

    fn close(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.check_context(context)?;
        self.transition_close(CloseState::Graceful, Some(context))
    }

    fn close_with_reset(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.check_context(context)?;
        self.transition_close(CloseState::Reset, Some(context))
    }

    fn local_address(&self, context: &WorkerContext) -> Result<SocketAddress, HttpError> {
        self.check_context(context)?;
        Ok(self
            .local_endpoint
            .socket_address()
            .cloned()
            .unwrap_or_default())
    }

    fn peer_address(&self, context: &WorkerContext) -> Result<SocketAddress, HttpError> {
        self.check_context(context)?;
        Ok(self
            .peer_endpoint
            .socket_address()
            .cloned()
            .unwrap_or_default())
    }

    fn local_endpoint(&self, context: &WorkerContext) -> Result<TransportEndpoint, HttpError> {
        self.check_context(context)?;
        Ok(self.local_endpoint.clone())
    }

    fn peer_endpoint(&self, context: &WorkerContext) -> Result<TransportEndpoint, HttpError> {
        self.check_context(context)?;
        Ok(self.peer_endpoint.clone())
    }

    fn peer_identity(&self, context: &WorkerContext) -> Result<Option<PeerIdentity>, HttpError> {
        self.check_context(context)?;
        Ok(self.peer_identity.clone())
    }

    fn byte_event_capability(
        &self,
        context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError> {
        self.check_context(context)?;
        Ok(self.byte_event_capability)
    }

    fn register_byte_event(
        &self,
        context: &WorkerContext,
        mut registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError> {
        self.assert_owner();
        if self.check_identity(context).is_err() {
            registration.cancel(
                &HttpError::with_message(
                    HttpErrorCode::INVALID_STATE_TRANSITION,
                    b"byte event registered from a different worker context",
                ),
                None,
            );
            return Err(ByteEventRegistrationError::WrongWorker);
        }
        if self.worker.state() != WorkerState::Open {
            registration.cancel(&dropped_error(), None);
            return Err(ByteEventRegistrationError::Closed);
        }
        if self.byte_event_capability == ByteEventCapability::Unsupported {
            registration.cancel(
                &HttpError::with_message(
                    HttpErrorCode::INVALID_STATE_TRANSITION,
                    b"byte events are unsupported by this transport",
                ),
                None,
            );
            return Err(ByteEventRegistrationError::Unsupported);
        }
        let mut state = self.state.borrow_mut();
        if state.close_state != CloseState::Open || state.write_shutdown {
            registration.cancel(&dropped_error(), None);
            return Err(ByteEventRegistrationError::Closed);
        }
        if state.registrations.len() >= self.limits.maximum_byte_event_registrations {
            registration.cancel(
                &HttpError::with_message(
                    HttpErrorCode::ENHANCE_YOUR_CALM,
                    b"byte event registration capacity exhausted",
                ),
                None,
            );
            return Err(ByteEventRegistrationError::CapacityExhausted);
        }
        let write_operation = state
            .active_write
            .as_ref()
            .map(|operation| operation.operation);
        state.registrations.push(PendingByteEventRegistration {
            write_operation,
            registration,
        });
        Ok(())
    }

    fn write_queue(
        &self,
        context: &WorkerContext,
        maximum_write_size: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> Result<Option<OwnedWriteQueue>, HttpError> {
        if !timeout.is_zero()
            || !self
                .io
                .upgrade()
                .is_some_and(|io| io.queued_writer().is_some())
        {
            return Ok(None);
        }
        Ok(Some(OwnedWriteQueue {
            worker: self.worker.clone(),
            limits: self.limits,
            byte_event_capability: self.byte_event_capability,
            context: context.clone(),
            maximum_write_size,
            cancellation_wake: cancellation.wake_registration(),
            cancellation,
            operation: WriteOperation::idle(
                self.io.clone(),
                Rc::clone(&self.state),
                WriteProgress::new(0),
            ),
            held: Held::Free,
            receipts: VecDeque::with_capacity(WRITE_QUEUE_RECEIPTS),
        }))
    }

    fn reader(
        &self,
        context: &WorkerContext,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> Result<Option<OwnedReader>, HttpError> {
        if !timeout.is_zero() {
            return Ok(None);
        }
        Ok(Some(OwnedReader {
            worker: self.worker.clone(),
            limits: self.limits,
            context: context.clone(),
            read: ReaderRead::Idle,
            polled: self
                .io
                .upgrade()
                .and_then(|io| io.polled_reader())
                .filter(|reader| reader.reports_drained())
                .map(|reader| Rc::downgrade(&reader)),
            operation: ReadOperation::idle(self.io.clone(), Rc::clone(&self.state)),
            cancellation: ReaderCancellation::new(cancellation),
        }))
    }
}

impl Drop for OwnedByteStream {
    fn drop(&mut self) {
        let unwinding = std::thread::panicking();
        let mut failure = catch_unwind(AssertUnwindSafe(|| {
            let _ = self.transition_close(CloseState::Dropped, None);
        }))
        .err();
        if let Some(registration) = self.cleanup_registration.take() {
            retain_transport_panic(
                &mut failure,
                catch_unwind(AssertUnwindSafe(|| {
                    let _ = registration.disarm();
                }))
                .err(),
            );
        }
        finish_transport_panic(failure, unwinding);
    }
}

struct ReadOperation {
    io: Weak<dyn ByteStreamIo>,
    state: Rc<RefCell<TransportState>>,
    operation: u64,
    maximum: usize,
    complete: bool,
}

impl ReadOperation {
    fn new(
        io: Weak<dyn ByteStreamIo>,
        state: Rc<RefCell<TransportState>>,
        operation: u64,
        maximum: usize,
    ) -> Self {
        #[cfg(test)]
        READ_OPERATIONS.with(|count| count.set(count.get() + 1));
        Self {
            io,
            state,
            operation,
            maximum,
            complete: false,
        }
    }

    /// An [`OwnedReader`]'s operation slot, complete until its first read.
    fn idle(io: Weak<dyn ByteStreamIo>, state: Rc<RefCell<TransportState>>) -> Self {
        let mut slot = Self::new(io, state, 0, 0);
        slot.complete = true;
        slot
    }

    /// Reuses a complete slot for the transport's read `operation`.
    fn restart(&mut self, operation: u64, maximum: usize) {
        debug_assert!(self.complete, "only a complete read's slot is reused");
        self.operation = operation;
        self.maximum = maximum;
        self.complete = false;
    }

    fn finish(&mut self, cancel: Option<HttpErrorCode>) {
        if self.complete {
            return;
        }
        let active = {
            let mut state = self.state.borrow_mut();
            if state
                .active_read
                .as_ref()
                .is_some_and(|active| active.operation == self.operation)
            {
                state.active_read.take()
            } else {
                None
            }
        };
        if let Some(active) = active {
            match cancel {
                Some(code) => Self::cancel_active(&self.io, active, code),
                None => drop(active),
            }
        }
        self.complete = true;
    }

    /// Ends `active` with `code`. The backend's cancellation releases the
    /// socket waker, and dropping that waker can start a replacement read. So
    /// an [`OwnedReader`] read's state is detached and its guard dropped first,
    /// and its buffers and waker drop after the cancellation returns or
    /// unwinds. Cold and out of line, so that the `finish(None)` that ends each
    /// completed read stays small.
    #[cold]
    #[inline(never)]
    fn cancel_active(io: &Weak<dyn ByteStreamIo>, active: ActiveRead, code: HttpErrorCode) {
        let Some(io) = io.upgrade() else {
            drop(active);
            return;
        };
        let ActiveRead { drive, waker, .. } = active;
        let mut io = Some(io);
        let mut cancel_io = || {
            io.take()
                .expect("a read's cancellation runs once")
                .cancel_read(code);
        };
        match drive {
            ReadDrive::Untimed { io: active_io } => active_io.cancel(&mut cancel_io),
            timed @ ReadDrive::Timed { .. } => {
                cancel_io();
                drop(timed);
            }
        }
        drop(waker);
    }

    fn close_error(close_state: CloseState) -> Option<HttpError> {
        match close_state {
            CloseState::Open => None,
            CloseState::Graceful => None,
            CloseState::Reset => Some(read_error(b"byte-stream was reset")),
            CloseState::Dropped => Some(dropped_error()),
        }
    }
}

impl Future for ReadOperation {
    type Output = Result<TransportRead, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().poll_with(context, None, false)
    }
}

impl ReadOperation {
    /// Polls the read. An [`OwnedReader`]'s read observes `reader`'s
    /// cancellation, and with `arm` its backend reports whether the read
    /// drained the stream (`PolledReader::poll_read`). An ordinary read passes
    /// neither. Inlined into each caller, which then compiles for its own kind
    /// of read.
    #[inline(always)]
    fn poll_with(
        &mut self,
        context: &mut Context<'_>,
        reader: Option<&mut ReaderCancellation>,
        arm: bool,
    ) -> Poll<Result<TransportRead, HttpError>> {
        if let Some(result) = self.closed() {
            return Poll::Ready(result);
        }

        enum ReadPoll {
            Pending,
            Cancelled,
            TimedOut,
            Io(Result<TransportRead, HttpError>),
        }
        impl ReadPoll {
            /// Inlined into each kind of read's poll.
            #[inline(always)]
            fn new(io: Poll<Result<TransportRead, HttpError>>, wait: Poll<WaitStatus>) -> Self {
                match (io, wait) {
                    (Poll::Ready(Err(error)), _) => Self::Io(Err(error)),
                    (
                        Poll::Ready(Ok(result)),
                        Poll::Ready(WaitStatus::Cancelled | WaitStatus::TimedOut),
                    ) if result.is_eof() || result.bytes_read() != 0 => Self::Io(Ok(result)),
                    (_, Poll::Ready(WaitStatus::Cancelled)) => Self::Cancelled,
                    (_, Poll::Ready(WaitStatus::TimedOut)) => Self::TimedOut,
                    (Poll::Ready(result), _) => Self::Io(result),
                    (Poll::Pending, _) => Self::Pending,
                }
            }
        }
        let mut outcome = {
            let mut state = self.state.borrow_mut();
            let active = state
                .active_read
                .as_mut()
                .filter(|active| active.operation == self.operation)
                .expect("an open read operation remains registered with its transport");
            if active
                .waker
                .as_ref()
                .is_none_or(|stored| !stored.will_wake(context.waker()))
            {
                active.waker = Some(context.waker().clone());
            }
            match &mut active.drive {
                ReadDrive::Timed { deadline, .. } if deadline.status() == WaitStatus::TimedOut => {
                    ReadPoll::TimedOut
                }
                ReadDrive::Timed {
                    io_future, wait, ..
                } => {
                    let io = io_future.as_mut().poll(context);
                    let wait = wait.as_mut().poll(context);
                    ReadPoll::new(io, wait)
                }
                ReadDrive::Untimed { io } => {
                    let reader = reader
                        .as_deref()
                        .expect("an OwnedReader read polls with its reader's cancellation");
                    let io = io.poll(context, arm);
                    let wait = if reader.token.is_cancellation_requested() {
                        Poll::Ready(WaitStatus::Cancelled)
                    } else {
                        Poll::Pending
                    };
                    ReadPoll::new(io, wait)
                }
            }
        };
        // An `OwnedReader` read's cancellation wake follows the waker once the
        // borrow ends, because a waker that the update releases can start a
        // replacement read.
        if let ReadPoll::Pending = outcome
            && let Some(reader) = reader
            && reader.listener.update_waker(context.waker())
        {
            outcome = ReadPoll::Cancelled;
        }
        let result = match outcome {
            ReadPoll::Pending => return Poll::Pending,
            ReadPoll::Cancelled => {
                self.finish(Some(HttpErrorCode::CORO_CANCELLED));
                return Poll::Ready(Err(cancelled_error(b"byte-stream read cancelled")));
            }
            ReadPoll::TimedOut => {
                self.finish(Some(HttpErrorCode::READ_TIMEOUT));
                return Poll::Ready(Err(HttpError::with_message(
                    HttpErrorCode::READ_TIMEOUT,
                    b"byte-stream read timed out",
                )));
            }
            ReadPoll::Io(result) => result,
        };
        let result = bounded_read(result, self.maximum);
        // A read that drained its stream stays registered for its reader
        // (`OwnedReader::settle_drained`).
        if !(arm && is_drained(&result)) {
            self.finish(None);
        }
        Poll::Ready(deliver_read(&self.state, result))
    }

    /// Finishes the read and returns its result if the transport is closed,
    /// reset or dropped.
    #[inline(always)]
    fn closed(&mut self) -> Option<Result<TransportRead, HttpError>> {
        let close_state = {
            let state = self.state.borrow();
            state.assert_owner();
            state.close_state
        };
        if let Some(error) = Self::close_error(close_state) {
            self.finish(None);
            return Some(Err(error));
        }
        if close_state == CloseState::Graceful {
            self.finish(None);
            return Some(Ok(TransportRead::eof()));
        }
        None
    }

    /// Makes the read's registration, if the transport still holds it, wake
    /// `context`'s waker.
    fn rewake(&self, context: &Context<'_>) {
        let replaced = {
            let mut state = self.state.borrow_mut();
            let Some(active) = state
                .active_read
                .as_mut()
                .filter(|active| active.operation == self.operation)
            else {
                return;
            };
            if active
                .waker
                .as_ref()
                .is_some_and(|stored| stored.will_wake(context.waker()))
            {
                return;
            }
            active.waker.replace(context.waker().clone())
        };
        drop(replaced);
    }

    /// Gives the registered read the bound of the read it now holds.
    fn rebound(&mut self, maximum: usize) {
        debug_assert!(!self.complete, "only a registered read is rebounded");
        self.maximum = maximum;
    }
}

/// A completed read's result within its bound (`normalize_read`).
#[inline(always)]
fn bounded_read(
    result: Result<TransportRead, HttpError>,
    maximum: usize,
) -> Result<TransportRead, HttpError> {
    match result {
        Ok(result) => normalize_read(result, maximum),
        Err(error) => Err(error),
    }
}

fn is_drained(result: &Result<TransportRead, HttpError>) -> bool {
    matches!(result, Ok(read) if read.drained)
}

/// What a completed read returns: an EOF that arrives with bytes is deferred to
/// the next read, and a read with neither bytes nor EOF fails.
#[inline(always)]
fn deliver_read(
    state: &RefCell<TransportState>,
    result: Result<TransportRead, HttpError>,
) -> Result<TransportRead, HttpError> {
    match result {
        Ok(result) if result.is_eof() && result.bytes_read() != 0 => {
            state.borrow_mut().deferred_eof = true;
            Ok(TransportRead::from_io(result.into_data(), false))
        }
        Ok(result) if !result.is_eof() && result.bytes_read() == 0 => {
            Err(read_error(b"byte-stream adapter made no read progress"))
        }
        result => result,
    }
}

impl Drop for ReadOperation {
    fn drop(&mut self) {
        if !self.complete {
            self.state.borrow().assert_owner();
            self.finish(Some(HttpErrorCode::CORO_CANCELLED));
        }
    }
}

struct WriteOperation {
    io: Weak<dyn ByteStreamIo>,
    state: Rc<RefCell<TransportState>>,
    operation: u64,
    expected: usize,
    progress: WriteProgress,
    complete: bool,
}

impl WriteOperation {
    fn new(
        io: Weak<dyn ByteStreamIo>,
        state: Rc<RefCell<TransportState>>,
        operation: u64,
        expected: usize,
        progress: WriteProgress,
    ) -> Self {
        #[cfg(test)]
        WRITE_OPERATIONS.with(|count| count.set(count.get() + 1));
        Self {
            io,
            state,
            operation,
            expected,
            progress,
            complete: false,
        }
    }

    /// An [`OwnedWriteQueue`]'s operation slot, complete until its first write.
    fn idle(
        io: Weak<dyn ByteStreamIo>,
        state: Rc<RefCell<TransportState>>,
        progress: WriteProgress,
    ) -> Self {
        let mut slot = Self::new(io, state, 0, 0, progress);
        slot.complete = true;
        slot
    }

    /// Reuses a complete slot for the transport's write `operation`.
    fn restart(&mut self, operation: u64, expected: usize, progress: WriteProgress) {
        debug_assert!(self.complete, "only a complete write's slot is reused");
        self.operation = operation;
        self.expected = expected;
        self.progress = progress;
        self.complete = false;
    }

    fn finish(
        &mut self,
        cancel: Option<HttpErrorCode>,
        registration_error: Option<&HttpError>,
    ) -> usize {
        if self.complete {
            return self.progress.get();
        }
        let (mut active, mut registrations, graceful_close_context) = {
            let mut state = self.state.borrow_mut();
            let active = if state
                .active_write
                .as_ref()
                .is_some_and(|active| active.operation == self.operation)
            {
                state.active_write.take()
            } else {
                None
            };
            let registrations = if registration_error.is_some() {
                state.take_registrations_for_write(self.operation)
            } else {
                Vec::new()
            };
            let graceful_close_context = if active.is_some()
                && state.close_state == CloseState::Graceful
                && state.active_write.is_none()
            {
                state.graceful_close_context.take()
            } else {
                None
            };
            (active, registrations, graceful_close_context)
        };
        // Borrowed, so that a panic from the cancellations below unwinds
        // `graceful_close_context`, then `registrations`, then `active`. The
        // wait is retired first, so that `active`'s drop still wakes the task.
        if let Some(active) = active.as_mut() {
            if matches!(active.wait, WriteWait::Unarmed { .. }) {
                active.wait = WriteWait::Retired {
                    _wake: CompletionWake(active.waker.take()),
                };
            }
            if let Some(code) = cancel {
                if let Some(io) = self.io.upgrade() {
                    io.cancel_write(code);
                }
            }
            if let Some(error) = registration_error {
                if let Some(byte_events) = active.byte_events.as_ref() {
                    byte_events.cancel(error);
                }
            }
        }
        if active.is_some() {
            drop(active);
        }
        if let Some(error) = registration_error {
            for registration in &mut registrations {
                registration.cancel(error, None);
            }
        }
        if let Some(context) = graceful_close_context
            && let Some(io) = self.io.upgrade()
            && io.close(&context, false).is_err()
        {
            transition_close_resources(io.as_ref(), &self.state, CloseState::Reset, None)
                .expect("resetting after failed graceful shutdown cannot fail");
        }
        self.complete = true;
        self.progress.get()
    }

    fn reset_transport(&mut self) -> usize {
        if self.complete {
            return self.progress.get();
        }
        self.complete = true;
        if let Some(io) = self.io.upgrade() {
            transition_close_resources(io.as_ref(), &self.state, CloseState::Reset, None)
                .expect("resetting a byte-stream backend cannot fail");
        }
        self.progress.get()
    }

    fn close_error(&self) -> Option<HttpError> {
        match self.state.borrow().close_state {
            CloseState::Open => None,
            CloseState::Graceful => None,
            CloseState::Reset => Some(write_error(b"byte-stream write was closed")),
            CloseState::Dropped => Some(dropped_error()),
        }
    }
}

impl Future for WriteOperation {
    type Output = Result<WriteInfo, TransportWriteError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().poll_with(context, None)
    }
}

impl WriteOperation {
    /// Polls the write. An [`OwnedWriteQueue`]'s write observes the queue's
    /// `cancellation`, and an ordinary write does not pass one. Inlined into
    /// each caller, which then compiles for its own kind of write.
    #[inline(always)]
    fn poll_with(
        &mut self,
        context: &mut Context<'_>,
        queue_cancellation: Option<&CancellationToken>,
    ) -> Poll<Result<WriteInfo, TransportWriteError>> {
        self.state.borrow().assert_owner();
        if let Some(error) = self.close_error() {
            let written = self.finish(None, None);
            return Poll::Ready(Err(TransportWriteError::new(error, written)));
        }

        enum WritePoll {
            Pending,
            Cancelled,
            TimedOut,
            Io(Result<(), HttpError>),
        }
        let outcome = {
            let mut state = self.state.borrow_mut();
            let TransportState {
                active_write, host, ..
            } = &mut *state;
            let active = active_write
                .as_mut()
                .filter(|active| active.operation == self.operation)
                .expect("an open write operation remains registered with its transport");
            active.waker = Some(context.waker().clone());
            match &mut active.wait {
                WriteWait::Unarmed {
                    timeout,
                    cancellation,
                    started,
                } => {
                    if cancellation.is_cancellation_requested() {
                        WritePoll::Cancelled
                    } else {
                        if started.is_none() && !timeout.is_zero() {
                            *started = Some(host.now());
                        }
                        let started = *started;
                        let io = catch_unwind(AssertUnwindSafe(|| active.io.poll(context)));
                        match io {
                            Ok(Poll::Ready(result)) => WritePoll::Io(result),
                            Ok(Poll::Pending) => {
                                arm_write_deadline(&mut active.wait, started, host, context);
                                WritePoll::Pending
                            }
                            Err(panic) => {
                                // Arm the deadline and cancellation before the
                                // panic resumes, so that a re-poll has them.
                                arm_write_deadline(&mut active.wait, started, host, context);
                                resume_unwind(panic);
                            }
                        }
                    }
                }
                WriteWait::Armed { deadline, wait } => match deadline.status() {
                    WaitStatus::TimedOut => WritePoll::TimedOut,
                    WaitStatus::Cancelled => WritePoll::Cancelled,
                    WaitStatus::NotReady | WaitStatus::Signalled => {
                        match wait.as_mut().poll(context) {
                            Poll::Ready(WaitStatus::Cancelled) => WritePoll::Cancelled,
                            Poll::Ready(WaitStatus::TimedOut) => WritePoll::TimedOut,
                            Poll::Pending if deadline.status() == WaitStatus::TimedOut => {
                                WritePoll::Pending
                            }
                            Poll::Ready(WaitStatus::Signalled | WaitStatus::NotReady)
                            | Poll::Pending => {
                                let io = active.io.poll(context);
                                if deadline.status() == WaitStatus::TimedOut {
                                    WritePoll::TimedOut
                                } else {
                                    match io {
                                        Poll::Ready(result) => WritePoll::Io(result),
                                        Poll::Pending => WritePoll::Pending,
                                    }
                                }
                            }
                        }
                    }
                },
                WriteWait::Untimed => {
                    let cancellation =
                        queue_cancellation.expect("an untimed write is polled by its queue");
                    if cancellation.is_cancellation_requested() {
                        WritePoll::Cancelled
                    } else {
                        match active.io.poll(context) {
                            Poll::Ready(result) => WritePoll::Io(result),
                            Poll::Pending => WritePoll::Pending,
                        }
                    }
                }
                WriteWait::Retired { .. } => {
                    unreachable!("a retired write is no longer registered")
                }
            }
        };
        let result = match outcome {
            WritePoll::Pending => return Poll::Pending,
            WritePoll::Cancelled => {
                let error = cancelled_error(b"byte-stream write cancelled");
                let written = self.reset_transport();
                return Poll::Ready(Err(TransportWriteError::new(error, written)));
            }
            WritePoll::TimedOut => {
                let error = HttpError::with_message(
                    HttpErrorCode::WRITE_TIMEOUT,
                    b"byte-stream write timed out",
                );
                let written = self.finish(Some(HttpErrorCode::WRITE_TIMEOUT), Some(&error));
                return Poll::Ready(Err(TransportWriteError::new(error, written)));
            }
            WritePoll::Io(result) => result,
        };
        let written = self.progress.get();
        match finished_write(result, written, self.expected) {
            Ok(info) => {
                self.finish(None, None);
                Poll::Ready(Ok(info))
            }
            Err(error) => {
                self.finish(None, Some(&error));
                Poll::Ready(Err(TransportWriteError::new(error, written)))
            }
        }
    }
}

impl Drop for WriteOperation {
    fn drop(&mut self) {
        if !self.complete {
            self.state.borrow().assert_owner();
            self.reset_transport();
        }
    }
}

/// Returns a completed read's bytes in a queue bounded at `maximum`, with the segments its
/// backend filled. A backend queue with that bound, which every Tokio-backed read has, is
/// returned as it is. Any other queue is moved into a bounded one, keeping its segments and
/// their backing.
fn normalize_read(result: TransportRead, maximum: usize) -> Result<TransportRead, HttpError> {
    if result.bytes_read() > maximum {
        return Err(read_error(
            b"byte-stream adapter exceeded the requested read bound",
        ));
    }
    if result.data().limit() == maximum {
        return Ok(result);
    }
    let drained = result.drained;
    let eof = result.is_eof();
    let mut data = result.into_data();
    #[cfg(test)]
    READ_QUEUE_REBUILDS.with(|count| count.set(count.get() + 1));
    let mut bounded = BufQueue::new(maximum);
    bounded
        .append(&mut data)
        .map_err(|_| read_error(b"byte-stream read buffer exceeded its bound"))?;
    Ok(TransportRead::from_io(bounded, eof).with_drained(drained))
}

fn cancelled_error(message: &'static [u8]) -> HttpError {
    HttpError::with_message(HttpErrorCode::CORO_CANCELLED, message)
}

fn dropped_error() -> HttpError {
    HttpError::with_message(HttpErrorCode::DROPPED, b"byte-stream transport was dropped")
}

fn read_error(message: &'static [u8]) -> HttpError {
    HttpError::with_message(HttpErrorCode::TRANSPORT_READ_ERROR, message)
}

fn write_error(message: &'static [u8]) -> HttpError {
    HttpError::with_message(HttpErrorCode::TRANSPORT_WRITE_ERROR, message)
}
