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

/// Failure to confirm a backend value or its registered close future retired.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
pub enum TransportRetirementError {
    #[error("transport I/O value destruction was not confirmed")]
    IoDestructionUnconfirmed,
    #[error("transport close-scope retirement was not confirmed")]
    DrainRetirementUnconfirmed,
    #[error("transport I/O value and close-scope retirement were not confirmed")]
    BothUnconfirmed,
}

/// Repeatable observation of backend-value and existing close-scope retirement.
///
/// Confirmation follows destruction of the complete I/O value and its owned
/// fields, and of the registered close future and captures. It does not certify
/// I/O success, peer delivery, shared referents, TLS background-task retirement,
/// zero-copy completion descriptor aliases, or physical worker joins. The
/// original worker/reactor must remain driven. Observation retains no I/O or
/// worker ownership; dropping it does not activate or cancel cleanup.
#[derive(Clone)]
pub struct TransportRetirement {
    completion: AdoptedIoCompletion,
}

impl TransportRetirement {
    pub(crate) fn new(completion: AdoptedIoCompletion) -> Self {
        Self { completion }
    }

    /// Waits for both boundaries even when either is unconfirmed.
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
}

impl TransportRead {
    /// Builds the result of an external `ByteStreamTransport::read`, whose
    /// contract nothing checks. `data` holds at most the `maximum` passed to
    /// `read`. For a non-zero `maximum`, `data` is empty only with `eof`, as
    /// `folly::coro::TransportIf` reports EOF as a 0-byte read. The session
    /// reads again at once after an empty result without `eof`, so a transport
    /// that returns one while idle spins instead of reaching its read timeout.
    pub fn new(data: BufQueue, eof: bool) -> Self {
        Self { data, eof }
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
        Self {
            data: BufQueue::new(0),
            eof: false,
        }
    }

    fn eof() -> Self {
        Self {
            data: BufQueue::new(0),
            eof: true,
        }
    }

    pub(crate) fn from_io(data: BufQueue, eof: bool) -> Self {
        Self { data, eof }
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

/// The poll-based reader of an [`OwnedByteStream`] ([`ByteStreamTransport::reader`]),
/// through which the owned uniplex drivers read instead of with one read future
/// per read.
///
/// It starts one read at a time. Each read has `read`'s entry checks, clamps,
/// results, close and cancellation outcomes, and `DIV-016`/`DIV-017`/`DIV-018`
/// behavior, except for timing: it has no deadline, its completion wakes no
/// task, and over a Tokio-backed stream its I/O state lives in the backend, so
/// it allocates no future. A cancellation wakes a pending read, as a timed
/// read's wait does. Dropping the reader with a read started cancels that read,
/// as dropping a read future does.
pub struct OwnedReader {
    io: Weak<dyn ByteStreamIo>,
    state: Rc<RefCell<TransportState>>,
    worker: WorkerHandle,
    limits: ByteStreamLimits,
    context: WorkerContext,
    cancellation: CancellationToken,
    /// The started read, until `poll_read` returns its result.
    read: Option<ReadStart>,
}

impl OwnedReader {
    /// Starts a read of at most `maximum` bytes that first offers
    /// `allocation_size` bytes, with `read`'s entry checks and clamps. An error
    /// is interface misuse: a started read has not yet been returned.
    pub fn start_read(&mut self, maximum: usize, allocation_size: usize) -> Result<(), HttpError> {
        if self.read.is_some() {
            return Err(HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"owned reader already has a started read",
            ));
        }
        let target = ReadTarget {
            io: &self.io,
            state: &self.state,
            worker: &self.worker,
            limits: self.limits,
        };
        self.read = Some(start_read_operation(
            target,
            &self.context,
            maximum,
            allocation_size,
            ReadKind::Reader,
            self.cancellation.clone(),
        ));
        Ok(())
    }

    /// Polls the started read with `context`'s waker. Polling with no started
    /// read is interface misuse, refused with an error.
    pub fn poll_read(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<TransportRead, HttpError>> {
        let Some(read) = self.read.as_mut() else {
            return Poll::Ready(Err(HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"owned reader has no started read",
            )));
        };
        let result = match read {
            ReadStart::Started(operation) => ready!(Pin::new(operation).poll(context)),
            ReadStart::Finished(_) => {
                let Some(ReadStart::Finished(result)) = self.read.take() else {
                    unreachable!("the started read finished at entry");
                };
                result
            }
        };
        self.read = None;
        Poll::Ready(result)
    }
}

/// Unconsumed receipts an [`OwnedWriteQueue`] holds before it refuses submissions.
const WRITE_QUEUE_RECEIPTS: usize = 8;

/// One retired submission of an [`OwnedWriteQueue`].
#[derive(Debug)]
pub struct QueuedWriteCompletion {
    pub requested_bytes: usize,
    pub result: Result<WriteInfo, TransportWriteError>,
}

/// A submission whose receipt the queue has not yet delivered.
enum QueuedReceipt {
    /// The physical write of this many bytes is outstanding.
    Writing(usize),
    Done(QueuedWriteCompletion),
}

/// The native write queue of an [`OwnedByteStream`] over plaintext TCP
/// ([`ByteStreamTransport::write_queue`]): a bounded writer with ordered
/// completion receipts, which the owned uniplex drivers use instead of one
/// write future per write.
///
/// Like Proxygen coro's uniplex write loop, which keeps one transport write
/// outstanding while later egress gathers in its write buffer, the queue admits
/// a submission only while no physical write is outstanding, and holds at most
/// `WRITE_QUEUE_RECEIPTS` unconsumed receipts. A submitted write is registered
/// with the transport at once, and its bytes are written as
/// [`OwnedWriteQueue::poll_complete`] polls it, with the transport's
/// single-write semantics except for timing: the write has no deadline and
/// allocates nothing, and its completion wakes no task. A cancellation is
/// observed at the next poll; one requested from another task wakes the task
/// that last polled the outstanding write, as a reset of the transport does.
/// Dropping the queue with a write outstanding resets the transport, as
/// dropping a write future does.
pub struct OwnedWriteQueue {
    io: Weak<dyn ByteStreamIo>,
    state: Rc<RefCell<TransportState>>,
    worker: WorkerHandle,
    limits: ByteStreamLimits,
    byte_event_capability: ByteEventCapability,
    context: WorkerContext,
    maximum_write_size: usize,
    cancellation: CancellationToken,
    /// Wakes the task that last polled the outstanding write when another task
    /// requests cancellation; it installs no listener until a write pends.
    cancellation_wake: CancellationWakeRegistration,
    /// Counts each write's progress in turn.
    progress: WriteProgress,
    operation: Option<WriteOperation>,
    receipts: VecDeque<QueuedReceipt>,
}

impl OwnedWriteQueue {
    /// Reports capacity only, for a submission of any size up to the maximum.
    ///
    /// The outstanding write progresses only while `poll_complete` is polled,
    /// so capacity is withheld until it completes. That refusal is what arms
    /// owned HTTP/2's writer for every write the queue holds
    /// (`UniplexWriteQueue::can_write`).
    pub fn can_write(&self, _bytes: usize) -> bool {
        self.operation.is_none() && self.receipts.len() < WRITE_QUEUE_RECEIPTS
    }

    /// Submits `data`, which needs capacity and at most the maximum write
    /// size; an error is interface misuse and leaves no receipt. A submission
    /// the transport refuses at entry, or an empty one, gets its receipt at
    /// once.
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
            io: &self.io,
            state: &self.state,
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
                progress: &self.progress,
            },
            self.cancellation.clone(),
        ) {
            WriteStart::Started(operation) => {
                self.operation = Some(operation);
                self.receipts
                    .push_back(QueuedReceipt::Writing(requested_bytes));
            }
            WriteStart::Finished(result) => {
                self.receipts
                    .push_back(QueuedReceipt::Done(QueuedWriteCompletion {
                        requested_bytes,
                        result,
                    }));
            }
        }
        Ok(())
    }

    /// Drives the outstanding write with `context`'s waker and yields the
    /// oldest receipt once its submission is complete. An idle queue registers
    /// nothing.
    pub fn poll_complete(&mut self, context: &mut Context<'_>) -> Poll<QueuedWriteCompletion> {
        if let Some(operation) = self.operation.as_mut() {
            match Pin::new(operation).poll(context) {
                Poll::Ready(result) => {
                    self.operation = None;
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
            }
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
    /// not yet accepted: a physical write is outstanding, or the transport can
    /// no longer write.
    pub fn is_saturated(&self) -> bool {
        let state = self.state.borrow();
        self.operation.is_some() || state.write_shutdown || state.close_state != CloseState::Open
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
    /// capacity first; `begin_read` clamps both and keeps `allocation_size`
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

    /// The writer of an [`OwnedWriteQueue`] over this backend; `None` offers no
    /// queue.
    fn queued_writer(&self) -> Option<Rc<dyn QueuedWriter>> {
        None
    }

    /// The reader of an [`OwnedReader`]'s reads over this backend; `None`
    /// reads through `read`'s future.
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

    /// Makes `close_with_reset` close the socket without the linger-zero reset;
    /// adapters without an abortive reset ignore it.
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
    /// An [`OwnedReader`]'s read: no deadline. While it pends, `listener`
    /// wakes it on cancellation, as a timed read's wait does.
    Untimed {
        io: ActiveReadIo,
        cancellation: CancellationToken,
        listener: Option<CancellationWakeRegistration>,
    },
}

/// The I/O of an [`OwnedReader`] read: `ByteStreamIo::read`'s boxed future,
/// or the backend that holds the read's state.
enum ActiveReadIo {
    Future(LocalTransportFuture<Result<TransportRead, HttpError>>),
    Polled(PolledReadIo),
}

impl ActiveReadIo {
    fn poll(&mut self, context: &mut Context<'_>) -> Poll<Result<TransportRead, HttpError>> {
        match self {
            Self::Future(future) => future.as_mut().poll(context),
            Self::Polled(polled) => polled.0.poll_read(context),
        }
    }
}

/// A read that a [`PolledReader`] holds. Dropping it ends the read, as
/// dropping `ByteStreamIo::read`'s future does; after `Ready` that does
/// nothing.
struct PolledReadIo(Rc<dyn PolledReader>);

impl Drop for PolledReadIo {
    fn drop(&mut self) {
        self.0.retire_read();
    }
}

/// A backend that keeps one read's state itself, so that an [`OwnedReader`]
/// read allocates no future. Each read has the effects of
/// `ByteStreamIo::read`'s future.
pub(crate) trait PolledReader {
    fn start_read(&self, maximum: usize, allocation_size: usize);

    /// `Ready` ends the read.
    fn poll_read(&self, context: &mut Context<'_>) -> Poll<Result<TransportRead, HttpError>>;

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
}

#[cfg(test)]
std::thread_local! {
    static READ_DEADLINE_BATONS: Cell<usize> = const { Cell::new(0) };
}

/// Read-direction deadline batons built on this thread.
#[cfg(test)]
pub(crate) fn read_deadline_batons_for_test() -> usize {
    READ_DEADLINE_BATONS.with(Cell::get)
}

/// Write progress cells built on this thread.
#[cfg(test)]
pub(crate) fn write_progress_cells_for_test() -> usize {
    WRITE_PROGRESS_CELLS.with(Cell::get)
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
            ReadDrive::Untimed {
                io,
                cancellation,
                listener,
            } => {
                retire_transport_value((cancellation, listener), &mut failure);
                retire_transport_value(io, &mut failure);
            }
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

/// The I/O of one physical write: an ordinary write's boxed future, or the
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
/// dropping an ordinary write's future does; after `Ready` that does nothing.
struct QueuedIo(Rc<dyn QueuedWriter>);

impl Drop for QueuedIo {
    fn drop(&mut self) {
        self.0.retire_write();
    }
}

/// A backend that keeps one write's state itself, so that an
/// [`OwnedWriteQueue`] write allocates no future. Each write has the effects of
/// `ByteStreamIo::write`'s future.
pub(crate) trait QueuedWriter {
    fn start_write(
        &self,
        data: BufQueue,
        progress: WriteProgress,
        flags: ByteEventWriteFlags,
        byte_events: Option<ByteEventWriteState>,
    );

    /// `Ready` ends the write.
    fn poll_write(&self, context: &mut Context<'_>) -> Poll<Result<(), HttpError>>;

    /// Ends an unfinished write, as dropping `ByteStreamIo::write`'s future does.
    fn retire_write(&self);
}

/// A write's deadline, built once a backend poll leaves the write pending, as
/// `folly::AsyncSocket::writeImpl` schedules its send timeout only then. A
/// first backend poll that panics also builds it before the panic resumes, so
/// that a re-poll finds the deadline and cancellation registered.
enum WriteWait {
    /// `started`, the first poll's time, anchors a nonzero timeout's deadline.
    /// A zero timeout reads no clock.
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
    /// An [`OwnedWriteQueue`]'s write: no deadline, and cancellation is observed
    /// when the write is polled, which wakes no task.
    Untimed { cancellation: CancellationToken },
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
    // A completed wait cannot be polled again; the next poll reads its outcome
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

    /// The transport's worker cleanup becomes the only owner of `io`; an
    /// error drops `io` before returning.
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

    /// Returns a cached, non-owning retirement receipt when the backend supports it.
    ///
    /// `None` is unsupported observation, not completed cleanup. This receipt is
    /// still available after the backend has retired.
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

    /// Sets the Folly-style re-enable interval after `ENOBUFS`.
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

    /// Roxygen-only (`DIV-038`): every later reset of this stream (`close_with_reset`,
    /// and the resets its write failures and abandoned writes cause) closes the socket
    /// without `SO_LINGER` zero, as dropping a Tokio stream does: the kernel still sends
    /// the bytes already written, then FIN, or RST only if received bytes are unread.
    /// The transport-side effects of a reset are unchanged: pending reads and writes
    /// fail, unwritten bytes are discarded, and byte events are cancelled as on a reset.
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
            ReadKind::Ordinary { timeout },
            cancellation,
        ) {
            ReadStart::Started(operation) => Box::pin(operation),
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
            WriteKind::Ordinary { timeout },
            cancellation,
        ) {
            WriteStart::Started(operation) => Box::pin(operation),
            WriteStart::Finished(result) => Box::pin(std::future::ready(result)),
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
    Started(ReadOperation),
    Finished(Result<TransportRead, HttpError>),
}

/// How [`start_read_operation`] reads.
#[derive(Clone, Copy)]
enum ReadKind {
    /// An ordinary read, timed by `timeout`.
    Ordinary { timeout: Duration },
    /// An [`OwnedReader`]'s read: untimed, and its I/O state kept by the
    /// backend's [`PolledReader`] when it has one.
    Reader,
}

/// Inlined into each caller, which then compiles for its own `kind`.
#[inline(always)]
fn start_read_operation(
    target: ReadTarget<'_>,
    context: &WorkerContext,
    maximum: usize,
    allocation_size: usize,
    kind: ReadKind,
    cancellation: CancellationToken,
) -> ReadStart {
    if let Err(error) = target.check_context(context) {
        return ReadStart::Finished(Err(error));
    }
    if cancellation.is_cancellation_requested() {
        return ReadStart::Finished(Err(cancelled_error(
            b"byte-stream read cancelled before entry",
        )));
    }
    if maximum == 0 {
        return ReadStart::Finished(Ok(TransportRead::empty()));
    }
    if allocation_size == 0 {
        return ReadStart::Finished(Err(HttpError::with_message(
            HttpErrorCode::INVALID_STATE_TRANSITION,
            b"byte-stream read allocation size must be non-zero",
        )));
    }

    let maximum = maximum.min(target.limits.maximum_read_size);
    let allocation_size = allocation_size.min(maximum);
    let operation = {
        let mut state = target.state.borrow_mut();
        match state.close_state {
            CloseState::Open => {}
            CloseState::Graceful => {
                return ReadStart::Finished(Ok(TransportRead::eof()));
            }
            CloseState::Reset => {
                return ReadStart::Finished(Err(read_error(b"byte-stream was reset")));
            }
            CloseState::Dropped => {
                return ReadStart::Finished(Err(dropped_error()));
            }
        }
        if state.deferred_eof {
            state.deferred_eof = false;
            return ReadStart::Finished(Ok(TransportRead::eof()));
        }
        if state.active_read.is_some() {
            return ReadStart::Finished(Err(HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"byte-stream already has a pending read",
            )));
        }
        state.next_operation()
    };

    let Some(io) = target.io.upgrade() else {
        return ReadStart::Finished(Err(dropped_error()));
    };
    let drive = match kind {
        ReadKind::Ordinary { timeout } => {
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
        ReadKind::Reader => {
            let io = match io.polled_reader() {
                Some(reader) => {
                    reader.start_read(maximum, allocation_size);
                    ActiveReadIo::Polled(PolledReadIo(reader))
                }
                None => ActiveReadIo::Future(io.read(maximum, allocation_size)),
            };
            ReadDrive::Untimed {
                io,
                cancellation,
                listener: None,
            }
        }
    };
    target.state.borrow_mut().active_read = Some(ActiveRead {
        operation,
        drive,
        waker: None,
    });
    ReadStart::Started(ReadOperation::new(
        target.io.clone(),
        Rc::clone(target.state),
        operation,
        maximum,
        allocation_size,
    ))
}

/// A write that [`start_write_operation`] began, or the result it ended with at entry.
enum WriteStart {
    Started(WriteOperation),
    Finished(Result<WriteInfo, TransportWriteError>),
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
#[derive(Clone, Copy)]
enum WriteKind<'a> {
    /// An ordinary write, timed by `timeout`.
    Ordinary { timeout: Duration },
    /// An [`OwnedWriteQueue`]'s write: untimed, counting its progress in the queue's cell.
    Queued { progress: &'a WriteProgress },
}

fn start_write_operation(
    target: WriteTarget<'_>,
    context: &WorkerContext,
    data: BufQueue,
    flags: ByteEventWriteFlags,
    kind: WriteKind<'_>,
    cancellation: CancellationToken,
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
    if cancellation.is_cancellation_requested() {
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
    let (progress, wait) = match kind {
        WriteKind::Ordinary { timeout } => {
            io.prepare_write_timeout(timeout);
            (
                WriteProgress::new(length),
                WriteWait::Unarmed {
                    timeout,
                    cancellation,
                    started: None,
                },
            )
        }
        WriteKind::Queued { progress } => {
            io.prepare_write_timeout(Duration::ZERO);
            (
                progress.restart(length),
                WriteWait::Untimed { cancellation },
            )
        }
    };
    let queued_writer = match kind {
        WriteKind::Ordinary { .. } => None,
        WriteKind::Queued { .. } => io.queued_writer(),
    };
    let io = match queued_writer {
        Some(writer) => {
            writer.start_write(data, progress.clone(), flags, byte_events.clone());
            ActiveIo::Queued(QueuedIo(writer))
        }
        None => ActiveIo::Future(io.write(data, progress.clone(), flags, byte_events.clone())),
    };
    target.state.borrow_mut().active_write = Some(ActiveWrite {
        operation,
        io,
        byte_events: byte_events.clone(),
        _progress: progress.clone(),
        wait,
        waker: None,
    });
    WriteStart::Started(WriteOperation::new(
        target.io.clone(),
        Rc::clone(target.state),
        operation,
        length,
        progress,
    ))
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
            io: self.io.clone(),
            state: Rc::clone(&self.state),
            worker: self.worker.clone(),
            limits: self.limits,
            byte_event_capability: self.byte_event_capability,
            context: context.clone(),
            maximum_write_size,
            cancellation_wake: cancellation.wake_registration(),
            cancellation,
            progress: WriteProgress::new(0),
            operation: None,
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
            io: self.io.clone(),
            state: Rc::clone(&self.state),
            worker: self.worker.clone(),
            limits: self.limits,
            context: context.clone(),
            cancellation,
            read: None,
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
    allocation_size: usize,
    complete: bool,
}

impl ReadOperation {
    fn new(
        io: Weak<dyn ByteStreamIo>,
        state: Rc<RefCell<TransportState>>,
        operation: u64,
        maximum: usize,
        allocation_size: usize,
    ) -> Self {
        Self {
            io,
            state,
            operation,
            maximum,
            allocation_size,
            complete: false,
        }
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
        if active.is_some() {
            if let Some(code) = cancel {
                if let Some(io) = self.io.upgrade() {
                    io.cancel_read(code);
                }
            }
            drop(active);
        }
        self.complete = true;
    }

    fn close_error(&self) -> Option<HttpError> {
        match self.state.borrow().close_state {
            CloseState::Open => None,
            CloseState::Graceful => None,
            CloseState::Reset => Some(read_error(b"byte-stream was reset")),
            CloseState::Dropped => Some(dropped_error()),
        }
    }
}

impl Future for ReadOperation {
    type Output = Result<TransportRead, HttpError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.state.borrow().assert_owner();
        if let Some(error) = self.close_error() {
            self.finish(None);
            return Poll::Ready(Err(error));
        }
        if self.state.borrow().close_state == CloseState::Graceful {
            self.finish(None);
            return Poll::Ready(Ok(TransportRead::eof()));
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
        let outcome = {
            let mut state = self.state.borrow_mut();
            let active = state
                .active_read
                .as_mut()
                .filter(|active| active.operation == self.operation)
                .expect("an open read operation remains registered with its transport");
            active.waker = Some(context.waker().clone());
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
                ReadDrive::Untimed {
                    io,
                    cancellation,
                    listener,
                } => {
                    let io = io.poll(context);
                    let cancelled = if io.is_pending() {
                        listener
                            .get_or_insert_with(|| cancellation.wake_registration())
                            .update_waker(context.waker())
                    } else {
                        cancellation.is_cancellation_requested()
                    };
                    let wait = if cancelled {
                        Poll::Ready(WaitStatus::Cancelled)
                    } else {
                        Poll::Pending
                    };
                    ReadPoll::new(io, wait)
                }
            }
        };
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
        self.finish(None);
        let result = match result {
            Ok(result) => normalize_read(result, self.maximum, self.allocation_size),
            Err(error) => Err(error),
        };
        match result {
            Ok(result) if result.is_eof() && result.bytes_read() != 0 => {
                self.state.borrow_mut().deferred_eof = true;
                Poll::Ready(Ok(TransportRead::from_io(result.into_data(), false)))
            }
            Ok(result) if !result.is_eof() && result.bytes_read() == 0 => Poll::Ready(Err(
                read_error(b"byte-stream adapter made no read progress"),
            )),
            result => Poll::Ready(result),
        }
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
        Self {
            io,
            state,
            operation,
            expected,
            progress,
            complete: false,
        }
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

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
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
                                // A re-poll after a contained backend panic
                                // finds its deadline and cancellation armed.
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
                WriteWait::Untimed { cancellation } => {
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
        match result {
            Ok(()) if written == self.expected => {
                self.finish(None, None);
                Poll::Ready(Ok(WriteInfo {
                    bytes_written: written,
                }))
            }
            Ok(()) => {
                let error = write_error(b"byte-stream adapter completed a partial write");
                self.finish(None, Some(&error));
                Poll::Ready(Err(TransportWriteError::new(error, written)))
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

fn normalize_read(
    result: TransportRead,
    maximum: usize,
    allocation_size: usize,
) -> Result<TransportRead, HttpError> {
    let length = result.bytes_read();
    if length > maximum {
        return Err(read_error(
            b"byte-stream adapter exceeded the requested read bound",
        ));
    }
    let eof = result.is_eof();
    let mut data = result.into_data();
    let mut bounded = BufQueue::new(maximum);
    if data
        .segments()
        .all(|segment| segment.len() <= allocation_size)
    {
        bounded
            .append(&mut data)
            .map_err(|_| read_error(b"byte-stream read buffer exceeded its bound"))?;
        return Ok(TransportRead::from_io(bounded, eof));
    }
    for mut segment in data.into_bytes_segments() {
        while segment.len() > allocation_size {
            bounded
                .push_bytes(segment.split_to(allocation_size))
                .map_err(|_| read_error(b"byte-stream read buffer exceeded its bound"))?;
        }
        bounded
            .push_bytes(segment)
            .map_err(|_| read_error(b"byte-stream read buffer exceeded its bound"))?;
    }
    Ok(TransportRead::from_io(bounded, eof))
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
