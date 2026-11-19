/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! `HttpConnectTransport`, a `ByteStreamTransport` over a CONNECT stream.
//!
//! Reads are lazy: one body-read future is kept across `read` calls, and only
//! a `read` polls it. Writes wait on a `TimedBaton` that is signalled while
//! the egress window is open.

use std::cell::Cell;
use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use roxygen_core::buf_queue::BufQueue;
use roxygen_core::byte_event::HttpByteEventRegistration;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpBodyEventKind;
use roxygen_core::ids::StreamId;
use roxygen_core::ingress_completion::IngressCompletion;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::stream_source::FlowControlState;
use roxygen_core::stream_source::HttpStreamSourceSharedCallback;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::cancellation::CancellationWakeRegistration;
use roxygen_runtime::host::Host;
use roxygen_runtime::host::LocalFuture;
use roxygen_runtime::wait::TimedBaton;
use roxygen_runtime::wait::TimedWait;
use roxygen_runtime::wait::WaitStatus;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerId;
use roxygen_transport::byte_stream::ByteEventCapability;
use roxygen_transport::byte_stream::ByteEventRegistrationError;
use roxygen_transport::byte_stream::ByteStreamTransport;
use roxygen_transport::byte_stream::LocalTransportFuture;
use roxygen_transport::byte_stream::PeerIdentity;
use roxygen_transport::byte_stream::TransportRead;
use roxygen_transport::byte_stream::TransportWriteError;
use roxygen_transport::byte_stream::WriteInfo;

use crate::stream::HttpConnectStream;

/// A `ByteStreamTransport` over a CONNECT tunnel.
///
/// Read and write futures hold the transport weakly. Dropping the transport
/// fails a parked read or write with `CORO_CANCELLED`, then closes the
/// stream. A read or write first polled after the drop begins fails the same
/// way. If the drop begins inside a stream or source call made by a read or
/// write, the stream closes once that call returns and the read has dropped
/// its body read.
pub struct HttpConnectTransport {
    state: Rc<TransportState>,
}

struct TransportState {
    worker_id: WorkerId,
    host: Host,
    /// Set when the handle's Drop begins. Caller code run by a stored read's
    /// release can drop the handle while a future still holds this state, so
    /// a future checks this after it upgrades and after each step that can
    /// run caller code, before it touches the stream or the baton.
    dropped: Cell<bool>,
    /// Shared, as is `window`, so that the slots do not stay borrowed across
    /// a stream call or a baton signal. Those can run caller code inline,
    /// such as a waker, that drops the handle or calls back into the
    /// transport.
    stream: RefCell<Option<Rc<HttpConnectStream>>>,
    /// The write baton, signalled while the egress window is open.
    window: RefCell<Option<Rc<TimedBaton>>>,
    ingress: Cell<IngressLoan>,
    /// The egress source has completed.
    egress_complete: Cell<bool>,
    write_in_progress: Cell<bool>,
    pending_eom: Cell<bool>,
    /// Set while a write's `egress_body` call carries the pending EOM. A
    /// `shutdown_write` or `close_with_reset` that the call runs inline treats
    /// the EOM as already sent.
    eom_in_body: Cell<bool>,
    read: RefCell<ReadState>,
    next_read: Cell<u64>,
}

/// Tracks whether a read or a release is using the response source while it
/// can run caller code. A read that starts a body read takes the source out of
/// the stream while it calls it. A read that polls the stored read holds that
/// read outside `ReadState` while the body read and the read's timer run, and
/// `release_stored` does the same while a stored body read drops.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IngressLoan {
    InStream,
    Lent,
    /// `close` or `close_with_reset` ran while `Lent`, and takes effect when
    /// the source returns to `InStream`.
    LentAcrossClose,
}

#[derive(Default)]
struct ReadState {
    stored: Option<StoredRead>,
    /// Bytes of a body event beyond the read that took it.
    remainder: Option<BufQueue>,
    /// The next read's error after a cancellation or a close ended the ingress.
    error: Option<HttpError>,
    pending: Option<u64>,
    /// The pending read's waker while it awaits the stored read or a close.
    waker: Option<Waker>,
    /// A close is releasing the stored read or stopping the source, either of
    /// which can run caller code that polls a read.
    closing: bool,
}

impl ReadState {
    fn awaits_close(&self) -> bool {
        self.closing && self.error.is_none()
    }

    fn holds_stored(&self) -> bool {
        self.stored.is_some()
    }
}

struct StoredRead {
    future: LocalSourceFuture<Result<HttpBodyEvent, HttpError>>,
    completion: Option<IngressCompletion>,
    /// The read that started the body read, while it awaits it.
    starter: Option<u64>,
    /// Whether the body read has returned `Pending`.
    parked: bool,
}

fn cancelled() -> HttpError {
    HttpError::with_message(HttpErrorCode::CORO_CANCELLED, b"Cancelled")
}

fn wrong_worker() -> HttpError {
    HttpError::with_message(
        HttpErrorCode::INVALID_STATE_TRANSITION,
        b"HTTPConnectTransport used from a different worker context",
    )
}

/// Wraps an ingress error in a `TRANSPORT_READ_ERROR`.
fn transport_read_error(error: HttpError) -> HttpError {
    HttpError::with_message(HttpErrorCode::TRANSPORT_READ_ERROR, error.describe())
        .with_cause(Arc::new(error))
}

fn eof() -> TransportRead {
    TransportRead::new(BufQueue::new(0), true)
}

impl TransportState {
    fn stream(&self) -> Rc<HttpConnectStream> {
        Rc::clone(
            self.stream
                .borrow()
                .as_ref()
                .expect("should hold the stream until the transport drops"),
        )
    }

    fn window(&self) -> Option<Rc<TimedBaton>> {
        self.window.borrow().clone()
    }

    fn window_status(&self) -> Option<WaitStatus> {
        self.window.borrow().as_ref().map(|window| window.status())
    }

    /// Sends EOM, or defers it while a write waits for the egress window.
    fn shutdown_write(&self) {
        if self.eom_in_body.get() {
            return;
        }
        let stream = self.stream();
        if !stream.can_write() {
            return;
        }
        if self.write_in_progress.get() && self.window_status() == Some(WaitStatus::NotReady) {
            self.pending_eom.set(true);
        } else {
            stream.egress_eom();
        }
    }

    /// Stops the response source. While the source is `Lent`, it only records
    /// the close.
    fn shutdown_read(&self) {
        if self.ingress.get() != IngressLoan::InStream {
            self.ingress.set(IngressLoan::LentAcrossClose);
            return;
        }
        let readable = self
            .stream
            .borrow()
            .as_ref()
            .is_some_and(|stream| stream.can_read());
        if readable {
            self.stop_ingress(None);
        }
    }

    /// Starts a body read, and reports whether `close` or `close_with_reset`
    /// ran meanwhile. The source is out of the stream while the read calls
    /// it, because those calls can run caller code that closes the transport
    /// or drops its handle. `stream` keeps the stream until the source is
    /// back.
    fn start_body_read(
        &self,
        stream: &HttpConnectStream,
        id: u64,
        maximum: u32,
    ) -> (StoredRead, bool) {
        let mut ingress = stream.with_ingress_source(std::mem::take);
        self.ingress.set(IngressLoan::Lent);
        ingress.set_read_timeout(Duration::ZERO);
        let future = ingress.read_body_event(maximum);
        let completion = ingress.ingress_completion();
        let placeholder = stream.with_ingress_source(|slot| std::mem::replace(slot, ingress));
        drop(placeholder);
        let closed = self.ingress.replace(IngressLoan::InStream) == IngressLoan::LentAcrossClose;
        let stored = StoredRead {
            future,
            completion,
            starter: Some(id),
            parked: false,
        };
        (stored, closed)
    }

    /// Drops a stored body read while the source is marked `Lent`. Returns
    /// the source's completion snapshot and whether `close` or
    /// `close_with_reset` ran meanwhile. The drop can run caller code, such as
    /// the source's completion callback or a caller-provided body read. The
    /// session's own source releases the body read's saved waker, which can
    /// run caller code too, while it borrows its event baton. A close made
    /// during the drop does not stop the source until the body read is gone.
    fn release_stored(&self, stored: StoredRead) -> (Option<Result<Instant, HttpError>>, bool) {
        let loan = self.ingress.replace(IngressLoan::Lent);
        assert_eq!(
            loan,
            IngressLoan::InStream,
            "should release a stored read only while no other loan holds the source"
        );
        drop(stored.future);
        let closed = self.ingress.replace(IngressLoan::InStream) == IngressLoan::LentAcrossClose;
        let snapshot = stored
            .completion
            .as_ref()
            .and_then(IngressCompletion::snapshot);
        (snapshot, closed)
    }

    /// Stops the response source. The holder is taken out of the stream
    /// first, because the stop can run the source's completion callback, and
    /// with it caller code, inline. Caller code run by a future's release of
    /// its body read may have dropped the transport, and the transport's Drop
    /// took the stream. The future's clone of the stream then closes it when
    /// the future releases it, so this returns early when the stream is gone.
    fn stop_ingress(&self, error: Option<HttpErrorCode>) {
        let Some(mut ingress) = self
            .stream
            .borrow()
            .as_ref()
            .map(|stream| stream.with_ingress_source(std::mem::take))
        else {
            return;
        };
        ingress.clear(error);
    }

    /// Releases `stored` and ends the ingress after the read that started its
    /// body read is cancelled. An error that the source committed before the
    /// release goes to the next read. A close made during the release is
    /// redundant, because this stop follows it.
    fn cancel_ingress(&self, stored: StoredRead) {
        let (committed, _) = self.release_stored(stored);
        self.stop_ingress(Some(HttpErrorCode::CORO_CANCELLED));
        let cause = match committed {
            Some(Err(error)) => error,
            _ => HttpError::with_message(HttpErrorCode::CORO_CANCELLED, b"Read cancelled"),
        };
        self.read.borrow_mut().error = Some(transport_read_error(cause));
    }

    /// Ends the ingress for `close` and `close_with_reset`.
    fn close_ingress(&self) {
        let stored = self.read.borrow_mut().stored.take();
        match stored {
            Some(stored) => self.close_stored(stored),
            None => self.shutdown_read(),
        }
    }

    /// Releases `stored` and ends the ingress for a close. `stored` may come
    /// from a read that held it while the close ran. The release comes before
    /// the stop, because the stop cannot complete the source while a read is
    /// active. The release's snapshot gives the next read's error. Both steps
    /// can run the source's completion callback, and with it caller code that
    /// polls a read. So `closing` stays set across both, and the error is
    /// published before the stop. A close made during the release is
    /// redundant, because this stop follows it.
    fn close_stored(&self, stored: StoredRead) {
        self.read.borrow_mut().closing = true;
        let (snapshot, _) = self.release_stored(stored);
        let cause = match snapshot {
            Some(Err(error)) => error,
            _ => HttpError::with_message(HttpErrorCode::NO_ERROR, b"Abandoned reading"),
        };
        self.read.borrow_mut().error = Some(transport_read_error(cause));
        self.shutdown_read();
        let waker = {
            let mut read = self.read.borrow_mut();
            read.closing = false;
            read.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Saves a clone of `waker` for a read that parks while `parks` holds,
    /// and reports whether it parked. The clone can run caller code, so it
    /// happens outside any borrow, and the read parks only if the handle's
    /// Drop has not begun and `parks` still holds.
    fn park_read(&self, waker: &Waker, parks: fn(&ReadState) -> bool) -> bool {
        let waker = waker.clone();
        let saved = {
            let mut read = self.read.borrow_mut();
            if self.dropped.get() || !parks(&read) {
                Err(waker)
            } else {
                Ok(read.waker.replace(waker))
            }
        };
        // Releasing a waker can drop the handle, whose Drop then wakes the
        // waker just saved.
        match saved {
            Ok(previous) => {
                drop(previous);
                true
            }
            Err(waker) => {
                drop(waker);
                false
            }
        }
    }

    /// Clears read `id` as the pending read and as the stored read's starter.
    /// The saved waker drops after the borrow, because releasing a waker can
    /// drop a task that owns the handle.
    fn finish_read(&self, id: u64) {
        let waker = {
            let mut read = self.read.borrow_mut();
            if let Some(stored) = read.stored.as_mut()
                && stored.starter == Some(id)
            {
                stored.starter = None;
            }
            if read.pending != Some(id) {
                return;
            }
            read.pending = None;
            read.waker.take()
        };
        drop(waker);
    }
}

impl HttpStreamSourceSharedCallback for TransportState {
    fn window_open(&self, _id: StreamId) {
        if let Some(window) = self.window() {
            window.signal();
        }
    }

    fn source_complete(&self, _id: StreamId, _error: Option<HttpError>) {
        self.egress_complete.set(true);
    }
}

impl HttpConnectTransport {
    /// Takes the stream and installs the transport as its egress callback.
    pub fn new(context: &WorkerContext, stream: HttpConnectStream) -> Rc<Self> {
        assert_eq!(
            context.worker_id(),
            stream.worker_id(),
            "a CONNECT transport must run on its stream's worker"
        );
        let window = TimedBaton::with_host(None, context.host().clone());
        if stream.egress_buffer_size() > 0 {
            window.signal();
        }
        let state = Rc::new(TransportState {
            worker_id: stream.worker_id(),
            host: context.host().clone(),
            dropped: Cell::new(false),
            stream: RefCell::new(Some(Rc::new(stream))),
            window: RefCell::new(Some(Rc::new(window))),
            ingress: Cell::new(IngressLoan::InStream),
            egress_complete: Cell::new(false),
            write_in_progress: Cell::new(false),
            pending_eom: Cell::new(false),
            eom_in_body: Cell::new(false),
            read: RefCell::default(),
            next_read: Cell::new(0),
        });
        let callback: Weak<TransportState> = Rc::downgrade(&state);
        let callback: Weak<dyn HttpStreamSourceSharedCallback> = callback;
        state
            .stream()
            .set_http_stream_source_callback(Some(callback));
        Rc::new(Self { state })
    }

    /// The worker that owns the tunnel.
    pub fn worker_id(&self) -> WorkerId {
        self.state.worker_id
    }

    fn check_context(&self, context: &WorkerContext) -> Result<(), HttpError> {
        if context.worker_id() == self.state.worker_id {
            Ok(())
        } else {
            Err(wrong_worker())
        }
    }
}

impl ByteStreamTransport for HttpConnectTransport {
    fn read(
        &self,
        context: &WorkerContext,
        maximum: usize,
        allocation_size: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<TransportRead, HttpError>> {
        if let Err(error) = self.check_context(context) {
            return Box::pin(std::future::ready(Err(error)));
        }
        Box::pin(ReadOperation {
            transport: Rc::downgrade(&self.state),
            host: self.state.host.clone(),
            maximum,
            allocation_size,
            timeout,
            registration: cancellation.wake_registration(),
            cancellation,
            stage: ReadStage::NotStarted,
            deadline: None,
            timer: None,
        })
    }

    fn write(
        &self,
        context: &WorkerContext,
        data: BufQueue,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        if let Err(error) = self.check_context(context) {
            return Box::pin(std::future::ready(Err(TransportWriteError::new(error, 0))));
        }
        Box::pin(WriteOperation {
            transport: Rc::downgrade(&self.state),
            length: data.chain_length(),
            data: Some(data),
            timeout,
            cancellation,
            stage: WriteStage::NotStarted,
            in_progress: false,
        })
    }

    fn shutdown_write(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.check_context(context)?;
        self.state.shutdown_write();
        Ok(())
    }

    /// Sends EOM, deferred while a write waits for the egress window, then
    /// stops the response source.
    fn close(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.check_context(context)?;
        self.state.shutdown_write();
        self.state.close_ingress();
        Ok(())
    }

    /// Aborts the egress with `CANCEL`, then stops the response source.
    /// Inside a write's `egress_body` call that carries the EOM, the abort is
    /// skipped because the EOM counts as sent.
    fn close_with_reset(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.check_context(context)?;
        if !self.state.eom_in_body.get() {
            self.state.stream().egress_abort(HttpErrorCode::CANCEL);
        }
        self.state.close_ingress();
        Ok(())
    }

    fn local_address(&self, context: &WorkerContext) -> Result<SocketAddress, HttpError> {
        self.check_context(context)?;
        Ok(self.state.stream().local_address().clone())
    }

    /// The `X-Connected-To` address, or else the proxy session's peer.
    fn peer_address(&self, context: &WorkerContext) -> Result<SocketAddress, HttpError> {
        self.check_context(context)?;
        Ok(self.state.stream().peer_address().clone())
    }

    /// The proxy session's local endpoint.
    fn local_endpoint(&self, context: &WorkerContext) -> Result<TransportEndpoint, HttpError> {
        self.check_context(context)?;
        Ok(self.state.stream().local_endpoint().clone())
    }

    /// The immediate proxy hop, never `X-Connected-To`.
    fn peer_endpoint(&self, context: &WorkerContext) -> Result<TransportEndpoint, HttpError> {
        self.check_context(context)?;
        Ok(self.state.stream().peer_endpoint().clone())
    }

    /// Always `None`.
    fn peer_identity(&self, context: &WorkerContext) -> Result<Option<PeerIdentity>, HttpError> {
        self.check_context(context)?;
        Ok(None)
    }

    /// Always `Unsupported`.
    fn byte_event_capability(
        &self,
        context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError> {
        self.check_context(context)?;
        Ok(ByteEventCapability::Unsupported)
    }

    fn register_byte_event(
        &self,
        context: &WorkerContext,
        mut registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError> {
        if let Err(error) = self.check_context(context) {
            registration.cancel(&error, None);
            return Err(ByteEventRegistrationError::WrongWorker);
        }
        registration.cancel(
            &HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"byte events are unsupported by this transport",
            ),
            None,
        );
        Err(ByteEventRegistrationError::Unsupported)
    }
}

impl Drop for HttpConnectTransport {
    /// Cancels a waiting write by dropping the write baton, wakes a parked
    /// read, then releases the stream.
    fn drop(&mut self) {
        self.state.dropped.set(true);
        let window = self.state.window.borrow_mut().take();
        drop(window);
        let (stored, waker) = {
            let mut read = self.state.read.borrow_mut();
            (read.stored.take(), read.waker.take())
        };
        drop(stored);
        if let Some(waker) = waker {
            waker.wake();
        }
        let stream = self.state.stream.borrow_mut().take();
        drop(stream);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReadStage {
    NotStarted,
    Pending(u64),
    Done,
}

/// The next step of a read that a remainder, cancellation, or error has not
/// ended.
enum NextRead {
    /// A close is releasing the stored read or stopping the source.
    AwaitClose,
    Poll(StoredRead),
    Start,
}

/// The result of a read's poll of the stored read and its timer.
enum StoredPoll {
    Ready {
        result: Result<HttpBodyEvent, HttpError>,
        /// `close` or `close_with_reset` ran during the poll.
        closed: bool,
    },
    Pending {
        expired: bool,
    },
    /// `close` or `close_with_reset` ran during a poll that left the body
    /// read pending.
    Closed,
}

/// The future returned by `HttpConnectTransport::read`. It starts at its first
/// poll.
struct ReadOperation {
    transport: Weak<TransportState>,
    host: Host,
    maximum: usize,
    allocation_size: usize,
    timeout: Duration,
    cancellation: CancellationToken,
    registration: CancellationWakeRegistration,
    stage: ReadStage,
    /// The deadline of the current wait for a body event.
    deadline: Option<Instant>,
    timer: Option<LocalFuture>,
}

impl ReadOperation {
    /// Runs the entry checks and marks the read pending.
    fn start(
        &mut self,
        state: &TransportState,
        cancelled: bool,
    ) -> Result<u64, Result<TransportRead, HttpError>> {
        if cancelled {
            return Err(Err(self::cancelled()));
        }
        if self.maximum == 0 {
            return Err(Ok(TransportRead::new(BufQueue::new(0), false)));
        }
        if self.allocation_size == 0 {
            return Err(Err(HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"HTTPConnectTransport read allocation size must be non-zero",
            )));
        }
        let mut read = state.read.borrow_mut();
        if read.pending.is_some() {
            return Err(Err(HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"HTTPConnectTransport already has a pending read",
            )));
        }
        let id = state.next_read.get();
        state.next_read.set(id.wrapping_add(1));
        read.pending = Some(id);
        Ok(id)
    }

    /// Arms the read timer for the current wait if needed, and polls it.
    fn poll_timer(&mut self, context: &mut Context<'_>) -> bool {
        let host = &self.host;
        let deadline = *self
            .deadline
            .get_or_insert_with(|| host.deadline_after(self.timeout));
        let timer = self.timer.get_or_insert_with(|| host.sleep_until(deadline));
        timer.as_mut().poll(context).is_ready()
    }

    /// Polls the stored read and, while it is pending, the read timer. The
    /// body read and the clock and timer of a caller-provided host can run
    /// caller code, so the read marks the source `Lent` meanwhile, and the
    /// caller keeps the stream. A result goes to the caller even after a
    /// close, because dropping it can run caller code too, which must wait
    /// until the body read is released.
    fn poll_stored(
        &mut self,
        state: &TransportState,
        stored: &mut StoredRead,
        cancelled: bool,
        context: &mut Context<'_>,
    ) -> StoredPoll {
        state.ingress.set(IngressLoan::Lent);
        let polled = stored.future.as_mut().poll(context);
        let expired = polled.is_pending()
            && !cancelled
            && !self.timeout.is_zero()
            && !state.dropped.get()
            && state.ingress.get() == IngressLoan::Lent
            && self.poll_timer(context);
        let closed = state.ingress.replace(IngressLoan::InStream) == IngressLoan::LentAcrossClose;
        match polled {
            Poll::Ready(result) => StoredPoll::Ready { result, closed },
            Poll::Pending if closed => StoredPoll::Closed,
            Poll::Pending => StoredPoll::Pending { expired },
        }
    }

    /// Fails a read polled once the handle's Drop has begun.
    fn cancel_after_drop(
        &mut self,
        state: &TransportState,
    ) -> Poll<Result<TransportRead, HttpError>> {
        if let ReadStage::Pending(id) = self.stage {
            state.finish_read(id);
        }
        self.stage = ReadStage::Done;
        Poll::Ready(Err(cancelled()))
    }

    /// Fails read `id` on a sampled cancellation. The caller keeps the stream
    /// across the call, because the release can drop the handle.
    fn cancel(&mut self, state: &TransportState, id: u64, stored: StoredRead) -> HttpError {
        if stored.starter == Some(id) {
            state.cancel_ingress(stored);
        } else {
            state.read.borrow_mut().stored = Some(stored);
        }
        cancelled()
    }

    fn poll_read(
        &mut self,
        state: &TransportState,
        id: u64,
        cancelled: bool,
        context: &mut Context<'_>,
    ) -> Poll<Result<TransportRead, HttpError>> {
        let buffer = self.maximum.min(self.allocation_size);
        loop {
            if state.dropped.get() {
                return Poll::Ready(Err(self::cancelled()));
            }
            let next = {
                let mut read = state.read.borrow_mut();
                if let Some(remainder) = read.remainder.as_mut() {
                    let data = remainder.split_at_most(buffer);
                    if remainder.is_empty() {
                        read.remainder = None;
                    }
                    return Poll::Ready(Ok(TransportRead::new(data, false)));
                }
                // Only a read parked on a stored read or on a close gets here
                // with a sampled cancellation, so a missing stored read means
                // that a close released it. The cancellation wins, and the
                // close's error stays for the next read.
                if cancelled && read.stored.is_none() {
                    return Poll::Ready(Err(self::cancelled()));
                }
                if let Some(error) = read.error.take() {
                    return Poll::Ready(Err(error));
                }
                if read.closing {
                    NextRead::AwaitClose
                } else {
                    read.stored.take().map_or(NextRead::Start, NextRead::Poll)
                }
            };
            // If the handle's Drop begins inside a source call, including the
            // release of the body read, the stream's Drop runs in this read,
            // after the read drops its body read.
            let (mut stored, stream) = match next {
                NextRead::AwaitClose => {
                    // The close wakes the read once it has published the error.
                    if state.park_read(context.waker(), ReadState::awaits_close) {
                        return Poll::Pending;
                    }
                    continue;
                }
                NextRead::Poll(stored) => (stored, state.stream()),
                NextRead::Start => {
                    let stream = state.stream();
                    if !stream.can_read() {
                        return Poll::Ready(Ok(eof()));
                    }
                    let maximum = u32::try_from(buffer).unwrap_or(u32::MAX);
                    let (stored, closed) = state.start_body_read(&stream, id, maximum);
                    if state.dropped.get() {
                        drop(stored);
                        drop(stream);
                        return Poll::Ready(Err(self::cancelled()));
                    }
                    if closed {
                        state.close_stored(stored);
                        drop(stream);
                        continue;
                    }
                    (stored, stream)
                }
            };
            if cancelled && stored.parked {
                let error = self.cancel(state, id, stored);
                drop(stream);
                return Poll::Ready(Err(error));
            }
            let (event, closed) = match self.poll_stored(state, &mut stored, cancelled, context) {
                StoredPoll::Closed => {
                    state.close_stored(stored);
                    drop(stream);
                    continue;
                }
                StoredPoll::Pending { expired } => {
                    if state.dropped.get() {
                        drop(stored);
                        drop(stream);
                        return Poll::Ready(Err(self::cancelled()));
                    }
                    stored.parked = true;
                    if cancelled {
                        let error = self.cancel(state, id, stored);
                        drop(stream);
                        return Poll::Ready(Err(error));
                    }
                    drop(stream);
                    state.read.borrow_mut().stored = Some(stored);
                    if expired {
                        return Poll::Ready(Err(HttpError::with_message(
                            HttpErrorCode::READ_TIMEOUT,
                            b"Timed out waiting for body event",
                        )));
                    }
                    // The stored read is in place for caller code that the
                    // waker's clone runs, so a close there releases it.
                    if state.park_read(context.waker(), ReadState::holds_stored) {
                        return Poll::Pending;
                    }
                    continue;
                }
                StoredPoll::Ready { result, closed } => (result, closed),
            };
            // A close made during the poll or the release did not see a stored
            // read, so it did not queue an error, and the read keeps the body
            // read's result.
            let (_, released_closed) = state.release_stored(stored);
            if closed || released_closed {
                state.shutdown_read();
            }
            drop(stream);
            self.deadline = None;
            self.timer = None;
            let event = match event {
                Ok(event) => event,
                Err(error) => return Poll::Ready(Err(transport_read_error(error))),
            };
            let eom = event.eom();
            match event.into_kind() {
                HttpBodyEventKind::Body(mut body) if !body.is_empty() => {
                    let data = body.split_at_most(buffer);
                    if !body.is_empty() {
                        state.read.borrow_mut().remainder = Some(body);
                    }
                    return Poll::Ready(Ok(TransportRead::new(data, false)));
                }
                _ if eom => return Poll::Ready(Ok(eof())),
                _ => {}
            }
        }
    }
}

impl Future for ReadOperation {
    type Output = Result<TransportRead, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert_ne!(
            this.stage,
            ReadStage::Done,
            "a completed read cannot be polled again"
        );
        let Some(state) = this.transport.upgrade() else {
            this.stage = ReadStage::Done;
            return Poll::Ready(Err(cancelled()));
        };
        if state.dropped.get() {
            return this.cancel_after_drop(&state);
        }
        let cancelled = this.registration.update_waker(context.waker());
        // The registration clones and releases wakers, which can run caller
        // code that drops the handle.
        if state.dropped.get() {
            return this.cancel_after_drop(&state);
        }
        let id = match this.stage {
            ReadStage::Pending(id) => id,
            _ => match this.start(&state, cancelled) {
                Ok(id) => {
                    this.stage = ReadStage::Pending(id);
                    id
                }
                Err(result) => {
                    this.stage = ReadStage::Done;
                    return Poll::Ready(result);
                }
            },
        };
        let result = this.poll_read(&state, id, cancelled, context);
        if result.is_ready() {
            this.stage = ReadStage::Done;
            state.finish_read(id);
        }
        result
    }
}

impl Drop for ReadOperation {
    fn drop(&mut self) {
        let ReadStage::Pending(id) = self.stage else {
            return;
        };
        self.stage = ReadStage::Done;
        let Some(state) = self.transport.upgrade() else {
            return;
        };
        let starter = state
            .read
            .borrow()
            .stored
            .as_ref()
            .is_some_and(|stored| stored.starter == Some(id));
        if starter && self.cancellation.is_cancellation_requested() {
            let stored = state.read.borrow_mut().stored.take();
            if let Some(stored) = stored {
                // The release can drop the handle, so the stream's close waits
                // until the release returns.
                let stream = state.stream.borrow().clone();
                state.cancel_ingress(stored);
                drop(stream);
            }
        }
        state.finish_read(id);
    }
}

enum WriteStage {
    NotStarted,
    Waiting(TimedWait),
    Done,
}

/// The future returned by `HttpConnectTransport::write`. It starts at its
/// first poll.
struct WriteOperation {
    transport: Weak<TransportState>,
    data: Option<BufQueue>,
    length: usize,
    timeout: Duration,
    cancellation: CancellationToken,
    stage: WriteStage,
    in_progress: bool,
}

impl WriteOperation {
    fn end(&mut self, state: &TransportState) {
        self.stage = WriteStage::Done;
        if std::mem::take(&mut self.in_progress) {
            state.write_in_progress.set(false);
            state.pending_eom.set(false);
        }
    }

    fn fail(
        &mut self,
        state: &TransportState,
        code: HttpErrorCode,
        message: impl AsRef<[u8]>,
    ) -> Poll<Result<WriteInfo, TransportWriteError>> {
        self.end(state);
        Poll::Ready(Err(TransportWriteError::new(
            HttpError::with_message(code, message),
            0,
        )))
    }

    /// Runs the entry checks, marks the write in progress, and starts the
    /// baton wait.
    fn start(
        &mut self,
        state: &TransportState,
    ) -> Option<Poll<Result<WriteInfo, TransportWriteError>>> {
        if self.cancellation.is_cancellation_requested() {
            self.stage = WriteStage::Done;
            return Some(Poll::Ready(Err(TransportWriteError::new(cancelled(), 0))));
        }
        if self.length == 0 {
            self.stage = WriteStage::Done;
            return Some(Poll::Ready(Ok(WriteInfo::default())));
        }
        if state.write_in_progress.get() {
            self.stage = WriteStage::Done;
            return Some(Poll::Ready(Err(TransportWriteError::new(
                HttpError::with_message(
                    HttpErrorCode::INVALID_STATE_TRANSITION,
                    b"HTTPConnectTransport already has a write in progress",
                ),
                0,
            ))));
        }
        state.write_in_progress.set(true);
        self.in_progress = true;
        let not_open = {
            let stream = state.stream();
            (!stream.can_write()).then(|| {
                stream
                    .egress_error()
                    .map_or_else(|| b"write after close".to_vec(), HttpError::describe)
            })
        };
        if let Some(message) = not_open {
            return Some(self.fail(state, HttpErrorCode::TRANSPORT_WRITE_ERROR, message));
        }
        let window = state
            .window()
            .expect("should hold the write baton until the transport drops");
        if !self.timeout.is_zero() {
            window.set_timeout(Some(self.timeout));
        }
        self.stage = WriteStage::Waiting(window.wait(self.cancellation.clone()));
        None
    }

    /// Sends the data after the baton wait succeeds.
    fn commit(&mut self, state: &TransportState) -> Poll<Result<WriteInfo, TransportWriteError>> {
        if state.egress_complete.get() {
            return self.fail(
                state,
                HttpErrorCode::TRANSPORT_WRITE_ERROR,
                b"HTTPConnectTransport: egress closed",
            );
        }
        let data = self.data.take().expect("should commit its data once");
        let stream = state.stream();
        let eom = state.pending_eom.replace(false);
        state.eom_in_body.set(eom);
        let flow = stream.egress_body(data);
        state.eom_in_body.set(false);
        // `egress_body` can wake the egress consumer inline, whose waker can
        // drop the handle. The write keeps its result, and the stream's close
        // then sends the EOM.
        if eom && !state.dropped.get() && stream.can_write() {
            stream.egress_eom();
        }
        if flow != FlowControlState::Open
            && let Some(window) = state.window()
        {
            window.reset();
        }
        self.end(state);
        drop(stream);
        Poll::Ready(Ok(WriteInfo {
            bytes_written: self.length,
        }))
    }
}

impl Future for WriteOperation {
    type Output = Result<WriteInfo, TransportWriteError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert!(
            !matches!(this.stage, WriteStage::Done),
            "a completed write cannot be polled again"
        );
        let Some(state) = this.transport.upgrade() else {
            this.stage = WriteStage::Done;
            this.in_progress = false;
            return Poll::Ready(Err(TransportWriteError::new(cancelled(), 0)));
        };
        if state.dropped.get() {
            this.end(&state);
            return Poll::Ready(Err(TransportWriteError::new(cancelled(), 0)));
        }
        if matches!(this.stage, WriteStage::NotStarted)
            && let Some(result) = this.start(&state)
        {
            return result;
        }
        let WriteStage::Waiting(wait) = &mut this.stage else {
            unreachable!("a started write waits on the baton");
        };
        // The wait releases a replaced waker while it borrows the baton's
        // state, and that release can drop a task that owns the handle.
        // Holding `window` defers the baton's Drop, which borrows that state
        // too, until the wait returns.
        let window = state.window();
        let status = std::task::ready!(Pin::new(wait).poll(context));
        drop(window);
        this.stage = WriteStage::Done;
        if state.dropped.get() {
            this.end(&state);
            return Poll::Ready(Err(TransportWriteError::new(cancelled(), 0)));
        }
        match status {
            WaitStatus::Signalled => this.commit(&state),
            WaitStatus::TimedOut => {
                this.fail(&state, HttpErrorCode::WRITE_TIMEOUT, b"Write timed out")
            }
            WaitStatus::Cancelled | WaitStatus::NotReady => {
                this.end(&state);
                Poll::Ready(Err(TransportWriteError::new(cancelled(), 0)))
            }
        }
    }
}

impl Drop for WriteOperation {
    fn drop(&mut self) {
        let state = self.transport.upgrade();
        // A dropped wait commits a requested cancellation to the baton. It
        // releases the write's waker while it borrows the baton's state, and
        // that release can drop a task that owns the handle. Holding `window`
        // defers the baton's Drop, which borrows that state too, until the
        // wait is gone.
        let window = state.as_ref().and_then(|state| state.window());
        self.stage = WriteStage::Done;
        drop(window);
        if std::mem::take(&mut self.in_progress)
            && let Some(state) = state
        {
            state.write_in_progress.set(false);
            state.pending_eom.set(false);
        }
    }
}
