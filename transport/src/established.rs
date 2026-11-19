/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Optional Tokio adapter for I/O established by an application connector.
//!
//! The connector retains responsibility for routing, authentication, protocol
//! selection, and keeping the I/O's originating reactor alive. Adoption does
//! not move reactor ownership or reinterpret an HTTP origin as a socket peer.

use std::future::Future;
use std::io;
use std::io::IoSlice;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_runtime::worker::WorkerCleanupObserver;
use roxygen_runtime::worker::WorkerContext;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;

use crate::byte_stream::ByteStreamLimits;
use crate::byte_stream::OwnedByteStream;
use crate::byte_stream::PeerIdentity;
use crate::byte_stream::TransportEndpoint;
use crate::byte_stream::TransportPanic;
use crate::byte_stream::finish_transport_panic;
use crate::byte_stream::retain_transport_panic;
use crate::tcp::StreamAttachment;
use crate::tcp::TokioByteStream;
use crate::tcp::owned_from_stream;
use crate::tcp::owned_from_stream_with_completion;

/// An established connection and metadata supplied by its trusted connector.
///
/// This adapter does not authenticate `peer_identity`. Applications must not
/// populate it from untrusted request headers. Unix endpoints describe the
/// immediate hop even when a proxy represents a secure upstream origin.
pub struct EstablishedIo<Io> {
    /// The connected I/O, including any already-completed TLS handshake.
    pub io: Io,
    /// The actual local transport endpoint.
    pub local_endpoint: TransportEndpoint,
    /// The actual remote transport endpoint.
    pub peer_endpoint: TransportEndpoint,
    /// The connector's already-verified peer identity, if any.
    pub peer_identity: Option<PeerIdentity>,
}

impl<Io> EstablishedIo<Io> {
    /// Packages established I/O without connecting, handshaking, or inspecting it.
    pub fn new(
        io: Io,
        local_endpoint: TransportEndpoint,
        peer_endpoint: TransportEndpoint,
        peer_identity: Option<PeerIdentity>,
    ) -> Self {
        Self {
            io,
            local_endpoint,
            peer_endpoint,
            peer_identity,
        }
    }
}

/// Attaches established Tokio I/O to its permanent owner worker.
///
/// Neither `Send` nor `Unpin` is required. A facade transferring an
/// `EstablishedIo` between threads must separately require `Io: Send`.
/// Polling and destruction stay on this worker. The originating reactor must
/// remain alive until connection cleanup completes.
///
/// Each bounded write is flushed before successful completion. Reported partial
/// progress counts bytes accepted by `AsyncWrite`, not peer delivery. Write
/// cancellation, timeout, or I/O failure discards the connection rather than
/// replaying potentially buffered bytes. Read cancellation releases the pending
/// operation without discarding the connection.
///
/// Byte events and kernel zero-copy are unsupported. `close_with_reset` cancels
/// and drops the I/O but cannot guarantee a TCP RST. Graceful close drives
/// flush and shutdown on the worker-owned shutdown task. The synchronous
/// `close` and `shutdown_write` request that work but do not wait for it to
/// complete.
/// Non-Internet endpoints remain available through `local_endpoint` and
/// `peer_endpoint`. The socket-address accessors return uninitialized values
/// for them.
pub fn adopt_local<Io>(
    worker: &WorkerContext,
    established: EstablishedIo<Io>,
    limits: ByteStreamLimits,
) -> Result<OwnedByteStream, HttpError>
where
    Io: AsyncRead + AsyncWrite + 'static,
{
    let EstablishedIo {
        io,
        local_endpoint,
        peer_endpoint,
        peer_identity,
    } = established;
    let local_address = local_endpoint.socket_address().cloned().unwrap_or_default();
    let peer_address = peer_endpoint.socket_address().cloned().unwrap_or_default();
    owned_from_stream(
        worker,
        AdoptedStream::new(io),
        local_address,
        peer_address,
        peer_identity,
        limits,
    )
    .map(|transport| transport.with_endpoints(local_endpoint, peer_endpoint))
}

/// Adopts I/O like [`adopt_local`] and returns a handle that cancels it and
/// reports when it has been destroyed.
///
/// The I/O remains owner-local and need not be `Send` or `Unpin`. Cancellation
/// uses the transport's own cleanup scope, so it stays effective through
/// half-close, session completion, and drop of the public transport. The
/// original reactor must remain alive until retirement. Everything else
/// behaves as in [`adopt_local`].
pub fn adopt_local_with_completion<Io>(
    worker: &WorkerContext,
    established: EstablishedIo<Io>,
    limits: ByteStreamLimits,
) -> Result<(OwnedByteStream, AdoptedIoControl), HttpError>
where
    Io: AsyncRead + AsyncWrite + 'static,
{
    if !worker.is_owner_thread() {
        return Err(HttpError::with_message(
            HttpErrorCode::INVALID_STATE_TRANSITION,
            b"adopted I/O requires its worker owner thread",
        ));
    }
    if !worker.handle().is_accepting() {
        return Err(HttpError::with_message(
            HttpErrorCode::DROPPED,
            b"worker is not accepting adopted I/O",
        ));
    }
    let EstablishedIo {
        io,
        local_endpoint,
        peer_endpoint,
        peer_identity,
    } = established;
    let shared = Arc::new(AdoptedIoState::default());
    let stream = AdoptedStream::with_completion(io, Arc::clone(&shared));
    let (transport, scope) = owned_from_stream_with_completion(
        worker,
        stream,
        StreamAttachment {
            local_address: local_endpoint.socket_address().cloned().unwrap_or_default(),
            peer_address: peer_endpoint.socket_address().cloned().unwrap_or_default(),
            peer_identity,
            limits,
        },
        Arc::clone(&shared),
    )?;
    if !worker.handle().is_accepting() {
        return Err(HttpError::with_message(
            HttpErrorCode::DROPPED,
            b"worker stopped accepting adopted I/O during initialization",
        ));
    }
    Ok((
        transport.with_endpoints(local_endpoint, peer_endpoint),
        AdoptedIoControl {
            completion: AdoptedIoCompletion::new(shared, scope),
        },
    ))
}

/// Transferable cancellation handle that does not own the I/O or its worker.
///
/// Dropping this handle has no effect on cleanup.
#[derive(Clone)]
pub struct AdoptedIoControl {
    completion: AdoptedIoCompletion,
}

impl AdoptedIoControl {
    /// Requests disposal of the I/O on its owner worker, bypassing worker
    /// admission. The request is sticky and repeated calls are harmless. Await
    /// [`AdoptedIoCompletion::wait_retired`] to confirm retirement.
    pub fn cancel(&self) {
        let waker = {
            let mut state = self.completion.shared.lock();
            state.cancelled = true;
            state.driver.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Returns an independent observation that does not retain I/O ownership.
    pub fn completion(&self) -> AdoptedIoCompletion {
        self.completion.clone()
    }
}

/// Error naming the adopted-I/O retirement steps that could not be confirmed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AdoptedIoCompletionError {
    /// I/O destruction panicked or its publisher was lost.
    #[error("adopted I/O destruction was not confirmed")]
    IoDestructionUnconfirmed,
    /// Destruction of the registered drain future was not confirmed.
    #[error("adopted I/O drain-scope retirement was not confirmed")]
    DrainRetirementUnconfirmed,
    /// Both the I/O destruction and the drain retirement are unconfirmed.
    #[error("adopted I/O destruction and drain-scope retirement were not confirmed")]
    BothUnconfirmed,
}

/// Transferable, repeatable observation of I/O destruction and drain retirement.
///
/// Success means the adopted value, its owned fields, the registered drain
/// future and its captures have all been dropped. It does not report whether
/// the I/O succeeded or reached the peer, and handles to the same connection
/// held outside the adopted value can still be open. Dropping this value or
/// its wait future has no effect on cleanup.
#[derive(Clone)]
pub struct AdoptedIoCompletion {
    shared: Arc<AdoptedIoState>,
    scope: WorkerCleanupObserver,
}

impl AdoptedIoCompletion {
    pub(crate) fn new(shared: Arc<AdoptedIoState>, scope: WorkerCleanupObserver) -> Self {
        Self { shared, scope }
    }

    /// Waits for both the I/O value and the drain future, even when one of them
    /// fails.
    pub fn wait_retired(
        &self,
    ) -> impl Future<Output = Result<(), AdoptedIoCompletionError>> + Send + 'static {
        let io = IoDropWait {
            shared: Arc::clone(&self.shared),
            slot: None,
        };
        let scope = self.scope.wait();
        async move {
            match tokio::join!(io, scope) {
                (true, Ok(())) => Ok(()),
                (false, Ok(())) => Err(AdoptedIoCompletionError::IoDestructionUnconfirmed),
                (true, Err(_)) => Err(AdoptedIoCompletionError::DrainRetirementUnconfirmed),
                (false, Err(_)) => Err(AdoptedIoCompletionError::BothUnconfirmed),
            }
        }
    }
}

#[derive(Default)]
struct IoRetirementState {
    outcome: Option<bool>,
    cancelled: bool,
    driver: Option<Waker>,
    waiters: Vec<Option<Waker>>,
}

#[derive(Default)]
pub(crate) struct AdoptedIoState(Mutex<IoRetirementState>);

impl AdoptedIoState {
    fn lock(&self) -> std::sync::MutexGuard<'_, IoRetirementState> {
        self.0.lock().unwrap_or_else(|error| error.into_inner())
    }

    pub(crate) fn poll_control(&self, context: &Context<'_>) -> (bool, bool) {
        let waker = context.waker().clone();
        let (cancelled, disposed, previous) = {
            let mut state = self.lock();
            (
                state.cancelled,
                state.outcome.is_some(),
                state.driver.replace(waker),
            )
        };
        drop(previous);
        (cancelled, disposed)
    }

    fn publish(&self, confirmed: bool) -> Option<TransportPanic> {
        let (driver, waiters) = {
            let mut state = self.lock();
            if state.outcome.is_some() {
                return None;
            }
            state.outcome = Some(confirmed);
            (state.driver.take(), std::mem::take(&mut state.waiters))
        };
        let mut failure = None;
        for waker in driver.into_iter().chain(waiters.into_iter().flatten()) {
            retain_transport_panic(
                &mut failure,
                catch_unwind(AssertUnwindSafe(|| waker.wake())).err(),
            );
        }
        failure
    }

    pub(crate) fn retire_driver(&self) {
        let waker = self.lock().driver.take();
        drop(waker);
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.lock().cancelled
    }
}

pub(crate) struct IoDropPublisher(Arc<AdoptedIoState>);

impl IoDropPublisher {
    pub(crate) fn new(shared: Arc<AdoptedIoState>) -> Self {
        Self(shared)
    }

    pub(crate) fn finish(self, confirmed: bool) -> Option<TransportPanic> {
        self.0.publish(confirmed)
    }
}

impl Drop for IoDropPublisher {
    fn drop(&mut self) {
        finish_transport_panic(self.0.publish(false), true);
    }
}

struct IoDropWait {
    shared: Arc<AdoptedIoState>,
    slot: Option<usize>,
}

impl Future for IoDropWait {
    type Output = bool;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<bool> {
        let wait = self.get_mut();
        let waker = context.waker().clone();
        let mut state = wait.shared.lock();
        if let Some(outcome) = state.outcome {
            return Poll::Ready(outcome);
        }
        let previous = if let Some(slot) = wait.slot {
            state.waiters[slot].replace(waker)
        } else if let Some(slot) = state.waiters.iter().position(Option::is_none) {
            state.waiters[slot] = Some(waker);
            wait.slot = Some(slot);
            None
        } else {
            wait.slot = Some(state.waiters.len());
            state.waiters.push(Some(waker));
            None
        };
        drop(state);
        drop(previous);
        Poll::Pending
    }
}

impl Drop for IoDropWait {
    fn drop(&mut self) {
        let waker = self.slot.and_then(|slot| {
            self.shared
                .lock()
                .waiters
                .get_mut(slot)
                .and_then(Option::take)
        });
        drop(waker);
    }
}

#[derive(Default)]
struct IoWake {
    observers: Mutex<[Option<Waker>; 3]>,
}

#[derive(Clone, Copy)]
enum IoOperation {
    Read,
    Write,
    Shutdown,
}

impl IoWake {
    fn register(&self, operation: IoOperation, waker: &Waker) {
        let replacement = waker.clone();
        let previous = self
            .observers
            .lock()
            .expect("I/O wake registration contains no user code")[operation as usize]
            .replace(replacement);
        drop(previous);
    }

    fn take(&self, operation: IoOperation) -> Option<Waker> {
        self.observers
            .lock()
            .expect("I/O wake registration contains no user code")[operation as usize]
            .take()
    }

    fn take_all(&self) -> [Option<Waker>; 3] {
        let mut observers = self
            .observers
            .lock()
            .expect("I/O wake registration contains no user code");
        std::mem::take(&mut *observers)
    }
}

impl Wake for IoWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        wake_observers(self.take_all());
    }
}

fn wake_observers(observers: [Option<Waker>; 3]) {
    let mut first_panic = None;
    for waker in observers.into_iter().flatten() {
        let result = catch_unwind(AssertUnwindSafe(|| waker.wake()));
        retain_transport_panic(&mut first_panic, result.err());
    }
    if let Some(payload) = first_panic {
        resume_unwind(payload);
    }
}

pub(crate) struct AdoptedStream<Io> {
    io: Option<Pin<Box<Io>>>,
    wake: Arc<IoWake>,
    physical_waker: Waker,
    shutdown_flushed: bool,
    retirement: Option<IoDropPublisher>,
}

impl<Io> AdoptedStream<Io> {
    pub(crate) fn with_completion(io: Io, shared: Arc<AdoptedIoState>) -> Self {
        let mut stream = Self::new(io);
        stream.retirement = Some(IoDropPublisher(shared));
        stream
    }

    pub(crate) fn new(io: Io) -> Self {
        let wake = Arc::new(IoWake::default());
        let physical_waker = Waker::from(Arc::clone(&wake));
        Self {
            io: Some(Box::pin(io)),
            wake,
            physical_waker,
            shutdown_flushed: false,
            retirement: None,
        }
    }

    fn close_io(&mut self) {
        let observers = self.wake.take_all();
        let mut failure = catch_unwind(AssertUnwindSafe(|| drop(self.io.take()))).err();
        if let Some(retirement) = self.retirement.take() {
            let confirmed = failure.is_none();
            retain_transport_panic(&mut failure, retirement.0.publish(confirmed));
        }
        retain_transport_panic(
            &mut failure,
            catch_unwind(AssertUnwindSafe(|| wake_observers(observers))).err(),
        );
        finish_transport_panic(failure, false);
    }

    fn poll_write_operation<Output>(
        &mut self,
        context: &mut Context<'_>,
        operation: IoOperation,
        poll: impl FnOnce(Pin<&mut Io>, &mut Context<'_>) -> Poll<io::Result<Output>>,
    ) -> Poll<io::Result<Output>> {
        let Some(io) = self.io.as_mut() else {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        };
        self.wake.register(operation, context.waker());
        let result = poll(io.as_mut(), &mut Context::from_waker(&self.physical_waker));
        let mut failure = None;
        if result.is_ready() {
            retain_transport_panic(
                &mut failure,
                catch_unwind(AssertUnwindSafe(|| {
                    drop(self.wake.take(operation));
                }))
                .err(),
            );
        }
        if matches!(&result, Poll::Ready(Err(_))) {
            retain_transport_panic(
                &mut failure,
                catch_unwind(AssertUnwindSafe(|| {
                    self.close_io();
                }))
                .err(),
            );
        }
        if failure.is_some() {
            retain_transport_panic(
                &mut failure,
                catch_unwind(AssertUnwindSafe(|| {
                    drop(result);
                }))
                .err(),
            );
            std::panic::resume_unwind(
                failure.expect("failed readiness cleanup retains its first panic"),
            );
        }
        result
    }
}

impl<Io> Drop for AdoptedStream<Io> {
    fn drop(&mut self) {
        if self.retirement.is_some() {
            let unwinding = std::thread::panicking();
            let failure = catch_unwind(AssertUnwindSafe(|| self.close_io())).err();
            finish_transport_panic(failure, unwinding);
        } else {
            drop(self.wake.take_all());
        }
    }
}

impl<Io> AsyncRead for AdoptedStream<Io>
where
    Io: AsyncRead,
{
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let stream = self.get_mut();
        let Some(io) = stream.io.as_mut() else {
            return Poll::Ready(Err(io::ErrorKind::NotConnected.into()));
        };
        stream.wake.register(IoOperation::Read, context.waker());
        let result = io
            .as_mut()
            .poll_read(&mut Context::from_waker(&stream.physical_waker), buffer);
        if result.is_ready()
            && let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
                drop(stream.wake.take(IoOperation::Read));
            }))
        {
            let mut failure = Some(payload);
            retain_transport_panic(
                &mut failure,
                catch_unwind(AssertUnwindSafe(|| drop(result))).err(),
            );
            resume_unwind(failure.expect("failed read readiness cleanup retains its first panic"));
        }
        result
    }
}

impl<Io> AsyncWrite for AdoptedStream<Io>
where
    Io: AsyncWrite,
{
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut()
            .poll_write_operation(context, IoOperation::Write, |io, context| {
                check_write_progress(io.poll_write(context, buffer), !buffer.is_empty())
            })
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut()
            .poll_write_operation(context, IoOperation::Write, |io, context| {
                io.poll_flush(context)
            })
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let stream = self.get_mut();
        if !stream.shutdown_flushed {
            match stream.poll_write_operation(context, IoOperation::Shutdown, |io, context| {
                io.poll_flush(context)
            }) {
                Poll::Ready(Ok(())) => stream.shutdown_flushed = true,
                result => return result,
            }
        }
        stream.poll_write_operation(context, IoOperation::Shutdown, |io, context| {
            io.poll_shutdown(context)
        })
    }

    fn is_write_vectored(&self) -> bool {
        self.io.as_ref().is_some_and(|io| io.is_write_vectored())
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let nonempty = buffers.iter().any(|buffer| !buffer.is_empty());
        self.get_mut()
            .poll_write_operation(context, IoOperation::Write, |io, context| {
                check_write_progress(io.poll_write_vectored(context, buffers), nonempty)
            })
    }
}

fn check_write_progress(
    result: Poll<io::Result<usize>>,
    nonempty: bool,
) -> Poll<io::Result<usize>> {
    match result {
        Poll::Ready(Ok(0)) if nonempty => Poll::Ready(Err(io::ErrorKind::WriteZero.into())),
        result => result,
    }
}

impl<Io> TokioByteStream for AdoptedStream<Io>
where
    Io: AsyncRead + AsyncWrite + 'static,
{
    fn prepare_reset(&self) {}

    fn cancel_read(&mut self) {
        drop(self.wake.take(IoOperation::Read));
    }

    fn cancel_owned_write(&mut self) {
        self.close_io();
    }

    fn poll_write_flush(&mut self, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self).poll_flush(context)
    }
}
