//! Tokio TCP adapter for the worker-local byte-stream contract.

use std::cell::Cell;
use std::cell::RefCell;
use std::future::Future;
use std::future::poll_fn;
use std::io;
use std::io::IoSlice;
use std::mem::MaybeUninit;
use std::net::IpAddr;
use std::net::SocketAddr;
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::task::ready;
use std::thread::ThreadId;
use std::time::Duration;

use roxygen_core::buf_queue::BufQueue;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::message::SocketAddress;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::thread::current_id;
use roxygen_runtime::wait::TimedBaton;
use roxygen_runtime::wait::WaitStatus;
use roxygen_runtime::worker::CleanupRegistrationError;
use roxygen_runtime::worker::WorkerCleanupObserver;
use roxygen_runtime::worker::WorkerCleanupRegistration;
use roxygen_runtime::worker::WorkerCleanupTaskRegistration;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerState;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::net::TcpSocket;
use tokio::net::TcpStream;
use tokio::task::coop;
use tokio::time::Instant;

use crate::byte_event::ByteEventWriteFlags;
use crate::byte_stream::ByteEventCapability;
use crate::byte_stream::ByteEventWriteState;
use crate::byte_stream::ByteStreamIo;
use crate::byte_stream::ByteStreamLimits;
use crate::byte_stream::LocalTransportFuture;
use crate::byte_stream::OwnedByteStream;
use crate::byte_stream::OwnedTransportReset;
use crate::byte_stream::PeerIdentity;
use crate::byte_stream::PolledReader;
use crate::byte_stream::QueuedWriter;
use crate::byte_stream::TransportPanic;
use crate::byte_stream::TransportRead;
use crate::byte_stream::TransportRetirement;
use crate::byte_stream::WriteProgress;
use crate::byte_stream::finish_transport_panic;
#[cfg(test)]
use crate::byte_stream::record_close_path_error;
use crate::byte_stream::retain_transport_panic;
use crate::established::AdoptedIoCompletion;
use crate::established::AdoptedIoState;
use crate::established::IoDropPublisher;
use crate::tcp_zero_copy::TcpZeroCopy;
use crate::tls::CiphertextWriteTimeout;

/// Concrete TCP constructor whose public surface contains only Roxygen types.
pub struct TcpTransport;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TcpSocketOption {
    pub level: i32,
    pub name: i32,
    pub value: TcpSocketOptionValue,
    pub phase: TcpSocketOptionPhase,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TcpSocketOptionValue {
    Integer(i32),
    Bytes(Box<[u8]>),
}

impl TcpSocketOption {
    /// Enables or disables TCP keepalive after a connection is established.
    #[cfg(unix)]
    pub fn keep_alive(enabled: bool) -> Self {
        Self {
            level: libc::SOL_SOCKET,
            name: libc::SO_KEEPALIVE,
            value: TcpSocketOptionValue::Integer(enabled as i32),
            phase: TcpSocketOptionPhase::PostConnect,
        }
    }

    #[cfg(unix)]
    pub fn no_delay(enabled: bool) -> Self {
        Self::no_delay_at(enabled, TcpSocketOptionPhase::PostConnect)
    }

    #[cfg(unix)]
    pub fn no_delay_at(enabled: bool, phase: TcpSocketOptionPhase) -> Self {
        Self {
            level: libc::IPPROTO_TCP,
            name: libc::TCP_NODELAY,
            value: TcpSocketOptionValue::Integer(enabled as i32),
            phase,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TcpSocketOptionPhase {
    PreBind,
    PreConnect,
    PostConnect,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TcpConnectOptions {
    pub bind_address: Option<SocketAddress>,
    pub socket_options: Vec<TcpSocketOption>,
    pub congestion_flavor: Option<Box<str>>,
}

impl TcpTransport {
    /// Connects a TCP socket and binds the resulting byte stream to `context`.
    pub fn connect(
        context: &WorkerContext,
        address: SocketAddress,
        limits: ByteStreamLimits,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<OwnedByteStream, HttpError>> {
        Self::connect_with_options(
            context,
            address,
            limits,
            timeout,
            cancellation,
            TcpConnectOptions::default(),
        )
    }

    pub fn connect_with_options(
        context: &WorkerContext,
        address: SocketAddress,
        limits: ByteStreamLimits,
        timeout: Duration,
        cancellation: CancellationToken,
        options: TcpConnectOptions,
    ) -> LocalTransportFuture<Result<OwnedByteStream, HttpError>> {
        if let Err(error) = validate_context(context) {
            return Box::pin(std::future::ready(Err(error)));
        }
        let setup_context = context.clone();
        let setup = async move {
            let deadline = ConnectDeadline::new(timeout);
            let congestion_flavor = options.congestion_flavor.clone();
            let connected =
                connect_socket_with_options(address, &deadline, cancellation, options).await?;
            set_congestion_flavor_best_effort(&connected.stream, congestion_flavor.as_deref());
            if setup_context.handle().state() != WorkerState::Open {
                return Err(dropped_error());
            }
            owned_from_stream(
                &setup_context,
                AcceptedTcpStream::new(connected.stream),
                connected.local_address,
                connected.peer_address,
                None,
                limits,
            )
        };
        setup_future(context, setup)
            .unwrap_or_else(|error| Box::pin(std::future::ready(Err(error))))
    }

    /// Adopts a newly accepted standard TCP stream on its permanent worker.
    pub fn from_accepted(
        context: &WorkerContext,
        stream: std::net::TcpStream,
        limits: ByteStreamLimits,
    ) -> Result<OwnedByteStream, HttpError> {
        validate_context(context)?;
        let (stream, local_address, peer_address) = prepare_accepted_stream(stream)?;
        owned_from_stream(context, stream, local_address, peer_address, None, limits)
    }
}

/// Bytes gathered by one Tokio-backed read, in stream order.
pub(crate) struct OwnedRead {
    filled: BufQueue,
    tail: Vec<u8>,
}

impl OwnedRead {
    pub(crate) fn new(maximum: usize) -> Self {
        Self {
            filled: BufQueue::new(maximum),
            tail: Vec::new(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.filled.chain_length() + self.tail.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Makes an exact-size copy of `bytes` the read's only buffer.
    pub(crate) fn copy_from_slice(&mut self, bytes: &[u8]) {
        self.tail = bytes.to_vec();
    }

    /// Reserves the uninitialized first buffer that `poll_fill` then fills.
    fn reserve_first(&mut self, capacity: usize) {
        self.tail.reserve_exact(capacity);
        #[cfg(test)]
        record_tcp_first_reservation(self.tail.as_ptr());
    }

    /// Frees the reserved first buffer after a first poll returns `Pending`.
    fn release_first(&mut self) {
        self.tail = Vec::new();
    }

    /// Polls `stream` once into `capacity` bytes of a stack buffer, then
    /// copies the returned bytes into an exact-size first buffer (`DIV-008`).
    /// An offer above `STAGING_LIMIT` is instead polled into new uninitialized
    /// space.
    fn poll_stage<S>(
        &mut self,
        stream: &mut S,
        context: &mut Context<'_>,
        capacity: usize,
    ) -> Poll<io::Result<usize>>
    where
        S: AsyncRead + Unpin + ?Sized,
    {
        if capacity > STAGING_LIMIT {
            return self.poll_fill(stream, context, capacity);
        }
        let mut staging = [MaybeUninit::<u8>::uninit(); STAGING_LIMIT];
        let spare = &mut staging[..capacity];
        let start = spare.as_ptr().cast::<u8>();
        let mut buffer = tokio::io::ReadBuf::uninit(spare);
        ready!(Pin::new(stream).poll_read(context, &mut buffer))?;
        let filled = buffer.filled();
        if filled.as_ptr() != start || filled.len() > capacity {
            return Poll::Ready(Err(io::Error::other(
                "AsyncRead replaced the read buffer it was given",
            )));
        }
        self.copy_from_slice(filled);
        Poll::Ready(Ok(filled.len()))
    }

    /// Polls `stream` once into `capacity` bytes of new uninitialized space.
    fn poll_fill<S>(
        &mut self,
        stream: &mut S,
        context: &mut Context<'_>,
        capacity: usize,
    ) -> Poll<io::Result<usize>>
    where
        S: AsyncRead + Unpin + ?Sized,
    {
        if !self.tail.is_empty() {
            self.filled
                .push_vec(std::mem::take(&mut self.tail))
                .map_err(|_| io::Error::other("owned read exceeded its queue bound"))?;
        }
        self.tail.reserve_exact(capacity);
        let spare = &mut self.tail.spare_capacity_mut()[..capacity];
        let start = spare.as_ptr().cast::<u8>();
        let mut buffer = tokio::io::ReadBuf::uninit(spare);
        if let Err(error) = ready!(Pin::new(stream).poll_read(context, &mut buffer)) {
            return Poll::Ready(Err(error));
        }
        let filled = buffer.filled();
        if filled.as_ptr() != start || filled.len() > capacity {
            return Poll::Ready(Err(io::Error::other(
                "AsyncRead replaced the read buffer it was given",
            )));
        }
        let length = filled.len();
        // SAFETY: `tail` was empty, so its first `capacity` spare bytes were
        // offered. `filled` starts there and is no longer than that, so it is
        // the original `ReadBuf`, whose filled prefix is initialized: safe code
        // cannot rebuild a `ReadBuf` over this borrowed memory, and one over
        // other memory fails the start check.
        unsafe { self.tail.set_len(length) };
        Poll::Ready(Ok(length))
    }

    #[cfg(test)]
    pub(crate) fn tail_for_test(&self) -> &[u8] {
        &self.tail
    }

    /// Capacity the read holds, empty buffers included.
    #[cfg(test)]
    pub(crate) fn held_capacity_for_test(&self) -> usize {
        self.filled
            .backing_capacity_upper_bound()
            .expect("filled buffers are moved vectors with known capacity")
            + self.tail.capacity()
    }

    pub(crate) fn into_data(self) -> Result<BufQueue, HttpError> {
        let Self {
            filled: mut data,
            tail,
        } = self;
        if !tail.is_empty() {
            data.push_vec(tail)
                .map_err(|_| read_error(b"TCP read exceeded its requested bound"))?;
        }
        Ok(data)
    }
}

#[cfg(test)]
std::thread_local! {
    static TCP_FIRST_RESERVATIONS: Cell<usize> = const { Cell::new(0) };
    static TCP_FIRST_RESERVATION_START: Cell<Option<usize>> = const { Cell::new(None) };
}

#[cfg(test)]
fn record_tcp_first_reservation(start: *const u8) {
    TCP_FIRST_RESERVATIONS.with(|count| count.set(count.get() + 1));
    TCP_FIRST_RESERVATION_START.with(|recorded| recorded.set(Some(start as usize)));
}

/// TCP first-step reservations made on this thread.
#[cfg(test)]
pub(crate) fn tcp_first_reservations_for_test() -> usize {
    TCP_FIRST_RESERVATIONS.with(Cell::get)
}

/// Start address of this thread's latest TCP first-step reservation.
#[cfg(test)]
pub(crate) fn last_tcp_first_reservation_for_test() -> Option<usize> {
    TCP_FIRST_RESERVATION_START.with(Cell::get)
}

/// Largest first offer that a default read polls into a stack buffer.
const STAGING_LIMIT: usize = 16 * 1024;

/// The read loop that `TokioByteStream::poll_read_owned` documents, with
/// `first` as its first poll.
fn poll_gather<S, First>(
    stream: &mut S,
    context: &mut Context<'_>,
    maximum: usize,
    allocation_size: usize,
    read: &mut OwnedRead,
    mut first: First,
) -> Poll<io::Result<bool>>
where
    S: AsyncRead + Unpin + ?Sized,
    First: FnMut(&mut OwnedRead, &mut S, &mut Context<'_>, usize) -> Poll<io::Result<usize>>,
{
    loop {
        let gathered = read.len();
        let capacity = if gathered == 0 {
            allocation_size.min(maximum)
        } else {
            gathered.min(maximum - gathered)
        };
        let polled = if gathered == 0 {
            first(read, stream, context, capacity)
        } else {
            // A continuation refused for budget would end the batch early,
            // and the read returns after a pending one, so that poll must
            // not leave `context`'s waker registered.
            pin!(coop::unconstrained(poll_fn(|context| {
                read.poll_fill(stream, context, capacity)
            })))
            .poll(&mut Context::from_waker(Waker::noop()))
        };
        match polled {
            Poll::Pending if gathered == 0 => return Poll::Pending,
            Poll::Pending => return Poll::Ready(Ok(false)),
            Poll::Ready(Ok(0)) => return Poll::Ready(Ok(true)),
            Poll::Ready(Ok(length)) if length < capacity || gathered + length == maximum => {
                return Poll::Ready(Ok(false));
            }
            Poll::Ready(Ok(_)) => {}
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
        }
    }
}

/// A TCP read's first poll: Tokio's own `poll_read` into `capacity` bytes of
/// new uninitialized space, reserved only once Tokio reports the socket
/// readable and freed again if that poll returns `Pending`.
fn poll_tcp_first(
    read: &mut OwnedRead,
    stream: &mut TcpStream,
    context: &mut Context<'_>,
    capacity: usize,
) -> Poll<io::Result<usize>> {
    // The readiness poll below spends no budget, so a task without budget is
    // refused here, as Tokio's `poll_read` would refuse it.
    if !coop::has_budget_remaining() {
        drop(ready!(coop::poll_proceed(context)));
    }
    ready!(
        pin!(coop::unconstrained(poll_fn(|context| {
            stream.poll_read_ready(context)
        })))
        .poll(context)
    )?;
    read.reserve_first(capacity);
    // Constrained and with `context`, so that Tokio's retry after
    // `WouldBlock` spends budget and registers the caller's waker.
    let polled = read.poll_fill(stream, context, capacity);
    if polled.is_pending() {
        read.release_first();
    }
    polled
}

#[cfg(test)]
std::thread_local! {
    static IO_WRITE_FUTURES: Cell<usize> = const { Cell::new(0) };
    static IO_READ_FUTURES: Cell<usize> = const { Cell::new(0) };
}

/// Boxed `ByteStreamIo::read` futures built on this thread.
#[cfg(test)]
pub(crate) fn io_read_futures_for_test() -> usize {
    IO_READ_FUTURES.with(Cell::get)
}

/// Boxed `ByteStreamIo::write` futures built on this thread.
#[cfg(test)]
pub(crate) fn io_write_futures_for_test() -> usize {
    IO_WRITE_FUTURES.with(Cell::get)
}

pub(crate) trait TokioByteStream: AsyncRead + AsyncWrite + Unpin + 'static {
    /// Whether the transport offers a [`crate::byte_stream::OwnedWriteQueue`] over this
    /// stream: plaintext TCP.
    const WRITE_QUEUE: bool = false;

    fn prepare_reset(&self);

    fn prepare_write_timeout(&mut self, _timeout: Duration) {}

    /// Gathers at most `maximum` bytes for one read into `read`, returning
    /// whether the stream reached EOF after them. An error may also follow
    /// bytes already in `read`.
    ///
    /// The default polls into uninitialized capacity, never a zero-filled
    /// buffer. It offers `allocation_size` bytes first and, while a poll fills
    /// what it was offered (Folly's `READ_CONTINUE`), offers the gathered
    /// length again, capped at the rest of `maximum`, where Folly keeps a
    /// constant step (`DIV-017`). Once data is available a read therefore
    /// polls at most `1 + ceil(log2(ceil(maximum /
    /// allocation_size)))` times. It returns at a short poll, EOF, an error or
    /// `maximum`, and never waits once it holds bytes. Only the first poll
    /// spends Tokio's cooperative budget or registers `context`'s waker.
    ///
    /// A first offer of at most `STAGING_LIMIT` bytes is polled into a stack
    /// buffer, and the bytes it returns are copied into a buffer of exactly
    /// their length (`DIV-008`), so a pending read holds no buffer. A larger
    /// first offer is reserved before the poll and held while the read waits.
    fn poll_read_owned(
        &mut self,
        context: &mut Context<'_>,
        maximum: usize,
        allocation_size: usize,
        read: &mut OwnedRead,
    ) -> Poll<io::Result<bool>> {
        poll_gather(
            self,
            context,
            maximum,
            allocation_size,
            read,
            OwnedRead::poll_stage,
        )
    }

    fn cancel_read(&mut self) {}

    fn enable_zero_copy(&self, _context: &WorkerContext, _threshold: usize) -> io::Result<bool> {
        Ok(false)
    }

    fn set_zero_copy_reenable_threshold(&self, _threshold: usize) {}

    fn begin_owned_write(&mut self, _length: usize) -> bool {
        false
    }

    /// Attempts an ordinary borrowed write without registering a socket waker.
    fn try_write_borrowed(
        &mut self,
        _context: &mut Context<'_>,
        _buffers: &[IoSlice<'_>],
    ) -> io::Result<usize> {
        Err(io::ErrorKind::WouldBlock.into())
    }

    fn poll_write_owned(
        &mut self,
        context: &mut Context<'_>,
        data: &BufQueue,
        _zero_copy: bool,
    ) -> Poll<io::Result<usize>> {
        let buffers = data.iovecs().take(1024).collect::<Vec<_>>();
        Pin::new(self).poll_write_vectored(context, &buffers)
    }

    fn abandon_zero_copy(&mut self) {}

    fn cancel_owned_write(&mut self) {}

    fn poll_write_flush(&mut self, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    #[cfg(test)]
    fn zero_copy_owner_for_test(&self) -> Option<usize> {
        None
    }

    #[cfg(test)]
    fn zero_copy_pending_for_test(&self) -> usize {
        0
    }

    fn byte_event_capability(&self) -> ByteEventCapability {
        ByteEventCapability::Unsupported
    }

    fn register_byte_events(
        &mut self,
        _flags: ByteEventWriteFlags,
        _transport_offset: u64,
        _byte_events: Option<ByteEventWriteState>,
    ) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "transport byte events are unsupported",
        ))
    }

    fn take_byte_events(&mut self) -> Vec<ByteEventWriteState> {
        Vec::new()
    }
}

impl TokioByteStream for TcpStream {
    const WRITE_QUEUE: bool = true;

    fn prepare_reset(&self) {
        #[expect(
            deprecated,
            reason = "Tokio exposes abortive linger-zero reset only through this setter"
        )]
        let _ = self.set_linger(Some(Duration::ZERO));
    }

    /// Reads as the default does, except that the first poll is Tokio's own
    /// `poll_read` into a buffer reserved only once the socket is readable,
    /// so a pending read holds no buffer and the bytes are not copied.
    fn poll_read_owned(
        &mut self,
        context: &mut Context<'_>,
        maximum: usize,
        allocation_size: usize,
        read: &mut OwnedRead,
    ) -> Poll<io::Result<bool>> {
        poll_gather(
            self,
            context,
            maximum,
            allocation_size,
            read,
            poll_tcp_first,
        )
    }

    fn cancel_read(&mut self) {
        release_read_waker(self);
    }
}

/// Tokio keeps the waker of a pending read until the socket becomes readable
/// or is polled again, so a read ended without data polls readiness once more
/// with a waker that holds nothing. The budget is ignored so that this poll
/// always reaches the reactor.
fn release_read_waker(stream: &TcpStream) {
    let _ = pin!(coop::unconstrained(poll_fn(|context| {
        stream.poll_read_ready(context)
    })))
    .poll(&mut Context::from_waker(Waker::noop()));
}

pub(crate) struct AcceptedTcpStream {
    stream: TcpStream,
    write_shutdown_complete: bool,
    zero_copy: RefCell<TcpZeroCopy>,
}

impl AcceptedTcpStream {
    pub(crate) fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            write_shutdown_complete: false,
            zero_copy: RefCell::new(TcpZeroCopy::new()),
        }
    }
}

impl AsyncRead for AcceptedTcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(context, buffer)
    }
}

impl AsyncWrite for AcceptedTcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(context, buffer)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.write_shutdown_complete {
            match Pin::new(&mut self.stream).poll_shutdown(context) {
                Poll::Ready(Ok(())) => self.write_shutdown_complete = true,
                result => return result,
            }
        }
        self.zero_copy
            .get_mut()
            .poll_drain(context)
            .map(|()| Ok(()))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write_vectored(context, buffers)
    }
}

impl TokioByteStream for AcceptedTcpStream {
    const WRITE_QUEUE: bool = true;

    fn prepare_reset(&self) {
        #[expect(
            deprecated,
            reason = "Tokio exposes abortive linger-zero reset only through this setter"
        )]
        let _ = self.stream.set_linger(Some(Duration::ZERO));
    }

    fn poll_read_owned(
        &mut self,
        context: &mut Context<'_>,
        maximum: usize,
        allocation_size: usize,
        read: &mut OwnedRead,
    ) -> Poll<io::Result<bool>> {
        self.stream
            .poll_read_owned(context, maximum, allocation_size, read)
    }

    fn cancel_read(&mut self) {
        release_read_waker(&self.stream);
    }

    fn enable_zero_copy(&self, _context: &WorkerContext, threshold: usize) -> io::Result<bool> {
        #[cfg(target_os = "linux")]
        return self
            .zero_copy
            .borrow_mut()
            .enable(_context, self.stream.as_raw_fd(), threshold);

        #[cfg(not(target_os = "linux"))]
        self.zero_copy.borrow_mut().enable(_context, 0, threshold)
    }

    fn set_zero_copy_reenable_threshold(&self, threshold: usize) {
        self.zero_copy
            .borrow_mut()
            .set_reenable_threshold(threshold);
    }

    fn begin_owned_write(&mut self, length: usize) -> bool {
        self.zero_copy.get_mut().begin_write(length)
    }

    fn try_write_borrowed(
        &mut self,
        context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> io::Result<usize> {
        if !self.zero_copy.get_mut().allows_borrowed_write() {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let Poll::Ready(budget) = coop::poll_proceed(context) else {
            return Err(io::ErrorKind::WouldBlock.into());
        };
        let result = self.stream.try_write_vectored(buffers);
        if result.is_ok() {
            budget.made_progress();
        }
        result
    }

    fn poll_write_owned(
        &mut self,
        context: &mut Context<'_>,
        data: &BufQueue,
        zero_copy: bool,
    ) -> Poll<io::Result<usize>> {
        self.zero_copy
            .get_mut()
            .poll_write(&self.stream, context, data, zero_copy)
    }

    fn abandon_zero_copy(&mut self) {
        self.zero_copy.get_mut().force_teardown();
    }

    #[cfg(test)]
    fn zero_copy_owner_for_test(&self) -> Option<usize> {
        Some(self.zero_copy.borrow().completion_owner_for_test())
    }

    #[cfg(test)]
    fn zero_copy_pending_for_test(&self) -> usize {
        self.zero_copy.borrow().pending_count_for_test()
    }
}

struct ObservedStream<S> {
    stream: Option<S>,
    publisher: Option<IoDropPublisher>,
}

impl<S> ObservedStream<S> {
    fn stream(&self) -> &S {
        self.stream.as_ref().expect("live observed stream")
    }

    fn stream_mut(&mut self) -> &mut S {
        self.stream.as_mut().expect("live observed stream")
    }
}

impl<S> Drop for ObservedStream<S> {
    fn drop(&mut self) {
        let unwinding = std::thread::panicking();
        let mut failure = catch_unwind(AssertUnwindSafe(|| drop(self.stream.take()))).err();
        if let Some(publisher) = self.publisher.take() {
            let confirmed = failure.is_none();
            retain_transport_panic(&mut failure, publisher.finish(confirmed));
        }
        finish_transport_panic(failure, unwinding);
    }
}

impl<S: TokioByteStream> AsyncRead for ObservedStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().stream_mut()).poll_read(context, buffer)
    }
}

impl<S: TokioByteStream> AsyncWrite for ObservedStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.get_mut().stream_mut()).poll_write(context, bytes)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.get_mut().stream_mut()).poll_write_vectored(context, bytes)
    }

    fn is_write_vectored(&self) -> bool {
        self.stream().is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().stream_mut()).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().stream_mut()).poll_shutdown(context)
    }
}

impl<S: TokioByteStream> TokioByteStream for ObservedStream<S> {
    const WRITE_QUEUE: bool = S::WRITE_QUEUE;

    fn prepare_reset(&self) {
        self.stream().prepare_reset();
    }

    fn prepare_write_timeout(&mut self, timeout: Duration) {
        self.stream_mut().prepare_write_timeout(timeout);
    }

    fn poll_read_owned(
        &mut self,
        context: &mut Context<'_>,
        maximum: usize,
        allocation_size: usize,
        read: &mut OwnedRead,
    ) -> Poll<io::Result<bool>> {
        self.stream_mut()
            .poll_read_owned(context, maximum, allocation_size, read)
    }

    fn cancel_read(&mut self) {
        self.stream_mut().cancel_read();
    }

    fn enable_zero_copy(&self, context: &WorkerContext, threshold: usize) -> io::Result<bool> {
        self.stream().enable_zero_copy(context, threshold)
    }

    fn set_zero_copy_reenable_threshold(&self, threshold: usize) {
        self.stream().set_zero_copy_reenable_threshold(threshold);
    }

    fn begin_owned_write(&mut self, length: usize) -> bool {
        self.stream_mut().begin_owned_write(length)
    }

    fn poll_write_owned(
        &mut self,
        context: &mut Context<'_>,
        data: &BufQueue,
        zero_copy: bool,
    ) -> Poll<io::Result<usize>> {
        self.stream_mut().poll_write_owned(context, data, zero_copy)
    }

    fn abandon_zero_copy(&mut self) {
        self.stream_mut().abandon_zero_copy();
    }

    fn cancel_owned_write(&mut self) {
        self.stream_mut().cancel_owned_write();
    }

    fn poll_write_flush(&mut self, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.stream_mut().poll_write_flush(context)
    }

    #[cfg(test)]
    fn zero_copy_owner_for_test(&self) -> Option<usize> {
        self.stream().zero_copy_owner_for_test()
    }

    #[cfg(test)]
    fn zero_copy_pending_for_test(&self) -> usize {
        self.stream().zero_copy_pending_for_test()
    }

    fn byte_event_capability(&self) -> ByteEventCapability {
        self.stream().byte_event_capability()
    }

    fn register_byte_events(
        &mut self,
        flags: ByteEventWriteFlags,
        transport_offset: u64,
        byte_events: Option<ByteEventWriteState>,
    ) -> io::Result<()> {
        self.stream_mut()
            .register_byte_events(flags, transport_offset, byte_events)
    }

    fn take_byte_events(&mut self) -> Vec<ByteEventWriteState> {
        self.stream_mut().take_byte_events()
    }
}

/// The I/O's owner-local cells, shared by the transport and its read, write,
/// and close-drain futures. Each cell is borrowed on its own, and some borrows
/// overlap.
struct TokioIoShared<S> {
    stream: RefCell<Option<S>>,
    read_error: Cell<Option<HttpError>>,
    write_active: Cell<bool>,
    write_shutdown: Cell<bool>,
    write_shutdown_result: RefCell<Option<Result<(), HttpError>>>,
    write_offset: Cell<u64>,
    close_after_shutdown: Cell<bool>,
    close_wait_waker: RefCell<Option<Waker>>,
    non_abortive_close: Cell<bool>,
    /// An `OwnedWriteQueue` write's state while it is active (`QueuedWriter`).
    queued_write: RefCell<Option<IoWrite>>,
    /// An `OwnedReader` read's state while it is active (`PolledReader`).
    polled_read: RefCell<Option<IoRead>>,
}

// Drop order follows declaration order here and in `TokioIoShared`: the I/O
// drops before the other shared cells, and `shared` before `close_drain`.
struct TokioByteStreamIo<S> {
    shared: Rc<TokioIoShared<S>>,
    close_drain: Rc<CloseDrainState>,
}

impl<S> TokioByteStreamIo<S> {
    fn new(context: &WorkerContext, stream: S) -> Result<(Self, WorkerCleanupObserver), HttpError> {
        let (close_drain, observer) = reserve_close_drain(context)?;
        Ok((Self::with_close_drain(stream, close_drain), observer))
    }

    fn with_close_drain(stream: S, close_drain: Rc<CloseDrainState>) -> Self {
        Self {
            shared: Rc::new(TokioIoShared {
                stream: RefCell::new(Some(stream)),
                read_error: Cell::new(None),
                write_active: Cell::new(false),
                write_shutdown: Cell::new(false),
                write_shutdown_result: RefCell::new(None),
                write_offset: Cell::new(0),
                close_after_shutdown: Cell::new(false),
                close_wait_waker: RefCell::new(None),
                non_abortive_close: Cell::new(false),
                queued_write: RefCell::new(None),
                polled_read: RefCell::new(None),
            }),
            close_drain,
        }
    }
}

impl<S> Drop for TokioByteStreamIo<S> {
    fn drop(&mut self) {
        let started = self.close_drain.inner.borrow().started;
        if !started {
            cancel_close_drain(&self.close_drain);
        }
    }
}

impl<S> ByteStreamIo for TokioByteStreamIo<S>
where
    S: TokioByteStream,
{
    fn read(
        &self,
        maximum: usize,
        allocation_size: usize,
    ) -> LocalTransportFuture<Result<TransportRead, HttpError>> {
        #[cfg(test)]
        IO_READ_FUTURES.with(|count| count.set(count.get() + 1));
        let shared = Rc::clone(&self.shared);
        Box::pin(async move {
            if let Some(error) = shared.read_error.take() {
                return Err(error);
            }
            let mut read = OwnedRead::new(maximum);
            let ended = poll_fn(|context| {
                poll_stream_read(&shared, context, maximum, allocation_size, &mut read)
            })
            .await;
            let eof = read_eof(&shared, &read, ended)?;
            Ok(TransportRead::from_io(read.into_data()?, eof))
        })
    }

    fn prepare_write_timeout(&self, timeout: Duration) {
        if let Some(stream) = self.shared.stream.borrow_mut().as_mut() {
            stream.prepare_write_timeout(timeout);
        }
    }

    fn write(
        &self,
        data: BufQueue,
        progress: WriteProgress,
        flags: ByteEventWriteFlags,
        byte_events: Option<ByteEventWriteState>,
    ) -> LocalTransportFuture<Result<(), HttpError>> {
        assert!(!self.shared.write_active.replace(true));
        #[cfg(test)]
        IO_WRITE_FUTURES.with(|count| count.set(count.get() + 1));
        let shared = Rc::clone(&self.shared);
        let active = WriteActiveGuard {
            shared: Rc::clone(&shared),
        };
        let mut write = IoWrite::new(data, progress, flags, byte_events);
        Box::pin(async move {
            let _active = active;
            poll_fn(|context| poll_io_write(&shared, context, &mut write)).await
        })
    }

    fn cancel_read(&self, _error: HttpErrorCode) {
        if let Some(stream) = self.shared.stream.borrow_mut().as_mut() {
            stream.cancel_read();
        }
    }

    fn cancel_write(&self, _error: HttpErrorCode) {
        self.shared.write_shutdown.set(false);
        if let Some(stream) = self.shared.stream.borrow_mut().as_mut() {
            stream.cancel_owned_write();
        }
    }

    fn shutdown_write(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.shared.write_shutdown.set(true);
        self.ensure_shutdown_drain(context, false)
    }

    fn close(&self, context: &WorkerContext, _drain_write: bool) -> Result<(), HttpError> {
        self.shared.write_shutdown.set(true);
        self.ensure_shutdown_drain(context, true)
    }

    fn drop_transport(&self) {
        // The worker-local drain owns the stream until the requested shutdown
        // finishes, even after the public transport handle has gone away.
        if self.shared.write_shutdown.get()
            && self.shared.write_shutdown_result.borrow().is_none()
            && self.close_drain.inner.borrow().started
        {
            if self.close_drain.controlled {
                self.shared.close_after_shutdown.set(true);
            }
            let failure = catch_unwind(AssertUnwindSafe(|| {
                cancel_stream_byte_events(&self.shared.stream, dropped_error);
            }))
            .err();
            finish_transport_panic(failure, std::thread::panicking());
            return;
        }
        self.retire_stream(false);
    }

    fn close_with_reset(&self) {
        self.retire_stream(true);
    }

    fn enable_non_abortive_close(&self) {
        self.shared.non_abortive_close.set(true);
    }

    fn queued_writer(&self) -> Option<Rc<dyn QueuedWriter>> {
        S::WRITE_QUEUE.then(|| Rc::clone(&self.shared) as Rc<dyn QueuedWriter>)
    }

    fn polled_reader(&self) -> Option<Rc<dyn PolledReader>> {
        Some(Rc::clone(&self.shared) as Rc<dyn PolledReader>)
    }

    fn enable_zero_copy(
        &self,
        context: &WorkerContext,
        threshold: usize,
    ) -> Result<bool, HttpError> {
        let mut stream = self.shared.stream.borrow_mut();
        let Some(stream) = stream.as_mut() else {
            return Err(dropped_error());
        };
        stream
            .enable_zero_copy(context, threshold)
            .map_err(write_io_error)
    }

    fn set_zero_copy_reenable_threshold(&self, threshold: usize) -> Result<(), HttpError> {
        let mut stream = self.shared.stream.borrow_mut();
        let Some(stream) = stream.as_mut() else {
            return Err(dropped_error());
        };
        stream.set_zero_copy_reenable_threshold(threshold);
        Ok(())
    }

    #[cfg(test)]
    fn zero_copy_owner_for_test(&self) -> Option<usize> {
        self.shared
            .stream
            .borrow()
            .as_ref()
            .and_then(TokioByteStream::zero_copy_owner_for_test)
    }

    #[cfg(test)]
    fn zero_copy_pending_for_test(&self) -> usize {
        self.shared
            .stream
            .borrow()
            .as_ref()
            .map_or(0, TokioByteStream::zero_copy_pending_for_test)
    }
}

impl<S> TokioByteStreamIo<S>
where
    S: TokioByteStream,
{
    fn retire_stream(&self, reset: bool) {
        let mut failure =
            catch_unwind(AssertUnwindSafe(|| cancel_close_drain(&self.close_drain))).err();
        retain_transport_panic(
            &mut failure,
            catch_unwind(AssertUnwindSafe(|| {
                if reset
                    && !self.shared.non_abortive_close.get()
                    && let Some(stream) = self.shared.stream.borrow().as_ref()
                {
                    stream.prepare_reset();
                }
            }))
            .err(),
        );
        retain_transport_panic(
            &mut failure,
            catch_unwind(AssertUnwindSafe(|| {
                cancel_stream_byte_events(&self.shared.stream, || {
                    if reset {
                        write_error(b"transport reset cancelled pending byte event")
                    } else {
                        dropped_error()
                    }
                });
            }))
            .err(),
        );
        retain_transport_panic(
            &mut failure,
            catch_unwind(AssertUnwindSafe(|| {
                if let Some(stream) = self.shared.stream.borrow_mut().as_mut() {
                    stream.abandon_zero_copy();
                }
            }))
            .err(),
        );
        retain_transport_panic(&mut failure, dispose_stream(&self.shared.stream));
        finish_transport_panic(failure, std::thread::panicking());
    }

    fn ensure_shutdown_drain(
        &self,
        _context: &WorkerContext,
        close_after_shutdown: bool,
    ) -> Result<(), HttpError> {
        let shared = &self.shared;
        if close_after_shutdown {
            shared.close_after_shutdown.set(true);
        }
        let completed = shared.write_shutdown_result.borrow().clone();
        if let Some(result) = completed {
            if shared.close_after_shutdown.get() {
                return retire_shutdown_result(
                    &shared.stream,
                    &self.close_drain.graceful_retirement,
                    &shared.write_shutdown_result,
                    result,
                );
            }
            return result;
        }
        if self.close_drain.inner.borrow().started {
            return Ok(());
        }
        let close_shared = Rc::clone(shared);
        let graceful_retirement = Rc::clone(&self.close_drain.graceful_retirement);
        let result = start_close_drain(&self.close_drain, async move {
            let result = poll_fn(|context| {
                if close_shared.write_active.get() {
                    close_shared
                        .close_wait_waker
                        .replace(Some(context.waker().clone()));
                    return Poll::Pending;
                }
                if let Some(result) = close_shared.write_shutdown_result.borrow().clone() {
                    return Poll::Ready(result);
                }
                let mut stream = close_shared.stream.borrow_mut();
                let Some(stream) = stream.as_mut() else {
                    return Poll::Ready(Err(dropped_error()));
                };
                match Pin::new(stream).poll_shutdown(context) {
                    Poll::Ready(result) => {
                        let result = result.map_err(write_io_error);
                        close_shared
                            .write_shutdown_result
                            .replace(Some(result.clone()));
                        Poll::Ready(result)
                    }
                    Poll::Pending => Poll::Pending,
                }
            })
            .await;
            if close_shared.close_after_shutdown.get() {
                return retire_shutdown_result(
                    &close_shared.stream,
                    &graceful_retirement,
                    &close_shared.write_shutdown_result,
                    result,
                );
            }
            result
        });
        if result.is_err() && shared.close_after_shutdown.get() {
            return retire_shutdown_result(
                &shared.stream,
                &self.close_drain.graceful_retirement,
                &shared.write_shutdown_result,
                result,
            );
        }
        result
    }
}

fn dispose_stream<S>(stream: &RefCell<Option<S>>) -> Option<TransportPanic> {
    catch_unwind(AssertUnwindSafe(|| {
        let stream = stream.borrow_mut().take();
        drop(stream);
    }))
    .err()
}

fn retire_shutdown_result<S: TokioByteStream>(
    stream: &RefCell<Option<S>>,
    graceful_retirement: &Cell<bool>,
    cached_result: &RefCell<Option<Result<(), HttpError>>>,
    result: Result<(), HttpError>,
) -> Result<(), HttpError> {
    let mut failure = catch_unwind(AssertUnwindSafe(|| {
        cancel_stream_byte_events(stream, || {
            cancelled_error(b"transport closed before byte event")
        });
    }))
    .err();
    retain_transport_panic(&mut failure, dispose_stream(stream));
    if failure.is_some() {
        retain_transport_panic(
            &mut failure,
            catch_unwind(AssertUnwindSafe(|| {
                let cached = cached_result.borrow_mut().take();
                drop(cached);
            }))
            .err(),
        );
        retain_transport_panic(
            &mut failure,
            catch_unwind(AssertUnwindSafe(|| drop(result))).err(),
        );
        finish_transport_panic(failure, false);
        unreachable!("a retained cleanup panic was resumed");
    }
    graceful_retirement.set(result.is_ok());
    result
}

struct WriteActiveGuard<S>
where
    S: TokioByteStream,
{
    shared: Rc<TokioIoShared<S>>,
}

impl<S> Drop for WriteActiveGuard<S>
where
    S: TokioByteStream,
{
    fn drop(&mut self) {
        end_write(&self.shared);
    }
}

/// Ends a write: a close drain waiting for it resumes, or a shutdown requested
/// during it is attempted once.
fn end_write<S>(shared: &TokioIoShared<S>)
where
    S: TokioByteStream,
{
    shared.write_active.set(false);
    if let Some(waker) = shared.close_wait_waker.borrow_mut().take() {
        waker.wake();
    } else if shared.write_shutdown.get() && shared.write_shutdown_result.borrow().is_none() {
        if let Some(result) = poll_shutdown_once(&shared.stream) {
            shared.write_shutdown_result.replace(Some(result));
        }
    }
}

/// One write's progress through [`poll_io_write`].
struct IoWrite {
    data: BufQueue,
    progress: WriteProgress,
    flags: ByteEventWriteFlags,
    byte_events: Option<ByteEventWriteState>,
    stage: IoWriteStage,
}

enum IoWriteStage {
    /// The first poll makes the whole write's zero-copy decision.
    Begin,
    Write {
        zero_copy: bool,
    },
    Flush,
    /// The write side was shut during the write, which shuts it after its bytes.
    Shutdown,
}

impl IoWrite {
    fn new(
        data: BufQueue,
        progress: WriteProgress,
        flags: ByteEventWriteFlags,
        byte_events: Option<ByteEventWriteState>,
    ) -> Self {
        Self {
            data,
            progress,
            flags,
            byte_events,
            stage: IoWriteStage::Begin,
        }
    }
}

/// Writes all of `write`'s bytes, flushes, registers its byte events, then
/// shuts the write side if a shutdown was requested during the write.
fn poll_io_write<S>(
    shared: &TokioIoShared<S>,
    context: &mut Context<'_>,
    write: &mut IoWrite,
) -> Poll<Result<(), HttpError>>
where
    S: TokioByteStream,
{
    loop {
        match write.stage {
            IoWriteStage::Begin => {
                let mut stream = shared.stream.borrow_mut();
                let Some(stream) = stream.as_mut() else {
                    return Poll::Ready(Err(dropped_error()));
                };
                write.stage = IoWriteStage::Write {
                    zero_copy: stream.begin_owned_write(write.data.chain_length()),
                };
            }
            IoWriteStage::Write { zero_copy } => {
                if write.data.is_empty() {
                    write.stage = IoWriteStage::Flush;
                    continue;
                }
                let written = {
                    let mut stream = shared.stream.borrow_mut();
                    let Some(stream) = stream.as_mut() else {
                        return Poll::Ready(Err(dropped_error()));
                    };
                    match ready!(stream.poll_write_owned(context, &write.data, zero_copy)) {
                        Ok(0) => {
                            return Poll::Ready(Err(write_error(b"TCP write made no progress")));
                        }
                        Ok(written) => written,
                        Err(error) => return Poll::Ready(Err(write_io_error(error))),
                    }
                };
                if let Err(error) = write.progress.record(written) {
                    return Poll::Ready(Err(error));
                }
                shared
                    .write_offset
                    .set(shared.write_offset.get().saturating_add(written as u64));
                if write.data.trim_start(written).is_err() {
                    return Poll::Ready(Err(write_error(
                        b"TCP write progress exceeded queued bytes",
                    )));
                }
            }
            IoWriteStage::Flush => {
                {
                    let mut stream = shared.stream.borrow_mut();
                    let Some(stream) = stream.as_mut() else {
                        return Poll::Ready(Err(dropped_error()));
                    };
                    if let Err(error) = ready!(stream.poll_write_flush(context)) {
                        return Poll::Ready(Err(write_io_error(error)));
                    }
                }
                if write.flags != ByteEventWriteFlags::default() || write.byte_events.is_some() {
                    let mut stream = shared.stream.borrow_mut();
                    let Some(stream) = stream.as_mut() else {
                        return Poll::Ready(Err(dropped_error()));
                    };
                    if let Err(error) = stream.register_byte_events(
                        write.flags,
                        shared.write_offset.get(),
                        write.byte_events.take(),
                    ) {
                        return Poll::Ready(Err(write_io_error(error)));
                    }
                }
                if !shared.write_shutdown.get() {
                    return Poll::Ready(Ok(()));
                }
                let shutdown_result = shared.write_shutdown_result.borrow().clone();
                if let Some(result) = shutdown_result {
                    return Poll::Ready(result);
                }
                write.stage = IoWriteStage::Shutdown;
            }
            IoWriteStage::Shutdown => {
                let result = ready!(poll_shutdown(&shared.stream, context));
                shared.write_shutdown_result.replace(Some(result.clone()));
                return Poll::Ready(result);
            }
        }
    }
}

/// Polls the stream once to gather at most `maximum` bytes into `read`;
/// `Ready` carries whether the stream reached EOF. Inlined into both read
/// paths, which poll once per wake.
#[inline(always)]
fn poll_stream_read<S>(
    shared: &TokioIoShared<S>,
    context: &mut Context<'_>,
    maximum: usize,
    allocation_size: usize,
    read: &mut OwnedRead,
) -> Poll<Result<bool, HttpError>>
where
    S: TokioByteStream,
{
    let mut stream = shared.stream.borrow_mut();
    let Some(stream) = stream.as_mut() else {
        return Poll::Ready(Err(dropped_error()));
    };
    stream
        .poll_read_owned(context, maximum, allocation_size, read)
        .map_err(read_io_error)
}

/// Whether a read that gathered `read` and `ended` reached EOF. An error ends
/// a read that gathered nothing; after gathered bytes it is deferred to the
/// next read (`DIV-016`).
fn read_eof<S>(
    shared: &TokioIoShared<S>,
    read: &OwnedRead,
    ended: Result<bool, HttpError>,
) -> Result<bool, HttpError>
where
    S: TokioByteStream,
{
    match ended {
        Ok(eof) => Ok(eof),
        Err(error) if read.is_empty() => Err(error),
        // A socket reports a failure once, so the next read returns it
        // after these bytes, as EOF is deferred (`deferredReadEOF_`).
        // Folly's `Transport::read` throws instead, so Proxygen never
        // parses these bytes (`DIV-016`).
        Err(error) => {
            shared.read_error.set(Some(error));
            Ok(false)
        }
    }
}

/// One read's progress through [`poll_io_read`].
struct IoRead {
    maximum: usize,
    allocation_size: usize,
    /// The gathered bytes, from the first poll on.
    read: Option<OwnedRead>,
}

impl IoRead {
    fn new(maximum: usize, allocation_size: usize) -> Self {
        Self {
            maximum,
            allocation_size,
            read: None,
        }
    }
}

/// Polls a `PolledReader` read with the steps of `ByteStreamIo::read`'s
/// future: a read error deferred from the previous read, else at most
/// `maximum` gathered bytes (`DIV-016`).
fn poll_io_read<S>(
    shared: &TokioIoShared<S>,
    context: &mut Context<'_>,
    io: &mut IoRead,
) -> Poll<Result<TransportRead, HttpError>>
where
    S: TokioByteStream,
{
    let IoRead {
        maximum,
        allocation_size,
        read,
    } = io;
    let gathered = match read {
        Some(read) => read,
        None => {
            if let Some(error) = shared.read_error.take() {
                return Poll::Ready(Err(error));
            }
            read.insert(OwnedRead::new(*maximum))
        }
    };
    let ended = ready!(poll_stream_read(
        shared,
        context,
        *maximum,
        *allocation_size,
        gathered
    ));
    let read = read.take().expect("a polled read holds its gathered bytes");
    Poll::Ready(
        read_eof(shared, &read, ended)
            .and_then(|eof| Ok(TransportRead::from_io(read.into_data()?, eof))),
    )
}

impl<S> PolledReader for TokioIoShared<S>
where
    S: TokioByteStream,
{
    fn start_read(&self, maximum: usize, allocation_size: usize) {
        let replaced = self
            .polled_read
            .replace(Some(IoRead::new(maximum, allocation_size)));
        assert!(replaced.is_none(), "a polled read ended before the next");
    }

    fn poll_read(&self, context: &mut Context<'_>) -> Poll<Result<TransportRead, HttpError>> {
        let polled = {
            let mut read = self.polled_read.borrow_mut();
            let read = read.as_mut().expect("a polled read is active");
            poll_io_read(self, context, read)
        };
        if polled.is_ready() {
            drop(self.polled_read.take());
        }
        polled
    }

    fn retire_read(&self) {
        drop(self.polled_read.take());
    }
}

impl<S> QueuedWriter for TokioIoShared<S>
where
    S: TokioByteStream,
{
    fn start_write(
        &self,
        data: BufQueue,
        progress: WriteProgress,
        flags: ByteEventWriteFlags,
        byte_events: Option<ByteEventWriteState>,
    ) {
        assert!(!self.write_active.replace(true));
        let replaced =
            self.queued_write
                .replace(Some(IoWrite::new(data, progress, flags, byte_events)));
        assert!(replaced.is_none(), "a queued write ended before the next");
    }

    fn poll_write(&self, context: &mut Context<'_>) -> Poll<Result<(), HttpError>> {
        let polled = {
            let mut write = self.queued_write.borrow_mut();
            let write = write.as_mut().expect("a polled queued write is active");
            poll_io_write(self, context, write)
        };
        if polled.is_ready() {
            self.end_queued_write();
        }
        polled
    }

    fn retire_write(&self) {
        if self.queued_write.borrow().is_some() {
            self.end_queued_write();
        }
    }
}

impl<S> TokioIoShared<S>
where
    S: TokioByteStream,
{
    /// Ends the queued write as an ordinary write's future does: its
    /// `WriteActiveGuard` drops first, then its state.
    fn end_queued_write(&self) {
        let write = self.queued_write.borrow_mut().take();
        end_write(self);
        drop(write);
    }
}

fn poll_shutdown<S>(
    stream: &RefCell<Option<S>>,
    context: &mut Context<'_>,
) -> Poll<Result<(), HttpError>>
where
    S: TokioByteStream,
{
    let mut stream = stream.borrow_mut();
    let Some(stream) = stream.as_mut() else {
        return Poll::Ready(Err(dropped_error()));
    };
    match Pin::new(stream).poll_shutdown(context) {
        Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
        Poll::Ready(Err(error)) => Poll::Ready(Err(write_io_error(error))),
        Poll::Pending => Poll::Pending,
    }
}

fn poll_shutdown_once<S>(stream: &RefCell<Option<S>>) -> Option<Result<(), HttpError>>
where
    S: TokioByteStream,
{
    let mut stream = stream.borrow_mut();
    let Some(stream) = stream.as_mut() else {
        return Some(Err(dropped_error()));
    };
    let mut context = Context::from_waker(Waker::noop());
    match Pin::new(stream).poll_shutdown(&mut context) {
        Poll::Ready(result) => Some(result.map_err(write_io_error)),
        Poll::Pending => None,
    }
}

fn cancel_stream_byte_events<S>(stream: &RefCell<Option<S>>, error: impl FnOnce() -> HttpError)
where
    S: TokioByteStream,
{
    let byte_events = stream
        .borrow_mut()
        .as_mut()
        .map(|stream| stream.take_byte_events())
        .unwrap_or_default();
    if byte_events.is_empty() {
        return;
    }
    #[cfg(test)]
    record_close_path_error();
    let error = error();
    for byte_events in byte_events {
        byte_events.cancel(&error);
    }
}

struct CloseDrainInner {
    future: Option<LocalTransportFuture<Result<(), HttpError>>>,
    cleanup_registration: Option<WorkerCleanupRegistration>,
    task_registration: Option<WorkerCleanupTaskRegistration>,
    waker: Option<Waker>,
    started: bool,
    cancelled: bool,
}

struct CloseDrainState {
    owner_thread: ThreadId,
    inner: RefCell<CloseDrainInner>,
    controlled: bool,
    graceful_retirement: Rc<Cell<bool>>,
}

struct CloseDrainTask {
    state: Rc<CloseDrainState>,
    control: Option<CloseDrainControl>,
}

struct CloseDrainControl {
    reset: OwnedTransportReset,
    shared: Arc<AdoptedIoState>,
    graceful: Cell<bool>,
}

impl Drop for CloseDrainControl {
    fn drop(&mut self) {
        let unwinding = std::thread::panicking();
        let mut failure = catch_unwind(AssertUnwindSafe(|| {
            if !self.graceful.get() || self.shared.is_cancelled() {
                self.reset.reset();
            }
        }))
        .err();
        retain_transport_panic(
            &mut failure,
            catch_unwind(AssertUnwindSafe(|| self.shared.retire_driver())).err(),
        );
        finish_transport_panic(failure, unwinding);
    }
}

impl CloseDrainTask {
    fn retire_disposed(&self, control: &CloseDrainControl) -> Poll<()> {
        if self.state.graceful_retirement.get() {
            retire_close_drain(&self.state);
            control.shared.retire_driver();
            if !control.shared.is_cancelled() {
                control.graceful.set(true);
                return Poll::Ready(());
            }
        }
        control.reset.reset();
        Poll::Ready(())
    }

    fn poll_controlled(&self, context: &mut Context<'_>) -> Poll<()> {
        let control = self
            .control
            .as_ref()
            .expect("controlled drain has its owner");
        let (cancelled, disposed) = control.shared.poll_control(context);
        if cancelled || self.state.inner.borrow().cancelled {
            control.reset.reset();
            return Poll::Ready(());
        }
        if disposed {
            return self.retire_disposed(control);
        }
        let future = {
            let mut inner = self.state.inner.borrow_mut();
            inner.waker = Some(context.waker().clone());
            inner.future.take()
        };
        if let Some(mut future) = future {
            if future.as_mut().poll(context).is_pending() {
                self.state.inner.borrow_mut().future = Some(future);
            }
        }
        let (cancelled, disposed) = control.shared.poll_control(context);
        if cancelled || self.state.inner.borrow().cancelled {
            control.reset.reset();
            Poll::Ready(())
        } else if disposed {
            self.retire_disposed(control)
        } else {
            Poll::Pending
        }
    }
}

impl Future for CloseDrainTask {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        assert_eq!(current_id(), self.state.owner_thread);
        if self.control.is_some() {
            return self.poll_controlled(context);
        }
        let mut future = {
            let mut inner = self.state.inner.borrow_mut();
            let Some(future) = inner.future.take() else {
                return Poll::Ready(());
            };
            inner.waker = Some(context.waker().clone());
            future
        };
        match future.as_mut().poll(context) {
            Poll::Pending => {
                self.state.inner.borrow_mut().future = Some(future);
                Poll::Pending
            }
            Poll::Ready(_) => {
                let registration = {
                    let mut inner = self.state.inner.borrow_mut();
                    inner.waker = None;
                    inner.cleanup_registration.take()
                };
                if let Some(registration) = registration {
                    let _ = registration.disarm();
                }
                Poll::Ready(())
            }
        }
    }
}

fn reserve_close_drain(
    context: &WorkerContext,
) -> Result<(Rc<CloseDrainState>, WorkerCleanupObserver), HttpError> {
    let state = new_close_drain_state(false);
    let registration = register_close_drain(context, &state, None)?;
    let observer = registration.observe();
    state.inner.borrow_mut().task_registration = Some(registration);
    Ok((state, observer))
}

fn new_close_drain_state(controlled: bool) -> Rc<CloseDrainState> {
    Rc::new(CloseDrainState {
        owner_thread: current_id(),
        controlled,
        graceful_retirement: Rc::new(Cell::new(false)),
        inner: RefCell::new(CloseDrainInner {
            future: None,
            cleanup_registration: None,
            task_registration: None,
            waker: None,
            started: false,
            cancelled: false,
        }),
    })
}

fn register_close_drain(
    context: &WorkerContext,
    state: &Rc<CloseDrainState>,
    control: Option<CloseDrainControl>,
) -> Result<WorkerCleanupTaskRegistration, HttpError> {
    let task_registration = context
        .register_scope(CloseDrainTask {
            state: Rc::clone(state),
            control,
        })
        .map_err(close_registration_error)?;
    let cleanup_state = Rc::clone(state);
    let registration = match context.register_cleanup(move || cancel_close_drain(&cleanup_state)) {
        Ok(registration) => registration,
        Err(error) => {
            cancel_close_drain(state);
            let _ = task_registration.disarm();
            return Err(close_registration_error(error));
        }
    };
    state.inner.borrow_mut().cleanup_registration = Some(registration);
    Ok(task_registration)
}

fn start_close_drain<F>(state: &Rc<CloseDrainState>, future: F) -> Result<(), HttpError>
where
    F: Future<Output = Result<(), HttpError>> + 'static,
{
    assert_eq!(current_id(), state.owner_thread);
    if state.controlled {
        let waker = {
            let mut inner = state.inner.borrow_mut();
            if inner.cancelled || inner.started {
                return Err(dropped_error());
            }
            inner.started = true;
            inner.future = Some(Box::pin(future));
            inner.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        return Ok(());
    }
    let registration = {
        let mut inner = state.inner.borrow_mut();
        let Some(registration) = inner.task_registration.take() else {
            return Err(dropped_error());
        };
        inner.started = true;
        inner.future = Some(Box::pin(future));
        registration
    };
    match registration.activate() {
        Ok(completion) => {
            drop(completion);
            Ok(())
        }
        Err(error) => {
            cancel_close_drain(state);
            Err(close_registration_error(error))
        }
    }
}

fn cancel_close_drain(state: &Rc<CloseDrainState>) {
    assert_eq!(current_id(), state.owner_thread);
    state.inner.borrow_mut().cancelled = true;
    retire_close_drain(state);
}

fn retire_close_drain(state: &Rc<CloseDrainState>) {
    assert_eq!(current_id(), state.owner_thread);
    let (future, registration, task_registration, waker) = {
        let mut inner = state.inner.borrow_mut();
        (
            inner.future.take(),
            inner.cleanup_registration.take(),
            inner.task_registration.take(),
            inner.waker.take(),
        )
    };
    let mut failure = catch_unwind(AssertUnwindSafe(|| drop(future))).err();
    retain_transport_panic(
        &mut failure,
        catch_unwind(AssertUnwindSafe(|| {
            if let Some(task_registration) = task_registration {
                let _ = task_registration.disarm();
            }
        }))
        .err(),
    );
    retain_transport_panic(
        &mut failure,
        catch_unwind(AssertUnwindSafe(|| {
            if let Some(registration) = registration {
                let _ = registration.disarm();
            }
        }))
        .err(),
    );
    retain_transport_panic(
        &mut failure,
        catch_unwind(AssertUnwindSafe(|| {
            if let Some(waker) = waker {
                waker.wake();
            }
        }))
        .err(),
    );
    finish_transport_panic(failure, false);
}

fn close_registration_error(error: CleanupRegistrationError) -> HttpError {
    HttpError::with_message(
        HttpErrorCode::INVALID_STATE_TRANSITION,
        error.to_string().as_bytes(),
    )
}

pub(crate) fn owned_from_stream<S>(
    context: &WorkerContext,
    stream: S,
    local_address: SocketAddress,
    peer_address: SocketAddress,
    peer_identity: Option<PeerIdentity>,
    limits: ByteStreamLimits,
) -> Result<OwnedByteStream, HttpError>
where
    S: TokioByteStream,
{
    let byte_event_capability = stream.byte_event_capability();
    let shared = Arc::new(AdoptedIoState::default());
    let stream = ObservedStream {
        stream: Some(stream),
        publisher: Some(IoDropPublisher::new(Arc::clone(&shared))),
    };
    let (io, scope) = TokioByteStreamIo::new(context, stream)?;
    let retirement = TransportRetirement::new(AdoptedIoCompletion::new(shared, scope));
    OwnedByteStream::new(
        io,
        context.handle().clone(),
        context,
        limits,
        local_address,
        peer_address,
        peer_identity,
        byte_event_capability,
    )
    .map(|transport| transport.with_retirement(retirement))
}

pub(crate) struct StreamAttachment {
    pub(crate) local_address: SocketAddress,
    pub(crate) peer_address: SocketAddress,
    pub(crate) peer_identity: Option<PeerIdentity>,
    pub(crate) limits: ByteStreamLimits,
}

pub(crate) fn owned_from_stream_with_completion<S>(
    context: &WorkerContext,
    stream: S,
    attachment: StreamAttachment,
    shared: Arc<AdoptedIoState>,
) -> Result<(OwnedByteStream, WorkerCleanupObserver), HttpError>
where
    S: TokioByteStream,
{
    let state = new_close_drain_state(true);
    let capability = stream.byte_event_capability();
    let transport = OwnedByteStream::new(
        TokioByteStreamIo::with_close_drain(stream, Rc::clone(&state)),
        context.handle().clone(),
        context,
        attachment.limits,
        attachment.local_address,
        attachment.peer_address,
        attachment.peer_identity,
        capability,
    )?;
    let control = CloseDrainControl {
        reset: transport.terminal_reset()?,
        shared: Arc::clone(&shared),
        graceful: Cell::new(false),
    };
    let registration = register_close_drain(context, &state, Some(control))?;
    let observer = registration.observe();
    registration
        .activate()
        .map_err(close_registration_error)?
        .detach();
    let retirement = TransportRetirement::new(AdoptedIoCompletion::new(shared, observer.clone()));
    Ok((transport.with_retirement(retirement), observer))
}

pub(crate) struct ConnectedTcp {
    pub(crate) stream: TcpStream,
    pub(crate) local_address: SocketAddress,
    pub(crate) peer_address: SocketAddress,
}

pub(crate) fn prepare_accepted_stream(
    stream: std::net::TcpStream,
) -> Result<(AcceptedTcpStream, SocketAddress, SocketAddress), HttpError> {
    stream.set_nonblocking(true).map_err(connect_io_error)?;
    let local_address = stream
        .local_addr()
        .map(socket_address)
        .map_err(connect_io_error)?;
    let peer_address = stream
        .peer_addr()
        .map(socket_address)
        .map_err(connect_io_error)?;
    let stream = TcpStream::from_std(stream).map_err(connect_io_error)?;
    Ok((AcceptedTcpStream::new(stream), local_address, peer_address))
}

pub(crate) async fn connect_socket_with_options(
    address: SocketAddress,
    deadline: &ConnectDeadline,
    cancellation: CancellationToken,
    options: TcpConnectOptions,
) -> Result<ConnectedTcp, HttpError> {
    if cancellation.is_cancellation_requested() {
        return Err(cancelled_error(b"TCP connect cancelled before entry"));
    }
    if !address.is_initialized() {
        return Err(connect_error(b"TCP connect address is uninitialized"));
    }
    let host = std::str::from_utf8(address.host())
        .map_err(|_| connect_error(b"TCP connect host is not UTF-8"))?
        .to_owned();
    let port = address.port();
    let connect = async {
        let needs_socket = options.bind_address.is_some()
            || options
                .socket_options
                .iter()
                .any(|option| option.phase != TcpSocketOptionPhase::PostConnect);
        if !needs_socket {
            let stream = TcpStream::connect((host.as_str(), port)).await?;
            let _ = stream.set_nodelay(true);
            return Ok(stream);
        }
        let remote_ip = host.parse::<IpAddr>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "a bound TCP connect requires a numeric destination address",
            )
        })?;
        let socket = if remote_ip.is_ipv6() {
            TcpSocket::new_v6()?
        } else {
            TcpSocket::new_v4()?
        };
        let _ = socket.set_nodelay(true);
        options
            .socket_options
            .iter()
            .filter(|option| option.phase == TcpSocketOptionPhase::PreBind)
            .try_for_each(|option| set_socket_option(&socket, option))
            .map_err(http_error_to_io)?;
        if let Some(bind_address) = options.bind_address.as_ref() {
            let bind_ip = std::str::from_utf8(bind_address.host())
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "bind address is not UTF-8")
                })?
                .parse::<IpAddr>()
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "bind address is not numeric")
                })?;
            socket.bind(SocketAddr::new(bind_ip, bind_address.port()))?;
        }
        options
            .socket_options
            .iter()
            .filter(|option| option.phase == TcpSocketOptionPhase::PreConnect)
            .try_for_each(|option| set_socket_option(&socket, option))
            .map_err(http_error_to_io)?;
        socket.connect(SocketAddr::new(remote_ip, port)).await
    };
    let stream = match await_connect_with_deadline(connect, deadline, cancellation).await {
        Ok(result) => result.map_err(connect_io_error)?,
        Err(AwaitError::Cancelled) => return Err(cancelled_error(b"TCP connect cancelled")),
        Err(AwaitError::TimedOut) => {
            return Err(HttpError::with_message(
                HttpErrorCode::READ_TIMEOUT,
                b"TCP connect timed out",
            ));
        }
    };
    options
        .socket_options
        .iter()
        .filter(|option| option.phase == TcpSocketOptionPhase::PostConnect)
        .try_for_each(|option| set_socket_option(&stream, option))?;
    let local_address = stream
        .local_addr()
        .map(socket_address)
        .map_err(connect_io_error)?;
    let peer_address = stream
        .peer_addr()
        .map(socket_address)
        .map_err(connect_io_error)?;
    Ok(ConnectedTcp {
        stream,
        local_address,
        peer_address,
    })
}

#[cfg(unix)]
fn set_socket_option(socket: &impl AsRawFd, option: &TcpSocketOption) -> Result<(), HttpError> {
    let descriptor = socket.as_raw_fd();
    let (value, length) = match &option.value {
        TcpSocketOptionValue::Integer(value) => (
            std::ptr::from_ref(value).cast(),
            std::mem::size_of_val(value),
        ),
        TcpSocketOptionValue::Bytes(value) => (value.as_ptr().cast(), value.len()),
    };
    let length = libc::socklen_t::try_from(length)
        .map_err(|_| connect_error(b"TCP socket option value is too large"))?;
    // SAFETY: `socket` borrows a live socket descriptor. `value` points into
    // the borrowed option and remains valid for exactly `length` bytes
    // throughout this synchronous syscall.
    let result = unsafe { libc::setsockopt(descriptor, option.level, option.name, value, length) };
    if result == 0 {
        Ok(())
    } else {
        Err(connect_io_error(io::Error::last_os_error()))
    }
}

fn http_error_to_io(error: HttpError) -> io::Error {
    io::Error::other(String::from_utf8_lossy(error.message()).into_owned())
}

#[cfg(not(unix))]
fn set_socket_option<S>(_socket: &S, _option: &TcpSocketOption) -> Result<(), HttpError> {
    Err(connect_error(
        b"TCP socket options are unsupported on this platform",
    ))
}

#[cfg(target_os = "linux")]
fn set_congestion_flavor(stream: &TcpStream, flavor: &str) -> Result<(), HttpError> {
    let flavor = std::ffi::CString::new(flavor)
        .map_err(|_| connect_error(b"TCP congestion flavor contains a NUL byte"))?;
    // SAFETY: `stream` owns a live file descriptor, and the CString pointer is
    // valid for its reported byte length throughout the `setsockopt` call.
    let result = unsafe {
        libc::setsockopt(
            stream.as_raw_fd(),
            libc::IPPROTO_TCP,
            libc::TCP_CONGESTION,
            flavor.as_ptr().cast(),
            flavor.as_bytes().len() as libc::socklen_t,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(connect_io_error(io::Error::last_os_error()))
    }
}

pub(crate) fn set_congestion_flavor_best_effort(stream: &TcpStream, flavor: Option<&str>) {
    if let Some(flavor) = flavor {
        let _ = set_congestion_flavor(stream, flavor);
    }
}

#[cfg(not(target_os = "linux"))]
fn set_congestion_flavor(_stream: &TcpStream, _flavor: &str) -> Result<(), HttpError> {
    Err(connect_error(
        b"TCP congestion selection is unsupported on this platform",
    ))
}

pub(crate) struct ConnectDeadline(Option<Instant>);

impl ConnectDeadline {
    pub(crate) fn new(timeout: Duration) -> Self {
        Self((timeout != Duration::ZERO).then(|| Instant::now() + timeout))
    }

    fn remaining(&self) -> Result<Duration, AwaitError> {
        match self.0 {
            None => Ok(Duration::ZERO),
            Some(deadline) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining == Duration::ZERO {
                    Err(AwaitError::TimedOut)
                } else {
                    Ok(remaining)
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AwaitError {
    Cancelled,
    TimedOut,
}

pub(crate) async fn await_connect_with_deadline<F>(
    future: F,
    deadline: &ConnectDeadline,
    cancellation: CancellationToken,
) -> Result<F::Output, AwaitError>
where
    F: Future,
{
    await_with_deadline(future, deadline, cancellation).await
}

struct SetupState<T> {
    owner_thread: ThreadId,
    closed: bool,
    future: Option<LocalTransportFuture<Result<T, HttpError>>>,
    waker: Option<Waker>,
}

struct SetupFuture<T> {
    state: Rc<RefCell<SetupState<T>>>,
    cleanup_registration: Option<WorkerCleanupRegistration>,
    complete: bool,
}

impl<T> Future for SetupFuture<T>
where
    T: 'static,
{
    type Output = Result<T, HttpError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        let mut future = {
            let mut state = this.state.borrow_mut();
            assert_eq!(current_id(), state.owner_thread);
            if state.closed {
                this.complete = true;
                return Poll::Ready(Err(dropped_error()));
            }
            state.waker = Some(context.waker().clone());
            state
                .future
                .take()
                .expect("an open setup operation retains its future")
        };
        match future.as_mut().poll(context) {
            Poll::Pending => {
                this.state.borrow_mut().future = Some(future);
                Poll::Pending
            }
            Poll::Ready(result) => {
                this.state.borrow_mut().waker = None;
                this.complete = true;
                if let Some(registration) = this.cleanup_registration.take() {
                    let _ = registration.disarm();
                }
                Poll::Ready(result)
            }
        }
    }
}

impl<T> Unpin for SetupFuture<T> {}

impl<T> Drop for SetupFuture<T> {
    fn drop(&mut self) {
        if !self.complete {
            let future = {
                let mut state = self.state.borrow_mut();
                assert_eq!(current_id(), state.owner_thread);
                state.closed = true;
                state.waker = None;
                state.future.take()
            };
            drop(future);
        }
        if let Some(registration) = self.cleanup_registration.take() {
            let _ = registration.disarm();
        }
    }
}

pub(crate) fn setup_future<T, F>(
    context: &WorkerContext,
    future: F,
) -> Result<LocalTransportFuture<Result<T, HttpError>>, HttpError>
where
    T: 'static,
    F: Future<Output = Result<T, HttpError>> + 'static,
{
    let state = Rc::new(RefCell::new(SetupState {
        owner_thread: current_id(),
        closed: false,
        future: Some(Box::pin(future)),
        waker: None,
    }));
    let cleanup_state = Rc::clone(&state);
    let cleanup_registration = context
        .register_cleanup(move || {
            let (future, waker) = {
                let mut state = cleanup_state.borrow_mut();
                assert_eq!(current_id(), state.owner_thread);
                state.closed = true;
                (state.future.take(), state.waker.take())
            };
            drop(future);
            if let Some(waker) = waker {
                waker.wake();
            }
        })
        .map_err(|error| {
            HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                error.to_string().as_bytes(),
            )
        })?;
    Ok(Box::pin(SetupFuture {
        state,
        cleanup_registration: Some(cleanup_registration),
        complete: false,
    }))
}

pub(crate) async fn await_with_deadline<F>(
    future: F,
    deadline: &ConnectDeadline,
    cancellation: CancellationToken,
) -> Result<F::Output, AwaitError>
where
    F: Future,
{
    if cancellation.is_cancellation_requested() {
        return Err(AwaitError::Cancelled);
    }
    let observed_cancellation = cancellation.clone();
    let timeout = deadline.remaining()?;
    let baton = TimedBaton::new(Some(timeout));
    let mut wait = Box::pin(baton.wait(cancellation));
    let mut future = Box::pin(future);
    poll_fn(|context| {
        let wait_poll = wait.as_mut().poll(context);
        if observed_cancellation.is_cancellation_requested() {
            return Poll::Ready(Err(AwaitError::Cancelled));
        }
        match wait_poll {
            Poll::Ready(WaitStatus::Cancelled) => return Poll::Ready(Err(AwaitError::Cancelled)),
            Poll::Ready(WaitStatus::TimedOut) => return Poll::Ready(Err(AwaitError::TimedOut)),
            Poll::Ready(WaitStatus::Signalled | WaitStatus::NotReady) | Poll::Pending => {}
        }
        if baton.status() == WaitStatus::TimedOut {
            return Poll::Ready(Err(AwaitError::TimedOut));
        }
        let future_poll = future.as_mut().poll(context);
        if observed_cancellation.is_cancellation_requested() {
            return Poll::Ready(Err(AwaitError::Cancelled));
        }
        if baton.status() == WaitStatus::TimedOut {
            return Poll::Ready(Err(AwaitError::TimedOut));
        }
        future_poll.map(Ok)
    })
    .await
}

fn validate_context(context: &WorkerContext) -> Result<(), HttpError> {
    if !context.is_owner_thread() {
        return Err(HttpError::with_message(
            HttpErrorCode::INVALID_STATE_TRANSITION,
            b"TCP connect requires its worker owner thread",
        ));
    }
    if context.handle().state() != WorkerState::Open {
        return Err(dropped_error());
    }
    Ok(())
}

pub(crate) fn socket_address(address: std::net::SocketAddr) -> SocketAddress {
    let ip = match address.ip() {
        IpAddr::V6(ip) => ip.to_ipv4_mapped().map_or(IpAddr::V6(ip), IpAddr::V4),
        ip => ip,
    };
    SocketAddress::new(ip.to_string(), address.port())
}

fn connect_io_error(error: io::Error) -> HttpError {
    HttpError::with_message(HttpErrorCode::CONNECT_ERROR, error.to_string().as_bytes())
        .with_cause(Arc::new(error))
}

fn read_io_error(error: io::Error) -> HttpError {
    HttpError::with_message(
        if error.kind() == io::ErrorKind::UnexpectedEof {
            HttpErrorCode::TRANSPORT_EOF
        } else {
            HttpErrorCode::TRANSPORT_READ_ERROR
        },
        error.to_string().as_bytes(),
    )
    .with_cause(Arc::new(error))
}

fn write_io_error(error: io::Error) -> HttpError {
    HttpError::with_message(
        if error
            .get_ref()
            .is_some_and(|cause| cause.is::<CiphertextWriteTimeout>())
        {
            HttpErrorCode::WRITE_TIMEOUT
        } else {
            HttpErrorCode::TRANSPORT_WRITE_ERROR
        },
        error.to_string().as_bytes(),
    )
    .with_cause(Arc::new(error))
}

fn cancelled_error(message: &'static [u8]) -> HttpError {
    HttpError::with_message(HttpErrorCode::CORO_CANCELLED, message)
}

fn connect_error(message: &'static [u8]) -> HttpError {
    HttpError::with_message(HttpErrorCode::CONNECT_ERROR, message)
}

fn read_error(message: &'static [u8]) -> HttpError {
    HttpError::with_message(HttpErrorCode::TRANSPORT_READ_ERROR, message)
}

fn write_error(message: &'static [u8]) -> HttpError {
    HttpError::with_message(HttpErrorCode::TRANSPORT_WRITE_ERROR, message)
}

fn dropped_error() -> HttpError {
    HttpError::with_message(HttpErrorCode::DROPPED, b"TCP transport owner was dropped")
}
