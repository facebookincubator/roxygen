/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

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

use bytes::Bytes;
use bytes::BytesMut;
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
use crate::byte_stream::TryRead;
use crate::byte_stream::WriteProgress;
use crate::byte_stream::WriteThrough;
use crate::byte_stream::finish_transport_panic;
#[cfg(test)]
use crate::byte_stream::record_close_path_error;
use crate::byte_stream::retain_transport_panic;
use crate::established::AdoptedIoCompletion;
use crate::established::AdoptedIoState;
use crate::established::IoDropPublisher;
use crate::tcp_zero_copy::TcpZeroCopy;
use crate::tcp_zero_copy::poll_write_ordinary;
use crate::tls::CiphertextWriteTimeout;

/// TCP transport constructors. Their signatures do not expose Tokio types.
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

    /// Makes `bytes`, a view of an allocation of `backing` bytes that a
    /// reader's [`ReadRoom`] handed out, the read's first buffer.
    fn push_first(&mut self, bytes: Bytes, backing: usize) -> io::Result<()> {
        self.filled
            .push_bytes_with_backing(bytes, backing)
            .map_err(|_| io::Error::other("owned read exceeded its queue bound"))
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
    /// copies the returned bytes into an exact-size first buffer. An offer
    /// above `STAGING_LIMIT` is instead polled into new uninitialized space.
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
            // A continuation stopped by the budget would end the batch early.
            // The read returns after a pending continuation, so that poll must
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
    // The readiness poll below does not spend budget, so a task that is out
    // of budget gets `Pending` here, as it would from Tokio's `poll_read`.
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

/// The minimum tailroom that an `OwnedReader` read's first offer reuses. This
/// matches Proxygen's minimum read size.
const MIN_READ_SIZE: usize = 1_460;

/// The most buffers that a thread's free list of read rooms keeps.
const FREE_READ_ROOM_LIMIT: usize = 16;

std::thread_local! {
    /// Buffers that this thread's read rooms released, most recent last, each
    /// with the capacity of its allocation. Views of a buffer may still be held.
    static FREE_READ_ROOMS: RefCell<Vec<(BytesMut, usize)>> = const { RefCell::new(Vec::new()) };
}

/// Takes from this thread's free list the latest buffer that offers `capacity`
/// bytes, after a whole reclaim if its views are all dropped, or else the
/// latest buffer.
fn take_free_read_room(capacity: usize) -> Option<(BytesMut, usize)> {
    FREE_READ_ROOMS
        .try_with(|rooms| {
            let mut rooms = rooms.borrow_mut();
            let whole = rooms.iter_mut().rposition(|(buffer, _)| {
                buffer.capacity() >= capacity || buffer.try_reclaim(capacity)
            });
            match whole {
                Some(index) => Some(rooms.remove(index)),
                None => rooms.pop(),
            }
        })
        .ok()
        .flatten()
}

/// The buffer, if any, whose tailroom a TCP stream's next `OwnedReader` read
/// fills. A read hands out the bytes it filled as a view of the buffer, and the
/// buffer does not hold unread bytes between reads. A reader read that pends or
/// drains the stream, or dropping the room, releases the buffer to the thread's
/// free list, so the room of an idle connection does not hold an allocation.
#[derive(Default)]
pub(crate) struct ReadRoom {
    buffer: BytesMut,
    /// The capacity of the allocation that `buffer` lies in, or 0 when the room
    /// does not hold one.
    backing: usize,
}

impl Drop for ReadRoom {
    fn drop(&mut self) {
        self.release();
    }
}

impl ReadRoom {
    /// Readies the tailroom for a first offer of at most `capacity` bytes and
    /// returns the offer. A room without a buffer first takes one from its
    /// thread's free list. When the tailroom is short of `capacity`, the whole
    /// buffer is reclaimed if no view of it is left. Otherwise the tailroom is
    /// offered while it holds at least `min(MIN_READ_SIZE, capacity)` bytes,
    /// and a new buffer of `capacity` bytes replaces it when it holds less.
    fn prepare(&mut self, capacity: usize) -> usize {
        if self.backing == 0
            && let Some((buffer, backing)) = take_free_read_room(capacity)
        {
            self.buffer = buffer;
            self.backing = backing;
        }
        let tailroom = self.buffer.capacity();
        if tailroom < capacity {
            if self.buffer.try_reclaim(capacity) {
                self.backing = self.buffer.capacity();
            } else if tailroom < MIN_READ_SIZE.min(capacity) {
                self.buffer = BytesMut::with_capacity(capacity);
                self.backing = self.buffer.capacity();
                #[cfg(test)]
                record_tcp_first_reservation(self.buffer.as_ptr());
            }
        }
        self.buffer.capacity().min(capacity)
    }

    /// Polls `stream` once into the first `offered` bytes of the tailroom.
    fn poll_fill<S>(
        &mut self,
        stream: &mut S,
        context: &mut Context<'_>,
        offered: usize,
    ) -> Poll<io::Result<usize>>
    where
        S: AsyncRead + Unpin + ?Sized,
    {
        let spare = &mut self.buffer.spare_capacity_mut()[..offered];
        let start = spare.as_ptr().cast::<u8>();
        let mut buffer = tokio::io::ReadBuf::uninit(spare);
        ready!(Pin::new(stream).poll_read(context, &mut buffer))?;
        let filled = buffer.filled();
        if filled.as_ptr() != start || filled.len() > offered {
            return Poll::Ready(Err(io::Error::other(
                "AsyncRead replaced the read buffer it was given",
            )));
        }
        let length = filled.len();
        // SAFETY: `buffer` holds no bytes between reads, so its first `offered`
        // spare bytes were offered. `filled` starts there and is no longer than
        // that, so it is the original `ReadBuf`, whose filled prefix is
        // initialized: safe code cannot rebuild a `ReadBuf` over this borrowed
        // memory, and one over other memory fails the start check.
        unsafe { self.buffer.set_len(length) };
        Poll::Ready(Ok(length))
    }

    /// Hands out the filled bytes as a view of the buffer, with the capacity
    /// of the allocation that the view keeps alive.
    fn take_filled(&mut self) -> (Bytes, usize) {
        (self.buffer.split().freeze(), self.backing)
    }

    /// Moves the buffer to this thread's free list, or drops it when the list
    /// is full.
    fn release(&mut self) {
        if self.backing == 0 {
            return;
        }
        let released = (std::mem::take(&mut self.buffer), self.backing);
        self.backing = 0;
        // The list can be gone during thread teardown, and the buffer is then dropped.
        let _ = FREE_READ_ROOMS.try_with(|rooms| {
            let mut rooms = rooms.borrow_mut();
            if rooms.len() < FREE_READ_ROOM_LIMIT {
                rooms.push(released);
            }
        });
    }
}

/// Buffers on this thread's free list of read rooms.
#[cfg(test)]
pub(crate) fn free_read_rooms_for_test() -> usize {
    FREE_READ_ROOMS.with_borrow(Vec::len)
}

/// Like `poll_tcp_first`, but fills the reader's `room`, which is prepared
/// only once Tokio reports the socket readable. `Ready` carries the bytes read
/// and the bytes offered.
fn poll_tcp_first_into_room(
    room: &mut ReadRoom,
    read: &mut OwnedRead,
    stream: &mut TcpStream,
    context: &mut Context<'_>,
    capacity: usize,
) -> Poll<io::Result<(usize, usize)>> {
    if !coop::has_budget_remaining() {
        drop(ready!(coop::poll_proceed(context)));
    }
    ready!(
        pin!(coop::unconstrained(poll_fn(|context| {
            stream.poll_read_ready(context)
        })))
        .poll(context)
    )?;
    let offered = room.prepare(capacity);
    let length = ready!(room.poll_fill(stream, context, offered))?;
    if length != 0 {
        let (bytes, backing) = room.take_filled();
        read.push_first(bytes, backing)?;
    }
    Poll::Ready(Ok((length, offered)))
}

/// The read loop of `poll_gather` for an `OwnedReader` read over TCP, with a
/// first poll that fills the reader's `room`. A first poll that fills what it
/// offered, which can be less than `allocation_size`, continues the read.
fn poll_gather_into_room(
    stream: &mut TcpStream,
    context: &mut Context<'_>,
    maximum: usize,
    allocation_size: usize,
    read: &mut OwnedRead,
    room: &mut ReadRoom,
) -> Poll<io::Result<bool>> {
    debug_assert!(read.is_empty(), "a pending reader read gathered nothing");
    let (length, offered) = ready!(poll_tcp_first_into_room(
        room,
        read,
        stream,
        context,
        allocation_size.min(maximum),
    ))?;
    match length {
        0 => return Poll::Ready(Ok(true)),
        length if length < offered || length == maximum => return Poll::Ready(Ok(false)),
        _ => {}
    }
    poll_gather(
        stream,
        context,
        maximum,
        allocation_size,
        read,
        |_, _, _, _| unreachable!("the room's first poll gathered bytes"),
    )
}

#[cfg(test)]
std::thread_local! {
    static IO_WRITE_FUTURES: Cell<usize> = const { Cell::new(0) };
    static IO_READ_FUTURES: Cell<usize> = const { Cell::new(0) };
    static HELD_QUEUED_WRITES: Cell<usize> = const { Cell::new(0) };
}

/// `OwnedWriteQueue` writes whose state a backend held on this thread.
#[cfg(test)]
pub(crate) fn held_queued_writes_for_test() -> usize {
    HELD_QUEUED_WRITES.with(Cell::get)
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
    /// stream. Only plaintext TCP does.
    const WRITE_QUEUE: bool = false;

    /// Whether `poll_read_drained` can report a drained stream. Only plaintext
    /// TCP can.
    const READ_DRAINS: bool = false;

    fn prepare_reset(&self);

    fn prepare_write_timeout(&mut self, _timeout: Duration) {}

    /// Gathers at most `maximum` bytes for one read into `read`, returning
    /// whether the stream reached EOF after them. An error may also follow
    /// bytes already in `read`.
    ///
    /// The default polls into uninitialized capacity, never a zero-filled
    /// buffer. It offers `allocation_size` bytes first and, while a poll fills
    /// what it was offered, offers the gathered length again, capped at the
    /// rest of `maximum`, so that the gathered length doubles with each filled
    /// poll. Once data is available a read therefore polls at most
    /// `1 + ceil(log2(ceil(maximum / allocation_size)))` times. It returns at a
    /// short poll, EOF, an error or `maximum`, and never waits once it holds
    /// bytes. Only the first poll spends Tokio's cooperative budget or
    /// registers `context`'s waker.
    ///
    /// A first offer of at most `STAGING_LIMIT` bytes is polled into a stack
    /// buffer, and the bytes it returns are copied into a buffer of exactly
    /// their length, so a pending read does not hold a buffer. A larger first
    /// offer is reserved before the poll and held while the read waits.
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

    /// `poll_read_owned` for an `OwnedReader` read, which may first fill the
    /// reader's `room`. The default leaves `room` unused.
    fn poll_read_reusing(
        &mut self,
        context: &mut Context<'_>,
        maximum: usize,
        allocation_size: usize,
        read: &mut OwnedRead,
        _room: &mut ReadRoom,
    ) -> Poll<io::Result<bool>> {
        self.poll_read_owned(context, maximum, allocation_size, read)
    }

    fn cancel_read(&mut self) {}

    /// Called after an `OwnedReader` read that gathered bytes without an error
    /// or EOF. Returns whether the stream is drained, with its read readiness
    /// cleared, in which case this poll has registered `context`'s waker for
    /// the next read. The default returns `false`.
    fn poll_read_drained(&self, _context: &mut Context<'_>) -> bool {
        false
    }

    /// Backs `OwnedReader::peer_closed`. The default returns `false`.
    fn peer_closed(&self) -> bool {
        false
    }

    fn enable_zero_copy(
        &mut self,
        _context: &WorkerContext,
        _threshold: usize,
    ) -> io::Result<bool> {
        Ok(false)
    }

    fn set_zero_copy_reenable_threshold(&mut self, _threshold: usize) {}

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
    const READ_DRAINS: bool = true;

    fn prepare_reset(&self) {
        #[expect(
            deprecated,
            reason = "Tokio exposes abortive linger-zero reset only through this setter"
        )]
        let _ = self.set_linger(Some(Duration::ZERO));
    }

    /// Reads as the default does, except that the first poll is Tokio's own
    /// `poll_read` into a buffer reserved only once the socket is readable,
    /// so a pending read does not hold a buffer and the bytes are not copied.
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

    /// Reads as `poll_read_owned` does, except that the first poll fills the
    /// reader's `room`.
    fn poll_read_reusing(
        &mut self,
        context: &mut Context<'_>,
        maximum: usize,
        allocation_size: usize,
        read: &mut OwnedRead,
        room: &mut ReadRoom,
    ) -> Poll<io::Result<bool>> {
        poll_gather_into_room(self, context, maximum, allocation_size, read, room)
    }

    fn cancel_read(&mut self) {
        release_read_waker(self);
    }

    /// Tokio clears a socket's read readiness at a read shorter than its
    /// buffer, or one that would block. Its readiness poll then registers
    /// `context`'s waker. The budget is ignored so that the poll reports the
    /// reactor's readiness.
    fn poll_read_drained(&self, context: &mut Context<'_>) -> bool {
        poll_tcp_drained(self, context)
    }

    fn peer_closed(&self) -> bool {
        tcp_peer_closed(self)
    }
}

fn poll_tcp_drained(stream: &TcpStream, context: &mut Context<'_>) -> bool {
    pin!(coop::unconstrained(poll_fn(|context| {
        stream.poll_read_ready(context)
    })))
    .poll(context)
    .is_pending()
}

/// Tokio keeps the waker of a pending read until the socket becomes readable
/// or is polled again, so a read ended without data polls readiness once more
/// with a no-op waker. The budget is ignored so that this poll always reaches
/// the reactor.
fn release_read_waker(stream: &TcpStream) {
    let _ = pin!(coop::unconstrained(poll_fn(|context| {
        stream.poll_read_ready(context)
    })))
    .poll(&mut Context::from_waker(Waker::noop()));
}

/// `POLLRDHUP` reports a FIN the kernel has processed, even behind unread
/// bytes, and `POLLHUP` a reset. `POLLERR` alone is not a close: zero-copy
/// completions raise it.
#[cfg(target_os = "linux")]
fn tcp_peer_closed(stream: &TcpStream) -> bool {
    let mut socket = libc::pollfd {
        fd: stream.as_raw_fd(),
        events: libc::POLLRDHUP,
        revents: 0,
    };
    // SAFETY: `poll` reads and writes one `pollfd` at the pointer, `socket`,
    // which is initialized and outlives the call.
    let ready = unsafe { libc::poll(&mut socket, 1, 0) };
    ready == 1 && socket.revents & (libc::POLLRDHUP | libc::POLLHUP) != 0
}

#[cfg(not(target_os = "linux"))]
fn tcp_peer_closed(_stream: &TcpStream) -> bool {
    false
}

pub(crate) struct AcceptedTcpStream {
    stream: TcpStream,
    write_shutdown_complete: bool,
    zero_copy: Option<Box<TcpZeroCopy>>,
}

impl AcceptedTcpStream {
    pub(crate) fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            write_shutdown_complete: false,
            zero_copy: None,
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
        let Some(zero_copy) = self.zero_copy.as_deref_mut() else {
            return Poll::Ready(Ok(()));
        };
        zero_copy.poll_drain(context).map(|()| Ok(()))
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
    const READ_DRAINS: bool = true;

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

    fn poll_read_reusing(
        &mut self,
        context: &mut Context<'_>,
        maximum: usize,
        allocation_size: usize,
        read: &mut OwnedRead,
        room: &mut ReadRoom,
    ) -> Poll<io::Result<bool>> {
        self.stream
            .poll_read_reusing(context, maximum, allocation_size, read, room)
    }

    fn cancel_read(&mut self) {
        release_read_waker(&self.stream);
    }

    fn poll_read_drained(&self, context: &mut Context<'_>) -> bool {
        poll_tcp_drained(&self.stream, context)
    }

    fn peer_closed(&self) -> bool {
        tcp_peer_closed(&self.stream)
    }

    fn enable_zero_copy(&mut self, context: &WorkerContext, threshold: usize) -> io::Result<bool> {
        #[cfg(target_os = "linux")]
        let descriptor = self.stream.as_raw_fd();
        #[cfg(not(target_os = "linux"))]
        let descriptor = 0;

        if let Some(zero_copy) = self.zero_copy.as_deref_mut() {
            return zero_copy.enable(context, descriptor, threshold);
        }
        if threshold == 0 {
            return Ok(false);
        }
        let mut zero_copy = Box::new(TcpZeroCopy::new());
        let enabled = zero_copy.enable(context, descriptor, threshold)?;
        if enabled {
            self.zero_copy = Some(zero_copy);
        }
        Ok(enabled)
    }

    fn set_zero_copy_reenable_threshold(&mut self, threshold: usize) {
        match self.zero_copy.as_deref_mut() {
            Some(zero_copy) => zero_copy.set_reenable_threshold(threshold),
            None if threshold != 0 => self
                .zero_copy
                .insert(Box::new(TcpZeroCopy::new()))
                .set_reenable_threshold(threshold),
            None => {}
        }
    }

    fn begin_owned_write(&mut self, length: usize) -> bool {
        self.zero_copy
            .as_deref_mut()
            .is_some_and(|zero_copy| zero_copy.begin_write(length))
    }

    fn try_write_borrowed(
        &mut self,
        context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> io::Result<usize> {
        if !self
            .zero_copy
            .as_deref()
            .is_none_or(TcpZeroCopy::allows_borrowed_write)
        {
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
        match self.zero_copy.as_deref_mut() {
            Some(state) => state.poll_write(&self.stream, context, data, zero_copy),
            None => poll_write_ordinary(&self.stream, context, data),
        }
    }

    fn abandon_zero_copy(&mut self) {
        if let Some(zero_copy) = self.zero_copy.as_deref_mut() {
            zero_copy.force_teardown();
        }
    }

    #[cfg(test)]
    fn zero_copy_owner_for_test(&self) -> Option<usize> {
        self.zero_copy
            .as_deref()
            .map(TcpZeroCopy::completion_owner_for_test)
    }

    #[cfg(test)]
    fn zero_copy_pending_for_test(&self) -> usize {
        self.zero_copy
            .as_deref()
            .map_or(0, TcpZeroCopy::pending_count_for_test)
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
    const READ_DRAINS: bool = S::READ_DRAINS;

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

    fn poll_read_reusing(
        &mut self,
        context: &mut Context<'_>,
        maximum: usize,
        allocation_size: usize,
        read: &mut OwnedRead,
        room: &mut ReadRoom,
    ) -> Poll<io::Result<bool>> {
        self.stream_mut()
            .poll_read_reusing(context, maximum, allocation_size, read, room)
    }

    fn cancel_read(&mut self) {
        self.stream_mut().cancel_read();
    }

    fn poll_read_drained(&self, context: &mut Context<'_>) -> bool {
        self.stream().poll_read_drained(context)
    }

    fn peer_closed(&self) -> bool {
        self.stream().peer_closed()
    }

    fn enable_zero_copy(&mut self, context: &WorkerContext, threshold: usize) -> io::Result<bool> {
        self.stream_mut().enable_zero_copy(context, threshold)
    }

    fn set_zero_copy_reenable_threshold(&mut self, threshold: usize) {
        self.stream_mut()
            .set_zero_copy_reenable_threshold(threshold);
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
    /// An `OwnedReader` read's state while it is active, and the room that
    /// those reads reuse (`PolledReader`).
    polled_read: RefCell<PolledRead>,
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
                polled_read: RefCell::new(PolledRead::default()),
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

/// Ends a write. A close drain waiting for it resumes, or a shutdown requested
/// during the write is attempted once.
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
    /// Shutdown was requested during the write, so the write side shuts after
    /// its bytes.
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

/// Polls the stream once to gather at most `maximum` bytes into `read`.
/// `Ready` carries whether the stream reached EOF. Inlined into
/// `TokioByteStreamIo::read`'s future, which polls once per wake.
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
/// a read that has not gathered any bytes. After gathered bytes, the error is
/// deferred to the next read.
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
        // A socket reports a failure only once, so the next read returns it
        // after these bytes are delivered, as EOF is deferred.
        Err(error) => {
            shared.read_error.set(Some(error));
            Ok(false)
        }
    }
}

/// A [`PolledReader`]'s state: the active read, and the room that its reads
/// reuse.
#[derive(Default)]
struct PolledRead {
    read: Option<IoRead>,
    room: ReadRoom,
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

/// Polls a `PolledReader` read with the same steps as the future of
/// `ByteStreamIo::read`. It returns a read error deferred from the previous
/// read, or else at most `maximum` gathered bytes, the first of them in `room`
/// when the stream reuses it. With `arm`, a read that gathered bytes without
/// an error or EOF also polls whether the stream is drained
/// (`TokioByteStream::poll_read_drained`). A read that pends or drains the
/// stream releases `room`'s buffer.
fn poll_io_read<S>(
    shared: &TokioIoShared<S>,
    context: &mut Context<'_>,
    io: &mut IoRead,
    room: &mut ReadRoom,
    arm: bool,
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
    let (ended, drained) = {
        let mut stream = shared.stream.borrow_mut();
        match stream.as_mut() {
            Some(stream) => {
                let Poll::Ready(ended) =
                    stream.poll_read_reusing(context, *maximum, *allocation_size, gathered, room)
                else {
                    room.release();
                    return Poll::Pending;
                };
                let ended = ended.map_err(read_io_error);
                let drained =
                    arm && matches!(ended, Ok(false)) && stream.poll_read_drained(context);
                if drained {
                    room.release();
                }
                (ended, drained)
            }
            None => (Err(dropped_error()), false),
        }
    };
    let read = read.take().expect("a polled read holds its gathered bytes");
    Poll::Ready(
        read_eof(shared, &read, ended).and_then(|eof| {
            Ok(TransportRead::from_io(read.into_data()?, eof).with_drained(drained))
        }),
    )
}

impl<S> PolledReader for TokioIoShared<S>
where
    S: TokioByteStream,
{
    fn start_read(&self, maximum: usize, allocation_size: usize) {
        let replaced = self
            .polled_read
            .borrow_mut()
            .read
            .replace(IoRead::new(maximum, allocation_size));
        assert!(replaced.is_none(), "a polled read ended before the next");
    }

    fn poll_read(
        &self,
        context: &mut Context<'_>,
        arm: bool,
    ) -> Poll<Result<TransportRead, HttpError>> {
        let polled = {
            let mut polled = self.polled_read.borrow_mut();
            let PolledRead { read, room } = &mut *polled;
            let read = read.as_mut().expect("a polled read is active");
            poll_io_read(self, context, read, room, arm)
        };
        if polled.is_ready() {
            self.retire_read();
        }
        polled
    }

    fn try_read(
        &self,
        context: &mut Context<'_>,
        maximum: usize,
        allocation_size: usize,
    ) -> TryRead {
        let mut io = IoRead::new(maximum, allocation_size);
        let attempt = catch_unwind(AssertUnwindSafe(|| {
            let mut polled = self.polled_read.borrow_mut();
            debug_assert!(polled.read.is_none(), "a polled read ended before the next");
            poll_io_read(self, context, &mut io, &mut polled.room, true)
        }));
        match attempt {
            Ok(Poll::Ready(result)) => TryRead::Ready(result),
            Ok(Poll::Pending) => {
                self.polled_read.borrow_mut().read = Some(io);
                TryRead::Pending
            }
            Err(panic) => {
                self.polled_read.borrow_mut().read = Some(io);
                TryRead::Panicked(panic)
            }
        }
    }

    fn reports_drained(&self) -> bool {
        S::READ_DRAINS
    }

    fn peer_closed(&self) -> bool {
        self.stream.borrow().as_ref().is_some_and(S::peer_closed)
    }

    fn cancel_read(&self, cancel: &mut dyn FnMut()) {
        let read = self.polled_read.borrow_mut().read.take();
        cancel();
        drop(read);
    }

    fn retire_read(&self) {
        let read = self.polled_read.borrow_mut().read.take();
        drop(read);
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
        self.hold_queued_write(IoWrite::new(data, progress, flags, byte_events));
    }

    /// Makes one `poll_io_write` attempt at a new active write. The write is
    /// held as the queued write only if the attempt leaves it unfinished or
    /// panics before it ends. A finished write ends within the attempt, as in
    /// `poll_write`.
    fn write_through(&self, data: BufQueue, progress: &WriteProgress) -> WriteThrough {
        if let Some(stream) = self.stream.borrow_mut().as_mut() {
            stream.prepare_write_timeout(Duration::ZERO);
        }
        assert!(!self.write_active.replace(true));
        let mut write = Some(IoWrite::new(
            data,
            progress.clone(),
            ByteEventWriteFlags::default(),
            None,
        ));
        let attempt = catch_unwind(AssertUnwindSafe(|| {
            let polled = poll_io_write(
                self,
                &mut Context::from_waker(Waker::noop()),
                write.as_mut().expect("the attempt's write has not ended"),
            );
            if polled.is_ready() {
                let ended = write.take();
                end_write(self);
                drop(ended);
            }
            polled
        }));
        match attempt {
            Ok(Poll::Ready(result)) => WriteThrough::Finished(result),
            Ok(Poll::Pending) => {
                self.hold_queued_write(write.expect("a pending attempt's write has not ended"));
                WriteThrough::Held
            }
            Err(panic) => {
                if let Some(write) = write {
                    self.hold_queued_write(write);
                }
                WriteThrough::Panicked(panic)
            }
        }
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
    /// Holds `write`, the active write, as the queued write that `poll_write`
    /// continues.
    fn hold_queued_write(&self, write: IoWrite) {
        #[cfg(test)]
        HELD_QUEUED_WRITES.with(|count| count.set(count.get() + 1));
        let replaced = self.queued_write.replace(Some(write));
        assert!(replaced.is_none(), "a queued write ended before the next");
    }

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
