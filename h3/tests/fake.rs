/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! An in-memory transport for the session tests: a connection whose streams record what the
//! session sent and did to them, and a pull sink with a set allowance.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Wake;
use std::task::Waker;
use std::time::Instant;

use bytes::Bytes;
use mvfstr_driver::SinkError;
use mvfstr_proto::AppCode;
use mvfstr_proto::BufQueue;
use mvfstr_proto::CreditDue;
use mvfstr_proto::Dir;
use mvfstr_proto::OpenError;
use mvfstr_proto::SendError;
use mvfstr_proto::SendOrder;
use mvfstr_proto::Side;
use mvfstr_proto::StreamError;
use mvfstr_proto::StreamId;

use crate::transport::PullSink;
use crate::transport::SessionWake;
use crate::transport::Transport;
use crate::transport::TransportStream;

/// One stream of the fake connection.
#[derive(Debug)]
pub(crate) struct FakeStream {
    /// Received bytes not yet consumed.
    pub(crate) readable: BufQueue,
    /// Whether the peer's FIN arrived with every byte before it.
    pub(crate) peer_finished: bool,
    /// Bytes consumed so far: the credit returned.
    pub(crate) consumed: u64,
    /// Bytes the session sent, through `send` or the pull sink.
    pub(crate) sent: Vec<u8>,
    pub(crate) finished: bool,
    pub(crate) reset: Option<u64>,
    pub(crate) stop_sending: Option<u64>,
    pub(crate) order: Option<SendOrder>,
    /// The code of the peer's STOP_SENDING, after which sends fail.
    pub(crate) peer_stopped: Option<u64>,
    /// The bytes `send` takes at most, per call.
    pub(crate) send_window: usize,
    /// The bytes `send_parts` takes before the stream's allowance runs out.
    pub(crate) capacity: usize,
}

impl FakeStream {
    fn new() -> Self {
        Self {
            readable: BufQueue::new(usize::MAX),
            peer_finished: false,
            consumed: 0,
            sent: Vec::new(),
            finished: false,
            reset: None,
            stop_sending: None,
            order: None,
            peer_stopped: None,
            send_window: usize::MAX,
            capacity: usize::MAX,
        }
    }

    fn check_send(&self) -> Result<(), SendError> {
        if let Some(code) = self.peer_stopped {
            return Err(SendError::Stopped {
                code: AppCode::from_u64(code),
            });
        }
        if self.finished || self.reset.is_some() {
            return Err(SendError::Ended);
        }
        Ok(())
    }
}

/// The fake connection.
pub(crate) struct FakeConn {
    pub(crate) now: Instant,
    pub(crate) streams: BTreeMap<StreamId, FakeStream>,
    pub(crate) next_uni: u64,
    /// The server unidirectional streams the peer permits.
    pub(crate) uni_limit: u64,
    /// The opens that the peer's stream limit refused.
    pub(crate) open_refusals: usize,
    /// The send window of the unidirectional streams the session opens.
    pub(crate) uni_send_window: usize,
    pub(crate) closed: Option<u64>,
    pub(crate) shutdown: bool,
    /// Session wakes delivered through the connection handle.
    pub(crate) session_wakes: Arc<AtomicUsize>,
    /// How many of the next handle wakes are dropped, as by a full command queue.
    pub(crate) drop_wakes: Arc<AtomicUsize>,
    pub(crate) stream_wakes: Arc<Mutex<Vec<StreamId>>>,
    /// The wakers `stream_waker` created.
    pub(crate) stream_wakers: Cell<usize>,
}

impl FakeConn {
    pub(crate) fn new() -> Self {
        Self {
            now: Instant::now(),
            streams: BTreeMap::new(),
            next_uni: 0,
            uni_limit: 100,
            open_refusals: 0,
            uni_send_window: usize::MAX,
            closed: None,
            shutdown: false,
            session_wakes: Arc::new(AtomicUsize::new(0)),
            drop_wakes: Arc::new(AtomicUsize::new(0)),
            stream_wakes: Arc::new(Mutex::new(Vec::new())),
            stream_wakers: Cell::new(0),
        }
    }

    /// Returns the stream, which must exist.
    pub(crate) fn get(&mut self, id: StreamId) -> &mut FakeStream {
        self.streams.get_mut(&id).expect("the stream exists")
    }

    /// Opens a peer stream, if new, and appends received bytes to it.
    pub(crate) fn receive(&mut self, id: StreamId, bytes: &[u8]) {
        let stream = self.streams.entry(id).or_insert_with(FakeStream::new);
        if !bytes.is_empty() {
            stream
                .readable
                .push_bytes(Bytes::copy_from_slice(bytes))
                .expect("the queue is unbounded");
        }
    }

    pub(crate) fn session_wakes(&self) -> usize {
        self.session_wakes.load(Ordering::SeqCst)
    }

    /// Returns the streams whose wakers fired since the last call.
    pub(crate) fn take_stream_wakes(&self) -> Vec<StreamId> {
        std::mem::take(&mut *self.stream_wakes.lock().unwrap())
    }

    pub(crate) fn stream_waker_for(&self, id: StreamId) -> Waker {
        Waker::from(Arc::new(StreamWake {
            id,
            wakes: Arc::clone(&self.stream_wakes),
        }))
    }
}

struct StreamWake {
    id: StreamId,
    wakes: Arc<Mutex<Vec<StreamId>>>,
}

impl Wake for StreamWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.lock().unwrap().push(self.id);
    }
}

/// A connection handle that counts the wakes it delivers and drops as many as `drop_wakes` holds.
#[derive(Debug, Default)]
pub(crate) struct FakeHandle {
    pub(crate) wakes: Arc<AtomicUsize>,
    pub(crate) drop_wakes: Arc<AtomicUsize>,
}

impl SessionWake for FakeHandle {
    fn wake(&self) -> bool {
        let dropping = self.drop_wakes.load(Ordering::SeqCst);
        if dropping > 0 {
            self.drop_wakes.store(dropping - 1, Ordering::SeqCst);
            return false;
        }
        self.wakes.fetch_add(1, Ordering::SeqCst);
        true
    }
}

impl Transport for FakeConn {
    type Stream<'a> = &'a mut FakeStream;

    fn now(&self) -> Instant {
        self.now
    }

    fn open_uni(&mut self, order: SendOrder) -> Result<StreamId, OpenError> {
        if self.closed.is_some() || self.shutdown {
            return Err(OpenError::Closed);
        }
        if self.next_uni >= self.uni_limit {
            self.open_refusals += 1;
            return Err(OpenError::StreamLimit);
        }
        let id = StreamId::new(Side::Server, Dir::Uni, self.next_uni).unwrap();
        self.next_uni += 1;
        let mut stream = FakeStream::new();
        stream.order = Some(order);
        stream.send_window = self.uni_send_window;
        self.streams.insert(id, stream);
        Ok(id)
    }

    fn stream(&mut self, id: StreamId) -> Result<&mut FakeStream, StreamError> {
        self.streams.get_mut(&id).ok_or(StreamError::Closed)
    }

    fn stream_waker(&self, id: StreamId) -> Waker {
        self.stream_wakers.set(self.stream_wakers.get() + 1);
        self.stream_waker_for(id)
    }

    fn session_handle(&self) -> Box<dyn SessionWake> {
        Box::new(FakeHandle {
            wakes: Arc::clone(&self.session_wakes),
            drop_wakes: Arc::clone(&self.drop_wakes),
        })
    }

    fn close(&mut self, code: AppCode, _reason: Bytes) {
        if self.closed.is_none() {
            self.closed = Some(code.0.into_inner());
        }
    }

    fn shutdown(&mut self) {
        self.shutdown = true;
    }
}

impl TransportStream for &mut FakeStream {
    fn readable(&self) -> &BufQueue {
        &self.readable
    }

    fn peer_finished(&self) -> bool {
        self.peer_finished
    }

    fn consume(&mut self, n: usize) -> Result<CreditDue, StreamError> {
        if n > self.readable.chain_length() {
            return Err(StreamError::BeyondReadable);
        }
        self.readable.trim_start(n).unwrap();
        self.consumed += n as u64;
        Ok(CreditDue::NotDue)
    }

    fn send(&mut self, data: &mut Bytes) -> Result<usize, SendError> {
        self.check_send()?;
        let take = data.len().min(self.send_window);
        self.sent.extend_from_slice(&data.split_to(take));
        Ok(take)
    }

    fn send_capacity(&self) -> usize {
        if self.check_send().is_err() {
            0
        } else {
            self.capacity
        }
    }

    fn send_parts(&mut self, header: Bytes, shared: Bytes) -> Result<(), SendError> {
        self.check_send()?;
        let len = header.len() + shared.len();
        if len > self.capacity {
            return Err(SendError::OverAllowance);
        }
        self.capacity -= len;
        self.sent.extend_from_slice(&header);
        self.sent.extend_from_slice(&shared);
        Ok(())
    }

    fn finish(&mut self) -> Result<(), SendError> {
        self.check_send()?;
        self.finished = true;
        Ok(())
    }

    fn reset(&mut self, code: AppCode) -> Result<(), SendError> {
        if self.finished || self.reset.is_some() {
            return Err(SendError::Ended);
        }
        self.reset = Some(code.0.into_inner());
        Ok(())
    }

    fn stop_sending(&mut self, code: AppCode) -> Result<(), StreamError> {
        if self.stop_sending.is_some() {
            return Err(StreamError::ReceiveEnded);
        }
        self.stop_sending = Some(code.0.into_inner());
        let left = self.readable.chain_length();
        self.readable.trim_start(left).unwrap();
        Ok(())
    }

    fn set_order(&mut self, order: SendOrder) {
        self.order = Some(order);
    }
}

/// A pull sink with a set allowance.
#[derive(Debug)]
pub(crate) struct FakeSink {
    pub(crate) allowance: usize,
    pub(crate) pushed: Vec<u8>,
    /// The `push` calls that staged their bytes.
    pub(crate) pushes: usize,
    /// The `push_parts` calls, with the length of each shared payload.
    pub(crate) parts: Vec<usize>,
    pub(crate) finished: bool,
    pub(crate) closed: bool,
}

impl FakeSink {
    pub(crate) fn new(allowance: usize) -> Self {
        Self {
            allowance,
            pushed: Vec::new(),
            pushes: 0,
            parts: Vec::new(),
            finished: false,
            closed: false,
        }
    }

    fn check(&self, len: usize) -> Result<(), SinkError> {
        if self.closed || self.finished {
            return Err(SinkError::Closed);
        }
        if len > self.allowance {
            return Err(SinkError::OverAllowance);
        }
        Ok(())
    }
}

impl PullSink for FakeSink {
    fn allowance(&self) -> usize {
        if self.closed || self.finished {
            0
        } else {
            self.allowance
        }
    }

    fn push(&mut self, data: Bytes) -> Result<(), SinkError> {
        self.check(data.len())?;
        self.allowance -= data.len();
        self.pushed.extend_from_slice(&data);
        self.pushes += 1;
        Ok(())
    }

    fn push_parts(&mut self, header: Bytes, shared: Bytes) -> Result<(), SinkError> {
        self.check(header.len() + shared.len())?;
        self.allowance -= header.len() + shared.len();
        self.pushed.extend_from_slice(&header);
        self.pushed.extend_from_slice(&shared);
        self.parts.push(shared.len());
        Ok(())
    }

    fn finish(&mut self) -> Result<(), SinkError> {
        if self.closed || self.finished {
            return Err(SinkError::Closed);
        }
        self.finished = true;
        Ok(())
    }
}

/// A conversion from `u64` to an application code, for the fake's stream operations.
trait FromU64 {
    fn from_u64(code: u64) -> Self;
}

impl FromU64 for AppCode {
    fn from_u64(code: u64) -> Self {
        AppCode(mvfstr_proto::VarInt::from_u64(code).unwrap())
    }
}
