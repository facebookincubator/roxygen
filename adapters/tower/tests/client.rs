/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::io::IoSlice;
use std::io::Write;
use std::marker::PhantomPinned;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use futures::FutureExt;
use futures::task::AtomicWaker;
use http::Request;
use http::Response;
use http_body::Body;
use http_body::Frame;
use http_body::SizeHint;
use http_body_util::BodyExt;
use http_body_util::Full;
use http_body_util::combinators::UnsyncBoxBody;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::http::CodecProtocol;
use roxygen_runtime::driver::WorkerDriver;
use roxygen_runtime::worker::WorkerHandle;
use roxygen_transport::established::EstablishedIo;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tower_service::Service;

use crate::client::BoxError;
use crate::client::Client;
use crate::client::ClientError;
use crate::client::ClientOptions;
use crate::client::ClientOwner;
use crate::client::ClientShutdownError;
use crate::client::connector::ConnectRequest;
use crate::client::connector::ConnectedIo;
use crate::route::Origin;
use crate::route::ProtocolPolicy;
use crate::route::RouteSpec;

pub(crate) type TestFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;
pub(crate) type PeerBody = UnsyncBoxBody<Bytes, BoxError>;
pub(crate) type Handler =
    Arc<dyn Fn(Request<Incoming>) -> TestFuture<Response<PeerBody>> + Send + Sync>;
pub(crate) trait TestIo: AsyncRead + AsyncWrite + Send {}
impl<Io: AsyncRead + AsyncWrite + Send> TestIo for Io {}
pub(crate) type Io = Pin<Box<dyn TestIo>>;

const H2_TRACE_RECORDS: usize = 256;
const H2_TRACE_CONNECTIONS: usize = 2;
const H2_PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WireEndpoint {
    Native,
    Peer,
}

impl WireEndpoint {
    fn index(self) -> usize {
        match self {
            Self::Native => 0,
            Self::Peer => 1,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct WireFrame {
    length: u32,
    kind: u8,
    flags: u8,
    stream: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WireControl {
    Goaway { cutoff: u32, error: u32 },
    Reset { error: u32 },
    Window { increment: u32 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct WireError {
    kind: io::ErrorKind,
    os_code: Option<i32>,
}

impl WireError {
    fn from_io(error: &io::Error) -> Self {
        Self {
            kind: error.kind(),
            os_code: error.raw_os_error(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum H2DrainMode {
    Healthy,
    Idle,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PeerDrainPhase {
    Initial,
    Ping,
    Final,
    Complete,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DrainFailure {
    WrongConnection,
    InvalidTrace,
    UnexpectedControl,
    EarlyTerminal,
    UnexpectedWrite,
    Abandoned,
}

#[derive(Clone, Copy)]
struct DrainState {
    mode: H2DrainMode,
    bound: bool,
    failure: Option<DrainFailure>,
    peer: PeerDrainPhase,
    native_goaway: bool,
    native_pong: bool,
    initial_end: Option<u64>,
    held_write: bool,
    native_dropped: bool,
    released: bool,
    rejected: bool,
    validated: bool,
}

impl DrainState {
    fn new(mode: H2DrainMode) -> Self {
        Self {
            mode,
            bound: false,
            failure: None,
            peer: PeerDrainPhase::Initial,
            native_goaway: false,
            native_pong: false,
            initial_end: None,
            held_write: false,
            native_dropped: false,
            released: false,
            rejected: false,
            validated: false,
        }
    }

    fn fail(&mut self, failure: DrainFailure) {
        self.failure.get_or_insert(failure);
    }

    fn completed(
        &mut self,
        endpoint: WireEndpoint,
        frame: WireFrame,
        control: Option<WireControl>,
        end: u64,
    ) {
        if !matches!(frame.kind, 6 | 7) {
            return;
        }
        if frame.length != 8 || frame.stream != 0 {
            self.fail(DrainFailure::UnexpectedControl);
            return;
        }
        match (endpoint, frame.kind, frame.flags, control) {
            (WireEndpoint::Peer, 7, 0, Some(WireControl::Goaway { cutoff, error: 0 }))
                if self.peer == PeerDrainPhase::Initial && cutoff == 0x7fff_ffff =>
            {
                self.peer = PeerDrainPhase::Ping;
            }
            (WireEndpoint::Peer, 6, 0, None) if self.peer == PeerDrainPhase::Ping => {
                self.peer = PeerDrainPhase::Final;
                self.initial_end = Some(end);
            }
            (
                WireEndpoint::Peer,
                7,
                0,
                Some(WireControl::Goaway {
                    cutoff: 3,
                    error: 0,
                }),
            ) if self.mode == H2DrainMode::Healthy && self.peer == PeerDrainPhase::Final => {
                self.peer = PeerDrainPhase::Complete;
            }
            (
                WireEndpoint::Native,
                7,
                0,
                Some(WireControl::Goaway {
                    cutoff: 0,
                    error: 0,
                }),
            ) if !self.native_goaway && !self.native_pong => {
                self.native_goaway = true;
            }
            (WireEndpoint::Native, 6, 1, None) if self.native_goaway && !self.native_pong => {
                self.native_pong = true;
            }
            _ => self.fail(DrainFailure::UnexpectedControl),
        }
    }

    fn healthy(&self) -> bool {
        self.mode == H2DrainMode::Healthy
            && self.bound
            && self.failure.is_none()
            && self.peer == PeerDrainPhase::Complete
            && self.native_goaway
            && self.native_pong
    }

    fn idle_boundary(&self) -> bool {
        self.mode == H2DrainMode::Idle
            && self.bound
            && self.failure.is_none()
            && self.peer == PeerDrainPhase::Final
            && self.native_goaway
            && self.native_pong
            && self.held_write
            && self.native_dropped
    }
}

#[derive(Default)]
struct DrainWakers {
    parent: AtomicWaker,
    writer: AtomicWaker,
}

#[derive(Clone)]
pub(crate) struct H2DrainReceipt(H2WireTrace);

impl H2DrainReceipt {
    fn poll_ready(
        &self,
        context: &mut Context<'_>,
        ready: fn(&DrainState) -> bool,
    ) -> Poll<Result<(), DrainFailure>> {
        self.0.1.as_ref().unwrap().parent.register(context.waker());
        let snapshot = self.0.snapshot();
        let state = snapshot.drain.unwrap();
        if let Some(failure) = state.failure {
            drop(self.0.1.as_ref().unwrap().parent.take());
            Poll::Ready(Err(failure))
        } else if ready(&state) && snapshot.ends.iter().all(|end| end.parser.complete()) {
            drop(self.0.1.as_ref().unwrap().parent.take());
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    async fn wait(&self, ready: fn(&DrainState) -> bool) {
        tokio::time::timeout(
            Duration::from_secs(3),
            std::future::poll_fn(|context| self.poll_ready(context, ready)),
        )
        .await
        .expect("bounded connection-specific H2 drain receipt")
        .expect("valid complete H2 drain receipt");
    }

    pub async fn healthy(&self) {
        self.wait(DrainState::healthy).await;
    }

    async fn idle_boundary(&self) {
        self.wait(DrainState::idle_boundary).await;
    }

    fn release_idle(&self) {
        let ready = {
            let mut state = self.0.0.lock().unwrap();
            let drain = state.drain.as_mut().unwrap();
            let ready = drain.idle_boundary() && !drain.released;
            if ready {
                drain.released = true;
            }
            ready
        };
        assert!(
            ready,
            "held nonempty write and post-inner-Drop precede release"
        );
        self.0.1.as_ref().unwrap().writer.wake();
    }

    pub fn release_on_drop(&self) -> H2WriteRelease {
        H2WriteRelease(self.clone())
    }
}

pub(crate) struct H2WriteRelease(H2DrainReceipt);

impl Drop for H2WriteRelease {
    fn drop(&mut self) {
        {
            let mut state = self.0.0.0.lock().unwrap_or_else(|error| error.into_inner());
            let drain = state.drain.as_mut().unwrap();
            if !drain.released {
                drain.fail(DrainFailure::Abandoned);
                drain.released = true;
            }
        }
        let wakers = self.0.0.1.as_ref().unwrap();
        wakers.writer.wake();
        wakers.parent.wake();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WirePeerOutcome {
    Success,
    Error,
    Cancelled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WireMarker {
    GracefulRequested,
    GracefulInvoked,
    PeerOutcome(WirePeerOutcome),
    ClientShutdownRequested,
    ClientRunsReaped { successful: bool },
    IoRetirementBranchObserved { reached: bool },
    WorkerJoined { successful: bool },
}

impl WireMarker {
    fn index(self) -> usize {
        match self {
            Self::GracefulRequested => 0,
            Self::GracefulInvoked => 1,
            Self::PeerOutcome(_) => 2,
            Self::ClientShutdownRequested => 3,
            Self::ClientRunsReaped { .. } => 4,
            Self::IoRetirementBranchObserved { .. } => 5,
            Self::WorkerJoined { .. } => 6,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WireEvent {
    Accepted { bytes: usize },
    Preface,
    Frame(WireFrame),
    Control(WireControl),
    WriteError(WireError),
    ShutdownCalled,
    ShutdownReady(Option<WireError>),
    DropStarted,
    Dropped,
    Marker(WireMarker),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct WireRecord {
    sequence: u64,
    endpoint: WireEndpoint,
    offset: u64,
    event: WireEvent,
}

#[derive(Clone, Copy)]
struct WireLog {
    records: [Option<WireRecord>; H2_TRACE_RECORDS],
    used: usize,
    sequence: u64,
    omitted: u64,
    sequence_exhausted: bool,
}

impl WireLog {
    fn new() -> Self {
        Self {
            records: [None; H2_TRACE_RECORDS],
            used: 0,
            sequence: 0,
            omitted: 0,
            sequence_exhausted: false,
        }
    }

    fn push(&mut self, endpoint: WireEndpoint, offset: u64, event: WireEvent) -> WireRecord {
        self.sequence_exhausted |= self.sequence == u64::MAX;
        self.sequence = self.sequence.saturating_add(1);
        let record = WireRecord {
            sequence: self.sequence,
            endpoint,
            offset,
            event,
        };
        if let Some(slot) = self.records.get_mut(self.used) {
            *slot = Some(record);
            self.used += 1;
        } else {
            self.omitted = self.omitted.saturating_add(1);
        }
        record
    }
}

#[derive(Clone, Copy)]
struct WireParser {
    offset: u64,
    preface: usize,
    header: [u8; 9],
    header_used: usize,
    frame: Option<WireFrame>,
    frame_offset: u64,
    remaining: u32,
    control: [u8; 8],
    control_used: usize,
    control_needed: usize,
    completed_control: Option<WireControl>,
    valid: bool,
}

impl WireParser {
    fn new(endpoint: WireEndpoint) -> Self {
        Self {
            offset: 0,
            preface: if endpoint == WireEndpoint::Native {
                0
            } else {
                H2_PREFACE.len()
            },
            header: [0; 9],
            header_used: 0,
            frame: None,
            frame_offset: 0,
            remaining: 0,
            control: [0; 8],
            control_used: 0,
            control_needed: 0,
            completed_control: None,
            valid: true,
        }
    }

    fn complete(&self) -> bool {
        self.valid
            && self.preface == H2_PREFACE.len()
            && self.header_used == 0
            && self.remaining == 0
    }

    fn advance(&mut self, bytes: usize) {
        if let Some(offset) = self.offset.checked_add(bytes as u64) {
            self.offset = offset;
        } else {
            self.offset = u64::MAX;
            self.valid = false;
        }
    }

    fn accept(
        &mut self,
        mut bytes: &[u8],
        endpoint: WireEndpoint,
        log: &mut WireLog,
        drain: &mut Option<DrainState>,
    ) {
        while !bytes.is_empty() {
            if !self.valid {
                self.advance(bytes.len());
                return;
            }
            if self.preface < H2_PREFACE.len() {
                self.valid &= bytes[0] == H2_PREFACE[self.preface];
                self.preface += 1;
                self.advance(1);
                bytes = &bytes[1..];
                if self.valid && self.preface == H2_PREFACE.len() {
                    log.push(endpoint, self.offset, WireEvent::Preface);
                }
            } else if self.remaining != 0 {
                let count = bytes.len().min(self.remaining as usize);
                let retained = count.min(self.control_needed - self.control_used);
                self.control[self.control_used..self.control_used + retained]
                    .copy_from_slice(&bytes[..retained]);
                self.control_used += retained;
                if self.control_needed != 0 && self.control_used == self.control_needed {
                    let first = u32::from_be_bytes(self.control[..4].try_into().unwrap());
                    let metadata = match self.frame.unwrap().kind {
                        7 => WireControl::Goaway {
                            cutoff: first & 0x7fff_ffff,
                            error: u32::from_be_bytes(self.control[4..8].try_into().unwrap()),
                        },
                        3 => WireControl::Reset { error: first },
                        _ => WireControl::Window {
                            increment: first & 0x7fff_ffff,
                        },
                    };
                    log.push(endpoint, self.frame_offset, WireEvent::Control(metadata));
                    self.completed_control = Some(metadata);
                    self.control_needed = 0;
                    self.control_used = 0;
                    self.control = [0; 8];
                }
                self.remaining -= count as u32;
                self.advance(count);
                bytes = &bytes[count..];
                if self.remaining == 0 {
                    if let Some(drain) = drain {
                        drain.completed(
                            endpoint,
                            self.frame.unwrap(),
                            self.completed_control,
                            self.offset,
                        );
                    }
                    self.frame = None;
                    self.completed_control = None;
                }
            } else {
                if self.header_used == 0 {
                    self.frame_offset = self.offset;
                }
                let count = bytes.len().min(self.header.len() - self.header_used);
                self.header[self.header_used..self.header_used + count]
                    .copy_from_slice(&bytes[..count]);
                self.header_used += count;
                self.advance(count);
                bytes = &bytes[count..];
                if self.header_used == self.header.len() {
                    let frame = WireFrame {
                        length: u32::from_be_bytes([
                            0,
                            self.header[0],
                            self.header[1],
                            self.header[2],
                        ]),
                        kind: self.header[3],
                        flags: self.header[4],
                        stream: u32::from_be_bytes(self.header[5..9].try_into().unwrap())
                            & 0x7fff_ffff,
                    };
                    log.push(endpoint, self.frame_offset, WireEvent::Frame(frame));
                    self.control_needed = match (frame.kind, frame.length, frame.stream) {
                        (7, 8.., 0) => 8,
                        (3, 4, 1..) | (8, 4, _) => 4,
                        (7 | 3 | 8, _, _) => {
                            self.valid = false;
                            0
                        }
                        _ => 0,
                    };
                    self.remaining = frame.length;
                    self.frame = (frame.length != 0).then_some(frame);
                    self.header_used = 0;
                    self.control_used = 0;
                    self.completed_control = None;
                    if frame.length == 0
                        && let Some(drain) = drain
                    {
                        drain.completed(endpoint, frame, None, self.offset);
                    }
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
struct WireEnd {
    parser: WireParser,
    first_error: Option<WireRecord>,
    shutdown_called: Option<WireRecord>,
    shutdown_ready: Option<WireRecord>,
    drop_started: Option<WireRecord>,
    dropped: Option<WireRecord>,
}

impl WireEnd {
    fn new(endpoint: WireEndpoint) -> Self {
        Self {
            parser: WireParser::new(endpoint),
            first_error: None,
            shutdown_called: None,
            shutdown_ready: None,
            drop_started: None,
            dropped: None,
        }
    }
}

#[derive(Clone, Copy)]
struct WireState {
    log: WireLog,
    ends: [WireEnd; 2],
    markers: [Option<WireRecord>; 7],
    drain: Option<DrainState>,
}

impl WireState {
    fn check_drain(&mut self) {
        if let Some(drain) = &mut self.drain {
            if self.log.omitted != 0
                || self.log.sequence_exhausted
                || self.ends.iter().any(|end| !end.parser.valid)
            {
                drain.fail(DrainFailure::InvalidTrace);
            }
            if drain.mode == H2DrainMode::Idle
                && drain
                    .initial_end
                    .is_some_and(|end| self.ends[WireEndpoint::Peer.index()].parser.offset != end)
            {
                drain.fail(DrainFailure::UnexpectedWrite);
            }
        }
    }

    fn complete(&self) -> bool {
        self.log.omitted == 0
            && !self.log.sequence_exhausted
            && self
                .ends
                .iter()
                .all(|end| end.parser.complete() && end.dropped.is_some())
    }
}

#[derive(Clone)]
struct H2WireTrace(Arc<Mutex<WireState>>, Option<Arc<DrainWakers>>);

impl H2WireTrace {
    fn new() -> Self {
        Self(
            Arc::new(Mutex::new(WireState {
                log: WireLog::new(),
                ends: [
                    WireEnd::new(WireEndpoint::Native),
                    WireEnd::new(WireEndpoint::Peer),
                ],
                markers: [None; 7],
                drain: None,
            })),
            None,
        )
    }

    fn with_drain(mode: H2DrainMode) -> Self {
        let mut trace = Self::new();
        trace.0.lock().unwrap().drain = Some(DrainState::new(mode));
        trace.1 = Some(Arc::new(DrainWakers::default()));
        trace
    }

    fn notify(&self) {
        if let Some(wakers) = &self.1 {
            wakers.parent.wake();
        }
    }

    fn bind(&self) -> bool {
        let first = {
            let mut state = self.0.lock().unwrap();
            let drain = state.drain.as_mut().unwrap();
            if drain.bound {
                drain.fail(DrainFailure::WrongConnection);
                false
            } else {
                drain.bound = true;
                true
            }
        };
        self.notify();
        first
    }

    fn hold_write(&self, endpoint: WireEndpoint, nonempty: bool, context: &Context<'_>) -> bool {
        if endpoint != WireEndpoint::Peer || !nonempty {
            return false;
        }
        let Some(wakers) = &self.1 else {
            return false;
        };
        let armed = {
            let state = self.0.lock().unwrap();
            let drain = state.drain.as_ref().unwrap();
            drain.mode == H2DrainMode::Idle && drain.initial_end.is_some() && !drain.released
        };
        if !armed {
            return false;
        }
        wakers.writer.register(context.waker());
        let held = {
            let mut state = self.0.lock().unwrap();
            let drain = state.drain.as_mut().unwrap();
            let held =
                drain.mode == H2DrainMode::Idle && drain.initial_end.is_some() && !drain.released;
            if held {
                drain.held_write = true;
            }
            held
        };
        if held {
            self.notify();
        } else {
            drop(wakers.writer.take());
        }
        held
    }

    fn idle(&self) -> bool {
        self.0
            .lock()
            .unwrap()
            .drain
            .is_some_and(|drain| drain.mode == H2DrainMode::Idle)
    }

    fn accepted<'bytes>(
        &self,
        endpoint: WireEndpoint,
        buffers: impl IntoIterator<Item = &'bytes [u8]>,
        accepted: usize,
    ) {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        let WireState {
            log, ends, drain, ..
        } = &mut *state;
        let parser = &mut ends[endpoint.index()].parser;
        log.push(
            endpoint,
            parser.offset,
            WireEvent::Accepted { bytes: accepted },
        );
        let mut remaining = accepted;
        for buffer in buffers {
            let count = remaining.min(buffer.len());
            parser.accept(&buffer[..count], endpoint, log, drain);
            remaining -= count;
            if remaining == 0 {
                break;
            }
        }
        parser.valid &= remaining == 0;
        if let Some(drain) = drain
            && drain.mode == H2DrainMode::Idle
            && endpoint == WireEndpoint::Peer
            && drain.released
        {
            drain.fail(DrainFailure::UnexpectedWrite);
        }
        state.check_drain();
        drop(state);
        self.notify();
    }

    fn terminal(&self, endpoint: WireEndpoint, event: WireEvent) {
        self.terminal_write(endpoint, event, false);
    }

    fn terminal_write(&self, endpoint: WireEndpoint, event: WireEvent, nonempty: bool) {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        let WireState { log, ends, .. } = &mut *state;
        let end = &mut ends[endpoint.index()];
        let slot = match event {
            WireEvent::WriteError(_) => &mut end.first_error,
            WireEvent::ShutdownCalled => &mut end.shutdown_called,
            WireEvent::ShutdownReady(_) => &mut end.shutdown_ready,
            WireEvent::DropStarted => &mut end.drop_started,
            WireEvent::Dropped => &mut end.dropped,
            _ => return,
        };
        if slot.is_none() {
            *slot = Some(log.push(endpoint, end.parser.offset, event));
        }
        let incomplete_drop = event == WireEvent::Dropped && !end.parser.complete();
        if let Some(drain) = &mut state.drain {
            if incomplete_drop {
                drain.fail(DrainFailure::InvalidTrace);
            }
            match event {
                WireEvent::WriteError(error) => {
                    if endpoint == WireEndpoint::Peer
                        && nonempty
                        && drain.idle_boundary()
                        && drain.released
                        && !drain.rejected
                        && error.kind == io::ErrorKind::BrokenPipe
                        && error.os_code.is_none()
                    {
                        drain.rejected = true;
                    } else {
                        drain.fail(DrainFailure::UnexpectedWrite);
                    }
                }
                WireEvent::ShutdownReady(Some(_)) => drain.fail(DrainFailure::EarlyTerminal),
                WireEvent::ShutdownCalled | WireEvent::DropStarted | WireEvent::Dropped => {
                    if drain.mode == H2DrainMode::Healthy {
                        if !drain.healthy() {
                            drain.fail(DrainFailure::EarlyTerminal);
                        }
                    } else if endpoint == WireEndpoint::Peer && !drain.rejected {
                        drain.fail(DrainFailure::EarlyTerminal);
                    } else if endpoint == WireEndpoint::Native && event == WireEvent::Dropped {
                        if !drain.native_goaway || !drain.native_pong {
                            drain.fail(DrainFailure::EarlyTerminal);
                        }
                        drain.native_dropped = true;
                    }
                }
                _ => {}
            }
        }
        state.check_drain();
        drop(state);
        self.notify();
    }

    fn marker(&self, marker: WireMarker) {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        if state.markers[marker.index()].is_some() {
            return;
        }
        let endpoint = match marker {
            WireMarker::GracefulRequested
            | WireMarker::GracefulInvoked
            | WireMarker::PeerOutcome(_) => WireEndpoint::Peer,
            _ => WireEndpoint::Native,
        };
        let offset = state.ends[endpoint.index()].parser.offset;
        let record = state.log.push(endpoint, offset, WireEvent::Marker(marker));
        state.markers[marker.index()] = Some(record);
        state.check_drain();
        drop(state);
        self.notify();
    }

    fn snapshot(&self) -> WireState {
        *self.0.lock().unwrap_or_else(|error| error.into_inner())
    }
}

struct WireIo<IoType> {
    inner: Option<IoType>,
    trace: H2WireTrace,
    endpoint: WireEndpoint,
}

impl<IoType> WireIo<IoType> {
    fn new(inner: IoType, trace: H2WireTrace, endpoint: WireEndpoint) -> Self {
        Self {
            inner: Some(inner),
            trace,
            endpoint,
        }
    }
}

impl<IoType: AsyncRead + Unpin> AsyncRead for WireIo<IoType> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(self.inner.as_mut().unwrap()).poll_read(context, buffer)
    }
}

impl<IoType: AsyncWrite + Unpin> AsyncWrite for WireIo<IoType> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self
            .trace
            .hold_write(self.endpoint, !bytes.is_empty(), context)
        {
            return Poll::Pending;
        }
        let result = Pin::new(self.inner.as_mut().unwrap()).poll_write(context, bytes);
        match &result {
            Poll::Ready(Ok(accepted)) => self.trace.accepted(self.endpoint, [bytes], *accepted),
            Poll::Ready(Err(error)) => self.trace.terminal_write(
                self.endpoint,
                WireEvent::WriteError(WireError::from_io(error)),
                !bytes.is_empty(),
            ),
            Poll::Pending => {}
        }
        result
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let nonempty = buffers.iter().any(|buffer| !buffer.is_empty());
        if self.trace.hold_write(self.endpoint, nonempty, context) {
            return Poll::Pending;
        }
        let result = Pin::new(self.inner.as_mut().unwrap()).poll_write_vectored(context, buffers);
        match &result {
            Poll::Ready(Ok(accepted)) => self.trace.accepted(
                self.endpoint,
                buffers.iter().map(|buffer| &buffer[..]),
                *accepted,
            ),
            Poll::Ready(Err(error)) => self.trace.terminal_write(
                self.endpoint,
                WireEvent::WriteError(WireError::from_io(error)),
                nonempty,
            ),
            Poll::Pending => {}
        }
        result
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.as_ref().unwrap().is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.inner.as_mut().unwrap()).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.trace
            .terminal(self.endpoint, WireEvent::ShutdownCalled);
        let result = Pin::new(self.inner.as_mut().unwrap()).poll_shutdown(context);
        if let Poll::Ready(outcome) = &result {
            self.trace.terminal(
                self.endpoint,
                WireEvent::ShutdownReady(outcome.as_ref().err().map(WireError::from_io)),
            );
        }
        result
    }
}

impl<IoType> Drop for WireIo<IoType> {
    fn drop(&mut self) {
        self.trace.terminal(self.endpoint, WireEvent::DropStarted);
        drop(self.inner.take());
        self.trace.terminal(self.endpoint, WireEvent::Dropped);
    }
}

struct WireInventory {
    connections: [Option<H2WireTrace>; H2_TRACE_CONNECTIONS],
    untraced: u64,
}

struct WireRegistry {
    inventory: Mutex<WireInventory>,
    io: Arc<crate::client::owner::TestIoJoin>,
    drain: Option<H2WireTrace>,
}

impl WireRegistry {
    fn new() -> Self {
        Self {
            inventory: Mutex::new(WireInventory {
                connections: std::array::from_fn(|_| None),
                untraced: 0,
            }),
            io: Arc::new(crate::client::owner::TestIoJoin::default()),
            drain: None,
        }
    }

    fn connection(&self) -> Option<H2WireTrace> {
        let selected = self.drain.as_ref().filter(|trace| trace.bind()).cloned();
        let mut inventory = self
            .inventory
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(slot) = inventory.connections.iter_mut().find(|slot| slot.is_none()) {
            let trace = selected.unwrap_or_else(H2WireTrace::new);
            *slot = Some(trace.clone());
            Some(trace)
        } else {
            inventory.untraced = inventory.untraced.saturating_add(1);
            None
        }
    }

    fn connections(&self) -> [Option<H2WireTrace>; H2_TRACE_CONNECTIONS] {
        self.inventory
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .connections
            .clone()
    }

    fn marker(&self, marker: WireMarker) {
        for trace in self.connections().into_iter().flatten() {
            trace.marker(marker);
        }
    }

    fn emit(&self, output: &mut impl Write) -> io::Result<()> {
        let untraced = self
            .inventory
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .untraced;
        writeln!(
            output,
            "h2-wire-trace connection_limit={H2_TRACE_CONNECTIONS} \
             record_limit={H2_TRACE_RECORDS} untraced={untraced} observation_order_only=true"
        )?;
        for (connection, trace) in self.connections().into_iter().enumerate() {
            let Some(trace) = trace else { continue };
            let state = trace.snapshot();
            writeln!(
                output,
                "h2-wire-trace connection={connection} accepted_trace_complete={} \
                 records={} omitted={} sequence_exhausted={} lifecycle_markers_present={}",
                untraced == 0 && state.complete(),
                state.log.used,
                state.log.omitted,
                state.log.sequence_exhausted,
                state.markers.iter().all(Option::is_some),
            )?;
            for record in state.log.records.iter().flatten() {
                write_wire_record(output, connection, "record", *record)?;
            }
            for endpoint in [WireEndpoint::Native, WireEndpoint::Peer] {
                let end = &state.ends[endpoint.index()];
                let parser = &end.parser;
                writeln!(
                    output,
                    "h2-wire-trace connection={connection} endpoint={endpoint:?} \
                     accepted={} framing_complete={} valid={} preface_remaining={} \
                     header_bytes={} frame_remaining={} control_bytes={} current_frame={:?}",
                    parser.offset,
                    parser.complete(),
                    parser.valid,
                    H2_PREFACE.len() - parser.preface,
                    parser.header_used,
                    parser.remaining,
                    parser.control_used,
                    parser.frame,
                )?;
                for record in [
                    end.first_error,
                    end.shutdown_called,
                    end.shutdown_ready,
                    end.drop_started,
                    end.dropped,
                ]
                .into_iter()
                .flatten()
                {
                    write_wire_record(output, connection, "terminal", record)?;
                }
            }
            for record in state.markers.into_iter().flatten() {
                write_wire_record(output, connection, "marker", record)?;
            }
        }
        Ok(())
    }

    fn emit_safely(&self) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
            self.emit_to_safely(&mut io::stderr().lock());
        })) {
            crate::client::owner::dispose(payload);
        }
    }

    fn emit_to_safely(&self, output: &mut impl Write) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
            let _ = self.emit(output);
        })) {
            crate::client::owner::dispose(payload);
        }
    }
}

fn write_wire_record(
    output: &mut impl Write,
    connection: usize,
    section: &str,
    record: WireRecord,
) -> io::Result<()> {
    write!(
        output,
        "h2-wire-trace connection={connection} {section}={} endpoint={:?} offset={} ",
        record.sequence, record.endpoint, record.offset,
    )?;
    match record.event {
        WireEvent::Accepted { bytes } => writeln!(output, "accepted bytes={bytes}"),
        WireEvent::Preface => writeln!(output, "preface_complete"),
        WireEvent::Frame(frame) => writeln!(
            output,
            "frame type={} flags={} length={} stream={}",
            frame.kind, frame.flags, frame.length, frame.stream,
        ),
        WireEvent::Control(WireControl::Goaway { cutoff, error }) => {
            writeln!(output, "goaway cutoff={cutoff} error={error}")
        }
        WireEvent::Control(WireControl::Reset { error }) => writeln!(output, "reset error={error}"),
        WireEvent::Control(WireControl::Window { increment }) => {
            writeln!(output, "window increment={increment}")
        }
        WireEvent::WriteError(error) => writeln!(
            output,
            "write_error kind={:?} os_code={:?}",
            error.kind, error.os_code,
        ),
        WireEvent::ShutdownCalled => writeln!(output, "shutdown_called"),
        WireEvent::ShutdownReady(error) => writeln!(output, "shutdown_ready error={error:?}"),
        WireEvent::DropStarted => writeln!(output, "drop_started"),
        WireEvent::Dropped => writeln!(output, "dropped"),
        WireEvent::Marker(marker) => match marker {
            WireMarker::GracefulRequested => writeln!(output, "graceful_requested"),
            WireMarker::GracefulInvoked => writeln!(output, "graceful_invoked"),
            WireMarker::PeerOutcome(outcome) => writeln!(output, "peer_outcome={outcome:?}"),
            WireMarker::ClientShutdownRequested => writeln!(output, "client_shutdown_requested"),
            WireMarker::ClientRunsReaped { successful } => {
                writeln!(output, "client_runs_reaped successful={successful}")
            }
            WireMarker::IoRetirementBranchObserved { reached } => {
                writeln!(output, "io_retirement_branch_observed reached={reached}")
            }
            WireMarker::WorkerJoined { successful } => {
                writeln!(output, "worker_joined successful={successful}")
            }
        },
    }
}

#[test]
fn h2_drain_phase_receipt_requires_complete_controls() {
    const INITIAL: [u8; 17] = [0, 0, 8, 7, 0, 0, 0, 0, 0, 127, 255, 255, 255, 0, 0, 0, 0];
    const PING: [u8; 17] = [0, 0, 8, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    const NATIVE: [u8; 17] = [0, 0, 8, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    const PONG: [u8; 17] = [0, 0, 8, 6, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    const FINAL: [u8; 17] = [0, 0, 8, 7, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0];

    struct Delegate {
        limit: usize,
        pending: bool,
        error: Option<io::ErrorKind>,
        calls: usize,
        accepted: usize,
    }

    impl Delegate {
        fn new() -> Self {
            Self {
                limit: usize::MAX,
                pending: false,
                error: None,
                calls: 0,
                accepted: 0,
            }
        }

        fn write(&mut self, length: usize) -> Poll<io::Result<usize>> {
            self.calls += 1;
            if self.pending {
                return Poll::Pending;
            }
            if let Some(error) = self.error {
                return Poll::Ready(Err(error.into()));
            }
            let accepted = length.min(self.limit);
            self.accepted += accepted;
            Poll::Ready(Ok(accepted))
        }
    }

    impl AsyncWrite for Delegate {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.write(bytes.len())
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buffers: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            self.write(buffers.iter().map(|buffer| buffer.len()).sum())
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    struct DelayedPeerPublication {
        peer: tokio::io::DuplexStream,
        native: Option<WireIo<tokio::io::DuplexStream>>,
        trace: H2WireTrace,
        accepted: usize,
        calls: usize,
    }

    impl DelayedPeerPublication {
        fn after_accept(&mut self, result: &Poll<io::Result<usize>>) {
            let Poll::Ready(Ok(accepted)) = result else {
                return;
            };
            self.accepted += *accepted;
            if self.accepted != INITIAL.len() + PING.len() || self.native.is_none() {
                return;
            }
            let before = self.trace.snapshot();
            assert_eq!(before.drain.unwrap().peer, PeerDrainPhase::Ping);
            assert_eq!(before.ends[WireEndpoint::Peer.index()].parser.offset, 32);
            assert!(!before.drain.unwrap().native_dropped);
            let mut native = self.native.take().unwrap();
            let mut bytes = [0; 34];
            let mut buffer = ReadBuf::new(&mut bytes);
            let mut context = Context::from_waker(futures::task::noop_waker_ref());
            assert!(matches!(
                Pin::new(&mut native).poll_read(&mut context, &mut buffer),
                Poll::Ready(Ok(()))
            ));
            assert_eq!(buffer.filled().len(), 34);
            assert_eq!(&buffer.filled()[..17], &INITIAL);
            assert_eq!(&buffer.filled()[17..], &PING);
            assert_eq!(write(&mut native, &NATIVE), 17);
            assert_eq!(write(&mut native, &PONG), 17);
            drop(native);
            let after = self.trace.snapshot();
            assert!(after.drain.unwrap().native_dropped);
            assert!(after.drain.unwrap().failure.is_none());
            assert_eq!(after.drain.unwrap().peer, PeerDrainPhase::Ping);
            assert_eq!(after.ends[WireEndpoint::Peer.index()].parser.offset, 32);
            assert!(
                H2DrainReceipt(self.trace.clone())
                    .poll_ready(&mut context, DrainState::idle_boundary)
                    .is_pending()
            );
        }
    }

    impl AsyncWrite for DelayedPeerPublication {
        fn poll_write(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.calls += 1;
            let result = Pin::new(&mut self.peer).poll_write(context, bytes);
            self.after_accept(&result);
            result
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
            buffers: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            self.calls += 1;
            let result = Pin::new(&mut self.peer).poll_write_vectored(context, buffers);
            self.after_accept(&result);
            result
        }

        fn is_write_vectored(&self) -> bool {
            self.peer.is_write_vectored()
        }

        fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.peer).poll_flush(context)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.peer).poll_shutdown(context)
        }
    }

    fn write(writer: &mut (impl AsyncWrite + Unpin), bytes: &[u8]) -> usize {
        let mut context = Context::from_waker(futures::task::noop_waker_ref());
        match Pin::new(writer).poll_write(&mut context, bytes) {
            Poll::Ready(Ok(accepted)) => accepted,
            _ => panic!("control delegate must accept its declared prefix"),
        }
    }

    fn prepare(mode: H2DrainMode) -> (H2DrainReceipt, WireIo<Delegate>, WireIo<Delegate>) {
        let trace = H2WireTrace::with_drain(mode);
        assert!(trace.bind());
        let mut native = WireIo::new(Delegate::new(), trace.clone(), WireEndpoint::Native);
        let mut peer = WireIo::new(Delegate::new(), trace.clone(), WireEndpoint::Peer);
        assert_eq!(write(&mut native, H2_PREFACE), 24);
        assert_eq!(write(&mut native, &NATIVE), 17);
        assert_eq!(write(&mut native, &PONG), 17);
        assert_eq!(write(&mut peer, &INITIAL), 17);
        assert_eq!(write(&mut peer, &PING), 17);
        (H2DrainReceipt(trace), native, peer)
    }

    struct UnlockedWake {
        state: std::sync::Weak<Mutex<WireState>>,
        count: AtomicUsize,
    }

    impl futures::task::ArcWake for UnlockedWake {
        fn wake_by_ref(wake: &Arc<Self>) {
            let state = wake.state.upgrade().unwrap();
            assert!(
                state.try_lock().is_ok(),
                "publication must release the trace mutex"
            );
            wake.count.fetch_add(1, Ordering::SeqCst);
        }
    }

    let (receipt, native, mut peer) = prepare(H2DrainMode::Healthy);
    let wake = Arc::new(UnlockedWake {
        state: Arc::downgrade(&receipt.0.0),
        count: AtomicUsize::new(0),
    });
    let waker = futures::task::waker(Arc::clone(&wake));
    let mut context = Context::from_waker(&waker);
    assert!(
        receipt
            .poll_ready(&mut context, DrainState::healthy)
            .is_pending()
    );
    peer.inner.as_mut().unwrap().pending = true;
    assert!(
        Pin::new(&mut peer)
            .poll_write(&mut context, &FINAL)
            .is_pending()
    );
    assert_eq!(peer.inner.as_ref().unwrap().accepted, 34);
    peer.inner.as_mut().unwrap().pending = false;
    peer.inner.as_mut().unwrap().limit = 0;
    assert_eq!(write(&mut peer, &FINAL), 0);
    assert!(
        receipt
            .poll_ready(&mut context, DrainState::healthy)
            .is_pending()
    );
    peer.inner.as_mut().unwrap().limit = 9;
    assert_eq!(write(&mut peer, &FINAL), 9);
    assert!(
        receipt
            .poll_ready(&mut context, DrainState::healthy)
            .is_pending()
    );
    peer.inner.as_mut().unwrap().limit = 7;
    assert!(matches!(
        Pin::new(&mut peer).poll_write_vectored(
            &mut context,
            &[IoSlice::new(&FINAL[9..12]), IoSlice::new(&FINAL[12..])]
        ),
        Poll::Ready(Ok(7))
    ));
    assert!(
        receipt
            .poll_ready(&mut context, DrainState::healthy)
            .is_pending()
    );
    assert_eq!(write(&mut peer, &FINAL[16..]), 1);
    assert_eq!(
        receipt.poll_ready(&mut context, DrainState::healthy),
        Poll::Ready(Ok(()))
    );
    assert_eq!(peer.inner.as_ref().unwrap().accepted, 51);
    assert!(wake.count.load(Ordering::SeqCst) > 0);
    drop(native);
    drop(peer);
    assert!(receipt.0.snapshot().complete());

    let mut context = Context::from_waker(futures::task::noop_waker_ref());
    let (receipt, _native, mut peer) = prepare(H2DrainMode::Healthy);
    assert_eq!(write(&mut peer, &FINAL), 17);
    assert_eq!(
        receipt.poll_ready(&mut context, DrainState::healthy),
        Poll::Ready(Ok(()))
    );

    let trace = H2WireTrace::with_drain(H2DrainMode::Healthy);
    assert!(trace.bind());
    let receipt = H2DrainReceipt(trace.clone());
    let mut native = WireIo::new(Delegate::new(), trace.clone(), WireEndpoint::Native);
    let mut peer = WireIo::new(Delegate::new(), trace, WireEndpoint::Peer);
    assert_eq!(write(&mut native, H2_PREFACE), 24);
    for (endpoint, frame) in [
        (WireEndpoint::Peer, INITIAL),
        (WireEndpoint::Peer, PING),
        (WireEndpoint::Native, NATIVE),
        (WireEndpoint::Native, PONG),
        (WireEndpoint::Peer, FINAL),
    ] {
        let writer = if endpoint == WireEndpoint::Peer {
            &mut peer
        } else {
            &mut native
        };
        assert_eq!(write(writer, &frame[..16]), 16);
        assert!(
            receipt
                .poll_ready(&mut context, DrainState::healthy)
                .is_pending()
        );
        assert_eq!(write(writer, &frame[16..]), 1);
    }
    assert_eq!(
        receipt.poll_ready(&mut context, DrainState::healthy),
        Poll::Ready(Ok(()))
    );

    for (index, value) in [(12, 5), (16, 1), (2, 7), (4, 1), (8, 1)] {
        let (receipt, _native, mut peer) = prepare(H2DrainMode::Healthy);
        let mut wrong = FINAL;
        wrong[index] = value;
        assert_eq!(write(&mut peer, &wrong), 17);
        assert!(matches!(
            receipt.poll_ready(&mut context, DrainState::healthy),
            Poll::Ready(Err(_))
        ));
    }
    let (receipt, _native, mut peer) = prepare(H2DrainMode::Healthy);
    let mut debug = [0; 18];
    debug[..17].copy_from_slice(&FINAL);
    debug[2] = 9;
    assert_eq!(write(&mut peer, &debug[..17]), 17);
    assert!(
        receipt
            .poll_ready(&mut context, DrainState::healthy)
            .is_pending()
    );
    assert_eq!(write(&mut peer, &debug[17..]), 1);
    assert_eq!(
        receipt.poll_ready(&mut context, DrainState::healthy),
        Poll::Ready(Err(DrainFailure::UnexpectedControl))
    );

    let (receipt, native, _peer) = prepare(H2DrainMode::Healthy);
    drop(native);
    assert_eq!(
        receipt.poll_ready(&mut context, DrainState::healthy),
        Poll::Ready(Err(DrainFailure::EarlyTerminal))
    );
    let (receipt, _native, mut peer) = prepare(H2DrainMode::Healthy);
    peer.inner.as_mut().unwrap().error = Some(io::ErrorKind::BrokenPipe);
    assert!(matches!(
        Pin::new(&mut peer).poll_write(&mut context, &FINAL),
        Poll::Ready(Err(_))
    ));
    assert_eq!(
        receipt.poll_ready(&mut context, DrainState::healthy),
        Poll::Ready(Err(DrainFailure::UnexpectedWrite))
    );

    let (receipt, _native, mut peer) = prepare(H2DrainMode::Healthy);
    for _record in 0..=H2_TRACE_RECORDS {
        assert_eq!(write(&mut peer, &[]), 0);
    }
    assert_eq!(write(&mut peer, &FINAL), 17);
    assert_eq!(
        receipt.poll_ready(&mut context, DrainState::healthy),
        Poll::Ready(Err(DrainFailure::InvalidTrace))
    );
    let mut registry = WireRegistry::new();
    let trace = H2WireTrace::with_drain(H2DrainMode::Healthy);
    registry.drain = Some(trace.clone());
    let original = registry.connection().unwrap();
    let other = registry.connection().unwrap();
    assert!(Arc::ptr_eq(&original.0, &trace.0));
    assert!(!Arc::ptr_eq(&other.0, &trace.0));
    assert_eq!(
        H2DrainReceipt(trace).poll_ready(&mut context, DrainState::healthy),
        Poll::Ready(Err(DrainFailure::WrongConnection))
    );

    for vectored in [false, true] {
        let trace = H2WireTrace::with_drain(H2DrainMode::Idle);
        assert!(trace.bind());
        let receipt = H2DrainReceipt(trace.clone());
        let _release = receipt.release_on_drop();
        let (native, peer) = tokio::io::duplex(4096);
        let mut native = WireIo::new(native, trace.clone(), WireEndpoint::Native);
        let mut peer = WireIo::new(peer, trace.clone(), WireEndpoint::Peer);
        assert_eq!(write(&mut native, H2_PREFACE), 24);
        assert_eq!(write(&mut native, &NATIVE), 17);
        assert_eq!(write(&mut native, &PONG), 17);
        assert_eq!(write(&mut peer, &INITIAL), 17);
        assert_eq!(write(&mut peer, &PING), 17);
        assert_eq!(write(&mut peer, &[]), 0);
        assert!(!trace.snapshot().drain.unwrap().held_write);
        let wake = Arc::new(UnlockedWake {
            state: Arc::downgrade(&trace.0),
            count: AtomicUsize::new(0),
        });
        let waker = futures::task::waker(Arc::clone(&wake));
        let mut context = Context::from_waker(&waker);
        if !vectored {
            assert!(
                Pin::new(&mut peer)
                    .poll_write(&mut context, &FINAL)
                    .is_pending()
            );
            assert!(trace.snapshot().drain.unwrap().held_write);
        }
        assert!(
            receipt
                .poll_ready(&mut context, DrainState::idle_boundary)
                .is_pending()
        );
        assert!(matches!(
            Pin::new(&mut native).poll_shutdown(&mut context),
            Poll::Ready(Ok(()))
        ));
        assert!(!trace.snapshot().drain.unwrap().native_dropped);
        assert!(
            receipt
                .poll_ready(&mut context, DrainState::idle_boundary)
                .is_pending()
        );
        drop(native);
        if vectored {
            assert!(
                receipt
                    .poll_ready(&mut context, DrainState::idle_boundary)
                    .is_pending()
            );
            assert!(
                Pin::new(&mut peer)
                    .poll_write_vectored(
                        &mut context,
                        &[IoSlice::new(&FINAL[..9]), IoSlice::new(&FINAL[9..])]
                    )
                    .is_pending()
            );
            assert!(trace.snapshot().drain.unwrap().held_write);
        }
        assert_eq!(
            receipt.poll_ready(&mut context, DrainState::idle_boundary),
            Poll::Ready(Ok(()))
        );
        assert!(!trace.snapshot().drain.unwrap().released);
        receipt.release_idle();
        assert!(wake.count.load(Ordering::SeqCst) > 0);
        let result = if vectored {
            Pin::new(&mut peer).poll_write_vectored(
                &mut context,
                &[IoSlice::new(&FINAL[..9]), IoSlice::new(&FINAL[9..])],
            )
        } else {
            Pin::new(&mut peer).poll_write(&mut context, &FINAL)
        };
        let Poll::Ready(Err(error)) = result else {
            panic!("real duplex write must reject after native destruction")
        };
        assert_eq!(
            WireError::from_io(&error),
            WireError {
                kind: io::ErrorKind::BrokenPipe,
                os_code: None
            }
        );
        assert!(trace.snapshot().drain.unwrap().rejected);
        assert_eq!(
            trace.snapshot().ends[WireEndpoint::Peer.index()]
                .parser
                .offset,
            34
        );
        drop(peer);
        assert!(trace.snapshot().complete());
    }

    for vectored in [false, true] {
        let trace = H2WireTrace::with_drain(H2DrainMode::Idle);
        assert!(trace.bind());
        let receipt = H2DrainReceipt(trace.clone());
        let _release = receipt.release_on_drop();
        let (native, peer) = tokio::io::duplex(4096);
        let mut native = WireIo::new(native, trace.clone(), WireEndpoint::Native);
        assert_eq!(write(&mut native, H2_PREFACE), 24);
        let inner = DelayedPeerPublication {
            peer,
            native: Some(native),
            trace: trace.clone(),
            accepted: 0,
            calls: 0,
        };
        let mut peer = WireIo::new(inner, trace.clone(), WireEndpoint::Peer);
        assert_eq!(write(&mut peer, &INITIAL), 17);
        assert_eq!(write(&mut peer, &PING[..15]), 15);
        let result = if vectored {
            Pin::new(&mut peer).poll_write_vectored(
                &mut context,
                &[IoSlice::new(&PING[15..16]), IoSlice::new(&PING[16..])],
            )
        } else {
            Pin::new(&mut peer).poll_write(&mut context, &PING[15..])
        };
        assert!(matches!(result, Poll::Ready(Ok(2))));
        assert!(peer.inner.as_ref().unwrap().native.is_none());
        let published = trace.snapshot();
        assert!(published.drain.unwrap().native_dropped);
        assert!(published.drain.unwrap().failure.is_none());
        assert_eq!(published.drain.unwrap().peer, PeerDrainPhase::Final);
        assert_eq!(published.ends[WireEndpoint::Peer.index()].parser.offset, 34);
        assert!(!published.drain.unwrap().held_write);
        assert!(!published.drain.unwrap().released);
        assert!(
            receipt
                .poll_ready(&mut context, DrainState::idle_boundary)
                .is_pending()
        );
        let calls = peer.inner.as_ref().unwrap().calls;
        let held = if vectored {
            Pin::new(&mut peer).poll_write_vectored(
                &mut context,
                &[IoSlice::new(&FINAL[..9]), IoSlice::new(&FINAL[9..])],
            )
        } else {
            Pin::new(&mut peer).poll_write(&mut context, &FINAL)
        };
        assert!(held.is_pending());
        assert_eq!(peer.inner.as_ref().unwrap().calls, calls);
        assert_eq!(
            receipt.poll_ready(&mut context, DrainState::idle_boundary),
            Poll::Ready(Ok(()))
        );
        receipt.release_idle();
        let result = if vectored {
            Pin::new(&mut peer).poll_write_vectored(
                &mut context,
                &[IoSlice::new(&FINAL[..9]), IoSlice::new(&FINAL[9..])],
            )
        } else {
            Pin::new(&mut peer).poll_write(&mut context, &FINAL)
        };
        let Poll::Ready(Err(error)) = result else {
            panic!("actual delegate must reject after the interposed native destruction")
        };
        assert_eq!(
            WireError::from_io(&error),
            WireError {
                kind: io::ErrorKind::BrokenPipe,
                os_code: None,
            }
        );
        assert_eq!(peer.inner.as_ref().unwrap().calls, calls + 1);
        assert_eq!(peer.inner.as_ref().unwrap().accepted, 34);
        assert_eq!(
            trace.snapshot().ends[WireEndpoint::Peer.index()]
                .parser
                .offset,
            34
        );
        assert!(trace.snapshot().drain.unwrap().rejected);
        drop(peer);
        assert!(trace.snapshot().complete());
    }

    let (receipt, _native, mut peer) = prepare(H2DrainMode::Idle);
    let release = receipt.release_on_drop();
    let before = peer.inner.as_ref().unwrap().calls;
    assert!(
        Pin::new(&mut peer)
            .poll_write(&mut context, &FINAL)
            .is_pending()
    );
    assert_eq!(peer.inner.as_ref().unwrap().calls, before);
    drop(release);
    assert_eq!(write(&mut peer, &FINAL), 17);
    assert_eq!(
        receipt.poll_ready(&mut context, DrainState::idle_boundary),
        Poll::Ready(Err(DrainFailure::Abandoned))
    );

    for progress in [0, 1] {
        let (receipt, native, mut peer) = prepare(H2DrainMode::Idle);
        let _release = receipt.release_on_drop();
        assert!(
            Pin::new(&mut peer)
                .poll_write(&mut context, &FINAL)
                .is_pending()
        );
        assert!(catch_unwind(AssertUnwindSafe(|| receipt.release_idle())).is_err());
        drop(native);
        receipt.release_idle();
        peer.inner.as_mut().unwrap().limit = progress;
        assert_eq!(write(&mut peer, &FINAL), progress);
        assert_eq!(
            receipt.poll_ready(&mut context, DrainState::idle_boundary),
            Poll::Ready(Err(DrainFailure::UnexpectedWrite))
        );
    }

    let trace = H2WireTrace::with_drain(H2DrainMode::Idle);
    assert!(trace.bind());
    let mut peer = WireIo::new(Delegate::new(), trace.clone(), WireEndpoint::Peer);
    let coalesced = [INITIAL, PING, FINAL].concat();
    assert_eq!(write(&mut peer, &coalesced), 51);
    assert!(trace.snapshot().drain.unwrap().failure.is_some());

    let held = Gate::new();
    let release = Gate::new();
    let release_guard = release.release_on_drop();
    let mut body = held_sibling_body(held.clone(), release.clone());
    let Poll::Ready(Some(Ok(prefix))) = Pin::new(&mut body).poll_frame(&mut context) else {
        panic!("actual prefix")
    };
    assert_eq!(prefix.into_data().unwrap(), "sib");
    assert!(!body.is_end_stream());
    assert!(Pin::new(&mut body).poll_frame(&mut context).is_pending());
    assert!(held.is_open());
    assert!(!release.is_open());
    drop(release_guard);
    let Poll::Ready(Some(Ok(suffix))) = Pin::new(&mut body).poll_frame(&mut context) else {
        panic!("released suffix")
    };
    assert_eq!(suffix.into_data().unwrap(), "ling");
    assert!(matches!(
        Pin::new(&mut body).poll_frame(&mut context),
        Poll::Ready(None)
    ));
}

#[test]
fn h2_wire_trace_preserves_segmented_frame_metadata() {
    #[derive(Default)]
    struct Observed {
        bytes: Vec<u8>,
        scalar: usize,
        vectored: usize,
        reads: usize,
        flushes: usize,
        shutdowns: usize,
        drops: usize,
    }

    struct PartialIo {
        observed: Arc<Mutex<Observed>>,
        limit: usize,
        pending: bool,
        error: Option<i32>,
    }

    impl PartialIo {
        fn write(&mut self, buffers: &[IoSlice<'_>]) -> Poll<io::Result<usize>> {
            if self.pending {
                return Poll::Pending;
            }
            if let Some(code) = self.error.take() {
                return Poll::Ready(Err(io::Error::from_raw_os_error(code)));
            }
            let mut accepted = 0;
            let mut observed = self.observed.lock().unwrap();
            for buffer in buffers {
                let count = buffer.len().min(self.limit - accepted);
                observed.bytes.extend_from_slice(&buffer[..count]);
                accepted += count;
            }
            Poll::Ready(Ok(accepted))
        }
    }

    impl AsyncRead for PartialIo {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            self.observed.lock().unwrap().reads += 1;
            buffer.put_slice(b"ok");
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for PartialIo {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.observed.lock().unwrap().scalar += 1;
            self.write(&[IoSlice::new(bytes)])
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buffers: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            self.observed.lock().unwrap().vectored += 1;
            self.write(buffers)
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.observed.lock().unwrap().flushes += 1;
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.observed.lock().unwrap().shutdowns += 1;
            Poll::Ready(Ok(()))
        }
    }

    impl Drop for PartialIo {
        fn drop(&mut self) {
            self.observed.lock().unwrap().drops += 1;
        }
    }

    fn frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes()[1..]);
        bytes.extend_from_slice(&[kind, flags]);
        bytes.extend_from_slice(&stream.to_be_bytes());
        bytes.extend_from_slice(payload);
        bytes
    }

    fn write_all(writer: &mut WireIo<PartialIo>, context: &mut Context<'_>, mut bytes: &[u8]) {
        while !bytes.is_empty() {
            let Poll::Ready(Ok(accepted)) = Pin::new(&mut *writer).poll_write(context, bytes)
            else {
                panic!("the controlled writer must accept its bounded prefix");
            };
            assert!(accepted > 0);
            bytes = &bytes[accepted..];
        }
    }

    let registry = WireRegistry::new();
    let trace = registry.connection().unwrap();
    let observed = Arc::new(Mutex::new(Observed::default()));
    let mut writer = WireIo::new(
        PartialIo {
            observed: Arc::clone(&observed),
            limit: 3,
            pending: true,
            error: None,
        },
        trace.clone(),
        WireEndpoint::Native,
    );
    let mut context = Context::from_waker(std::task::Waker::noop());
    assert!(
        Pin::new(&mut writer)
            .poll_write(&mut context, H2_PREFACE)
            .is_pending()
    );
    assert!(
        Pin::new(&mut writer)
            .poll_write_vectored(&mut context, &[IoSlice::new(H2_PREFACE)])
            .is_pending()
    );
    assert_eq!(trace.snapshot().log.used, 0);
    writer.inner.as_mut().unwrap().pending = false;
    writer.inner.as_mut().unwrap().limit = 0;
    assert!(matches!(
        Pin::new(&mut writer).poll_write(&mut context, H2_PREFACE),
        Poll::Ready(Ok(0))
    ));
    assert_eq!(trace.snapshot().ends[0].parser.offset, 0);
    writer.inner.as_mut().unwrap().limit = 3;
    for bytes in H2_PREFACE.chunks(5) {
        write_all(&mut writer, &mut context, bytes);
    }

    let data = frame(0, 1, 1, b"synthetic-data-not-retained");
    let headers = frame(1, 4, 3, b"synthetic-header-block-not-retained");
    let reset = frame(3, 0, 1, &8_u32.to_be_bytes());
    let window = frame(8, 0, 0, &0x8000_1234_u32.to_be_bytes());
    let mut goaway_payload = vec![0x80, 0, 0, 3, 0, 0, 0, 2];
    goaway_payload.extend_from_slice(b"synthetic-goaway-debug-not-retained");
    let goaway = frame(7, 0, 0, &goaway_payload);
    let frames = [
        data.as_slice(),
        headers.as_slice(),
        reset.as_slice(),
        window.as_slice(),
        goaway.as_slice(),
    ]
    .concat();
    writer.inner.as_mut().unwrap().limit = 5;
    assert!(writer.is_write_vectored());
    let mut offset = 0;
    while offset < frames.len() {
        let middle = (offset + 2).min(frames.len());
        let buffers = [
            IoSlice::new(&frames[offset..middle]),
            IoSlice::new(&frames[middle..]),
        ];
        let Poll::Ready(Ok(accepted)) =
            Pin::new(&mut writer).poll_write_vectored(&mut context, &buffers)
        else {
            panic!("the vectored delegate must accept the same bounded prefix");
        };
        assert_eq!(accepted, 5.min(frames.len() - offset));
        offset += accepted;
    }
    assert_eq!(
        observed.lock().unwrap().bytes,
        [H2_PREFACE.as_slice(), frames.as_slice()].concat()
    );
    let snapshot = trace.snapshot();
    assert!(snapshot.ends[0].parser.complete());
    assert_eq!(snapshot.ends[0].parser.control, [0; 8]);
    let metadata: Vec<_> = snapshot
        .log
        .records
        .iter()
        .flatten()
        .filter_map(|record| {
            if let WireEvent::Frame(frame) = record.event {
                Some((record.offset, frame))
            } else {
                None
            }
        })
        .collect();
    assert_eq!(metadata.len(), 5);
    assert_eq!(
        metadata[0],
        (
            24,
            WireFrame {
                length: 27,
                kind: 0,
                flags: 1,
                stream: 1
            }
        )
    );
    assert_eq!(metadata[1].0, 24 + data.len() as u64);
    assert_eq!(metadata[1].1.flags, 4);
    assert_eq!(metadata[1].1.stream, 3);
    let controls: Vec<_> = snapshot
        .log
        .records
        .iter()
        .flatten()
        .filter_map(|record| {
            if let WireEvent::Control(control) = record.event {
                Some(control)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(
        controls,
        [
            WireControl::Reset { error: 8 },
            WireControl::Window { increment: 0x1234 },
            WireControl::Goaway {
                cutoff: 3,
                error: 2
            },
        ]
    );
    let accepted_offset = 24 + frames.len() as u64;
    writer.inner.as_mut().unwrap().error = Some(32);
    let Poll::Ready(Err(error)) =
        Pin::new(&mut writer).poll_write(&mut context, b"attempt-not-accepted")
    else {
        panic!("preserve the delegate's actual error");
    };
    assert_eq!(error.raw_os_error(), Some(32));
    let first_error = trace.snapshot().ends[0].first_error.unwrap();
    assert_eq!(first_error.offset, accepted_offset);
    assert_eq!(
        first_error.event,
        WireEvent::WriteError(WireError {
            kind: io::ErrorKind::BrokenPipe,
            os_code: Some(32),
        })
    );
    assert_eq!(trace.snapshot().ends[0].parser.offset, accepted_offset);
    writer.inner.as_mut().unwrap().error = Some(5);
    let Poll::Ready(Err(error)) = Pin::new(&mut writer)
        .poll_write_vectored(&mut context, &[IoSlice::new(b"another-unaccepted-attempt")])
    else {
        panic!("preserve a later vectored error without replacing the first summary");
    };
    assert_eq!(error.raw_os_error(), Some(5));
    assert_eq!(trace.snapshot().ends[0].first_error, Some(first_error));
    assert_eq!(trace.snapshot().ends[0].parser.offset, accepted_offset);
    let mut read_storage = [0; 2];
    let mut read_buffer = ReadBuf::new(&mut read_storage);
    assert!(matches!(
        Pin::new(&mut writer).poll_read(&mut context, &mut read_buffer),
        Poll::Ready(Ok(()))
    ));
    assert_eq!(read_buffer.filled(), b"ok");
    assert!(matches!(
        Pin::new(&mut writer).poll_flush(&mut context),
        Poll::Ready(Ok(()))
    ));
    assert!(matches!(
        Pin::new(&mut writer).poll_shutdown(&mut context),
        Poll::Ready(Ok(()))
    ));
    drop(writer);
    {
        let observed = observed.lock().unwrap();
        assert!(observed.scalar > 1 && observed.vectored > 1);
        assert_eq!(
            (
                observed.reads,
                observed.flushes,
                observed.shutdowns,
                observed.drops
            ),
            (1, 1, 1, 1)
        );
    }
    let end = trace.snapshot().ends[0];
    assert!(end.shutdown_called.unwrap().sequence < end.shutdown_ready.unwrap().sequence);
    assert!(end.shutdown_ready.unwrap().sequence < end.drop_started.unwrap().sequence);
    assert!(end.drop_started.unwrap().sequence < end.dropped.unwrap().sequence);

    let mut peer = WireIo::new(
        PartialIo {
            observed: Arc::clone(&observed),
            limit: 16,
            pending: false,
            error: None,
        },
        trace.clone(),
        WireEndpoint::Peer,
    );
    write_all(&mut peer, &mut context, &frame(4, 0, 0, &[]));
    drop(peer);
    assert!(trace.snapshot().complete());
    assert_eq!(observed.lock().unwrap().drops, 2);

    let mut output = Vec::new();
    registry.emit(&mut output).unwrap();
    let output = String::from_utf8(output).unwrap();
    for forbidden in [
        "synthetic-data",
        "synthetic-header-block",
        "synthetic-goaway-debug",
        "attempt-not-accepted",
        "another-unaccepted-attempt",
    ] {
        assert!(!output.contains(forbidden));
    }

    struct FailedOutput(bool);
    impl Write for FailedOutput {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            if self.0 {
                panic!("synthetic diagnostic output panic");
            }
            Err(io::Error::from(io::ErrorKind::BrokenPipe))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let primary = Arc::new(());
    let retained: Result<(), Arc<()>> = Err(Arc::clone(&primary));
    registry.emit_to_safely(&mut FailedOutput(false));
    registry.emit_to_safely(&mut FailedOutput(true));
    assert!(Arc::ptr_eq(retained.as_ref().unwrap_err(), &primary));

    let overflow = registry.connection().unwrap();
    overflow.accepted(
        WireEndpoint::Native,
        [H2_PREFACE.as_slice()],
        H2_PREFACE.len(),
    );
    let empty = frame(4, 0, 0, &[]);
    for _record in 0..H2_TRACE_RECORDS {
        overflow.accepted(WireEndpoint::Peer, [empty.as_slice()], empty.len());
    }
    for endpoint in [WireEndpoint::Native, WireEndpoint::Peer] {
        overflow.terminal(
            endpoint,
            WireEvent::WriteError(WireError {
                kind: io::ErrorKind::BrokenPipe,
                os_code: Some(32),
            }),
        );
        overflow.terminal(endpoint, WireEvent::ShutdownCalled);
        overflow.terminal(endpoint, WireEvent::ShutdownReady(None));
        overflow.terminal(endpoint, WireEvent::DropStarted);
        overflow.terminal(endpoint, WireEvent::Dropped);
    }
    overflow.marker(WireMarker::PeerOutcome(WirePeerOutcome::Error));
    overflow.marker(WireMarker::WorkerJoined { successful: true });
    let snapshot = overflow.snapshot();
    assert_eq!(snapshot.log.used, H2_TRACE_RECORDS);
    assert!(snapshot.log.omitted > 0);
    assert!(!snapshot.complete());
    assert_eq!(
        snapshot.ends[1].first_error.unwrap().offset,
        (H2_TRACE_RECORDS * 9) as u64
    );
    assert!(snapshot.ends.iter().all(|end| {
        end.first_error.is_some() && end.shutdown_ready.is_some() && end.dropped.is_some()
    }));
    assert!(snapshot.markers[2].is_some() && snapshot.markers[6].is_some());
    assert!(registry.connection().is_none());
    assert_eq!(registry.inventory.lock().unwrap().untraced, 1);

    let incomplete = H2WireTrace::new();
    incomplete.accepted(WireEndpoint::Peer, [&reset[..5]], 5);
    assert_eq!(incomplete.snapshot().ends[1].parser.header_used, 5);
    assert!(!incomplete.snapshot().ends[1].parser.complete());
    incomplete.accepted(WireEndpoint::Peer, [&reset[5..11]], 6);
    assert_eq!(incomplete.snapshot().ends[1].parser.control_used, 2);
    assert!(!incomplete.snapshot().ends[1].parser.complete());
    incomplete.accepted(WireEndpoint::Peer, [&reset[11..]], 2);
    assert!(incomplete.snapshot().ends[1].parser.complete());
    assert!(!incomplete.snapshot().complete());

    let invalid = H2WireTrace::new();
    let opaque = b"invalid-preface-and-opaque-bytes";
    invalid.accepted(WireEndpoint::Native, [opaque.as_slice()], opaque.len());
    let snapshot = invalid.snapshot();
    assert!(!snapshot.ends[0].parser.valid);
    assert!(!snapshot.ends[0].parser.complete());
    assert!(
        snapshot
            .log
            .records
            .iter()
            .flatten()
            .all(|record| { matches!(record.event, WireEvent::Accepted { .. }) })
    );

    let final_byte_mismatch = H2WireTrace::new();
    let bad_observed = Arc::new(Mutex::new(Observed::default()));
    let mut bad_preface = *H2_PREFACE;
    bad_preface[23] ^= 1;
    let mut writer = WireIo::new(
        PartialIo {
            observed: Arc::clone(&bad_observed),
            limit: 23,
            pending: false,
            error: None,
        },
        final_byte_mismatch.clone(),
        WireEndpoint::Native,
    );
    write_all(&mut writer, &mut context, &bad_preface[..23]);
    write_all(&mut writer, &mut context, &bad_preface[23..]);
    drop(writer);
    let observed = bad_observed.lock().unwrap();
    assert_eq!(observed.bytes.as_slice(), bad_preface.as_slice());
    assert_eq!((observed.scalar, observed.drops), (2, 1));
    let snapshot = final_byte_mismatch.snapshot();
    assert_eq!(snapshot.ends[0].parser.offset, 24);
    assert!(!snapshot.ends[0].parser.valid);
    assert!(!snapshot.ends[0].parser.complete());
    assert!(snapshot.log.records.iter().flatten().all(|record| {
        !matches!(
            record.event,
            WireEvent::Preface | WireEvent::Frame(_) | WireEvent::Control(_)
        )
    }));
}

#[derive(Clone)]
pub(crate) struct Gate(watch::Sender<bool>);
impl Gate {
    pub fn new() -> Self {
        Self(watch::channel(false).0)
    }
    pub fn open(&self) {
        self.0.send_replace(true);
    }
    pub fn is_open(&self) -> bool {
        *self.0.borrow()
    }
    pub fn release_on_drop(&self) -> AsyncGateRelease {
        AsyncGateRelease(self.clone())
    }
    pub async fn wait(&self) {
        let mut receiver = self.0.subscribe();
        while !*receiver.borrow_and_update() {
            if receiver.changed().await.is_err() {
                break;
            }
        }
    }
}

pub(crate) struct AsyncGateRelease(Gate);

impl Drop for AsyncGateRelease {
    fn drop(&mut self) {
        self.0.open();
    }
}

pub(crate) fn held_sibling_body(held: Gate, release: Gate) -> PeerBody {
    use futures::StreamExt;

    http_body_util::StreamBody::new(
        futures::stream::iter([Ok::<_, BoxError>(Frame::data(Bytes::from_static(b"sib")))]).chain(
            futures::stream::once(async move {
                let wait = release.wait();
                tokio::pin!(wait);
                std::future::poll_fn(|context| {
                    let result = wait.as_mut().poll(context);
                    if result.is_pending() {
                        held.open();
                    }
                    result
                })
                .await;
                Ok(Frame::data(Bytes::from_static(b"ling")))
            }),
        ),
    )
    .boxed_unsync()
}

pub(crate) struct DropGate {
    released: Mutex<bool>,
    changed: std::sync::Condvar,
    pub entered: Gate,
}
impl DropGate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            released: Mutex::new(false),
            changed: std::sync::Condvar::new(),
            entered: Gate::new(),
        })
    }
    pub fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
    pub fn block(&self) {
        self.entered.open();
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.changed.wait(released).unwrap();
        }
    }
}
pub(crate) struct ReleaseGate(pub Arc<DropGate>);
impl Drop for ReleaseGate {
    fn drop(&mut self) {
        self.0.release();
    }
}

pub(crate) fn forced_run(error: &ClientError) -> bool {
    matches!(error, ClientError::CleanupUnconfirmed)
}
pub(crate) fn panicked_run(error: &ClientError) -> bool {
    matches!(error, ClientError::Panicked)
}
fn peer_saw_forced_close(error: &hyper::Error) -> bool {
    std::error::Error::source(error)
        .and_then(|cause| cause.downcast_ref::<io::Error>())
        .is_some_and(|cause| {
            matches!(
                cause.kind(),
                io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
            )
        })
}

pub(crate) async fn producer_reaped(probe: &crate::client::owner::TestApplicationJoin) {
    assert!(
        probe.reaped.load(Ordering::Acquire),
        "actual application join driver returned after destruction"
    );
    assert_ne!(*probe.outcome.lock().unwrap(), Some("driver-panic"));
    let cleanup = probe
        .cleanup
        .lock()
        .unwrap()
        .clone()
        .expect("prepared producer cleanup receipt");
    if let Err(error) = cleanup.wait().await {
        assert!(
            !error.producer_join_unconfirmed(),
            "worker loss cannot stand in for producer reaping"
        );
    }
}

enum WorkerAction {
    Pause(oneshot::Sender<()>),
    Resume,
    Drop(oneshot::Sender<()>),
}

#[derive(Clone)]
pub(crate) struct WorkerControl {
    sender: mpsc::Sender<WorkerAction>,
    pub(crate) forced: Arc<AtomicUsize>,
}
impl WorkerControl {
    pub async fn pause(&self) {
        let (reply, done) = oneshot::channel();
        self.sender.send(WorkerAction::Pause(reply)).await.unwrap();
        done.await.unwrap();
    }
    pub async fn resume(&self) {
        let _ = self.sender.send(WorkerAction::Resume).await;
    }
    pub async fn force_drop(&self) {
        let (reply, done) = oneshot::channel();
        if self.sender.send(WorkerAction::Drop(reply)).await.is_ok() {
            let _ = done.await;
        }
    }
}

struct TestWorker {
    handle: WorkerHandle,
    control: WorkerControl,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl TestWorker {
    fn new() -> Self {
        let (sender, mut controls) = mpsc::channel(4);
        let (started, ready) = std::sync::mpsc::sync_channel(1);
        let forced = Arc::new(AtomicUsize::new(0));
        let entered = Arc::clone(&forced);
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let local = tokio::task::LocalSet::new();
            runtime.block_on(local.run_until(async move {
                let (handle, driver) = WorkerDriver::new(128).unwrap();
                started.send(handle).unwrap();
                let mut driver = Box::pin(driver);
                loop {
                    tokio::select! {
                        _ = &mut driver => break,
                        action = controls.recv() => match action {
                            Some(WorkerAction::Pause(reply)) => {
                                let _ = reply.send(());
                                loop {
                                    match controls.recv().await {
                                        Some(WorkerAction::Resume) => break,
                                        Some(WorkerAction::Drop(reply)) => {
                                            entered.fetch_add(1, Ordering::SeqCst);
                                            drop(driver);
                                            let _ = reply.send(());
                                            return;
                                        }
                                        Some(WorkerAction::Pause(reply)) => { let _ = reply.send(()); }
                                        None => return,
                                    }
                                }
                            }
                            Some(WorkerAction::Drop(reply)) => {
                                entered.fetch_add(1, Ordering::SeqCst);
                                drop(driver);
                                let _ = reply.send(());
                                return;
                            }
                            Some(WorkerAction::Resume) => {}
                            None => break,
                        }
                    }
                }
            }));
        });
        Self {
            handle: ready.recv().unwrap(),
            control: WorkerControl { sender, forced },
            thread: Some(thread),
        }
    }

    fn join(&mut self, expected_panic: Option<&str>) -> Result<(), String> {
        let _ = self.control.sender.try_send(WorkerAction::Resume);
        self.handle.begin_shutdown();
        let result = self
            .thread
            .take()
            .expect("one actual worker parent join")
            .join();
        let entered = self.control.forced.load(Ordering::SeqCst);
        match result {
            Ok(()) => match expected_panic {
                None => Ok(()),
                Some(expected) => Err(format!(
                    "forced worker returned success instead of {expected:?}; forced actions={entered}"
                )),
            },
            Err(payload) => {
                let actual = payload
                    .downcast_ref::<&str>()
                    .copied()
                    .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                    .map(str::to_owned);
                let retired = crate::client::owner::dispose(payload);
                if entered == 1
                    && retired
                    && expected_panic.is_some()
                    && actual.as_deref() == expected_panic
                {
                    Ok(())
                } else {
                    Err(format!(
                        "worker parent joined with unexpected panic: actual={actual:?}, expected={expected_panic:?}, forced actions={entered}, payload retired={retired}"
                    ))
                }
            }
        }
    }
}

impl Drop for TestWorker {
    fn drop(&mut self) {
        if self.thread.is_some() {
            self.join(None).expect("worker parent joins actual thread");
        }
    }
}

#[derive(Clone)]
pub(crate) struct PeerExecutor {
    tasks: Arc<Mutex<Option<JoinSet<()>>>>,
    stop: Gate,
    graceful: Option<Gate>,
    failed: Arc<AtomicBool>,
    outcomes: Arc<Mutex<Vec<PeerOutcome>>>,
    before_admission: Arc<Mutex<Option<Arc<DropGate>>>>,
    wire_trace: Option<H2WireTrace>,
    safe_wire_diagnostics: bool,
}

enum PeerOutcome {
    Finished(Result<(), hyper::Error>),
    Idle {
        result: Result<(), hyper::Error>,
        trace: H2WireTrace,
    },
    Cancelled,
}

impl PeerExecutor {
    fn close(&self) -> JoinSet<()> {
        self.tasks.lock().unwrap().take().unwrap_or_default()
    }
}

impl<F> hyper::rt::Executor<F> for PeerExecutor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, future: F) {
        let before = self.before_admission.lock().unwrap().clone();
        if let Some(before) = before {
            before.block();
        }
        let stop = self.stop.clone();
        let failed = Arc::clone(&self.failed);
        let task = async move {
            let outcome = AssertUnwindSafe(async move {
                tokio::select! { _ = stop.wait() => {}, _ = future => {} }
            })
            .catch_unwind()
            .await;
            if outcome.is_err() {
                failed.store(true, Ordering::SeqCst);
            }
        };
        let mut tasks = self.tasks.lock().unwrap();
        if let Some(tasks) = tasks.as_mut() {
            tasks.spawn(task);
        } else {
            drop(tasks);
            assert!(crate::client::owner::dispose(task));
        }
    }
}

pub(crate) fn full(bytes: impl Into<Bytes>) -> PeerBody {
    Full::new(bytes.into())
        .map_err(|never| match never {})
        .boxed_unsync()
}

pub(crate) async fn serve_peer<IoType>(
    io: IoType,
    protocol: CodecProtocol,
    handler: Handler,
    executor: PeerExecutor,
) where
    IoType: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let failed = Arc::clone(&executor.failed);
    let service = service_fn(move |request| {
        let future = handler(request);
        let failed = Arc::clone(&failed);
        async move {
            let response = match AssertUnwindSafe(future).catch_unwind().await {
                Ok(response) => response,
                Err(_) => {
                    failed.store(true, Ordering::SeqCst);
                    Response::builder()
                        .status(500)
                        .body(full("peer panic"))
                        .unwrap()
                }
            };
            Ok::<_, Infallible>(response)
        }
    });
    let stop = executor.stop.clone();
    let graceful = executor.graceful.clone();
    let mut graceful_started = false;
    let outcomes = Arc::clone(&executor.outcomes);
    let wire_trace = executor.wire_trace.clone();
    let safe_wire_diagnostics = executor.safe_wire_diagnostics;
    if protocol == CodecProtocol::Http2 {
        let connection = hyper::server::conn::http2::Builder::new(executor)
            .serve_connection(TokioIo::new(io), service);
        tokio::pin!(connection);
        let result = loop {
            tokio::select! {
                biased;
                _ = stop.wait() => break PeerOutcome::Cancelled,
                _ = async {
                    graceful.as_ref().unwrap().wait().await;
                }, if graceful.is_some() && !graceful_started => {
                    connection.as_mut().graceful_shutdown();
                    graceful_started = true;
                    if let Some(trace) = &wire_trace {
                        trace.marker(WireMarker::GracefulInvoked);
                    }
                }
                result = &mut connection => break PeerOutcome::Finished(result),
            }
        };
        if let Some(trace) = &wire_trace {
            let outcome = match &result {
                PeerOutcome::Finished(Ok(())) | PeerOutcome::Idle { result: Ok(()), .. } => {
                    WirePeerOutcome::Success
                }
                PeerOutcome::Finished(Err(_)) | PeerOutcome::Idle { result: Err(_), .. } => {
                    WirePeerOutcome::Error
                }
                PeerOutcome::Cancelled => WirePeerOutcome::Cancelled,
            };
            trace.marker(WireMarker::PeerOutcome(outcome));
        }
        let result = match (result, wire_trace.filter(H2WireTrace::idle)) {
            (PeerOutcome::Finished(result), Some(trace)) => PeerOutcome::Idle { result, trace },
            (result, _) => result,
        };
        let failure = {
            let mut retained_outcomes = outcomes.lock().unwrap();
            retained_outcomes.push(result);
            if graceful.is_some()
                && !safe_wire_diagnostics
                && retained_outcomes.len() <= 2
                && let Some(PeerOutcome::Finished(Err(error))) = retained_outcomes.last()
            {
                Some((stop.is_open(), format!("{error:?}")))
            } else {
                None
            }
        };
        if let Some((stop_open, error)) = failure {
            eprintln!(
                "graceful peer error: protocol={protocol:?} graceful_started={graceful_started} \
                 stop_open={stop_open} error={error}"
            );
        }
    } else {
        let connection =
            hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(io), service);
        tokio::pin!(connection);
        let result = loop {
            tokio::select! {
                biased;
                _ = stop.wait() => break PeerOutcome::Cancelled,
                _ = async {
                    graceful.as_ref().unwrap().wait().await;
                }, if graceful.is_some() && !graceful_started => {
                    connection.as_mut().graceful_shutdown();
                    graceful_started = true;
                }
                result = &mut connection => break PeerOutcome::Finished(result),
            }
        };
        let failure = {
            let mut retained_outcomes = outcomes.lock().unwrap();
            retained_outcomes.push(result);
            if graceful.is_some()
                && retained_outcomes.len() <= 2
                && let Some(PeerOutcome::Finished(Err(error))) = retained_outcomes.last()
            {
                Some((stop.is_open(), format!("{error:?}")))
            } else {
                None
            }
        };
        if let Some((stop_open, error)) = failure {
            eprintln!(
                "graceful peer error: protocol={protocol:?} graceful_started={graceful_started} \
                 stop_open={stop_open} error={error}"
            );
        }
    }
    assert!(
        graceful.is_none() || graceful_started,
        "each enabled peer must invoke Hyper graceful shutdown before retiring"
    );
}

pub(crate) struct TestConnector {
    pub calls: Arc<AtomicUsize>,
    pub events: Arc<Mutex<Vec<(&'static str, tokio::runtime::Id)>>>,
    app: tokio::runtime::Id,
    pub ready: Option<TestFuture<()>>,
    pub factory:
        Box<dyn FnMut(ConnectRequest) -> TestFuture<Result<ConnectedIo<Io>, BoxError>> + Send>,
    not_sync: Cell<usize>,
}

impl TestConnector {
    pub fn new(
        factory: impl FnMut(ConnectRequest) -> TestFuture<Result<ConnectedIo<Io>, BoxError>>
        + Send
        + 'static,
    ) -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
            events: Arc::new(Mutex::new(Vec::new())),
            app: Handle::current().id(),
            ready: None,
            factory: Box::new(factory),
            not_sync: Cell::new(0),
        }
    }
    fn record(&self, phase: &'static str) {
        let current = Handle::current().id();
        assert_eq!(current, self.app);
        self.events.lock().unwrap().push((phase, current));
    }

    pub fn take_factory(
        &mut self,
    ) -> Box<dyn FnMut(ConnectRequest) -> TestFuture<Result<ConnectedIo<Io>, BoxError>> + Send>
    {
        std::mem::replace(
            &mut self.factory,
            Box::new(|_| {
                Box::pin(async { Err(Box::new(OriginalError(Arc::new(()))) as BoxError) })
            }),
        )
    }
}

impl Service<ConnectRequest> for TestConnector {
    type Response = ConnectedIo<Io>;
    type Error = BoxError;
    type Future = TestFuture<Result<Self::Response, BoxError>>;
    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        self.record("ready");
        if let Some(ready) = &mut self.ready {
            if ready.as_mut().poll(context).is_pending() {
                return Poll::Pending;
            }
            self.ready = None;
        }
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: ConnectRequest) -> Self::Future {
        self.record("call");
        self.not_sync.set(self.not_sync.get() + 1);
        self.calls.fetch_add(1, Ordering::SeqCst);
        (self.factory)(request)
    }
}
impl Drop for TestConnector {
    fn drop(&mut self) {
        self.record("connector-drop");
    }
}

pub(crate) struct Fixture {
    pub worker: WorkerHandle,
    pub control: WorkerControl,
    pub runs: JoinSet<Result<(), ClientShutdownError>>,
    pub owners: Vec<ClientOwner>,
    pub peers: Arc<Mutex<JoinSet<()>>>,
    pub executor: PeerExecutor,
    sealed_peer_children: Option<JoinSet<()>>,
    pub stop: Gate,
    pub run_results: Vec<Result<(), ClientShutdownError>>,
    pub expected_run_error: Option<fn(&ClientError) -> bool>,
    pub expected_peer_error: Option<fn(&hyper::Error) -> bool>,
    expected_worker_panic: Option<&'static str>,
    forced_fallback: bool,
    wire_trace: Option<Arc<WireRegistry>>,
}

impl Fixture {
    pub fn enable_h2_wire_trace(&mut self) -> Arc<crate::client::owner::TestIoJoin> {
        assert!(self.wire_trace.is_none());
        let trace = Arc::new(WireRegistry::new());
        let io = Arc::clone(&trace.io);
        self.wire_trace = Some(trace);
        io
    }

    pub fn enable_h2_drain(
        &mut self,
        mode: H2DrainMode,
    ) -> (Arc<crate::client::owner::TestIoJoin>, H2DrainReceipt) {
        let io = self.enable_h2_wire_trace();
        let trace = H2WireTrace::with_drain(mode);
        Arc::get_mut(self.wire_trace.as_mut().unwrap())
            .unwrap()
            .drain = Some(trace.clone());
        (io, H2DrainReceipt(trace))
    }

    fn seal_peer_executor(&mut self) {
        if self.sealed_peer_children.is_none() {
            self.sealed_peer_children = Some(self.executor.close());
        }
    }

    async fn join_peer_executor(&mut self) -> bool {
        self.seal_peer_executor();
        let children = self
            .sealed_peer_children
            .as_mut()
            .expect("sealed executor children remain fixture-owned");
        let mut failed = false;
        while let Some(result) = children.join_next().await {
            if result.is_err() {
                failed = true;
                self.executor.failed.store(true, Ordering::SeqCst);
            }
        }
        failed
    }

    pub async fn cancel_and_join_pending_peer(&mut self) {
        self.control.pause().await;
        assert_eq!(self.peers.lock().unwrap().len(), 1);
        let before = std::future::poll_fn(|context| {
            Poll::Ready(self.peers.lock().unwrap().poll_join_next(context))
        })
        .await;
        if matches!(&before, Poll::Ready(Some(Err(_)))) {
            self.executor.failed.store(true, Ordering::SeqCst);
        }
        assert!(
            before.is_pending(),
            "the peer must still be running before cancellation: {before:?}"
        );
        self.seal_peer_executor();
        assert!(
            !self.sealed_peer_children.as_ref().unwrap().is_empty(),
            "the pending HTTP/2 request must retain actual executor children"
        );
        self.stop.open();
        let mut failed = false;
        while let Some(result) =
            std::future::poll_fn(|context| self.peers.lock().unwrap().poll_join_next(context)).await
        {
            if result.is_err() {
                failed = true;
                self.executor.failed.store(true, Ordering::SeqCst);
            }
        }
        failed |= self.join_peer_executor().await;
        let cancelled = matches!(
            self.executor.outcomes.lock().unwrap().as_slice(),
            [PeerOutcome::Cancelled]
        );
        assert!(!failed, "all peer parents must join without a task failure");
        assert!(
            !self.executor.failed.load(Ordering::SeqCst),
            "peer service/executor panic before native destruction"
        );
        assert!(self.executor.tasks.lock().unwrap().is_none());
        assert!(self.sealed_peer_children.as_ref().unwrap().is_empty());
        assert!(
            cancelled,
            "the joined peer must select cancellation before native destruction"
        );
    }

    pub fn enable_graceful_reuse_peers(&mut self) {
        let no_peers = self.peers.lock().unwrap().is_empty();
        assert!(no_peers, "enable graceful shutdown before creating peers");
        assert!(self.owners.is_empty());
        assert!(self.executor.graceful.is_none());
        self.executor.graceful = Some(Gate::new());
    }

    pub async fn gracefully_join_reuse_peers(&mut self) {
        let retirement = async {
            let graceful = self
                .executor
                .graceful
                .clone()
                .expect("graceful shutdown enabled before both original connectors");
            assert!(!graceful.is_open(), "graceful shutdown must not have begun");
            assert!(
                !self.stop.is_open(),
                "peer cancellation must not have begun"
            );
            let peer_count = self.peers.lock().unwrap().len();
            assert_eq!(
                peer_count, 2,
                "both original adopted duplex peers are owned"
            );
            let before = std::future::poll_fn(|context| {
                Poll::Ready(self.peers.lock().unwrap().poll_join_next(context))
            })
            .await;
            if matches!(&before, Poll::Ready(Some(Err(_)))) {
                self.executor.failed.store(true, Ordering::SeqCst);
            }
            assert!(
                before.is_pending(),
                "both original peers must still be running before graceful shutdown: {before:?}"
            );
            let no_outcomes = self.executor.outcomes.lock().unwrap().is_empty();
            assert!(
                no_outcomes,
                "no peer terminal may precede graceful shutdown"
            );
            if let Some(trace) = &self.wire_trace {
                trace.marker(WireMarker::GracefulRequested);
            }
            graceful.open();
            let mut failed = false;
            while let Some(result) =
                std::future::poll_fn(|context| self.peers.lock().unwrap().poll_join_next(context))
                    .await
            {
                if result.is_err() {
                    failed = true;
                    self.executor.failed.store(true, Ordering::SeqCst);
                }
            }
            failed |= self.join_peer_executor().await;
            let finished = matches!(
                self.executor.outcomes.lock().unwrap().as_slice(),
                [PeerOutcome::Finished(Ok(())), PeerOutcome::Finished(Ok(()))]
            );
            let parents_joined = self.peers.lock().unwrap().is_empty();
            let executor_sealed = self.executor.tasks.lock().unwrap().is_none();
            assert!(!failed, "both peer parents and executor children must join");
            assert!(
                !self.executor.failed.load(Ordering::SeqCst),
                "peer service/executor panic before adopted client drain"
            );
            assert!(
                parents_joined,
                "no original peer parent may remain unjoined"
            );
            assert!(
                executor_sealed,
                "peer executor admission must remain closed"
            );
            assert!(self.sealed_peer_children.as_ref().unwrap().is_empty());
            assert!(
                finished,
                "both original peers must finish successfully before adopted client drain"
            );
            assert!(
                !self.stop.is_open(),
                "healthy graceful retirement must not use cancellation fallback"
            );
        };
        tokio::time::timeout(Duration::from_secs(3), retirement)
            .await
            .expect("bounded graceful shutdown must join both original adopted duplex peers");
    }

    pub async fn force_drop_live_session(&mut self) {
        self.control.pause().await;
        self.expected_worker_panic = Some("session streams must be detached");
        // A peer poll already past its `stop` check can still write to or read
        // from the connection the forced drop aborts.
        self.expected_peer_error = Some(peer_saw_forced_close);
        self.stop.open();
        self.control.force_drop().await;
        assert_eq!(
            self.control.forced.load(Ordering::SeqCst),
            1,
            "the paused worker must enter the actual forced-drop action"
        );
    }

    pub async fn join_idle_peer_close(&mut self, receipt: &H2DrainReceipt) {
        let retirement = async {
            let graceful = self.executor.graceful.clone().unwrap();
            assert!(!graceful.is_open());
            assert!(!self.stop.is_open());
            assert!(self.expected_peer_error.is_none());
            assert!(self.expected_run_error.is_none());
            assert_eq!(self.peers.lock().unwrap().len(), 2);
            assert!(self.executor.outcomes.lock().unwrap().is_empty());
            assert!(self.run_results.is_empty());
            self.wire_trace
                .as_ref()
                .unwrap()
                .marker(WireMarker::GracefulRequested);
            graceful.open();
            receipt.idle_boundary().await;
            let before_release = receipt.0.snapshot();
            assert!(before_release.markers[WireMarker::ClientShutdownRequested.index()].is_none());
            assert!(
                before_release.ends[WireEndpoint::Peer.index()]
                    .shutdown_called
                    .is_none()
            );
            assert!(
                before_release.ends[WireEndpoint::Peer.index()]
                    .first_error
                    .is_none()
            );
            assert!(!self.stop.is_open());
            receipt.release_idle();
            let mut failed = false;
            while let Some(result) =
                std::future::poll_fn(|context| self.peers.lock().unwrap().poll_join_next(context))
                    .await
            {
                if result.is_err() {
                    failed = true;
                    self.executor.failed.store(true, Ordering::SeqCst);
                }
            }
            failed |= self.join_peer_executor().await;
            assert!(
                !failed,
                "actual idle peer parents and executor children join"
            );
            assert!(!self.executor.failed.load(Ordering::SeqCst));
            assert!(self.peers.lock().unwrap().is_empty());
            assert!(self.executor.tasks.lock().unwrap().is_none());
            assert!(self.sealed_peer_children.as_ref().unwrap().is_empty());
            assert!(!self.stop.is_open());
            let outcomes = self.executor.outcomes.lock().unwrap();
            assert_eq!(outcomes.len(), 2);
            let mut healthy = 0;
            let mut rejected = 0;
            for outcome in outcomes.iter() {
                match outcome {
                    PeerOutcome::Finished(Ok(())) => healthy += 1,
                    PeerOutcome::Idle {
                        result: Err(error),
                        trace,
                    } => {
                        assert!(Arc::ptr_eq(&trace.0, &receipt.0.0));
                        assert_idle_write_error(trace, error);
                        rejected += 1;
                    }
                    _ => panic!("only the witnessed original idle Hyper write may fail"),
                }
            }
            assert_eq!((healthy, rejected), (1, 1));
            drop(outcomes);
            receipt
                .0
                .0
                .lock()
                .unwrap()
                .drain
                .as_mut()
                .unwrap()
                .validated = true;
        };
        tokio::time::timeout(Duration::from_secs(3), retirement)
            .await
            .expect("bounded idle peer rejection and actual joins");
    }

    pub fn connector(&self, protocol: CodecProtocol, handler: Handler) -> TestConnector {
        let peers = Arc::clone(&self.peers);
        let executor = self.executor.clone();
        let wire_trace = self.wire_trace.clone();
        TestConnector::new(move |request| {
            let (client, peer) = tokio::io::duplex(4096);
            let handler = Arc::clone(&handler);
            let mut executor = executor.clone();
            executor.safe_wire_diagnostics =
                protocol == CodecProtocol::Http2 && wire_trace.is_some();
            let trace = if protocol == CodecProtocol::Http2 {
                wire_trace
                    .as_ref()
                    .and_then(|registry| registry.connection())
            } else {
                None
            };
            let client: Io = if let Some(trace) = trace {
                executor.wire_trace = Some(trace.clone());
                peers.lock().unwrap().spawn(serve_peer(
                    WireIo::new(peer, trace.clone(), WireEndpoint::Peer),
                    protocol,
                    handler,
                    executor,
                ));
                Box::pin(WireIo::new(client, trace, WireEndpoint::Native))
            } else {
                peers
                    .lock()
                    .unwrap()
                    .spawn(serve_peer(peer, protocol, handler, executor));
                Box::pin(client)
            };
            Box::pin(async move {
                Ok(ConnectedIo::new(
                    request.route().clone(),
                    EstablishedIo::new(
                        client,
                        TransportEndpoint::Unknown,
                        TransportEndpoint::Unknown,
                        None,
                    ),
                    protocol,
                    TransportSecurity::Plaintext,
                ))
            })
        })
    }

    pub async fn start<C, IoType>(&mut self, connector: C, options: ClientOptions) -> Client
    where
        C: Service<ConnectRequest, Response = ConnectedIo<IoType>> + Send + 'static,
        C::Future: Send + 'static,
        C::Error: Into<BoxError>,
        IoType: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (startup, run) = Client::prepare(self.worker.clone(), connector, options).unwrap();
        self.runs.spawn(run);
        let (client, owner) = startup.start().await.unwrap();
        self.owners.push(owner);
        client
    }

    pub async fn close_clients(&mut self) {
        if let Some(trace) = &self.wire_trace {
            trace.marker(WireMarker::ClientShutdownRequested);
        }
        for owner in &self.owners {
            owner.shutdown();
        }
        self.control.resume().await;
        let drain = async {
            while let Some(result) = self.runs.join_next().await {
                if result.is_err() {
                    self.executor.failed.store(true, Ordering::SeqCst);
                }
                self.run_results
                    .push(result.unwrap_or(Err(ClientShutdownError(ClientError::Panicked))));
            }
        };
        if tokio::time::timeout(Duration::from_secs(3), drain)
            .await
            .is_err()
        {
            self.forced_fallback = true;
            self.control.force_drop().await;
            while let Some(result) = self.runs.join_next().await {
                if result.is_err() {
                    self.executor.failed.store(true, Ordering::SeqCst);
                }
                self.run_results
                    .push(result.unwrap_or(Err(ClientShutdownError(ClientError::Panicked))));
            }
        }
        if let Some(trace) = &self.wire_trace {
            trace.marker(WireMarker::ClientRunsReaped {
                successful: !self.forced_fallback && self.run_results.iter().all(Result::is_ok),
            });
            trace.marker(WireMarker::IoRetirementBranchObserved {
                reached: trace.io.retired.load(Ordering::Acquire),
            });
        }
    }

    async fn cleanup(&mut self) {
        self.close_clients().await;
        self.stop.open();
        let mut peers = std::mem::replace(&mut *self.peers.lock().unwrap(), JoinSet::new());
        let mut failed = false;
        while let Some(result) = peers.join_next().await {
            failed |= result.is_err();
        }
        failed |= self.join_peer_executor().await;
        assert!(
            !failed,
            "peer/executor failures are reported after all joins"
        );
        assert!(
            !self.executor.failed.load(Ordering::SeqCst),
            "peer service/executor panic"
        );
        assert!(
            !self.forced_fallback,
            "emergency cleanup reaped work but is not a passing shutdown"
        );
        let mut expected = false;
        for result in &self.run_results {
            if let Err(error) = result {
                assert!(
                    self.expected_run_error
                        .is_some_and(|accept| accept(&error.0)),
                    "unexpected inner run result: {result:?}"
                );
                expected = true;
            }
        }
        assert_eq!(
            expected,
            self.expected_run_error.is_some(),
            "expected inner shutdown outcome"
        );
        if let Some(registry) = &self.wire_trace
            && let Some(trace) = &registry.drain
        {
            assert!(
                registry.io.retired.load(Ordering::Acquire),
                "adopted I/O retirement branch reached after clean native runs"
            );
            let state = trace.snapshot();
            assert!(state.complete());
            let drain = state.drain.unwrap();
            assert!(drain.failure.is_none());
            if drain.mode == H2DrainMode::Healthy {
                assert!(drain.healthy());
            }
        }
        for result in self.executor.outcomes.lock().unwrap().iter() {
            if let PeerOutcome::Idle { result, trace } = result {
                assert!(
                    trace.snapshot().drain.unwrap().validated,
                    "the failing idle path must finish its drain checks and joins"
                );
                let error = result.as_ref().expect_err("retain actual Hyper error");
                assert_idle_write_error(trace, error);
            }
            if let PeerOutcome::Finished(Err(error)) = result {
                assert!(
                    self.expected_peer_error.is_some_and(|accept| accept(error)),
                    "unexpected peer connection result: {error:?}"
                );
            }
        }
    }
}

fn assert_idle_write_error(trace: &H2WireTrace, error: &hyper::Error) {
    let state = trace.snapshot();
    let drain = state.drain.unwrap();
    assert!(drain.idle_boundary() && drain.released && drain.rejected);
    assert!(
        state.complete(),
        "valid lossless metadata and both inner endpoints destroyed"
    );
    let cause = std::error::Error::source(error)
        .and_then(|cause| cause.downcast_ref::<io::Error>())
        .expect("Hyper retains the actual h2 I/O cause");
    assert_eq!(
        WireError::from_io(cause),
        WireError {
            kind: io::ErrorKind::BrokenPipe,
            os_code: None
        }
    );
    assert_eq!(
        state.ends[WireEndpoint::Peer.index()].parser.offset,
        drain.initial_end.unwrap()
    );
}

pub(crate) fn options() -> ClientOptions {
    ClientOptions {
        maximum_exchanges: 4,
        command_capacity: 2,
        maximum_physical_connections: 2,
        maximum_connector_tasks: 2,
        maximum_routes: 8,
        pool: roxygen_client::pool::AttachedPoolParams {
            max_connections: 2,
            max_connection_attempts: 1,
            max_age: Duration::ZERO,
            connect_timeout: Duration::from_secs(2),
            ..Default::default()
        },
        ..Default::default()
    }
}

pub(crate) fn route(protocol: CodecProtocol) -> RouteSpec {
    RouteSpec::direct(
        "http://example.test".parse::<Origin>().unwrap(),
        if protocol == CodecProtocol::Http2 {
            ProtocolPolicy::Http2Only
        } else {
            ProtocolPolicy::Http1Only
        },
    )
}

pub(crate) fn case(
    test: impl for<'fixture> FnOnce(
        &'fixture mut Fixture,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'fixture>>,
) {
    let mut worker = TestWorker::new();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let result = runtime.block_on(async {
        let stop = Gate::new();
        let mut fixture = Fixture {
            worker: worker.handle.clone(),
            control: worker.control.clone(),
            runs: JoinSet::new(),
            owners: Vec::new(),
            peers: Arc::new(Mutex::new(JoinSet::new())),
            executor: PeerExecutor {
                tasks: Arc::new(Mutex::new(Some(JoinSet::new()))),
                stop: stop.clone(),
                graceful: None,
                failed: Arc::new(AtomicBool::new(false)),
                outcomes: Arc::new(Mutex::new(Vec::new())),
                before_admission: Arc::new(Mutex::new(None)),
                wire_trace: None,
                safe_wire_diagnostics: false,
            },
            sealed_peer_children: None,
            stop,
            run_results: Vec::new(),
            expected_run_error: None,
            expected_peer_error: None,
            expected_worker_panic: None,
            forced_fallback: false,
            wire_trace: None,
        };
        let result = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(15),
            test(&mut fixture),
        ))
        .catch_unwind()
        .await;
        let cleanup = AssertUnwindSafe(fixture.cleanup()).catch_unwind().await;
        (
            result,
            cleanup,
            fixture.expected_worker_panic,
            fixture.wire_trace.clone(),
        )
    });
    let worker_join = worker.join(result.2);
    drop(worker);
    drop(runtime);
    if let Some(trace) = result.3 {
        trace.marker(WireMarker::WorkerJoined {
            successful: worker_join.is_ok(),
        });
        trace.emit_safely();
    }
    match result.0 {
        Ok(Ok(())) => {}
        Ok(Err(_)) => panic!("bounded client case timed out"),
        Err(payload) => std::panic::resume_unwind(payload),
    }
    if let Err(payload) = result.1 {
        std::panic::resume_unwind(payload);
    }
    worker_join.expect("actual worker parent join matches the declared phase");
}

pub(crate) struct TestBody {
    frames: RefCell<VecDeque<Frame<Bytes>>>,
    gate: RefCell<Option<TestFuture<()>>>,
    pub error: RefCell<Option<BoxError>>,
    app: tokio::runtime::Id,
    pub drops: Arc<AtomicUsize>,
    pub polls: Arc<AtomicUsize>,
    hint: SizeHint,
    ended: Cell<bool>,
    pub panic_drop: bool,
    pub drop_gate: Option<Arc<DropGate>>,
    _pin: PhantomPinned,
}

impl TestBody {
    pub fn new(frames: Vec<Frame<Bytes>>) -> Self {
        Self {
            frames: RefCell::new(frames.into()),
            gate: RefCell::new(None),
            error: RefCell::new(None),
            app: Handle::current().id(),
            drops: Arc::new(AtomicUsize::new(0)),
            polls: Arc::new(AtomicUsize::new(0)),
            hint: SizeHint::new(),
            ended: Cell::new(false),
            panic_drop: false,
            drop_gate: None,
            _pin: PhantomPinned,
        }
    }
    pub fn gated(mut self, gate: Gate) -> Self {
        self.gate = RefCell::new(Some(Box::pin(async move { gate.wait().await })));
        self
    }
    pub fn zero_hint(mut self) -> Self {
        self.hint = SizeHint::with_exact(0);
        self
    }
}

impl Body for TestBody {
    type Data = Bytes;
    type Error = BoxError;
    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        assert_eq!(Handle::current().id(), self.app);
        self.polls.fetch_add(1, Ordering::SeqCst);
        if let Some(frame) = self.frames.borrow_mut().pop_front() {
            return Poll::Ready(Some(Ok(frame)));
        }
        if let Some(gate) = &mut *self.gate.borrow_mut() {
            if gate.as_mut().poll(context).is_pending() {
                return Poll::Pending;
            }
        }
        self.gate.borrow_mut().take();
        if let Some(error) = self.error.borrow_mut().take() {
            return Poll::Ready(Some(Err(error)));
        }
        self.ended.set(true);
        Poll::Ready(None)
    }
    fn is_end_stream(&self) -> bool {
        assert_eq!(Handle::current().id(), self.app);
        self.ended.get()
    }
    fn size_hint(&self) -> SizeHint {
        assert_eq!(Handle::current().id(), self.app);
        self.hint
    }
}
impl Drop for TestBody {
    fn drop(&mut self) {
        assert_eq!(Handle::current().id(), self.app);
        self.drops.fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = &self.drop_gate {
            gate.block();
        }
        assert!(!self.panic_drop, "deliberate app body destructor panic");
    }
}

#[test]
fn peer_executor_close_seals_nested_admission() {
    case(|fixture| {
        Box::pin(async move {
            use hyper::rt::Executor;
            struct Rejected {
                dropped: Arc<AtomicUsize>,
                polls: Arc<AtomicUsize>,
            }
            impl Future for Rejected {
                type Output = ();
                fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
                    self.polls.fetch_add(1, Ordering::SeqCst);
                    Poll::Pending
                }
            }
            impl Drop for Rejected {
                fn drop(&mut self) {
                    self.dropped.fetch_add(1, Ordering::SeqCst);
                }
            }
            let gate = DropGate::new();
            let release = ReleaseGate(Arc::clone(&gate));
            let launch = Gate::new();
            let start = launch.clone();
            let executor = fixture.executor.clone();
            let dropped = Arc::new(AtomicUsize::new(0));
            let polls = Arc::new(AtomicUsize::new(0));
            let rejected = Rejected {
                dropped: Arc::clone(&dropped),
                polls: Arc::clone(&polls),
            };
            fixture.executor.execute(async move {
                start.wait().await;
                executor.execute(rejected);
            });
            *fixture.executor.before_admission.lock().unwrap() = Some(Arc::clone(&gate));
            launch.open();
            gate.entered.wait().await;
            let mut children = fixture.executor.close();
            let joined = Gate::new();
            let observed = joined.clone();
            fixture.peers.lock().unwrap().spawn(async move {
                while let Some(result) = children.join_next().await {
                    result.unwrap();
                }
                observed.open();
            });
            gate.release();
            joined.wait().await;
            assert_eq!(dropped.load(Ordering::SeqCst), 1);
            assert_eq!(polls.load(Ordering::SeqCst), 0);
            assert!(fixture.executor.tasks.lock().unwrap().is_none());
            drop(release);
        })
    });
}

pub(crate) async fn body_bytes(body: crate::client::ClientBody) -> Bytes {
    body.collect().await.unwrap().to_bytes()
}

pub(crate) struct ProbedIo {
    pub inner: Io,
    pub worker_claimed: Arc<AtomicBool>,
    pub shutdown: TestFuture<()>,
    pub shutdown_complete: bool,
    pub claimed: Gate,
    pub dropped: Arc<AtomicUsize>,
    pub worker: std::thread::ThreadId,
    pub app: tokio::runtime::Id,
}
impl AsyncRead for ProbedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        assert_eq!(std::thread::current().id(), self.worker);
        self.claimed.open();
        self.inner.as_mut().poll_read(context, buffer)
    }
}
impl AsyncWrite for ProbedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        assert_eq!(std::thread::current().id(), self.worker);
        self.claimed.open();
        self.inner.as_mut().poll_write(context, bytes)
    }
    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        assert_eq!(std::thread::current().id(), self.worker);
        self.inner.as_mut().poll_flush(context)
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        assert_eq!(std::thread::current().id(), self.worker);
        if !self.shutdown_complete {
            if self.shutdown.as_mut().poll(context).is_pending() {
                return Poll::Pending;
            }
            self.shutdown_complete = true;
        }
        self.inner.as_mut().poll_shutdown(context)
    }
}
impl Drop for ProbedIo {
    fn drop(&mut self) {
        if self.worker_claimed.load(Ordering::Acquire) {
            assert_eq!(std::thread::current().id(), self.worker);
        } else {
            assert_eq!(Handle::current().id(), self.app);
        }
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

pub(crate) fn probe_connector(
    connector: &mut TestConnector,
    worker: &WorkerHandle,
    shutdown: Gate,
    claimed: Gate,
    dropped: Arc<AtomicUsize>,
) {
    let mut factory = connector.take_factory();
    let worker = worker.owner_thread();
    let app = Handle::current().id();
    connector.factory = Box::new(move |request| {
        let worker_claimed = Arc::clone(&request.claimed);
        let future = factory(request);
        let shutdown = shutdown.clone();
        let claimed = claimed.clone();
        let dropped = Arc::clone(&dropped);
        Box::pin(async move {
            let (route, established, protocol, security, extensions) = future.await?.into_parts();
            let EstablishedIo {
                io,
                local_endpoint,
                peer_endpoint,
                peer_identity,
            } = established;
            let io: Io = Box::pin(ProbedIo {
                inner: io,
                worker_claimed,
                shutdown_complete: false,
                shutdown: Box::pin(async move { shutdown.wait().await }),
                claimed,
                dropped,
                worker,
                app,
            });
            Ok(ConnectedIo::new(
                route,
                EstablishedIo::new(io, local_endpoint, peer_endpoint, peer_identity),
                protocol,
                security,
            )
            .with_extensions(extensions))
        })
    });
}

#[test]
fn h1_streaming_upload_response_and_trailers() {
    case(|fixture| {
        Box::pin(async move {
            let (observed, mut observations) = mpsc::channel(1);
            let handler: Handler = Arc::new(move |request| {
                let observed = observed.clone();
                Box::pin(async move {
                    assert_eq!(request.headers()[http::header::TE], "trailers");
                    let collected = request.into_body().collect().await.unwrap();
                    assert_eq!(collected.trailers().unwrap()["x-upload"], "finished");
                    assert_eq!(collected.to_bytes(), "firstsecond");
                    observed.send(()).await.unwrap();
                    let mut trailers = http::HeaderMap::new();
                    trailers.insert("x-reply", "finished".parse().unwrap());
                    let frames = futures::stream::iter(vec![
                        Ok::<_, BoxError>(Frame::data(Bytes::from_static(b"reply"))),
                        Ok(Frame::trailers(trailers)),
                    ]);
                    Response::builder()
                        .header("trailer", "x-reply")
                        .body(http_body_util::StreamBody::new(frames).boxed_unsync())
                        .unwrap()
                })
            });
            let connector = fixture.connector(CodecProtocol::Http11, handler);
            let application = Arc::new(crate::client::owner::TestApplicationJoin::default());
            let mut settings = options();
            settings.test_application = Some(Arc::clone(&application));
            let client = fixture.start(connector, settings).await;
            let route = client.route(route(CodecProtocol::Http11)).unwrap();
            let mut trailers = http::HeaderMap::new();
            trailers.insert("x-upload", "finished".parse().unwrap());
            let body = TestBody::new(vec![
                Frame::data(Bytes::from_static(b"first")),
                Frame::data(Bytes::from_static(b"second")),
                Frame::trailers(trailers),
            ]);
            let drops = Arc::clone(&body.drops);
            let polls = Arc::clone(&body.polls);
            let request = Request::builder()
                .method("POST")
                .uri("/upload")
                .header("trailer", "x-upload")
                .header(http::header::TE, "trailers")
                .body(body)
                .unwrap();
            let future = client.try_request(&route, request).unwrap();
            let exchange = future.exchange();
            let response = future.await.unwrap_or_else(|error| {
                panic!("H1 response failed: {error:?}; upload polls={}, drops={}, driver_reaped={}, driver_outcome={:?}",
                    polls.load(Ordering::SeqCst), drops.load(Ordering::SeqCst), application.reaped.load(Ordering::Acquire),
                    *application.outcome.lock().unwrap());
            });
            let collected = response.into_body().collect().await.unwrap();
            assert_eq!(collected.trailers().unwrap()["x-reply"], "finished");
            assert_eq!(collected.to_bytes(), "reply");
            observations.recv().await.unwrap();
            exchange.wait().await.unwrap();
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        })
    });
}

#[test]
fn h2_streaming_response_before_upload_end() {
    case(|fixture| {
        Box::pin(async move {
            let release = Gate::new();
            let executor = fixture.executor.clone();
            let handler: Handler = Arc::new(move |request| {
                hyper::rt::Executor::execute(&executor, async move {
                    let _ = request.into_body().collect().await;
                });
                Box::pin(async { Response::new(full("early")) })
            });
            let connector = fixture.connector(CodecProtocol::Http2, handler);
            let client = fixture.start(connector, options()).await;
            let route = client.route(route(CodecProtocol::Http2)).unwrap();
            let body = TestBody::new(vec![Frame::data(Bytes::from_static(b"prefix"))])
                .gated(release.clone());
            let future = client
                .try_request(
                    &route,
                    Request::builder()
                        .method("POST")
                        .uri("/")
                        .body(body)
                        .unwrap(),
                )
                .unwrap();
            let exchange = future.exchange();
            assert_eq!(body_bytes(future.await.unwrap().into_body()).await, "early");
            assert!(!release.is_open());
            release.open();
            exchange.wait().await.unwrap();
        })
    });
}

#[test]
fn non_sync_non_unpin_body_is_application_owned() {
    case(|fixture| {
        Box::pin(async move {
            let handler: Handler = Arc::new(|request| {
                Box::pin(async move {
                    assert_eq!(
                        request.into_body().collect().await.unwrap().to_bytes(),
                        "bytes"
                    );
                    Response::new(full("ok"))
                })
            });
            let connector = fixture.connector(CodecProtocol::Http11, handler);
            let client = fixture.start(connector, options()).await;
            let route = client.route(route(CodecProtocol::Http11)).unwrap();
            let body = TestBody::new(vec![Frame::data(Bytes::from_static(b"bytes"))]);
            let drops = Arc::clone(&body.drops);
            let polls = Arc::clone(&body.polls);
            let future = client
                .try_request(
                    &route,
                    Request::builder()
                        .method("POST")
                        .uri("/")
                        .body(body)
                        .unwrap(),
                )
                .unwrap();
            let exchange = future.exchange();
            assert_eq!(body_bytes(future.await.unwrap().into_body()).await, "ok");
            exchange.wait().await.unwrap();
            assert!(polls.load(Ordering::SeqCst) > 0);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        })
    });
}

#[test]
fn response_projection_preserves_headers_and_connection_extras() {
    case(|fixture| {
        Box::pin(async move {
            let handler: Handler = Arc::new(|_request| {
                Box::pin(async {
                    Response::builder()
                        .header("set-cookie", "first=1")
                        .header("set-cookie", "second=2")
                        .body(full("ok"))
                        .unwrap()
                })
            });
            let mut connector = fixture.connector(CodecProtocol::Http11, handler);
            let mut factory = connector.take_factory();
            let extra = Arc::new(123_u64);
            let expected = Arc::clone(&extra);
            connector.factory = Box::new(move |request| {
                let future = factory(request);
                let extra = Arc::clone(&extra);
                Box::pin(async move {
                    let mut extensions = http::Extensions::new();
                    extensions.insert(extra);
                    Ok(future.await?.with_extensions(extensions))
                })
            });
            let client = fixture.start(connector, options()).await;
            let route = client.route(route(CodecProtocol::Http11)).unwrap();
            let response = client
                .try_request(
                    &route,
                    Request::builder()
                        .uri("/")
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
                .unwrap()
                .await
                .unwrap();
            assert!(Arc::ptr_eq(
                response.extensions().get::<Arc<u64>>().unwrap(),
                &expected
            ));
            let cookies: Vec<_> = response
                .headers()
                .get_all("set-cookie")
                .iter()
                .map(|value| value.to_str().unwrap())
                .collect();
            assert_eq!(cookies, ["first=1", "second=2"]);
            let exchange = response.body().exchange();
            body_bytes(response.into_body()).await;
            exchange.wait().await.unwrap();
        })
    });
}

#[test]
fn header_eom_and_zero_hint_have_distinct_source_lifetimes() {
    case(|fixture| {
        Box::pin(async move {
            let handler: Handler = Arc::new(|request| {
                Box::pin(async move {
                    let path = request.uri().path().to_owned();
                    let request_body = request.into_body().collect().await.unwrap();
                    assert!(request_body.trailers().is_none());
                    assert!(request_body.to_bytes().is_empty());
                    if path == "/empty" {
                        return Response::builder()
                            .status(204)
                            .body(full(Bytes::new()))
                            .unwrap();
                    }
                    let mut trailers = http::HeaderMap::new();
                    trailers.insert("x-end", "present".parse().unwrap());
                    Response::new(
                        http_body_util::StreamBody::new(futures::stream::iter([
                            Ok::<_, BoxError>(Frame::trailers(trailers)),
                        ]))
                        .boxed_unsync(),
                    )
                })
            });
            let connector = fixture.connector(CodecProtocol::Http2, handler);
            let client = fixture.start(connector, options()).await;
            let route = client.route(route(CodecProtocol::Http2)).unwrap();
            let response = client
                .try_request(
                    &route,
                    Request::builder()
                        .uri("/empty")
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
                .unwrap()
                .await
                .unwrap();
            assert!(response.body().is_end_stream());
            let empty = response.body().exchange();
            drop(response);
            empty.wait().await.unwrap();
            let body = TestBody::new(vec![]).zero_hint();
            assert_eq!(body.size_hint().exact(), Some(0));
            assert!(!body.is_end_stream());
            let polls = Arc::clone(&body.polls);
            let drops = Arc::clone(&body.drops);
            let response = client
                .try_request(
                    &route,
                    Request::builder().uri("/trailers").body(body).unwrap(),
                )
                .unwrap()
                .await
                .unwrap();
            let exchange = response.body().exchange();
            let collected = response.into_body().collect().await.unwrap();
            assert_eq!(collected.trailers().unwrap()["x-end"], "present");
            exchange.wait().await.unwrap();
            assert_eq!(polls.load(Ordering::SeqCst), 1);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        })
    });
}

#[derive(Debug)]
pub(crate) struct OriginalError(pub Arc<()>);
impl std::fmt::Display for OriginalError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("original test cause")
    }
}
impl std::error::Error for OriginalError {}

#[test]
fn original_upload_error_remains_observable_after_response_headers() {
    case(|fixture| {
        Box::pin(async move {
            let executor = fixture.executor.clone();
            let handler: Handler = Arc::new(move |request| {
                hyper::rt::Executor::execute(&executor, async move {
                    let _ = request.into_body().collect().await;
                });
                Box::pin(async { Response::new(full("early")) })
            });
            let connector = fixture.connector(CodecProtocol::Http2, handler);
            let client = fixture.start(connector, options()).await;
            let route = client.route(route(CodecProtocol::Http2)).unwrap();
            let gate = Gate::new();
            let identity = Arc::new(());
            let body =
                TestBody::new(vec![Frame::data(Bytes::from_static(b"start"))]).gated(gate.clone());
            *body.error.borrow_mut() = Some(Box::new(OriginalError(Arc::clone(&identity))));
            let future = client
                .try_request(
                    &route,
                    Request::builder()
                        .method("POST")
                        .uri("/")
                        .body(body)
                        .unwrap(),
                )
                .unwrap();
            let exchange = future.exchange();
            let response = future.await.unwrap();
            gate.open();
            body_bytes(response.into_body()).await;
            let error = exchange
                .wait()
                .await
                .expect_err("original upload failure survives headers");
            let ClientError::Application(cause) = error.cause() else {
                panic!("expected application cause: {error:?}")
            };
            let original = cause
                .external_cause()
                .unwrap()
                .downcast_ref::<OriginalError>()
                .unwrap();
            assert!(Arc::ptr_eq(&original.0, &identity));
        })
    });
}
