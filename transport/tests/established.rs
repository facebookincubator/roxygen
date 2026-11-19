/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::error::Error;
use std::future::Future;
use std::future::poll_fn;
use std::io;
use std::io::IoSlice;
use std::marker::PhantomPinned;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Wake;
use std::task::Waker;
use std::thread::ThreadId;
use std::time::Duration;

use roxygen_core::buf_queue::BufQueue;
use roxygen_core::byte_event::ByteEventType;
use roxygen_core::byte_event::HttpByteEvent;
use roxygen_core::byte_event::HttpByteEventCallback;
use roxygen_core::byte_event::HttpByteEventRegistration;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::message::SocketAddress;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::driver::WorkerDriver;
use roxygen_runtime::managed::RuntimeConfig;
use roxygen_runtime::managed::RuntimeOwner;
use roxygen_runtime::task_group::TaskGroup;
use roxygen_runtime::worker::SubmitError;
use roxygen_runtime::worker::WorkerCleanupObserver;
use roxygen_runtime::worker::WorkerCleanupRegistration;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerState;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::io::ReadBuf;
use tokio::io::duplex;
use tokio::runtime::Builder;
use tokio::sync::oneshot;
use tokio::task::LocalSet;
use tokio::time;

use crate::byte_event::ByteEventWriteFlags;
use crate::byte_stream::ByteEventCapability;
use crate::byte_stream::ByteEventRegistrationError;
use crate::byte_stream::ByteStreamLimits;
use crate::byte_stream::ByteStreamTransport;
use crate::byte_stream::LocalTransportFuture;
use crate::byte_stream::OwnedByteStream;
use crate::byte_stream::PeerIdentity;
use crate::byte_stream::TransportEndpoint;
use crate::byte_stream::TransportRead;
use crate::byte_stream::TransportWriteError;
use crate::byte_stream::WriteInfo;
use crate::established::AdoptedIoCompletion;
use crate::established::AdoptedIoCompletionError;
use crate::established::AdoptedIoControl;
use crate::established::AdoptedIoState;
use crate::established::AdoptedStream;
use crate::established::EstablishedIo;
use crate::established::IoDropPublisher;
use crate::established::adopt_local;
use crate::established::adopt_local_with_completion;
use crate::tcp::OwnedRead;
use crate::tcp::StreamAttachment;
use crate::tcp::TokioByteStream;
use crate::tcp::owned_from_stream;
use crate::tcp::owned_from_stream_with_completion;

fn on_worker<Factory, Work>(factory: Factory)
where
    Factory: FnOnce(WorkerContext) -> Work + Send + 'static,
    Work: Future<Output = ()> + 'static,
{
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
        .block_on(LocalSet::new().run_until(async move {
            let (worker, driver) = WorkerDriver::new(32).expect("test worker");
            let work = worker.call_local(factory).expect("admit test operation");
            let driver_task = tokio::task::spawn_local(driver);
            time::timeout(Duration::from_secs(5), work)
                .await
                .expect("bounded test operation")
                .expect("worker operation succeeds");
            assert!(worker.begin_shutdown());
            time::timeout(Duration::from_secs(5), driver_task)
                .await
                .expect("bounded worker cleanup")
                .expect("worker driver joins");
            assert_eq!(worker.state(), WorkerState::Closed);
        }));
}

fn limits() -> ByteStreamLimits {
    ByteStreamLimits::new(4, 64, 4).expect("nonzero limits")
}

fn queue(segments: &[&[u8]]) -> BufQueue {
    let mut data = BufQueue::new(segments.iter().map(|segment| segment.len()).sum());
    for segment in segments {
        data.push_bytes(segment.to_vec().into())
            .expect("bounded segment");
    }
    data
}

fn read_bytes(read: TransportRead) -> Vec<u8> {
    read.into_data()
        .into_bytes_segments()
        .into_iter()
        .flat_map(|segment| segment.to_vec())
        .collect()
}

fn poll_once<Work: Future + ?Sized>(work: Pin<&mut Work>) -> Poll<Work::Output> {
    work.poll(&mut Context::from_waker(Waker::noop()))
}

fn poll_with_waker<Work: Future + ?Sized>(
    work: Pin<&mut Work>,
    waker: &Waker,
) -> Poll<Work::Output> {
    work.poll(&mut Context::from_waker(waker))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Action {
    Read,
    Write,
    FlushPending,
    Flushed,
    ShutdownPending,
    Shutdown,
    Dropped,
}

struct ProbeState {
    owner: ThreadId,
    inbound: VecDeque<u8>,
    read_cause: Option<io::Error>,
    read_eof: bool,
    accepted: Vec<u8>,
    buffered: Vec<u8>,
    flushed: Vec<u8>,
    write_allowance: usize,
    write_error: Option<io::ErrorKind>,
    write_cause: Option<io::Error>,
    write_zero: bool,
    flush_ready: bool,
    flush_error: Option<io::ErrorKind>,
    flush_cause: Option<io::Error>,
    shutdown_ready: bool,
    shutdown_error: Option<io::ErrorKind>,
    shutdown_cause: Option<io::Error>,
    read_waker: Option<Waker>,
    write_waker: Option<Waker>,
    flush_waker: Option<Waker>,
    shutdown_waker: Option<Waker>,
    observer: Option<Waker>,
    actions: Vec<(Action, ThreadId)>,
}

impl ProbeState {
    fn record(&mut self, action: Action) {
        let current = std::thread::current().id();
        assert_eq!(self.owner, current, "I/O remains on its owner");
        self.actions.push((action, current));
        if let Some(waker) = self.observer.take() {
            waker.wake();
        }
    }
}

#[derive(Clone)]
struct Probe(Rc<RefCell<ProbeState>>);

impl Probe {
    fn adopt_with_completion(
        &self,
        context: &WorkerContext,
    ) -> (OwnedByteStream, AdoptedIoControl) {
        adopt_local_with_completion(
            context,
            EstablishedIo::new(
                ProbeIo {
                    probe: self.clone(),
                    _pinned: PhantomPinned,
                },
                TransportEndpoint::UnnamedUnix,
                TransportEndpoint::Unknown,
                None,
            ),
            limits(),
        )
        .expect("completion-aware local !Send, !Unpin I/O")
    }

    fn new() -> Self {
        Self(Rc::new(RefCell::new(ProbeState {
            owner: std::thread::current().id(),
            inbound: VecDeque::new(),
            read_cause: None,
            read_eof: false,
            accepted: Vec::new(),
            buffered: Vec::new(),
            flushed: Vec::new(),
            write_allowance: usize::MAX,
            write_error: None,
            write_cause: None,
            write_zero: false,
            flush_ready: false,
            flush_error: None,
            flush_cause: None,
            shutdown_ready: false,
            shutdown_error: None,
            shutdown_cause: None,
            read_waker: None,
            write_waker: None,
            flush_waker: None,
            shutdown_waker: None,
            observer: None,
            actions: Vec::new(),
        })))
    }

    fn adopt(&self, context: &WorkerContext) -> OwnedByteStream {
        adopt_local(
            context,
            EstablishedIo::new(
                ProbeIo {
                    probe: self.clone(),
                    _pinned: PhantomPinned,
                },
                TransportEndpoint::UnnamedUnix,
                TransportEndpoint::UnixPath(b"/local/proxy.sock".to_vec().into_boxed_slice()),
                Some(PeerIdentity::new(b"verified-by-connector")),
            ),
            limits(),
        )
        .expect("adopt local !Send, !Unpin I/O")
    }

    fn allow_flush(&self) {
        let waker = {
            let mut state = self.0.borrow_mut();
            state.flush_ready = true;
            state.flush_waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn allow_shutdown(&self) {
        let waker = {
            let mut state = self.0.borrow_mut();
            state.shutdown_ready = true;
            state.shutdown_waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn saw(&self, action: Action) -> bool {
        self.0
            .borrow()
            .actions
            .iter()
            .any(|(seen, _)| *seen == action)
    }

    async fn wait_for(&self, action: Action) {
        poll_fn(|context| {
            if self.saw(action) {
                Poll::Ready(())
            } else {
                self.0.borrow_mut().observer = Some(context.waker().clone());
                Poll::Pending
            }
        })
        .await;
    }
}

struct ProbeIo {
    probe: Probe,
    _pinned: PhantomPinned,
}

impl AsyncRead for ProbeIo {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut state = self.probe.0.borrow_mut();
        state.record(Action::Read);
        if state.inbound.is_empty() {
            if let Some(error) = state.read_cause.take() {
                return Poll::Ready(Err(error));
            }
            if state.read_eof {
                return Poll::Ready(Ok(()));
            }
            state.read_waker = Some(context.waker().clone());
            return Poll::Pending;
        }
        let length = buffer.remaining().min(state.inbound.len());
        let bytes = state.inbound.drain(..length).collect::<Vec<_>>();
        buffer.put_slice(&bytes);
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ProbeIo {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut state = self.probe.0.borrow_mut();
        state.record(Action::Write);
        if state.write_allowance == 0 {
            if let Some(error) = state.write_cause.take() {
                return Poll::Ready(Err(error));
            }
            if state.write_zero {
                return Poll::Ready(Ok(0));
            }
            if let Some(kind) = state.write_error {
                return Poll::Ready(Err(kind.into()));
            }
            state.write_waker = Some(context.waker().clone());
            return Poll::Pending;
        }
        let length = bytes.len().min(2).min(state.write_allowance);
        state.write_allowance -= length;
        state.accepted.extend_from_slice(&bytes[..length]);
        state.buffered.extend_from_slice(&bytes[..length]);
        Poll::Ready(Ok(length))
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut state = self.probe.0.borrow_mut();
        if let Some(error) = state.flush_cause.take() {
            return Poll::Ready(Err(error));
        }
        if let Some(kind) = state.flush_error {
            return Poll::Ready(Err(kind.into()));
        }
        if !state.flush_ready {
            state.record(Action::FlushPending);
            state.flush_waker = Some(context.waker().clone());
            return Poll::Pending;
        }
        let buffered = std::mem::take(&mut state.buffered);
        state.flushed.extend_from_slice(&buffered);
        state.record(Action::Flushed);
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut state = self.probe.0.borrow_mut();
        assert!(state.buffered.is_empty(), "flush precedes shutdown");
        if let Some(error) = state.shutdown_cause.take() {
            state.record(Action::Shutdown);
            return Poll::Ready(Err(error));
        }
        if let Some(kind) = state.shutdown_error {
            return Poll::Ready(Err(kind.into()));
        }
        if !state.shutdown_ready {
            state.record(Action::ShutdownPending);
            state.shutdown_waker = Some(context.waker().clone());
            return Poll::Pending;
        }
        state.record(Action::Shutdown);
        Poll::Ready(Ok(()))
    }
}

impl Drop for ProbeIo {
    fn drop(&mut self) {
        let mut state = self.probe.0.borrow_mut();
        state.read_waker = None;
        state.write_waker = None;
        state.flush_waker = None;
        state.shutdown_waker = None;
        state.record(Action::Dropped);
    }
}

#[derive(Debug, thiserror::Error)]
#[error("original adopted I/O failure")]
struct AdoptedCause(Arc<()>);

fn cause_io(kind: io::ErrorKind, identity: &Arc<()>) -> io::Error {
    io::Error::new(kind, AdoptedCause(Arc::clone(identity)))
}

fn assert_adopted_cause(error: &HttpError, kind: io::ErrorKind, identity: &Arc<()>) {
    let original = error.source().unwrap().downcast_ref::<io::Error>().unwrap();
    assert_eq!(original.kind(), kind);
    let cause = original
        .get_ref()
        .unwrap()
        .downcast_ref::<AdoptedCause>()
        .unwrap();
    assert!(Arc::ptr_eq(&cause.0, identity));
}

#[test]
fn adopted_read_retains_original_io_cause() {
    for kind in [
        io::ErrorKind::TimedOut,
        io::ErrorKind::BrokenPipe,
        io::ErrorKind::UnexpectedEof,
    ] {
        on_worker(move |context| async move {
            let identity = Arc::new(());
            let probe = Probe::new();
            probe.0.borrow_mut().inbound.extend(b"abc");
            probe.0.borrow_mut().read_cause = Some(cause_io(kind, &identity));
            let (transport, control) = probe.adopt_with_completion(&context);
            let mut retired = Box::pin(control.completion().wait_retired());
            assert!(poll_once(retired.as_mut()).is_pending());
            let prefix = transport
                .read(&context, 4, 4, Duration::ZERO, CancellationToken::never())
                .await
                .unwrap();
            assert_eq!(read_bytes(prefix), b"abc");
            let error = transport
                .read(&context, 4, 4, Duration::ZERO, CancellationToken::never())
                .await
                .expect_err("actual read failure");
            assert_eq!(
                error.code(),
                if kind == io::ErrorKind::UnexpectedEof {
                    HttpErrorCode::TRANSPORT_EOF
                } else {
                    HttpErrorCode::TRANSPORT_READ_ERROR
                }
            );
            assert_eq!(error.message(), b"original adopted I/O failure");
            assert_adopted_cause(&error, kind, &identity);
            transport.close_with_reset(&context).unwrap();
            drop(transport);
            retired.await.unwrap();
            assert_adopted_cause(&error.clone(), kind, &identity);
            assert_eq!(
                probe
                    .0
                    .borrow()
                    .actions
                    .iter()
                    .filter(|(action, _)| *action == Action::Dropped)
                    .count(),
                1
            );
        });
    }
}

#[test]
fn adopted_write_retains_cause_and_progress() {
    for flush in [false, true] {
        on_worker(move |context| async move {
            let identity = Arc::new(());
            let probe = Probe::new();
            if flush {
                probe.0.borrow_mut().flush_cause =
                    Some(cause_io(io::ErrorKind::TimedOut, &identity));
            } else {
                probe.0.borrow_mut().write_allowance = 3;
                probe.0.borrow_mut().write_cause =
                    Some(cause_io(io::ErrorKind::TimedOut, &identity));
            }
            let (transport, control) = probe.adopt_with_completion(&context);
            let error = transport
                .write(
                    &context,
                    queue(&[b"abcd", b"efgh"]),
                    Duration::ZERO,
                    CancellationToken::never(),
                )
                .await
                .expect_err("actual write or flush failure");
            assert_eq!(error.code(), HttpErrorCode::TRANSPORT_WRITE_ERROR);
            assert_eq!(error.bytes_written(), if flush { 8 } else { 3 });
            assert_eq!(
                probe.0.borrow().accepted.as_slice(),
                if flush {
                    b"abcdefgh".as_slice()
                } else {
                    b"abc".as_slice()
                }
            );
            assert!(probe.0.borrow().flushed.is_empty());
            assert_adopted_cause(error.error(), io::ErrorKind::TimedOut, &identity);
            drop(transport);
            control.completion().wait_retired().await.unwrap();
            assert!(probe.saw(Action::Dropped));
        });
    }
    on_worker(|context| async move {
        let identity = Arc::new(());
        let probe = Probe::new();
        probe.0.borrow_mut().shutdown_cause = Some(cause_io(io::ErrorKind::BrokenPipe, &identity));
        let transport = probe.adopt(&context);
        transport.shutdown_write(&context).unwrap();
        probe.wait_for(Action::FlushPending).await;
        assert!(!probe.saw(Action::Shutdown));
        probe.allow_flush();
        probe.wait_for(Action::Shutdown).await;
        let error = transport
            .close(&context)
            .expect_err("cached original shutdown result");
        assert_eq!(error.code(), HttpErrorCode::TRANSPORT_WRITE_ERROR);
        assert_adopted_cause(&error, io::ErrorKind::BrokenPipe, &identity);
        let cloned = error.clone();
        assert!(std::ptr::eq(
            error.source().unwrap(),
            cloned.source().unwrap()
        ));
        drop(transport);
        assert_eq!(
            probe
                .0
                .borrow()
                .actions
                .iter()
                .filter(|(action, _)| *action == Action::Shutdown)
                .count(),
            1
        );
        assert_eq!(
            probe
                .0
                .borrow()
                .actions
                .iter()
                .filter(|(action, _)| *action == Action::Dropped)
                .count(),
            1
        );
    });
    on_worker(|context| async move {
        let identity = Arc::new(());
        let probe = Probe::new();
        probe.0.borrow_mut().shutdown_cause = Some(cause_io(io::ErrorKind::BrokenPipe, &identity));
        let (transport, control) = probe.adopt_with_completion(&context);
        transport.shutdown_write(&context).unwrap();
        probe.wait_for(Action::FlushPending).await;
        assert!(!probe.saw(Action::Shutdown));
        probe.allow_flush();
        probe.wait_for(Action::Shutdown).await;
        control.completion().wait_retired().await.unwrap();
        let error = transport
            .read(&context, 4, 4, Duration::ZERO, CancellationToken::never())
            .await
            .expect_err("failed completion-aware shutdown resets the public transport");
        assert_eq!(error.code(), HttpErrorCode::TRANSPORT_READ_ERROR);
        assert_eq!(error.message(), b"byte-stream was reset");
        assert!(error.source().is_none());
        transport
            .close(&context)
            .expect("close after terminal reset is idempotent");
        drop(transport);
        assert_eq!(
            probe
                .0
                .borrow()
                .actions
                .iter()
                .filter(|(action, _)| *action == Action::Shutdown)
                .count(),
            1
        );
        assert_eq!(
            probe
                .0
                .borrow()
                .actions
                .iter()
                .filter(|(action, _)| *action == Action::Dropped)
                .count(),
            1
        );
    });
}

#[test]
fn ordinary_eof_has_no_synthetic_io_cause() {
    on_worker(|context| async move {
        let probe = Probe::new();
        probe.0.borrow_mut().read_eof = true;
        let (transport, control) = probe.adopt_with_completion(&context);
        let read = transport
            .read(&context, 4, 4, Duration::ZERO, CancellationToken::never())
            .await
            .unwrap();
        assert!(read.is_eof());
        assert_eq!(read.bytes_read(), 0);
        assert!(
            HttpError::new(HttpErrorCode::TRANSPORT_EOF)
                .source()
                .is_none()
        );
        transport.close_with_reset(&context).unwrap();
        let synthetic = transport
            .read(&context, 4, 4, Duration::ZERO, CancellationToken::never())
            .await
            .expect_err("reset is a library diagnostic, not an I/O error");
        assert!(synthetic.source().is_none());
        drop(transport);
        control.completion().wait_retired().await.unwrap();
        assert!(probe.saw(Action::Dropped));
    });
}

#[test]
fn established_io_preserves_bytes() {
    on_worker(|context| async move {
        let (stream, mut peer) = duplex(4);
        let local = SocketAddress::new("127.0.0.1", 1234);
        let remote = SocketAddress::new("127.0.0.1", 4321);
        let identity = PeerIdentity::new(b"already-verified");
        let transport = adopt_local(
            &context,
            EstablishedIo::new(
                stream,
                TransportEndpoint::Socket(local.clone()),
                TransportEndpoint::Socket(remote.clone()),
                Some(identity.clone()),
            ),
            limits(),
        )
        .expect("duplex attachment");
        assert_eq!(transport.local_address(&context).unwrap(), local);
        assert_eq!(transport.peer_address(&context).unwrap(), remote);
        assert_eq!(transport.peer_identity(&context).unwrap(), Some(identity));
        let exchange = async {
            let mut received = Vec::new();
            loop {
                let read = transport
                    .read(&context, 64, 2, Duration::ZERO, CancellationToken::never())
                    .await
                    .expect("bounded read");
                assert!(read.bytes_read() <= 4);
                if read.is_eof() {
                    break;
                }
                received.extend(read_bytes(read));
            }
            assert_eq!(received, b"request-bytes");
            let written = transport
                .write(
                    &context,
                    queue(&[b"res", b"ponse", b"-xyz"]),
                    Duration::ZERO,
                    CancellationToken::never(),
                )
                .await
                .expect("segmented response write");
            assert_eq!(written.bytes_written, 12);
            transport
                .shutdown_write(&context)
                .expect("request half-close");
        };
        let peer_exchange = async {
            peer.write_all(b"request-bytes")
                .await
                .expect("peer request");
            peer.shutdown().await.expect("peer half-close");
            let mut response = Vec::new();
            peer.read_to_end(&mut response)
                .await
                .expect("peer response");
            assert_eq!(response, b"response-xyz");
        };
        tokio::join!(exchange, peer_exchange);
        drop(transport);
    });
}

#[test]
fn buffered_write_waits_for_flush_and_close_waits_for_shutdown() {
    on_worker(|context| async move {
        let probe = Probe::new();
        let transport = probe.adopt(&context);
        let mut write = transport.write(
            &context,
            queue(&[b"abc", b"def"]),
            Duration::ZERO,
            CancellationToken::never(),
        );
        assert!(poll_once(write.as_mut()).is_pending());
        assert_eq!(probe.0.borrow().accepted, b"abcdef");
        assert!(probe.0.borrow().flushed.is_empty());
        assert!(transport.has_pending_write(&context).unwrap());
        probe.0.borrow_mut().inbound.extend(b"read");
        let read = transport
            .read(&context, 4, 2, Duration::ZERO, CancellationToken::never())
            .await
            .expect("pending flush does not block ingress");
        assert_eq!(read_bytes(read), b"read");
        probe.allow_flush();
        assert_eq!(write.await.unwrap().bytes_written, 6);
        assert_eq!(probe.0.borrow().flushed, b"abcdef");
        transport.close(&context).expect("request close");
        probe.wait_for(Action::ShutdownPending).await;
        assert!(!probe.saw(Action::Dropped));
        drop(transport);
        assert!(!probe.saw(Action::Dropped));
        probe.allow_shutdown();
        probe.wait_for(Action::Dropped).await;
        let actions = &probe.0.borrow().actions;
        assert!(
            actions
                .iter()
                .all(|(_, owner)| *owner == std::thread::current().id())
        );
        assert_eq!(actions.last().unwrap().0, Action::Dropped);
        assert_eq!(actions[actions.len() - 2].0, Action::Shutdown);
    });
}

#[test]
fn half_close_flushes_preexisting_buffer_and_survives_handle_drop() {
    on_worker(|context| async move {
        let probe = Probe::new();
        probe.0.borrow_mut().buffered.extend(b"connector-buffer");
        let transport = probe.adopt(&context);
        transport
            .shutdown_write(&context)
            .expect("request half-close");
        probe.wait_for(Action::FlushPending).await;
        assert!(!probe.saw(Action::ShutdownPending));
        probe.allow_flush();
        probe.wait_for(Action::ShutdownPending).await;
        assert_eq!(probe.0.borrow().flushed, b"connector-buffer");
        drop(transport);
        assert!(!probe.saw(Action::Dropped));
        probe.allow_shutdown();
        probe.wait_for(Action::Dropped).await;
    });
}

#[test]
fn graceful_close_drains_pending_write_before_shutdown() {
    on_worker(|context| async move {
        let probe = Probe::new();
        let transport = probe.adopt(&context);
        let mut write = transport.write(
            &context,
            queue(&[b"pending"]),
            Duration::ZERO,
            CancellationToken::never(),
        );
        assert!(poll_once(write.as_mut()).is_pending());
        transport.close(&context).expect("drain active write");
        assert!(!probe.saw(Action::ShutdownPending));
        probe.allow_flush();
        assert!(poll_once(write.as_mut()).is_pending());
        assert_eq!(probe.0.borrow().flushed, b"pending");
        assert!(probe.saw(Action::ShutdownPending));
        assert!(!probe.saw(Action::Dropped));
        probe.allow_shutdown();
        assert_eq!(write.await.unwrap().bytes_written, 7);
        probe.wait_for(Action::Dropped).await;
        drop(transport);
    });
}

#[test]
fn graceful_shutdown_error_finishes_write_without_panicking() {
    on_worker(|context| async move {
        let probe = Probe::new();
        let transport = probe.adopt(&context);
        let mut write = transport.write(
            &context,
            queue(&[b"pending"]),
            Duration::ZERO,
            CancellationToken::never(),
        );
        assert!(poll_once(write.as_mut()).is_pending());
        transport
            .close(&context)
            .expect("request close during write");
        probe.0.borrow_mut().shutdown_error = Some(io::ErrorKind::BrokenPipe);
        probe.allow_flush();
        let error = write
            .await
            .expect_err("shutdown failure is a transport error");
        assert_eq!(error.code(), HttpErrorCode::TRANSPORT_WRITE_ERROR);
        assert_eq!(error.bytes_written(), 7);
        assert_eq!(probe.0.borrow().flushed, b"pending");
        assert!(probe.saw(Action::Dropped));
        drop(transport);
    });
}

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn cancelled_read_revokes_wake_observer_and_preserves_connection() {
    on_worker(|context| async move {
        let probe = Probe::new();
        let transport = probe.adopt(&context);
        let mut read = transport.read(&context, 4, 2, Duration::ZERO, CancellationToken::never());
        let counter = Arc::new(WakeCount::default());
        let waker = Waker::from(Arc::clone(&counter));
        assert!(
            read.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert!(Arc::strong_count(&counter) > 2);
        drop(read);
        assert_eq!(Arc::strong_count(&counter), 2);
        assert!(!transport.has_pending_read(&context).unwrap());
        let cancellation_wakes = counter.0.load(Ordering::Relaxed);
        probe.0.borrow().read_waker.as_ref().unwrap().wake_by_ref();
        assert_eq!(counter.0.load(Ordering::Relaxed), cancellation_wakes);
        assert!(!probe.saw(Action::Dropped));
        probe.0.borrow_mut().inbound.extend(b"next");
        let next = transport
            .read(&context, 4, 2, Duration::ZERO, CancellationToken::never())
            .await
            .expect("new read after cancellation");
        assert_eq!(read_bytes(next), b"next");
        drop(transport);
        assert!(probe.saw(Action::Dropped));
    });
}

#[test]
fn cancelled_writes_report_accepted_prefix_and_never_replay() {
    for (allowance, expected, abandon) in [(3, 3, false), (64, 8, false), (3, 3, true)] {
        on_worker(move |context| async move {
            let probe = Probe::new();
            probe.0.borrow_mut().write_allowance = allowance;
            let transport = probe.adopt(&context);
            let cancellation = CancellationSource::new();
            let mut write = transport.write(
                &context,
                queue(&[b"abcd", b"efgh"]),
                Duration::ZERO,
                cancellation.token(),
            );
            let mut read =
                transport.read(&context, 4, 2, Duration::ZERO, CancellationToken::never());
            assert!(poll_once(read.as_mut()).is_pending());
            assert!(poll_once(write.as_mut()).is_pending());
            if abandon {
                drop(write);
            } else {
                assert!(cancellation.request_cancellation());
                let error = write.await.expect_err("cancel write");
                assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                assert_eq!(error.bytes_written(), expected);
            }
            assert!(probe.saw(Action::Dropped));
            assert_eq!(probe.0.borrow().accepted, &b"abcdefgh"[..expected]);
            assert!(probe.0.borrow().flushed.is_empty());
            assert!(read.await.is_err());
            assert!(!transport.has_pending_read(&context).unwrap());
            assert!(!transport.has_pending_write(&context).unwrap());
            let error = transport
                .write(
                    &context,
                    queue(&[b"again"]),
                    Duration::ZERO,
                    CancellationToken::never(),
                )
                .await
                .expect_err("cancelled transport cannot replay or accept more bytes");
            assert_eq!(error.bytes_written(), 0);
            drop(transport);
            assert_eq!(
                probe
                    .0
                    .borrow()
                    .actions
                    .iter()
                    .filter(|(action, _)| *action == Action::Dropped)
                    .count(),
                1,
            );
        });
    }
}

#[test]
fn write_and_flush_errors_retain_partial_progress() {
    for flush_error in [false, true] {
        on_worker(move |context| async move {
            let probe = Probe::new();
            if flush_error {
                probe.0.borrow_mut().flush_error = Some(io::ErrorKind::BrokenPipe);
            } else {
                probe.0.borrow_mut().write_allowance = 3;
                probe.0.borrow_mut().write_error = Some(io::ErrorKind::BrokenPipe);
            }
            let transport = probe.adopt(&context);
            let error = transport
                .write(
                    &context,
                    queue(&[b"abcd", b"efgh"]),
                    Duration::ZERO,
                    CancellationToken::never(),
                )
                .await
                .expect_err("injected I/O failure");
            assert_eq!(error.code(), HttpErrorCode::TRANSPORT_WRITE_ERROR);
            assert_eq!(error.bytes_written(), if flush_error { 8 } else { 3 });
            assert!(probe.saw(Action::Dropped));
            assert!(probe.0.borrow().flushed.is_empty());
            drop(transport);
        });
    }
}

#[test]
fn write_timeout_during_flush_discards_buffer_without_replay() {
    on_worker(|context| async move {
        time::pause();
        let probe = Probe::new();
        let transport = probe.adopt(&context);
        let mut write = transport.write(
            &context,
            queue(&[b"buffered"]),
            Duration::from_millis(10),
            CancellationToken::never(),
        );
        assert!(poll_once(write.as_mut()).is_pending());
        time::advance(Duration::from_millis(20)).await;
        let error = write.await.expect_err("flush deadline");
        assert_eq!(error.code(), HttpErrorCode::WRITE_TIMEOUT);
        assert_eq!(error.bytes_written(), 8);
        assert!(probe.saw(Action::Dropped));
        assert!(probe.0.borrow().flushed.is_empty());
        drop(transport);
    });
}

#[derive(Default)]
struct EventLog {
    cancelled: RefCell<Vec<(ByteEventType, ThreadId)>>,
}

impl HttpByteEventCallback for EventLog {
    fn on_byte_event(&self, _event: HttpByteEvent) {
        panic!("generic I/O cannot report native byte events");
    }

    fn on_byte_event_cancelled(&self, event: HttpByteEvent, _error: HttpError) {
        self.cancelled
            .borrow_mut()
            .push((event.event_type, std::thread::current().id()));
    }
}

#[test]
fn generic_capabilities_and_unix_metadata_are_honest() {
    on_worker(|context| async move {
        let probe = Probe::new();
        let transport = probe.adopt(&context);
        assert_eq!(
            transport.local_endpoint(&context).unwrap(),
            TransportEndpoint::UnnamedUnix,
        );
        assert_eq!(
            transport.peer_endpoint(&context).unwrap(),
            TransportEndpoint::UnixPath(b"/local/proxy.sock".to_vec().into_boxed_slice()),
        );
        assert!(!transport.local_address(&context).unwrap().is_initialized());
        assert!(!transport.peer_address(&context).unwrap().is_initialized());
        assert_eq!(
            transport.peer_identity(&context).unwrap(),
            Some(PeerIdentity::new(b"verified-by-connector")),
        );
        assert!(!transport.enable_zero_copy(&context, 1).unwrap());
        assert_eq!(transport.zero_copy_owner_for_test(&context).unwrap(), None);
        assert_eq!(
            transport.byte_event_capability(&context).unwrap(),
            ByteEventCapability::Unsupported,
        );
        let events = Rc::new(EventLog::default());
        assert_eq!(
            transport.register_byte_event(
                &context,
                HttpByteEventRegistration::new(None, ByteEventType::CumulativeAck.bit(), &events),
            ),
            Err(ByteEventRegistrationError::Unsupported),
        );
        assert_eq!(
            events.cancelled.borrow().as_slice(),
            &[(ByteEventType::CumulativeAck, std::thread::current().id())],
        );
        let flags = ByteEventWriteFlags {
            timestamp_ack: true,
            ..ByteEventWriteFlags::default()
        };
        let error = transport
            .write_with_byte_events(
                &context,
                queue(&[b"no ACK"]),
                flags,
                Duration::ZERO,
                CancellationToken::never(),
            )
            .await
            .expect_err("no synthetic acknowledgement");
        assert_eq!(error.bytes_written(), 0);
        assert!(!probe.saw(Action::Write));
        transport
            .close_with_reset(&context)
            .expect("force close by dropping I/O");
        assert!(probe.saw(Action::Dropped));
        assert!(!probe.saw(Action::Shutdown));
        assert!(!probe.saw(Action::Flushed));
        drop(transport);
    });
}

struct EscapedOperations {
    transport: OwnedByteStream,
    read: LocalTransportFuture<Result<TransportRead, HttpError>>,
    write: LocalTransportFuture<Result<WriteInfo, TransportWriteError>>,
    probe: Probe,
}

thread_local! {
    static ESCAPED: RefCell<Option<EscapedOperations>> = const { RefCell::new(None) };
    static CLOSING: RefCell<Option<(OwnedByteStream, Probe)>> = const { RefCell::new(None) };
}

struct CompletionScopeDropPanic;

impl Drop for CompletionScopeDropPanic {
    fn drop(&mut self) {
        panic!("injected scope-capture destructor panic");
    }
}

#[test]
fn worker_shutdown_destroys_io_even_with_escaped_operations() {
    on_worker(|context| async move {
        let probe = Probe::new();
        let transport = probe.adopt(&context);
        let mut read = transport.read(&context, 4, 2, Duration::ZERO, CancellationToken::never());
        let mut write = transport.write(
            &context,
            queue(&[b"held"]),
            Duration::ZERO,
            CancellationToken::never(),
        );
        assert!(poll_once(read.as_mut()).is_pending());
        assert!(poll_once(write.as_mut()).is_pending());
        ESCAPED.with(|escaped| {
            assert!(
                escaped
                    .replace(Some(EscapedOperations {
                        transport,
                        read,
                        write,
                        probe,
                    }))
                    .is_none()
            );
        });
    });
    ESCAPED.with(|escaped| {
        let mut escaped = escaped.take().expect("escaped owner-local operations");
        assert!(escaped.probe.saw(Action::Dropped));
        assert!(
            matches!(poll_once(escaped.read.as_mut()), Poll::Ready(Err(error)) if error.code() == HttpErrorCode::DROPPED)
        );
        assert!(
            matches!(poll_once(escaped.write.as_mut()), Poll::Ready(Err(error)) if error.code() == HttpErrorCode::DROPPED && error.bytes_written() == 4)
        );
        drop(escaped.transport);
        assert_eq!(
            escaped.probe.0.borrow().actions.last().unwrap().0,
            Action::Dropped,
        );
    });
}

#[test]
fn worker_shutdown_cancels_pending_graceful_flush_on_owner() {
    on_worker(|context| async move {
        let probe = Probe::new();
        probe.0.borrow_mut().buffered.extend(b"not-flushed");
        let transport = probe.adopt(&context);
        transport.close(&context).expect("request close");
        probe.wait_for(Action::FlushPending).await;
        assert!(!probe.saw(Action::Dropped));
        CLOSING.with(|closing| {
            assert!(closing.replace(Some((transport, probe))).is_none());
        });
    });
    CLOSING.with(|closing| {
        let (transport, probe) = closing.take().expect("escaped closing transport");
        assert!(probe.saw(Action::Dropped));
        assert!(!probe.saw(Action::Shutdown));
        assert!(probe.0.borrow().flushed.is_empty());
        let actions = probe.0.borrow().actions.clone();
        drop(transport);
        assert_eq!(probe.0.borrow().actions, actions);
    });
}

#[test]
fn bounded_admission_rejects_large_write_without_touching_io() {
    on_worker(|context| async move {
        let probe = Probe::new();
        let transport = probe.adopt(&context);
        let error = transport
            .write(
                &context,
                queue(&[&[0; 65]]),
                Duration::ZERO,
                CancellationToken::never(),
            )
            .await
            .expect_err("write exceeds 64-byte limit");
        assert_eq!(error.bytes_written(), 0);
        assert!(!probe.saw(Action::Write));
        assert!(!probe.saw(Action::Dropped));
        probe.allow_flush();
        assert_eq!(
            transport
                .write(
                    &context,
                    queue(&[b"ok"]),
                    Duration::ZERO,
                    CancellationToken::never(),
                )
                .await
                .unwrap()
                .bytes_written,
            2,
        );
        assert_eq!(probe.0.borrow().flushed, b"ok");
        drop(transport);
    });
}

#[cfg(target_os = "linux")]
#[test]
fn established_unix_stream_preserves_bytes() {
    on_worker(|context| async move {
        let (stream, mut peer) = tokio::net::UnixStream::pair().expect("Unix stream pair");
        let transport = adopt_local(
            &context,
            EstablishedIo::new(
                stream,
                TransportEndpoint::UnnamedUnix,
                TransportEndpoint::UnnamedUnix,
                None,
            ),
            limits(),
        )
        .expect("Unix stream attachment");
        let exchange = async {
            transport
                .write(
                    &context,
                    queue(&[b"pi", b"ng"]),
                    Duration::ZERO,
                    CancellationToken::never(),
                )
                .await
                .expect("Unix write");
            let mut received = Vec::new();
            while received.len() < 4 {
                received.extend(read_bytes(
                    transport
                        .read(&context, 4, 2, Duration::ZERO, CancellationToken::never())
                        .await
                        .expect("Unix read"),
                ));
            }
            assert_eq!(received, b"pong");
        };
        let peer_exchange = async {
            let mut request = [0; 4];
            peer.read_exact(&mut request).await.expect("peer Unix read");
            assert_eq!(&request, b"ping");
            peer.write_all(b"pong").await.expect("peer Unix write");
        };
        tokio::join!(exchange, peer_exchange);
        drop(transport);
    });
}

#[test]
fn pre_cancelled_write_drops_unpolled_io_on_owner() {
    on_worker(|context| async move {
        let probe = Probe::new();
        let transport = probe.adopt(&context);
        let cancellation = CancellationSource::new();
        assert!(cancellation.request_cancellation());
        let error = transport
            .write(
                &context,
                queue(&[b"not-written"]),
                Duration::ZERO,
                cancellation.token(),
            )
            .await
            .expect_err("cancelled before any I/O");
        assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
        assert_eq!(error.bytes_written(), 0);
        assert!(!probe.saw(Action::Write));
        assert!(probe.saw(Action::Dropped));
        drop(transport);
    });
}

struct SharedReadinessState {
    owner: ThreadId,
    lower_waker: Option<Waker>,
    changed_physical_waker: bool,
    ready: bool,
    dropped: bool,
}

#[derive(Clone)]
struct SharedReadiness(Rc<RefCell<SharedReadinessState>>);

impl SharedReadiness {
    fn new() -> Self {
        Self(Rc::new(RefCell::new(SharedReadinessState {
            owner: std::thread::current().id(),
            lower_waker: None,
            changed_physical_waker: false,
            ready: false,
            dropped: false,
        })))
    }

    fn stream(&self) -> CrossDirectionIo {
        CrossDirectionIo {
            readiness: self.clone(),
        }
    }

    fn adopt(&self, context: &WorkerContext) -> OwnedByteStream {
        adopt_local(
            context,
            EstablishedIo::new(
                self.stream(),
                TransportEndpoint::Unknown,
                TransportEndpoint::Unknown,
                None,
            ),
            limits(),
        )
        .expect("adopt cross-direction I/O")
    }

    fn poll_ready(&self, context: &mut Context<'_>) -> Poll<()> {
        let mut state = self.0.borrow_mut();
        assert_eq!(state.owner, std::thread::current().id());
        if state.ready {
            return Poll::Ready(());
        }
        if let Some(previous) = state.lower_waker.as_ref()
            && !previous.will_wake(context.waker())
        {
            state.changed_physical_waker = true;
        }
        state.lower_waker = Some(context.waker().clone());
        Poll::Pending
    }

    fn notify_once(&self) {
        let waker = {
            let mut state = self.0.borrow_mut();
            state.ready = true;
            state.lower_waker.take().expect("one lower readiness slot")
        };
        waker.wake();
    }
}

struct CrossDirectionIo {
    readiness: SharedReadiness,
}

impl AsyncRead for CrossDirectionIo {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.readiness.poll_ready(context).map(|()| {
            if buffer.remaining() != 0 {
                buffer.put_slice(b"r");
            }
            Ok(())
        })
    }
}

impl AsyncWrite for CrossDirectionIo {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.readiness
            .poll_ready(context)
            .map(|()| Ok(buffer.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.readiness.poll_ready(context).map(|()| Ok(()))
    }
}

impl Drop for CrossDirectionIo {
    fn drop(&mut self) {
        let mut state = self.readiness.0.borrow_mut();
        assert_eq!(state.owner, std::thread::current().id());
        state.dropped = true;
        state.lower_waker = None;
    }
}

#[test]
fn shared_lower_readiness_wakes_both_logical_operations() {
    for read_first in [true, false] {
        on_worker(move |context| async move {
            let readiness = SharedReadiness::new();
            let transport = readiness.adopt(&context);
            let read_count = Arc::new(WakeCount::default());
            let write_count = Arc::new(WakeCount::default());
            let read_waker = Waker::from(Arc::clone(&read_count));
            let write_waker = Waker::from(Arc::clone(&write_count));
            let mut read =
                transport.read(&context, 4, 2, Duration::ZERO, CancellationToken::never());
            let mut write = transport.write(
                &context,
                queue(&[b"w"]),
                Duration::ZERO,
                CancellationToken::never(),
            );
            if read_first {
                assert!(poll_with_waker(read.as_mut(), &read_waker).is_pending());
                assert!(poll_with_waker(write.as_mut(), &write_waker).is_pending());
            } else {
                assert!(poll_with_waker(write.as_mut(), &write_waker).is_pending());
                assert!(poll_with_waker(read.as_mut(), &read_waker).is_pending());
            }
            assert!(!readiness.0.borrow().changed_physical_waker);
            readiness.notify_once();
            assert_eq!(read_count.0.load(Ordering::Relaxed), 1);
            assert_eq!(write_count.0.load(Ordering::Relaxed), 1);
            assert_eq!(read_bytes(read.await.unwrap()), b"r");
            assert_eq!(write.await.unwrap().bytes_written, 1);
            drop(transport);
            assert!(readiness.0.borrow().dropped);
        });
    }
}

#[test]
fn read_cancellation_preserves_shared_write_notification() {
    on_worker(|context| async move {
        let readiness = SharedReadiness::new();
        let transport = readiness.adopt(&context);
        let read_count = Arc::new(WakeCount::default());
        let write_count = Arc::new(WakeCount::default());
        let read_waker = Waker::from(Arc::clone(&read_count));
        let write_waker = Waker::from(Arc::clone(&write_count));
        let mut write = transport.write(
            &context,
            queue(&[b"w"]),
            Duration::ZERO,
            CancellationToken::never(),
        );
        let mut read = transport.read(&context, 4, 2, Duration::ZERO, CancellationToken::never());
        assert!(poll_with_waker(write.as_mut(), &write_waker).is_pending());
        assert!(poll_with_waker(read.as_mut(), &read_waker).is_pending());
        drop(read);
        let cancellation_wakes = read_count.0.load(Ordering::Relaxed);
        readiness.notify_once();
        assert_eq!(read_count.0.load(Ordering::Relaxed), cancellation_wakes);
        assert_eq!(write_count.0.load(Ordering::Relaxed), 1);
        assert_eq!(write.await.unwrap().bytes_written, 1);
        drop(transport);
    });
}

#[test]
fn read_cancellation_preserves_shared_shutdown_notification() {
    on_worker(|_context| async move {
        let readiness = SharedReadiness::new();
        let mut stream = AdoptedStream::new(readiness.stream());
        let read_count = Arc::new(WakeCount::default());
        let shutdown_count = Arc::new(WakeCount::default());
        let read_waker = Waker::from(Arc::clone(&read_count));
        let shutdown_waker = Waker::from(Arc::clone(&shutdown_count));
        assert!(
            Pin::new(&mut stream)
                .poll_shutdown(&mut Context::from_waker(&shutdown_waker))
                .is_pending()
        );
        let mut buffer = [0; 4];
        assert!(
            Pin::new(&mut stream)
                .poll_read(
                    &mut Context::from_waker(&read_waker),
                    &mut ReadBuf::new(&mut buffer),
                )
                .is_pending()
        );
        stream.cancel_read();
        assert!(!readiness.0.borrow().changed_physical_waker);
        readiness.notify_once();
        assert_eq!(read_count.0.load(Ordering::Relaxed), 0);
        assert_eq!(shutdown_count.0.load(Ordering::Relaxed), 1);
        assert!(matches!(
            Pin::new(&mut stream).poll_shutdown(&mut Context::from_waker(&shutdown_waker)),
            Poll::Ready(Ok(())),
        ));
        drop(stream);
        assert!(readiness.0.borrow().dropped);
    });
}

struct PanicWake;

impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        panic!("injected logical observer panic");
    }
}

#[test]
fn panicking_observer_does_not_skip_other_readiness_observers() {
    for force_close in [false, true] {
        on_worker(move |_context| async move {
            let readiness = SharedReadiness::new();
            let mut stream = AdoptedStream::new(readiness.stream());
            let read_waker = Waker::from(Arc::new(PanicWake));
            let write_count = Arc::new(WakeCount::default());
            let write_waker = Waker::from(Arc::clone(&write_count));
            let mut buffer = [0; 4];
            assert!(
                Pin::new(&mut stream)
                    .poll_read(
                        &mut Context::from_waker(&read_waker),
                        &mut ReadBuf::new(&mut buffer),
                    )
                    .is_pending()
            );
            assert!(
                Pin::new(&mut stream)
                    .poll_write(&mut Context::from_waker(&write_waker), b"w")
                    .is_pending()
            );
            let notification = catch_unwind(AssertUnwindSafe(|| {
                if force_close {
                    stream.cancel_owned_write();
                } else {
                    readiness.notify_once();
                }
            }));
            assert!(notification.is_err());
            assert_eq!(write_count.0.load(Ordering::Relaxed), 1);
            assert_eq!(readiness.0.borrow().dropped, force_close);
            stream.cancel_read();
            stream.cancel_owned_write();
            assert!(readiness.0.borrow().dropped);
        });
    }
}

#[test]
fn zero_write_discards_buffered_prefix_and_wakes_pending_reader() {
    on_worker(|context| async move {
        let probe = Probe::new();
        {
            let mut state = probe.0.borrow_mut();
            state.write_allowance = 3;
            state.write_zero = true;
            state.flush_ready = true;
            state.shutdown_ready = true;
        }
        let transport = probe.adopt(&context);
        let read_count = Arc::new(WakeCount::default());
        let read_waker = Waker::from(Arc::clone(&read_count));
        let mut read = transport.read(&context, 4, 2, Duration::ZERO, CancellationToken::never());
        assert!(poll_with_waker(read.as_mut(), &read_waker).is_pending());
        let error = transport
            .write(
                &context,
                queue(&[b"abcd", b"efgh"]),
                Duration::ZERO,
                CancellationToken::never(),
            )
            .await
            .expect_err("nonempty zero write is fatal");
        assert_eq!(error.code(), HttpErrorCode::TRANSPORT_WRITE_ERROR);
        assert_eq!(error.bytes_written(), 3);
        assert_eq!(probe.0.borrow().accepted, b"abc");
        assert!(probe.saw(Action::Dropped));
        assert_eq!(read_count.0.load(Ordering::Relaxed), 1);
        assert_eq!(
            read.await.unwrap_err().code(),
            HttpErrorCode::TRANSPORT_READ_ERROR,
        );
        let rejected = transport
            .write(
                &context,
                queue(&[b"again"]),
                Duration::ZERO,
                CancellationToken::never(),
            )
            .await
            .expect_err("failed I/O cannot accept another write");
        assert_eq!(rejected.bytes_written(), 0);
        transport
            .close(&context)
            .expect("close already disposed I/O");
        drop(transport);
        assert!(probe.0.borrow().flushed.is_empty());
        assert!(!probe.saw(Action::Flushed));
        assert!(!probe.saw(Action::Shutdown));
        assert_eq!(probe.0.borrow().actions.last().unwrap().0, Action::Dropped);
    });
}

#[test]
fn scalar_and_vectored_zero_writes_distinguish_empty_input() {
    for vectored in [false, true] {
        on_worker(move |_context| async move {
            let probe = Probe::new();
            probe.0.borrow_mut().write_allowance = 0;
            probe.0.borrow_mut().write_zero = true;
            let mut stream = AdoptedStream::new(ProbeIo {
                probe: probe.clone(),
                _pinned: PhantomPinned,
            });
            let mut context = Context::from_waker(Waker::noop());
            let empty = if vectored {
                Pin::new(&mut stream).poll_write_vectored(&mut context, &[IoSlice::new(b"")])
            } else {
                Pin::new(&mut stream).poll_write(&mut context, b"")
            };
            assert!(matches!(empty, Poll::Ready(Ok(0))));
            assert!(!probe.saw(Action::Dropped));
            let nonempty = if vectored {
                Pin::new(&mut stream)
                    .poll_write_vectored(&mut context, &[IoSlice::new(b""), IoSlice::new(b"w")])
            } else {
                Pin::new(&mut stream).poll_write(&mut context, b"w")
            };
            assert!(
                matches!(nonempty, Poll::Ready(Err(error)) if error.kind() == io::ErrorKind::WriteZero)
            );
            assert!(probe.saw(Action::Dropped));
        });
    }
}

#[derive(Default)]
struct QuiesceEvidence {
    actions: Vec<(Action, ThreadId)>,
    flushed: Vec<u8>,
}

struct QuiesceIo {
    owner: ThreadId,
    evidence: Arc<Mutex<QuiesceEvidence>>,
    buffered: Vec<u8>,
    flush_started: Option<oneshot::Sender<()>>,
    flush_permit: oneshot::Receiver<()>,
    shutdown_started: Option<oneshot::Sender<()>>,
    shutdown_permit: oneshot::Receiver<()>,
    flushed: bool,
    shutdown: bool,
}

impl QuiesceIo {
    fn record(&self, action: Action) {
        let current = std::thread::current().id();
        assert_eq!(current, self.owner);
        self.evidence
            .lock()
            .unwrap()
            .actions
            .push((action, current));
    }
}

impl AsyncRead for QuiesceIo {
    fn poll_read(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        _buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.record(Action::Read);
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for QuiesceIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.record(Action::Write);
        self.buffered.extend_from_slice(buffer);
        Poll::Ready(Ok(buffer.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.flushed {
            return Poll::Ready(Ok(()));
        }
        if let Some(started) = self.flush_started.take() {
            self.record(Action::FlushPending);
            started.send(()).expect("root observes pending flush");
        }
        match Pin::new(&mut self.flush_permit).poll(context) {
            Poll::Ready(Ok(())) => {
                let buffered = std::mem::take(&mut self.buffered);
                self.evidence.lock().unwrap().flushed.extend(buffered);
                self.flushed = true;
                self.record(Action::Flushed);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(_)) => Poll::Ready(Err(io::ErrorKind::BrokenPipe.into())),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.shutdown {
            return Poll::Ready(Ok(()));
        }
        match self.as_mut().poll_flush(context) {
            Poll::Ready(Ok(())) => {}
            result => return result,
        }
        if let Some(started) = self.shutdown_started.take() {
            self.record(Action::ShutdownPending);
            started.send(()).expect("root observes pending shutdown");
        }
        match Pin::new(&mut self.shutdown_permit).poll(context) {
            Poll::Ready(Ok(())) => {
                self.shutdown = true;
                self.record(Action::Shutdown);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(_)) => Poll::Ready(Err(io::ErrorKind::BrokenPipe.into())),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl TokioByteStream for QuiesceIo {
    fn prepare_reset(&self) {}
}

impl Drop for QuiesceIo {
    fn drop(&mut self) {
        self.record(Action::Dropped);
    }
}

#[test]
fn runtime_quiesce_joins_transport_drain_after_scope_and_full_queue() {
    for adopted in [false, true] {
        for half_close in [false, true] {
            let mut owner = RuntimeOwner::start(RuntimeConfig {
                workers: 1,
                command_capacity: 1,
            })
            .expect("managed runtime starts");
            let handle = owner.handle();
            let worker = handle.worker(0).expect("owner worker exists");
            let quiesce_handle = handle.clone();
            let evidence = Arc::new(Mutex::new(QuiesceEvidence::default()));
            let worker_evidence = Arc::clone(&evidence);
            let (start_scope, start_scope_rx) = oneshot::channel();
            let (scope_finished, scope_finished_rx) = oneshot::channel();
            let (flush_started, flush_started_rx) = oneshot::channel();
            let (flush_permit, flush_permit_rx) = oneshot::channel();
            let (shutdown_started, shutdown_started_rx) = oneshot::channel();
            let (shutdown_permit, shutdown_permit_rx) = oneshot::channel();
            let initialized = worker
                .call_local(move |context| async move {
                    let stream = QuiesceIo {
                        owner: std::thread::current().id(),
                        evidence: worker_evidence,
                        buffered: b"buffered-before-quiesce".to_vec(),
                        flush_started: Some(flush_started),
                        flush_permit: flush_permit_rx,
                        shutdown_started: Some(shutdown_started),
                        shutdown_permit: shutdown_permit_rx,
                        flushed: false,
                        shutdown: false,
                    };
                    let transport = if adopted {
                        adopt_local(
                            &context,
                            EstablishedIo::new(
                                stream,
                                TransportEndpoint::Unknown,
                                TransportEndpoint::Unknown,
                                None,
                            ),
                            limits(),
                        )
                    } else {
                        owned_from_stream(
                            &context,
                            stream,
                            SocketAddress::default(),
                            SocketAddress::default(),
                            None,
                            limits(),
                        )
                    }
                    .expect("transport reserves cleanup before quiescence");
                    let group = Rc::new(RefCell::new(Some(TaskGroup::new())));
                    let cleanup_group = Rc::clone(&group);
                    let scope = context
                        .register_scope(async move {
                            let mut group = cleanup_group
                                .borrow_mut()
                                .take()
                                .expect("application scope owns group");
                            group.join().await.expect("application child joins");
                            scope_finished
                                .send(())
                                .expect("root observes application join");
                        })
                        .expect("application scope registers before child starts");
                    group
                        .borrow_mut()
                        .as_mut()
                        .expect("scope group is live")
                        .spawn_local(move |_| async move {
                            start_scope_rx.await.expect("initializer has returned");
                            context
                                .handle()
                                .submit(|_| {})
                                .expect("fill sole command slot");
                            assert_eq!(context.handle().queued_commands(), 1);
                            assert_eq!(
                                context.handle().submit(|_| {}),
                                Err(SubmitError::Overloaded),
                            );
                            drop(quiesce_handle.quiesce(Duration::from_secs(30)));
                            context.quiescing().await;
                            assert!(!context.handle().is_accepting());
                            assert!(!context.is_cancelled());
                            assert_eq!(context.handle().queued_commands(), 1);
                            assert_eq!(context.handle().submit(|_| {}), Err(SubmitError::Closed),);
                            if half_close {
                                transport.shutdown_write(&context)
                            } else {
                                transport.close(&context)
                            }
                            .expect("reserved drain starts without ordinary admission");
                            assert_eq!(context.handle().queued_commands(), 1);
                            drop(transport);
                            drop(scope);
                        })
                        .expect("application child admitted");
                })
                .expect("initializer admitted");
            Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("root observer runtime")
                .block_on(async {
                    time::timeout(Duration::from_secs(10), async {
                        initialized.await.expect("scope outlives initializer");
                        start_scope.send(()).expect("start admitted scope");
                        scope_finished_rx
                            .await
                            .expect("application scope has joined");
                        flush_started_rx.await.expect("flush starts during grace");
                        let mut quiesced = Box::pin(handle.quiesce(Duration::from_secs(30)));
                        assert!(poll_once(quiesced.as_mut()).is_pending());
                        assert_eq!(worker.state(), WorkerState::Open);
                        assert_eq!(worker.submit(|_| {}), Err(SubmitError::Closed));
                        assert_eq!(
                            evidence
                                .lock()
                                .unwrap()
                                .actions
                                .iter()
                                .map(|(action, _)| *action)
                                .collect::<Vec<_>>(),
                            vec![Action::FlushPending],
                        );
                        flush_permit.send(()).expect("complete pending flush");
                        shutdown_started_rx
                            .await
                            .expect("shutdown starts after flush");
                        assert!(poll_once(quiesced.as_mut()).is_pending());
                        assert_eq!(worker.state(), WorkerState::Open);
                        assert_eq!(evidence.lock().unwrap().flushed, b"buffered-before-quiesce",);
                        shutdown_permit.send(()).expect("complete pending shutdown");
                        quiesced
                            .await
                            .expect("transport drain is joined during grace");
                    })
                    .await
                    .expect("bounded cross-runtime cleanup");
                });
            let report = owner.join().expect("root physically joins owner worker");
            assert_eq!(report.workers_joined, 1);
            assert_eq!(report.workers_forced, 0);
            assert_eq!(worker.state(), WorkerState::Closed);
            let evidence = evidence.lock().unwrap();
            assert_eq!(
                evidence
                    .actions
                    .iter()
                    .map(|(action, _)| *action)
                    .collect::<Vec<_>>(),
                vec![
                    Action::FlushPending,
                    Action::Flushed,
                    Action::ShutdownPending,
                    Action::Shutdown,
                    Action::Dropped,
                ],
            );
            assert!(
                evidence
                    .actions
                    .iter()
                    .all(|(_, thread)| *thread == worker.owner_thread())
            );
            assert_ne!(worker.owner_thread(), std::thread::current().id());
        }
    }
}

#[test]
fn unused_transport_cleanup_reservation_does_not_hold_quiescence() {
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 1,
    })
    .expect("managed runtime starts");
    let handle = owner.handle();
    let initialized = handle
        .worker(0)
        .unwrap()
        .call_local(|context| async move {
            let probe = Probe::new();
            let transport = probe.adopt(&context);
            drop(transport);
            assert!(probe.saw(Action::Dropped));
        })
        .expect("transport initializer admitted");
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("root observer runtime")
        .block_on(async {
            time::timeout(Duration::from_secs(10), async {
                initialized.await.expect("transport was dropped");
                handle
                    .quiesce(Duration::from_secs(30))
                    .await
                    .expect("no leaked reservation");
            })
            .await
            .expect("unused reservation is disarmed rather than waiting for grace");
        });
    let report = owner.join().expect("owner physically joined");
    assert_eq!(report.workers_forced, 0);
    assert_eq!(report.workers_joined, 1);
}

struct DestructionEvidence {
    owner: ThreadId,
    drops: AtomicUsize,
    fields: AtomicUsize,
    receipt: Mutex<Option<AdoptedIoCompletion>>,
}

fn on_completion_worker<Factory, Work>(factory: Factory)
where
    Factory: FnOnce(WorkerContext) -> Work + Send + 'static,
    Work: Future<Output = ()> + 'static,
{
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 32,
    })
    .expect("completion-test owner");
    let runtime = owner.handle();
    let initialized = runtime
        .worker(0)
        .expect("worker")
        .call_local(factory)
        .expect("initializer admitted");
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("application runtime")
        .block_on(async {
            time::timeout(Duration::from_secs(10), async {
                initialized.await.expect("owner-local assertions");
                runtime
                    .quiesce(Duration::from_secs(30))
                    .await
                    .expect("all scopes retire within grace");
            })
            .await
            .expect("bounded retirement");
        });
    let report = owner.join().expect("physical owner join");
    assert_eq!(report.workers_joined, 1);
    assert_eq!(report.workers_forced, 0);
}

impl DestructionEvidence {
    fn assert_pending(&self) {
        assert_eq!(std::thread::current().id(), self.owner);
        if let Some(receipt) = self.receipt.lock().expect("receipt").as_ref() {
            assert!(poll_once(Box::pin(receipt.wait_retired()).as_mut()).is_pending());
        }
    }
}

struct DestructionField(Arc<DestructionEvidence>);

impl Drop for DestructionField {
    fn drop(&mut self) {
        self.0.assert_pending();
        self.0.fields.fetch_add(1, Ordering::SeqCst);
    }
}

struct DestructionIo {
    inner: Pin<Box<ProbeIo>>,
    field: DestructionField,
    panic_on_drop: bool,
}

impl AsyncRead for DestructionIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.inner.as_mut().poll_read(context, output)
    }
}

impl AsyncWrite for DestructionIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.inner.as_mut().poll_write(context, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.as_mut().poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.as_mut().poll_shutdown(context)
    }
}

impl Drop for DestructionIo {
    fn drop(&mut self) {
        self.field.0.assert_pending();
        self.field.0.drops.fetch_add(1, Ordering::SeqCst);
        assert!(!self.panic_on_drop, "injected I/O destructor panic");
    }
}

fn observed_adoption(
    context: &WorkerContext,
    probe: &Probe,
    panic_on_drop: bool,
) -> (OwnedByteStream, AdoptedIoControl, Arc<DestructionEvidence>) {
    let evidence = Arc::new(DestructionEvidence {
        owner: std::thread::current().id(),
        drops: AtomicUsize::new(0),
        fields: AtomicUsize::new(0),
        receipt: Mutex::new(None),
    });
    let (transport, control) = adopt_local_with_completion(
        context,
        EstablishedIo::new(
            DestructionIo {
                inner: Box::pin(ProbeIo {
                    probe: probe.clone(),
                    _pinned: PhantomPinned,
                }),
                field: DestructionField(Arc::clone(&evidence)),
                panic_on_drop,
            },
            TransportEndpoint::UnnamedUnix,
            TransportEndpoint::Unknown,
            None,
        ),
        limits(),
    )
    .expect("observed adoption");
    *evidence.receipt.lock().expect("receipt") = Some(control.completion());
    (transport, control, evidence)
}

#[test]
fn adopted_io_completion_requires_io_drop_and_scope_retirement() {
    on_completion_worker(|context| async move {
        let probe = Probe::new();
        let (transport, control, evidence) = observed_adoption(&context, &probe, false);
        let receipt = control.completion();
        let (release, release_rx) = oneshot::channel();
        let unrelated = context
            .register_scope(async move {
                release_rx.await.expect("release unrelated scope");
            })
            .expect("unrelated scope");
        let unrelated_receipt = unrelated.observe();
        unrelated
            .activate()
            .expect("activate unrelated scope")
            .detach();
        transport
            .close_with_reset(&context)
            .expect("dispose synchronously");
        assert_eq!(evidence.drops.load(Ordering::SeqCst), 1);
        assert_eq!(evidence.fields.load(Ordering::SeqCst), 1);
        assert!(poll_once(Box::pin(receipt.wait_retired()).as_mut()).is_pending());
        drop(transport);
        receipt
            .wait_retired()
            .await
            .expect("both boundaries retire");
        receipt.wait_retired().await.expect("sticky retirement");
        assert!(poll_once(Box::pin(unrelated_receipt.wait()).as_mut()).is_pending());
        release.send(()).expect("unrelated scope remains live");
        unrelated_receipt
            .wait()
            .await
            .expect("unrelated scope retires");
    });
}

#[test]
fn adopted_io_completion_survives_half_close_and_root_drop() {
    for mode in 0..5 {
        on_completion_worker(move |context| async move {
            let probe = Probe::new();
            probe.0.borrow_mut().buffered.extend(b"buffered");
            let (transport, control) = probe.adopt_with_completion(&context);
            let receipt = control.completion();
            transport.shutdown_write(&context).expect("half close");
            transport
                .shutdown_write(&context)
                .expect("duplicate half close");
            probe.wait_for(Action::FlushPending).await;
            assert!(poll_once(Box::pin(receipt.wait_retired()).as_mut()).is_pending());
            if mode == 0 || mode == 4 {
                probe.allow_flush();
                probe.wait_for(Action::ShutdownPending).await;
                probe.allow_shutdown();
                probe.wait_for(Action::Shutdown).await;
                assert!(!probe.saw(Action::Dropped));
                probe.0.borrow_mut().inbound.extend(b"read");
                let read = transport
                    .read(&context, 4, 2, Duration::ZERO, CancellationToken::never())
                    .await
                    .expect("half close remains readable");
                assert_eq!(read_bytes(read), b"read");
                assert!(poll_once(Box::pin(receipt.wait_retired()).as_mut()).is_pending());
                if mode == 0 {
                    transport
                        .close(&context)
                        .expect("upgrade completed half close");
                    transport.close(&context).expect("duplicate full close");
                } else {
                    control.cancel();
                    receipt
                        .wait_retired()
                        .await
                        .expect("cancellation remains live after completed half close");
                }
                drop(transport);
            } else {
                if mode == 1 {
                    transport
                        .close(&context)
                        .expect("upgrade pending half close");
                    transport.close(&context).expect("duplicate pending close");
                }
                drop(transport);
                assert!(!probe.saw(Action::Dropped));
                if mode == 1 || mode == 3 {
                    probe.allow_flush();
                    probe.wait_for(Action::ShutdownPending).await;
                    probe.allow_shutdown();
                } else {
                    control.cancel();
                    control.cancel();
                }
            }
            receipt
                .wait_retired()
                .await
                .expect("root drop leaves a live terminal owner");
            assert_eq!(
                probe
                    .0
                    .borrow()
                    .actions
                    .iter()
                    .filter(|(action, _)| *action == Action::Dropped)
                    .count(),
                1
            );
            if mode == 2 {
                assert!(probe.0.borrow().flushed.is_empty());
                assert!(!probe.saw(Action::Shutdown));
            } else {
                assert_eq!(probe.0.borrow().flushed, b"buffered");
                assert_eq!(
                    probe
                        .0
                        .borrow()
                        .actions
                        .iter()
                        .filter(|(action, _)| *action == Action::Shutdown)
                        .count(),
                    1
                );
            }
        });
    }
}

#[test]
fn adopted_io_control_cancels_without_command_admission() {
    for phase in 0..3 {
        let mut owner = RuntimeOwner::start(RuntimeConfig {
            workers: 1,
            command_capacity: 1,
        })
        .expect("runtime");
        let runtime = owner.handle();
        let worker = runtime.worker(0).expect("worker");
        let evidence = Arc::new(Mutex::new(QuiesceEvidence::default()));
        let worker_evidence = Arc::clone(&evidence);
        let (published, published_rx) = oneshot::channel();
        let (release_root, release_root_rx) = oneshot::channel();
        let (flush_started, flush_started_rx) = oneshot::channel();
        let (flush_permit, flush_permit_rx) = oneshot::channel();
        let (shutdown_started, shutdown_started_rx) = oneshot::channel();
        let (shutdown_permit, shutdown_permit_rx) = oneshot::channel();
        let initialized = worker
            .call_local(move |context| async move {
                let (transport, control) = adopt_local_with_completion(
                    &context,
                    EstablishedIo::new(
                        QuiesceIo {
                            owner: std::thread::current().id(),
                            evidence: worker_evidence,
                            buffered: b"held".to_vec(),
                            flush_started: Some(flush_started),
                            flush_permit: flush_permit_rx,
                            shutdown_started: Some(shutdown_started),
                            shutdown_permit: shutdown_permit_rx,
                            flushed: false,
                            shutdown: false,
                        },
                        TransportEndpoint::UnnamedUnix,
                        TransportEndpoint::Unknown,
                        None,
                    ),
                    limits(),
                )
                .expect("adoption");
                context.handle().submit(|_| {}).expect("fill command queue");
                assert_eq!(
                    context.handle().submit(|_| {}),
                    Err(SubmitError::Overloaded)
                );
                if phase == 0 {
                    control.cancel();
                } else {
                    transport.close(&context).expect("start close");
                }
                assert!(published.send(control).is_ok());
                release_root_rx
                    .await
                    .expect("root keeps public transport alive");
                drop(transport);
            })
            .expect("admit initializer");
        Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("application runtime")
            .block_on(async {
                time::timeout(Duration::from_secs(10), async {
                    let control = published_rx.await.expect("control published");
                    let receipt = control.completion();
                    let mut flush_permit = Some(flush_permit);
                    if phase != 0 {
                        flush_started_rx.await.expect("actual flush pending");
                        if phase == 2 {
                            flush_permit
                                .take()
                                .expect("flush permit")
                                .send(())
                                .expect("allow flush only");
                            shutdown_started_rx.await.expect("actual shutdown pending");
                        }
                        assert!(poll_once(Box::pin(receipt.wait_retired()).as_mut()).is_pending());
                    }
                    let mut quiesced = Box::pin(runtime.quiesce(Duration::from_secs(30)));
                    assert!(poll_once(quiesced.as_mut()).is_pending());
                    assert_eq!(worker.submit(|_| {}), Err(SubmitError::Closed));
                    control.cancel();
                    receipt
                        .wait_retired()
                        .await
                        .expect("cancellation bypasses closed admission");
                    assert!(shutdown_permit.is_closed());
                    if let Some(permit) = flush_permit.as_ref() {
                        assert!(permit.is_closed());
                    }
                    assert!(
                        poll_once(quiesced.as_mut()).is_pending(),
                        "initializer still owns unrelated work"
                    );
                    release_root
                        .send(())
                        .expect("drop public root after retirement");
                    initialized.await.expect("initializer completes");
                    quiesced.await.expect("all scopes retire within grace");
                })
                .await
                .expect("bounded cancellation");
            });
        let report = owner.join().expect("physical join");
        assert_eq!(report.workers_forced, 0);
        assert_eq!(report.workers_joined, 1);
        let evidence = evidence.lock().expect("evidence");
        assert_eq!(
            evidence
                .actions
                .iter()
                .filter(|(action, _)| *action == Action::Dropped)
                .count(),
            1
        );
        assert!(
            evidence
                .actions
                .iter()
                .all(|(_, thread)| *thread == worker.owner_thread())
        );
        assert!(
            !evidence
                .actions
                .iter()
                .any(|(action, _)| *action == Action::Shutdown)
        );
    }
}

#[test]
fn adopted_io_completion_observers_are_independent() {
    fn transferable<Value: Send + Sync + Clone>() {}
    fn send_future<Work: Future + Send + 'static>(_: &Work) {}
    transferable::<AdoptedIoControl>();
    transferable::<AdoptedIoCompletion>();
    on_completion_worker(|context| async move {
        let probe = Probe::new();
        let (transport, control) = probe.adopt_with_completion(&context);
        let receipt = control.completion();
        let first_count = Arc::new(WakeCount::default());
        let second_count = Arc::new(WakeCount::default());
        let mut abandoned = Box::pin(receipt.wait_retired());
        send_future(&receipt.wait_retired());
        assert!(
            poll_with_waker(abandoned.as_mut(), &Waker::from(Arc::clone(&first_count)))
                .is_pending()
        );
        drop(abandoned);
        drop(control.clone());
        assert!(!probe.saw(Action::Dropped));
        let mut waiting = Box::pin(receipt.wait_retired());
        assert!(
            poll_with_waker(waiting.as_mut(), &Waker::from(Arc::clone(&second_count))).is_pending()
        );
        control.cancel();
        control.cancel();
        receipt
            .wait_retired()
            .await
            .expect("independent observer drives completion");
        waiting.await.expect("retirement");
        receipt
            .clone()
            .wait_retired()
            .await
            .expect("independent sticky receipt");
        assert_eq!(first_count.0.load(Ordering::SeqCst), 0);
        assert!(second_count.0.load(Ordering::SeqCst) > 0);
        drop(transport);
        assert_eq!(
            probe
                .0
                .borrow()
                .actions
                .iter()
                .filter(|(action, _)| *action == Action::Dropped)
                .count(),
            1
        );
    });
}

thread_local! {
    static COMPLETION_ESCAPED: RefCell<Option<OwnedByteStream>> = const { RefCell::new(None) };
}

#[test]
fn adopted_io_completion_covers_failure_and_forced_cleanup() {
    on_completion_worker(|context| async move {
        let probe = Probe::new();
        let shared = Arc::new(AdoptedIoState::default());
        let io = AdoptedStream::with_completion(
            ProbeIo {
                probe: probe.clone(),
                _pinned: PhantomPinned,
            },
            Arc::clone(&shared),
        );
        let capture = CompletionScopeDropPanic;
        let scope = context
            .register_scope(async move {
                let _capture = capture;
                std::future::pending::<()>().await;
            })
            .expect("separate composition boundary");
        let receipt = AdoptedIoCompletion::new(shared, scope.observe());
        assert!(catch_unwind(AssertUnwindSafe(|| scope.disarm())).is_err());
        assert!(
            poll_once(Box::pin(receipt.wait_retired()).as_mut()).is_pending(),
            "scope failure does not short-circuit pending I/O destruction"
        );
        assert!(!probe.saw(Action::Dropped));
        drop(io);
        assert_eq!(
            receipt.wait_retired().await,
            Err(AdoptedIoCompletionError::DrainRetirementUnconfirmed)
        );

        let probe = Probe::new();
        probe.0.borrow_mut().write_allowance = 3;
        probe.0.borrow_mut().write_zero = true;
        let (transport, control) = probe.adopt_with_completion(&context);
        let error = transport
            .write(
                &context,
                queue(&[b"abcdef"]),
                Duration::ZERO,
                CancellationToken::never(),
            )
            .await
            .expect_err("fatal partial write");
        assert_eq!(error.code(), HttpErrorCode::TRANSPORT_WRITE_ERROR);
        assert_eq!(error.bytes_written(), 3);
        assert_eq!(probe.0.borrow().accepted, b"abc");
        assert!(poll_once(Box::pin(control.completion().wait_retired()).as_mut()).is_pending());
        control
            .completion()
            .wait_retired()
            .await
            .expect("failure still retires");
        drop(transport);
        assert!(probe.0.borrow().flushed.is_empty());

        let probe = Probe::new();
        let (transport, control, evidence) = observed_adoption(&context, &probe, true);
        assert!(catch_unwind(AssertUnwindSafe(|| transport.close_with_reset(&context))).is_err());
        let receipt = control.completion();
        assert!(
            poll_once(Box::pin(receipt.wait_retired()).as_mut()).is_pending(),
            "I/O failure does not short-circuit pending scope retirement"
        );
        assert_eq!(
            receipt.wait_retired().await,
            Err(AdoptedIoCompletionError::IoDestructionUnconfirmed)
        );
        assert_eq!(evidence.fields.load(Ordering::SeqCst), 1);
        drop(transport);
    });

    for mode in 0..3 {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("owner runtime")
            .block_on(LocalSet::new().run_until(async move {
                let (worker, driver) = WorkerDriver::new(4).expect("worker driver");
                let initialized = worker
                    .call_local(move |context| async move {
                        let probe = Probe::new();
                        let (transport, control, evidence) =
                            observed_adoption(&context, &probe, mode == 2);
                        transport.close(&context).expect("gated close");
                        COMPLETION_ESCAPED
                            .with(|escaped| assert!(escaped.replace(Some(transport)).is_none()));
                        (control, evidence)
                    })
                    .expect("initializer admitted");
                let mut driver = Box::pin(driver);
                assert!(poll_once(driver.as_mut()).is_pending());
                let (control, evidence) = initialized.await.expect("control published");
                let mut notification = Box::pin(control.completion().wait_retired());
                if mode == 1 {
                    assert!(
                        poll_with_waker(notification.as_mut(), &Waker::from(Arc::new(PanicWake)))
                            .is_pending()
                    );
                }
                let stopped = catch_unwind(AssertUnwindSafe(|| drop(driver)));
                assert_eq!(stopped.is_err(), mode != 0);
                let expected = match mode {
                    0 => Ok(()),
                    1 => Err(AdoptedIoCompletionError::DrainRetirementUnconfirmed),
                    _ => Err(AdoptedIoCompletionError::BothUnconfirmed),
                };
                assert_eq!(control.completion().wait_retired().await, expected);
                assert_eq!(control.completion().wait_retired().await, expected);
                assert_eq!(evidence.drops.load(Ordering::SeqCst), 1);
                assert_eq!(evidence.fields.load(Ordering::SeqCst), 1);
                COMPLETION_ESCAPED.with(|escaped| drop(escaped.take()));
                assert_eq!(worker.state(), WorkerState::Closed);
            }));
    }
}

#[test]
fn adopted_io_completion_unused_adoption_retires_without_grace_expiry() {
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 1,
    })
    .expect("runtime");
    let runtime = owner.handle();
    let worker_runtime = runtime.clone();
    let initialized = runtime
        .worker(0)
        .expect("worker")
        .call_local(move |context| async move {
            for failed_publication in [false, true] {
                let probe = Probe::new();
                let (transport, control) = probe.adopt_with_completion(&context);
                let receipt = control.completion();
                if failed_publication {
                    let (publish, receiver) = oneshot::channel();
                    drop(receiver);
                    assert!(publish.send(control).is_err());
                } else {
                    drop(control);
                }
                assert!(!probe.saw(Action::Dropped));
                drop(transport);
                assert!(probe.saw(Action::Dropped));
                receipt.wait_retired().await.expect("unused scope retires");
            }
            drop(worker_runtime.quiesce(Duration::from_secs(30)));
            assert!(!context.handle().is_accepting());
            let probe = Probe::new();
            let rejected = adopt_local_with_completion(
                &context,
                EstablishedIo::new(
                    ProbeIo {
                        probe: probe.clone(),
                        _pinned: PhantomPinned,
                    },
                    TransportEndpoint::UnnamedUnix,
                    TransportEndpoint::Unknown,
                    None,
                ),
                limits(),
            );
            assert!(matches!(rejected, Err(error) if error.code() == HttpErrorCode::DROPPED));
            assert!(probe.saw(Action::Dropped));
        })
        .expect("initializer");
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("application runtime")
        .block_on(async {
            time::timeout(Duration::from_secs(10), async {
                initialized.await.expect("unused roots disposed");
                runtime
                    .quiesce(Duration::from_secs(30))
                    .await
                    .expect("no leaked scope reservation");
            })
            .await
            .expect("cleanup does not await grace expiry");
        });
    let report = owner.join().expect("physical join");
    assert_eq!(report.workers_joined, 1);
    assert_eq!(report.workers_forced, 0);
}

struct RepeatedPanicWake(AtomicUsize);

impl Wake for RepeatedPanicWake {
    fn wake(self: Arc<Self>) {
        if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            panic!("pending write observer A");
        }
        panic!("pending write observer B");
    }
}

struct CleanupDropCount {
    owner: ThreadId,
    drops: Arc<AtomicUsize>,
}

impl Drop for CleanupDropCount {
    fn drop(&mut self) {
        assert_eq!(std::thread::current().id(), self.owner);
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn adopted_io_reset_retires_pending_write_after_repeated_waker_panic() {
    on_completion_worker(|context| async move {
        let probe = Probe::new();
        probe.0.borrow_mut().write_allowance = 0;
        let (transport, control, evidence) = observed_adoption(&context, &probe, false);
        let read_count = Arc::new(WakeCount::default());
        let mut reading =
            transport.read(&context, 4, 2, Duration::ZERO, CancellationToken::never());
        assert!(
            poll_with_waker(reading.as_mut(), &Waker::from(Arc::clone(&read_count))).is_pending()
        );
        let write_count = Arc::new(RepeatedPanicWake(AtomicUsize::new(0)));
        let mut writing = transport.write(
            &context,
            queue(&[b"pending"]),
            Duration::ZERO,
            CancellationToken::never(),
        );
        assert!(
            poll_with_waker(writing.as_mut(), &Waker::from(Arc::clone(&write_count))).is_pending()
        );
        assert!(probe.saw(Action::Write));
        assert_eq!(write_count.0.load(Ordering::SeqCst), 0);

        let cleanup_drops = Arc::new(AtomicUsize::new(0));
        let cleanup = CleanupDropCount {
            owner: std::thread::current().id(),
            drops: Arc::clone(&cleanup_drops),
        };
        let (release, released) = oneshot::channel();
        let scope = context
            .register_scope(async move {
                let _cleanup = cleanup;
                released.await.expect("independent cleanup released");
            })
            .expect("independent cleanup scope");
        let cleanup_receipt = scope.observe();
        scope
            .activate()
            .expect("activate independent cleanup")
            .detach();
        let receipt = control.completion();
        let mut observer = Box::pin(receipt.wait_retired());
        let notification = Arc::new(WakeCount::default());
        assert!(
            poll_with_waker(observer.as_mut(), &Waker::from(Arc::clone(&notification)))
                .is_pending()
        );

        let panic = catch_unwind(AssertUnwindSafe(|| transport.close_with_reset(&context)))
            .expect_err("logical observer panics during cancellation");
        assert_eq!(
            *panic
                .downcast::<&'static str>()
                .expect("first observer panic"),
            "pending write observer A"
        );
        assert_eq!(
            write_count.0.load(Ordering::SeqCst),
            2,
            "logical and terminal notifications run, without a registered baton wake"
        );
        assert_eq!(read_count.0.load(Ordering::Relaxed), 1);
        assert!(notification.0.load(Ordering::Relaxed) > 0);
        assert_eq!(evidence.drops.load(Ordering::SeqCst), 1);
        assert_eq!(evidence.fields.load(Ordering::SeqCst), 1);
        assert!(
            poll_once(observer.as_mut()).is_pending(),
            "I/O disposal is not scope retirement"
        );
        assert_eq!(
            reading.await.expect_err("reset reader").code(),
            HttpErrorCode::TRANSPORT_READ_ERROR
        );
        let write_error = writing.await.expect_err("reset writer");
        assert_eq!(write_error.code(), HttpErrorCode::TRANSPORT_WRITE_ERROR);
        assert_eq!(write_error.bytes_written(), 0);
        observer
            .await
            .expect("actual drain retires after caught notification panic");
        receipt
            .wait_retired()
            .await
            .expect("sticky combined retirement");
        assert_eq!(cleanup_drops.load(Ordering::SeqCst), 0);
        release.send(()).expect("independent cleanup remains live");
        cleanup_receipt
            .wait()
            .await
            .expect("independent cleanup retires");
        assert_eq!(cleanup_drops.load(Ordering::SeqCst), 1);
        drop(transport);
        assert_eq!(evidence.drops.load(Ordering::SeqCst), 1);
    });
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReadyErrorPhase {
    Write,
    Flush,
    Shutdown,
}

#[derive(Debug, thiserror::Error)]
#[error("injected ready I/O error")]
struct ReadyDropError {
    errors: Arc<AtomicUsize>,
    payloads: Arc<AtomicUsize>,
}

impl Drop for ReadyDropError {
    fn drop(&mut self) {
        self.errors.fetch_add(1, Ordering::SeqCst);
        std::panic::panic_any(ReadyErrorPayload(Arc::clone(&self.payloads)));
    }
}

struct ReadyErrorPayload(Arc<AtomicUsize>);

impl Drop for ReadyErrorPayload {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("ready error payload destructor C");
    }
}

struct ReadyErrorIo {
    inner: Pin<Box<ProbeIo>>,
    field: DestructionField,
    phase: ReadyErrorPhase,
    errors: Arc<AtomicUsize>,
    payloads: Arc<AtomicUsize>,
}

impl ReadyErrorIo {
    fn error(&self) -> io::Error {
        io::Error::other(ReadyDropError {
            errors: Arc::clone(&self.errors),
            payloads: Arc::clone(&self.payloads),
        })
    }
}

impl AsyncRead for ReadyErrorIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.inner.as_mut().poll_read(context, output)
    }
}

impl AsyncWrite for ReadyErrorIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.phase == ReadyErrorPhase::Write {
            return Poll::Ready(Err(self.error()));
        }
        self.inner.as_mut().poll_write(context, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.phase == ReadyErrorPhase::Flush {
            return Poll::Ready(Err(self.error()));
        }
        self.inner.as_mut().poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        assert!(self.phase == ReadyErrorPhase::Shutdown);
        Poll::Ready(Err(self.error()))
    }
}

impl Drop for ReadyErrorIo {
    fn drop(&mut self) {
        self.field.0.assert_pending();
        self.field.0.drops.fetch_add(1, Ordering::SeqCst);
        panic!("ready I/O destructor A");
    }
}

struct RetirementFixture {
    transport: OwnedByteStream,
    context: WorkerContext,
    reading: Option<LocalTransportFuture<Result<TransportRead, HttpError>>>,
    _cleanup: WorkerCleanupRegistration,
}

thread_local! {
    static RETIREMENT_FIXTURE: RefCell<Option<RetirementFixture>> = const { RefCell::new(None) };
}

fn ready_error_preserves_first_cleanup_panic(phase: ReadyErrorPhase) {
    Builder::new_current_thread().enable_all().build().expect("ready-error runtime")
        .block_on(LocalSet::new().run_until(async move {
            let (worker, driver) = WorkerDriver::new(4).expect("ready-error worker");
            let initialized = worker.call_local(move |context| async move {
                let probe = Probe::new();
                probe.allow_flush();
                let evidence = Arc::new(DestructionEvidence {
                    owner: std::thread::current().id(), drops: AtomicUsize::new(0), fields: AtomicUsize::new(0), receipt: Mutex::new(None),
                });
                let errors = Arc::new(AtomicUsize::new(0));
                let payloads = Arc::new(AtomicUsize::new(0));
                let (transport, control) = adopt_local_with_completion(&context, EstablishedIo::new(
                    ReadyErrorIo {
                        inner: Box::pin(ProbeIo { probe: probe.clone(), _pinned: PhantomPinned }),
                        field: DestructionField(Arc::clone(&evidence)), phase,
                        errors: Arc::clone(&errors), payloads: Arc::clone(&payloads),
                    }, TransportEndpoint::UnnamedUnix, TransportEndpoint::Unknown, None,
                ), limits()).expect("ready-error adoption");
                *evidence.receipt.lock().expect("receipt") = Some(control.completion());
                let read_count = Arc::new(WakeCount::default());
                let mut reading = transport.read(&context, 4, 2, Duration::ZERO, CancellationToken::never());
                assert!(poll_with_waker(reading.as_mut(), &Waker::from(Arc::clone(&read_count))).is_pending());
                let cleaned = Arc::new(AtomicUsize::new(0));
                let cleanup_count = Arc::clone(&cleaned);
                let owner = std::thread::current().id();
                let cleanup = context.register_cleanup(move || {
                    assert_eq!(std::thread::current().id(), owner);
                    cleanup_count.fetch_add(1, Ordering::SeqCst);
                }).expect("independent cleanup");
                RETIREMENT_FIXTURE.with(|fixture| assert!(fixture.replace(Some(RetirementFixture {
                    transport, context, reading: Some(reading), _cleanup: cleanup,
                })).is_none()));
                (control, evidence, errors, payloads, read_count, cleaned)
            }).expect("ready-error initializer");
            let mut driver = Box::pin(driver);
            assert!(poll_once(driver.as_mut()).is_pending());
            let (control, evidence, errors, payloads, read_count, cleaned) = initialized.await.expect("initialized");
            let receipt = control.completion();
            let mut observing = Box::pin(receipt.wait_retired());
            let notifications = Arc::new(WakeCount::default());
            assert!(poll_with_waker(observing.as_mut(), &Waker::from(Arc::clone(&notifications))).is_pending());

            let panic = if phase == ReadyErrorPhase::Shutdown {
                RETIREMENT_FIXTURE.with(|fixture| {
                    let fixture = fixture.borrow();
                    let fixture = fixture.as_ref().expect("owner fixture");
                    fixture.transport.close(&fixture.context).expect("start shutdown path");
                });
                catch_unwind(AssertUnwindSafe(|| poll_once(driver.as_mut())))
                    .expect_err("shutdown destructor panic reaches driver")
            } else {
                RETIREMENT_FIXTURE.with(|fixture| {
                    let fixture = fixture.borrow();
                    let fixture = fixture.as_ref().expect("owner fixture");
                    let mut writing = fixture.transport.write(&fixture.context, queue(&[b"bytes"]), Duration::ZERO, CancellationToken::never());
                    let panic = catch_unwind(AssertUnwindSafe(|| poll_once(writing.as_mut())))
                        .expect_err("ready result cleanup preserves destructor panic");
                    assert!(poll_once(Box::pin(receipt.wait_retired()).as_mut()).is_pending(), "I/O failure cannot bypass the actual drain scope");
                    drop(writing);
                    panic
                })
            };
            assert_eq!(*panic.downcast::<&'static str>().expect("first I/O panic"), "ready I/O destructor A");
            assert_eq!(errors.load(Ordering::SeqCst), 1);
            assert_eq!(payloads.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.drops.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.fields.load(Ordering::SeqCst), 1);
            assert!(read_count.0.load(Ordering::Relaxed) > 0);
            assert!(notifications.0.load(Ordering::Relaxed) > 0);
            if phase != ReadyErrorPhase::Shutdown {
                assert!(poll_once(driver.as_mut()).is_pending());
                assert!(worker.begin_shutdown());
                assert!(poll_once(driver.as_mut()).is_ready());
            }
            assert_eq!(worker.state(), WorkerState::Closed);
            assert_eq!(cleaned.load(Ordering::SeqCst), 1);
            let expected = Err(AdoptedIoCompletionError::IoDestructionUnconfirmed);
            assert_eq!(poll_once(observing.as_mut()), Poll::Ready(expected));
            assert_eq!(poll_once(Box::pin(receipt.wait_retired()).as_mut()), Poll::Ready(expected));
            RETIREMENT_FIXTURE.with(|fixture| {
                let mut fixture = fixture.take().expect("retire owner fixture");
                let mut reading = fixture.reading.take().expect("pending reader");
                assert!(matches!(poll_once(reading.as_mut()), Poll::Ready(Err(error)) if error.code() == HttpErrorCode::TRANSPORT_READ_ERROR));
                drop(fixture);
            });
            assert_eq!(evidence.drops.load(Ordering::SeqCst), 1);
        }));
}

#[test]
fn adopted_io_ready_write_error_preserves_first_cleanup_panic() {
    ready_error_preserves_first_cleanup_panic(ReadyErrorPhase::Write);
}

#[test]
fn adopted_io_ready_flush_error_preserves_first_cleanup_panic() {
    ready_error_preserves_first_cleanup_panic(ReadyErrorPhase::Flush);
}

#[test]
fn adopted_io_ready_shutdown_error_preserves_first_cleanup_panic() {
    ready_error_preserves_first_cleanup_panic(ReadyErrorPhase::Shutdown);
}

#[derive(Default)]
struct ConditionalPanicWake {
    enabled: AtomicBool,
    panics: AtomicUsize,
}

impl Wake for ConditionalPanicWake {
    fn wake(self: Arc<Self>) {
        if self.enabled.load(Ordering::SeqCst) {
            self.panics.fetch_add(1, Ordering::SeqCst);
            panic!("unused drain notification B");
        }
    }
}

#[test]
fn adopted_io_unused_root_drop_preserves_existing_unwind() {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("root-drop runtime")
        .block_on(LocalSet::new().run_until(async {
            let (worker, driver) = WorkerDriver::new(4).expect("root-drop worker");
            let initialized = worker
                .call_local(|context| async move {
                    let probe = Probe::new();
                    let (transport, control, evidence) = observed_adoption(&context, &probe, false);
                    let cleaned = Arc::new(AtomicUsize::new(0));
                    let cleanup_count = Arc::clone(&cleaned);
                    let owner = std::thread::current().id();
                    let cleanup = context
                        .register_cleanup(move || {
                            assert_eq!(std::thread::current().id(), owner);
                            cleanup_count.fetch_add(1, Ordering::SeqCst);
                        })
                        .expect("independent root cleanup");
                    RETIREMENT_FIXTURE.with(|fixture| {
                        assert!(
                            fixture
                                .replace(Some(RetirementFixture {
                                    transport,
                                    context,
                                    reading: None,
                                    _cleanup: cleanup,
                                }))
                                .is_none()
                        )
                    });
                    (control, evidence, cleaned)
                })
                .expect("root-drop initializer");
            let mut driver = Box::pin(driver);
            let conditional = Arc::new(ConditionalPanicWake::default());
            assert!(poll_once(driver.as_mut()).is_pending());
            let (control, evidence, cleaned) =
                initialized.await.expect("unused adoption published");
            let receipt = control.completion();
            assert_eq!(evidence.drops.load(Ordering::SeqCst), 0);
            let mut observing = Box::pin(receipt.wait_retired());
            assert!(
                poll_with_waker(observing.as_mut(), &Waker::from(Arc::clone(&conditional)))
                    .is_pending()
            );
            let RetirementFixture {
                transport,
                _cleanup: cleanup,
                ..
            } = RETIREMENT_FIXTURE.with(|fixture| fixture.take().expect("root fixture"));
            conditional.enabled.store(true, Ordering::SeqCst);
            let panic = catch_unwind(AssertUnwindSafe(move || {
                let _transport = transport;
                panic!("unrelated root unwind A");
            }))
            .expect_err("original unwind survives root destruction");
            conditional.enabled.store(false, Ordering::SeqCst);
            assert_eq!(
                *panic.downcast::<&'static str>().expect("original panic"),
                "unrelated root unwind A"
            );
            assert!(
                conditional.panics.load(Ordering::SeqCst) > 0,
                "I/O destruction notified its waiter and the notification really panicked"
            );
            assert!(
                poll_once(Box::pin(receipt.wait_retired()).as_mut()).is_pending(),
                "root Drop is not actual scope retirement"
            );
            assert!(poll_once(driver.as_mut()).is_pending());
            assert_eq!(
                poll_once(Box::pin(receipt.wait_retired()).as_mut()),
                Poll::Ready(Ok(()))
            );
            assert_eq!(evidence.drops.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.fields.load(Ordering::SeqCst), 1);
            assert_eq!(cleaned.load(Ordering::SeqCst), 0);
            assert!(worker.begin_shutdown());
            assert!(poll_once(driver.as_mut()).is_ready());
            assert_eq!(worker.state(), WorkerState::Closed);
            assert_eq!(cleaned.load(Ordering::SeqCst), 1);
            assert_eq!(
                poll_once(Box::pin(receipt.wait_retired()).as_mut()),
                Poll::Ready(Ok(()))
            );
            drop(cleanup);
        }));
}

async fn graceful_retirement_preserves_eof(
    context: WorkerContext,
    half_close: bool,
    completed: bool,
) {
    let probe = Probe::new();
    probe.0.borrow_mut().buffered.extend(b"buffered");
    let (transport, control, evidence) = observed_adoption(&context, &probe, false);
    let receipt = control.completion();
    if half_close {
        transport
            .shutdown_write(&context)
            .expect("start half close");
        probe.wait_for(Action::FlushPending).await;
        assert!(poll_once(Box::pin(receipt.wait_retired()).as_mut()).is_pending());
        if completed {
            probe.allow_flush();
            probe.wait_for(Action::ShutdownPending).await;
            assert_eq!(evidence.drops.load(Ordering::SeqCst), 0);
            probe.allow_shutdown();
            probe.wait_for(Action::Shutdown).await;
            assert!(poll_once(Box::pin(receipt.wait_retired()).as_mut()).is_pending());
            assert_eq!(evidence.drops.load(Ordering::SeqCst), 0);
        }
    }
    let mut reading = transport.read(&context, 4, 2, Duration::ZERO, CancellationToken::never());
    assert!(poll_once(reading.as_mut()).is_pending());
    transport.close(&context).expect("full close");
    transport.close(&context).expect("repeat full close");
    if !completed {
        probe.wait_for(Action::FlushPending).await;
        assert_eq!(evidence.drops.load(Ordering::SeqCst), 0);
        assert!(poll_once(Box::pin(receipt.wait_retired()).as_mut()).is_pending());
        probe.allow_flush();
        probe.wait_for(Action::ShutdownPending).await;
        assert_eq!(evidence.drops.load(Ordering::SeqCst), 0);
        assert!(poll_once(Box::pin(receipt.wait_retired()).as_mut()).is_pending());
        probe.allow_shutdown();
    }
    receipt
        .wait_retired()
        .await
        .expect("graceful I/O and scope retirement");
    let pending = reading
        .await
        .expect("pending read preserves graceful EOF after retirement");
    assert!(pending.is_eof());
    assert_eq!(pending.bytes_read(), 0);
    let later = transport
        .read(&context, 4, 2, Duration::ZERO, CancellationToken::never())
        .await
        .expect("later read preserves graceful EOF");
    assert!(later.is_eof());
    assert_eq!(later.bytes_read(), 0);
    transport
        .close(&context)
        .expect("close remains idempotent after retirement");
    receipt
        .wait_retired()
        .await
        .expect("sticky graceful retirement");
    assert_eq!(probe.0.borrow().flushed, b"buffered");
    assert_eq!(
        probe
            .0
            .borrow()
            .actions
            .iter()
            .filter(|(action, _)| *action == Action::Shutdown)
            .count(),
        1
    );
    assert_eq!(evidence.drops.load(Ordering::SeqCst), 1);
    assert_eq!(evidence.fields.load(Ordering::SeqCst), 1);
    drop(transport);
    assert_eq!(evidence.drops.load(Ordering::SeqCst), 1);
}

#[test]
fn adopted_io_graceful_retirement_preserves_pending_and_later_eof() {
    for half_close in [false, true] {
        on_completion_worker(move |context| {
            graceful_retirement_preserves_eof(context, half_close, false)
        });
    }
}

#[test]
fn adopted_io_completed_half_close_upgrade_preserves_eof() {
    on_completion_worker(|context| graceful_retirement_preserves_eof(context, true, true));
}

#[derive(Default)]
struct ReadWakerDropState {
    armed: AtomicBool,
    panics: AtomicUsize,
    wakes: AtomicUsize,
}

thread_local! {
    static READ_WAKER_DROP_STATE: RefCell<Option<Arc<ReadWakerDropState>>> = const { RefCell::new(None) };
}

const READ_DROP_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    clone_read_drop_waker,
    wake_read_drop_waker,
    wake_read_drop_waker,
    drop_read_drop_waker,
);

fn clone_read_drop_waker(_data: *const ()) -> RawWaker {
    RawWaker::new(std::ptr::null(), &READ_DROP_WAKER_VTABLE)
}

fn wake_read_drop_waker(_data: *const ()) {
    READ_WAKER_DROP_STATE.with(|state| {
        if let Some(state) = state.borrow().as_ref() {
            state.wakes.fetch_add(1, Ordering::SeqCst);
        }
    });
}

fn drop_read_drop_waker(_data: *const ()) {
    let armed = READ_WAKER_DROP_STATE.with(|state| {
        state.borrow().as_ref().is_some_and(|state| {
            if state.armed.swap(false, Ordering::SeqCst) {
                state.panics.fetch_add(1, Ordering::SeqCst);
                true
            } else {
                false
            }
        })
    });
    assert!(!armed, "ready read waker destructor A");
}

fn read_drop_waker() -> Waker {
    unsafe { Waker::from_raw(clone_read_drop_waker(std::ptr::null())) }
}

struct ReadyReadErrorIo {
    inner: Pin<Box<ProbeIo>>,
    field: DestructionField,
    first_read: bool,
    panic_cleanup: bool,
    waker: Arc<ReadWakerDropState>,
    errors: Arc<AtomicUsize>,
    payloads: Arc<AtomicUsize>,
}

impl AsyncRead for ReadyReadErrorIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.first_read {
            self.first_read = false;
            let error = if self.panic_cleanup {
                let error = io::Error::other(ReadyDropError {
                    errors: Arc::clone(&self.errors),
                    payloads: Arc::clone(&self.payloads),
                });
                self.waker.armed.store(true, Ordering::SeqCst);
                error
            } else {
                io::ErrorKind::ConnectionReset.into()
            };
            return Poll::Ready(Err(error));
        }
        self.inner.as_mut().poll_read(context, output)
    }
}

impl AsyncWrite for ReadyReadErrorIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.inner.as_mut().poll_write(context, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.as_mut().poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.as_mut().poll_shutdown(context)
    }
}

impl Drop for ReadyReadErrorIo {
    fn drop(&mut self) {
        self.field.0.assert_pending();
        self.field.0.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn adopted_io_ready_read_error_preserves_first_cleanup_panic() {
    for panic_cleanup in [false, true] {
        on_completion_worker(move |context| async move {
            let probe = Probe::new();
            let evidence = Arc::new(DestructionEvidence {
                owner: std::thread::current().id(),
                drops: AtomicUsize::new(0),
                fields: AtomicUsize::new(0),
                receipt: Mutex::new(None),
            });
            let waker_state = Arc::new(ReadWakerDropState::default());
            READ_WAKER_DROP_STATE.with(|state| {
                assert!(state.replace(Some(Arc::clone(&waker_state))).is_none());
            });
            let errors = Arc::new(AtomicUsize::new(0));
            let payloads = Arc::new(AtomicUsize::new(0));
            let (transport, control) = adopt_local_with_completion(
                &context,
                EstablishedIo::new(
                    ReadyReadErrorIo {
                        inner: Box::pin(ProbeIo {
                            probe: probe.clone(),
                            _pinned: PhantomPinned,
                        }),
                        field: DestructionField(Arc::clone(&evidence)),
                        first_read: true,
                        panic_cleanup,
                        waker: Arc::clone(&waker_state),
                        errors: Arc::clone(&errors),
                        payloads: Arc::clone(&payloads),
                    },
                    TransportEndpoint::UnnamedUnix,
                    TransportEndpoint::Unknown,
                    None,
                ),
                limits(),
            )
            .expect("public read-error adoption");
            let receipt = control.completion();
            *evidence.receipt.lock().expect("receipt") = Some(receipt.clone());
            let mut observing = Box::pin(receipt.wait_retired());
            let notifications = Arc::new(WakeCount::default());
            let notification_waker = Waker::from(Arc::clone(&notifications));
            assert!(poll_with_waker(observing.as_mut(), &notification_waker).is_pending());

            let cleanup_drops = Arc::new(AtomicUsize::new(0));
            let cleanup = CleanupDropCount {
                owner: std::thread::current().id(),
                drops: Arc::clone(&cleanup_drops),
            };
            let (release, released) = oneshot::channel();
            let scope = context
                .register_scope(async move {
                    let _cleanup = cleanup;
                    released.await.expect("independent cleanup released");
                })
                .expect("independent read-error cleanup scope");
            let cleanup_receipt = scope.observe();
            scope
                .activate()
                .expect("activate independent scope")
                .detach();

            let write_count = Arc::new(WakeCount::default());
            let mut writing = transport.write(
                &context,
                queue(&[b"buffered"]),
                Duration::ZERO,
                CancellationToken::never(),
            );
            assert!(
                poll_with_waker(writing.as_mut(), &Waker::from(Arc::clone(&write_count)))
                    .is_pending()
            );
            assert!(probe.saw(Action::FlushPending));
            let mut reading =
                transport.read(&context, 4, 2, Duration::ZERO, CancellationToken::never());
            let waker = read_drop_waker();
            let outcome = catch_unwind(AssertUnwindSafe(|| {
                poll_with_waker(reading.as_mut(), &waker)
            }));
            if panic_cleanup {
                let panic = outcome.expect_err("ready read waker destruction panics");
                assert_eq!(
                    *panic
                        .downcast::<&'static str>()
                        .expect("first destructor panic"),
                    "ready read waker destructor A"
                );
                assert_eq!(waker_state.panics.load(Ordering::SeqCst), 1);
                assert_eq!(
                    waker_state.wakes.load(Ordering::SeqCst),
                    0,
                    "the triggering observer operation was Drop, not wake"
                );
                assert_eq!(errors.load(Ordering::SeqCst), 1);
                assert_eq!(payloads.load(Ordering::SeqCst), 1);
            } else {
                let Poll::Ready(Err(error)) = outcome.expect("ordinary read error does not panic")
                else {
                    panic!("the first read returns its ordinary transport error");
                };
                assert_eq!(error.code(), HttpErrorCode::TRANSPORT_READ_ERROR);
                assert_eq!(waker_state.panics.load(Ordering::SeqCst), 0);
                assert_eq!(errors.load(Ordering::SeqCst), 0);
                assert_eq!(payloads.load(Ordering::SeqCst), 0);
            }
            drop(reading);
            drop(waker);
            assert_eq!(evidence.drops.load(Ordering::SeqCst), 0);
            assert_eq!(evidence.fields.load(Ordering::SeqCst), 0);
            assert_eq!(
                write_count.0.load(Ordering::Relaxed),
                0,
                "read failure does not cancel the pending write"
            );
            assert_eq!(probe.0.borrow().accepted, b"buffered");
            assert!(!probe.saw(Action::Flushed));
            assert!(!probe.saw(Action::ShutdownPending));
            assert!(poll_with_waker(observing.as_mut(), &notification_waker).is_pending());
            probe.0.borrow_mut().inbound.extend(b"read");
            let recovered = transport
                .read(&context, 4, 2, Duration::ZERO, CancellationToken::never())
                .await
                .expect("read error did not close or reset I/O");
            assert_eq!(read_bytes(recovered), b"read");

            transport
                .close(&context)
                .expect("explicit graceful close retains pending write");
            assert_eq!(evidence.drops.load(Ordering::SeqCst), 0);
            assert!(poll_with_waker(observing.as_mut(), &notification_waker).is_pending());
            probe.allow_flush();
            assert!(write_count.0.load(Ordering::Relaxed) > 0);
            assert!(poll_once(writing.as_mut()).is_pending());
            assert!(probe.saw(Action::ShutdownPending));
            assert_eq!(probe.0.borrow().flushed, b"buffered");
            assert_eq!(evidence.drops.load(Ordering::SeqCst), 0);
            assert!(poll_with_waker(observing.as_mut(), &notification_waker).is_pending());
            probe.allow_shutdown();
            let Poll::Ready(Ok(written)) = poll_once(writing.as_mut()) else {
                panic!("released flush and shutdown complete the retained write");
            };
            assert_eq!(written.bytes_written, 8);
            drop(writing);
            assert_eq!(evidence.drops.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.fields.load(Ordering::SeqCst), 1);
            assert!(notifications.0.load(Ordering::Relaxed) > 0);
            assert!(
                poll_with_waker(observing.as_mut(), &notification_waker).is_pending(),
                "I/O destruction cannot bypass the unpolled existing drain scope"
            );
            observing.await.expect("actual I/O and scope retirement");
            receipt
                .wait_retired()
                .await
                .expect("sticky confirmed retirement");
            assert_eq!(cleanup_drops.load(Ordering::SeqCst), 0);
            assert!(poll_once(Box::pin(cleanup_receipt.wait()).as_mut()).is_pending());
            assert!(!release.is_closed());
            release
                .send(())
                .expect("independent cleanup survives read panic");
            cleanup_receipt
                .wait()
                .await
                .expect("independent cleanup actually retires");
            assert_eq!(cleanup_drops.load(Ordering::SeqCst), 1);
            drop(transport);
            assert_eq!(evidence.drops.load(Ordering::SeqCst), 1);
            READ_WAKER_DROP_STATE.with(|state| drop(state.take()));
        });
    }
}

#[derive(Debug)]
enum ScriptedRead {
    /// Fills the offered buffer; bytes beyond it stay first in the script.
    Data(Vec<u8>),
    Eof,
    Error(io::ErrorKind),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReadOffer {
    capacity: usize,
    initialized: usize,
}

#[derive(Default)]
struct ScriptedReadState {
    script: VecDeque<ScriptedRead>,
    offers: Vec<ReadOffer>,
    offer_starts: Vec<usize>,
    waker: Option<Waker>,
    /// Most bytes one poll returns, if limited.
    poll_cap: Option<usize>,
}

/// Adopted I/O that serves a read script and records every buffer offered.
#[derive(Clone, Default)]
struct ScriptedReads(Rc<RefCell<ScriptedReadState>>);

impl ScriptedReads {
    fn adopt(&self, context: &WorkerContext, maximum_read_size: usize) -> OwnedByteStream {
        adopt_local(
            context,
            EstablishedIo::new(
                ScriptedReadIo(self.clone()),
                TransportEndpoint::UnnamedUnix,
                TransportEndpoint::Unknown,
                None,
            ),
            ByteStreamLimits::new(maximum_read_size, 64, 4).expect("nonzero limits"),
        )
        .expect("adopt scripted reads")
    }

    fn push(&self, read: ScriptedRead) {
        let waker = {
            let mut state = self.0.borrow_mut();
            state.script.push_back(read);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn offers(&self) -> Vec<ReadOffer> {
        self.0.borrow().offers.clone()
    }

    fn cap_each_poll(&self, cap: usize) {
        self.0.borrow_mut().poll_cap = Some(cap);
    }

    fn offer_starts(&self) -> Vec<usize> {
        self.0.borrow().offer_starts.clone()
    }

    fn wake_reader(&self) {
        let waker = self.0.borrow_mut().waker.take();
        waker.expect("a pending poll stored its waker").wake();
    }
}

struct ScriptedReadIo(ScriptedReads);

impl AsyncRead for ScriptedReadIo {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut state = self.0.0.borrow_mut();
        state.offers.push(ReadOffer {
            capacity: buffer.remaining(),
            initialized: buffer.initialized().len(),
        });
        state.offer_starts.push(buffer.filled().as_ptr() as usize);
        match state.script.pop_front() {
            None => {
                state.waker = Some(context.waker().clone());
                Poll::Pending
            }
            Some(ScriptedRead::Data(mut bytes)) => {
                let length = bytes
                    .len()
                    .min(buffer.remaining())
                    .min(state.poll_cap.unwrap_or(usize::MAX));
                buffer.put_slice(&bytes[..length]);
                if length < bytes.len() {
                    state
                        .script
                        .push_front(ScriptedRead::Data(bytes.split_off(length)));
                }
                Poll::Ready(Ok(()))
            }
            Some(ScriptedRead::Eof) => Poll::Ready(Ok(())),
            Some(ScriptedRead::Error(kind)) => Poll::Ready(Err(kind.into())),
        }
    }
}

impl AsyncWrite for ScriptedReadIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[test]
fn pending_idle_read_holds_at_most_allocation_size() {
    let mut stream = AdoptedStream::new(ScriptedReadIo(ScriptedReads::default()));
    let mut owned = OwnedRead::new(64 * 1024);
    assert!(
        stream
            .poll_read_owned(
                &mut Context::from_waker(Waker::noop()),
                64 * 1024,
                1_024,
                &mut owned,
            )
            .is_pending()
    );
    assert_eq!(
        owned.held_capacity_for_test(),
        0,
        "a pending adopted read holds no buffer",
    );
    TokioByteStream::cancel_read(&mut stream);

    on_worker(|context| async move {
        let reads = ScriptedReads::default();
        let transport = reads.adopt(&context, 64 * 1024);
        let mut read = transport.read(
            &context,
            64 * 1024,
            1_024,
            Duration::ZERO,
            CancellationToken::never(),
        );
        assert!(poll_once(read.as_mut()).is_pending());
        assert_eq!(
            reads.offers(),
            vec![ReadOffer {
                capacity: 1_024,
                initialized: 0,
            }],
            "an idle read offers allocation_size bytes of uninitialized capacity, \
             not a zero-filled maximum-size buffer",
        );

        reads.push(ScriptedRead::Data(b"response".to_vec()));
        let response = read.await.unwrap();
        let segments = response
            .data()
            .segments()
            .map(|segment| segment.as_ptr() as usize)
            .collect::<Vec<_>>();
        assert_eq!(segments.len(), 1);
        assert!(
            !reads.offer_starts().contains(&segments[0]),
            "the response is an exact-size copy out of the staging buffer, not an offered buffer",
        );
        assert_eq!(
            response.data().backing_capacity_upper_bound(),
            Some(8),
            "the copy holds exactly the 8 bytes returned",
        );
        assert_eq!(read_bytes(response), b"response");
        assert!(
            reads
                .offers()
                .iter()
                .all(|offer| offer.capacity <= 1_024 && offer.initialized == 0),
            "{:?}",
            reads.offers(),
        );
        transport.close_with_reset(&context).unwrap();
    });
}

/// Pins the memory shape recorded as `DIV-017`.
#[test]
fn read_continuations_double_and_hold_under_twice_the_bytes_returned() {
    let read = |payload: usize| {
        let reads = ScriptedReads::default();
        reads.push(ScriptedRead::Data(vec![b'a'; payload]));
        let mut stream = AdoptedStream::new(ScriptedReadIo(reads.clone()));
        let mut owned = OwnedRead::new(64 * 1024);
        let Poll::Ready(eof) = stream.poll_read_owned(
            &mut Context::from_waker(Waker::noop()),
            64 * 1024,
            1_024,
            &mut owned,
        ) else {
            panic!("a read holding bytes does not wait for more");
        };
        assert!(!eof.unwrap());
        let capacities: Vec<usize> = reads.offers().iter().map(|offer| offer.capacity).collect();
        assert_eq!(capacities, vec![1_024, 1_024, 2_048, 4_096]);
        owned
    };

    let short_last_buffer = read(4_097);
    assert_eq!(short_last_buffer.len(), 4_097);
    assert_eq!(
        short_last_buffer.held_capacity_for_test(),
        8_192,
        "the last buffer holds 1 byte in 4_096 bytes of capacity",
    );

    let pending_continuation = read(4_096);
    assert_eq!(
        pending_continuation.held_capacity_for_test(),
        8_192,
        "the pending continuation allocated its 4_096-byte buffer",
    );
    let data = pending_continuation.into_data().unwrap();
    assert_eq!(data.chain_length(), 4_096);
    assert_eq!(
        data.backing_capacity_upper_bound(),
        Some(4_096),
        "the returned data does not keep the pending continuation's buffer",
    );
}

/// Reads `stream` until a read is pending and returns each read's length.
fn read_lengths<S: TokioByteStream>(
    stream: &mut S,
    maximum: usize,
    allocation_size: usize,
) -> Vec<usize> {
    std::iter::from_fn(|| {
        let mut owned = OwnedRead::new(maximum);
        let polled = stream.poll_read_owned(
            &mut Context::from_waker(Waker::noop()),
            maximum,
            allocation_size,
            &mut owned,
        );
        let Poll::Ready(eof) = polled else {
            return None;
        };
        assert!(!eof.unwrap());
        Some(owned.len())
    })
    .collect()
}

/// Pins the adapter cases recorded in `DIV-017`: like Folly's
/// `processNormalRead`, a read ends at the first poll that returns less than
/// it was offered, even if the adapter could return more.
#[test]
fn adopted_read_ends_at_the_first_poll_that_returns_less_than_offered() {
    let capped = ScriptedReads::default();
    capped.cap_each_poll(1_500);
    capped.push(ScriptedRead::Data(vec![b'a'; 5_000]));
    assert_eq!(
        read_lengths(
            &mut AdoptedStream::new(ScriptedReadIo(capped)),
            8_000,
            1_000
        ),
        vec![3_500, 1_500],
        "offers of 1_000, 1_000 and 2_000 bytes get 1_000, 1_000 and 1_500",
    );

    let buffered = || {
        let reads = ScriptedReads::default();
        reads.push(ScriptedRead::Data(vec![b'b'; 10_000]));
        BufReader::with_capacity(8_192, ScriptedReadIo(reads))
    };
    assert_eq!(
        read_lengths(&mut AdoptedStream::new(buffered()), 64 * 1024, 1_000),
        vec![8_192, 1_808],
        "offers smaller than its buffer make `BufReader` buffer 8_192 bytes, so the \
         8_000-byte offer gets only the last 192 of them",
    );
    let mut single = buffered();
    let mut whole = vec![0; 64 * 1024];
    let mut buffer = ReadBuf::new(&mut whole);
    assert!(matches!(
        Pin::new(&mut single).poll_read(&mut Context::from_waker(Waker::noop()), &mut buffer),
        Poll::Ready(Ok(())),
    ));
    assert_eq!(
        buffer.filled().len(),
        10_000,
        "the single maximum-sized poll used before this unit bypasses the buffer",
    );
}

fn completed<T>(mut work: LocalTransportFuture<T>) -> T {
    match poll_once(work.as_mut()) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("the read must complete without polling the I/O again"),
    }
}

#[test]
fn eof_after_gathered_bytes_is_returned_by_the_next_read() {
    on_worker(|context| async move {
        let reads = ScriptedReads::default();
        reads.push(ScriptedRead::Data(vec![b'a'; 1_024]));
        reads.push(ScriptedRead::Eof);
        let transport = reads.adopt(&context, 64 * 1024);
        let read = || {
            transport.read(
                &context,
                64 * 1024,
                1_024,
                Duration::ZERO,
                CancellationToken::never(),
            )
        };

        let data = completed(read()).unwrap();
        assert_eq!(read_bytes(data), vec![b'a'; 1_024]);
        let polls = reads.offers().len();
        assert_eq!(
            polls, 2,
            "the filled first buffer is followed by a poll that sees EOF"
        );
        let eof = completed(read()).unwrap();
        assert!(eof.is_eof());
        assert_eq!(eof.bytes_read(), 0);
        assert_eq!(reads.offers().len(), polls, "the EOF was already observed");
        transport.close_with_reset(&context).unwrap();
    });
}

#[test]
fn error_after_gathered_bytes_is_returned_by_the_next_read() {
    on_worker(|context| async move {
        let reads = ScriptedReads::default();
        reads.push(ScriptedRead::Data(vec![b'a'; 1_024]));
        reads.push(ScriptedRead::Error(io::ErrorKind::ConnectionReset));
        let transport = reads.adopt(&context, 64 * 1024);
        let read = || {
            transport.read(
                &context,
                64 * 1024,
                1_024,
                Duration::ZERO,
                CancellationToken::never(),
            )
        };

        let data = completed(read()).unwrap();
        assert_eq!(read_bytes(data), vec![b'a'; 1_024]);
        let polls = reads.offers().len();
        assert_eq!(
            polls, 2,
            "the filled first buffer is followed by a failing poll"
        );
        let error = completed(read()).unwrap_err();
        assert_eq!(error.code(), HttpErrorCode::TRANSPORT_READ_ERROR);
        let cause = error.source().unwrap().downcast_ref::<io::Error>().unwrap();
        assert_eq!(cause.kind(), io::ErrorKind::ConnectionReset);
        assert_eq!(
            reads.offers().len(),
            polls,
            "the error was already observed"
        );
        transport.close_with_reset(&context).unwrap();
    });
}

#[test]
fn read_that_returns_after_a_pending_poll_releases_its_waker() {
    on_worker(|context| async move {
        let reads = ScriptedReads::default();
        reads.push(ScriptedRead::Data(vec![b'a'; 1_024]));
        let transport = reads.adopt(&context, 64 * 1024);
        let counter = Arc::new(WakeCount::default());
        let waker = Waker::from(Arc::clone(&counter));
        let mut read = transport.read(
            &context,
            64 * 1024,
            1_024,
            Duration::ZERO,
            CancellationToken::never(),
        );

        let Poll::Ready(data) = poll_with_waker(read.as_mut(), &waker) else {
            panic!("a read holding bytes does not wait for more");
        };
        assert_eq!(read_bytes(data.unwrap()), vec![b'a'; 1_024]);
        assert_eq!(
            reads.offers().len(),
            2,
            "the filled first buffer is followed by a pending poll",
        );
        drop(read);
        assert_eq!(
            Arc::strong_count(&counter),
            2,
            "the completed read left no clone of its waker behind",
        );
        let wakes = counter.0.load(Ordering::Relaxed);
        reads.wake_reader();
        assert_eq!(counter.0.load(Ordering::Relaxed), wakes);
        transport.close_with_reset(&context).unwrap();
    });
}

/// Keeps each leaked replacement buffer reachable for LeakSanitizer.
static REPLACEMENT_BUFFERS: Mutex<Vec<usize>> = Mutex::new(Vec::new());

/// Fills the first `genuine_polls` offers, then replaces the `ReadBuf` it is
/// given, as safe code may, with a filled one.
struct ReplacingReadIo {
    genuine_polls: usize,
    polls: Rc<Cell<usize>>,
}

impl AsyncRead for ReplacingReadIo {
    fn poll_read(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let polls = self.polls.get();
        self.polls.set(polls + 1);
        if polls < self.genuine_polls {
            buffer.put_slice(&vec![b'a'; buffer.remaining()]);
            return Poll::Ready(Ok(()));
        }
        let replacement: &'static mut [u8] = Box::leak(vec![b'x'; 16].into_boxed_slice());
        REPLACEMENT_BUFFERS
            .lock()
            .unwrap()
            .push(replacement.as_ptr() as usize);
        *buffer = ReadBuf::new(replacement);
        buffer.set_filled(16);
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ReplacingReadIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn adopt_replacing(
    context: &WorkerContext,
    genuine_polls: usize,
) -> (OwnedByteStream, Rc<Cell<usize>>) {
    let polls = Rc::new(Cell::new(0));
    let transport = adopt_local(
        context,
        EstablishedIo::new(
            ReplacingReadIo {
                genuine_polls,
                polls: Rc::clone(&polls),
            },
            TransportEndpoint::UnnamedUnix,
            TransportEndpoint::Unknown,
            None,
        ),
        ByteStreamLimits::new(64, 64, 4).expect("nonzero limits"),
    )
    .expect("adopt replacing I/O");
    (transport, polls)
}

#[test]
fn read_rejects_an_async_read_that_replaces_its_buffer() {
    on_worker(|context| async move {
        let read = |transport: &OwnedByteStream| {
            transport.read(&context, 64, 32, Duration::ZERO, CancellationToken::never())
        };

        let (transport, _) = adopt_replacing(&context, 0);
        let error = read(&transport)
            .await
            .expect_err("bytes written outside the offered buffer are never exposed");
        assert_eq!(error.code(), HttpErrorCode::TRANSPORT_READ_ERROR);
        transport.close_with_reset(&context).unwrap();

        // On a continuation the failure follows the gathered bytes (`DIV-016`).
        let (transport, polls) = adopt_replacing(&context, 1);
        let data = completed(read(&transport)).unwrap();
        assert_eq!(
            read_bytes(data),
            vec![b'a'; 32],
            "the replacement buffer's bytes are never exposed",
        );
        assert_eq!(
            polls.get(),
            2,
            "the filled offer is followed by a replacing poll"
        );
        let error = completed(read(&transport)).unwrap_err();
        assert_eq!(error.code(), HttpErrorCode::TRANSPORT_READ_ERROR);
        assert_eq!(polls.get(), 2, "the failure was already observed");
        transport.close_with_reset(&context).unwrap();
    });
}

fn data_bytes(data: &BufQueue) -> Vec<u8> {
    data.segments()
        .flat_map(|segment| segment.iter().copied())
        .collect()
}

fn data_starts(data: &BufQueue) -> Vec<usize> {
    data.segments()
        .map(|segment| segment.as_ptr() as usize)
        .collect()
}

/// Polls an adopted read once with a no-op waker.
fn poll_adopted<Io>(
    stream: &mut AdoptedStream<Io>,
    maximum: usize,
    allocation_size: usize,
    owned: &mut OwnedRead,
) -> Poll<io::Result<bool>>
where
    Io: AsyncRead + AsyncWrite + 'static,
{
    stream.poll_read_owned(
        &mut Context::from_waker(Waker::noop()),
        maximum,
        allocation_size,
        owned,
    )
}

#[test]
fn pending_adopted_read_holds_no_buffer_and_returns_an_exact_copy() {
    let reads = ScriptedReads::default();
    let mut stream = AdoptedStream::new(ScriptedReadIo(reads.clone()));
    let mut owned = OwnedRead::new(64 * 1024);
    assert!(poll_adopted(&mut stream, 64 * 1024, 1_024, &mut owned).is_pending());
    assert_eq!(
        owned.held_capacity_for_test(),
        0,
        "a pending adopted read holds no buffer",
    );
    assert_eq!(
        reads.offers(),
        vec![ReadOffer {
            capacity: 1_024,
            initialized: 0,
        }],
        "its first poll is still offered allocation_size bytes of uninitialized capacity",
    );

    reads.push(ScriptedRead::Data(b"response".to_vec()));
    let Poll::Ready(eof) = poll_adopted(&mut stream, 64 * 1024, 1_024, &mut owned) else {
        panic!("the queued response completes the read");
    };
    assert!(!eof.unwrap());
    let data = owned.into_data().unwrap();
    assert_eq!(data_bytes(&data), b"response");
    let starts = data_starts(&data);
    assert_eq!(starts.len(), 1);
    assert!(
        !reads.offer_starts().contains(&starts[0]),
        "the response is an exact-size copy out of the staging buffer",
    );
    assert_eq!(
        data.backing_capacity_upper_bound(),
        Some(8),
        "the copy holds exactly the 8 bytes returned",
    );

    let payload: Vec<u8> = (0..4_097).map(|index| (index % 251) as u8).collect();
    let reads = ScriptedReads::default();
    reads.push(ScriptedRead::Data(payload.clone()));
    let mut stream = AdoptedStream::new(ScriptedReadIo(reads.clone()));
    let mut owned = OwnedRead::new(64 * 1024);
    let Poll::Ready(eof) = poll_adopted(&mut stream, 64 * 1024, 1_024, &mut owned) else {
        panic!("a read holding bytes does not wait for more");
    };
    assert!(!eof.unwrap());
    assert_eq!(
        reads.offers(),
        [1_024, 1_024, 2_048, 4_096]
            .map(|capacity| ReadOffer {
                capacity,
                initialized: 0,
            })
            .to_vec(),
        "a filled first offer continues with DIV-017's offers",
    );
    let data = owned.into_data().unwrap();
    assert!(data_bytes(&data) == payload);
    let starts = data_starts(&data);
    let offer_starts = reads.offer_starts();
    assert!(
        !offer_starts.contains(&starts[0]),
        "the filled first offer is copied out of the staging buffer",
    );
    assert_eq!(
        starts[1..],
        offer_starts[1..],
        "each continuation buffer is the one the stream filled",
    );
    assert_eq!(
        data.backing_capacity_upper_bound(),
        Some(8_192),
        "a 1_024-byte copy, then continuation buffers of 1_024, 2_048 and 4_096 bytes",
    );
}

#[test]
fn first_offer_above_the_staging_bound_keeps_the_heap_buffer() {
    // (first offer, capacity held while pending, response backing capacity,
    // whether the response is in an offered buffer)
    for (allocation_size, held, capacity, offered) in
        [(16_384, 0, 8, false), (16_385, 16_385, 16_385, true)]
    {
        let reads = ScriptedReads::default();
        let mut stream = AdoptedStream::new(ScriptedReadIo(reads.clone()));
        let mut owned = OwnedRead::new(64 * 1024);
        assert!(poll_adopted(&mut stream, 64 * 1024, allocation_size, &mut owned).is_pending());
        assert_eq!(
            owned.held_capacity_for_test(),
            held,
            "a {allocation_size}-byte first offer",
        );

        reads.push(ScriptedRead::Data(b"response".to_vec()));
        let Poll::Ready(eof) = poll_adopted(&mut stream, 64 * 1024, allocation_size, &mut owned)
        else {
            panic!("the queued response completes the read");
        };
        assert!(!eof.unwrap());
        let data = owned.into_data().unwrap();
        assert_eq!(data_bytes(&data), b"response");
        assert_eq!(
            data.backing_capacity_upper_bound(),
            Some(capacity),
            "a {allocation_size}-byte first offer",
        );
        let offer_starts = reads.offer_starts();
        assert_eq!(
            offer_starts.contains(&data_starts(&data)[0]),
            offered,
            "a {allocation_size}-byte first offer",
        );
        assert!(
            reads.offers().iter().all(|offer| *offer
                == ReadOffer {
                    capacity: allocation_size,
                    initialized: 0,
                }),
            "{:?}",
            reads.offers(),
        );
    }
}

/// Initializes every byte it is offered to `0xEE`, then fills only the next
/// scripted count of them, numbered from 1.
struct PartialFillIo(VecDeque<usize>);

impl AsyncRead for PartialFillIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let filled = self.0.pop_front().expect("a scripted fill per poll");
        let unfilled = buffer.initialize_unfilled();
        unfilled.fill(0xEE);
        unfilled[..filled]
            .iter_mut()
            .zip(1..)
            .for_each(|(byte, value)| *byte = value);
        buffer.advance(filled);
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for PartialFillIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[test]
fn staging_read_returns_only_bytes_the_stream_initialized() {
    let mut stream = AdoptedStream::new(PartialFillIo(VecDeque::from([5, 3])));
    for expected in [vec![1, 2, 3, 4, 5], vec![1, 2, 3]] {
        let mut owned = OwnedRead::new(64);
        let Poll::Ready(eof) = poll_adopted(&mut stream, 64, 32, &mut owned) else {
            panic!("a partial fill completes the read");
        };
        assert!(!eof.unwrap());
        let data = owned.into_data().unwrap();
        assert_eq!(
            data_bytes(&data),
            expected,
            "only the filled bytes return, not the initialized 0xEE bytes after them",
        );
        assert_eq!(
            data.backing_capacity_upper_bound(),
            Some(expected.len()),
            "no other byte of the staging buffer reaches the returned data",
        );
    }
}

#[test]
fn staging_read_rejects_an_async_read_that_replaces_its_buffer() {
    let polls = Rc::new(Cell::new(0));
    let mut stream = AdoptedStream::new(ReplacingReadIo {
        genuine_polls: 0,
        polls: Rc::clone(&polls),
    });
    let mut owned = OwnedRead::new(64);
    let Poll::Ready(Err(error)) = poll_adopted(&mut stream, 64, 32, &mut owned) else {
        panic!("a replaced staging buffer fails the read");
    };
    assert_eq!(
        error.to_string(),
        "AsyncRead replaced the read buffer it was given"
    );
    assert_eq!(polls.get(), 1);
    assert!(owned.is_empty(), "none of the replacement's bytes return");

    on_worker(|context| async move {
        let (transport, _) = adopt_replacing(&context, 0);
        let error = transport
            .read(&context, 64, 32, Duration::ZERO, CancellationToken::never())
            .await
            .expect_err("a replaced staging buffer fails the read");
        assert_eq!(error.code(), HttpErrorCode::TRANSPORT_READ_ERROR);
        let cause = error.source().unwrap().downcast_ref::<io::Error>().unwrap();
        assert_eq!(
            cause.to_string(),
            "AsyncRead replaced the read buffer it was given",
            "the error that a replaced continuation buffer also returns",
        );
        transport.close_with_reset(&context).unwrap();
    });
}

#[derive(Clone, Copy, Debug)]
enum ControlledClose {
    Graceful,
    Reset,
    HandleDrop,
}

const CONTROLLED_CLOSES: [ControlledClose; 3] = [
    ControlledClose::Graceful,
    ControlledClose::Reset,
    ControlledClose::HandleDrop,
];

/// A receipt that waits only for `scope`, because its I/O publisher has
/// already published.
fn scope_receipt(scope: &WorkerCleanupObserver) -> AdoptedIoCompletion {
    let published = Arc::new(AdoptedIoState::default());
    assert!(
        IoDropPublisher::new(Arc::clone(&published))
            .finish(true)
            .is_none()
    );
    AdoptedIoCompletion::new(published, scope.clone())
}

/// Adopts as `adopt_local_with_completion` does while keeping the controlled
/// close drain's scope apart from the I/O's destruction publisher. The I/O's
/// destructor asserts that the scope has not retired.
fn controlled_adoption(
    context: &WorkerContext,
    probe: &Probe,
) -> (
    OwnedByteStream,
    WorkerCleanupObserver,
    Arc<AdoptedIoState>,
    Arc<DestructionEvidence>,
) {
    let evidence = Arc::new(DestructionEvidence {
        owner: std::thread::current().id(),
        drops: AtomicUsize::new(0),
        fields: AtomicUsize::new(0),
        receipt: Mutex::new(None),
    });
    let shared = Arc::new(AdoptedIoState::default());
    let (transport, scope) = owned_from_stream_with_completion(
        context,
        AdoptedStream::with_completion(
            DestructionIo {
                inner: Box::pin(ProbeIo {
                    probe: probe.clone(),
                    _pinned: PhantomPinned,
                }),
                field: DestructionField(Arc::clone(&evidence)),
                panic_on_drop: false,
            },
            Arc::clone(&shared),
        ),
        StreamAttachment {
            local_address: SocketAddress::default(),
            peer_address: SocketAddress::default(),
            peer_identity: None,
            limits: limits(),
        },
        Arc::clone(&shared),
    )
    .expect("controlled adoption");
    *evidence.receipt.lock().expect("receipt") = Some(scope_receipt(&scope));
    (transport, scope, shared, evidence)
}

fn dropped_count(probe: &Probe) -> usize {
    probe
        .0
        .borrow()
        .actions
        .iter()
        .filter(|(action, _)| *action == Action::Dropped)
        .count()
}

async fn yield_to_driver() {
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
}

/// Requests `close` and drives it until the adopted I/O is disposed.
async fn dispose_controlled(
    context: &WorkerContext,
    probe: &Probe,
    transport: OwnedByteStream,
    receipt: &AdoptedIoCompletion,
    evidence: &DestructionEvidence,
    close: ControlledClose,
) {
    match close {
        ControlledClose::Graceful => {
            transport.close(context).expect("start graceful close");
            probe.wait_for(Action::FlushPending).await;
            assert!(poll_once(Box::pin(receipt.wait_retired()).as_mut()).is_pending());
            probe.allow_flush();
            probe.wait_for(Action::ShutdownPending).await;
            assert!(poll_once(Box::pin(receipt.wait_retired()).as_mut()).is_pending());
            assert_eq!(evidence.drops.load(Ordering::SeqCst), 0);
            probe.allow_shutdown();
            probe.wait_for(Action::Dropped).await;
            drop(transport);
        }
        ControlledClose::Reset => {
            transport.close_with_reset(context).expect("reset");
            assert_eq!(evidence.drops.load(Ordering::SeqCst), 1);
            drop(transport);
        }
        ControlledClose::HandleDrop => {
            drop(transport);
            assert_eq!(evidence.drops.load(Ordering::SeqCst), 1);
        }
    }
}

#[test]
fn controlled_close_drain_stays_pending_until_disposal_or_cancel() {
    for close in CONTROLLED_CLOSES {
        on_worker(move |context| async move {
            let probe = Probe::new();
            let (transport, scope, _shared, evidence) = controlled_adoption(&context, &probe);
            let receipt = scope_receipt(&scope);
            assert!(
                poll_once(Box::pin(receipt.wait_retired()).as_mut()).is_pending(),
                "{close:?}: the drain is pending between adoption and its first poll",
            );
            yield_to_driver().await;
            assert!(
                poll_once(Box::pin(receipt.wait_retired()).as_mut()).is_pending(),
                "{close:?}: the polled drain stays pending before disposal",
            );
            assert_eq!(evidence.drops.load(Ordering::SeqCst), 0);
            dispose_controlled(&context, &probe, transport, &receipt, &evidence, close).await;
            assert_eq!(receipt.wait_retired().await, Ok(()), "{close:?}");
            assert_eq!(dropped_count(&probe), 1, "{close:?}");
        });
    }

    on_worker(|context| async move {
        let probe = Probe::new();
        let (transport, control, evidence) = observed_adoption(&context, &probe, false);
        let receipt = control.completion();
        assert!(
            poll_once(Box::pin(receipt.wait_retired()).as_mut()).is_pending(),
            "the drain is pending between adoption and its first poll",
        );
        yield_to_driver().await;
        transport.shutdown_write(&context).expect("half close");
        probe.wait_for(Action::FlushPending).await;
        assert!(poll_once(Box::pin(receipt.wait_retired()).as_mut()).is_pending());
        assert_eq!(evidence.drops.load(Ordering::SeqCst), 0);
        control.cancel();
        assert_eq!(receipt.wait_retired().await, Ok(()));
        assert_eq!(evidence.drops.load(Ordering::SeqCst), 1);
        assert_eq!(evidence.fields.load(Ordering::SeqCst), 1);
        assert_eq!(dropped_count(&probe), 1);
        drop(transport);
        assert_eq!(dropped_count(&probe), 1);
    });
}

#[test]
fn controlled_close_drain_publishes_ok_only_after_io_destruction() {
    for close in CONTROLLED_CLOSES {
        on_worker(move |context| async move {
            let probe = Probe::new();
            let (transport, scope, shared, evidence) = controlled_adoption(&context, &probe);
            let receipt = scope_receipt(&scope);
            yield_to_driver().await;
            dispose_controlled(&context, &probe, transport, &receipt, &evidence, close).await;
            assert_eq!(receipt.wait_retired().await, Ok(()), "{close:?}");
            assert_eq!(
                poll_once(
                    Box::pin(AdoptedIoCompletion::new(shared, scope).wait_retired()).as_mut()
                ),
                Poll::Ready(Ok(())),
                "{close:?}: the I/O's destruction published before its scope did",
            );
            assert_eq!(evidence.drops.load(Ordering::SeqCst), 1, "{close:?}");
            assert_eq!(evidence.fields.load(Ordering::SeqCst), 1, "{close:?}");
        });
    }

    on_worker(|context| async move {
        let probe = Probe::new();
        let (transport, control, evidence) = observed_adoption(&context, &probe, false);
        let receipt = control.completion();
        yield_to_driver().await;
        control.cancel();
        assert_eq!(receipt.wait_retired().await, Ok(()));
        assert_eq!(evidence.drops.load(Ordering::SeqCst), 1);
        assert_eq!(evidence.fields.load(Ordering::SeqCst), 1);
        drop(transport);
        assert_eq!(dropped_count(&probe), 1);
    });
}
