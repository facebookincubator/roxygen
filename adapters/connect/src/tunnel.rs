/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Application side of an established tunnel: the choice of its data path, the bounded bridge,
//! its errors, and tunnel info.

use std::cell::Cell;
use std::fmt;
use std::future::Future;
use std::future::pending;
use std::future::poll_fn;
use std::io;
use std::mem;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::ready;
use std::time::Duration;

use futures::task::AtomicWaker;
use http::Response;
use roxygen_connect::async_transport::HttpConnectAsyncTransport;
use roxygen_connect::experimental::ConnectAbortExt;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::DuplexStream;
use tokio::io::ReadBuf;
use tokio::io::copy_bidirectional_with_sizes;
use tokio::io::duplex;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::oneshot;

use crate::error::TunnelFailure;
use crate::relay;
use crate::relay::RelayIo;
use crate::relay::RelayOutcome;
use crate::relay::RelayRequest;
use crate::stats::Counter;
use crate::stats::GaugeHold;
use crate::stats::Recorder;
use crate::worker::RouteUse;
use crate::worker::StreamWatch;
use crate::worker::cancelled;

/// The bridge's and the relay's copy buffer per direction.
pub(crate) const COPY_BUFFER: usize = 16 * 1024;

/// How an established tunnel was set up.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct TunnelInfo {
    /// The tunnel's session had carried an earlier CONNECT stream.
    pub reused_session: bool,
    /// Retries of provably unprocessed CONNECTs before this one succeeded.
    pub retries: u32,
    /// Admission to the stream reservation of the successful attempt.
    pub acquire: Duration,
    /// The successful attempt's request commit to its final response head.
    pub response_head: Duration,
}

/// An established tunnel whose data path the caller has not chosen yet.
///
/// The response head belongs to CONNECT. Until [`into_tunnel`](Self::into_tunnel) or
/// [`relay`](Self::relay) chooses, the adapter sends no tunnel byte and reads nothing from the
/// stream: bytes the proxy sends meanwhile wait in the stream, within `stream_receive_window`,
/// and reach the chosen path first. A pending tunnel has no deadline; until it is chosen or
/// dropped it holds its stream, admission permit, and route. Dropping it unchosen is an abortive
/// close, as dropping a [`Tunnel`] is. A stream that ends before the choice fails the chosen path
/// with that ending.
pub struct PendingTunnel {
    response: Response<()>,
    info: TunnelInfo,
    bridge_capacity: usize,
    cancellation: Arc<CancellationSource>,
    progress: Option<Arc<Progress>>,
    choice: Option<oneshot::Sender<Choice>>,
}

impl fmt::Debug for PendingTunnel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingTunnel")
            .field("status", &self.response.status())
            .field("info", &self.info)
            .finish_non_exhaustive()
    }
}

impl PendingTunnel {
    /// The final 2xx CONNECT response: status, version, and headers.
    pub fn response(&self) -> &Response<()> {
        &self.response
    }

    pub fn info(&self) -> TunnelInfo {
        self.info
    }

    /// Chooses the bridge: a transferable [`Tunnel`] with a bounded buffer in each direction.
    ///
    /// After the runtime shut down, the tunnel's reads, writes, flushes, and shutdowns fail with
    /// `ConnectionAborted`.
    pub fn into_tunnel(mut self) -> Tunnel {
        let (application, worker) = duplex(self.bridge_capacity);
        if let Some(choice) = self.choice.take() {
            let _ = choice.send(Choice::Bridge(worker));
        }
        Tunnel {
            io: application,
            response: mem::take(&mut self.response),
            info: self.info,
            cancellation: Arc::clone(&self.cancellation),
            progress: self
                .progress
                .take()
                .expect("should share its progress until it is chosen"),
            submitted: 0,
        }
    }

    /// Chooses the relay: hands `io` to the protocol worker, which copies between it and the
    /// stream in the tunnel's own task, and returns the relay's outcome.
    ///
    /// The relay starts when this is called; the returned future only reports its outcome and
    /// may be awaited on any executor. Dropping the future before the stream is closed on the
    /// wire resets the stream and closes `io`. The relay forwards end of stream in each direction
    /// as a half-close. When the proxy resets the stream, its session fails, or a PING probe
    /// goes unanswered, the relay first writes the rest of the download to `io` if it has read
    /// the download's END_STREAM (after an upload write error it reads on while only the stream
    /// holds it up), then shuts down `io`'s write half, together for at most
    /// `shutdown_timeout`, and closes it; if `io` is a socket still holding application bytes
    /// the relay has not read, Linux then resets the connection, discarding unsent data. An error of `io`
    /// resets the stream. The relay returns its admission permit before its outcome resolves. After the
    /// runtime shut down, the relay closes `io` and resolves `Aborted`.
    pub fn relay(
        mut self,
        io: impl Into<RelayIo>,
    ) -> impl Future<Output = RelayOutcome> + Send + 'static {
        let choice = self.choice.take();
        // Only the worker keeps the relay's admission permit, so it returns with the relay's work.
        drop(self.progress.take());
        relay::start(choice, io.into(), Arc::clone(&self.cancellation))
    }

    /// Counts the tunnel as returned by `connect()`, with how it ended if that came first.
    pub(crate) fn received(&self, stats: &Recorder) {
        stats.tunnels_established.add();
        if self.info.reused_session {
            stats.streams_on_reused_session.add();
        }
        if let Some(ended) = self
            .progress
            .as_ref()
            .and_then(|progress| progress.receive())
        {
            ended(stats).add();
        }
    }
}

impl Drop for PendingTunnel {
    fn drop(&mut self) {
        if self.choice.is_some() {
            self.cancellation.request_cancellation();
        }
    }
}

/// The data path that a pending tunnel's caller chose.
pub(crate) enum Choice {
    /// The worker half of the bridge.
    Bridge(DuplexStream),
    Relay(RelayRequest),
}

/// A transferable tunnel with a bounded buffer in each direction.
///
/// The response head belongs to CONNECT; tunnel bytes have no HTTP framing. `poll_flush`
/// completes once the native stream accepted the written bytes, not when they reach the wire.
/// `poll_shutdown` queues END_STREAM after the queued bytes and keeps the download open. A proxy
/// reset or session failure fails reads with `ConnectionReset` instead of EOF, and later writes,
/// flushes, and shutdowns too, also when it comes after the download's end of stream but before
/// the upload's END_STREAM went out. An unanswered PING
/// probe (`ping_timeout`) fails reads, writes, flushes, and shutdowns with `TimedOut`, unless the
/// download already reached END_STREAM: then the failure is `ConnectionReset`. A probe drop
/// before the response head retries or fails the connect instead, by pooled CONNECT specification
/// D8 and the acquire window (`ConnectOptions::ping_timeout`).
///
/// Dropping the tunnel before its stream is wire-complete is an abortive close: it resets the
/// stream, and upload bytes not yet sent may be discarded, also after `poll_shutdown` and end
/// of stream while the proxy's flow control still holds them.
pub struct Tunnel {
    io: DuplexStream,
    response: Response<()>,
    info: TunnelInfo,
    cancellation: Arc<CancellationSource>,
    progress: Arc<Progress>,
    submitted: u64,
}

impl fmt::Debug for Tunnel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Tunnel")
            .field("status", &self.response.status())
            .field("info", &self.info)
            .finish_non_exhaustive()
    }
}

impl Tunnel {
    /// The final 2xx CONNECT response: status, version, and headers.
    pub fn response(&self) -> &Response<()> {
        &self.response
    }

    pub fn info(&self) -> TunnelInfo {
        self.info
    }
}

impl AsyncRead for Tunnel {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buffer.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let this = self.get_mut();
        let before = buffer.filled().len();
        ready!(Pin::new(&mut this.io).poll_read(context, buffer))?;
        if buffer.filled().len() == before
            && let Some(error) = this.progress.error()
        {
            return Poll::Ready(Err(error));
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for Tunnel {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if let Some(error) = this.progress.error() {
            return Poll::Ready(Err(error));
        }
        match ready!(Pin::new(&mut this.io).poll_write(context, buffer)) {
            Ok(written) => {
                this.submitted = this.submitted.saturating_add(written as u64);
                Poll::Ready(Ok(written))
            }
            Err(error) => Poll::Ready(Err(this.progress.error().unwrap_or(error))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.progress.poll_flushed(context, self.submitted, false)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if let Some(error) = this.progress.error() {
            return Poll::Ready(Err(error));
        }
        this.progress.write_shutdown.store(true, Ordering::Release);
        if let Err(error) = ready!(Pin::new(&mut this.io).poll_shutdown(context)) {
            return Poll::Ready(Err(this.progress.error().unwrap_or(error)));
        }
        this.progress.poll_flushed(context, this.submitted, true)
    }
}

impl Drop for Tunnel {
    fn drop(&mut self) {
        self.cancellation.request_cancellation();
    }
}

/// The counter of how a tunnel ended.
pub(crate) type Ending = fn(&Recorder) -> &Counter;

/// Whether `connect()` returned the pending tunnel. Only returned tunnels count as established,
/// reset, or aborted, so an ending that comes first waits for the receipt.
enum Delivery {
    Pending(Option<Ending>),
    Received,
}

/// State shared by a tunnel and its worker-side job. It owns the connect's admission permit, so
/// the permit returns once both sides are gone.
pub(crate) struct Progress {
    _permit: OwnedSemaphorePermit,
    failure: Mutex<Option<Arc<io::Error>>>,
    delivery: Mutex<Delivery>,
    flushed: AtomicU64,
    shutdown_flushed: AtomicBool,
    /// The application shut down its write half, so the bridge's EOF is not a drop.
    write_shutdown: AtomicBool,
    writer: AtomicWaker,
}

impl Progress {
    pub(crate) fn new(permit: OwnedSemaphorePermit) -> Self {
        Self {
            _permit: permit,
            failure: Mutex::new(None),
            delivery: Mutex::new(Delivery::Pending(None)),
            flushed: AtomicU64::new(0),
            shutdown_flushed: AtomicBool::new(false),
            write_shutdown: AtomicBool::new(false),
            writer: AtomicWaker::new(),
        }
    }

    /// Marks the tunnel received; returns how it already ended, if it did.
    fn receive(&self) -> Option<Ending> {
        let mut delivery = self
            .delivery
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match mem::replace(&mut *delivery, Delivery::Received) {
            Delivery::Pending(ended) => ended,
            Delivery::Received => None,
        }
    }

    /// Records how the tunnel ended; returns whether it counts now, since it was received.
    pub(crate) fn end(&self, ending: Ending) -> bool {
        let mut delivery = self
            .delivery
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match &mut *delivery {
            Delivery::Pending(ended) => {
                *ended = Some(ending);
                false
            }
            Delivery::Received => true,
        }
    }

    /// Records the tunnel's failure without waking the application.
    fn fail(&self, error: io::Error) {
        self.failure
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get_or_insert_with(|| Arc::new(error));
    }

    fn error(&self) -> Option<io::Error> {
        self.failure
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map(|error| io::Error::new(error.kind(), Arc::clone(error)))
    }

    fn poll_flushed(
        &self,
        context: &mut Context<'_>,
        target: u64,
        shutdown: bool,
    ) -> Poll<io::Result<()>> {
        self.writer.register(context.waker());
        if let Some(error) = self.error() {
            return Poll::Ready(Err(error));
        }
        if self.flushed.load(Ordering::Acquire) >= target
            && (!shutdown || self.shutdown_flushed.load(Ordering::Acquire))
        {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

/// The worker half of the bridge. An EOF that the application did not request with
/// `poll_shutdown` means the tunnel was dropped, which must not reach the proxy as END_STREAM.
struct BridgeIo {
    io: DuplexStream,
    progress: Arc<Progress>,
}

impl AsyncRead for BridgeIo {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buffer.filled().len();
        ready!(Pin::new(&mut this.io).poll_read(context, buffer))?;
        if buffer.filled().len() == before
            && buffer.remaining() != 0
            && !this.progress.write_shutdown.load(Ordering::Acquire)
        {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "CONNECT tunnel dropped",
            )));
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for BridgeIo {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write(context, buffer)
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(context)
    }
}

/// The native transport, publishing how many written bytes it accepted.
struct ObservedTransport {
    native: HttpConnectAsyncTransport,
    progress: Arc<Progress>,
    written: u64,
    /// The download reached end of stream; shared, so the bridge sees it while the copy holds
    /// this.
    read_ended: Rc<Cell<bool>>,
    /// A write, flush, or shutdown failed, so the stream's read error explains the failure.
    write_failed: bool,
}

impl ObservedTransport {
    fn flushed(&self, shutdown: bool) {
        self.progress.flushed.store(self.written, Ordering::Release);
        if shutdown {
            self.progress
                .shutdown_flushed
                .store(true, Ordering::Release);
        }
        self.progress.writer.wake();
    }
}

impl AsyncRead for ObservedTransport {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buffer.filled().len();
        ready!(Pin::new(&mut this.native).poll_read(context, buffer))?;
        if buffer.filled().len() == before && buffer.remaining() != 0 {
            this.read_ended.set(true);
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ObservedTransport {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let written = ready!(Pin::new(&mut this.native).poll_write(context, buffer))
            .inspect_err(|_| this.write_failed = true)?;
        this.written = this.written.saturating_add(written as u64);
        Poll::Ready(Ok(written))
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(Pin::new(&mut this.native).poll_flush(context))
            .inspect_err(|_| this.write_failed = true)?;
        this.flushed(false);
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(Pin::new(&mut this.native).poll_shutdown(context))
            .inspect_err(|_| this.write_failed = true)?;
        this.flushed(true);
        Poll::Ready(Ok(()))
    }
}

/// After a native write, flush, or shutdown error, reads the stream to its end for at most
/// `limit`, discarding what it reads, and returns the read error that explains the failure, if
/// one arrives: a write error carries only `DROPPED`.
pub(crate) async fn read_error_after_write_error<Io>(
    native: &mut Io,
    limit: Duration,
) -> Option<io::Error>
where
    Io: AsyncRead + Unpin,
{
    let mut discarded = vec![0; COPY_BUFFER];
    let drained = tokio::time::timeout(limit, async {
        loop {
            match native.read(&mut discarded).await {
                Ok(0) => return None,
                Ok(_) => {}
                Err(error) => return Some(error),
            }
        }
    })
    .await;
    drained.ok().flatten()
}

/// Resolves once a peer reset or session failure closes the stream after the copy read the
/// download's end: from then on the copy waits only for the application, so the stream's read
/// cannot report the ending. A stream closed after its upload's END_STREAM went out finishes
/// through the copy instead.
pub(crate) async fn closed_after_download(watch: &StreamWatch, download_ended: &Cell<bool>) {
    watch.wait_closed().await;
    if watch.upload_ended() {
        return pending().await;
    }
    // Only the copy sets `download_ended`, in the same task and just before the select that holds
    // both polls this, so the check needs no waker of its own.
    poll_fn(|_| {
        if download_ended.get() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
}

/// The failure of a stream that a peer reset or session failure closed before its upload's
/// END_STREAM went out, also after both copy directions ended.
pub(crate) fn upload_discarded() -> io::Error {
    io::Error::new(
        io::ErrorKind::ConnectionReset,
        "CONNECT stream ended before its upload completed on the wire",
    )
}

/// Records how a tunnel ended: counts the ending once the tunnel is delivered, publishes the
/// failure to its application side, and resets the stream.
fn end_tunnel(
    progress: &Progress,
    stats: &Recorder,
    watch: &StreamWatch,
    transport: &mut HttpConnectAsyncTransport,
    failure: TunnelFailure,
) {
    let ending: Option<Ending> = match failure.kind() {
        io::ErrorKind::ConnectionReset => Some(|stats| &stats.tunnels_reset_by_peer),
        io::ErrorKind::ConnectionAborted if !watch.closed() => Some(|stats| &stats.tunnels_aborted),
        _ => None,
    };
    if let Some(ending) = ending
        && progress.end(ending)
    {
        ending(stats).add();
    }
    // Publish before the bridge closes, so a reset never reads as EOF, and reset before any
    // application waker runs, so a panicking waker cannot keep the stream open.
    progress.fail(failure.into_io_error());
    transport.abort();
    progress.writer.wake();
}

/// What a worker needs to build an established tunnel.
pub(crate) struct Established {
    pub(crate) transport: HttpConnectAsyncTransport,
    pub(crate) watch: Rc<StreamWatch>,
    pub(crate) response: Response<()>,
    pub(crate) info: TunnelInfo,
}

/// The worker's hold on an established stream: its transport, its tunnel state shared with the
/// application, and its `active_tunnels` count.
pub(crate) struct StreamHold {
    pub(crate) transport: HttpConnectAsyncTransport,
    pub(crate) watch: Rc<StreamWatch>,
    pub(crate) progress: Arc<Progress>,
    pub(crate) stats: Arc<Recorder>,
    pub(crate) active: GaugeHold,
}

impl StreamHold {
    fn end(mut self, failure: TunnelFailure) {
        end_tunnel(
            &self.progress,
            &self.stats,
            &self.watch,
            &mut self.transport,
            failure,
        );
    }
}

/// The worker side of an established tunnel until its caller chooses a data path, owned by the
/// tunnel's task. Dropped before the choice, as by a forced shutdown, it fails a later `Tunnel`
/// with `ConnectionAborted`, resets the stream, and releases its route use before a choice still
/// in flight closes.
pub(crate) struct Unchosen {
    hold: Option<StreamHold>,
    route: Option<RouteUse>,
    chosen: oneshot::Receiver<Choice>,
}

impl Unchosen {
    pub(crate) fn new(
        established: Established,
        bridge_capacity: usize,
        permit: OwnedSemaphorePermit,
        route: RouteUse,
        cancellation: Arc<CancellationSource>,
        stats: Arc<Recorder>,
    ) -> (PendingTunnel, Self) {
        let progress = Arc::new(Progress::new(permit));
        let (choice, chosen) = oneshot::channel();
        let pending = PendingTunnel {
            response: established.response,
            info: established.info,
            bridge_capacity,
            cancellation,
            progress: Some(Arc::clone(&progress)),
            choice: Some(choice),
        };
        let hold = StreamHold {
            transport: established.transport,
            watch: established.watch,
            progress,
            active: GaugeHold::new(Arc::clone(&stats), |stats| &stats.active_tunnels),
            stats,
        };
        let unchosen = Self {
            hold: Some(hold),
            route: Some(route),
            chosen,
        };
        (pending, unchosen)
    }

    /// The caller's choice, or `None` once its pending tunnel was dropped unchosen.
    pub(crate) async fn choice(&mut self, dropped: CancellationToken) -> Option<Choice> {
        tokio::select! {
            biased;
            choice = &mut self.chosen => choice.ok(),
            () = cancelled(dropped) => None,
        }
    }

    /// Ends a tunnel dropped unchosen: resets its stream unless it is closed on the wire.
    pub(crate) fn abandon(mut self) {
        if let Some(hold) = self.hold.take() {
            hold.end(TunnelFailure::Aborted("CONNECT tunnel dropped"));
        }
    }

    /// The stream and route use, for a bridge or relay that takes over their release.
    pub(crate) fn choose(mut self) -> (StreamHold, RouteUse) {
        let hold = self
            .hold
            .take()
            .expect("should hold the stream until the choice");
        let route = self
            .route
            .take()
            .expect("should hold the route use until the choice");
        (hold, route)
    }
}

impl Drop for Unchosen {
    fn drop(&mut self) {
        if let Some(hold) = self.hold.take() {
            hold.end(TunnelFailure::Aborted("CONNECT tunnel job ended"));
        }
        // `chosen` drops after this, so a choice still in it closes only once the failure is
        // published and admission and the route use are released.
        drop(self.route.take());
    }
}

/// The worker side of a bridged tunnel, owned by its task. Until it records how the tunnel ended,
/// dropping it fails the application side with `ConnectionAborted` and resets the stream.
pub(crate) struct Bridge {
    bridge: BridgeIo,
    transport: ObservedTransport,
    watch: Rc<StreamWatch>,
    stats: Arc<Recorder>,
    _active: GaugeHold,
    shutdown_timeout: Duration,
    ended: bool,
}

impl Bridge {
    pub(crate) fn new(hold: StreamHold, worker: DuplexStream, shutdown_timeout: Duration) -> Self {
        let StreamHold {
            transport,
            watch,
            progress,
            stats,
            active,
        } = hold;
        Self {
            bridge: BridgeIo {
                io: worker,
                progress: Arc::clone(&progress),
            },
            transport: ObservedTransport {
                native: transport,
                progress,
                written: 0,
                read_ended: Rc::default(),
                write_failed: false,
            },
            watch,
            stats,
            _active: active,
            shutdown_timeout,
            ended: false,
        }
    }

    /// Copies tunnel bytes until both directions finish and the stream is closed on the wire, the
    /// tunnel is dropped, or either side fails.
    pub(crate) async fn run(&mut self, dropped: CancellationToken) {
        let download_ended = Rc::clone(&self.transport.read_ended);
        let copied = tokio::select! {
            biased;
            () = cancelled(dropped.clone()) => None,
            copied = copy_bidirectional_with_sizes(
                &mut self.bridge,
                &mut self.transport,
                COPY_BUFFER,
                COPY_BUFFER,
            ) => Some(copied),
            () = closed_after_download(&self.watch, &download_ended) => {
                self.fail(TunnelFailure::Native(upload_discarded()));
                return;
            }
        };
        let failure = match copied {
            // Flow control may still hold the upload and its END_STREAM, which a drop must reset
            // and a peer reset or session failure discards.
            Some(Ok(_)) => tokio::select! {
                biased;
                () = self.watch.wait_closed() => {
                    if self.watch.upload_ended() {
                        self.ended = true;
                        return;
                    }
                    TunnelFailure::Native(upload_discarded())
                }
                () = cancelled(dropped) => TunnelFailure::Aborted("CONNECT tunnel dropped"),
            },
            // A bridge error after the tunnel dropped comes from its closed application half.
            Some(Err(error)) if !dropped.is_cancellation_requested() => {
                if self.transport.write_failed {
                    tokio::select! {
                        biased;
                        () = cancelled(dropped) => TunnelFailure::Aborted("CONNECT tunnel dropped"),
                        read = read_error_after_write_error(
                            &mut self.transport.native,
                            self.shutdown_timeout,
                        ) => TunnelFailure::Native(read.unwrap_or(error)),
                    }
                } else {
                    TunnelFailure::Native(error)
                }
            }
            _ => TunnelFailure::Aborted("CONNECT tunnel dropped"),
        };
        self.fail(failure);
    }

    fn fail(&mut self, failure: TunnelFailure) {
        self.ended = true;
        end_tunnel(
            &self.transport.progress,
            &self.stats,
            &self.watch,
            &mut self.transport.native,
            failure,
        );
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        if !self.ended {
            self.fail(TunnelFailure::Aborted("CONNECT tunnel job ended"));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;

    use futures::task::noop_waker;
    use tokio::io::AsyncRead;
    use tokio::io::ReadBuf;
    use tokio::io::duplex;
    use tokio::sync::Semaphore;

    use super::BridgeIo;
    use super::Progress;

    fn read_after_the_application_closed(write_shutdown: bool) -> io::Result<usize> {
        let (application, worker) = duplex(64);
        drop(application);
        let progress = Arc::new(Progress::new(
            Arc::new(Semaphore::new(1))
                .try_acquire_owned()
                .expect("permit"),
        ));
        progress
            .write_shutdown
            .store(write_shutdown, Ordering::Release);
        let mut bridge = BridgeIo {
            io: worker,
            progress,
        };
        let mut storage = [0; 8];
        let mut buffer = ReadBuf::new(&mut storage);
        let waker = noop_waker();
        match Pin::new(&mut bridge).poll_read(&mut Context::from_waker(&waker), &mut buffer) {
            Poll::Ready(result) => result.map(|()| buffer.filled().len()),
            Poll::Pending => panic!("a closed duplex half is readable"),
        }
    }

    #[test]
    fn application_eof_is_an_abort_unless_its_writes_were_shut_down() {
        let dropped = read_after_the_application_closed(false).expect_err("a dropped tunnel");
        assert_eq!(dropped.kind(), io::ErrorKind::ConnectionAborted);
        let finished = read_after_the_application_closed(true).expect("a shut-down tunnel");
        assert_eq!(
            finished, 0,
            "poll_shutdown makes the bridge's EOF an upload END_STREAM"
        );
    }
}
