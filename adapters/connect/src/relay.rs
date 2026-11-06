//! Worker-side relay of an established tunnel to the application's own I/O.

use std::future::Future;
use std::future::poll_fn;
use std::io;
use std::io::IoSlice;
use std::mem;
use std::net::TcpStream;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::ready;
use std::time::Duration;

use roxygen_connect::async_transport::HttpConnectAsyncTransport;
use roxygen_connect::experimental::ConnectAbortExt;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::ReadBuf;
use tokio::sync::oneshot;
use tokio::task::coop;
use tokio::time::Instant;

use crate::error::TunnelFailure;
use crate::runtime::CancelOnDrop;
use crate::runtime::ProxyIo;
use crate::stats::Recorder;
use crate::tunnel::COPY_BUFFER;
use crate::tunnel::Choice;
use crate::tunnel::Ending;
use crate::tunnel::read_error_after_write_error;
use crate::tunnel::upload_discarded;
use crate::worker::Settlement;
use crate::worker::StreamWatch;
use crate::worker::cancelled;

/// The application I/O that `PendingTunnel::relay` hands to the protocol worker.
pub enum RelayIo {
    /// A socket that the worker sets non-blocking and registers with its own reactor, so the
    /// relay is independent of the runtime the socket came from. A Tokio caller converts its
    /// stream with `into_std()` and hands over its only handle; bytes it only peeked stay in the
    /// socket and are relayed first.
    Tcp(TcpStream),
    /// Any I/O, polled on the worker but driven by the reactor of the runtime that created it,
    /// which must keep running for the relay's life: once it stops, the I/O's error ends the
    /// relay with `Application`.
    Boxed(Box<dyn ProxyIo>),
}

impl From<TcpStream> for RelayIo {
    fn from(socket: TcpStream) -> Self {
        Self::Tcp(socket)
    }
}

impl From<Box<dyn ProxyIo>> for RelayIo {
    fn from(io: Box<dyn ProxyIo>) -> Self {
        Self::Boxed(io)
    }
}

/// How a relay ended.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RelayClose {
    /// Both directions ended, the session wrote the upload's END_STREAM onto the connection's
    /// egress, and the stream is closed on the wire.
    Finished,
    /// The proxy reset the stream, a GOAWAY ended it with an error, or its session failed, also
    /// after both directions ended while flow control still held upload bytes or their end, and
    /// a PING probe drop after the download reached END_STREAM.
    /// [`RelayOutcome::stream_reset_by_peer`] tells the proxy's RST_STREAM from the rest.
    PeerReset,
    /// The stream's session was dropped after an unanswered PING probe before the download
    /// reached END_STREAM.
    TimedOut,
    /// The application I/O failed; the relay reset the stream.
    Application,
    /// The relay future was dropped, the runtime shut down, or the relay's job was lost; the
    /// relay reset the stream. A relay that had already recorded another ending, as one writing
    /// the rest of the download after a peer reset does, keeps that ending instead.
    Aborted,
}

/// The bytes a relay moved and how it ended.
#[derive(Debug)]
pub struct RelayOutcome {
    relayed: Relayed,
    close: RelayClose,
    stream_reset_by_peer: bool,
    error: Option<io::Error>,
}

impl RelayOutcome {
    fn aborted(relayed: Relayed) -> Self {
        Self {
            relayed,
            close: RelayClose::Aborted,
            stream_reset_by_peer: false,
            error: Some(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "CONNECT relay aborted",
            )),
        }
    }

    /// Bytes that the stream accepted from the application.
    pub fn uploaded(&self) -> u64 {
        self.relayed.uploaded
    }

    /// Bytes written to the application.
    pub fn downloaded(&self) -> u64 {
        self.relayed.downloaded
    }

    /// Whether the relay wrote the download through its END_STREAM to the application I/O and
    /// shut down the I/O's write half. Written is not read: on a socket the bytes and the end may
    /// still be queued. Always true for `Finished`. Once the relay has read the END_STREAM, a
    /// later ending gives it up to `shutdown_timeout` to finish this, so it is false only when
    /// the ending came first, or the bound, an application I/O error, or a local abort (the
    /// relay future dropped or the runtime's forced shutdown) cut the writing short.
    pub fn download_ended(&self) -> bool {
        self.relayed.download_ended
    }

    /// Whether the relay read the download's END_STREAM, whether or not it then wrote the whole
    /// download to the application, also when it read it while discarding the download after an
    /// upload write error.
    pub fn peer_ended_download(&self) -> bool {
        self.relayed.peer_ended_download
    }

    pub fn close(&self) -> RelayClose {
        self.close
    }

    /// Whether a `PeerReset` relay ended at the proxy's RST_STREAM on its own stream, rather
    /// than at an error GOAWAY, a session failure, or a PING probe drop. A `REFUSED_STREAM`
    /// reset never sets it: it refuses a CONNECT before it commits. With
    /// [`peer_ended_download`](Self::peer_ended_download), it marks a proxy that ended its side
    /// and then reset the stream, as one that closes first may.
    pub fn stream_reset_by_peer(&self) -> bool {
        self.stream_reset_by_peer
    }

    /// The error of every ending but `Finished`: `ConnectionReset`, `TimedOut`, the application
    /// I/O's own error, or `ConnectionAborted`.
    pub fn error(&self) -> Option<&io::Error> {
        self.error.as_ref()
    }
}

/// What a relay moved, as its outcome reports it.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Relayed {
    pub(crate) uploaded: u64,
    pub(crate) downloaded: u64,
    pub(crate) download_ended: bool,
    pub(crate) peer_ended_download: bool,
}

/// A relay choice on its way to the worker.
pub(crate) struct RelayRequest {
    io: RelayIo,
    outcome: oneshot::Sender<RelayOutcome>,
}

impl RelayRequest {
    pub(crate) fn into_parts(self) -> (RelayIo, oneshot::Sender<RelayOutcome>) {
        (self.io, self.outcome)
    }
}

/// Publishes a relay's outcome as it drops: the ending the relay recorded, or `Aborted` when its
/// job was lost first, with what it relayed so far.
pub(crate) struct Publisher {
    outcome: Option<oneshot::Sender<RelayOutcome>>,
    relayed: Relayed,
    ending: Option<(RelayClose, bool, Option<io::Error>)>,
}

impl Publisher {
    pub(crate) fn new(outcome: oneshot::Sender<RelayOutcome>) -> Self {
        Self {
            outcome: Some(outcome),
            relayed: Relayed::default(),
            ending: None,
        }
    }

    pub(crate) fn relayed(&mut self, relayed: Relayed) {
        self.relayed = relayed;
    }

    pub(crate) fn ended(
        &mut self,
        close: RelayClose,
        stream_reset_by_peer: bool,
        error: Option<io::Error>,
    ) {
        self.ending = Some((close, stream_reset_by_peer, error));
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        let Some(sender) = self.outcome.take() else {
            return;
        };
        let outcome = match self.ending.take() {
            Some((close, stream_reset_by_peer, error)) => RelayOutcome {
                relayed: self.relayed,
                close,
                stream_reset_by_peer,
                error,
            },
            None => RelayOutcome::aborted(self.relayed),
        };
        let _ = sender.send(outcome);
    }
}

/// Delivers a relay choice to the tunnel's task and returns the future of its outcome. A worker
/// that is gone refuses the choice, which closes the I/O as it drops and resolves the relay
/// `Aborted`.
pub(crate) fn start(
    choice: Option<oneshot::Sender<Choice>>,
    io: RelayIo,
    cancellation: Arc<CancellationSource>,
) -> impl Future<Output = RelayOutcome> + Send + 'static {
    let (outcome, published) = oneshot::channel();
    if let Some(choice) = choice {
        let _ = choice.send(Choice::Relay(RelayRequest { io, outcome }));
    }
    let mut dropped = CancelOnDrop {
        source: cancellation,
        armed: true,
    };
    async move {
        let outcome = published
            .await
            .unwrap_or_else(|_| RelayOutcome::aborted(Relayed::default()));
        dropped.disarm();
        outcome
    }
}

/// The application I/O as the worker polls it.
enum ApplicationIo {
    Tcp(tokio::net::TcpStream),
    Boxed(Box<dyn ProxyIo>),
}

impl ApplicationIo {
    /// Registers a socket with the reactor of the worker that runs this.
    fn adopt(io: RelayIo) -> io::Result<Self> {
        match io {
            RelayIo::Tcp(socket) => {
                socket.set_nonblocking(true)?;
                Ok(Self::Tcp(tokio::net::TcpStream::from_std(socket)?))
            }
            RelayIo::Boxed(io) => Ok(Self::Boxed(io)),
        }
    }
}

impl AsyncRead for ApplicationIo {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(socket) => Pin::new(socket).poll_read(context, buffer),
            Self::Boxed(io) => Pin::new(io).poll_read(context, buffer),
        }
    }
}

impl AsyncWrite for ApplicationIo {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Tcp(socket) => Pin::new(socket).poll_write(context, buffer),
            Self::Boxed(io) => Pin::new(io).poll_write(context, buffer),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Tcp(socket) => Pin::new(socket).poll_write_vectored(context, buffers),
            Self::Boxed(io) => Pin::new(io).poll_write_vectored(context, buffers),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Tcp(socket) => socket.is_write_vectored(),
            Self::Boxed(io) => io.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(socket) => Pin::new(socket).poll_flush(context),
            Self::Boxed(io) => Pin::new(io).poll_flush(context),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(socket) => Pin::new(socket).poll_shutdown(context),
            Self::Boxed(io) => Pin::new(io).poll_shutdown(context),
        }
    }
}

/// One side of a relay: counts the bytes written to it and records which of its directions
/// failed and whether its reads reached end of stream.
struct Tracked<Io> {
    io: Io,
    written: u64,
    read_ended: bool,
    read_failed: bool,
    write_failed: bool,
    shut_down: bool,
}

impl<Io> Tracked<Io> {
    fn new(io: Io) -> Self {
        Self {
            io,
            written: 0,
            read_ended: false,
            read_failed: false,
            write_failed: false,
            shut_down: false,
        }
    }

    fn failed(&self) -> bool {
        self.read_failed || self.write_failed
    }
}

impl<Io> AsyncRead for Tracked<Io>
where
    Io: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buffer.filled().len();
        let read = ready!(Pin::new(&mut this.io).poll_read(context, buffer));
        match &read {
            Ok(()) => this.read_ended |= buffer.filled().len() == before && buffer.remaining() != 0,
            Err(_) => this.read_failed = true,
        }
        Poll::Ready(read)
    }
}

impl<Io> AsyncWrite for Tracked<Io>
where
    Io: AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let written = ready!(Pin::new(&mut this.io).poll_write(context, buffer));
        match &written {
            // The copy fails a writer that accepts nothing.
            Ok(0) if !buffer.is_empty() => this.write_failed = true,
            Ok(written) => this.written = this.written.saturating_add(*written as u64),
            Err(_) => this.write_failed = true,
        }
        Poll::Ready(written)
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let flushed = ready!(Pin::new(&mut this.io).poll_flush(context));
        this.write_failed |= flushed.is_err();
        Poll::Ready(flushed)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let shut_down = ready!(Pin::new(&mut this.io).poll_shutdown(context));
        this.write_failed |= shut_down.is_err();
        this.shut_down |= shut_down.is_ok();
        Poll::Ready(shut_down)
    }
}

/// Where a pump is.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Copying,
    /// The reader ended and every byte was written and flushed; once begun, the writer's
    /// shutdown is all that is polled, as some writers reject a flush or write after it.
    ShuttingDown,
    Done,
}

/// One direction of a relay: Tokio's copy buffer, which reads ahead while it has room, kept by
/// the relay so that an ending can still write what it read.
struct Pump {
    buffer: Box<[u8]>,
    position: usize,
    filled: usize,
    read_ended: bool,
    need_flush: bool,
    phase: Phase,
    /// The writer held up the last poll while the task still had coop budget, so the writer
    /// itself, not the spent budget, made it wait.
    writer_pending: bool,
}

impl Pump {
    fn new() -> Self {
        Self {
            buffer: vec![0; COPY_BUFFER].into_boxed_slice(),
            position: 0,
            filled: 0,
            read_ended: false,
            need_flush: false,
            phase: Phase::Copying,
            writer_pending: false,
        }
    }

    fn ended(&self) -> bool {
        self.phase == Phase::Done
    }

    /// Copies from `reader` to `writer` until the reader's end of stream, then shuts down the
    /// writer.
    fn poll_pump<R, W>(
        &mut self,
        context: &mut Context<'_>,
        mut reader: Pin<&mut R>,
        mut writer: Pin<&mut W>,
    ) -> Poll<io::Result<()>>
    where
        R: AsyncRead + ?Sized,
        W: AsyncWrite + ?Sized,
    {
        self.writer_pending = false;
        loop {
            match self.phase {
                Phase::Copying => {
                    ready!(self.poll_copy(context, reader.as_mut(), writer.as_mut()))?;
                    self.phase = Phase::ShuttingDown;
                }
                Phase::ShuttingDown => {
                    if writer.as_mut().poll_shutdown(context)?.is_pending() {
                        self.writer_held();
                        return Poll::Pending;
                    }
                    self.phase = Phase::Done;
                }
                Phase::Done => return Poll::Ready(Ok(())),
            }
        }
    }

    fn poll_copy<R, W>(
        &mut self,
        context: &mut Context<'_>,
        mut reader: Pin<&mut R>,
        mut writer: Pin<&mut W>,
    ) -> Poll<io::Result<()>>
    where
        R: AsyncRead + ?Sized,
        W: AsyncWrite + ?Sized,
    {
        let budget = ready!(coop::poll_proceed(context));
        loop {
            if self.filled < self.buffer.len() && !self.read_ended {
                match self.poll_fill(context, reader.as_mut()) {
                    Poll::Ready(read) => {
                        budget.made_progress();
                        read?;
                    }
                    // A reader that depends on a buffered writer needs the flush to progress.
                    Poll::Pending if self.position == self.filled => {
                        if self.need_flush {
                            ready!(self.poll_flush(context, writer.as_mut()))?;
                            budget.made_progress();
                            self.need_flush = false;
                        }
                        return Poll::Pending;
                    }
                    Poll::Pending => {}
                }
            }
            while self.position < self.filled {
                let written = writer
                    .as_mut()
                    .poll_write(context, &self.buffer[self.position..self.filled]);
                let written = match written {
                    Poll::Ready(written) => written?,
                    Poll::Pending => {
                        self.writer_held();
                        // Top up the buffer toward a larger write.
                        if !self.read_ended && self.filled < self.buffer.len() {
                            ready!(self.poll_fill(context, reader.as_mut()))?;
                        }
                        return Poll::Pending;
                    }
                };
                budget.made_progress();
                if written == 0 {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "write zero byte into writer",
                    )));
                }
                self.position += written;
                self.need_flush = true;
            }
            self.position = 0;
            self.filled = 0;
            if self.read_ended {
                ready!(self.poll_flush(context, writer.as_mut()))?;
                budget.made_progress();
                return Poll::Ready(Ok(()));
            }
        }
    }

    fn poll_fill<R>(
        &mut self,
        context: &mut Context<'_>,
        reader: Pin<&mut R>,
    ) -> Poll<io::Result<()>>
    where
        R: AsyncRead + ?Sized,
    {
        let mut buffer = ReadBuf::new(&mut self.buffer);
        buffer.set_filled(self.filled);
        let read = ready!(reader.poll_read(context, &mut buffer));
        if read.is_ok() {
            let filled = buffer.filled().len();
            self.read_ended = filled == self.filled;
            self.filled = filled;
        }
        Poll::Ready(read)
    }

    fn poll_flush<W>(
        &mut self,
        context: &mut Context<'_>,
        writer: Pin<&mut W>,
    ) -> Poll<io::Result<()>>
    where
        W: AsyncWrite + ?Sized,
    {
        let flushed = writer.poll_flush(context);
        if flushed.is_pending() {
            self.writer_held();
        }
        flushed
    }

    fn writer_held(&mut self) {
        self.writer_pending = coop::has_budget_remaining();
    }
}

/// Why a relay stopped copying.
enum Stop {
    /// Both directions ended.
    Done,
    Failed(io::Error),
    /// A peer reset or session failure closed the stream after the download's end was read and
    /// before the upload's END_STREAM went out. From then on the relay reads only the
    /// application, so the stream's I/O cannot report the ending.
    ClosedAfterDownload,
}

/// How the download stood when the relay stopped waiting after a native write error.
enum ReadAhead {
    /// The relay has read the download's END_STREAM.
    Ended,
    /// A native read failed before it.
    Failed(io::Error),
    /// The application held the download up first, failed, or the bound passed.
    Stalled,
}

/// The worker side of a relay, run in its tunnel's task with one copy buffer per direction and
/// no other buffering than the native stream's. Dropped before it settles, as by a forced
/// shutdown, it resets the stream, closes the application I/O, and resolves the relay with the
/// ending it recorded, or `Aborted`.
pub(crate) struct Relay {
    /// Declared first, so the application I/O closes before the settlement releases admission.
    application: Option<Tracked<ApplicationIo>>,
    native: Tracked<HttpConnectAsyncTransport>,
    upload: Pump,
    download: Pump,
    watch: Rc<StreamWatch>,
    stats: Arc<Recorder>,
    shutdown_timeout: Duration,
    /// When an ending started the bounded writing of the rest of the download.
    ending_at: Option<Instant>,
    /// The ending is counted and kept for the outcome.
    ended: bool,
    settlement: Option<Settlement>,
}

impl Relay {
    pub(crate) fn new(
        native: HttpConnectAsyncTransport,
        watch: Rc<StreamWatch>,
        stats: Arc<Recorder>,
        settlement: Settlement,
        shutdown_timeout: Duration,
    ) -> Self {
        Self {
            application: None,
            native: Tracked::new(native),
            upload: Pump::new(),
            download: Pump::new(),
            watch,
            stats,
            shutdown_timeout,
            ending_at: None,
            ended: false,
            settlement: Some(settlement),
        }
    }

    /// Relays until both directions end and the stream is closed on the wire, either side
    /// fails, or the relay future is dropped, then publishes the outcome.
    pub(crate) async fn run(mut self, io: RelayIo, dropped: CancellationToken) {
        let close = match ApplicationIo::adopt(io) {
            Ok(io) => {
                self.application = Some(Tracked::new(io));
                self.relay(dropped).await
            }
            Err(error) => {
                self.end(RelayClose::Application, Some(error));
                RelayClose::Application
            }
        };
        self.finish(close).await;
    }

    /// Relays, records the ending, and returns how to close: as the ending, or `Aborted` once the
    /// relay future is dropped.
    async fn relay(&mut self, dropped: CancellationToken) -> RelayClose {
        let stop = tokio::select! {
            biased;
            () = cancelled(dropped.clone()) => return self.end_aborted(),
            stop = poll_fn(|context| self.poll_relay(context)) => stop,
        };
        let error = match stop {
            // Flow control may still hold the upload and its END_STREAM, which a drop must reset
            // and a peer reset or session failure discards. A stream whose END_STREAM goes out
            // last releases its capacity just before its `RequestEnd`, in the same callout, so the
            // watch has both by the time this wakes.
            Stop::Done => {
                let closed = tokio::select! {
                    biased;
                    () = self.watch.wait_closed() => true,
                    () = cancelled(dropped) => false,
                };
                if !closed {
                    return self.end_aborted();
                }
                if self.watch.upload_ended() {
                    self.end(RelayClose::Finished, None);
                    return RelayClose::Finished;
                }
                upload_discarded()
            }
            Stop::ClosedAfterDownload => {
                self.end(RelayClose::PeerReset, Some(upload_discarded()));
                return self.deliver(dropped).await;
            }
            Stop::Failed(error) if self.application_failed() => {
                self.end(RelayClose::Application, Some(error));
                return RelayClose::Application;
            }
            // A write error carries only `DROPPED`; the download's read tells how it ended.
            Stop::Failed(error) if self.native.write_failed => {
                let read = tokio::select! {
                    biased;
                    () = cancelled(dropped.clone()) => return self.end_aborted(),
                    read = self.read_ahead() => read,
                };
                match read {
                    ReadAhead::Ended => {
                        let failure = TunnelFailure::Native(error);
                        self.end(RelayClose::PeerReset, Some(failure.into_io_error()));
                        return self.deliver(dropped).await;
                    }
                    ReadAhead::Failed(read) => read,
                    ReadAhead::Stalled => {
                        let remaining = self.remaining();
                        let read = tokio::select! {
                            biased;
                            () = cancelled(dropped) => return self.end_aborted(),
                            read = read_error_after_write_error(&mut self.native, remaining) => read,
                        };
                        read.unwrap_or(error)
                    }
                }
            }
            Stop::Failed(error) => error,
        };
        let failure = TunnelFailure::Native(error);
        let close = match failure.kind() {
            io::ErrorKind::TimedOut => RelayClose::TimedOut,
            _ => RelayClose::PeerReset,
        };
        self.end(close, Some(failure.into_io_error()));
        close
    }

    fn poll_relay(&mut self, context: &mut Context<'_>) -> Poll<Stop> {
        let Self {
            application,
            native,
            upload,
            download,
            watch,
            ..
        } = self;
        let application = application
            .as_mut()
            .expect("should hold the application I/O while it relays");
        let uploaded =
            upload.poll_pump(context, Pin::new(&mut *application), Pin::new(&mut *native));
        if let Poll::Ready(Err(error)) = uploaded {
            return Poll::Ready(Stop::Failed(error));
        }
        let downloaded =
            download.poll_pump(context, Pin::new(&mut *native), Pin::new(&mut *application));
        if let Poll::Ready(Err(error)) = downloaded {
            return Poll::Ready(Stop::Failed(error));
        }
        if uploaded.is_ready() && downloaded.is_ready() {
            return Poll::Ready(Stop::Done);
        }
        // A stream closed after its upload's END_STREAM went out finishes through the copy.
        if download.read_ended && watch.poll_closed(context).is_ready() && !watch.upload_ended() {
            return Poll::Ready(Stop::ClosedAfterDownload);
        }
        Poll::Pending
    }

    fn application_failed(&self) -> bool {
        self.application
            .as_ref()
            .is_some_and(|application| application.failed())
    }

    /// Records how the relay ended, once: counts it and keeps it for the outcome, which a drop
    /// during the writing of the download or the close that follows still publishes.
    fn end(&mut self, close: RelayClose, error: Option<io::Error>) {
        if mem::replace(&mut self.ended, true) {
            return;
        }
        let settlement = self
            .settlement
            .as_mut()
            .expect("should record an ending before it settles");
        let ending: Option<Ending> = match close {
            RelayClose::PeerReset => Some(|stats| &stats.tunnels_reset_by_peer),
            RelayClose::Application | RelayClose::Aborted if !self.watch.closed() => {
                Some(|stats| &stats.tunnels_aborted)
            }
            _ => None,
        };
        if let Some(ending) = ending {
            settlement.record(ending, &self.stats);
        }
        let stream_reset_by_peer = close == RelayClose::PeerReset && self.watch.peer_reset();
        settlement.ended(close, stream_reset_by_peer, error);
    }

    /// Ends the relay `Aborted` for its dropped future, unless it already recorded an ending.
    fn end_aborted(&mut self) -> RelayClose {
        let outcome = RelayOutcome::aborted(Relayed::default());
        self.end(outcome.close, outcome.error);
        RelayClose::Aborted
    }

    /// What remains of the `shutdown_timeout` an ending gives the relay to finish with the
    /// application I/O.
    fn remaining(&self) -> Duration {
        self.ending_at.map_or(self.shutdown_timeout, |ending| {
            self.shutdown_timeout.saturating_sub(ending.elapsed())
        })
    }

    /// After a native write error, moves the download on for as long as only the native stream
    /// holds it up, within `shutdown_timeout`.
    async fn read_ahead(&mut self) -> ReadAhead {
        self.ending_at = Some(Instant::now());
        let limit = self.shutdown_timeout;
        let Self {
            application,
            native,
            download,
            ..
        } = self;
        let application = application
            .as_mut()
            .expect("should hold the application I/O while it relays");
        let read = poll_fn(|context| {
            let polled =
                download.poll_pump(context, Pin::new(&mut *native), Pin::new(&mut *application));
            match polled {
                Poll::Ready(Ok(())) => Poll::Ready(ReadAhead::Ended),
                Poll::Ready(Err(error)) if native.read_failed => {
                    Poll::Ready(ReadAhead::Failed(error))
                }
                Poll::Ready(Err(_)) => Poll::Ready(ReadAhead::Stalled),
                Poll::Pending if download.read_ended => Poll::Ready(ReadAhead::Ended),
                Poll::Pending if download.writer_pending => Poll::Ready(ReadAhead::Stalled),
                Poll::Pending => Poll::Pending,
            }
        });
        tokio::time::timeout(limit, read)
            .await
            .unwrap_or(ReadAhead::Stalled)
    }

    /// Writes the rest of a download whose END_STREAM the relay has read, and its end, to the
    /// application after the stream failed or closed, within `shutdown_timeout` (pooled CONNECT
    /// specification D3), and returns how to close.
    async fn deliver(&mut self, dropped: CancellationToken) -> RelayClose {
        self.ending_at.get_or_insert_with(Instant::now);
        let remaining = self.remaining();
        let Self {
            application,
            native,
            download,
            ..
        } = self;
        let application = application
            .as_mut()
            .expect("should hold the application I/O while it relays");
        let delivery = poll_fn(|context| {
            download.poll_pump(context, Pin::new(&mut *native), Pin::new(&mut *application))
        });
        tokio::select! {
            biased;
            () = cancelled(dropped) => RelayClose::Aborted,
            _ = tokio::time::timeout(remaining, delivery) => RelayClose::PeerReset,
        }
    }

    /// Resets an unfinished stream, closes the application I/O, then releases admission and
    /// publishes the recorded outcome, which a drop during the close keeps.
    async fn finish(mut self, close: RelayClose) {
        if close != RelayClose::Finished {
            self.native.io.abort();
        }
        let mut settlement = self.settlement.take().expect("should settle a relay once");
        let mut application = self.application.take();
        settlement.relayed(self.relayed(application.as_ref()));
        // The close that legacy HTTP/1.1 CONNECT gives the guest socket when its proxy connection
        // fails.
        if let Some(application) = application.as_mut()
            && matches!(close, RelayClose::PeerReset | RelayClose::TimedOut)
            && !application.shut_down
        {
            let _ = tokio::time::timeout(self.remaining(), application.shutdown()).await;
        }
        drop(application);
        settlement.settle();
    }

    fn relayed(&self, application: Option<&Tracked<ApplicationIo>>) -> Relayed {
        Relayed {
            uploaded: self.native.written,
            downloaded: application.map_or(0, |application| application.written),
            download_ended: self.download.ended(),
            peer_ended_download: self.native.read_ended,
        }
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        let relayed = self.relayed(self.application.as_ref());
        let Some(settlement) = self.settlement.as_mut() else {
            return;
        };
        if !self.ended && !self.watch.closed() {
            settlement.record(|stats| &stats.tunnels_aborted, &self.stats);
        }
        self.native.io.abort();
        settlement.relayed(relayed);
        // The application I/O closes as its field drops, then the settlement releases admission
        // and publishes the recorded ending, or `Aborted`.
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::Arc;

    use futures::FutureExt;
    use roxygen_runtime::cancellation::CancellationSource;
    use tokio::io::duplex;
    use tokio::sync::oneshot;

    use super::Publisher;
    use super::RelayClose;
    use super::RelayIo;
    use super::RelayOutcome;
    use super::Relayed;
    use super::start;
    use crate::tunnel::Choice;

    /// A relay future and the request the worker received for it.
    fn chosen_relay(
        cancellation: &Arc<CancellationSource>,
    ) -> (
        impl Future<Output = RelayOutcome> + Send + 'static,
        super::RelayRequest,
    ) {
        let (choice, mut chosen) = oneshot::channel();
        let (application, _peer) = duplex(64);
        let relay = start(
            Some(choice),
            RelayIo::Boxed(Box::new(application)),
            Arc::clone(cancellation),
        );
        let Ok(Choice::Relay(request)) = chosen.try_recv() else {
            panic!("relay() delivers its request when it is called");
        };
        (relay, request)
    }

    fn assert_aborted(outcome: RelayOutcome, uploaded: u64, downloaded: u64, case: &str) {
        assert_eq!(outcome.close(), RelayClose::Aborted, "{case}");
        assert_eq!(
            outcome.error().map(io::Error::kind),
            Some(io::ErrorKind::ConnectionAborted),
            "{case}",
        );
        assert_eq!(
            (outcome.uploaded(), outcome.downloaded()),
            (uploaded, downloaded),
            "{case}: the outcome keeps the bytes relayed before the job ended",
        );
    }

    #[test]
    fn relay_job_dropped_without_an_outcome_resolves_aborted() {
        let cancellation = Arc::new(CancellationSource::new());
        let (relay, request) = chosen_relay(&cancellation);
        let mut relay = Box::pin(relay);
        assert!(
            relay.as_mut().now_or_never().is_none(),
            "the relay is unresolved while its job lives",
        );
        let (io, outcome) = request.into_parts();
        drop(io);
        let mut publisher = Publisher::new(outcome);
        publisher.relayed(Relayed {
            uploaded: 3,
            downloaded: 4,
            ..Relayed::default()
        });
        drop(publisher);
        assert_aborted(
            relay
                .now_or_never()
                .expect("the drop guard publishes at once"),
            3,
            4,
            "a job dropped mid-relay",
        );
        assert!(
            !cancellation.is_cancellation_requested(),
            "a resolved relay does not cancel its tunnel",
        );

        let (relay, request) = chosen_relay(&cancellation);
        let (_io, outcome) = request.into_parts();
        let mut publisher = Publisher::new(outcome);
        publisher.relayed(Relayed {
            uploaded: 5,
            downloaded: 6,
            ..Relayed::default()
        });
        publisher.ended(
            RelayClose::PeerReset,
            true,
            Some(io::Error::from(io::ErrorKind::ConnectionReset)),
        );
        drop(publisher);
        let outcome = relay
            .now_or_never()
            .expect("the drop guard publishes at once");
        assert_eq!(outcome.close(), RelayClose::PeerReset);
        assert_eq!(
            outcome.error().map(io::Error::kind),
            Some(io::ErrorKind::ConnectionReset)
        );
        assert_eq!(
            (outcome.uploaded(), outcome.downloaded()),
            (5, 6),
            "a job dropped while it closes the I/O keeps the ending it recorded",
        );

        let (relay, request) = chosen_relay(&cancellation);
        drop(request);
        assert_aborted(
            relay
                .now_or_never()
                .expect("a request dropped before its job started resolves at once"),
            0,
            0,
            "a request the worker dropped unstarted",
        );

        let (relay, _request) = chosen_relay(&cancellation);
        drop(relay);
        assert!(
            cancellation.is_cancellation_requested(),
            "dropping the relay future cancels its tunnel",
        );
    }
}
