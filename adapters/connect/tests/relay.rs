/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Two-phase connect and worker-side relay oracles against independent `h2` proxy peers.

use std::collections::HashSet;
use std::fs::File;
use std::io;
use std::io::Read;
use std::io::Write;
use std::mem;
use std::net::Shutdown;
use std::os::fd::OwnedFd;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc as synchronous;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use http::StatusCode;
use roxygen_connect_client::error::ConnectErrorKind;
use roxygen_connect_client::options::ConnectOptions;
use roxygen_connect_client::relay::RelayClose;
use roxygen_connect_client::relay::RelayIo;
use roxygen_connect_client::relay::RelayOutcome;
use roxygen_connect_client::tunnel::PendingTunnel;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::ReadBuf;
use tokio::net::TcpListener;
use tokio::net::TcpSocket;
use tokio::net::TcpStream;
use tokio::runtime::Builder;
use tokio::task::JoinSet;
use tokio::time::timeout;

use crate::proxy::AFTER_HALF_CLOSE;
use crate::proxy::BEFORE_RESET;
use crate::proxy::Client;
use crate::proxy::ENDED_DOWNLOAD;
use crate::proxy::GREETING;
use crate::proxy::GREETING_RESET_DELAY;
use crate::proxy::Proxy;
use crate::proxy::ProxyConfig;
use crate::proxy::RawProxy;
use crate::proxy::RawScript;
use crate::proxy::Tap;
use crate::proxy::TestConnector;
use crate::proxy::echo;
use crate::proxy::eventually;
use crate::proxy::pattern;
use crate::proxy::request;
use crate::proxy::request_with;
use crate::proxy::run;
use crate::proxy::run_owned;
use crate::proxy::shutdown;
use crate::proxy::written_data;
use crate::proxy::written_data_frames;
use crate::proxy::written_resets;

const PAYLOAD: &[u8] = b"relayed through the protocol worker";
/// The relay's copy buffer per direction.
const COPY_BUFFER: usize = 16 * 1024;
/// The native session's per-stream egress queue limit (`DEFAULT_BODY_BUFFER_LIMIT`).
const NATIVE_EGRESS_QUEUE: usize = 65_535;
/// The `h2` proxy's default stream window.
const PROXY_STREAM_WINDOW: usize = 65_535;
const SETTLE: Duration = Duration::from_millis(200);

async fn connect_pending(client: &Client, proxy: &Proxy, action: &str) -> PendingTunnel {
    client
        .connect("vm", request(action), proxy.connector())
        .await
        .expect("CONNECT succeeds")
}

/// A connected pair: the application's end and the relay's end as a `std` socket.
async fn sockets() -> (TcpStream, std::net::TcpStream) {
    let (application, relayed) = tokio_sockets().await;
    (application, relayed.into_std().expect("std socket"))
}

async fn tokio_sockets() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let application = TcpStream::connect(listener.local_addr().expect("address"))
        .await
        .expect("connect");
    let (relayed, _) = listener.accept().await.expect("accept");
    (application, relayed)
}

/// Like `sockets`, with small kernel buffers between the relay and the application, whose sizes
/// as the kernel reports them it returns too.
async fn small_buffer_sockets(size: u32) -> (TcpStream, std::net::TcpStream, usize) {
    let listening = TcpSocket::new_v4().expect("socket");
    listening
        .set_recv_buffer_size(size)
        .expect("receive buffer");
    let received = listening.recv_buffer_size().expect("receive buffer size");
    listening
        .bind("127.0.0.1:0".parse().expect("loopback"))
        .expect("bind");
    let listener = listening.listen(1).expect("listen");
    let relaying = TcpSocket::new_v4().expect("socket");
    relaying.set_send_buffer_size(size).expect("send buffer");
    let sent = relaying.send_buffer_size().expect("send buffer size");
    let relayed = relaying
        .connect(listener.local_addr().expect("address"))
        .await
        .expect("connect");
    let (application, _) = listener.accept().await.expect("accept");
    let kernel = (received + sent) as usize;
    (application, relayed.into_std().expect("std socket"), kernel)
}

async fn round_trip(application: &mut TcpStream, payload: &[u8]) {
    application.write_all(payload).await.expect("upload");
    let mut received = vec![0; payload.len()];
    application
        .read_exact(&mut received)
        .await
        .expect("echo arrives");
    assert_eq!(received, payload);
}

/// Ends the application's upload and reads its download to end of stream.
async fn finish(application: &mut TcpStream) -> Vec<u8> {
    application.shutdown().await.expect("upload EOF");
    let mut received = Vec::new();
    application
        .read_to_end(&mut received)
        .await
        .expect("download EOF");
    received
}

fn assert_outcome(
    outcome: &RelayOutcome,
    close: RelayClose,
    kind: Option<io::ErrorKind>,
    bytes: (u64, u64),
) {
    assert_eq!(outcome.close(), close, "{outcome:?}");
    assert_eq!(outcome.error().map(io::Error::kind), kind, "{outcome:?}");
    assert_eq!(
        (outcome.uploaded(), outcome.downloaded()),
        bytes,
        "{outcome:?}"
    );
}

/// What a `GenericIo` saw and what it is told to do.
#[derive(Default)]
struct IoControl {
    read: AtomicU64,
    threads: Mutex<HashSet<String>>,
    read_failure: Mutex<Option<io::ErrorKind>>,
    write_failure: Mutex<Option<io::ErrorKind>>,
    shutdown_failure: Mutex<Option<io::ErrorKind>>,
    reader: Mutex<Option<Waker>>,
    shutdowns: AtomicU64,
    /// What a `coop_writes` source was written.
    written: Mutex<Vec<u8>>,
    first_shutdown: OnceLock<Instant>,
    dropped: OnceLock<Instant>,
}

impl IoControl {
    fn read(&self) -> u64 {
        self.read.load(Ordering::SeqCst)
    }

    fn dropped(&self) -> bool {
        self.dropped.get().is_some()
    }

    /// How often the relay polled the I/O's write-half shutdown.
    fn shutdowns(&self) -> u64 {
        self.shutdowns.load(Ordering::SeqCst)
    }

    /// The time from the first shutdown poll to the I/O's drop.
    fn shutdown_to_drop(&self) -> Option<Duration> {
        let first = self.first_shutdown.get()?;
        Some(self.dropped.get()?.duration_since(*first))
    }

    fn written(&self) -> Vec<u8> {
        self.written.lock().expect("written lock").clone()
    }

    fn threads(&self) -> HashSet<String> {
        self.threads.lock().expect("threads lock").clone()
    }

    fn fail_reads(&self, kind: io::ErrorKind) {
        *self.read_failure.lock().expect("failure lock") = Some(kind);
        let reader = self.reader.lock().expect("reader lock").take();
        if let Some(reader) = reader {
            reader.wake();
        }
    }

    fn fail_writes(&self, kind: io::ErrorKind) {
        *self.write_failure.lock().expect("failure lock") = Some(kind);
    }

    fn fail_shutdowns(&self, kind: io::ErrorKind) {
        *self.shutdown_failure.lock().expect("failure lock") = Some(kind);
    }

    fn record(&self) {
        let name = thread::current().name().unwrap_or("unnamed").to_owned();
        self.threads.lock().expect("threads lock").insert(name);
    }
}

/// Waits until the bytes read from a `GenericIo` stop growing for `SETTLE`, then returns them.
async fn parked_reads(control: &IoControl) -> u64 {
    let mut parked = control.read();
    loop {
        tokio::time::sleep(SETTLE).await;
        let read = control.read();
        if read == parked {
            return parked;
        }
        parked = read;
    }
}

/// Waits until shutdown has closed admission. Every permit must be held, so until then each
/// connect fails at once with `Overloaded`, and afterwards with `Closed`.
async fn admission_closed(client: &Client, proxy: &Proxy) {
    loop {
        let error = client
            .connect("vm", request("echo"), proxy.connector())
            .await
            .expect_err("every permit is held");
        match error.kind() {
            ConnectErrorKind::Closed => return,
            ConnectErrorKind::Overloaded => tokio::time::sleep(Duration::from_millis(1)).await,
            kind => panic!("a connect at the bound failed with {kind:?}"),
        }
    }
}

/// An application-side generic I/O: a socket, or an upload source. It records the thread of
/// every poll, counts the bytes read from it, fails on request, and records its drop.
struct GenericIo {
    socket: Option<TcpStream>,
    /// What a source still uploads before its end of stream; `None` is endless.
    upload: Option<usize>,
    stall_shutdown: bool,
    /// Its first shutdown poll is Pending, and a write or flush after any shutdown poll fails,
    /// as an encoder's does.
    strict_shutdown: bool,
    shutdown_polled: bool,
    /// A source whose writes each take a unit of the task's coop budget, as a Tokio resource's
    /// do, so it is never full but runs out of budget, and that keeps what is written to it.
    coop_writes: bool,
    control: Arc<IoControl>,
}

impl GenericIo {
    fn new(socket: Option<TcpStream>, upload: Option<usize>) -> (Self, Arc<IoControl>) {
        let control = Arc::new(IoControl::default());
        let io = Self {
            socket,
            upload,
            stall_shutdown: false,
            strict_shutdown: false,
            shutdown_polled: false,
            coop_writes: false,
            control: Arc::clone(&control),
        };
        (io, control)
    }

    fn socket(socket: TcpStream) -> (Self, Arc<IoControl>) {
        Self::new(Some(socket), None)
    }

    fn source() -> (Self, Arc<IoControl>) {
        Self::new(None, None)
    }

    fn finite_source(length: usize) -> (Self, Arc<IoControl>) {
        Self::new(None, Some(length))
    }

    fn relay_io(self) -> RelayIo {
        RelayIo::Boxed(Box::new(self))
    }
}

impl AsyncRead for GenericIo {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.control.record();
        *this.control.reader.lock().expect("reader lock") = Some(context.waker().clone());
        if let Some(kind) = *this.control.read_failure.lock().expect("failure lock") {
            return Poll::Ready(Err(io::Error::new(kind, "injected read failure")));
        }
        let before = buffer.filled().len();
        match this.socket.as_mut() {
            Some(socket) => std::task::ready!(Pin::new(socket).poll_read(context, buffer))?,
            None => {
                let length = this
                    .upload
                    .map_or(buffer.remaining(), |left| left.min(buffer.remaining()));
                buffer.put_slice(&pattern(length));
                if let Some(left) = this.upload.as_mut() {
                    *left -= length;
                }
            }
        }
        let read = buffer.filled().len() - before;
        this.control.read.fetch_add(read as u64, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for GenericIo {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.control.record();
        if let Some(kind) = *this.control.write_failure.lock().expect("failure lock") {
            return Poll::Ready(Err(io::Error::new(kind, "injected write failure")));
        }
        if this.strict_shutdown && this.shutdown_polled {
            return Poll::Ready(Err(io::Error::other("Write after close")));
        }
        match this.socket.as_mut() {
            Some(socket) => Pin::new(socket).poll_write(context, buffer),
            None if this.coop_writes => {
                let budget = std::task::ready!(tokio::task::coop::poll_proceed(context));
                let mut written = this.control.written.lock().expect("written lock");
                written.extend_from_slice(buffer);
                budget.made_progress();
                Poll::Ready(Ok(buffer.len()))
            }
            None => Poll::Ready(Ok(buffer.len())),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.control.record();
        if this.strict_shutdown && this.shutdown_polled {
            return Poll::Ready(Err(io::Error::other("Flush after close")));
        }
        match this.socket.as_mut() {
            Some(socket) => Pin::new(socket).poll_flush(context),
            None => Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.control.record();
        this.control.shutdowns.fetch_add(1, Ordering::SeqCst);
        let _ = this.control.first_shutdown.set(Instant::now());
        if let Some(kind) = *this.control.shutdown_failure.lock().expect("failure lock") {
            return Poll::Ready(Err(io::Error::new(kind, "injected shutdown failure")));
        }
        if this.stall_shutdown {
            return Poll::Pending;
        }
        if this.strict_shutdown && !mem::replace(&mut this.shutdown_polled, true) {
            context.waker().wake_by_ref();
            return Poll::Pending;
        }
        match this.socket.as_mut() {
            Some(socket) => Pin::new(socket).poll_shutdown(context),
            None => Poll::Ready(Ok(())),
        }
    }
}

impl Drop for GenericIo {
    fn drop(&mut self) {
        let _ = self.control.dropped.set(Instant::now());
    }
}

#[test]
fn connect_returns_the_response_head_before_the_data_path_is_chosen() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let tap = Arc::new(Tap::default());
        let connector = Arc::new(TestConnector::plain(proxy.address).tapped(Arc::clone(&tap)));
        let pending = client
            .connect("vm", request_with("echo", "head"), connector.clone())
            .await
            .expect("CONNECT");
        assert_eq!(pending.response().status(), StatusCode::OK);
        assert_eq!(
            pending.response().headers()["x-test-proxy"],
            "independent-h2"
        );
        assert_eq!(pending.response().headers()["x-test-id"], "head");
        let info = pending.info();
        assert!(!info.reused_session);
        assert_eq!(info.retries, 0);
        let stats = client.stats();
        assert_eq!(
            (stats.tunnels_established, stats.active_tunnels),
            (1, 1),
            "a pending tunnel is established and active from its handover",
        );
        tokio::time::sleep(SETTLE).await;
        assert_eq!(
            written_data_frames(&tap.written()[0]),
            0,
            "nothing, not even an empty END_STREAM, is sent before the choice"
        );

        let headers = pending.response().headers().clone();
        let mut tunnel = pending.into_tunnel();
        assert_eq!(tunnel.response().status(), StatusCode::OK);
        assert_eq!(tunnel.response().headers(), &headers);
        assert_eq!(tunnel.info(), info);
        echo(&mut tunnel, PAYLOAD).await;

        let pending = client
            .connect("vm", request("echo"), connector)
            .await
            .expect("CONNECT");
        assert_eq!(pending.response().status(), StatusCode::OK);
        assert!(pending.info().reused_session);
        let (mut application, relayed) = sockets().await;
        let relay = tokio::spawn(pending.relay(relayed));
        round_trip(&mut application, PAYLOAD).await;
        assert!(finish(&mut application).await.is_empty());
        let outcome = relay.await.expect("relay joins");
        let length = PAYLOAD.len() as u64;
        assert_outcome(&outcome, RelayClose::Finished, None, (length, length));
        assert_eq!(client.stats().tunnels_established, 2);
        drop(tunnel);
        proxy
    });
}

#[test]
fn bytes_sent_before_the_choice_reach_the_tunnel_first() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let pending = connect_pending(&client, &proxy, "greet").await;
        proxy
            .observations
            .wait("the greeting", |observed| {
                observed.downloaded() == GREETING.len() as u64
            })
            .await;
        tokio::time::sleep(SETTLE).await;
        let mut tunnel = pending.into_tunnel();
        let mut greeting = vec![0; GREETING.len()];
        tunnel
            .read_exact(&mut greeting)
            .await
            .expect("the early bytes");
        assert_eq!(greeting, GREETING);
        echo(&mut tunnel, PAYLOAD).await;
        drop(tunnel);
        proxy
    });
}

#[test]
fn bytes_sent_before_the_choice_are_relayed_first() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let pending = connect_pending(&client, &proxy, "greet").await;
        proxy
            .observations
            .wait("the greeting", |observed| {
                observed.downloaded() == GREETING.len() as u64
            })
            .await;
        tokio::time::sleep(SETTLE).await;
        let (mut application, relayed) = sockets().await;
        let relay = tokio::spawn(pending.relay(relayed));
        let mut greeting = vec![0; GREETING.len()];
        application
            .read_exact(&mut greeting)
            .await
            .expect("the early bytes");
        assert_eq!(greeting, GREETING);
        round_trip(&mut application, PAYLOAD).await;
        assert!(finish(&mut application).await.is_empty());
        let outcome = relay.await.expect("relay joins");
        assert_outcome(
            &outcome,
            RelayClose::Finished,
            None,
            (
                PAYLOAD.len() as u64,
                (GREETING.len() + PAYLOAD.len()) as u64,
            ),
        );
        proxy
    });

    // Until the choice nothing reads the stream, so the proxy stops at the stream window.
    const WINDOW: usize = 64 * 1024;
    const FLOOD: usize = 4 * WINDOW;
    let options = ConnectOptions {
        stream_receive_window: WINDOW as u32,
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let pending = connect_pending(&client, &proxy, &format!("flood:{FLOOD}")).await;
        proxy
            .observations
            .wait("the flood", |observed| observed.downloaded() > 0)
            .await;
        tokio::time::sleep(SETTLE).await;
        let sent = proxy.observations.downloaded();
        assert!(
            sent <= WINDOW as u64,
            "the proxy sent {sent} bytes before the choice"
        );
        let (mut application, relayed) = sockets().await;
        let relay = tokio::spawn(pending.relay(relayed));
        let mut received = vec![0; FLOOD];
        application
            .read_exact(&mut received)
            .await
            .expect("the whole flood");
        assert!(received == pattern(FLOOD), "the flood arrives in order");
        relay.abort();
        proxy
    });
}

#[test]
fn dropping_an_unchosen_pending_tunnel_aborts_its_stream() {
    let options = ConnectOptions {
        max_tunnels: 2,
        ..Default::default()
    };
    run(options.clone(), |client| async move {
        let proxy = Proxy::start().await;
        let mut sibling = connect_pending(&client, &proxy, "echo").await.into_tunnel();
        let unchosen = connect_pending(&client, &proxy, "hold-open").await;
        drop(unchosen);
        proxy
            .observations
            .wait("the unchosen stream's reset", |observed| {
                observed.cancel_resets() == 1
            })
            .await;
        eventually(Duration::from_secs(5), "the unchosen task's end", || {
            client.stats().active_tunnels == 1
        })
        .await;
        let mut released = connect_pending(&client, &proxy, "echo").await.into_tunnel();
        echo(&mut released, b"on the released permit").await;
        echo(&mut sibling, b"the sibling is untouched").await;
        let stats = client.stats();
        assert_eq!((stats.tunnels_aborted, stats.tunnels_reset_by_peer), (1, 0));
        assert_eq!(proxy.connections(), 1);
        drop((sibling, released));
        proxy
    });

    // A stream the peer reset before the drop gets nothing and counts as neither ending.
    run(options.clone(), |client| async move {
        let proxy = Proxy::start().await;
        let tap = Arc::new(Tap::default());
        let connector = Arc::new(TestConnector::plain(proxy.address).tapped(Arc::clone(&tap)));
        let ended = client
            .connect("vm", request("greet-reset"), connector.clone())
            .await
            .expect("CONNECT");
        proxy
            .observations
            .wait("the greeting before the reset", |observed| {
                observed.downloaded() == GREETING.len() as u64
            })
            .await;
        tokio::time::sleep(GREETING_RESET_DELAY + SETTLE).await;
        drop(ended);
        eventually(Duration::from_secs(5), "the dropped tunnel's task", || {
            client.stats().active_tunnels == 0
        })
        .await;
        let mut sibling = client
            .connect("vm", request("echo"), connector)
            .await
            .expect("CONNECT")
            .into_tunnel();
        // The sibling's round trip comes after anything the drop wrote.
        echo(&mut sibling, PAYLOAD).await;
        assert_eq!(
            written_resets(&tap.written()[0]),
            [],
            "the drop sent nothing"
        );
        let stats = client.stats();
        assert_eq!((stats.tunnels_aborted, stats.tunnels_reset_by_peer), (0, 0));
        drop(sibling);
        proxy
    });

    // A stream that the peer ends before the choice fails whichever path the caller chooses.
    run(options.clone(), |client| async move {
        let proxy = Proxy::start().await;
        let bridged = connect_pending(&client, &proxy, "hold-open").await;
        let relayed = connect_pending(&client, &proxy, "hold-open").await;
        proxy.abort_connections();
        tokio::time::sleep(SETTLE).await;
        let mut tunnel = bridged.into_tunnel();
        assert_eq!(
            tunnel
                .read(&mut [0; 8])
                .await
                .expect_err("the stream ended before the choice")
                .kind(),
            io::ErrorKind::ConnectionReset,
        );
        let (mut application, socket) = sockets().await;
        let outcome = relayed.relay(socket).await;
        assert_outcome(
            &outcome,
            RelayClose::PeerReset,
            Some(io::ErrorKind::ConnectionReset),
            (0, 0),
        );
        let mut rest = Vec::new();
        application
            .read_to_end(&mut rest)
            .await
            .expect("the relay closed the application socket");
        assert!(rest.is_empty());
        let stats = client.stats();
        assert_eq!((stats.tunnels_reset_by_peer, stats.tunnels_aborted), (2, 0));
        drop(tunnel);
        proxy
    });

    // A peer reset before the choice reaches either path; it discarded the unread greeting
    // (pooled CONNECT specification D3).
    run(options.clone(), |client| async move {
        let proxy = Proxy::start().await;
        let tap = Arc::new(Tap::default());
        let connector = Arc::new(TestConnector::plain(proxy.address).tapped(Arc::clone(&tap)));
        let bridged = client
            .connect("vm", request("greet-reset"), connector)
            .await
            .expect("CONNECT");
        let relayed = connect_pending(&client, &proxy, "greet-reset").await;
        proxy
            .observations
            .wait("both greetings", |observed| {
                observed.downloaded() == 2 * GREETING.len() as u64
            })
            .await;
        tokio::time::sleep(GREETING_RESET_DELAY + SETTLE).await;
        let mut tunnel = bridged.into_tunnel();
        assert_eq!(
            tunnel
                .read(&mut [0; 8])
                .await
                .expect_err("the reset")
                .kind(),
            io::ErrorKind::ConnectionReset,
        );
        let (mut application, socket) = sockets().await;
        let outcome = relayed.relay(socket).await;
        assert_outcome(
            &outcome,
            RelayClose::PeerReset,
            Some(io::ErrorKind::ConnectionReset),
            (0, 0),
        );
        let mut rest = Vec::new();
        application
            .read_to_end(&mut rest)
            .await
            .expect("the relay closed the application socket");
        assert!(rest.is_empty());
        // The sibling's round trip comes after anything either ending wrote.
        let mut sibling = connect_pending(&client, &proxy, "echo").await.into_tunnel();
        echo(&mut sibling, PAYLOAD).await;
        assert_eq!(
            written_resets(&tap.written()[0]),
            [],
            "neither path sent an RST_STREAM for a stream the proxy reset"
        );
        let stats = client.stats();
        assert_eq!((stats.tunnels_reset_by_peer, stats.tunnels_aborted), (2, 0));
        drop((tunnel, sibling));
        proxy
    });

    // A probe drop before the choice times out either path and counts as neither ending.
    let probing = ConnectOptions {
        ping_interval: Duration::from_millis(100),
        ping_timeout: Duration::from_millis(100),
        ..options
    };
    run(probing, |client| async move {
        let proxy = Proxy::start().await;
        let bridged = connect_pending(&client, &proxy, "hold-open").await;
        let relayed = connect_pending(&client, &proxy, "hold-open").await;
        proxy.stop_reading();
        eventually(Duration::from_secs(5), "the probe drop", || {
            client.stats().ping_timeouts == 1
        })
        .await;
        let mut tunnel = bridged.into_tunnel();
        assert_eq!(
            tunnel
                .read(&mut [0; 8])
                .await
                .expect_err("the probe drop")
                .kind(),
            io::ErrorKind::TimedOut,
        );
        let (mut application, socket) = sockets().await;
        let outcome = relayed.relay(socket).await;
        assert_outcome(
            &outcome,
            RelayClose::TimedOut,
            Some(io::ErrorKind::TimedOut),
            (0, 0),
        );
        let mut rest = Vec::new();
        application
            .read_to_end(&mut rest)
            .await
            .expect("the relay closed the application socket");
        assert!(rest.is_empty());
        let stats = client.stats();
        assert_eq!((stats.tunnels_reset_by_peer, stats.tunnels_aborted), (0, 0));
        drop(tunnel);
        proxy
    });
}

#[test]
fn shutdown_aborts_unchosen_pending_tunnels() {
    let shutdown_timeout = Duration::from_millis(300);
    let options = ConnectOptions {
        shutdown_timeout,
        ..Default::default()
    };
    run_owned(options, |runtime, client| async move {
        let proxy = Proxy::start().await;
        let mut held = Vec::new();
        for _ in 0..3 {
            held.push(connect_pending(&client, &proxy, "hold-open").await);
        }
        let started = Instant::now();
        shutdown(runtime).await.expect("shutdown");
        let took = started.elapsed();
        assert!(
            took >= shutdown_timeout && took < shutdown_timeout + Duration::from_secs(2),
            "the pending tunnels held the graceful phase to its bound; shutdown took {took:?}"
        );
        proxy
            .observations
            .wait("the proxy connection to close", |observed| {
                observed.closed() == 1
            })
            .await;
        let stats = client.stats();
        assert_eq!(stats.active_tunnels, 0);
        assert_eq!(
            stats.tunnels_aborted, 3,
            "the forced phase aborted each pending tunnel"
        );
        drop(held);
        proxy
    });
}

#[test]
fn into_tunnel_after_shutdown_fails_with_connection_aborted() {
    let options = ConnectOptions {
        shutdown_timeout: Duration::from_millis(300),
        ..Default::default()
    };
    run_owned(options, |runtime, client| async move {
        let proxy = Proxy::start().await;
        let pending = connect_pending(&client, &proxy, "hold-open").await;
        shutdown(runtime).await.expect("shutdown");
        let mut tunnel = pending.into_tunnel();
        let aborted = io::ErrorKind::ConnectionAborted;
        let mut buffer = [0; 8];
        assert_eq!(
            tunnel.read(&mut buffer).await.expect_err("read").kind(),
            aborted
        );
        assert_eq!(
            tunnel.write(PAYLOAD).await.expect_err("write").kind(),
            aborted
        );
        assert_eq!(tunnel.flush().await.expect_err("flush").kind(), aborted);
        assert_eq!(
            tunnel.shutdown().await.expect_err("shutdown").kind(),
            aborted
        );
        proxy
    });
}

#[test]
fn relay_moves_payloads_larger_than_the_windows_and_counts_bytes() {
    const LENGTH: usize = 1 << 20;
    let options = ConnectOptions {
        stream_receive_window: 65_535,
        connection_receive_window: 65_535,
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let pending = connect_pending(&client, &proxy, "echo").await;
        let (application, relayed) = sockets().await;
        let relay = tokio::spawn(pending.relay(relayed));
        let (mut reader, mut writer) = application.into_split();
        let upload = tokio::spawn(async move {
            writer.write_all(&pattern(LENGTH)).await.expect("upload");
            writer.shutdown().await.expect("upload EOF");
        });
        let mut received = Vec::new();
        reader
            .read_to_end(&mut received)
            .await
            .expect("download EOF");
        assert!(received == pattern(LENGTH), "the echo arrives intact");
        upload.await.expect("upload task");
        let outcome = relay.await.expect("relay joins");
        let length = LENGTH as u64;
        assert_outcome(&outcome, RelayClose::Finished, None, (length, length));
        let stats = client.stats();
        assert_eq!(
            (
                stats.tunnels_relayed,
                stats.tunnels_reset_by_peer,
                stats.tunnels_aborted
            ),
            (1, 0, 0)
        );
        proxy
    });
}

#[test]
fn relay_preserves_upload_half_close() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let pending = connect_pending(&client, &proxy, "half-close").await;
        let (mut application, relayed) = sockets().await;
        let relay = tokio::spawn(pending.relay(relayed));
        application.write_all(PAYLOAD).await.expect("upload");
        let received = finish(&mut application).await;
        let mut expected = PAYLOAD.to_vec();
        expected.extend_from_slice(AFTER_HALF_CLOSE);
        assert_eq!(
            received, expected,
            "the download stays open after the upload's end"
        );
        assert_eq!(proxy.observations.upload_eofs(), 1);
        let outcome = relay.await.expect("relay joins");
        assert_outcome(
            &outcome,
            RelayClose::Finished,
            None,
            (PAYLOAD.len() as u64, expected.len() as u64),
        );
        proxy
    });
}

#[test]
fn relay_preserves_download_half_close() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let pending = connect_pending(&client, &proxy, "early-eof-sink").await;
        let (mut application, relayed) = sockets().await;
        let relay = tokio::spawn(pending.relay(relayed));
        let mut download = Vec::new();
        application
            .read_to_end(&mut download)
            .await
            .expect("the proxy's END_STREAM shuts down the application's read side");
        assert!(download.is_empty());
        for _ in 0..4 {
            application
                .write_all(PAYLOAD)
                .await
                .expect("the upload stays open");
        }
        application.shutdown().await.expect("upload EOF");
        let length = 4 * PAYLOAD.len() as u64;
        proxy
            .observations
            .wait("the upload and its end", |observed| {
                observed.uploaded() == length && observed.upload_eofs() == 1
            })
            .await;
        let outcome = relay.await.expect("relay joins");
        assert_outcome(&outcome, RelayClose::Finished, None, (length, 0));
        proxy
    });
}

#[test]
fn slow_application_reader_backpressures_only_its_relay() {
    const FLOOD: usize = 4 << 20;
    const WINDOW: usize = 64 * 1024;
    let options = ConnectOptions {
        stream_receive_window: WINDOW as u32,
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let slow = connect_pending(&client, &proxy, &format!("flood:{FLOOD}")).await;
        let (mut slow_application, relayed, kernel) = small_buffer_sockets(16 * 1024).await;
        let slow_relay = tokio::spawn(slow.relay(relayed));
        proxy
            .observations
            .wait("the flood", |observed| observed.downloaded() > 0)
            .await;
        tokio::time::sleep(SETTLE).await;
        // The stream window, what the kernel buffers toward the application, the relay's copy
        // buffer, and one more read the native transport may hold.
        let bound = (WINDOW + kernel + 2 * COPY_BUFFER) as u64;
        let stalled = proxy.observations.downloaded();
        assert!(
            stalled <= bound,
            "the relay stopped reading, so the proxy stopped at the stream window: {stalled} > {bound}"
        );

        let sibling = connect_pending(&client, &proxy, "echo").await;
        let (mut application, relayed) = sockets().await;
        let sibling_relay = tokio::spawn(sibling.relay(relayed));
        for _ in 0..8 {
            round_trip(&mut application, PAYLOAD).await;
        }
        assert!(finish(&mut application).await.is_empty());
        assert_eq!(
            sibling_relay.await.expect("relay joins").close(),
            RelayClose::Finished
        );
        let still = proxy.observations.downloaded();
        assert!(still <= bound, "still stalled: {still} > {bound}");

        let mut received = vec![0; FLOOD];
        slow_application
            .read_exact(&mut received)
            .await
            .expect("the whole flood");
        assert!(received == pattern(FLOOD), "the flood arrives intact");
        assert_eq!(proxy.connections(), 1);
        slow_relay.abort();
        proxy
    });
}

#[test]
fn withheld_credit_stops_reading_the_application_io() {
    const BRIDGE: usize = 32 * 1024;
    const FLOOD: usize = 256 * 1024;
    // The native egress buffer of `BRIDGE` plus one accepted write, the session's egress queue,
    // which fills from that buffer whatever the stream window, and the relay's buffered read.
    const BOUND: usize = BRIDGE + 2 * COPY_BUFFER + NATIVE_EGRESS_QUEUE - 1;
    let options = ConnectOptions {
        bridge_capacity: BRIDGE,
        ..Default::default()
    };
    run(options.clone(), |client| async move {
        let proxy = Proxy::with_config(ProxyConfig {
            initial_window_size: Some(0),
            ..Default::default()
        })
        .await;
        let tap = Arc::new(Tap::default());
        let connector = Arc::new(TestConnector::plain(proxy.address).tapped(Arc::clone(&tap)));
        let parked = client
            .connect("vm", request("sink"), connector)
            .await
            .expect("CONNECT");
        let (source, control) = GenericIo::source();
        let parked_relay = tokio::spawn(parked.relay(source.relay_io()));
        eventually(Duration::from_secs(5), "the parked upload", || {
            control.read() >= BRIDGE as u64
        })
        .await;
        let parked = parked_reads(&control).await;
        assert!(
            parked <= BOUND as u64,
            "read {parked} bytes from the application with a zero window"
        );
        assert_eq!(
            written_data(&tap.written()[0]),
            0,
            "the window admits no DATA"
        );

        let sibling = connect_pending(&client, &proxy, &format!("flood:{FLOOD}")).await;
        let (mut application, relayed) = sockets().await;
        let sibling_relay = tokio::spawn(sibling.relay(relayed));
        let mut received = vec![0; FLOOD];
        application
            .read_exact(&mut received)
            .await
            .expect("the sibling keeps flowing");
        assert!(received == pattern(FLOOD));
        assert_eq!(control.read(), parked, "still parked");
        assert_eq!(proxy.connections(), 1);
        sibling_relay.abort();
        parked_relay.abort();
        proxy
    });

    // Once the proxy returns credit, the parked upload drains and finishes with exact counts.
    run(options, |client| async move {
        const UPLOAD: usize = 1 << 20;
        let proxy = Proxy::start().await;
        proxy.close_gate();
        let pending = connect_pending(&client, &proxy, "gated-sink").await;
        let (source, control) = GenericIo::finite_source(UPLOAD);
        let relay = tokio::spawn(pending.relay(source.relay_io()));
        eventually(Duration::from_secs(5), "the parked upload", || {
            control.read() > PROXY_STREAM_WINDOW as u64
        })
        .await;
        let parked = parked_reads(&control).await;
        assert!(
            parked <= (PROXY_STREAM_WINDOW + BOUND) as u64,
            "read {parked} bytes from the application past the proxy's window"
        );
        assert!(!relay.is_finished());
        proxy.open_gate();
        let outcome = relay.await.expect("relay joins");
        let length = UPLOAD as u64;
        assert_outcome(&outcome, RelayClose::Finished, None, (length, 0));
        assert_eq!(control.read(), length);
        assert_eq!(proxy.observations.uploaded(), length);
        assert_eq!(proxy.observations.upload_eofs(), 1);
        proxy
    });
}

#[test]
fn relay_polls_the_application_io_on_the_worker() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let pending = connect_pending(&client, &proxy, "echo").await;
        let (mut application, relayed) = tokio_sockets().await;
        let (io, control) = GenericIo::socket(relayed);
        let relay = tokio::spawn(pending.relay(io.relay_io()));
        for _ in 0..4 {
            round_trip(&mut application, PAYLOAD).await;
        }
        assert!(finish(&mut application).await.is_empty());
        let outcome = relay.await.expect("relay joins");
        assert_eq!(outcome.close(), RelayClose::Finished);
        assert_eq!(control.read(), 4 * PAYLOAD.len() as u64);
        assert_eq!(
            control.threads(),
            HashSet::from(["roxygen-connect".to_owned()]),
            "only the protocol worker polls the application I/O",
        );
        proxy
    });
}

#[test]
fn peer_reset_closes_the_application_socket() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let tap = Arc::new(Tap::default());
        let connector = Arc::new(TestConnector::plain(proxy.address).tapped(Arc::clone(&tap)));
        let mut sibling = client
            .connect("vm", request("echo"), connector)
            .await
            .expect("CONNECT")
            .into_tunnel();
        let reset = connect_pending(&client, &proxy, "reset").await;
        let (mut application, relayed) = sockets().await;
        let relay = tokio::spawn(reset.relay(relayed));
        // The proxy resets the stream once it has read this whole upload.
        application.write_all(b"last").await.expect("upload");
        let mut rest = Vec::new();
        application
            .read_to_end(&mut rest)
            .await
            .expect("a quiesced application reads end of stream");
        assert!(rest.is_empty());
        let outcome = relay.await.expect("relay joins");
        assert_outcome(
            &outcome,
            RelayClose::PeerReset,
            Some(io::ErrorKind::ConnectionReset),
            (4, 0),
        );
        // The sibling's round trip comes after anything the relay's ending wrote.
        echo(&mut sibling, b"the sibling is untouched").await;
        assert_eq!(
            written_resets(&tap.written()[0]),
            [],
            "the relay sent no RST_STREAM for the stream the proxy reset"
        );
        let stats = client.stats();
        assert_eq!(
            (
                stats.tunnels_relayed,
                stats.tunnels_reset_by_peer,
                stats.tunnels_aborted
            ),
            (1, 1, 0)
        );
        assert_eq!(proxy.connections(), 1);
        drop(sibling);
        proxy
    });

    // A reset after both directions ended discards the upload and END_STREAM that flow control
    // still held, so the relay does not finish.
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::with_config(ProxyConfig {
            initial_window_size: Some(0),
            ..Default::default()
        })
        .await;
        let tap = Arc::new(Tap::default());
        let connector = Arc::new(TestConnector::plain(proxy.address).tapped(Arc::clone(&tap)));
        let pending = client
            .connect("vm", request("early-eof-gated-reset"), connector)
            .await
            .expect("CONNECT");
        let (mut application, relayed) = tokio_sockets().await;
        let (io, control) = GenericIo::socket(relayed);
        let relay = tokio::spawn(pending.relay(io.relay_io()));
        application.write_all(PAYLOAD).await.expect("upload");
        assert!(
            finish(&mut application).await.is_empty(),
            "both directions ended"
        );
        eventually(Duration::from_secs(5), "the queued upload", || {
            control.read() == PAYLOAD.len() as u64
        })
        .await;
        assert!(!relay.is_finished(), "flow control still holds the upload");
        assert_eq!(written_data(&tap.written()[0]), 0);
        proxy.close_gate();
        let outcome = relay.await.expect("relay joins");
        assert_outcome(
            &outcome,
            RelayClose::PeerReset,
            Some(io::ErrorKind::ConnectionReset),
            (PAYLOAD.len() as u64, 0),
        );
        // The sibling's HEADERS come after anything the relay's ending wrote.
        let sibling = connect_pending(&client, &proxy, "hold-open").await;
        assert!(sibling.info().reused_session);
        assert_eq!(
            written_resets(&tap.written()[0]),
            [],
            "the relay sent no RST_STREAM for the stream the proxy reset"
        );
        let stats = client.stats();
        assert_eq!((stats.tunnels_reset_by_peer, stats.tunnels_aborted), (1, 0));
        drop(sibling);
        proxy
    });
}

#[test]
fn proxy_connection_loss_closes_every_relayed_socket() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::with_config(ProxyConfig {
            reset_on_close: true,
            ..Default::default()
        })
        .await;
        let mut relays = Vec::new();
        for _ in 0..3 {
            let pending = connect_pending(&client, &proxy, "echo").await;
            let (mut application, relayed) = sockets().await;
            let relay = tokio::spawn(pending.relay(relayed));
            round_trip(&mut application, PAYLOAD).await;
            relays.push((application, relay));
        }
        proxy.close_connections();
        for (mut application, relay) in relays {
            let mut rest = Vec::new();
            application
                .read_to_end(&mut rest)
                .await
                .expect("each quiesced application reads end of stream");
            assert!(rest.is_empty());
            let outcome = relay.await.expect("relay joins");
            let length = PAYLOAD.len() as u64;
            assert_outcome(
                &outcome,
                RelayClose::PeerReset,
                Some(io::ErrorKind::ConnectionReset),
                (length, length),
            );
        }
        assert_eq!(client.stats().tunnels_reset_by_peer, 3);
        let mut recovered = connect_pending(&client, &proxy, "echo").await.into_tunnel();
        echo(&mut recovered, b"on a new connection").await;
        assert_eq!(proxy.connections(), 2);
        drop(recovered);
        proxy
    });

    // A session that fails after both directions ended discards the upload and END_STREAM that
    // flow control still held, also while it drains after a graceful GOAWAY, whether a TCP reset
    // or an unanswered PING probe fails it.
    for probe_drop in [false, true] {
        // Only the probe-drop leg may drop a probe; the TCP-reset leg needs every one answered.
        let ping_timeout = Duration::from_millis(if probe_drop { 100 } else { 400 });
        let options = ConnectOptions {
            ping_interval: Duration::from_millis(100),
            ping_timeout,
            ..Default::default()
        };
        run(options, move |client| async move {
            let proxy = Proxy::with_config(ProxyConfig {
                initial_window_size: Some(0),
                reset_on_close: true,
                ..Default::default()
            })
            .await;
            let pending = connect_pending(&client, &proxy, "early-eof").await;
            let (mut application, relayed) = sockets().await;
            let relay = tokio::spawn(pending.relay(relayed));
            application.write_all(PAYLOAD).await.expect("upload");
            assert!(
                finish(&mut application).await.is_empty(),
                "both directions ended"
            );
            proxy.goaway();
            eventually(Duration::from_secs(5), "the session's drain", || {
                client.stats().session_drains == 1
            })
            .await;
            tokio::time::sleep(SETTLE).await;
            assert!(!relay.is_finished(), "flow control still holds the upload");
            if probe_drop {
                proxy.stop_reading();
            } else {
                proxy.close_connections();
            }
            let outcome = relay.await.expect("relay joins");
            assert_outcome(
                &outcome,
                RelayClose::PeerReset,
                Some(io::ErrorKind::ConnectionReset),
                (PAYLOAD.len() as u64, 0),
            );
            let stats = client.stats();
            assert_eq!(
                (
                    stats.tunnels_reset_by_peer,
                    stats.tunnels_aborted,
                    stats.ping_timeouts
                ),
                (1, 0, u64::from(probe_drop)),
            );
            proxy
        });
    }
}

/// How the proxy ends a stream after its download's END_STREAM.
#[derive(Clone, Copy, Debug)]
enum LateEnding {
    StreamReset,
    ConnectionReset,
    ErrorGoaway,
    ProbeDrop,
}

impl LateEnding {
    fn end(self, proxy: &Proxy) {
        match self {
            Self::StreamReset => proxy.close_gate(),
            Self::ConnectionReset => proxy.close_connections(),
            Self::ErrorGoaway => proxy.abort_connections(),
            Self::ProbeDrop => proxy.stop_reading(),
        }
    }
}

#[test]
fn peer_ending_after_the_download_end_closes_a_quiet_relay() {
    let options = ConnectOptions {
        max_tunnels: 1,
        idle_timeout: Duration::from_millis(200),
        ..Default::default()
    };
    for ending in [LateEnding::StreamReset, LateEnding::ConnectionReset] {
        for generic in [false, true] {
            let case = format!("{ending:?}, generic I/O {generic}");
            run(options.clone(), move |client| async move {
                let proxy = Proxy::with_config(ProxyConfig {
                    reset_on_close: true,
                    ..Default::default()
                })
                .await;
                let action = match ending {
                    LateEnding::StreamReset => "early-eof-gated-reset",
                    _ => "early-eof-sink",
                };
                let pending = connect_pending(&client, &proxy, action).await;
                let (mut application, relayed) = tokio_sockets().await;
                let (relay, control) = if generic {
                    let (io, control) = GenericIo::socket(relayed);
                    (tokio::spawn(pending.relay(io.relay_io())), Some(control))
                } else {
                    let relayed = relayed.into_std().expect("std socket");
                    (tokio::spawn(pending.relay(relayed)), None)
                };
                let mut download = Vec::new();
                application
                    .read_to_end(&mut download)
                    .await
                    .expect("the download's end");
                assert!(download.is_empty(), "{case}");
                tokio::time::sleep(SETTLE).await;
                assert!(
                    !relay.is_finished(),
                    "{case}: the open upload keeps the relay"
                );

                ending.end(&proxy);
                let outcome = timeout(Duration::from_secs(5), relay)
                    .await
                    .unwrap_or_else(|_| panic!("{case}: the peer ending resolves the quiet relay"))
                    .expect("relay joins");
                assert_outcome(
                    &outcome,
                    RelayClose::PeerReset,
                    Some(io::ErrorKind::ConnectionReset),
                    (0, 0),
                );
                let stats = client.stats();
                assert_eq!(
                    (
                        stats.tunnels_reset_by_peer,
                        stats.tunnels_aborted,
                        stats.active_tunnels
                    ),
                    (1, 0, 0),
                    "{case}"
                );
                if let Some(control) = control {
                    assert!(
                        control.dropped(),
                        "{case}: the I/O closes before the outcome"
                    );
                }
                // The relay's closed socket answers a write with a reset.
                timeout(Duration::from_secs(5), async {
                    while application.write_all(PAYLOAD).await.is_ok() {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .unwrap_or_else(|_| panic!("{case}: the relay closed the application socket"));

                eventually(
                    Duration::from_secs(5),
                    "the relay's route to retire",
                    || client.stats().routes == 0,
                )
                .await;
                let mut next = connect_pending(&client, &proxy, "echo").await.into_tunnel();
                echo(&mut next, PAYLOAD).await;
                assert_eq!(
                    client.stats().overloaded,
                    0,
                    "{case}: the relay returned its permit"
                );
                drop(next);
                proxy
            });
        }
    }
}

/// `download_ended()` tells a peer reset after the download's END_STREAM, which the application
/// read to its end, from a reset before it; a finished relay's download ended, and one whose
/// application write side failed to end did not.
#[test]
fn download_ended_tells_a_reset_after_the_download_end_from_an_earlier_one() {
    let greeting = GREETING.len() as u64;
    // DATA and END_STREAM, then RST_STREAM while the application's upload is quiet.
    run(ConnectOptions::default(), move |client| async move {
        let proxy = Proxy::start().await;
        let pending = connect_pending(&client, &proxy, "greet-end-gated-reset").await;
        let (mut application, relayed) = sockets().await;
        let relay = tokio::spawn(pending.relay(relayed));
        let mut download = Vec::new();
        application
            .read_to_end(&mut download)
            .await
            .expect("the download's end");
        assert_eq!(download, GREETING);
        proxy.close_gate();
        let outcome = relay.await.expect("relay joins");
        assert_outcome(
            &outcome,
            RelayClose::PeerReset,
            Some(io::ErrorKind::ConnectionReset),
            (0, greeting),
        );
        assert!(outcome.download_ended(), "{outcome:?}");
        assert!(outcome.peer_ended_download(), "{outcome:?}");
        assert!(outcome.stream_reset_by_peer(), "{outcome:?}");
        proxy
    });

    // DATA, then RST_STREAM before END_STREAM.
    run(ConnectOptions::default(), move |client| async move {
        let proxy = Proxy::start().await;
        let pending = connect_pending(&client, &proxy, "greet-reset").await;
        let (mut application, relayed) = sockets().await;
        let relay = tokio::spawn(pending.relay(relayed));
        let mut download = Vec::new();
        application
            .read_to_end(&mut download)
            .await
            .expect("the relay closes the application socket");
        assert_eq!(download, GREETING);
        let outcome = relay.await.expect("relay joins");
        assert_outcome(
            &outcome,
            RelayClose::PeerReset,
            Some(io::ErrorKind::ConnectionReset),
            (0, greeting),
        );
        assert!(!outcome.download_ended(), "{outcome:?}");
        assert!(!outcome.peer_ended_download(), "{outcome:?}");
        assert!(outcome.stream_reset_by_peer(), "{outcome:?}");
        proxy
    });

    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let pending = connect_pending(&client, &proxy, "echo").await;
        let (mut application, relayed) = sockets().await;
        let relay = tokio::spawn(pending.relay(relayed));
        round_trip(&mut application, PAYLOAD).await;
        assert!(finish(&mut application).await.is_empty());
        let outcome = relay.await.expect("relay joins");
        let length = PAYLOAD.len() as u64;
        assert_outcome(&outcome, RelayClose::Finished, None, (length, length));
        assert!(outcome.download_ended(), "{outcome:?}");
        assert!(outcome.peer_ended_download(), "{outcome:?}");
        assert!(!outcome.stream_reset_by_peer(), "{outcome:?}");

        // The shutdown that forwards the proxy's END_STREAM fails.
        let pending = connect_pending(&client, &proxy, "early-eof-sink").await;
        let (_application, relayed) = tokio_sockets().await;
        let (io, control) = GenericIo::socket(relayed);
        control.fail_shutdowns(io::ErrorKind::NotConnected);
        let outcome = pending.relay(io.relay_io()).await;
        assert_outcome(
            &outcome,
            RelayClose::Application,
            Some(io::ErrorKind::NotConnected),
            (0, 0),
        );
        assert!(!outcome.download_ended(), "{outcome:?}");
        assert!(outcome.peer_ended_download(), "{outcome:?}");
        assert!(!outcome.stream_reset_by_peer(), "{outcome:?}");
        proxy
    });
}

/// After the download's END_STREAM, `stream_reset_by_peer()` tells the proxy's RST_STREAM from a
/// proxy connection reset, an error GOAWAY, and a PING probe drop, all of which end the relay
/// `PeerReset` with the download ended.
#[test]
fn stream_reset_by_peer_tells_a_reset_from_a_session_ending_after_the_download_end() {
    let greeting = GREETING.len() as u64;
    for ending in [
        LateEnding::StreamReset,
        LateEnding::ConnectionReset,
        LateEnding::ErrorGoaway,
        LateEnding::ProbeDrop,
    ] {
        let probe_drop = matches!(ending, LateEnding::ProbeDrop);
        // Only the probe-drop leg may drop a probe.
        let options = ConnectOptions {
            ping_interval: Duration::from_millis(100),
            ping_timeout: Duration::from_millis(if probe_drop { 100 } else { 400 }),
            ..Default::default()
        };
        run(options, move |client| async move {
            let proxy = Proxy::with_config(ProxyConfig {
                reset_on_close: true,
                ..Default::default()
            })
            .await;
            let pending = connect_pending(&client, &proxy, "greet-end-gated-reset").await;
            let (mut application, relayed) = sockets().await;
            let relay = tokio::spawn(pending.relay(relayed));
            let mut download = Vec::new();
            application
                .read_to_end(&mut download)
                .await
                .expect("the download's end");
            assert_eq!(download, GREETING);
            ending.end(&proxy);
            let outcome = relay.await.expect("relay joins");
            assert_outcome(
                &outcome,
                RelayClose::PeerReset,
                Some(io::ErrorKind::ConnectionReset),
                (0, greeting),
            );
            assert!(outcome.download_ended(), "{ending:?}: {outcome:?}");
            assert!(outcome.peer_ended_download(), "{ending:?}: {outcome:?}");
            assert_eq!(
                outcome.stream_reset_by_peer(),
                matches!(ending, LateEnding::StreamReset),
                "{ending:?}: {outcome:?}"
            );
            let stats = client.stats();
            assert_eq!(
                (stats.tunnels_reset_by_peer, stats.ping_timeouts),
                (1, u64::from(probe_drop)),
                "{ending:?}"
            );
            proxy
        });
    }
}

/// A proxy that ends the download and at once resets the stream, as fwdproxy-hatch does when it
/// closes first, loses no download byte to an application that reads slowly: the relay writes
/// what it read ahead, then the end.
#[test]
fn completed_download_reaches_a_slow_application_after_a_peer_reset() {
    run(ConnectOptions::default(), |client| async move {
        let raw = RawProxy::start(RawScript::EndThenReset(ENDED_DOWNLOAD)).await;
        let pending = client
            .connect("vm", request("raw"), raw.connector())
            .await
            .expect("CONNECT");
        // The download's last frame outgrows these buffers, so the relay reads the END_STREAM
        // while it still holds download bytes.
        let (mut application, relayed, _) = small_buffer_sockets(1024).await;
        let relay = tokio::spawn(pending.relay(relayed));
        application.write_all(PAYLOAD).await.expect("upload");
        let mut download = Vec::new();
        let mut chunk = [0; 1024];
        loop {
            let read = application.read(&mut chunk).await.expect("the download");
            if read == 0 {
                break;
            }
            download.extend_from_slice(&chunk[..read]);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(
            download == pattern(ENDED_DOWNLOAD),
            "every byte, then end of stream: read {} of {ENDED_DOWNLOAD}",
            download.len()
        );
        let outcome = relay.await.expect("relay joins");
        assert_outcome(
            &outcome,
            RelayClose::PeerReset,
            Some(io::ErrorKind::ConnectionReset),
            (PAYLOAD.len() as u64, ENDED_DOWNLOAD as u64),
        );
        assert!(outcome.download_ended(), "{outcome:?}");
        assert!(outcome.peer_ended_download(), "{outcome:?}");
        assert!(outcome.stream_reset_by_peer(), "{outcome:?}");
        raw.shutdown().await;
    });
}

/// An upload write that fails because the proxy ended the download and reset the stream loses no
/// download byte, whether the upload flows or waits for credit.
#[test]
fn completed_download_survives_an_upload_write_error() {
    const UPLOAD: usize = 1 << 20;
    for script in [
        RawScript::EndThenReset(BEFORE_RESET),
        RawScript::EndThenResetOnFullWindow(BEFORE_RESET),
    ] {
        run(ConnectOptions::default(), move |client| async move {
            let raw = RawProxy::start(script).await;
            let pending = client
                .connect("vm", request("raw"), raw.connector())
                .await
                .expect("CONNECT");
            let (application, relayed) = sockets().await;
            let relay = tokio::spawn(pending.relay(relayed));
            let (mut reader, mut writer) = application.into_split();
            // The upload never ends; the relay stops reading it at the reset.
            let upload = tokio::spawn(async move {
                if writer.write_all(&pattern(UPLOAD)).await.is_ok() {
                    std::future::pending::<()>().await;
                }
            });
            let mut download = Vec::new();
            reader
                .read_to_end(&mut download)
                .await
                .expect("the download's end");
            assert!(
                download == pattern(BEFORE_RESET),
                "{script:?}: every byte, then end of stream: read {} of {BEFORE_RESET}",
                download.len()
            );
            let outcome = relay.await.expect("relay joins");
            assert_eq!(outcome.close(), RelayClose::PeerReset, "{outcome:?}");
            assert_eq!(
                outcome.error().map(io::Error::kind),
                Some(io::ErrorKind::ConnectionReset),
                "{outcome:?}"
            );
            assert_eq!(outcome.downloaded(), BEFORE_RESET as u64, "{outcome:?}");
            assert!(outcome.download_ended(), "{script:?}: {outcome:?}");
            assert!(outcome.peer_ended_download(), "{script:?}: {outcome:?}");
            assert!(outcome.stream_reset_by_peer(), "{script:?}: {outcome:?}");
            upload.abort();
            raw.shutdown().await;
        });
    }
}

/// After an upload write error, a relay whose application keeps up writes a long download whose
/// END_STREAM came before the reset: running out of the task's coop budget part way through is
/// no application stall.
#[test]
fn coop_budget_is_no_application_stall_after_an_upload_write_error() {
    // More copy buffers than one task poll's budget of 128 writes.
    const DOWNLOAD: usize = 200 * COPY_BUFFER;
    let options = ConnectOptions {
        stream_receive_window: 8 << 20,
        ..Default::default()
    };
    run(options, |client| async move {
        let raw = RawProxy::start(RawScript::EndThenResetAtOnce(DOWNLOAD)).await;
        let pending = client
            .connect("vm", request("raw"), raw.connector())
            .await
            .expect("CONNECT");
        // The stream holds the download, its END_STREAM, and the reset until the choice, so the
        // relay's first upload write fails before the download moves.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let (mut io, control) = GenericIo::source();
        io.coop_writes = true;
        let outcome = pending.relay(io.relay_io()).await;
        assert_eq!(outcome.close(), RelayClose::PeerReset, "{outcome:?}");
        assert_eq!(
            outcome.error().map(io::Error::kind),
            Some(io::ErrorKind::ConnectionReset),
            "{outcome:?}"
        );
        assert_eq!(outcome.downloaded(), DOWNLOAD as u64, "{outcome:?}");
        assert!(outcome.download_ended(), "{outcome:?}");
        assert!(outcome.peer_ended_download(), "{outcome:?}");
        assert!(outcome.stream_reset_by_peer(), "{outcome:?}");
        assert!(
            control.written() == pattern(DOWNLOAD),
            "every download byte"
        );
        assert_eq!(control.shutdowns(), 1, "then the download's end");
        raw.shutdown().await;
    });
}

/// When `shutdown_timeout` cuts short the writing of a download whose END_STREAM the relay has
/// read, `peer_ended_download()` still reports the proxy's end and `download_ended()` does not;
/// the application reads every written byte, then end of stream.
#[test]
fn cut_short_delivery_still_reports_the_proxys_download_end() {
    const DOWNLOAD: usize = COPY_BUFFER - 1;
    let options = ConnectOptions {
        shutdown_timeout: Duration::from_millis(300),
        ..Default::default()
    };
    run(options, |client| async move {
        let raw = RawProxy::start(RawScript::EndThenReset(DOWNLOAD)).await;
        let pending = client
            .connect("vm", request("raw"), raw.connector())
            .await
            .expect("CONNECT");
        // One read takes the whole download and the next its END_STREAM, while these buffers
        // hold only part of it.
        let (mut application, relayed, _) = small_buffer_sockets(1024).await;
        let relay = tokio::spawn(pending.relay(relayed));
        application.write_all(PAYLOAD).await.expect("upload");
        let outcome = relay.await.expect("relay joins");
        assert_eq!(outcome.close(), RelayClose::PeerReset, "{outcome:?}");
        assert_eq!(
            outcome.error().map(io::Error::kind),
            Some(io::ErrorKind::ConnectionReset),
            "{outcome:?}"
        );
        assert!(outcome.stream_reset_by_peer(), "{outcome:?}");
        assert!(outcome.peer_ended_download(), "{outcome:?}");
        assert!(!outcome.download_ended(), "{outcome:?}");
        let written = outcome.downloaded() as usize;
        assert!(written < DOWNLOAD, "{outcome:?}");
        let mut download = Vec::new();
        application
            .read_to_end(&mut download)
            .await
            .expect("the written bytes, then end of stream");
        assert!(
            download == pattern(written),
            "read {} of {written}",
            download.len()
        );
        raw.shutdown().await;
    });
}

/// An upload write error before the download's END_STREAM, with an application that has stopped
/// reading, ends the relay at once: it discards the download for the read error that explains
/// the failure instead of waiting `shutdown_timeout` for the application.
#[test]
fn upload_write_error_before_the_download_end_discards_at_once() {
    const UPLOAD: usize = 1 << 20;
    let shutdown_timeout = Duration::from_secs(5);
    let options = ConnectOptions {
        shutdown_timeout,
        ..Default::default()
    };
    run(options, move |client| async move {
        let raw = RawProxy::start(RawScript::DataThenLateResetOnFullWindow(ENDED_DOWNLOAD)).await;
        let pending = client
            .connect("vm", request("raw"), raw.connector())
            .await
            .expect("CONNECT");
        let (application, relayed, _) = small_buffer_sockets(1024).await;
        let relay = tokio::spawn(pending.relay(relayed));
        let (_reader, mut writer) = application.into_split();
        let upload = tokio::spawn(async move {
            if writer.write_all(&pattern(UPLOAD)).await.is_ok() {
                std::future::pending::<()>().await;
            }
        });
        let outcome = timeout(shutdown_timeout / 2, relay)
            .await
            .expect("the relay ends well before shutdown_timeout")
            .expect("relay joins");
        assert_eq!(outcome.close(), RelayClose::PeerReset, "{outcome:?}");
        assert_eq!(
            outcome.error().map(io::Error::kind),
            Some(io::ErrorKind::ConnectionReset),
            "{outcome:?}"
        );
        assert!(!outcome.download_ended(), "{outcome:?}");
        assert!(!outcome.peer_ended_download(), "{outcome:?}");
        upload.abort();
        raw.shutdown().await;
    });
}

#[test]
fn goaway_with_an_error_closes_the_relayed_socket() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let pending = connect_pending(&client, &proxy, "echo").await;
        let (mut application, relayed) = sockets().await;
        let relay = tokio::spawn(pending.relay(relayed));
        round_trip(&mut application, PAYLOAD).await;
        proxy.abort_connections();
        let mut rest = Vec::new();
        application
            .read_to_end(&mut rest)
            .await
            .expect("a quiesced application reads end of stream");
        assert!(rest.is_empty());
        let outcome = relay.await.expect("relay joins");
        let length = PAYLOAD.len() as u64;
        assert_outcome(
            &outcome,
            RelayClose::PeerReset,
            Some(io::ErrorKind::ConnectionReset),
            (length, length),
        );
        assert_eq!(client.stats().tunnels_reset_by_peer, 1);
        proxy
    });

    // The GOAWAY alone ends the relay: this proxy keeps its socket open after it.
    run(ConnectOptions::default(), |client| async move {
        let raw = RawProxy::start(RawScript::ErrorGoawayOnSecondData).await;
        let pending = client
            .connect("vm", request("echo"), raw.connector())
            .await
            .expect("CONNECT");
        let (mut application, relayed) = sockets().await;
        let relay = tokio::spawn(pending.relay(relayed));
        round_trip(&mut application, PAYLOAD).await;
        application.write_all(b"last").await.expect("upload");
        let mut rest = Vec::new();
        application
            .read_to_end(&mut rest)
            .await
            .expect("a quiesced application reads end of stream");
        assert!(rest.is_empty());
        let outcome = relay.await.expect("relay joins");
        let length = PAYLOAD.len() as u64;
        assert_outcome(
            &outcome,
            RelayClose::PeerReset,
            Some(io::ErrorKind::ConnectionReset),
            (length + 4, length),
        );
        assert_eq!(client.stats().tunnels_reset_by_peer, 1);
        raw.shutdown().await;
    });
}

#[test]
fn application_io_error_aborts_the_stream() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let length = PAYLOAD.len() as u64;

        let pending_read = connect_pending(&client, &proxy, "echo").await;
        let (mut application, relayed) = tokio_sockets().await;
        let (io, control) = GenericIo::socket(relayed);
        let relay = tokio::spawn(pending_read.relay(io.relay_io()));
        round_trip(&mut application, PAYLOAD).await;
        control.fail_reads(io::ErrorKind::InvalidData);
        let outcome = relay.await.expect("relay joins");
        assert_outcome(
            &outcome,
            RelayClose::Application,
            Some(io::ErrorKind::InvalidData),
            (length, length),
        );
        assert!(control.dropped(), "the I/O closes before the outcome");
        assert_eq!(control.shutdowns(), 0, "the I/O drops without a shutdown");
        proxy
            .observations
            .wait("the stream's reset", |observed| {
                observed.cancel_resets() == 1
            })
            .await;
        let _ = application.read_to_end(&mut Vec::new()).await;

        let pending_write = connect_pending(&client, &proxy, "echo").await;
        let (mut application, relayed) = tokio_sockets().await;
        let (io, control) = GenericIo::socket(relayed);
        let relay = tokio::spawn(pending_write.relay(io.relay_io()));
        round_trip(&mut application, PAYLOAD).await;
        control.fail_writes(io::ErrorKind::BrokenPipe);
        application.write_all(PAYLOAD).await.expect("upload");
        let outcome = relay.await.expect("relay joins");
        assert_outcome(
            &outcome,
            RelayClose::Application,
            Some(io::ErrorKind::BrokenPipe),
            (2 * length, length),
        );
        assert!(control.dropped(), "the I/O closes before the outcome");
        assert_eq!(control.shutdowns(), 0, "the I/O drops without a shutdown");
        proxy
            .observations
            .wait("the second reset", |observed| observed.cancel_resets() == 2)
            .await;

        // The shutdown that forwards the proxy's END_STREAM fails.
        let pending_shutdown = connect_pending(&client, &proxy, "early-eof-sink").await;
        let (_application, relayed) = tokio_sockets().await;
        let (io, control) = GenericIo::socket(relayed);
        control.fail_shutdowns(io::ErrorKind::NotConnected);
        let outcome = pending_shutdown.relay(io.relay_io()).await;
        assert_outcome(
            &outcome,
            RelayClose::Application,
            Some(io::ErrorKind::NotConnected),
            (0, 0),
        );
        proxy
            .observations
            .wait("the third reset", |observed| observed.cancel_resets() == 3)
            .await;

        // The worker cannot register the socket: epoll refuses `/dev/null`.
        let pending_adopt = connect_pending(&client, &proxy, "echo").await;
        let unpollable = File::open("/dev/null").expect("/dev/null");
        let socket = std::net::TcpStream::from(OwnedFd::from(unpollable));
        let outcome = pending_adopt.relay(socket).await;
        assert_outcome(
            &outcome,
            RelayClose::Application,
            Some(io::ErrorKind::PermissionDenied),
            (0, 0),
        );
        proxy
            .observations
            .wait("the fourth reset", |observed| observed.cancel_resets() == 4)
            .await;
        let stats = client.stats();
        assert_eq!((stats.tunnels_aborted, stats.tunnels_reset_by_peer), (4, 0));
        assert_eq!(proxy.connections(), 1);
        proxy
    });
}

#[test]
fn dropping_the_relay_future_aborts_the_stream() {
    let options = ConnectOptions {
        max_tunnels: 1,
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let pending = connect_pending(&client, &proxy, "echo").await;
        let (mut application, relayed) = sockets().await;
        let relay = tokio::spawn(pending.relay(relayed));
        round_trip(&mut application, PAYLOAD).await;
        relay.abort();
        proxy
            .observations
            .wait("the stream's reset", |observed| {
                observed.cancel_resets() == 1
            })
            .await;
        let _ = application.read_to_end(&mut Vec::new()).await;
        eventually(Duration::from_secs(5), "the relay's end", || {
            client.stats().active_tunnels == 0
        })
        .await;
        assert_eq!(client.stats().tunnels_aborted, 1);
        let mut next = connect_pending(&client, &proxy, "echo").await.into_tunnel();
        echo(&mut next, PAYLOAD).await;
        drop(next);
        proxy
    });

    // Dropped after both directions closed on the wire, while the shutdown that forwards the
    // proxy's END_STREAM stalls, the relay sends nothing and counts no abort.
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let tap = Arc::new(Tap::default());
        let connector = Arc::new(TestConnector::plain(proxy.address).tapped(Arc::clone(&tap)));
        let pending = client
            .connect("vm", request("early-eof-sink"), connector)
            .await
            .expect("CONNECT");
        let (mut application, relayed) = tokio_sockets().await;
        let (mut io, control) = GenericIo::socket(relayed);
        io.stall_shutdown = true;
        let relay = tokio::spawn(pending.relay(io.relay_io()));
        application.write_all(PAYLOAD).await.expect("upload");
        application.shutdown().await.expect("upload EOF");
        let length = PAYLOAD.len() as u64;
        proxy
            .observations
            .wait("the upload and its end", |observed| {
                observed.uploaded() == length && observed.upload_eofs() == 1
            })
            .await;
        assert!(
            control.shutdowns() > 0,
            "the forwarding shutdown is pending"
        );
        tokio::time::sleep(SETTLE).await;
        assert!(!relay.is_finished(), "the stalled shutdown holds the relay");
        relay.abort();
        eventually(Duration::from_secs(5), "the relay's end", || {
            client.stats().active_tunnels == 0
        })
        .await;
        assert!(control.dropped());
        // The sibling's round trip comes after anything the drop wrote.
        let mut sibling = connect_pending(&client, &proxy, "echo").await.into_tunnel();
        echo(&mut sibling, PAYLOAD).await;
        assert_eq!(
            written_resets(&tap.written()[0]),
            [],
            "the drop sent nothing"
        );
        assert_eq!(client.stats().tunnels_aborted, 0);
        drop(sibling);
        proxy
    });
}

#[test]
fn relay_dropped_before_its_upload_end_reaches_the_wire_aborts() {
    let options = ConnectOptions {
        max_streams: 1,
        max_connections: 1,
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::with_config(ProxyConfig {
            initial_window_size: Some(0),
            ..Default::default()
        })
        .await;
        let blocked = connect_pending(&client, &proxy, "early-eof").await;
        let (mut application, relayed) = sockets().await;
        let relay = tokio::spawn(blocked.relay(relayed));
        application
            .write_all(b"an upload the proxy's window never admits")
            .await
            .expect("upload");
        assert!(
            finish(&mut application).await.is_empty(),
            "both directions ended"
        );
        tokio::time::sleep(SETTLE).await;
        assert!(
            !relay.is_finished(),
            "the relay keeps its abort until its stream is closed on the wire",
        );
        assert_eq!(proxy.observations.cancel_resets(), 0);
        relay.abort();
        proxy
            .observations
            .wait("the blocked stream's reset", |observed| {
                observed.cancel_resets() == 1
            })
            .await;
        let sibling = connect_pending(&client, &proxy, "early-eof").await;
        assert!(
            sibling.info().reused_session,
            "the reset freed the session's only stream slot"
        );
        assert_eq!(proxy.connections(), 1);
        assert_eq!(client.stats().tunnels_aborted, 1);
        drop(sibling);
        proxy
    });
}

#[test]
fn shutdown_lets_relays_finish_then_aborts_the_rest() {
    let shutdown_timeout = Duration::from_millis(500);
    let options = ConnectOptions {
        shutdown_timeout,
        ..Default::default()
    };
    run_owned(options, |runtime, client| async move {
        let proxy = Proxy::start().await;
        let tap = Arc::new(Tap::default());
        let connector = Arc::new(TestConnector::plain(proxy.address).tapped(Arc::clone(&tap)));
        let finishing = client
            .connect("vm", request("late-response:100"), connector.clone())
            .await
            .expect("CONNECT");
        let (mut finishing_application, relayed) = sockets().await;
        let finishing_relay = tokio::spawn(finishing.relay(relayed));
        let held = client
            .connect("vm", request("hold-open"), connector)
            .await
            .expect("CONNECT");
        let (mut held_application, relayed) = tokio_sockets().await;
        let (mut held_io, held_control) = GenericIo::socket(relayed);
        held_io.stall_shutdown = true;
        let held_relay = tokio::spawn(held.relay(held_io.relay_io()));
        round_trip(&mut held_application, PAYLOAD).await;
        finishing_application
            .write_all(PAYLOAD)
            .await
            .expect("upload");

        let started = Instant::now();
        let shutting_down = tokio::spawn(shutdown(runtime));
        let received = finish(&mut finishing_application).await;
        let mut expected = PAYLOAD.to_vec();
        expected.extend_from_slice(AFTER_HALF_CLOSE);
        assert_eq!(
            received, expected,
            "the relay finishes while the runtime drains"
        );
        let finished = finishing_relay.await.expect("relay joins");
        assert_outcome(
            &finished,
            RelayClose::Finished,
            None,
            (PAYLOAD.len() as u64, expected.len() as u64),
        );
        let aborted = held_relay.await.expect("relay joins");
        let length = PAYLOAD.len() as u64;
        assert_outcome(
            &aborted,
            RelayClose::Aborted,
            Some(io::ErrorKind::ConnectionAborted),
            (length, length),
        );
        assert!(held_control.dropped());
        assert_eq!(
            held_control.shutdowns(),
            0,
            "the forced phase drops the I/O without a shutdown"
        );
        let _ = held_application.read_to_end(&mut Vec::new()).await;
        shutting_down
            .await
            .expect("shutdown task")
            .expect("shutdown");
        let took = started.elapsed();
        assert!(
            took < shutdown_timeout + Duration::from_secs(2),
            "shutdown took {took:?}"
        );
        proxy
            .observations
            .wait("the proxy connection to close", |observed| {
                observed.closed() == 1
            })
            .await;
        // The finishing stream is the session's first; the held stream's reset may not be sent.
        assert!(
            !written_resets(&tap.written()[0])
                .iter()
                .any(|&(stream, _)| stream == 1),
            "the finished relay sent no RST_STREAM"
        );
        let stats = client.stats();
        assert_eq!((stats.tunnels_aborted, stats.active_tunnels), (1, 0));
        proxy
    });
}

/// Once the relay has begun shutting down the application I/O's write half, it polls only that
/// shutdown: a writer whose shutdown is Pending once and that fails a flush after it, as an
/// encoder does, still gets the download's end.
#[test]
fn relay_polls_only_the_shutdown_once_it_began() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let pending = connect_pending(&client, &proxy, "early-eof-sink").await;
        let (mut application, relayed) = tokio_sockets().await;
        let (mut io, control) = GenericIo::socket(relayed);
        io.strict_shutdown = true;
        let relay = tokio::spawn(pending.relay(io.relay_io()));
        application.write_all(PAYLOAD).await.expect("upload");
        application.shutdown().await.expect("upload EOF");
        let outcome = relay.await.expect("relay joins");
        let length = PAYLOAD.len() as u64;
        assert_outcome(&outcome, RelayClose::Finished, None, (length, 0));
        assert!(outcome.download_ended(), "{outcome:?}");
        assert_eq!(
            control.shutdowns(),
            2,
            "the shutdown that was Pending, then the one that completed"
        );
        let mut download = Vec::new();
        application
            .read_to_end(&mut download)
            .await
            .expect("the download's end");
        assert!(download.is_empty());
        proxy
    });
}

/// A relay that has written the download of a `greet-end-gated-reset` stream and whose
/// forwarding shutdown stalls, with the application, which has read the download, and its I/O.
async fn relay_stalled_after_the_download_end(
    client: &Client,
    proxy: &Proxy,
) -> (
    tokio::task::JoinHandle<RelayOutcome>,
    TcpStream,
    Arc<IoControl>,
) {
    let pending = connect_pending(client, proxy, "greet-end-gated-reset").await;
    let (mut application, relayed) = tokio_sockets().await;
    let (mut io, control) = GenericIo::socket(relayed);
    io.stall_shutdown = true;
    let relay = tokio::spawn(pending.relay(io.relay_io()));
    let mut greeting = vec![0; GREETING.len()];
    application
        .read_exact(&mut greeting)
        .await
        .expect("the download");
    assert_eq!(greeting, GREETING);
    eventually(Duration::from_secs(5), "the forwarding shutdown", || {
        control.shutdowns() > 0
    })
    .await;
    (relay, application, control)
}

#[test]
fn stalled_application_io_shutdown_is_bounded() {
    let shutdown_timeout = Duration::from_millis(300);
    let options = ConnectOptions {
        shutdown_timeout,
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let reset = connect_pending(&client, &proxy, "reset").await;
        let (mut application, relayed) = tokio_sockets().await;
        let (mut io, control) = GenericIo::socket(relayed);
        io.stall_shutdown = true;
        let relay = tokio::spawn(reset.relay(io.relay_io()));
        let started = Instant::now();
        application.write_all(b"last").await.expect("upload");
        let outcome = relay.await.expect("relay joins");
        let took = started.elapsed();
        assert!(
            took >= shutdown_timeout && took < shutdown_timeout + Duration::from_secs(2),
            "the stalled shutdown ended after {took:?}"
        );
        assert_outcome(
            &outcome,
            RelayClose::PeerReset,
            Some(io::ErrorKind::ConnectionReset),
            (4, 0),
        );
        assert!(control.dropped(), "the I/O closes before the outcome");
        let _ = application.read_to_end(&mut Vec::new()).await;
        assert_eq!(client.stats().tunnels_reset_by_peer, 1);
        proxy
    });

    // The forced shutdown phase cuts the stalled shutdown short and keeps its ending: the reset
    // is delayed to separate the graceful-phase and relay shutdown deadlines.
    let shutdown_timeout = Duration::from_secs(3);
    let options = ConnectOptions {
        shutdown_timeout,
        max_tunnels: 1,
        ..Default::default()
    };
    run_owned(options.clone(), |runtime, client| async move {
        let proxy = Proxy::start().await;
        let reset = connect_pending(&client, &proxy, "reset").await;
        let (mut application, relayed) = tokio_sockets().await;
        let (mut io, control) = GenericIo::socket(relayed);
        io.stall_shutdown = true;
        let relay = tokio::spawn(reset.relay(io.relay_io()));
        let started = Instant::now();
        let shutting_down = tokio::spawn(shutdown(runtime));
        admission_closed(&client, &proxy).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        // The proxy resets the stream once it has read this upload.
        application.write_all(b"last").await.expect("upload");
        let outcome = relay.await.expect("relay joins");
        assert_outcome(
            &outcome,
            RelayClose::PeerReset,
            Some(io::ErrorKind::ConnectionReset),
            (4, 0),
        );
        shutting_down
            .await
            .expect("shutdown task")
            .expect("shutdown");
        let took = started.elapsed();
        assert!(
            took < shutdown_timeout + Duration::from_secs(2),
            "shutdown took {took:?}"
        );
        let cut = control
            .shutdown_to_drop()
            .expect("the relay began its shutdown before the drop");
        assert!(
            cut < shutdown_timeout,
            "the forced phase dropped the I/O {cut:?} into its shutdown, before the relay's own bound"
        );
        let stats = client.stats();
        assert_eq!((stats.tunnels_reset_by_peer, stats.tunnels_aborted), (1, 0));
        proxy
    });

    // A peer reset after the download's END_STREAM is recorded before the relay finishes the
    // download, so the forced phase, cutting that short, keeps its ending and counts it once.
    let options = ConnectOptions {
        shutdown_timeout: Duration::from_millis(400),
        ..options
    };
    run_owned(options.clone(), |runtime, client| async move {
        let proxy = Proxy::start().await;
        let (relay, _application, control) =
            relay_stalled_after_the_download_end(&client, &proxy).await;
        let shutting_down = tokio::spawn(shutdown(runtime));
        admission_closed(&client, &proxy).await;
        proxy.close_gate();
        let outcome = relay.await.expect("relay joins");
        assert_outcome(
            &outcome,
            RelayClose::PeerReset,
            Some(io::ErrorKind::ConnectionReset),
            (0, GREETING.len() as u64),
        );
        assert!(outcome.stream_reset_by_peer(), "{outcome:?}");
        assert!(outcome.peer_ended_download(), "{outcome:?}");
        assert!(!outcome.download_ended(), "{outcome:?}");
        assert!(control.dropped());
        shutting_down
            .await
            .expect("shutdown task")
            .expect("shutdown");
        let stats = client.stats();
        assert_eq!(
            (
                stats.tunnels_reset_by_peer,
                stats.tunnels_aborted,
                stats.active_tunnels
            ),
            (1, 0, 0)
        );
        proxy
    });

    // So does dropping the relay future while the relay finishes the download, and the relay
    // then returns its permit.
    let options = ConnectOptions {
        shutdown_timeout: Duration::from_secs(2),
        ..options
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let (relay, _application, control) =
            relay_stalled_after_the_download_end(&client, &proxy).await;
        proxy.close_gate();
        tokio::time::sleep(SETTLE).await;
        relay.abort();
        eventually(Duration::from_secs(5), "the relay's cleanup", || {
            control.dropped() && client.stats().active_tunnels == 0
        })
        .await;
        let stats = client.stats();
        assert_eq!((stats.tunnels_reset_by_peer, stats.tunnels_aborted), (1, 0));
        let next = connect_pending(&client, &proxy, "echo").await;
        drop(next);
        assert_eq!(
            client.stats().overloaded,
            0,
            "the relay returned its permit"
        );
        proxy
    });

    // The shutdown that forwards the proxy's END_STREAM has no bound of its own: the upload keeps
    // flowing past `shutdown_timeout`, or stays quiet, and a peer ending that finds the shutdown
    // pending bounds it from then.
    let shutdown_timeout = Duration::from_millis(300);
    let options = ConnectOptions {
        shutdown_timeout,
        ..Default::default()
    };
    for uploading in [true, false] {
        run(options.clone(), move |client| async move {
            let proxy = Proxy::start().await;
            let pending = connect_pending(&client, &proxy, "early-eof-sink").await;
            let (mut application, relayed) = tokio_sockets().await;
            let (mut io, control) = GenericIo::socket(relayed);
            io.stall_shutdown = true;
            let relay = tokio::spawn(pending.relay(io.relay_io()));
            let upload = tokio::spawn(async move {
                if uploading {
                    while application.write_all(PAYLOAD).await.is_ok() {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                } else {
                    let _ = application.read_to_end(&mut Vec::new()).await;
                }
            });
            eventually(Duration::from_secs(5), "the forwarding shutdown", || {
                control.shutdowns() > 0
            })
            .await;
            let before = proxy.observations.uploaded();
            tokio::time::sleep(2 * shutdown_timeout).await;
            assert_eq!(
                proxy.observations.uploaded() > before,
                uploading,
                "the upload flows only if the application writes while the shutdown is pending"
            );
            assert!(!relay.is_finished(), "uploading {uploading}");
            let reset = Instant::now();
            proxy.abort_connections();
            let outcome = timeout(Duration::from_secs(5), relay)
                .await
                .unwrap_or_else(|_| panic!("uploading {uploading}: the peer ending resolves it"))
                .expect("relay joins");
            let took = reset.elapsed();
            assert!(
                took >= shutdown_timeout && took < shutdown_timeout + Duration::from_secs(2),
                "uploading {uploading}: the relay closed the I/O {took:?} after the reset"
            );
            assert_eq!(outcome.close(), RelayClose::PeerReset, "{outcome:?}");
            assert_eq!(
                outcome.error().map(io::Error::kind),
                Some(io::ErrorKind::ConnectionReset)
            );
            assert_eq!(outcome.downloaded(), 0);
            assert!(outcome.uploaded() >= proxy.observations.uploaded());
            assert_eq!(outcome.uploaded() > 0, uploading, "{outcome:?}");
            assert!(control.dropped());
            upload.await.expect("upload task");
            assert_eq!(client.stats().tunnels_reset_by_peer, 1);
            proxy
        });
    }
}

#[test]
fn relay_after_shutdown_closes_the_io_and_resolves_aborted() {
    let options = ConnectOptions {
        shutdown_timeout: Duration::from_millis(300),
        ..Default::default()
    };
    run_owned(options, |runtime, client| async move {
        let proxy = Proxy::start().await;
        let pending = connect_pending(&client, &proxy, "hold-open").await;
        shutdown(runtime).await.expect("shutdown");
        let (mut application, relayed) = sockets().await;
        let outcome = pending.relay(relayed).await;
        assert_outcome(
            &outcome,
            RelayClose::Aborted,
            Some(io::ErrorKind::ConnectionAborted),
            (0, 0),
        );
        let mut rest = Vec::new();
        application
            .read_to_end(&mut rest)
            .await
            .expect("the relay closed the application socket");
        assert!(rest.is_empty());
        proxy
    });
}

/// A small deterministic generator for the stress test's choices.
struct Choices(u64);

impl Choices {
    fn next(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % bound
    }
}

/// How one stress connect ends.
#[derive(Clone, Copy)]
enum Ending {
    Finish,
    PeerReset,
    ApplicationError,
    FutureDrop(Duration),
    Unchosen,
    BridgeDrop,
}

async fn end_relay(
    client: Client,
    connector: Arc<TestConnector>,
    key: &'static str,
    ending: Ending,
) {
    let action = match ending {
        Ending::PeerReset => "reset",
        _ => "echo",
    };
    let pending = client
        .connect(key, request(action), connector)
        .await
        .expect("every stress connect succeeds");
    match ending {
        Ending::Finish => {
            let (mut application, relayed) = sockets().await;
            let relay = tokio::spawn(pending.relay(relayed));
            round_trip(&mut application, b"stress").await;
            finish(&mut application).await;
            assert_eq!(relay.await.expect("relay").close(), RelayClose::Finished);
        }
        Ending::PeerReset => {
            let (mut application, relayed) = sockets().await;
            let relay = tokio::spawn(pending.relay(relayed));
            application.write_all(b"last").await.expect("upload");
            let _ = application.read_to_end(&mut Vec::new()).await;
            assert_eq!(relay.await.expect("relay").close(), RelayClose::PeerReset);
        }
        Ending::ApplicationError => {
            let (mut application, relayed) = tokio_sockets().await;
            let (io, control) = GenericIo::socket(relayed);
            let relay = tokio::spawn(pending.relay(io.relay_io()));
            round_trip(&mut application, b"stress").await;
            control.fail_reads(io::ErrorKind::InvalidData);
            assert_eq!(relay.await.expect("relay").close(), RelayClose::Application);
        }
        Ending::FutureDrop(after) => {
            let (application, relayed) = sockets().await;
            assert!(
                timeout(after, pending.relay(relayed)).await.is_err(),
                "an open echo relay outlives the caller's wait"
            );
            drop(application);
        }
        Ending::Unchosen => drop(pending),
        Ending::BridgeDrop => {
            let mut tunnel = pending.into_tunnel();
            let _ = tunnel.write_all(b"stress").await;
        }
    }
}

#[test]
fn relay_endings_release_admission_and_route_once() {
    const TUNNELS: usize = 32;
    let options = ConnectOptions {
        max_tunnels: TUNNELS,
        max_streams: 16,
        idle_timeout: Duration::from_millis(200),
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let connector = proxy.connector();
        let mut choices = Choices(0x9e37_79b9_7f4a_7c15);
        let mut work = JoinSet::new();
        for _ in 0..300 {
            let key = ["a", "b", "c"][choices.next(3) as usize];
            let ending = match choices.next(6) {
                0 => Ending::Finish,
                1 => Ending::PeerReset,
                2 => Ending::ApplicationError,
                3 => Ending::FutureDrop(Duration::from_millis(choices.next(10))),
                4 => Ending::Unchosen,
                _ => Ending::BridgeDrop,
            };
            work.spawn(end_relay(client.clone(), connector.clone(), key, ending));
            if work.len() >= TUNNELS / 2 {
                work.join_next().await.expect("work").expect("stress task");
            }
        }
        while let Some(joined) = work.join_next().await {
            joined.expect("stress task");
        }
        eventually(Duration::from_secs(10), "admission accounting", || {
            let stats = client.stats();
            stats.waiters == 0 && stats.active_tunnels == 0
        })
        .await;
        let mut held = JoinSet::new();
        for _ in 0..TUNNELS {
            let client = client.clone();
            let connector = connector.clone();
            held.spawn(async move { client.connect("a", request("hold-open"), connector).await });
        }
        let mut pendings = Vec::new();
        while let Some(joined) = held.join_next().await {
            pendings.push(
                joined
                    .expect("caller joins")
                    .expect("every permit returned"),
            );
        }
        assert_eq!(pendings.len(), TUNNELS);
        // A permit released twice would admit one more connect.
        let excess = timeout(
            SETTLE,
            client.connect("a", request("hold-open"), connector.clone()),
        )
        .await
        .expect("admission fails at once")
        .expect_err("every permit is held");
        assert_eq!(excess.kind(), ConnectErrorKind::Overloaded);
        assert_eq!(client.stats().overloaded, 1);
        drop(pendings);
        eventually(Duration::from_secs(10), "idle routes to retire", || {
            client.stats().routes == 0
        })
        .await;
        let stats = client.stats();
        assert_eq!((stats.active_tunnels, stats.overloaded), (0, 1));
        proxy
    });

    // At the admission bound, a caller that awaited an outcome connects again at once.
    let options = ConnectOptions {
        max_tunnels: 1,
        ..Default::default()
    };
    run(options, |client| async move {
        let proxy = Proxy::start().await;
        let connector = proxy.connector();
        for ending in [Ending::Finish, Ending::PeerReset, Ending::ApplicationError] {
            end_relay(client.clone(), connector.clone(), "vm", ending).await;
            assert_eq!(
                client.stats().active_tunnels,
                0,
                "the relay released its admission before its outcome"
            );
        }
        let mut next = connect_pending(&client, &proxy, "echo").await.into_tunnel();
        echo(&mut next, PAYLOAD).await;
        assert_eq!(client.stats().overloaded, 0);
        drop(next);
        proxy
    });
}

#[test]
fn relay_stats_count_relays_and_their_endings() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let connector = proxy.connector();
        for ending in [
            Ending::Finish,
            Ending::PeerReset,
            Ending::ApplicationError,
            Ending::Unchosen,
        ] {
            end_relay(client.clone(), connector.clone(), "vm", ending).await;
        }
        let dropped = connect_pending(&client, &proxy, "echo").await;
        let (mut application, relayed) = sockets().await;
        let relay = tokio::spawn(dropped.relay(relayed));
        round_trip(&mut application, PAYLOAD).await;
        relay.abort();
        let mut bridged = connect_pending(&client, &proxy, "echo").await.into_tunnel();
        echo(&mut bridged, PAYLOAD).await;
        bridged.shutdown().await.expect("upload EOF");
        bridged
            .read_to_end(&mut Vec::new())
            .await
            .expect("download EOF");
        drop(bridged);
        eventually(Duration::from_secs(5), "every ending", || {
            client.stats().active_tunnels == 0
        })
        .await;
        let stats = client.stats();
        assert_eq!(stats.tunnels_established, 6);
        assert_eq!(stats.tunnels_relayed, 4);
        assert_eq!(stats.tunnels_reset_by_peer, 1);
        assert_eq!(
            stats.tunnels_aborted, 3,
            "an application error, a dropped relay future, and an unchosen drop"
        );
        assert_eq!(stats.streams_on_reused_session, 5);
        assert_eq!(proxy.connections(), 1);
        proxy
    });
}

#[test]
fn std_socket_keeps_relaying_after_its_original_runtime_shuts_down() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let pending = connect_pending(&client, &proxy, "echo").await;
        let (sockets_out, sockets_in) = synchronous::channel();
        let (stop, stopped) = synchronous::channel::<()>();
        let original = thread::spawn(move || {
            let runtime = Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .expect("original runtime");
            let (application, relayed) = runtime.block_on(sockets());
            let application = application.into_std().expect("std socket");
            sockets_out
                .send((application, relayed))
                .expect("sockets delivered");
            stopped.recv().expect("stop signal");
            drop(runtime);
        });
        let (application, relayed) = sockets_in.recv().expect("sockets");
        let mut application = TcpStream::from_std(application).expect("application socket");
        let relay = tokio::spawn(pending.relay(relayed));
        round_trip(&mut application, PAYLOAD).await;
        stop.send(()).expect("stop");
        tokio::task::spawn_blocking(move || original.join())
            .await
            .expect("join task")
            .expect("original runtime thread");
        for _ in 0..3 {
            round_trip(&mut application, PAYLOAD).await;
        }
        assert!(finish(&mut application).await.is_empty());
        let length = 4 * PAYLOAD.len() as u64;
        assert_outcome(
            &relay.await.expect("relay joins"),
            RelayClose::Finished,
            None,
            (length, length),
        );
        proxy
    });
}

/// A blocking `std` socket pair: the application's end and the relay's end.
fn blocking_sockets() -> (std::net::TcpStream, std::net::TcpStream) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("listener");
    let relayed =
        std::net::TcpStream::connect(listener.local_addr().expect("address")).expect("connect");
    let (application, _) = listener.accept().expect("accept");
    (application, relayed)
}

/// Round-trips `payload` over a blocking socket, ends the upload, and reads to end of stream.
fn blocking_echo(mut application: std::net::TcpStream, payload: &[u8]) {
    application.write_all(payload).expect("upload");
    let mut received = vec![0; payload.len()];
    application.read_exact(&mut received).expect("echo");
    assert_eq!(received, payload);
    application.shutdown(Shutdown::Write).expect("upload EOF");
    let mut rest = Vec::new();
    application.read_to_end(&mut rest).expect("download EOF");
    assert!(rest.is_empty());
}

#[test]
fn relay_progresses_while_the_caller_runtime_is_blocked() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let pending = connect_pending(&client, &proxy, "echo").await;
        let (application, relayed) = blocking_sockets();
        let (resume, resumed) = synchronous::channel::<()>();
        let caller = thread::spawn(move || {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("caller runtime");
            let relay = pending.relay(relayed);
            // The caller's only runtime thread blocks until the relay has finished.
            resumed.recv().expect("resume");
            runtime.block_on(relay)
        });
        tokio::task::spawn_blocking(move || blocking_echo(application, PAYLOAD))
            .await
            .expect("application");
        resume.send(()).expect("resume");
        let outcome = tokio::task::spawn_blocking(move || caller.join())
            .await
            .expect("join task")
            .expect("caller thread");
        let length = PAYLOAD.len() as u64;
        assert_outcome(&outcome, RelayClose::Finished, None, (length, length));
        proxy
    });
}

#[test]
fn blocking_std_socket_is_made_nonblocking_and_relayed() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let pending = connect_pending(&client, &proxy, "echo").await;
        let (application, relayed) = blocking_sockets();
        let relay = tokio::spawn(pending.relay(relayed));
        tokio::task::spawn_blocking(move || blocking_echo(application, PAYLOAD))
            .await
            .expect("application");
        let length = PAYLOAD.len() as u64;
        assert_outcome(
            &relay.await.expect("relay joins"),
            RelayClose::Finished,
            None,
            (length, length),
        );
        proxy
    });
}

#[test]
fn generic_io_relays_and_fails_when_its_runtime_shuts_down() {
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let pending = connect_pending(&client, &proxy, "echo").await;
        let (sockets_out, sockets_in) = synchronous::channel();
        let (stop, stopped) = synchronous::channel::<()>();
        let owner = thread::spawn(move || {
            let runtime = Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .expect("I/O runtime");
            let (application, relayed) = runtime.block_on(tokio_sockets());
            let application = application.into_std().expect("std socket");
            sockets_out
                .send((application, GenericIo::socket(relayed)))
                .expect("sockets delivered");
            stopped.recv().expect("stop signal");
            drop(runtime);
        });
        let (application, (io, control)) = sockets_in.recv().expect("sockets");
        let mut application = TcpStream::from_std(application).expect("application socket");
        let relay = tokio::spawn(pending.relay(io.relay_io()));
        round_trip(&mut application, PAYLOAD).await;
        assert_eq!(
            control.threads(),
            HashSet::from(["roxygen-connect".to_owned()])
        );
        stop.send(()).expect("stop");
        tokio::task::spawn_blocking(move || owner.join())
            .await
            .expect("join task")
            .expect("I/O runtime thread");
        let outcome = relay.await.expect("relay joins");
        let length = PAYLOAD.len() as u64;
        // Tokio fails I/O whose reactor is gone with `Other`.
        assert_outcome(
            &outcome,
            RelayClose::Application,
            Some(io::ErrorKind::Other),
            (length, length),
        );
        assert!(control.dropped(), "the I/O closes before the outcome");
        proxy
            .observations
            .wait("the stream's reset", |observed| {
                observed.cancel_resets() == 1
            })
            .await;
        assert_eq!(client.stats().tunnels_aborted, 1);
        proxy
    });
}

#[test]
fn peeked_application_bytes_are_relayed_first() {
    const PEEKED: &[u8] = b"a ClientHello the caller peeked";
    run(ConnectOptions::default(), |client| async move {
        let proxy = Proxy::start().await;
        let pending = connect_pending(&client, &proxy, "echo").await;
        let (mut application, relayed) = tokio_sockets().await;
        application.write_all(PEEKED).await.expect("upload");
        let mut peeked = vec![0; PEEKED.len()];
        loop {
            let seen = relayed.peek(&mut peeked).await.expect("peek");
            if seen == PEEKED.len() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(peeked, PEEKED);
        let relay = tokio::spawn(pending.relay(relayed.into_std().expect("std socket")));
        let mut echoed = vec![0; PEEKED.len()];
        application
            .read_exact(&mut echoed)
            .await
            .expect("the peeked bytes come first");
        assert_eq!(echoed, PEEKED);
        round_trip(&mut application, PAYLOAD).await;
        assert!(finish(&mut application).await.is_empty());
        let length = (PEEKED.len() + PAYLOAD.len()) as u64;
        assert_outcome(
            &relay.await.expect("relay joins"),
            RelayClose::Finished,
            None,
            (length, length),
        );
        proxy
    });
}
