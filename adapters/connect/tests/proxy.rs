/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Independent `h2` CONNECT proxy peers and test connectors shared by the integration tests.

use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::future::pending;
use std::future::poll_fn;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use h2::Reason;
use h2::RecvStream;
use h2::SendStream;
use h2::server::SendResponse;
use http::HeaderMap;
use http::Method;
use http::Request;
use http::Response;
use http::StatusCode;
use roxygen_connect_client::SocketAddress;
use roxygen_connect_client::TransportEndpoint;
use roxygen_connect_client::TransportSecurity;
use roxygen_connect_client::options::ConnectOptions;
use roxygen_connect_client::runtime::ConnectClient;
use roxygen_connect_client::runtime::ConnectRuntime;
use roxygen_connect_client::runtime::ConnectedIo;
use roxygen_connect_client::runtime::Connector;
use roxygen_connect_client::runtime::DialError;
use roxygen_connect_client::tunnel::Tunnel;
use roxygen_http2::hpack::HpackDecoder;
use roxygen_http2::hpack::HpackHeader;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::PrivateKeyDer;
use rustls::pki_types::ServerName;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::ReadBuf;
use tokio::net::TcpListener;
use tokio::net::TcpSocket;
use tokio::net::TcpStream;
use tokio::runtime::Builder;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::Interval;
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::TlsConnector;

pub(crate) const TEST_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const AFTER_HALF_CLOSE: &[u8] = b"response after upload EOF";
/// Longer than the native 5 s session timeouts that the adapter disables.
pub(crate) const BEYOND_NATIVE_TIMEOUT: Duration = Duration::from_secs(6);
/// DATA that a `RawScript::DataThen*Reset` stream sends before its RST_STREAM.
pub(crate) const BEFORE_RESET: usize = 4096;
/// How long a `RawScript::DataThenLateReset*` stream waits between its DATA and its RST_STREAM.
const RESET_DELAY: Duration = Duration::from_millis(500);
/// How long `RawScript::Informational*` proxies wait between the 103 and the final head.
pub(crate) const INFORMATIONAL_DELAY: Duration = Duration::from_millis(100);
/// DATA that a `RawScript::LargeResponseAfterUploadEnd` stream sends after its upload ended.
const LARGE_RESPONSE: usize = 64 * 1024;
/// The largest DATA payload a raw proxy sends: the peer's default `SETTINGS_MAX_FRAME_SIZE`.
const MAX_FRAME: usize = 16 * 1024;
/// A download of many frames whose last one is a byte short.
pub(crate) const ENDED_DOWNLOAD: usize = 17 * MAX_FRAME - 1;
/// The stream window a raw proxy grants, `SETTINGS_INITIAL_WINDOW_SIZE` at its default.
const RAW_STREAM_WINDOW: usize = 65_535;
/// What a `greet` stream sends right after its 2xx.
pub(crate) const GREETING: &[u8] = b"the proxy speaks first";
/// How long a `greet-reset` stream waits between its greeting and its RST_STREAM.
pub(crate) const GREETING_RESET_DELAY: Duration = Duration::from_millis(200);
/// How often a `RawScript::IdleWithoutPingAcks` connection sends a frame once it is idle.
pub(crate) const IDLE_FRAME_PERIOD: Duration = Duration::from_millis(150);

pub(crate) type Client = ConnectClient<&'static str>;

/// Proxies a test returns so they outlive the runtime's shutdown.
pub(crate) struct Proxies(Vec<Proxy>);

impl From<()> for Proxies {
    fn from((): ()) -> Self {
        Self(Vec::new())
    }
}

impl From<Proxy> for Proxies {
    fn from(proxy: Proxy) -> Self {
        Self(vec![proxy])
    }
}

impl From<Vec<Proxy>> for Proxies {
    fn from(proxies: Vec<Proxy>) -> Self {
        Self(proxies)
    }
}

fn application_runtime() -> tokio::runtime::Runtime {
    Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("application runtime starts")
}

/// Runs `test` with a fresh runtime, then shuts the runtime down, expecting a clean join.
pub(crate) fn run<Test, Work, Output>(options: ConnectOptions, test: Test)
where
    Test: FnOnce(Client) -> Work,
    Work: Future<Output = Output>,
    Output: Into<Proxies>,
{
    let (runtime, client) = ConnectRuntime::new(options).expect("CONNECT runtime starts");
    application_runtime().block_on(async move {
        let proxies = timeout(TEST_TIMEOUT, test(client))
            .await
            .expect("test progresses within its deadline")
            .into();
        shutdown(runtime).await.expect("CONNECT worker joins");
        for proxy in proxies.0 {
            proxy.shutdown().await;
        }
    });
}

/// Runs `test` with a runtime it must shut down itself.
pub(crate) fn run_owned<Test, Work, Output>(options: ConnectOptions, test: Test)
where
    Test: FnOnce(ConnectRuntime, Client) -> Work,
    Work: Future<Output = Output>,
    Output: Into<Proxies>,
{
    let (runtime, client) = ConnectRuntime::new(options).expect("CONNECT runtime starts");
    application_runtime().block_on(async move {
        let proxies = timeout(TEST_TIMEOUT, test(runtime, client))
            .await
            .expect("test progresses within its deadline")
            .into();
        for proxy in proxies.0 {
            proxy.shutdown().await;
        }
    });
}

/// Shuts `runtime` down on a blocking thread.
pub(crate) async fn shutdown(
    runtime: ConnectRuntime,
) -> Result<(), roxygen_connect_client::error::ConnectError> {
    timeout(
        TEST_TIMEOUT,
        tokio::task::spawn_blocking(move || runtime.shutdown()),
    )
    .await
    .expect("runtime shutdown progresses within its deadline")
    .expect("shutdown task joins")
}

pub(crate) fn request(action: &str) -> Request<()> {
    request_with(action, "-")
}

/// A CONNECT to `target.example:443` with a per-tunnel credential and test id.
pub(crate) fn request_with(action: &str, id: &str) -> Request<()> {
    let mut request = Request::builder()
        .method(Method::CONNECT)
        .uri("target.example:443")
        .header("x-test-action", action)
        .header("x-test-id", id)
        .header("proxy-authorization", format!("Bearer token-{id}"))
        .body(())
        .expect("CONNECT request");
    request
        .headers_mut()
        .get_mut("proxy-authorization")
        .expect("proxy credentials")
        .set_sensitive(true);
    request
}

/// A CONNECT whose head, or refusal, the proxy sends after `delay`.
pub(crate) fn delayed_request(action: &str, delay: Duration) -> Request<()> {
    let mut request = request(action);
    request.headers_mut().insert(
        "x-test-delay-ms",
        http::HeaderValue::from_str(&delay.as_millis().to_string()).expect("delay"),
    );
    request
}

pub(crate) async fn echo(tunnel: &mut Tunnel, payload: &[u8]) {
    tunnel.write_all(payload).await.expect("upload succeeds");
    tunnel.flush().await.expect("upload flush succeeds");
    let mut received = vec![0; payload.len()];
    tunnel
        .read_exact(&mut received)
        .await
        .expect("echo arrives");
    assert_eq!(received, payload);
}

/// Polls `condition` until it holds, failing after `limit`.
pub(crate) async fn eventually(limit: Duration, what: &str, mut condition: impl FnMut() -> bool) {
    let started = Instant::now();
    while !condition() {
        assert!(started.elapsed() < limit, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn endpoint(address: SocketAddr) -> TransportEndpoint {
    TransportEndpoint::Socket(SocketAddress::new(address.ip().to_string(), address.port()))
}

pub(crate) fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// A self-signed proxy identity: a server config selecting from `server_alpn` and a client
/// config that trusts it and offers h2 and http/1.1.
pub(crate) fn tls_identity(
    server_alpn: &[&[u8]],
) -> (Arc<rustls::ServerConfig>, Arc<rustls::ClientConfig>) {
    install_crypto_provider();
    let generated = rcgen::generate_simple_self_signed(vec!["proxy.example".to_owned()])
        .expect("self-signed certificate");
    let certificate: CertificateDer<'static> = generated.cert.der().clone();
    let key = PrivateKeyDer::try_from(generated.signing_key.serialized_der().to_vec())
        .expect("private key");
    let mut server = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certificate.clone()], key)
        .expect("server identity");
    server.alpn_protocols = server_alpn.iter().map(|alpn| alpn.to_vec()).collect();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate).expect("trusted root");
    let mut client = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    (Arc::new(server), Arc::new(client))
}

/// Copies of the bytes the adapter wrote on each proxy connection.
#[derive(Default)]
pub(crate) struct Tap {
    connections: Mutex<Vec<Arc<Mutex<Vec<u8>>>>>,
}

impl Tap {
    fn connection(&self) -> Arc<Mutex<Vec<u8>>> {
        let written = Arc::new(Mutex::new(Vec::new()));
        self.connections
            .lock()
            .expect("tap lock")
            .push(Arc::clone(&written));
        written
    }

    pub(crate) fn written(&self) -> Vec<Vec<u8>> {
        self.connections
            .lock()
            .expect("tap lock")
            .iter()
            .map(|written| written.lock().expect("tap lock").clone())
            .collect()
    }
}

struct TappedIo {
    io: TcpStream,
    written: Arc<Mutex<Vec<u8>>>,
}

impl AsyncRead for TappedIo {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_read(context, buffer)
    }
}

impl AsyncWrite for TappedIo {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let written = std::task::ready!(Pin::new(&mut this.io).poll_write(context, buffer))?;
        this.written
            .lock()
            .expect("tap lock")
            .extend_from_slice(&buffer[..written]);
        Poll::Ready(Ok(written))
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(context)
    }
}

/// A connection whose shutdown never completes, as a TLS `close_notify` to a peer that stopped
/// reading; it decrements its counter when dropped.
struct StalledShutdownIo {
    io: TcpStream,
    live: Arc<AtomicUsize>,
}

impl AsyncRead for StalledShutdownIo {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_read(context, buffer)
    }
}

impl AsyncWrite for StalledShutdownIo {
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

    fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

impl Drop for StalledShutdownIo {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Stalled dials that the adapter dropped.
#[derive(Default)]
pub(crate) struct DroppedDials {
    count: AtomicUsize,
    changed: Notify,
}

impl DroppedDials {
    pub(crate) fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    pub(crate) async fn wait_for(&self, expected: usize) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.count() >= expected {
                return;
            }
            changed.await;
        }
    }
}

struct DialDrop(Arc<DroppedDials>);

impl Drop for DialDrop {
    fn drop(&mut self) {
        self.0.count.fetch_add(1, Ordering::SeqCst);
        self.0.changed.notify_waiters();
    }
}

/// A connector to a test proxy with scripted dial behavior.
#[derive(Default)]
pub(crate) struct TestConnector {
    address: Option<SocketAddr>,
    pub(crate) attempts: AtomicUsize,
    /// Dials before this many attempts stall until dropped.
    stalled_dials: usize,
    /// Each dial waits for this long before connecting.
    delay: Duration,
    /// Each dial waits for a permit before connecting.
    gate: Option<Arc<tokio::sync::Semaphore>>,
    /// Dials after this many attempts report `H2NotNegotiated` without connecting.
    h2_dials: Option<usize>,
    tls: Option<Arc<rustls::ClientConfig>>,
    tap: Option<Arc<Tap>>,
    /// Plaintext connections whose shutdown never completes, counted while they live.
    stalled_shutdowns: Option<Arc<AtomicUsize>>,
    send_buffer: Option<u32>,
    /// A bound socket that never listens, held so that other sockets cannot take its port.
    _reserved: Option<TcpSocket>,
    pub(crate) dropped: Arc<DroppedDials>,
}

impl TestConnector {
    pub(crate) fn plain(address: SocketAddr) -> Self {
        Self {
            address: Some(address),
            ..Self::default()
        }
    }

    /// A connector whose dials fail with a refused TCP connection to a port it reserves.
    pub(crate) fn refused() -> Self {
        let reserved = TcpSocket::new_v4().expect("reserved socket");
        reserved
            .bind("127.0.0.1:0".parse().expect("loopback"))
            .expect("reserved socket binds");
        let address = reserved.local_addr().expect("reserved port");
        Self {
            _reserved: Some(reserved),
            ..Self::plain(address)
        }
    }

    pub(crate) fn stalling(mut self, dials: usize) -> Self {
        self.stalled_dials = dials;
        self
    }

    pub(crate) fn delayed(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    pub(crate) fn gated(mut self, gate: Arc<tokio::sync::Semaphore>) -> Self {
        self.gate = Some(gate);
        self
    }

    pub(crate) fn h2_dials(mut self, dials: usize) -> Self {
        self.h2_dials = Some(dials);
        self
    }

    pub(crate) fn tls(mut self, config: Arc<rustls::ClientConfig>) -> Self {
        self.tls = Some(config);
        self
    }

    pub(crate) fn tapped(mut self, tap: Arc<Tap>) -> Self {
        self.tap = Some(tap);
        self
    }

    pub(crate) fn stalled_shutdowns(mut self, live: Arc<AtomicUsize>) -> Self {
        self.stalled_shutdowns = Some(live);
        self
    }

    pub(crate) fn send_buffer(mut self, size: u32) -> Self {
        self.send_buffer = Some(size);
        self
    }

    pub(crate) fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }
}

impl Connector for TestConnector {
    fn security(&self) -> TransportSecurity {
        if self.tls.is_some() {
            TransportSecurity::Tls
        } else {
            TransportSecurity::Plaintext
        }
    }

    fn connect(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<ConnectedIo, DialError>> + Send + 'static>> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        let address = self.address.expect("connector address");
        let stall = attempt < self.stalled_dials;
        let not_h2 = self.h2_dials.is_some_and(|dials| attempt >= dials);
        let delay = self.delay;
        let gate = self.gate.clone();
        let tls = self.tls.clone();
        let tap = self.tap.as_ref().map(|tap| tap.connection());
        let stalled_shutdowns = self.stalled_shutdowns.clone();
        let dropped = Arc::clone(&self.dropped);
        let send_buffer = self.send_buffer;
        Box::pin(async move {
            if stall {
                let _dropped = DialDrop(dropped);
                pending::<()>().await;
            }
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            if let Some(gate) = gate {
                gate.acquire().await.expect("gate open").forget();
            }
            if not_h2 {
                return Err(DialError::H2NotNegotiated);
            }
            let other = |error: io::Error| DialError::Other(Arc::new(error));
            let socket = TcpSocket::new_v4().map_err(other)?;
            if let Some(size) = send_buffer {
                socket.set_send_buffer_size(size).map_err(other)?;
            }
            let io = socket.connect(address).await.map_err(other)?;
            io.set_nodelay(true)
                .map_err(|error| DialError::Other(Arc::new(error)))?;
            let local = io
                .local_addr()
                .map_err(|error| DialError::Other(Arc::new(error)))?;
            if let Some(config) = tls {
                let name = ServerName::try_from("proxy.example").expect("server name");
                let tls = TlsConnector::from(config)
                    .connect(name, io)
                    .await
                    .map_err(|error| DialError::Other(Arc::new(error)))?;
                if tls.get_ref().1.alpn_protocol() != Some(b"h2") {
                    return Err(DialError::H2NotNegotiated);
                }
                return Ok(ConnectedIo {
                    io: Box::new(tls),
                    local_endpoint: endpoint(local),
                    peer_endpoint: endpoint(address),
                    security: TransportSecurity::Tls,
                });
            }
            let io: Box<dyn roxygen_connect_client::runtime::ProxyIo> =
                match (tap, stalled_shutdowns) {
                    (Some(written), _) => Box::new(TappedIo { io, written }),
                    (None, Some(live)) => {
                        live.fetch_add(1, Ordering::SeqCst);
                        Box::new(StalledShutdownIo { io, live })
                    }
                    (None, None) => Box::new(io),
                };
            Ok(ConnectedIo {
                io,
                local_endpoint: endpoint(local),
                peer_endpoint: endpoint(address),
                security: TransportSecurity::Plaintext,
            })
        })
    }
}

/// What a proxy observed.
#[derive(Default)]
pub(crate) struct Observations {
    connections: AtomicUsize,
    requests: AtomicUsize,
    resets: AtomicUsize,
    cancel_resets: AtomicUsize,
    upload_eofs: AtomicUsize,
    closed: AtomicUsize,
    uploaded: AtomicU64,
    downloaded: AtomicU64,
    heads: Mutex<Vec<HeaderMap>>,
    authorities: Mutex<Vec<String>>,
    records: Mutex<HashMap<String, Record>>,
    changed: Notify,
}

/// The upload a `record` tunnel received and how its stream ended.
#[derive(Clone, Debug, Default)]
pub(crate) struct Record {
    pub(crate) bytes: Vec<u8>,
    pub(crate) end_stream: bool,
    pub(crate) reset: Option<Reason>,
}

impl Observations {
    fn notify(&self) {
        self.changed.notify_waiters();
    }

    fn bump(&self, counter: &AtomicUsize) {
        counter.fetch_add(1, Ordering::SeqCst);
        self.notify();
    }

    fn reset(&self, reason: Option<Reason>) {
        if reason == Some(Reason::CANCEL) {
            self.cancel_resets.fetch_add(1, Ordering::SeqCst);
        }
        self.bump(&self.resets);
    }

    /// Waits until `observed` holds, failing after the test timeout.
    pub(crate) async fn wait(&self, what: &str, observed: impl Fn(&Self) -> bool) {
        timeout(TEST_TIMEOUT, async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if observed(self) {
                    return;
                }
                changed.await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("proxy never observed {what}"));
    }

    pub(crate) fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    pub(crate) fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    pub(crate) fn resets(&self) -> usize {
        self.resets.load(Ordering::SeqCst)
    }

    pub(crate) fn cancel_resets(&self) -> usize {
        self.cancel_resets.load(Ordering::SeqCst)
    }

    pub(crate) fn upload_eofs(&self) -> usize {
        self.upload_eofs.load(Ordering::SeqCst)
    }

    pub(crate) fn closed(&self) -> usize {
        self.closed.load(Ordering::SeqCst)
    }

    pub(crate) fn uploaded(&self) -> u64 {
        self.uploaded.load(Ordering::SeqCst)
    }

    pub(crate) fn downloaded(&self) -> u64 {
        self.downloaded.load(Ordering::SeqCst)
    }

    pub(crate) fn heads(&self) -> Vec<HeaderMap> {
        self.heads.lock().expect("heads lock").clone()
    }

    /// The `:authority` of every CONNECT, in arrival order.
    pub(crate) fn authorities(&self) -> Vec<String> {
        self.authorities.lock().expect("authorities lock").clone()
    }

    pub(crate) fn record(&self, id: &str) -> Option<Record> {
        self.records.lock().expect("records lock").get(id).cloned()
    }
}

/// Connection-level behavior of an `h2` proxy.
#[derive(Clone, Default)]
pub(crate) struct ProxyConfig {
    pub(crate) max_concurrent_streams: Option<u32>,
    pub(crate) initial_window_size: Option<u32>,
    pub(crate) initial_connection_window_size: Option<u32>,
    pub(crate) recv_buffer_size: Option<u32>,
    pub(crate) tls: Option<Arc<rustls::ServerConfig>>,
    /// Closing a connection resets it instead of sending FIN.
    pub(crate) reset_on_close: bool,
}

/// Commands every live connection of a proxy follows.
#[derive(Clone)]
struct Controls {
    stop: watch::Receiver<bool>,
    goaways: watch::Receiver<u64>,
    aborts: watch::Receiver<u64>,
    closes: watch::Receiver<u64>,
    stop_reading: watch::Receiver<bool>,
    gate: watch::Receiver<bool>,
    refusals: Arc<AtomicUsize>,
    drain_refusals: Arc<AtomicUsize>,
    closings: Arc<AtomicUsize>,
}

struct ControlSenders {
    stop: watch::Sender<bool>,
    goaways: watch::Sender<u64>,
    aborts: watch::Sender<u64>,
    closes: watch::Sender<u64>,
    stop_reading: watch::Sender<bool>,
    gate: watch::Sender<bool>,
}

enum ConnectionCommand {
    GracefulShutdown(oneshot::Sender<()>),
    Close,
}

/// An independent `h2` CONNECT proxy. Each stream follows its `x-test-action` header.
pub(crate) struct Proxy {
    pub(crate) address: SocketAddr,
    pub(crate) observations: Arc<Observations>,
    senders: ControlSenders,
    refusals: Arc<AtomicUsize>,
    drain_refusals: Arc<AtomicUsize>,
    closings: Arc<AtomicUsize>,
    tasks: JoinSet<()>,
}

impl Proxy {
    pub(crate) async fn start() -> Self {
        Self::with_config(ProxyConfig::default()).await
    }

    pub(crate) async fn with_config(config: ProxyConfig) -> Self {
        let socket = TcpSocket::new_v4().expect("proxy socket");
        if let Some(size) = config.recv_buffer_size {
            socket.set_recv_buffer_size(size).expect("receive buffer");
        }
        socket
            .bind("127.0.0.1:0".parse().expect("loopback"))
            .expect("proxy binds");
        let listener = socket.listen(1024).expect("proxy listens");
        let address = listener.local_addr().expect("proxy address");
        let observations = Arc::new(Observations::default());
        let (stop, stop_receiver) = watch::channel(false);
        let (goaways, goaways_receiver) = watch::channel(0);
        let (aborts, aborts_receiver) = watch::channel(0);
        let (closes, closes_receiver) = watch::channel(0);
        let (stop_reading, stop_reading_receiver) = watch::channel(false);
        let (gate, gate_receiver) = watch::channel(true);
        let refusals = Arc::new(AtomicUsize::new(0));
        let drain_refusals = Arc::new(AtomicUsize::new(0));
        let closings = Arc::new(AtomicUsize::new(0));
        let controls = Controls {
            stop: stop_receiver,
            goaways: goaways_receiver,
            aborts: aborts_receiver,
            closes: closes_receiver,
            stop_reading: stop_reading_receiver,
            gate: gate_receiver,
            refusals: Arc::clone(&refusals),
            drain_refusals: Arc::clone(&drain_refusals),
            closings: Arc::clone(&closings),
        };
        let observed = Arc::clone(&observations);
        let mut tasks = JoinSet::new();
        tasks.spawn(accept_loop(listener, config, observed, controls));
        Self {
            address,
            observations,
            senders: ControlSenders {
                stop,
                goaways,
                aborts,
                closes,
                stop_reading,
                gate,
            },
            refusals,
            drain_refusals,
            closings,
            tasks,
        }
    }

    pub(crate) fn connector(&self) -> Arc<TestConnector> {
        Arc::new(TestConnector::plain(self.address))
    }

    pub(crate) fn connections(&self) -> usize {
        self.observations.connections()
    }

    /// Connects through this proxy with a fresh connector, expecting success.
    pub(crate) async fn connect_with(
        &self,
        client: &Client,
        key: &'static str,
        action: &str,
    ) -> Tunnel {
        client
            .connect(key, request(action), self.connector())
            .await
            .expect("CONNECT succeeds")
            .into_tunnel()
    }

    /// Sends a graceful GOAWAY on every live connection.
    pub(crate) fn goaway(&self) {
        self.senders.goaways.send_modify(|count| *count += 1);
    }

    /// Ends every live connection with GOAWAY(INTERNAL_ERROR), which fails its open streams.
    pub(crate) fn abort_connections(&self) {
        self.senders.aborts.send_modify(|count| *count += 1);
    }

    /// Closes every live connection, with a TCP reset under `reset_on_close`.
    pub(crate) fn close_connections(&self) {
        self.senders.closes.send_modify(|count| *count += 1);
    }

    /// Stops polling every connection, so the proxy stops reading from its sockets.
    pub(crate) fn stop_reading(&self) {
        self.senders.stop_reading.send_replace(true);
    }

    /// Closes the gate that `gated-sink` streams wait on before reading, which resets
    /// `early-eof-gated-reset` and `greet-end-gated-reset` streams.
    pub(crate) fn close_gate(&self) {
        self.senders.gate.send_replace(false);
    }

    pub(crate) fn open_gate(&self) {
        self.senders.gate.send_replace(true);
    }

    /// Refuses the next `count` streams with `REFUSED_STREAM`.
    pub(crate) fn refuse_next(&self, count: usize) {
        self.refusals.store(count, Ordering::SeqCst);
    }

    /// Responds to the next `count` streams by draining their connection with a graceful GOAWAY
    /// and then refusing the stream.
    pub(crate) fn drain_and_refuse_next(&self, count: usize) {
        self.drain_refusals.store(count, Ordering::SeqCst);
    }

    /// Closes the connection of each of the next `count` streams instead of responding.
    pub(crate) fn close_on_next(&self, count: usize) {
        self.closings.store(count, Ordering::SeqCst);
    }

    pub(crate) async fn shutdown(mut self) {
        self.senders.stop.send_replace(true);
        while let Some(joined) = self.tasks.join_next().await {
            joined.expect("proxy accept task joins");
        }
    }
}

async fn accept_loop(
    listener: TcpListener,
    config: ProxyConfig,
    observations: Arc<Observations>,
    controls: Controls,
) {
    let mut stop = controls.stop.clone();
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            _ = stop.changed() => break,
            accepted = listener.accept() => {
                let Ok((io, _)) = accepted else { continue };
                observations.bump(&observations.connections);
                if config.reset_on_close {
                    io.set_zero_linger().expect("zero linger");
                }
                let config = config.clone();
                let observations = Arc::clone(&observations);
                let controls = controls.clone();
                connections.spawn(async move {
                    match config.tls.clone() {
                        Some(tls) => {
                            let Ok(tls) = TlsAcceptor::from(tls).accept(io).await else { return };
                            if tls.get_ref().1.alpn_protocol() == Some(b"h2") {
                                serve_connection(tls, &config, &observations, controls).await;
                            } else {
                                let mut stop = controls.stop.clone();
                                let _ = stop.changed().await;
                            }
                        }
                        None => serve_connection(io, &config, &observations, controls).await,
                    }
                    observations.bump(&observations.closed);
                });
            }
            Some(joined) = connections.join_next(), if !connections.is_empty() => {
                joined.expect("proxy connection task");
            }
        }
    }
    connections.abort_all();
    while let Some(joined) = connections.join_next().await {
        if let Err(error) = joined {
            assert!(error.is_cancelled(), "proxy connection panicked: {error}");
        }
    }
}

enum ConnectionEvent {
    Stop,
    Goaway,
    Abort,
    Close,
    StopReading,
    Command(Option<ConnectionCommand>),
    Accepted(Option<Result<(Request<RecvStream>, SendResponse<Bytes>), h2::Error>>),
    Joined,
}

async fn serve_connection<Io>(
    io: Io,
    config: &ProxyConfig,
    observations: &Arc<Observations>,
    controls: Controls,
) where
    Io: AsyncRead + AsyncWrite + Unpin,
{
    let mut builder = h2::server::Builder::new();
    if let Some(maximum) = config.max_concurrent_streams {
        builder.max_concurrent_streams(maximum);
    }
    if let Some(window) = config.initial_window_size {
        builder.initial_window_size(window);
    }
    if let Some(window) = config.initial_connection_window_size {
        builder.initial_connection_window_size(window);
    }
    let mut stop = controls.stop.clone();
    let mut goaways = controls.goaways.clone();
    let mut aborts = controls.aborts.clone();
    let mut closes = controls.closes.clone();
    let mut stop_reading = controls.stop_reading.clone();
    goaways.mark_unchanged();
    aborts.mark_unchanged();
    closes.mark_unchanged();
    let mut connection = tokio::select! {
        _ = stop.changed() => return,
        connected = builder.handshake(io) => match connected {
            Ok(connection) => connection,
            Err(_) => return,
        },
    };
    let (commands, mut command_receiver) = mpsc::channel(16);
    let mut streams = JoinSet::new();
    loop {
        let event = tokio::select! {
            _ = stop.changed() => ConnectionEvent::Stop,
            _ = goaways.changed() => ConnectionEvent::Goaway,
            _ = aborts.changed() => ConnectionEvent::Abort,
            _ = closes.changed() => ConnectionEvent::Close,
            Ok(_) = stop_reading.wait_for(|stopped| *stopped) => ConnectionEvent::StopReading,
            command = command_receiver.recv() => ConnectionEvent::Command(command),
            accepted = connection.accept() => ConnectionEvent::Accepted(accepted),
            Some(_) = streams.join_next(), if !streams.is_empty() => ConnectionEvent::Joined,
        };
        match event {
            ConnectionEvent::Stop | ConnectionEvent::Close => break,
            ConnectionEvent::Goaway => connection.graceful_shutdown(),
            ConnectionEvent::Abort => connection.abrupt_shutdown(Reason::INTERNAL_ERROR),
            ConnectionEvent::StopReading => {
                let _ = stop.changed().await;
                break;
            }
            ConnectionEvent::Command(Some(ConnectionCommand::GracefulShutdown(done))) => {
                connection.graceful_shutdown();
                let _ = done.send(());
            }
            ConnectionEvent::Command(Some(ConnectionCommand::Close)) => break,
            ConnectionEvent::Command(None) | ConnectionEvent::Joined => {}
            ConnectionEvent::Accepted(Some(Ok((request, respond)))) => {
                observations
                    .heads
                    .lock()
                    .expect("heads lock")
                    .push(request.headers().clone());
                observations
                    .authorities
                    .lock()
                    .expect("authorities lock")
                    .push(
                        request
                            .uri()
                            .authority()
                            .map(ToString::to_string)
                            .unwrap_or_default(),
                    );
                observations.bump(&observations.requests);
                streams.spawn(serve_tunnel(
                    request,
                    respond,
                    Arc::clone(observations),
                    controls.clone(),
                    commands.clone(),
                ));
            }
            ConnectionEvent::Accepted(_) => break,
        }
    }
    streams.abort_all();
    while let Some(joined) = streams.join_next().await {
        if let Err(error) = joined {
            assert!(error.is_cancelled(), "proxy stream panicked: {error}");
        }
    }
}

/// Sends `bytes`, waiting for stream capacity; false once the stream is gone.
pub(crate) async fn send_data(stream: &mut SendStream<Bytes>, mut bytes: Bytes) -> bool {
    while !bytes.is_empty() {
        stream.reserve_capacity(bytes.len());
        let Some(Ok(capacity)) = poll_fn(|context| stream.poll_capacity(context)).await else {
            return false;
        };
        let amount = capacity.min(bytes.len());
        if amount != 0 && stream.send_data(bytes.split_to(amount), false).is_err() {
            return false;
        }
    }
    true
}

/// Sends `bytes` like `send_data`, counting each sent chunk as downloaded.
async fn send_counted(
    stream: &mut SendStream<Bytes>,
    mut bytes: Bytes,
    observations: &Observations,
) {
    while !bytes.is_empty() {
        stream.reserve_capacity(bytes.len());
        let Some(Ok(capacity)) = poll_fn(|context| stream.poll_capacity(context)).await else {
            return;
        };
        let amount = capacity.min(bytes.len());
        if amount == 0 {
            continue;
        }
        if stream.send_data(bytes.split_to(amount), false).is_err() {
            return;
        }
        observations
            .downloaded
            .fetch_add(amount as u64, Ordering::SeqCst);
        observations.notify();
    }
}

fn header(request: &Request<RecvStream>, name: &str) -> String {
    request
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned()
}

fn take_one(counter: &AtomicUsize) -> bool {
    let mut remaining = counter.load(Ordering::SeqCst);
    while let Some(next) = remaining.checked_sub(1) {
        match counter.compare_exchange_weak(remaining, next, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return true,
            Err(actual) => remaining = actual,
        }
    }
    false
}

/// The repeating byte pattern of floods and bulk uploads.
pub(crate) fn pattern(length: usize) -> Vec<u8> {
    (0..length).map(|index| (index % 251) as u8).collect()
}

async fn serve_tunnel(
    request: Request<RecvStream>,
    mut respond: SendResponse<Bytes>,
    observations: Arc<Observations>,
    controls: Controls,
    commands: mpsc::Sender<ConnectionCommand>,
) {
    assert_eq!(request.method(), Method::CONNECT);
    let action = header(&request, "x-test-action");
    let id = header(&request, "x-test-id");
    let authorization = header(&request, "proxy-authorization");
    if let Ok(delay) = header(&request, "x-test-delay-ms").parse::<u64>() {
        tokio::select! {
            biased;
            reset = poll_fn(|context| respond.poll_reset(context)) => {
                observations.reset(reset.ok());
                return;
            }
            () = tokio::time::sleep(Duration::from_millis(delay)) => {}
        }
    }
    if take_one(&controls.drain_refusals) {
        let (done, drained) = oneshot::channel();
        if commands
            .send(ConnectionCommand::GracefulShutdown(done))
            .await
            .is_ok()
        {
            let _ = drained.await;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        respond.send_reset(Reason::REFUSED_STREAM);
        return;
    }
    if take_one(&controls.refusals) || action == "refuse" {
        respond.send_reset(Reason::REFUSED_STREAM);
        return;
    }
    if take_one(&controls.closings) {
        let _ = commands.send(ConnectionCommand::Close).await;
        // Dropping the stream here would queue an RST_STREAM the closing connection may flush.
        return pending().await;
    }
    if action == "cancel" {
        respond.send_reset(Reason::CANCEL);
        return;
    }
    let mut incoming = request.into_body();
    // The response ends immediately; the upload stays open until the client ends or resets it.
    if action == "early-eof" {
        let response = Response::builder()
            .status(StatusCode::OK)
            .body(())
            .expect("CONNECT response");
        if respond.send_response(response, true).is_ok() {
            match incoming.data().await {
                Some(Err(error)) => observations.reset(error.reason()),
                Some(Ok(_)) | None => observations.bump(&observations.upload_eofs),
            }
        }
        return;
    }
    // The response ends immediately; the upload is counted until the client ends it.
    if action == "early-eof-sink" {
        let response = Response::builder()
            .status(StatusCode::OK)
            .body(())
            .expect("CONNECT response");
        if respond.send_response(response, true).is_err() {
            return;
        }
        while let Some(data) = incoming.data().await {
            let data = match data {
                Ok(data) => data,
                Err(error) => {
                    observations.reset(error.reason());
                    return;
                }
            };
            let _ = incoming.flow_control().release_capacity(data.len());
            observations
                .uploaded
                .fetch_add(data.len() as u64, Ordering::SeqCst);
            observations.notify();
        }
        observations.bump(&observations.upload_eofs);
        return;
    }
    // The response ends immediately; the stream is reset after `GREETING_RESET_DELAY` without
    // reading the upload.
    if action == "early-eof-reset" {
        let response = Response::builder()
            .status(StatusCode::OK)
            .body(())
            .expect("CONNECT response");
        if let Ok(mut outgoing) = respond.send_response(response, true) {
            tokio::time::sleep(GREETING_RESET_DELAY).await;
            outgoing.send_reset(Reason::CANCEL);
        }
        return;
    }
    // The response ends immediately; the stream is reset once the gate closes, without reading
    // the upload.
    if action == "early-eof-gated-reset" {
        let response = Response::builder()
            .status(StatusCode::OK)
            .body(())
            .expect("CONNECT response");
        if let Ok(mut outgoing) = respond.send_response(response, true) {
            let mut gate = controls.gate.clone();
            if gate.wait_for(|open| !*open).await.is_ok() {
                outgoing.send_reset(Reason::CANCEL);
            }
        }
        return;
    }
    if action == "stall" {
        let reset = poll_fn(|context| respond.poll_reset(context)).await;
        observations.reset(reset.ok());
        return;
    }
    let rejected = action == "reject";
    let response = Response::builder()
        .status(if rejected {
            StatusCode::FORBIDDEN
        } else {
            StatusCode::OK
        })
        .header("x-test-proxy", "independent-h2")
        .header("x-test-authorization", authorization)
        .header("x-test-id", id.clone())
        .body(())
        .expect("CONNECT response");
    let mut outgoing = match respond.send_response(response, rejected) {
        Ok(outgoing) => outgoing,
        // The client reset the stream before the head went out.
        Err(_) => {
            let reset = poll_fn(|context| respond.poll_reset(context)).await;
            observations.reset(reset.ok());
            return;
        }
    };
    if rejected {
        return;
    }
    match action.split_once(':').unwrap_or((action.as_str(), "")) {
        ("flood", length) => {
            let length = length.parse().expect("flood length");
            send_counted(&mut outgoing, Bytes::from(pattern(length)), &observations).await;
            let reset = poll_fn(|context| outgoing.poll_reset(context)).await;
            observations.reset(reset.ok());
            return;
        }
        ("sink" | "gated-sink", _) => {
            let mut gate = controls.gate.clone();
            loop {
                if action == "gated-sink" && gate.wait_for(|open| *open).await.is_err() {
                    return;
                }
                let Some(data) = incoming.data().await else {
                    break;
                };
                let data = match data {
                    Ok(data) => data,
                    Err(error) => {
                        observations.reset(error.reason());
                        return;
                    }
                };
                let _ = incoming.flow_control().release_capacity(data.len());
                observations
                    .uploaded
                    .fetch_add(data.len() as u64, Ordering::SeqCst);
                observations.notify();
            }
            observations.bump(&observations.upload_eofs);
            let _ = outgoing.send_data(Bytes::new(), true);
            return;
        }
        ("record", _) => {
            let mut record = Record::default();
            loop {
                match incoming.data().await {
                    Some(Ok(data)) => {
                        let _ = incoming.flow_control().release_capacity(data.len());
                        record.bytes.extend_from_slice(&data);
                    }
                    Some(Err(error)) => {
                        record.reset = error.reason();
                        break;
                    }
                    None => {
                        record.end_stream = true;
                        break;
                    }
                }
            }
            if record.end_stream {
                let reset = poll_fn(|context| outgoing.poll_reset(context)).await;
                record.reset = reset.ok();
            }
            let reason = record.reset;
            observations
                .records
                .lock()
                .expect("records lock")
                .insert(id, record);
            observations.reset(reason);
            return;
        }
        // Sends `GREETING` first, then echoes.
        ("greet", _) => {
            send_counted(&mut outgoing, Bytes::from_static(GREETING), &observations).await;
        }
        // Sends `GREETING` and ends the download, then resets the stream once the gate closes,
        // without reading the upload.
        ("greet-end-gated-reset", _) => {
            send_counted(&mut outgoing, Bytes::from_static(GREETING), &observations).await;
            if outgoing.send_data(Bytes::new(), true).is_ok() {
                let mut gate = controls.gate.clone();
                if gate.wait_for(|open| !*open).await.is_ok() {
                    outgoing.send_reset(Reason::CANCEL);
                }
            }
            return;
        }
        // Sends `GREETING` first, then resets the stream after `GREETING_RESET_DELAY`.
        ("greet-reset", _) => {
            send_counted(&mut outgoing, Bytes::from_static(GREETING), &observations).await;
            tokio::time::sleep(GREETING_RESET_DELAY).await;
            outgoing.send_reset(Reason::CANCEL);
            return;
        }
        _ => {}
    }
    while let Some(data) = incoming.data().await {
        let data = match data {
            Ok(data) => data,
            Err(error) => {
                observations.reset(error.reason());
                return;
            }
        };
        let _ = incoming.flow_control().release_capacity(data.len());
        if action == "reset" {
            outgoing.send_reset(Reason::CANCEL);
            return;
        }
        if !send_data(&mut outgoing, data).await {
            return;
        }
    }
    observations.bump(&observations.upload_eofs);
    match action.split_once(':').unwrap_or((action.as_str(), "")) {
        ("hold-open", _) => {
            let reset = poll_fn(|context| outgoing.poll_reset(context)).await;
            observations.reset(reset.ok());
            return;
        }
        ("half-close", _) => {
            if !send_data(&mut outgoing, Bytes::from_static(AFTER_HALF_CLOSE)).await {
                return;
            }
        }
        // `late-response:<ms>` waits that long, and plain `late-response` waits past the native
        // timeouts.
        ("late-response", delay) => {
            let delay = delay
                .parse()
                .map_or(BEYOND_NATIVE_TIMEOUT, Duration::from_millis);
            tokio::time::sleep(delay).await;
            if !send_data(&mut outgoing, Bytes::from_static(AFTER_HALF_CLOSE)).await {
                return;
            }
        }
        _ => {}
    }
    let _ = outgoing.send_data(Bytes::new(), true);
}

/// Frames of a raw HTTP/2 connection.
const DATA: u8 = 0;
const HEADERS: u8 = 1;
const RST_STREAM: u8 = 3;
const SETTINGS: u8 = 4;
const PING: u8 = 6;
const GOAWAY: u8 = 7;
const WINDOW_UPDATE: u8 = 8;
const CONTINUATION: u8 = 9;
const END_STREAM: u8 = 0x1;
const ACK: u8 = 0x1;
const INTERNAL_ERROR: u32 = 0x2;
const END_HEADERS: u8 = 0x4;
const PADDED: u8 = 0x8;
const PRIORITY: u8 = 0x20;
const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

struct Frame {
    kind: u8,
    flags: u8,
    stream: u32,
    payload: Vec<u8>,
}

fn encode_frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
    let length = u32::try_from(payload.len()).expect("frame length");
    let mut frame = length.to_be_bytes()[1..].to_vec();
    frame.extend_from_slice(&[kind, flags]);
    frame.extend_from_slice(&stream.to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

/// Splits `bytes` into whole frames, ignoring a trailing partial frame.
fn parse_frames(mut bytes: &[u8]) -> Vec<Frame> {
    let mut frames = Vec::new();
    while bytes.len() >= 9 {
        let length = u32::from_be_bytes([0, bytes[0], bytes[1], bytes[2]]) as usize;
        if bytes.len() < 9 + length {
            break;
        }
        frames.push(Frame {
            kind: bytes[3],
            flags: bytes[4],
            stream: u32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]) & 0x7fff_ffff,
            payload: bytes[9..9 + length].to_vec(),
        });
        bytes = &bytes[9 + length..];
    }
    frames
}

/// The header block fragment of a HEADERS frame, without padding and priority.
fn header_fragment(frame: &Frame) -> &[u8] {
    let mut payload = &frame.payload[..];
    let mut padding = 0;
    if frame.flags & PADDED != 0 {
        padding = usize::from(payload[0]);
        payload = &payload[1..];
    }
    if frame.flags & PRIORITY != 0 {
        payload = &payload[5..];
    }
    &payload[..payload.len() - padding]
}

/// The PING frames, acknowledgements excluded, that the client wrote on one connection.
pub(crate) fn written_pings(written: &[u8]) -> usize {
    let frames = written
        .strip_prefix(PREFACE)
        .expect("the client preface comes first");
    parse_frames(frames)
        .iter()
        .filter(|frame| frame.kind == PING && frame.flags & ACK == 0)
        .count()
}

/// The DATA payload bytes the client wrote on one connection.
pub(crate) fn written_data(written: &[u8]) -> usize {
    let frames = written
        .strip_prefix(PREFACE)
        .expect("the client preface comes first");
    parse_frames(frames)
        .iter()
        .filter(|frame| frame.kind == DATA)
        .map(|frame| frame.payload.len())
        .sum()
}

/// The DATA frames, empty ones included, that the client wrote on one connection.
pub(crate) fn written_data_frames(written: &[u8]) -> usize {
    let frames = written
        .strip_prefix(PREFACE)
        .expect("the client preface comes first");
    parse_frames(frames)
        .iter()
        .filter(|frame| frame.kind == DATA)
        .count()
}

/// `(stream, error code)` of every RST_STREAM the client wrote on one connection.
pub(crate) fn written_resets(written: &[u8]) -> Vec<(u32, u32)> {
    let frames = written
        .strip_prefix(PREFACE)
        .expect("the client preface comes first");
    parse_frames(frames)
        .into_iter()
        .filter(|frame| frame.kind == RST_STREAM)
        .map(|frame| {
            let code = frame.payload[..4].try_into().expect("RST code");
            (frame.stream, u32::from_be_bytes(code))
        })
        .collect()
}

/// Decodes every header block the client wrote on one connection, in order, with an
/// independent decoder: each block's headers and the decoder's dynamic table after it.
pub(crate) fn decode_header_blocks(written: &[u8]) -> Vec<(Vec<HpackHeader>, Vec<HpackHeader>)> {
    assert!(
        written.starts_with(PREFACE),
        "the client preface comes first"
    );
    let mut decoder = HpackDecoder::new();
    let mut blocks = Vec::new();
    let mut block: Option<Vec<u8>> = None;
    for frame in parse_frames(&written[PREFACE.len()..]) {
        match frame.kind {
            HEADERS => block = Some(header_fragment(&frame).to_vec()),
            CONTINUATION => block
                .as_mut()
                .expect("CONTINUATION follows HEADERS")
                .extend_from_slice(&frame.payload),
            _ => continue,
        }
        if frame.flags & END_HEADERS != 0 {
            let fragment = block.take().expect("a complete header block");
            let decoded = decoder.decode(&fragment).expect("valid header block");
            let table = decoder.context().table().entries().cloned().collect();
            blocks.push((decoded.headers, table));
        }
    }
    blocks
}

/// A scripted connection behavior of a raw HTTP/2 proxy. Connections after the first respond
/// to every CONNECT with 200 and echo its data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RawScript {
    /// The first connection responds to its first CONNECT with 200, then to its second with a
    /// GOAWAY whose last-stream-id is the first stream.
    GoawayBelowSecondStream,
    /// Like `GoawayBelowSecondStream`, but the GOAWAY carries `ENHANCE_YOUR_CALM`.
    ErrorGoawayBelowSecondStream,
    /// The first connection waits for two CONNECTs, rejects the first with 403, and immediately
    /// sends a GOAWAY whose last-stream-id is that first stream, as some forward proxies do.
    RejectThenGoaway,
    /// Every CONNECT gets a 200 head with a folded header value that `http` cannot represent.
    UnconvertibleHead,
    /// The first connection responds to its first CONNECT with 200, then resets its second with
    /// `CANCEL` and immediately closes the connection.
    ResetThenCloseSecondStream,
    /// The first connection responds to its first CONNECT with 200, then to its second by
    /// resetting the first stream with `CANCEL` and immediately closing the connection.
    ResetSiblingThenCloseSecondStream,
    /// The first connection responds to its first CONNECT with 200, then to its second with a
    /// 103 head, and immediately closes the connection.
    InformationalThenCloseSecondStream,
    /// The first connection responds to its first CONNECT with 200, then to its second with a
    /// complete header block whose second field is the invalid HPACK index 0.
    MalformedHeadOnSecondStream,
    /// The first connection responds to its first CONNECT with 200, then to its second with a
    /// GOAWAY that has that stream as its last-stream-id and `REFUSED_STREAM` as its error.
    RefusedGoawayCoveringSecondStream,
    /// Every CONNECT gets a 200 head. The proxy responds to the stream's first upload DATA, in
    /// one write, with `BEFORE_RESET` bytes of `pattern` and an RST_STREAM `CANCEL`.
    DataThenReset,
    /// Like `DataThenReset`, but the RST_STREAM follows the DATA after `RESET_DELAY`.
    DataThenLateReset,
    /// Every CONNECT gets a 200 head. The proxy responds to the stream's first upload DATA, in
    /// one write, with this many bytes of `pattern`, END_STREAM, and an RST_STREAM `CANCEL`.
    EndThenReset(usize),
    /// Like `EndThenReset`, but the stream's upload gets no credit, and the response comes once a
    /// whole stream window of it has arrived.
    EndThenResetOnFullWindow(usize),
    /// Like `EndThenResetOnFullWindow`, but without END_STREAM, and the RST_STREAM follows the
    /// DATA after `RESET_DELAY`.
    DataThenLateResetOnFullWindow(usize),
    /// Every CONNECT gets, in one write, a 200 head, this many bytes of `pattern`, END_STREAM,
    /// and an RST_STREAM `CANCEL`.
    EndThenResetAtOnce(usize),
    /// Every CONNECT gets a 103 head with `x-informational`, then after `INFORMATIONAL_DELAY` a
    /// 200 head, and its data is echoed.
    InformationalThenOk,
    /// Like `InformationalThenOk`, but the final head is a 403 that ends the stream.
    InformationalThenReject,
    /// Every CONNECT gets a 200 head. The first connection's first stream responds to its upload
    /// END_STREAM, in one write, with `LARGE_RESPONSE` bytes of `pattern` and END_STREAM. Every
    /// other stream echoes.
    LargeResponseAfterUploadEnd,
    /// The first connection responds to its CONNECTs before the given ordinal (1-based) with 200
    /// and echoes. From that CONNECT on it stops sending, PING acknowledgements included.
    SilentFrom(usize),
    /// The first connection responds to every CONNECT with 200 and echoes. Once its last open
    /// stream has ended, it stops acknowledging PINGs and sends a connection WINDOW_UPDATE every
    /// `IDLE_FRAME_PERIOD`.
    IdleWithoutPingAcks,
    /// Every CONNECT gets a 200 head and its data is echoed, except that the first connection
    /// responds to its second upload DATA with a GOAWAY(INTERNAL_ERROR) whose last-stream-id is
    /// that DATA's stream, then keeps its socket open and ignores every later frame.
    ErrorGoawayOnSecondData,
}

/// What a raw proxy observed.
#[derive(Default)]
pub(crate) struct RawObservations {
    connections: AtomicUsize,
    requests: AtomicUsize,
    streams: Mutex<Vec<(usize, u32)>>,
    resets: Mutex<Vec<(usize, u32, u32)>>,
    changed: Notify,
}

impl RawObservations {
    pub(crate) fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    pub(crate) fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    /// `(connection, stream)` of every CONNECT, in arrival order.
    pub(crate) fn streams(&self) -> Vec<(usize, u32)> {
        self.streams.lock().expect("streams lock").clone()
    }

    /// `(connection, stream, error code)` of every RST_STREAM the client sent.
    pub(crate) fn resets(&self) -> Vec<(usize, u32, u32)> {
        self.resets.lock().expect("resets lock").clone()
    }

    pub(crate) async fn wait(&self, what: &str, observed: impl Fn(&Self) -> bool) {
        timeout(TEST_TIMEOUT, async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if observed(self) {
                    return;
                }
                changed.await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("raw proxy never observed {what}"));
    }
}

/// An HTTP/2 proxy that writes frames itself, for wire behavior the `h2` crate hides.
pub(crate) struct RawProxy {
    pub(crate) address: SocketAddr,
    pub(crate) observations: Arc<RawObservations>,
    stop: watch::Sender<bool>,
    tasks: JoinSet<()>,
}

impl RawProxy {
    pub(crate) async fn start(script: RawScript) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("raw proxy binds");
        let address = listener.local_addr().expect("raw proxy address");
        let observations = Arc::new(RawObservations::default());
        let (stop, mut stopped) = watch::channel(false);
        let observed = Arc::clone(&observations);
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    _ = stopped.changed() => break,
                    accepted = listener.accept() => {
                        let Ok((io, _)) = accepted else { continue };
                        let index = observed.connections.fetch_add(1, Ordering::SeqCst);
                        observed.changed.notify_waiters();
                        connections.spawn(serve_raw(io, script, index, Arc::clone(&observed)));
                    }
                }
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        });
        Self {
            address,
            observations,
            stop,
            tasks,
        }
    }

    pub(crate) fn connector(&self) -> Arc<TestConnector> {
        Arc::new(TestConnector::plain(self.address))
    }

    pub(crate) async fn shutdown(mut self) {
        self.stop.send_replace(true);
        while self.tasks.join_next().await.is_some() {}
    }
}

/// An HPACK integer with a `prefix`-bit prefix (RFC 7541 section 5.1).
fn hpack_integer(prefix: u8, flags: u8, mut value: usize) -> Vec<u8> {
    let limit = (1_usize << prefix) - 1;
    if value < limit {
        return vec![flags | value as u8];
    }
    let mut encoded = vec![flags | limit as u8];
    value -= limit;
    while value >= 128 {
        encoded.push((value % 128) as u8 | 0x80);
        value /= 128;
    }
    encoded.push(value as u8);
    encoded
}

/// A literal header field without indexing and with a new name, without Huffman coding.
fn literal(name: &[u8], value: &[u8]) -> Vec<u8> {
    let mut field = vec![0];
    field.extend(hpack_integer(7, 0, name.len()));
    field.extend_from_slice(name);
    field.extend(hpack_integer(7, 0, value.len()));
    field.extend_from_slice(value);
    field
}

/// Reads whole frames. Cancel-safe, so a raw proxy can also wait on a timer.
#[derive(Default)]
struct FrameReader {
    buffered: Vec<u8>,
}

impl FrameReader {
    async fn next(&mut self, io: &mut TcpStream) -> io::Result<Frame> {
        loop {
            if let Some(frame) = parse_frames(&self.buffered).into_iter().next() {
                self.buffered.drain(..9 + frame.payload.len());
                return Ok(frame);
            }
            if io.read_buf(&mut self.buffered).await? == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
        }
    }
}

/// Resolves at the idle interval's next tick, never while the connection is not idle.
async fn idle_tick(idle: &mut Option<Interval>) {
    match idle {
        Some(interval) => {
            interval.tick().await;
        }
        None => pending().await,
    }
}

async fn serve_raw(
    mut io: TcpStream,
    script: RawScript,
    index: usize,
    observations: Arc<RawObservations>,
) {
    let mut preface = [0; PREFACE.len()];
    if io.read_exact(&mut preface).await.is_err() || preface != PREFACE {
        return;
    }
    if io
        .write_all(&encode_frame(SETTINGS, 0, 0, &[]))
        .await
        .is_err()
    {
        return;
    }
    let head = |status: &str, extra: &[(&str, &[u8])]| {
        let mut block = literal(b":status", status.as_bytes());
        extra
            .iter()
            .for_each(|(name, value)| block.extend(literal(name.as_bytes(), value)));
        block
    };
    let mut streams = Vec::new();
    let mut echoing = HashSet::new();
    let mut resetting = HashSet::new();
    let mut finishing = HashSet::new();
    let mut ending = HashMap::new();
    let mut block: Option<(u32, u8, Vec<u8>)> = None;
    let mut reader = FrameReader::default();
    let mut silent = false;
    let mut uploads = 0;
    let mut open = HashSet::new();
    let mut idle: Option<Interval> = None;
    loop {
        let read = tokio::select! {
            frame = reader.next(&mut io) => Some(frame),
            () = idle_tick(&mut idle) => None,
        };
        let Some(read) = read else {
            let update = encode_frame(WINDOW_UPDATE, 0, 0, &1_u32.to_be_bytes());
            if io.write_all(&update).await.is_err() {
                return;
            }
            continue;
        };
        let Ok(frame) = read else {
            return;
        };
        if silent {
            continue;
        }
        if script == RawScript::IdleWithoutPingAcks && index == 0 {
            let ended = match frame.kind {
                DATA if frame.flags & END_STREAM != 0 => open.remove(&frame.stream),
                RST_STREAM => open.remove(&frame.stream),
                _ => false,
            };
            if ended && open.is_empty() && idle.is_none() {
                idle = Some(tokio::time::interval(IDLE_FRAME_PERIOD));
            }
        }
        let mut output = Vec::new();
        match frame.kind {
            SETTINGS if frame.flags & ACK == 0 => output = encode_frame(SETTINGS, ACK, 0, &[]),
            PING if frame.flags & ACK == 0 && idle.is_none() => {
                output = encode_frame(PING, ACK, 0, &frame.payload);
            }
            RST_STREAM => {
                let code = u32::from_be_bytes(frame.payload[..4].try_into().expect("RST code"));
                observations
                    .resets
                    .lock()
                    .expect("resets lock")
                    .push((index, frame.stream, code));
                observations.changed.notify_waiters();
            }
            DATA => {
                let length = u32::try_from(frame.payload.len()).expect("DATA length");
                let credited = !matches!(
                    script,
                    RawScript::EndThenResetOnFullWindow(_)
                        | RawScript::DataThenLateResetOnFullWindow(_)
                );
                if length != 0 && credited {
                    output.extend(encode_frame(WINDOW_UPDATE, 0, 0, &length.to_be_bytes()));
                    output.extend(encode_frame(
                        WINDOW_UPDATE,
                        0,
                        frame.stream,
                        &length.to_be_bytes(),
                    ));
                }
                if script == RawScript::ErrorGoawayOnSecondData && index == 0 && length != 0 {
                    uploads += 1;
                }
                if uploads == 2 {
                    let mut payload = frame.stream.to_be_bytes().to_vec();
                    payload.extend_from_slice(&INTERNAL_ERROR.to_be_bytes());
                    output.extend(encode_frame(GOAWAY, 0, 0, &payload));
                    silent = true;
                } else if echoing.contains(&frame.stream) {
                    output.extend(encode_frame(
                        DATA,
                        frame.flags & END_STREAM,
                        frame.stream,
                        &frame.payload,
                    ));
                }
                if frame.flags & END_STREAM != 0 && finishing.remove(&frame.stream) {
                    for chunk in pattern(LARGE_RESPONSE).chunks(MAX_FRAME) {
                        output.extend(encode_frame(DATA, 0, frame.stream, chunk));
                    }
                    output.extend(encode_frame(DATA, END_STREAM, frame.stream, &[]));
                }
                if let Some(received) = ending.get_mut(&frame.stream) {
                    *received += frame.payload.len();
                    let full = *received >= RAW_STREAM_WINDOW;
                    let answer = match script {
                        RawScript::EndThenReset(download) if *received != 0 => {
                            Some((download, true))
                        }
                        RawScript::EndThenResetOnFullWindow(download) if full => {
                            Some((download, true))
                        }
                        RawScript::DataThenLateResetOnFullWindow(download) if full => {
                            Some((download, false))
                        }
                        _ => None,
                    };
                    if let Some((download, end)) = answer {
                        ending.remove(&frame.stream);
                        for chunk in pattern(download).chunks(MAX_FRAME) {
                            output.extend(encode_frame(DATA, 0, frame.stream, chunk));
                        }
                        if end {
                            output.extend(encode_frame(DATA, END_STREAM, frame.stream, &[]));
                        } else {
                            if io.write_all(&output).await.is_err() {
                                return;
                            }
                            output.clear();
                            tokio::time::sleep(RESET_DELAY).await;
                        }
                        let cancel = 8_u32.to_be_bytes();
                        output.extend(encode_frame(RST_STREAM, 0, frame.stream, &cancel));
                    }
                }
                if resetting.remove(&frame.stream) {
                    output.extend(encode_frame(DATA, 0, frame.stream, &pattern(BEFORE_RESET)));
                    if script == RawScript::DataThenLateReset {
                        if io.write_all(&output).await.is_err() {
                            return;
                        }
                        output.clear();
                        tokio::time::sleep(RESET_DELAY).await;
                    }
                    let cancel = 8_u32.to_be_bytes();
                    output.extend(encode_frame(RST_STREAM, 0, frame.stream, &cancel));
                }
            }
            HEADERS | CONTINUATION => {
                if frame.kind == HEADERS {
                    block = Some((frame.stream, frame.flags, header_fragment(&frame).to_vec()));
                } else if let Some((_, _, fragment)) = block.as_mut() {
                    fragment.extend_from_slice(&frame.payload);
                }
                if frame.flags & END_HEADERS != 0
                    && let Some((stream, _, fragment)) = block.take()
                {
                    assert!(!fragment.is_empty(), "the client sent a header block");
                    streams.push(stream);
                    observations
                        .streams
                        .lock()
                        .expect("streams lock")
                        .push((index, stream));
                    observations.requests.fetch_add(1, Ordering::SeqCst);
                    observations.changed.notify_waiters();
                    let ordinal = streams.len();
                    open.insert(stream);
                    match (script, index, ordinal) {
                        (RawScript::SilentFrom(first_silent), 0, _) if ordinal >= first_silent => {
                            silent = true;
                        }
                        (
                            RawScript::GoawayBelowSecondStream
                            | RawScript::ErrorGoawayBelowSecondStream,
                            0,
                            2,
                        ) => {
                            let code: u32 = if script == RawScript::ErrorGoawayBelowSecondStream {
                                11
                            } else {
                                0
                            };
                            let mut payload = streams[0].to_be_bytes().to_vec();
                            payload.extend_from_slice(&code.to_be_bytes());
                            output = encode_frame(GOAWAY, 0, 0, &payload);
                        }
                        (RawScript::ResetThenCloseSecondStream, 0, 2) => {
                            let cancel = 8_u32.to_be_bytes();
                            let reset = encode_frame(RST_STREAM, 0, stream, &cancel);
                            let _ = io.write_all(&reset).await;
                            return;
                        }
                        (RawScript::ResetSiblingThenCloseSecondStream, 0, 2) => {
                            let cancel = 8_u32.to_be_bytes();
                            let reset = encode_frame(RST_STREAM, 0, streams[0], &cancel);
                            let _ = io.write_all(&reset).await;
                            return;
                        }
                        (RawScript::RefusedGoawayCoveringSecondStream, 0, 2) => {
                            let mut payload = stream.to_be_bytes().to_vec();
                            payload.extend_from_slice(&7_u32.to_be_bytes());
                            output = encode_frame(GOAWAY, 0, 0, &payload);
                        }
                        (RawScript::MalformedHeadOnSecondStream, 0, 2) => {
                            // `:status: 200` from the static table, then index 0.
                            output = encode_frame(HEADERS, END_HEADERS, stream, &[0x88, 0x80]);
                        }
                        (RawScript::InformationalThenCloseSecondStream, 0, 2) => {
                            let early = head("103", &[("x-informational", b"early hint")]);
                            let early = encode_frame(HEADERS, END_HEADERS, stream, &early);
                            let _ = io.write_all(&early).await;
                            return;
                        }
                        (RawScript::LargeResponseAfterUploadEnd, 0, 1) => {
                            finishing.insert(stream);
                            output = encode_frame(HEADERS, END_HEADERS, stream, &head("200", &[]));
                        }
                        (RawScript::RejectThenGoaway, 0, 1) => {}
                        (RawScript::RejectThenGoaway, 0, 2) => {
                            output = encode_frame(
                                HEADERS,
                                END_HEADERS | END_STREAM,
                                streams[0],
                                &head("403", &[("x-test-proxy", b"raw")]),
                            );
                            let mut payload = streams[0].to_be_bytes().to_vec();
                            payload.extend_from_slice(&0_u32.to_be_bytes());
                            output.extend(encode_frame(GOAWAY, 0, 0, &payload));
                        }
                        (RawScript::DataThenReset | RawScript::DataThenLateReset, _, _) => {
                            resetting.insert(stream);
                            output = encode_frame(HEADERS, END_HEADERS, stream, &head("200", &[]));
                        }
                        (RawScript::EndThenResetAtOnce(download), _, _) => {
                            output = encode_frame(HEADERS, END_HEADERS, stream, &head("200", &[]));
                            for chunk in pattern(download).chunks(MAX_FRAME) {
                                output.extend(encode_frame(DATA, 0, stream, chunk));
                            }
                            output.extend(encode_frame(DATA, END_STREAM, stream, &[]));
                            let cancel = 8_u32.to_be_bytes();
                            output.extend(encode_frame(RST_STREAM, 0, stream, &cancel));
                        }
                        (
                            RawScript::EndThenReset(_)
                            | RawScript::EndThenResetOnFullWindow(_)
                            | RawScript::DataThenLateResetOnFullWindow(_),
                            _,
                            _,
                        ) => {
                            ending.insert(stream, 0);
                            output = encode_frame(HEADERS, END_HEADERS, stream, &head("200", &[]));
                        }
                        (
                            RawScript::InformationalThenOk | RawScript::InformationalThenReject,
                            _,
                            _,
                        ) => {
                            let early = head("103", &[("x-informational", b"early hint")]);
                            let early = encode_frame(HEADERS, END_HEADERS, stream, &early);
                            if io.write_all(&early).await.is_err() {
                                return;
                            }
                            tokio::time::sleep(INFORMATIONAL_DELAY).await;
                            output = if script == RawScript::InformationalThenOk {
                                echoing.insert(stream);
                                encode_frame(HEADERS, END_HEADERS, stream, &head("200", &[]))
                            } else {
                                let rejection = head("403", &[("x-test-proxy", b"raw")]);
                                encode_frame(HEADERS, END_HEADERS | END_STREAM, stream, &rejection)
                            };
                        }
                        (RawScript::UnconvertibleHead, _, _) => {
                            output = encode_frame(
                                HEADERS,
                                END_HEADERS,
                                stream,
                                &head("200", &[("x-folded", b"first\r\n second")]),
                            );
                        }
                        _ => {
                            echoing.insert(stream);
                            output = encode_frame(HEADERS, END_HEADERS, stream, &head("200", &[]));
                        }
                    }
                }
            }
            _ => {}
        }
        if !output.is_empty() && io.write_all(&output).await.is_err() {
            return;
        }
    }
}
