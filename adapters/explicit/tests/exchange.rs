/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Tests for the host-agnostic exchange over loopback and in-memory byte streams.

pub(crate) mod support {
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::future::Future;
    use std::future::poll_fn;
    use std::io;
    use std::io::Read;
    use std::io::Write;
    use std::net::SocketAddr;
    use std::net::TcpListener;
    use std::net::TcpStream;
    use std::num::NonZeroUsize;
    use std::rc::Rc;
    use std::rc::Weak;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::MutexGuard;
    use std::sync::PoisonError;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Poll;
    use std::task::Waker;
    use std::thread::JoinHandle;
    use std::time::Duration;
    use std::time::Instant;

    use futures::executor::LocalPool;
    use futures::executor::LocalSpawner;
    use futures::task::LocalSpawnExt;
    use http::HeaderName;
    use http::HeaderValue;
    use http::uri::Authority;
    use http::uri::PathAndQuery;
    use roxygen_core::connection::TransportEndpoint;
    use roxygen_core::connection::TransportSecurity;
    use roxygen_core::message::SocketAddress;
    use roxygen_runtime::cancellation::CancellationToken;
    use roxygen_runtime::driver::WorkerDriver;
    use roxygen_runtime::host::Host;
    use roxygen_runtime::host::LocalFuture;
    use roxygen_runtime::host::LocalHost;
    use roxygen_runtime::panic::discard_panic;
    use roxygen_runtime::worker::WorkerContext;
    use roxygen_runtime::worker::WorkerHandle;
    use roxygen_transport::established::EstablishedIo;
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::PrivateKeyDer;
    use rustls::server::ClientHello;
    use rustls::server::ResolvesServerCert;
    use rustls::sign::CertifiedKey;

    use crate::error::ExplicitError;
    use crate::exchange::ExchangeHooks;
    use crate::exchange::ExchangeInput;
    use crate::exchange::ExchangeProgress;
    use crate::exchange::Request;
    use crate::exchange::Response;
    use crate::exchange::TestPause;
    use crate::exchange::run_exchange;
    use crate::runtime::ExplicitRuntime;
    use crate::runtime::ExplicitRuntimeConfig;
    use crate::runtime::Security;
    use crate::runtime::Target;
    use crate::stats::ExplicitStats;

    /// Bounds a wait that only guards against a hang.
    pub(crate) const HANG_GUARD: Duration = Duration::from_secs(10);

    /// A real-time deadline budget for a test that needs its send to reach a given point,
    /// such as a dial, before the deadline passes: long enough for a loaded host's worker to get
    /// there well before it.
    pub(crate) const EXPIRY_BUDGET: Duration = Duration::from_secs(2);

    /// Releases its pause when dropped. Declared after the runtime, it drops first when a failed
    /// assertion unwinds, so the runtime's drop never waits on a cleanup or restart that stays
    /// held.
    pub(crate) struct ReleaseOnDrop(pub(crate) TestPause);

    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    pub(crate) trait ServerStream: Read + Write + Send {}
    impl<Stream: Read + Write + Send> ServerStream for Stream {}

    /// How a server-side read ended once the client stopped sending.
    #[derive(Debug, Eq, PartialEq)]
    pub(crate) enum ReadEnd {
        Eof,
        Error(io::ErrorKind),
    }

    /// One parsed request head, as the server received it.
    #[derive(Debug)]
    pub(crate) struct RawRequest {
        pub(crate) request_line: String,
        pub(crate) headers: Vec<(String, String)>,
    }

    impl RawRequest {
        pub(crate) fn values(&self, name: &str) -> Vec<&str> {
            self.headers
                .iter()
                .filter(|(header, _)| header.eq_ignore_ascii_case(name))
                .map(|(_, value)| value.as_str())
                .collect()
        }

        pub(crate) fn position(&self, name: &str) -> Option<usize> {
            self.headers
                .iter()
                .position(|(header, _)| header.eq_ignore_ascii_case(name))
        }
    }

    /// The server side of one accepted connection.
    pub(crate) struct Conn {
        stream: Box<dyn ServerStream>,
        tcp: TcpStream,
        pending: Vec<u8>,
    }

    impl Conn {
        /// Reads one request head, failing the handler on EOF or the hang guard.
        pub(crate) fn read_request(&mut self) -> RawRequest {
            self.try_read_request()
                .expect("client closed before sending a request")
        }

        /// Reads one request head, or `None` if the client closes first.
        pub(crate) fn try_read_request(&mut self) -> Option<RawRequest> {
            self.tcp
                .set_read_timeout(Some(HANG_GUARD))
                .expect("read timeout");
            loop {
                if let Some(end) = find(&self.pending, b"\r\n\r\n") {
                    let head: Vec<u8> = self.pending.drain(..end + 4).collect();
                    let head = String::from_utf8(head).expect("ASCII request head");
                    let mut lines = head.split("\r\n");
                    let request_line = lines.next().expect("request line").to_owned();
                    let headers = lines
                        .filter(|line| !line.is_empty())
                        .map(|line| {
                            let (name, value) = line.split_once(':').expect("header line");
                            (name.to_owned(), value.trim().to_owned())
                        })
                        .collect();
                    return Some(RawRequest {
                        request_line,
                        headers,
                    });
                }
                let mut buffer = [0; 4096];
                let read = self.stream.read(&mut buffer).expect("request bytes");
                if read == 0 {
                    return None;
                }
                self.pending.extend_from_slice(&buffer[..read]);
            }
        }

        pub(crate) fn write(&mut self, bytes: &[u8]) -> io::Result<()> {
            self.stream.write_all(bytes)?;
            self.stream.flush()
        }

        /// Reads until the client closes the connection or the guard expires.
        pub(crate) fn read_end(&mut self, guard: Duration) -> Option<ReadEnd> {
            let deadline = Instant::now() + guard;
            self.tcp
                .set_read_timeout(Some(Duration::from_millis(20)))
                .expect("read timeout");
            let mut buffer = [0; 4096];
            while Instant::now() < deadline {
                match self.stream.read(&mut buffer) {
                    Ok(0) => return Some(ReadEnd::Eof),
                    Ok(_) => {}
                    Err(error)
                        if matches!(
                            error.kind(),
                            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                        ) => {}
                    Err(error) => return Some(ReadEnd::Error(error.kind())),
                }
            }
            None
        }

        /// Whether the client closed the connection, by EOF, error, or reset, within the guard.
        pub(crate) fn sees_close(&mut self, guard: Duration) -> bool {
            self.read_end(guard).is_some()
        }

        /// Writes one byte per gap until a write fails or the guard expires.
        pub(crate) fn trickle_until_closed(&mut self, gap: Duration, guard: Duration) -> bool {
            let deadline = Instant::now() + guard;
            while Instant::now() < deadline {
                std::thread::sleep(gap);
                if self.write(b"a").is_err() {
                    return true;
                }
            }
            false
        }
    }

    fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack
            .windows(needle.len())
            .position(|window| window == needle)
    }

    type Handler = Arc<dyn Fn(Conn) + Send + Sync>;

    /// A blocking loopback server that runs `handler` on its own thread per connection and
    /// joins every thread when dropped.
    pub(crate) struct TestServer {
        pub(crate) address: SocketAddr,
        accepted: Arc<AtomicUsize>,
        peers: Arc<Mutex<Vec<SocketAddr>>>,
        stop: Arc<AtomicBool>,
        acceptor: Option<JoinHandle<()>>,
        handlers: Arc<Mutex<Vec<JoinHandle<()>>>>,
        gates: Vec<TestPause>,
    }

    impl TestServer {
        pub(crate) fn plain(handler: impl Fn(Conn) + Send + Sync + 'static) -> Self {
            Self::bind("127.0.0.1:0", None, handler)
        }

        pub(crate) fn tls(
            config: Arc<rustls::ServerConfig>,
            handler: impl Fn(Conn) + Send + Sync + 'static,
        ) -> Self {
            Self::bind("127.0.0.1:0", Some(config), handler)
        }

        pub(crate) fn bind(
            address: &str,
            tls: Option<Arc<rustls::ServerConfig>>,
            handler: impl Fn(Conn) + Send + Sync + 'static,
        ) -> Self {
            let listener = TcpListener::bind(address).expect("loopback listener");
            let address = listener.local_addr().expect("listener address");
            let accepted = Arc::new(AtomicUsize::new(0));
            let peers = Arc::new(Mutex::new(Vec::new()));
            let stop = Arc::new(AtomicBool::new(false));
            let handlers = Arc::new(Mutex::new(Vec::new()));
            let handler: Handler = Arc::new(handler);
            let acceptor = {
                let accepted = Arc::clone(&accepted);
                let peers = Arc::clone(&peers);
                let stop = Arc::clone(&stop);
                let handlers = Arc::clone(&handlers);
                std::thread::spawn(move || {
                    for stream in listener.incoming() {
                        if stop.load(Ordering::SeqCst) {
                            return;
                        }
                        let Ok(tcp) = stream else { continue };
                        let peer = tcp.peer_addr().expect("peer address");
                        accepted.fetch_add(1, Ordering::SeqCst);
                        peers.lock().unwrap().push(peer);
                        // Writes, like reads, are bounded, so the drop's joins are bounded even
                        // after a test fails.
                        tcp.set_write_timeout(Some(HANG_GUARD))
                            .expect("write timeout");
                        let handler = Arc::clone(&handler);
                        let tls = tls.clone();
                        let thread = std::thread::spawn(move || {
                            let control = tcp.try_clone().expect("socket clone");
                            let stream: Box<dyn ServerStream> = match tls {
                                Some(config) => Box::new(rustls::StreamOwned::new(
                                    rustls::ServerConnection::new(config).expect("TLS server"),
                                    tcp,
                                )),
                                None => Box::new(tcp),
                            };
                            handler(Conn {
                                stream,
                                tcp: control,
                                pending: Vec::new(),
                            });
                        });
                        handlers.lock().unwrap().push(thread);
                    }
                })
            };
            Self {
                address,
                accepted,
                peers,
                stop,
                acceptor: Some(acceptor),
                handlers,
                gates: Vec::new(),
            }
        }

        /// Releases `gate` when the server drops, before it joins its handlers.
        pub(crate) fn release_on_drop(&mut self, gate: TestPause) {
            self.gates.push(gate);
        }

        pub(crate) fn accepted(&self) -> usize {
            self.accepted.load(Ordering::SeqCst)
        }

        pub(crate) fn peer_ports(&self) -> Vec<u16> {
            self.peers
                .lock()
                .unwrap()
                .iter()
                .map(SocketAddr::port)
                .collect()
        }
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            let _ = TcpStream::connect(self.address);
            if let Some(acceptor) = self.acceptor.take() {
                let _ = acceptor.join();
            }
            self.gates.iter().for_each(TestPause::release);
            let unwinding = std::thread::panicking();
            let handlers = std::mem::take(&mut *lock(&self.handlers));
            let failure = handlers
                .into_iter()
                .filter_map(|handler| handler.join().err())
                .reduce(|first, later| {
                    discard_panic(later);
                    first
                });
            match failure {
                Some(payload) if !unwinding => std::panic::resume_unwind(payload),
                Some(payload) => discard_panic(payload),
                None => {}
            }
        }
    }

    /// Locks without panicking, since a drop during unwinding must not panic.
    fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
        mutex.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A server that responds to every request with `response` and then waits for the close.
    pub(crate) fn responding(response: impl Into<Vec<u8>>) -> TestServer {
        let response = response.into();
        TestServer::plain(move |mut conn| {
            conn.read_request();
            let _ = conn.write(&response);
            conn.sees_close(HANG_GUARD);
        })
    }

    #[derive(Debug)]
    struct RecordingResolver {
        key: Arc<CertifiedKey>,
        alpn_offers: Arc<Mutex<Vec<Option<Vec<Vec<u8>>>>>>,
    }

    impl ResolvesServerCert for RecordingResolver {
        fn resolve(&self, client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
            let offer = client_hello
                .alpn()
                .map(|protocols| protocols.map(<[u8]>::to_vec).collect());
            self.alpn_offers.lock().unwrap().push(offer);
            Some(Arc::clone(&self.key))
        }
    }

    /// A self-signed server identity for `localhost`, `127.0.0.1`, and `::1`.
    pub(crate) struct TlsFixture {
        pub(crate) server: Arc<rustls::ServerConfig>,
        pub(crate) client: Arc<rustls::ClientConfig>,
        pub(crate) alpn_offers: Arc<Mutex<Vec<Option<Vec<Vec<u8>>>>>>,
    }

    pub(crate) fn install_crypto_provider() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    pub(crate) fn tls_fixture() -> TlsFixture {
        install_crypto_provider();
        let generated = rcgen::generate_simple_self_signed(vec![
            "localhost".to_owned(),
            "127.0.0.1".to_owned(),
            "::1".to_owned(),
        ])
        .expect("self-signed certificate");
        let certificate: CertificateDer<'static> = generated.cert.der().clone();
        let key = PrivateKeyDer::try_from(generated.signing_key.serialized_der().to_vec())
            .expect("private key");
        let signing_key =
            rustls::crypto::ring::sign::any_supported_type(&key).expect("signing key");
        let alpn_offers = Arc::new(Mutex::new(Vec::new()));
        let server = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(RecordingResolver {
                key: Arc::new(CertifiedKey::new(vec![certificate.clone()], signing_key)),
                alpn_offers: Arc::clone(&alpn_offers),
            }));
        let mut roots = rustls::RootCertStore::empty();
        roots.add(certificate).expect("trusted root");
        let client = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        TlsFixture {
            server: Arc::new(server),
            client: Arc::new(client),
            alpn_offers,
        }
    }

    pub(crate) fn untrusting_client() -> Arc<rustls::ClientConfig> {
        install_crypto_provider();
        Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
        )
    }

    pub(crate) fn request(authority: &str, path: &str, budget: Duration) -> Request {
        request_with_headers(authority, path, budget, Vec::new())
    }

    pub(crate) fn request_with_headers(
        authority: &str,
        path: &str,
        budget: Duration,
        headers: Vec<(&'static str, &'static str)>,
    ) -> Request {
        Request {
            path_and_query: PathAndQuery::try_from(path).expect("path and query"),
            authority: Authority::try_from(authority).expect("authority"),
            headers: headers
                .into_iter()
                .map(|(name, value)| {
                    (
                        HeaderName::from_static(name),
                        HeaderValue::from_static(value),
                    )
                })
                .collect(),
            deadline: Instant::now() + budget,
        }
    }

    pub(crate) fn plain_target(address: SocketAddr) -> Arc<Target> {
        Arc::new(Target {
            address,
            security: Security::Plaintext,
        })
    }

    /// A loopback address that refuses connections. Its port stays bound, with no listener, for
    /// as long as the returned socket lives, so that another test's listener cannot take it.
    pub(crate) fn refusing_address() -> (SocketAddr, tokio::net::TcpSocket) {
        let socket = tokio::net::TcpSocket::new_v4().expect("socket");
        socket
            .bind("127.0.0.1:0".parse().expect("address"))
            .expect("bind");
        let address = socket.local_addr().expect("bound address");
        (address, socket)
    }

    pub(crate) fn tls_target(
        address: SocketAddr,
        config: &Arc<rustls::ClientConfig>,
        host: &str,
    ) -> Arc<Target> {
        Arc::new(Target {
            address,
            security: Security::tls(Arc::clone(config), host).expect("TLS security"),
        })
    }

    pub(crate) fn config(
        workers: usize,
        command_capacity: usize,
        thread_name_prefix: &'static str,
    ) -> ExplicitRuntimeConfig {
        ExplicitRuntimeConfig {
            workers: NonZeroUsize::new(workers).expect("workers"),
            command_capacity: NonZeroUsize::new(command_capacity).expect("capacity"),
            max_sends_in_flight: NonZeroUsize::new(command_capacity).expect("capacity"),
            thread_name_prefix,
            max_restarts_per_minute: 10,
        }
    }

    pub(crate) fn start(workers: usize, thread_name_prefix: &'static str) -> ExplicitRuntime {
        ExplicitRuntime::start(config(workers, 64, thread_name_prefix)).expect("runtime starts")
    }

    /// Runs `future` on a fresh multi-thread Tokio runtime, bounded by the hang guard.
    pub(crate) fn block_on<Work: Future>(future: Work) -> Work::Output {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("caller runtime")
            .block_on(async {
                tokio::time::timeout(HANG_GUARD * 3, future)
                    .await
                    .expect("bounded test")
            })
    }

    /// Polls `condition` until it holds, failing after the hang guard.
    pub(crate) fn wait_until(what: &str, condition: impl FnMut() -> bool) {
        assert!(eventually(condition), "timed out waiting for {what}");
    }

    /// Polls `condition` until it holds or the hang guard passes, and reports whether it held.
    pub(crate) fn eventually(mut condition: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + HANG_GUARD;
        while !condition() {
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        true
    }

    /// Counts this process's threads whose name starts with `{prefix}-` and that have not begun
    /// to exit.
    pub(crate) fn live_threads(prefix: &str) -> usize {
        // A listing of `/proc/self/task` ends early when the thread it is at exits, and the next
        // read resumes by position, so a thread that exits during a listing (another test's, or
        // one just joined) can hide a live one. Two listings in a row that agree did not hide a
        // thread.
        let wanted = format!("{prefix}-");
        let deadline = Instant::now() + HANG_GUARD;
        let mut previous = live_thread_ids(&wanted);
        loop {
            let listed = live_thread_ids(&wanted);
            if listed == previous {
                return listed.len();
            }
            assert!(
                Instant::now() < deadline,
                "the count of threads named {wanted}* did not stabilize"
            );
            previous = listed;
        }
    }

    /// The IDs of this process's threads whose name starts with `wanted` and that have not begun
    /// to exit, in listing order.
    fn live_thread_ids(wanted: &str) -> Vec<String> {
        // `pthread_join` returns once the kernel clears the thread's ID, which happens after the
        // thread sets `PF_EXITING` but before it leaves `/proc/self/task`, so a joined thread
        // can still be listed with that flag set.
        const PF_EXITING: u64 = 0x4;
        std::fs::read_dir("/proc/self/task")
            .expect("thread list")
            .filter_map(Result::ok)
            .filter_map(|task| {
                let stat = std::fs::read_to_string(task.path().join("stat")).ok()?;
                // `tid (comm) state ppid pgrp session tty_nr tpgid flags ...`, where `comm` may
                // hold spaces or parentheses.
                let (open, close) = (stat.find('(')?, stat.rfind(')')?);
                let flags = stat[close + 1..]
                    .split_whitespace()
                    .nth(6)?
                    .parse::<u64>()
                    .ok()?;
                (stat[open + 1..close].starts_with(wanted) && flags & PF_EXITING == 0)
                    .then(|| task.file_name().to_string_lossy().into_owned())
            })
            .collect()
    }

    pub(crate) fn endpoint(address: SocketAddr) -> TransportEndpoint {
        TransportEndpoint::Socket(SocketAddress::new(address.ip().to_string(), address.port()))
    }

    /// Runs `factory` as a command on a `WorkerDriver::with_host(Host::tokio())` that the test
    /// owns, then shuts that driver down and waits for it.
    pub(crate) fn run_on_tokio_driver<Output, Factory, Work>(factory: Factory) -> Output
    where
        Output: Send + 'static,
        Factory: FnOnce(WorkerContext, WorkerHandle) -> Work + Send + 'static,
        Work: Future<Output = Output> + 'static,
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("driver runtime");
        let local = tokio::task::LocalSet::new();
        local.block_on(&runtime, async move {
            let (handle, driver) = WorkerDriver::with_host(16, Host::tokio()).expect("driver");
            let driver = tokio::task::spawn_local(driver);
            let worker = handle.clone();
            let call = handle
                .call_local(move |context| factory(context, worker))
                .expect("command admitted");
            let output = tokio::time::timeout(HANG_GUARD, call)
                .await
                .expect("bounded command")
                .expect("command result");
            handle.begin_shutdown();
            tokio::time::timeout(HANG_GUARD, driver)
                .await
                .expect("bounded driver shutdown")
                .expect("driver task");
            output
        })
    }

    struct Timer {
        deadline: Instant,
        waker: RefCell<Option<Waker>>,
    }

    /// A non-Tokio `LocalHost` over a futures `LocalPool` whose clock the test advances.
    pub(crate) struct ManualHost {
        spawner: LocalSpawner,
        now: Rc<Cell<Instant>>,
        timers: RefCell<Vec<Weak<Timer>>>,
    }

    impl ManualHost {
        pub(crate) fn create() -> (LocalPool, Rc<Self>, Host) {
            let pool = LocalPool::new();
            let implementation = Rc::new(Self {
                spawner: pool.spawner(),
                now: Rc::new(Cell::new(Instant::now() + Duration::from_secs(3600))),
                timers: RefCell::new(Vec::new()),
            });
            let host = Host::new(implementation.clone());
            (pool, implementation, host)
        }

        pub(crate) fn advance(&self, duration: Duration) {
            self.now.set(self.now.get() + duration);
            let ready: Vec<_> = self
                .timers
                .borrow()
                .iter()
                .filter_map(Weak::upgrade)
                .filter(|timer| timer.deadline <= self.now.get())
                .filter_map(|timer| timer.waker.borrow_mut().take())
                .collect();
            ready.into_iter().for_each(Waker::wake);
        }
    }

    impl LocalHost for ManualHost {
        fn spawn_local(&self, future: LocalFuture) {
            self.spawner.spawn_local(future).unwrap();
        }

        fn now(&self) -> Instant {
            self.now.get()
        }

        fn sleep_until(&self, deadline: Instant) -> LocalFuture {
            let timer = Rc::new(Timer {
                deadline,
                waker: RefCell::new(None),
            });
            let mut timers = self.timers.borrow_mut();
            timers.retain(|timer| timer.strong_count() != 0);
            timers.push(Rc::downgrade(&timer));
            let now = Rc::clone(&self.now);
            Box::pin(poll_fn(move |context| {
                if now.get() >= timer.deadline {
                    Poll::Ready(())
                } else {
                    *timer.waker.borrow_mut() = Some(context.waker().clone());
                    Poll::Pending
                }
            }))
        }

        fn yield_to_io(&self) -> LocalFuture {
            let mut yielded = false;
            Box::pin(poll_fn(move |context| {
                if yielded {
                    Poll::Ready(())
                } else {
                    yielded = true;
                    context.waker().wake_by_ref();
                    Poll::Pending
                }
            }))
        }
    }

    /// Runs one core exchange on a non-Tokio host against a scripted in-memory peer. With
    /// `advance_past_deadline`, the manual clock moves to the deadline once everything stalls,
    /// and then a `before_head` pause in `hooks` is released.
    pub(crate) fn run_on_manual_host<Peer, PeerWork>(
        budget: Duration,
        hooks: ExchangeHooks,
        peer: Peer,
        advance_past_deadline: bool,
    ) -> Result<Response, ExplicitError>
    where
        Peer: FnOnce(tokio::io::DuplexStream) -> PeerWork,
        PeerWork: Future<Output = ()> + 'static,
    {
        // The adopted stream's transport arms its read and write timers with
        // `TimedBaton::new`, which needs a Tokio context, so the exchange runs inside this one.
        // This runtime is never driven. The exchange's deadline and the session's timers run on
        // the manual clock, and the transport's timers never fire.
        let timers = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("transport timer context");
        let (mut pool, clock, host) = ManualHost::create();
        let _timers = timers.enter();
        assert_eq!(tokio::runtime::Handle::current().id(), timers.handle().id());
        let (handle, driver) = WorkerDriver::with_host(16, host).expect("driver");
        let (client_io, peer_io) = tokio::io::duplex(64 * 1024);
        let address = "127.0.0.1:80".parse().expect("address");
        let command_hooks = hooks.clone();
        let call = handle
            .call_local(move |context| async move {
                let started = context.host().now();
                let mut request = request("peer.example", "/", budget);
                request.deadline = started + budget;
                let progress = Arc::new(ExchangeProgress::new());
                assert!(progress.try_start());
                let io = EstablishedIo::new(client_io, endpoint(address), endpoint(address), None);
                let input = ExchangeInput {
                    worker: context,
                    io,
                    security: TransportSecurity::Plaintext,
                    request,
                    cancellation: CancellationToken::never(),
                    progress,
                    started,
                    stats: Arc::new(ExplicitStats::new()),
                };
                run_exchange(input, &command_hooks).await
            })
            .expect("command admitted");
        let result = Rc::new(RefCell::new(None));
        let completed = Rc::clone(&result);
        let worker = handle.clone();
        pool.spawner()
            .spawn_local(async move {
                let outcome = call.await.expect("command result");
                *completed.borrow_mut() = Some(outcome);
                worker.begin_shutdown();
            })
            .expect("caller task");
        pool.spawner().spawn_local(driver).expect("driver task");
        pool.spawner()
            .spawn_local(peer(peer_io))
            .expect("peer task");
        pool.run_until_stalled();
        if advance_past_deadline {
            assert!(
                result.borrow().is_none(),
                "exchange ended before the deadline"
            );
            clock.advance(budget);
            if let Some(pause) = &hooks.before_head {
                pause.release();
            }
            pool.run_until_stalled();
        }
        result.borrow_mut().take().expect("exchange finished")
    }
}

mod exchange {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::sync::mpsc::Receiver;
    use std::time::Duration;

    use http::HeaderValue;
    use roxygen_core::connection::TransportSecurity;
    use roxygen_core::error::HttpErrorCode;
    use roxygen_runtime::cancellation::CancellationToken;
    use roxygen_runtime::driver::WorkerDriver;
    use roxygen_transport::established::EstablishedIo;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    use super::support::EXPIRY_BUDGET;
    use super::support::HANG_GUARD;
    use super::support::ReadEnd;
    use super::support::TestServer;
    use super::support::block_on;
    use super::support::endpoint;
    use super::support::plain_target;
    use super::support::request;
    use super::support::request_with_headers;
    use super::support::responding;
    use super::support::run_on_manual_host;
    use super::support::run_on_tokio_driver;
    use super::support::start;
    use super::support::tls_fixture;
    use super::support::tls_target;
    use super::support::wait_until;
    use crate::error::ErrorKind;
    use crate::error::Phase;
    use crate::exchange::BodyOutcome;
    use crate::exchange::ExchangeHooks;
    use crate::exchange::ExchangeInput;
    use crate::exchange::ExchangeProgress;
    use crate::exchange::Response;
    use crate::exchange::TestPause;
    use crate::exchange::exchange;
    use crate::exchange::run_exchange;
    use crate::runtime::ExplicitClient;
    use crate::stats::ExplicitStats;

    fn complete_bytes(response: &Response) -> u64 {
        match &response.body {
            BodyOutcome::Complete { bytes } => *bytes,
            BodyOutcome::Truncated { bytes, cause } => {
                panic!("expected a complete body, got {bytes} bytes and {cause}")
            }
        }
    }

    fn started_progress() -> Arc<ExchangeProgress> {
        let progress = Arc::new(ExchangeProgress::new());
        assert!(progress.try_start());
        progress
    }

    #[test]
    fn core_exchange_runs_on_caller_owned_tokio_hosted_driver_over_tcp_and_tls() {
        let fixture = tls_fixture();
        let plain = responding("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nplain");
        let secure = TestServer::tls(Arc::clone(&fixture.server), |mut conn| {
            conn.read_request();
            let _ = conn.write(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nsecure");
            conn.sees_close(HANG_GUARD);
        });
        let plain_address = plain.address;
        let secure_address = secure.address;
        let client_config = Arc::clone(&fixture.client);
        let stats = Arc::new(ExplicitStats::new());
        let core_stats = Arc::clone(&stats);
        let progress = started_progress();
        let plain_progress = Arc::clone(&progress);
        let (plain_status, plain_bytes, secure_status, secure_bytes) =
            run_on_tokio_driver(move |context, _| async move {
                let tcp = tokio::net::TcpStream::connect(plain_address)
                    .await
                    .expect("plaintext dial");
                let io =
                    EstablishedIo::new(tcp, endpoint(plain_address), endpoint(plain_address), None);
                let plain = exchange(ExchangeInput {
                    worker: context.clone(),
                    io,
                    security: TransportSecurity::Plaintext,
                    request: request("plain.example", "/plain", HANG_GUARD),
                    cancellation: CancellationToken::never(),
                    progress: plain_progress,
                    started: context.host().now(),
                    stats: Arc::clone(&core_stats),
                })
                .await
                .expect("plaintext exchange");
                let tcp = tokio::net::TcpStream::connect(secure_address)
                    .await
                    .expect("TLS dial");
                let tls = tokio_rustls::TlsConnector::from(client_config)
                    .connect("localhost".try_into().expect("server name"), tcp)
                    .await
                    .expect("TLS handshake");
                let io = EstablishedIo::new(
                    tls,
                    endpoint(secure_address),
                    endpoint(secure_address),
                    None,
                );
                let secure = exchange(ExchangeInput {
                    worker: context.clone(),
                    io,
                    security: TransportSecurity::Tls,
                    request: request("localhost", "/secure", HANG_GUARD),
                    cancellation: CancellationToken::never(),
                    progress: started_progress(),
                    started: context.host().now(),
                    stats: Arc::clone(&core_stats),
                })
                .await
                .expect("TLS exchange");
                (
                    plain.status,
                    complete_bytes(&plain),
                    secure.status,
                    complete_bytes(&secure),
                )
            });
        assert_eq!((plain_status, plain_bytes), (200, 5));
        assert_eq!((secure_status, secure_bytes), (200, 6));
        assert!(progress.reached_session());
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.pending_teardowns, 0);
        assert_eq!(snapshot.unconfirmed_retirements, 0);

        // Shutdown begun before the cleanup reservation: the reservation is rejected.
        let (closed_tx, closed_rx) = mpsc::channel();
        let peer = TestServer::plain(move |mut conn| {
            let _ = closed_tx.send(conn.read_end(HANG_GUARD));
        });
        let address = peer.address;
        let stats = Arc::new(ExplicitStats::new());
        let core_stats = Arc::clone(&stats);
        let progress = started_progress();
        let core_progress = Arc::clone(&progress);
        let error = run_on_tokio_driver(move |context, worker| async move {
            let tcp = tokio::net::TcpStream::connect(address).await.expect("dial");
            let io = EstablishedIo::new(tcp, endpoint(address), endpoint(address), None);
            assert!(worker.begin_shutdown());
            exchange(ExchangeInput {
                worker: context.clone(),
                io,
                security: TransportSecurity::Plaintext,
                request: request("peer.example", "/", HANG_GUARD),
                cancellation: CancellationToken::never(),
                progress: core_progress,
                started: context.host().now(),
                stats: core_stats,
            })
            .await
            .expect_err("cleanup reservation refused")
        });
        assert_eq!(
            (error.phase, error.kind, error.reached_wire),
            (Phase::Connect, ErrorKind::WorkerLost, false)
        );
        assert!(!progress.reached_session());
        assert_eq!(
            closed_rx.recv_timeout(HANG_GUARD).expect("peer result"),
            Some(ReadEnd::Eof)
        );
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.pending_teardowns, 0);
        assert_eq!(snapshot.unconfirmed_retirements, 0);

        // Claimed before the hand-off: the request never reaches the session, and the exchange
        // tears down the connection it adopted and attached.
        let (closed_tx, closed_rx) = mpsc::channel();
        let peer = TestServer::plain(move |mut conn| {
            let _ = closed_tx.send(conn.read_end(HANG_GUARD));
        });
        let address = peer.address;
        let stats = Arc::new(ExplicitStats::new());
        let core_stats = Arc::clone(&stats);
        let progress = started_progress();
        assert!(progress.claim());
        let core_progress = Arc::clone(&progress);
        let error = run_on_tokio_driver(move |context, _| async move {
            let tcp = tokio::net::TcpStream::connect(address).await.expect("dial");
            let io = EstablishedIo::new(tcp, endpoint(address), endpoint(address), None);
            exchange(ExchangeInput {
                worker: context.clone(),
                io,
                security: TransportSecurity::Plaintext,
                request: request("peer.example", "/", HANG_GUARD),
                cancellation: CancellationToken::never(),
                progress: core_progress,
                started: context.host().now(),
                stats: core_stats,
            })
            .await
            .expect_err("claimed exchange")
        });
        assert_eq!(
            (error.phase, error.kind, error.reached_wire),
            (Phase::Read, ErrorKind::Cancelled, false)
        );
        assert!(!progress.reached_session());
        assert!(
            closed_rx
                .recv_timeout(HANG_GUARD)
                .expect("peer result")
                .is_some()
        );
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.pending_teardowns, 0);
        assert_eq!(snapshot.unconfirmed_retirements, 0);

        // Progress that was never started breaks the entry's precondition: the core fails
        // before it adopts the stream, which closes unadopted.
        let (closed_tx, closed_rx) = mpsc::channel();
        let peer = TestServer::plain(move |mut conn| {
            let _ = closed_tx.send(conn.read_end(HANG_GUARD));
        });
        let address = peer.address;
        let progress = Arc::new(ExchangeProgress::new());
        let core_progress = Arc::clone(&progress);
        let error = run_on_tokio_driver(move |context, _| async move {
            let tcp = tokio::net::TcpStream::connect(address).await.expect("dial");
            let io = EstablishedIo::new(tcp, endpoint(address), endpoint(address), None);
            exchange(ExchangeInput {
                worker: context.clone(),
                io,
                security: TransportSecurity::Plaintext,
                request: request("peer.example", "/", HANG_GUARD),
                cancellation: CancellationToken::never(),
                progress: core_progress,
                started: context.host().now(),
                stats: Arc::new(ExplicitStats::new()),
            })
            .await
            .expect_err("progress not started")
        });
        assert_eq!(
            (error.phase, error.kind, error.reached_wire),
            (Phase::Submit, ErrorKind::Other, false),
            "{error}"
        );
        assert!(!progress.reached_session());
        assert_eq!(
            closed_rx.recv_timeout(HANG_GUARD).expect("peer result"),
            Some(ReadEnd::Eof)
        );

        // Shutdown begun between the cleanup reservation and adoption.
        let (closed_tx, closed_rx) = mpsc::channel();
        let peer = TestServer::plain(move |mut conn| {
            let _ = closed_tx.send(conn.read_end(HANG_GUARD));
        });
        let address = peer.address;
        let pause = TestPause::new();
        let hooks = ExchangeHooks {
            before_adoption: Some(pause.clone()),
            ..ExchangeHooks::default()
        };
        let stats = Arc::new(ExplicitStats::new());
        let core_stats = Arc::clone(&stats);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("driver runtime");
        let local = tokio::task::LocalSet::new();
        let outcome = local.block_on(&runtime, async move {
            let (handle, driver) =
                WorkerDriver::with_host(16, roxygen_runtime::host::Host::tokio()).expect("driver");
            let driver = tokio::task::spawn_local(driver);
            let call = handle
                .call_local(move |context| async move {
                    let tcp = tokio::net::TcpStream::connect(address).await.expect("dial");
                    let io = EstablishedIo::new(tcp, endpoint(address), endpoint(address), None);
                    run_exchange(
                        ExchangeInput {
                            worker: context.clone(),
                            io,
                            security: TransportSecurity::Plaintext,
                            request: request("peer.example", "/", HANG_GUARD),
                            cancellation: CancellationToken::never(),
                            progress: started_progress(),
                            started: context.host().now(),
                            stats: core_stats,
                        },
                        &hooks,
                    )
                    .await
                })
                .expect("command admitted");
            pause.reached_async(1).await;
            handle.begin_shutdown();
            pause.release();
            let outcome = tokio::time::timeout(HANG_GUARD, call)
                .await
                .expect("bounded command");
            tokio::time::timeout(HANG_GUARD, driver)
                .await
                .expect("bounded driver shutdown")
                .expect("driver task");
            outcome
        });
        match outcome {
            Err(_dropped_with_command) => {}
            Ok(Err(error)) => {
                assert_eq!(
                    (error.phase, error.kind, error.reached_wire),
                    (Phase::Connect, ErrorKind::WorkerLost, false)
                );
            }
            Ok(Ok(response)) => panic!("exchange succeeded after shutdown: {response:?}"),
        }
        assert_eq!(
            closed_rx.recv_timeout(HANG_GUARD).expect("peer result"),
            Some(ReadEnd::Eof)
        );
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.pending_teardowns, 0);
        assert_eq!(snapshot.unconfirmed_retirements, 0);
    }

    #[test]
    fn core_exchange_runs_on_external_local_host_over_in_memory_stream() {
        let response = run_on_manual_host(
            Duration::from_secs(30),
            ExchangeHooks::default(),
            |mut peer| async move {
                let mut head = Vec::new();
                let mut buffer = [0; 1024];
                while !head.windows(4).any(|window| window == b"\r\n\r\n") {
                    let read = peer.read(&mut buffer).await.expect("request");
                    assert_ne!(read, 0);
                    head.extend_from_slice(&buffer[..read]);
                }
                assert!(head.starts_with(b"GET / HTTP/1.1\r\n"));
                peer.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nlocal")
                    .await
                    .expect("response");
                let _ = peer.read(&mut buffer).await;
            },
            false,
        )
        .expect("scripted exchange");
        assert_eq!(response.status, 200);
        assert_eq!(complete_bytes(&response), 4);

        let error = run_on_manual_host(
            Duration::from_secs(30),
            ExchangeHooks::default(),
            |mut peer| async move {
                let mut buffer = [0; 1024];
                let _ = peer.read(&mut buffer).await;
                let _ = peer.read(&mut buffer).await;
            },
            true,
        )
        .expect_err("stalled peer");
        assert_eq!(error.kind, ErrorKind::Timeout);
        assert_eq!(error.phase, Phase::Read);
        assert_eq!(error.code, None);
    }

    #[test]
    fn request_is_one_http11_get_with_host_and_caller_headers() {
        let (request_tx, request_rx) = mpsc::channel();
        let server = TestServer::plain(move |mut conn| {
            let _ = request_tx.send(conn.read_request());
            let _ = conn.write(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
            conn.sees_close(HANG_GUARD);
        });
        let runtime = start(1, "rxq");
        let mut client = runtime.client(plain_target(server.address));
        let response = block_on(client.send(request_with_headers(
            "origin.example:8443",
            "/path?q=1",
            HANG_GUARD,
            vec![
                ("x-first", "a"),
                ("host", "caller-host.example"),
                ("x-second", " padded\t"),
                ("user-agent", "SyntheticE2E/2.0"),
            ],
        )))
        .expect("exchange");
        assert_eq!(response.status, 200);
        let received = request_rx.recv_timeout(HANG_GUARD).expect("request");
        assert_eq!(received.request_line, "GET /path?q=1 HTTP/1.1");
        assert_eq!(received.values("host"), ["origin.example:8443"]);
        assert_eq!(received.values("x-first"), ["a"]);
        assert_eq!(received.values("x-second"), ["padded"]);
        assert_eq!(received.values("user-agent"), ["SyntheticE2E/2.0"]);
        assert!(received.position("x-first") < received.position("x-second"));
        assert!(received.position("x-second") < received.position("user-agent"));
        assert!(
            received
                .values("connection")
                .iter()
                .all(|value| !value.eq_ignore_ascii_case("close")),
            "{received:?}"
        );
        assert_eq!(server.accepted(), 1);
    }

    #[test]
    fn repeated_request_header_sends_every_value_in_order() {
        let (request_tx, request_rx) = mpsc::channel();
        let server = TestServer::plain(move |mut conn| {
            let _ = request_tx.send(conn.read_request());
            let _ = conn.write(b"HTTP/1.1 206 Partial Content\r\nContent-Length: 0\r\n\r\n");
            conn.sees_close(HANG_GUARD);
        });
        let runtime = start(1, "rxd");
        let mut client = runtime.client(plain_target(server.address));
        let response = block_on(client.send(request_with_headers(
            "localhost",
            "/direct",
            HANG_GUARD,
            vec![("x-request", "first"), ("x-request", "value")],
        )))
        .expect("exchange");
        assert_eq!(response.status, 206);
        let received = request_rx.recv_timeout(HANG_GUARD).expect("request");
        assert_eq!(received.values("x-request"), ["first", "value"]);
    }

    #[test]
    fn content_length_body_counts_exact_bytes() {
        let (end_tx, end_rx) = mpsc::channel();
        let server = TestServer::plain(move |mut conn| {
            conn.read_request();
            let mut response = b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\n".to_vec();
            response.extend(std::iter::repeat_n(b'x', 1000));
            conn.write(&response).expect("response");
            let _ = end_tx.send(conn.read_end(HANG_GUARD));
        });
        let runtime = start(1, "rxl");
        let mut client = runtime.client(plain_target(server.address));
        let response = block_on(client.send(request("localhost", "/", HANG_GUARD))).expect("ok");
        assert_eq!(complete_bytes(&response), 1000);
        assert_eq!(
            end_rx.recv_timeout(HANG_GUARD).expect("server read"),
            Some(ReadEnd::Eof)
        );
        wait_until("transport retirement", || {
            runtime.stats().close_to_retirement_us.count == 1
        });
        let stats = runtime.stats();
        assert_eq!(stats.pending_teardowns, 0);
        assert_eq!(stats.unconfirmed_retirements, 0);
        assert_eq!(stats.truncated_bodies, 0);

        // A body that spans many reads, of the 4_000-byte first allocation and its doubled
        // continuations, and over TLS many 16 KiB records.
        const LARGE: usize = 70_000;
        let fixture = tls_fixture();
        let large_body = |tls| {
            TestServer::bind("127.0.0.1:0", tls, |mut conn| {
                conn.read_request();
                let mut response =
                    format!("HTTP/1.1 200 OK\r\nContent-Length: {LARGE}\r\n\r\n").into_bytes();
                response.extend(std::iter::repeat_n(b'y', LARGE));
                let _ = conn.write(&response);
                conn.sees_close(HANG_GUARD);
            })
        };
        let plain = large_body(None);
        let secure = large_body(Some(Arc::clone(&fixture.server)));
        for target in [
            plain_target(plain.address),
            tls_target(secure.address, &fixture.client, "localhost"),
        ] {
            let mut client = runtime.client(target);
            let response =
                block_on(client.send(request("localhost", "/", HANG_GUARD))).expect("ok");
            assert_eq!(complete_bytes(&response), LARGE as u64);
        }
        assert_eq!(runtime.stats().truncated_bodies, 0);
    }

    #[test]
    fn chunked_body_counts_exact_bytes() {
        let plain_chunks = responding(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\
             5\r\nhello\r\n6;ext=1\r\n world\r\n0\r\n\r\n",
        );
        let with_trailers = responding(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\
             3\r\nabc\r\n0\r\nX-Trailer: ignored\r\n\r\n",
        );
        let runtime = start(1, "rxc");
        let mut client = runtime.client(plain_target(plain_chunks.address));
        let response = block_on(client.send(request("localhost", "/", HANG_GUARD))).expect("ok");
        assert_eq!(complete_bytes(&response), 11);
        let mut client = runtime.client(plain_target(with_trailers.address));
        let response = block_on(client.send(request("localhost", "/", HANG_GUARD))).expect("ok");
        assert_eq!(complete_bytes(&response), 3);
        assert!(response.headers.get("x-trailer").is_none());
        wait_until("both transports retire", || {
            let stats = runtime.stats();
            stats.pending_teardowns == 0 && stats.close_to_retirement_us.count == 2
        });
    }

    #[test]
    fn close_delimited_body_counts_exact_bytes() {
        let server = TestServer::plain(|mut conn| {
            conn.read_request();
            let mut response = b"HTTP/1.1 200 OK\r\n\r\n".to_vec();
            response.extend(std::iter::repeat_n(b'c', 777));
            let _ = conn.write(&response);
        });
        let runtime = start(1, "rxe");
        let mut client = runtime.client(plain_target(server.address));
        let response = block_on(client.send(request("localhost", "/", HANG_GUARD))).expect("ok");
        assert_eq!(complete_bytes(&response), 777);
    }

    /// Heads whose status line parses but which the codec rejects, with the code each fails with.
    const MALFORMED_HEADS: [(&str, &[u8], HttpErrorCode); 4] = [
        (
            "/bad-header",
            b"HTTP/1.1 200 OK\r\nX-Good: 1\r\nno colon\r\nContent-Length: 0\r\n\r\n",
            HttpErrorCode::HEADER_PARSE_ERROR,
        ),
        (
            "/conflicting-length",
            b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nx",
            HttpErrorCode::HEADER_PARSE_ERROR,
        ),
        (
            "/bad-transfer-encoding",
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\n\r\n",
            HttpErrorCode::HEADER_PARSE_ERROR,
        ),
        (
            "/bad-value",
            b"HTTP/1.1 200 OK\r\nX-Bad: a\x7f\r\nContent-Length: 0\r\n\r\n",
            HttpErrorCode::PROTOCOL_ERROR,
        ),
    ];

    /// Response bytes that end, at the peer's close, before any final head is complete.
    const INCOMPLETE_HEADS: [(&str, &[u8]); 4] = [
        ("/eof-in-status-line", b"H"),
        ("/eof-after-status-line", b"HTTP/1.1 200 OK\r\n"),
        ("/eof-in-headers", b"HTTP/1.1 200 OK\r\nX: y\r\n"),
        (
            "/eof-after-informational",
            b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\n",
        ),
    ];

    #[test]
    fn error_after_final_head_is_truncated_outcome() {
        let (closed_tx, closed_rx) = mpsc::channel();
        let (trailer_tx, trailer_rx) = mpsc::channel();
        let counted = Arc::new(AtomicU64::new(0));
        let body_counted = Arc::clone(&counted);
        let server = TestServer::plain(move |mut conn| {
            let request = conn.read_request();
            let mut short = b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\n".to_vec();
            let is = |path: &str| request.request_line == format!("GET {path} HTTP/1.1");
            let malformed = MALFORMED_HEADS.iter().find(|(path, _, _)| is(path));
            let incomplete = INCOMPLETE_HEADS.iter().find(|(path, _)| is(path));
            if let Some((_, head, _)) = malformed {
                let _ = conn.write(head);
                conn.sees_close(HANG_GUARD);
            } else if let Some((_, partial)) = incomplete {
                let _ = conn.write(partial);
            } else if is("/mid-body-error") {
                // The close waits until the client has counted the body sent before it.
                let _ = conn.write(&short);
                for _ in 0..5 {
                    let _ = conn.write(&[b'a'; 100]);
                }
                wait_until("the client to count the body", || {
                    body_counted.load(Ordering::Acquire) == 500
                });
            } else if is("/single-write") {
                // The head, part of the body, and the close in one write.
                short.extend([b'a'; 500]);
                let _ = conn.write(&short);
                drop(conn);
                let _ = closed_tx.send(());
            } else if is("/bad-trailer") {
                // A valid chunked head and a trailer value with a DEL byte in one write.
                let _ = conn.write(
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nX-Bad: a\x7f\r\n\r\n",
                );
                let _ = trailer_tx.send(());
                conn.sees_close(HANG_GUARD);
            } else {
                let _ = conn.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
                conn.sees_close(HANG_GUARD);
            }
        });
        let truncated = |response: &Response, read: Option<u64>| match &response.body {
            BodyOutcome::Truncated { bytes, cause } => {
                assert!(read.is_none_or(|read| *bytes == read), "{bytes} bytes");
                assert!(*bytes <= 500, "{bytes} bytes");
                assert_eq!(cause.phase, Phase::Body);
                assert!(cause.reached_wire);
                assert!(!cause.detail.is_empty());
                assert_eq!(*cause.timings, response.timings);
                assert!(cause.timings.tcp_connect.is_some());
                assert!(cause.timings.first_byte.is_some());
                cause.code
            }
            BodyOutcome::Complete { bytes } => panic!("truncated body reported complete: {bytes}"),
        };
        let runtime = start(1, "rxt");
        let mut client = runtime.client(plain_target(server.address));
        runtime.set_exchange_hooks(ExchangeHooks {
            body_counted: Some(counted),
            ..ExchangeHooks::default()
        });
        let response =
            block_on(client.send(request("localhost", "/mid-body-error", HANG_GUARD))).expect("ok");
        runtime.set_exchange_hooks(ExchangeHooks::default());
        assert_eq!(response.status, 200);
        truncated(&response, Some(500));
        assert_eq!(runtime.stats().truncated_bodies, 1);
        let next = block_on(client.send(request("localhost", "/", HANG_GUARD))).expect("next");
        assert_eq!(complete_bytes(&next), 2);

        // Unpaced, however the client's reads split the response.
        let response =
            block_on(client.send(request("localhost", "/single-write", HANG_GUARD))).expect("ok");
        assert_eq!(response.status, 200);
        truncated(&response, None);
        closed_rx.recv_timeout(HANG_GUARD).expect("server closed");

        // A head that the codec rejects is not a final head, though its status line parsed.
        for (path, _, code) in MALFORMED_HEADS {
            let error = block_on(client.send(request("localhost", path, HANG_GUARD)))
                .expect_err("malformed head");
            assert_eq!(
                (error.phase, error.code, error.reached_wire),
                (Phase::Read, Some(code.as_u16()), true),
                "{path}: {error}"
            );
            assert_eq!(error.timings.first_byte, None, "{path}");
        }

        // Nor is a head that the close cuts short.
        for (path, _) in INCOMPLETE_HEADS {
            let error = block_on(client.send(request("localhost", path, HANG_GUARD)))
                .expect_err("incomplete head");
            assert_eq!(
                (error.phase, error.kind, error.code, error.reached_wire),
                (
                    Phase::Read,
                    ErrorKind::RemoteClosed,
                    Some(HttpErrorCode::TRANSPORT_EOF.as_u16()),
                    true
                ),
                "{path}: {error}"
            );
        }
        assert_eq!(runtime.stats().truncated_bodies, 2);

        // Held before its first head read until the server's write has arrived, so the session
        // ends the stream with the final head still undelivered.
        let send_held = |mut client: ExplicitClient, path: &'static str, written: Receiver<()>| {
            let pause = TestPause::new();
            runtime.set_exchange_hooks(ExchangeHooks {
                before_head: Some(pause.clone()),
                ..ExchangeHooks::default()
            });
            block_on(async {
                let pending = tokio::spawn(async move {
                    let result = client.send(request("localhost", path, HANG_GUARD)).await;
                    (client, result)
                });
                pause.reached_async(1).await;
                tokio::task::spawn_blocking(move || written.recv_timeout(HANG_GUARD))
                    .await
                    .expect("blocking wait")
                    .expect("server wrote");
                tokio::time::sleep(Duration::from_millis(100)).await;
                pause.release();
                pending.await.expect("send task")
            })
        };
        let (client, response) = send_held(client, "/single-write", closed_rx);
        let response = response.expect("final head arrived");
        assert_eq!(response.status, 200);
        assert_eq!(
            response
                .headers
                .get("content-length")
                .expect("final head headers"),
            "1000"
        );
        assert!(response.timings.first_byte.is_some());
        assert_eq!(response.timings.last_byte, None);
        truncated(&response, None);
        assert_eq!(runtime.stats().truncated_bodies, 3);

        // Uniplex's HTTP/1.1 codec does not validate strictly, so a trailer that fails
        // validation fails the stream as a body error, and the head that came with it stands.
        let (_, response) = send_held(client, "/bad-trailer", trailer_rx);
        let response = response.expect("final head arrived");
        assert_eq!(response.status, 200);
        assert_eq!(
            truncated(&response, Some(0)),
            Some(HttpErrorCode::BODY_PARSE_ERROR.as_u16())
        );
        assert_eq!(runtime.stats().truncated_bodies, 4);
        wait_until("every transport retires", || {
            runtime.stats().pending_teardowns == 0
        });
    }

    #[test]
    fn informational_heads_are_skipped_before_final_head() {
        let server = responding(
            "HTTP/1.1 100 Continue\r\n\r\n\
             HTTP/1.1 103 Early Hints\r\nLink: </style.css>; rel=preload\r\n\r\n\
             HTTP/1.1 200 OK\r\nContent-Length: 2\r\nX-Final: yes\r\n\r\nok",
        );
        let runtime = start(1, "rxi");
        let mut client = runtime.client(plain_target(server.address));
        let response = block_on(client.send(request("localhost", "/", HANG_GUARD))).expect("ok");
        assert_eq!(response.status, 200);
        assert_eq!(complete_bytes(&response), 2);
        assert_eq!(
            response.headers.get("x-final").expect("final header"),
            "yes"
        );
        assert!(response.headers.get("link").is_none());

        // An informational head is the first byte even when no final head follows.
        let stalled = TestServer::plain(|mut conn| {
            conn.read_request();
            let _ = conn.write(b"HTTP/1.1 103 Early Hints\r\nLink: </a.css>\r\n\r\n");
            conn.sees_close(HANG_GUARD);
        });
        let before = runtime.stats().first_byte_us.count;
        let mut client = runtime.client(plain_target(stalled.address));
        let error = block_on(client.send(request("localhost", "/", EXPIRY_BUDGET)))
            .expect_err("no final head");
        assert_eq!((error.phase, error.kind), (Phase::Read, ErrorKind::Timeout));
        let first_byte = error.timings.first_byte.expect("informational head timed");
        assert!(error.timings.tcp_connect.expect("connect timed") <= first_byte);
        assert_eq!(error.timings.last_byte, None);
        assert_eq!(runtime.stats().first_byte_us.count, before + 1);
    }

    #[test]
    fn no_content_response_completes_with_zero_bytes() {
        let server = responding("HTTP/1.1 204 No Content\r\nX-Empty: 1\r\n\r\n");
        let runtime = start(1, "rxn");
        let mut client = runtime.client(plain_target(server.address));
        let response = block_on(client.send(request("localhost", "/", HANG_GUARD))).expect("ok");
        assert_eq!(response.status, 204);
        assert_eq!(complete_bytes(&response), 0);
    }

    #[test]
    fn unrepresentable_response_header_is_dropped_and_counted() {
        let server = responding(
            b"HTTP/1.1 200 OK\r\nX-Kept: yes\r\nX-Unrepresentable: a\\\r\\b\r\nContent-Length: 0\r\n\r\n"
                .to_vec(),
        );
        let runtime = start(1, "rxh");
        let mut client = runtime.client(plain_target(server.address));
        let response = block_on(client.send(request("localhost", "/", HANG_GUARD))).expect("ok");
        assert_eq!(response.status, 200);
        assert_eq!(response.dropped_headers, 1);
        assert_eq!(response.headers.get("x-kept").expect("kept header"), "yes");
        assert!(response.headers.get("x-unrepresentable").is_none());
        assert_eq!(runtime.stats().dropped_response_headers, 1);

        // More headers than `HeaderMap` can index, within the codec's head limit, using bare-LF
        // lines and empty values.
        let mut crowded = b"HTTP/1.1 200 OK\n".to_vec();
        (0..25_000).for_each(|_| crowded.extend_from_slice(b"a:\n"));
        crowded.extend_from_slice(b"Content-Length: 0\n\n");
        let server = responding(crowded);
        let mut client = runtime.client(plain_target(server.address));
        let response = block_on(client.send(request("localhost", "/", HANG_GUARD))).expect("ok");
        assert_eq!(response.status, 200);
        assert_eq!(
            response.headers.get_all("a").iter().count() + response.dropped_headers,
            25_000
        );
        assert_eq!(runtime.stats().panics, 0);
    }

    /// A raw-headers client does not build a `HeaderMap`, and its `RawHeaders` lookups return what
    /// a default client's map holds: the same first values, drops, and counted drops.
    #[test]
    fn raw_headers_read_as_the_default_client_s_map() {
        let mut crowded = b"HTTP/1.1 200 OK\n".to_vec();
        (0..25_000).for_each(|_| crowded.extend_from_slice(b"a:\n"));
        crowded.extend_from_slice(b"Content-Length: 0\n\n");
        let heads = [
            b"HTTP/1.1 200 OK\r\nX-Kept: yes\r\nX-Unrepresentable: a\\\r\\b\r\n\
                x-unrepresentable: after\r\nX-Dup: first\r\nx-dup: second\r\n\
                X-Obs: caf\xe9\r\nLocation: /elsewhere\r\nContent-Length: 0\r\n\r\n"
                .to_vec(),
            crowded,
        ];
        let names = [
            "x-kept",
            "X-KEPT",
            "x-unrepresentable",
            "x-dup",
            "x-obs",
            "location",
            "content-length",
            "a",
            "absent",
            "bad name",
        ];
        let runtime = start(1, "rxr");
        heads.into_iter().for_each(|head| {
            let send = |raw: bool| {
                let server = responding(head.clone());
                let client = runtime.client(plain_target(server.address));
                let mut client = if raw {
                    client.with_raw_headers()
                } else {
                    client
                };
                block_on(client.send(request("localhost", "/", HANG_GUARD))).expect("ok")
            };
            let mapped = send(false);
            let dropped_before = runtime.stats().dropped_response_headers;
            let raw = send(true);
            assert!(mapped.raw_headers.is_none());
            assert!(raw.headers.is_empty(), "{:?}", raw.headers);
            assert_eq!(raw.dropped_headers, mapped.dropped_headers);
            assert_eq!(
                runtime.stats().dropped_response_headers - dropped_before,
                u64::try_from(raw.dropped_headers).unwrap()
            );
            let headers = raw.raw_headers.as_ref().expect("raw headers");
            names.iter().for_each(|name| {
                let expected = mapped.headers.get(*name);
                assert_eq!(
                    headers.get(name),
                    expected.map(HeaderValue::as_bytes),
                    "{name}"
                );
                assert_eq!(
                    headers.get_str(name),
                    expected.and_then(|value| value.to_str().ok()),
                    "{name}"
                );
                assert_eq!(
                    headers.contains_key(name),
                    mapped.headers.contains_key(*name),
                    "{name}"
                );
            });
        });
        let first = block_on(async {
            let server = responding(
                b"HTTP/1.1 200 OK\r\nX-Unrepresentable: a\\\r\\b\r\nx-unrepresentable: after\r\n\
                    Content-Length: 0\r\n\r\n"
                    .to_vec(),
            );
            let mut client = runtime
                .client(plain_target(server.address))
                .with_raw_headers();
            client.send(request("localhost", "/", HANG_GUARD)).await
        })
        .expect("ok");
        // The map drops the first value, so the second is the first it holds.
        assert_eq!(
            first
                .raw_headers
                .as_ref()
                .and_then(|headers| headers.get_str("x-unrepresentable")),
            Some("after")
        );
        assert_eq!(first.dropped_headers, 1);
    }

    #[test]
    fn response_keeps_status_lowercase_names_and_duplicate_values_in_order() {
        let mut response = b"HTTP/1.1 206 Partial Content\r\nX-Duplicate: first\r\n\
            x-duplicate: second\r\nContent-Length: 5\r\n\r\nbin\0"
            .to_vec();
        response.push(0xff);
        let server = responding(response);
        let runtime = start(1, "rxp");
        let mut client = runtime.client(plain_target(server.address));
        let response =
            block_on(client.send(request("localhost", "/direct", HANG_GUARD))).expect("exchange");
        assert_eq!(response.status, 206);
        assert_eq!(complete_bytes(&response), 5);
        for name in response.headers.keys() {
            assert_eq!(name.as_str(), name.as_str().to_ascii_lowercase());
        }
        let duplicates: Vec<_> = response
            .headers
            .get_all("x-duplicate")
            .iter()
            .map(|value| value.to_str().expect("ASCII value"))
            .collect();
        assert_eq!(duplicates, ["first", "second"]);
    }
}
