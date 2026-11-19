/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Tests for `ExplicitRuntime` slots, restarts, fail-open state, TLS ownership, and Send + Sync.

mod runtime {
    use std::cell::RefCell;
    use std::collections::BTreeSet;
    use std::future::Future;
    use std::io::Read;
    use std::net::IpAddr;
    use std::net::Ipv4Addr;
    use std::net::SocketAddr;
    use std::net::SocketAddrV6;
    use std::net::TcpListener;
    use std::net::TcpStream;
    use std::num::NonZeroUsize;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::pin::Pin;
    use std::pin::pin;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::RawWaker;
    use std::task::RawWakerVTable;
    use std::task::Wake;
    use std::task::Waker;
    use std::thread::JoinHandle;
    use std::time::Duration;
    use std::time::Instant;

    use http::HeaderMap;
    use http::HeaderName;
    use http::HeaderValue;
    use http::header;
    use roxygen_core::connection::TransportEndpoint;
    use roxygen_core::headers::HttpCommonHeaderTable;
    use roxygen_core::headers::HttpCommonHeaders;
    use roxygen_core::headers::HttpHeaderCode;
    use roxygen_core::headers::HttpHeaders;
    use roxygen_core::message::SocketAddress;
    use roxygen_runtime::worker::WorkerState;
    use rustls::pki_types::ServerName;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    use crate::error::ErrorKind;
    use crate::error::ExplicitError;
    use crate::error::Phase;
    use crate::exchange::AttachRecord;
    use crate::exchange::BodyOutcome;
    use crate::exchange::ExchangeHooks;
    use crate::exchange::ExchangeProbe;
    use crate::exchange::RAW_HEADER_LIMIT;
    use crate::exchange::RawHeaders;
    use crate::exchange::Request;
    use crate::exchange::Response;
    use crate::exchange::TestPause;
    use crate::exchange::convert_headers;
    use crate::exchange::name_parses;
    use crate::exchange_tests::support::EXPIRY_BUDGET;
    use crate::exchange_tests::support::HANG_GUARD;
    use crate::exchange_tests::support::RawRequest;
    use crate::exchange_tests::support::ReadEnd;
    use crate::exchange_tests::support::ReleaseOnDrop;
    use crate::exchange_tests::support::TestServer;
    use crate::exchange_tests::support::TlsFixture;
    use crate::exchange_tests::support::block_on;
    use crate::exchange_tests::support::config;
    use crate::exchange_tests::support::eventually;
    use crate::exchange_tests::support::live_threads;
    use crate::exchange_tests::support::plain_target;
    use crate::exchange_tests::support::refusing_address;
    use crate::exchange_tests::support::request;
    use crate::exchange_tests::support::request_with_headers;
    use crate::exchange_tests::support::responding;
    use crate::exchange_tests::support::run_on_manual_host;
    use crate::exchange_tests::support::start;
    use crate::exchange_tests::support::tls_fixture;
    use crate::exchange_tests::support::tls_target;
    use crate::exchange_tests::support::wait_until;
    use crate::runtime::ExplicitClient;
    use crate::runtime::ExplicitRuntime;
    use crate::runtime::ExplicitRuntimeConfig;
    use crate::runtime::RuntimeHooks;
    use crate::runtime::Security;
    use crate::runtime::SlotHealth;
    use crate::runtime::SlotState;
    use crate::runtime::Target;
    use crate::runtime::dialed_endpoint;
    use crate::stats::ExplicitStats;
    use crate::stats::TeardownGuard;

    const OK: &str = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";

    fn ok_server() -> TestServer {
        responding(OK)
    }

    fn force_close(runtime: &ExplicitRuntime, slot: usize) {
        runtime
            .worker_handle(slot)
            .expect("published worker")
            .begin_shutdown();
    }

    /// Keeps the slot's current worker from finishing shutdown until `barrier` is released.
    fn hold_cleanup(runtime: &ExplicitRuntime, slot: usize, barrier: &TestPause) {
        let barrier = barrier.clone();
        let call = runtime
            .worker_handle(slot)
            .expect("published worker")
            .call(move |context| {
                context
                    .register_cleanup_task(async move { barrier.pause().await })
                    .expect("cleanup reserved")
                    .activate()
                    .expect("cleanup activated")
                    .detach();
            })
            .expect("command admitted");
        block_on(call).expect("cleanup registered");
    }

    /// Blocks the slot's worker thread until the returned sender is dropped.
    fn hold_worker(runtime: &ExplicitRuntime, slot: usize) -> mpsc::Sender<()> {
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let (started_tx, started_rx) = mpsc::channel();
        runtime
            .worker_handle(slot)
            .expect("published worker")
            .submit(move |_| {
                let _ = started_tx.send(());
                let _ = release_rx.recv_timeout(HANG_GUARD);
            })
            .expect("hold admitted");
        started_rx.recv_timeout(HANG_GUARD).expect("hold started");
        release_tx
    }

    fn wait_for_state(client: &ExplicitClient, state: SlotState) {
        wait_until("slot state", || client.slot_health().state == state);
    }

    fn ok_request() -> Request {
        request("localhost", "/", HANG_GUARD)
    }

    fn assert_ok(result: Result<Response, ExplicitError>) {
        let response = result.expect("exchange succeeds");
        assert!(matches!(response.body, BodyOutcome::Complete { .. }));
    }

    fn assert_worker_lost(result: Result<Response, ExplicitError>, reached_wire: bool) {
        let error = result.expect_err("worker lost");
        assert_eq!(error.kind, ErrorKind::WorkerLost, "{error}");
        assert_eq!(error.reached_wire, reached_wire, "{error}");
    }

    fn recording_server() -> (TestServer, mpsc::Receiver<RawRequest>) {
        let (request_tx, request_rx) = mpsc::channel();
        let server = TestServer::plain(move |mut conn| {
            let _ = request_tx.send(conn.read_request());
            let _ = conn.write(OK.as_bytes());
            conn.sees_close(HANG_GUARD);
        });
        (server, request_rx)
    }

    /// A server that signals once it has read a request and responds only on release.
    fn parking_server() -> (TestServer, mpsc::Receiver<()>, Arc<TestPause>) {
        let (read_tx, read_rx) = mpsc::channel();
        let release = Arc::new(TestPause::new());
        let gate = Arc::clone(&release);
        let mut server = TestServer::plain(move |mut conn| {
            conn.read_request();
            let _ = read_tx.send(());
            gate.pause_blocking();
            let _ = conn.write(OK.as_bytes());
            conn.sees_close(HANG_GUARD);
        });
        server.release_on_drop(TestPause::clone(&release));
        (server, read_rx, release)
    }

    /// A waker that, when woken, polls its client's slot health from another thread and records
    /// whether that poll had to wait for the slot lock.
    struct LockProbe {
        client: ExplicitClient,
        wakes: AtomicUsize,
        blocked: AtomicBool,
        polls: Mutex<Vec<JoinHandle<()>>>,
    }

    impl LockProbe {
        fn new(client: ExplicitClient) -> Arc<Self> {
            Arc::new(Self {
                client,
                wakes: AtomicUsize::new(0),
                blocked: AtomicBool::new(false),
                polls: Mutex::new(Vec::new()),
            })
        }

        /// Waits for a finished wake, joins its health poll, and reports whether the poll was
        /// blocked.
        fn woken_under_lock(&self) -> bool {
            wait_until("probe woken", || self.wakes.load(Ordering::SeqCst) > 0);
            std::mem::take(&mut *self.polls.lock().unwrap())
                .into_iter()
                .for_each(|poll| poll.join().expect("health poll"));
            self.blocked.load(Ordering::SeqCst)
        }
    }

    impl Wake for LockProbe {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            let (done_tx, done_rx) = mpsc::channel();
            let probe = Arc::clone(self);
            let poll = std::thread::spawn(move || {
                let _ = probe.client.slot_health();
                let _ = done_tx.send(());
            });
            if done_rx.recv_timeout(Duration::from_millis(500)).is_err() {
                self.blocked.store(true, Ordering::SeqCst);
            }
            self.polls.lock().unwrap().push(poll);
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Polls `send` once inside `caller`'s context with `probe` as its waker.
    fn poll_with_probe<Work: Future>(
        caller: &tokio::runtime::Runtime,
        send: Pin<&mut Work>,
        probe: &Arc<LockProbe>,
    ) -> Poll<Work::Output> {
        let _context = caller.enter();
        let waker = Waker::from(Arc::clone(probe));
        send.poll(&mut Context::from_waker(&waker))
    }

    const WAKER_PANIC: &str = "a caller's waker panicked";

    /// A panic payload whose destructor panics.
    struct PanickingPayload;

    impl Drop for PanickingPayload {
        fn drop(&mut self) {
            std::panic::panic_any("a waker's panic payload panicked when dropped");
        }
    }

    /// A caller's waker that panics when woken, with `WAKER_PANIC` or with a `PanickingPayload`.
    enum PanickingWaker {
        Message,
        PanickingPayload,
    }

    impl Wake for PanickingWaker {
        fn wake(self: Arc<Self>) {
            match *self {
                Self::Message => std::panic::panic_any(WAKER_PANIC),
                Self::PanickingPayload => std::panic::panic_any(PanickingPayload),
            }
        }
    }

    /// A panic payload that reports its drop and then, if `panics`, panics as well.
    struct ReportingPayload {
        dropped: mpsc::Sender<()>,
        panics: bool,
    }

    impl Drop for ReportingPayload {
        fn drop(&mut self) {
            let _ = self.dropped.send(());
            if self.panics {
                std::panic::panic_any("a waker's panic payload panicked when dropped");
            }
        }
    }

    /// A caller's waker that panics with a `ReportingPayload` when woken.
    struct ReportingPanickingWaker {
        dropped: mpsc::Sender<()>,
        panics: bool,
    }

    impl Wake for ReportingPanickingWaker {
        fn wake(self: Arc<Self>) {
            std::panic::panic_any(ReportingPayload {
                dropped: self.dropped.clone(),
                panics: self.panics,
            });
        }
    }

    /// A caller's waker that records that it was woken.
    #[derive(Default)]
    struct RecordingWaker(AtomicBool);

    impl RecordingWaker {
        fn woken(&self) -> bool {
            self.0.load(Ordering::SeqCst)
        }
    }

    impl Wake for RecordingWaker {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    /// Polls `send` once inside `caller`'s context with `waker`.
    fn poll_with_waker<Work: Future>(
        caller: &tokio::runtime::Runtime,
        send: Pin<&mut Work>,
        waker: Waker,
    ) -> Poll<Work::Output> {
        let _context = caller.enter();
        send.poll(&mut Context::from_waker(&waker))
    }

    /// Polls `send` once inside `caller`'s context with a waker that panics when woken.
    fn poll_with_panicking_waker<Work: Future>(
        caller: &tokio::runtime::Runtime,
        send: Pin<&mut Work>,
        waker: PanickingWaker,
    ) -> Poll<Work::Output> {
        poll_with_waker(caller, send, Waker::from(Arc::new(waker)))
    }

    /// Polls each send once inside `caller`'s context with its own `RecordingWaker`, and
    /// returns the wakers.
    fn poll_with_recording_wakers<Work: Future>(
        caller: &tokio::runtime::Runtime,
        sends: &mut [Pin<Box<Work>>],
    ) -> Vec<Arc<RecordingWaker>> {
        sends
            .iter_mut()
            .map(|send| {
                let waker = Arc::new(RecordingWaker::default());
                let poll = poll_with_waker(caller, send.as_mut(), Waker::from(Arc::clone(&waker)));
                assert!(poll.is_pending(), "the send waits for the restart");
                waker
            })
            .collect()
    }

    fn current_thread_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("caller runtime")
    }

    /// A caller's waker that owns a runtime clone and drops it the first time it is woken, and
    /// then panics with a `PanickingPayload` if `panic_after` is set.
    struct DroppingWaker {
        runtime: Mutex<Option<ExplicitRuntime>>,
        prefix: &'static str,
        panic_after: bool,
        /// The thread that ran the drop, and how many threads named by `prefix` were alive once
        /// it returned.
        dropped: Mutex<Option<(String, usize)>>,
    }

    impl DroppingWaker {
        fn new(runtime: ExplicitRuntime, prefix: &'static str, panic_after: bool) -> Arc<Self> {
            Arc::new(Self {
                runtime: Mutex::new(Some(runtime)),
                prefix,
                panic_after,
                dropped: Mutex::new(None),
            })
        }

        /// Waits for the drop to return and reports what the waker recorded.
        fn dropped(&self) -> (String, usize) {
            wait_until("the waker's runtime drop to return", || {
                self.dropped.lock().unwrap().is_some()
            });
            self.dropped.lock().unwrap().clone().expect("recorded drop")
        }
    }

    impl Wake for DroppingWaker {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            let Some(runtime) = self.runtime.lock().unwrap().take() else {
                return;
            };
            drop(runtime);
            let thread = std::thread::current().name().unwrap_or_default().to_owned();
            *self.dropped.lock().unwrap() = Some((thread, live_threads(self.prefix)));
            if self.panic_after {
                std::panic::panic_any(PanickingPayload);
            }
        }
    }

    /// A caller's waker that owns a runtime clone and drops it only when the waker itself is
    /// dropped, recording the thread that ran that drop and how many threads named by `prefix`
    /// were alive once it returned.
    struct OwningWaker {
        runtime: Option<ExplicitRuntime>,
        prefix: &'static str,
        dropped: Arc<Mutex<Option<(String, usize)>>>,
    }

    #[expect(
        clippy::manual_noop_waker,
        reason = "the test checks where this waker is dropped, which `Waker::noop()` cannot report"
    )]
    impl Wake for OwningWaker {
        fn wake(self: Arc<Self>) {}
    }

    impl Drop for OwningWaker {
        fn drop(&mut self) {
            drop(self.runtime.take());
            let thread = std::thread::current().name().unwrap_or_default().to_owned();
            *self.dropped.lock().unwrap() = Some((thread, live_threads(self.prefix)));
        }
    }

    fn waiting_request() -> Request {
        request("localhost", "/", Duration::from_secs(60))
    }

    #[test]
    fn each_send_uses_a_new_connection() {
        let server = ok_server();
        let runtime = start(1, "rnc");
        let mut client = runtime.client(plain_target(server.address));
        assert_ok(block_on(client.send(ok_request())));
        assert_ok(block_on(client.send(ok_request())));
        let ports = server.peer_ports();
        assert_eq!(ports.len(), 2);
        assert_ne!(ports[0], ports[1]);
    }

    fn sends_without_dns(
        server: TestServer,
        requests: mpsc::Receiver<RawRequest>,
        prefix: &'static str,
    ) {
        let runtime = start(1, prefix);
        let mut client = runtime.client(plain_target(server.address));
        assert_ok(block_on(client.send(request(
            "does-not-exist.invalid",
            "/",
            HANG_GUARD,
        ))));
        let received = requests.recv_timeout(HANG_GUARD).expect("request");
        assert_eq!(received.values("host"), ["does-not-exist.invalid"]);
        assert_eq!(server.accepted(), 1);
    }

    #[test]
    fn numeric_target_with_unresolvable_authority_sends_without_dns() {
        let (server, requests) = recording_server();
        sends_without_dns(server, requests, "rnd");
    }

    #[test]
    fn ipv6_target_with_unresolvable_authority_sends_without_dns() {
        let (request_tx, requests) = mpsc::channel();
        let server = TestServer::bind("[::1]:0", None, move |mut conn| {
            let _ = request_tx.send(conn.read_request());
            let _ = conn.write(OK.as_bytes());
            conn.sees_close(HANG_GUARD);
        });
        assert!(server.address.is_ipv6());
        sends_without_dns(server, requests, "rn6");
    }

    #[test]
    fn tls_config_arc_is_shared_across_100_sends() {
        let fixture = tls_fixture();
        let server = TestServer::tls(Arc::clone(&fixture.server), |mut conn| {
            conn.read_request();
            let _ = conn.write(OK.as_bytes());
            conn.sees_close(HANG_GUARD);
        });
        let config = Arc::clone(&fixture.client);
        let target = tls_target(server.address, &config, "localhost");
        let baseline = Arc::strong_count(&config);
        let runtime = start(1, "rta");
        let mut client = runtime.client(target);
        let caller = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("caller runtime");
        for _ in 0..100 {
            assert_ok(caller.block_on(client.send(ok_request())));
            wait_until("transport retirement", || {
                runtime.stats().pending_teardowns == 0
            });
            assert_eq!(Arc::strong_count(&config), baseline);
        }
        let dialed = runtime.hooks().dialed_tls_configs.lock().unwrap().clone();
        assert_eq!(dialed.len(), 100);
        assert!(
            dialed
                .iter()
                .all(|pointer| *pointer == Arc::as_ptr(&config) as usize)
        );
    }

    #[test]
    fn tls_without_alpn_speaks_http11() {
        let fixture = tls_fixture();
        let (request_tx, requests) = mpsc::channel();
        let server = TestServer::tls(Arc::clone(&fixture.server), move |mut conn| {
            let _ = request_tx.send(conn.read_request());
            let _ = conn.write(OK.as_bytes());
            conn.sees_close(HANG_GUARD);
        });
        let runtime = start(1, "rth");
        let mut client = runtime.client(tls_target(server.address, &fixture.client, "localhost"));
        assert_ok(block_on(client.send(ok_request())));
        assert_eq!(*fixture.alpn_offers.lock().unwrap(), [None]);
        let received = requests.recv_timeout(HANG_GUARD).expect("request");
        assert!(received.request_line.ends_with(" HTTP/1.1"));
    }

    #[test]
    fn timings_and_connection_info_describe_the_exchange() {
        let fixture = tls_fixture();
        let secure = TestServer::tls(Arc::clone(&fixture.server), |mut conn| {
            conn.read_request();
            let _ = conn.write(OK.as_bytes());
            conn.sees_close(HANG_GUARD);
        });
        let plain = ok_server();
        let runtime = start(1, "rti");

        let mut client = runtime.client(tls_target(secure.address, &fixture.client, "localhost"));
        let response = block_on(client.send(ok_request())).expect("TLS exchange");
        let timings = &response.timings;
        let tcp_connect = timings.tcp_connect.expect("tcp_connect");
        let tls_handshake = timings.tls_handshake.expect("tls_handshake");
        let first_byte = timings.first_byte.expect("first_byte");
        let last_byte = timings.last_byte.expect("last_byte");
        assert!(tcp_connect <= tls_handshake, "{timings:?}");
        assert!(tls_handshake <= first_byte, "{timings:?}");
        assert!(first_byte <= last_byte, "{timings:?}");
        assert_eq!(response.connection.peer, Some(secure.address));
        let local = response.connection.local.expect("local address");
        assert_eq!(local.ip(), "127.0.0.1".parse::<IpAddr>().unwrap());
        assert!(response.connection.tls_version.is_some());

        let mut client = runtime.client(plain_target(plain.address));
        let response = block_on(client.send(ok_request())).expect("plaintext exchange");
        let timings = &response.timings;
        assert_eq!(timings.tls_handshake, None);
        assert!(
            timings.tcp_connect.expect("tcp_connect") <= timings.first_byte.expect("first_byte")
        );
        assert!(timings.first_byte <= timings.last_byte);
        assert_eq!(response.connection.peer, Some(plain.address));
        assert_eq!(response.connection.tls_version, None);
    }

    #[test]
    fn ipv6_literal_target_builds_and_sends_over_tls() {
        let fixture = tls_fixture();
        for host in ["[::1]", "::1", "127.0.0.1"] {
            match Security::tls(Arc::clone(&fixture.client), host).expect("IP literal") {
                Security::Tls {
                    server_name: ServerName::IpAddress(_),
                    ..
                } => {}
                _ => panic!("{host} did not give an IP-address server name"),
            }
        }
        // Brackets enclose only an IP literal, so a bracketed DNS name is not a valid host.
        for host in ["not a host!", "[localhost]"] {
            let error = Security::tls(Arc::clone(&fixture.client), host).expect_err(host);
            assert_eq!(
                (error.phase, error.kind),
                (Phase::Tls, ErrorKind::Other),
                "{host}"
            );
            assert!(!error.detail.is_empty());
            assert!(!error.reached_wire);
        }

        let server = TestServer::bind("[::1]:0", Some(Arc::clone(&fixture.server)), |mut conn| {
            conn.read_request();
            let _ = conn.write(OK.as_bytes());
            conn.sees_close(HANG_GUARD);
        });
        let runtime = start(1, "rt6");
        let mut client = runtime.client(tls_target(server.address, &fixture.client, "[::1]"));
        let authority = format!("[::1]:{}", server.address.port());
        let result = block_on(client.send(request(&authority, "/", HANG_GUARD)));
        let peer = result
            .as_ref()
            .ok()
            .and_then(|response| response.connection.peer);
        assert_ok(result);
        assert_eq!(peer, Some(server.address));
    }

    #[test]
    fn v4_mapped_dial_reports_the_dialed_v6_address() {
        let server = ok_server();
        let runtime = start(1, "rtm");
        let dialed = SocketAddr::new(
            IpAddr::V6(Ipv4Addr::LOCALHOST.to_ipv6_mapped()),
            server.address.port(),
        );
        let mut client = runtime.client(plain_target(dialed));
        let response = block_on(client.send(ok_request())).expect("exchange succeeds");
        assert_eq!(response.connection.peer, Some(dialed));
        assert_eq!(server.accepted(), 1);
    }

    #[test]
    fn unspecified_dial_reports_the_dialed_address_as_its_peer() {
        let server = ok_server();
        let runtime = start(1, "rtz");
        // Linux connects a dial of the unspecified address to loopback.
        let dialed = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), server.address.port());
        let mut client = runtime.client(plain_target(dialed));
        let response = block_on(client.send(ok_request())).expect("exchange succeeds");
        assert_eq!(response.connection.peer, Some(dialed));
        let local = response.connection.local.expect("local address");
        assert_eq!(local.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(server.accepted(), 1);
    }

    #[test]
    fn slot_threads_are_named_by_prefix_and_clients_pin_round_robin() {
        let runtime = ExplicitRuntime::start(config(3, 8, "rxs")).expect("runtime");
        let target = plain_target("127.0.0.1:1".parse().unwrap());
        let slots: Vec<_> = (0..4)
            .map(|_| runtime.client(Arc::clone(&target)).slot())
            .collect();
        assert_eq!(slots, [0, 1, 2, 0]);
        for slot in 0..3 {
            let call = runtime
                .worker_handle(slot)
                .expect("published worker")
                .call(|_| std::thread::current().name().map(str::to_owned))
                .expect("command admitted");
            assert_eq!(
                block_on(call).expect("thread name"),
                Some(format!("rxs-{slot}"))
            );
        }
        assert_eq!(live_threads("rxs"), 3);
    }

    #[test]
    fn dialed_sockets_set_tcp_nodelay() {
        let server = ok_server();
        let runtime = start(1, "rnt");
        let mut client = runtime.client(plain_target(server.address));
        for _ in 0..3 {
            assert_ok(block_on(client.send(ok_request())));
        }
        assert_eq!(*runtime.hooks().nodelay.lock().unwrap(), [true, true, true]);
    }

    #[test]
    fn exchange_panic_reports_panicked_and_slot_keeps_serving() {
        let (read_tx, read_rx) = mpsc::channel();
        let (closed_tx, closed_rx) = mpsc::channel();
        let server = TestServer::plain(move |mut conn| {
            // The panic before the handoff closes its connection before any request.
            let Some(request) = conn.try_read_request() else {
                return;
            };
            if request.request_line.starts_with("GET /park ") {
                let _ = read_tx.send(());
                let _ = closed_tx.send(conn.sees_close(HANG_GUARD));
                return;
            }
            let _ = conn.write(OK.as_bytes());
            conn.sees_close(HANG_GUARD);
        });
        let runtime = start(1, "rpc");
        let mut client = runtime.client(plain_target(server.address));

        runtime.set_exchange_hooks(ExchangeHooks {
            panic_before_handoff: true,
            ..ExchangeHooks::default()
        });
        let error = block_on(client.send(ok_request())).expect_err("panic");
        assert_eq!(error.kind, ErrorKind::Panicked);
        assert!(!error.reached_wire);
        // The stages measured before the panic survive it.
        assert!(error.timings.tcp_connect.is_some(), "{:?}", error.timings);
        assert_eq!(runtime.stats().tcp_connect_us.count, 1);
        assert!(
            error
                .detail
                .contains("injected panic before the request is handed to the session"),
            "{error}"
        );
        runtime.set_exchange_hooks(ExchangeHooks::default());
        assert_ok(block_on(client.send(ok_request())));

        let pause = TestPause::new();
        runtime.set_exchange_hooks(ExchangeHooks {
            before_head: Some(pause.clone()),
            panic_before_head: true,
            ..ExchangeHooks::default()
        });
        let error = block_on(async {
            let pending = tokio::spawn(async move {
                client
                    .send(request("localhost", "/park", Duration::from_secs(60)))
                    .await
            });
            pause.reached_async(1).await;
            tokio::task::spawn_blocking(move || read_rx.recv_timeout(HANG_GUARD))
                .await
                .expect("blocking wait")
                .expect("server read the request");
            pause.release();
            pending.await.expect("send task")
        })
        .expect_err("panic");
        assert_eq!(error.kind, ErrorKind::Panicked);
        assert!(error.reached_wire);
        assert!(error.timings.tcp_connect.is_some(), "{:?}", error.timings);
        assert!(closed_rx.recv_timeout(HANG_GUARD).expect("server result"));
        wait_until("transport retirement", || {
            runtime.stats().pending_teardowns == 0
        });
        assert_eq!(runtime.stats().panics, 2);

        // A panic in an exchange whose send was dropped is still counted.
        let pause = TestPause::new();
        runtime.set_exchange_hooks(ExchangeHooks {
            before_head: Some(pause.clone()),
            panic_before_head: true,
            ..ExchangeHooks::default()
        });
        let mut client = runtime.client(plain_target(server.address));
        block_on(async {
            let pending = tokio::spawn(async move { client.send(ok_request()).await });
            pause.reached_async(1).await;
            pending.abort();
            assert!(pending.await.expect_err("aborted").is_cancelled());
        });
        pause.release();
        wait_until("the dropped send's panic", || runtime.stats().panics == 3);
        wait_until("transport retirement", || {
            runtime.stats().pending_teardowns == 0
        });
        assert_eq!(runtime.stats().caller_cancellations, 1);
        assert_eq!(runtime.stats().restarts, 0);

        // A caller's waker that panics when the worker wakes it with the response, also with a
        // payload whose destructor panics, leaves the worker serving.
        runtime.set_exchange_hooks(ExchangeHooks::default());
        let caller = current_thread_runtime();
        for panics in [false, true] {
            let (parked, read_rx, release) = parking_server();
            let mut parked_client = runtime.client(plain_target(parked.address));
            let (dropped_tx, dropped_rx) = mpsc::channel();
            let waker = Arc::new(ReportingPanickingWaker {
                dropped: dropped_tx,
                panics,
            });
            let mut send = pin!(parked_client.send(ok_request()));
            let poll = poll_with_waker(&caller, send.as_mut(), Waker::from(waker));
            assert!(poll.is_pending(), "the send waits for its response");
            read_rx
                .recv_timeout(HANG_GUARD)
                .expect("server read the request");
            release.release();
            dropped_rx
                .recv_timeout(HANG_GUARD)
                .expect("the waker's panic payload is dropped");
            assert_ok(caller.block_on(send));
            let mut client = runtime.client(plain_target(server.address));
            assert_ok(block_on(client.send(ok_request())));
            assert_eq!(runtime.stats().restarts, 0, "panics: {panics}");
        }
    }

    #[test]
    fn closed_slot_restarts_and_joins_old_worker_off_the_caller_thread() {
        let server = ok_server();
        let runtime = start(1, "rro");
        let mut client = runtime.client(plain_target(server.address));
        assert_ok(block_on(client.send(ok_request())));
        force_close(&runtime, 0);
        assert_ok(block_on(client.send(ok_request())));
        assert_eq!(runtime.stats().restarts, 1);
        wait_until("old worker joined", || {
            !runtime.hooks().join_threads.lock().unwrap().is_empty()
        });
        let joins = runtime.hooks().join_threads.lock().unwrap().clone();
        assert_eq!(joins, ["rro-restart-0"]);
    }

    #[test]
    fn closed_slot_without_sends_recovers_through_slot_health_polls() {
        let runtime = start(1, "rrh");
        let client = runtime.client(plain_target("127.0.0.1:1".parse().unwrap()));
        force_close(&runtime, 0);
        std::thread::spawn(move || {
            assert!(tokio::runtime::Handle::try_current().is_err());
            assert_eq!(client.slot_health().state, SlotState::Closed);
            wait_for_state(&client, SlotState::Open);
        })
        .join()
        .expect("polling thread");
        assert_eq!(runtime.stats().restarts, 1);

        // `is_healthy` polling alone also recovers the slot.
        force_close(&runtime, 0);
        let poller = runtime.clone();
        std::thread::spawn(move || {
            assert!(tokio::runtime::Handle::try_current().is_err());
            assert!(!poller.is_healthy());
            wait_until("healthy runtime", || poller.is_healthy());
        })
        .join()
        .expect("polling thread");
        assert_eq!(runtime.stats().restarts, 2);
    }

    #[test]
    fn stale_handle_and_concurrent_sends_restart_a_slot_once() {
        let server = ok_server();
        let runtime = start(1, "rsg");
        let old_generation = runtime.generation(0);
        force_close(&runtime, 0);
        let clients: Vec<_> = (0..8)
            .map(|_| runtime.client(plain_target(server.address)))
            .collect();
        let pollers: Vec<_> = (0..4)
            .map(|_| runtime.client(plain_target(server.address)))
            .collect();
        block_on(async move {
            let sends: Vec<_> = clients
                .into_iter()
                .map(|mut client| tokio::spawn(async move { client.send(ok_request()).await }))
                .collect();
            let polls: Vec<_> = pollers
                .into_iter()
                .map(|client| {
                    tokio::task::spawn_blocking(move || {
                        (0..20).for_each(|_| {
                            let _ = client.slot_health();
                            std::thread::sleep(Duration::from_millis(1));
                        });
                    })
                })
                .collect();
            for send in sends {
                assert_ok(send.await.expect("send task"));
            }
            for poll in polls {
                poll.await.expect("poll task");
            }
        });
        assert_eq!(runtime.stats().restarts, 1);
        let new_generation = runtime.generation(0);
        assert_ne!(new_generation, old_generation);

        let (server, read_rx, release) = parking_server();
        let mut client = runtime.client(plain_target(server.address));
        let result = block_on(async {
            let pending = tokio::spawn(async move { client.send(ok_request()).await });
            tokio::task::spawn_blocking(move || read_rx.recv_timeout(HANG_GUARD))
                .await
                .expect("blocking wait")
                .expect("server read the request");
            runtime.report_closed_generation(0, old_generation);
            release.release();
            pending.await.expect("send task")
        });
        assert_ok(result);
        assert_eq!(runtime.stats().restarts, 1);
        assert_eq!(runtime.generation(0), new_generation);

        // A stale report does not restart the slot, even once the newer generation is closed too.
        force_close(&runtime, 0);
        runtime.report_closed_generation(0, old_generation);
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(runtime.generation(0), new_generation);
        assert_eq!(runtime.stats().restarts, 1);
        let client = runtime.client(plain_target("127.0.0.1:1".parse().unwrap()));
        assert_eq!(client.slot_health().state, SlotState::Closed);
        wait_for_state(&client, SlotState::Open);
        assert_eq!(runtime.stats().restarts, 2);
    }

    #[test]
    fn restart_publishes_before_old_worker_cleanup_finishes() {
        let server = ok_server();
        let runtime = start(1, "rpb");
        let mut client = runtime.client(plain_target(server.address));
        let barrier = TestPause::new();
        let _release = ReleaseOnDrop(barrier.clone());
        for restarts in 1..=2 {
            hold_cleanup(&runtime, 0, &barrier);
            force_close(&runtime, 0);
            assert_ok(block_on(client.send(ok_request())));
            assert_eq!(client.slot_health().state, SlotState::Open);
            assert_eq!(runtime.stats().restarts, restarts);
        }
        wait_until("both old workers joining", || {
            runtime.stats().restarts_joining == 2
        });
        assert_eq!(barrier.reached(), 2);
        barrier.release();
        wait_until("old workers joined", || {
            runtime.stats().restarts_joining == 0
        });
        assert_ok(block_on(client.send(ok_request())));
    }

    #[test]
    fn restarts_and_last_drop_use_none_of_the_callers_blocking_pool() {
        let server = ok_server();
        let runtime = start(1, "rbp");
        let mut client = runtime.client(plain_target(server.address));
        let caller = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .expect("caller runtime");
        let (hold_tx, hold_rx) = mpsc::channel::<()>();
        let (held_tx, held_rx) = mpsc::channel();
        let held = caller.spawn_blocking(move || {
            let _ = held_tx.send(());
            let _ = hold_rx.recv_timeout(HANG_GUARD * 2);
        });
        held_rx
            .recv_timeout(HANG_GUARD)
            .expect("blocking pool held");

        let barrier = TestPause::new();
        let _release = ReleaseOnDrop(barrier.clone());
        for _ in 0..2 {
            hold_cleanup(&runtime, 0, &barrier);
            force_close(&runtime, 0);
            assert_ok(caller.block_on(client.send(ok_request())));
        }
        assert_eq!(runtime.stats().restarts, 2);
        barrier.release();
        drop(hold_tx);
        caller.block_on(held).expect("held closure");

        force_close(&runtime, 0);
        let state = caller
            .block_on(caller.spawn_blocking(move || {
                let state = client.slot_health().state;
                drop(runtime);
                state
            }))
            .expect("closure");
        assert_eq!(state, SlotState::Closed);
        assert_eq!(live_threads("rbp"), 0);
    }

    #[test]
    fn failed_worker_spawn_at_start_or_restart_leaves_no_stuck_state() {
        // Start: the second worker fails, and the first is joined.
        let hooks = Arc::new(RuntimeHooks::default());
        hooks
            .worker_spawn_failures
            .lock()
            .unwrap()
            .extend([false, true]);
        let error = ExplicitRuntime::start_with_hooks(config(2, 8, "rfs"), hooks)
            .err()
            .expect("start fails");
        assert_eq!(
            (error.phase, error.kind),
            (Phase::Submit, ErrorKind::WorkerLost)
        );
        assert!(!error.reached_wire);
        assert!(!error.detail.is_empty());
        assert_eq!(live_threads("rfs"), 0);

        // Restart: the new worker fails to spawn, and the waiting send fails.
        let server = ok_server();
        let runtime = start(1, "rfr");
        let mut client = runtime.client(plain_target(server.address));
        force_close(&runtime, 0);
        runtime
            .hooks()
            .worker_spawn_failures
            .lock()
            .unwrap()
            .push_back(true);
        assert_worker_lost(block_on(client.send(ok_request())), false);
        assert_eq!(runtime.stats().failed_restarts, 1);
        assert_eq!(client.slot_health().state, SlotState::Closed);
        wait_for_state(&client, SlotState::Open);
        assert_ok(block_on(client.send(ok_request())));

        // Restart: the restart thread fails to spawn.
        force_close(&runtime, 0);
        runtime
            .hooks()
            .fail_next_thread_spawns
            .store(1, Ordering::SeqCst);
        assert_eq!(client.slot_health().state, SlotState::Closed);
        assert_eq!(runtime.stats().failed_restarts, 2);
        assert_eq!(client.slot_health().state, SlotState::Closed);
        wait_for_state(&client, SlotState::Open);

        // A send paused between releasing the slot lock and awaiting publication fails as
        // WorkerLost once released.
        let spawn = TestPause::new();
        let _release_spawn = ReleaseOnDrop(spawn.clone());
        let wait = TestPause::new();
        *runtime.hooks().spawn_pause.lock().unwrap() = Some(spawn.clone());
        *runtime.hooks().send_wait_pause.lock().unwrap() = Some(wait.clone());
        runtime
            .hooks()
            .worker_spawn_failures
            .lock()
            .unwrap()
            .push_back(true);
        force_close(&runtime, 0);
        let started = Instant::now();
        let result = block_on(async {
            let pending = tokio::spawn(async move {
                client
                    .send(request("localhost", "/", Duration::from_secs(60)))
                    .await
            });
            wait.reached_async(1).await;
            spawn.reached_async(1).await;
            spawn.release();
            let runtime = &runtime;
            wait_until_async(|| runtime.stats().failed_restarts == 3).await;
            wait.release();
            pending.await.expect("send task")
        });
        assert_worker_lost(result, false);
        assert!(started.elapsed() < HANG_GUARD);

        // The failure's signal wakes a waiting send with no slot lock held.
        let spawn = TestPause::new();
        let _release_spawn = ReleaseOnDrop(spawn.clone());
        *runtime.hooks().spawn_pause.lock().unwrap() = Some(spawn.clone());
        *runtime.hooks().send_wait_pause.lock().unwrap() = None;
        runtime
            .hooks()
            .worker_spawn_failures
            .lock()
            .unwrap()
            .push_back(true);
        force_close(&runtime, 0);
        let observer = runtime.client(plain_target(server.address));
        let probe = LockProbe::new(runtime.client(plain_target(server.address)));
        let caller = current_thread_runtime();
        let mut client = runtime.client(plain_target(server.address));
        let mut send = pin!(client.send(request("localhost", "/", Duration::from_secs(60))));
        let first = poll_with_probe(&caller, send.as_mut(), &probe);
        spawn.wait_reached(1);
        spawn.release();
        assert!(first.is_pending());
        assert!(!probe.woken_under_lock(), "woken under the slot lock");
        assert_worker_lost(caller.block_on(send), false);

        // A send that waited across a failed restart fails as WorkerLost even when a later
        // restart has published by the time it resumes.
        wait_for_state(&observer, SlotState::Open);
        let spawn = TestPause::new();
        let _release_spawn = ReleaseOnDrop(spawn.clone());
        let wait = TestPause::new();
        *runtime.hooks().spawn_pause.lock().unwrap() = Some(spawn.clone());
        *runtime.hooks().send_wait_pause.lock().unwrap() = Some(wait.clone());
        runtime
            .hooks()
            .worker_spawn_failures
            .lock()
            .unwrap()
            .push_back(true);
        force_close(&runtime, 0);
        let failed = runtime.stats().failed_restarts;
        let mut client = runtime.client(plain_target(server.address));
        let result = block_on(async {
            let pending = tokio::spawn(async move {
                client
                    .send(request("localhost", "/", Duration::from_secs(60)))
                    .await
            });
            wait.reached_async(1).await;
            spawn.reached_async(1).await;
            spawn.release();
            let runtime = &runtime;
            wait_until_async(|| runtime.stats().failed_restarts == failed + 1).await;
            wait_until_async(|| observer.slot_health().state == SlotState::Open).await;
            wait.release();
            pending.await.expect("send task")
        });
        assert_worker_lost(result, false);

        // A send whose own restart fails to spawn its thread fails as WorkerLost. A fresh
        // runtime, since the steps above have used most of the restart cap.
        let runtime = start(1, "rft");
        let mut client = runtime.client(plain_target(server.address));
        force_close(&runtime, 0);
        runtime
            .hooks()
            .fail_next_thread_spawns
            .store(1, Ordering::SeqCst);
        assert_worker_lost(block_on(client.send(ok_request())), false);
        assert_eq!(runtime.stats().failed_restarts, 1);
        wait_for_state(&client, SlotState::Open);
        assert_ok(block_on(client.send(ok_request())));

        // A failed attempt that reaches the cap leaves the slot Capped.
        let mut capped = config(1, 8, "rfc");
        capped.max_restarts_per_minute = 1;
        let runtime = ExplicitRuntime::start(capped).expect("runtime");
        let client = runtime.client(plain_target(server.address));
        runtime
            .hooks()
            .worker_spawn_failures
            .lock()
            .unwrap()
            .push_back(true);
        force_close(&runtime, 0);
        assert_eq!(client.slot_health().state, SlotState::Closed);
        wait_until("failed restart", || runtime.stats().failed_restarts == 1);
        assert_eq!(client.slot_health().state, SlotState::Capped);
    }

    async fn wait_until_async(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + HANG_GUARD;
        while !condition() {
            assert!(Instant::now() < deadline, "timed out");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[test]
    fn parked_send_fails_as_worker_lost_when_its_worker_closes() {
        let (read_tx, read_rx) = mpsc::channel();
        let (closed_tx, closed_rx) = mpsc::channel();
        let server = TestServer::plain(move |mut conn| {
            conn.read_request();
            let _ = read_tx.send(());
            let _ = closed_tx.send(conn.sees_close(HANG_GUARD));
        });
        let runtime = start(1, "rpw");
        let mut client = runtime.client(plain_target(server.address));
        let result = block_on(async {
            let pending = tokio::spawn(async move {
                let result = client
                    .send(request("localhost", "/", Duration::from_secs(60)))
                    .await;
                (client, result)
            });
            tokio::task::spawn_blocking(move || read_rx.recv_timeout(HANG_GUARD))
                .await
                .expect("blocking wait")
                .expect("server read the request");
            force_close(&runtime, 0);
            pending.await.expect("send task")
        });
        let (client, result) = result;
        assert_worker_lost(result, true);
        assert_eq!(runtime.stats().worker_losses, 1);
        assert!(closed_rx.recv_timeout(HANG_GUARD).expect("server result"));
        assert_eq!(runtime.stats().caller_cancellations, 0);
        wait_for_state(&client, SlotState::Open);
        wait_until("old worker joined", || {
            runtime.stats().restarts_joining == 0
        });
        wait_until("transport retirement", || {
            runtime.stats().pending_teardowns == 0
        });

        // A command that the closing worker drops before it starts the exchange.
        let mut client = client;
        let hold = hold_worker(&runtime, 0);
        let error = block_on(async {
            let pending = tokio::spawn(async move {
                client
                    .send(request("localhost", "/", Duration::from_secs(60)))
                    .await
            });
            let observer = runtime.client(plain_target(server.address));
            wait_until_async(|| observer.slot_health().queued_commands == 1).await;
            force_close(&runtime, 0);
            drop(hold);
            pending.await.expect("send task")
        })
        .expect_err("worker lost");
        assert_eq!(
            (error.phase, error.kind, error.reached_wire),
            (Phase::Submit, ErrorKind::WorkerLost, false),
            "{error}"
        );
        assert_eq!(runtime.stats().worker_losses, 2);
        assert_eq!(server.accepted(), 1);

        // A command that the closing worker drops after the dial, before the hand-off.
        let (closed_tx, closed_rx) = mpsc::channel();
        let silent = TestServer::plain(move |mut conn| {
            let _ = closed_tx.send(conn.read_end(HANG_GUARD));
        });
        let pause = TestPause::new();
        runtime.set_exchange_hooks(ExchangeHooks {
            before_adoption: Some(pause.clone()),
            ..ExchangeHooks::default()
        });
        let mut client = runtime.client(plain_target(silent.address));
        let error = block_on(async {
            let pending = tokio::spawn(async move {
                client
                    .send(request("localhost", "/", Duration::from_secs(60)))
                    .await
            });
            pause.reached_async(1).await;
            force_close(&runtime, 0);
            pending.await.expect("send task")
        })
        .expect_err("worker lost");
        assert_eq!(
            (error.phase, error.kind, error.reached_wire),
            (Phase::Connect, ErrorKind::WorkerLost, false),
            "{error}"
        );
        assert_eq!(runtime.stats().worker_losses, 3);
        assert!(
            closed_rx
                .recv_timeout(HANG_GUARD)
                .expect("server result")
                .is_some()
        );
        assert_eq!(runtime.stats().caller_cancellations, 0);
    }

    #[test]
    fn job_futures_hold_no_dead_stream_or_input_slots() {
        let fixture = tls_fixture();
        let secure = TestServer::tls(Arc::clone(&fixture.server), |mut conn| {
            conn.read_request();
            let _ = conn.write(OK.as_bytes());
            conn.sees_close(HANG_GUARD);
        });
        let plain = ok_server();
        let runtime = start(1, "rjl");
        let mut client = runtime.client(tls_target(secure.address, &fixture.client, "localhost"));
        assert_ok(block_on(client.send(ok_request())));
        let mut client = runtime.client(plain_target(plain.address));
        assert_ok(block_on(client.send(ok_request())));

        let sizes = runtime.hooks().job_future_sizes.lock().unwrap().clone();
        let [tls, plaintext] = sizes[..] else {
            panic!("one job future per send: {sizes:?}");
        };
        // The bound catches a handshake state stored beside the exchange's, and dead copies of
        // the stream or the exchange input in more than one layer, but not a single dead copy.
        // The plaintext check catches an exchange whose held state keeps the stream, which is
        // larger by value than a boxed TLS stream.
        assert!(tls <= 4096, "a TLS send's job future is {tls} B");
        assert!(
            plaintext <= tls,
            "a plaintext send's job future is {plaintext} B, a TLS send's {tls} B"
        );
    }

    /// A TLS server that signals once it has read a request and then reports whether the client
    /// closed the connection.
    fn parked_tls_server(
        fixture: &TlsFixture,
    ) -> (TestServer, mpsc::Receiver<()>, mpsc::Receiver<bool>) {
        let (read_tx, read_rx) = mpsc::channel();
        let (closed_tx, closed_rx) = mpsc::channel();
        let server = TestServer::tls(Arc::clone(&fixture.server), move |mut conn| {
            conn.read_request();
            let _ = read_tx.send(());
            let _ = closed_tx.send(conn.sees_close(HANG_GUARD));
        });
        (server, read_rx, closed_rx)
    }

    /// A peer that accepts one connection, reports the size of the client's first read, never
    /// responds, and then reports how its reads ended.
    fn silent_peer() -> (SocketAddr, mpsc::Receiver<usize>, SilentPeer) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("loopback listener");
        let address = listener.local_addr().expect("listener address");
        let (hello_tx, hello_rx) = mpsc::channel();
        let peer = std::thread::spawn(move || {
            let (mut tcp, _) = listener.accept().expect("accepted connection");
            tcp.set_read_timeout(Some(HANG_GUARD))
                .expect("read timeout");
            let mut buffer = [0; 4096];
            let _ = hello_tx.send(tcp.read(&mut buffer).expect("first read"));
            loop {
                match tcp.read(&mut buffer) {
                    Ok(0) => return ReadEnd::Eof,
                    Ok(_) => {}
                    Err(error) => return ReadEnd::Error(error.kind()),
                }
            }
        });
        let peer = SilentPeer {
            address,
            thread: Some(peer),
        };
        (address, hello_rx, peer)
    }

    /// The thread of a `silent_peer`. Dropping it unblocks the peer's accept and joins the
    /// thread, so a failed test does not leave the thread and its listener behind.
    struct SilentPeer {
        address: SocketAddr,
        thread: Option<JoinHandle<ReadEnd>>,
    }

    impl SilentPeer {
        fn read_end(mut self) -> ReadEnd {
            let thread = self.thread.take().expect("peer thread");
            thread.join().expect("peer thread")
        }
    }

    impl Drop for SilentPeer {
        fn drop(&mut self) {
            if let Some(thread) = self.thread.take() {
                let _ = TcpStream::connect(self.address);
                let _ = thread.join();
            }
        }
    }

    #[test]
    fn tls_jobs_keep_the_worker_lost_and_panic_outcomes() {
        let fixture = tls_fixture();
        let ok = TestServer::tls(Arc::clone(&fixture.server), |mut conn| {
            conn.read_request();
            let _ = conn.write(OK.as_bytes());
            conn.sees_close(HANG_GUARD);
        });
        let runtime = start(1, "rtj");
        let tls = |server: &TestServer| tls_target(server.address, &fixture.client, "localhost");
        let mut client = runtime.client(tls(&ok));

        // A panic before the hand-off: the peer completed the handshake and does not receive a
        // request.
        let (ended_tx, ended_rx) = mpsc::channel();
        let dropped = TestServer::tls(Arc::clone(&fixture.server), move |mut conn| {
            let _ = ended_tx.send(conn.read_end(HANG_GUARD));
        });
        runtime.set_exchange_hooks(ExchangeHooks {
            panic_before_handoff: true,
            ..ExchangeHooks::default()
        });
        let error = block_on(runtime.client(tls(&dropped)).send(ok_request())).expect_err("panic");
        assert_eq!(
            (error.phase, error.kind, error.reached_wire),
            (Phase::Connect, ErrorKind::Panicked, false),
            "{error}"
        );
        assert!(error.timings.tcp_connect.is_some(), "{:?}", error.timings);
        assert!(error.timings.tls_handshake.is_some(), "{:?}", error.timings);
        assert!(
            error
                .detail
                .contains("injected panic before the request is handed to the session"),
            "{error}"
        );
        assert!(
            ended_rx
                .recv_timeout(HANG_GUARD)
                .expect("server result")
                .is_some()
        );
        runtime.set_exchange_hooks(ExchangeHooks::default());
        assert_ok(block_on(client.send(ok_request())));

        // A panic after the hand-off, while the exchange holds its adopted TLS stream.
        let (parked, read_rx, closed_rx) = parked_tls_server(&fixture);
        let pause = TestPause::new();
        let _release_head = ReleaseOnDrop(pause.clone());
        runtime.set_exchange_hooks(ExchangeHooks {
            before_head: Some(pause.clone()),
            panic_before_head: true,
            ..ExchangeHooks::default()
        });
        let mut parking = runtime.client(tls(&parked));
        let error = block_on(async {
            let pending = tokio::spawn(async move { parking.send(waiting_request()).await });
            pause.reached_async(1).await;
            tokio::task::spawn_blocking(move || read_rx.recv_timeout(HANG_GUARD))
                .await
                .expect("blocking wait")
                .expect("server read the request");
            pause.release();
            pending.await.expect("send task")
        })
        .expect_err("panic");
        assert_eq!(
            (error.phase, error.kind, error.reached_wire),
            (Phase::Read, ErrorKind::Panicked, true),
            "{error}"
        );
        assert!(error.timings.tls_handshake.is_some(), "{:?}", error.timings);
        assert!(closed_rx.recv_timeout(HANG_GUARD).expect("server result"));
        wait_until("transport retirement", || {
            runtime.stats().pending_teardowns == 0
        });
        runtime.set_exchange_hooks(ExchangeHooks::default());
        assert_ok(block_on(client.send(ok_request())));
        assert_eq!(runtime.stats().panics, 2);
        assert_eq!(runtime.stats().restarts, 0);

        // A send parked in its exchange when its worker closes.
        let (parked, read_rx, closed_rx) = parked_tls_server(&fixture);
        let mut parking = runtime.client(tls(&parked));
        let result = block_on(async {
            let pending = tokio::spawn(async move { parking.send(waiting_request()).await });
            tokio::task::spawn_blocking(move || read_rx.recv_timeout(HANG_GUARD))
                .await
                .expect("blocking wait")
                .expect("server read the request");
            force_close(&runtime, 0);
            pending.await.expect("send task")
        });
        assert_worker_lost(result, true);
        assert_eq!(runtime.stats().worker_losses, 1);
        assert!(closed_rx.recv_timeout(HANG_GUARD).expect("server result"));
        wait_for_state(&client, SlotState::Open);
        wait_until("old worker joined", || {
            runtime.stats().restarts_joining == 0
        });
        wait_until("transport retirement", || {
            runtime.stats().pending_teardowns == 0
        });

        // A send whose worker closes while its handshake is pending: the peer has read the
        // ClientHello, so the handshake has been polled, and it never responds.
        let (silent, hello_rx, silent_peer) = silent_peer();
        let mut waiting = runtime.client(tls_target(silent, &fixture.client, "localhost"));
        let error = block_on(async {
            let pending = tokio::spawn(async move { waiting.send(waiting_request()).await });
            let hello = tokio::task::spawn_blocking(move || hello_rx.recv_timeout(HANG_GUARD))
                .await
                .expect("blocking wait")
                .expect("the peer read the client's first flight");
            assert!(hello > 0, "the client sent its ClientHello");
            force_close(&runtime, 0);
            pending.await.expect("send task")
        })
        .expect_err("worker lost");
        assert_eq!(
            (error.phase, error.kind, error.reached_wire),
            (Phase::Connect, ErrorKind::WorkerLost, false),
            "{error}"
        );
        assert_eq!(silent_peer.read_end(), ReadEnd::Eof);
        assert_eq!(runtime.stats().worker_losses, 2);
        assert_eq!(runtime.stats().panics, 2);
        assert_eq!(runtime.stats().caller_cancellations, 0);
        wait_for_state(&client, SlotState::Open);
        assert_ok(block_on(client.send(ok_request())));

        // A panic from the last drop of a TLS configuration, which the job owns once its client
        // and send are dropped before it dials, is caught as the exchange's own panics are.
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let mut owned = (*fixture.client).clone();
        owned.key_log = Arc::new(PanickingKeyLog(dropped_tx));
        let pause = TestPause::new();
        let _release_connect = ReleaseOnDrop(pause.clone());
        *runtime.hooks().connect_pause.lock().unwrap() = Some(pause.clone());
        let generation = runtime.generation(0);
        let restarts = runtime.stats().restarts;
        let mut owning = runtime.client(tls_target(ok.address, &Arc::new(owned), "localhost"));
        block_on(async {
            let pending = tokio::spawn(async move { owning.send(waiting_request()).await });
            pause.reached_async(1).await;
            pending.abort();
            assert!(pending.await.expect_err("aborted").is_cancelled());
        });
        *runtime.hooks().connect_pause.lock().unwrap() = None;
        pause.release();
        dropped_rx
            .recv_timeout(HANG_GUARD)
            .expect("the configuration's last drop");
        wait_until("the drop's panic is handled", || {
            runtime.stats().panics == 3
                || runtime.worker_handle(0).map(|handle| handle.state()) != Some(WorkerState::Open)
        });
        assert_eq!(runtime.stats().panics, 3);
        assert_eq!(runtime.stats().caller_cancellations, 1);
        assert_ok(block_on(client.send(ok_request())));
        assert_eq!(runtime.generation(0), generation);
        assert_eq!(runtime.stats().restarts, restarts);
    }

    /// A key log whose drop reports itself and then panics, unless a panic is unwinding.
    #[derive(Debug)]
    struct PanickingKeyLog(mpsc::Sender<()>);

    impl rustls::KeyLog for PanickingKeyLog {
        fn log(&self, _: &str, _: &[u8], _: &[u8]) {}
    }

    impl Drop for PanickingKeyLog {
        fn drop(&mut self) {
            let _ = self.0.send(());
            if !std::thread::panicking() {
                panic!("injected panic from the TLS configuration's last drop");
            }
        }
    }

    #[test]
    fn slot_health_tracks_worker_state_and_overloads() {
        let server = ok_server();
        let runtime = ExplicitRuntime::start(config(1, 1, "rsh")).expect("runtime");
        let mut client = runtime.client(plain_target(server.address));
        let health = client.slot_health();
        assert_eq!(health.state, SlotState::Open);
        assert_eq!(
            (
                health.sends_in_flight,
                health.queued_commands,
                health.command_capacity,
                health.consecutive_overloads
            ),
            (0, 0, 1, 0)
        );

        let release = hold_worker(&runtime, 0);
        runtime
            .worker_handle(0)
            .expect("published worker")
            .submit(|_| {})
            .expect("filler admitted");
        for _ in 0..2 {
            let error = block_on(client.send(ok_request())).expect_err("overloaded");
            assert_eq!(error.kind, ErrorKind::Overloaded);
        }
        let health = client.slot_health();
        assert_eq!(
            (
                health.sends_in_flight,
                health.queued_commands,
                health.consecutive_overloads
            ),
            (0, 1, 2)
        );
        drop(release);
        wait_until("queue drained", || {
            client.slot_health().consecutive_overloads == 0
        });
        assert_eq!(client.slot_health().queued_commands, 0);
        assert_ok(block_on(client.send(ok_request())));

        force_close(&runtime, 0);
        assert_eq!(client.slot_health().state, SlotState::Closed);
        wait_for_state(&client, SlotState::Open);
    }

    #[test]
    fn restarting_slot_reports_state_and_unhealthy() {
        let runtime = start(1, "rrs");
        let client = runtime.client(plain_target("127.0.0.1:1".parse().unwrap()));
        assert!(runtime.is_healthy());
        let pause = TestPause::new();
        let _release = ReleaseOnDrop(pause.clone());
        *runtime.hooks().spawn_pause.lock().unwrap() = Some(pause.clone());
        force_close(&runtime, 0);
        assert_eq!(client.slot_health().state, SlotState::Closed);
        assert_eq!(client.slot_health().state, SlotState::Restarting);
        assert!(!runtime.is_healthy());
        pause.wait_reached(1);
        assert_eq!(client.slot_health().state, SlotState::Restarting);
        pause.release();
        wait_for_state(&client, SlotState::Open);
        assert!(runtime.is_healthy());
        assert_eq!(runtime.stats().restarts, 1);
    }

    #[test]
    fn restart_cap_reports_capped_and_fails_as_worker_lost() {
        let server = ok_server();
        let mut capped = config(1, 8, "rcp");
        capped.max_restarts_per_minute = 2;
        let hooks = Arc::new(RuntimeHooks::default());
        *hooks.cap_window.lock().unwrap() = Duration::from_millis(300);
        let runtime = ExplicitRuntime::start_with_hooks(capped, hooks).expect("runtime");
        let mut client = runtime.client(plain_target(server.address));
        let barrier = TestPause::new();
        let _release = ReleaseOnDrop(barrier.clone());
        for _ in 0..2 {
            hold_cleanup(&runtime, 0, &barrier);
            force_close(&runtime, 0);
            assert_eq!(client.slot_health().state, SlotState::Closed);
            wait_for_state(&client, SlotState::Open);
        }
        // An open slot reports Open even at the cap.
        assert_eq!(client.slot_health().state, SlotState::Open);

        force_close(&runtime, 0);
        assert_eq!(client.slot_health().state, SlotState::Capped);
        assert_worker_lost(block_on(client.send(ok_request())), false);
        assert!(!runtime.is_healthy());
        std::thread::sleep(Duration::from_millis(400));
        // The window has passed, but both restarts are still joining their old workers.
        assert_eq!(client.slot_health().state, SlotState::Capped);
        barrier.release();
        wait_until("old workers joined", || {
            runtime.stats().restarts_joining == 0
        });
        assert_eq!(client.slot_health().state, SlotState::Closed);
        wait_for_state(&client, SlotState::Open);
        assert_ok(block_on(client.send(ok_request())));
        assert_eq!(runtime.stats().restarts, 3);

        let mut never = config(1, 8, "rcz");
        never.max_restarts_per_minute = 0;
        let runtime = ExplicitRuntime::start(never).expect("runtime");
        let client = runtime.client(plain_target(server.address));
        force_close(&runtime, 0);
        assert_eq!(client.slot_health().state, SlotState::Capped);
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(client.slot_health().state, SlotState::Capped);
        assert_eq!(runtime.stats().restarts, 0);
    }

    #[test]
    fn slot_at_command_capacity_reports_overloaded() {
        let server = ok_server();
        let runtime = ExplicitRuntime::start(config(1, 1, "roq")).expect("runtime");
        let mut client = runtime.client(plain_target(server.address));
        let release = hold_worker(&runtime, 0);
        runtime
            .worker_handle(0)
            .expect("published worker")
            .submit(|_| {})
            .expect("filler admitted");
        let error = block_on(client.send(ok_request())).expect_err("queue full");
        assert_eq!(error.kind, ErrorKind::Overloaded);
        assert!(!error.reached_wire);
        drop(release);
        assert_eq!(runtime.stats().overloads, 1);

        let (server, read_rx, release) = parking_server();
        let runtime = ExplicitRuntime::start(config(1, 2, "rop")).expect("runtime");
        let target = plain_target(server.address);
        let parked: Vec<ExplicitClient> = (0..2)
            .map(|_| runtime.client(Arc::clone(&target)))
            .collect();
        let mut third = runtime.client(Arc::clone(&target));
        block_on(async {
            let pending: Vec<_> = parked
                .into_iter()
                .map(|mut client| tokio::spawn(async move { client.send(ok_request()).await }))
                .collect();
            tokio::task::spawn_blocking(move || {
                read_rx.recv_timeout(HANG_GUARD).expect("first request");
                read_rx.recv_timeout(HANG_GUARD).expect("second request");
            })
            .await
            .expect("blocking wait");
            assert_eq!(
                runtime.worker_handle(0).expect("worker").queued_commands(),
                0
            );
            let error = third.send(ok_request()).await.expect_err("in-flight bound");
            assert_eq!(error.kind, ErrorKind::Overloaded);
            assert!(!error.reached_wire);
            release.release();
            for send in pending {
                assert_ok(send.await.expect("send task"));
            }
        });
        assert_eq!(server.accepted(), 2);
    }

    #[test]
    fn stats_report_counts_histograms_and_pending_teardowns() {
        let fixture = tls_fixture();
        let server = TestServer::tls(Arc::clone(&fixture.server), |mut conn| {
            conn.read_request();
            let _ = conn.write(OK.as_bytes());
            conn.sees_close(HANG_GUARD);
        });
        let runtime = start(1, "rst");
        let mut client = runtime.client(tls_target(server.address, &fixture.client, "localhost"));
        for _ in 0..2 {
            assert_ok(block_on(client.send(ok_request())));
        }
        wait_until("transport retirement", || {
            let stats = runtime.stats();
            stats.pending_teardowns == 0 && stats.close_to_retirement_us.count == 2
        });
        let stats = runtime.stats();
        assert_eq!(stats.sends, 2);
        assert_eq!(stats.sends_in_flight, 0);
        for histogram in [
            &stats.queued_us,
            &stats.tcp_connect_us,
            &stats.tls_handshake_us,
            &stats.first_byte_us,
            &stats.close_to_retirement_us,
        ] {
            assert_eq!(histogram.count, 2);
            assert_eq!(
                histogram.counts.len(),
                histogram.bucket_upper_bounds_us.len() + 1
            );
            assert_eq!(histogram.counts.iter().sum::<u64>(), 2);
        }

        let (server, read_rx, release) = parking_server();
        let mut client = runtime.client(plain_target(server.address));
        block_on(async {
            let pending = tokio::spawn(async move { client.send(ok_request()).await });
            tokio::task::spawn_blocking(move || read_rx.recv_timeout(HANG_GUARD))
                .await
                .expect("blocking wait")
                .expect("server read the request");
            let stats = runtime.stats();
            assert_eq!((stats.sends_in_flight, stats.pending_teardowns), (1, 1));
            release.release();
            assert_ok(pending.await.expect("send task"));
        });
        wait_until("transport retirement", || {
            runtime.stats().pending_teardowns == 0
        });
        let stats = runtime.stats();
        assert_eq!((stats.sends, stats.sends_in_flight), (3, 0));
        assert_eq!(stats.tls_handshake_us.count, 2);
        assert_eq!(stats.tcp_connect_us.count, 3);
        assert_eq!(stats.queued_us.count, 3);
        assert_eq!(stats.unconfirmed_retirements, 0);

        // A retirement that the cleanup cannot confirm, as when `wait_retired` fails, lowers the
        // gauge once and counts as unconfirmed, with no histogram sample.
        runtime.set_exchange_hooks(ExchangeHooks {
            unconfirmed_retirement: true,
            ..ExchangeHooks::default()
        });
        let unconfirmed = ok_server();
        let mut client = runtime.client(plain_target(unconfirmed.address));
        assert_ok(block_on(client.send(ok_request())));
        wait_until("unconfirmed retirement", || {
            let stats = runtime.stats();
            stats.pending_teardowns == 0 && stats.unconfirmed_retirements == 1
        });
        assert_eq!(
            runtime.stats().close_to_retirement_us.count,
            stats.close_to_retirement_us.count
        );
        runtime.set_exchange_hooks(ExchangeHooks::default());

        // A send whose dial fails still reports its queue delay.
        let (closed, _bound) = refusing_address();
        let mut refused = runtime.client(plain_target(closed));
        let error = block_on(refused.send(ok_request())).expect_err("refused");
        assert_eq!(error.kind, ErrorKind::Refused);
        let after_refusal = runtime.stats();
        assert_eq!(after_refusal.queued_us.count, 5);
        assert_eq!(after_refusal.tcp_connect_us.count, 4);

        // So does a send whose deadline passes while its command is queued.
        let release = hold_worker(&runtime, 0);
        let mut queued = runtime.client(plain_target(closed));
        let budget = EXPIRY_BUDGET;
        let error = block_on(queued.send(request("localhost", "/", budget)))
            .expect_err("expired in the queue");
        assert_eq!(
            (error.phase, error.kind),
            (Phase::Submit, ErrorKind::Timeout)
        );
        // The deadline was fixed just before `send` began, so the queue delay is nearly the
        // whole budget.
        assert!(error.timings.queued >= budget / 2, "{:?}", error.timings);
        drop(release);
        let stats = runtime.stats();
        assert_eq!(stats.queued_us.count, 6);
        assert!(stats.queued_us.sum_us >= after_refusal.queued_us.sum_us + 100_000);
    }

    #[test]
    fn teardown_guards_count_each_stream_until_it_is_destroyed() {
        let fixture = tls_fixture();
        let secure = TestServer::tls(Arc::clone(&fixture.server), |mut conn| {
            conn.read_request();
            let _ = conn.write(OK.as_bytes());
            conn.sees_close(HANG_GUARD);
        });
        let plain = ok_server();
        let runtime = start(1, "rtg");
        // A cleanup task told not to confirm would count each stream as unconfirmed. Guarded
        // streams do not register one.
        runtime.set_exchange_hooks(ExchangeHooks {
            unconfirmed_retirement: true,
            ..ExchangeHooks::default()
        });
        for target in [
            plain_target(plain.address),
            tls_target(secure.address, &fixture.client, "localhost"),
        ] {
            let mut client = runtime.client(target).with_teardown_guard();
            assert_ok(block_on(client.send(ok_request())));
        }
        wait_until("guarded streams destroyed", || {
            runtime.stats().pending_teardowns == 0
        });
        let stats = runtime.stats();
        assert_eq!(stats.unconfirmed_retirements, 0);
        assert_eq!(stats.close_to_retirement_us.count, 0);
        runtime.set_exchange_hooks(ExchangeHooks::default());

        // The stream is counted while the server holds its response.
        let (server, read_rx, release) = parking_server();
        let mut client = runtime
            .client(plain_target(server.address))
            .with_teardown_guard();
        block_on(async {
            let pending = tokio::spawn(async move { client.send(ok_request()).await });
            tokio::task::spawn_blocking(move || read_rx.recv_timeout(HANG_GUARD))
                .await
                .expect("blocking wait")
                .expect("server read the request");
            assert_eq!(runtime.stats().pending_teardowns, 1);
            release.release();
            assert_ok(pending.await.expect("send task"));
        });
        wait_until("guarded stream destroyed", || {
            runtime.stats().pending_teardowns == 0
        });

        // A client without the guard still confirms through its cleanup task.
        let mut client = runtime.client(plain_target(plain.address));
        assert_ok(block_on(client.send(ok_request())));
        wait_until("transport retirement", || {
            let stats = runtime.stats();
            stats.pending_teardowns == 0 && stats.close_to_retirement_us.count == 1
        });
        assert_eq!(runtime.stats().unconfirmed_retirements, 0);
    }

    #[test]
    fn a_teardown_guard_dropped_while_its_thread_panics_counts_as_unconfirmed() {
        struct PanicsOnDrop;
        impl Drop for PanicsOnDrop {
            fn drop(&mut self) {
                panic!("injected panic destroying the stream");
            }
        }
        /// Drops its guard after its stream, as `WatchedStream` does.
        struct Guarded {
            _io: PanicsOnDrop,
            _teardown: TeardownGuard,
        }
        let stats = Arc::new(ExplicitStats::new());
        let counts = || {
            let snapshot = stats.snapshot();
            (snapshot.pending_teardowns, snapshot.unconfirmed_retirements)
        };
        let clean = TeardownGuard::new(&stats);
        assert_eq!(counts(), (1, 0));
        drop(clean);
        assert_eq!(counts(), (0, 0), "a clean drop is confirmed");
        let guarded = Guarded {
            _io: PanicsOnDrop,
            _teardown: TeardownGuard::new(&stats),
        };
        assert_eq!(counts(), (1, 0));
        assert!(catch_unwind(AssertUnwindSafe(move || drop(guarded))).is_err());
        assert_eq!(counts(), (0, 1), "a panicking destruction is unconfirmed");
    }

    #[test]
    fn public_types_are_send_and_sync() {
        fn send<Value: Send>(_: &Value) {}
        fn send_sync<Value: Send + Sync + 'static>() {}
        send_sync::<ExplicitRuntime>();
        send_sync::<Response>();
        send_sync::<ExplicitError>();
        send_sync::<Target>();
        let runtime = start(1, "rss");
        let mut client = runtime.client(plain_target("127.0.0.1:1".parse().unwrap()));
        send(&client);
        let future = client.send(ok_request());
        send(&future);
    }

    #[test]
    fn dropping_last_runtime_handle_joins_while_clients_outlive_it() {
        let (server, read_rx, release) = parking_server();
        let runtime = start(1, "rdj");
        let last = runtime.clone();
        drop(runtime);
        let mut client = last.client(plain_target(server.address));
        let mut other = last.client(plain_target(server.address));
        let caller = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("caller runtime");
        let pending = caller.spawn(async move {
            client
                .send(request("localhost", "/", Duration::from_secs(60)))
                .await
        });
        read_rx
            .recv_timeout(HANG_GUARD)
            .expect("server read the request");
        drop(last);
        assert_eq!(live_threads("rdj"), 0);
        assert_worker_lost(caller.block_on(pending).expect("send task"), true);
        assert_worker_lost(caller.block_on(other.send(ok_request())), false);
        drop(other);
        release.release();

        // While the drop still joins slot 0, slot 1's worker is published and open, but a send
        // on it does not start an exchange.
        let server = responding(OK);
        let runtime = start(2, "rdk");
        let barrier = TestPause::new();
        let _release = ReleaseOnDrop(barrier.clone());
        hold_cleanup(&runtime, 0, &barrier);
        let _first = runtime.client(plain_target(server.address));
        let mut second = runtime.client(plain_target(server.address));
        assert_eq!(second.slot(), 1);
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let dropper = std::thread::spawn(move || {
            drop(runtime);
            let _ = dropped_tx.send(());
        });
        wait_until("the shutdown flag", || {
            second.slot_health().state == SlotState::Closed
        });
        assert_worker_lost(caller.block_on(second.send(ok_request())), false);
        assert_eq!(server.accepted(), 0);
        assert!(
            dropped_rx.try_recv().is_err(),
            "the drop still joins slot 0"
        );
        barrier.release();
        dropped_rx.recv_timeout(HANG_GUARD).expect("drop returns");
        dropper.join().expect("dropper");
        assert_eq!(live_threads("rdk"), 0);
    }

    #[test]
    fn dropping_last_runtime_handle_during_a_restart_leaves_no_worker_thread() {
        let server = ok_server();
        let runtime = start(1, "rdr");
        let hooks = Arc::clone(runtime.hooks());
        let spawn = TestPause::new();
        let _release = ReleaseOnDrop(spawn.clone());
        *hooks.spawn_pause.lock().unwrap() = Some(spawn.clone());
        let observer = runtime.client(plain_target(server.address));
        let clients: Vec<_> = (0..4)
            .map(|_| runtime.client(plain_target(server.address)))
            .collect();
        force_close(&runtime, 0);
        let caller = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("caller runtime");
        let sends: Vec<_> = clients
            .into_iter()
            .map(|mut client| caller.spawn(async move { client.send(ok_request()).await }))
            .collect();
        // A send admitted the restart, which the hook holds in flight through the drop.
        spawn.wait_reached(1);
        let dropper = caller.spawn_blocking(move || drop(runtime));
        let flagged = eventually(|| observer.slot_health().state == SlotState::Closed);
        spawn.release();
        assert!(flagged, "the drop set the shutdown flag");
        caller.block_on(dropper).expect("drop");
        assert_eq!(live_threads("rdr"), 0);
        for send in sends {
            assert_worker_lost(caller.block_on(send).expect("send task"), false);
        }
        assert!(
            hooks
                .join_threads
                .lock()
                .unwrap()
                .iter()
                .all(|name| name.starts_with("rdr-restart-"))
        );
    }

    #[test]
    fn last_drop_waits_for_a_restart_admitted_before_its_handle_is_registered() {
        let runtime = start(1, "rdw");
        let client = runtime.client(plain_target("127.0.0.1:1".parse().unwrap()));
        let pause = TestPause::new();
        let _release = ReleaseOnDrop(pause.clone());
        *runtime.hooks().requester_pause.lock().unwrap() = Some(pause.clone());
        force_close(&runtime, 0);
        let poller = std::thread::spawn(move || client.slot_health().state);
        pause.wait_reached(1);
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let dropper = std::thread::spawn(move || {
            drop(runtime);
            let _ = dropped_tx.send(());
        });
        assert!(dropped_rx.recv_timeout(Duration::from_millis(200)).is_err());
        pause.release();
        dropped_rx.recv_timeout(HANG_GUARD).expect("drop returns");
        dropper.join().expect("dropper");
        assert_eq!(poller.join().expect("poller"), SlotState::Closed);
        assert_eq!(live_threads("rdw"), 0);
    }

    #[test]
    fn send_waiting_for_publication_leaves_its_runtime_thread_free() {
        let server = ok_server();
        let runtime = start(1, "rsw");
        let pause = TestPause::new();
        let _release = ReleaseOnDrop(pause.clone());
        *runtime.hooks().spawn_pause.lock().unwrap() = Some(pause.clone());
        force_close(&runtime, 0);
        let mut client = runtime.client(plain_target(server.address));
        let caller = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("caller runtime");
        let client = caller.block_on(async move {
            let pending = tokio::spawn(async move {
                let result = client
                    .send(request("localhost", "/", Duration::from_secs(60)))
                    .await;
                (client, result)
            });
            let ticks = Arc::new(AtomicUsize::new(0));
            let counter = Arc::clone(&ticks);
            let ticker = tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    counter.fetch_add(1, Ordering::SeqCst);
                }
            });
            pause.reached_async(1).await;
            let before = ticks.load(Ordering::SeqCst);
            wait_until_async(|| ticks.load(Ordering::SeqCst) > before + 10).await;
            pause.release();
            let (client, result) = pending.await.expect("send task");
            assert_ok(result);
            ticker.abort();
            assert!(ticker.await.expect_err("aborted").is_cancelled());
            client
        });
        *runtime.hooks().spawn_pause.lock().unwrap() = None;

        let wait = TestPause::new();
        *runtime.hooks().send_wait_pause.lock().unwrap() = Some(wait.clone());
        let generation = runtime.generation(0);
        force_close(&runtime, 0);
        let started = Instant::now();
        let mut client = client;
        let observer = runtime.client(plain_target(server.address));
        let result = caller.block_on(async {
            let pending = tokio::spawn(async move {
                client
                    .send(request("localhost", "/", Duration::from_secs(60)))
                    .await
            });
            wait.reached_async(1).await;
            wait_until_async(|| {
                runtime.generation(0) != generation
                    && observer.slot_health().state == SlotState::Open
            })
            .await;
            wait.release();
            pending.await.expect("send task")
        });
        assert_ok(result);
        assert!(started.elapsed() < HANG_GUARD);

        // Publication wakes a waiting send with no slot lock held.
        let spawn = TestPause::new();
        let _release_spawn = ReleaseOnDrop(spawn.clone());
        *runtime.hooks().spawn_pause.lock().unwrap() = Some(spawn.clone());
        *runtime.hooks().send_wait_pause.lock().unwrap() = None;
        force_close(&runtime, 0);
        let probe = LockProbe::new(runtime.client(plain_target(server.address)));
        let mut client = runtime.client(plain_target(server.address));
        let mut send = pin!(client.send(request("localhost", "/", Duration::from_secs(60))));
        let first = poll_with_probe(&caller, send.as_mut(), &probe);
        spawn.wait_reached(1);
        spawn.release();
        assert!(first.is_pending());
        assert!(!probe.woken_under_lock(), "woken under the slot lock");
        assert_ok(caller.block_on(send));
    }

    #[test]
    fn last_drop_wakes_a_send_waiting_for_publication_as_worker_lost() {
        for (prefix, pause_send) in [("rlw", false), ("rlx", true)] {
            let server = ok_server();
            let runtime = start(1, prefix);
            let spawn = TestPause::new();
            let _release = ReleaseOnDrop(spawn.clone());
            let wait = TestPause::new();
            *runtime.hooks().spawn_pause.lock().unwrap() = Some(spawn.clone());
            if pause_send {
                *runtime.hooks().send_wait_pause.lock().unwrap() = Some(wait.clone());
            }
            force_close(&runtime, 0);
            let mut client = runtime.client(plain_target(server.address));
            let caller = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .expect("caller runtime");
            let pending = caller.spawn(async move {
                client
                    .send(request("localhost", "/", Duration::from_secs(60)))
                    .await
            });
            spawn.wait_reached(1);
            if pause_send {
                wait.wait_reached(1);
            }
            let (dropped_tx, dropped_rx) = mpsc::channel();
            let dropper = std::thread::spawn(move || {
                drop(runtime);
                let _ = dropped_tx.send(());
            });
            if pause_send {
                std::thread::sleep(Duration::from_millis(100));
                wait.release();
            }
            let result = caller
                .block_on(async { tokio::time::timeout(HANG_GUARD, pending).await })
                .expect("send resolves before the restart is released")
                .expect("send task");
            assert_worker_lost(result, false);
            assert!(dropped_rx.try_recv().is_err(), "drop waits for the restart");
            spawn.release();
            dropped_rx.recv_timeout(HANG_GUARD).expect("drop returns");
            dropper.join().expect("dropper");
            assert_eq!(live_threads(prefix), 0);
        }

        // A send first polled after the flag fails immediately, though a restart is still in
        // flight.
        let server = ok_server();
        let runtime = start(1, "rly");
        let spawn = TestPause::new();
        let _release = ReleaseOnDrop(spawn.clone());
        *runtime.hooks().spawn_pause.lock().unwrap() = Some(spawn.clone());
        force_close(&runtime, 0);
        let observer = runtime.client(plain_target(server.address));
        assert_eq!(observer.slot_health().state, SlotState::Closed);
        spawn.wait_reached(1);
        let mut client = runtime.client(plain_target(server.address));
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let dropper = std::thread::spawn(move || {
            drop(runtime);
            let _ = dropped_tx.send(());
        });
        wait_until("the shutdown flag", || {
            observer.slot_health().state == SlotState::Closed
        });
        assert_worker_lost(
            block_on(client.send(request("localhost", "/", Duration::from_secs(60)))),
            false,
        );
        assert!(dropped_rx.try_recv().is_err(), "drop waits for the restart");
        spawn.release();
        dropped_rx.recv_timeout(HANG_GUARD).expect("drop returns");
        dropper.join().expect("dropper");
        assert_eq!(live_threads("rly"), 0);
    }

    #[test]
    fn publication_waker_panic_leaves_restart_and_teardown_bookkeeping_complete() {
        const UNWIND_PANIC: &str = "the thread that drops the runtime is unwinding";
        let caller = current_thread_runtime();
        let waiting_request = || request("localhost", "/", Duration::from_secs(60));

        // The restart that publishes still counts its old worker's join, and the send it
        // published for proceeds on the new worker. The waker's payload panics when dropped: the
        // next restart, which joins the first one's thread, still publishes, and the last drop
        // returns.
        let server = ok_server();
        let runtime = start(1, "rpv");
        let spawn = TestPause::new();
        let _release = ReleaseOnDrop(spawn.clone());
        *runtime.hooks().spawn_pause.lock().unwrap() = Some(spawn.clone());
        force_close(&runtime, 0);
        let observer = runtime.client(plain_target(server.address));
        let mut client = runtime.client(plain_target(server.address));
        let mut send = pin!(client.send(waiting_request()));
        let waker = PanickingWaker::PanickingPayload;
        assert!(poll_with_panicking_waker(&caller, send.as_mut(), waker).is_pending());
        spawn.wait_reached(1);
        spawn.release();
        wait_until("the old worker's join to be counted", || {
            observer.slot_health().state == SlotState::Open && runtime.stats().restarts_joining == 0
        });
        assert_eq!(runtime.stats().restarts, 1);
        assert_ok(caller.block_on(send));
        wait_until("the restart thread's exit", || {
            live_threads("rpv-restart") == 0
        });
        force_close(&runtime, 0);
        let mut next = runtime.client(plain_target(server.address));
        assert_ok(caller.block_on(next.send(ok_request())));
        assert_eq!(runtime.stats().restarts, 2);
        drop(runtime);
        assert_eq!(live_threads("rpv"), 0);

        // The last drop still flags, signals, and joins every slot, disposes of a later panic
        // whose payload panics when dropped, and then resumes the first panic.
        let server = ok_server();
        let runtime = start(3, "rpx");
        let spawn = TestPause::new();
        let _release = ReleaseOnDrop(spawn.clone());
        *runtime.hooks().spawn_pause.lock().unwrap() = Some(spawn.clone());
        force_close(&runtime, 0);
        force_close(&runtime, 1);
        let mut first = runtime.client(plain_target(server.address));
        let mut second = runtime.client(plain_target(server.address));
        let mut third = runtime.client(plain_target(server.address));
        assert_eq!((second.slot(), third.slot()), (1, 2));
        let mut first_send = pin!(first.send(waiting_request()));
        let waker = PanickingWaker::Message;
        assert!(poll_with_panicking_waker(&caller, first_send.as_mut(), waker).is_pending());
        let mut second_send = pin!(second.send(waiting_request()));
        let waker = PanickingWaker::PanickingPayload;
        assert!(poll_with_panicking_waker(&caller, second_send.as_mut(), waker).is_pending());
        spawn.wait_reached(2);
        let dropper = std::thread::spawn(move || drop(runtime));
        wait_until("the shutdown flag on slot 2", || {
            third.slot_health().state == SlotState::Closed
        });
        assert!(!dropper.is_finished(), "the drop waits for the restarts");
        spawn.release();
        let panic = dropper
            .join()
            .expect_err("the drop resumes the first waker's panic");
        assert_eq!(panic.downcast_ref::<&str>(), Some(&WAKER_PANIC));
        assert_eq!(live_threads("rpx"), 0);
        assert_worker_lost(caller.block_on(first_send), false);
        assert_worker_lost(caller.block_on(second_send), false);
        assert_worker_lost(caller.block_on(third.send(ok_request())), false);
        assert_eq!(server.accepted(), 0);

        // A last drop on a thread that is already unwinding disposes of the waker's panic, whose
        // payload panics when dropped, instead of aborting the process.
        let server = ok_server();
        let runtime = start(1, "rpy");
        let spawn = TestPause::new();
        let _release = ReleaseOnDrop(spawn.clone());
        *runtime.hooks().spawn_pause.lock().unwrap() = Some(spawn.clone());
        force_close(&runtime, 0);
        let observer = runtime.client(plain_target(server.address));
        let mut client = runtime.client(plain_target(server.address));
        let mut send = pin!(client.send(waiting_request()));
        let waker = PanickingWaker::PanickingPayload;
        assert!(poll_with_panicking_waker(&caller, send.as_mut(), waker).is_pending());
        spawn.wait_reached(1);
        let dropper = std::thread::spawn(move || {
            catch_unwind(AssertUnwindSafe(move || {
                let _runtime = runtime;
                std::panic::panic_any(UNWIND_PANIC);
            }))
        });
        wait_until("the shutdown flag", || {
            observer.slot_health().state == SlotState::Closed
        });
        spawn.release();
        let unwind = dropper
            .join()
            .expect("the dropping thread finishes its unwind")
            .expect_err("the unwind continues past the drop");
        assert_eq!(unwind.downcast_ref::<&str>(), Some(&UNWIND_PANIC));
        assert_eq!(live_threads("rpy"), 0);
        assert_worker_lost(caller.block_on(send), false);
        assert_eq!(server.accepted(), 0);
    }

    #[test]
    fn publication_waker_panic_still_wakes_every_other_waiting_send() {
        const OTHERS: usize = 32;
        let caller = current_thread_runtime();
        let waiting_request = || request("localhost", "/", Duration::from_secs(60));

        // A restart that publishes wakes every other send waiting for it, though the first waker
        // it wakes panics, and each proceeds on the new worker.
        let server = ok_server();
        let runtime = start(1, "rwo");
        let spawn = TestPause::new();
        let _release = ReleaseOnDrop(spawn.clone());
        *runtime.hooks().spawn_pause.lock().unwrap() = Some(spawn.clone());
        force_close(&runtime, 0);
        let mut panicking = runtime.client(plain_target(server.address));
        let mut send = pin!(panicking.send(waiting_request()));
        let waker = PanickingWaker::Message;
        assert!(poll_with_panicking_waker(&caller, send.as_mut(), waker).is_pending());
        spawn.wait_reached(1);
        let mut clients: Vec<_> = (0..OTHERS)
            .map(|_| runtime.client(plain_target(server.address)))
            .collect();
        let mut others: Vec<_> = clients
            .iter_mut()
            .map(|client| Box::pin(client.send(waiting_request())))
            .collect();
        let wakers = poll_with_recording_wakers(&caller, &mut others);
        spawn.release();
        wait_until("every other waiting send's wake", || {
            wakers.iter().all(|waker| waker.woken())
        });
        assert_ok(caller.block_on(send));
        others
            .into_iter()
            .for_each(|send| assert_ok(caller.block_on(send)));
        drop(runtime);
        assert_eq!(live_threads("rwo"), 0);

        // The last drop's shutdown flag immediately wakes every other send waiting on the
        // flagged slot, long before their deadlines, and each fails as WorkerLost.
        let server = ok_server();
        let runtime = start(2, "rwp");
        let spawn = TestPause::new();
        let _release = ReleaseOnDrop(spawn.clone());
        *runtime.hooks().spawn_pause.lock().unwrap() = Some(spawn.clone());
        force_close(&runtime, 0);
        let mut clients: Vec<_> = (0..2 * (OTHERS + 1))
            .map(|_| runtime.client(plain_target(server.address)))
            .filter(|client| client.slot() == 0)
            .collect();
        let mut panicking = clients.pop().expect("a client on slot 0");
        let mut send = pin!(panicking.send(waiting_request()));
        let waker = PanickingWaker::Message;
        assert!(poll_with_panicking_waker(&caller, send.as_mut(), waker).is_pending());
        spawn.wait_reached(1);
        let mut others: Vec<_> = clients
            .iter_mut()
            .map(|client| Box::pin(client.send(waiting_request())))
            .collect();
        let wakers = poll_with_recording_wakers(&caller, &mut others);
        let dropper = std::thread::spawn(move || drop(runtime));
        wait_until("every other waiting send's wake", || {
            wakers.iter().all(|waker| waker.woken())
        });
        assert!(
            !dropper.is_finished(),
            "the drop waits for slot 0's restart"
        );
        spawn.release();
        let panic = dropper
            .join()
            .expect_err("the drop resumes the waker's panic");
        assert_eq!(panic.downcast_ref::<&str>(), Some(&WAKER_PANIC));
        assert_eq!(live_threads("rwp"), 0);
        assert_worker_lost(caller.block_on(send), false);
        others
            .into_iter()
            .for_each(|send| assert_worker_lost(caller.block_on(send), false));
        assert_eq!(server.accepted(), 0);
    }

    #[test]
    fn last_drop_from_a_publication_waker_on_its_restart_thread_returns() {
        // One- and two-slot runtimes, a waker that panics with a `PanickingPayload` after its
        // drop, and a control whose last clone the test thread drops.
        for (prefix, workers, extra_clone, panic_after) in [
            ("rqa", 1, false, false),
            ("rqb", 2, false, false),
            ("rqp", 1, false, true),
            ("rqc", 1, true, false),
        ] {
            let server = ok_server();
            let runtime = start(workers, prefix);
            let hooks = Arc::clone(runtime.hooks());
            let barrier = TestPause::new();
            let _release_barrier = ReleaseOnDrop(barrier.clone());
            hold_cleanup(&runtime, 0, &barrier);
            let spawn = TestPause::new();
            let _release_spawn = ReleaseOnDrop(spawn.clone());
            *hooks.spawn_pause.lock().unwrap() = Some(spawn.clone());
            force_close(&runtime, 0);
            let mut client = runtime.client(plain_target(server.address));
            assert_eq!(client.slot(), 0);
            let extra = extra_clone.then(|| runtime.clone());
            let waker = DroppingWaker::new(runtime, prefix, panic_after);
            let caller = current_thread_runtime();
            let mut send = pin!(client.send(waiting_request()));
            let poll = poll_with_waker(&caller, send.as_mut(), Waker::from(Arc::clone(&waker)));
            assert!(poll.is_pending(), "the send waits for the restart");
            spawn.wait_reached(1);
            spawn.release();

            let (thread, alive) = waker.dropped();
            assert_eq!(
                thread,
                format!("{prefix}-restart-0"),
                "woken by publication"
            );
            match extra {
                None => {
                    // Only the restart thread and the old worker, which that thread joins once
                    // the barrier is released, are left: every other worker is already joined.
                    assert_eq!(alive, 2, "{prefix}");
                    barrier.release();
                    wait_until("the restart thread's exit", || live_threads(prefix) == 0);
                }
                Some(extra) => {
                    barrier.release();
                    drop(extra);
                    assert_eq!(live_threads(prefix), 0);
                }
            }
            assert_worker_lost(caller.block_on(send), false);
            assert_eq!(
                *hooks.join_threads.lock().unwrap(),
                [format!("{prefix}-restart-0")]
            );
            assert_eq!(server.accepted(), 0);
        }
    }

    #[test]
    fn last_drop_from_a_publication_waker_waits_for_its_requesters_registration() {
        for (prefix, extra_clone) in [("rqr", false), ("rqq", true)] {
            let server = ok_server();
            let runtime = start(1, prefix);
            let hooks = Arc::clone(runtime.hooks());
            let barrier = TestPause::new();
            let _release_barrier = ReleaseOnDrop(barrier.clone());
            hold_cleanup(&runtime, 0, &barrier);
            let spawn = TestPause::new();
            let _release_spawn = ReleaseOnDrop(spawn.clone());
            *hooks.spawn_pause.lock().unwrap() = Some(spawn.clone());
            let registration = TestPause::new();
            let _release_registration = ReleaseOnDrop(registration.clone());
            *hooks.registration_pause.lock().unwrap() = Some(registration.clone());
            force_close(&runtime, 0);
            let health = runtime.client(plain_target(server.address));
            // A name outside `{prefix}-`, so the waker does not count the requester.
            let requester = std::thread::Builder::new()
                .name(format!("{prefix}_requester"))
                .spawn(move || health.slot_health().state)
                .expect("requester thread");
            registration.wait_reached(1);
            spawn.wait_reached(1);
            let observer = runtime.client(plain_target(server.address));
            let mut client = runtime.client(plain_target(server.address));
            assert_eq!(client.slot(), 0);
            let extra = extra_clone.then(|| runtime.clone());
            let waker = DroppingWaker::new(runtime, prefix, false);
            let caller = current_thread_runtime();
            let mut send = pin!(client.send(waiting_request()));
            let poll = poll_with_waker(&caller, send.as_mut(), Waker::from(Arc::clone(&waker)));
            assert!(poll.is_pending(), "the send waits for the restart");
            spawn.release();
            if extra.is_none() {
                // The drop on the restart thread has set the shutdown flag, and the requester,
                // held before it registers that thread, still holds its registration step.
                wait_for_state(&observer, SlotState::Closed);
                std::thread::sleep(Duration::from_millis(100));
                assert!(
                    waker.dropped.lock().unwrap().is_none(),
                    "the drop waits for the requester's registration"
                );
            }
            registration.release();

            let (thread, alive) = waker.dropped();
            assert_eq!(
                thread,
                format!("{prefix}-restart-0"),
                "woken by publication"
            );
            assert_eq!(requester.join().expect("requester"), SlotState::Closed);
            match extra {
                None => {
                    // The drop leaves the restart thread's registered handle unjoined, and the
                    // held old worker is the only other thread.
                    assert_eq!(alive, 2, "{prefix}");
                    barrier.release();
                    wait_until("the restart thread's exit", || live_threads(prefix) == 0);
                }
                Some(extra) => {
                    barrier.release();
                    drop(extra);
                    assert_eq!(live_threads(prefix), 0);
                }
            }
            assert_worker_lost(caller.block_on(send), false);
            assert_eq!(
                *hooks.join_threads.lock().unwrap(),
                [format!("{prefix}-restart-0")]
            );
            assert_eq!(server.accepted(), 0);
        }
    }

    #[test]
    fn last_drop_from_a_failed_restarts_waker_on_its_restart_thread_returns() {
        for (prefix, extra_clone) in [("rqf", false), ("rqg", true)] {
            let runtime = start(1, prefix);
            let hooks = Arc::clone(runtime.hooks());
            let spawn = TestPause::new();
            let _release = ReleaseOnDrop(spawn.clone());
            *hooks.spawn_pause.lock().unwrap() = Some(spawn.clone());
            hooks.worker_spawn_failures.lock().unwrap().push_back(true);
            force_close(&runtime, 0);
            let mut client = runtime.client(plain_target("127.0.0.1:1".parse().unwrap()));
            let extra = extra_clone.then(|| runtime.clone());
            let waker = DroppingWaker::new(runtime, prefix, false);
            let caller = current_thread_runtime();
            let mut send = pin!(client.send(waiting_request()));
            let poll = poll_with_waker(&caller, send.as_mut(), Waker::from(Arc::clone(&waker)));
            assert!(poll.is_pending(), "the send waits for the restart");
            spawn.wait_reached(1);
            spawn.release();

            let (thread, alive) = waker.dropped();
            assert_eq!(
                thread,
                format!("{prefix}-restart-0"),
                "woken by the failure"
            );
            match extra {
                None => {
                    // The closed worker is joined; only the restart thread is left.
                    assert_eq!(alive, 1, "{prefix}");
                    wait_until("the restart thread's exit", || live_threads(prefix) == 0);
                }
                Some(extra) => {
                    drop(extra);
                    assert_eq!(live_threads(prefix), 0);
                }
            }
            assert_worker_lost(caller.block_on(send), false);
        }
    }

    #[test]
    fn last_drop_from_a_failed_thread_spawns_waker_on_the_requester_returns() {
        for (prefix, extra_clone) in [("rqs", false), ("rqt", true)] {
            let runtime = start(1, prefix);
            let hooks = Arc::clone(runtime.hooks());
            let requested = TestPause::new();
            let _release = ReleaseOnDrop(requested.clone());
            *hooks.requester_pause.lock().unwrap() = Some(requested.clone());
            hooks.fail_next_thread_spawns.store(1, Ordering::SeqCst);
            force_close(&runtime, 0);
            let target = plain_target("127.0.0.1:1".parse().unwrap());
            let health = runtime.client(Arc::clone(&target));
            // A name outside `{prefix}-`, so the waker does not count the requester.
            let requester = std::thread::Builder::new()
                .name(format!("{prefix}_requester"))
                .spawn(move || health.slot_health().state)
                .expect("requester thread");
            requested.wait_reached(1);
            let mut client = runtime.client(target);
            let extra = extra_clone.then(|| runtime.clone());
            let waker = DroppingWaker::new(runtime, prefix, false);
            let caller = current_thread_runtime();
            let mut send = pin!(client.send(waiting_request()));
            let poll = poll_with_waker(&caller, send.as_mut(), Waker::from(Arc::clone(&waker)));
            assert!(poll.is_pending(), "the send waits for the admitted restart");
            requested.release();

            let (thread, alive) = waker.dropped();
            assert_eq!(thread, format!("{prefix}_requester"), "woken by the guard");
            if extra.is_none() {
                assert_eq!(
                    alive, 0,
                    "the drop on the requester joins every runtime thread"
                );
            }
            assert_eq!(requester.join().expect("requester"), SlotState::Closed);
            drop(extra);
            assert_eq!(live_threads(prefix), 0);
            assert_worker_lost(caller.block_on(send), false);
        }
    }

    /// Where a slot's restart stands when a `WorkerCall` waker on its retiring worker's owner
    /// thread drops the last runtime clone.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Retiring {
        /// The restart has published and is joining the retiring worker.
        Joining,
        /// The restart is held before it publishes.
        BeforeSwap,
        /// The restart is joining the retiring worker, and a later restart of the slot is held.
        LaterRestart,
    }

    #[test]
    fn last_drop_from_a_worker_call_waker_on_a_retiring_worker_returns() {
        // `joins` counts the restarts that publish and then join their old worker.
        for (prefix, retiring, extra_clone, joins) in [
            ("rqw", Retiring::Joining, false, 1),
            ("rqx", Retiring::Joining, true, 1),
            ("rqy", Retiring::BeforeSwap, false, 0),
            ("rqz", Retiring::BeforeSwap, true, 1),
            ("rqj", Retiring::LaterRestart, false, 1),
            ("rqk", Retiring::LaterRestart, true, 2),
        ] {
            let runtime = start(1, prefix);
            let hooks = Arc::clone(runtime.hooks());
            let connect = TestPause::new();
            let _release_connect = ReleaseOnDrop(connect.clone());
            *hooks.connect_pause.lock().unwrap() = Some(connect.clone());
            let spawn = TestPause::new();
            let _release_spawn = ReleaseOnDrop(spawn.clone());
            let old = runtime.worker_handle(0).expect("published worker");
            let target = plain_target("127.0.0.1:1".parse().unwrap());
            let observer = runtime.client(Arc::clone(&target));
            let mut client = runtime.client(target);
            let extra = extra_clone.then(|| runtime.clone());
            let waker = DroppingWaker::new(runtime, prefix, false);
            let caller = current_thread_runtime();
            let mut send = pin!(client.send(waiting_request()));
            let poll = poll_with_waker(&caller, send.as_mut(), Waker::from(Arc::clone(&waker)));
            assert!(poll.is_pending(), "the send waits for its exchange");
            // The exchange holds the old worker's owner thread before its dial.
            connect.wait_reached(1);
            if retiring == Retiring::BeforeSwap {
                *hooks.spawn_pause.lock().unwrap() = Some(spawn.clone());
            }
            old.begin_shutdown();
            assert_eq!(observer.slot_health().state, SlotState::Closed);
            if retiring == Retiring::BeforeSwap {
                spawn.wait_reached(1);
            } else {
                wait_until("the restart thread's join of the old worker", || {
                    !hooks.join_threads.lock().unwrap().is_empty()
                });
            }
            if retiring == Retiring::LaterRestart {
                let published = waker
                    .runtime
                    .lock()
                    .unwrap()
                    .as_ref()
                    .expect("the waker's runtime")
                    .worker_handle(0)
                    .expect("published worker");
                *hooks.spawn_pause.lock().unwrap() = Some(spawn.clone());
                published.begin_shutdown();
                assert_eq!(observer.slot_health().state, SlotState::Closed);
                spawn.wait_reached(1);
            }
            connect.release();
            if retiring != Retiring::Joining {
                if extra.is_none() {
                    // The drop on the old worker's owner thread has set the shutdown flag and
                    // waits for the held restart, which does not join a worker of that thread.
                    wait_for_state(&observer, SlotState::Closed);
                    std::thread::sleep(Duration::from_millis(100));
                    assert!(
                        waker.dropped.lock().unwrap().is_none(),
                        "{prefix}: the drop waits for the held restart"
                    );
                }
                spawn.release();
                if extra.is_some() {
                    wait_for_state(&observer, SlotState::Open);
                }
            }

            let (thread, alive) = waker.dropped();
            assert_eq!(thread, format!("{prefix}-0"), "woken on the old worker");
            match extra {
                None => {
                    // The old worker's owner thread is left, with the restart thread joining it
                    // once a restart has published; every other worker and restart thread,
                    // including a held restart, is already joined.
                    assert_eq!(alive, 1 + joins, "{prefix}");
                    wait_until("the old worker's and restart thread's exit", || {
                        live_threads(prefix) == 0
                    });
                }
                Some(extra) => {
                    drop(extra);
                    assert_eq!(live_threads(prefix), 0);
                }
            }
            assert_worker_lost(caller.block_on(send), false);
            assert_eq!(
                *hooks.join_threads.lock().unwrap(),
                vec![format!("{prefix}-restart-0"); joins]
            );
        }
    }

    #[test]
    fn last_drop_from_a_worker_call_waker_on_its_published_worker_returns() {
        for (prefix, workers, extra_clone) in
            [("rqo", 1, false), ("rqm", 2, false), ("rqn", 1, true)]
        {
            let (server, read_rx, release) = parking_server();
            let runtime = start(workers, prefix);
            let mut client = runtime.client(plain_target(server.address));
            assert_eq!(client.slot(), 0);
            let extra = extra_clone.then(|| runtime.clone());
            let waker = DroppingWaker::new(runtime, prefix, false);
            let caller = current_thread_runtime();
            let mut send = pin!(client.send(waiting_request()));
            let poll = poll_with_waker(&caller, send.as_mut(), Waker::from(Arc::clone(&waker)));
            assert!(poll.is_pending(), "the send waits for its exchange");
            read_rx
                .recv_timeout(HANG_GUARD)
                .expect("server read the request");
            release.release();

            let (thread, alive) = waker.dropped();
            assert_eq!(
                thread,
                format!("{prefix}-0"),
                "woken on the published worker"
            );
            match extra {
                None => {
                    // `HostedWorker::drop` skips only the join of the owner thread that runs it,
                    // so slot 1's worker is already joined.
                    assert_eq!(alive, 1, "{prefix}");
                    wait_until("the worker's exit", || live_threads(prefix) == 0);
                }
                Some(extra) => {
                    drop(extra);
                    assert_eq!(live_threads(prefix), 0);
                }
            }
            assert_ok(caller.block_on(send));
            assert_eq!(server.accepted(), 1);
        }
    }

    /// Where the last runtime clone is dropped while slot 0's restart joins its old worker.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum JoiningDrop {
        /// On a thread that the runtime does not own.
        Foreign,
        /// From the response's `WorkerCall` waker on the published worker's owner thread.
        PublishedWorker,
    }

    #[test]
    fn last_drop_waits_for_a_restart_joining_another_threads_worker() {
        for (prefix, from, extra_clone) in [
            ("rqi", JoiningDrop::Foreign, false),
            ("rql", JoiningDrop::PublishedWorker, false),
            ("rqu", JoiningDrop::PublishedWorker, true),
        ] {
            let (server, read_rx, release) = parking_server();
            let runtime = start(1, prefix);
            let hooks = Arc::clone(runtime.hooks());
            let barrier = TestPause::new();
            let _release_barrier = ReleaseOnDrop(barrier.clone());
            hold_cleanup(&runtime, 0, &barrier);
            force_close(&runtime, 0);
            let observer = runtime.client(plain_target(server.address));
            assert_eq!(observer.slot_health().state, SlotState::Closed);
            wait_until("the restart thread's join of the old worker", || {
                !hooks.join_threads.lock().unwrap().is_empty()
            });

            match from {
                JoiningDrop::Foreign => {
                    let (dropped_tx, dropped_rx) = mpsc::channel();
                    let dropper = std::thread::spawn(move || {
                        drop(runtime);
                        let _ = dropped_tx.send(());
                    });
                    assert!(
                        dropped_rx.recv_timeout(Duration::from_millis(100)).is_err(),
                        "{prefix}: the drop waits for the restart joining the old worker"
                    );
                    barrier.release();
                    dropped_rx.recv_timeout(HANG_GUARD).expect("drop returns");
                    dropper.join().expect("dropper");
                    assert_eq!(live_threads(prefix), 0);
                }
                JoiningDrop::PublishedWorker => {
                    let mut client = runtime.client(plain_target(server.address));
                    let extra = extra_clone.then(|| runtime.clone());
                    let waker = DroppingWaker::new(runtime, prefix, false);
                    let caller = current_thread_runtime();
                    let mut send = pin!(client.send(waiting_request()));
                    let poll =
                        poll_with_waker(&caller, send.as_mut(), Waker::from(Arc::clone(&waker)));
                    assert!(poll.is_pending(), "the send waits for its exchange");
                    read_rx
                        .recv_timeout(HANG_GUARD)
                        .expect("server read the request");
                    release.release();
                    if extra.is_none() {
                        // The drop on the published worker's owner thread has set the shutdown
                        // flag and waits for the restart, which joins another thread's worker.
                        wait_for_state(&observer, SlotState::Closed);
                        std::thread::sleep(Duration::from_millis(100));
                        assert!(
                            waker.dropped.lock().unwrap().is_none(),
                            "{prefix}: the drop waits for the restart joining the old worker"
                        );
                    }
                    barrier.release();

                    let (thread, alive) = waker.dropped();
                    assert_eq!(
                        thread,
                        format!("{prefix}-0"),
                        "woken on the published worker"
                    );
                    match extra {
                        None => {
                            // The restart thread, and with it the old worker, is joined.
                            assert_eq!(alive, 1, "{prefix}");
                            wait_until("the worker's exit", || live_threads(prefix) == 0);
                        }
                        Some(extra) => {
                            drop(extra);
                            assert_eq!(live_threads(prefix), 0);
                        }
                    }
                    assert_ok(caller.block_on(send));
                    assert_eq!(server.accepted(), 1);
                }
            }
            assert_eq!(
                *hooks.join_threads.lock().unwrap(),
                [format!("{prefix}-restart-0")]
            );
        }
    }

    #[test]
    fn last_drop_from_a_replaced_worker_call_waker_returns() {
        // A poll with a waker that will not wake the same task replaces the one the send holds
        // for its `WorkerCall`, and drops it with no lock held that the worker takes.
        for (prefix, extra_clone) in [("rqe", false), ("rqh", true)] {
            let (server, read_rx, release) = parking_server();
            let runtime = start(1, prefix);
            let mut client = runtime.client(plain_target(server.address));
            let extra = extra_clone.then(|| runtime.clone());
            let dropped = Arc::new(Mutex::new(None));
            let waker = Waker::from(Arc::new(OwningWaker {
                runtime: Some(runtime),
                prefix,
                dropped: Arc::clone(&dropped),
            }));
            let caller = Arc::new(current_thread_runtime());
            let mut send = Box::pin(async move { client.send(waiting_request()).await });
            let poll = poll_with_waker(&caller, send.as_mut(), waker);
            assert!(poll.is_pending(), "the send waits for its exchange");
            read_rx
                .recv_timeout(HANG_GUARD)
                .expect("server read the request");

            // A name outside `{prefix}-`, so the waker does not count the poller.
            let (polled_tx, polled_rx) = mpsc::channel();
            let poller_caller = Arc::clone(&caller);
            let poller = std::thread::Builder::new()
                .name(format!("{prefix}_poller"))
                .spawn(move || {
                    let recording = Waker::from(Arc::new(RecordingWaker::default()));
                    let poll = poll_with_waker(&poller_caller, send.as_mut(), recording);
                    let _ = polled_tx.send((poll.is_pending(), send));
                })
                .expect("poller thread");
            let (pending, send) = polled_rx
                .recv_timeout(HANG_GUARD)
                .expect("the poll that replaces the waker returns");
            poller.join().expect("poller");
            assert!(pending, "the send still waits for its exchange");

            let (thread, alive) = dropped
                .lock()
                .unwrap()
                .clone()
                .expect("the replaced waker was dropped");
            assert_eq!(thread, format!("{prefix}_poller"), "dropped by the poll");
            match extra {
                None => {
                    // The poller is not a runtime thread, so the teardown joined every one.
                    assert_eq!(alive, 0, "{prefix}");
                    assert_worker_lost(caller.block_on(send), true);
                }
                Some(extra) => {
                    release.release();
                    assert_ok(caller.block_on(send));
                    drop(extra);
                    assert_eq!(live_threads(prefix), 0);
                }
            }
            assert_eq!(server.accepted(), 1);
        }
    }

    /// Which callback of the armed `RawWaker` instance releases the only runtime clone.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum SweepRelease {
        /// The k-th clone callback, before it returns its new instance.
        Clone,
        /// The instance that the k-th clone made, when its token is consumed: by its drop
        /// callback, or by a by-value wake, which wakes and then drops.
        Drop,
    }

    /// How a sweep run's script ends after its second poll.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum SweepEnd {
        /// The server responds, and a third poll with the second poll's waker resolves the send.
        Resolve,
        /// The send is dropped unresolved.
        Drop,
    }

    /// What every `RawWaker` instance of one sweep run shares.
    struct SweepFamily {
        prefix: &'static str,
        runtime: Mutex<Option<ExplicitRuntime>>,
        /// Clone callbacks run so far. The test's own wakers are not clones.
        clones: AtomicUsize,
        /// The armed clone, counted from 1, or 0 for an unarmed run.
        armed: usize,
        release: SweepRelease,
        woken: AtomicBool,
        /// The thread that released the runtime, and how many threads named by `prefix` were
        /// alive once that release returned.
        released: Mutex<Option<(String, usize)>>,
    }

    /// One `RawWaker` instance, an independent token in its own allocation: a clone makes a new
    /// instance over the same family, and a by-value wake or a drop consumes one.
    struct SweepInstance {
        family: Arc<SweepFamily>,
        releases: bool,
    }

    /// The vtable of every sweep waker. Each waker's data is a `SweepInstance` that
    /// `sweep_raw` leaked and that only `sweep_wake` or `sweep_drop` frees, which `Waker`'s
    /// contract runs exactly once per waker, after which it runs no other callback with that
    /// data.
    static SWEEP_VTABLE: RawWakerVTable =
        RawWakerVTable::new(sweep_clone, sweep_wake, sweep_wake_by_ref, sweep_drop);

    impl SweepFamily {
        /// A waker that the test owns, which does not release the runtime itself.
        fn root(self: &Arc<Self>) -> Waker {
            let instance = Box::new(SweepInstance {
                family: Arc::clone(self),
                releases: false,
            });
            // SAFETY: `RawWaker`'s contract holds for `SWEEP_VTABLE`: its data is a leaked
            // `SweepInstance` that stays live until the one by-value wake or drop that frees it,
            // and `SweepInstance` is `Send + Sync`, so any thread may run the callbacks.
            unsafe { Waker::from_raw(sweep_raw(instance)) }
        }

        fn release_runtime(&self) {
            let Some(runtime) = self.runtime.lock().unwrap().take() else {
                return;
            };
            drop(runtime);
            let thread = std::thread::current().name().unwrap_or_default().to_owned();
            *self.released.lock().unwrap() = Some((thread, live_threads(self.prefix)));
        }
    }

    fn sweep_raw(instance: Box<SweepInstance>) -> RawWaker {
        RawWaker::new(Box::into_raw(instance).cast_const().cast(), &SWEEP_VTABLE)
    }

    /// # Safety
    ///
    /// `data` is the data of a live sweep waker, and the instance is not freed while the
    /// returned reference is used.
    unsafe fn sweep_instance<'a>(data: *const ()) -> &'a SweepInstance {
        // SAFETY: a live sweep waker's data points to its leaked, not yet freed `SweepInstance`
        // (`SWEEP_VTABLE`), which the caller keeps alive while the reference is used.
        unsafe { &*data.cast::<SweepInstance>() }
    }

    /// # Safety
    ///
    /// `data` is the data of a live sweep waker, as `RawWakerVTable` passes to `clone`.
    unsafe fn sweep_clone(data: *const ()) -> RawWaker {
        // SAFETY: the waker being cloned stays live, and its instance unfreed, for the whole
        // clone callback.
        let family = &unsafe { sweep_instance(data) }.family;
        let armed = family.clones.fetch_add(1, Ordering::SeqCst) + 1 == family.armed;
        if armed && family.release == SweepRelease::Clone {
            family.release_runtime();
        }
        sweep_raw(Box::new(SweepInstance {
            family: Arc::clone(family),
            releases: armed && family.release == SweepRelease::Drop,
        }))
    }

    /// # Safety
    ///
    /// `data` is the data of a live sweep waker that this call consumes, as `RawWakerVTable`
    /// passes to `wake`.
    unsafe fn sweep_wake(data: *const ()) {
        // SAFETY: the woken waker is live until this call consumes it, which `sweep_drop` does
        // last, after `sweep_wake_by_ref` has returned.
        unsafe {
            sweep_wake_by_ref(data);
            sweep_drop(data);
        }
    }

    /// # Safety
    ///
    /// `data` is the data of a live sweep waker, as `RawWakerVTable` passes to `wake_by_ref`.
    unsafe fn sweep_wake_by_ref(data: *const ()) {
        // SAFETY: the waker woken by reference stays live, and its instance unfreed, for the
        // whole callback.
        let family = &unsafe { sweep_instance(data) }.family;
        family.woken.store(true, Ordering::SeqCst);
    }

    /// # Safety
    ///
    /// `data` is the data of a live sweep waker that this call consumes, as `RawWakerVTable`
    /// passes to `drop` (or `sweep_wake` passes on).
    unsafe fn sweep_drop(data: *const ()) {
        // SAFETY: `data` came from `Box::into_raw` in `sweep_raw`, and `Waker`'s contract runs
        // this, or the by-value wake that ends in it, once per waker and no callback after it,
        // so the box is freed exactly once.
        let instance = unsafe { Box::from_raw(data.cast_mut().cast::<SweepInstance>()) };
        if instance.releases {
            instance.family.release_runtime();
        }
    }

    /// Runs a sweep run's steps, each on a new thread named `{prefix}_{step}` and bounded by
    /// the hang guard.
    struct SweepSteps {
        case: String,
        prefix: &'static str,
        caller: Arc<tokio::runtime::Runtime>,
    }

    impl SweepSteps {
        fn run<Output: Send + 'static>(
            &self,
            step: &str,
            work: impl FnOnce() -> Output + Send + 'static,
        ) -> Output {
            let name = format!("{}_{step}", self.prefix);
            let (done_tx, done_rx) = mpsc::channel();
            let thread = std::thread::Builder::new()
                .name(name.clone())
                .spawn(move || {
                    let _ = done_tx.send(work());
                })
                .expect("step thread");
            let output = done_rx
                .recv_timeout(HANG_GUARD)
                .unwrap_or_else(|error| panic!("{}: {name}'s step returns: {error:?}", self.case));
            thread.join().expect("step thread");
            output
        }

        /// Polls `send` once with `waker` inside the caller's context, and returns the waker with
        /// the send.
        fn poll<Work>(
            &self,
            step: &str,
            mut send: Pin<Box<Work>>,
            waker: Waker,
        ) -> (Poll<Work::Output>, Pin<Box<Work>>, Waker)
        where
            Work: Future + Send + 'static,
            Work::Output: Send,
        {
            let caller = Arc::clone(&self.caller);
            self.run(step, move || {
                let _context = caller.enter();
                let poll = send.as_mut().poll(&mut Context::from_waker(&waker));
                (poll, send, waker)
            })
        }
    }

    /// One run of the sweep's script: a first poll, a second poll with another waker on a thread
    /// named outside the prefix, and then the response and a resolving poll with that waker, or
    /// the send's drop.
    struct SweepRun {
        prefix: &'static str,
        end: SweepEnd,
        release: SweepRelease,
        /// The armed clone, counted from 1, or 0 for an unarmed run.
        armed: usize,
        /// Whether the test holds a runtime clone besides the family's, as a control does.
        extra_clone: bool,
    }

    impl SweepRun {
        /// Runs the script and returns the clone callbacks run.
        fn run(self) -> usize {
            let Self {
                prefix,
                end,
                release,
                armed,
                extra_clone,
            } = self;
            let case = format!("{prefix} {end:?} {release:?} k={armed}");
            // A parking server whose client may close before its request, as an early release
            // does.
            let (read_tx, read_rx) = mpsc::channel();
            let response = Arc::new(TestPause::new());
            let gate = Arc::clone(&response);
            let mut server = TestServer::plain(move |mut conn| {
                if conn.try_read_request().is_none() {
                    return;
                }
                let _ = read_tx.send(());
                gate.pause_blocking();
                let _ = conn.write(OK.as_bytes());
                conn.sees_close(HANG_GUARD);
            });
            server.release_on_drop(TestPause::clone(&response));
            let runtime = start(1, prefix);
            let mut client = runtime.client(plain_target(server.address));
            let extra = extra_clone.then(|| runtime.clone());
            let family = Arc::new(SweepFamily {
                prefix,
                runtime: Mutex::new(Some(runtime)),
                clones: AtomicUsize::new(0),
                armed,
                release,
                woken: AtomicBool::new(false),
                released: Mutex::new(None),
            });
            let steps = SweepSteps {
                case: case.clone(),
                prefix,
                caller: Arc::new(current_thread_runtime()),
            };
            let send = Box::pin(async move { client.send(waiting_request()).await });

            let (poll, send, first) = steps.poll("first", send, family.root());
            drop(first);
            let result = match poll {
                Poll::Ready(result) => Some(result),
                Poll::Pending => {
                    // A release in the first poll has already shut the worker down.
                    if family.released.lock().unwrap().is_none() {
                        read_rx
                            .recv_timeout(HANG_GUARD)
                            .expect("server read the request");
                    }
                    let (poll, send, second) = steps.poll("poller", send, family.root());
                    match (poll, end) {
                        (Poll::Ready(result), _) => Some(result),
                        (Poll::Pending, SweepEnd::Resolve) => {
                            response.release();
                            wait_until("the send's wake", || family.woken.load(Ordering::SeqCst));
                            let (poll, send, second) = steps.poll("resolver", send, second);
                            drop((send, second));
                            let Poll::Ready(result) = poll else {
                                panic!("{case}: the woken send resolves");
                            };
                            Some(result)
                        }
                        (Poll::Pending, SweepEnd::Drop) => {
                            steps.run("dropper", move || drop((send, second)));
                            None
                        }
                    }
                }
            };

            if armed > 0 {
                // A worker may still hold an instance that the send gave it, and consume it later.
                wait_until("the armed callback's release", || {
                    family.released.lock().unwrap().is_some()
                });
            }
            let released = family.released.lock().unwrap().take();
            match (armed, extra) {
                (0, extra) => {
                    assert!(released.is_none(), "{case}: nothing is armed");
                    if end == SweepEnd::Resolve {
                        assert_ok(result.expect("resolved"));
                    }
                    drop(family.runtime.lock().unwrap().take());
                    drop(extra);
                }
                (_, Some(extra)) => {
                    // The armed callback dropped a clone that was not the last.
                    if end == SweepEnd::Resolve {
                        assert_ok(result.expect("resolved"));
                    }
                    drop(extra);
                }
                (_, None) => {
                    let (thread, alive) = released.expect("released");
                    if thread.starts_with(&format!("{prefix}-")) {
                        // Only the published worker's owner thread, which `HostedWorker::drop`
                        // leaves unjoined when it runs there.
                        assert_eq!((thread, alive), (format!("{prefix}-0"), 1), "{case}");
                        wait_until("the worker's exit", || live_threads(prefix) == 0);
                    } else {
                        assert_eq!(alive, 0, "{case}: released on {thread}");
                    }
                    if let Some(Err(error)) = result {
                        assert_eq!(error.kind, ErrorKind::WorkerLost, "{case}: {error}");
                    }
                }
            }
            assert_eq!(live_threads(prefix), 0, "{case}");
            family.clones.load(Ordering::SeqCst)
        }
    }

    /// Sweeps `release` over every clone callback that a send runs in the unarmed script, each
    /// armed run with an extra-clone control.
    fn sweep(release: SweepRelease, prefix: &'static str, control: &'static str) {
        for end in [SweepEnd::Resolve, SweepEnd::Drop] {
            let run = |prefix, armed, extra_clone| {
                SweepRun {
                    prefix,
                    end,
                    release,
                    armed,
                    extra_clone,
                }
                .run()
            };
            let clones = run(prefix, 0, false);
            assert!(clones > 0, "{end:?}: the send clones its caller's waker");
            for armed in 1..=clones {
                run(prefix, armed, false);
                run(control, armed, true);
            }
        }
    }

    #[test]
    fn last_drop_from_a_raw_waker_clone_callback_returns() {
        sweep(SweepRelease::Clone, "rka", "rkb");
    }

    #[test]
    fn last_drop_from_a_raw_waker_drop_callback_returns() {
        sweep(SweepRelease::Drop, "rkc", "rkd");
    }

    /// A reference `convert_headers` that copies every name and value.
    fn copying_reference(headers: &HttpHeaders) -> (HeaderMap, usize) {
        let mut converted = HeaderMap::try_with_capacity(headers.len()).unwrap_or_default();
        let mut dropped = 0;
        headers.for_each(|name, value| {
            let appended = HeaderName::from_bytes(name)
                .ok()
                .zip(HeaderValue::from_bytes(value).ok())
                .is_some_and(|(name, value)| converted.try_append(name, value).is_ok());
            if !appended {
                dropped += 1;
            }
        });
        (converted, dropped)
    }

    /// Every `http::header` constant.
    fn http_header_constants() -> Vec<HeaderName> {
        vec![
            header::ACCEPT,
            header::ACCEPT_CHARSET,
            header::ACCEPT_ENCODING,
            header::ACCEPT_LANGUAGE,
            header::ACCEPT_RANGES,
            header::ACCESS_CONTROL_ALLOW_CREDENTIALS,
            header::ACCESS_CONTROL_ALLOW_HEADERS,
            header::ACCESS_CONTROL_ALLOW_METHODS,
            header::ACCESS_CONTROL_ALLOW_ORIGIN,
            header::ACCESS_CONTROL_EXPOSE_HEADERS,
            header::ACCESS_CONTROL_MAX_AGE,
            header::ACCESS_CONTROL_REQUEST_HEADERS,
            header::ACCESS_CONTROL_REQUEST_METHOD,
            header::AGE,
            header::ALLOW,
            header::ALT_SVC,
            header::AUTHORIZATION,
            header::CACHE_CONTROL,
            header::CACHE_STATUS,
            header::CDN_CACHE_CONTROL,
            header::CONNECTION,
            header::CONTENT_DISPOSITION,
            header::CONTENT_ENCODING,
            header::CONTENT_LANGUAGE,
            header::CONTENT_LENGTH,
            header::CONTENT_LOCATION,
            header::CONTENT_RANGE,
            header::CONTENT_SECURITY_POLICY,
            header::CONTENT_SECURITY_POLICY_REPORT_ONLY,
            header::CONTENT_TYPE,
            header::COOKIE,
            header::DNT,
            header::DATE,
            header::ETAG,
            header::EXPECT,
            header::EXPIRES,
            header::FORWARDED,
            header::FROM,
            header::HOST,
            header::IF_MATCH,
            header::IF_MODIFIED_SINCE,
            header::IF_NONE_MATCH,
            header::IF_RANGE,
            header::IF_UNMODIFIED_SINCE,
            header::LAST_MODIFIED,
            header::LINK,
            header::LOCATION,
            header::MAX_FORWARDS,
            header::ORIGIN,
            header::PRAGMA,
            header::PROXY_AUTHENTICATE,
            header::PROXY_AUTHORIZATION,
            header::PUBLIC_KEY_PINS,
            header::PUBLIC_KEY_PINS_REPORT_ONLY,
            header::RANGE,
            header::REFERER,
            header::REFERRER_POLICY,
            header::REFRESH,
            header::RETRY_AFTER,
            header::SEC_WEBSOCKET_ACCEPT,
            header::SEC_WEBSOCKET_EXTENSIONS,
            header::SEC_WEBSOCKET_KEY,
            header::SEC_WEBSOCKET_PROTOCOL,
            header::SEC_WEBSOCKET_VERSION,
            header::SERVER,
            header::SET_COOKIE,
            header::STRICT_TRANSPORT_SECURITY,
            header::TE,
            header::TRAILER,
            header::TRANSFER_ENCODING,
            header::USER_AGENT,
            header::UPGRADE,
            header::UPGRADE_INSECURE_REQUESTS,
            header::VARY,
            header::VIA,
            header::WARNING,
            header::WWW_AUTHENTICATE,
            header::X_CONTENT_TYPE_OPTIONS,
            header::X_DNS_PREFETCH_CONTROL,
            header::X_FRAME_OPTIONS,
            header::X_XSS_PROTECTION,
        ]
    }

    /// Every code once in order and once in reverse, each with its own value.
    fn every_code() -> HttpHeaders {
        let mut headers = HttpHeaders::new();
        HttpHeaderCode::ALL
            .iter()
            .for_each(|code| headers.add(*code, format!("first-{}", code.as_u8())));
        HttpHeaderCode::ALL
            .iter()
            .rev()
            .for_each(|code| headers.add(*code, format!("second-{}", code.as_u8())));
        headers
    }

    fn assert_converts_as_the_reference(case: &str, headers: &HttpHeaders) {
        let (converted, dropped) = convert_headers(headers);
        let (reference, reference_dropped) = copying_reference(headers);
        assert_eq!(dropped, reference_dropped, "{case}: dropped");
        assert_eq!(converted.len(), reference.len(), "{case}: values");
        assert_eq!(converted.keys_len(), reference.keys_len(), "{case}: names");
        assert_eq!(
            converted.capacity(),
            reference.capacity(),
            "{case}: capacity"
        );
        let mismatch = converted.iter().zip(reference.iter()).position(
            |((name, value), (expected_name, expected_value))| {
                name != expected_name
                    || value != expected_value
                    || value.is_sensitive() != expected_value.is_sensitive()
            },
        );
        assert_eq!(mismatch, None, "{case}: first differing entry");
    }

    #[test]
    fn header_conversion_matches_the_copying_reference() {
        let mut mixed = HttpHeaders::new();
        let entries: [(&[u8], &[u8]); 20] = [
            (b"X-Mixed-Case", b"a"),
            (b"CONTENT-type", b"text/plain"),
            (b"x-LOWER-Upper", b"b"),
            (b"Set-Cookie", b"c1"),
            (b"x-custom", b"d"),
            (b"set-cookie", b"c2"),
            (b"X-CUSTOM", b"e"),
            (b"bad name", b"f"),
            (b"x(paren)", b"g"),
            (b"x\x01y", b"h"),
            (b"x-control", b"a\x01b"),
            (b"x-del", b"a\x7fb"),
            (b"x-cr", b"a\rb"),
            (b"x-tab", b"a\tb"),
            (b"x-obs", b"o\x80\xffk"),
            (b"x-empty", b""),
            (b"x-blank", b"   "),
            (b":status", b"200"),
            (b"Content-Length", b"0"),
            (b"x-custom", b"f"),
        ];
        entries
            .iter()
            .for_each(|(name, value)| mixed.add(*name, value));
        assert_converts_as_the_reference("mixed", &mixed);

        // More names than a `HeaderMap` can hold (2^15), then more values for names it holds.
        let mut crowded = HttpHeaders::new();
        (0..40_000).for_each(|index| {
            crowded.add(
                format!("x-h{}", index % 35_000).as_str(),
                format!("{index}"),
            )
        });
        assert!(copying_reference(&crowded).1 > 0, "the map overflows");
        assert_converts_as_the_reference("crowded", &crowded);

        let headers = every_code();
        assert_converts_as_the_reference("every code", &headers);
        let (converted, _) = convert_headers(&headers);
        let constants = http_header_constants();
        let named = HttpHeaderCode::ALL
            .iter()
            .filter_map(|code| {
                let name = HttpCommonHeaders::name(*code, HttpCommonHeaderTable::Lowercase);
                let constant = constants
                    .iter()
                    .find(|constant| constant.as_str().as_bytes() == name)?;
                let (key, value) = converted
                    .iter()
                    .find(|(key, _)| key.as_str().as_bytes() == name)
                    .unwrap_or_else(|| panic!("{code:?} converted"));
                assert_eq!(key, constant, "{code:?}");
                assert_eq!(
                    value.as_bytes(),
                    format!("first-{}", code.as_u8()).as_bytes(),
                    "{code:?}"
                );
                Some(code)
            })
            .count();
        // The common-header table names 49 of `http`'s headers.
        assert_eq!(named, 49);
    }

    #[test]
    fn converted_header_values_share_one_allocation() {
        let wire: [(&str, &[u8]); 7] = [
            ("Content-Type", b"text/html"),
            ("x-one", b"abc"),
            ("Set-Cookie", b"first=1"),
            ("x-empty", b""),
            ("Server", b"roxygen-test"),
            ("set-cookie", b"second=22"),
            ("X-Last", b"z"),
        ];
        let mut headers = HttpHeaders::new();
        wire.iter()
            .for_each(|(name, value)| headers.add(*name, value));
        let (converted, dropped) = convert_headers(&headers);
        assert_eq!(dropped, 0);
        let values: Vec<_> = wire
            .iter()
            .filter(|(_, value)| !value.is_empty())
            .map(|(name, value)| {
                converted
                    .get_all(*name)
                    .iter()
                    .find(|converted| converted.as_bytes() == *value)
                    .unwrap_or_else(|| panic!("{name} converted"))
                    .as_bytes()
            })
            .collect();
        // Each value starts where the one before it in wire order ends. The lengths fall in
        // different allocator size classes, so separate copies cannot line up this way.
        values.windows(2).for_each(|pair| {
            assert_eq!(
                pair[1].as_ptr(),
                pair[0].as_ptr().wrapping_add(pair[0].len()),
                "{:?} follows {:?}",
                String::from_utf8_lossy(pair[1]),
                String::from_utf8_lossy(pair[0]),
            );
        });
        assert_eq!(
            converted.get("x-empty").map(HeaderValue::as_bytes),
            Some(&b""[..])
        );
    }

    /// Headers with `names` in order, each with its own value.
    fn named_headers<Name: AsRef<[u8]>>(names: impl IntoIterator<Item = Name>) -> HttpHeaders {
        let mut headers = HttpHeaders::new();
        names
            .into_iter()
            .enumerate()
            .for_each(|(index, name)| headers.add(name.as_ref(), format!("value-{index}")));
        headers
    }

    /// The header names that `convert_headers` parses on this thread to convert `headers`.
    fn name_parses_converting(headers: &HttpHeaders) -> usize {
        let before = name_parses();
        drop(convert_headers(headers));
        name_parses() - before
    }

    /// Runs `work` on a new thread, whose header-name cache starts empty.
    fn on_new_thread<Output: Send + 'static>(
        work: impl FnOnce() -> Output + Send + 'static,
    ) -> Output {
        std::thread::spawn(work)
            .join()
            .unwrap_or_else(|payload| std::panic::resume_unwind(payload))
    }

    /// The common codes other than the pseudo-headers'.
    fn regular_common_codes() -> impl Iterator<Item = HttpHeaderCode> {
        HttpHeaderCode::ALL.iter().copied().filter(|code| {
            code.as_u8() >= HttpHeaderCode::COMMON_OFFSET
                && !HttpCommonHeaders::name(*code, HttpCommonHeaderTable::Lowercase)
                    .starts_with(b":")
        })
    }

    /// The header names of a benchmark server's response head, with the `date` and
    /// `content-length` that hyper adds.
    const BENCH_HEAD: [&str; 25] = [
        "content-type",
        "x-fb-debug",
        "x-fb-trip-id",
        "x-fb-connection-quality",
        "alt-svc",
        "proxy-status",
        "x-enc-edge-host",
        "x-enc-edge-hit",
        "x-enc-edge-hit-type",
        "x-edge-cache-key",
        "x-enc-origin-host",
        "x-enc-origin-hit",
        "x-enc-origin-hit-type",
        "x-origin-cache-key",
        "x-enc-edge-req-rules",
        "strict-transport-security",
        "cache-control",
        "pragma",
        "expires",
        "vary",
        "x-content-type-options",
        "x-frame-options",
        "server-timing",
        "date",
        "content-length",
    ];

    #[test]
    fn common_header_constants_equal_their_parsed_table_names() {
        let constants = http_header_constants();
        let without_constant: Vec<_> = regular_common_codes()
            .filter(|code| {
                let name = HttpCommonHeaders::name(*code, HttpCommonHeaderTable::Lowercase);
                !constants
                    .iter()
                    .any(|constant| constant.as_str().as_bytes() == name)
            })
            .collect();
        // The common-header table names 20 headers that `http` does not have constants for.
        assert_eq!(without_constant.len(), 20, "{without_constant:?}");
        without_constant.iter().for_each(|code| {
            let mut headers = HttpHeaders::new();
            headers.add(*code, "value");
            let (converted, dropped) = convert_headers(&headers);
            let parsed = HeaderName::from_bytes(HttpCommonHeaders::name(
                *code,
                HttpCommonHeaderTable::Lowercase,
            ))
            .expect("a valid name");
            assert_eq!(dropped, 0, "{code:?}");
            assert_eq!(converted.keys().collect::<Vec<_>>(), [&parsed], "{code:?}");
        });
        assert_converts_as_the_reference("every code", &every_code());
    }

    #[test]
    fn cached_header_names_convert_as_the_copying_reference() {
        on_new_thread(|| {
            let repeated = named_headers([
                "X-Repeat",
                "x-repeat",
                "x-other",
                "Content-Type",
                "x-repeat",
                "bad name",
            ]);
            (0..3).for_each(|pass| {
                assert_converts_as_the_reference(&format!("repeated, pass {pass}"), &repeated)
            });

            let variants = ["x-case", "X-Case", "X-CASE", "x-cAsE"];
            variants.iter().for_each(|variant| {
                assert_converts_as_the_reference(variant, &named_headers([variant]))
            });
            assert_converts_as_the_reference("variants together", &named_headers(variants));

            // Names whose bytes differ only in the bit that ASCII case changes, but not by case.
            let near_variants: [&[u8]; 8] = [
                b"x-a`b", b"x-a@b", b"x-a~b", b"x-a^b", b"x-a|b", b"x-a\\b", b"x!y", b"x\x01y",
            ];
            near_variants
                .iter()
                .chain(near_variants.iter().rev())
                .for_each(|name| {
                    let case = String::from_utf8_lossy(name);
                    assert_converts_as_the_reference(&case, &named_headers([name]));
                });
            assert_converts_as_the_reference(
                "near variants together",
                &named_headers(near_variants),
            );

            // More distinct names than the cache holds, so that later names replace earlier ones.
            let crowd = named_headers((0..200).map(|index| format!("x-crowd-{index}")));
            (0..2).for_each(|pass| {
                assert_converts_as_the_reference(&format!("crowd, pass {pass}"), &crowd)
            });

            // Names at and beyond the longest name that the cache holds.
            let long: Vec<String> = [63, 64, 65, 100]
                .into_iter()
                .flat_map(|length| {
                    let name = format!("x-{}", "a".repeat(length - 2));
                    let upper = name.to_ascii_uppercase();
                    let invalid = format!("x({}", "a".repeat(length - 2));
                    [name, upper, invalid]
                })
                .collect();
            (0..2).for_each(|pass| {
                assert_converts_as_the_reference(
                    &format!("long, pass {pass}"),
                    &named_headers(&long),
                )
            });

            let invalid: [&[u8]; 4] = [b"bad name", b"x(paren)", b"x\x01y", b"x\xffy"];
            (0..2).for_each(|pass| {
                assert_converts_as_the_reference(
                    &format!("invalid, pass {pass}"),
                    &named_headers(invalid),
                )
            });
        });
    }

    #[test]
    fn other_coded_standard_names_keep_their_standard_repr() {
        on_new_thread(|| {
            let other_coded: Vec<HeaderName> = http_header_constants()
                .into_iter()
                .filter(|constant| {
                    named_headers([constant.as_str()])
                        .iter()
                        .all(|(code, _, _)| code == HttpHeaderCode::Other)
                })
                .collect();
            let names: Vec<_> = other_coded.iter().map(HeaderName::as_str).collect();
            ["allow", "if-match", "warning"]
                .iter()
                .for_each(|name| assert!(names.contains(name), "{name} is Other-coded"));
            // `HeaderName` equality compares representations, so a custom name spelled as a
            // standard one would not equal its constant.
            other_coded.iter().for_each(|constant| {
                let upper = constant.as_str().to_ascii_uppercase();
                [constant.as_str(), constant.as_str(), upper.as_str()]
                    .iter()
                    .enumerate()
                    .for_each(|(pass, spelling)| {
                        let (converted, dropped) = convert_headers(&named_headers([spelling]));
                        assert_eq!(dropped, 0, "{spelling}, pass {pass}");
                        assert_eq!(
                            converted.keys().next(),
                            Some(constant),
                            "{spelling}, pass {pass}"
                        );
                    });
            });
        });
    }

    /// A head of three valid custom names, one common name, and one invalid name.
    fn probe_head() -> HttpHeaders {
        named_headers([
            "x-probe-one",
            "X-Probe-Two",
            "x-probe-three",
            "content-type",
            "bad name",
        ])
    }

    /// Converts `probe_head` while its thread's thread-locals are destroyed, and reports how
    /// many names that conversion parsed once it has matched the reference.
    struct ConversionProbe(RefCell<Option<mpsc::Sender<std::thread::Result<usize>>>>);

    impl Drop for ConversionProbe {
        fn drop(&mut self) {
            let Some(report) = self.0.borrow_mut().take() else {
                return;
            };
            let result = catch_unwind(|| {
                let headers = probe_head();
                let before = name_parses();
                assert_converts_as_the_reference("in a thread-local destructor", &headers);
                name_parses() - before
            });
            let _ = report.send(result);
        }
    }

    thread_local! {
        static CONVERSION_PROBE: ConversionProbe = const { ConversionProbe(RefCell::new(None)) };
    }

    #[test]
    fn header_conversion_falls_back_during_thread_local_destruction() {
        // Whether the thread converts the head before it sets the probe and after, and the names
        // that the probe's conversion parses. Thread-locals are destroyed in the reverse order
        // of their first use.
        let cases = [
            // The cache is destroyed before the probe, so every custom name is parsed.
            (false, true, Some(4)),
            // The cache outlives the probe.
            (true, false, None),
            // The probe's conversion is the cache's first use.
            (false, false, Some(4)),
        ];
        for (before, after, parses) in cases {
            let (report, reported) = mpsc::channel();
            std::thread::spawn(move || {
                if before {
                    drop(convert_headers(&probe_head()));
                }
                CONVERSION_PROBE.with(|probe| *probe.0.borrow_mut() = Some(report));
                if after {
                    drop(convert_headers(&probe_head()));
                }
            })
            .join()
            .expect("the thread exits");
            let parsed = reported
                .recv_timeout(HANG_GUARD)
                .expect("the destructor reports")
                .unwrap_or_else(|payload| std::panic::resume_unwind(payload));
            if let Some(parses) = parses {
                assert_eq!(parsed, parses, "before {before}, after {after}");
            }
        }
    }

    #[test]
    fn repeated_heads_parse_no_header_names() {
        on_new_thread(|| {
            let bench = named_headers(BENCH_HEAD);
            let codes: Vec<_> = bench.iter().map(|(code, _, _)| code).collect();
            let other_coded = codes
                .iter()
                .filter(|code| **code == HttpHeaderCode::Other)
                .count();
            assert_eq!(other_coded, 13);
            assert!(codes.contains(&HttpHeaderCode::ProxyStatus));
            assert_eq!(name_parses_converting(&bench), 13, "first bench conversion");
            assert_eq!(name_parses_converting(&bench), 0, "second bench conversion");

            let wide = named_headers((0..32).map(|index| format!("x-wide-{index}")));
            assert_eq!(name_parses_converting(&wide), 32, "first wide conversion");
            assert_eq!(name_parses_converting(&wide), 0, "second wide conversion");
        });
    }

    #[test]
    fn common_codes_parse_no_header_names() {
        let mut headers = HttpHeaders::new();
        regular_common_codes().for_each(|code| headers.add(code, "value"));
        let (converted, dropped) = convert_headers(&headers);
        assert_eq!((converted.len(), dropped), (69, 0));
        assert_eq!(name_parses_converting(&headers), 0);

        // The pseudo-header codes are still parsed, and dropped.
        let mut pseudo = HttpHeaders::new();
        HttpHeaderCode::ALL
            .iter()
            .filter(|code| {
                HttpCommonHeaders::name(**code, HttpCommonHeaderTable::Lowercase).starts_with(b":")
            })
            .for_each(|code| pseudo.add(*code, "value"));
        assert_eq!(name_parses_converting(&pseudo), 6);
        assert_eq!(convert_headers(&pseudo).1, 6);
    }

    /// A head holding `name` with `value`, added by name as a request's headers are.
    fn head_from_name(name: &HeaderName, value: &str) -> HttpHeaders {
        let mut headers = HttpHeaders::new();
        headers.add(name.as_str(), value);
        headers
    }

    #[test]
    fn x_robots_tag_round_trips_without_a_name_parse() {
        on_new_thread(|| {
            let name = HeaderName::from_static("x-robots-tag");
            let headers = head_from_name(&name, "noindex");
            assert_eq!(name_parses_converting(&headers), 0);
            let (converted, dropped) = convert_headers(&headers);
            assert_eq!(dropped, 0);
            assert_eq!(converted.keys().collect::<Vec<_>>(), [&name]);
            assert_eq!(
                converted.get(&name).map(HeaderValue::as_bytes),
                Some(&b"noindex"[..])
            );
        });
    }

    #[test]
    fn x_forwarded_proto_round_trips_as_an_uncommon_name() {
        on_new_thread(|| {
            let name = HeaderName::from_static("x-forwarded-proto");
            let headers = head_from_name(&name, "https");
            assert_eq!(
                headers
                    .iter()
                    .map(|(code, name, _)| (code, name.to_vec()))
                    .collect::<Vec<_>>(),
                [(HttpHeaderCode::Other, b"x-forwarded-proto".to_vec())]
            );
            let (converted, dropped) = convert_headers(&headers);
            assert_eq!(dropped, 0);
            assert_eq!(converted.keys().collect::<Vec<_>>(), [&name]);
            assert_eq!(
                converted.get(&name).map(HeaderValue::as_bytes),
                Some(&b"https"[..])
            );
        });
    }

    /// Asserts that `RawHeaders` drops as many headers as `convert_headers`, and reads each of
    /// `names` as the converted map does.
    fn assert_reads_as_the_map(case: &str, headers: &HttpHeaders, names: &[String]) {
        let (converted, dropped) = convert_headers(headers);
        let (raw, raw_dropped) = RawHeaders::new(headers.clone());
        assert_eq!(raw_dropped, dropped, "{case}: dropped");
        names.iter().for_each(|name| {
            let expected = converted.get(name.as_str());
            assert_eq!(
                raw.get(name),
                expected.map(HeaderValue::as_bytes),
                "{case}: get {name:?}"
            );
            assert_eq!(
                raw.get_str(name),
                expected.and_then(|value| value.to_str().ok()),
                "{case}: get_str {name:?}"
            );
            assert_eq!(
                raw.contains_key(name),
                converted.contains_key(name.as_str()),
                "{case}: contains_key {name:?}"
            );
        });
    }

    /// Each name of `headers` as written, in lowercase, and in uppercase, and names that the
    /// head does not carry or that `HeaderName` rejects.
    fn query_names(headers: &HttpHeaders) -> Vec<String> {
        headers
            .iter()
            .flat_map(|(_, name, _)| {
                let name = String::from_utf8_lossy(name).into_owned();
                [name.to_ascii_lowercase(), name.to_ascii_uppercase(), name]
            })
            .chain(["absent", "bad name", "", ":status", "x(paren)"].map(str::to_owned))
            .collect()
    }

    #[test]
    fn raw_headers_read_as_the_converted_map() {
        let mut mixed = HttpHeaders::new();
        let entries: [(&[u8], &[u8]); 25] = [
            (b"X-Mixed-Case", b"a"),
            (b"CONTENT-type", b"text/plain"),
            (b"x-LOWER-Upper", b"b"),
            (b"Set-Cookie", b"c1"),
            (b"x-custom", b"d"),
            (b"set-cookie", b"c2"),
            (b"X-CUSTOM", b"e"),
            (b"bad name", b"f"),
            (b"x(paren)", b"g"),
            (b"x\x01y", b"h"),
            (b"x-control", b"a\x01b"),
            (b"x-control", b"after"),
            (b"x-del", b"a\x7fb"),
            (b"x-cr", b"a\rb"),
            (b"x-tab", b"a\tb"),
            (b"x-obs", b"o\x80\xffk"),
            (b"x-obs", b"later"),
            (b"x-empty", b""),
            (b"x-blank", b"   "),
            (b":status", b"200"),
            (b"Content-Length", b"0"),
            (b"x-custom", b"f"),
            (b"Location", b"/elsewhere"),
            (b"x-under_score", b"u"),
            (b"x-under-score", b"hyphen"),
        ];
        entries
            .iter()
            .for_each(|(name, value)| mixed.add(*name, value));
        assert_reads_as_the_map("mixed", &mixed, &query_names(&mixed));

        // Every header held, with repeated names in mixed case: the first value is read.
        let mut held = HttpHeaders::new();
        let entries: [(&[u8], &[u8]); 9] = [
            (b"Set-Cookie", b"c1"),
            (b"x-dup", b"1"),
            (b"SET-COOKIE", b"c2"),
            (b"X-Dup", b"2"),
            (b"Content-Length", b"0"),
            (b"x-under_score", b"u"),
            (b"x-under-score", b"hyphen"),
            (b"x-obs", b"o\x80k"),
            (b"X-OBS", b"later"),
        ];
        entries
            .iter()
            .for_each(|(name, value)| held.add(*name, value));
        assert_eq!(RawHeaders::new(held.clone()).1, 0);
        assert_reads_as_the_map("all held", &held, &query_names(&held));

        let every = every_code();
        assert_reads_as_the_map("every code", &every, &query_names(&every));

        let bench = named_headers(BENCH_HEAD);
        assert_reads_as_the_map("bench head", &bench, &query_names(&bench));

        // At the limit the headers stay as parsed; above it they are converted.
        [RAW_HEADER_LIMIT, RAW_HEADER_LIMIT + 1]
            .into_iter()
            .for_each(|count| {
                let mut headers = HttpHeaders::new();
                headers.add("x-dup", "a\x01b");
                (1..count).for_each(|index| headers.add(format!("x-h{index}").as_str(), "v"));
                let names = ["x-dup", "x-h1", "X-H1023", "x-h1024", "absent"].map(str::to_owned);
                assert_reads_as_the_map(&format!("{count} headers"), &headers, &names);
            });

        // More names than a `HeaderMap` can hold (2^15), then more values for names it holds.
        let mut crowded = HttpHeaders::new();
        (0..40_000).for_each(|index| {
            crowded.add(
                format!("x-h{}", index % 35_000).as_str(),
                format!("{index}"),
            )
        });
        let names = ["x-h0", "x-h1", "x-h24575", "x-h34999", "absent"].map(str::to_owned);
        assert_reads_as_the_map("crowded", &crowded, &names);
    }

    #[test]
    fn raw_headers_parse_no_header_names_below_the_limit() {
        on_new_thread(|| {
            let bench = named_headers(BENCH_HEAD);
            let before = name_parses();
            let (raw, dropped) = RawHeaders::new(bench.clone());
            assert_eq!(name_parses() - before, 0, "kept as parsed");
            assert_eq!(dropped, 0);
            assert_eq!(raw.get_str("X-Enc-Edge-Host"), Some("value-6"));
            // The thread has not parsed a benchmark header name yet.
            assert_eq!(name_parses_converting(&bench), 13);

            let crowded = named_headers((0..=RAW_HEADER_LIMIT).map(|index| format!("x-c{index}")));
            let before = name_parses();
            drop(RawHeaders::new(crowded));
            assert!(name_parses() - before > 0, "converted above the limit");
        });
    }

    fn probing(probe: &Arc<ExchangeProbe>) -> ExchangeHooks {
        ExchangeHooks {
            probe: Some(Arc::clone(probe)),
            ..ExchangeHooks::default()
        }
    }

    /// Sends `request` with a new probe in the runtime's exchange hooks, and returns the outcome
    /// and that probe.
    fn probed_send(
        runtime: &ExplicitRuntime,
        client: &mut ExplicitClient,
        request: Request,
    ) -> (Result<Response, ExplicitError>, Arc<ExchangeProbe>) {
        let probe = Arc::new(ExchangeProbe::default());
        runtime.set_exchange_hooks(probing(&probe));
        let result = block_on(client.send(request));
        runtime.set_exchange_hooks(ExchangeHooks::default());
        (result, probe)
    }

    fn deadline_sleeps(probe: &ExchangeProbe) -> usize {
        probe.deadline_sleeps.load(Ordering::SeqCst)
    }

    fn validations(probe: &ExchangeProbe) -> usize {
        probe.validations.load(Ordering::SeqCst)
    }

    /// A scripted peer that responds to one request head with `OK` and then waits for the close.
    async fn answer(mut peer: tokio::io::DuplexStream) {
        let mut head = Vec::new();
        let mut buffer = [0; 1024];
        while !head.windows(4).any(|window| window == b"\r\n\r\n") {
            match peer.read(&mut buffer).await {
                Ok(0) | Err(_) => return,
                Ok(read) => head.extend_from_slice(&buffer[..read]),
            }
        }
        let _ = peer.write_all(OK.as_bytes()).await;
        let _ = peer.read(&mut buffer).await;
    }

    /// A scripted peer that reads and never responds.
    async fn stall(mut peer: tokio::io::DuplexStream) {
        let mut buffer = [0; 1024];
        let _ = peer.read(&mut buffer).await;
        let _ = peer.read(&mut buffer).await;
    }

    /// A loopback address whose listener's accept queue is full, so a connect's SYN is dropped,
    /// with the listener and the queued connections that keep it full.
    fn full_accept_queue() -> (SocketAddr, TcpListener, Vec<TcpStream>) {
        block_on(async {
            let socket = tokio::net::TcpSocket::new_v4().expect("socket");
            socket
                .bind("127.0.0.1:0".parse().expect("address"))
                .expect("bind");
            let listener = socket.listen(0).expect("listen");
            let address = listener.local_addr().expect("listener address");
            let mut queued = Vec::new();
            while let Ok(Ok(stream)) = tokio::time::timeout(
                Duration::from_millis(200),
                tokio::net::TcpStream::connect(address),
            )
            .await
            {
                queued.push(stream.into_std().expect("std stream"));
                assert!(queued.len() < 16, "the accept queue never filled");
            }
            (address, listener.into_std().expect("std listener"), queued)
        })
    }

    /// Summarizes an expiry: its phase, kind, and reached-wire flag, whether it truncated the body
    /// instead of failing the send, and which of the TCP connect, TLS handshake, first byte, and
    /// last byte timings its error carries.
    fn expiry(
        result: Result<Response, ExplicitError>,
    ) -> (Phase, ErrorKind, bool, bool, [bool; 4]) {
        let (error, truncated) = match result {
            Ok(Response {
                body: BodyOutcome::Truncated { cause, .. },
                ..
            }) => (cause, true),
            Ok(response) => panic!("completed: {response:?}"),
            Err(error) => (error, false),
        };
        let timings = &error.timings;
        (
            error.phase,
            error.kind,
            error.reached_wire,
            truncated,
            [
                timings.tcp_connect.is_some(),
                timings.tls_handshake.is_some(),
                timings.first_byte.is_some(),
                timings.last_byte.is_some(),
            ],
        )
    }

    #[test]
    fn each_exchange_arms_one_worker_deadline_sleep() {
        let fixture = tls_fixture();
        let stalled = |tls: Option<Arc<rustls::ServerConfig>>| {
            TestServer::bind("127.0.0.1:0", tls, |mut conn| {
                conn.read_request();
                conn.sees_close(HANG_GUARD);
            })
        };
        let stalled_body = |tls: Option<Arc<rustls::ServerConfig>>| {
            TestServer::bind("127.0.0.1:0", tls, |mut conn| {
                conn.read_request();
                let _ = conn.write(b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\n");
                conn.sees_close(HANG_GUARD);
            })
        };
        let plain_ok = ok_server();
        let tls_ok = TestServer::tls(Arc::clone(&fixture.server), |mut conn| {
            conn.read_request();
            let _ = conn.write(OK.as_bytes());
            conn.sees_close(HANG_GUARD);
        });
        let silent = TestServer::plain(|mut conn| {
            conn.sees_close(HANG_GUARD);
        });
        let plain_head = stalled(None);
        let tls_head = stalled(Some(Arc::clone(&fixture.server)));
        let plain_body = stalled_body(None);
        let tls_body = stalled_body(Some(Arc::clone(&fixture.server)));
        let (full, _listener, _queued) = full_accept_queue();
        let tls = |address| tls_target(address, &fixture.client, "localhost");
        let runtime = start(1, "rds");

        for target in [plain_target(plain_ok.address), tls(tls_ok.address)] {
            let mut client = runtime.client(target);
            let (result, probe) = probed_send(&runtime, &mut client, ok_request());
            assert_ok(result);
            assert_eq!(deadline_sleeps(&probe), 1);
        }

        // Each case's phase, reached-wire flag, truncation, and timings, as `expiry` reports them.
        let expiries = [
            (
                "plaintext connect",
                plain_target(full),
                (Phase::Connect, false, false, [false; 4]),
            ),
            (
                "TLS connect",
                tls(full),
                (Phase::Connect, false, false, [false; 4]),
            ),
            (
                "TLS handshake",
                tls(silent.address),
                (Phase::Tls, false, false, [true, false, false, false]),
            ),
            (
                "plaintext head",
                plain_target(plain_head.address),
                (Phase::Read, true, false, [true, false, false, false]),
            ),
            (
                "TLS head",
                tls(tls_head.address),
                (Phase::Read, true, false, [true, true, false, false]),
            ),
            (
                "plaintext body",
                plain_target(plain_body.address),
                (Phase::Body, true, true, [true, false, true, false]),
            ),
            (
                "TLS body",
                tls(tls_body.address),
                (Phase::Body, true, true, [true, true, true, false]),
            ),
        ];
        for (case, target, (phase, reached_wire, truncated, timings)) in expiries {
            let mut client = runtime.client(target);
            let late = request("localhost", "/", EXPIRY_BUDGET);
            let (result, probe) = probed_send(&runtime, &mut client, late);
            assert_eq!(
                expiry(result),
                (phase, ErrorKind::Timeout, reached_wire, truncated, timings),
                "{case}"
            );
            assert_eq!(deadline_sleeps(&probe), 1, "{case}");
            assert_eq!(validations(&probe), 1, "{case}");
        }

        // The core exchange on its own, completed and expired before the head.
        let probe = Arc::new(ExchangeProbe::default());
        run_on_manual_host(Duration::from_secs(30), probing(&probe), answer, false)
            .expect("scripted exchange");
        assert_eq!(deadline_sleeps(&probe), 1);
        let probe = Arc::new(ExchangeProbe::default());
        let error = run_on_manual_host(Duration::from_secs(30), probing(&probe), stall, true)
            .expect_err("stalled peer");
        assert_eq!((error.phase, error.kind), (Phase::Read, ErrorKind::Timeout));
        assert_eq!(deadline_sleeps(&probe), 1);
    }

    #[test]
    fn each_send_validates_its_request_once() {
        let fixture = tls_fixture();
        let plain = ok_server();
        let secure = TestServer::tls(Arc::clone(&fixture.server), |mut conn| {
            conn.read_request();
            let _ = conn.write(OK.as_bytes());
            conn.sees_close(HANG_GUARD);
        });
        let runtime = start(1, "rvo");
        let targets = [
            plain_target(plain.address),
            tls_target(secure.address, &fixture.client, "localhost"),
        ];
        for target in targets {
            let mut client = runtime.client(target);
            let (result, probe) = probed_send(&runtime, &mut client, ok_request());
            assert_ok(result);
            assert_eq!(validations(&probe), 1);
        }

        let mut client = runtime.client(plain_target(plain.address));
        let refused =
            request_with_headers("localhost", "/", HANG_GUARD, vec![("connection", "close")]);
        let (result, probe) = probed_send(&runtime, &mut client, refused);
        let error = result.expect_err("an adapter-owned header");
        assert_eq!(
            (error.phase, error.kind),
            (Phase::Submit, ErrorKind::Protocol)
        );
        assert_eq!(validations(&probe), 1);
        assert_eq!(plain.accepted(), 1);

        let probe = Arc::new(ExchangeProbe::default());
        run_on_manual_host(Duration::from_secs(30), probing(&probe), answer, false)
            .expect("scripted exchange");
        assert_eq!(validations(&probe), 1);
    }

    #[test]
    fn attached_session_timeouts_stay_the_remaining_deadline() {
        let record = |timeout| AttachRecord {
            max_concurrent_outgoing_streams: Some(1),
            stream_read_timeout: Some(timeout),
            connection_read_timeout: Some(timeout),
            write_timeout: Some(timeout),
        };
        // On a manual clock that stands still, the remaining deadline is the whole budget, and
        // one under 1 ms is raised to 1 ms.
        let budgets = [
            (Duration::from_secs(30), Duration::from_secs(30)),
            (Duration::from_micros(500), Duration::from_millis(1)),
        ];
        for (budget, timeout) in budgets {
            let probe = Arc::new(ExchangeProbe::default());
            run_on_manual_host(budget, probing(&probe), answer, false).expect("scripted exchange");
            assert_eq!(
                *probe.attach_records.lock().unwrap(),
                [record(timeout)],
                "{budget:?}"
            );
        }

        // Runtime sends whose budget is beyond Uniplex's 5 s default. The worker holds each before
        // it dials until `held` has passed since the send's deadline was set, so the time left
        // when it attaches is at most the budget less `held`.
        let fixture = tls_fixture();
        let plain = ok_server();
        let secure = TestServer::tls(Arc::clone(&fixture.server), |mut conn| {
            conn.read_request();
            let _ = conn.write(OK.as_bytes());
            conn.sees_close(HANG_GUARD);
        });
        let runtime = start(1, "rat");
        let hooks = runtime.hooks();
        let budget = Duration::from_secs(30);
        let held = Duration::from_millis(200);
        let targets = [
            plain_target(plain.address),
            tls_target(secure.address, &fixture.client, "localhost"),
        ];
        for target in targets {
            let pause = TestPause::new();
            let _release = ReleaseOnDrop(pause.clone());
            *hooks.connect_pause.lock().unwrap() = Some(pause.clone());
            let mut client = runtime.client(target);
            let send = request("localhost", "/", budget);
            let deadline = send.deadline;
            let releaser = std::thread::spawn(move || {
                pause.wait_reached(1);
                sleep_past(deadline - budget + held);
                pause.release();
            });
            let (result, probe) = probed_send(&runtime, &mut client, send);
            let left_after = deadline.saturating_duration_since(Instant::now());
            releaser.join().expect("releaser");
            *hooks.connect_pause.lock().unwrap() = None;
            assert_ok(result);
            let records = probe.attach_records.lock().unwrap().clone();
            let [
                AttachRecord {
                    stream_read_timeout: Some(timeout),
                    ..
                },
            ] = records[..]
            else {
                panic!("one attach with a stream read timeout: {records:?}");
            };
            assert_eq!(records, [record(timeout)]);
            assert!(
                left_after <= timeout && timeout <= budget - held,
                "{timeout:?} is not the time left when attached"
            );
            assert!(timeout > Duration::from_secs(5), "{timeout:?}");
        }
    }

    /// Blocks the calling thread until `deadline` has passed.
    fn sleep_past(deadline: Instant) {
        while Instant::now() <= deadline {
            std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
        }
    }

    #[test]
    fn cancellation_still_wins_over_a_passed_deadline_in_connect_and_handshake() {
        let fixture = tls_fixture();
        let plain = ok_server();
        let secure = TestServer::tls(Arc::clone(&fixture.server), |mut conn| {
            let _ = conn.read_end(HANG_GUARD);
        });
        let runtime = start(1, "rcw");
        let hooks = runtime.hooks();
        let cases = [
            (
                plain_target(plain.address),
                &hooks.connect_pause,
                (Phase::Connect, "cancelled during connect"),
            ),
            (
                tls_target(secure.address, &fixture.client, "localhost"),
                &hooks.handshake_pause,
                (Phase::Tls, "cancelled during the TLS handshake"),
            ),
        ];
        for (target, hold, (phase, detail)) in cases {
            // The worker holds before the stage's first poll while the send is dropped, which
            // cancels its token, and its deadline passes.
            let pause = TestPause::new();
            let _release = ReleaseOnDrop(pause.clone());
            *hold.lock().unwrap() = Some(pause.clone());
            let mut client = runtime.client(target);
            let late = request("localhost", "/", EXPIRY_BUDGET);
            let deadline = late.deadline;
            block_on(async {
                let pending = tokio::spawn(async move { client.send(late).await });
                pause.reached_async(1).await;
                pending.abort();
                assert!(pending.await.expect_err("aborted").is_cancelled());
            });
            *hold.lock().unwrap() = None;
            sleep_past(deadline);
            pause.release();
            wait_until("the job's outcome", || {
                !hooks.job_errors.lock().unwrap().is_empty()
            });
            let errors = std::mem::take(&mut *hooks.job_errors.lock().unwrap());
            assert_eq!(
                errors,
                [(phase, ErrorKind::Cancelled, false, Box::from(detail))]
            );
        }
        assert_eq!(plain.accepted(), 0);
        assert_eq!(runtime.stats().caller_cancellations, 2);
    }

    /// A reference endpoint and `ConnectionInfo` address for a dialed address.
    fn endpoint_reference(address: Option<SocketAddr>) -> (TransportEndpoint, Option<SocketAddr>) {
        let endpoint = address.map_or(TransportEndpoint::Unknown, |address| {
            TransportEndpoint::Socket(SocketAddress::new(address.ip().to_string(), address.port()))
        });
        let reported = endpoint.socket_address().and_then(|address| {
            let ip: IpAddr = std::str::from_utf8(address.host()).ok()?.parse().ok()?;
            Some(SocketAddr::new(ip, address.port()))
        });
        (endpoint, reported)
    }

    #[test]
    fn endpoints_and_connection_info_match_the_reference() {
        let scoped = SocketAddr::V6(SocketAddrV6::new("fe80::1".parse().unwrap(), 443, 7, 2));
        let cases: [(Option<SocketAddr>, Option<&[u8]>); 7] = [
            (
                Some("127.0.0.1:8080".parse().unwrap()),
                Some(b"127.0.0.1".as_slice()),
            ),
            (
                Some("255.255.255.255:0".parse().unwrap()),
                Some(b"255.255.255.255".as_slice()),
            ),
            (Some("[::1]:443".parse().unwrap()), Some(b"::1".as_slice())),
            (
                Some(
                    "[ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffe]:65535"
                        .parse()
                        .unwrap(),
                ),
                Some(b"ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffe".as_slice()),
            ),
            (
                Some("[::ffff:127.0.0.1]:80".parse().unwrap()),
                Some(b"::ffff:127.0.0.1".as_slice()),
            ),
            (Some(scoped), Some(b"fe80::1".as_slice())),
            (None, None),
        ];
        for (address, host) in cases {
            let (endpoint, reported) = dialed_endpoint(address);
            assert_eq!(
                endpoint.socket_address().map(SocketAddress::host),
                host,
                "{address:?}"
            );
            assert_eq!(
                (endpoint, reported),
                endpoint_reference(address),
                "{address:?}"
            );
            assert_eq!(
                reported,
                address.map(|address| SocketAddr::new(address.ip(), address.port())),
                "{address:?}"
            );
        }
        // As in the reference, a scoped address loses its flow label and scope.
        let (_, reported) = dialed_endpoint(Some(scoped));
        let Some(SocketAddr::V6(reported)) = reported else {
            panic!("an IPv6 address: {reported:?}");
        };
        assert_eq!((reported.flowinfo(), reported.scope_id()), (0, 0));
        assert_ne!(SocketAddr::V6(reported), scoped);
    }

    /// Checks `dialed_endpoint`'s host text, port, and reported address for `ip` against `std`.
    fn assert_ipv4_endpoint(ip: Ipv4Addr, port: u16) {
        let address = SocketAddr::new(IpAddr::V4(ip), port);
        let (endpoint, reported) = dialed_endpoint(Some(address));
        let host = endpoint.socket_address().expect("a socket endpoint");
        assert_eq!(host.host(), ip.to_string().as_bytes(), "{ip}");
        assert_eq!(host.port(), port, "{ip}");
        assert_eq!(
            reported,
            Some(SocketAddr::new(IpAddr::V4(ip), port)),
            "{ip}"
        );
    }

    #[test]
    fn ipv4_endpoint_text_matches_std_for_every_octet_position() {
        // One, two, and three digits, with and without inner zero digits.
        const OTHERS: [u8; 8] = [0, 9, 10, 99, 100, 199, 200, 255];
        for position in 0..4 {
            for value in 0..=u8::MAX {
                for first in OTHERS {
                    for second in OTHERS {
                        for third in OTHERS {
                            let mut others = [first, second, third].into_iter();
                            let octets: [u8; 4] = std::array::from_fn(|index| {
                                if index == position {
                                    value
                                } else {
                                    others.next().expect("three other octets")
                                }
                            });
                            assert_ipv4_endpoint(Ipv4Addr::from(octets), u16::from(value));
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn ipv4_endpoint_text_matches_std_across_a_u32_stride() {
        (0..=u32::MAX).step_by(40_009).for_each(|bits| {
            let port = u16::try_from(bits % 65_536).expect("a port");
            assert_ipv4_endpoint(Ipv4Addr::from(bits), port);
        });
    }

    #[test]
    fn v4_mapped_endpoint_text_stays_on_the_std_path() {
        let ip = Ipv4Addr::new(192, 0, 2, 235).to_ipv6_mapped();
        let address = SocketAddr::new(IpAddr::V6(ip), 8443);
        let (endpoint, reported) = dialed_endpoint(Some(address));
        let host = endpoint.socket_address().expect("a socket endpoint");
        assert_eq!(host.host(), ip.to_string().as_bytes());
        assert_eq!(host.host(), b"::ffff:192.0.2.235");
        assert_eq!(host.port(), 8443);
        assert_eq!(reported, Some(address));
    }

    /// A configuration with at most `command_capacity` queued commands per worker and at most
    /// `max_sends_in_flight` sends per slot.
    fn split_config(
        workers: usize,
        command_capacity: usize,
        max_sends_in_flight: usize,
        prefix: &'static str,
    ) -> ExplicitRuntimeConfig {
        ExplicitRuntimeConfig {
            max_sends_in_flight: NonZeroUsize::new(max_sends_in_flight).expect("in-flight bound"),
            ..config(workers, command_capacity, prefix)
        }
    }

    fn multi_thread_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("caller runtime")
    }

    /// The slots that `count` new clients of `runtime` pin, in creation order.
    fn pins(runtime: &ExplicitRuntime, target: &Arc<Target>, count: usize) -> Vec<usize> {
        (0..count)
            .map(|_| runtime.client(Arc::clone(target)).slot())
            .collect()
    }

    /// Starts each client's send on `caller`, one at a time, each once the previous one's request
    /// has reached the server and its slot's queue is empty again, so that a send does not meet
    /// the queue bound by racing the worker's drain.
    fn park_sends(
        caller: &tokio::runtime::Runtime,
        runtime: &ExplicitRuntime,
        clients: Vec<ExplicitClient>,
        read_rx: &mpsc::Receiver<()>,
    ) -> Vec<tokio::task::JoinHandle<Result<Response, ExplicitError>>> {
        clients
            .into_iter()
            .map(|mut client| {
                let slot = client.slot();
                let send = caller.spawn(async move { client.send(ok_request()).await });
                let mut read = false;
                wait_until("the request to reach the server", || {
                    read = read_rx.try_recv().is_ok();
                    read || send.is_finished()
                });
                if !read {
                    let result = caller.block_on(send).expect("send task");
                    panic!("a parked send resolved: {result:?}");
                }
                wait_until("the slot's queue to drain", || {
                    runtime
                        .worker_handle(slot)
                        .expect("published worker")
                        .queued_commands()
                        == 0
                });
                send
            })
            .collect()
    }

    /// A server that signals once it has read each request and responds to one request for each
    /// token sent on the returned sender. Once the sender is dropped, it responds to every
    /// remaining request.
    fn token_server() -> (TestServer, mpsc::Receiver<()>, mpsc::Sender<()>) {
        let (read_tx, read_rx) = mpsc::channel();
        let (token_tx, token_rx) = mpsc::channel::<()>();
        let tokens = Mutex::new(token_rx);
        let server = TestServer::plain(move |mut conn| {
            conn.read_request();
            let _ = read_tx.send(());
            let _ = tokens.lock().unwrap().recv_timeout(HANG_GUARD);
            let _ = conn.write(OK.as_bytes());
            conn.sees_close(HANG_GUARD);
        });
        (server, read_rx, token_tx)
    }

    #[test]
    fn sends_above_command_capacity_within_the_in_flight_bound_are_admitted() {
        let (server, read_rx, release) = parking_server();
        let runtime = ExplicitRuntime::start(split_config(1, 1, 4, "aba")).expect("runtime");
        let caller = multi_thread_runtime();
        let target = plain_target(server.address);
        let observer = runtime.client(Arc::clone(&target));
        let clients = (0..4)
            .map(|_| runtime.client(Arc::clone(&target)))
            .collect();
        let sends = park_sends(&caller, &runtime, clients, &read_rx);
        let health = observer.slot_health();
        assert_eq!((health.sends_in_flight, health.queued_commands), (4, 0));
        release.release();
        sends
            .into_iter()
            .for_each(|send| assert_ok(caller.block_on(send).expect("send task")));
        assert_eq!(runtime.stats().overloads, 0);
        assert_eq!(server.accepted(), 4);
        assert_eq!(observer.slot_health().sends_in_flight, 0);
    }

    #[test]
    fn send_beyond_max_sends_in_flight_is_overloaded_before_dialing() {
        let (server, read_rx, release) = parking_server();
        let runtime = ExplicitRuntime::start(split_config(1, 1, 4, "abb")).expect("runtime");
        let caller = multi_thread_runtime();
        let target = plain_target(server.address);
        let observer = runtime.client(Arc::clone(&target));
        let mut fifth = runtime.client(Arc::clone(&target));
        let clients = (0..4)
            .map(|_| runtime.client(Arc::clone(&target)))
            .collect();
        let sends = park_sends(&caller, &runtime, clients, &read_rx);

        let error = caller
            .block_on(fifth.send(ok_request()))
            .expect_err("in-flight bound");
        assert_eq!(
            (error.phase, error.kind, error.reached_wire, &*error.detail),
            (
                Phase::Submit,
                ErrorKind::Overloaded,
                false,
                "the slot's sends in flight are at max_sends_in_flight"
            )
        );
        assert_eq!(server.accepted(), 4, "the refused send dials nothing");
        assert_eq!(runtime.stats().overloads, 1);
        let health = observer.slot_health();
        assert_eq!(
            (health.sends_in_flight, health.consecutive_overloads),
            (4, 1)
        );
        release.release();
        sends
            .into_iter()
            .for_each(|send| assert_ok(caller.block_on(send).expect("send task")));
        assert_eq!(server.accepted(), 4);
    }

    #[test]
    fn full_command_queue_is_still_overloaded() {
        let server = ok_server();
        let runtime = ExplicitRuntime::start(split_config(1, 1, 8, "abq")).expect("runtime");
        let mut client = runtime.client(plain_target(server.address));
        let release = hold_worker(&runtime, 0);
        runtime
            .worker_handle(0)
            .expect("published worker")
            .submit(|_| {})
            .expect("filler admitted");
        let error = block_on(client.send(ok_request())).expect_err("queue full");
        assert_eq!(
            (error.phase, error.kind, error.reached_wire, &*error.detail),
            (
                Phase::Submit,
                ErrorKind::Overloaded,
                false,
                "the worker's command queue is full"
            )
        );
        let health = client.slot_health();
        assert_eq!(
            (
                health.sends_in_flight,
                health.queued_commands,
                health.consecutive_overloads
            ),
            (0, 1, 1)
        );
        assert_eq!(runtime.stats().overloads, 1);
        drop(release);
        assert_eq!(server.accepted(), 0);
    }

    #[test]
    fn black_holed_target_times_out_instead_of_overloading() {
        let server = TestServer::plain(|mut conn| {
            conn.read_request();
            conn.sees_close(HANG_GUARD);
        });
        let runtime = ExplicitRuntime::start(split_config(1, 2, 8, "abh")).expect("runtime");
        let caller = multi_thread_runtime();
        let target = plain_target(server.address);
        let observer = runtime.client(Arc::clone(&target));
        let deadline = Instant::now() + 2 * EXPIRY_BUDGET;
        let sends: Vec<_> = (1..=8)
            .map(|count| {
                let mut client = runtime.client(Arc::clone(&target));
                let request = Request {
                    deadline,
                    ..ok_request()
                };
                let send = caller.spawn(async move { client.send(request).await });
                wait_until("the connection to be accepted", || {
                    server.accepted() == count || send.is_finished()
                });
                if send.is_finished() {
                    let result = caller.block_on(send).expect("send task");
                    panic!("send {count} resolved before the deadline: {result:?}");
                }
                send
            })
            .collect();
        let health = observer.slot_health();
        assert!(
            Instant::now() < deadline,
            "the slot was read before the deadline"
        );
        assert_eq!((health.sends_in_flight, health.queued_commands), (8, 0));
        for send in sends {
            let error = caller
                .block_on(send)
                .expect("send task")
                .expect_err("timeout");
            assert_eq!(
                (error.phase, error.kind, error.reached_wire),
                (Phase::Read, ErrorKind::Timeout, true),
                "{error}"
            );
        }
        assert_eq!(runtime.stats().overloads, 0);
        assert_eq!(observer.slot_health().sends_in_flight, 0);
    }

    #[test]
    fn slot_health_reports_both_bounds() {
        let (server, read_rx, tokens) = token_server();
        let runtime = ExplicitRuntime::start(split_config(1, 1, 3, "abr")).expect("runtime");
        let caller = multi_thread_runtime();
        let target = plain_target(server.address);
        let observer = runtime.client(Arc::clone(&target));
        let mut refused = runtime.client(Arc::clone(&target));
        let health = |sends_in_flight, queued_commands, consecutive_overloads| SlotHealth {
            state: SlotState::Open,
            sends_in_flight,
            queued_commands,
            command_capacity: 1,
            max_sends_in_flight: 3,
            consecutive_overloads,
        };
        assert_eq!(observer.slot_health(), health(0, 0, 0));

        // At the in-flight bound, the overload count is not reset.
        let clients = (0..3)
            .map(|_| runtime.client(Arc::clone(&target)))
            .collect();
        let mut sends = park_sends(&caller, &runtime, clients, &read_rx);
        let error = caller
            .block_on(refused.send(ok_request()))
            .expect_err("in-flight bound");
        assert_eq!(error.kind, ErrorKind::Overloaded);
        assert_eq!(observer.slot_health(), health(3, 0, 1));

        // Below both bounds, it is reset, although the slot's sends in flight exceed the
        // queue's capacity.
        tokens.send(()).expect("token");
        wait_until("one send's task to finish", || {
            sends.iter().filter(|send| send.is_finished()).count() == 1
        });
        assert_eq!(observer.slot_health(), health(2, 0, 0));

        // At the queue bound, below the in-flight bound, the overload count is not reset.
        let hold = hold_worker(&runtime, 0);
        runtime
            .worker_handle(0)
            .expect("published worker")
            .submit(|_| {})
            .expect("filler admitted");
        let error = caller
            .block_on(refused.send(ok_request()))
            .expect_err("queue bound");
        assert_eq!(error.kind, ErrorKind::Overloaded);
        assert_eq!(observer.slot_health(), health(2, 1, 1));
        drop(hold);
        wait_until("the queue to drain", || {
            observer.slot_health().queued_commands == 0
        });
        assert_eq!(observer.slot_health(), health(2, 0, 0));

        // A dropped send releases its admission once.
        let parked = sends
            .iter()
            .position(|send| !send.is_finished())
            .expect("a parked send");
        let dropped = sends.swap_remove(parked);
        dropped.abort();
        assert!(
            caller
                .block_on(dropped)
                .expect_err("aborted")
                .is_cancelled()
        );
        assert_eq!(observer.slot_health().sends_in_flight, 1);
        drop(tokens);
        sends
            .into_iter()
            .for_each(|send| assert_ok(caller.block_on(send).expect("send task")));
        assert_eq!(observer.slot_health(), health(0, 0, 0));
        assert_eq!(runtime.stats().overloads, 2);
    }

    #[test]
    fn clients_pin_to_the_less_loaded_slot() {
        let (server, read_rx, release) = parking_server();
        let target = plain_target(server.address);
        let single = start(1, "abl");
        assert_eq!(pins(&single, &target, 4), [0, 0, 0, 0]);

        let runtime = start(2, "abm");
        assert_eq!(pins(&runtime, &target, 4), [0, 1, 0, 1]);
        let caller = multi_thread_runtime();
        let parked = runtime.client(Arc::clone(&target));
        assert_eq!(parked.slot(), 0);
        let sends = park_sends(&caller, &runtime, vec![parked], &read_rx);
        // Round robin alternates the clients' first candidates, 1, 0, 1, 0.
        assert_eq!(pins(&runtime, &target, 4), [1, 1, 1, 1]);
        release.release();
        sends
            .into_iter()
            .for_each(|send| assert_ok(caller.block_on(send).expect("send task")));
        assert_eq!(pins(&runtime, &target, 4), [1, 0, 1, 0]);

        // A forced-closed slot still draws its round-robin clients.
        force_close(&runtime, 1);
        assert_eq!(pins(&runtime, &target, 4), [1, 0, 1, 0]);
        assert_eq!(runtime.slot_state(1), SlotState::Closed);
        assert_eq!(runtime.stats().restarts, 0);

        // A restarting slot competes on the sends that wait for its publication.
        let (server, read_rx, release) = parking_server();
        let target = plain_target(server.address);
        let runtime = start(2, "abn");
        let (on_0, on_1): (Vec<_>, Vec<_>) = (0..4)
            .map(|_| runtime.client(Arc::clone(&target)))
            .partition(|client| client.slot() == 0);
        let Ok([first, second]) = <[ExplicitClient; 2]>::try_from(on_1) else {
            panic!("two clients on each slot");
        };
        let mut sends = park_sends(&caller, &runtime, on_0, &read_rx);
        let spawn = TestPause::new();
        let _release_spawn = ReleaseOnDrop(spawn.clone());
        *runtime.hooks().spawn_pause.lock().unwrap() = Some(spawn.clone());
        force_close(&runtime, 1);
        let wait_on_1 = |mut client: ExplicitClient| {
            caller.spawn(async move { client.send(ok_request()).await })
        };
        sends.push(wait_on_1(first));
        spawn.wait_reached(1);
        assert_eq!(runtime.slot_state(1), SlotState::Restarting);
        // Slot 0 has two sends in flight, and slot 1 one.
        assert_eq!(pins(&runtime, &target, 4), [1, 1, 1, 1]);
        sends.push(wait_on_1(second));
        wait_until("the second send on slot 1 to be admitted", || {
            runtime.stats().sends_in_flight == 4
        });
        assert_eq!(runtime.slot_state(1), SlotState::Restarting);
        assert_eq!(pins(&runtime, &target, 4), [0, 1, 0, 1], "a tie at two");
        spawn.release();
        release.release();
        sends
            .into_iter()
            .for_each(|send| assert_ok(caller.block_on(send).expect("send task")));
        assert_eq!(runtime.stats().restarts, 1);
    }

    #[test]
    fn clients_never_pin_a_capped_slot_while_another_slot_is_not_capped() {
        let (server, read_rx, release) = parking_server();
        let target = plain_target(server.address);
        let never_restarting = |workers, prefix| {
            let mut never = config(workers, 8, prefix);
            never.max_restarts_per_minute = 0;
            ExplicitRuntime::start(never).expect("runtime")
        };

        let runtime = never_restarting(2, "abc");
        force_close(&runtime, 1);
        assert_eq!(runtime.slot_state(1), SlotState::Capped);
        assert_eq!(pins(&runtime, &target, 8), [0; 8]);
        let caller = multi_thread_runtime();
        let parked = runtime.client(Arc::clone(&target));
        let sends = park_sends(&caller, &runtime, vec![parked], &read_rx);
        assert_eq!(
            pins(&runtime, &target, 8),
            [0; 8],
            "slot 0 has a send in flight"
        );
        release.release();
        sends
            .into_iter()
            .for_each(|send| assert_ok(caller.block_on(send).expect("send task")));

        let runtime = never_restarting(4, "abd");
        force_close(&runtime, 0);
        force_close(&runtime, 1);
        let clients: Vec<_> = (0..64)
            .map(|_| runtime.client(Arc::clone(&target)))
            .collect();
        let slots: BTreeSet<_> = clients.iter().map(ExplicitClient::slot).collect();
        assert_eq!(slots, BTreeSet::from([2, 3]));
        // A client whose two candidates are capped pins the first slot after its round-robin
        // slot that is not capped.
        let fallbacks = |clients: &[ExplicitClient], capped: [usize; 2]| -> BTreeSet<_> {
            clients
                .iter()
                .filter_map(|client| match client.candidates() {
                    (round_robin, Some(second))
                        if capped.contains(&round_robin) && capped.contains(&second) =>
                    {
                        Some((round_robin, client.slot()))
                    }
                    _ => None,
                })
                .collect()
        };
        assert_eq!(
            fallbacks(&clients, [0, 1]),
            BTreeSet::from([(0, 2), (1, 2)])
        );

        let runtime = never_restarting(4, "abo");
        force_close(&runtime, 1);
        force_close(&runtime, 3);
        let clients: Vec<_> = (0..64)
            .map(|_| runtime.client(Arc::clone(&target)))
            .collect();
        assert_eq!(
            fallbacks(&clients, [1, 3]),
            BTreeSet::from([(1, 2), (3, 0)]),
            "the fallback wraps around"
        );

        let runtime = never_restarting(2, "abe");
        force_close(&runtime, 0);
        force_close(&runtime, 1);
        assert_eq!(
            pins(&runtime, &target, 4),
            [0, 1, 0, 1],
            "every slot is capped"
        );

        // Creating clients does not admit a restart.
        let runtime = start(2, "abf");
        force_close(&runtime, 1);
        let clients: Vec<_> = (0..8)
            .map(|_| runtime.client(Arc::clone(&target)))
            .collect();
        assert_eq!(runtime.slot_state(1), SlotState::Closed);
        let stats = runtime.stats();
        assert_eq!((stats.restarts, stats.failed_restarts), (0, 0));
        assert_eq!(live_threads("abf-restart"), 0);
        let closed = clients
            .iter()
            .find(|client| client.slot() == 1)
            .expect("a client on slot 1");
        assert_eq!(closed.slot_health().state, SlotState::Closed);
        assert_eq!(server.accepted(), 1);
    }

    #[test]
    fn a_capped_slot_competes_again_once_its_bounds_admit_a_restart() {
        let server = ok_server();
        let target = plain_target(server.address);
        let mut capped = config(2, 8, "abg");
        capped.max_restarts_per_minute = 1;
        let hooks = Arc::new(RuntimeHooks::default());
        *hooks.cap_window.lock().unwrap() = Duration::from_millis(300);
        let runtime = ExplicitRuntime::start_with_hooks(capped, hooks).expect("runtime");
        assert_eq!(pins(&runtime, &target, 1), [0]);
        let first = runtime.client(Arc::clone(&target));
        assert_eq!(first.slot(), 1);
        // The old worker's held join keeps the slot capped however long the window takes.
        let barrier = TestPause::new();
        let _release = ReleaseOnDrop(barrier.clone());
        hold_cleanup(&runtime, 1, &barrier);
        force_close(&runtime, 1);
        assert_eq!(first.slot_health().state, SlotState::Closed);
        wait_for_state(&first, SlotState::Open);
        force_close(&runtime, 1);
        assert_eq!(first.slot_health().state, SlotState::Capped);
        assert_eq!(pins(&runtime, &target, 2), [0, 0]);

        barrier.release();
        wait_until("the window to pass and the old worker's join", || {
            runtime.stats().restarts_joining == 0 && runtime.slot_state(1) == SlotState::Closed
        });
        assert_eq!(pins(&runtime, &target, 1), [0]);
        let mut returned = runtime.client(Arc::clone(&target));
        assert_eq!((returned.candidates().0, returned.slot()), (1, 1));
        assert_eq!(runtime.slot_state(1), SlotState::Closed);
        assert_eq!(runtime.stats().restarts, 1);
        assert_ok(block_on(returned.send(ok_request())));
        assert_eq!(runtime.stats().restarts, 2);
    }

    #[test]
    fn clients_pin_a_closed_slot_whose_restarts_fail_until_it_is_capped() {
        let (server, read_rx, release) = parking_server();
        let target = plain_target(server.address);
        let mut bounded = config(2, 8, "abk");
        bounded.max_restarts_per_minute = 2;
        let runtime = ExplicitRuntime::start(bounded).expect("runtime");
        let caller = multi_thread_runtime();
        let parked = runtime.client(Arc::clone(&target));
        assert_eq!(parked.slot(), 0);
        let sends = park_sends(&caller, &runtime, vec![parked], &read_rx);
        force_close(&runtime, 1);
        runtime
            .hooks()
            .fail_next_thread_spawns
            .store(2, Ordering::SeqCst);
        for restart in 1..=2 {
            let mut client = runtime.client(Arc::clone(&target));
            assert_eq!(client.slot(), 1, "client {restart}");
            let error = caller
                .block_on(client.send(ok_request()))
                .expect_err("the restart fails");
            assert_eq!(
                (error.phase, error.kind, error.reached_wire, &*error.detail),
                (
                    Phase::Submit,
                    ErrorKind::WorkerLost,
                    false,
                    "the slot's restart failed"
                )
            );
            assert_eq!(runtime.stats().failed_restarts, restart);
        }
        assert_eq!(runtime.slot_state(1), SlotState::Capped);
        assert_eq!(pins(&runtime, &target, 4), [0, 0, 0, 0]);
        release.release();
        sends
            .into_iter()
            .for_each(|send| assert_ok(caller.block_on(send).expect("send task")));
        assert_eq!(server.accepted(), 1);
    }

    #[test]
    fn clients_pin_the_less_loaded_of_two_distinct_candidates() {
        let (server, read_rx, release) = parking_server();
        let target = plain_target(server.address);
        let runtime = start(4, "abi");
        let caller = multi_thread_runtime();
        let pool: Vec<_> = (0..8)
            .map(|_| runtime.client(Arc::clone(&target)))
            .collect();
        let pool_slots: Vec<_> = pool.iter().map(ExplicitClient::slot).collect();
        assert_eq!(pool_slots, [0, 1, 2, 3, 0, 1, 2, 3]);
        let idle: Vec<_> = (0..64)
            .map(|_| runtime.client(Arc::clone(&target)))
            .collect();
        for client in &idle {
            let (round_robin, second) = client.candidates();
            let second = second.expect("a second candidate");
            assert!(
                second < 4 && second != round_robin,
                "{round_robin} {second}"
            );
            assert_eq!(client.slot(), round_robin, "a tie goes to round robin");
        }
        for round_robin in 0..4 {
            let drawn: BTreeSet<_> = idle
                .iter()
                .map(ExplicitClient::candidates)
                .filter(|(first, _)| *first == round_robin)
                .filter_map(|(_, second)| second)
                .collect();
            let others: BTreeSet<_> = (0..4).filter(|slot| *slot != round_robin).collect();
            assert_eq!(drawn, others, "second candidates of slot {round_robin}");
        }

        // One send parked on slot 0, one on slot 1, and two on slot 3.
        let loads = [1, 1, 0, 2];
        let parked = pool
            .into_iter()
            .enumerate()
            .filter(|(index, _)| [0, 1, 3, 7].contains(index))
            .map(|(_, client)| client)
            .collect();
        let sends = park_sends(&caller, &runtime, parked, &read_rx);
        for (slot, load) in loads.iter().enumerate() {
            assert_eq!(idle[slot].slot(), slot);
            assert_eq!(idle[slot].slot_health().sends_in_flight, *load);
        }
        let loaded: Vec<_> = (0..64)
            .map(|_| runtime.client(Arc::clone(&target)))
            .collect();
        for client in &loaded {
            let (round_robin, second) = client.candidates();
            let second = second.expect("a second candidate");
            let expected = if loads[second] < loads[round_robin] {
                second
            } else {
                round_robin
            };
            assert_eq!(
                client.slot(),
                expected,
                "candidates {round_robin} and {second}"
            );
        }
        let chose = |pinned_second: bool, tied: bool| {
            loaded.iter().any(|client| {
                let (round_robin, second) = client.candidates();
                let second = second.expect("a second candidate");
                (client.slot() == second) == pinned_second
                    && (loads[round_robin] == loads[second]) == tied
            })
        };
        assert!(chose(true, false), "some client pins its second candidate");
        assert!(
            chose(false, true),
            "some client breaks a tie by round robin"
        );
        release.release();
        sends
            .into_iter()
            .for_each(|send| assert_ok(caller.block_on(send).expect("send task")));
    }

    #[test]
    fn concurrent_client_creation_takes_one_slot_lock_at_a_time() {
        const CREATORS: usize = 4;
        const CLIENTS: usize = 256;
        let server = ok_server();
        let target = plain_target(server.address);
        let mut churn = config(4, 64, "abj");
        churn.max_restarts_per_minute = 1000;
        let runtime = ExplicitRuntime::start(churn).expect("runtime");
        let observers: Vec<_> = (0..4)
            .map(|_| runtime.client(Arc::clone(&target)))
            .collect();
        let mut restarter = (0..4)
            .map(|_| runtime.client(Arc::clone(&target)))
            .find(|client| client.slot() == 1)
            .expect("a client on slot 1");
        let stop = Arc::new(AtomicBool::new(false));
        let start = Arc::new(Barrier::new(CREATORS + 2));

        let poller = {
            let (stop, start) = (Arc::clone(&stop), Arc::clone(&start));
            std::thread::spawn(move || {
                start.wait();
                while !stop.load(Ordering::SeqCst) {
                    observers.iter().for_each(|observer| {
                        observer.slot_health();
                    });
                }
            })
        };
        let closer = {
            let (stop, start) = (Arc::clone(&stop), Arc::clone(&start));
            let runtime = runtime.clone();
            std::thread::spawn(move || {
                let caller = current_thread_runtime();
                start.wait();
                loop {
                    force_close(&runtime, 1);
                    assert_ok(caller.block_on(restarter.send(ok_request())));
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                }
            })
        };
        let (done_tx, done_rx) = mpsc::channel();
        let creators: Vec<_> = (0..CREATORS)
            .map(|_| {
                let (runtime, target, start) =
                    (runtime.clone(), Arc::clone(&target), Arc::clone(&start));
                let done = done_tx.clone();
                std::thread::spawn(move || {
                    start.wait();
                    let _ = done.send(pins(&runtime, &target, CLIENTS));
                })
            })
            .collect();
        for _ in 0..CREATORS {
            let slots = done_rx
                .recv_timeout(HANG_GUARD)
                .expect("every client() call returns");
            assert_eq!(slots.len(), CLIENTS);
            assert!(slots.iter().all(|slot| *slot < 4), "{slots:?}");
        }
        stop.store(true, Ordering::SeqCst);
        creators
            .into_iter()
            .chain([poller, closer])
            .for_each(|thread| thread.join().expect("test thread"));
        assert!(runtime.stats().restarts > 0);
    }
}
