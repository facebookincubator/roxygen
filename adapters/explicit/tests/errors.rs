//! Error classification, deadline, and cancellation oracles.

mod errors {
    use std::future::Future;
    use std::future::poll_fn;
    use std::io;
    use std::pin::pin;
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::task::Poll;
    use std::thread::JoinHandle;
    use std::time::Duration;
    use std::time::Instant;

    use roxygen_core::connection::TransportSecurity;
    use roxygen_core::error::HttpError;
    use roxygen_core::error::HttpErrorCode;
    use roxygen_runtime::cancellation::CancellationToken;
    use roxygen_runtime::driver::WorkerDriver;
    use roxygen_runtime::host::Host;
    use roxygen_runtime::worker::WorkerState;
    use roxygen_transport::established::EstablishedIo;
    use rustls::pki_types::ServerName;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    use crate::error::ErrorKind;
    use crate::error::ExplicitError;
    use crate::error::Observed;
    use crate::error::Phase;
    use crate::error::Position;
    use crate::exchange::BodyOutcome;
    use crate::exchange::ExchangeHooks;
    use crate::exchange::ExchangeInput;
    use crate::exchange::ExchangeProgress;
    use crate::exchange::Request;
    use crate::exchange::Response;
    use crate::exchange::TestPause;
    use crate::exchange::exchange;
    use crate::exchange_tests::support::EXPIRY_BUDGET;
    use crate::exchange_tests::support::HANG_GUARD;
    use crate::exchange_tests::support::ReadEnd;
    use crate::exchange_tests::support::ReleaseOnDrop;
    use crate::exchange_tests::support::TestServer;
    use crate::exchange_tests::support::block_on;
    use crate::exchange_tests::support::endpoint;
    use crate::exchange_tests::support::eventually;
    use crate::exchange_tests::support::live_threads;
    use crate::exchange_tests::support::plain_target;
    use crate::exchange_tests::support::refusing_address;
    use crate::exchange_tests::support::request;
    use crate::exchange_tests::support::request_with_headers;
    use crate::exchange_tests::support::run_on_manual_host;
    use crate::exchange_tests::support::start;
    use crate::exchange_tests::support::tls_fixture;
    use crate::exchange_tests::support::tls_target;
    use crate::exchange_tests::support::untrusting_client;
    use crate::exchange_tests::support::wait_until;
    use crate::runtime::Security;
    use crate::runtime::Target;
    use crate::stats::ExplicitStats;

    const OPEN: Observed = Observed {
        own_cancelled: false,
        worker_open: true,
        deadline_passed: false,
    };

    fn row(
        name: &str,
        error: ExplicitError,
        phase: Phase,
        kind: ErrorKind,
        code: Option<HttpErrorCode>,
        io_kind: Option<io::ErrorKind>,
    ) {
        assert_eq!(error.phase, phase, "{name}: phase");
        assert_eq!(error.kind, kind, "{name}: kind");
        assert_eq!(error.code, code.map(HttpErrorCode::as_u16), "{name}: code");
        assert_eq!(error.io_kind, io_kind, "{name}: io_kind");
        assert!(!error.detail.is_empty(), "{name}: detail");
    }

    fn http(code: HttpErrorCode) -> HttpError {
        HttpError::with_message(code, b"injected")
    }

    fn http_with_io(code: HttpErrorCode, kind: io::ErrorKind) -> HttpError {
        http(code).with_cause(Arc::new(io::Error::new(kind, "injected")))
    }

    #[test]
    fn classification_matrix_keeps_phase_kind_and_code() {
        use ErrorKind::*;
        use io::ErrorKind as Io;

        let connect = |kind| ExplicitError::from_connect_io(&io::Error::new(kind, "injected"));
        row(
            "ConnectRefused",
            connect(Io::ConnectionRefused),
            Phase::Connect,
            Refused,
            None,
            Some(Io::ConnectionRefused),
        );
        row(
            "ConnectReset",
            connect(Io::ConnectionReset),
            Phase::Connect,
            Reset,
            None,
            Some(Io::ConnectionReset),
        );
        row(
            "ConnectAborted",
            connect(Io::ConnectionAborted),
            Phase::Connect,
            Reset,
            None,
            Some(Io::ConnectionAborted),
        );
        row(
            "ConnectTimeout",
            connect(Io::TimedOut),
            Phase::Connect,
            Timeout,
            None,
            Some(Io::TimedOut),
        );
        row(
            "ConnectFallbackIsOther",
            connect(Io::HostUnreachable),
            Phase::Connect,
            Other,
            None,
            Some(Io::HostUnreachable),
        );

        let certificate =
            rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer);
        let handshake = rustls::Error::AlertReceived(rustls::AlertDescription::HandshakeFailure);
        row(
            "TlsVerification",
            ExplicitError::from_rustls(&certificate),
            Phase::Tls,
            TlsVerification,
            None,
            None,
        );
        row(
            "TlsHandshake",
            ExplicitError::from_rustls(&handshake),
            Phase::Tls,
            TlsHandshake,
            None,
            None,
        );
        let wrapped = |error: rustls::Error| io::Error::new(Io::InvalidData, error);
        row(
            "WrappedTlsVerification",
            ExplicitError::from_tls_io(&wrapped(certificate.clone())),
            Phase::Tls,
            TlsVerification,
            None,
            None,
        );
        row(
            "WrappedTlsHandshake",
            ExplicitError::from_tls_io(&wrapped(handshake.clone())),
            Phase::Tls,
            TlsHandshake,
            None,
            None,
        );
        let tls = |kind| ExplicitError::from_tls_io(&io::Error::new(kind, "injected"));
        row(
            "TlsTimeout",
            tls(Io::TimedOut),
            Phase::Tls,
            Timeout,
            None,
            Some(Io::TimedOut),
        );
        row(
            "TlsReset",
            tls(Io::ConnectionReset),
            Phase::Tls,
            Reset,
            None,
            Some(Io::ConnectionReset),
        );
        row(
            "TlsAborted",
            tls(Io::ConnectionAborted),
            Phase::Tls,
            Reset,
            None,
            Some(Io::ConnectionAborted),
        );
        row(
            "TlsRemoteClosed",
            tls(Io::UnexpectedEof),
            Phase::Tls,
            RemoteClosed,
            None,
            Some(Io::UnexpectedEof),
        );
        row(
            "TlsFallbackIsOther",
            tls(Io::Other),
            Phase::Tls,
            Other,
            None,
            Some(Io::Other),
        );

        let head = |error: HttpError, observed| {
            ExplicitError::from_http(&error, Position::BeforeHead, observed)
        };
        let body = |error: HttpError, observed| {
            ExplicitError::from_http(&error, Position::AfterHead, observed)
        };
        let attach = |error: HttpError, observed| {
            ExplicitError::from_http(&error, Position::Attach, observed)
        };
        let cancelled = Observed {
            own_cancelled: true,
            ..OPEN
        };
        let closed = Observed {
            worker_open: false,
            ..OPEN
        };
        let expired = Observed {
            deadline_passed: true,
            ..OPEN
        };
        use HttpErrorCode as C;
        row(
            "ReadTimeout",
            head(http(C::READ_TIMEOUT), OPEN),
            Phase::Read,
            Timeout,
            Some(C::READ_TIMEOUT),
            None,
        );
        row(
            "ReadTimeoutInBody",
            body(http(C::READ_TIMEOUT), OPEN),
            Phase::Body,
            Timeout,
            Some(C::READ_TIMEOUT),
            None,
        );
        row(
            "WriteTimeout",
            head(http(C::WRITE_TIMEOUT), OPEN),
            Phase::Write,
            Timeout,
            Some(C::WRITE_TIMEOUT),
            None,
        );
        row(
            "ReadReset",
            head(
                http_with_io(C::TRANSPORT_READ_ERROR, Io::ConnectionReset),
                OPEN,
            ),
            Phase::Read,
            Reset,
            Some(C::TRANSPORT_READ_ERROR),
            Some(Io::ConnectionReset),
        );
        row(
            "WriteAborted",
            head(
                http_with_io(C::TRANSPORT_WRITE_ERROR, Io::ConnectionAborted),
                OPEN,
            ),
            Phase::Write,
            Reset,
            Some(C::TRANSPORT_WRITE_ERROR),
            Some(Io::ConnectionAborted),
        );
        row(
            "ReadErrorWithoutResetIsOther",
            body(http_with_io(C::TRANSPORT_READ_ERROR, Io::Other), OPEN),
            Phase::Body,
            Other,
            Some(C::TRANSPORT_READ_ERROR),
            Some(Io::Other),
        );
        row(
            "TransportEof",
            body(http(C::TRANSPORT_EOF), OPEN),
            Phase::Body,
            RemoteClosed,
            Some(C::TRANSPORT_EOF),
            None,
        );
        row(
            "RefusedStream",
            head(http(C::REFUSED_STREAM), OPEN),
            Phase::Read,
            StreamRefused,
            Some(C::REFUSED_STREAM),
            None,
        );
        row(
            "ExcessiveLoad",
            head(http(C::ENHANCE_YOUR_CALM), OPEN),
            Phase::Read,
            ExcessiveLoad,
            Some(C::ENHANCE_YOUR_CALM),
            None,
        );
        row(
            "Protocol",
            head(http(C::PROTOCOL_ERROR), OPEN),
            Phase::Read,
            Protocol,
            Some(C::PROTOCOL_ERROR),
            None,
        );
        row(
            "OwnCancel",
            head(http(C::CANCEL), cancelled),
            Phase::Read,
            Cancelled,
            Some(C::CANCEL),
            None,
        );
        row(
            "OwnCoroCancelled",
            body(http(C::CORO_CANCELLED), cancelled),
            Phase::Body,
            Cancelled,
            Some(C::CORO_CANCELLED),
            None,
        );
        row(
            "ForeignCoroCancelledIsOther",
            head(http(C::CORO_CANCELLED), OPEN),
            Phase::Read,
            Other,
            Some(C::CORO_CANCELLED),
            None,
        );
        row(
            "InternalIsOther",
            head(http(C::INTERNAL_ERROR), OPEN),
            Phase::Read,
            Other,
            Some(C::INTERNAL_ERROR),
            None,
        );
        row(
            "VersionFallbackIsOther",
            head(http(C::HTTP_1_1_REQUIRED), OPEN),
            Phase::Read,
            Other,
            Some(C::HTTP_1_1_REQUIRED),
            None,
        );
        row(
            "ContentLengthMismatchIsOther",
            body(http(C::CONTENT_LENGTH_MISMATCH), OPEN),
            Phase::Body,
            Other,
            Some(C::CONTENT_LENGTH_MISMATCH),
            None,
        );
        row(
            "ClosedWorkerIsWorkerLost",
            body(http(C::TRANSPORT_EOF), closed),
            Phase::Body,
            WorkerLost,
            Some(C::TRANSPORT_EOF),
            None,
        );
        row(
            "OwnCancellationOnClosedWorker",
            head(
                http(C::CORO_CANCELLED),
                Observed {
                    own_cancelled: true,
                    worker_open: false,
                    deadline_passed: false,
                },
            ),
            Phase::Read,
            Cancelled,
            Some(C::CORO_CANCELLED),
            None,
        );
        row(
            "ExpiredReadTimeout",
            head(http(C::READ_TIMEOUT), expired),
            Phase::Read,
            Timeout,
            None,
            None,
        );
        row(
            "ExpiredWriteTimeout",
            head(http(C::WRITE_TIMEOUT), expired),
            Phase::Read,
            Timeout,
            None,
            None,
        );
        row(
            "ExpiredReadTimeoutInBody",
            body(http(C::READ_TIMEOUT), expired),
            Phase::Body,
            Timeout,
            None,
            None,
        );
        row(
            "AttachDropped",
            attach(http(C::DROPPED), OPEN),
            Phase::Connect,
            Other,
            Some(C::DROPPED),
            None,
        );
        row(
            "AttachCancelled",
            attach(http(C::CORO_CANCELLED), cancelled),
            Phase::Connect,
            Cancelled,
            Some(C::CORO_CANCELLED),
            None,
        );
        row(
            "AttachOnClosedWorker",
            attach(http(C::DROPPED), closed),
            Phase::Connect,
            WorkerLost,
            Some(C::DROPPED),
            None,
        );
        row(
            "ConnectOnClosedWorker",
            connect(Io::ConnectionRefused).observed(closed),
            Phase::Connect,
            WorkerLost,
            None,
            Some(Io::ConnectionRefused),
        );

        assert!(!attach(http(C::DROPPED), OPEN).reached_wire);
        assert!(head(http(C::READ_TIMEOUT), OPEN).reached_wire);
        assert!(!connect(Io::ConnectionRefused).reached_wire);
        assert!(!tls(Io::UnexpectedEof).reached_wire);
    }

    #[test]
    fn refused_port_is_connect_refused_with_io_kind() {
        let runtime = start(1, "rer");
        let (refusing, _bound) = refusing_address();
        let mut client = runtime.client(plain_target(refusing));
        // A numeric address with no hostname override fails at connect time, not at input
        // validation.
        let error = block_on(client.send(request("example.com", "/", HANG_GUARD)))
            .expect_err("refused port");
        assert_eq!(error.phase, Phase::Connect);
        assert_eq!(error.kind, ErrorKind::Refused);
        assert_eq!(error.io_kind, Some(io::ErrorKind::ConnectionRefused));
        assert_eq!(error.code, None);
        assert!(!error.reached_wire);
        assert!(!error.detail.is_empty());
    }

    #[test]
    fn untrusted_certificate_is_tls_verification() {
        let fixture = tls_fixture();
        let server = TestServer::tls(Arc::clone(&fixture.server), |mut conn| {
            conn.sees_close(HANG_GUARD);
        });
        let runtime = start(1, "rev");
        let mut client = runtime.client(tls_target(
            server.address,
            &untrusting_client(),
            "localhost",
        ));
        let error = block_on(client.send(request("localhost", "/", HANG_GUARD)))
            .expect_err("untrusted certificate");
        assert_eq!(error.phase, Phase::Tls);
        assert_eq!(error.kind, ErrorKind::TlsVerification);
        assert!(!error.reached_wire);
    }

    #[test]
    fn config_offering_alpn_fails_before_dialing() {
        let fixture = tls_fixture();
        let mut offering = (*fixture.client).clone();
        offering.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        let offering = Arc::new(offering);
        let error = Security::tls(Arc::clone(&offering), "localhost").expect_err("ALPN offered");
        assert_eq!((error.phase, error.kind), (Phase::Tls, ErrorKind::Protocol));
        assert!(!error.reached_wire);

        let server = TestServer::tls(Arc::clone(&fixture.server), |mut conn| {
            conn.sees_close(HANG_GUARD);
        });
        let runtime = start(1, "rea");
        let mut client = runtime.client(Arc::new(Target {
            address: server.address,
            security: Security::Tls {
                config: offering,
                server_name: ServerName::try_from("localhost").expect("name"),
            },
        }));
        let error =
            block_on(client.send(request("localhost", "/", HANG_GUARD))).expect_err("ALPN offered");
        assert_eq!((error.phase, error.kind), (Phase::Tls, ErrorKind::Protocol));
        assert!(!error.reached_wire);
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(server.accepted(), 0);
    }

    #[test]
    fn stalled_server_is_read_timeout() {
        let server = TestServer::plain(|mut conn| {
            conn.read_request();
            conn.sees_close(HANG_GUARD);
        });
        let runtime = start(1, "res");
        let mut client = runtime.client(plain_target(server.address));
        let error = block_on(client.send(request("localhost", "/", EXPIRY_BUDGET)))
            .expect_err("stalled server");
        assert_eq!((error.phase, error.kind), (Phase::Read, ErrorKind::Timeout));
        assert_eq!(error.code, None);
        assert!(error.reached_wire);
    }

    /// Runs one core exchange over an in-memory peer on a paused Tokio clock.
    fn run_on_paused_clock<Peer, PeerWork>(
        budget: Duration,
        security: TransportSecurity,
        peer: Peer,
    ) -> Result<Response, ExplicitError>
    where
        Peer: FnOnce(tokio::io::DuplexStream) -> PeerWork + Send + 'static,
        PeerWork: Future<Output = ()> + 'static,
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .expect("paused runtime");
        let local = tokio::task::LocalSet::new();
        local.block_on(&runtime, async move {
            let (handle, driver) = WorkerDriver::with_host(16, Host::tokio()).expect("driver");
            let driver = tokio::task::spawn_local(driver);
            let call = handle
                .call_local(move |context| async move {
                    let (client_io, peer_io) = tokio::io::duplex(64 * 1024);
                    let peer = tokio::task::spawn_local(peer(peer_io));
                    let started = context.host().now();
                    let mut request = request("peer.example", "/", budget);
                    request.deadline = started + budget;
                    let address = "127.0.0.1:80".parse().expect("address");
                    let outcome = exchange(ExchangeInput {
                        worker: context,
                        io: EstablishedIo::new(
                            client_io,
                            endpoint(address),
                            endpoint(address),
                            None,
                        ),
                        security,
                        request,
                        cancellation: CancellationToken::never(),
                        progress: started_progress(),
                        started,
                        stats: Arc::new(ExplicitStats::new()),
                    })
                    .await;
                    (outcome, peer)
                })
                .expect("command admitted");
            let (outcome, peer) = call.await.expect("command result");
            handle.begin_shutdown();
            driver.await.expect("driver");
            // The peer ends once the retired transport drops the client's half of the stream.
            tokio::time::timeout(HANG_GUARD, peer)
                .await
                .expect("peer ends")
                .expect("peer task");
            outcome
        })
    }

    fn started_progress() -> Arc<ExchangeProgress> {
        let progress = Arc::new(ExchangeProgress::new());
        assert!(progress.try_start());
        progress
    }

    async fn read_head(peer: &mut tokio::io::DuplexStream) {
        let mut head = Vec::new();
        let mut buffer = [0; 1024];
        while !head.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = peer.read(&mut buffer).await.expect("request");
            if read == 0 {
                return;
            }
            head.extend_from_slice(&buffer[..read]);
        }
    }

    async fn trickle(peer: &mut tokio::io::DuplexStream) {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if peer.write_all(b"a").await.is_err() {
                return;
            }
        }
    }

    /// Blocks the calling thread until `deadline` has passed.
    fn sleep_past(deadline: Instant) {
        while Instant::now() <= deadline {
            std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
        }
    }

    /// Releases `pause` on another thread once the worker has reached it, `ready` has returned,
    /// and `deadline` has passed, and reports whether the worker arrived and `ready` held. It
    /// releases in every case, so a failed step never leaves the worker blocked.
    fn release_after_deadline(
        pause: &TestPause,
        deadline: Instant,
        ready: impl FnOnce() -> bool + Send + 'static,
    ) -> JoinHandle<bool> {
        let pause = pause.clone();
        std::thread::spawn(move || {
            let reached = eventually(|| pause.reached() == 1);
            let ready = ready();
            sleep_past(deadline);
            pause.release();
            reached && ready
        })
    }

    #[test]
    fn deadline_bounds_connect_tls_head_and_body() {
        // Before the final head, on a controlled clock, with every gap inside the session timeouts.
        let error = run_on_paused_clock(
            Duration::from_secs(1),
            TransportSecurity::Plaintext,
            |mut peer| async move {
                read_head(&mut peer).await;
                let _ = peer.write_all(b"HTTP/1.1 200 OK\r\nX-Slow: ").await;
                trickle(&mut peer).await;
            },
        )
        .expect_err("head deadline");
        assert_eq!(
            (error.phase, error.kind, error.code),
            (Phase::Read, ErrorKind::Timeout, None)
        );
        assert_eq!(error.io_kind, None);
        assert!(error.reached_wire);

        // After the final head the expiry truncates the body.
        let response = run_on_paused_clock(
            Duration::from_secs(1),
            TransportSecurity::Plaintext,
            |mut peer| async move {
                read_head(&mut peer).await;
                let _ = peer
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\n")
                    .await;
                trickle(&mut peer).await;
            },
        )
        .expect("head arrived");
        match response.body {
            BodyOutcome::Truncated { cause, .. } => {
                assert_eq!(
                    (cause.phase, cause.kind, cause.code),
                    (Phase::Body, ErrorKind::Timeout, None)
                );
            }
            BodyOutcome::Complete { bytes } => panic!("trickled body completed with {bytes} bytes"),
        }

        // A whole response arrives while the exchange is held before its head read, and the
        // exchange reads only after the clock reaches the deadline: the expiry wins over the
        // ready head.
        let pause = TestPause::new();
        let hooks = ExchangeHooks {
            before_head: Some(pause.clone()),
            ..ExchangeHooks::default()
        };
        let error = run_on_manual_host(
            Duration::from_secs(1),
            hooks,
            |mut peer| async move {
                read_head(&mut peer).await;
                let _ = peer
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                    .await;
                let mut buffer = [0; 64];
                let _ = peer.read(&mut buffer).await;
            },
            true,
        )
        .expect_err("expired with the response queued");
        assert_eq!(
            (error.phase, error.kind, error.code),
            (Phase::Read, ErrorKind::Timeout, None)
        );
        assert!(error.reached_wire);
        assert_eq!(pause.reached(), 1);

        // A stream handed to the core with no time left fails before adoption, and its peer
        // reads EOF.
        for (security, phase) in [
            (TransportSecurity::Plaintext, Phase::Connect),
            (TransportSecurity::Tls, Phase::Tls),
        ] {
            let (eof_tx, eof_rx) = mpsc::channel();
            let error = run_on_paused_clock(Duration::ZERO, security, move |mut peer| async move {
                let mut buffer = [0; 64];
                let _ = eof_tx.send(peer.read(&mut buffer).await.map_err(|error| error.kind()));
            })
            .expect_err("no time left");
            assert_eq!(
                (error.phase, error.kind, error.code),
                (phase, ErrorKind::Timeout, None)
            );
            assert!(!error.reached_wire);
            assert_eq!(eof_rx.recv_timeout(HANG_GUARD).expect("peer read"), Ok(0));
        }

        // The deadline passed before the send dialed.
        let server = TestServer::plain(|mut conn| {
            conn.read_request();
        });
        let runtime = start(1, "reb");
        let mut client = runtime.client(plain_target(server.address));
        let mut expired = request("localhost", "/", HANG_GUARD);
        expired.deadline = Instant::now();
        let error = block_on(client.send(expired)).expect_err("expired before dialing");
        assert_eq!(
            (error.phase, error.kind, error.code),
            (Phase::Submit, ErrorKind::Timeout, None)
        );
        assert!(!error.reached_wire);
        assert_eq!(server.accepted(), 0);

        // The deadline passed during TCP connect: a listener whose accept queue is full drops
        // the SYN.
        let (full_address, _full_listener, _queued_connections) = block_on(async {
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
        });
        let mut client = runtime.client(plain_target(full_address));
        let error = block_on(client.send(request("localhost", "/", EXPIRY_BUDGET)))
            .expect_err("connect deadline");
        assert_eq!(
            (error.phase, error.kind, error.code, error.io_kind),
            (Phase::Connect, ErrorKind::Timeout, None, None)
        );
        assert!(!error.reached_wire);

        // The deadline passed during the TLS handshake.
        let silent = TestServer::plain(|mut conn| {
            conn.sees_close(HANG_GUARD);
        });
        let fixture = tls_fixture();
        let mut client = runtime.client(tls_target(silent.address, &fixture.client, "localhost"));
        let error = block_on(client.send(request("localhost", "/", EXPIRY_BUDGET)))
            .expect_err("handshake deadline");
        assert_eq!(
            (error.phase, error.kind, error.code),
            (Phase::Tls, ErrorKind::Timeout, None)
        );
        assert!(!error.reached_wire);

        // The worker reaches the TCP connect only after the deadline, with a refusal ready on
        // the first poll: the expiry wins over the ready error.
        let pause = TestPause::new();
        *runtime.hooks().connect_pause.lock().unwrap() = Some(pause.clone());
        let (refusing, _bound) = refusing_address();
        let mut client = runtime.client(plain_target(refusing));
        let late = request("localhost", "/", EXPIRY_BUDGET);
        let release = release_after_deadline(&pause, late.deadline, || true);
        let error = block_on(client.send(late)).expect_err("connect expiry");
        assert!(release.join().expect("releaser"), "the worker was held");
        *runtime.hooks().connect_pause.lock().unwrap() = None;
        assert_eq!(
            (error.phase, error.kind, error.code, error.io_kind),
            (Phase::Connect, ErrorKind::Timeout, None, None)
        );
        assert!(!error.reached_wire);

        // The worker reaches the TLS handshake only after the deadline, with the peer's fatal
        // alert already sent: the expiry wins over the ready handshake failure.
        let (alert_tx, alert_rx) = mpsc::channel();
        let alerting = TestServer::plain(move |mut conn| {
            // A fatal handshake_failure alert record.
            conn.write(&[0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x28])
                .expect("alert");
            let _ = alert_tx.send(());
            conn.sees_close(HANG_GUARD);
        });
        let pause = TestPause::new();
        *runtime.hooks().handshake_pause.lock().unwrap() = Some(pause.clone());
        let mut client = runtime.client(tls_target(alerting.address, &fixture.client, "localhost"));
        let late = request("localhost", "/", EXPIRY_BUDGET);
        let release = release_after_deadline(&pause, late.deadline, move || {
            alert_rx.recv_timeout(HANG_GUARD).is_ok()
        });
        let error = block_on(client.send(late)).expect_err("handshake expiry");
        assert!(
            release.join().expect("releaser"),
            "the worker was held and the alert was sent"
        );
        *runtime.hooks().handshake_pause.lock().unwrap() = None;
        assert_eq!(
            (error.phase, error.kind, error.code, error.io_kind),
            (Phase::Tls, ErrorKind::Timeout, None, None)
        );
        assert!(!error.reached_wire);

        // Through `send` with a short real deadline after the worker started the exchange.
        let stalled = TestServer::plain(|mut conn| {
            conn.read_request();
            conn.sees_close(HANG_GUARD);
        });
        let mut client = runtime.client(plain_target(stalled.address));
        let error = block_on(client.send(request("localhost", "/", EXPIRY_BUDGET)))
            .expect_err("stalled server");
        assert_eq!(
            (error.phase, error.kind, error.code),
            (Phase::Read, ErrorKind::Timeout, None)
        );
        assert!(error.reached_wire);
    }

    #[test]
    fn deadline_expiry_closes_the_connection_and_retires_its_transport() {
        let (closed_tx, closed_rx) = mpsc::channel();
        let server = TestServer::plain(move |mut conn| {
            let request = conn.read_request();
            let prefix: &[u8] = if request.request_line.starts_with("GET /head ") {
                b"HTTP/1.1 200 OK\r\nX-Slow: "
            } else {
                b"HTTP/1.1 200 OK\r\nContent-Length: 100000\r\n\r\n"
            };
            let _ = conn.write(prefix);
            let _ =
                closed_tx.send(conn.trickle_until_closed(Duration::from_millis(50), HANG_GUARD));
        });
        let runtime = start(1, "ret");
        let mut client = runtime.client(plain_target(server.address));
        let budget = EXPIRY_BUDGET;

        let error = block_on(client.send(request("localhost", "/head", budget)))
            .expect_err("head deadline");
        assert_eq!(
            (error.phase, error.kind, error.code),
            (Phase::Read, ErrorKind::Timeout, None)
        );
        assert!(closed_rx.recv_timeout(HANG_GUARD).expect("server result"));

        let response =
            block_on(client.send(request("localhost", "/body", budget))).expect("head arrived");
        match response.body {
            BodyOutcome::Truncated { cause, .. } => {
                assert_eq!((cause.phase, cause.kind), (Phase::Body, ErrorKind::Timeout));
            }
            BodyOutcome::Complete { bytes } => panic!("trickled body completed with {bytes} bytes"),
        }
        assert!(closed_rx.recv_timeout(HANG_GUARD).expect("server result"));

        wait_until("transport retirement", || {
            runtime.stats().pending_teardowns == 0
        });
        assert_eq!(runtime.stats().caller_cancellations, 0);
    }

    #[test]
    fn body_pause_beyond_uniplex_default_completes_within_deadline() {
        let response = run_on_paused_clock(
            Duration::from_secs(30),
            TransportSecurity::Plaintext,
            |mut peer| async move {
                read_head(&mut peer).await;
                let _ = peer
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 20\r\n\r\n0123456789")
                    .await;
                tokio::time::sleep(Duration::from_secs(6)).await;
                let _ = peer.write_all(b"0123456789").await;
                let mut buffer = [0; 64];
                let _ = peer.read(&mut buffer).await;
            },
        )
        .expect("paused body");
        match response.body {
            BodyOutcome::Complete { bytes } => assert_eq!(bytes, 20),
            BodyOutcome::Truncated { bytes, cause } => {
                panic!("body pause truncated at {bytes} bytes: {cause}")
            }
        }
    }

    #[test]
    fn dropping_send_future_mid_response_closes_the_connection() {
        let (sent_tx, mut sent_rx) = tokio::sync::mpsc::unbounded_channel();
        let (closed_tx, closed_rx) = mpsc::channel();
        let server = TestServer::plain(move |mut conn| {
            let request = conn.read_request();
            if request.request_line.starts_with("GET /next ") {
                let _ = conn.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
                conn.sees_close(HANG_GUARD);
                return;
            }
            let _ = conn.write(b"HTTP/1.1 200 OK\r\nContent-Length: 10000\r\n\r\n");
            let _ = conn.write(&[b'a'; 100]);
            let _ = sent_tx.send(());
            let _ = closed_tx.send(conn.sees_close(HANG_GUARD));
        });
        let runtime = start(1, "red");
        let mut client = runtime.client(plain_target(server.address));
        let budget = Duration::from_secs(60);
        block_on(async move {
            let pending =
                tokio::spawn(
                    async move { client.send(request("localhost", "/partial", budget)).await },
                );
            sent_rx.recv().await.expect("partial body sent");
            pending.abort();
            assert!(pending.await.expect_err("aborted").is_cancelled());
        });
        assert!(closed_rx.recv_timeout(HANG_GUARD).expect("server result"));
        let mut client = runtime.client(plain_target(server.address));
        let next = block_on(client.send(request("localhost", "/next", budget))).expect("next");
        assert!(matches!(next.body, BodyOutcome::Complete { bytes: 2 }));
        wait_until("transport retirement", || {
            runtime.stats().pending_teardowns == 0
        });
        assert_eq!(runtime.stats().caller_cancellations, 1);
    }

    #[test]
    fn dropping_send_future_before_dial_or_during_tls_stops_the_attempt() {
        let server = TestServer::plain(|mut conn| {
            conn.read_request();
        });
        let runtime = start(1, "reg");
        let budget = Duration::from_secs(60);

        // Dropped while a test command holds the worker: the worker never dials.
        let (release_tx, release_rx) = mpsc::channel::<()>();
        runtime
            .worker_handle(0)
            .expect("published worker")
            .submit(move |_| {
                let _ = release_rx.recv_timeout(HANG_GUARD);
            })
            .expect("hold admitted");
        let mut client = runtime.client(plain_target(server.address));
        block_on(async {
            let pending =
                tokio::spawn(async move { client.send(request("localhost", "/", budget)).await });
            tokio::time::sleep(Duration::from_millis(50)).await;
            pending.abort();
            assert!(pending.await.expect_err("aborted").is_cancelled());
        });
        drop(release_tx);
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(server.accepted(), 0);

        // Dropped after the worker started the exchange and before it polls the TCP connect:
        // the connect's token race stops the attempt, so the worker never dials.
        let pause = TestPause::new();
        let _release = ReleaseOnDrop(pause.clone());
        *runtime.hooks().connect_pause.lock().unwrap() = Some(pause.clone());
        let mut client = runtime.client(plain_target(server.address));
        block_on(async {
            let pending =
                tokio::spawn(async move { client.send(request("localhost", "/", budget)).await });
            pause.reached_async(1).await;
            pending.abort();
            assert!(pending.await.expect_err("aborted").is_cancelled());
        });
        pause.release();
        *runtime.hooks().connect_pause.lock().unwrap() = None;
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(server.accepted(), 0);
        assert_eq!(runtime.stats().caller_cancellations, 2);

        // Dropped while a peer that accepted TCP never answers the ClientHello.
        let (closed_tx, closed_rx) = mpsc::channel();
        let silent = TestServer::plain(move |mut conn| {
            let _ = closed_tx.send(conn.read_end(HANG_GUARD));
        });
        let fixture = tls_fixture();
        let mut client = runtime.client(tls_target(silent.address, &fixture.client, "localhost"));
        block_on(async {
            let pending =
                tokio::spawn(async move { client.send(request("localhost", "/", budget)).await });
            while silent.accepted() == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            pending.abort();
            assert!(pending.await.expect_err("aborted").is_cancelled());
        });
        assert_eq!(
            closed_rx.recv_timeout(HANG_GUARD).expect("server result"),
            Some(ReadEnd::Eof)
        );
        assert_eq!(runtime.stats().caller_cancellations, 3);
    }

    #[test]
    fn completed_send_disarms_cancellation_before_teardown() {
        let (end_tx, end_rx) = mpsc::channel();
        let server = TestServer::plain(move |mut conn| {
            conn.read_request();
            let _ = conn.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
            let _ = end_tx.send(conn.read_end(HANG_GUARD));
        });
        let runtime = start(1, "rec");
        let budget = Duration::from_secs(60);

        // Paused after the swap to finished: dropping the send requests no cancellation.
        let pause = TestPause::new();
        runtime.set_exchange_hooks(ExchangeHooks {
            before_close: Some(pause.clone()),
            ..ExchangeHooks::default()
        });
        let mut client = runtime.client(plain_target(server.address));
        block_on(async {
            let pending =
                tokio::spawn(async move { client.send(request("localhost", "/", budget)).await });
            pause.reached_async(1).await;
            pending.abort();
            assert!(pending.await.expect_err("aborted").is_cancelled());
        });
        pause.release();
        assert_eq!(
            end_rx.recv_timeout(HANG_GUARD).expect("server read"),
            Some(ReadEnd::Eof)
        );
        wait_until("transport retirement", || {
            runtime.stats().pending_teardowns == 0
        });
        assert_eq!(runtime.stats().caller_cancellations, 0);
        assert_eq!(runtime.stats().close_to_retirement_us.count, 1);

        // Paused before that swap: dropping the send claims the exchange, the worker's swap
        // loses, and it skips close_when_idle.
        let pause = TestPause::new();
        runtime.set_exchange_hooks(ExchangeHooks {
            before_finish: Some(pause.clone()),
            ..ExchangeHooks::default()
        });
        let mut client = runtime.client(plain_target(server.address));
        block_on(async {
            let pending =
                tokio::spawn(async move { client.send(request("localhost", "/", budget)).await });
            pause.reached_async(1).await;
            pending.abort();
            assert!(pending.await.expect_err("aborted").is_cancelled());
        });
        pause.release();
        assert!(
            end_rx
                .recv_timeout(HANG_GUARD)
                .expect("server read")
                .is_some()
        );
        wait_until("transport retirement", || {
            runtime.stats().pending_teardowns == 0
        });
        assert_eq!(runtime.stats().caller_cancellations, 1);
        assert_eq!(runtime.stats().close_to_retirement_us.count, 1);

        // A truncated body also reaches the swap before its result is sent.
        let truncating = TestServer::plain(|mut conn| {
            conn.read_request();
            let _ = conn.write(b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\n");
            std::thread::sleep(Duration::from_millis(50));
            let _ = conn.write(b"partial");
            std::thread::sleep(Duration::from_millis(100));
        });
        let pause = TestPause::new();
        runtime.set_exchange_hooks(ExchangeHooks {
            before_finish: Some(pause.clone()),
            ..ExchangeHooks::default()
        });
        let mut client = runtime.client(plain_target(truncating.address));
        block_on(async {
            let pending =
                tokio::spawn(async move { client.send(request("localhost", "/", budget)).await });
            pause.reached_async(1).await;
            pending.abort();
            assert!(pending.await.expect_err("aborted").is_cancelled());
        });
        pause.release();
        wait_until("transport retirement", || {
            runtime.stats().pending_teardowns == 0
        });
        assert_eq!(runtime.stats().caller_cancellations, 2);
        assert_eq!(runtime.stats().truncated_bodies, 1);

        // A resolved send disarms its guard.
        runtime.set_exchange_hooks(ExchangeHooks::default());
        let mut client = runtime.client(plain_target(server.address));
        let response = block_on(client.send(request("localhost", "/", budget))).expect("ok");
        drop(response);
        drop(client);
        assert_eq!(
            end_rx.recv_timeout(HANG_GUARD).expect("server read"),
            Some(ReadEnd::Eof)
        );
        assert_eq!(runtime.stats().caller_cancellations, 2);
    }

    #[test]
    fn deadline_passing_while_the_command_is_queued_fails_as_submit_timeout_and_never_dials() {
        let server = TestServer::plain(|mut conn| {
            conn.read_request();
        });
        let runtime = start(1, "req");
        let (release_tx, release_rx) = mpsc::channel::<()>();
        runtime
            .worker_handle(0)
            .expect("published worker")
            .submit(move |_| {
                let _ = release_rx.recv_timeout(HANG_GUARD);
            })
            .expect("hold admitted");
        let mut client = runtime.client(plain_target(server.address));
        let error = block_on(client.send(request("localhost", "/", Duration::from_millis(200))))
            .expect_err("queued past its deadline");
        assert_eq!(
            (error.phase, error.kind, error.code),
            (Phase::Submit, ErrorKind::Timeout, None)
        );
        assert!(!error.reached_wire);
        drop(release_tx);
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(server.accepted(), 0);

        // The deadline passes while the send waits for a restart's publication.
        let spawn = TestPause::new();
        let _release_spawn = ReleaseOnDrop(spawn.clone());
        *runtime.hooks().spawn_pause.lock().unwrap() = Some(spawn.clone());
        runtime
            .worker_handle(0)
            .expect("published worker")
            .begin_shutdown();
        let budget = EXPIRY_BUDGET;
        let result = block_on(client.send(request("localhost", "/", budget)));
        // Held before it spawns the new worker, the restart cannot have published.
        let held = eventually(|| spawn.reached() == 1);
        spawn.release();
        let error = result.expect_err("expired waiting for publication");
        assert!(held, "the restart was held while the send waited");
        assert_eq!(
            (error.phase, error.kind, error.code),
            (Phase::Submit, ErrorKind::Timeout, None)
        );
        assert!(!error.reached_wire);
        // The deadline was fixed just before `send` began, so the queue delay is nearly the
        // whole budget.
        assert!(error.timings.queued >= budget / 2, "{:?}", error.timings);
        wait_until("restart publishes", || runtime.stats().restarts == 1);
        assert_eq!(server.accepted(), 0);

        // The caller resumes only after its deadline, when the closing worker has already
        // dropped the queued command: the expiry wins over the ready worker loss.
        let caller = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("caller runtime");
        let worker = runtime.worker_handle(0).expect("published worker");
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let (started_tx, started_rx) = mpsc::channel::<()>();
        worker
            .submit(move |_| {
                let _ = started_tx.send(());
                let _ = release_rx.recv_timeout(HANG_GUARD);
            })
            .expect("hold admitted");
        started_rx.recv_timeout(HANG_GUARD).expect("hold started");
        let mut client = runtime.client(plain_target(server.address));
        let late = request("localhost", "/", budget);
        let deadline = late.deadline;
        let mut send = pin!(client.send(late));
        let first = caller.block_on(poll_fn(|context| Poll::Ready(send.as_mut().poll(context))));
        let polled = Instant::now();
        assert!(first.is_pending());
        assert_eq!(worker.queued_commands(), 1);
        worker.begin_shutdown();
        drop(release_tx);
        wait_until("the closing worker drops the command", || {
            worker.state() == WorkerState::Closed
        });
        sleep_past(deadline);
        let error = caller
            .block_on(send)
            .expect_err("expired with the worker lost");
        assert_eq!(
            (error.phase, error.kind, error.code),
            (Phase::Submit, ErrorKind::Timeout, None)
        );
        assert!(!error.reached_wire);
        // The send began before its first poll returned.
        assert!(
            error.timings.queued >= deadline.saturating_duration_since(polled),
            "{:?}",
            error.timings
        );

        // The caller resumes only after its deadline, when the restart it waits for has already
        // failed: the expiry wins over the ready restart failure.
        let spawn = TestPause::new();
        let _release_spawn = ReleaseOnDrop(spawn.clone());
        *runtime.hooks().spawn_pause.lock().unwrap() = Some(spawn.clone());
        runtime
            .hooks()
            .worker_spawn_failures
            .lock()
            .unwrap()
            .push_back(true);
        let mut client = runtime.client(plain_target(server.address));
        let late = request("localhost", "/", budget);
        let deadline = late.deadline;
        let mut send = pin!(client.send(late));
        let first = caller.block_on(poll_fn(|context| Poll::Ready(send.as_mut().poll(context))));
        let polled = Instant::now();
        assert!(first.is_pending());
        spawn.wait_reached(1);
        sleep_past(deadline);
        spawn.release();
        // The failed restart raises its signal before its thread exits.
        wait_until("the restart fails", || {
            runtime.stats().failed_restarts == 1 && live_threads("req-restart") == 0
        });
        let error = caller
            .block_on(send)
            .expect_err("expired with the restart failed");
        assert_eq!(
            (error.phase, error.kind, error.code),
            (Phase::Submit, ErrorKind::Timeout, None)
        );
        assert!(!error.reached_wire);
        // The send began before its first poll returned.
        assert!(
            error.timings.queued >= deadline.saturating_duration_since(polled),
            "{:?}",
            error.timings
        );
        assert_eq!(runtime.stats().worker_losses, 0);
        assert_eq!(server.accepted(), 0);
    }

    fn rejected(request: Request, runtime: &crate::runtime::ExplicitRuntime, target: &Arc<Target>) {
        let mut client = runtime.client(Arc::clone(target));
        let error = block_on(client.send(request)).expect_err("rejected request");
        assert_eq!(
            (error.phase, error.kind),
            (Phase::Submit, ErrorKind::Protocol)
        );
        assert!(!error.reached_wire);
    }

    #[test]
    fn adapter_owned_headers_userinfo_and_non_origin_targets_fail_before_dialing() {
        let server = TestServer::plain(|mut conn| {
            conn.read_request();
        });
        let runtime = start(1, "rei");
        let target = plain_target(server.address);
        // `PathAndQuery` already rejects relative and absolute targets; these are the
        // non-origin forms it admits.
        for path in ["*", "?query=1"] {
            rejected(request("localhost", path, HANG_GUARD), &runtime, &target);
        }
        rejected(
            request("user:secret@localhost", "/", HANG_GUARD),
            &runtime,
            &target,
        );
        for name in [
            "connection",
            "content-length",
            "transfer-encoding",
            "upgrade",
        ] {
            rejected(
                request_with_headers("localhost", "/", HANG_GUARD, vec![(name, "1")]),
                &runtime,
                &target,
            );
        }
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(server.accepted(), 0);

        // The core refuses the same input before adoption, and its peer reads EOF.
        let (eof_tx, eof_rx) = mpsc::channel();
        let progress = started_progress();
        let core_progress = Arc::clone(&progress);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("driver runtime");
        let local = tokio::task::LocalSet::new();
        let error = local.block_on(&runtime, async move {
            let (handle, driver) = WorkerDriver::with_host(16, Host::tokio()).expect("driver");
            let driver = tokio::task::spawn_local(driver);
            let (client_io, mut peer_io) = tokio::io::duplex(1024);
            let peer = tokio::task::spawn_local(async move {
                let mut buffer = [0; 64];
                let _ = eof_tx.send(
                    peer_io
                        .read(&mut buffer)
                        .await
                        .map_err(|error| error.kind()),
                );
            });
            let call = handle
                .call_local(move |context| async move {
                    let address = "127.0.0.1:80".parse().expect("address");
                    exchange(ExchangeInput {
                        worker: context.clone(),
                        io: EstablishedIo::new(
                            client_io,
                            endpoint(address),
                            endpoint(address),
                            None,
                        ),
                        security: TransportSecurity::Plaintext,
                        request: request_with_headers(
                            "localhost",
                            "/",
                            HANG_GUARD,
                            vec![("connection", "close")],
                        ),
                        cancellation: CancellationToken::never(),
                        progress: core_progress,
                        started: context.host().now(),
                        stats: Arc::new(ExplicitStats::new()),
                    })
                    .await
                })
                .expect("command admitted");
            let outcome = call.await.expect("command result");
            handle.begin_shutdown();
            driver.await.expect("driver");
            peer.await.expect("peer");
            outcome.expect_err("refused input")
        });
        assert_eq!(
            (error.phase, error.kind),
            (Phase::Submit, ErrorKind::Protocol)
        );
        assert!(!error.reached_wire);
        assert!(!progress.reached_session());
        assert_eq!(eof_rx.recv_timeout(HANG_GUARD).expect("peer read"), Ok(0));
    }
}
