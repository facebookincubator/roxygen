use std::collections::VecDeque;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::headers::HttpHeaders;
use roxygen_core::http::CodecProtocol;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_core::url::Url;
use roxygen_core::url::UrlMode;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::tokio_host::ManagedWorker;
use roxygen_runtime::worker::WorkerContext;
use roxygen_server::acceptor::AcceptedConnectionObserver;
use roxygen_server::acceptor::ConnectionInfo;
use roxygen_server::scoped_server::ScopedHttpServer;
use roxygen_server::server::HandlerFactory;
use roxygen_server::server::HttpServerState;
use roxygen_server::server::ServerConfig;
use roxygen_server::server::ServerSocketConfig;
use roxygen_server::server::StaticTlsCredentials;
use roxygen_transport::tls::TlsClientOptions;
use roxygen_transport::tls::TlsServerOptions;

use crate::HttpClient;
use crate::RequestSpec;
use crate::connector::ClientTransportOptions;
use crate::connector::ConnectionParams;
use crate::connector::HttpConnector;
use crate::connector::SessionParams;

#[derive(Default)]
struct ServerStats {
    requests: AtomicUsize,
}

struct EchoHandler {
    stats: Arc<ServerStats>,
}

struct InformationalResponse {
    headers: VecDeque<HttpHeaderEvent>,
}

struct FailingResponse {
    header: Option<HttpHeaderEvent>,
    body_sent: bool,
}

struct PendingHeadersResponse;

impl HttpSource for PendingHeadersResponse {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::pending())
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(std::future::pending())
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

impl FailingResponse {
    fn at_body() -> Self {
        let mut message = HttpMessage::new();
        message.set_status_code(200).expect("test status is valid");
        message
            .set_status_message(HttpMessage::default_reason(200))
            .expect("test reason is valid");
        message.set_http_version(1, 1);
        Self {
            header: Some(HttpHeaderEvent::new(message, false).expect("header event is valid")),
            body_sent: false,
        }
    }
}

impl HttpSource for FailingResponse {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let result = self.header.take().ok_or_else(|| {
            HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"response header was already consumed",
            )
        });
        Box::pin(std::future::ready(result))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        if !self.body_sent {
            self.body_sent = true;
            return Box::pin(std::future::ready(Ok(HttpBodyEvent::body(
                BufQueue::from_bytes(Bytes::from_static(b"hello world"), 11)
                    .expect("partial test body is bounded"),
                false,
            ))));
        }
        Box::pin(std::future::ready(Err(HttpError::with_message(
            HttpErrorCode::PROTOCOL_ERROR,
            b"server body source failed",
        ))))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

impl InformationalResponse {
    fn new() -> Self {
        let headers = [103, 200]
            .into_iter()
            .map(|status| {
                let mut message = HttpMessage::new();
                message
                    .set_status_code(status)
                    .expect("test status is valid");
                message
                    .set_status_message(HttpMessage::default_reason(status))
                    .expect("test reason is valid");
                message.set_http_version(1, 1);
                HttpHeaderEvent::new(message, status == 200).expect("header event is valid")
            })
            .collect();
        Self { headers }
    }
}

impl HttpSource for InformationalResponse {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let event = self.headers.pop_front().ok_or_else(|| {
            HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"informational response headers exhausted",
            )
        });
        Box::pin(std::future::ready(event))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(std::future::ready(Err(HttpError::with_message(
            HttpErrorCode::INVALID_STATE_TRANSITION,
            b"header-only informational response has no body",
        ))))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

impl HttpHandler for EchoHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let stats = Arc::clone(&self.stats);
        Box::pin(async move {
            stats.requests.fetch_add(1, Ordering::Relaxed);
            let header_event = request.read_header_event().await?;
            let header_eom = header_event.eom();
            let mut request_message = header_event.into_headers();
            let method = request_message
                .method_string()
                .expect("test request has request fields")
                .as_bytes()
                .to_vec();
            let path = request_message
                .path()
                .expect("test request has a path")
                .to_vec();
            let host = request_message
                .headers()
                .get_single_or_empty(b"host")
                .to_vec();
            let custom_count = request_message
                .headers()
                .number_of_values(b"custom".as_slice());
            let mut request_body = BufQueue::new(2 * 1024 * 1024);
            if !header_eom && path != b"/earlyreturn" {
                loop {
                    let event = request.read_body_event(u32::MAX).await?;
                    let eom = event.eom();
                    if let Some(body) = event.as_body() {
                        let mut body = body.clone_shared();
                        request_body.append(&mut body).map_err(|error| {
                            HttpError::with_message(
                                HttpErrorCode::INTERNAL_ERROR,
                                error.to_string(),
                            )
                        })?;
                    }
                    if eom {
                        break;
                    }
                }
            }
            if path == b"/informational" {
                return Ok(HttpSourceHolder::new(
                    Box::new(InformationalResponse::new()),
                ));
            }
            if path == b"/abortHeaders" {
                return Err(HttpError::with_message(
                    HttpErrorCode::INTERNAL_ERROR,
                    b"server handler failed before response headers",
                ));
            }
            if path == b"/abortBody" {
                return Ok(HttpSourceHolder::new(Box::new(FailingResponse::at_body())));
            }
            if path == b"/pendingHeaders" {
                return Ok(HttpSourceHolder::new(Box::new(PendingHeadersResponse)));
            }
            let status = if path == b"/error" { 500 } else { 200 };
            let response_body = if path == b"/trailers" {
                Some(
                    BufQueue::from_bytes(Bytes::from_static(b"x"), 1)
                        .expect("test response is bounded"),
                )
            } else if method == b"POST" && !request_body.is_empty() {
                Some(request_body)
            } else {
                None
            };
            let mut response = HttpFixedSource::make_fixed_response(status, response_body)
                .expect("test response is valid");
            let message = response
                .message_mut()
                .expect("fixed response retains its headers");
            message.headers_mut().set(b"x-method", method);
            message.headers_mut().set(b"x-host", host);
            message
                .headers_mut()
                .set(b"x-custom-header-count", custom_count.to_string());
            if path == b"/trailers" {
                let mut trailers = HttpHeaders::new();
                trailers.set(b"Test", b"Success");
                response.set_trailers(trailers);
            }
            Ok(HttpSourceHolder::new(Box::new(response)))
        })
    }
}

fn handler_factory(stats: Arc<ServerStats>) -> HandlerFactory {
    Arc::new(move |_| {
        Rc::new(EchoHandler {
            stats: Arc::clone(&stats),
        }) as HttpHandlerRef
    })
}

fn start_plain_server() -> (ScopedHttpServer, Arc<ServerStats>) {
    let stats = Arc::new(ServerStats::default());
    let server = ScopedHttpServer::start(
        ServerConfig::default(),
        handler_factory(Arc::clone(&stats)),
        None,
    )
    .expect("test server starts");
    (server, stats)
}

fn start_tls_server(protocols: &[&[u8]]) -> (ScopedHttpServer, Vec<u8>) {
    let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("test certificate is generated");
    let certificate = generated.cert.der().to_vec();
    let tls = TlsServerOptions::new(
        vec![certificate.clone().into_boxed_slice()],
        generated
            .signing_key
            .serialized_der()
            .to_vec()
            .into_boxed_slice(),
        protocols
            .iter()
            .map(|protocol| protocol.to_vec().into_boxed_slice())
            .collect(),
    )
    .expect("test TLS options are valid");
    let config = ServerConfig {
        tls_credentials: Some(Arc::new(StaticTlsCredentials::new(tls))),
        ..ServerConfig::default()
    };
    let server = ScopedHttpServer::start(
        config,
        handler_factory(Arc::new(ServerStats::default())),
        None,
    )
    .expect("TLS test server starts");
    (server, certificate)
}

fn run_on_worker<T, F>(factory: F) -> T
where
    T: Send + 'static,
    F: FnOnce(WorkerContext) -> Pin<Box<dyn Future<Output = T> + 'static>> + Send + 'static,
{
    let worker = ManagedWorker::new(64).expect("client worker starts");
    let call = worker
        .handle()
        .call_local(factory)
        .expect("client task is admitted");
    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
        .block_on(call)
        .expect("client task completes");
    worker.shutdown().expect("client worker shuts down");
    result
}

fn socket_address(address: SocketAddr) -> roxygen_core::message::SocketAddress {
    roxygen_core::message::SocketAddress::new(address.ip().to_string(), address.port())
}

#[test]
fn zero_copy_h1_h2_plaintext_tls_matrix() {
    use std::cell::RefCell;

    use roxygen_session::session::LifecycleEvent;
    use roxygen_session::session::LifecycleObserver;

    const PAYLOAD_LENGTH: usize = 256 * 1024;

    #[derive(Debug, Eq, PartialEq)]
    enum CaseProgress {
        ForceStopStarted,
        Finished,
    }

    struct ZeroCopyObserver(Arc<Mutex<Vec<bool>>>);

    impl AcceptedConnectionObserver for ZeroCopyObserver {
        fn on_accepted(&self, connection: &ConnectionInfo) {
            self.0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(connection.zero_copy_enabled);
        }
    }

    #[derive(Default)]
    struct CloseLifecycleTrace(RefCell<VecDeque<LifecycleEvent>>);

    impl LifecycleObserver for CloseLifecycleTrace {
        fn on_event(&self, event: LifecycleEvent) {
            if matches!(
                event,
                LifecycleEvent::Http2Read { .. } | LifecycleEvent::Http2Write { .. }
            ) {
                return;
            }
            let mut events = self.0.borrow_mut();
            if events.len() == 64 {
                let _ = events.pop_front();
            }
            events.push_back(event);
        }
    }

    #[derive(Clone, Debug, Default)]
    struct ServerCloseTrace {
        first_drain: Option<LifecycleEvent>,
        last_64_lifecycle_events: VecDeque<LifecycleEvent>,
        successful_h2_read_bytes: usize,
        successful_h2_write_bytes: usize,
    }

    struct ServerCloseObserver(Arc<Mutex<ServerCloseTrace>>);

    impl LifecycleObserver for ServerCloseObserver {
        fn on_event(&self, event: LifecycleEvent) {
            let mut trace = self
                .0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match event {
                LifecycleEvent::Http2Read { bytes, .. } => {
                    trace.successful_h2_read_bytes =
                        trace.successful_h2_read_bytes.saturating_add(bytes);
                    return;
                }
                LifecycleEvent::Http2Write { bytes } => {
                    trace.successful_h2_write_bytes =
                        trace.successful_h2_write_bytes.saturating_add(bytes);
                    return;
                }
                LifecycleEvent::DrainStarted(_) if trace.first_drain.is_none() => {
                    trace.first_drain = Some(event);
                }
                _ => {}
            }
            if trace.last_64_lifecycle_events.len() == 64 {
                let _ = trace.last_64_lifecycle_events.pop_front();
            }
            trace.last_64_lifecycle_events.push_back(event);
        }
    }

    fn server_trace_snapshot(trace: &Mutex<ServerCloseTrace>) -> ServerCloseTrace {
        trace
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    let payload = (0..PAYLOAD_LENGTH)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    for (name, protocol, use_tls) in [
        ("H1 plaintext", CodecProtocol::Http11, false),
        ("H1 TLS", CodecProtocol::Http11, true),
        ("H2 plaintext", CodecProtocol::Http2, false),
        ("H2 TLS", CodecProtocol::Http2, true),
    ] {
        let request_payload = payload.clone();
        let server_trace = Arc::new(Mutex::new(ServerCloseTrace::default()));
        let case_server_trace = Arc::clone(&server_trace);
        let (progress_tx, progress_rx) = mpsc::sync_channel(1);
        let case_thread = thread::spawn(move || {
            let stats = Arc::new(ServerStats::default());
            let zero_copy = Arc::new(Mutex::new(Vec::new()));
            let mut config = ServerConfig {
                socket: ServerSocketConfig {
                    use_zero_copy: true,
                    ..ServerSocketConfig::default()
                },
                plaintext_protocol: protocol,
                connection_observer: Some(Arc::new(ZeroCopyObserver(Arc::clone(&zero_copy)))),
                zero_copy_enable_threshold: 1_024,
                ..ServerConfig::default()
            };
            let observer_trace = Arc::clone(&case_server_trace);
            config.session.lifecycle_observer_factory = Some(Arc::new(move || {
                Rc::new(ServerCloseObserver(Arc::clone(&observer_trace)))
            }));
            let certificate = use_tls.then(|| {
                let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
                    .expect("test certificate is generated");
                let certificate = generated.cert.der().to_vec();
                let alpn = match protocol {
                    CodecProtocol::Http2 => b"h2".as_slice(),
                    CodecProtocol::Http11 => b"http/1.1".as_slice(),
                    _ => unreachable!("the matrix contains only H1 and H2"),
                };
                let tls = TlsServerOptions::new(
                    vec![certificate.clone().into_boxed_slice()],
                    generated
                        .signing_key
                        .serialized_der()
                        .to_vec()
                        .into_boxed_slice(),
                    vec![alpn.to_vec().into_boxed_slice()],
                )
                .expect("test TLS options are valid");
                config.tls_credentials = Some(Arc::new(StaticTlsCredentials::new(tls)));
                certificate
            });
            let server = ScopedHttpServer::start(config, handler_factory(Arc::clone(&stats)), None)
                .unwrap_or_else(|error| panic!("{name} server starts: {error}"));
            let address = server.address().expect("matrix server has an address");
            let expected_payload = request_payload.clone();

            let (negotiated, response_body) = run_on_worker(move |context| {
                Box::pin(async move {
                    let close_server_trace = Arc::clone(&case_server_trace);
                    tokio::time::timeout(Duration::from_secs(10), async move {
                        let transport = match certificate {
                            Some(certificate) => {
                                let mut tls = TlsClientOptions::new("localhost");
                                tls.add_root_certificate(certificate);
                                let alpn = match protocol {
                                    CodecProtocol::Http2 => b"h2".as_slice(),
                                    CodecProtocol::Http11 => b"http/1.1".as_slice(),
                                    _ => unreachable!("the matrix contains only H1 and H2"),
                                };
                                tls.set_alpn_protocols([alpn]);
                                ClientTransportOptions::Tls(tls)
                            }
                            None => ClientTransportOptions::Plaintext,
                        };
                        let lifecycle = Rc::new(CloseLifecycleTrace::default());
                        let session = HttpConnector::connect(
                            &context,
                            socket_address(address),
                            Duration::from_secs(2),
                            ConnectionParams {
                                transport,
                                plaintext_protocol: protocol,
                                ..ConnectionParams::default()
                            },
                            SessionParams {
                                lifecycle_observer: Some(lifecycle.clone()),
                                ..SessionParams::default()
                            },
                            CancellationToken::never(),
                        )
                        .await
                        .unwrap_or_else(|error| panic!("{name} client connects: {error}"));
                        let scheme = if use_tls { "https" } else { "http" };
                        let url = Url::parse(
                            format!("{scheme}://localhost:{}/matrix", address.port()),
                            false,
                            UrlMode::StrictCompat,
                        );
                        let mut response = HttpClient::request(
                            &session,
                            None,
                            RequestSpec::post(url, &request_payload),
                        )
                        .await
                        .unwrap_or_else(|error| panic!("{name} request succeeds: {error}"));
                        assert_eq!(
                            response
                                .headers
                                .status_code()
                                .expect("matrix response has a status"),
                            200,
                            "{name} response status"
                        );
                        let response_body =
                            response
                                .body
                                .coalesce(PAYLOAD_LENGTH)
                                .unwrap_or_else(|error| {
                                    panic!("{name} response body is bounded: {error}")
                                });
                        tokio::time::timeout(Duration::from_secs(2), async {
                            while session.active_transactions() != 0 {
                                tokio::task::yield_now().await;
                            }
                        })
                        .await
                        .unwrap_or_else(|_| {
                            panic!(
                                "{name} transaction detaches within the deadline; server_trace={:?}",
                                server_trace_snapshot(&close_server_trace)
                            )
                        });
                        assert_eq!(
                            session.active_transactions(),
                            0,
                            "{name} transaction detaches after its body completes"
                        );
                        let close_result = session.close_when_idle();
                        let failed_close_completion = if matches!(close_result, Ok(true)) {
                            None
                        } else {
                            Some(
                                tokio::time::timeout(
                                    Duration::from_secs(2),
                                    session.wait_closed(),
                                )
                                .await,
                            )
                        };
                        assert!(
                            close_result.unwrap_or_else(|error| panic!(
                                "{name} close-when-idle succeeds: {error}; native_completion={failed_close_completion:?}; last_64_lifecycle_events={:?}; server_trace={:?}",
                                lifecycle.0.borrow(),
                                server_trace_snapshot(&close_server_trace)
                            )),
                            "{name} starts graceful session close; native_completion={failed_close_completion:?}; last_64_lifecycle_events={:?}; server_trace={:?}",
                            lifecycle.0.borrow(),
                            server_trace_snapshot(&close_server_trace)
                        );
                        tokio::time::timeout(Duration::from_secs(2), session.wait_closed())
                        .await
                        .unwrap_or_else(|_| panic!(
                            "{name} session closes within the deadline; last_64_lifecycle_events={:?}; server_trace={:?}",
                            lifecycle.0.borrow(),
                            server_trace_snapshot(&close_server_trace)
                        ))
                        .unwrap_or_else(|error| panic!(
                            "{name} native session close succeeds: {error:?}; last_64_lifecycle_events={:?}; server_trace={:?}",
                            lifecycle.0.borrow(),
                            server_trace_snapshot(&close_server_trace)
                        ));
                        assert!(
                            session.is_finished(),
                            "{name} native session is finished; server_trace={:?}",
                            server_trace_snapshot(&close_server_trace)
                        );
                        (session.protocol(), response_body)
                    })
                    .await
                    .unwrap_or_else(|_| panic!(
                        "{name} client exchange remains bounded; server_trace={:?}",
                        server_trace_snapshot(&case_server_trace)
                    ))
                })
            });

            let zero_copy_enabled = {
                let modes = zero_copy
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                assert_eq!(modes.len(), 1, "{name} accepts exactly one connection");
                modes[0]
            };
            let egress_path = if zero_copy_enabled {
                "kernel zero-copy"
            } else {
                "ordinary-write fallback"
            };
            assert_eq!(negotiated, protocol, "{name} negotiated protocol");
            assert_eq!(
                response_body.as_ref(),
                expected_payload.as_slice(),
                "{name} exact payload over {egress_path}"
            );
            assert_eq!(
                stats.requests.load(Ordering::Acquire),
                1,
                "{name} server handles exactly one request over {egress_path}"
            );

            progress_tx
                .send(CaseProgress::ForceStopStarted)
                .expect("matrix driver waits for force-stop start");
            assert!(
                server
                    .force_stop()
                    .unwrap_or_else(|error| panic!("{name} server stops: {error}"))
            );
            assert_eq!(server.state(), HttpServerState::Stopped);
            drop(server);
            let _ = progress_tx.send(CaseProgress::Finished);
        });

        match progress_rx.recv_timeout(Duration::from_secs(12)) {
            Ok(CaseProgress::ForceStopStarted) => {}
            Ok(CaseProgress::Finished) => panic!("{name} skipped its force-stop start signal"),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                case_thread.join().expect("matrix case reports its panic");
                panic!("{name} exited before force-stop");
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                drop(case_thread);
                panic!(
                    "{name} client exchange exceeded its deadline; its helper thread retains server cleanup ownership; server_trace={:?}",
                    server_trace_snapshot(&server_trace)
                );
            }
        }
        match progress_rx.recv_timeout(Duration::from_secs(2)) {
            Ok(CaseProgress::Finished) => case_thread
                .join()
                .expect("completed matrix case joins without panicking"),
            Ok(CaseProgress::ForceStopStarted) => {
                panic!("{name} repeated its force-stop start signal")
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                case_thread.join().expect("matrix case reports its panic");
                panic!("{name} exited during force-stop");
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                drop(case_thread);
                panic!(
                    "{name} server force-stop exceeded its deadline; its helper thread retains cleanup ownership; server_trace={:?}",
                    server_trace_snapshot(&server_trace)
                );
            }
        }
    }
}

mod client {
    use crate::connector::IdentityValidation;
    use crate::connector::ValidationPolicy;
    use crate::connector::get_fallback_ca_path;
    use crate::connector::validation_policy;

    #[test]
    fn identity_validation_policy_maps_disabled_enforcing_and_logging() {
        assert_eq!(
            validation_policy(IdentityValidation::DisabledInsecure),
            None
        );
        assert_eq!(
            validation_policy(IdentityValidation::Enforcing),
            Some(ValidationPolicy::Enforcing)
        );
        assert_eq!(
            validation_policy(IdentityValidation::Logging),
            Some(ValidationPolicy::Logging)
        );
    }

    #[test]
    fn identity_validation_unknown_mode_is_rejected() {
        assert!(IdentityValidation::try_from(3).is_err());
    }

    #[test]
    fn fallback_ca_path_matches_the_build_configuration() {
        assert_eq!(
            get_fallback_ca_path(),
            option_env!("PROXYGEN_FALLBACK_CA_PATH")
        );
    }
}

mod dns {
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::net::IpAddr;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::time::Duration;

    use roxygen_core::error::HttpError;
    use roxygen_core::error::HttpErrorCode;
    use roxygen_core::message::SocketAddress;
    use roxygen_runtime::cancellation::CancellationToken;
    use roxygen_runtime::tokio_host::CallerDrivenGuard;
    use roxygen_runtime::tokio_host::ManagedWorker;
    use roxygen_runtime::wait::CancellableBaton;

    use crate::connector::CoroDnsResolver;
    use crate::connector::DnsResolver;
    use crate::connector::DnsStatsCollector;
    use crate::connector::LocalClientFuture;
    use crate::integration_tests::run_on_worker;
    use crate::pool::ClientConnectionCache;
    use crate::pool::PoolError;

    struct FixedResolver(Result<Vec<SocketAddress>, HttpError>);

    #[derive(Default)]
    struct RecordingDnsStats {
        successes: Cell<usize>,
        errors: Cell<usize>,
    }

    impl DnsStatsCollector for RecordingDnsStats {
        fn record_success(&self, addresses: &[SocketAddress], _latency: Duration) {
            assert!(!addresses.is_empty());
            self.successes.set(self.successes.get() + 1);
        }

        fn record_error(&self, error: &HttpError, _latency: Duration) {
            assert_eq!(error.code(), HttpErrorCode::CONNECT_ERROR);
            self.errors.set(self.errors.get() + 1);
        }
    }

    struct DelayedResolver {
        gate: Rc<CancellableBaton>,
    }

    struct ShutdownResolver {
        started: RefCell<Option<mpsc::Sender<()>>>,
        dropped: Arc<AtomicBool>,
    }

    impl Drop for ShutdownResolver {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Release);
        }
    }

    impl DnsResolver for FixedResolver {
        fn resolve(
            &self,
            _hostname: Box<str>,
            _timeout: Duration,
            _cancellation: CancellationToken,
        ) -> LocalClientFuture<Result<Vec<SocketAddress>, HttpError>> {
            let result = self.0.clone();
            Box::pin(std::future::ready(result))
        }
    }

    impl DnsResolver for DelayedResolver {
        fn resolve(
            &self,
            _hostname: Box<str>,
            _timeout: Duration,
            cancellation: CancellationToken,
        ) -> LocalClientFuture<Result<Vec<SocketAddress>, HttpError>> {
            let gate = Rc::clone(&self.gate);
            Box::pin(async move {
                let _ = gate.wait(cancellation).await;
                Ok(vec![SocketAddress::new("192.0.2.1", 0)])
            })
        }
    }

    impl DnsResolver for ShutdownResolver {
        fn resolve(
            &self,
            _hostname: Box<str>,
            _timeout: Duration,
            _cancellation: CancellationToken,
        ) -> LocalClientFuture<Result<Vec<SocketAddress>, HttpError>> {
            self.started
                .borrow_mut()
                .take()
                .expect("shutdown resolver starts once")
                .send(())
                .expect("shutdown test receives start");
            Box::pin(std::future::pending())
        }
    }

    fn resolve(addresses: Vec<SocketAddress>) -> crate::connector::ResolvedHost {
        run_on_worker(move |context| {
            Box::pin(async move {
                CoroDnsResolver::resolve_host(
                    &context,
                    "example.com",
                    Duration::from_secs(1),
                    CancellationToken::never(),
                    Some(Rc::new(FixedResolver(Ok(addresses)))),
                )
                .await
                .expect("resolution succeeds")
            })
        })
    }

    #[test]
    fn zero_timeout_does_not_expire_the_default_resolver() {
        let addresses = run_on_worker(|context| {
            Box::pin(async move {
                CoroDnsResolver::resolve_host_all(
                    &context,
                    "localhost",
                    Duration::ZERO,
                    CancellationToken::never(),
                    None,
                )
                .await
                .expect("zero means no DNS timeout")
            })
        });
        assert!(!addresses.is_empty());
    }

    #[test]
    fn primary_and_cross_family_fallback() {
        let resolved = resolve(vec![
            SocketAddress::new("2001:db8::1", 0),
            SocketAddress::new("192.0.2.1", 0),
        ]);
        let fallback = resolved
            .fallback
            .expect("cross-family fallback is retained");
        assert_ne!(resolved.primary.is_ipv6(), fallback.is_ipv6());
    }

    #[test]
    fn resolver_error_is_preserved() {
        let error = HttpError::with_message(HttpErrorCode::CONNECT_ERROR, b"no names");
        let result = run_on_worker(move |context| {
            Box::pin(async move {
                CoroDnsResolver::resolve_host(
                    &context,
                    "missing.example",
                    Duration::from_secs(1),
                    CancellationToken::never(),
                    Some(Rc::new(FixedResolver(Err(error)))),
                )
                .await
            })
        });
        assert_eq!(result.expect_err("resolution fails").message(), b"no names");
    }

    #[test]
    fn worker_dns_stats_are_first_wins_and_attach_only_to_default_creation() {
        let observed = run_on_worker(move |context| {
            Box::pin(async move {
                let first = Rc::new(RecordingDnsStats::default());
                let ignored = Rc::new(RecordingDnsStats::default());
                assert!(
                    CoroDnsResolver::emplace_worker_stats_collector(&context, first.clone(),)
                        .expect("worker DNS stats are installed")
                );
                assert!(
                    !CoroDnsResolver::emplace_worker_stats_collector(&context, ignored.clone(),)
                        .expect("second worker DNS stats installation is ignored")
                );

                CoroDnsResolver::resolve_host_all(
                    &context,
                    "example.com",
                    Duration::from_secs(1),
                    CancellationToken::never(),
                    Some(Rc::new(FixedResolver(Ok(vec![SocketAddress::new(
                        "127.0.0.1",
                        0,
                    )])))),
                )
                .await
                .expect("explicit resolution succeeds without worker stats");
                let before_default = first.successes.get();

                CoroDnsResolver::resolve_host_all(
                    &context,
                    "localhost",
                    Duration::from_secs(1),
                    CancellationToken::never(),
                    None,
                )
                .await
                .expect("default resolution succeeds with worker stats");
                CoroDnsResolver::set_worker_resolver(
                    &context,
                    Rc::new(FixedResolver(Ok(vec![SocketAddress::new("127.0.0.1", 0)]))),
                )
                .expect("custom worker resolver is installed");
                CoroDnsResolver::resolve_host_all(
                    &context,
                    "example.com",
                    Duration::from_secs(1),
                    CancellationToken::never(),
                    None,
                )
                .await
                .expect("custom worker resolution succeeds without default stats");
                (
                    before_default,
                    first.successes.get(),
                    first.errors.get(),
                    ignored.successes.get(),
                )
            })
        });
        assert_eq!(observed, (0, 1, 0, 0));
    }

    #[test]
    fn late_dns_stats_install_does_not_modify_an_existing_default_resolver() {
        let observed = run_on_worker(move |context| {
            Box::pin(async move {
                CoroDnsResolver::resolve_host_all(
                    &context,
                    "localhost",
                    Duration::from_secs(1),
                    CancellationToken::never(),
                    None,
                )
                .await
                .expect("default resolver is created before stats installation");
                let stats = Rc::new(RecordingDnsStats::default());
                assert!(
                    CoroDnsResolver::emplace_worker_stats_collector(&context, stats.clone(),)
                        .expect("late worker DNS stats are stored")
                );
                CoroDnsResolver::resolve_host_all(
                    &context,
                    "localhost",
                    Duration::from_secs(1),
                    CancellationToken::never(),
                    None,
                )
                .await
                .expect("existing default resolver continues to work");
                stats.successes.get()
            })
        });
        assert_eq!(observed, 0);
    }

    #[test]
    fn resolve_host_selects_fallback() {
        let resolved = resolve(vec![
            SocketAddress::new("192.0.2.1", 0),
            SocketAddress::new("192.0.2.2", 0),
            SocketAddress::new("2001:db8::1", 0),
        ]);
        let fallback = resolved
            .fallback
            .expect("cross-family fallback is selected");
        assert_ne!(resolved.primary.is_ipv6(), fallback.is_ipv6());
    }

    #[test]
    fn resolve_all_applies_rfc6724_precedence() {
        let input = vec![
            SocketAddress::new("192.0.2.1", 0),
            SocketAddress::new("2001:db8::1", 0),
        ];
        let actual = run_on_worker(move |context| {
            Box::pin(async move {
                CoroDnsResolver::resolve_host_all(
                    &context,
                    "example.com",
                    Duration::from_secs(1),
                    CancellationToken::never(),
                    Some(Rc::new(FixedResolver(Ok(input)))),
                )
                .await
                .expect("resolution succeeds")
            })
        });
        assert_eq!(actual.len(), 2);
        assert_ne!(actual[0].is_ipv6(), actual[1].is_ipv6());
    }

    #[test]
    fn rfc6724_rejects_unusable_destinations_before_precedence() {
        let actual = run_on_worker(move |context| {
            Box::pin(async move {
                CoroDnsResolver::resolve_host_all(
                    &context,
                    "example.com",
                    Duration::from_secs(1),
                    CancellationToken::never(),
                    Some(Rc::new(FixedResolver(Ok(vec![
                        SocketAddress::new("fe80::1", 0),
                        SocketAddress::new("127.0.0.1", 0),
                    ])))),
                )
                .await
                .expect("resolution succeeds")
            })
        });
        assert_eq!(actual[0], SocketAddress::new("127.0.0.1", 0));
    }

    #[test]
    fn rfc6724_prefers_scope_label_and_ipv6_prefix_but_keeps_ipv4_order() {
        let link_source = "fe80::2".parse::<IpAddr>().expect("link-local source");
        let scope_sorted = CoroDnsResolver::sort_addresses_with_source(
            vec![
                SocketAddress::new("2001:db8::1", 0),
                SocketAddress::new("fe80::1", 0),
            ],
            link_source,
        );
        assert_eq!(scope_sorted[0], SocketAddress::new("fe80::1", 0));

        let ula_source = "fd00::2".parse::<IpAddr>().expect("ULA source");
        let label_sorted = CoroDnsResolver::sort_addresses_with_source(
            vec![
                SocketAddress::new("2001:db8::1", 0),
                SocketAddress::new("fd00::1", 0),
            ],
            ula_source,
        );
        assert_eq!(label_sorted[0], SocketAddress::new("fd00::1", 0));

        let global_source = "2001:db8:1::9".parse::<IpAddr>().expect("IPv6 source");
        let prefix_sorted = CoroDnsResolver::sort_addresses_with_source(
            vec![
                SocketAddress::new("2001:db8:2::1", 0),
                SocketAddress::new("2001:db8:1::1", 0),
            ],
            global_source,
        );
        assert_eq!(prefix_sorted[0], SocketAddress::new("2001:db8:1::1", 0));

        let ipv4_source = "192.0.2.99".parse::<IpAddr>().expect("IPv4 source");
        let ipv4_sorted = CoroDnsResolver::sort_addresses_with_source(
            vec![
                SocketAddress::new("192.0.2.200", 0),
                SocketAddress::new("192.0.2.1", 0),
            ],
            ipv4_source,
        );
        assert_eq!(
            ipv4_sorted,
            vec![
                SocketAddress::new("192.0.2.200", 0),
                SocketAddress::new("192.0.2.1", 0),
            ]
        );
    }

    #[test]
    fn explicit_resolver_overrides_worker_default() {
        let resolved = run_on_worker(move |context| {
            Box::pin(async move {
                CoroDnsResolver::set_worker_resolver(
                    &context,
                    Rc::new(FixedResolver(Err(HttpError::new(
                        HttpErrorCode::INTERNAL_ERROR,
                    )))),
                )
                .expect("worker resolver is installed");
                let resolved = CoroDnsResolver::resolve_host(
                    &context,
                    "example.com",
                    Duration::from_secs(1),
                    CancellationToken::never(),
                    Some(Rc::new(FixedResolver(Ok(vec![SocketAddress::new(
                        "192.0.2.8",
                        0,
                    )])))),
                )
                .await
                .expect("explicit resolver succeeds");
                CoroDnsResolver::restore_default_worker_resolver(&context)
                    .expect("worker resolver is restored");
                resolved
            })
        });
        assert_eq!(resolved.primary, SocketAddress::new("192.0.2.8", 0));
    }

    #[test]
    fn disabled_worker_resolver_fails_without_hanging() {
        let result = run_on_worker(move |context| {
            Box::pin(async move {
                CoroDnsResolver::disable_worker_resolver(&context)
                    .expect("worker resolver is disabled");
                let result = CoroDnsResolver::resolve_host(
                    &context,
                    "example.com",
                    Duration::from_secs(1),
                    CancellationToken::never(),
                    None,
                )
                .await;
                CoroDnsResolver::restore_default_worker_resolver(&context)
                    .expect("worker resolver is restored");
                result
            })
        });
        assert_eq!(
            result.expect_err("disabled resolver fails").code(),
            HttpErrorCode::DROPPED
        );
    }

    #[test]
    fn resolver_state_is_isolated_for_workers_on_one_thread() {
        let isolated = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime")
            .block_on(async {
                let mut first = CallerDrivenGuard::new(8).expect("first worker starts");
                let first_call = first
                    .handle()
                    .call_local(|context| async move {
                        CoroDnsResolver::disable_worker_resolver(&context)
                            .expect("first worker resolver is disabled");
                    })
                    .expect("first worker accepts command");
                first
                    .run_until(first_call)
                    .await
                    .expect("first worker is driven")
                    .expect("first command completes");

                let mut second = CallerDrivenGuard::new(8).expect("second worker starts");
                let second_call = second
                    .handle()
                    .call_local(|context| async move {
                        CoroDnsResolver::resolve_host(
                            &context,
                            "localhost",
                            Duration::from_secs(1),
                            CancellationToken::never(),
                            None,
                        )
                        .await
                        .is_ok()
                    })
                    .expect("second worker accepts command");
                let resolved = second
                    .run_until(second_call)
                    .await
                    .expect("second worker is driven")
                    .expect("second command completes");
                first.shutdown().await.expect("first worker shuts down");
                second.shutdown().await.expect("second worker shuts down");
                resolved
            });
        assert!(isolated);
    }

    #[test]
    fn replacing_resolver_rejects_outstanding_resolution() {
        let code = run_on_worker(|context| {
            Box::pin(async move {
                let gate = Rc::new(CancellableBaton::new());
                CoroDnsResolver::set_worker_resolver(
                    &context,
                    Rc::new(DelayedResolver {
                        gate: Rc::clone(&gate),
                    }),
                )
                .expect("worker resolver is installed");
                let resolution = CoroDnsResolver::resolve_host(
                    &context,
                    "example.com",
                    Duration::from_secs(1),
                    CancellationToken::never(),
                    None,
                );
                tokio::pin!(resolution);
                tokio::select! {
                    _ = &mut resolution => panic!("resolution completed before resolver replacement"),
                    () = tokio::time::sleep(Duration::from_millis(5)) => {}
                }
                CoroDnsResolver::disable_worker_resolver(&context)
                    .expect("worker resolver is disabled");
                tokio::time::timeout(Duration::from_secs(1), resolution)
                    .await
                    .expect("replacement wakes the outstanding resolution")
                    .expect_err("replaced resolver rejects outstanding result")
                    .code()
            })
        });
        assert_eq!(code, HttpErrorCode::DROPPED);
    }

    #[test]
    fn replacing_worker_resolver_does_not_cancel_explicit_resolution() {
        let resolved = run_on_worker(|context| {
            Box::pin(async move {
                let gate = Rc::new(CancellableBaton::new());
                let resolution = CoroDnsResolver::resolve_host_all(
                    &context,
                    "example.com",
                    Duration::from_secs(1),
                    CancellationToken::never(),
                    Some(Rc::new(DelayedResolver {
                        gate: Rc::clone(&gate),
                    })),
                );
                tokio::pin!(resolution);
                tokio::select! {
                    _ = &mut resolution => panic!("explicit resolution completed before release"),
                    () = tokio::time::sleep(Duration::from_millis(5)) => {}
                }
                CoroDnsResolver::disable_worker_resolver(&context)
                    .expect("worker resolver is disabled");
                gate.signal();
                resolution.await.expect("explicit resolver remains active")
            })
        });
        assert_eq!(resolved, [SocketAddress::new("192.0.2.1", 0)]);
    }

    #[test]
    fn replacing_or_draining_cache_resolver_wakes_pending_resolution() {
        let codes = run_on_worker(|context| {
            Box::pin(async move {
                let mut codes = Vec::new();
                for drain in [false, true] {
                    let gate = Rc::new(CancellableBaton::new());
                    let cache = ClientConnectionCache::with_default_limit(context.clone());
                    cache
                        .set_dns_resolver(Some(Rc::new(DelayedResolver {
                            gate: Rc::clone(&gate),
                        })))
                        .expect("cache resolver is installed");
                    let resolution = cache.get_session_with_reservation(
                        "example.com",
                        80,
                        false,
                        Duration::from_secs(30),
                        None,
                        CancellationToken::never(),
                    );
                    tokio::pin!(resolution);
                    tokio::select! {
                        _ = &mut resolution => panic!("resolution completed before cache mutation"),
                        () = tokio::time::sleep(Duration::from_millis(5)) => {}
                    }
                    if drain {
                        assert!(cache.drain());
                    } else {
                        cache
                            .set_dns_resolver(Some(Rc::new(FixedResolver(Ok(vec![
                                SocketAddress::new("127.0.0.1", 0),
                            ])))))
                            .expect("cache resolver is replaced");
                    }
                    let result = tokio::time::timeout(Duration::from_secs(1), resolution)
                        .await
                        .expect("cache mutation wakes the outstanding resolution");
                    let error = match result {
                        Ok(_) => panic!("stale cache resolution unexpectedly succeeded"),
                        Err(error) => error,
                    };
                    let PoolError::ConnectFailed { source } = error else {
                        panic!("cache resolver mutation preserves the DNS failure: {error}");
                    };
                    codes.push(source.code());
                }
                codes
            })
        });
        assert_eq!(codes, [HttpErrorCode::DROPPED, HttpErrorCode::DROPPED]);
    }

    #[test]
    fn worker_shutdown_cancels_and_drops_pending_resolution() {
        let worker = ManagedWorker::new(8).expect("DNS worker starts");
        let (started_tx, started_rx) = mpsc::channel();
        let dropped = Arc::new(AtomicBool::new(false));
        let resolver_dropped = Arc::clone(&dropped);
        let call = worker
            .handle()
            .call_local(move |context| async move {
                CoroDnsResolver::resolve_host_all(
                    &context,
                    "example.com",
                    Duration::from_secs(30),
                    CancellationToken::never(),
                    Some(Rc::new(ShutdownResolver {
                        started: RefCell::new(Some(started_tx)),
                        dropped: resolver_dropped,
                    })),
                )
                .await
            })
            .expect("pending DNS call is admitted");
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("DNS lookup starts before shutdown");
        worker.shutdown().expect("DNS worker shutdown completes");
        assert!(dropped.load(Ordering::Acquire));
        let result = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime")
            .block_on(call);
        assert!(result.is_err(), "pending DNS lookup must not succeed");
    }
}

mod identity {
    use std::net::IpAddr;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::Mutex;

    use roxygen_core::error::HttpError;
    use roxygen_core::error::HttpErrorCode;

    use crate::connector::CertVerifyResult;
    use crate::connector::CertificateVerifier;
    use crate::connector::ExpectedIdentity;
    use crate::connector::PresentedCertificate;
    use crate::connector::ValidationPolicy;
    use crate::connector::make_verifier;

    struct InnerVerifier {
        result: Result<(), HttpError>,
        insecure: bool,
    }

    impl CertificateVerifier for InnerVerifier {
        fn verify(&self, _certificate: &PresentedCertificate) -> Result<(), HttpError> {
            self.result.clone()
        }

        fn is_insecure(&self) -> bool {
            self.insecure
        }
    }

    fn trusted() -> Rc<dyn CertificateVerifier> {
        Rc::new(InnerVerifier {
            result: Ok(()),
            insecure: false,
        })
    }

    fn certificate() -> PresentedCertificate {
        PresentedCertificate {
            encoded: b"leaf".to_vec().into_boxed_slice(),
            dns_names: vec![Box::from("example.com")],
            ip_addresses: vec!["127.0.0.1".parse().expect("valid test IP")],
        }
    }

    #[test]
    fn matching_hostname_succeeds() {
        let verifier = make_verifier(
            Some(trusted()),
            ExpectedIdentity::dns("example.com"),
            ValidationPolicy::Enforcing,
            None,
        )
        .expect("verifier is constructed");
        verifier.verify(&certificate()).expect("identity matches");
    }

    #[test]
    fn mismatched_hostname_fails() {
        let verifier = make_verifier(
            Some(trusted()),
            ExpectedIdentity::dns("wrong.example.com"),
            ValidationPolicy::Enforcing,
            None,
        )
        .expect("verifier is constructed");
        let error = verifier
            .verify(&certificate())
            .expect_err("identity differs");
        assert_eq!(error.code(), HttpErrorCode::INADEQUATE_SECURITY);
        assert!(String::from_utf8_lossy(error.message()).contains("expected=wrong.example.com"));
    }

    #[test]
    fn logging_policy_soft_fails_mismatched_hostname() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let callback_events = Arc::clone(&events);
        let verifier = make_verifier(
            Some(trusted()),
            ExpectedIdentity::dns("wrong.example.com"),
            ValidationPolicy::Logging,
            Some(Arc::new(move |result, _| {
                callback_events
                    .lock()
                    .expect("verification events lock is available")
                    .push(result)
            })),
        )
        .expect("verifier is constructed");
        verifier
            .verify(&certificate())
            .expect("logging permits mismatch");
        assert_eq!(
            &*events
                .lock()
                .expect("verification events lock is available"),
            &[CertVerifyResult::SoftFail]
        );
    }

    #[test]
    fn mismatched_ip_fails() {
        let verifier = make_verifier(
            Some(trusted()),
            ExpectedIdentity::ip(IpAddr::V4("127.0.0.2".parse().expect("valid test IP"))),
            ValidationPolicy::Enforcing,
            None,
        )
        .expect("verifier is constructed");
        assert!(verifier.verify(&certificate()).is_err());
    }

    #[test]
    fn mismatch_error_includes_dns_and_ip_sans() {
        let verifier = make_verifier(
            Some(trusted()),
            ExpectedIdentity::ip(IpAddr::V4("127.0.0.2".parse().expect("valid test IP"))),
            ValidationPolicy::Enforcing,
            None,
        )
        .expect("verifier is constructed");
        let error = verifier
            .verify(&certificate())
            .expect_err("identity differs");
        let message = String::from_utf8_lossy(error.message());
        assert!(message.contains("cert_sans=[example.com, 127.0.0.1]"));
    }

    #[test]
    fn underlying_verifier_failure_wins() {
        let verifier = make_verifier(
            Some(Rc::new(InnerVerifier {
                result: Err(HttpError::with_message(
                    HttpErrorCode::INADEQUATE_SECURITY,
                    b"untrusted chain",
                )),
                insecure: false,
            })),
            ExpectedIdentity::dns("example.com"),
            ValidationPolicy::Logging,
            None,
        )
        .expect("verifier is constructed");
        assert_eq!(
            verifier
                .verify(&certificate())
                .expect_err("chain validation fails")
                .message(),
            b"untrusted chain"
        );
    }

    #[test]
    fn insecure_verifier_bypasses_identity() {
        let inner: Rc<dyn CertificateVerifier> = Rc::new(InnerVerifier {
            result: Ok(()),
            insecure: true,
        });
        let verifier = make_verifier(
            Some(Rc::clone(&inner)),
            ExpectedIdentity::dns("wrong.example.com"),
            ValidationPolicy::Enforcing,
            None,
        )
        .expect("verifier is constructed");
        assert!(Rc::ptr_eq(&verifier, &inner));
    }

    #[test]
    fn missing_inner_verifier_is_rejected() {
        let result = make_verifier(
            None,
            ExpectedIdentity::dns("example.com"),
            ValidationPolicy::Logging,
            None,
        );
        let Err(error) = result else {
            panic!("missing verifier unexpectedly succeeded");
        };
        assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
    }
}

mod connector {
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::net::SocketAddr;
    use std::net::TcpListener;
    use std::net::TcpStream;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::thread;
    use std::time::Duration;

    use roxygen_core::error::HttpError;
    use roxygen_core::error::HttpErrorCode;
    use roxygen_core::http::CodecProtocol;
    use roxygen_core::url::Url;
    use roxygen_core::url::UrlMode;
    use roxygen_runtime::cancellation::CancellationSource;
    use roxygen_runtime::cancellation::CancellationToken;
    use roxygen_runtime::worker::WorkerContext;
    use roxygen_session::http2::stats::Http2SessionStats;
    use roxygen_session::session::LifecycleEvent;
    use roxygen_session::session::LifecycleObserver;
    use roxygen_transport::tls::TlsCertificateVerifier;
    use roxygen_transport::tls::TlsClientOptions;

    use super::run_on_worker;
    use super::socket_address;
    use super::start_plain_server;
    use super::start_tls_server;
    use crate::HttpClient;
    use crate::RequestSpec;
    use crate::connector::CertVerifyResult;
    use crate::connector::ClientSession;
    use crate::connector::ClientTransportOptions;
    use crate::connector::ConnectionParams;
    use crate::connector::HttpConnector;
    use crate::connector::IdentityValidation;
    use crate::connector::LocalClientFuture;
    use crate::connector::SessionConnector;
    use crate::connector::SessionParams;
    use crate::connector::TlsConnectionStats;

    fn failed_code(result: Result<ClientSession, roxygen_core::error::HttpError>) -> HttpErrorCode {
        match result {
            Ok(_) => panic!("connection unexpectedly succeeded"),
            Err(error) => error.code(),
        }
    }

    /// Holds one accepted connection open so a TLS client stalls in its handshake.
    struct StallListener {
        listener: TcpListener,
        thread: thread::JoinHandle<()>,
    }

    impl StallListener {
        fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("stall listener binds");
            let accepting = listener.try_clone().expect("stall listener clones");
            let thread = thread::spawn(move || {
                let _stream = accepting.accept().expect("stalled connection accepted").0;
                thread::sleep(Duration::from_millis(100));
            });
            Self { listener, thread }
        }

        fn address(&self) -> SocketAddr {
            self.listener.local_addr().expect("stall listener address")
        }

        fn join(self) {
            // A connect cancelled before it reaches TCP never wakes `accept`.
            let _ = TcpStream::connect(self.address());
            self.thread.join().expect("stall server exits");
        }
    }

    #[derive(Debug)]
    struct AcceptAllCertificates {
        calls: Arc<AtomicUsize>,
        insecure: bool,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(super) enum TlsStatEvent {
        Connection(bool),
        Error(bool),
    }

    pub(super) struct RecordingTlsStats(pub(super) Arc<Mutex<Vec<TlsStatEvent>>>);

    #[derive(Default)]
    struct RecordingSessionStats {
        opened: Cell<usize>,
        closed: Cell<usize>,
        reused: Cell<usize>,
        buffered_read: RefCell<Vec<i64>>,
        buffered_write: RefCell<Vec<i64>>,
    }

    struct PanickingDrainLifecycleObserver;

    impl LifecycleObserver for PanickingDrainLifecycleObserver {
        fn on_event(&self, event: LifecycleEvent) {
            if matches!(event, LifecycleEvent::DrainStarted(_)) {
                panic!("injected lifecycle observer panic during session drop");
            }
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum SessionCallbackEvent {
        Opened,
        Attached,
        Closed,
        Detached,
    }

    struct OrderedSessionCallbacks(Rc<RefCell<Vec<SessionCallbackEvent>>>);

    struct CancellationLifecycleProbe {
        dropped: Arc<AtomicUsize>,
        callbacks: Arc<AtomicUsize>,
    }

    impl Drop for CancellationLifecycleProbe {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl roxygen_session::session::LifecycleObserver for CancellationLifecycleProbe {
        fn on_event(&self, _event: roxygen_session::session::LifecycleEvent) {
            self.callbacks.fetch_add(1, Ordering::Relaxed);
            panic!("cancelled connection must not invoke session lifecycle callbacks");
        }
    }

    impl Http2SessionStats for RecordingSessionStats {
        fn record_transaction_opened(&self) {
            self.opened.set(self.opened.get() + 1);
        }

        fn record_transaction_closed(&self) {
            self.closed.set(self.closed.get() + 1);
        }

        fn record_session_reused(&self) {
            self.reused.set(self.reused.get() + 1);
        }

        fn record_buffered_read_bytes(&self, delta: i64) {
            self.buffered_read.borrow_mut().push(delta);
        }

        fn record_buffered_write_bytes(&self, delta: i64) {
            self.buffered_write.borrow_mut().push(delta);
        }
    }

    impl Http2SessionStats for OrderedSessionCallbacks {
        fn record_transaction_opened(&self) {
            self.0.borrow_mut().push(SessionCallbackEvent::Opened);
        }

        fn record_transaction_closed(&self) {
            self.0.borrow_mut().push(SessionCallbackEvent::Closed);
        }
    }

    impl roxygen_session::session::LifecycleObserver for OrderedSessionCallbacks {
        fn on_event(&self, event: roxygen_session::session::LifecycleEvent) {
            let event = match event {
                roxygen_session::session::LifecycleEvent::TransactionAttached => {
                    SessionCallbackEvent::Attached
                }
                roxygen_session::session::LifecycleEvent::TransactionDetached { .. } => {
                    SessionCallbackEvent::Detached
                }
                _ => return,
            };
            self.0.borrow_mut().push(event);
        }
    }

    impl TlsConnectionStats for RecordingTlsStats {
        fn on_connection(&self, full_handshake: bool) {
            self.0
                .lock()
                .expect("TLS stats lock is available")
                .push(TlsStatEvent::Connection(full_handshake));
        }

        fn on_connection_error(&self, verification_error: bool) {
            self.0
                .lock()
                .expect("TLS stats lock is available")
                .push(TlsStatEvent::Error(verification_error));
        }
    }

    impl TlsCertificateVerifier for AcceptAllCertificates {
        fn verify_server_certificate(
            &self,
            _server_name: &str,
            certificate_chain_der: &[&[u8]],
        ) -> Result<(), HttpError> {
            assert!(!certificate_chain_der.is_empty());
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        fn is_insecure(&self) -> bool {
            self.insecure
        }
    }

    struct CountingConnector {
        attempts: Rc<Cell<u32>>,
        connected: Option<ClientSession>,
        delayed_port: Option<u16>,
        delay_first: bool,
        delay: Duration,
    }

    impl SessionConnector for CountingConnector {
        fn connect(
            &self,
            context: WorkerContext,
            address: roxygen_core::message::SocketAddress,
            timeout: Duration,
            connection: ConnectionParams,
            session: SessionParams,
            cancellation: CancellationToken,
        ) -> LocalClientFuture<Result<ClientSession, HttpError>> {
            let attempt = self.attempts.get().saturating_add(1);
            self.attempts.set(attempt);
            if let Some(session) = &self.connected {
                return Box::pin(std::future::ready(Ok(session.clone())));
            }
            if address.port() == 0 {
                return Box::pin(std::future::ready(Err(HttpError::with_message(
                    HttpErrorCode::CONNECT_ERROR,
                    b"injected fast connect failure",
                ))));
            }
            let delay = (self.delayed_port == Some(address.port())
                || (self.delay_first && attempt == 1))
                .then_some(self.delay);
            Box::pin(async move {
                if let Some(delay) = delay {
                    tokio::time::sleep(delay).await;
                }
                HttpConnector::connect(
                    &context,
                    address,
                    timeout,
                    connection,
                    session,
                    cancellation,
                )
                .await
            })
        }
    }

    #[test]
    fn connect_applies_protocol_and_session_options() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let observed = run_on_worker(move |context| {
            Box::pin(async move {
                let session_params = SessionParams {
                    max_concurrent_outgoing_streams: Some(10),
                    connection_flow_control: Some(128_000),
                    stream_read_timeout: Some(Duration::from_secs(5)),
                    connection_read_timeout: Some(Duration::from_secs(3)),
                    write_timeout: Some(Duration::from_secs(1)),
                    ..SessionParams::default()
                };
                let session = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams::default(),
                    session_params,
                    CancellationToken::never(),
                )
                .await
                .expect("plaintext connection succeeds");
                (
                    session.protocol(),
                    session.peer_address().port(),
                    session.worker_id(),
                    context.worker_id(),
                )
            })
        });
        assert_eq!(observed.0, CodecProtocol::Http11);
        assert_eq!(observed.1, address.port());
        assert_eq!(observed.2, observed.3);
    }

    #[test]
    fn last_session_drop_suppresses_lifecycle_observer_panics_during_unwind() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let survived = run_on_worker(move |context| {
            Box::pin(async move {
                let session = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams::default(),
                    SessionParams {
                        lifecycle_observer: Some(Rc::new(PanickingDrainLifecycleObserver)),
                        ..SessionParams::default()
                    },
                    CancellationToken::never(),
                )
                .await
                .expect("plaintext connection succeeds");
                catch_unwind(AssertUnwindSafe(|| {
                    let _last_session = session;
                    panic!("injected outer unwind");
                }))
                .is_err()
            })
        });
        assert!(survived);
    }

    #[test]
    fn zero_client_h2_stream_limit_retains_the_default() {
        let (server, certificate) = start_tls_server(&[b"h2"]);
        let address = server.address().expect("server has an address");
        let admitted = run_on_worker(move |context| {
            Box::pin(async move {
                let mut tls = TlsClientOptions::new("localhost");
                tls.add_root_certificate(certificate);
                tls.set_alpn_protocols([b"h2".as_slice()]);
                let session = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(tls),
                        ..ConnectionParams::default()
                    },
                    SessionParams {
                        max_concurrent_outgoing_streams: Some(0),
                        ..SessionParams::default()
                    },
                    CancellationToken::never(),
                )
                .await
                .expect("HTTP/2 connection succeeds");
                session.reserve_request().is_ok()
            })
        });
        assert!(admitted);
    }

    #[test]
    fn session_stats_apply_to_http1_transactions() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let observed = run_on_worker(move |context| {
            Box::pin(async move {
                let stats = Rc::new(RecordingSessionStats::default());
                let session = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams::default(),
                    SessionParams {
                        session_stats: Some(stats.clone()),
                        ..SessionParams::default()
                    },
                    CancellationToken::never(),
                )
                .await
                .expect("plaintext connection succeeds");
                for index in 0..2 {
                    let url = Url::parse(
                        format!("http://localhost:{}/", address.port()),
                        false,
                        UrlMode::StrictCompat,
                    );
                    let request = if index == 0 {
                        RequestSpec::get(url)
                    } else {
                        RequestSpec::post(url, b"session stats body")
                    };
                    HttpClient::request(&session, None, request)
                        .await
                        .expect("HTTP/1 request succeeds");
                    tokio::time::timeout(Duration::from_secs(1), async {
                        while stats.closed.get() != index + 1 {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .expect("HTTP/1 transaction detaches");
                }
                (
                    stats.opened.get(),
                    stats.closed.get(),
                    stats.reused.get(),
                    stats.buffered_read.borrow().clone(),
                    stats.buffered_write.borrow().clone(),
                )
            })
        });
        assert_eq!((observed.0, observed.1, observed.2), (2, 2, 1));
        assert!(observed.3.iter().any(|delta| *delta > 0));
        assert_eq!(observed.3.iter().sum::<i64>(), 0);
        assert!(observed.4.iter().any(|delta| *delta > 0));
        assert_eq!(observed.4.iter().sum::<i64>(), 0);
    }

    #[test]
    fn session_stats_outlive_the_last_client_handle_until_response_detach() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let observed = run_on_worker(move |context| {
            Box::pin(async move {
                let stats = Rc::new(RecordingSessionStats::default());
                let session = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams::default(),
                    SessionParams {
                        session_stats: Some(stats.clone()),
                        ..SessionParams::default()
                    },
                    CancellationToken::never(),
                )
                .await
                .expect("plaintext connection succeeds");
                let spec = RequestSpec::get(Url::parse(
                    format!("http://localhost:{}/", address.port()),
                    false,
                    UrlMode::StrictCompat,
                ));
                let response = session
                    .send_request(
                        crate::request_source(&spec).expect("request source is valid"),
                        CancellationToken::never(),
                    )
                    .await
                    .expect("request is admitted");
                drop(session);
                HttpClient::read_response(response.source)
                    .await
                    .expect("response completes after the client handle is dropped");
                tokio::task::yield_now().await;
                (stats.opened.get(), stats.closed.get())
            })
        });
        assert_eq!(observed, (1, 1));
    }

    #[test]
    fn session_stats_are_not_double_counted_for_http2() {
        let (server, certificate) = start_tls_server(&[b"h2"]);
        let address = server.address().expect("server has an address");
        let observed = run_on_worker(move |context| {
            Box::pin(async move {
                let stats = Rc::new(RecordingSessionStats::default());
                let mut tls = TlsClientOptions::new("localhost");
                tls.add_root_certificate(certificate);
                tls.set_alpn_protocols([b"h2".as_slice()]);
                let session = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(tls),
                        ..ConnectionParams::default()
                    },
                    SessionParams {
                        session_stats: Some(stats.clone()),
                        ..SessionParams::default()
                    },
                    CancellationToken::never(),
                )
                .await
                .expect("HTTP/2 connection succeeds");
                let url = Url::parse(
                    format!("https://localhost:{}/", address.port()),
                    false,
                    UrlMode::StrictCompat,
                );
                HttpClient::request(&session, None, RequestSpec::get(url))
                    .await
                    .expect("HTTP/2 request succeeds");
                tokio::task::yield_now().await;
                (stats.opened.get(), stats.closed.get(), stats.reused.get())
            })
        });
        assert_eq!(observed, (1, 1, 0));
    }

    #[test]
    fn transaction_stats_bracket_lifecycle_callbacks_for_http1() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let observed = run_on_worker(move |context| {
            Box::pin(async move {
                let events = Rc::new(RefCell::new(Vec::new()));
                let callbacks = Rc::new(OrderedSessionCallbacks(Rc::clone(&events)));
                let session = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams::default(),
                    SessionParams {
                        lifecycle_observer: Some(callbacks.clone()),
                        session_stats: Some(callbacks),
                        ..SessionParams::default()
                    },
                    CancellationToken::never(),
                )
                .await
                .expect("HTTP/1 connection succeeds");
                let url = Url::parse(
                    format!("http://localhost:{}/", address.port()),
                    false,
                    UrlMode::StrictCompat,
                );
                HttpClient::request(&session, None, RequestSpec::get(url))
                    .await
                    .expect("HTTP/1 request succeeds");
                tokio::task::yield_now().await;
                events.borrow().clone()
            })
        });
        assert_eq!(
            observed,
            [
                SessionCallbackEvent::Attached,
                SessionCallbackEvent::Opened,
                SessionCallbackEvent::Closed,
                SessionCallbackEvent::Detached,
            ]
        );
    }

    #[test]
    fn transaction_stats_bracket_lifecycle_callbacks_for_http2() {
        let (server, certificate) = start_tls_server(&[b"h2"]);
        let address = server.address().expect("server has an address");
        let observed = run_on_worker(move |context| {
            Box::pin(async move {
                let events = Rc::new(RefCell::new(Vec::new()));
                let callbacks = Rc::new(OrderedSessionCallbacks(Rc::clone(&events)));
                let mut tls = TlsClientOptions::new("localhost");
                tls.add_root_certificate(certificate);
                tls.set_alpn_protocols([b"h2".as_slice()]);
                let session = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(tls),
                        ..ConnectionParams::default()
                    },
                    SessionParams {
                        lifecycle_observer: Some(callbacks.clone()),
                        session_stats: Some(callbacks),
                        ..SessionParams::default()
                    },
                    CancellationToken::never(),
                )
                .await
                .expect("HTTP/2 connection succeeds");
                let url = Url::parse(
                    format!("https://localhost:{}/", address.port()),
                    false,
                    UrlMode::StrictCompat,
                );
                HttpClient::request(&session, None, RequestSpec::get(url))
                    .await
                    .expect("HTTP/2 request succeeds");
                tokio::task::yield_now().await;
                events.borrow().clone()
            })
        });
        assert_eq!(
            observed,
            [
                SessionCallbackEvent::Attached,
                SessionCallbackEvent::Opened,
                SessionCallbackEvent::Closed,
                SessionCallbackEvent::Detached,
            ]
        );
    }

    #[test]
    fn tls_identity_is_enforced_end_to_end() {
        let (server, certificate) = start_tls_server(&[b"h2", b"http/1.1"]);
        let address = server.address().expect("server has an address");
        let (matched, mismatched) = run_on_worker(move |context| {
            Box::pin(async move {
                let mut matching = TlsClientOptions::new("localhost");
                matching.add_root_certificate(&certificate);
                matching.set_alpn_protocols([b"h2".as_slice(), b"http/1.1".as_slice()]);
                let matched = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(matching),
                        identity_validation: IdentityValidation::Enforcing,
                        ..ConnectionParams::default()
                    },
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .map(|session| session.protocol());

                let mut wrong = TlsClientOptions::new("wrong.example.com");
                wrong.add_root_certificate(certificate);
                wrong.set_alpn_protocols([b"h2".as_slice(), b"http/1.1".as_slice()]);
                let mismatched = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(wrong),
                        identity_validation: IdentityValidation::Enforcing,
                        ..ConnectionParams::default()
                    },
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await;
                (matched, mismatched.map(|_| ()))
            })
        });
        assert_eq!(
            matched.expect("matching identity connects"),
            CodecProtocol::Http2
        );
        assert!(mismatched.is_err());
    }

    #[test]
    fn tls_connection_failure_stats_preserve_frozen_verify_flag() {
        let (server, certificate) = start_tls_server(&[b"http/1.1"]);
        let address = server.address().expect("server has an address");
        let events = Arc::new(Mutex::new(Vec::new()));
        let callback_events = Arc::clone(&events);
        let failed = run_on_worker(move |context| {
            Box::pin(async move {
                let mut tls = TlsClientOptions::new("wrong.example.com");
                tls.add_root_certificate(certificate);
                HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(tls),
                        identity_validation: IdentityValidation::Enforcing,
                        tls_stats: Some(Arc::new(RecordingTlsStats(callback_events))),
                        ..ConnectionParams::default()
                    },
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .is_err()
            })
        });
        assert!(failed);
        assert_eq!(
            *events.lock().expect("TLS stats lock is available"),
            [TlsStatEvent::Error(false)]
        );
    }

    #[test]
    fn default_tls_options_prefer_h2() {
        let (server, certificate) = start_tls_server(&[b"h2", b"http/1.1"]);
        let address = server.address().expect("server has an address");
        let protocol = run_on_worker(move |context| {
            Box::pin(async move {
                let mut tls = TlsClientOptions::new("localhost");
                tls.add_root_certificate(certificate);
                HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(tls),
                        identity_validation: IdentityValidation::Enforcing,
                        ..ConnectionParams::default()
                    },
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .expect("default TLS connection succeeds")
                .protocol()
            })
        });
        assert_eq!(protocol, CodecProtocol::Http2);
    }

    #[test]
    fn tls_logging_policy_soft_fails_end_to_end() {
        let (server, certificate) = start_tls_server(&[b"http/1.1"]);
        let address = server.address().expect("server has an address");
        let events = Arc::new(Mutex::new(Vec::new()));
        let callback_events = Arc::clone(&events);
        let connected = run_on_worker(move |context| {
            Box::pin(async move {
                let mut tls = TlsClientOptions::new("wrong.example.com");
                tls.add_root_certificate(certificate);
                HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(tls),
                        identity_validation: IdentityValidation::Logging,
                        cert_verify_log: Some(Arc::new(move |result, _| {
                            callback_events
                                .lock()
                                .expect("verification events lock is available")
                                .push(result);
                        })),
                        ..ConnectionParams::default()
                    },
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .is_ok()
            })
        });
        assert!(connected);
        assert_eq!(
            &*events
                .lock()
                .expect("verification events lock is available"),
            &[CertVerifyResult::SoftFail]
        );
    }

    #[test]
    fn custom_certificate_verifier_still_enforces_identity() {
        let (server, _) = start_tls_server(&[b"http/1.1"]);
        let address = server.address().expect("server has an address");
        let calls = Arc::new(AtomicUsize::new(0));
        let verifier = Arc::new(AcceptAllCertificates {
            calls: Arc::clone(&calls),
            insecure: false,
        });
        let connected = run_on_worker(move |context| {
            Box::pin(async move {
                let mut tls = TlsClientOptions::new("wrong.example.com");
                tls.set_ca_paths(["/definitely/not/a/real/ca.pem"]);
                tls.set_certificate_verifier(Some(verifier));
                HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(tls),
                        identity_validation: IdentityValidation::Enforcing,
                        ..ConnectionParams::default()
                    },
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .is_ok()
            })
        });
        assert!(!connected);
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        let (server, _) = start_tls_server(&[b"http/1.1"]);
        let address = server.address().expect("server has an address");
        let verifier = Arc::new(AcceptAllCertificates {
            calls: Arc::clone(&calls),
            insecure: true,
        });
        let connected = run_on_worker(move |context| {
            Box::pin(async move {
                let mut tls = TlsClientOptions::new("wrong.example.com");
                tls.set_ca_paths(["/definitely/not/a/real/ca.pem"]);
                tls.set_certificate_verifier(Some(verifier));
                HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(tls),
                        identity_validation: IdentityValidation::Enforcing,
                        ..ConnectionParams::default()
                    },
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .is_ok()
            })
        });
        assert!(connected);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn default_certificate_verifier_does_not_override_explicit_tls_roots() {
        let (server, _) = start_tls_server(&[b"http/1.1"]);
        let address = server.address().expect("server has an address");
        let (_untrusted_server, untrusted_certificate) = start_tls_server(&[b"http/1.1"]);
        let calls = Arc::new(AtomicUsize::new(0));
        let verifier: Arc<dyn TlsCertificateVerifier> = Arc::new(AcceptAllCertificates {
            calls: Arc::clone(&calls),
            insecure: true,
        });
        let outcomes =
            run_on_worker(move |context| {
                Box::pin(async move {
                    let mut explicit_tls = TlsClientOptions::new("localhost");
                    explicit_tls.add_root_certificate(untrusted_certificate);
                    let explicit = ConnectionParams {
                        transport: ClientTransportOptions::Tls(explicit_tls),
                        ..ConnectionParams::default()
                    }
                    .for_destination_with_default_verifier(
                        "localhost",
                        true,
                        Some(Arc::clone(&verifier)),
                    );
                    let explicit_failed = HttpConnector::connect(
                        &context,
                        socket_address(address),
                        Duration::from_secs(1),
                        explicit,
                        SessionParams::default(),
                        CancellationToken::never(),
                    )
                    .await
                    .is_err();

                    let implicit = ConnectionParams::default()
                        .for_destination_with_default_verifier("localhost", true, Some(verifier));
                    let implicit_succeeded = HttpConnector::connect(
                        &context,
                        socket_address(address),
                        Duration::from_secs(1),
                        implicit,
                        SessionParams::default(),
                        CancellationToken::never(),
                    )
                    .await
                    .is_ok();
                    (explicit_failed, implicit_succeeded)
                })
            });
        assert_eq!(outcomes, (true, true));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn tls_options_share_session_resumption_state() {
        let (server, certificate) = start_tls_server(&[b"http/1.1"]);
        let address = server.address().expect("server has an address");
        let events = Arc::new(Mutex::new(Vec::new()));
        let callback_events = Arc::clone(&events);
        let resumed = run_on_worker(move |context| {
            Box::pin(async move {
                let mut tls = TlsClientOptions::new("localhost");
                tls.add_root_certificate(certificate);
                let params = ConnectionParams {
                    transport: ClientTransportOptions::Tls(tls),
                    identity_validation: IdentityValidation::Enforcing,
                    tls_stats: Some(Arc::new(RecordingTlsStats(callback_events))),
                    ..ConnectionParams::default()
                };
                let first = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    params.clone(),
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .expect("first TLS connection succeeds");
                let url = Url::parse(
                    format!("https://localhost:{}/", address.port()),
                    false,
                    UrlMode::StrictCompat,
                );
                HttpClient::request(&first, None, RequestSpec::get(url))
                    .await
                    .expect("first TLS request processes session tickets");
                tokio::time::sleep(Duration::from_millis(50)).await;
                let second = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    params,
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .expect("second TLS connection succeeds");
                second.tls_session_reused()
            })
        });
        assert!(resumed);
        assert_eq!(
            &*events.lock().expect("TLS stats lock is available"),
            &[
                TlsStatEvent::Connection(true),
                TlsStatEvent::Connection(false)
            ]
        );
    }

    #[cfg(unix)]
    #[test]
    fn tcp_connect_applies_bind_and_phase_aware_socket_options() {
        use roxygen_core::message::SocketAddress;
        use roxygen_transport::tcp::TcpConnectOptions;
        use roxygen_transport::tcp::TcpSocketOption;
        use roxygen_transport::tcp::TcpSocketOptionPhase;

        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let local = run_on_worker(move |context| {
            Box::pin(async move {
                HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams {
                        tcp_options: TcpConnectOptions {
                            bind_address: Some(SocketAddress::new("127.0.0.1", 0)),
                            socket_options: vec![TcpSocketOption::no_delay_at(
                                true,
                                TcpSocketOptionPhase::PreConnect,
                            )],
                            congestion_flavor: Some("not-a-real-congestion-flavor".into()),
                        },
                        ..ConnectionParams::default()
                    },
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .expect("bound TCP connection succeeds")
                .local_address()
                .clone()
            })
        });
        assert_eq!(local.host(), b"127.0.0.1");
        assert_ne!(local.port(), 0);
    }

    #[test]
    fn unknown_tls_alpn_falls_back_to_http1() {
        let (server, certificate) = start_tls_server(&[b"proxygen-test"]);
        let address = server.address().expect("server has an address");
        let protocol = run_on_worker(move |context| {
            Box::pin(async move {
                let mut tls = TlsClientOptions::new("localhost");
                tls.add_root_certificate(certificate);
                tls.set_alpn_protocols([b"proxygen-test".as_slice()]);
                HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(tls),
                        identity_validation: IdentityValidation::Enforcing,
                        ..ConnectionParams::default()
                    },
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .expect("custom ALPN connection succeeds")
                .protocol()
            })
        });
        assert_eq!(protocol, CodecProtocol::Http11);
    }

    #[test]
    fn connect_cancellation_and_timeout_are_distinct() {
        let stall = StallListener::start();
        let address = stall.address();
        let cancelled = run_on_worker(move |context| {
            Box::pin(async move {
                let cancellation = CancellationSource::new();
                let connect = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(TlsClientOptions::new("localhost")),
                        ..ConnectionParams::default()
                    },
                    SessionParams::default(),
                    cancellation.token(),
                );
                tokio::pin!(connect);
                tokio::select! {
                    _ = &mut connect => panic!("TLS connect completed before cancellation"),
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
                cancellation.request_cancellation();
                failed_code(connect.await)
            })
        });
        stall.join();
        assert_eq!(cancelled, HttpErrorCode::CORO_CANCELLED);

        let stall = StallListener::start();
        let address = stall.address();
        let timed_out = run_on_worker(move |context| {
            Box::pin(async move {
                let options = TlsClientOptions::new("localhost");
                failed_code(
                    HttpConnector::connect(
                        &context,
                        socket_address(address),
                        Duration::from_millis(10),
                        ConnectionParams {
                            transport: ClientTransportOptions::Tls(options),
                            ..ConnectionParams::default()
                        },
                        SessionParams::default(),
                        CancellationToken::never(),
                    )
                    .await,
                )
            })
        });
        stall.join();
        assert_eq!(timed_out, HttpErrorCode::READ_TIMEOUT);
    }

    #[test]
    fn cancelled_connect_releases_session_state() {
        let stall = StallListener::start();
        let stalled_address = stall.address();
        let (server, _) = start_plain_server();
        let healthy_address = server.address().expect("server has an address");
        let (code, recovered, dropped, callbacks) = run_on_worker(move |context| {
            Box::pin(async move {
                let cancellation = CancellationSource::new();
                let dropped = Arc::new(AtomicUsize::new(0));
                let callbacks = Arc::new(AtomicUsize::new(0));
                let observer = Rc::new(CancellationLifecycleProbe {
                    dropped: Arc::clone(&dropped),
                    callbacks: Arc::clone(&callbacks),
                });
                let connect = HttpConnector::connect(
                    &context,
                    socket_address(stalled_address),
                    Duration::from_secs(1),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(TlsClientOptions::new("localhost")),
                        ..ConnectionParams::default()
                    },
                    SessionParams {
                        lifecycle_observer: Some(observer.clone()),
                        ..SessionParams::default()
                    },
                    cancellation.token(),
                );
                drop(observer);
                tokio::pin!(connect);
                tokio::select! {
                    _ = &mut connect => panic!("TLS connect completed before cancellation"),
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
                cancellation.request_cancellation();
                let code = failed_code(connect.await);
                let recovered = HttpConnector::connect(
                    &context,
                    socket_address(healthy_address),
                    Duration::from_secs(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .is_ok();
                (
                    code,
                    recovered,
                    dropped.load(Ordering::Relaxed),
                    callbacks.load(Ordering::Relaxed),
                )
            })
        });
        stall.join();
        assert_eq!(code, HttpErrorCode::CORO_CANCELLED);
        assert!(recovered);
        assert_eq!(dropped, 1);
        assert_eq!(callbacks, 0);
    }

    #[test]
    fn session_operations_remain_on_the_owner_worker() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let same_worker = run_on_worker(move |context| {
            Box::pin(async move {
                let session = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .expect("connection succeeds");
                session.worker_id() == context.worker_id() && session.reserve_request().is_ok()
            })
        });
        assert!(same_worker);
    }

    #[test]
    fn tls_connections_reuse_shared_options() {
        let (server, certificate) = start_tls_server(&[b"http/1.1"]);
        let address = server.address().expect("server has an address");
        let connected = run_on_worker(move |context| {
            Box::pin(async move {
                let mut options = TlsClientOptions::new("localhost");
                options.add_root_certificate(certificate);
                options.set_alpn_protocols([b"http/1.1".as_slice()]);
                let params = ConnectionParams {
                    transport: ClientTransportOptions::Tls(options),
                    ..ConnectionParams::default()
                };
                let first = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    params.clone(),
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await;
                let second = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    params,
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await;
                (first.is_ok(), second.is_ok())
            })
        });
        assert_eq!(connected, (true, true));
    }

    #[test]
    fn connect_error_is_preserved() {
        let code = run_on_worker(|context| {
            Box::pin(async move {
                failed_code(
                    HttpConnector::connect(
                        &context,
                        roxygen_core::message::SocketAddress::new("127.0.0.1", 0),
                        Duration::from_secs(5),
                        ConnectionParams::default(),
                        SessionParams::default(),
                        CancellationToken::never(),
                    )
                    .await,
                )
            })
        });
        assert_eq!(code, HttpErrorCode::CONNECT_ERROR);
    }

    #[test]
    fn tls_handshake_error_is_preserved() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let failed = run_on_worker(move |context| {
            Box::pin(async move {
                HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_millis(100),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(TlsClientOptions::new("localhost")),
                        ..ConnectionParams::default()
                    },
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .is_err()
            })
        });
        assert!(failed);
    }

    #[test]
    fn happy_eyeballs_races_primary_and_fallback() {
        let (server, _) = start_plain_server();
        let good = server.address().expect("server has an address");
        let observed = run_on_worker(move |context| {
            Box::pin(async move {
                let attempts = Rc::new(Cell::new(0));
                let connector = Rc::new(CountingConnector {
                    attempts: Rc::clone(&attempts),
                    connected: None,
                    delayed_port: Some(1),
                    delay_first: false,
                    delay: Duration::from_millis(100),
                });
                let session = HttpConnector::happy_eyeballs_connect_with(
                    connector,
                    &context,
                    roxygen_core::message::SocketAddress::new("127.0.0.1", 1),
                    socket_address(good),
                    Duration::from_secs(1),
                    Duration::from_millis(5),
                    ConnectionParams::default(),
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .expect("fallback connects")
                .peer_address()
                .port();
                (attempts.get(), session)
            })
        });
        assert_eq!(observed, (2, good.port()));
    }

    #[test]
    fn happy_eyeballs_allows_both_attempts_to_succeed() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let attempts = run_on_worker(move |context| {
            Box::pin(async move {
                let attempts = Rc::new(Cell::new(0));
                let connector = Rc::new(CountingConnector {
                    attempts: Rc::clone(&attempts),
                    connected: None,
                    delayed_port: None,
                    delay_first: true,
                    delay: Duration::from_millis(10),
                });
                HttpConnector::happy_eyeballs_connect_with(
                    connector,
                    &context,
                    socket_address(address),
                    socket_address(address),
                    Duration::from_secs(1),
                    Duration::ZERO,
                    ConnectionParams::default(),
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .expect("one successful connection wins");
                attempts.get()
            })
        });
        assert_eq!(attempts, 2);
    }

    #[test]
    fn happy_eyeballs_returns_fallback_error_after_both_fail() {
        let code = run_on_worker(|context| {
            Box::pin(async move {
                failed_code(
                    HttpConnector::happy_eyeballs_connect(
                        &context,
                        roxygen_core::message::SocketAddress::new("127.0.0.1", 0),
                        roxygen_core::message::SocketAddress::new("127.0.0.1", 0),
                        Duration::from_secs(5),
                        Duration::ZERO,
                        ConnectionParams::default(),
                        SessionParams::default(),
                        CancellationToken::never(),
                    )
                    .await,
                )
            })
        });
        assert_eq!(code, HttpErrorCode::CONNECT_ERROR);
    }

    #[test]
    fn happy_eyeballs_cancels_the_losing_attempt() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let attempts = run_on_worker(move |context| {
            Box::pin(async move {
                let primary = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(5),
                    ConnectionParams::default(),
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .expect("primary connection is ready before the race");
                tokio::time::pause();
                let attempts = Rc::new(Cell::new(0));
                let connector = Rc::new(CountingConnector {
                    attempts: Rc::clone(&attempts),
                    connected: Some(primary),
                    delayed_port: None,
                    delay_first: false,
                    delay: Duration::ZERO,
                });
                let session = HttpConnector::happy_eyeballs_connect_with(
                    connector,
                    &context,
                    socket_address(address),
                    socket_address(address),
                    Duration::from_secs(1),
                    Duration::from_millis(150),
                    ConnectionParams::default(),
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .expect("primary connection wins");
                tokio::time::advance(Duration::from_millis(300)).await;
                tokio::time::resume();
                (attempts.get(), session.peer_address().port())
            })
        });
        assert_eq!(attempts, (1, address.port()));
    }

    #[test]
    fn happy_eyeballs_starts_fallback_after_fast_failure() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let attempts = run_on_worker(move |context| {
            Box::pin(async move {
                let attempts = Rc::new(Cell::new(0));
                let connector = Rc::new(CountingConnector {
                    attempts: Rc::clone(&attempts),
                    connected: None,
                    delayed_port: None,
                    delay_first: false,
                    delay: Duration::ZERO,
                });
                let session = HttpConnector::happy_eyeballs_connect_with(
                    connector,
                    &context,
                    roxygen_core::message::SocketAddress::new("127.0.0.1", 0),
                    socket_address(address),
                    Duration::from_secs(1),
                    Duration::from_secs(10),
                    ConnectionParams::default(),
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .expect("fallback starts immediately after primary failure");
                (attempts.get(), session.peer_address().port())
            })
        });
        assert_eq!(attempts, (2, address.port()));
    }

    #[test]
    fn address_override_bypasses_dns() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let peer = run_on_worker(move |context| {
            Box::pin(async move {
                HttpClient::get_session(
                    &context,
                    "not-a-real-host.invalid",
                    address.port(),
                    false,
                    Duration::from_secs(1),
                    Duration::from_secs(1),
                    Some(address.ip().to_string().into_boxed_str()),
                    None,
                    None,
                    CancellationToken::never(),
                )
                .await
                .expect("address override connects")
                .peer_address()
                .port()
            })
        });
        assert_eq!(peer, address.port());
    }
}

mod request {
    use std::cell::RefCell;
    use std::future::Future;
    use std::future::poll_fn;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;
    use std::time::Duration;

    use roxygen_core::buf_queue::BufQueue;
    use roxygen_core::error::HttpError;
    use roxygen_core::error::HttpErrorCode;
    use roxygen_core::events::HttpBodyEvent;
    use roxygen_core::events::HttpHeaderEvent;
    use roxygen_core::fixed_source::HttpFixedSource;
    use roxygen_core::handler::HandlerSessionContext;
    use roxygen_core::handler::HttpHandler;
    use roxygen_core::handler::HttpHandlerRef;
    use roxygen_core::handler::LocalHandlerFuture;
    use roxygen_core::headers::HttpHeaders;
    use roxygen_core::http::HttpMethod;
    use roxygen_core::message::HttpMessage;
    use roxygen_core::source::HttpSource;
    use roxygen_core::source::LocalSourceFuture;
    use roxygen_core::source_filter::HttpSourceFilter;
    use roxygen_core::source_filter::HttpSourceFilterHooks;
    use roxygen_core::source_holder::HttpSourceHolder;
    use roxygen_core::source_reader::ErrorContext;
    use roxygen_core::source_reader::HttpSourceReader;
    use roxygen_core::url::Url;
    use roxygen_core::url::UrlMode;
    use roxygen_runtime::cancellation::CancellationSource;
    use roxygen_runtime::cancellation::CancellationToken;
    use roxygen_server::scoped_server::ScopedHttpServer;
    use roxygen_server::server::ServerConfig;
    use tokio::sync::oneshot;

    use super::FailingResponse;
    use super::run_on_worker;
    use super::socket_address;
    use super::start_plain_server;
    use super::start_tls_server;
    use crate::ClientLoggerSelection;
    use crate::ClientRequestLog;
    use crate::ClientRequestLogger;
    use crate::HttpClient;
    use crate::RequestOptions;
    use crate::RequestSpec;
    use crate::Response;
    use crate::ResponseVisitor;
    use crate::SourceRequest;
    use crate::VisitAction;
    use crate::connector::ClientTransportOptions;
    use crate::connector::ConnectionParams;
    use crate::connector::HttpConnector;
    use crate::connector::LocalClientFuture;
    use crate::connector::SessionParams;

    struct RecordingClientLogger(Arc<Mutex<Vec<&'static str>>>);

    struct RecordingClientRequestLog(Arc<Mutex<Vec<&'static str>>>);

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum CorrelatedLogEvent {
        Created(usize),
        Completed(usize),
        Abandoned(usize),
    }

    struct CorrelatedClientLogger {
        next_id: AtomicUsize,
        events: Arc<Mutex<Vec<CorrelatedLogEvent>>>,
        first_waiting: Arc<tokio::sync::Notify>,
        first_release: Arc<tokio::sync::Notify>,
    }

    struct CorrelatedClientRequestLog {
        id: usize,
        events: Arc<Mutex<Vec<CorrelatedLogEvent>>>,
        first_waiting: Arc<tokio::sync::Notify>,
        first_release: Arc<tokio::sync::Notify>,
    }

    struct TimeoutShieldLogger;

    struct TimeoutShieldRequestLog;

    struct TimeoutShieldSource(HttpSourceHolder);

    impl HttpSource for TimeoutShieldSource {
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            self.0.read_header_event()
        }

        fn read_body_event(
            &mut self,
            maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            self.0.read_body_event(maximum)
        }

        fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
            self.0.stop_reading(error);
        }
    }

    impl ClientRequestLogger for TimeoutShieldLogger {
        fn create(&self, _session: &crate::connector::ClientSession) -> Box<dyn ClientRequestLog> {
            Box::new(TimeoutShieldRequestLog)
        }
    }

    impl ClientRequestLog for TimeoutShieldRequestLog {
        fn response_filter(&self, source: HttpSourceHolder) -> Result<HttpSourceHolder, HttpError> {
            Ok(HttpSourceHolder::new(Box::new(TimeoutShieldSource(source))))
        }
    }

    impl ClientRequestLogger for CorrelatedClientLogger {
        fn create(&self, _session: &crate::connector::ClientSession) -> Box<dyn ClientRequestLog> {
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            self.events
                .lock()
                .expect("logger events lock is available")
                .push(CorrelatedLogEvent::Created(id));
            Box::new(CorrelatedClientRequestLog {
                id,
                events: Arc::clone(&self.events),
                first_waiting: Arc::clone(&self.first_waiting),
                first_release: Arc::clone(&self.first_release),
            })
        }
    }

    impl ClientRequestLog for CorrelatedClientRequestLog {
        fn log_when_done(&self) -> LocalClientFuture<()> {
            let id = self.id;
            let events = Arc::clone(&self.events);
            let first_waiting = Arc::clone(&self.first_waiting);
            let first_release = Arc::clone(&self.first_release);
            Box::pin(async move {
                if id == 0 {
                    first_waiting.notify_one();
                    first_release.notified().await;
                }
                events
                    .lock()
                    .expect("logger events lock is available")
                    .push(CorrelatedLogEvent::Completed(id));
            })
        }

        fn log_on_drop(&self) {
            self.events
                .lock()
                .expect("logger events lock is available")
                .push(CorrelatedLogEvent::Abandoned(self.id));
        }
    }

    impl ClientRequestLogger for RecordingClientLogger {
        fn create(&self, _session: &crate::connector::ClientSession) -> Box<dyn ClientRequestLog> {
            Box::new(RecordingClientRequestLog(Arc::clone(&self.0)))
        }
    }

    impl ClientRequestLog for RecordingClientRequestLog {
        fn request_filter(&self, source: HttpSourceHolder) -> Result<HttpSourceHolder, HttpError> {
            self.0
                .lock()
                .expect("logger actions lock is available")
                .push("request");
            Ok(source)
        }

        fn response_filter(&self, source: HttpSourceHolder) -> Result<HttpSourceHolder, HttpError> {
            self.0
                .lock()
                .expect("logger actions lock is available")
                .push("response");
            Ok(source)
        }

        fn log_when_done(&self) -> LocalClientFuture<()> {
            let actions = Arc::clone(&self.0);
            Box::pin(async move {
                tokio::task::yield_now().await;
                actions
                    .lock()
                    .expect("logger actions lock is available")
                    .push("done");
            })
        }
    }

    fn test_url(port: u16, path: &str) -> String {
        format!("http://example.test:{port}{path}")
    }

    struct FailingRequestBody {
        header: Option<HttpHeaderEvent>,
    }

    struct FailingHeaders;

    impl HttpSource for FailingHeaders {
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            Box::pin(std::future::ready(Err(HttpError::with_message(
                HttpErrorCode::INTERNAL_ERROR,
                b"header source failed",
            ))))
        }

        fn read_body_event(
            &mut self,
            _maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            Box::pin(std::future::ready(Err(HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"body read after header failure",
            ))))
        }

        fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
    }

    impl FailingRequestBody {
        fn new(url: &Url) -> Self {
            let mut message = HttpMessage::new();
            message
                .set_url(url.make_relative_url(), false)
                .expect("test URL is valid");
            message
                .set_method(HttpMethod::Post)
                .expect("test method is valid");
            message.set_http_version(1, 1);
            message
                .headers_mut()
                .set(b"host", url.host_and_port_omit_default());
            Self {
                header: Some(HttpHeaderEvent::new(message, false).expect("header event is valid")),
            }
        }
    }

    impl HttpSource for FailingRequestBody {
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            let result = self.header.take().ok_or_else(|| {
                HttpError::with_message(
                    HttpErrorCode::INVALID_STATE_TRANSITION,
                    b"request header was already consumed",
                )
            });
            Box::pin(std::future::ready(result))
        }

        fn read_body_event(
            &mut self,
            _maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            Box::pin(std::future::ready(Err(HttpError::with_message(
                HttpErrorCode::TRANSPORT_WRITE_ERROR,
                b"request body source failed",
            ))))
        }

        fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
    }

    fn request_options(address: std::net::SocketAddr) -> RequestOptions {
        RequestOptions {
            server_address: Some(address.ip().to_string().into_boxed_str()),
            ..RequestOptions::default()
        }
    }

    #[test]
    fn convenience_timeout_drives_default_session_timeouts() {
        let options = RequestOptions {
            timeout: Some(Duration::from_secs(10)),
            ..RequestOptions::default()
        };
        let derived = crate::session_params_for_request(&options);
        assert_eq!(derived.stream_read_timeout, Some(Duration::from_secs(10)));
        assert_eq!(
            derived.connection_read_timeout,
            Some(Duration::from_secs(10))
        );

        let options = RequestOptions {
            timeout: Some(Duration::from_secs(10)),
            session: Some(SessionParams {
                stream_read_timeout: Some(Duration::from_secs(3)),
                connection_read_timeout: Some(Duration::from_secs(4)),
                ..SessionParams::default()
            }),
            ..RequestOptions::default()
        };
        let explicit = crate::session_params_for_request(&options);
        assert_eq!(explicit.stream_read_timeout, Some(Duration::from_secs(3)));
        assert_eq!(
            explicit.connection_read_timeout,
            Some(Duration::from_secs(4))
        );
    }

    #[test]
    fn default_session_params_preserve_all_session_timeouts() {
        let config = crate::connector::uniplex_config_for_session_params(&SessionParams::default());
        assert_eq!(config.read_timeout, Duration::from_secs(5));
        assert_eq!(config.stream_read_timeout, Duration::from_secs(5));
        assert_eq!(config.write_timeout, Duration::from_secs(5));

        let config = crate::connector::uniplex_config_for_session_params(&SessionParams {
            connection_read_timeout: Some(Duration::from_secs(1)),
            stream_read_timeout: Some(Duration::from_secs(2)),
            write_timeout: Some(Duration::from_secs(3)),
            ..SessionParams::default()
        });
        assert_eq!(config.read_timeout, Duration::from_secs(1));
        assert_eq!(config.stream_read_timeout, Duration::from_secs(2));
        assert_eq!(config.write_timeout, Duration::from_secs(3));
    }

    #[test]
    fn caller_content_length_is_added_after_the_fixed_body_length() {
        let values = run_on_worker(|_context| {
            Box::pin(async move {
                let mut spec = RequestSpec::post(
                    Url::parse("http://example.com/", false, UrlMode::StrictCompat),
                    b"body",
                );
                spec.headers
                    .insert(Box::from(&b"content-length"[..]), Box::from(&b"99"[..]));
                let mut source = crate::request_source(&spec).expect("request source is valid");
                let headers = source
                    .read_header_event()
                    .await
                    .expect("request headers are readable")
                    .into_headers();
                headers
                    .headers()
                    .iter()
                    .filter(|(code, _, _)| {
                        *code == roxygen_core::headers::HttpHeaderCode::ContentLength
                    })
                    .map(|(_, _, value)| value.to_vec())
                    .collect::<Vec<_>>()
            })
        });
        assert_eq!(values, [b"4".to_vec(), b"99".to_vec()]);
    }

    fn get(path: &str) -> Response {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let url = test_url(address.port(), path);
        run_on_worker(move |context| {
            Box::pin(async move {
                HttpClient::get(&context, url, request_options(address))
                    .await
                    .expect("request succeeds")
            })
        })
    }

    fn status(mut response: Response) -> u16 {
        response
            .headers
            .status_code()
            .expect("response has a status")
    }

    #[test]
    fn get_collects_headers_and_empty_body() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let url = test_url(address.port(), "/");
        let response = run_on_worker(move |context| {
            Box::pin(async move {
                let mut options = request_options(address);
                options
                    .headers
                    .insert(Box::from(&b"custom"[..]), Box::from(&b"header"[..]));
                HttpClient::get(&context, url, options)
                    .await
                    .expect("request succeeds")
            })
        });
        assert_eq!(
            response.headers.headers().get_single_or_empty(b"x-method"),
            b"GET"
        );
        assert_eq!(
            response
                .headers
                .headers()
                .get_single_or_empty(b"x-custom-header-count"),
            b"1"
        );
        assert!(response.body.is_empty());
    }

    #[test]
    fn explicit_logger_filters_and_completion_are_awaited() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let url = test_url(address.port(), "/");
        let actions = Arc::new(Mutex::new(Vec::new()));
        let logger_actions = Arc::clone(&actions);
        run_on_worker(move |context| {
            Box::pin(async move {
                let mut options = request_options(address);
                options.logger = ClientLoggerSelection::Explicit(Arc::new(RecordingClientLogger(
                    logger_actions,
                )));
                HttpClient::get(&context, url, options)
                    .await
                    .expect("logged request succeeds");
            })
        });
        assert_eq!(
            *actions.lock().expect("logger actions lock is available"),
            ["request", "response", "done"]
        );
    }

    #[test]
    fn concurrent_request_logs_are_correlated_and_abandonment_is_logged() {
        let (server, certificate) = start_tls_server(&[b"h2"]);
        let address = server.address().expect("server has an address");
        let events = Arc::new(Mutex::new(Vec::new()));
        let recorded_events = Arc::clone(&events);
        let first_waiting = Arc::new(tokio::sync::Notify::new());
        let first_release = Arc::new(tokio::sync::Notify::new());
        run_on_worker(move |context| {
            Box::pin(async move {
                let logger: Arc<dyn ClientRequestLogger> = Arc::new(CorrelatedClientLogger {
                    next_id: AtomicUsize::new(0),
                    events: recorded_events,
                    first_waiting: Arc::clone(&first_waiting),
                    first_release: Arc::clone(&first_release),
                });
                let mut tls = roxygen_transport::tls::TlsClientOptions::new("localhost");
                tls.add_root_certificate(certificate);
                tls.set_alpn_protocols([b"h2".as_slice()]);
                let session = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(tls),
                        ..ConnectionParams::default()
                    },
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .expect("HTTP/2 connection succeeds");

                let mut first = RequestSpec::get(Url::parse(
                    format!("https://localhost:{}/", address.port()),
                    false,
                    UrlMode::StrictCompat,
                ));
                first.logger = ClientLoggerSelection::Explicit(Arc::clone(&logger));
                let mut first = Box::pin(HttpClient::request(&session, None, first));
                tokio::select! {
                    result = first.as_mut() => {
                        panic!("first request completed before logger release: {result:?}")
                    }
                    () = first_waiting.notified() => {}
                }

                let mut second = RequestSpec::get(Url::parse(
                    format!("https://localhost:{}/", address.port()),
                    false,
                    UrlMode::StrictCompat,
                ));
                second.logger = ClientLoggerSelection::Explicit(Arc::clone(&logger));
                HttpClient::request(&session, None, second)
                    .await
                    .expect("second request completes first");
                first_release.notify_one();
                first
                    .await
                    .expect("first request completes after logger release");

                let cancellation = CancellationSource::new();
                let mut cancelled = RequestSpec::get(Url::parse(
                    format!("https://localhost:{}/pendingHeaders", address.port()),
                    false,
                    UrlMode::StrictCompat,
                ));
                cancelled.cancellation = cancellation.token();
                cancelled.logger = ClientLoggerSelection::Explicit(Arc::clone(&logger));
                let mut cancelled = Box::pin(HttpClient::request(&session, None, cancelled));
                poll_fn(|task| match cancelled.as_mut().poll(task) {
                    Poll::Pending => Poll::Ready(()),
                    Poll::Ready(_) => panic!("request completed before cancellation"),
                })
                .await;
                cancellation.request_cancellation();
                assert_eq!(
                    cancelled
                        .await
                        .expect_err("request observes cancellation")
                        .code(),
                    HttpErrorCode::CORO_CANCELLED
                );

                let mut abandoned = RequestSpec::get(Url::parse(
                    format!("https://localhost:{}/pendingHeaders", address.port()),
                    false,
                    UrlMode::StrictCompat,
                ));
                abandoned.logger = ClientLoggerSelection::Explicit(logger);
                let mut abandoned = Box::pin(HttpClient::request(&session, None, abandoned));
                poll_fn(|task| match abandoned.as_mut().poll(task) {
                    Poll::Pending => Poll::Ready(()),
                    Poll::Ready(_) => panic!("abandoned request completed unexpectedly"),
                })
                .await;
                drop(abandoned);
            })
        });
        assert_eq!(
            *events.lock().expect("logger events lock is available"),
            [
                CorrelatedLogEvent::Created(0),
                CorrelatedLogEvent::Created(1),
                CorrelatedLogEvent::Completed(1),
                CorrelatedLogEvent::Completed(0),
                CorrelatedLogEvent::Created(2),
                CorrelatedLogEvent::Completed(2),
                CorrelatedLogEvent::Created(3),
                CorrelatedLogEvent::Abandoned(3),
            ]
        );
    }

    #[test]
    fn response_timeout_is_applied_before_the_logger_filter() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let code = run_on_worker(move |context| {
            Box::pin(async move {
                let session = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .expect("connection succeeds");
                let mut request = RequestSpec::get(Url::parse(
                    format!("http://localhost:{}/pendingHeaders", address.port()),
                    false,
                    UrlMode::StrictCompat,
                ));
                request.timeout = Duration::from_millis(20);
                request.logger = ClientLoggerSelection::Explicit(Arc::new(TimeoutShieldLogger));
                tokio::time::timeout(
                    Duration::from_secs(1),
                    HttpClient::request(&session, None, request),
                )
                .await
                .expect("inner response timeout remains active")
                .expect_err("pending response times out")
                .code()
            })
        });
        assert_eq!(code, HttpErrorCode::READ_TIMEOUT);
    }

    #[test]
    fn existing_session_default_preserves_the_session_read_timeout() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let code = run_on_worker(move |context| {
            Box::pin(async move {
                let session = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams::default(),
                    SessionParams {
                        stream_read_timeout: Some(Duration::from_millis(20)),
                        connection_read_timeout: Some(Duration::from_millis(20)),
                        ..SessionParams::default()
                    },
                    CancellationToken::never(),
                )
                .await
                .expect("connection succeeds");
                let url = Url::parse(
                    format!("http://localhost:{}/pendingHeaders", address.port()),
                    false,
                    UrlMode::StrictCompat,
                );
                tokio::time::timeout(
                    Duration::from_secs(1),
                    HttpClient::get_with_session(&session, url, RequestOptions::default()),
                )
                .await
                .expect("session-level timeout remains in force")
                .expect_err("pending response times out")
                .code()
            })
        });
        assert_eq!(code, HttpErrorCode::READ_TIMEOUT);
    }

    #[test]
    fn explicit_host_header_is_preserved() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let url = test_url(address.port(), "/");
        let host = run_on_worker(move |context| {
            Box::pin(async move {
                let mut options = request_options(address);
                options
                    .headers
                    .insert(Box::from(&b"host"[..]), Box::from(&b"override.example"[..]));
                HttpClient::get(&context, url, options)
                    .await
                    .expect("request succeeds")
                    .headers
                    .headers()
                    .get_single_or_empty(b"x-host")
                    .to_vec()
            })
        });
        assert_eq!(host, b"override.example");
    }

    #[test]
    fn request_uses_supplied_reservation() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let url = test_url(address.port(), "/");
        let (custom_count, body_is_empty) = run_on_worker(move |context| {
            Box::pin(async move {
                let session = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .expect("connection succeeds");
                let reservation = session.reserve_request().expect("capacity is reserved");
                let parsed = Url::parse(url, false, UrlMode::StrictCompat);
                let mut spec = RequestSpec::get(parsed);
                spec.headers
                    .insert(Box::from(&b"custom"[..]), Box::from(&b"header"[..]));
                let response = HttpClient::request(&session, Some(reservation), spec)
                    .await
                    .expect("reserved request succeeds");
                (
                    response
                        .headers
                        .headers()
                        .get_single_or_empty(b"x-custom-header-count")
                        .to_vec(),
                    response.body.is_empty(),
                )
            })
        });
        assert_eq!(custom_count, b"1");
        assert!(body_is_empty);
    }

    #[test]
    fn response_trailers_are_collected() {
        let (server, certificate) = start_tls_server(&[b"h2"]);
        let address = server.address().expect("server has an address");
        let url = format!("https://localhost:{}/trailers", address.port());
        let response = run_on_worker(move |context| {
            Box::pin(async move {
                let mut tls = roxygen_transport::tls::TlsClientOptions::new("localhost");
                tls.add_root_certificate(certificate);
                tls.set_alpn_protocols([b"h2".as_slice()]);
                let options = RequestOptions {
                    connection: Some(ConnectionParams {
                        transport: ClientTransportOptions::Tls(tls),
                        ..ConnectionParams::default()
                    }),
                    ..request_options(address)
                };
                HttpClient::get(&context, url, options)
                    .await
                    .expect("HTTP/2 request succeeds")
            })
        });
        let trailers = response.trailers.expect("response has trailers");
        assert_eq!(trailers.get_single_or_empty(b"test"), b"Success");
    }

    #[test]
    fn post_sends_and_collects_body() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let url = test_url(address.port(), "/");
        let body = run_on_worker(move |context| {
            Box::pin(async move {
                HttpClient::post(&context, url, b"request body", request_options(address))
                    .await
                    .expect("post succeeds")
                    .body
                    .coalesce(64)
                    .expect("body is bounded")
            })
        });
        assert_eq!(body, b"request body"[..]);
    }

    #[test]
    fn early_response_stops_large_request_body() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let url = test_url(address.port(), "/earlyreturn");
        let body = vec![b'x'; 256 * 1024];
        let response = run_on_worker(move |context| {
            Box::pin(async move {
                HttpClient::post(&context, url, body, request_options(address))
                    .await
                    .expect("early response completes")
            })
        });
        assert_eq!(status(response), 200);
    }

    #[test]
    fn cancelled_post_does_not_detach_work() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let url = test_url(address.port(), "/");
        let code = run_on_worker(move |context| {
            Box::pin(async move {
                let session = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .expect("connection succeeds");
                let cancellation = CancellationSource::new();
                let options = RequestOptions {
                    cancellation: cancellation.token(),
                    ..RequestOptions::default()
                };
                let parsed = Url::parse(url, false, UrlMode::StrictCompat);
                let request =
                    HttpClient::post_with_session(&session, parsed, vec![b'a'; 1_000_000], options);
                tokio::pin!(request);
                poll_fn(|task| match request.as_mut().poll(task) {
                    Poll::Pending => Poll::Ready(()),
                    Poll::Ready(_) => panic!("large POST completed before cancellation"),
                })
                .await;
                cancellation.request_cancellation();
                request
                    .await
                    .expect_err("in-flight post is cancelled")
                    .code()
            })
        });
        assert_eq!(code, HttpErrorCode::CORO_CANCELLED);
    }

    #[test]
    fn post_with_existing_session_uses_same_connection() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let url = test_url(address.port(), "/");
        let (before, after, body) = run_on_worker(move |context| {
            Box::pin(async move {
                let session = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .expect("connection succeeds");
                let before = session.local_address().clone();
                let parsed = Url::parse(url, false, UrlMode::StrictCompat);
                let response = HttpClient::post_with_session(
                    &session,
                    parsed,
                    b"same connection",
                    RequestOptions::default(),
                )
                .await
                .expect("post succeeds");
                (
                    before,
                    session.local_address().clone(),
                    response.body.coalesce(64).expect("body is bounded"),
                )
            })
        });
        assert_eq!(before, after);
        assert_eq!(body, b"same connection"[..]);
    }

    struct CountingVisitor {
        bytes: Rc<RefCell<usize>>,
    }

    struct HeaderVisitor {
        statuses: Rc<RefCell<Vec<u16>>>,
    }

    impl ResponseVisitor for HeaderVisitor {
        fn on_headers(
            &mut self,
            headers: &HttpMessage,
            _is_final: bool,
            _eom: bool,
        ) -> VisitAction {
            let mut headers = headers.clone();
            self.statuses
                .borrow_mut()
                .push(headers.status_code().expect("response has a status"));
            VisitAction::Continue
        }
    }

    impl ResponseVisitor for CountingVisitor {
        fn on_body(&mut self, body: &BufQueue, _eom: bool) -> VisitAction {
            *self.bytes.borrow_mut() += body.chain_length();
            VisitAction::Continue
        }
    }

    #[test]
    fn response_visitor_observes_body() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let url = test_url(address.port(), "/");
        let observed = run_on_worker(move |context| {
            Box::pin(async move {
                let session = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .expect("connection succeeds");
                let bytes = Rc::new(RefCell::new(0));
                let visitor = CountingVisitor {
                    bytes: Rc::clone(&bytes),
                };
                let parsed = Url::parse(url, false, UrlMode::StrictCompat);
                let spec = RequestSpec::post(parsed, b"OK");
                HttpClient::request_with_visitor(&session, None, spec, Box::new(visitor))
                    .await
                    .expect("request succeeds");
                *bytes.borrow()
            })
        });
        assert_eq!(observed, 2);
    }

    #[test]
    fn post_with_source_reader_delivers_owned_body() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let url = test_url(address.port(), "/");
        let observed = run_on_worker(move |context| {
            Box::pin(async move {
                let observed = Rc::new(RefCell::new(0));
                let callback_observed = Rc::clone(&observed);
                let mut reader = HttpSourceReader::new();
                reader
                    .on_body(move |body, _| {
                        *callback_observed.borrow_mut() += body.chain_length();
                        HttpSourceReader::CONTINUE
                    })
                    .expect("body callback is installed");
                HttpClient::post_with_reader(
                    &context,
                    url,
                    vec![b'a'; 100],
                    request_options(address),
                    reader,
                )
                .await
                .expect("custom source reader succeeds");
                *observed.borrow()
            })
        });
        assert_eq!(observed, 100);
    }

    #[test]
    fn informational_headers_precede_the_final_response() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let url = test_url(address.port(), "/informational");
        let (statuses, final_status) = run_on_worker(move |context| {
            Box::pin(async move {
                let session = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .expect("connection succeeds");
                let statuses = Rc::new(RefCell::new(Vec::new()));
                let visitor = HeaderVisitor {
                    statuses: Rc::clone(&statuses),
                };
                let parsed = Url::parse(url, false, UrlMode::StrictCompat);
                let mut response = HttpClient::request_with_visitor(
                    &session,
                    None,
                    RequestSpec::get(parsed),
                    Box::new(visitor),
                )
                .await
                .expect("informational response succeeds");
                let final_status = response
                    .headers
                    .status_code()
                    .expect("final response has a status");
                let observed = statuses.borrow().clone();
                (observed, final_status)
            })
        });
        assert_eq!(statuses, [103, 200]);
        assert_eq!(final_status, 200);
    }

    #[test]
    fn arbitrary_request_source_is_supported() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let url = test_url(address.port(), "/");
        let method = run_on_worker(move |context| {
            Box::pin(async move {
                let session = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .expect("connection succeeds");
                let parsed = Url::parse(url, false, UrlMode::StrictCompat);
                let mut message = HttpMessage::new();
                message
                    .set_url(parsed.make_relative_url(), false)
                    .expect("test URL is valid");
                message
                    .set_method(HttpMethod::Options)
                    .expect("test method is valid");
                message.set_http_version(1, 1);
                message
                    .headers_mut()
                    .set(b"host", parsed.host_and_port_omit_default());
                let reservation = session.reserve_request().expect("capacity is reserved");
                let mut request = SourceRequest::new(HttpSourceHolder::new(Box::new(
                    HttpFixedSource::new(message, None),
                )));
                request.reservation = Some(reservation);
                HttpClient::request_source(&session, request)
                    .await
                    .expect("custom request succeeds")
                    .headers
                    .headers()
                    .get_single_or_empty(b"x-method")
                    .to_vec()
            })
        });
        assert_eq!(method, b"OPTIONS");
    }

    #[test]
    fn request_builder_preserves_method_headers_and_body() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let url = test_url(address.port(), "/");
        let observed = run_on_worker(move |context| {
            Box::pin(async move {
                let session = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .expect("connection succeeds");
                let parsed = Url::parse(url, false, UrlMode::StrictCompat);
                let mut spec = RequestSpec::post(parsed, b"payload");
                spec.headers
                    .insert(Box::from(&b"custom"[..]), Box::from(&b"one"[..]));
                let response = HttpClient::request(&session, None, spec)
                    .await
                    .expect("built request succeeds");
                (
                    response
                        .headers
                        .headers()
                        .get_single_or_empty(b"x-method")
                        .to_vec(),
                    response
                        .headers
                        .headers()
                        .get_single_or_empty(b"x-custom-header-count")
                        .to_vec(),
                    response.body.coalesce(64).expect("body is bounded"),
                )
            })
        });
        assert_eq!(observed.0, b"POST");
        assert_eq!(observed.1, b"1");
        assert_eq!(observed.2, b"payload"[..]);
    }

    #[test]
    fn invalid_url_is_rejected_before_connect() {
        let code = run_on_worker(|context| {
            Box::pin(async move {
                HttpClient::get(&context, b"not a URL", RequestOptions::default())
                    .await
                    .expect_err("invalid URL fails")
                    .code()
            })
        });
        assert_eq!(code, HttpErrorCode::INVALID_STATE_TRANSITION);
    }

    #[test]
    fn default_response_limit_matches_source_reader_maximum() {
        assert_eq!(
            RequestOptions::default().maximum_response_body,
            u32::MAX as usize
        );
        let url = Url::parse("http://example.test/", false, UrlMode::StrictCompat);
        assert_eq!(
            RequestSpec::get(url).maximum_response_body,
            u32::MAX as usize
        );
    }

    #[test]
    fn default_response_reader_contextualizes_header_source_error() {
        let error = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime")
            .block_on(crate::read_response(
                HttpSourceHolder::new(Box::new(FailingHeaders)),
                u32::MAX as usize,
                None,
                CancellationToken::never(),
            ))
            .expect_err("header source failure is propagated");
        assert_eq!(error.code(), HttpErrorCode::INTERNAL_ERROR);
        assert_eq!(
            error.message(),
            b"Error receiving response headers, err=header source failed"
        );
    }

    #[test]
    fn connection_error_is_propagated() {
        let code = run_on_worker(|context| {
            Box::pin(async move {
                let options = RequestOptions {
                    server_address: Some(Box::from("127.0.0.1")),
                    timeout: Some(Duration::from_secs(5)),
                    ..RequestOptions::default()
                };
                HttpClient::get(&context, b"http://example.test:0/", options)
                    .await
                    .expect_err("connection fails")
                    .code()
            })
        });
        assert_eq!(code, HttpErrorCode::CONNECT_ERROR);
    }

    #[test]
    fn non_success_status_is_returned() {
        assert_eq!(status(get("/error")), 500);
    }

    #[test]
    fn server_header_source_failure_is_synthesized_as_500() {
        assert_eq!(status(get("/abortHeaders")), 500);
    }

    #[test]
    fn response_body_error_is_propagated() {
        use self::body_error_fixture::EXPECTED_PREFIX;

        let (server, acknowledge_body, failure_released) = body_error_fixture::start_server();
        let address = server.address().expect("server has an address");
        let url = test_url(address.port(), "/abortBody");
        let prefix = Arc::new(Mutex::new(BufQueue::new(EXPECTED_PREFIX.len())));
        let client_prefix = Arc::clone(&prefix);
        let error = run_on_worker(move |context| {
            Box::pin(async move {
                let mut options = request_options(address);
                options.logger = ClientLoggerSelection::Explicit(
                    body_error_fixture::observation_logger(acknowledge_body, client_prefix),
                );
                HttpClient::get(&context, url, options)
                    .await
                    .expect_err("server body failure reaches the client")
            })
        });
        let prefix = prefix
            .lock()
            .expect("body observation")
            .coalesce(EXPECTED_PREFIX.len())
            .expect("observed response prefix");
        let failure_released = failure_released.load(Ordering::SeqCst);
        assert_ne!(
            error.code(),
            HttpErrorCode::READ_TIMEOUT,
            "message: {:?}; observed prefix: {prefix:?}; source failure released: {failure_released}",
            String::from_utf8_lossy(error.message())
        );
        assert!(
            String::from_utf8_lossy(error.message()).contains("response body"),
            "error code: {:?}; message: {:?}; observed prefix: {prefix:?}; source failure released: {failure_released}",
            error.code(),
            String::from_utf8_lossy(error.message())
        );
        assert_eq!(prefix.as_ref(), EXPECTED_PREFIX);
        assert!(
            failure_released,
            "the original source failure must be released after acknowledgement"
        );
    }

    #[test]
    fn request_body_error_is_not_rewritten_as_timeout() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let url = test_url(address.port(), "/");
        let error = run_on_worker(move |context| {
            Box::pin(async move {
                let session = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .expect("connection succeeds");
                let parsed = Url::parse(url, false, UrlMode::StrictCompat);
                let reservation = session.reserve_request().expect("capacity is reserved");
                let mut request = SourceRequest::new(HttpSourceHolder::new(Box::new(
                    FailingRequestBody::new(&parsed),
                )));
                request.reservation = Some(reservation);
                HttpClient::request_source(&session, request)
                    .await
                    .expect_err("request body failure reaches the client")
            })
        });
        assert_ne!(error.code(), HttpErrorCode::READ_TIMEOUT);
    }

    mod body_error_fixture {
        use super::*;

        pub(super) const EXPECTED_PREFIX: &[u8] = b"hello world";
        const ACKNOWLEDGEMENT_TIMEOUT: Duration = Duration::from_secs(5);

        struct BodyErrorBarrier {
            state: Rc<RefCell<BodyErrorGate>>,
        }

        struct BodyErrorGate {
            acknowledgement: Option<oneshot::Receiver<()>>,
            deadline: Option<tokio::time::Instant>,
            pending_error: Option<LocalSourceFuture<HttpError>>,
            outcome: Option<HttpError>,
            failure_released: Arc<AtomicBool>,
        }

        impl BodyErrorBarrier {
            fn new(
                acknowledgement: oneshot::Receiver<()>,
                failure_released: Arc<AtomicBool>,
            ) -> Self {
                Self {
                    state: Rc::new(RefCell::new(BodyErrorGate {
                        acknowledgement: Some(acknowledgement),
                        deadline: None,
                        pending_error: None,
                        outcome: None,
                        failure_released,
                    })),
                }
            }
        }

        impl HttpSourceFilterHooks for BodyErrorBarrier {
            fn filter_name(&self) -> &'static str {
                "BodyErrorBarrier"
            }

            fn read_body_event(
                &mut self,
                downstream: LocalSourceFuture<Result<HttpBodyEvent, HttpError>>,
            ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
                let state = Rc::downgrade(&self.state);
                let mut downstream = Some(downstream);
                Box::pin(poll_fn(move |context| {
                    let Some(state) = state.upgrade() else {
                        return Poll::Ready(Err(HttpError::with_message(
                            HttpErrorCode::DROPPED,
                            b"body-error source dropped",
                        )));
                    };
                    let mut state = state.borrow_mut();
                    if let Some(error) = &state.outcome {
                        return Poll::Ready(Err(error.clone()));
                    }
                    if state.pending_error.is_none() {
                        let error = match downstream
                            .as_mut()
                            .expect("body read retains downstream until its first error")
                            .as_mut()
                            .poll(context)
                        {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(Ok(event)) => return Poll::Ready(Ok(event)),
                            Poll::Ready(Err(error)) => error,
                        };
                        let acknowledgement = state
                            .acknowledgement
                            .take()
                            .expect("first terminal source failure owns the acknowledgement");
                        let deadline = tokio::time::Instant::now() + ACKNOWLEDGEMENT_TIMEOUT;
                        state.deadline = Some(deadline);
                        let failure_released = Arc::clone(&state.failure_released);
                        state.pending_error = Some(Box::pin(async move {
                            match tokio::time::timeout_at(deadline, acknowledgement).await {
                                Ok(Ok(())) => {
                                    failure_released.store(true, Ordering::SeqCst);
                                    error
                                }
                                Ok(Err(_)) => HttpError::with_message(
                                    HttpErrorCode::CORO_CANCELLED,
                                    b"client reader dropped before body acknowledgement",
                                ),
                                Err(_) => HttpError::with_message(
                                    HttpErrorCode::READ_TIMEOUT,
                                    b"client body acknowledgement timed out",
                                ),
                            }
                        }));
                    }
                    drop(downstream.take());
                    let Poll::Ready(error) = state
                        .pending_error
                        .as_mut()
                        .expect("pending source failure owns its gate")
                        .as_mut()
                        .poll(context)
                    else {
                        return Poll::Pending;
                    };
                    state.outcome = Some(error.clone());
                    state.pending_error = None;
                    Poll::Ready(Err(error))
                }))
            }
        }

        pub(super) async fn check_cancelled_gate_read() {
            for dispose_source in [false, true] {
                let (acknowledge, acknowledgement) = oneshot::channel();
                let failure_released = Arc::new(AtomicBool::new(false));
                let barrier = BodyErrorBarrier::new(acknowledgement, Arc::clone(&failure_released));
                let state = Rc::downgrade(&barrier.state);
                let mut source =
                    HttpSourceFilter::with_source(barrier, Box::new(FailingResponse::at_body()));
                source.read_header_event().await.expect("fixture headers");
                let body = source
                    .read_body_event(11)
                    .await
                    .expect("fixture body prefix");
                assert_eq!(
                    body.as_body()
                        .expect("fixture body event")
                        .coalesce(EXPECTED_PREFIX.len())
                        .expect("fixture body bytes")
                        .as_ref(),
                    EXPECTED_PREFIX
                );
                let mut first_read = source.read_body_event(11);
                assert!(
                    first_read
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                let deadline = state.upgrade().expect("source owns gate").borrow().deadline;
                assert!(deadline.is_some());
                drop(first_read);
                assert!(
                    !acknowledge.is_closed(),
                    "read cancellation retains receiver"
                );
                assert!(!failure_released.load(Ordering::SeqCst));

                let mut next_read = source.read_body_event(11);
                assert!(
                    next_read
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                assert_eq!(
                    state.upgrade().expect("source owns gate").borrow().deadline,
                    deadline,
                    "recreated reads must not restart the deadline"
                );
                if dispose_source {
                    drop(source);
                    assert!(
                        state.upgrade().is_none(),
                        "read futures do not own the gate"
                    );
                    assert!(
                        acknowledge.send(()).is_err(),
                        "source drop disposes receiver"
                    );
                    assert_eq!(
                        next_read.await.err().expect("source was dropped").code(),
                        HttpErrorCode::DROPPED
                    );
                    assert!(!failure_released.load(Ordering::SeqCst));
                } else {
                    acknowledge
                        .send(())
                        .expect("recreated read retains receiver");
                    let error = next_read
                        .await
                        .err()
                        .expect("acknowledged original source failure");
                    assert_eq!(error.code(), HttpErrorCode::PROTOCOL_ERROR);
                    assert_eq!(error.message(), b"server body source failed");
                    assert!(failure_released.load(Ordering::SeqCst));
                    let replay = source
                        .read_body_event(11)
                        .await
                        .err()
                        .expect("sticky gate result");
                    assert_eq!(replay.code(), error.code());
                    assert_eq!(replay.message(), error.message());
                    drop(source);
                    assert!(state.upgrade().is_none());
                }
            }
        }

        struct BodyErrorHandler {
            inner: HttpHandlerRef,
            acknowledgement: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
            failure_released: Arc<AtomicBool>,
        }

        impl HttpHandler for BodyErrorHandler {
            fn handle_request(
                &self,
                context: HandlerSessionContext,
                request: HttpSourceHolder,
            ) -> LocalHandlerFuture {
                let response = self.inner.handle_request(context, request);
                let acknowledgement = Arc::clone(&self.acknowledgement);
                let failure_released = Arc::clone(&self.failure_released);
                Box::pin(async move {
                    let response = response.await?;
                    let acknowledgement = acknowledgement
                        .lock()
                        .expect("body acknowledgement ownership")
                        .take()
                        .expect("one request uses the body-error fixture");
                    Ok(HttpSourceHolder::new(Box::new(
                        HttpSourceFilter::with_source(
                            BodyErrorBarrier::new(acknowledgement, failure_released),
                            Box::new(response),
                        ),
                    )))
                })
            }
        }

        pub(super) fn start_server() -> (ScopedHttpServer, oneshot::Sender<()>, Arc<AtomicBool>) {
            let (acknowledge_body, body_acknowledgement) = oneshot::channel();
            let body_acknowledgement = Arc::new(Mutex::new(Some(body_acknowledgement)));
            let failure_released = Arc::new(AtomicBool::new(false));
            let server_failure_released = Arc::clone(&failure_released);
            let factory =
                super::super::handler_factory(Arc::new(super::super::ServerStats::default()));
            let server = ScopedHttpServer::start(
                ServerConfig::default(),
                Arc::new(move |worker| {
                    Rc::new(BodyErrorHandler {
                        inner: factory(worker),
                        acknowledgement: Arc::clone(&body_acknowledgement),
                        failure_released: Arc::clone(&server_failure_released),
                    })
                }),
                None,
            )
            .expect("body-error test server starts");
            (server, acknowledge_body, failure_released)
        }

        struct BodyObservationLogger {
            acknowledgement: Mutex<Option<oneshot::Sender<()>>>,
            prefix: Arc<Mutex<BufQueue>>,
        }

        struct BodyObservationLog {
            acknowledgement: RefCell<Option<oneshot::Sender<()>>>,
            prefix: Arc<Mutex<BufQueue>>,
        }

        struct BodyObservationFilter {
            acknowledgement: Rc<RefCell<Option<oneshot::Sender<()>>>>,
            prefix: Arc<Mutex<BufQueue>>,
        }

        pub(super) fn observation_logger(
            acknowledgement: oneshot::Sender<()>,
            prefix: Arc<Mutex<BufQueue>>,
        ) -> Arc<dyn ClientRequestLogger> {
            Arc::new(BodyObservationLogger {
                acknowledgement: Mutex::new(Some(acknowledgement)),
                prefix,
            })
        }

        impl ClientRequestLogger for BodyObservationLogger {
            fn create(
                &self,
                _session: &crate::connector::ClientSession,
            ) -> Box<dyn ClientRequestLog> {
                let acknowledgement = self
                    .acknowledgement
                    .lock()
                    .expect("body observation logger ownership")
                    .take()
                    .expect("one body observation request");
                Box::new(BodyObservationLog {
                    acknowledgement: RefCell::new(Some(acknowledgement)),
                    prefix: Arc::clone(&self.prefix),
                })
            }
        }

        impl ClientRequestLog for BodyObservationLog {
            fn response_filter(
                &self,
                source: HttpSourceHolder,
            ) -> Result<HttpSourceHolder, HttpError> {
                let acknowledgement = self
                    .acknowledgement
                    .borrow_mut()
                    .take()
                    .expect("one body observation response");
                Ok(HttpSourceHolder::new(Box::new(
                    HttpSourceFilter::with_source(
                        BodyObservationFilter {
                            acknowledgement: Rc::new(RefCell::new(Some(acknowledgement))),
                            prefix: Arc::clone(&self.prefix),
                        },
                        Box::new(source),
                    ),
                )))
            }
        }

        impl HttpSourceFilterHooks for BodyObservationFilter {
            fn filter_name(&self) -> &'static str {
                "BodyObservationFilter"
            }

            fn read_body_event(
                &mut self,
                downstream: LocalSourceFuture<Result<HttpBodyEvent, HttpError>>,
            ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
                let acknowledgement = Rc::downgrade(&self.acknowledgement);
                let prefix = Arc::clone(&self.prefix);
                Box::pin(async move {
                    let event = downstream.await?;
                    if let Some(body) = event.as_body() {
                        assert!(
                            !event.eom(),
                            "failing response must not finish successfully"
                        );
                        let acknowledgement = acknowledgement.upgrade().ok_or_else(|| {
                            HttpError::with_message(
                                HttpErrorCode::DROPPED,
                                b"body observation source dropped",
                            )
                        })?;
                        let complete = {
                            let mut prefix = prefix.lock().expect("body observation");
                            prefix
                                .append(&mut body.clone_shared())
                                .expect("body prefix stays bounded");
                            let bytes = prefix
                                .coalesce(EXPECTED_PREFIX.len())
                                .expect("observed prefix bytes");
                            assert!(
                                EXPECTED_PREFIX.starts_with(bytes.as_ref()),
                                "unexpected partial-body prefix: {bytes:?}"
                            );
                            bytes.len() == EXPECTED_PREFIX.len()
                        };
                        if complete {
                            let acknowledgement = acknowledgement.borrow_mut().take();
                            if let Some(acknowledgement) = acknowledgement {
                                acknowledgement
                                    .send(())
                                    .expect("server is awaiting complete body observation");
                            }
                        }
                    }
                    Ok(event)
                })
            }
        }
    }

    #[test]
    fn source_reader_handles_real_server_body_error() {
        use self::body_error_fixture::EXPECTED_PREFIX;

        let (server, acknowledge_body, failure_released) = body_error_fixture::start_server();
        let address = server.address().expect("server has an address");
        let url = test_url(address.port(), "/abortBody");
        let (context, prefix) = run_on_worker(move |context| {
            Box::pin(async move {
                body_error_fixture::check_cancelled_gate_read().await;
                let observed = Rc::new(RefCell::new(None));
                let callback_observed = Rc::clone(&observed);
                let prefix = Rc::new(RefCell::new(BufQueue::new(EXPECTED_PREFIX.len())));
                let callback_prefix = Rc::clone(&prefix);
                let mut acknowledge_body = Some(acknowledge_body);
                let mut reader = HttpSourceReader::new();
                reader
                    .on_body(move |mut body, eom| {
                        assert!(!eom, "failing response must not finish successfully");
                        let mut prefix = callback_prefix.borrow_mut();
                        prefix.append(&mut body).expect("body prefix stays bounded");
                        let bytes = prefix
                            .coalesce(EXPECTED_PREFIX.len())
                            .expect("observed prefix bytes");
                        assert!(
                            EXPECTED_PREFIX.starts_with(bytes.as_ref()),
                            "unexpected partial-body prefix: {bytes:?}"
                        );
                        if bytes.len() == EXPECTED_PREFIX.len()
                            && let Some(acknowledge_body) = acknowledge_body.take()
                        {
                            acknowledge_body
                                .send(())
                                .expect("server is awaiting complete body observation");
                        }
                        HttpSourceReader::CONTINUE
                    })
                    .expect("body observation callback is installed");
                reader
                    .on_error(move |context, error| {
                        callback_observed.replace(Some((context, error.code())));
                    })
                    .expect("error callback is installed");
                HttpClient::get_with_reader(&context, url, request_options(address), reader)
                    .await
                    .expect("the source reader handles the source error");
                let observed = observed.borrow_mut().take();
                let prefix = prefix
                    .borrow()
                    .coalesce(EXPECTED_PREFIX.len())
                    .expect("final observed prefix bytes");
                (observed, prefix)
            })
        });
        let (context, code) = context.expect("body error callback runs");
        let failure_released = failure_released.load(Ordering::SeqCst);
        assert_eq!(
            context,
            ErrorContext::Body,
            "error code: {code:?}; observed prefix: {prefix:?}; source failure released: {failure_released}"
        );
        assert_ne!(code, HttpErrorCode::READ_TIMEOUT);
        assert_eq!(prefix.as_ref(), EXPECTED_PREFIX);
        assert!(
            failure_released,
            "the original source failure must be released after acknowledgement"
        );
    }

    #[test]
    fn source_reader_without_error_callback_propagates_partial_body_failure() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let error = runtime.block_on(async {
            let mut reader = HttpSourceReader::with_source(Box::new(FailingResponse::at_body()));
            reader
                .read(u32::MAX)
                .await
                .expect_err("an unhandled source error propagates")
        });
        assert_eq!(error.code(), HttpErrorCode::PROTOCOL_ERROR);
        assert_eq!(error.message(), b"server body source failed");
    }

    #[test]
    fn public_default_reader_collects_response_and_contextualizes_errors() {
        let mut response = HttpFixedSource::make_fixed_response(
            200,
            Some(
                BufQueue::from_bytes(bytes::Bytes::from_static(b"body"), 4)
                    .expect("test body is bounded"),
            ),
        )
        .expect("test response is valid");
        response.set_trailers(HttpHeaders::new());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let collected = runtime
            .block_on(HttpClient::read_response(HttpSourceHolder::new(Box::new(
                response,
            ))))
            .expect("default reader collects a valid response");
        assert_eq!(
            collected.body.coalesce(4).expect("body is bounded"),
            b"body"[..]
        );

        let error = runtime
            .block_on(HttpClient::read_response(HttpSourceHolder::new(Box::new(
                FailingHeaders,
            ))))
            .expect_err("default reader contextualizes source errors");
        assert_eq!(
            error.message(),
            b"Error receiving response headers, err=header source failed"
        );
    }

    #[test]
    fn reader_and_visitor_requests_reject_invalid_url_before_admission() {
        let (server, stats) = start_plain_server();
        let address = server.address().expect("server has an address");
        let rejected = run_on_worker(move |context| {
            Box::pin(async move {
                let session = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .expect("connection succeeds");
                let visitor = HttpClient::request_with_visitor(
                    &session,
                    None,
                    RequestSpec::get(Url::parse("not a URL", false, UrlMode::StrictCompat)),
                    Box::new(CountingVisitor {
                        bytes: Rc::new(RefCell::new(0)),
                    }),
                )
                .await;
                let reader = HttpClient::request_with_reader(
                    &session,
                    None,
                    RequestSpec::get(Url::parse("not a URL", false, UrlMode::StrictCompat)),
                    HttpSourceReader::new(),
                )
                .await;
                matches!(visitor, Err(error) if error.code() == HttpErrorCode::INVALID_STATE_TRANSITION)
                    && matches!(reader, Err(error) if error.code() == HttpErrorCode::INVALID_STATE_TRANSITION)
                    && session.active_transactions() == 0
            })
        });
        assert!(rejected);
        assert_eq!(stats.requests.load(Ordering::Relaxed), 0);
    }
}

mod pool {
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::future::Future;
    use std::future::poll_fn;
    use std::rc::Rc;
    use std::rc::Weak;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;
    use std::time::Duration;

    use roxygen_core::error::HttpError;
    use roxygen_core::error::HttpErrorCode;
    use roxygen_core::message::SocketAddress;
    use roxygen_core::url::Url;
    use roxygen_core::url::UrlMode;
    use roxygen_runtime::cancellation::CancellationSource;
    use roxygen_runtime::cancellation::CancellationToken;
    use roxygen_runtime::worker::WorkerContext;
    use roxygen_session::session::LifecycleEvent;
    use roxygen_session::session::LifecycleObserver;
    use roxygen_session::session::LifecycleObserverRegistration;
    use roxygen_session::session::RequestReservation;
    use roxygen_transport::tls::TlsClientOptions;
    use rustls::NamedGroup;
    use rustls::client::ClientSessionMemoryCache;
    use rustls::client::ClientSessionStore;
    use rustls::client::Tls12ClientSessionValue;
    use rustls::client::Tls13ClientSessionValue;
    use rustls::pki_types::ServerName;

    use super::connector::RecordingTlsStats;
    use super::connector::TlsStatEvent;
    use super::run_on_worker;
    use super::socket_address;
    use super::start_plain_server;
    use super::start_tls_server;
    use crate::HttpClient;
    use crate::RequestSpec;
    use crate::connector::ClientSession;
    use crate::connector::ClientTransportOptions;
    use crate::connector::ConnectionParams;
    use crate::connector::HttpConnector;
    use crate::connector::LocalClientFuture;
    use crate::connector::SessionConnector;
    use crate::connector::SessionParams;
    use crate::pool::GetSessionResult;
    use crate::pool::HttpSessionPool;
    use crate::pool::IdleSessionObserver;
    use crate::pool::PoolError;
    use crate::pool::PoolObserver;
    use crate::pool::PoolParams;

    struct RecordingConnector {
        attempts: Rc<Cell<u32>>,
        parameter_keys: Rc<RefCell<Vec<usize>>>,
        session_caching: Option<Rc<Cell<Option<bool>>>>,
        fail_after: Option<u32>,
        delay: Duration,
    }

    #[derive(Debug)]
    struct CountingSessionStore {
        inner: ClientSessionMemoryCache,
        inserted_tls13_tickets: AtomicUsize,
        taken_tls13_tickets: AtomicUsize,
    }

    impl CountingSessionStore {
        fn new(capacity: usize) -> Self {
            Self {
                inner: ClientSessionMemoryCache::new(capacity),
                inserted_tls13_tickets: AtomicUsize::new(0),
                taken_tls13_tickets: AtomicUsize::new(0),
            }
        }
    }

    impl ClientSessionStore for CountingSessionStore {
        fn set_kx_hint(&self, server_name: ServerName<'static>, group: NamedGroup) {
            self.inner.set_kx_hint(server_name, group);
        }

        fn kx_hint(&self, server_name: &ServerName<'_>) -> Option<NamedGroup> {
            self.inner.kx_hint(server_name)
        }

        fn set_tls12_session(
            &self,
            server_name: ServerName<'static>,
            value: Tls12ClientSessionValue,
        ) {
            self.inner.set_tls12_session(server_name, value);
        }

        fn tls12_session(&self, server_name: &ServerName<'_>) -> Option<Tls12ClientSessionValue> {
            self.inner.tls12_session(server_name)
        }

        fn remove_tls12_session(&self, server_name: &ServerName<'static>) {
            self.inner.remove_tls12_session(server_name);
        }

        fn insert_tls13_ticket(
            &self,
            server_name: ServerName<'static>,
            value: Tls13ClientSessionValue,
        ) {
            self.inserted_tls13_tickets.fetch_add(1, Ordering::Relaxed);
            self.inner.insert_tls13_ticket(server_name, value);
        }

        fn take_tls13_ticket(
            &self,
            server_name: &ServerName<'static>,
        ) -> Option<Tls13ClientSessionValue> {
            self.taken_tls13_tickets.fetch_add(1, Ordering::Relaxed);
            self.inner.take_tls13_ticket(server_name)
        }
    }

    impl SessionConnector for RecordingConnector {
        fn connect(
            &self,
            context: WorkerContext,
            address: SocketAddress,
            timeout: Duration,
            connection: ConnectionParams,
            session: SessionParams,
            cancellation: CancellationToken,
        ) -> LocalClientFuture<Result<ClientSession, HttpError>> {
            let attempt = self.attempts.get().saturating_add(1);
            self.attempts.set(attempt);
            self.parameter_keys
                .borrow_mut()
                .push(connection.identity_validation as usize);
            if let Some(observed) = &self.session_caching {
                observed.set(Some(match &connection.transport {
                    ClientTransportOptions::Plaintext => false,
                    ClientTransportOptions::Tls(options) => options.session_caching_enabled(),
                }));
            }
            let fail = self.fail_after.is_some_and(|limit| attempt > limit);
            let delay = self.delay;
            Box::pin(async move {
                if delay != Duration::ZERO {
                    tokio::time::sleep(delay).await;
                }
                if fail {
                    return Err(HttpError::with_message(
                        HttpErrorCode::CONNECT_ERROR,
                        b"injected connection failure",
                    ));
                }
                HttpConnector::connect(
                    &context,
                    address,
                    timeout,
                    connection,
                    session,
                    cancellation,
                )
                .await
            })
        }
    }

    fn pool_params(max_connections: u32) -> PoolParams {
        PoolParams {
            max_connections,
            connect_timeout: Duration::from_secs(5),
            ..PoolParams::default()
        }
    }

    #[test]
    fn requests_reuse_available_session() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let reused = run_on_worker(move |context| {
            Box::pin(async move {
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    pool_params(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                let first = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("first acquisition succeeds");
                let first_address = first.session.local_address().clone();
                drop(first.reservation);
                let second = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("second acquisition succeeds");
                first_address == *second.session.local_address() && pool.session_count() == 1
            })
        });
        assert!(reused);
    }

    #[test]
    fn pool_configures_tls_session_caching() {
        let observed = run_on_worker(|context| {
            Box::pin(async move {
                let mut results = Vec::new();
                for enabled in [false, true] {
                    let caching = Rc::new(Cell::new(None));
                    let connector = Rc::new(RecordingConnector {
                        attempts: Rc::new(Cell::new(0)),
                        parameter_keys: Rc::new(RefCell::new(Vec::new())),
                        session_caching: Some(Rc::clone(&caching)),
                        fail_after: Some(0),
                        delay: Duration::ZERO,
                    });
                    let pool = HttpSessionPool::with_connector(
                        context.clone(),
                        SocketAddress::new("127.0.0.1", 1),
                        PoolParams {
                            enable_tls_session_caching: enabled,
                            ..pool_params(1)
                        },
                        ConnectionParams {
                            transport: ClientTransportOptions::Tls(TlsClientOptions::new(
                                "localhost",
                            )),
                            ..ConnectionParams::default()
                        },
                        SessionParams::default(),
                        connector,
                    );
                    let _ = pool
                        .get_session_with_reservation(CancellationToken::never())
                        .await;
                    results.push(caching.get());
                }
                results
            })
        });
        assert_eq!(observed, [Some(false), Some(true)]);
    }

    fn tls_flush_reconnect(custom_server_name: &str) -> (bool, Vec<TlsStatEvent>) {
        let (server, certificate) = start_tls_server(&[b"http/1.1"]);
        let address = server.address().expect("server has an address");
        let server_name = custom_server_name.to_owned();
        run_on_worker(move |context| {
            Box::pin(async move {
                let events = Arc::new(Mutex::new(Vec::new()));
                let mut tls = TlsClientOptions::new(server_name);
                tls.add_root_certificate(certificate);
                tls.set_alpn_protocols([b"http/1.1".as_slice()]);
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    pool_params(1),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(tls),
                        tls_stats: Some(Arc::new(RecordingTlsStats(Arc::clone(&events)))),
                        ..ConnectionParams::default()
                    },
                    SessionParams::default(),
                );
                let first = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("first TLS pool acquisition succeeds");
                let url = Url::parse(
                    format!("https://localhost:{}/", address.port()),
                    false,
                    UrlMode::StrictCompat,
                );
                HttpClient::request(
                    &first.session,
                    Some(first.reservation),
                    RequestSpec::get(url),
                )
                .await
                .expect("first TLS pool request processes session tickets");
                tokio::time::sleep(Duration::from_millis(50)).await;
                pool.flush();
                let resumed = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .is_ok_and(|second| second.session.tls_session_reused());
                let events = events.lock().expect("TLS stats lock is available").clone();
                (resumed, events)
            })
        })
    }

    #[test]
    fn flush_replaces_tls_session_without_resetting_options() {
        let (resumed, events) = tls_flush_reconnect("localhost");
        assert!(resumed);
        assert_eq!(
            events,
            [
                TlsStatEvent::Connection(true),
                TlsStatEvent::Connection(false)
            ]
        );
    }

    #[test]
    fn custom_tls_options_survive_flush() {
        let (resumed, events) = tls_flush_reconnect("localhost");
        assert!(resumed);
        assert_eq!(
            events,
            [
                TlsStatEvent::Connection(true),
                TlsStatEvent::Connection(false)
            ]
        );
    }

    #[test]
    fn pool_uses_a_custom_tls_session_store_for_resumption() {
        let (server, certificate) = start_tls_server(&[b"http/1.1"]);
        let address = server.address().expect("server has an address");
        let store = Arc::new(CountingSessionStore::new(32));
        let counters = Arc::clone(&store);
        let events = Arc::new(Mutex::new(Vec::new()));
        let recorded_events = Arc::clone(&events);
        let resumed = run_on_worker(move |context| {
            Box::pin(async move {
                let mut tls = TlsClientOptions::new("localhost");
                tls.add_root_certificate(certificate);
                tls.set_session_store(store);
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    PoolParams {
                        enable_tls_session_caching: false,
                        ..pool_params(1)
                    },
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(tls),
                        tls_stats: Some(Arc::new(RecordingTlsStats(recorded_events))),
                        ..ConnectionParams::default()
                    },
                    SessionParams::default(),
                );
                let first = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("first TLS pool acquisition succeeds");
                let url = Url::parse(
                    format!("https://localhost:{}/", address.port()),
                    false,
                    UrlMode::StrictCompat,
                );
                HttpClient::request(
                    &first.session,
                    Some(first.reservation),
                    RequestSpec::get(url),
                )
                .await
                .expect("first TLS pool request processes session tickets");
                tokio::time::sleep(Duration::from_millis(50)).await;
                pool.flush();
                pool.get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("second TLS pool acquisition succeeds")
                    .session
                    .tls_session_reused()
            })
        });
        assert!(resumed);
        assert!(counters.inserted_tls13_tickets.load(Ordering::Relaxed) > 0);
        assert!(counters.taken_tls13_tickets.load(Ordering::Relaxed) > 0);
        assert_eq!(
            *events.lock().expect("TLS stats lock is available"),
            [
                TlsStatEvent::Connection(true),
                TlsStatEvent::Connection(false)
            ]
        );
    }

    #[test]
    fn late_binding_reuses_released_capacity() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let reused = run_on_worker(move |context| {
            Box::pin(async move {
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    pool_params(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                let first = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("first acquisition succeeds");
                let first_address = first.session.local_address().clone();
                let second = pool.get_session_with_reservation(CancellationToken::never());
                tokio::pin!(second);
                tokio::select! {
                    _ = &mut second => panic!("second acquisition completed while pool was full"),
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
                drop(first.reservation);
                let second = second.await.expect("released capacity wakes waiter");
                first_address == *second.session.local_address()
            })
        });
        assert!(reused);
    }

    #[test]
    fn late_binding_times_out_at_capacity() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let timed_out = run_on_worker(move |context| {
            Box::pin(async move {
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    PoolParams {
                        connect_timeout: Duration::from_secs(1),
                        ..pool_params(1)
                    },
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                let _held = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("first acquisition succeeds");
                matches!(
                    pool.get_session_with_reservation(CancellationToken::never())
                        .await,
                    Err(PoolError::Timeout)
                )
            })
        });
        assert!(timed_out);
    }

    #[test]
    fn waiter_gets_a_fresh_timeout_after_a_wakeup_without_capacity() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let acquired = run_on_worker(move |context| {
            Box::pin(async move {
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    PoolParams {
                        connect_timeout: Duration::from_millis(500),
                        max_waiters: 2,
                        ..pool_params(1)
                    },
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                let held = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("first acquisition succeeds");
                let first_cancellation = CancellationSource::new();
                let second_cancellation = CancellationSource::new();
                let mut first = pool.get_session_with_reservation(first_cancellation.token());
                let mut second = pool.get_session_with_reservation(second_cancellation.token());
                poll_fn(|context| {
                    assert!(matches!(first.as_mut().poll(context), Poll::Pending));
                    assert!(matches!(second.as_mut().poll(context), Poll::Pending));
                    Poll::Ready(())
                })
                .await;

                tokio::time::sleep(Duration::from_millis(200)).await;
                pool.wake_waiters_for_test(2);
                poll_fn(|context| {
                    assert!(matches!(first.as_mut().poll(context), Poll::Pending));
                    assert!(matches!(second.as_mut().poll(context), Poll::Pending));
                    Poll::Ready(())
                })
                .await;
                tokio::time::sleep(Duration::from_millis(350)).await;
                drop(held.reservation);

                let first = tokio::time::timeout(Duration::from_millis(100), first)
                    .await
                    .expect("fresh waiter timeout leaves time for released capacity")
                    .expect("oldest waiter acquires the released slot");
                second_cancellation.request_cancellation();
                assert!(matches!(second.await, Err(PoolError::Cancelled)));
                drop(first.reservation);
                true
            })
        });
        assert!(acquired);
    }

    #[test]
    fn pending_headers_can_open_another_connection() {
        let (server, certificate) = start_tls_server(&[b"h2"]);
        let address = server.address().expect("server has an address");
        let distinct = run_on_worker(move |context| {
            Box::pin(async move {
                let mut tls = TlsClientOptions::new("localhost");
                tls.add_root_certificate(certificate);
                tls.set_alpn_protocols([b"h2".as_slice()]);
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    pool_params(2),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(tls),
                        ..ConnectionParams::default()
                    },
                    SessionParams {
                        max_concurrent_outgoing_streams: Some(1),
                        ..SessionParams::default()
                    },
                );
                let first = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("first acquisition succeeds");
                let cancellation = CancellationSource::new();
                let mut request = RequestSpec::get(Url::parse(
                    format!("https://localhost:{}/pendingHeaders", address.port()),
                    false,
                    UrlMode::StrictCompat,
                ));
                request.cancellation = cancellation.token();
                let pending = HttpClient::request(&first.session, Some(first.reservation), request);
                tokio::pin!(pending);
                tokio::select! {
                    _ = &mut pending => panic!("request completed before response headers"),
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
                let second = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("second acquisition succeeds");
                let distinct = first.session.local_address() != second.session.local_address();
                cancellation.request_cancellation();
                let error = pending.await.expect_err("pending header read is cancelled");
                distinct && error.code() == HttpErrorCode::CORO_CANCELLED
            })
        });
        assert!(distinct);
    }

    #[test]
    fn h2_capacity_increase_wakes_two_waiters_on_one_connection() {
        let (server, certificate) = start_tls_server(&[b"h2"]);
        let address = server.address().expect("server has an address");
        let reused = run_on_worker(move |context| {
            Box::pin(async move {
                let mut tls = TlsClientOptions::new("localhost");
                tls.add_root_certificate(certificate);
                tls.set_alpn_protocols([b"h2".as_slice()]);
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    PoolParams {
                        max_waiters: 2,
                        ..pool_params(1)
                    },
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(tls),
                        ..ConnectionParams::default()
                    },
                    SessionParams {
                        max_concurrent_outgoing_streams: Some(1),
                        ..SessionParams::default()
                    },
                );
                let first = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("first stream is reserved");
                let second = pool.get_session_with_reservation(CancellationToken::never());
                let third = pool.get_session_with_reservation(CancellationToken::never());
                tokio::pin!(second);
                tokio::pin!(third);
                tokio::select! {
                    _ = &mut second => panic!("second stream admitted before capacity increase"),
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
                tokio::select! {
                    _ = &mut third => panic!("third stream admitted before capacity increase"),
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
                assert!(matches!(
                    pool.get_session_with_reservation(CancellationToken::never())
                        .await,
                    Err(PoolError::MaxWaiters)
                ));
                assert!(
                    first
                        .session
                        .set_max_concurrent_outgoing_streams(2)
                        .expect("H2 capacity update succeeds")
                );
                let second = second.await.expect("second waiter wakes");
                tokio::select! {
                    _ = &mut third => panic!("third stream admitted while two slots are reserved"),
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
                drop(first.reservation);
                let third = third.await.expect("third waiter wakes");
                first.session.is_same_session(&second.session)
                    && first.session.is_same_session(&third.session)
                    && pool.session_count() == 1
            })
        });
        assert!(reused);
    }

    #[test]
    fn h2_limit_decrease_recovers_capacity_after_transactions_detach() {
        let (server, certificate) = start_tls_server(&[b"h2"]);
        let address = server.address().expect("server has an address");
        let recovered = run_on_worker(move |context| {
            Box::pin(async move {
                let mut tls = TlsClientOptions::new("localhost");
                tls.add_root_certificate(certificate);
                tls.set_alpn_protocols([b"h2".as_slice()]);
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    pool_params(1),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(tls),
                        ..ConnectionParams::default()
                    },
                    SessionParams {
                        max_concurrent_outgoing_streams: Some(2),
                        ..SessionParams::default()
                    },
                );
                let first = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("first stream is reserved");
                let second = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("second stream is reserved");
                assert!(first.session.is_same_session(&second.session));
                assert!(
                    first
                        .session
                        .set_max_concurrent_outgoing_streams(1)
                        .expect("H2 limit reduction succeeds")
                );
                let mut waiter =
                    Box::pin(pool.get_session_with_reservation(CancellationToken::never()));
                poll_fn(|task| match waiter.as_mut().poll(task) {
                    Poll::Pending => Poll::Ready(()),
                    Poll::Ready(_) => panic!("waiter completed while the session was over limit"),
                })
                .await;
                drop(first.reservation);
                poll_fn(|task| match waiter.as_mut().poll(task) {
                    Poll::Pending => Poll::Ready(()),
                    Poll::Ready(_) => panic!("waiter completed while one slot remained occupied"),
                })
                .await;
                drop(second.reservation);
                let resumed = waiter
                    .await
                    .expect("detaching to below the limit wakes waiter");
                first.session.is_same_session(&resumed.session)
            })
        });
        assert!(recovered);
    }

    #[test]
    fn draining_session_is_replaced_for_waiter() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let replaced = run_on_worker(move |context| {
            Box::pin(async move {
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    pool_params(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                let first = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("first acquisition succeeds");
                let first_address = first.session.local_address().clone();
                first
                    .session
                    .initiate_drain()
                    .expect("session starts draining");
                drop(first.reservation);
                let second = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("replacement acquisition succeeds");
                first_address != *second.session.local_address()
            })
        });
        assert!(replaced);
    }

    #[test]
    fn last_failed_connect_releases_waiters() {
        let released = run_on_worker(|context| {
            Box::pin(async move {
                let attempts = Rc::new(Cell::new(0));
                let connector = Rc::new(RecordingConnector {
                    attempts: Rc::clone(&attempts),
                    parameter_keys: Rc::new(RefCell::new(Vec::new())),
                    session_caching: None,
                    fail_after: Some(0),
                    delay: Duration::from_millis(10),
                });
                let pool = HttpSessionPool::with_connector(
                    context,
                    SocketAddress::new("127.0.0.1", 1),
                    PoolParams {
                        connect_timeout: Duration::from_secs(5),
                        max_connection_attempts: 1,
                        ..pool_params(1)
                    },
                    ConnectionParams::default(),
                    SessionParams::default(),
                    connector,
                );
                let first = pool.get_session_with_reservation(CancellationToken::never());
                let second = pool.get_session_with_reservation(CancellationToken::never());
                let (first, second) = tokio::join!(first, second);
                matches!(first, Err(PoolError::ConnectFailed { .. }))
                    && matches!(second, Err(PoolError::ConnectFailed { .. }))
                    && attempts.get() == 1
            })
        });
        assert!(released);
    }

    struct ReentrantFailureObserver {
        pool: RefCell<Weak<HttpSessionPool>>,
        reentered: Cell<bool>,
        pending: Cell<i32>,
        acquisition: RefCell<Option<LocalClientFuture<Result<GetSessionResult, PoolError>>>>,
    }

    impl PoolObserver for ReentrantFailureObserver {
        fn on_pending_connections_changed(&self, delta: i32) {
            self.pending.set(self.pending.get().saturating_add(delta));
            if delta != -1 || self.reentered.replace(true) {
                return;
            }
            let pool = self
                .pool
                .borrow()
                .upgrade()
                .expect("pool remains alive during its observer callback");
            let mut acquisition = pool.get_session_with_reservation(CancellationToken::never());
            let mut context = Context::from_waker(Waker::noop());
            assert!(matches!(
                acquisition.as_mut().poll(&mut context),
                Poll::Pending
            ));
            self.acquisition.borrow_mut().replace(acquisition);
        }
    }

    #[test]
    fn final_connect_failure_cancels_original_waiter_before_reentrant_observer() {
        let released = run_on_worker(|context| {
            Box::pin(async move {
                let attempts = Rc::new(Cell::new(0));
                let connector = Rc::new(RecordingConnector {
                    attempts: Rc::clone(&attempts),
                    parameter_keys: Rc::new(RefCell::new(Vec::new())),
                    session_caching: None,
                    fail_after: Some(0),
                    delay: Duration::from_millis(10),
                });
                let pool = Rc::new(HttpSessionPool::with_connector(
                    context,
                    SocketAddress::new("127.0.0.1", 1),
                    PoolParams {
                        connect_timeout: Duration::from_secs(5),
                        max_connection_attempts: 1,
                        ..pool_params(1)
                    },
                    ConnectionParams::default(),
                    SessionParams::default(),
                    connector,
                ));
                let observer = Rc::new(ReentrantFailureObserver {
                    pool: RefCell::new(Rc::downgrade(&pool)),
                    reentered: Cell::new(false),
                    pending: Cell::new(0),
                    acquisition: RefCell::new(None),
                });
                pool.set_observer(Some(observer.clone()))
                    .expect("open pool accepts observer");

                let first = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await;
                let reentrant = observer
                    .acquisition
                    .borrow_mut()
                    .take()
                    .expect("failure observer starts a new acquisition");
                let second = reentrant.await;
                matches!(first, Err(PoolError::ConnectFailed { .. }))
                    && matches!(second, Err(PoolError::ConnectFailed { .. }))
                    && attempts.get() == 2
                    && observer.pending.get() == 0
            })
        });
        assert!(released);
    }

    #[test]
    fn drain_rejects_and_wakes_waiters() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let drained = run_on_worker(move |context| {
            Box::pin(async move {
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    pool_params(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                let _held = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("first acquisition succeeds");
                let pending = pool.get_session_with_reservation(CancellationToken::never());
                tokio::pin!(pending);
                tokio::select! {
                    _ = &mut pending => panic!("pending acquisition completed before drain"),
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
                assert!(pool.drain());
                matches!(pending.await, Err(PoolError::Draining))
            })
        });
        assert!(drained);
    }

    #[test]
    fn waiter_limit_rejects_newest_waiter() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let limited = run_on_worker(move |context| {
            Box::pin(async move {
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    PoolParams {
                        max_waiters: 1,
                        ..pool_params(1)
                    },
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                let _held = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("first acquisition succeeds");
                let waiting = pool.get_session_with_reservation(CancellationToken::never());
                tokio::pin!(waiting);
                tokio::select! {
                    _ = &mut waiting => panic!("first waiter completed while pool was full"),
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
                let newest = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await;
                matches!(newest, Err(PoolError::MaxWaiters))
            })
        });
        assert!(limited);
    }

    #[test]
    fn capacity_release_wakes_one_waiter_per_slot() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let acquired = run_on_worker(move |context| {
            Box::pin(async move {
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    pool_params(2),
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                let first = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("first acquisition succeeds");
                let second = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("second acquisition succeeds");
                pool.full() && first.session.local_address() != second.session.local_address()
            })
        });
        assert!(acquired);
    }

    #[test]
    fn pool_reports_security_owner_and_description() {
        let observed = run_on_worker(|context| {
            Box::pin(async move {
                let worker = context.worker_id();
                let plain = HttpSessionPool::new(
                    context.clone(),
                    SocketAddress::new("127.0.0.1", 80),
                    pool_params(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                let secure = HttpSessionPool::new(
                    context,
                    SocketAddress::new("127.0.0.1", 443),
                    pool_params(1),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(TlsClientOptions::new("localhost")),
                        ..ConnectionParams::default()
                    },
                    SessionParams::default(),
                );
                (
                    !plain.is_secure(),
                    secure.is_secure(),
                    plain.worker_id() == worker,
                    secure.describe().contains("secure=true"),
                )
            })
        });
        assert_eq!(observed, (true, true, true, true));
    }

    #[test]
    fn two_h2_sessions_release_one_waiter_each() {
        let (server, certificate) = start_tls_server(&[b"h2"]);
        let address = server.address().expect("server has an address");
        let ordered = run_on_worker(move |context| {
            Box::pin(async move {
                let mut tls = TlsClientOptions::new("localhost");
                tls.add_root_certificate(certificate);
                tls.set_alpn_protocols([b"h2".as_slice()]);
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    pool_params(2),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(tls),
                        ..ConnectionParams::default()
                    },
                    SessionParams {
                        max_concurrent_outgoing_streams: Some(1),
                        ..SessionParams::default()
                    },
                );
                let first = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("first H2 session is acquired");
                let second = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("second H2 session is acquired");
                assert_ne!(
                    first.session.local_address(),
                    second.session.local_address()
                );

                let third = pool.get_session_with_reservation(CancellationToken::never());
                let fourth = pool.get_session_with_reservation(CancellationToken::never());
                tokio::pin!(third);
                tokio::pin!(fourth);
                tokio::select! {
                    _ = &mut third => panic!("third acquisition completed while both streams were reserved"),
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
                tokio::select! {
                    _ = &mut fourth => panic!("fourth acquisition completed while both streams were reserved"),
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }

                drop(first.reservation);
                let third = tokio::time::timeout(Duration::from_secs(1), &mut third)
                    .await
                    .expect("one released stream wakes one waiter")
                    .expect("third acquisition succeeds");
                tokio::select! {
                    _ = &mut fourth => panic!("one released stream woke two waiters"),
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
                drop(second.reservation);
                let fourth = fourth
                    .await
                    .expect("second released stream wakes next waiter");
                third.session.local_address() != fourth.session.local_address()
            })
        });
        assert!(ordered);
    }

    #[test]
    fn expired_session_is_replaced() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let replaced = run_on_worker(move |context| {
            Box::pin(async move {
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    PoolParams {
                        max_age: Duration::from_millis(1),
                        ..pool_params(1)
                    },
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                let first = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("first acquisition succeeds");
                let first_address = first.session.local_address().clone();
                drop(first.reservation);
                tokio::time::sleep(Duration::from_millis(5)).await;
                let second = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("replacement acquisition succeeds");
                first_address != *second.session.local_address()
            })
        });
        assert!(replaced);
    }

    #[test]
    fn active_expired_session_is_replaced_only_after_release() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let replaced = run_on_worker(move |context| {
            Box::pin(async move {
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    PoolParams {
                        max_age: Duration::from_millis(1),
                        ..pool_params(1)
                    },
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                let first = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("first acquisition succeeds");
                let first_address = first.session.local_address().clone();
                tokio::time::sleep(Duration::from_millis(5)).await;
                assert!(!first.session.is_draining());
                let second = pool.get_session_with_reservation(CancellationToken::never());
                tokio::pin!(second);
                tokio::select! {
                    _ = &mut second => panic!("expired active session was replaced before release"),
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
                drop(first.reservation);
                let second = second.await.expect("replacement acquisition succeeds");
                first_address != *second.session.local_address()
            })
        });
        assert!(replaced);
    }

    #[test]
    fn disabled_pooling_drains_each_released_session() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let empty = run_on_worker(move |context| {
            Box::pin(async move {
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    pool_params(0),
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                let acquired = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("acquisition succeeds");
                drop(acquired.reservation);
                tokio::task::yield_now().await;
                pool.session_count() == 0
            })
        });
        assert!(empty);
    }

    #[test]
    fn disabling_a_full_pool_drains_the_session_when_released() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let drained = run_on_worker(move |context| {
            Box::pin(async move {
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    pool_params(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                let acquired = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("acquisition succeeds");
                assert!(pool.full());
                pool.set_pooling_enabled(false)
                    .expect("open pool can disable pooling");
                drop(acquired.reservation);
                tokio::task::yield_now().await;
                pool.session_count() == 0
            })
        });
        assert!(drained);
    }

    #[test]
    fn disabling_pooling_drains_h2_after_one_of_two_streams_detaches() {
        let (server, certificate) = start_tls_server(&[b"h2"]);
        let address = server.address().expect("server has an address");
        let replaced = run_on_worker(move |context| {
            Box::pin(async move {
                let mut tls = TlsClientOptions::new("localhost");
                tls.add_root_certificate(certificate);
                tls.set_alpn_protocols([b"h2".as_slice()]);
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    pool_params(1),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(tls),
                        ..ConnectionParams::default()
                    },
                    SessionParams {
                        max_concurrent_outgoing_streams: Some(2),
                        ..SessionParams::default()
                    },
                );
                let first = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("first H2 acquisition succeeds");
                let second = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("second H2 acquisition succeeds");
                assert!(first.session.is_same_session(&second.session));
                pool.set_pooling_enabled(false)
                    .expect("open pool can disable pooling");
                drop(first.reservation);
                assert!(second.session.is_draining());
                assert_eq!(pool.session_count(), 0);

                let replacement = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("detached stream permits a replacement connection");
                let replaced = !second.session.is_same_session(&replacement.session);
                drop(second.reservation);
                drop(replacement.reservation);
                replaced
            })
        });
        assert!(replaced);
    }

    #[test]
    fn drop_cancels_pending_acquisition() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let cancelled = run_on_worker(move |context| {
            Box::pin(async move {
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    pool_params(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                let _held = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("first acquisition succeeds");
                let pending = pool.get_session_with_reservation(CancellationToken::never());
                tokio::pin!(pending);
                tokio::select! {
                    _ = &mut pending => panic!("pending acquisition completed before pool drop"),
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
                drop(pool);
                matches!(pending.await, Err(PoolError::Draining))
            })
        });
        assert!(cancelled);
    }

    #[test]
    fn last_failed_connect_preserves_existing_capacity() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let preserved = run_on_worker(move |context| {
            Box::pin(async move {
                let attempts = Rc::new(Cell::new(0));
                let connector = Rc::new(RecordingConnector {
                    attempts: Rc::clone(&attempts),
                    parameter_keys: Rc::new(RefCell::new(Vec::new())),
                    session_caching: None,
                    fail_after: Some(1),
                    delay: Duration::ZERO,
                });
                let pool = HttpSessionPool::with_connector(
                    context,
                    socket_address(address),
                    PoolParams {
                        max_connection_attempts: 1,
                        ..pool_params(1)
                    },
                    ConnectionParams::default(),
                    SessionParams::default(),
                    connector,
                );
                let first = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("first acquisition succeeds");
                let first_address = first.session.local_address().clone();
                let second = pool.get_session_with_reservation(CancellationToken::never());
                tokio::pin!(second);
                tokio::select! {
                    _ = &mut second => panic!("second acquisition completed while pool was full"),
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
                pool.set_max_connections(2)
                    .expect("open pool accepts a new connection limit");
                let third = pool.get_session_with_reservation(CancellationToken::never());
                tokio::pin!(third);
                tokio::select! {
                    _ = &mut third => panic!("failed extra connect completed a waiter"),
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
                drop(first.reservation);
                let second = second.await.expect("oldest waiter receives capacity");
                let second_reused = first_address == *second.session.local_address();
                drop(second.reservation);
                let third = third.await.expect("next waiter receives capacity");
                second_reused
                    && first_address == *third.session.local_address()
                    && attempts.get() == 2
            })
        });
        assert!(preserved);
    }

    struct IdleCounter(Rc<Cell<u32>>);

    struct IdleSnapshots(Rc<RefCell<Vec<bool>>>);

    impl IdleSessionObserver for IdleCounter {
        fn on_idle_sessions_changed(&self, _pool: &HttpSessionPool) {
            self.0.set(self.0.get().saturating_add(1));
        }
    }

    impl IdleSessionObserver for IdleSnapshots {
        fn on_idle_sessions_changed(&self, pool: &HttpSessionPool) {
            self.0.borrow_mut().push(pool.has_idle_sessions());
        }
    }

    struct ReentrantIdleObserver {
        calls: Rc<Cell<u32>>,
        saw_idle: Rc<Cell<bool>>,
    }

    struct DrainOrderObserver {
        label: &'static str,
        order: Rc<RefCell<Vec<&'static str>>>,
        pool: Rc<RefCell<Weak<HttpSessionPool>>>,
        remaining: Rc<RefCell<Vec<usize>>>,
    }

    struct ReentrantLifecyclePoolObserver {
        pool: Rc<RefCell<Weak<HttpSessionPool>>>,
        saw_detached_available: Rc<Cell<bool>>,
        saw_deactivate_idle: Rc<Cell<bool>>,
        detached_on_deactivate: Rc<Cell<bool>>,
    }

    struct CaptureLifecycleRegistration(Rc<RefCell<Option<LifecycleObserverRegistration>>>);

    struct DropReservationOnDetach(RefCell<Option<RequestReservation>>);

    struct PanickingDrainIdleObserver {
        calls: Rc<Cell<u32>>,
    }

    impl IdleSessionObserver for PanickingDrainIdleObserver {
        fn on_idle_sessions_changed(&self, pool: &HttpSessionPool) {
            self.calls.set(self.calls.get().saturating_add(1));
            if !pool.has_idle_sessions() {
                panic!("injected idle observer panic during drain");
            }
        }
    }

    impl IdleSessionObserver for ReentrantIdleObserver {
        fn on_idle_sessions_changed(&self, pool: &HttpSessionPool) {
            self.calls.set(self.calls.get().saturating_add(1));
            self.saw_idle.set(pool.has_idle_sessions());
        }
    }

    impl roxygen_session::session::LifecycleObserver for DrainOrderObserver {
        fn on_event(&self, event: roxygen_session::session::LifecycleEvent) {
            if matches!(
                event,
                roxygen_session::session::LifecycleEvent::DrainStarted(_)
            ) {
                self.order.borrow_mut().push(self.label);
                if let Some(pool) = self.pool.borrow().upgrade() {
                    self.remaining.borrow_mut().push(pool.session_count());
                }
            }
        }
    }

    impl LifecycleObserver for ReentrantLifecyclePoolObserver {
        fn on_event(&self, event: LifecycleEvent) {
            let Some(pool) = self.pool.borrow().upgrade() else {
                return;
            };
            match event {
                LifecycleEvent::TransactionDetached { .. } => {
                    self.saw_detached_available
                        .set(pool.has_available_sessions());
                }
                LifecycleEvent::DeactivateConnection => {
                    self.saw_deactivate_idle.set(pool.has_idle_sessions());
                    self.detached_on_deactivate
                        .set(pool.detach_idle_session().is_some());
                }
                _ => {}
            }
        }
    }

    impl LifecycleObserver for CaptureLifecycleRegistration {
        fn on_attached_with_registration(&self, registration: LifecycleObserverRegistration) {
            self.0.replace(Some(registration));
        }
    }

    impl LifecycleObserver for DropReservationOnDetach {
        fn on_event(&self, event: LifecycleEvent) {
            if matches!(event, LifecycleEvent::TransactionDetached { .. }) {
                self.0.borrow_mut().take();
            }
        }
    }

    struct PanickingPoolObserver;

    impl PoolObserver for PanickingPoolObserver {
        fn on_pending_connections_changed(&self, _delta: i32) {
            panic!("injected observer panic");
        }
    }

    #[test]
    fn idle_sessions_stay_pinned_to_the_owner_worker() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let idle = run_on_worker(move |context| {
            Box::pin(async move {
                let worker = context.worker_id();
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    pool_params(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                let notifications = Rc::new(Cell::new(0));
                pool.set_idle_session_observer(Some(Rc::new(IdleCounter(Rc::clone(
                    &notifications,
                )))))
                .expect("open pool accepts idle observer");
                let acquired = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("acquisition succeeds");
                let owned = acquired.session.worker_id() == worker;
                let url = Url::parse(
                    format!("http://localhost:{}/", address.port()),
                    false,
                    UrlMode::StrictCompat,
                );
                HttpClient::request(
                    &acquired.session,
                    Some(acquired.reservation),
                    RequestSpec::get(url),
                )
                .await
                .expect("request completes");
                tokio::task::yield_now().await;
                owned && pool.has_idle_sessions() && notifications.get() > 0
            })
        });
        assert!(idle);
    }

    #[test]
    fn detached_h2_sessions_reinsert_and_consolidate_new_streams() {
        let (server, certificate) = start_tls_server(&[b"h2"]);
        let address = server.address().expect("server has an address");
        let consolidated = run_on_worker(move |context| {
            Box::pin(async move {
                let mut tls = TlsClientOptions::new("localhost");
                tls.add_root_certificate(certificate);
                tls.set_alpn_protocols([b"h2".as_slice()]);
                let connection = ConnectionParams {
                    transport: ClientTransportOptions::Tls(tls),
                    ..ConnectionParams::default()
                };
                let session_params = SessionParams {
                    max_concurrent_outgoing_streams: Some(2),
                    ..SessionParams::default()
                };
                let first_pool = HttpSessionPool::new(
                    context.clone(),
                    socket_address(address),
                    pool_params(1),
                    connection.clone(),
                    session_params.clone(),
                );
                let second_pool = HttpSessionPool::new(
                    context.clone(),
                    socket_address(address),
                    pool_params(1),
                    connection.clone(),
                    session_params.clone(),
                );
                let target_pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    pool_params(2),
                    connection,
                    session_params,
                );
                let url = Url::parse(
                    format!("https://localhost:{}/", address.port()),
                    false,
                    UrlMode::StrictCompat,
                );
                for pool in [&first_pool, &second_pool] {
                    let acquired = pool
                        .get_session_with_reservation(CancellationToken::never())
                        .await
                        .expect("source pool acquisition succeeds");
                    HttpClient::request(
                        &acquired.session,
                        Some(acquired.reservation),
                        RequestSpec::get(url.clone()),
                    )
                    .await
                    .expect("source pool request completes");
                }
                tokio::task::yield_now().await;
                let first = first_pool
                    .detach_idle_session()
                    .expect("first idle session detaches");
                let second = second_pool
                    .detach_idle_session()
                    .expect("second idle session detaches");
                target_pool
                    .insert_idle_session(first)
                    .expect("first detached session inserts");
                target_pool
                    .insert_idle_session(second)
                    .expect("second detached session inserts");
                let first = target_pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("first target acquisition succeeds");
                let second = target_pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("second target acquisition succeeds");
                first.session.is_same_session(&second.session)
                    && first_pool.session_count() == 0
                    && second_pool.session_count() == 0
                    && target_pool.session_count() == 2
            })
        });
        assert!(consolidated);
    }

    #[test]
    fn reinserted_session_retains_its_original_age() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let replaced = run_on_worker(move |context| {
            Box::pin(async move {
                let source_pool = HttpSessionPool::new(
                    context.clone(),
                    socket_address(address),
                    pool_params(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                let acquired = source_pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("source acquisition succeeds");
                let old_address = acquired.session.local_address().clone();
                let url = Url::parse(
                    format!("http://localhost:{}/", address.port()),
                    false,
                    UrlMode::StrictCompat,
                );
                HttpClient::request(
                    &acquired.session,
                    Some(acquired.reservation),
                    RequestSpec::get(url),
                )
                .await
                .expect("source request completes");
                tokio::time::sleep(Duration::from_millis(10)).await;
                let detached = source_pool
                    .detach_idle_session()
                    .expect("old session detaches");
                let target_pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    PoolParams {
                        max_age: Duration::from_millis(1),
                        ..pool_params(1)
                    },
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                target_pool
                    .insert_idle_session(detached)
                    .expect("old session inserts");
                let replacement = target_pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("expired inserted session is replaced");
                old_address != *replacement.session.local_address()
            })
        });
        assert!(replaced);
    }

    #[test]
    fn draining_detached_session_cannot_be_reinserted() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let rejected = run_on_worker(move |context| {
            Box::pin(async move {
                let source_pool = HttpSessionPool::new(
                    context.clone(),
                    socket_address(address),
                    pool_params(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                let acquired = source_pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("source acquisition succeeds");
                let url = Url::parse(
                    format!("http://localhost:{}/", address.port()),
                    false,
                    UrlMode::StrictCompat,
                );
                HttpClient::request(
                    &acquired.session,
                    Some(acquired.reservation),
                    RequestSpec::get(url),
                )
                .await
                .expect("source request completes");
                tokio::task::yield_now().await;
                let detached = source_pool
                    .detach_idle_session()
                    .expect("idle session detaches");
                detached
                    .session()
                    .initiate_drain()
                    .expect("detached session starts draining");
                let target_pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    pool_params(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                matches!(
                    target_pool.insert_idle_session(detached),
                    Err(PoolError::InvalidSessionState)
                )
            })
        });
        assert!(rejected);
    }

    #[test]
    fn idle_observer_can_reenter_the_pool() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let observed = run_on_worker(move |context| {
            Box::pin(async move {
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    pool_params(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                let calls = Rc::new(Cell::new(0));
                let saw_idle = Rc::new(Cell::new(false));
                pool.set_idle_session_observer(Some(Rc::new(ReentrantIdleObserver {
                    calls: Rc::clone(&calls),
                    saw_idle: Rc::clone(&saw_idle),
                })))
                .expect("open pool accepts reentrant observer");
                let acquired = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("acquisition succeeds");
                let url = Url::parse(
                    format!("http://localhost:{}/", address.port()),
                    false,
                    UrlMode::StrictCompat,
                );
                HttpClient::request(
                    &acquired.session,
                    Some(acquired.reservation),
                    RequestSpec::get(url),
                )
                .await
                .expect("request completes");
                tokio::task::yield_now().await;
                (calls.get(), saw_idle.get())
            })
        });
        assert!(observed.0 > 0);
        assert!(observed.1);
    }

    #[test]
    fn drain_suppresses_idle_observer_after_entering_draining_state() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let observed = run_on_worker(move |context| {
            Box::pin(async move {
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    pool_params(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                let calls = Rc::new(Cell::new(0));
                pool.set_idle_session_observer(Some(Rc::new(PanickingDrainIdleObserver {
                    calls: Rc::clone(&calls),
                })))
                .expect("open pool accepts drain observer");
                let acquired = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("acquisition succeeds");
                let url = Url::parse(
                    format!("http://localhost:{}/", address.port()),
                    false,
                    UrlMode::StrictCompat,
                );
                HttpClient::request(
                    &acquired.session,
                    Some(acquired.reservation),
                    RequestSpec::get(url),
                )
                .await
                .expect("request completes");
                tokio::time::timeout(Duration::from_secs(1), async {
                    while !pool.has_idle_sessions() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("session becomes idle");
                let before = calls.get();
                let drained = pool.drain();
                (drained, before, calls.get(), pool.has_idle_sessions())
            })
        });
        assert!(observed.0);
        assert_eq!(observed.2, observed.1);
        assert!(!observed.3);
    }

    #[test]
    fn flush_notifies_once_for_each_idle_session_removed() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let snapshots = run_on_worker(move |context| {
            Box::pin(async move {
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    pool_params(2),
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                let snapshots = Rc::new(RefCell::new(Vec::new()));
                pool.set_idle_session_observer(Some(Rc::new(IdleSnapshots(Rc::clone(&snapshots)))))
                    .expect("open pool accepts idle observer");
                let first = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("first acquisition succeeds");
                let second = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("second acquisition succeeds");
                let first_url = Url::parse(
                    format!("http://localhost:{}/", address.port()),
                    false,
                    UrlMode::StrictCompat,
                );
                let second_url = first_url.clone();
                let first_request = HttpClient::request(
                    &first.session,
                    Some(first.reservation),
                    RequestSpec::get(first_url),
                );
                let second_request = HttpClient::request(
                    &second.session,
                    Some(second.reservation),
                    RequestSpec::get(second_url),
                );
                let (first_result, second_result) = tokio::join!(first_request, second_request);
                first_result.expect("first request completes");
                second_result.expect("second request completes");
                tokio::time::timeout(Duration::from_secs(1), async {
                    while snapshots.borrow().len() < 2 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("both sessions become idle");
                let before = snapshots.borrow().len();
                pool.flush();
                snapshots.borrow()[before..].to_vec()
            })
        });
        assert_eq!(snapshots, [true, false]);
    }

    #[test]
    fn panicking_pool_observer_does_not_leak_connection_capacity() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let acquired = run_on_worker(move |context| {
            Box::pin(async move {
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    pool_params(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                pool.set_observer(Some(Rc::new(PanickingPoolObserver)))
                    .expect("open pool accepts observer");
                let first = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("observer panic does not abort connection");
                drop(first.reservation);
                pool.get_session_with_reservation(CancellationToken::never())
                    .await
                    .is_ok()
            })
        });
        assert!(acquired);
    }

    #[test]
    fn flush_drains_sessions_and_wakes_waiters() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let flushed = run_on_worker(move |context| {
            Box::pin(async move {
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    pool_params(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                let _held = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("first acquisition succeeds");
                let pending = pool.get_session_with_reservation(CancellationToken::never());
                tokio::pin!(pending);
                tokio::select! {
                    _ = &mut pending => panic!("pending acquisition completed before flush"),
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
                pool.flush();
                matches!(pending.await, Err(PoolError::Draining)) && pool.session_count() == 0
            })
        });
        assert!(flushed);
    }

    #[test]
    fn drain_is_reentrant_and_orders_full_available_then_idle_sessions() {
        let (plain_server, _) = start_plain_server();
        let plain_address = plain_server.address().expect("server has an address");
        let (tls_server, certificate) = start_tls_server(&[b"h2"]);
        let tls_address = tls_server.address().expect("server has an address");
        let observed = run_on_worker(move |context| {
            Box::pin(async move {
                let order = Rc::new(RefCell::new(Vec::new()));
                let remaining = Rc::new(RefCell::new(Vec::new()));
                let pool_slot = Rc::new(RefCell::new(Weak::new()));
                let idle = HttpConnector::connect(
                    &context,
                    socket_address(plain_address),
                    Duration::from_secs(1),
                    ConnectionParams::default(),
                    SessionParams {
                        lifecycle_observer: Some(Rc::new(DrainOrderObserver {
                            label: "idle",
                            order: Rc::clone(&order),
                            pool: Rc::clone(&pool_slot),
                            remaining: Rc::clone(&remaining),
                        })),
                        ..SessionParams::default()
                    },
                    CancellationToken::never(),
                )
                .await
                .expect("idle HTTP/1 session connects");
                let url = Url::parse(
                    format!("http://localhost:{}/", plain_address.port()),
                    false,
                    UrlMode::StrictCompat,
                );
                HttpClient::request(&idle, None, RequestSpec::get(url))
                    .await
                    .expect("HTTP/1 request makes the session idle");
                tokio::task::yield_now().await;
                assert!(idle.is_idle());

                let mut tls = TlsClientOptions::new("localhost");
                tls.add_root_certificate(certificate);
                tls.set_alpn_protocols([b"h2".as_slice()]);
                let available = HttpConnector::connect(
                    &context,
                    socket_address(tls_address),
                    Duration::from_secs(1),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(tls.clone()),
                        ..ConnectionParams::default()
                    },
                    SessionParams {
                        max_concurrent_outgoing_streams: Some(2),
                        lifecycle_observer: Some(Rc::new(DrainOrderObserver {
                            label: "available",
                            order: Rc::clone(&order),
                            pool: Rc::clone(&pool_slot),
                            remaining: Rc::clone(&remaining),
                        })),
                        ..SessionParams::default()
                    },
                    CancellationToken::never(),
                )
                .await
                .expect("available HTTP/2 session connects");
                let available_reservation = available
                    .reserve_request()
                    .expect("one of two slots is reserved");

                let full = HttpConnector::connect(
                    &context,
                    socket_address(tls_address),
                    Duration::from_secs(1),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(tls),
                        ..ConnectionParams::default()
                    },
                    SessionParams {
                        max_concurrent_outgoing_streams: Some(1),
                        lifecycle_observer: Some(Rc::new(DrainOrderObserver {
                            label: "full",
                            order: Rc::clone(&order),
                            pool: Rc::clone(&pool_slot),
                            remaining: Rc::clone(&remaining),
                        })),
                        ..SessionParams::default()
                    },
                    CancellationToken::never(),
                )
                .await
                .expect("full HTTP/2 session connects");
                let full_reservation = full
                    .reserve_request()
                    .expect("only stream slot is reserved");

                let pool = Rc::new(HttpSessionPool::new(
                    context,
                    socket_address(plain_address),
                    pool_params(3),
                    ConnectionParams::default(),
                    SessionParams::default(),
                ));
                pool_slot.replace(Rc::downgrade(&pool));
                pool.insert_session_for_teardown_test(idle);
                pool.insert_session_for_teardown_test(available);
                pool.insert_session_for_teardown_test(full);
                assert!(pool.drain());
                drop(available_reservation);
                drop(full_reservation);
                (order.borrow().clone(), remaining.borrow().clone())
            })
        });
        assert_eq!(observed.0, ["full", "available", "idle"]);
        assert_eq!(observed.1, [2, 1, 0]);
    }

    #[test]
    fn h2_full_transitions_preserve_fifo_teardown_order() {
        let (server, certificate) = start_tls_server(&[b"h2"]);
        let address = server.address().expect("server has an address");
        let observed = run_on_worker(move |context| {
            Box::pin(async move {
                let order = Rc::new(RefCell::new(Vec::new()));
                let remaining = Rc::new(RefCell::new(Vec::new()));
                let pool_slot = Rc::new(RefCell::new(Weak::new()));
                let pool = Rc::new(HttpSessionPool::new(
                    context.clone(),
                    socket_address(address),
                    pool_params(3),
                    ConnectionParams::default(),
                    SessionParams::default(),
                ));
                pool_slot.replace(Rc::downgrade(&pool));

                let mut tls = TlsClientOptions::new("localhost");
                tls.add_root_certificate(certificate);
                tls.set_alpn_protocols([b"h2".as_slice()]);
                let first = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(tls.clone()),
                        ..ConnectionParams::default()
                    },
                    SessionParams {
                        max_concurrent_outgoing_streams: Some(1),
                        lifecycle_observer: Some(Rc::new(DrainOrderObserver {
                            label: "first",
                            order: Rc::clone(&order),
                            pool: Rc::clone(&pool_slot),
                            remaining: Rc::clone(&remaining),
                        })),
                        ..SessionParams::default()
                    },
                    CancellationToken::never(),
                )
                .await
                .expect("first HTTP/2 session connects");
                let second = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(tls.clone()),
                        ..ConnectionParams::default()
                    },
                    SessionParams {
                        max_concurrent_outgoing_streams: Some(1),
                        lifecycle_observer: Some(Rc::new(DrainOrderObserver {
                            label: "second",
                            order: Rc::clone(&order),
                            pool: Rc::clone(&pool_slot),
                            remaining: Rc::clone(&remaining),
                        })),
                        ..SessionParams::default()
                    },
                    CancellationToken::never(),
                )
                .await
                .expect("second HTTP/2 session connects");
                let settings_limited = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(tls),
                        ..ConnectionParams::default()
                    },
                    SessionParams {
                        max_concurrent_outgoing_streams: Some(2),
                        lifecycle_observer: Some(Rc::new(DrainOrderObserver {
                            label: "settings",
                            order: Rc::clone(&order),
                            pool: Rc::clone(&pool_slot),
                            remaining: Rc::clone(&remaining),
                        })),
                        ..SessionParams::default()
                    },
                    CancellationToken::never(),
                )
                .await
                .expect("settings-limited HTTP/2 session connects");
                pool.insert_session_for_teardown_test(first.clone());
                pool.insert_session_for_teardown_test(second.clone());
                pool.insert_session_for_teardown_test(settings_limited.clone());

                let second_reservation = second
                    .reserve_request()
                    .expect("second session becomes full first");
                let first_reservation = first
                    .reserve_request()
                    .expect("first session becomes full second");
                let settings_reservation = settings_limited
                    .reserve_request()
                    .expect("settings session retains one slot");
                assert!(
                    settings_limited
                        .set_max_concurrent_outgoing_streams(1)
                        .expect("stream limit update succeeds")
                );

                assert!(pool.drain());
                drop(second_reservation);
                drop(first_reservation);
                drop(settings_reservation);
                (order.borrow().clone(), remaining.borrow().clone())
            })
        });
        assert_eq!(observed.0, ["second", "first", "settings"]);
        assert_eq!(observed.1, [2, 1, 0]);
    }

    #[test]
    fn external_lifecycle_observer_sees_pool_updates_before_callbacks() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let observed = run_on_worker(move |context| {
            Box::pin(async move {
                let pool_slot = Rc::new(RefCell::new(Weak::new()));
                let saw_detached_available = Rc::new(Cell::new(false));
                let saw_deactivate_idle = Rc::new(Cell::new(false));
                let detached_on_deactivate = Rc::new(Cell::new(false));
                let pool = Rc::new(HttpSessionPool::new(
                    context,
                    socket_address(address),
                    pool_params(1),
                    ConnectionParams::default(),
                    SessionParams {
                        lifecycle_observer: Some(Rc::new(ReentrantLifecyclePoolObserver {
                            pool: Rc::clone(&pool_slot),
                            saw_detached_available: Rc::clone(&saw_detached_available),
                            saw_deactivate_idle: Rc::clone(&saw_deactivate_idle),
                            detached_on_deactivate: Rc::clone(&detached_on_deactivate),
                        })),
                        ..SessionParams::default()
                    },
                ));
                pool_slot.replace(Rc::downgrade(&pool));
                let acquired = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("acquisition succeeds");
                let url = Url::parse(
                    format!("http://localhost:{}/", address.port()),
                    false,
                    UrlMode::StrictCompat,
                );
                HttpClient::request(
                    &acquired.session,
                    Some(acquired.reservation),
                    RequestSpec::get(url),
                )
                .await
                .expect("request completes");
                tokio::task::yield_now().await;
                (
                    saw_detached_available.get(),
                    saw_deactivate_idle.get(),
                    detached_on_deactivate.get(),
                    pool.session_count(),
                )
            })
        });
        assert_eq!(observed, (true, true, true, 0));
    }

    #[test]
    fn batched_h2_detaches_requeue_full_session_once() {
        let (server, certificate) = start_tls_server(&[b"h2"]);
        let address = server.address().expect("server has an address");
        let reused_other = run_on_worker(move |context| {
            Box::pin(async move {
                let pool = HttpSessionPool::new(
                    context.clone(),
                    socket_address(address),
                    pool_params(2),
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                let registration = Rc::new(RefCell::new(None));
                let mut tls = TlsClientOptions::new("localhost");
                tls.add_root_certificate(certificate);
                tls.set_alpn_protocols([b"h2".as_slice()]);
                let full = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(tls.clone()),
                        ..ConnectionParams::default()
                    },
                    SessionParams {
                        max_concurrent_outgoing_streams: Some(3),
                        lifecycle_observer: Some(Rc::new(CaptureLifecycleRegistration(Rc::clone(
                            &registration,
                        )))),
                        ..SessionParams::default()
                    },
                    CancellationToken::never(),
                )
                .await
                .expect("full candidate connects");
                pool.insert_session_for_teardown_test(full.clone());
                let first = full.reserve_request().expect("first slot is reserved");
                let second = full.reserve_request().expect("second slot is reserved");
                let third = full.reserve_request().expect("third slot is reserved");

                let available = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(tls),
                        ..ConnectionParams::default()
                    },
                    SessionParams {
                        max_concurrent_outgoing_streams: Some(3),
                        ..SessionParams::default()
                    },
                    CancellationToken::never(),
                )
                .await
                .expect("available session connects");
                let available_address = available.local_address().clone();
                pool.insert_session_for_teardown_test(available);

                registration
                    .borrow()
                    .as_ref()
                    .expect("lifecycle observer captures its registration")
                    .add_observer(Rc::new(DropReservationOnDetach(RefCell::new(Some(second)))));
                drop(first);
                let selected = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("an available session is selected");
                let reused_other = *selected.session.local_address() == available_address;
                drop(selected.reservation);
                drop(third);
                reused_other
            })
        });
        assert!(reused_other);
    }

    struct PendingCounter(Rc<Cell<i32>>);

    impl PoolObserver for PendingCounter {
        fn on_pending_connections_changed(&self, delta: i32) {
            self.0.set(self.0.get().saturating_add(delta));
        }
    }

    #[test]
    fn reload_callback_updates_future_connections() {
        let (server, certificate) = start_tls_server(&[b"http/1.1"]);
        let address = server.address().expect("server has an address");
        let events = Arc::new(Mutex::new(Vec::new()));
        let initial_events = Arc::clone(&events);
        let callback_events = Arc::clone(&events);
        let reloaded = run_on_worker(move |context| {
            Box::pin(async move {
                let mut initial_tls = TlsClientOptions::new("localhost");
                initial_tls.add_root_certificate(certificate.clone());
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    pool_params(1),
                    ConnectionParams {
                        transport: ClientTransportOptions::Tls(initial_tls),
                        tls_stats: Some(Arc::new(RecordingTlsStats(initial_events))),
                        ..ConnectionParams::default()
                    },
                    SessionParams::default(),
                );
                let pending = Rc::new(Cell::new(0));
                pool.set_observer(Some(Rc::new(PendingCounter(Rc::clone(&pending)))))
                    .expect("open pool accepts observer");
                let reloads = Rc::new(Cell::<u32>::new(0));
                let callback_reloads = Rc::clone(&reloads);
                let reload_events = Arc::clone(&callback_events);
                pool.set_reload_callback(
                    move |pool| {
                        callback_reloads.set(callback_reloads.get().saturating_add(1));
                        let mut tls = TlsClientOptions::new("localhost");
                        tls.add_root_certificate(certificate.clone());
                        pool.set_connection_params(ConnectionParams {
                            transport: ClientTransportOptions::Tls(tls),
                            tls_stats: Some(Arc::new(RecordingTlsStats(Arc::clone(
                                &reload_events,
                            )))),
                            ..ConnectionParams::default()
                        })
                        .expect("reload callback updates TLS connection parameters");
                    },
                    Duration::from_millis(5),
                )
                .expect("open pool accepts reload callback");
                let first = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("first acquisition succeeds");
                let url = Url::parse(
                    format!("https://localhost:{}/", address.port()),
                    false,
                    UrlMode::StrictCompat,
                );
                HttpClient::request(
                    &first.session,
                    Some(first.reservation),
                    RequestSpec::get(url),
                )
                .await
                .expect("first TLS request completes");
                pool.flush();
                tokio::time::sleep(Duration::from_millis(15)).await;
                let second = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await;
                second.is_ok() && reloads.get() > 0 && pending.get() == 0
            })
        });
        assert!(reloaded);
        assert_eq!(
            *events.lock().expect("TLS stats lock is available"),
            [
                TlsStatEvent::Connection(true),
                TlsStatEvent::Connection(true)
            ]
        );
    }

    #[test]
    fn replaced_reload_callback_cannot_commit_stale_parameters() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let stable = run_on_worker(move |context| {
            Box::pin(async move {
                let keys = Rc::new(RefCell::new(Vec::new()));
                let connector = Rc::new(RecordingConnector {
                    attempts: Rc::new(Cell::new(0)),
                    parameter_keys: Rc::clone(&keys),
                    session_caching: None,
                    fail_after: None,
                    delay: Duration::ZERO,
                });
                let pool = Rc::new(HttpSessionPool::with_connector(
                    context,
                    socket_address(address),
                    pool_params(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                    connector,
                ));
                let first = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("first acquisition succeeds");
                drop(first.reservation);
                pool.flush();

                let weak_pool = Rc::downgrade(&pool);
                pool.set_reload_callback(
                    move |pool| {
                        assert!(weak_pool.upgrade().is_some());
                        pool.set_reload_callback(|_| {}, Duration::from_secs(60))
                            .expect("open pool replaces reload callback");
                    },
                    Duration::from_millis(1),
                )
                .expect("open pool accepts reload callback");
                tokio::time::sleep(Duration::from_millis(10)).await;
                let second = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("second acquisition succeeds");
                drop(second.reservation);
                let keys = keys.borrow();
                keys.len() == 2 && keys[0] == keys[1]
            })
        });
        assert!(stable);
    }

    #[test]
    fn cancelled_waiter_is_removed() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let removed = run_on_worker(move |context| {
            Box::pin(async move {
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    pool_params(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                let held = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("first acquisition succeeds");
                let cancellation = CancellationSource::new();
                let pending = pool.get_session_with_reservation(cancellation.token());
                tokio::pin!(pending);
                tokio::select! {
                    _ = &mut pending => panic!("pending acquisition completed before cancellation"),
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
                cancellation.request_cancellation();
                let cancelled = matches!(pending.await, Err(PoolError::Cancelled));
                drop(held.reservation);
                cancelled
                    && pool
                        .get_session_with_reservation(CancellationToken::never())
                        .await
                        .is_ok()
            })
        });
        assert!(removed);
    }

    #[test]
    fn cancelling_requester_does_not_cancel_pool_owned_connect() {
        use tokio::sync::oneshot;

        struct GatedConnector {
            inner: RecordingConnector,
            gate: RefCell<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
        }

        impl SessionConnector for GatedConnector {
            fn connect(
                &self,
                context: WorkerContext,
                address: SocketAddress,
                timeout: Duration,
                connection: ConnectionParams,
                session: SessionParams,
                cancellation: CancellationToken,
            ) -> LocalClientFuture<Result<ClientSession, HttpError>> {
                let (started, release) = self
                    .gate
                    .borrow_mut()
                    .take()
                    .expect("one connection attempt");
                let connecting = self.inner.connect(
                    context,
                    address,
                    timeout,
                    connection,
                    session,
                    cancellation,
                );
                Box::pin(async move {
                    let _ = started.send(());
                    let _ = release.await;
                    connecting.await
                })
            }
        }

        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let preserved = run_on_worker(move |context| {
            Box::pin(async move {
                let attempts = Rc::new(Cell::new(0));
                let (started_tx, started_rx) = oneshot::channel();
                let (release_tx, release_rx) = oneshot::channel();
                let connector = Rc::new(GatedConnector {
                    inner: RecordingConnector {
                        attempts: Rc::clone(&attempts),
                        parameter_keys: Rc::new(RefCell::new(Vec::new())),
                        session_caching: None,
                        fail_after: None,
                        delay: Duration::ZERO,
                    },
                    gate: RefCell::new(Some((started_tx, release_rx))),
                });
                let pool = HttpSessionPool::with_connector(
                    context,
                    socket_address(address),
                    pool_params(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                    connector,
                );
                let cancellation = CancellationSource::new();
                let pending = pool.get_session_with_reservation(cancellation.token());
                tokio::pin!(pending);
                tokio::time::timeout(Duration::from_secs(5), async {
                    tokio::select! {
                        _ = &mut pending => panic!("connect completed before cancellation"),
                        started = started_rx => started.expect("connect reports it started"),
                    }
                })
                .await
                .expect("pool-owned connect starts");
                cancellation.request_cancellation();
                let cancelled = tokio::time::timeout(Duration::from_secs(5), pending).await;
                let released = release_tx.send(());
                assert!(matches!(
                    cancelled.expect("requester cancellation completes"),
                    Err(PoolError::Cancelled)
                ));
                released.expect("pool-owned connect remains alive after requester cancellation");
                tokio::time::timeout(Duration::from_secs(5), async {
                    while pool.session_count() != 1 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("pool admits the connection after requester cancellation");
                attempts.get() == 1 && pool.session_count() == 1
            })
        });
        assert!(preserved);
    }

    #[test]
    fn completed_connection_tasks_are_reaped_while_pool_is_open() {
        let reaped = run_on_worker(|context| {
            Box::pin(async move {
                let connector = Rc::new(RecordingConnector {
                    attempts: Rc::new(Cell::new(0)),
                    parameter_keys: Rc::new(RefCell::new(Vec::new())),
                    session_caching: None,
                    fail_after: Some(0),
                    delay: Duration::ZERO,
                });
                let pool = HttpSessionPool::with_connector(
                    context,
                    SocketAddress::new("127.0.0.1", 1),
                    PoolParams {
                        max_connection_attempts: 1,
                        ..pool_params(1)
                    },
                    ConnectionParams::default(),
                    SessionParams::default(),
                    connector,
                );
                for _ in 0..16 {
                    assert!(matches!(
                        pool.get_session_with_reservation(CancellationToken::never())
                            .await,
                        Err(PoolError::ConnectFailed { .. })
                    ));
                    tokio::task::yield_now().await;
                }
                tokio::task::yield_now().await;
                pool.task_count_for_test()
            })
        });
        assert_eq!(reaped, 0);
    }

    #[test]
    fn waiter_limit_does_not_reject_while_the_connection_is_pending() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let admitted = run_on_worker(move |context| {
            Box::pin(async move {
                let connector = Rc::new(RecordingConnector {
                    attempts: Rc::new(Cell::new(0)),
                    parameter_keys: Rc::new(RefCell::new(Vec::new())),
                    session_caching: None,
                    fail_after: None,
                    delay: Duration::from_millis(30),
                });
                let pool = HttpSessionPool::with_connector(
                    context,
                    socket_address(address),
                    PoolParams {
                        max_waiters: 0,
                        ..pool_params(1)
                    },
                    ConnectionParams::default(),
                    SessionParams::default(),
                    connector,
                );
                let first_cancellation = CancellationSource::new();
                let second_cancellation = CancellationSource::new();
                let mut first =
                    Box::pin(pool.get_session_with_reservation(first_cancellation.token()));
                poll_fn(|task| match first.as_mut().poll(task) {
                    Poll::Pending => Poll::Ready(()),
                    Poll::Ready(_) => panic!("delayed first acquisition completed early"),
                })
                .await;
                let mut second =
                    Box::pin(pool.get_session_with_reservation(second_cancellation.token()));
                poll_fn(|task| match second.as_mut().poll(task) {
                    Poll::Pending => Poll::Ready(()),
                    Poll::Ready(Err(PoolError::MaxWaiters)) => {
                        panic!("pending connect incorrectly consumed the waiter limit")
                    }
                    Poll::Ready(_) => panic!("delayed second acquisition completed early"),
                })
                .await;
                first_cancellation.request_cancellation();
                second_cancellation.request_cancellation();
                matches!(first.await, Err(PoolError::Cancelled))
                    && matches!(second.await, Err(PoolError::Cancelled))
            })
        });
        assert!(admitted);
    }

    #[test]
    fn overflowed_includes_pending_connections_after_limit_reduction() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let overflowed = run_on_worker(move |context| {
            Box::pin(async move {
                let connector = Rc::new(RecordingConnector {
                    attempts: Rc::new(Cell::new(0)),
                    parameter_keys: Rc::new(RefCell::new(Vec::new())),
                    session_caching: None,
                    fail_after: None,
                    delay: Duration::from_millis(30),
                });
                let pool = HttpSessionPool::with_connector(
                    context,
                    socket_address(address),
                    pool_params(2),
                    ConnectionParams::default(),
                    SessionParams::default(),
                    connector,
                );
                let first_cancellation = CancellationSource::new();
                let second_cancellation = CancellationSource::new();
                let mut first =
                    Box::pin(pool.get_session_with_reservation(first_cancellation.token()));
                let mut second =
                    Box::pin(pool.get_session_with_reservation(second_cancellation.token()));
                poll_fn(|task| match first.as_mut().poll(task) {
                    Poll::Pending => Poll::Ready(()),
                    Poll::Ready(_) => panic!("first delayed connection completed early"),
                })
                .await;
                poll_fn(|task| match second.as_mut().poll(task) {
                    Poll::Pending => Poll::Ready(()),
                    Poll::Ready(_) => panic!("second delayed connection completed early"),
                })
                .await;
                pool.set_max_connections(1)
                    .expect("open pool accepts a reduced connection limit");
                let overflowed = pool.overflowed();
                first_cancellation.request_cancellation();
                second_cancellation.request_cancellation();
                let _ = first.await;
                let _ = second.await;
                overflowed
            })
        });
        assert!(overflowed);
    }

    #[test]
    fn drain_synchronously_clears_pending_connection_accounting() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let observed = run_on_worker(move |context| {
            Box::pin(async move {
                let pending_count = Rc::new(Cell::new(0));
                let connector = Rc::new(RecordingConnector {
                    attempts: Rc::new(Cell::new(0)),
                    parameter_keys: Rc::new(RefCell::new(Vec::new())),
                    session_caching: None,
                    fail_after: None,
                    delay: Duration::from_millis(30),
                });
                let pool = HttpSessionPool::with_connector(
                    context,
                    socket_address(address),
                    pool_params(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                    connector,
                );
                pool.set_observer(Some(Rc::new(PendingCounter(Rc::clone(&pending_count)))))
                    .expect("open pool accepts pending observer");
                let mut acquisition =
                    Box::pin(pool.get_session_with_reservation(CancellationToken::never()));
                poll_fn(|task| match acquisition.as_mut().poll(task) {
                    Poll::Pending => Poll::Ready(()),
                    Poll::Ready(_) => panic!("delayed connection completed early"),
                })
                .await;
                assert_eq!(pending_count.get(), 1);
                assert!(pool.drain());
                let state_cleared = pool.empty()
                    && !pool.full()
                    && !pool.overflowed()
                    && pool.describe().contains("pending=0");
                assert!(matches!(acquisition.await, Err(PoolError::Draining)));
                tokio::time::timeout(Duration::from_secs(1), async {
                    while pending_count.get() != 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("pending observer sees connection cancellation");
                state_cleared
            })
        });
        assert!(observed);
    }
}

mod cache {
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use roxygen_core::message::SocketAddress;
    use roxygen_core::url::Url;
    use roxygen_core::url::UrlMode;
    use roxygen_runtime::cancellation::CancellationSource;
    use roxygen_runtime::cancellation::CancellationToken;
    use roxygen_runtime::tokio_host::ManagedWorker;
    use roxygen_session::session::LifecycleEvent;
    use roxygen_session::session::LifecycleObserver;
    use roxygen_transport::tls::TlsClientOptions;

    use super::connector::RecordingTlsStats;
    use super::connector::TlsStatEvent;
    use super::run_on_worker;
    use super::socket_address;
    use super::start_plain_server;
    use super::start_tls_server;
    use crate::HttpClient;
    use crate::RequestSpec;
    use crate::connector::CertVerifyResult;
    use crate::connector::ClientTransportOptions;
    use crate::connector::ConnectionParams;
    use crate::connector::IdentityValidation;
    use crate::connector::SessionParams;
    use crate::pool::ClientConnectionCache;
    use crate::pool::HttpSessionFactory;
    use crate::pool::HttpSessionPool;
    use crate::pool::PoolError;
    use crate::pool::PoolParams;
    use crate::pool::SimpleHttpSessionFactory;

    struct DrainFlag(Arc<AtomicBool>);

    fn pool_params(max_connections: u32) -> PoolParams {
        PoolParams {
            max_connections,
            connect_timeout: Duration::from_secs(1),
            ..PoolParams::default()
        }
    }

    impl LifecycleObserver for DrainFlag {
        fn on_event(&self, event: LifecycleEvent) {
            if matches!(event, LifecycleEvent::DrainStarted(_)) {
                self.0.store(true, Ordering::Release);
            }
        }
    }

    #[test]
    fn destination_key_reuses_pool_and_bounds_lru() {
        let bounded = run_on_worker(|context| {
            Box::pin(async move {
                let cache = ClientConnectionCache::new(context, 2);
                let first = cache
                    .pool_for_address("first", "127.0.0.1", 1001, false, None)
                    .expect("open cache creates first pool");
                let second = cache
                    .pool_for_address("second", "127.0.0.1", 1002, false, None)
                    .expect("open cache creates second pool");
                let first_again = cache
                    .pool_for_address("first", "127.0.0.1", 1001, false, None)
                    .expect("open cache reuses first pool");
                cache
                    .pool_for_address("third", "127.0.0.1", 1003, false, None)
                    .expect("open cache creates third pool");
                Rc::ptr_eq(&first, &first_again)
                    && cache.pool_count() == 2
                    && cache.pool_exists("127.0.0.1", 1001, false, None)
                    && !cache.pool_exists("127.0.0.1", 1002, false, None)
                    && second.empty()
            })
        });
        assert!(bounded);
    }

    #[test]
    fn explicit_connection_options_extend_pool_key() {
        let distinct = run_on_worker(|context| {
            Box::pin(async move {
                let cache = ClientConnectionCache::new(context, 4);
                let default = cache
                    .pool_for_address("host", "127.0.0.1", 1001, true, None)
                    .expect("default options create a pool");
                let mut params = ConnectionParams {
                    transport: ClientTransportOptions::Tls(TlsClientOptions::new("host")),
                    ..ConnectionParams::default()
                };
                let explicit = cache
                    .pool_for_address("host", "127.0.0.1", 1001, true, Some(params.clone()))
                    .expect("explicit options create a pool");
                let explicit_again = cache
                    .pool_for_address("host", "127.0.0.1", 1001, true, Some(params.clone()))
                    .expect("unchanged explicit options reuse the pool");
                let ClientTransportOptions::Tls(tls) = &mut params.transport else {
                    panic!("test connection options are TLS");
                };
                tls.set_early_data(true);
                let mutated = cache
                    .pool_for_address("host", "127.0.0.1", 1001, true, Some(params.clone()))
                    .expect("mutated options create a distinct pool");
                let mutated_again = cache
                    .pool_for_address("host", "127.0.0.1", 1001, true, Some(params))
                    .expect("unchanged mutated options reuse the pool");
                !Rc::ptr_eq(&default, &explicit)
                    && Rc::ptr_eq(&explicit, &explicit_again)
                    && !Rc::ptr_eq(&explicit, &mutated)
                    && Rc::ptr_eq(&mutated, &mutated_again)
            })
        });
        assert!(distinct);
    }

    #[test]
    fn cache_rejects_plaintext_default_connection_params() {
        let rejected = run_on_worker(|context| {
            Box::pin(async move {
                matches!(
                    ClientConnectionCache::new(context, 4)
                        .set_connection_params(ConnectionParams::default()),
                    Err(PoolError::InvalidConnectionParams)
                )
            })
        });
        assert!(rejected);
    }

    #[test]
    fn cache_certificate_verification_logger_applies_as_a_default() {
        let (server, certificate) = start_tls_server(&[b"http/1.1"]);
        let address = server.address().expect("server has an address");
        let events = Arc::new(Mutex::new(Vec::new()));
        let callback_events = Arc::clone(&events);
        let connected = run_on_worker(move |context| {
            Box::pin(async move {
                let mut tls = TlsClientOptions::new("wrong.example.com");
                tls.add_root_certificate(certificate);
                let cache = ClientConnectionCache::with_default_limit(context);
                cache
                    .set_cert_verify_log_fn(Some(Arc::new(move |result, _| {
                        callback_events
                            .lock()
                            .expect("verification events lock is available")
                            .push(result);
                    })))
                    .expect("cache verification logger is configured");
                cache
                    .get_session_with_connection_params(
                        "wrong.example.com",
                        address.port(),
                        true,
                        Duration::from_secs(1),
                        Some(address.ip().to_string().into_boxed_str()),
                        Some(ConnectionParams {
                            transport: ClientTransportOptions::Tls(tls),
                            identity_validation: IdentityValidation::Logging,
                            ..ConnectionParams::default()
                        }),
                        CancellationToken::never(),
                    )
                    .await
                    .is_ok()
            })
        });
        assert!(connected);
        assert_eq!(
            *events
                .lock()
                .expect("verification events lock is available"),
            [CertVerifyResult::SoftFail]
        );
    }

    #[test]
    fn destination_security_normalizes_explicit_connection_options() {
        let (server, certificate) = start_tls_server(&[b"http/1.1"]);
        let address = server.address().expect("server has an address");
        let connected = run_on_worker(move |context| {
            Box::pin(async move {
                let mut tls = TlsClientOptions::new("wrong.example.com");
                tls.add_root_certificate(certificate);
                let connection = ConnectionParams {
                    transport: ClientTransportOptions::Tls(tls),
                    identity_validation: IdentityValidation::Enforcing,
                    ..ConnectionParams::default()
                };
                let cache = ClientConnectionCache::with_default_limit(context);
                cache
                    .get_session_with_connection_params(
                        "localhost",
                        address.port(),
                        true,
                        Duration::from_secs(1),
                        Some(address.ip().to_string().into_boxed_str()),
                        Some(connection),
                        CancellationToken::never(),
                    )
                    .await
                    .is_ok()
            })
        });
        assert!(connected);
    }

    #[test]
    fn configured_pool_limit_is_reported() {
        let maximum = run_on_worker(|context| {
            Box::pin(async move { ClientConnectionCache::new(context, 7).maximum_pools() })
        });
        assert_eq!(maximum, 7);
    }

    #[test]
    fn session_lookup_resolves_or_uses_override() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let peer = run_on_worker(move |context| {
            Box::pin(async move {
                let cache = ClientConnectionCache::with_default_limit(context);
                cache
                    .get_session_with_reservation(
                        "unresolvable.invalid",
                        address.port(),
                        false,
                        Duration::from_secs(1),
                        Some(address.ip().to_string().into_boxed_str()),
                        CancellationToken::never(),
                    )
                    .await
                    .expect("address override connects")
                    .session
                    .peer_address()
                    .port()
            })
        });
        assert_eq!(peer, address.port());
    }

    #[test]
    fn simple_and_cached_factories_share_the_session_contract() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let peers = run_on_worker(move |context| {
            Box::pin(async move {
                let simple = SimpleHttpSessionFactory::new(context.clone());
                let simple_result = HttpSessionFactory::get_session_with_reservation(
                    &simple,
                    Box::from("localhost"),
                    address.port(),
                    false,
                    Duration::from_secs(1),
                    Some(address.ip().to_string().into_boxed_str()),
                    CancellationToken::never(),
                )
                .await
                .expect("simple factory connects");
                let cache = ClientConnectionCache::with_default_limit(context);
                let cached_result = HttpSessionFactory::get_session_with_reservation(
                    &cache,
                    Box::from("localhost"),
                    address.port(),
                    false,
                    Duration::from_secs(1),
                    Some(address.ip().to_string().into_boxed_str()),
                    CancellationToken::never(),
                )
                .await
                .expect("cache factory connects");
                (
                    simple.requires_absolute_urls(),
                    simple_result.session.peer_address().port(),
                    cached_result.session.peer_address().port(),
                )
            })
        });
        assert_eq!(peers, (false, address.port(), address.port()));
    }

    #[test]
    fn reap_removes_empty_pool() {
        let count = run_on_worker(|context| {
            Box::pin(async move {
                let cache = ClientConnectionCache::new(context, 4);
                cache
                    .pool_for_address("host", "127.0.0.1", 1001, false, None)
                    .expect("open cache creates a pool");
                cache.reap_all_empty_pools();
                cache.pool_count()
            })
        });
        assert_eq!(count, 0);
    }

    #[test]
    fn reap_preserves_nonempty_pools() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let preserved = run_on_worker(move |context| {
            Box::pin(async move {
                let cache = ClientConnectionCache::new(context, 4);
                let active = cache
                    .pool_for_address(
                        "active",
                        address.ip().to_string(),
                        address.port(),
                        false,
                        None,
                    )
                    .expect("open cache creates active pool");
                let _held = active
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("active pool connects");
                cache
                    .pool_for_address("empty", "127.0.0.1", 1002, false, None)
                    .expect("open cache creates empty pool");
                cache.reap_all_empty_pools();
                cache.pool_count() == 1
                    && cache.pool_exists(address.ip().to_string(), address.port(), false, None)
            })
        });
        assert!(preserved);
    }

    #[test]
    fn reap_traversal_is_bounded() {
        let count = run_on_worker(|context| {
            Box::pin(async move {
                let cache = ClientConnectionCache::new(context, 10);
                (0..6).for_each(|offset| {
                    cache
                        .pool_for_address("host", "127.0.0.1", 1100 + offset, false, None)
                        .expect("open cache creates bounded pools");
                });
                cache.reap_empty_pools(5);
                cache.pool_count()
            })
        });
        assert_eq!(count, 1);
    }

    #[test]
    fn drain_rejects_new_sessions() {
        let rejected = run_on_worker(|context| {
            Box::pin(async move {
                let cache = ClientConnectionCache::new(context, 2);
                assert!(cache.drain());
                matches!(
                    cache
                        .get_session_with_reservation(
                            "host",
                            80,
                            false,
                            Duration::from_secs(1),
                            Some(Box::from("127.0.0.1")),
                            CancellationToken::never(),
                        )
                        .await,
                    Err(PoolError::Draining)
                )
            })
        });
        assert!(rejected);
    }

    #[test]
    fn pre_cancelled_request_is_rejected() {
        let rejected = run_on_worker(|context| {
            Box::pin(async move {
                let cache = ClientConnectionCache::new(context, 2);
                let cancellation = CancellationSource::new();
                cancellation.request_cancellation();
                matches!(
                    cache
                        .get_session_with_reservation(
                            "host",
                            80,
                            false,
                            Duration::from_secs(1),
                            Some(Box::from("127.0.0.1")),
                            cancellation.token(),
                        )
                        .await,
                    Err(PoolError::Cancelled)
                )
            })
        });
        assert!(rejected);
    }

    #[test]
    fn drain_wakes_pending_session_request() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let rejected = run_on_worker(move |context| {
            Box::pin(async move {
                let cache = ClientConnectionCache::new(context, 2);
                cache
                    .set_pool_params(PoolParams {
                        max_connections: 1,
                        connect_timeout: Duration::from_secs(1),
                        ..PoolParams::default()
                    })
                    .expect("open cache accepts pool parameters");
                let held = cache
                    .get_session_with_reservation(
                        "host",
                        address.port(),
                        false,
                        Duration::from_secs(1),
                        Some(address.ip().to_string().into_boxed_str()),
                        CancellationToken::never(),
                    )
                    .await
                    .expect("first acquisition succeeds");
                let pending = cache.get_session_with_reservation(
                    "host",
                    address.port(),
                    false,
                    Duration::from_secs(1),
                    Some(address.ip().to_string().into_boxed_str()),
                    CancellationToken::never(),
                );
                tokio::pin!(pending);
                tokio::select! {
                    _ = &mut pending => panic!("pending acquisition completed before drain"),
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
                assert!(cache.drain());
                let result = pending.await;
                drop(held.reservation);
                matches!(result, Err(PoolError::Draining))
            })
        });
        assert!(rejected);
    }

    #[test]
    fn worker_shutdown_drains_an_escaped_cache() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let drained = Arc::new(AtomicBool::new(false));
        let worker_drained = Arc::clone(&drained);
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let worker = ManagedWorker::new(8).expect("client worker starts");
        let _call = worker
            .handle()
            .call_local(move |context| async move {
                let cache = ClientConnectionCache::new(context, 1);
                cache
                    .set_session_params(SessionParams {
                        lifecycle_observer: Some(Rc::new(DrainFlag(worker_drained))),
                        ..SessionParams::default()
                    })
                    .expect("open cache accepts session parameters");
                let _held = cache
                    .get_session_with_reservation(
                        "localhost",
                        address.port(),
                        false,
                        Duration::from_secs(1),
                        Some(address.ip().to_string().into_boxed_str()),
                        CancellationToken::never(),
                    )
                    .await
                    .expect("cache establishes a session");
                ready_tx.try_send(()).expect("test receiver is ready");
                std::future::pending::<()>().await;
            })
            .expect("worker accepts cache task");
        ready_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("cache session is established");
        worker.shutdown().expect("worker shutdown completes");
        assert!(drained.load(Ordering::Acquire));
    }

    #[test]
    fn cancellation_wakes_pending_session_request() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let rejected = run_on_worker(move |context| {
            Box::pin(async move {
                let cache = ClientConnectionCache::new(context, 2);
                cache
                    .set_pool_params(PoolParams {
                        max_connections: 1,
                        connect_timeout: Duration::from_secs(1),
                        ..PoolParams::default()
                    })
                    .expect("open cache accepts pool parameters");
                let held = cache
                    .get_session_with_reservation(
                        "host",
                        address.port(),
                        false,
                        Duration::from_secs(1),
                        Some(address.ip().to_string().into_boxed_str()),
                        CancellationToken::never(),
                    )
                    .await
                    .expect("first acquisition succeeds");
                let cancellation = CancellationSource::new();
                let pending = cache.get_session_with_reservation(
                    "host",
                    address.port(),
                    false,
                    Duration::from_secs(1),
                    Some(address.ip().to_string().into_boxed_str()),
                    cancellation.token(),
                );
                tokio::pin!(pending);
                tokio::select! {
                    _ = &mut pending => panic!("pending acquisition completed before cancellation"),
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
                cancellation.request_cancellation();
                let result = pending.await;
                drop(held.reservation);
                matches!(result, Err(PoolError::Cancelled))
            })
        });
        assert!(rejected);
    }

    #[test]
    fn configured_pool_and_session_limits_are_applied() {
        let (server, certificate) = start_tls_server(&[b"h2"]);
        let address = server.address().expect("server has an address");
        let events = Arc::new(Mutex::new(Vec::new()));
        let recorded_events = Arc::clone(&events);
        let limited = run_on_worker(move |context| {
            Box::pin(async move {
                let cache = ClientConnectionCache::new(context, 2);
                cache
                    .set_pool_params(PoolParams {
                        max_connections: 1,
                        ..PoolParams::default()
                    })
                    .expect("open cache accepts pool parameters");
                let mut tls = TlsClientOptions::new("localhost");
                tls.add_root_certificate(certificate);
                tls.set_alpn_protocols([b"h2".as_slice()]);
                cache
                    .set_connection_params(ConnectionParams {
                        transport: ClientTransportOptions::Tls(tls),
                        tls_stats: Some(Arc::new(RecordingTlsStats(recorded_events))),
                        ..ConnectionParams::default()
                    })
                    .expect("open cache accepts connection parameters");
                cache
                    .set_session_params(SessionParams {
                        max_concurrent_outgoing_streams: Some(1),
                        ..SessionParams::default()
                    })
                    .expect("open cache accepts session parameters");
                let held = cache
                    .get_session_with_reservation(
                        "localhost",
                        address.port(),
                        true,
                        Duration::from_secs(1),
                        Some(address.ip().to_string().into_boxed_str()),
                        CancellationToken::never(),
                    )
                    .await
                    .expect("first acquisition succeeds");
                let no_capacity = held.session.available_transactions() == 0;
                let pool = cache
                    .pool_for_address(
                        "localhost",
                        address.ip().to_string(),
                        address.port(),
                        true,
                        None,
                    )
                    .expect("configured pool is retrievable");
                let full = pool.full();
                let url = Url::parse(
                    format!("https://localhost:{}/", address.port()),
                    false,
                    UrlMode::StrictCompat,
                );
                HttpClient::request(&held.session, Some(held.reservation), RequestSpec::get(url))
                    .await
                    .expect("configured cached request succeeds");
                no_capacity && full
            })
        });
        assert!(limited);
        assert_eq!(
            *events.lock().expect("TLS stats lock is available"),
            [TlsStatEvent::Connection(true)]
        );
    }

    #[test]
    fn zero_waiter_limit_allows_initial_connect_but_rejects_when_full() {
        let (server, _) = start_plain_server();
        let address = server.address().expect("server has an address");
        let observed = run_on_worker(move |context| {
            Box::pin(async move {
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    PoolParams {
                        max_waiters: 0,
                        ..pool_params(1)
                    },
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                let held = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("the initial connection bypasses the waiter cap");
                let full_result = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await;
                drop(held.reservation);
                matches!(full_result, Err(PoolError::MaxWaiters))
            })
        });
        assert!(observed);
    }

    #[test]
    fn drained_pool_and_cache_reject_new_admission_and_mutation() {
        let rejected = run_on_worker(|context| {
            Box::pin(async move {
                let pool = HttpSessionPool::new(
                    context.clone(),
                    SocketAddress::new("127.0.0.1", 1),
                    pool_params(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                );
                assert!(pool.drain());
                let pool_rejected =
                    matches!(
                        pool.get_session_with_reservation(CancellationToken::never())
                            .await,
                        Err(PoolError::Draining)
                    ) && matches!(pool.set_max_connections(2), Err(PoolError::Draining));

                let cache = ClientConnectionCache::new(context, 1);
                assert!(cache.drain());
                let cache_rejected = matches!(
                    cache.pool_for_address("host", "127.0.0.1", 80, false, None),
                    Err(PoolError::Draining)
                ) && matches!(
                    cache.set_pool_params(PoolParams::default()),
                    Err(PoolError::Draining)
                );
                pool_rejected && cache_rejected
            })
        });
        assert!(rejected);
    }
}

mod owned_header_admission {
    use std::cell::Cell;

    use roxygen_core::http::HttpMethod;
    use roxygen_runtime::cancellation::CancellationSource;

    use super::*;
    use crate::connector::ClientSession;

    struct RemainingBody {
        source: HttpFixedSource,
        session: ClientSession,
        stops: Rc<Cell<usize>>,
    }

    impl HttpSource for RemainingBody {
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            panic!("the request header was already consumed");
        }

        fn read_body_event(
            &mut self,
            maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            self.source.read_body_event(maximum)
        }

        fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {
            self.stops.set(self.stops.get() + 1);
            let _ = self.session.active_transactions();
        }
    }

    fn request(
        session: &ClientSession,
        stops: Rc<Cell<usize>>,
        payload: &[u8],
    ) -> (HttpHeaderEvent, HttpSourceHolder) {
        let mut message = HttpMessage::new();
        message.set_method(HttpMethod::Post).unwrap();
        message.set_url(b"/", false).unwrap();
        message.set_http_version(1, 1);
        message.headers_mut().set("host", b"localhost");
        message
            .headers_mut()
            .set("content-length", payload.len().to_string().as_bytes());
        let header = HttpHeaderEvent::new(message, payload.is_empty()).unwrap();
        let body = HttpSourceHolder::new(Box::new(RemainingBody {
            source: HttpFixedSource::make_fixed_body(
                BufQueue::copy_from_slice(payload, payload.len()).unwrap(),
            ),
            session: session.clone(),
            stops,
        }));
        (header, body)
    }

    #[test]
    fn reserved_headers_send_body_without_reading_another_header() {
        let (server, _) = start_plain_server();
        let address = server.address().unwrap();
        run_on_worker(move |context| {
            Box::pin(async move {
                let session = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    Duration::from_secs(1),
                    ConnectionParams::default(),
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .unwrap();
                for payload in [b"".as_slice(), b"abc".as_slice()] {
                    let stops = Rc::new(Cell::new(0));
                    let (header, body) = request(&session, Rc::clone(&stops), payload);
                    let mut response = session
                        .send_reserved_request_with_headers(
                            header,
                            body,
                            session.reserve_request().unwrap(),
                            CancellationToken::never(),
                        )
                        .unwrap();
                    let header = response.source.read_header_event().await.unwrap();
                    assert_eq!(header.headers().response_status(), Some(200));
                    let mut received = Vec::new();
                    if !header.eom() {
                        loop {
                            let event = response.source.read_body_event(1024).await.unwrap();
                            if let Some(body) = event.as_body() {
                                for segment in body.segments() {
                                    received.extend_from_slice(segment);
                                }
                            }
                            if event.eom() {
                                break;
                            }
                        }
                    }
                    assert_eq!(received, payload);
                    response.completion.wait().await.unwrap();
                    assert_eq!(stops.get(), usize::from(payload.is_empty()));
                }
                session.initiate_drain().unwrap();
                session.wait_closed().await.unwrap();
            })
        });
    }

    #[derive(Clone, Copy)]
    enum Rejection {
        ForeignReservation,
        CallerCancellation,
        SessionCancellation,
    }

    #[test]
    fn reserved_headers_reject_cancellation_and_foreign_reservations() {
        let (server, stats) = start_plain_server();
        let address = server.address().unwrap();
        run_on_worker(move |context| {
            Box::pin(async move {
                let connect = || {
                    HttpConnector::connect(
                        &context,
                        socket_address(address),
                        Duration::from_secs(1),
                        ConnectionParams::default(),
                        SessionParams::default(),
                        CancellationToken::never(),
                    )
                };
                let owner = connect().await.unwrap();
                let foreign = connect().await.unwrap();
                for case in [
                    Rejection::ForeignReservation,
                    Rejection::CallerCancellation,
                    Rejection::SessionCancellation,
                ] {
                    let stops = Rc::new(Cell::new(0));
                    let reservation = owner.reserve_request().unwrap();
                    let (header, body) = request(&owner, Rc::clone(&stops), b"abc");
                    let caller = CancellationSource::new();
                    let target = match case {
                        Rejection::ForeignReservation => &foreign,
                        Rejection::CallerCancellation => {
                            caller.request_cancellation();
                            &owner
                        }
                        Rejection::SessionCancellation => {
                            owner.cancel();
                            &owner
                        }
                    };
                    let error = match target.send_reserved_request_with_headers(
                        header,
                        body,
                        reservation,
                        caller.token(),
                    ) {
                        Err(error) => error,
                        Ok(_) => panic!("invalid admission succeeded"),
                    };
                    if !matches!(case, Rejection::ForeignReservation) {
                        assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                    }
                    assert_eq!(stops.get(), 1);
                    assert_eq!(owner.active_transactions(), 0);
                    assert_eq!(foreign.active_transactions(), 0);
                }
                foreign.initiate_drain().unwrap();
                foreign.wait_closed().await.unwrap();
                let _ = owner.wait_closed().await;
            })
        });
        assert_eq!(stats.requests.load(Ordering::Relaxed), 0);
    }
}
