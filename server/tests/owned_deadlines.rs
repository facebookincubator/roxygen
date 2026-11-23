/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! `ServerSessionConfig::owned_deadlines` on the owned drivers (`owned_http1`, `owned_http2`).

use std::future::Future;
use std::io::Write;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::os::fd::AsRawFd;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::http::CodecProtocol;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::tokio_host::HostedWorker;
use roxygen_runtime::tokio_host::HostedWorkerOptions;
use roxygen_runtime::worker::WorkerContext;
use roxygen_session::DeadlineExpiry;
use roxygen_session::OwnedConnectionDeadlines;
use roxygen_transport::tcp::TcpTransport;
use roxygen_transport::tls::TlsServerOptions;
use roxygen_transport::tls::builtin_crypto_provider;
use rustls::ClientConfig;
use rustls::ClientConnection;
use rustls::RootCertStore;
use rustls::StreamOwned;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::ServerName;
use socket2::Domain;
use socket2::SockRef;
use socket2::Socket;
use socket2::Type;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::ConnectionId;
use crate::ConnectionLifecycleObserver;
use crate::ConnectionOutcome;
use crate::acceptor::ServerSessionConfig;
use crate::acceptor::owned_http2_session_config;
use crate::connection::ServeTransportOptions;
use crate::connection::serve_transport;
use crate::scoped_server::ScopedHttpServer;
use crate::server::HandlerFactory;
use crate::server::ServerConfig;
use crate::server::StaticTlsCredentials;

const TIMEOUT: Duration = Duration::from_secs(10);

/// Runs `test` on a `HostedWorker` and waits for it, within `TIMEOUT`.
fn on_worker<Test, Work>(test: Test)
where
    Test: FnOnce(WorkerContext) -> Work + Send + 'static,
    Work: Future<Output = ()> + 'static,
{
    let worker = HostedWorker::spawn(HostedWorkerOptions {
        command_capacity: NonZeroUsize::new(4).expect("nonzero capacity"),
        thread_name: "owned-deadlines".to_owned(),
        on_owner_thread_stop: None,
    })
    .expect("hosted worker starts");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("application runtime")
        .block_on(async {
            tokio::time::timeout(
                TIMEOUT,
                worker.handle().call_local(test).expect("test admitted"),
            )
            .await
            .expect("bounded test completion")
            .expect("test finishes")
        });
    drop(worker);
}

/// Answers every request with an empty 200.
struct Ok200;

impl HttpHandler for Ok200 {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(std::future::ready(Ok(HttpSourceHolder::new(Box::new(
            HttpFixedSource::make_fixed_response(200, None).expect("response"),
        )))))
    }
}

/// The owned HTTP/1.1 path takes its deadlines from `owned_deadlines`: a request head that has
/// not ended by the handshake deadline resets the connection then, with no response.
#[test]
fn server_owned_h1_path_applies_session_deadlines() {
    const HANDSHAKE: Duration = Duration::from_millis(200);
    on_worker(|context| async move {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let connect = TcpStream::connect(listener.local_addr().expect("address"));
        let (client, accepted) = tokio::join!(connect, listener.accept());
        let mut client = client.expect("client connects");
        let (accepted, _) = accepted.expect("connection accepted");
        let session = ServerSessionConfig {
            stream_read_timeout: Duration::ZERO,
            connection_idle_timeout: Duration::ZERO,
            write_timeout: Duration::ZERO,
            owned_http1: true,
            owned_deadlines: OwnedConnectionDeadlines::new(
                HANDSHAKE,
                Duration::ZERO,
                DeadlineExpiry::HardClose,
            ),
            ..ServerSessionConfig::default()
        };
        let transport = TcpTransport::from_accepted(
            &context,
            accepted.into_std().expect("standard stream"),
            session.byte_stream_limits,
        )
        .expect("transport adopted");
        let mut options =
            ServeTransportOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext);
        options.session = session;
        let connection =
            serve_transport(&context, transport, Rc::new(Ok200), options).expect("attached");
        let started = Instant::now();
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n")
            .await
            .expect("partial head written");
        let mut received = Vec::new();
        let read = tokio::time::timeout(TIMEOUT / 2, client.read_to_end(&mut received))
            .await
            .expect("the server closes the connection");
        let elapsed = started.elapsed();
        assert!(
            read.is_ok() || received.is_empty(),
            "the connection ends with EOF or a reset: {read:?}"
        );
        assert!(
            received.is_empty(),
            "no response: {:?}",
            String::from_utf8_lossy(&received)
        );
        assert!(
            elapsed >= HANDSHAKE,
            "the close came before the deadline: {elapsed:?}"
        );
        let closed = connection.wait_closed().await;
        assert_eq!(
            closed.err().map(|error| error.code()),
            Some(HttpErrorCode::READ_TIMEOUT)
        );
    });
}

/// The owned HTTP/2 path takes its deadlines from `owned_deadlines` too.
#[test]
fn owned_http2_path_copies_session_deadlines() {
    let deadlines = OwnedConnectionDeadlines::new(
        Duration::from_secs(3),
        Duration::from_secs(7),
        DeadlineExpiry::Drain,
    );
    let session = ServerSessionConfig {
        owned_http2: true,
        owned_deadlines: deadlines,
        ..ServerSessionConfig::default()
    };
    assert_eq!(owned_http2_session_config(&session).deadlines, deadlines);
    assert_eq!(
        ServerSessionConfig::default().owned_deadlines,
        OwnedConnectionDeadlines::default(),
        "no deadlines by default"
    );
}

/// Outside the session crate the non-exhaustive deadlines are built from `default()` or `new`,
/// and each builder method sets one field.
#[test]
fn owned_connection_deadlines_builders_set_each_field() {
    let built = OwnedConnectionDeadlines::default()
        .with_handshake_timeout(Duration::from_secs(3))
        .with_idle_timeout(Duration::from_secs(7))
        .with_on_expiry(DeadlineExpiry::Drain);
    assert_eq!(
        built,
        OwnedConnectionDeadlines::new(
            Duration::from_secs(3),
            Duration::from_secs(7),
            DeadlineExpiry::Drain,
        )
    );
    assert_eq!(
        OwnedConnectionDeadlines::default().with_idle_timeout(Duration::from_secs(7)),
        OwnedConnectionDeadlines::new(
            Duration::ZERO,
            Duration::from_secs(7),
            DeadlineExpiry::HardClose,
        ),
        "a builder leaves the other fields as they were"
    );
}

/// The idle deadline of the TLS test.
const TLS_IDLE: Duration = Duration::from_millis(200);
/// How long the fill waits after each chunk for the server and both kernels to take it.
const FILL_SETTLE: Duration = Duration::from_millis(50);
/// The most fill chunks before the fill gives up.
const MAXIMUM_FILL_CHUNKS: usize = 4_096;
/// How late the TLS test's hard close may be past `T` after the last staged write: the wheel
/// tick plus the time for the write to reach the server and its close to reach the observer.
const CLOSE_SLACK: Duration = Duration::from_millis(50);

/// A chunked response whose body chunks the test sends from its own thread.
struct ChannelHandler(Arc<Mutex<Option<mpsc::UnboundedReceiver<Vec<u8>>>>>);

impl HttpHandler for ChannelHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let chunks = self
            .0
            .lock()
            .expect("the receiver slot")
            .take()
            .expect("one request");
        let mut message = HttpMessage::new();
        message.set_status_code(200).expect("status");
        message.set_http_version(1, 1);
        let header = HttpHeaderEvent::new(message, false).expect("response head");
        Box::pin(std::future::ready(Ok(HttpSourceHolder::new(Box::new(
            ChannelBody {
                header: Some(header),
                chunks,
            },
        )))))
    }
}

struct ChannelBody {
    header: Option<HttpHeaderEvent>,
    chunks: mpsc::UnboundedReceiver<Vec<u8>>,
}

impl HttpSource for ChannelBody {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(self.header.take().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"response header exhausted")
        })))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        unreachable!("a polling source is never read through a future")
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}

    fn polls_body_events(&self) -> bool {
        true
    }

    fn poll_body_event(
        &mut self,
        context: &mut Context<'_>,
        _maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        self.chunks.poll_recv(context).map(|chunk| {
            let chunk = chunk.expect("the test keeps the sender until the connection closes");
            let length = chunk.len();
            Ok(HttpBodyEvent::body(
                BufQueue::from_bytes(Bytes::from(chunk), length).expect("a chunk fits its queue"),
                false,
            ))
        })
    }
}

/// Records when and how the connection ended.
#[derive(Default)]
struct CloseObserver(Mutex<Option<(Instant, Option<HttpErrorCode>)>>);

impl ConnectionLifecycleObserver for CloseObserver {
    fn on_socket_closed(&self, _id: ConnectionId, outcome: &ConnectionOutcome<'_>) {
        let error = match outcome {
            ConnectionOutcome::Closed { result, .. } => {
                result.as_ref().err().map(|error| error.code())
            }
            _ => Some(HttpErrorCode::DROPPED),
        };
        *self.0.lock().expect("the close record") = Some((Instant::now(), error));
    }
}

fn generated_tls() -> (TlsServerOptions, Vec<u8>) {
    let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let certificate = generated.cert.der().to_vec();
    let options = TlsServerOptions::new(
        vec![certificate.clone().into_boxed_slice()],
        generated
            .signing_key
            .serialized_der()
            .to_vec()
            .into_boxed_slice(),
        vec![b"http/1.1".to_vec().into_boxed_slice()],
    )
    .unwrap();
    (options, certificate)
}

fn tls_client(certificate: &[u8]) -> ClientConnection {
    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from(certificate.to_vec()))
        .unwrap();
    let mut config = ClientConfig::builder_with_provider(Arc::new(builtin_crypto_provider()))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    ClientConnection::new(
        Arc::new(config),
        ServerName::try_from("localhost".to_owned()).unwrap(),
    )
    .unwrap()
}

/// A loopback client socket with the smallest receive buffer, set before it connects.
fn small_window_client(address: SocketAddr) -> std::net::TcpStream {
    let socket = Socket::new(Domain::IPV4, Type::STREAM, None).expect("client socket");
    socket
        .set_recv_buffer_size(1)
        .expect("client receive buffer");
    socket.connect(&address.into()).expect("client connects");
    socket.into()
}

/// The bytes the socket `fd` holds for its owner to read (`FIONREAD`).
fn readable(fd: i32) -> i32 {
    let mut bytes: libc::c_int = 0;
    // SAFETY: `fd` is an open socket and `bytes` outlives the call.
    assert_eq!(unsafe { libc::ioctl(fd, libc::FIONREAD, &mut bytes) }, 0);
    bytes
}

/// The bytes the socket `fd` has not yet sent or had acknowledged (`SIOCOUTQ`).
fn unsent(fd: i32) -> i32 {
    let mut bytes: libc::c_int = 0;
    // SAFETY: `fd` is an open socket and `bytes` outlives the call.
    assert_eq!(unsafe { libc::ioctl(fd, libc::TIOCOUTQ, &mut bytes) }, 0);
    bytes
}

/// Over TLS, a response write that reaches only Roxygen's ciphertext staging, because the peer
/// stopped reading and both kernels' buffers are full, still counts as progress: writes every
/// `T/2` keep the connection open for `3 T`, and the hard close follows the last of them by
/// about `T`.
#[test]
fn tls_idle_is_refreshed_by_staged_plaintext_write() {
    let (options, certificate) = generated_tls();
    let (sender, receiver) = mpsc::unbounded_channel::<Vec<u8>>();
    let receiver = Arc::new(Mutex::new(Some(receiver)));
    let dup: Arc<Mutex<Option<std::net::TcpStream>>> = Arc::default();
    let closed = Arc::new(CloseObserver::default());
    let mut config = ServerConfig {
        tls_credentials: Some(Arc::new(StaticTlsCredentials::new(options))),
        num_io_threads: 1,
        lifecycle_observer: Some(Arc::clone(&closed) as Arc<dyn ConnectionLifecycleObserver>),
        ..ServerConfig::default()
    };
    config.session = ServerSessionConfig {
        stream_read_timeout: Duration::ZERO,
        connection_idle_timeout: Duration::ZERO,
        write_timeout: Duration::ZERO,
        owned_http1: true,
        owned_deadlines: OwnedConnectionDeadlines::new(
            TIMEOUT,
            TLS_IDLE,
            DeadlineExpiry::HardClose,
        ),
        ..ServerSessionConfig::default()
    };
    config.session.accepted_socket_profile = Some(Arc::new({
        let dup = Arc::clone(&dup);
        move |socket| {
            let failed = |error: std::io::Error| {
                HttpError::with_message(HttpErrorCode::CONNECT_ERROR, error.to_string().as_bytes())
            };
            SockRef::from(socket)
                .set_send_buffer_size(1)
                .map_err(failed)?;
            *dup.lock().expect("the dup slot") = Some(socket.try_clone().map_err(failed)?);
            Ok(())
        }
    }));
    let handler_factory: HandlerFactory =
        Arc::new(move |_| Rc::new(ChannelHandler(Arc::clone(&receiver))) as HttpHandlerRef);
    let server = ScopedHttpServer::start(config, handler_factory, None).expect("server starts");
    let address = server.address().expect("server address");

    let socket = small_window_client(address);
    socket.set_write_timeout(Some(TIMEOUT)).unwrap();
    let mut client = StreamOwned::new(tls_client(&certificate), socket);
    client
        .write_all(b"GET /fill HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .expect("request written");
    client.flush().expect("request flushed");
    let client_fd = client.sock.as_raw_fd();
    let server_fd = {
        let started = Instant::now();
        loop {
            if let Some(socket) = dup.lock().expect("the dup slot").as_ref() {
                break socket.as_raw_fd();
            }
            assert!(started.elapsed() < TIMEOUT, "the server accepts");
            std::thread::sleep(Duration::from_millis(1));
        }
    };

    // Fill both kernels' buffers until a chunk reaches neither of them.
    let mut queues = (readable(client_fd), unsent(server_fd));
    let mut fill_chunks = 0;
    loop {
        assert!(
            fill_chunks < MAXIMUM_FILL_CHUNKS,
            "the kernels kept taking chunks: {fill_chunks} sent, queues {queues:?}"
        );
        sender.send(vec![b'f'; 1_024]).unwrap_or_else(|_| {
            panic!("the connection closed during the fill, after {fill_chunks} chunks")
        });
        fill_chunks += 1;
        std::thread::sleep(FILL_SETTLE);
        let next = (readable(client_fd), unsent(server_fd));
        let grew = next.0 > queues.0 || next.1 > queues.1;
        queues = next;
        if !grew {
            break;
        }
    }

    // Writes that reach only the staging queue, every `T/2` for `3 T`.
    let mut last_write = Instant::now();
    for _ in 0..6 {
        sender.send(vec![b's'; 256]).unwrap_or_else(|_| {
            panic!(
                "the connection closed during the staged writes: last fill chunk \
                 {fill_chunks}, queues {queues:?}"
            )
        });
        last_write = Instant::now();
        std::thread::sleep(TLS_IDLE / 2);
    }
    let after = (readable(client_fd), unsent(server_fd));
    assert_eq!(
        after, queues,
        "the staged writes reached neither kernel: last fill chunk {fill_chunks}, \
         (client FIONREAD, server SIOCOUTQ) before {queues:?}, after {after:?}"
    );
    assert!(
        closed.0.lock().expect("the close record").is_none(),
        "the connection is open after 3 T of staged writes: last fill chunk {fill_chunks}, \
         queues {queues:?}"
    );

    drop(dup.lock().expect("the dup slot").take());
    let started = Instant::now();
    let (closed_at, error) = loop {
        if let Some(record) = *closed.0.lock().expect("the close record") {
            break record;
        }
        assert!(started.elapsed() < TIMEOUT, "the idle connection closes");
        std::thread::sleep(Duration::from_millis(1));
    };
    let idle = closed_at.duration_since(last_write);
    assert_eq!(error, Some(HttpErrorCode::READ_TIMEOUT));
    assert!(
        idle >= TLS_IDLE && idle < TLS_IDLE + CLOSE_SLACK,
        "the hard close follows the last staged write by T: {idle:?}"
    );
    drop(client);
    drop(sender);
    assert!(server.force_stop().expect("server stops"));
}
