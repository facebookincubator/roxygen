//! `SessionParams::non_abortive_close` (`DIV-038`): a connector-built plaintext
//! session that resets its connection (here because the caller dropped a
//! response) closes it as hyper and reqwest do, by dropping the socket, so the
//! engine sees FIN rather than RST.

use std::future::Future;
use std::io::ErrorKind;
use std::io::Read;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;
use std::time::Duration;

use roxygen_core::connection::TransportEndpoint;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::http::CodecProtocol;
use roxygen_core::message::SocketAddress;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::tokio_host::HostedWorker;
use roxygen_runtime::tokio_host::HostedWorkerOptions;
use roxygen_runtime::tokio_host::ManagedWorker;
use roxygen_runtime::worker::WorkerCall;
use roxygen_runtime::worker::WorkerContext;
use roxygen_transport::byte_stream::ByteStreamLimits;
use roxygen_transport::established::EstablishedIo;
use roxygen_transport::established::adopt_local;
use roxygen_transport::tls::TlsClientOptions;

use crate::connector::ClientAttachOptions;
use crate::connector::ClientSession;
use crate::connector::ClientTransportOptions;
use crate::connector::ConnectionParams;
use crate::connector::SessionParams;
use crate::pool::GetSessionResult;
use crate::pool::HttpSessionPool;
use crate::pool::PoolError;
use crate::pool::PoolParams;

/// How the engine's connection ended after the request it never answered.
#[derive(Debug, PartialEq)]
enum EngineSaw {
    Fin,
    Reset,
}

/// An HTTP/1.1 engine that reads one request head, never answers, and reports
/// how the client ended the connection. `head_seen` is set once the head is read.
fn silent_engine() -> (SocketAddr, Arc<AtomicBool>, JoinHandle<EngineSaw>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("engine binds");
    let address = listener.local_addr().expect("engine address");
    let head_seen = Arc::new(AtomicBool::new(false));
    let seen = Arc::clone(&head_seen);
    let engine = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("engine accepts");
        let mut request = Vec::new();
        let mut buffer = [0; 1024];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = stream.read(&mut buffer).expect("engine reads");
            assert_ne!(read, 0, "the request head arrives");
            request.extend_from_slice(&buffer[..read]);
        }
        seen.store(true, Ordering::SeqCst);
        loop {
            match stream.read(&mut buffer) {
                Ok(0) => return EngineSaw::Fin,
                Ok(_) => {}
                Err(error) if error.kind() == ErrorKind::ConnectionReset => {
                    return EngineSaw::Reset;
                }
                Err(error) => panic!("unexpected engine read error: {error}"),
            }
        }
    });
    (address, head_seen, engine)
}

fn bounded<T>(call: WorkerCall<T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(10), call)
                .await
                .expect("bounded client task")
        })
        .expect("client task completes")
}

/// Runs the client task on a `ManagedWorker` and on a `HostedWorker` and
/// returns both results.
fn on_workers<F, T>(factory: F) -> [T; 2]
where
    F: FnOnce(WorkerContext) -> Pin<Box<dyn Future<Output = T> + 'static>> + Clone + Send + 'static,
    T: Send + 'static,
{
    let worker = ManagedWorker::new(64).expect("client worker starts");
    let managed = bounded(
        worker
            .handle()
            .call_local(factory.clone())
            .expect("client task is admitted"),
    );
    worker.shutdown().expect("client worker shuts down");

    let worker = HostedWorker::spawn(HostedWorkerOptions {
        command_capacity: NonZeroUsize::new(64).expect("nonzero capacity"),
        thread_name: "non-abortive-client".to_owned(),
    })
    .expect("hosted client worker starts");
    let hosted = bounded(
        worker
            .handle()
            .call_local(factory)
            .expect("client task is admitted"),
    );
    let completion = worker.completion();
    drop(worker);
    assert!(
        matches!(*completion.borrow(), Some(Ok(()))),
        "the hosted worker joins cleanly"
    );
    [managed, hosted]
}

fn session_params(non_abortive_close: bool) -> SessionParams {
    SessionParams {
        stream_read_timeout: Some(Duration::ZERO),
        connection_read_timeout: Some(Duration::ZERO),
        write_timeout: Some(Duration::ZERO),
        non_abortive_close,
        ..SessionParams::default()
    }
}

/// Sends a GET through an `HttpSessionPool`, drops its response before the
/// engine answers (as a router does when its client cancels), drains the pool,
/// and returns how the engine saw the connection end, on both executors.
fn dropped_response_before_headers(non_abortive_close: bool) -> [EngineSaw; 2] {
    on_workers(move |context| {
        Box::pin(async move {
            let (address, head_seen, engine) = silent_engine();
            let pool = HttpSessionPool::new(
                context,
                SocketAddress::new(address.ip().to_string(), address.port()),
                PoolParams {
                    connect_timeout: Duration::from_secs(1),
                    ..PoolParams::default()
                },
                ConnectionParams::default(),
                session_params(non_abortive_close),
            );
            let GetSessionResult {
                reservation,
                session,
            } = pool
                .get_session_with_reservation(CancellationToken::never())
                .await
                .expect("acquisition succeeds");
            let request =
                HttpFixedSource::make_get_request(b"/", None).expect("test request is valid");
            let response = session
                .send_reserved_request(
                    HttpSourceHolder::new(Box::new(request)),
                    reservation,
                    CancellationToken::never(),
                )
                .await
                .expect("request is sent");
            while !head_seen.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            drop(response);
            let _ = session.wait_closed().await;
            let _ = pool.drain_and_join().await;
            engine.join().expect("engine finishes")
        })
    })
}

#[test]
fn non_abortive_close_is_off_by_default() {
    assert!(!SessionParams::default().non_abortive_close);
}

/// Pins the default: the session resets the connection with `SO_LINGER` zero.
#[test]
fn a_dropped_response_resets_the_engine_connection_by_default() {
    assert_eq!(
        dropped_response_before_headers(false),
        [EngineSaw::Reset, EngineSaw::Reset]
    );
}

/// As hyper and reqwest, which drop the connection of a dropped response: the
/// engine sees FIN.
#[test]
fn a_dropped_response_closes_the_engine_connection_with_fin_when_enabled() {
    assert_eq!(
        dropped_response_before_headers(true),
        [EngineSaw::Fin, EngineSaw::Fin]
    );
}

/// The TLS adapter's own failure paths still reset, so a TLS connection refuses
/// the option before it dials.
#[test]
fn non_abortive_close_refuses_a_tls_connection() {
    let [managed, hosted] = on_workers(|context| {
        Box::pin(async move {
            let pool = HttpSessionPool::new(
                context,
                SocketAddress::new("127.0.0.1", 9),
                PoolParams {
                    connect_timeout: Duration::from_secs(1),
                    ..PoolParams::default()
                },
                ConnectionParams {
                    transport: ClientTransportOptions::Tls(TlsClientOptions::new("localhost")),
                    ..ConnectionParams::default()
                },
                session_params(true),
            );
            let error = pool
                .get_session_with_reservation(CancellationToken::never())
                .await
                .err()
                .expect("the TLS connection is refused");
            let _ = pool.drain_and_join().await;
            match error {
                PoolError::ConnectFailed { source } => source.code(),
                other => panic!("unexpected pool error: {other}"),
            }
        })
    });
    assert_eq!(managed, HttpErrorCode::INVALID_STATE_TRANSITION);
    assert_eq!(hosted, HttpErrorCode::INVALID_STATE_TRANSITION);
}

/// An attached transport that the caller declares TLS refuses the option too.
#[test]
fn non_abortive_close_refuses_an_attached_tls_transport() {
    let [managed, hosted] = on_workers(|context| {
        Box::pin(async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("listener");
            let (client, _accepted) = tokio::join!(
                tokio::net::TcpStream::connect(listener.local_addr().expect("address")),
                listener.accept()
            );
            let transport = adopt_local(
                &context,
                EstablishedIo::new(
                    client.expect("client connects"),
                    TransportEndpoint::Unknown,
                    TransportEndpoint::Unknown,
                    None,
                ),
                ByteStreamLimits::new(64 * 1024, 64 * 1024, 8).expect("bounded transport"),
            )
            .expect("I/O adopted");
            let mut options =
                ClientAttachOptions::new(CodecProtocol::Http11, TransportSecurity::Tls);
            options.session = session_params(true);
            ClientSession::attach_transport(&context, transport, options)
                .err()
                .expect("the attachment is refused")
                .code()
        })
    });
    assert_eq!(managed, HttpErrorCode::INVALID_STATE_TRANSITION);
    assert_eq!(hosted, HttpErrorCode::INVALID_STATE_TRANSITION);
}
