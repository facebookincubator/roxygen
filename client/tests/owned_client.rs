//! The experimental owned client session (`DIV-046`), over loopback TCP and
//! adopted transports, against a hyper origin and scripted peers, on a
//! `RuntimeOwner` worker that must quiesce without forcing.

use std::cell::Cell;
use std::convert::Infallible;
use std::future::Future;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Write;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::net::TcpStream;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::mpsc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::BodyExt;
use http_body_util::Full;
use http_body_util::StreamBody;
use http_body_util::combinators::BoxBody;
use hyper::body::Frame;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;
use roxygen_codec::CodecSetting;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpBodyEventKind;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::http::CodecProtocol;
use roxygen_core::http::HttpHeaderSize;
use roxygen_core::http::HttpMethod;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_http2::hpack::HeaderCodecStats;
use roxygen_http2::hpack::HeaderIndexingStrategy;
use roxygen_http2::hpack::HeaderName;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::managed::RuntimeConfig;
use roxygen_runtime::managed::RuntimeHandle;
use roxygen_runtime::managed::RuntimeOwner;
use roxygen_runtime::worker::WorkerContext;
use roxygen_session::experimental::OwnedRequestProgressExt;
use roxygen_session::experimental::PingProbeConfig;
use roxygen_session::http2::stats::Http2SessionStats;
use roxygen_session::session::LifecycleObserver;
use roxygen_session::session::RequestReservation;
use roxygen_transport::byte_stream::ByteStreamLimits;
use roxygen_transport::established::EstablishedIo;
use roxygen_transport::established::adopt_local;
use roxygen_transport::tcp::TcpTransport;
use roxygen_transport::tls::TlsClientOptions;

use crate::connector::ClientAttachOptions;
use crate::connector::ClientSession;
use crate::connector::ClientTransportOptions;
use crate::connector::ConnectionParams;
use crate::connector::HttpConnector;
use crate::connector::SessionParams;
use crate::experimental::owned::OwnedClientReservation;
use crate::experimental::owned::OwnedClientSession;

const TIMEOUT: Duration = Duration::from_secs(10);
const UPLOAD: usize = 128 * 1024;
const LARGE: usize = 300_000;
const ENABLE_PUSH: u16 = 2;
const MAX_CONCURRENT_STREAMS: u16 = 3;

fn wait<Work: Future>(work: Work) -> Work::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
        .block_on(async {
            tokio::time::timeout(TIMEOUT, work)
                .await
                .expect("bounded scenario")
        })
}

/// Runs `factory` on a one-worker runtime, then quiesces it: every session
/// the scenario left must close without a forced worker.
fn run_on_worker<Output, Factory, Work>(factory: Factory) -> Output
where
    Output: Send + 'static,
    Factory: FnOnce(WorkerContext) -> Work + Send + 'static,
    Work: Future<Output = Output> + 'static,
{
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 4,
    })
    .expect("owner starts");
    let runtime = owner.handle();
    let worker = runtime.worker(0).expect("worker exists");
    let result =
        wait(worker.call_local(factory).expect("scenario admitted")).expect("scenario ends");
    wait(runtime.quiesce(TIMEOUT)).expect("all scopes joined");
    assert_eq!(owner.join().expect("physical join").workers_forced, 0);
    result
}

/// Every session timer disabled explicitly.
fn untimed() -> SessionParams {
    SessionParams {
        stream_read_timeout: Some(Duration::ZERO),
        connection_read_timeout: Some(Duration::ZERO),
        write_timeout: Some(Duration::ZERO),
        ..SessionParams::default()
    }
}

fn plaintext(protocol: CodecProtocol) -> ConnectionParams {
    ConnectionParams {
        plaintext_protocol: protocol,
        ..ConnectionParams::default()
    }
}

fn socket_address(address: SocketAddr) -> SocketAddress {
    SocketAddress::new(address.ip().to_string(), address.port())
}

fn endpoint(address: SocketAddr) -> TransportEndpoint {
    TransportEndpoint::Socket(socket_address(address))
}

/// Polls `ready` every millisecond until it holds.
async fn until(mut ready: impl FnMut() -> bool) {
    while !ready() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Waits for a message from a peer thread.
async fn received<T>(channel: &mpsc::Receiver<T>) -> T {
    loop {
        match channel.try_recv() {
            Ok(value) => return value,
            Err(mpsc::TryRecvError::Empty) => tokio::time::sleep(Duration::from_millis(1)).await,
            Err(mpsc::TryRecvError::Disconnected) => panic!("the peer thread ended early"),
        }
    }
}

type OriginBody = BoxBody<Bytes, Infallible>;

fn full(body: impl Into<Bytes>) -> OriginBody {
    Full::new(body.into()).boxed()
}

fn large_body() -> Vec<u8> {
    (0..LARGE).map(|index| (index % 251) as u8).collect()
}

/// The origin's routes, one per exchange of the differential table.
async fn respond(
    request: hyper::Request<Incoming>,
) -> Result<hyper::Response<OriginBody>, Infallible> {
    let response = hyper::Response::builder();
    let response = match request.uri().path() {
        // hyper's HTTP/2 server would send a HEAD response's body.
        "/fixed" if request.method() == hyper::Method::HEAD => response
            .status(200)
            .header("x-route", "fixed")
            .header("content-length", "10")
            .body(full(Bytes::new())),
        "/fixed" => response
            .status(200)
            .header("x-route", "fixed")
            .body(full("fixed body")),
        "/echo" => {
            let method = request.method().to_string();
            let body = request
                .into_body()
                .collect()
                .await
                .expect("the upload arrives")
                .to_bytes();
            response
                .status(201)
                .header("x-method", method)
                .header("x-request-bytes", body.len().to_string())
                .body(full(body))
        }
        "/stream" => {
            let mut trailers = hyper::HeaderMap::new();
            trailers.insert("x-trailer", hyper::header::HeaderValue::from_static("done"));
            let frames: Vec<Result<Frame<Bytes>, Infallible>> = vec![
                Ok(Frame::data(Bytes::from_static(b"one"))),
                Ok(Frame::data(Bytes::from_static(b"two"))),
                Ok(Frame::data(Bytes::from_static(b"three"))),
                Ok(Frame::trailers(trailers)),
            ];
            // hyper's HTTP/1.1 server sends only the trailers `Trailer` names.
            response
                .status(200)
                .header("trailer", "x-trailer")
                .body(StreamBody::new(futures::stream::iter(frames)).boxed())
        }
        "/empty" => response.status(204).body(full(Bytes::new())),
        "/large" => response.status(200).body(full(large_body())),
        "/hang" => return std::future::pending().await,
        _ => response.status(404).body(full(Bytes::new())),
    };
    Ok(response.expect("valid origin response"))
}

async fn serve(stream: tokio::net::TcpStream, protocol: CodecProtocol) {
    let io = TokioIo::new(stream);
    // A connection the client resets ends with an error no oracle reads.
    if protocol == CodecProtocol::Http2 {
        let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
            .serve_connection(io, service_fn(respond))
            .await;
    } else {
        let _ = hyper::server::conn::http1::Builder::new()
            .serve_connection(io, service_fn(respond))
            .await;
    }
}

/// A hyper origin on its own thread, serving `protocol` (HTTP/2 with prior
/// knowledge) on loopback until dropped.
struct Origin {
    address: SocketAddr,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Origin {
    fn start(protocol: CodecProtocol) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("origin binds");
        listener.set_nonblocking(true).expect("nonblocking origin");
        let address = listener.local_addr().expect("origin address");
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let thread = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("origin runtime")
                .block_on(async move {
                    let listener =
                        tokio::net::TcpListener::from_std(listener).expect("origin listener");
                    let mut connections = tokio::task::JoinSet::new();
                    tokio::pin!(stopped);
                    loop {
                        tokio::select! {
                            _ = &mut stopped => break,
                            accepted = listener.accept() => {
                                let (stream, _) = accepted.expect("origin accepts");
                                connections.spawn(serve(stream, protocol));
                            }
                        }
                    }
                    connections.shutdown().await;
                });
        });
        Self {
            address,
            stop: Some(stop),
            thread: Some(thread),
        }
    }
}

impl Drop for Origin {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// One exchange of the differential table.
#[derive(Clone, Copy, Debug)]
enum Exchange {
    Fixed,
    Echo,
    Stream,
    Empty,
    Head,
    Large,
    Hang,
}

impl Exchange {
    const TABLE: [Self; 6] = [
        Self::Fixed,
        Self::Echo,
        Self::Stream,
        Self::Empty,
        Self::Head,
        Self::Large,
    ];

    /// The request's head, with `Host` (and `TE: trailers` for the trailered
    /// route), and the rest of its source.
    async fn request(self) -> (HttpHeaderEvent, HttpSourceHolder) {
        let (method, path, body) = match self {
            Self::Fixed => (HttpMethod::Get, "/fixed", None),
            Self::Echo => (HttpMethod::Post, "/echo", Some(vec![0x5a; UPLOAD])),
            Self::Stream => (HttpMethod::Get, "/stream", None),
            Self::Empty => (HttpMethod::Get, "/empty", None),
            Self::Head => (HttpMethod::Head, "/fixed", None),
            Self::Large => (HttpMethod::Get, "/large", None),
            Self::Hang => (HttpMethod::Get, "/hang", None),
        };
        let body = body.map(|bytes: Vec<u8>| {
            BufQueue::copy_from_slice(&bytes, bytes.len()).expect("bounded upload")
        });
        let mut source = HttpSourceHolder::new(Box::new(
            HttpFixedSource::make_fixed_request(path, method, body).expect("valid request"),
        ));
        let mut head = source.read_header_event().await.expect("request head");
        let headers = head.headers_mut().headers_mut();
        headers.add("host", "origin.example");
        if matches!(self, Self::Stream) {
            headers.add("te", "trailers");
        }
        let body = if head.eom() {
            HttpSourceHolder::empty()
        } else {
            source
        };
        (head, body)
    }
}

/// A response as a reader sees it, without the origin's `Date`.
#[derive(Debug, Default, PartialEq)]
struct Received {
    status: Option<u16>,
    headers: Vec<(Vec<u8>, Vec<u8>)>,
    body: Vec<u8>,
    trailers: Vec<(Vec<u8>, Vec<u8>)>,
    error: Option<HttpErrorCode>,
    sequence: u64,
}

async fn read(source: &mut HttpSourceHolder) -> Received {
    let mut received = Received::default();
    let head = match source.read_header_event().await {
        Ok(head) => head,
        Err(error) => {
            received.error = Some(error.code());
            return received;
        }
    };
    received.status = head.headers().response_status();
    head.headers().headers().for_each(|name, value| {
        if !name.eq_ignore_ascii_case(b"date") {
            received.headers.push((name.to_vec(), value.to_vec()));
        }
    });
    let mut eom = head.eom();
    while !eom {
        match source.read_body_event(u32::MAX).await {
            Ok(event) => {
                if let Some(bytes) = event.as_body() {
                    received.body.extend_from_slice(
                        &bytes.coalesce(bytes.chain_length()).expect("bounded body"),
                    );
                }
                if let HttpBodyEventKind::Trailers(trailers) = event.kind() {
                    trailers.for_each(|name, value| {
                        received.trailers.push((name.to_vec(), value.to_vec()));
                    });
                }
                eom = event.eom();
            }
            Err(error) => {
                received.error = Some(error.code());
                break;
            }
        }
    }
    received
}

async fn reserve_uniplex(session: &ClientSession) -> RequestReservation {
    loop {
        if let Ok(reservation) = session.reserve_request() {
            return reservation;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

async fn reserve_owned(session: &OwnedClientSession) -> OwnedClientReservation {
    loop {
        if let Ok(reservation) = session.reserve_request() {
            return reservation;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

async fn connect_owned(
    context: &WorkerContext,
    address: SocketAddr,
    protocol: CodecProtocol,
    session: SessionParams,
) -> OwnedClientSession {
    OwnedClientSession::connect(
        context,
        socket_address(address),
        TIMEOUT,
        plaintext(protocol),
        session,
        CancellationToken::never(),
    )
    .await
    .expect("owned session connects")
}

/// One exchange on an owned session, read to its end.
async fn exchange(session: &OwnedClientSession, exchange: Exchange) -> Received {
    let (head, body) = exchange.request().await;
    let response = reserve_owned(session)
        .await
        .submit(head, body, CancellationToken::never())
        .await
        .expect("request is committed");
    let mut source = response.source.into_source_owner().into_source_holder();
    let mut received = read(&mut source).await;
    received.sequence = response.sequence;
    received
}

async fn close(session: &OwnedClientSession) {
    session.close_when_idle().expect("owned session drains");
    session.wait_closed().await.expect("owned session closes");
}

/// Every exchange of the table, in order, on one uniplex `ClientSession`.
async fn through_client_session(
    context: &WorkerContext,
    address: SocketAddr,
    protocol: CodecProtocol,
) -> Vec<Received> {
    let session = HttpConnector::connect(
        context,
        socket_address(address),
        TIMEOUT,
        plaintext(protocol),
        untimed(),
        CancellationToken::never(),
    )
    .await
    .expect("client session connects");
    let mut table = Vec::new();
    for exchange in Exchange::TABLE {
        let (head, body) = exchange.request().await;
        let reservation = reserve_uniplex(&session).await;
        let mut response = session
            .send_reserved_request_with_headers(head, body, reservation, CancellationToken::never())
            .expect("request is committed");
        let mut received = read(&mut response.source).await;
        received.sequence = response.sequence_number;
        table.push(received);
    }
    session.close_when_idle().expect("client session drains");
    session.wait_closed().await.expect("client session closes");
    table
}

/// The same exchanges on one owned session.
async fn through_owned_session(
    context: &WorkerContext,
    address: SocketAddr,
    protocol: CodecProtocol,
) -> Vec<Received> {
    let session = connect_owned(context, address, protocol, untimed()).await;
    let mut table = Vec::new();
    for entry in Exchange::TABLE {
        table.push(exchange(&session, entry).await);
    }
    close(&session).await;
    table
}

#[test]
fn responses_equal_the_uniplex_client_session_s_over_http1_and_http2() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let (uniplex, owned) = run_on_worker(move |context| async move {
            (
                through_client_session(&context, address, protocol).await,
                through_owned_session(&context, address, protocol).await,
            )
        });
        assert_eq!(owned, uniplex, "{protocol:?}");
        assert_eq!(
            owned
                .iter()
                .map(|received| (
                    received.status,
                    received.body.len(),
                    received.trailers.len(),
                    received.error,
                    received.sequence,
                ))
                .collect::<Vec<_>>(),
            [
                (Some(200), 10, 0, None, 0),
                (Some(201), UPLOAD, 0, None, 1),
                (Some(200), 11, 1, None, 2),
                (Some(204), 0, 0, None, 3),
                (Some(200), 0, 0, None, 4),
                (Some(200), LARGE, 0, None, 5),
            ],
            "{protocol:?}: every exchange of the table is served"
        );
    }
}

#[test]
fn an_attached_transport_serves_exchanges_and_reports_its_connection() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let observed = run_on_worker(move |context| async move {
            let stream = tokio::net::TcpStream::connect(address)
                .await
                .expect("application connector");
            let local = endpoint(stream.local_addr().expect("local address"));
            let transport = adopt_local(
                &context,
                EstablishedIo::new(stream, local.clone(), endpoint(address), None),
                ByteStreamLimits::new(4096, 4096, 8).expect("bounded transport"),
            )
            .expect("I/O adopted");
            let mut options = ClientAttachOptions::new(protocol, TransportSecurity::Plaintext);
            options.session = untimed();
            let session = OwnedClientSession::attach(&context, transport, options)
                .expect("owned session attaches");
            let metadata = session.connection_context();
            let received = exchange(&session, Exchange::Large).await;
            let observed = (
                session.protocol(),
                metadata.local_endpoint() == &local,
                metadata.peer_endpoint() == &endpoint(address),
                metadata.security(),
                received.status,
                received.body == large_body(),
            );
            close(&session).await;
            observed
        });
        assert_eq!(
            observed,
            (
                protocol,
                true,
                true,
                TransportSecurity::Plaintext,
                Some(200),
                true
            )
        );
    }
}

#[test]
fn owned_http2_accepts_default_and_explicit_session_timers() {
    let origin = Origin::start(CodecProtocol::Http2);
    let address = origin.address;
    run_on_worker(move |context| async move {
        for params in [
            SessionParams::default(),
            SessionParams {
                stream_read_timeout: Some(TIMEOUT),
                connection_read_timeout: Some(TIMEOUT),
                write_timeout: Some(TIMEOUT),
                ..SessionParams::default()
            },
        ] {
            for attached in [false, true] {
                let session = if attached {
                    attach_to_origin(
                        &context,
                        address,
                        CodecProtocol::Http2,
                        params.clone(),
                        CancellationToken::never(),
                    )
                    .await
                } else {
                    connect_owned(&context, address, CodecProtocol::Http2, params.clone()).await
                };
                let response = exchange(&session, Exchange::Fixed).await;
                assert_eq!(response.status, Some(200));
                assert_eq!(response.body, b"fixed body");
                assert_eq!(response.error, None);
                close(&session).await;
            }
        }
    });
}

#[test]
fn owned_http2_times_out_a_response_head_and_keeps_the_connection_usable() {
    let origin = Origin::start(CodecProtocol::Http2);
    let address = origin.address;
    run_on_worker(move |context| async move {
        for attached in [false, true] {
            let params = SessionParams {
                stream_read_timeout: Some(Duration::from_secs(1)),
                connection_read_timeout: Some(TIMEOUT),
                write_timeout: Some(TIMEOUT),
                ..SessionParams::default()
            };
            let session = if attached {
                attach_to_origin(
                    &context,
                    address,
                    CodecProtocol::Http2,
                    params,
                    CancellationToken::never(),
                )
                .await
            } else {
                connect_owned(&context, address, CodecProtocol::Http2, params).await
            };
            let stalled = exchange(&session, Exchange::Hang).await;
            assert_eq!(stalled.status, None);
            assert_eq!(stalled.error, Some(HttpErrorCode::READ_TIMEOUT));
            let next = exchange(&session, Exchange::Fixed).await;
            assert_eq!(next.status, Some(200));
            assert_eq!(next.body, b"fixed body");
            assert_eq!(next.error, None);
            close(&session).await;
        }
    });
}

#[test]
fn http1_capacity_is_one() {
    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let observed = run_on_worker(move |context| async move {
        let session = connect_owned(&context, address, CodecProtocol::Http11, untimed()).await;
        let mut capacities = vec![session.available_capacity()];
        let mut idle = vec![session.is_idle()];
        let reservation = session.reserve_request().expect("the slot is free");
        capacities.push(session.available_capacity());
        idle.push(session.is_idle());
        let refused = session.reserve_request().err().map(|error| error.code());
        let (head, body) = Exchange::Fixed.request().await;
        let response = reservation
            .submit(head, body, CancellationToken::never())
            .await
            .expect("request is committed");
        let status = read(&mut response.source.into_source_owner().into_source_holder())
            .await
            .status;
        until(|| session.is_idle()).await;
        capacities.push(session.available_capacity());
        close(&session).await;
        (capacities, idle, refused, status)
    });
    assert_eq!(
        observed,
        (
            vec![1, 0, 1],
            vec![true, false],
            Some(HttpErrorCode::REFUSED_STREAM),
            Some(200)
        ),
        "the slot is taken while reserved and returned once the exchange ends"
    );
}

fn frame(kind: u8, flags: u8, payload: &[u8]) -> Vec<u8> {
    let length = u32::try_from(payload.len())
        .expect("small frame")
        .to_be_bytes();
    let mut frame = vec![length[1], length[2], length[3], kind, flags, 0, 0, 0, 0];
    frame.extend_from_slice(payload);
    frame
}

/// Reads one frame: its type, flags and payload; `None` at EOF or a reset.
fn read_frame(stream: &mut TcpStream) -> Option<(u8, u8, Vec<u8>)> {
    let mut header = [0; 9];
    stream.read_exact(&mut header).ok()?;
    let length = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
    let mut payload = vec![0; length];
    stream.read_exact(&mut payload).ok()?;
    Some((header[3], header[4], payload))
}

const SETTINGS: u8 = 4;
const GOAWAY: u8 = 7;
const ACK: u8 = 1;

fn settings_list(payload: &[u8]) -> Vec<(u16, u32)> {
    payload
        .as_chunks::<6>()
        .0
        .iter()
        .map(|setting| {
            (
                u16::from_be_bytes([setting[0], setting[1]]),
                u32::from_be_bytes([setting[2], setting[3], setting[4], setting[5]]),
            )
        })
        .collect()
}

/// A raw HTTP/2 peer: it reads the client's preface and SETTINGS, then, for
/// each limit `advertise` sends, sends SETTINGS with that
/// `MAX_CONCURRENT_STREAMS` and reports when the client acknowledged it. Once
/// `advertise` closes, it reads until the client's GOAWAY or EOF.
fn settings_peer() -> (SocketAddr, mpsc::Sender<u32>, mpsc::Receiver<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("peer binds");
    let address = listener.local_addr().expect("peer address");
    let (advertise, limits) = mpsc::channel::<u32>();
    let (acknowledged, acknowledgments) = mpsc::channel();
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("peer accepts");
        let mut preface = [0; 24];
        stream.read_exact(&mut preface).expect("client preface");
        while read_frame(&mut stream).expect("client SETTINGS").0 != SETTINGS {}
        stream
            .write_all(&frame(SETTINGS, ACK, &[]))
            .expect("peer acknowledges");
        for limit in limits {
            let mut payload = MAX_CONCURRENT_STREAMS.to_be_bytes().to_vec();
            payload.extend_from_slice(&limit.to_be_bytes());
            stream
                .write_all(&frame(SETTINGS, 0, &payload))
                .expect("peer advertises");
            while read_frame(&mut stream).expect("client acknowledgment")
                != (SETTINGS, ACK, Vec::new())
            {}
            acknowledged.send(()).expect("the client waits");
        }
        while let Some((kind, _, _)) = read_frame(&mut stream) {
            if kind == GOAWAY {
                break;
            }
        }
    });
    (address, advertise, acknowledgments)
}

/// The owned HTTP/2 session's capacity with `local` as its own outgoing
/// limit: before the peer's SETTINGS, after SETTINGS of 3 and of 5, with
/// every slot reserved, and once those reservations drop; and the refusal of
/// one reservation more.
fn http2_capacities(local: Option<u32>) -> (Vec<u32>, Option<HttpErrorCode>) {
    let (address, advertise, acknowledgments) = settings_peer();
    run_on_worker(move |context| async move {
        let session = connect_owned(
            &context,
            address,
            CodecProtocol::Http2,
            SessionParams {
                max_concurrent_outgoing_streams: local,
                ..untimed()
            },
        )
        .await;
        let mut capacities = vec![session.available_capacity()];
        for limit in [3, 5] {
            advertise.send(limit).expect("the peer listens");
            received(&acknowledgments).await;
            capacities.push(session.available_capacity());
        }
        let reservations: Vec<_> = (0..session.available_capacity())
            .map(|_| session.reserve_request().expect("a slot is free"))
            .collect();
        capacities.push(session.available_capacity());
        let refused = session.reserve_request().err().map(|error| error.code());
        drop(reservations);
        capacities.push(session.available_capacity());
        drop(advertise);
        close(&session).await;
        (capacities, refused)
    })
}

#[test]
fn http2_capacity_follows_the_peer_s_settings() {
    assert_eq!(
        [None, Some(4)].map(http2_capacities),
        [
            (vec![100, 3, 5, 0, 5], Some(HttpErrorCode::REFUSED_STREAM)),
            (vec![4, 3, 4, 0, 4], Some(HttpErrorCode::REFUSED_STREAM)),
        ]
    );
}

/// Reads one request head from `stream`; `None` at EOF first.
fn read_head(stream: &mut TcpStream) -> Option<Vec<u8>> {
    let mut input = Vec::new();
    let mut buffer = [0; 1024];
    while !input.windows(4).any(|window| window == b"\r\n\r\n") {
        match stream.read(&mut buffer) {
            Ok(0) | Err(_) => return None,
            Ok(read) => input.extend_from_slice(&buffer[..read]),
        }
    }
    Some(input)
}

#[test]
fn a_connection_close_response_finishes_the_session() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("engine binds");
    let address = listener.local_addr().expect("engine address");
    let engine = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("engine accepts");
        read_head(&mut stream).expect("request head");
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            .expect("engine answers");
        let mut rest = Vec::new();
        stream.read_to_end(&mut rest).expect("the client closes");
        rest
    });
    let observed = run_on_worker(move |context| async move {
        let session = connect_owned(&context, address, CodecProtocol::Http11, untimed()).await;
        let received = exchange(&session, Exchange::Fixed).await;
        let summary = session.wait_closed().await.expect("the session finishes");
        (
            received.status,
            received.body,
            summary.reset,
            session.is_finished(),
            session.available_capacity(),
        )
    });
    assert_eq!(observed, (Some(200), b"ok".to_vec(), false, true, 0));
    assert_eq!(
        engine.join().expect("engine ends"),
        b"",
        "no request follows"
    );
}

#[test]
fn cancelling_the_session_fails_its_exchange_and_closes_it() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let observed = run_on_worker(move |context| async move {
            let session = connect_owned(&context, address, protocol, untimed()).await;
            let (head, body) = Exchange::Hang.request().await;
            let response = reserve_owned(&session)
                .await
                .submit(head, body, CancellationToken::never())
                .await
                .expect("request is committed");
            let closed = session.wait_closed();
            session.cancel();
            let head_error = read(&mut response.source.into_source_owner().into_source_holder())
                .await
                .error;
            let outcome = closed.await.err().map(|error| error.code());
            (head_error, outcome, session.is_finished())
        });
        assert_eq!(
            observed,
            (
                Some(HttpErrorCode::CORO_CANCELLED),
                Some(HttpErrorCode::CORO_CANCELLED),
                true
            ),
            "{protocol:?}"
        );
    }
}

/// The first SETTINGS a client's HTTP/2 connection sends, read by a peer
/// that answers nothing, then the client cancels.
fn first_settings(owned: bool) -> Vec<(u16, u32)> {
    let listener = TcpListener::bind("127.0.0.1:0").expect("peer binds");
    let address = listener.local_addr().expect("peer address");
    let (report, reports) = mpsc::channel();
    let peer = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("peer accepts");
        let mut preface = [0; 24];
        stream.read_exact(&mut preface).expect("client preface");
        let (kind, flags, payload) = read_frame(&mut stream).expect("client SETTINGS");
        assert_eq!((kind, flags), (SETTINGS, 0), "the preface's SETTINGS");
        report
            .send(settings_list(&payload))
            .expect("the client waits");
        while read_frame(&mut stream).is_some() {}
    });
    let settings = run_on_worker(move |context| async move {
        if owned {
            let session = connect_owned(&context, address, CodecProtocol::Http2, untimed()).await;
            let settings = received(&reports).await;
            session.cancel();
            let _ = session.wait_closed().await;
            settings
        } else {
            let session = HttpConnector::connect(
                &context,
                socket_address(address),
                TIMEOUT,
                plaintext(CodecProtocol::Http2),
                untimed(),
                CancellationToken::never(),
            )
            .await
            .expect("client session connects");
            let settings = received(&reports).await;
            session.cancel();
            let _ = session.wait_closed().await;
            settings
        }
    });
    peer.join().expect("peer ends");
    settings
}

#[test]
fn the_owned_http2_session_disables_push_where_the_client_session_enables_it() {
    let uniplex = first_settings(false);
    let owned = first_settings(true);
    assert!(uniplex.contains(&(ENABLE_PUSH, 1)), "{uniplex:?}");
    assert!(owned.contains(&(ENABLE_PUSH, 0)), "{owned:?}");
    let without_push = uniplex
        .iter()
        .map(|&(identifier, value)| {
            if identifier == ENABLE_PUSH {
                (identifier, 0)
            } else {
                (identifier, value)
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(owned, without_push, "nothing else differs");
}

/// How the engine's read ends after an owned session with
/// `non_abortive_close` is cancelled while its request waits for a response:
/// a session that dialed, or one that attached a TCP transport its caller
/// dialed.
fn engine_read_after_cancel((attached, non_abortive_close): (bool, bool)) -> &'static str {
    let listener = TcpListener::bind("127.0.0.1:0").expect("engine binds");
    let address = listener.local_addr().expect("engine address");
    let (report, reports) = mpsc::channel();
    let engine = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("engine accepts");
        read_head(&mut stream).expect("request head");
        report.send(()).expect("the client waits");
        let mut buffer = [0; 64];
        match stream.read(&mut buffer) {
            Ok(0) => "fin",
            Err(error) if error.kind() == ErrorKind::ConnectionReset => "reset",
            other => panic!("unexpected engine read: {other:?}"),
        }
    });
    run_on_worker(move |context| async move {
        let session = SessionParams {
            non_abortive_close,
            ..untimed()
        };
        let session = if attached {
            let transport = TcpTransport::connect(
                &context,
                socket_address(address),
                ByteStreamLimits::new(64 * 1024, 64 * 1024, 8).expect("bounded transport"),
                TIMEOUT,
                CancellationToken::never(),
            )
            .await
            .expect("caller dials");
            let mut options =
                ClientAttachOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext);
            options.session = session;
            OwnedClientSession::attach(&context, transport, options)
                .expect("owned session attaches")
        } else {
            connect_owned(&context, address, CodecProtocol::Http11, session).await
        };
        let (head, body) = Exchange::Hang.request().await;
        let submission =
            reserve_owned(&session)
                .await
                .submit(head, body, CancellationToken::never());
        received(&reports).await;
        session.cancel();
        let _ = session.wait_closed().await;
        drop(submission);
    });
    engine.join().expect("engine ends")
}

#[test]
fn a_cancelled_session_resets_its_connection_by_default_and_ends_it_with_fin_when_non_abortive() {
    assert_eq!(
        [(false, false), (false, true), (true, false), (true, true)].map(engine_read_after_cancel),
        ["reset", "fin", "reset", "fin"],
        "connected, then attached; each without and with non_abortive_close"
    );
}

/// A submission's progress before the driver runs, its response's once
/// claimed, and whether the response's capacity was released once read.
#[test]
fn submissions_and_responses_report_request_progress() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let observed = run_on_worker(move |context| async move {
            let session = connect_owned(&context, address, protocol, untimed()).await;
            let mut observed = Vec::new();
            for _ in 0..2 {
                let (head, body) = Exchange::Fixed.request().await;
                let submission =
                    reserve_owned(&session)
                        .await
                        .submit(head, body, CancellationToken::never());
                let before = (
                    submission.request_unprocessed(),
                    submission.sequence_number(),
                );
                let response = submission.await.expect("request is committed");
                let claimed = (
                    response.request.request_unprocessed(),
                    response.request.sequence_number(),
                    response.sequence,
                );
                let status = read(&mut response.source.into_source_owner().into_source_holder())
                    .await
                    .status;
                until(|| response.request.capacity_released()).await;
                observed.push((before, claimed, status));
            }
            close(&session).await;
            observed
        });
        assert_eq!(
            observed,
            [
                ((false, None), (false, Some(0), 0), Some(200)),
                ((false, None), (false, Some(1), 1), Some(200)),
            ],
            "{protocol:?}"
        );
    }
}

/// A request reserved before `close_when_idle` is refused unprocessed when
/// submitted, and the drained session closes.
#[test]
fn a_drained_session_refuses_a_reserved_request_unprocessed() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let observed = run_on_worker(move |context| async move {
            let session = connect_owned(&context, address, protocol, untimed()).await;
            let reservation = reserve_owned(&session).await;
            session.close_when_idle().expect("owned session drains");
            let (head, body) = Exchange::Fixed.request().await;
            let submission = reservation.submit(head, body, CancellationToken::never());
            let progress = (
                submission.request_unprocessed(),
                submission.sequence_number(),
            );
            let refused = submission.await.err().map(|error| error.code());
            let closed = session.wait_closed().await.is_ok();
            (progress, refused, closed)
        });
        assert_eq!(
            observed,
            ((true, Some(0)), Some(HttpErrorCode::REFUSED_STREAM), true),
            "{protocol:?}"
        );
    }
}

/// A response-interest submission stays pending while the origin holds the
/// response head, and fails with the submission's cancel; on another session
/// it resolves at the origin's head.
#[test]
fn a_response_interest_submission_waits_for_the_response_head() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let observed = run_on_worker(move |context| async move {
            let session = connect_owned(&context, address, protocol, untimed()).await;
            let (head, body) = Exchange::Hang.request().await;
            let mut held = reserve_owned(&session).await.submit_with_response_interest(
                head,
                body,
                CancellationToken::never(),
                None,
            );
            let pending = tokio::time::timeout(Duration::from_millis(100), &mut held)
                .await
                .is_err();
            held.cancel(HttpError::new(HttpErrorCode::READ_TIMEOUT));
            let cancelled = held.await.err().map(|error| error.code());
            session.cancel();
            let _ = session.wait_closed().await;

            let session = connect_owned(&context, address, protocol, untimed()).await;
            let (head, body) = Exchange::Fixed.request().await;
            let response = reserve_owned(&session)
                .await
                .submit_with_response_interest(head, body, CancellationToken::never(), None)
                .await
                .expect("the response head arrives");
            let status = read(&mut response.source.into_source_owner().into_source_holder())
                .await
                .status;
            close(&session).await;
            (pending, cancelled, status)
        });
        assert_eq!(
            observed,
            (true, Some(HttpErrorCode::READ_TIMEOUT), Some(200)),
            "{protocol:?}"
        );
    }
}

/// Cancelling a claimed request fails its response with the cancel's error.
#[test]
fn cancelling_a_claimed_request_fails_its_response() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let observed = run_on_worker(move |context| async move {
            let session = connect_owned(&context, address, protocol, untimed()).await;
            let (head, body) = Exchange::Hang.request().await;
            let response = reserve_owned(&session)
                .await
                .submit(head, body, CancellationToken::never())
                .await
                .expect("request is committed");
            response
                .request
                .cancel(HttpError::new(HttpErrorCode::READ_TIMEOUT));
            let error = read(&mut response.source.into_source_owner().into_source_holder())
                .await
                .error;
            let _ = session.close_when_idle();
            let _ = session.wait_closed().await;
            error
        });
        assert_eq!(observed, Some(HttpErrorCode::READ_TIMEOUT), "{protocol:?}");
    }
}

struct QuietObserver;

impl LifecycleObserver for QuietObserver {}

struct QuietStats;

impl Http2SessionStats for QuietStats {}

impl HeaderCodecStats for QuietStats {
    fn record_encode(&self, _size: HttpHeaderSize) {}

    fn record_decode(&self, _size: HttpHeaderSize) {}

    fn record_decode_error(&self) {}

    fn record_decode_too_large(&self) {}
}

/// A parameter the owned client refuses on `protocol`, and the field its
/// error names.
struct Refused {
    protocol: CodecProtocol,
    session: SessionParams,
    field: &'static str,
}

fn refused() -> Vec<Refused> {
    let second = Some(Duration::from_secs(1));
    let probes = PingProbeConfig {
        interval: Duration::from_secs(1),
        timeout: Duration::from_secs(1),
        ..PingProbeConfig::default()
    };
    let mut table = Vec::new();
    for timeout in [None, second] {
        for (session, field) in [
            (
                SessionParams {
                    stream_read_timeout: timeout,
                    ..untimed()
                },
                "stream_read_timeout",
            ),
            (
                SessionParams {
                    connection_read_timeout: timeout,
                    ..untimed()
                },
                "connection_read_timeout",
            ),
            (
                SessionParams {
                    write_timeout: timeout,
                    ..untimed()
                },
                "write_timeout",
            ),
        ] {
            table.push(Refused {
                protocol: CodecProtocol::Http11,
                session,
                field,
            });
        }
    }
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        let cases = [
            (
                SessionParams {
                    lifecycle_observer: Some(Rc::new(QuietObserver)),
                    ..untimed()
                },
                "lifecycle_observer",
            ),
            (
                SessionParams {
                    session_stats: Some(Rc::new(QuietStats)),
                    ..untimed()
                },
                "session_stats",
            ),
            (
                SessionParams {
                    header_codec_stats: Some(Rc::new(QuietStats)),
                    ..untimed()
                },
                "header_codec_stats",
            ),
            (
                SessionParams {
                    ping_probes: Some(probes),
                    ..untimed()
                },
                "ping_probes",
            ),
        ];
        table.extend(cases.map(|(session, field)| Refused {
            protocol,
            session,
            field,
        }));
    }
    table.push(Refused {
        protocol: CodecProtocol::Http2,
        session: SessionParams {
            settings: vec![CodecSetting {
                identifier: ENABLE_PUSH,
                value: 1,
            }],
            ..untimed()
        },
        field: "ENABLE_PUSH",
    });
    table
}

/// A refusal as observed: the case, and the error's code and whether its
/// message names the field; `None` when the session started instead.
type Refusal = (CodecProtocol, &'static str, Option<(HttpErrorCode, bool)>);

fn refusal(
    protocol: CodecProtocol,
    field: &'static str,
    result: Result<OwnedClientSession, HttpError>,
) -> Refusal {
    match result {
        Ok(session) => {
            session.cancel();
            (protocol, field, None)
        }
        Err(error) => (
            protocol,
            field,
            Some((
                error.code(),
                String::from_utf8_lossy(error.message()).contains(field),
            )),
        ),
    }
}

fn refused_as(code: HttpErrorCode, observed: &[Refusal]) -> Vec<Refusal> {
    observed
        .iter()
        .map(|&(protocol, field, _)| (protocol, field, Some((code, true))))
        .collect()
}

#[test]
fn connect_refuses_what_the_owned_drivers_cannot_honor_before_dialing() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener binds");
    listener
        .set_nonblocking(true)
        .expect("nonblocking listener");
    let address = listener.local_addr().expect("listener address");
    let observed = run_on_worker(move |context| async move {
        let mut observed = Vec::new();
        for case in refused() {
            let result = OwnedClientSession::connect(
                &context,
                socket_address(address),
                TIMEOUT,
                plaintext(case.protocol),
                case.session,
                CancellationToken::never(),
            )
            .await;
            observed.push(refusal(case.protocol, case.field, result));
        }
        let tls = ConnectionParams {
            transport: ClientTransportOptions::Tls(TlsClientOptions::new("origin.example")),
            ..plaintext(CodecProtocol::Http11)
        };
        let result = OwnedClientSession::connect(
            &context,
            socket_address(address),
            TIMEOUT,
            tls,
            untimed(),
            CancellationToken::never(),
        )
        .await;
        observed.push(refusal(CodecProtocol::Http11, "TLS", result));
        observed
    });
    assert_eq!(observed.len(), 16, "every case ran");
    assert_eq!(
        observed,
        refused_as(HttpErrorCode::INVALID_STATE_TRANSITION, &observed)
    );
    assert_eq!(
        listener.accept().map(|_| ()).map_err(|error| error.kind()),
        Err(ErrorKind::WouldBlock),
        "no refused session dialed"
    );
}

#[test]
fn attach_refuses_what_the_owned_drivers_cannot_honor() {
    let (observed, unsupported) = run_on_worker(move |context| async move {
        let attach = |protocol, security, session| {
            let (client, _peer) = tokio::io::duplex(4096);
            let transport = adopt_local(
                &context,
                EstablishedIo::new(
                    client,
                    TransportEndpoint::Socket(SocketAddress::new("127.0.0.1", 1)),
                    TransportEndpoint::Socket(SocketAddress::new("127.0.0.1", 2)),
                    None,
                ),
                ByteStreamLimits::new(4096, 4096, 8).expect("bounded transport"),
            )
            .expect("I/O adopted");
            let mut options = ClientAttachOptions::new(protocol, security);
            options.session = session;
            OwnedClientSession::attach(&context, transport, options)
        };
        let mut observed: Vec<_> = refused()
            .into_iter()
            .map(|case| {
                let result = attach(case.protocol, TransportSecurity::Plaintext, case.session);
                refusal(case.protocol, case.field, result)
            })
            .collect();
        let result = attach(
            CodecProtocol::Http11,
            TransportSecurity::Tls,
            SessionParams {
                non_abortive_close: true,
                ..untimed()
            },
        );
        observed.push(refusal(CodecProtocol::Http11, "TLS", result));
        let unsupported = attach(CodecProtocol::Hq, TransportSecurity::Plaintext, untimed())
            .err()
            .map(|error| error.code());
        (observed, unsupported)
    });
    assert_eq!(observed.len(), 16, "every case ran");
    assert_eq!(
        observed,
        refused_as(HttpErrorCode::INVALID_STATE_TRANSITION, &observed)
    );
    assert_eq!(unsupported, Some(HttpErrorCode::CONNECT_ERROR));
}

/// Counts the indexing decisions an HPACK encoder asks for, and indexes
/// nothing.
#[derive(Clone)]
struct CountingStrategy(Rc<Cell<usize>>);

impl HeaderIndexingStrategy for CountingStrategy {
    fn index_header(&self, _name: &HeaderName, _value: &[u8], _name_exists: bool) -> bool {
        self.0.set(self.0.get() + 1);
        false
    }

    fn clone_box(&self) -> Box<dyn HeaderIndexingStrategy> {
        Box::new(self.clone())
    }
}

/// The router's session parameters (prod_inference `RoxygenClientParams`):
/// timers off; `deliver_ingress_before_abort` (`DIV-033`) on both protocols;
/// on HTTP/1.1 every other hyper-parity option; on HTTP/2 hyper's SETTINGS, no
/// local stream limit and hyper's connection window, with the HTTP/1.1-only
/// options also on, which HTTP/2 sessions ignore.
fn router_session(protocol: CodecProtocol, strategy: &CountingStrategy) -> SessionParams {
    let http2 = protocol == CodecProtocol::Http2;
    let setting = |identifier, value| CodecSetting { identifier, value };
    SessionParams {
        settings: if http2 {
            vec![
                setting(ENABLE_PUSH, 0),
                setting(4, 2 << 20),
                setting(5, 16_384),
                setting(6, 16_384),
            ]
        } else {
            Vec::new()
        },
        max_concurrent_outgoing_streams: http2.then_some(u32::MAX),
        connection_flow_control: http2.then_some(5 << 20),
        header_indexing_strategy: Some(Rc::new(strategy.clone())),
        deliver_ingress_before_abort: true,
        unframed_bodiless_http1_requests: true,
        non_abortive_close: true,
        ..untimed()
    }
}

/// The statuses of two exchanges and the indexing decisions they asked for.
type Served = (Vec<Option<u16>>, usize);

/// With the router's parameters, what a uniplex `ClientSession` serves, and
/// what an owned session serves or its refusal's code.
fn router_exchanges(protocol: CodecProtocol) -> (Served, Result<Served, HttpErrorCode>) {
    let origin = Origin::start(protocol);
    let address = origin.address;
    run_on_worker(move |context| async move {
        let uniplex = {
            let strategy = CountingStrategy(Rc::default());
            let session = HttpConnector::connect(
                &context,
                socket_address(address),
                TIMEOUT,
                plaintext(protocol),
                router_session(protocol, &strategy),
                CancellationToken::never(),
            )
            .await
            .expect("client session connects");
            let mut statuses = Vec::new();
            for exchange in [Exchange::Fixed, Exchange::Echo] {
                let (head, body) = exchange.request().await;
                let reservation = reserve_uniplex(&session).await;
                let mut response = session
                    .send_reserved_request_with_headers(
                        head,
                        body,
                        reservation,
                        CancellationToken::never(),
                    )
                    .expect("request is committed");
                statuses.push(read(&mut response.source).await.status);
            }
            session.close_when_idle().expect("client session drains");
            session.wait_closed().await.expect("client session closes");
            (statuses, strategy.0.get())
        };
        let strategy = CountingStrategy(Rc::default());
        let result = OwnedClientSession::connect(
            &context,
            socket_address(address),
            TIMEOUT,
            plaintext(protocol),
            router_session(protocol, &strategy),
            CancellationToken::never(),
        )
        .await;
        let owned = match result {
            Ok(session) => {
                let mut statuses = Vec::new();
                for entry in [Exchange::Fixed, Exchange::Echo] {
                    statuses.push(exchange(&session, entry).await.status);
                }
                close(&session).await;
                Ok((statuses, strategy.0.get()))
            }
            Err(error) => Err(error.code()),
        };
        (uniplex, owned)
    })
}

#[test]
fn the_router_s_session_parameters_are_accepted_and_honored() {
    let (uniplex, owned) = router_exchanges(CodecProtocol::Http11);
    assert_eq!(owned, Ok(uniplex), "HTTP/1.1");
    assert_eq!(
        owned,
        Ok((vec![Some(200), Some(201)], 0)),
        "HTTP/1.1 asks for no HPACK decision"
    );
    let (uniplex, owned) = router_exchanges(CodecProtocol::Http2);
    assert_eq!(owned, Ok(uniplex.clone()), "HTTP/2");
    assert_eq!(uniplex.0, [Some(200), Some(201)]);
    assert_ne!(uniplex.1, 0, "the indexing strategy is consulted");
}

/// One HTTP/2 frame on `stream`.
fn stream_frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
    let mut frame = frame(kind, flags, payload);
    frame[5..9].copy_from_slice(&stream.to_be_bytes());
    frame
}

/// An h2c peer that answers stream 1 with a 200 head and `first`, then resets
/// it with `CANCEL`, in one write, and reads until the client's GOAWAY or EOF.
fn reset_after_data_peer() -> (SocketAddr, std::thread::JoinHandle<()>) {
    const HEADERS: u8 = 1;
    const DATA: u8 = 0;
    const RST_STREAM: u8 = 3;
    const END_HEADERS: u8 = 4;
    let listener = TcpListener::bind("127.0.0.1:0").expect("peer binds");
    let address = listener.local_addr().expect("peer address");
    let peer = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("peer accepts");
        let mut preface = [0; 24];
        stream.read_exact(&mut preface).expect("client preface");
        stream
            .write_all(&frame(SETTINGS, 0, &[]))
            .expect("peer settings");
        while read_frame(&mut stream).expect("client request").0 != HEADERS {}
        let mut reply = frame(SETTINGS, ACK, &[]);
        // `:status: 200` as HPACK static index 8.
        reply.extend(stream_frame(HEADERS, END_HEADERS, 1, &[0x88]));
        reply.extend(stream_frame(DATA, 0, 1, b"first"));
        reply.extend(stream_frame(RST_STREAM, 0, 1, &8_u32.to_be_bytes()));
        stream.write_all(&reply).expect("peer replies");
        while let Some((kind, _, _)) = read_frame(&mut stream) {
            if kind == GOAWAY {
                break;
            }
        }
    });
    (address, peer)
}

/// Reads `source` once its response's ingress has ended, as a proxy reads an
/// upstream body only after it has sent its own client the head.
async fn read_after_ingress_ends(mut source: HttpSourceHolder) -> Received {
    let receipt = source
        .ingress_completion()
        .expect("a fresh response has an ingress receipt");
    let _ = receipt.wait().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    read(&mut source).await
}

/// A GET to `reset_after_data_peer` with `deliver_ingress_before_abort` set to
/// `enabled`, through a uniplex `ClientSession` and through an owned session.
fn reset_after_data_reads(enabled: bool) -> (Received, Received) {
    let (uniplex, owned, peers) = run_on_worker(move |context| async move {
        let session = SessionParams {
            deliver_ingress_before_abort: enabled,
            ..untimed()
        };
        let (address, peer) = reset_after_data_peer();
        let client = HttpConnector::connect(
            &context,
            socket_address(address),
            TIMEOUT,
            plaintext(CodecProtocol::Http2),
            session.clone(),
            CancellationToken::never(),
        )
        .await
        .expect("client session connects");
        let (head, body) = Exchange::Fixed.request().await;
        let reservation = reserve_uniplex(&client).await;
        let response = client
            .send_reserved_request_with_headers(head, body, reservation, CancellationToken::never())
            .expect("request is committed");
        let uniplex = read_after_ingress_ends(response.source).await;
        client.close_when_idle().expect("client session drains");
        client.wait_closed().await.expect("client session closes");

        let (address, owned_peer) = reset_after_data_peer();
        let owned = connect_owned(&context, address, CodecProtocol::Http2, session).await;
        let (head, body) = Exchange::Fixed.request().await;
        let response = reserve_owned(&owned)
            .await
            .submit(head, body, CancellationToken::never())
            .await
            .expect("request is committed");
        let owned_read =
            read_after_ingress_ends(response.source.into_source_owner().into_source_holder()).await;
        close(&owned).await;
        (uniplex, owned_read, [peer, owned_peer])
    });
    for peer in peers {
        peer.join().expect("peer finishes");
    }
    (uniplex, owned)
}

/// `DIV-033` on an owned HTTP/2 session: with the option on, a response reset
/// after part of it arrived yields the head and bytes received, then the
/// reset's error, as through a `ClientSession`; with it off, both lose them.
#[test]
fn an_owned_http2_reset_after_data_yields_the_data_first_when_enabled() {
    let (uniplex, owned) = reset_after_data_reads(true);
    assert_eq!(
        owned,
        Received {
            status: Some(200),
            body: b"first".to_vec(),
            error: Some(HttpErrorCode::CANCEL),
            ..Received::default()
        },
        "enabled: the head and `first`, then CANCEL"
    );
    assert_eq!(owned, uniplex, "enabled: as through a ClientSession");
    let (uniplex, owned) = reset_after_data_reads(false);
    assert_eq!(
        owned,
        Received {
            error: Some(HttpErrorCode::CANCEL),
            ..Received::default()
        },
        "disabled: only CANCEL"
    );
    assert_eq!(owned, uniplex, "disabled: as through a ClientSession");
}

/// Cancelling an attachment's token cancels the attached session: its pending
/// exchange and `wait_closed` fail `CORO_CANCELLED`.
#[test]
fn cancelling_the_attachment_cancels_the_session() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let observed = run_on_worker(move |context| async move {
            let stream = tokio::net::TcpStream::connect(address)
                .await
                .expect("application connector");
            let local = endpoint(stream.local_addr().expect("local address"));
            let transport = adopt_local(
                &context,
                EstablishedIo::new(stream, local, endpoint(address), None),
                ByteStreamLimits::new(64 * 1024, 64 * 1024, 8).expect("bounded transport"),
            )
            .expect("I/O adopted");
            let attachment = CancellationSource::new();
            let mut options = ClientAttachOptions::new(protocol, TransportSecurity::Plaintext);
            options.session = untimed();
            options.cancellation = attachment.token();
            let session = OwnedClientSession::attach(&context, transport, options)
                .expect("owned session attaches");
            let served = exchange(&session, Exchange::Fixed).await.status;
            let (head, body) = Exchange::Hang.request().await;
            let response = reserve_owned(&session)
                .await
                .submit(head, body, CancellationToken::never())
                .await
                .expect("request is committed");
            attachment.request_cancellation();
            let head_error = read(&mut response.source.into_source_owner().into_source_holder())
                .await
                .error;
            let closed = session.wait_closed().await.err().map(|error| error.code());
            (served, head_error, closed, session.is_finished())
        });
        assert_eq!(
            observed,
            (
                Some(200),
                Some(HttpErrorCode::CORO_CANCELLED),
                Some(HttpErrorCode::CORO_CANCELLED),
                true
            ),
            "{protocol:?}"
        );
    }
}

/// An idle owned session when its worker quiesces: what the scenario saw
/// (the exchange's status, whether `wait_closed` resolved `Ok`, and
/// `is_finished`), whether the quiesce completed in time, and how many
/// workers were forced.
fn quiesce_with_an_idle_session(
    protocol: CodecProtocol,
) -> (Option<(Option<u16>, bool, bool)>, bool, usize) {
    let origin = Origin::start(protocol);
    let address = origin.address;
    let (ready, readiness) = mpsc::channel();
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 4,
    })
    .expect("owner starts");
    let runtime = owner.handle();
    let worker = runtime.worker(0).expect("worker exists");
    let scenario = worker
        .call_local(move |context| async move {
            let session = connect_owned(&context, address, protocol, untimed()).await;
            let status = exchange(&session, Exchange::Fixed).await.status;
            until(|| session.is_idle()).await;
            ready.send(()).expect("the test waits");
            let closed = session.wait_closed().await.is_ok();
            (status, closed, session.is_finished())
        })
        .expect("scenario admitted");
    readiness
        .recv_timeout(TIMEOUT)
        .expect("the session is idle");
    let (outcome, quiesced) =
        wait(async { tokio::join!(scenario, runtime.quiesce(Duration::from_secs(2))) });
    let forced = owner.join().map_or(usize::MAX, |join| join.workers_forced);
    (outcome.ok(), quiesced.is_ok(), forced)
}

#[test]
fn worker_quiescence_closes_an_idle_session() {
    assert_eq!(
        [CodecProtocol::Http11, CodecProtocol::Http2].map(quiesce_with_an_idle_session),
        [
            (Some((Some(200), true, true)), true, 0),
            (Some((Some(200), true, true)), true, 0),
        ]
    );
}

/// Runs `factory` on a one-worker runtime, with a handle to that runtime so
/// the scenario can begin quiescence itself, then quiesces it: every session
/// the scenario left must close without a forced worker.
fn run_on_quiescing_worker<Output, Factory, Work>(factory: Factory) -> Output
where
    Output: Send + 'static,
    Factory: FnOnce(WorkerContext, RuntimeHandle) -> Work + Send + 'static,
    Work: Future<Output = Output> + 'static,
{
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 4,
    })
    .expect("owner starts");
    let runtime = owner.handle();
    let worker = runtime.worker(0).expect("worker exists");
    let scenario_runtime = runtime.clone();
    let result = wait(
        worker
            .call_local(move |context| factory(context, scenario_runtime))
            .expect("scenario admitted"),
    )
    .expect("scenario ends");
    wait(runtime.quiesce(TIMEOUT)).expect("all scopes joined");
    assert_eq!(owner.join().expect("physical join").workers_forced, 0);
    result
}

/// Attaches an owned session to a loopback connection to `address`, with
/// `cancellation` as the attachment's token.
async fn attach_to_origin(
    context: &WorkerContext,
    address: SocketAddr,
    protocol: CodecProtocol,
    session: SessionParams,
    cancellation: CancellationToken,
) -> OwnedClientSession {
    let stream = tokio::net::TcpStream::connect(address)
        .await
        .expect("application connector");
    let local = endpoint(stream.local_addr().expect("local address"));
    let transport = adopt_local(
        context,
        EstablishedIo::new(stream, local, endpoint(address), None),
        ByteStreamLimits::new(64 * 1024, 64 * 1024, 8).expect("bounded transport"),
    )
    .expect("I/O adopted");
    let mut options = ClientAttachOptions::new(protocol, TransportSecurity::Plaintext);
    options.session = session;
    options.cancellation = cancellation;
    OwnedClientSession::attach(context, transport, options).expect("owned session attaches")
}

/// Cancelling the attachment closes the session's admission at once, before
/// its run observes the cancellation, as `ClientSession::reserve_request`
/// refuses with `DROPPED`: (capacity before, capacity after, the reservation's
/// error, `wait_closed`'s error).
#[test]
fn attachment_cancellation_closes_admission_before_the_run_observes_it() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let observed = run_on_worker(move |context| async move {
            let attachment = CancellationSource::new();
            let session =
                attach_to_origin(&context, address, protocol, untimed(), attachment.token()).await;
            let open = session.available_capacity() > 0;
            attachment.request_cancellation();
            let capacity = session.available_capacity();
            let reserved = session.reserve_request().err().map(|error| error.code());
            let closed = session.wait_closed().await.err().map(|error| error.code());
            (open, capacity, reserved, closed)
        });
        assert_eq!(
            observed,
            (
                true,
                0,
                Some(HttpErrorCode::DROPPED),
                Some(HttpErrorCode::CORO_CANCELLED)
            ),
            "{protocol:?}"
        );
    }
}

/// Worker quiescence closes an idle session's admission at once, before its
/// run observes it, as `ClientSession::reserve_request` refuses with
/// `DROPPED`; the session then closes: (the exchange's status, capacity
/// before, capacity after, the reservation's error, whether `wait_closed`
/// resolved `Ok`).
#[test]
fn worker_quiescence_closes_admission_before_the_run_observes_it() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let observed = run_on_quiescing_worker(move |context, runtime| async move {
            let session = connect_owned(&context, address, protocol, untimed()).await;
            let status = exchange(&session, Exchange::Fixed).await.status;
            until(|| session.is_idle()).await;
            let open = session.available_capacity() > 0;
            drop(runtime.quiesce(TIMEOUT));
            assert!(!context.handle().is_accepting(), "quiescence has begun");
            let capacity = session.available_capacity();
            let reserved = session.reserve_request().err().map(|error| error.code());
            let closed = session.wait_closed().await.is_ok();
            (status, open, capacity, reserved, closed)
        });
        assert_eq!(
            observed,
            (Some(200), true, 0, Some(HttpErrorCode::DROPPED), true),
            "{protocol:?}"
        );
    }
}

/// A request body that panics when dropped, after requesting `cancel`'s
/// cancellation when it has one.
struct PanicOnDrop {
    body: HttpSourceHolder,
    cancel: Option<CancellationSource>,
}

impl HttpSource for PanicOnDrop {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        HttpSource::read_header_event(&mut self.body)
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        HttpSource::read_body_event(&mut self.body, maximum)
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        HttpSource::stop_reading(&mut self.body, error);
    }
}

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            cancel.request_cancellation();
        }
        if !std::thread::panicking() {
            panic!("the refused request's body panics when dropped");
        }
    }
}

fn panicking_body(body: HttpSourceHolder, cancel: Option<CancellationSource>) -> HttpSourceHolder {
    HttpSourceHolder::new(Box::new(PanicOnDrop { body, cancel }))
}

/// Queues an upload whose body panics when dropped on an idle `session`, then
/// begins its worker's quiescence before the run polls again, so the
/// quiescence drain refuses the queued request and drops that body. Returns
/// the refused submission's error code.
async fn queue_a_panicking_upload_then_quiesce(
    session: &OwnedClientSession,
    runtime: &RuntimeHandle,
    cancel: Option<CancellationSource>,
) -> Option<HttpErrorCode> {
    until(|| session.is_idle()).await;
    let (head, body) = Exchange::Echo.request().await;
    let submission = reserve_owned(session).await.submit(
        head,
        panicking_body(body, cancel),
        CancellationToken::never(),
    );
    drop(runtime.quiesce(TIMEOUT));
    submission.await.err().map(|error| error.code())
}

/// A body that panics as worker quiescence's drain refuses its queued request
/// is contained as `ClientSession` contains a drain callback's panic: the
/// session is cancelled, its run's I/O cleanup completes without a forced
/// worker, and `wait_closed` fails `INTERNAL_ERROR`: (the exchange's status,
/// the refused submission's error, `wait_closed`'s error and message,
/// `is_finished`).
#[test]
fn a_panic_in_the_quiescence_drain_cancels_the_session_and_completes_its_cleanup() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let observed = run_on_quiescing_worker(move |context, runtime| async move {
            let session = connect_owned(&context, address, protocol, untimed()).await;
            let status = exchange(&session, Exchange::Fixed).await.status;
            let refused = queue_a_panicking_upload_then_quiesce(&session, &runtime, None).await;
            let closed = session
                .wait_closed()
                .await
                .err()
                .map(|error| (error.code(), error.message().to_vec()));
            (status, refused, closed, session.is_finished())
        });
        assert_eq!(
            observed,
            (
                Some(200),
                Some(HttpErrorCode::REFUSED_STREAM),
                Some((
                    HttpErrorCode::INTERNAL_ERROR,
                    b"session drain callback panicked".to_vec()
                )),
                true
            ),
            "{protocol:?}"
        );
    }
}

/// A failed quiescence drain cancels the session without an event its pool hears, so the
/// session sets the selection-index flag its pool handed it at `bind_pool` (S6.9), as its
/// cancellation does; a drain that closes admission leaves the flag alone. The pool is already
/// gone, so only the session can set the flag: (the exchange's status, the flag after a clean
/// drain), then the same after a drain whose refused upload panics.
#[test]
fn a_failed_quiescence_drain_unsettles_its_pool_s_index() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        let observed = [false, true].map(|fail| {
            let origin = Origin::start(protocol);
            let address = origin.address;
            run_on_quiescing_worker(move |context, runtime| async move {
                let session = connect_owned(&context, address, protocol, untimed()).await;
                let unsettled = Rc::new(Cell::new(false));
                session.bind_pool(Weak::new(), Some(Rc::clone(&unsettled)));
                let status = exchange(&session, Exchange::Fixed).await.status;
                if fail {
                    queue_a_panicking_upload_then_quiesce(&session, &runtime, None).await;
                } else {
                    until(|| session.is_idle()).await;
                    drop(runtime.quiesce(TIMEOUT));
                }
                let _ = session.wait_closed().await;
                (status, unsettled.get())
            })
        });
        assert_eq!(
            observed,
            [(Some(200), false), (Some(200), true)],
            "{protocol:?}: (status, unsettled) after a clean drain, then a failed one"
        );
    }
}

/// An attachment cancellation requested inside worker quiescence's drain wins
/// over the drain's failure, as `ClientSession`'s attachment cancellation
/// wins over its quiescence: (the exchange's status, the refused submission's
/// error, `wait_closed`'s error).
#[test]
fn an_attachment_cancellation_wins_over_a_failed_quiescence_drain() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let observed = run_on_quiescing_worker(move |context, runtime| async move {
            let attachment = CancellationSource::new();
            let token = attachment.token();
            let session = attach_to_origin(&context, address, protocol, untimed(), token).await;
            let status = exchange(&session, Exchange::Fixed).await.status;
            let refused =
                queue_a_panicking_upload_then_quiesce(&session, &runtime, Some(attachment)).await;
            let closed = session.wait_closed().await.err().map(|error| error.code());
            (status, refused, closed)
        });
        assert_eq!(
            observed,
            (
                Some(200),
                Some(HttpErrorCode::REFUSED_STREAM),
                Some(HttpErrorCode::CORO_CANCELLED)
            ),
            "{protocol:?}"
        );
    }
}

/// Worker quiescence after an attachment cancellation leaves the session to
/// the cancellation, as `ClientSession`'s owner drains no cancelled session:
/// a queued request fails `CORO_CANCELLED` rather than being refused as
/// drained: (the exchange's status, the queued submission's error,
/// `wait_closed`'s error).
#[test]
fn quiescence_after_an_attachment_cancellation_does_not_drain_the_session() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let observed = run_on_quiescing_worker(move |context, runtime| async move {
            let attachment = CancellationSource::new();
            let session =
                attach_to_origin(&context, address, protocol, untimed(), attachment.token()).await;
            let status = exchange(&session, Exchange::Fixed).await.status;
            until(|| session.is_idle()).await;
            let (head, body) = Exchange::Fixed.request().await;
            let submission =
                reserve_owned(&session)
                    .await
                    .submit(head, body, CancellationToken::never());
            attachment.request_cancellation();
            drop(runtime.quiesce(TIMEOUT));
            let refused = submission.await.err().map(|error| error.code());
            let closed = session.wait_closed().await.err().map(|error| error.code());
            (status, refused, closed)
        });
        assert_eq!(
            observed,
            (
                Some(200),
                Some(HttpErrorCode::CORO_CANCELLED),
                Some(HttpErrorCode::CORO_CANCELLED)
            ),
            "{protocol:?}"
        );
    }
}

/// Requests its canceller's cancellation when its last clone drops, which is
/// when the owned HTTP/2 driver holding it is destroyed in its run's final
/// poll.
#[derive(Clone)]
struct CancelOnLastDrop {
    _canceller: Rc<Canceller>,
}

struct Canceller(CancellationSource);

impl Drop for Canceller {
    fn drop(&mut self) {
        self.0.request_cancellation();
    }
}

impl HeaderIndexingStrategy for CancelOnLastDrop {
    fn index_header(&self, _name: &HeaderName, _value: &[u8], _name_exists: bool) -> bool {
        false
    }

    fn clone_box(&self) -> Box<dyn HeaderIndexingStrategy> {
        Box::new(self.clone())
    }
}

/// An attachment cancellation requested during the run's final poll, as the
/// owned HTTP/2 driver destroys its header indexing strategy, fails
/// `wait_closed` `CORO_CANCELLED`, as `ClientSession`'s owner rechecks its
/// attachment after its run's cleanup: (the exchange's status, whether the
/// attachment was cancelled before the close and after it, `wait_closed`'s
/// error).
#[test]
fn an_attachment_cancellation_during_the_run_s_final_poll_fails_wait_closed() {
    let origin = Origin::start(CodecProtocol::Http2);
    let address = origin.address;
    let observed = run_on_worker(move |context| async move {
        let attachment = CancellationSource::new();
        let token = attachment.token();
        let session = SessionParams {
            header_indexing_strategy: Some(Rc::new(CancelOnLastDrop {
                _canceller: Rc::new(Canceller(attachment)),
            })),
            ..untimed()
        };
        let session = attach_to_origin(
            &context,
            address,
            CodecProtocol::Http2,
            session,
            token.clone(),
        )
        .await;
        let status = exchange(&session, Exchange::Fixed).await.status;
        let before = token.is_cancellation_requested();
        session.close_when_idle().expect("owned session drains");
        let closed = session.wait_closed().await.err().map(|error| error.code());
        (status, before, token.is_cancellation_requested(), closed)
    });
    assert_eq!(
        observed,
        (Some(200), false, true, Some(HttpErrorCode::CORO_CANCELLED))
    );
}
