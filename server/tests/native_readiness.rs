/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::convert::Infallible;
use std::error::Error as StdError;
use std::future::Future;
use std::future::poll_fn;
use std::future::ready;
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use http::HeaderMap;
use http::HeaderValue;
use http::Request;
use http::Response;
use http::Version;
use http::header::CONTENT_LENGTH;
use http::header::HOST;
use http::header::TE;
use http::header::TRAILER;
use http::header::TRANSFER_ENCODING;
use http_body::Body;
use http_body::Frame;
use http_body::SizeHint;
use hyper::body::Incoming;
use hyper::client::conn::http1;
use hyper::client::conn::http2;
use hyper::rt::Executor;
use hyper_util::rt::TokioIo;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::connection::ConnectionContext;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpBodyEventKind;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::events::ResumeStatus;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::headers::HttpHeaders;
use roxygen_core::http::CodecProtocol;
use roxygen_core::ids::StreamId;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_transport::tls::TlsServerOptions;
use rustls::ClientConfig;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::ServerName;
use thiserror::Error;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::net::TcpStream;
use tokio::runtime::Builder;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::task::JoinSet;
use tokio::time::timeout;
use tokio_rustls::TlsConnector;

use crate::acceptor::AcceptedConnectionObserver;
use crate::acceptor::ConnectionInfo;
use crate::server::HandlerFactory;
use crate::server::HttpServer;
use crate::server::HttpServerState;
use crate::server::ServerConfig;
use crate::server::StaticTlsCredentials;

const PAYLOAD_SIZE: usize = 256 * 1024;
const CHUNK_SIZE: usize = 4096;
const TASK_LIMIT: usize = 32;
const STEP_TIMEOUT: Duration = Duration::from_secs(10);
const CASE_TIMEOUT: Duration = Duration::from_secs(30);
const UPLOAD_TRAILERS: &str = "x-upload-check, x-upload-operation";
const RESPONSE_TRAILERS: &str = "x-response-check, x-response-operation";

type BoxError = Box<dyn StdError + Send + Sync>;
type TestResult<Output> = Result<Output, BoxError>;

#[derive(Debug, Error)]
enum FixtureError {
    #[error("wire assertion failed: {0}")]
    Assertion(&'static str),
    #[error("deadline exceeded: {0}")]
    Timeout(&'static str),
    #[error("{phase}: {source}")]
    Operation {
        phase: &'static str,
        source: BoxError,
    },
    #[error("{case:?}: {errors:?}")]
    Case {
        case: WireCase,
        errors: Vec<BoxError>,
    },
}

fn require(condition: bool, message: &'static str) -> TestResult<()> {
    if condition {
        Ok(())
    } else {
        Err(FixtureError::Assertion(message).into())
    }
}

async fn bounded<Output, Failure>(
    phase: &'static str,
    work: impl Future<Output = Result<Output, Failure>>,
) -> TestResult<Output>
where
    Failure: Into<BoxError>,
{
    timeout(STEP_TIMEOUT, work)
        .await
        .map_err(|_| FixtureError::Timeout(phase))?
        .map_err(|source| {
            FixtureError::Operation {
                phase,
                source: source.into(),
            }
            .into()
        })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Protocol {
    H1,
    H2,
}

impl Protocol {
    fn codec(self) -> CodecProtocol {
        match self {
            Self::H1 => CodecProtocol::Http11,
            Self::H2 => CodecProtocol::Http2,
        }
    }

    fn version(self) -> Version {
        match self {
            Self::H1 => Version::HTTP_11,
            Self::H2 => Version::HTTP_2,
        }
    }

    fn alpn(self) -> &'static [u8] {
        match self {
            Self::H1 => b"http/1.1",
            Self::H2 => b"h2",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct WireCase {
    protocol: Protocol,
    tls: bool,
}

const CASES: [WireCase; 4] = [
    WireCase {
        protocol: Protocol::H1,
        tls: false,
    },
    WireCase {
        protocol: Protocol::H2,
        tls: false,
    },
    WireCase {
        protocol: Protocol::H1,
        tls: true,
    },
    WireCase {
        protocol: Protocol::H2,
        tls: true,
    },
];

#[derive(Clone, Copy)]
enum Operation {
    First,
    Second,
}

impl Operation {
    fn index(self) -> usize {
        match self {
            Self::First => 0,
            Self::Second => 1,
        }
    }

    fn tag(self) -> &'static str {
        match self {
            Self::First => "first",
            Self::Second => "second",
        }
    }

    fn status(self) -> u16 {
        match self {
            Self::First => 201,
            Self::Second => 202,
        }
    }

    fn parse(path: &[u8]) -> TestResult<Self> {
        match path {
            b"/first" => Ok(Self::First),
            b"/second" => Ok(Self::Second),
            _ => Err(FixtureError::Assertion("unexpected native request path").into()),
        }
    }
}

fn payload(operation: Operation, response: bool) -> Bytes {
    let seed = operation.index() * 17 + usize::from(response) * 73;
    Bytes::from(
        (0..PAYLOAD_SIZE)
            .map(|index| ((index + seed) % 251) as u8)
            .collect::<Vec<_>>(),
    )
}

struct NativeGates {
    upload_seen: oneshot::Sender<()>,
    response_held: oneshot::Sender<()>,
    response_release: oneshot::Receiver<()>,
}

struct PeerSignals {
    operation: Operation,
    upload_seen: oneshot::Receiver<()>,
    response_prefix: oneshot::Sender<()>,
    completed: oneshot::Sender<()>,
}

struct WireControl {
    response_held: oneshot::Receiver<()>,
    response_prefix: oneshot::Receiver<()>,
    response_release: oneshot::Sender<()>,
    completed: oneshot::Receiver<()>,
}

fn gates(operation: Operation) -> (NativeGates, PeerSignals, WireControl) {
    let (upload_tx, upload_rx) = oneshot::channel();
    let (held_tx, held_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let (prefix_tx, prefix_rx) = oneshot::channel();
    let (completed_tx, completed_rx) = oneshot::channel();
    (
        NativeGates {
            upload_seen: upload_tx,
            response_held: held_tx,
            response_release: release_rx,
        },
        PeerSignals {
            operation,
            upload_seen: upload_rx,
            response_prefix: prefix_tx,
            completed: completed_tx,
        },
        WireControl {
            response_held: held_rx,
            response_prefix: prefix_rx,
            response_release: release_tx,
            completed: completed_rx,
        },
    )
}

struct RequestEvidence {
    connection: Arc<ConnectionContext>,
    stream: Option<StreamId>,
}

struct NativeState {
    case: WireCase,
    gates: Mutex<[Option<NativeGates>; 2]>,
    requests: Mutex<[Option<RequestEvidence>; 2]>,
    accepted: Mutex<[Option<ConnectionInfo>; 2]>,
    errors: Mutex<[Option<BoxError>; 2]>,
}

impl NativeState {
    fn failure(&self, error: BoxError) -> HttpError {
        let message = error.to_string();
        let mut errors = self.errors.lock().expect("native failure storage");
        let slot = errors
            .iter_mut()
            .find(|entry| entry.is_none())
            .expect("at most two failing native operations");
        *slot = Some(error);
        HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, message.as_bytes())
    }

    fn verify(&self, concurrent: bool) -> TestResult<()> {
        let accepted = self.accepted.lock().expect("accepted connection evidence");
        let connections = if concurrent && self.case.protocol == Protocol::H1 {
            2
        } else {
            1
        };
        require(
            accepted.iter().flatten().count() == connections,
            "actual native accepted connection count",
        )?;
        for connection in accepted.iter().flatten() {
            require(
                connection.protocol == self.case.protocol.codec(),
                "actual native accepted codec protocol",
            )?;
            require(connection.secure == self.case.tls, "actual native TLS mode")?;
            require(!connection.zero_copy_enabled, "kernel zero-copy stays off")?;
            require(
                connection.peer_identity.is_none(),
                "server-auth TLS does not invent client authentication",
            )?;
        }
        let requests = self.requests.lock().expect("native request evidence");
        require(
            requests.iter().flatten().count() == if concurrent { 2 } else { 1 },
            "each expected upload was validated once by the native handler",
        )?;
        for request in requests.iter().flatten() {
            require(
                accepted
                    .iter()
                    .flatten()
                    .any(|connection| Arc::ptr_eq(&connection.context, &request.connection)),
                "handler belongs to an actually accepted native connection",
            )?;
        }
        if concurrent {
            let first = requests[0].as_ref().expect("verified first request");
            let second = requests[1].as_ref().expect("verified second request");
            match self.case.protocol {
                Protocol::H1 => require(
                    first.connection.peer_endpoint() != second.connection.peer_endpoint(),
                    "concurrent H1 operations use separate TCP connections",
                )?,
                Protocol::H2 => {
                    require(
                        Arc::ptr_eq(&first.connection, &second.connection),
                        "concurrent H2 streams use the same accepted connection",
                    )?;
                    require(
                        first.stream == Some(StreamId::new(1)),
                        "first H2 stream is 1",
                    )?;
                    require(
                        second.stream == Some(StreamId::new(3)),
                        "second H2 stream is 3",
                    )?;
                }
            }
        }
        Ok(())
    }
}

impl AcceptedConnectionObserver for NativeState {
    fn on_accepted(&self, connection: &ConnectionInfo) {
        let mut accepted = self.accepted.lock().expect("accepted connection evidence");
        let slot = accepted
            .iter_mut()
            .find(|entry| entry.is_none())
            .expect("the fixture opens at most two native connections");
        *slot = Some(connection.clone());
    }
}

struct NativeHandler(Arc<NativeState>);

impl HttpHandler for NativeHandler {
    fn handle_request(
        &self,
        context: HandlerSessionContext,
        request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let state = Arc::clone(&self.0);
        Box::pin(async move {
            receive_upload(&state, context, request)
                .await
                .map_err(|error| state.failure(error))
        })
    }
}

async fn receive_upload(
    state: &Arc<NativeState>,
    context: HandlerSessionContext,
    mut request: HttpSourceHolder,
) -> TestResult<HttpSourceHolder> {
    let stream = request.get_stream_id();
    let mut head = request.read_header_event().await?;
    require(!head.eom(), "streaming upload does not end at headers")?;
    require(
        head.headers_mut().method_string()? == "POST",
        "native upload method",
    )?;
    let operation = Operation::parse(head.headers_mut().path()?)?;
    let NativeGates {
        upload_seen,
        response_held,
        response_release,
    } = state.gates.lock().expect("native operation gates")[operation.index()]
        .take()
        .ok_or(FixtureError::Assertion(
            "native operation admitted exactly once",
        ))?;
    let mut upload_seen = Some(upload_seen);
    let expected = payload(operation, false);
    let mut received = Vec::with_capacity(PAYLOAD_SIZE);
    loop {
        let event = request.read_body_event(CHUNK_SIZE as u32).await?;
        let eom = event.eom();
        match event.into_kind() {
            HttpBodyEventKind::Body(body) => {
                require(!eom, "upload terminates in real trailers, not DATA")?;
                require(
                    received.len() + body.chain_length() <= PAYLOAD_SIZE,
                    "native upload never exceeds expected length",
                )?;
                for segment in body.segments() {
                    received.extend_from_slice(segment);
                }
                if received.len() == CHUNK_SIZE
                    && let Some(seen) = upload_seen.take()
                {
                    require(
                        received.as_slice() == &expected[..CHUNK_SIZE],
                        "native handler sees the exact upload prefix before producer release",
                    )?;
                    seen.send(())
                        .map_err(|_| FixtureError::Assertion("upload observer alive"))?;
                }
            }
            HttpBodyEventKind::Trailers(trailers) => {
                require(eom, "upload trailers carry native EOM")?;
                require(trailers.len() == 2, "exactly two upload trailer fields")?;
                require(
                    trailers.get_single_or_empty(b"x-upload-check") == b"upload-complete",
                    "native upload trailer value",
                )?;
                require(
                    trailers.get_single_or_empty(b"x-upload-operation")
                        == operation.tag().as_bytes(),
                    "native upload trailer association",
                )?;
                break;
            }
            HttpBodyEventKind::Suspend(resume) => {
                let status = timeout(STEP_TIMEOUT, resume)
                    .await
                    .map_err(|_| FixtureError::Timeout("native upload suspension"))?;
                require(
                    status == ResumeStatus::Signalled,
                    "native upload suspension resumes with Signalled",
                )?;
            }
            _ => return Err(FixtureError::Assertion("unexpected upload event").into()),
        }
    }
    require(
        received.as_slice() == expected.as_ref(),
        "byte-exact native streaming upload",
    )?;
    require(
        upload_seen.is_none(),
        "upload prefix was observed while producer was held",
    )?;
    let connection = Arc::clone(
        context
            .connection_context()
            .ok_or(FixtureError::Assertion("native handler connection context"))?,
    );
    state.requests.lock().expect("native request evidence")[operation.index()] =
        Some(RequestEvidence { connection, stream });
    let mut headers = HttpMessage::new();
    headers.set_status_code(operation.status())?;
    headers.set_status_message(HttpMessage::default_reason(operation.status()))?;
    headers.set_http_version(1, 1);
    headers.set_chunked(state.case.protocol == Protocol::H1);
    headers.headers_mut().add(b"trailer", RESPONSE_TRAILERS);
    headers
        .headers_mut()
        .add(b"x-wire-operation", operation.tag());
    let mut trailers = HttpHeaders::new();
    trailers.add(b"x-response-check", b"response-complete");
    trailers.add(b"x-response-operation", operation.tag());
    Ok(HttpSourceHolder::new(Box::new(NativeResponse {
        headers: Some(headers),
        body: payload(operation, true),
        trailers: Some(trailers),
        offset: 0,
        gate: Some((response_held, response_release)),
        state: Arc::clone(state),
    })))
}

struct NativeResponse {
    headers: Option<HttpMessage>,
    body: Bytes,
    trailers: Option<HttpHeaders>,
    offset: usize,
    gate: Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>,
    state: Arc<NativeState>,
}

impl HttpSource for NativeResponse {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let result = self
            .headers
            .take()
            .ok_or_else(|| {
                self.state
                    .failure(FixtureError::Assertion("one response head").into())
            })
            .and_then(|headers| {
                HttpHeaderEvent::new(headers, false)
                    .map_err(|error| self.state.failure(error.into()))
            });
        Box::pin(ready(result))
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let state = Arc::clone(&self.state);
        let gate = if self.offset == CHUNK_SIZE {
            self.gate.take()
        } else {
            None
        };
        let result: TestResult<HttpBodyEvent> = if self.offset == self.body.len() {
            self.trailers
                .take()
                .map(HttpBodyEvent::trailers)
                .ok_or_else(|| {
                    FixtureError::Assertion("one terminal response trailer event").into()
                })
        } else if maximum == 0 {
            Err(FixtureError::Assertion("nonzero response read maximum").into())
        } else {
            let next_boundary = if self.offset < CHUNK_SIZE {
                CHUNK_SIZE
            } else {
                self.body.len()
            };
            let length = (maximum as usize)
                .min(CHUNK_SIZE)
                .min(next_boundary - self.offset);
            let body = self.body.slice(self.offset..self.offset + length);
            self.offset += length;
            BufQueue::from_bytes(body, length)
                .map(|body| HttpBodyEvent::body(body, false))
                .map_err(Into::into)
        };
        Box::pin(async move {
            let work = async move {
                if let Some((held, release)) = gate {
                    held.send(())
                        .map_err(|_| FixtureError::Assertion("response gate observer alive"))?;
                    bounded("native response release", release).await?;
                }
                result
            };
            work.await.map_err(|error| state.failure(error))
        })
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {
        drop(self.headers.take());
        drop(self.trailers.take());
        drop(self.gate.take());
        self.body = Bytes::new();
    }
}

struct UploadBody {
    frames: mpsc::Receiver<Frame<Bytes>>,
    finished: bool,
}

impl Body for UploadBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        let this = self.get_mut();
        match this.frames.poll_recv(context) {
            Poll::Ready(Some(frame)) => Poll::Ready(Some(Ok(frame))),
            Poll::Ready(None) => {
                this.finished = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.finished
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}

#[derive(Clone)]
struct PeerTasks(Arc<Mutex<PeerTaskState>>);

struct PeerTaskState {
    tasks: JoinSet<TestResult<()>>,
    accepting: bool,
}

impl PeerTasks {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(PeerTaskState {
            tasks: JoinSet::new(),
            accepting: true,
        })))
    }

    fn spawn(
        &self,
        phase: &'static str,
        work: impl Future<Output = TestResult<()>> + Send + 'static,
    ) -> TestResult<()> {
        let mut state = self.0.lock().expect("peer task ownership");
        require(state.accepting, "peer task admission remains open")?;
        require(
            state.tasks.len() < TASK_LIMIT,
            "bounded peer task admission",
        )?;
        state.tasks.spawn(async move {
            work.await
                .map_err(|source| FixtureError::Operation { phase, source }.into())
        });
        Ok(())
    }

    async fn collect(&self, errors: &mut Vec<BoxError>) {
        while let Some(result) = poll_fn(|context| {
            self.0
                .lock()
                .expect("peer task ownership")
                .tasks
                .poll_join_next(context)
        })
        .await
        {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => errors.push(error),
                Err(error) => errors.push(error.into()),
            }
        }
    }

    async fn finish(&self, errors: &mut Vec<BoxError>) {
        if timeout(STEP_TIMEOUT, self.collect(errors)).await.is_err() {
            errors.push(FixtureError::Timeout("peer task completion").into());
            {
                let mut state = self.0.lock().expect("peer task ownership");
                state.accepting = false;
                state.tasks.abort_all();
            }
            self.collect(errors).await;
        }
        self.0.lock().expect("peer task ownership").accepting = false;
    }
}

impl<Work> Executor<Work> for PeerTasks
where
    Work: Future<Output = ()> + Send + 'static,
{
    fn execute(&self, work: Work) {
        self.spawn("Hyper internal H2 task", async move {
            work.await;
            Ok(())
        })
        .expect("Hyper internal tasks fit bounded fixture ownership");
    }
}

enum Peer {
    H1(http1::SendRequest<UploadBody>),
    H2(http2::SendRequest<UploadBody>),
}

impl Peer {
    async fn send(&mut self, request: Request<UploadBody>) -> TestResult<Response<Incoming>> {
        match self {
            Self::H1(sender) => {
                sender.ready().await?;
                Ok(sender.send_request(request).await?)
            }
            Self::H2(sender) => {
                sender.ready().await?;
                Ok(sender.send_request(request).await?)
            }
        }
    }
}

async fn handshake<Io>(io: Io, protocol: Protocol, tasks: &PeerTasks) -> TestResult<Peer>
where
    Io: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    match protocol {
        Protocol::H1 => {
            let (sender, connection) = http1::handshake::<_, UploadBody>(TokioIo::new(io)).await?;
            tasks.spawn("Hyper H1 connection driver", async move {
                Ok(connection.await?)
            })?;
            Ok(Peer::H1(sender))
        }
        Protocol::H2 => {
            let (sender, connection) =
                http2::handshake::<_, _, UploadBody>(tasks.clone(), TokioIo::new(io)).await?;
            tasks.spawn("Hyper H2 connection driver", async move {
                Ok(connection.await?)
            })?;
            Ok(Peer::H2(sender))
        }
    }
}

struct PeerEndpoint {
    address: SocketAddr,
    case: WireCase,
    tls: Option<Arc<ClientConfig>>,
}

impl PeerEndpoint {
    async fn connect(&self, tasks: &PeerTasks) -> TestResult<Peer> {
        let stream = bounded("peer TCP connect", TcpStream::connect(self.address)).await?;
        stream.set_nodelay(true)?;
        if let Some(config) = &self.tls {
            let stream = bounded(
                "independent peer TLS handshake",
                TlsConnector::from(Arc::clone(config))
                    .connect(ServerName::try_from("localhost")?, stream),
            )
            .await?;
            require(
                stream.get_ref().1.alpn_protocol() == Some(self.case.protocol.alpn()),
                "actual independently negotiated TLS ALPN",
            )?;
            bounded(
                "Hyper TLS protocol handshake",
                handshake(stream, self.case.protocol, tasks),
            )
            .await
        } else {
            bounded(
                "Hyper TCP protocol handshake",
                handshake(stream, self.case.protocol, tasks),
            )
            .await
        }
    }
}

fn tls_fixture(protocol: Protocol) -> TestResult<(TlsServerOptions, Arc<ClientConfig>)> {
    let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])?;
    let certificate = generated.cert.der().to_vec();
    let options = TlsServerOptions::new(
        vec![certificate.clone().into_boxed_slice()],
        generated
            .signing_key
            .serialized_der()
            .to_vec()
            .into_boxed_slice(),
        vec![protocol.alpn().to_vec().into_boxed_slice()],
    )?;
    let mut roots = RootCertStore::empty();
    roots.add(CertificateDer::from(certificate))?;
    let mut client = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client.alpn_protocols = vec![protocol.alpn().to_vec()];
    Ok((options, Arc::new(client)))
}

async fn produce_upload(
    operation: Operation,
    frames: mpsc::Sender<Frame<Bytes>>,
    upload_seen: oneshot::Receiver<()>,
) -> TestResult<()> {
    let body = payload(operation, false);
    bounded(
        "first upload frame",
        frames.send(Frame::data(body.slice(..CHUNK_SIZE))),
    )
    .await?;
    bounded(
        "native upload prefix before remaining production",
        upload_seen,
    )
    .await?;
    for offset in (CHUNK_SIZE..PAYLOAD_SIZE).step_by(CHUNK_SIZE) {
        let end = (offset + CHUNK_SIZE).min(PAYLOAD_SIZE);
        bounded(
            "streaming upload frame",
            frames.send(Frame::data(body.slice(offset..end))),
        )
        .await?;
    }
    let mut trailers = HeaderMap::new();
    trailers.insert(
        "x-upload-check",
        HeaderValue::from_static("upload-complete"),
    );
    trailers.insert(
        "x-upload-operation",
        HeaderValue::from_static(operation.tag()),
    );
    bounded(
        "upload trailer frame",
        frames.send(Frame::trailers(trailers)),
    )
    .await?;
    Ok(())
}

async fn consume_response(
    response: Response<Incoming>,
    case: WireCase,
    operation: Operation,
    prefix: oneshot::Sender<()>,
) -> TestResult<()> {
    require(
        response.status().as_u16() == operation.status(),
        "independent response status",
    )?;
    require(
        response.version() == case.protocol.version(),
        "actual independent HTTP version",
    )?;
    require(
        response.headers().get("x-wire-operation")
            == Some(&HeaderValue::from_static(operation.tag())),
        "response header belongs to the requested operation",
    )?;
    require(
        !response.headers().contains_key(CONTENT_LENGTH),
        "streaming length is not fabricated",
    )?;
    if case.protocol == Protocol::H1 {
        require(
            response.headers().get(TRANSFER_ENCODING) == Some(&HeaderValue::from_static("chunked")),
            "H1 response uses real chunked framing",
        )?;
    } else {
        require(
            !response.headers().contains_key(TRANSFER_ENCODING),
            "H2 has no transfer encoding",
        )?;
    }
    let expected = payload(operation, true);
    let mut body = Box::pin(response.into_body());
    require(
        body.size_hint().exact().is_none(),
        "unknown streaming response size hint",
    )?;
    let mut received = Vec::with_capacity(PAYLOAD_SIZE);
    let mut prefix = Some(prefix);
    let mut trailers_seen = false;
    loop {
        let (end_stream, frame) = poll_fn(|context| {
            let end_stream = body.is_end_stream();
            body.as_mut()
                .poll_frame(context)
                .map(|frame| (end_stream, frame))
        })
        .await;
        let Some(frame) = frame else {
            break;
        };
        let frame = frame?;
        require(
            !end_stream,
            "a true response end-stream hint yields EOF, not a frame",
        )?;
        require(!trailers_seen, "nothing follows terminal response trailers")?;
        match frame.into_data() {
            Ok(data) => {
                require(
                    received.len() + data.len() <= PAYLOAD_SIZE,
                    "bounded response byte count",
                )?;
                received.extend_from_slice(&data);
                if received.len() == CHUNK_SIZE
                    && let Some(prefix) = prefix.take()
                {
                    require(
                        received.as_slice() == &expected[..CHUNK_SIZE],
                        "independent peer sees exact prefix before native producer release",
                    )?;
                    prefix
                        .send(())
                        .map_err(|_| FixtureError::Assertion("prefix observer alive"))?;
                }
            }
            Err(frame) => {
                let trailers = frame
                    .into_trailers()
                    .map_err(|_| FixtureError::Assertion("response event is DATA or trailers"))?;
                require(trailers.len() == 2, "exactly two response trailer fields")?;
                require(
                    trailers.get("x-response-check")
                        == Some(&HeaderValue::from_static("response-complete")),
                    "independent response trailer value",
                )?;
                require(
                    trailers.get("x-response-operation")
                        == Some(&HeaderValue::from_static(operation.tag())),
                    "independent response trailer association",
                )?;
                trailers_seen = true;
            }
        }
    }
    require(trailers_seen, "real response trailers arrived")?;
    require(
        prefix.is_none(),
        "response prefix observed while native producer was held",
    )?;
    require(
        received.as_slice() == expected.as_ref(),
        "byte-exact streamed response",
    )?;
    Ok(())
}

fn start_exchange(
    tasks: &PeerTasks,
    endpoint: &PeerEndpoint,
    mut peer: Peer,
    signals: PeerSignals,
) -> TestResult<()> {
    let PeerSignals {
        operation,
        upload_seen,
        response_prefix,
        completed,
    } = signals;
    let (frames, receiver) = mpsc::channel(1);
    let request = Request::builder()
        .method("POST")
        .version(endpoint.case.protocol.version())
        .uri(format!(
            "{}://localhost:{}/{}",
            if endpoint.case.tls { "https" } else { "http" },
            endpoint.address.port(),
            operation.tag(),
        ))
        .header(HOST, format!("localhost:{}", endpoint.address.port()))
        .header(TE, "trailers")
        .header(TRAILER, UPLOAD_TRAILERS)
        .body(UploadBody {
            frames: receiver,
            finished: false,
        })?;
    tasks.spawn(
        "gated upload producer",
        produce_upload(operation, frames, upload_seen),
    )?;
    let case = endpoint.case;
    tasks.spawn("independent HTTP exchange", async move {
        let response = bounded("independent response headers", peer.send(request)).await?;
        bounded(
            "independent response body",
            consume_response(response, case, operation, response_prefix),
        )
        .await?;
        completed
            .send(())
            .map_err(|_| FixtureError::Assertion("exchange observer alive"))?;
        Ok(())
    })
}

async fn observe_held(control: &mut WireControl) -> TestResult<()> {
    bounded(
        "native response producer reached its gate",
        &mut control.response_held,
    )
    .await?;
    bounded(
        "peer received prefix while native producer is held",
        &mut control.response_prefix,
    )
    .await?;
    require(
        matches!(control.completed.try_recv(), Err(TryRecvError::Empty)),
        "held operation is still incomplete after positive progress barriers",
    )
}

async fn exercise(
    endpoint: &PeerEndpoint,
    tasks: &PeerTasks,
    first: (PeerSignals, WireControl),
    second: Option<(PeerSignals, WireControl)>,
) -> TestResult<()> {
    let peer = endpoint.connect(tasks).await?;
    let second_peer = if second.is_some() {
        Some(match &peer {
            Peer::H1(_) => endpoint.connect(tasks).await?,
            Peer::H2(sender) => Peer::H2(sender.clone()),
        })
    } else {
        None
    };
    let (signals, mut first) = first;
    start_exchange(tasks, endpoint, peer, signals)?;
    observe_held(&mut first).await?;
    if let Some((signals, mut second)) = second {
        let peer = second_peer.ok_or(FixtureError::Assertion("second independent peer exists"))?;
        start_exchange(tasks, endpoint, peer, signals)?;
        observe_held(&mut second).await?;
        second
            .response_release
            .send(())
            .map_err(|_| FixtureError::Assertion("second native gate alive"))?;
        bounded(
            "second operation completes while first is held",
            &mut second.completed,
        )
        .await?;
        require(
            matches!(first.completed.try_recv(), Err(TryRecvError::Empty)),
            "second completion did not release or complete the first operation",
        )?;
    }
    first
        .response_release
        .send(())
        .map_err(|_| FixtureError::Assertion("first native gate alive"))?;
    bounded(
        "first operation completes after explicit release",
        &mut first.completed,
    )
    .await?;
    Ok(())
}

fn run_case(case: WireCase, concurrent: bool) -> TestResult<()> {
    let runtime = Builder::new_current_thread().enable_all().build()?;
    let tasks = PeerTasks::new();
    let (first_native, first_peer, first_control) = gates(Operation::First);
    let (second_native, second) = if concurrent {
        let (native, peer, control) = gates(Operation::Second);
        (Some(native), Some((peer, control)))
    } else {
        (None, None)
    };
    let state = Arc::new(NativeState {
        case,
        gates: Mutex::new([Some(first_native), second_native]),
        requests: Mutex::new([None, None]),
        accepted: Mutex::new([None, None]),
        errors: Mutex::new([None, None]),
    });
    let factory: HandlerFactory = Arc::new({
        let state = Arc::clone(&state);
        move |_| Rc::new(NativeHandler(Arc::clone(&state))) as HttpHandlerRef
    });
    let mut config = ServerConfig {
        plaintext_protocol: case.protocol.codec(),
        command_capacity: 8,
        shutdown_on_signals: Vec::new(),
        connection_observer: Some(state.clone()),
        ..ServerConfig::default()
    };
    config.session.stream_read_timeout = CASE_TIMEOUT;
    config.session.connection_idle_timeout = CASE_TIMEOUT;
    config.session.write_timeout = CASE_TIMEOUT;
    let tls = if case.tls {
        let (options, client) = tls_fixture(case.protocol)?;
        config.tls_credentials = Some(Arc::new(StaticTlsCredentials::new(options)));
        Some(client)
    } else {
        None
    };
    let server = HttpServer::start(config, factory, None)?;
    let mut errors = Vec::new();
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        runtime.block_on(async {
            let endpoint = PeerEndpoint {
                address: server
                    .address()
                    .ok_or(FixtureError::Assertion("native listener address"))?,
                case,
                tls,
            };
            timeout(
                CASE_TIMEOUT,
                exercise(&endpoint, &tasks, (first_peer, first_control), second),
            )
            .await
            .map_err(|_| FixtureError::Timeout("native wire case"))??;
            state.verify(concurrent)
        })
    }));
    let panic = match outcome {
        Ok(Ok(())) => None,
        Ok(Err(error)) => {
            errors.push(error);
            None
        }
        Err(panic) => Some(panic),
    };
    if let Err(error) = require(server.drain(), "native root accepts graceful drain") {
        errors.push(error);
    }
    runtime.block_on(tasks.finish(&mut errors));
    let joined_at = Instant::now();
    match server.force_stop() {
        Ok(stopped) => {
            if let Err(error) = require(stopped, "native root stop physically joins workers") {
                errors.push(error);
            }
        }
        Err(error) => errors.push(error.into()),
    }
    if let Err(error) = require(
        joined_at.elapsed() < STEP_TIMEOUT,
        "native physical join deadline",
    ) {
        errors.push(error);
    }
    if let Err(error) = require(
        server.state() == HttpServerState::Stopped,
        "joined native root is stopped",
    ) {
        errors.push(error);
    }
    errors.extend(
        state
            .errors
            .lock()
            .expect("native failure storage")
            .iter_mut()
            .filter_map(Option::take),
    );
    drop(server);
    drop(runtime);
    if let Some(panic) = panic {
        for error in errors {
            eprintln!("{case:?} cleanup failure while retaining original panic: {error:?}");
        }
        resume_unwind(panic);
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(FixtureError::Case { case, errors }.into())
    }
}

#[test]
fn independent_peer_streaming_matrix() {
    for case in CASES {
        run_case(case, false).unwrap_or_else(|error| panic!("{error:?}"));
    }
}

#[test]
fn independent_peer_concurrent_progress() {
    for case in CASES {
        run_case(case, true).unwrap_or_else(|error| panic!("{error:?}"));
    }
}
