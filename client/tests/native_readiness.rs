/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::any::Any;
use std::cell::RefCell;
use std::error::Error;
use std::fmt::Display;
use std::future::Future;
use std::future::poll_fn;
use std::io;
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::panic_any;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::thread::ThreadId;
use std::time::Duration;

use bytes::Bytes;
use futures::FutureExt;
use http::HeaderMap;
use http::HeaderValue;
use http::Method;
use http::Request;
use http::Response as PeerResponse;
use http::StatusCode;
use http::Version;
use http_body::Body;
use http_body::Frame;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::rt::Executor;
use hyper::server::conn::http1;
use hyper::server::conn::http2;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpBodyEventKind;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::events::ResumeStatus;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::headers::HttpHeaders;
use roxygen_core::http::CodecProtocol;
use roxygen_core::http::HttpMethod;
use roxygen_core::message::HttpMessage;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::driver::WorkerDriver;
use roxygen_runtime::managed::RuntimeConfig;
use roxygen_runtime::managed::RuntimeHandle;
use roxygen_runtime::managed::RuntimeOwner;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerHandle;
use roxygen_runtime::worker::WorkerState;
use roxygen_transport::byte_stream::TransportRetirement;
use roxygen_transport::tls::TlsClientOptions;
use roxygen_transport::tls::builtin_crypto_provider;
use rustls::ServerConfig;
use rustls::pki_types::PrivatePkcs8KeyDer;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::ReadBuf;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio::time::timeout;
use tokio::time::timeout_at;
use tokio_rustls::TlsAcceptor;

use crate::HttpClient;
use crate::Response;
use crate::ResponseVisitor;
use crate::SourceRequest;
use crate::VisitAction;
use crate::connector::ClientSession;
use crate::connector::ClientTransportOptions;
use crate::connector::ConnectionParams;
use crate::connector::HttpConnector;
use crate::connector::IdentityValidation;
use crate::connector::SessionParams;

const OPERATION_BOUND: Duration = Duration::from_secs(10);
const CLEANUP_BOUND: Duration = Duration::from_secs(5);
const CASE_BOUND: Duration = Duration::from_secs(30);
const MAX_PEER_TASKS: usize = 16;
const MAX_BODY_BYTES: usize = 4096;
const UPLOAD_PREFIX: &[u8] = b"native upload prefix/";
const UPLOAD_SUFFIXES: [&[u8]; 2] = [b"native upload suffix/0", b"native upload suffix/1"];
const RESPONSE_PREFIX: &[u8] = b"independent response prefix/";
const RESPONSE_SUFFIXES: [&[u8]; 2] = [
    b"independent response suffix/0",
    b"independent response suffix/1",
];
const UPLOAD_TRAILER: &str = "x-native-upload";
const RESPONSE_TRAILER: &str = "x-independent-response";

fn failure(error: impl Display) -> io::Error {
    io::Error::other(error.to_string())
}

fn require(condition: bool, message: &'static str) -> io::Result<()> {
    if condition {
        Ok(())
    } else {
        Err(failure(message))
    }
}

fn native_failure(error: impl Display) -> HttpError {
    HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string())
}

fn retain(errors: &mut Vec<String>, stage: &str, result: io::Result<()>) {
    if let Err(error) = result {
        errors.push(format!("{stage}: {error}"));
    }
}

fn combined(errors: Vec<String>) -> io::Result<()> {
    if errors.is_empty() {
        Ok(())
    } else {
        Err(failure(errors.join("; ")))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Protocol {
    H1,
    H2,
}

impl Protocol {
    fn native(self) -> CodecProtocol {
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

    fn matches_message(self, message: &HttpMessage) -> bool {
        message.http_version() == (1, 1)
            && message.advanced_protocol_string()
                == match self {
                    Self::H1 => None,
                    Self::H2 => Some(b"h2".as_slice()),
                }
    }
}

#[derive(Clone, Copy, Debug)]
struct Case {
    protocol: Protocol,
    tls: bool,
    concurrent: bool,
}

impl Case {
    fn requests(self) -> usize {
        if self.concurrent { 2 } else { 1 }
    }

    fn connections(self) -> usize {
        if self.concurrent && self.protocol == Protocol::H1 {
            2
        } else {
            1
        }
    }
}

#[derive(Default)]
struct Proof {
    accepted: AtomicUsize,
    request_connections: Mutex<[Option<usize>; 2]>,
    upload_prefixes: AtomicUsize,
    uploads_complete: AtomicUsize,
    native_prefixes: [AtomicBool; 2],
    response_released: [AtomicBool; 2],
    response_tails: [AtomicBool; 2],
    live_bodies: AtomicUsize,
    maximum_live_bodies: AtomicUsize,
    body_drops: AtomicUsize,
    source_drops: AtomicUsize,
    source_state_drops: AtomicUsize,
    source_trailers: AtomicUsize,
    wrong_owner: AtomicBool,
    peer_error: Mutex<Option<String>>,
}

struct NativeChannels {
    upload_release: oneshot::Receiver<()>,
    response_release: oneshot::Sender<()>,
}

struct PeerChannels {
    upload_release: oneshot::Sender<()>,
    response_release: oneshot::Receiver<()>,
}

struct PeerState {
    case: Case,
    channels: Mutex<Vec<Option<PeerChannels>>>,
    proof: Arc<Proof>,
}

struct UploadState {
    prefix: Bytes,
    suffix: Bytes,
    release: oneshot::Receiver<()>,
    released: bool,
    trailers: Option<HttpHeaders>,
    stopped: bool,
    waiter: Option<Waker>,
    owner: ThreadId,
    proof: Arc<Proof>,
}

impl UploadState {
    fn poll_body(
        &mut self,
        context: &mut Context<'_>,
        maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        if self.owner != std::thread::current().id() {
            self.proof.wrong_owner.store(true, Ordering::SeqCst);
            return Poll::Ready(Err(native_failure("upload polled on another owner")));
        }
        if self.stopped || maximum == 0 {
            return Poll::Ready(Err(native_failure("stopped or zero-size upload read")));
        }
        if self.prefix.is_empty() && !self.released {
            match Pin::new(&mut self.release).poll(context) {
                Poll::Pending => {
                    self.waiter = Some(context.waker().clone());
                    return Poll::Pending;
                }
                Poll::Ready(Err(error)) => {
                    self.stopped = true;
                    return Poll::Ready(Err(native_failure(error)));
                }
                Poll::Ready(Ok(())) => self.released = true,
            }
        }
        self.waiter = None;
        let chunk = if !self.prefix.is_empty() {
            Some(
                self.prefix
                    .split_to(self.prefix.len().min(maximum as usize)),
            )
        } else if !self.suffix.is_empty() {
            Some(
                self.suffix
                    .split_to(self.suffix.len().min(maximum as usize)),
            )
        } else {
            None
        };
        if let Some(chunk) = chunk {
            return Poll::Ready(
                BufQueue::from_bytes(chunk, MAX_BODY_BYTES)
                    .map(|body| HttpBodyEvent::body(body, false))
                    .map_err(native_failure),
            );
        }
        let Some(trailers) = self.trailers.take() else {
            return Poll::Ready(Err(native_failure("upload read after terminal trailers")));
        };
        self.proof.source_trailers.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Ok(HttpBodyEvent::trailers(trailers)))
    }
}

impl Drop for UploadState {
    fn drop(&mut self) {
        if self.owner != std::thread::current().id() {
            self.proof.wrong_owner.store(true, Ordering::SeqCst);
        }
        self.proof.source_state_drops.fetch_add(1, Ordering::SeqCst);
    }
}

struct UploadSource {
    header: Option<HttpHeaderEvent>,
    state: Rc<RefCell<UploadState>>,
}

impl HttpSource for UploadSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let header = self
            .header
            .take()
            .ok_or_else(|| native_failure("upload header read twice"));
        Box::pin(std::future::ready(header))
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let state = Rc::clone(&self.state);
        Box::pin(poll_fn(move |context| {
            state.borrow_mut().poll_body(context, maximum)
        }))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {
        let waiter = {
            let mut state = self.state.borrow_mut();
            state.stopped = true;
            state.waiter.take()
        };
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }
}

impl Drop for UploadSource {
    fn drop(&mut self) {
        let state = self.state.borrow();
        if state.owner != std::thread::current().id() {
            state.proof.wrong_owner.store(true, Ordering::SeqCst);
        }
        state.proof.source_drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct ResponseObservation {
    final_headers: usize,
    body_events: usize,
    bytes: usize,
    trailers: usize,
    error: Option<&'static str>,
}

struct Visitor {
    protocol: Protocol,
    ordinal: usize,
    prefix_ready: Option<oneshot::Sender<()>>,
    observation: Rc<RefCell<ResponseObservation>>,
    proof: Arc<Proof>,
}

impl ResponseVisitor for Visitor {
    fn on_headers(&mut self, headers: &HttpMessage, is_final: bool, eom: bool) -> VisitAction {
        let mut observed = self.observation.borrow_mut();
        if !is_final
            || eom
            || observed.final_headers != 0
            || !self.protocol.matches_message(headers)
        {
            observed.error = Some("unexpected response header sequence");
            return VisitAction::Cancel;
        }
        observed.final_headers += 1;
        VisitAction::Continue
    }

    fn on_body(&mut self, body: &BufQueue, eom: bool) -> VisitAction {
        let mut observed = self.observation.borrow_mut();
        observed.body_events += 1;
        observed.bytes += body.chain_length();
        if eom || observed.trailers != 0 || observed.bytes > MAX_BODY_BYTES {
            observed.error = Some("unexpected response body ordering or size");
            return VisitAction::Cancel;
        }
        if let Some(prefix_ready) = self.prefix_ready.take() {
            if observed.bytes < RESPONSE_PREFIX.len() {
                self.prefix_ready = Some(prefix_ready);
            } else if observed.bytes != RESPONSE_PREFIX.len() {
                observed.error = Some("response suffix escaped its positive prefix barrier");
                return VisitAction::Cancel;
            } else {
                self.proof.native_prefixes[self.ordinal].store(true, Ordering::SeqCst);
                if prefix_ready.send(()).is_err() {
                    observed.error = Some("response prefix observer disappeared");
                    return VisitAction::Cancel;
                }
            }
        }
        VisitAction::Continue
    }

    fn on_trailers(&mut self, trailers: &HttpHeaders) {
        let mut observed = self.observation.borrow_mut();
        if observed.trailers != 0
            || observed.bytes != RESPONSE_PREFIX.len() + RESPONSE_SUFFIXES[self.ordinal].len()
            || trailers.len() != 1
            || trailers.get_single_or_empty(RESPONSE_TRAILER.as_bytes()) != b"verified"
        {
            observed.error = Some("response trailers were missing, early or incorrect");
        }
        observed.trailers += 1;
    }
}

struct Exchange {
    response: LocalSourceFuture<Result<Response, HttpError>>,
    prefix_ready: oneshot::Receiver<()>,
    release: Option<oneshot::Sender<()>>,
    observation: Rc<RefCell<ResponseObservation>>,
    ordinal: usize,
    protocol: Protocol,
}

struct RequestContext {
    case: Case,
    cancellation: CancellationToken,
    proof: Arc<Proof>,
}

fn exchange(
    session: &ClientSession,
    ordinal: usize,
    channels: NativeChannels,
    context: &RequestContext,
) -> io::Result<Exchange> {
    let mut message = HttpMessage::new();
    message.set_method(HttpMethod::Post).map_err(failure)?;
    message
        .set_url(format!("/wire/{ordinal}").as_bytes(), false)
        .map_err(failure)?;
    message.set_http_version(1, 1);
    message.set_secure(context.case.tls);
    message.set_wants_keepalive(true);
    message.set_chunked(context.case.protocol == Protocol::H1);
    message.headers_mut().set(b"host", b"localhost");
    message.headers_mut().set(b"te", b"trailers");
    message
        .headers_mut()
        .set(b"trailer", UPLOAD_TRAILER.as_bytes());
    let mut trailers = HttpHeaders::new();
    trailers.set(UPLOAD_TRAILER.as_bytes(), b"verified");
    let source = UploadSource {
        header: Some(HttpHeaderEvent::new(message, false).map_err(failure)?),
        state: Rc::new(RefCell::new(UploadState {
            prefix: Bytes::from_static(UPLOAD_PREFIX),
            suffix: Bytes::from_static(UPLOAD_SUFFIXES[ordinal]),
            release: channels.upload_release,
            released: false,
            trailers: Some(trailers),
            stopped: false,
            waiter: None,
            owner: std::thread::current().id(),
            proof: Arc::clone(&context.proof),
        })),
    };
    let (prefix_ready, prefix_observed) = oneshot::channel();
    let observation = Rc::new(RefCell::new(ResponseObservation::default()));
    let mut request = SourceRequest::new(HttpSourceHolder::new(Box::new(source)));
    request.cancellation = context.cancellation.clone();
    request.timeout = OPERATION_BOUND;
    request.maximum_response_body = MAX_BODY_BYTES;
    request.visitor = Some(Box::new(Visitor {
        protocol: context.case.protocol,
        ordinal,
        prefix_ready: Some(prefix_ready),
        observation: Rc::clone(&observation),
        proof: Arc::clone(&context.proof),
    }));
    Ok(Exchange {
        response: HttpClient::request_source(session, request),
        prefix_ready: prefix_observed,
        release: Some(channels.response_release),
        observation,
        ordinal,
        protocol: context.case.protocol,
    })
}

fn release_response(exchange: &mut Exchange, proof: &Proof) -> io::Result<()> {
    proof.response_released[exchange.ordinal].store(true, Ordering::SeqCst);
    exchange
        .release
        .take()
        .ok_or_else(|| failure("response released twice"))?
        .send(())
        .map_err(|_| failure("independent response producer disappeared"))
}

fn verify_response(mut response: Response, exchange: &Exchange) -> io::Result<()> {
    require(
        response.headers.status_code().map_err(failure)? == 201,
        "wrong response status",
    )?;
    require(
        exchange.protocol.matches_message(&response.headers),
        "native response protocol metadata differs",
    )?;
    let expected = [RESPONSE_PREFIX, RESPONSE_SUFFIXES[exchange.ordinal]].concat();
    require(
        response
            .body
            .coalesce(MAX_BODY_BYTES)
            .map_err(failure)?
            .as_ref()
            == expected.as_slice(),
        "independent response bytes differ",
    )?;
    let trailers = response
        .trailers
        .ok_or_else(|| failure("response has no trailers"))?;
    require(
        trailers.len() == 1,
        "response must contain exactly one trailer field",
    )?;
    require(
        trailers.get_single_or_empty(RESPONSE_TRAILER.as_bytes()) == b"verified",
        "response trailer value differs",
    )?;
    let observed = exchange.observation.borrow();
    if let Some(error) = observed.error {
        return Err(failure(error));
    }
    require(
        observed.final_headers == 1 && observed.body_events >= 2 && observed.trailers == 1,
        "response did not stream headers, separate body phases and terminal trailers",
    )
}

async fn exercise(
    sessions: &[ClientSession],
    channels: Vec<NativeChannels>,
    context: RequestContext,
) -> io::Result<()> {
    let case = context.case;
    let proof = &context.proof;
    let mut channels = channels.into_iter();
    let mut first = exchange(
        &sessions[0],
        0,
        channels
            .next()
            .ok_or_else(|| failure("missing first request channels"))?,
        &context,
    )?;
    tokio::select! {
        biased;
        result = &mut first.response => {
            return Err(failure(format!(
                "first response completed before its prefix barrier: {result:?}"
            )));
        }
        result = &mut first.prefix_ready => result.map_err(failure)?,
    }
    require(
        !proof.response_tails[0].load(Ordering::SeqCst),
        "first response tail was not held",
    )?;
    if case.concurrent {
        let second_session = if case.protocol == Protocol::H2 {
            &sessions[0]
        } else {
            &sessions[1]
        };
        require(
            sessions[0].is_same_session(second_session) == (case.protocol == Protocol::H2),
            "wrong native session topology for concurrent requests",
        )?;
        let mut second = exchange(
            second_session,
            1,
            channels
                .next()
                .ok_or_else(|| failure("missing second request channels"))?,
            &context,
        )?;
        tokio::select! {
            biased;
            result = &mut first.response => {
                return Err(failure(format!(
                    "held response completed before second prefix: {result:?}"
                )));
            }
            result = &mut second.response => {
                return Err(failure(format!(
                    "second response completed before its prefix barrier: {result:?}"
                )));
            }
            result = &mut second.prefix_ready => result.map_err(failure)?,
        }
        release_response(&mut second, proof)?;
        let second_response = tokio::select! {
            biased;
            result = &mut first.response => {
                return Err(failure(format!(
                    "held response completed before healthy sibling: {result:?}"
                )));
            }
            result = &mut second.response => result.map_err(failure)?,
        };
        verify_response(second_response, &second)?;
        require(
            proof.native_prefixes[0].load(Ordering::SeqCst)
                && !proof.response_released[0].load(Ordering::SeqCst)
                && !proof.response_tails[0].load(Ordering::SeqCst)
                && proof.maximum_live_bodies.load(Ordering::SeqCst) == 2,
            "healthy sibling did not finish while the first response remained live and held",
        )?;
    }
    release_response(&mut first, proof)?;
    let response = (&mut first.response).await.map_err(failure)?;
    verify_response(response, &first)
}

struct NativeSetup {
    case: Case,
    address: SocketAddr,
    certificate: Option<Vec<u8>>,
    channels: Vec<NativeChannels>,
    proof: Arc<Proof>,
}

async fn retire_session(session: &ClientSession, graceful: bool) -> io::Result<()> {
    let mut errors = Vec::new();
    if graceful {
        let close = timeout(CLEANUP_BOUND, async {
            while session.active_transactions() != 0 {
                tokio::task::yield_now().await;
            }
            require(
                session.close_when_idle().map_err(failure)?,
                "native graceful close did not start",
            )
        })
        .await
        .map_err(failure)
        .and_then(|result| result);
        if close.is_err() {
            session.cancel();
        }
        retain(&mut errors, "native graceful close", close);
    } else {
        session.cancel();
    }
    let joined = timeout(CLEANUP_BOUND, session.wait_closed())
        .await
        .map_err(failure)
        .and_then(|result| result.map_err(failure));
    match joined {
        Ok(summary) => {
            if graceful {
                retain(
                    &mut errors,
                    "native session result",
                    require(
                        !summary.reset
                            && summary.bytes_read > 0
                            && summary.bytes_written > 0
                            && summary.bytes_scheduled == summary.bytes_written,
                        "native graceful completion reset or left unwritten bytes",
                    ),
                );
            }
        }
        Err(error) => {
            session.cancel();
            retain(&mut errors, "native session join", Err(error));
        }
    }
    retain(
        &mut errors,
        "native session finished",
        require(session.is_finished(), "native session did not finish"),
    );
    retain(
        &mut errors,
        "native transaction retirement",
        require(
            session.active_transactions() == 0,
            "native transactions remain after close",
        ),
    );
    combined(errors)
}

async fn run_native(context: WorkerContext, setup: NativeSetup) -> io::Result<()> {
    let cancellation = CancellationSource::new();
    let mut sessions = Vec::with_capacity(setup.case.connections());
    let operation = async {
        for _ in 0..setup.case.connections() {
            let transport = match &setup.certificate {
                Some(certificate) => {
                    let mut options = TlsClientOptions::new("localhost");
                    options.add_root_certificate(certificate);
                    options.set_alpn_protocols([setup.case.protocol.alpn()]);
                    ClientTransportOptions::Tls(options)
                }
                None => ClientTransportOptions::Plaintext,
            };
            let session = HttpConnector::connect(
                &context,
                SocketAddress::new(setup.address.ip().to_string(), setup.address.port()),
                OPERATION_BOUND,
                ConnectionParams {
                    transport,
                    plaintext_protocol: setup.case.protocol.native(),
                    identity_validation: IdentityValidation::Enforcing,
                    ..ConnectionParams::default()
                },
                SessionParams {
                    max_concurrent_outgoing_streams: Some(2),
                    ..SessionParams::default()
                },
                cancellation.token(),
            )
            .await
            .map_err(failure)?;
            let protocol = session.protocol();
            let worker_id = session.worker_id();
            sessions.push(session);
            require(
                protocol == setup.case.protocol.native(),
                "native protocol differs",
            )?;
            require(
                worker_id == context.worker_id(),
                "native session changed worker",
            )?;
        }
        exercise(
            &sessions,
            setup.channels,
            RequestContext {
                case: setup.case,
                cancellation: cancellation.token(),
                proof: setup.proof,
            },
        )
        .await
    };
    let mut errors = Vec::new();
    let result = timeout(OPERATION_BOUND, operation)
        .await
        .map_err(failure)
        .and_then(|result| result);
    let failed = result.is_err();
    retain(&mut errors, "native wire operations", result);
    if failed {
        retain(
            &mut errors,
            "native failure cancellation",
            require(
                cancellation.request_cancellation(),
                "native request cancellation was already requested",
            ),
        );
    }
    for session in &sessions {
        retain(
            &mut errors,
            "native session retirement",
            retire_session(session, !failed).await,
        );
    }
    combined(errors)
}

struct PeerBody {
    prefix: Option<Bytes>,
    suffix: Option<Bytes>,
    release: Option<oneshot::Receiver<()>>,
    trailers: Option<HeaderMap>,
    ordinal: usize,
    proof: Arc<Proof>,
}

impl Body for PeerBody {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let body = self.get_mut();
        if let Some(prefix) = body.prefix.take() {
            return Poll::Ready(Some(Ok(Frame::data(prefix))));
        }
        if let Some(release) = body.release.as_mut() {
            match Pin::new(release).poll(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    body.release = None;
                    body.suffix = None;
                    body.trailers = None;
                    return Poll::Ready(Some(Err(failure(error))));
                }
                Poll::Ready(Ok(())) => body.release = None,
            }
        }
        if let Some(suffix) = body.suffix.take() {
            body.proof.response_tails[body.ordinal].store(true, Ordering::SeqCst);
            return Poll::Ready(Some(Ok(Frame::data(suffix))));
        }
        Poll::Ready(
            body.trailers
                .take()
                .map(|trailers| Ok(Frame::trailers(trailers))),
        )
    }

    fn is_end_stream(&self) -> bool {
        self.trailers.is_none()
    }
}

impl Drop for PeerBody {
    fn drop(&mut self) {
        self.proof.live_bodies.fetch_sub(1, Ordering::SeqCst);
        self.proof.body_drops.fetch_add(1, Ordering::SeqCst);
    }
}

async fn read_upload(
    request: &mut Request<Incoming>,
    release: oneshot::Sender<()>,
    ordinal: usize,
    proof: &Proof,
) -> io::Result<()> {
    let expected = [UPLOAD_PREFIX, UPLOAD_SUFFIXES[ordinal]].concat();
    let mut received = Vec::with_capacity(expected.len());
    let mut release = Some(release);
    let mut trailers_seen = false;
    while let Some(frame) = request.body_mut().frame().await {
        let frame = frame.map_err(failure)?;
        if let Some(data) = frame.data_ref() {
            require(!trailers_seen, "upload data arrived after trailers")?;
            require(
                received.len() + data.len() <= expected.len(),
                "upload exceeded exact byte bound",
            )?;
            received.extend_from_slice(data);
            require(
                expected.starts_with(&received),
                "upload byte prefix differs",
            )?;
            if release.is_some() && received.len() >= UPLOAD_PREFIX.len() {
                require(
                    received.len() == UPLOAD_PREFIX.len(),
                    "upload suffix escaped the peer acknowledgement barrier",
                )?;
                proof.upload_prefixes.fetch_add(1, Ordering::SeqCst);
                release
                    .take()
                    .ok_or_else(|| failure("upload released twice"))?
                    .send(())
                    .map_err(|_| failure("native upload producer disappeared"))?;
            }
        } else if let Some(trailers) = frame.trailers_ref() {
            require(
                !trailers_seen && received == expected,
                "upload trailers were early or duplicated",
            )?;
            require(
                trailers.len() == 1,
                "upload must contain exactly one trailer field",
            )?;
            require(
                trailers.get(UPLOAD_TRAILER) == Some(&HeaderValue::from_static("verified")),
                "upload trailer differs",
            )?;
            trailers_seen = true;
        } else {
            return Err(failure("unexpected independent peer request frame"));
        }
    }
    require(
        received == expected && trailers_seen && release.is_none(),
        "upload did not complete bytes and trailers",
    )?;
    proof.uploads_complete.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

async fn peer_request(
    mut request: Request<Incoming>,
    connection: usize,
    state: Arc<PeerState>,
) -> io::Result<PeerResponse<PeerBody>> {
    require(
        request.method() == Method::POST,
        "peer received the wrong method",
    )?;
    require(
        request.version() == state.case.protocol.version(),
        "peer observed the wrong wire protocol",
    )?;
    let ordinal = match request.uri().path() {
        "/wire/0" => 0,
        "/wire/1" if state.case.concurrent => 1,
        _ => return Err(failure("unexpected request path or extra request")),
    };
    let channels = state.channels.lock().map_err(failure)?[ordinal]
        .take()
        .ok_or_else(|| failure("duplicate request on independent peer"))?;
    read_upload(&mut request, channels.upload_release, ordinal, &state.proof).await?;
    if ordinal == 1 {
        require(
            state.proof.native_prefixes[0].load(Ordering::SeqCst)
                && !state.proof.response_released[0].load(Ordering::SeqCst)
                && !state.proof.response_tails[0].load(Ordering::SeqCst),
            "second HTTP request did not overlap the held first response",
        )?;
    }
    {
        let mut connections = state.proof.request_connections.lock().map_err(failure)?;
        require(
            connections[ordinal].is_none(),
            "request connection recorded twice",
        )?;
        connections[ordinal] = Some(connection);
    }
    let live = state.proof.live_bodies.fetch_add(1, Ordering::SeqCst) + 1;
    state
        .proof
        .maximum_live_bodies
        .fetch_max(live, Ordering::SeqCst);
    let mut trailers = HeaderMap::new();
    trailers.insert(RESPONSE_TRAILER, HeaderValue::from_static("verified"));
    let body = PeerBody {
        prefix: Some(Bytes::from_static(RESPONSE_PREFIX)),
        suffix: Some(Bytes::from_static(RESPONSE_SUFFIXES[ordinal])),
        release: Some(channels.response_release),
        trailers: Some(trailers),
        ordinal,
        proof: Arc::clone(&state.proof),
    };
    PeerResponse::builder()
        .status(StatusCode::CREATED)
        .header("trailer", RESPONSE_TRAILER)
        .body(body)
        .map_err(failure)
}

#[derive(Default)]
struct ExecutorState {
    tasks: JoinSet<io::Result<()>>,
    closed: bool,
    rejected: bool,
}

#[derive(Clone, Default)]
struct PeerExecutor(Arc<Mutex<ExecutorState>>);

impl<Work> Executor<Work> for PeerExecutor
where
    Work: Future<Output = ()> + Send + 'static,
{
    fn execute(&self, work: Work) {
        let mut state = self.0.lock().expect("peer executor lock");
        if state.closed || state.tasks.len() >= MAX_PEER_TASKS {
            state.rejected = true;
            drop(state);
            drop(work);
            return;
        }
        state
            .tasks
            .spawn(async move { timeout(CASE_BOUND, work).await.map_err(failure) });
    }
}

impl PeerExecutor {
    async fn finish(&self, errors: &mut Vec<String>) {
        self.0.lock().expect("peer executor lock").closed = true;
        while let Some(result) = poll_fn(|context| {
            self.0
                .lock()
                .expect("peer executor lock")
                .tasks
                .poll_join_next(context)
        })
        .await
        {
            retain(
                errors,
                "Hyper child task",
                result.map_err(failure).and_then(|result| result),
            );
        }
        let state = self.0.lock().expect("peer executor lock");
        retain(
            errors,
            "Hyper task admission",
            require(
                !state.rejected,
                "Hyper exceeded or used closed task admission",
            ),
        );
        retain(
            errors,
            "Hyper task joins",
            require(state.tasks.is_empty(), "Hyper tasks remain"),
        );
    }
}

async fn retire_peers(
    peers: &mut JoinSet<io::Result<()>>,
    executor: &PeerExecutor,
    errors: &mut Vec<String>,
) {
    while let Some(result) = peers.join_next().await {
        retain(
            errors,
            "independent peer connection",
            result.map_err(failure).and_then(|result| result),
        );
    }
    executor.finish(errors).await;
}

async fn serve_peer<IoType>(
    stream: IoType,
    connection: usize,
    state: Arc<PeerState>,
    executor: PeerExecutor,
) -> io::Result<()>
where
    IoType: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let protocol = state.case.protocol;
    let service = service_fn(move |request| {
        let state = Arc::clone(&state);
        async move {
            let result = peer_request(request, connection, Arc::clone(&state)).await;
            if let Err(error) = &result {
                state
                    .proof
                    .peer_error
                    .lock()
                    .map_err(failure)?
                    .get_or_insert_with(|| error.to_string());
            }
            result
        }
    });
    match protocol {
        Protocol::H1 => http1::Builder::new()
            .serve_connection(TokioIo::new(stream), service)
            .await
            .map_err(|error| failure(format!("{error:?}"))),
        Protocol::H2 => http2::Builder::new(executor)
            .serve_connection(TokioIo::new(stream), service)
            .await
            .map_err(|error| failure(format!("{error:?}"))),
    }
}

fn tls_fixture(case: Case) -> io::Result<(Option<Vec<u8>>, Option<Arc<ServerConfig>>)> {
    if !case.tls {
        return Ok((None, None));
    }
    let generated =
        rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).map_err(failure)?;
    let certificate = generated.cert.der().to_vec();
    let key = PrivatePkcs8KeyDer::from(generated.signing_key.serialized_der().to_vec());
    let mut config = ServerConfig::builder_with_provider(Arc::new(builtin_crypto_provider()))
        .with_safe_default_protocol_versions()
        .map_err(failure)?
        .with_no_client_auth()
        .with_single_cert(vec![generated.cert.der().clone()], key.into())
        .map_err(failure)?;
    config.alpn_protocols = vec![case.protocol.alpn().to_vec()];
    Ok((Some(certificate), Some(Arc::new(config))))
}

fn verify_proof(case: Case, proof: &Proof) -> io::Result<()> {
    if let Some(error) = proof.peer_error.lock().map_err(failure)?.as_ref() {
        return Err(failure(format!("independent HTTP service: {error}")));
    }
    let expected = case.requests();
    require(
        proof.accepted.load(Ordering::SeqCst) == case.connections(),
        "wrong number of accepted TCP connections",
    )?;
    require(
        proof.upload_prefixes.load(Ordering::SeqCst) == expected,
        "missing peer upload prefix acknowledgements",
    )?;
    require(
        proof.uploads_complete.load(Ordering::SeqCst) == expected,
        "missing complete peer uploads",
    )?;
    require(
        proof.source_trailers.load(Ordering::SeqCst) == expected,
        "native sources did not finish trailers",
    )?;
    require(
        proof.source_drops.load(Ordering::SeqCst) == expected,
        "native source objects remain",
    )?;
    require(
        proof.source_state_drops.load(Ordering::SeqCst) == expected,
        "native source read futures retain state",
    )?;
    require(
        !proof.wrong_owner.load(Ordering::SeqCst),
        "native source changed owner during poll or destruction",
    )?;
    require(
        proof.live_bodies.load(Ordering::SeqCst) == 0,
        "independent peer response producers remain",
    )?;
    require(
        proof.body_drops.load(Ordering::SeqCst) == expected,
        "peer response producers were not all destroyed",
    )?;
    for ordinal in 0..expected {
        require(
            proof.native_prefixes[ordinal].load(Ordering::SeqCst),
            "native client did not observe a response prefix",
        )?;
        require(
            proof.response_tails[ordinal].load(Ordering::SeqCst),
            "peer did not produce the released response suffix",
        )?;
    }
    let connections = proof.request_connections.lock().map_err(failure)?;
    require(
        connections[0].is_some(),
        "first request has no connection identity",
    )?;
    if case.concurrent {
        require(
            connections[1].is_some(),
            "second request has no connection identity",
        )?;
        require(
            (connections[0] == connections[1]) == (case.protocol == Protocol::H2),
            "concurrent HTTP requests used the wrong connection topology",
        )?;
        require(
            proof.maximum_live_bodies.load(Ordering::SeqCst) == 2,
            "two HTTP response streams were not simultaneously live",
        )?;
    }
    Ok(())
}

async fn run_case(case: Case) -> io::Result<()> {
    let (certificate, tls) = tls_fixture(case)?;
    let listener = Arc::new(TcpListener::bind("127.0.0.1:0").await?);
    let address = listener.local_addr()?;
    let proof = Arc::new(Proof::default());
    let mut native_channels = Vec::with_capacity(case.requests());
    let mut peer_channels = Vec::with_capacity(case.requests());
    for _ in 0..case.requests() {
        let (upload_release, upload_ready) = oneshot::channel();
        let (response_release, response_ready) = oneshot::channel();
        native_channels.push(NativeChannels {
            upload_release: upload_ready,
            response_release,
        });
        peer_channels.push(Some(PeerChannels {
            upload_release,
            response_release: response_ready,
        }));
    }
    let state = Arc::new(PeerState {
        case,
        channels: Mutex::new(peer_channels),
        proof: Arc::clone(&proof),
    });
    let (worker, driver) = WorkerDriver::new(8).map_err(failure)?;
    let mut driver = Box::pin(driver);
    let executor = PeerExecutor::default();
    let mut peers = JoinSet::new();
    for connection in 0..case.connections() {
        let listener = Arc::clone(&listener);
        let state = Arc::clone(&state);
        let tls = tls.clone();
        let executor = executor.clone();
        peers.spawn(async move {
            timeout(CASE_BOUND, async move {
                let (stream, _) = listener.accept().await?;
                state.proof.accepted.fetch_add(1, Ordering::SeqCst);
                match tls {
                    Some(tls) => {
                        let stream = TlsAcceptor::from(tls).accept(stream).await?;
                        require(
                            stream.get_ref().1.alpn_protocol() == Some(case.protocol.alpn()),
                            "TLS peer negotiated the wrong ALPN",
                        )?;
                        serve_peer(stream, connection, state, executor).await
                    }
                    None => serve_peer(stream, connection, state, executor).await,
                }
            })
            .await
            .map_err(failure)
            .and_then(|result| result)
        });
    }
    drop(listener);
    let setup = NativeSetup {
        case,
        address,
        certificate,
        channels: native_channels,
        proof: Arc::clone(&proof),
    };
    let operation = AssertUnwindSafe(async {
        let mut call = Box::pin(
            worker
                .call_local(move |context| run_native(context, setup))
                .map_err(failure)?,
        );
        timeout(CASE_BOUND, async {
            tokio::select! {
                result = &mut call => result.map_err(failure)?,
                () = &mut driver => {
                    Err(failure("native driver exited before its owned call completed"))
                }
            }
        })
        .await
        .map_err(failure)?
    })
    .catch_unwind()
    .await;
    let mut errors = Vec::new();
    retain(
        &mut errors,
        "native call",
        operation.unwrap_or_else(|_| Err(failure("native driver or call panicked"))),
    );
    let mut peers_retired = false;
    if errors.is_empty() {
        let retirement = AssertUnwindSafe(timeout(CLEANUP_BOUND, async {
            require(
                worker.state() == WorkerState::Open,
                "native worker closed before peer retirement",
            )?;
            tokio::select! {
                biased;
                () = &mut driver => {
                    Err(failure("native driver exited before peer retirement completed"))
                }
                () = retire_peers(&mut peers, &executor, &mut errors) => {
                    peers_retired = true;
                    require(
                        worker.state() == WorkerState::Open,
                        "native worker closed during peer retirement",
                    )
                }
            }
        }))
        .catch_unwind()
        .await;
        retain(
            &mut errors,
            "peer retirement with open native driver",
            retirement
                .map_err(|_| failure("native driver or peer retirement panicked"))
                .and_then(|result| result.map_err(failure))
                .and_then(|result| result),
        );
    }
    let shutdown = std::panic::catch_unwind(AssertUnwindSafe(|| worker.begin_shutdown()));
    retain(
        &mut errors,
        "native shutdown request",
        shutdown
            .map_err(|_| failure("native shutdown notification panicked"))
            .and_then(|started| require(started, "native shutdown had already started")),
    );
    let joined = AssertUnwindSafe(timeout(CLEANUP_BOUND, driver.as_mut()))
        .catch_unwind()
        .await;
    retain(
        &mut errors,
        "native driver drain",
        joined
            .map_err(|_| failure("native driver drain panicked"))
            .and_then(|result| result.map_err(failure)),
    );
    let retired = std::panic::catch_unwind(AssertUnwindSafe(|| drop(driver)));
    retain(
        &mut errors,
        "native driver owner retirement",
        retired.map_err(|_| failure("native driver destruction panicked")),
    );
    retain(
        &mut errors,
        "native closed state",
        require(
            worker.state() == WorkerState::Closed,
            "native worker did not close",
        ),
    );
    if !peers_retired {
        retire_peers(&mut peers, &executor, &mut errors).await;
    }
    retain(
        &mut errors,
        "wire and owner observations",
        verify_proof(case, &proof),
    );
    combined(errors)
}

fn run_matrix(concurrent: bool) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("native wire test runtime");
    let mut errors = Vec::new();
    for protocol in [Protocol::H1, Protocol::H2] {
        for tls in [false, true] {
            let case = Case {
                protocol,
                tls,
                concurrent,
            };
            retain(
                &mut errors,
                &format!("{case:?}"),
                runtime.block_on(run_case(case)),
            );
        }
    }
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}

#[test]
fn independent_peer_streaming_matrix() {
    run_matrix(false);
}

#[test]
fn independent_peer_concurrent_progress() {
    run_matrix(true);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CheckedNativeOutcome {
    Passed,
    Failed,
    Panicked,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum NativeReleaseError {
    #[error("final checked native outcome was not published")]
    Missing,
    #[error("final checked native outcome was {0:?}")]
    Rejected(CheckedNativeOutcome),
}

pub(super) async fn publish_checked_native<T>(
    operation: impl Future<Output = io::Result<T>>,
    publication: oneshot::Sender<CheckedNativeOutcome>,
) -> io::Result<T> {
    let result = AssertUnwindSafe(operation).catch_unwind().await;
    let outcome = match &result {
        Ok(Ok(_)) => CheckedNativeOutcome::Passed,
        Ok(Err(_)) => CheckedNativeOutcome::Failed,
        Err(_) => CheckedNativeOutcome::Panicked,
    };
    let publication = publication
        .send(outcome)
        .map_err(|_| failure("final native outcome receiver disappeared"));
    match result {
        Ok(Ok(value)) => publication.map(|()| value),
        Ok(Err(error)) => {
            if let Err(publication) = publication {
                eprintln!("final native failure publication: {publication}");
            }
            Err(error)
        }
        Err(payload) => {
            eprintln!("final native panic publication: {publication:?}");
            resume_unwind(payload)
        }
    }
}

pub(super) async fn release_checked_peer_io<Io>(
    stream: Io,
    receipt: impl Future<Output = io::Result<TransportRetirement>>,
    checked: oneshot::Receiver<CheckedNativeOutcome>,
    bound: Duration,
) -> io::Result<()> {
    let result = timeout(bound, async {
        let (retired, checked) = tokio::join!(
            async { receipt.await?.wait_retired().await.map_err(failure) },
            async {
                match checked.await {
                    Ok(CheckedNativeOutcome::Passed) => Ok(()),
                    Ok(outcome) => Err(io::Error::other(NativeReleaseError::Rejected(outcome))),
                    Err(_) => Err(io::Error::other(NativeReleaseError::Missing)),
                }
            },
        );
        match retired {
            Ok(()) => checked,
            Err(error) => {
                if let Err(checked) = checked {
                    eprintln!("native checked outcome after retirement failure: {checked}");
                }
                Err(error)
            }
        }
    })
    .await
    .map_err(failure)
    .and_then(|result| result);
    drop(stream);
    result
}

struct RetirementIo<Io> {
    io: Option<Io>,
    drops: Arc<AtomicUsize>,
    owner: ThreadId,
}

impl<Io> Drop for RetirementIo<Io> {
    fn drop(&mut self) {
        drop(self.io.take());
        assert_eq!(self.owner, std::thread::current().id());
        assert_eq!(self.drops.fetch_add(1, Ordering::SeqCst), 0);
    }
}

impl<Io: AsyncRead + Unpin> AsyncRead for RetirementIo<Io> {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().io.as_mut().unwrap()).poll_read(context, output)
    }
}

impl<Io: AsyncWrite + Unpin> AsyncWrite for RetirementIo<Io> {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.get_mut().io.as_mut().unwrap()).poll_write(context, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().io.as_mut().unwrap()).poll_flush(context)
    }
    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().io.as_mut().unwrap()).poll_shutdown(context)
    }
}

struct RetirementPeerChannels {
    receipt: oneshot::Receiver<TransportRetirement>,
    checked: oneshot::Receiver<CheckedNativeOutcome>,
    response_consumed: oneshot::Receiver<()>,
    driver_retired: oneshot::Sender<()>,
}

async fn retirement_peer(
    mut stream: impl AsyncRead + AsyncWrite + Unpin,
    protocol: Protocol,
    channels: RetirementPeerChannels,
) -> io::Result<()> {
    let RetirementPeerChannels {
        receipt,
        checked,
        response_consumed,
        driver_retired,
    } = channels;
    let operation = async {
        match protocol {
            Protocol::H1 => {
                let mut header = Vec::new();
                while !header.ends_with(b"\r\n\r\n") {
                    require(
                        header.len() < MAX_BODY_BYTES,
                        "bounded native request header",
                    )?;
                    let mut byte = [0];
                    stream.read_exact(&mut byte).await?;
                    header.push(byte[0]);
                }
                require(
                    header.starts_with(b"GET /retirement HTTP/1.1\r\n"),
                    "real native H1 request",
                )?;
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                    .await?;
                stream.flush().await?;
                response_consumed.await.map_err(failure)?;
            }
            Protocol::H2 => {
                let mut connection = h2::server::handshake(&mut stream)
                    .await
                    .map_err(io::Error::other)?;
                let (request, mut response) = connection
                    .accept()
                    .await
                    .ok_or_else(|| failure("real native H2 request missing"))?
                    .map_err(io::Error::other)?;
                require(
                    request.method() == Method::GET
                        && request.uri().path() == "/retirement"
                        && request.body().is_end_stream(),
                    "real complete native H2 request",
                )?;
                response
                    .send_response(
                        PeerResponse::builder()
                            .status(200)
                            .header("content-length", "0")
                            .body(())
                            .map_err(failure)?,
                        true,
                    )
                    .map_err(io::Error::other)?;
                drop(request);
                drop(response);
                let mut driver = Box::pin(connection.accept());
                tokio::select! {
                    biased;
                    result = &mut driver => match result {
                        Some(Err(error)) => return Err(io::Error::other(error)),
                        Some(Ok(_)) => return Err(failure("unexpected second native request")),
                        None => return Err(failure("peer terminated before explicit retirement")),
                    },
                    result = response_consumed => result.map_err(failure)?,
                }
                drop(driver);
                drop(connection);
            }
        }
        driver_retired
            .send(())
            .map_err(|_| failure("native retirement controller disappeared"))
    };
    let result = AssertUnwindSafe(operation).catch_unwind().await;
    let retired = release_checked_peer_io(
        stream,
        async { receipt.await.map_err(failure) },
        checked,
        CLEANUP_BOUND,
    )
    .await;
    match result {
        Ok(result) => {
            let mut errors = Vec::new();
            retain(&mut errors, "owned peer operation", result);
            retain(
                &mut errors,
                "native value/scope before peer I/O drop",
                retired,
            );
            combined(errors)
        }
        Err(payload) => {
            eprintln!("peer cleanup after original panic: {retired:?}");
            resume_unwind(payload)
        }
    }
}

struct RetirementNativeSetup {
    case: Case,
    address: SocketAddr,
    certificate: Option<Vec<u8>>,
    receipt: oneshot::Sender<TransportRetirement>,
    response_consumed: oneshot::Sender<()>,
    driver_retired: oneshot::Receiver<()>,
    peer_drops: Arc<AtomicUsize>,
}

async fn retirement_native(
    context: WorkerContext,
    setup: RetirementNativeSetup,
) -> io::Result<TransportRetirement> {
    let owner = std::thread::current().id();
    let local_owner = Rc::new(());
    let transport = match &setup.certificate {
        Some(certificate) => {
            let mut options = TlsClientOptions::new("localhost");
            options.add_root_certificate(certificate);
            options.set_alpn_protocols([setup.case.protocol.alpn()]);
            ClientTransportOptions::Tls(options)
        }
        None => ClientTransportOptions::Plaintext,
    };
    let session = HttpConnector::connect(
        &context,
        SocketAddress::new(setup.address.ip().to_string(), setup.address.port()),
        OPERATION_BOUND,
        ConnectionParams {
            transport,
            plaintext_protocol: setup.case.protocol.native(),
            identity_validation: IdentityValidation::Enforcing,
            ..ConnectionParams::default()
        },
        SessionParams::default(),
        CancellationToken::never(),
    )
    .await
    .map_err(failure)?;
    let operation = AssertUnwindSafe(async {
        require(
            session.protocol() == setup.case.protocol.native(),
            "actual native session protocol",
        )?;
        let receipt = session
            .transport_retirement()
            .ok_or_else(|| failure("native receipt missing"))?;
        let mut pending = Box::pin(receipt.wait_retired());
        require(
            poll_fn(|context| Poll::Ready(pending.as_mut().poll(context)))
                .await
                .is_pending(),
            "live native transport has not retired",
        )?;
        drop(pending);
        setup
            .receipt
            .send(receipt)
            .map_err(|_| failure("peer receipt receiver disappeared"))?;
        let mut message = HttpMessage::new();
        message.set_method(HttpMethod::Get).map_err(failure)?;
        message.set_url(b"/retirement", false).map_err(failure)?;
        message.set_http_version(1, 1);
        message.set_secure(setup.case.tls);
        message.set_wants_keepalive(true);
        message.headers_mut().set(b"host", b"localhost");
        let mut response = session
            .send_request(
                HttpSourceHolder::new(Box::new(HttpFixedSource::new(message, None))),
                CancellationToken::never(),
            )
            .await
            .map_err(failure)?;
        let completion = response.completion.clone();
        let mut header = response.source.read_header_event().await.map_err(failure)?;
        require(
            header.headers_mut().status_code().map_err(failure)? == 200
                && setup.case.protocol.matches_message(header.headers()),
            "exact native response metadata",
        )?;
        let mut eom = header.eom();
        while !eom {
            let event = response.source.read_body_event(1).await.map_err(failure)?;
            eom = event.eom();
            match event.into_kind() {
                HttpBodyEventKind::Body(body) => {
                    require(eom && body.chain_length() == 0, "empty response EOM")?
                }
                HttpBodyEventKind::Suspend(resume) => require(
                    resume.await == ResumeStatus::Signalled,
                    "live response resume",
                )?,
                _ => return Err(failure("unexpected empty-response event")),
            }
        }
        drop(response);
        completion.wait().await.map_err(failure)?;
        completion.wait().await.map_err(failure)?;
        setup
            .response_consumed
            .send(())
            .map_err(|_| failure("peer driver observer disappeared"))?;
        setup.driver_retired.await.map_err(failure)?;
        require(
            setup.peer_drops.load(Ordering::SeqCst) == 0,
            "peer I/O remains owned after actual driver retirement",
        )?;
        session.initiate_drain().map_err(failure)?;
        Ok::<(), io::Error>(())
    })
    .catch_unwind()
    .await;
    let mut errors = Vec::new();
    let mut panic = None;
    match operation {
        Ok(result) => retain(&mut errors, "native live-worker operation", result),
        Err(payload) => panic = Some(payload),
    }
    if !errors.is_empty() || panic.is_some() {
        session.cancel();
    }
    let closed = timeout(CLEANUP_BOUND, session.wait_closed())
        .await
        .map_err(failure)
        .and_then(|result| result.map_err(failure));
    retain(
        &mut errors,
        "actual native Run",
        closed.and_then(|summary| {
            require(
                !summary.reset
                    && summary.bytes_read > 0
                    && summary.bytes_written > 0
                    && summary.bytes_scheduled == summary.bytes_written,
                "strict native Run and actual write accounting",
            )
        }),
    );
    retain(
        &mut errors,
        "actual native session and transaction retirement",
        require(
            session.is_finished() && session.active_transactions() == 0,
            "native Run retires every admitted transaction",
        ),
    );
    let receipt = session.transport_retirement();
    retain(
        &mut errors,
        "post-session native receipt",
        require(receipt.is_some(), "receipt survives native completion"),
    );
    drop(session);
    if let Some(receipt) = &receipt {
        for _ in 0..2 {
            retain(
                &mut errors,
                "sticky value and close-scope retirement",
                timeout(CLEANUP_BOUND, receipt.wait_retired())
                    .await
                    .map_err(failure)
                    .and_then(|result| result.map_err(failure)),
            );
        }
    }
    retain(
        &mut errors,
        "same accepting owner",
        require(
            context.handle().is_accepting()
                && context.is_owner_thread()
                && owner == std::thread::current().id()
                && Rc::strong_count(&local_owner) == 1,
            "retirement does not close or transfer the worker",
        ),
    );
    if let Some(payload) = panic {
        eprintln!("native cleanup after original panic: {errors:?}");
        resume_unwind(payload);
    }
    combined(errors)?;
    receipt.ok_or_else(|| failure("checked native receipt missing"))
}

type RetirementOutcome = Result<io::Result<()>, Box<dyn Any + Send>>;

struct RetirementWait {
    future: Option<Pin<Box<dyn Future<Output = RetirementOutcome>>>>,
    outcome: Option<RetirementOutcome>,
}

impl RetirementWait {
    fn new(future: impl Future<Output = RetirementOutcome> + 'static) -> Self {
        Self {
            future: Some(Box::pin(future)),
            outcome: None,
        }
    }

    async fn observe(&mut self) {
        if self.outcome.is_none() {
            self.outcome = Some(self.future.as_mut().unwrap().as_mut().await);
        }
    }

    fn retire(&mut self, failures: &mut RetirementFailures) {
        let future = self.future.take();
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(future))) {
            keep_retirement_panic(&mut failures.panic, payload);
        }
    }

    fn reconcile(&mut self, label: &str, failures: &mut RetirementFailures) {
        self.retire(failures);
        match self.outcome.take() {
            Some(Ok(result)) => retain(&mut failures.errors, label, result),
            Some(Err(payload)) => keep_retirement_panic(&mut failures.panic, payload),
            None => retain(
                &mut failures.errors,
                label,
                Err(failure(
                    "pending owner explicitly disposed, not successful completion",
                )),
            ),
        }
    }
}

#[derive(Default)]
struct RetirementFailures {
    errors: Vec<String>,
    panic: Option<Box<dyn Any + Send>>,
}

struct RetirementWork {
    native: RetirementWait,
    peer: RetirementWait,
    drops: Arc<AtomicUsize>,
}

impl RetirementWork {
    async fn observe(&mut self) {
        tokio::join!(self.native.observe(), self.peer.observe());
    }

    fn reconcile(&mut self, failures: &mut RetirementFailures) {
        self.native.reconcile("native command", failures);
        self.peer.reconcile("real peer I/O retirement", failures);
        retain(
            &mut failures.errors,
            "actual complete peer I/O Drop",
            require(
                self.drops.load(Ordering::SeqCst) == 1,
                "one owner-affine peer I/O value Drop",
            ),
        );
    }
}

struct RetirementPublication {
    ready: oneshot::Receiver<TransportRetirement>,
    release: oneshot::Sender<()>,
}

async fn retirement_case(
    worker: &WorkerHandle,
    case: Case,
) -> io::Result<(RetirementWork, RetirementPublication)> {
    let (certificate, tls) = tls_fixture(case)?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let drops = Arc::new(AtomicUsize::new(0));
    let (receipt_sender, receipt) = oneshot::channel();
    let (response_consumed, consumed) = oneshot::channel();
    let (driver_retired, retired) = oneshot::channel();
    let (publication, checked) = oneshot::channel();
    let (ready, publication_ready) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let call = worker
        .call_local({
            let peer_drops = Arc::clone(&drops);
            move |context| async move {
                AssertUnwindSafe(publish_checked_native(
                    async {
                        let receipt = retirement_native(
                            context,
                            RetirementNativeSetup {
                                case,
                                address,
                                certificate,
                                receipt: receipt_sender,
                                response_consumed,
                                driver_retired: retired,
                                peer_drops,
                            },
                        )
                        .await?;
                        ready
                            .send(receipt)
                            .map_err(|_| failure("checked receipt observer lost"))?;
                        timeout(CLEANUP_BOUND, released)
                            .await
                            .map_err(failure)?
                            .map_err(failure)
                    },
                    publication,
                ))
                .catch_unwind()
                .await
            }
        })
        .map_err(failure)?;
    let peer_drops = Arc::clone(&drops);
    let peer = async move {
        let (socket, _) = listener.accept().await?;
        socket.set_nodelay(true)?;
        let channels = RetirementPeerChannels {
            receipt,
            checked,
            response_consumed: consumed,
            driver_retired,
        };
        match tls {
            Some(config) => {
                let stream = TlsAcceptor::from(config).accept(socket).await?;
                require(
                    stream.get_ref().1.alpn_protocol() == Some(case.protocol.alpn()),
                    "actual enforcing peer ALPN",
                )?;
                retirement_peer(
                    RetirementIo {
                        io: Some(stream),
                        drops: Arc::clone(&peer_drops),
                        owner: std::thread::current().id(),
                    },
                    case.protocol,
                    channels,
                )
                .await
            }
            None => {
                retirement_peer(
                    RetirementIo {
                        io: Some(socket),
                        drops: Arc::clone(&peer_drops),
                        owner: std::thread::current().id(),
                    },
                    case.protocol,
                    channels,
                )
                .await
            }
        }
    };
    Ok((
        RetirementWork {
            native: RetirementWait::new(async move {
                match call.await {
                    Ok(result) => result,
                    Err(error) => Ok(Err(failure(error))),
                }
            }),
            peer: RetirementWait::new(AssertUnwindSafe(peer).catch_unwind()),
            drops,
        },
        RetirementPublication {
            ready: publication_ready,
            release,
        },
    ))
}

async fn observe_retirement_case(
    work: &mut RetirementWork,
    publication: RetirementPublication,
) -> io::Result<TransportRetirement> {
    let RetirementPublication { ready, release } = publication;
    let receipt = tokio::select! {
        biased;
        () = work.observe() => return Err(failure("case ended before final native publication gate")),
        receipt = ready => receipt.map_err(failure)?,
    };
    let mut retired = Box::pin(receipt.wait_retired());
    match poll_fn(|context| Poll::Ready(retired.as_mut().poll(context))).await {
        Poll::Ready(Ok(())) => {}
        _ => {
            return Err(failure(
                "held final publication requires actual ready retirement receipt",
            ));
        }
    }
    require(
        poll_fn(|context| {
            let mut observing = Box::pin(work.observe());
            Poll::Ready(observing.as_mut().poll(context))
        })
        .await
        .is_pending()
            && work.native.outcome.is_none()
            && work.peer.outcome.is_none()
            && work.drops.load(Ordering::SeqCst) == 0,
        "ready receipt cannot release peer I/O before final native outcome",
    )?;
    release
        .send(())
        .map_err(|_| failure("checked native publication owner lost"))?;
    work.observe().await;
    require(
        work.drops.load(Ordering::SeqCst) == 1,
        "checked success releases actual peer I/O",
    )?;
    Ok(receipt)
}

fn keep_retirement_panic(first: &mut Option<Box<dyn Any + Send>>, payload: Box<dyn Any + Send>) {
    if first.is_none() {
        *first = Some(payload);
    } else if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        roxygen_runtime::panic::discard_panic(secondary);
    }
}

fn quiesce_retirement_work(
    runtime: &tokio::runtime::Runtime,
    handle: &RuntimeHandle,
    work: &mut Option<RetirementWork>,
    failures: &mut RetirementFailures,
) {
    for _ in 0..2 {
        let cleanup = catch_unwind(AssertUnwindSafe(|| {
            let quiescence = handle.quiesce(CLEANUP_BOUND);
            runtime.block_on(async {
                tokio::pin!(quiescence);
                match work.as_mut() {
                    Some(work) => tokio::select! {
                        biased;
                        result = &mut quiescence => result,
                        () = work.observe() => quiescence.await,
                    },
                    None => quiescence.await,
                }
            })
        }));
        match cleanup {
            Ok(result) => {
                retain(
                    &mut failures.errors,
                    "actual quiescence",
                    result.map_err(failure),
                );
                break;
            }
            Err(payload) => keep_retirement_panic(&mut failures.panic, payload),
        }
    }
}

fn join_retirement_worker(owner: &mut RuntimeOwner, failures: &mut RetirementFailures) {
    match catch_unwind(AssertUnwindSafe(|| owner.join())) {
        Ok(result) => retain(
            &mut failures.errors,
            "physical join outside Tokio",
            result.map_err(failure).and_then(|report| {
                require(
                    report.workers_joined == 1 && report.workers_forced == 0,
                    "one actual thread joined without forced cleanup",
                )
            }),
        ),
        Err(payload) => keep_retirement_panic(&mut failures.panic, payload),
    }
}

fn reconcile_retirement_work(
    runtime: &tokio::runtime::Runtime,
    work: &mut Option<RetirementWork>,
    failures: &mut RetirementFailures,
) {
    if let Some(work) = work.as_mut() {
        let settled = catch_unwind(AssertUnwindSafe(|| {
            runtime.block_on(poll_fn(|context| {
                let mut observing = Box::pin(work.observe());
                Poll::Ready(observing.as_mut().poll(context))
            }))
        }));
        if let Err(payload) = settled {
            keep_retirement_panic(&mut failures.panic, payload);
        }
        let _entered = runtime.enter();
        work.reconcile(failures);
    }
    *work = None;
}

#[derive(Debug, thiserror::Error)]
#[error("injected final native check")]
struct NativeCheckCause {
    drops: Arc<AtomicUsize>,
}

impl Drop for NativeCheckCause {
    fn drop(&mut self) {
        assert_eq!(self.drops.fetch_add(1, Ordering::SeqCst), 0);
    }
}

async fn checked_native_release_controls(receipt: TransportRetirement) {
    let mut retired = Box::pin(receipt.wait_retired());
    assert!(matches!(
        poll_fn(|context| Poll::Ready(retired.as_mut().poll(context))).await,
        Poll::Ready(Ok(())),
    ));
    for outcome in [
        Some(CheckedNativeOutcome::Passed),
        Some(CheckedNativeOutcome::Failed),
        Some(CheckedNativeOutcome::Panicked),
        None,
    ] {
        let drops = Arc::new(AtomicUsize::new(0));
        let cause_drops = Arc::new(AtomicUsize::new(0));
        let cause = Box::new(NativeCheckCause {
            drops: Arc::clone(&cause_drops),
        });
        let identity = cause.as_ref() as *const NativeCheckCause;
        let (stream, other) = tokio::io::duplex(64);
        let (publication, checked) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let native = async move {
            if outcome.is_none() {
                released.await.map_err(failure)?;
                drop(publication);
                drop(cause);
                return Ok(());
            }
            publish_checked_native(
                async move {
                    released.await.map_err(failure)?;
                    match outcome.unwrap() {
                        CheckedNativeOutcome::Passed => {
                            drop(cause);
                            Ok(())
                        }
                        CheckedNativeOutcome::Failed => {
                            Err(io::Error::other(cause as Box<dyn Error + Send + Sync>))
                        }
                        CheckedNativeOutcome::Panicked => panic_any(cause),
                    }
                },
                publication,
            )
            .await
        };
        let mut native = Box::pin(AssertUnwindSafe(native).catch_unwind());
        let mut peer = Box::pin(release_checked_peer_io(
            RetirementIo {
                io: Some(stream),
                drops: Arc::clone(&drops),
                owner: std::thread::current().id(),
            },
            async { Ok(receipt.clone()) },
            checked,
            CLEANUP_BOUND,
        ));
        assert!(
            poll_fn(|context| Poll::Ready(native.as_mut().poll(context)))
                .await
                .is_pending()
        );
        assert!(
            poll_fn(|context| Poll::Ready(peer.as_mut().poll(context)))
                .await
                .is_pending()
        );
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(cause_drops.load(Ordering::SeqCst), 0);
        release.send(()).expect("held final native check");
        let (native, peer) = tokio::join!(native, peer);
        match outcome {
            Some(CheckedNativeOutcome::Passed) => {
                assert!(matches!(native, Ok(Ok(()))));
                peer.expect("checked success plus real receipt releases I/O");
            }
            Some(CheckedNativeOutcome::Failed) => {
                let error = native
                    .expect("returned native error, not panic")
                    .expect_err("native failure retained");
                assert!(std::ptr::eq(
                    error
                        .get_ref()
                        .unwrap()
                        .downcast_ref::<NativeCheckCause>()
                        .unwrap(),
                    identity,
                ));
                let peer = peer.expect_err("native failure is never healthy I/O release");
                assert!(matches!(
                    peer.get_ref().unwrap().downcast_ref::<NativeReleaseError>(),
                    Some(NativeReleaseError::Rejected(CheckedNativeOutcome::Failed))
                ));
                assert_eq!(cause_drops.load(Ordering::SeqCst), 0);
                drop(error);
            }
            Some(CheckedNativeOutcome::Panicked) => {
                let payload = native.expect_err("exact native panic retained");
                assert!(std::ptr::eq(
                    payload
                        .downcast_ref::<Box<NativeCheckCause>>()
                        .unwrap()
                        .as_ref(),
                    identity,
                ));
                let peer = peer.expect_err("native panic is never healthy I/O release");
                assert!(matches!(
                    peer.get_ref().unwrap().downcast_ref::<NativeReleaseError>(),
                    Some(NativeReleaseError::Rejected(CheckedNativeOutcome::Panicked))
                ));
                assert_eq!(cause_drops.load(Ordering::SeqCst), 0);
                drop(payload);
            }
            None => {
                assert!(matches!(native, Ok(Ok(()))));
                let peer = peer.expect_err("missing handoff is never healthy I/O release");
                assert!(matches!(
                    peer.get_ref().unwrap().downcast_ref::<NativeReleaseError>(),
                    Some(NativeReleaseError::Missing)
                ));
            }
        }
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(cause_drops.load(Ordering::SeqCst), 1);
        drop(other);
    }
}

struct RetirementCommandCapture {
    local: Rc<()>,
    owner: ThreadId,
    drops: Arc<AtomicUsize>,
}

impl Drop for RetirementCommandCapture {
    fn drop(&mut self) {
        assert_eq!(Rc::strong_count(&self.local), 1);
        assert_eq!(self.owner, std::thread::current().id());
        assert_eq!(self.drops.fetch_add(1, Ordering::SeqCst), 0);
    }
}

fn retirement_timeout_control() {
    for panic_result in [false, true] {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut owner = RuntimeOwner::start(RuntimeConfig {
            workers: 1,
            command_capacity: 8,
        })
        .unwrap();
        let handle = owner.handle();
        let worker = handle.worker(0).unwrap();
        let capture_drops = Arc::new(AtomicUsize::new(0));
        let peer_drops = Arc::new(AtomicUsize::new(0));
        let cause_drops = Arc::new(AtomicUsize::new(0));
        let submissions = Arc::new(AtomicUsize::new(0));
        let cause = Box::new(NativeCheckCause {
            drops: Arc::clone(&cause_drops),
        });
        let identity = cause.as_ref() as *const NativeCheckCause;
        let (entered, entry) = oneshot::channel();
        let mut pending = None;
        let mut failures = RetirementFailures::default();
        let mut elapsed = None;
        let operation = catch_unwind(AssertUnwindSafe(|| {
            let call = worker
                .call_local({
                    let drops = Arc::clone(&capture_drops);
                    let submissions = Arc::clone(&submissions);
                    move |context| async move {
                        AssertUnwindSafe(async move {
                            let _capture = RetirementCommandCapture {
                                local: Rc::new(()),
                                owner: std::thread::current().id(),
                                drops,
                            };
                            submissions.fetch_add(1, Ordering::SeqCst);
                            entered
                                .send(())
                                .map_err(|_| failure("held command entry observer lost"))?;
                            context.quiescing().await;
                            require(
                                !context.is_cancelled(),
                                "held command finishes during grace",
                            )?;
                            if panic_result {
                                panic_any(cause);
                            }
                            Err::<(), _>(io::Error::other(cause as Box<dyn Error + Send + Sync>))
                        })
                        .catch_unwind()
                        .await
                    }
                })
                .map_err(failure)?;
            let (stream, other) = tokio::io::duplex(64);
            let io = RetirementIo {
                io: Some((stream, other)),
                drops: Arc::clone(&peer_drops),
                owner: std::thread::current().id(),
            };
            pending = Some(RetirementWork {
                native: RetirementWait::new(async move {
                    match call.await {
                        Ok(result) => result,
                        Err(error) => Ok(Err(failure(error))),
                    }
                }),
                peer: RetirementWait::new(async move {
                    let _io = io;
                    std::future::pending::<RetirementOutcome>().await
                }),
                drops: Arc::clone(&peer_drops),
            });
            runtime.block_on(async {
                timeout(CLEANUP_BOUND, entry)
                    .await
                    .map_err(failure)?
                    .map_err(failure)
            })?;
            let work = pending.as_mut().unwrap();
            let observation = runtime.block_on(async {
                timeout_at(tokio::time::Instant::now(), work.observe()).await?;
                let next = worker
                    .call_local({
                        let submissions = Arc::clone(&submissions);
                        move |_| async move {
                            submissions.fetch_add(1, Ordering::SeqCst);
                        }
                    })
                    .expect("only a mistaken successful observation admits this command");
                next.await.expect("unexpected second command reconciled");
                Ok::<(), tokio::time::error::Elapsed>(())
            });
            elapsed = Some(
                observation
                    .expect_err("actual held work exceeds the control-only expired deadline"),
            );
            require(
                work.native.outcome.is_none() && work.peer.outcome.is_none(),
                "timeout retains both pending wait authorities",
            )?;
            require(
                capture_drops.load(Ordering::SeqCst) == 0 && peer_drops.load(Ordering::SeqCst) == 0,
                "timeout does not dispose admitted native capture or parent peer",
            )?;
            require(
                submissions.load(Ordering::SeqCst) == 1,
                "timeout stops next case admission",
            )
        }));
        match operation {
            Ok(result) => retain(&mut failures.errors, "held timeout control", result),
            Err(payload) => keep_retirement_panic(&mut failures.panic, payload),
        }
        quiesce_retirement_work(&runtime, &handle, &mut pending, &mut failures);
        join_retirement_worker(&mut owner, &mut failures);
        let verification = catch_unwind(AssertUnwindSafe(|| {
            let work = pending.as_mut().expect("timeout retains case owner");
            runtime.block_on(async {
                let mut observing = Box::pin(work.native.observe());
                assert!(
                    poll_fn(|context| Poll::Ready(observing.as_mut().poll(context)))
                        .await
                        .is_ready()
                );
            });
            assert!(
                work.peer.outcome.is_none(),
                "pending peer has no invented terminal outcome"
            );
            assert_eq!(peer_drops.load(Ordering::SeqCst), 0);
            let outcome = work
                .native
                .outcome
                .take()
                .expect("original native outcome survives cleanup and join");
            if panic_result {
                let payload = outcome.expect_err("original admitted command panic");
                assert!(std::ptr::eq(
                    payload
                        .downcast_ref::<Box<NativeCheckCause>>()
                        .unwrap()
                        .as_ref(),
                    identity,
                ));
                drop(payload);
            } else {
                let error = outcome
                    .expect("original command returned, not panicked")
                    .expect_err("original command failure");
                assert!(std::ptr::eq(
                    error
                        .get_ref()
                        .unwrap()
                        .downcast_ref::<NativeCheckCause>()
                        .unwrap(),
                    identity,
                ));
                drop(error);
            }
            assert_eq!(capture_drops.load(Ordering::SeqCst), 1);
            assert_eq!(cause_drops.load(Ordering::SeqCst), 1);
            assert_eq!(submissions.load(Ordering::SeqCst), 1);
            assert!(
                elapsed.is_some(),
                "original observed timeout remains a failure"
            );
        }));
        if let Err(payload) = verification {
            keep_retirement_panic(&mut failures.panic, payload);
        }
        if let Some(mut work) = pending {
            let _entered = runtime.enter();
            work.native.retire(&mut failures);
            work.peer.retire(&mut failures);
            if work.native.outcome.is_some() {
                work.native
                    .reconcile("remaining original timeout-native outcome", &mut failures);
            }
            if work.peer.outcome.is_some() {
                work.peer
                    .reconcile("remaining original timeout-peer outcome", &mut failures);
            }
        }
        retain(
            &mut failures.errors,
            "explicit pending-peer retirement after worker cleanup",
            require(
                peer_drops.load(Ordering::SeqCst) == 1,
                "one actual peer I/O Drop",
            ),
        );
        if let Some(payload) = failures.panic {
            eprintln!("timeout control joined cleanup: {:?}", failures.errors);
            resume_unwind(payload);
        }
        assert!(failures.errors.is_empty(), "{}", failures.errors.join("; "));
    }
}

#[test]
fn native_session_transport_retirement_on_live_worker() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 8,
    })
    .unwrap();
    let handle = owner.handle();
    let mut failures = RetirementFailures::default();
    let mut pending = None;
    let mut last_receipt = None;
    let operation = catch_unwind(AssertUnwindSafe(|| {
        let worker = handle.worker(0).expect("one real native worker");
        for protocol in [Protocol::H1, Protocol::H2] {
            for tls in [false, true] {
                let deadline = tokio::time::Instant::now() + CASE_BOUND;
                let (work, publication) = runtime.block_on(retirement_case(
                    &worker,
                    Case {
                        protocol,
                        tls,
                        concurrent: false,
                    },
                ))?;
                pending = Some(work);
                last_receipt = Some(runtime.block_on(async {
                    timeout_at(
                        deadline,
                        observe_retirement_case(pending.as_mut().unwrap(), publication),
                    )
                    .await
                    .map_err(failure)?
                })?);
                reconcile_retirement_work(&runtime, &mut pending, &mut failures);
                require(
                    failures.errors.is_empty() && failures.panic.is_none(),
                    "native/peer failure stops further case admission",
                )?;
            }
        }
        require(
            worker.is_accepting(),
            "same worker remains accepting after all four native sessions",
        )
    }));
    match operation {
        Ok(result) => retain(&mut failures.errors, "live worker", result),
        Err(payload) => keep_retirement_panic(&mut failures.panic, payload),
    }
    quiesce_retirement_work(&runtime, &handle, &mut pending, &mut failures);
    join_retirement_worker(&mut owner, &mut failures);
    reconcile_retirement_work(&runtime, &mut pending, &mut failures);
    if let Some(payload) = failures.panic {
        eprintln!(
            "native receipt root cleanup after panic: {:?}",
            failures.errors
        );
        resume_unwind(payload);
    }
    assert!(failures.errors.is_empty(), "{}", failures.errors.join("; "));
    runtime.block_on(async {
        timeout(
            CLEANUP_BOUND,
            checked_native_release_controls(last_receipt.unwrap()),
        )
        .await
        .expect("bounded final-outcome controls with actual native receipt");
    });
    retirement_timeout_control();
}
