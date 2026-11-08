//! Native fixed-worker HTTP/1.1 and HTTP/2 client surfaces.

#![cfg_attr(all(test, target_os = "linux", fbcode_build), feature(linux_pidfd))]

mod connect_session;
pub mod connector;
pub mod pool;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::future::poll_fn;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::path::PathBuf;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::RwLock;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEventKind;
use roxygen_core::events::ResumeStatus;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::headers::HttpHeaders;
use roxygen_core::http::HttpMethod;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_core::source_reader::ErrorContext;
use roxygen_core::source_reader::HttpSourceReader;
use roxygen_core::url::Url;
use roxygen_core::url::UrlMode;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::wait::CancellableBaton;
use roxygen_runtime::wait::WaitStatus;
use roxygen_runtime::worker::WorkerContext;
use roxygen_session::session::RequestReservation;
pub use roxygen_transport::tls::TlsCertificateVerifier;

use crate::connector::ClientSession;
use crate::connector::ConnectionParams;
use crate::connector::CoroDnsResolver;
use crate::connector::DnsResolver;
use crate::connector::HttpConnector;
use crate::connector::LocalClientFuture;
use crate::connector::SessionParams;

pub type RequestHeaderMap = BTreeMap<Box<[u8]>, Box<[u8]>>;

const DEFAULT_RESPONSE_LIMIT: usize = u32::MAX as usize;
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

pub trait ClientRequestLog: 'static {
    fn request_filter(&self, source: HttpSourceHolder) -> Result<HttpSourceHolder, HttpError> {
        Ok(source)
    }

    fn response_filter(&self, source: HttpSourceHolder) -> Result<HttpSourceHolder, HttpError> {
        Ok(source)
    }

    fn log_when_done(&self) -> LocalClientFuture<()> {
        Box::pin(std::future::ready(()))
    }

    fn log_on_drop(&self) {}
}

pub trait ClientRequestLogger: Send + Sync + 'static {
    fn create(&self, session: &ClientSession) -> Box<dyn ClientRequestLog>;
}

struct NoopClientRequestLogger;

struct NoopClientRequestLog;

impl ClientRequestLog for NoopClientRequestLog {}

impl ClientRequestLogger for NoopClientRequestLogger {
    fn create(&self, _session: &ClientSession) -> Box<dyn ClientRequestLog> {
        Box::new(NoopClientRequestLog)
    }
}

struct ClientRequestLogGuard {
    logger: Option<Box<dyn ClientRequestLog>>,
}

impl ClientRequestLogGuard {
    fn new(logger: Option<Box<dyn ClientRequestLog>>) -> Self {
        Self { logger }
    }

    fn get(&self) -> Option<&dyn ClientRequestLog> {
        self.logger.as_deref()
    }

    async fn finish<T>(mut self, result: Result<T, HttpError>) -> Result<T, HttpError> {
        if let Some(logger) = self.logger.as_ref() {
            logger.log_when_done().await;
        }
        self.logger.take();
        result
    }
}

impl Drop for ClientRequestLogGuard {
    fn drop(&mut self) {
        if let Some(logger) = self.logger.take() {
            let _ = catch_unwind(AssertUnwindSafe(|| logger.log_on_drop()));
        }
    }
}

static DEFAULT_LOGGER: OnceLock<RwLock<Option<Arc<dyn ClientRequestLogger>>>> = OnceLock::new();

fn default_logger_slot() -> &'static RwLock<Option<Arc<dyn ClientRequestLogger>>> {
    DEFAULT_LOGGER.get_or_init(|| RwLock::new(Some(Arc::new(NoopClientRequestLogger))))
}

#[derive(Clone, Default)]
pub enum ClientLoggerSelection {
    #[default]
    BuiltIn,
    Default,
    Explicit(Arc<dyn ClientRequestLogger>),
}

#[derive(Clone)]
pub struct RequestOptions {
    pub timeout: Option<Duration>,
    pub headers: RequestHeaderMap,
    pub cancellation: CancellationToken,
    pub connection: Option<ConnectionParams>,
    pub session: Option<SessionParams>,
    pub resolver: Option<Rc<dyn DnsResolver>>,
    pub server_address: Option<Box<str>>,
    pub maximum_response_body: usize,
    pub logger: ClientLoggerSelection,
}

impl Default for RequestOptions {
    fn default() -> Self {
        Self {
            timeout: None,
            headers: BTreeMap::new(),
            cancellation: CancellationToken::never(),
            connection: None,
            session: None,
            resolver: None,
            server_address: None,
            maximum_response_body: DEFAULT_RESPONSE_LIMIT,
            logger: ClientLoggerSelection::Default,
        }
    }
}

pub struct RequestSpec {
    pub method: HttpMethod,
    pub url: Url,
    pub headers: RequestHeaderMap,
    pub body: Option<Box<[u8]>>,
    pub timeout: Duration,
    pub cancellation: CancellationToken,
    pub maximum_response_body: usize,
    pub logger: ClientLoggerSelection,
}

impl RequestSpec {
    pub fn get(url: Url) -> Self {
        Self {
            method: HttpMethod::Get,
            url,
            headers: BTreeMap::new(),
            body: None,
            timeout: Duration::ZERO,
            cancellation: CancellationToken::never(),
            maximum_response_body: DEFAULT_RESPONSE_LIMIT,
            logger: ClientLoggerSelection::BuiltIn,
        }
    }

    pub fn post(url: Url, body: impl AsRef<[u8]>) -> Self {
        Self {
            method: HttpMethod::Post,
            url,
            headers: BTreeMap::new(),
            body: Some(body.as_ref().to_vec().into_boxed_slice()),
            timeout: Duration::ZERO,
            cancellation: CancellationToken::never(),
            maximum_response_body: DEFAULT_RESPONSE_LIMIT,
            logger: ClientLoggerSelection::BuiltIn,
        }
    }
}

#[derive(Debug)]
pub struct Response {
    pub headers: HttpMessage,
    pub body: BufQueue,
    pub trailers: Option<HttpHeaders>,
}

/// Shared result slot paired with [`HttpClient::make_default_reader`].
#[derive(Clone)]
pub struct DefaultResponseHandle {
    state: Rc<RefCell<DefaultResponseState>>,
}

struct DefaultResponseState {
    headers: Option<HttpMessage>,
    body: BufQueue,
    trailers: Option<HttpHeaders>,
    error: Option<HttpError>,
    maximum_body: usize,
}

impl DefaultResponseHandle {
    pub fn take_response(&self) -> Result<Response, HttpError> {
        let mut state = self.state.borrow_mut();
        if let Some(error) = state.error.take() {
            return Err(error);
        }
        let headers = state.headers.take().ok_or_else(|| {
            client_error(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"response reader completed without final headers",
            )
        })?;
        let maximum_body = state.maximum_body;
        Ok(Response {
            headers,
            body: std::mem::replace(&mut state.body, BufQueue::new(maximum_body)),
            trailers: state.trailers.take(),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VisitAction {
    Continue,
    Cancel,
}

pub trait ResponseVisitor {
    fn on_headers(&mut self, _headers: &HttpMessage, _is_final: bool, _eom: bool) -> VisitAction {
        VisitAction::Continue
    }

    fn on_body(&mut self, _body: &BufQueue, _eom: bool) -> VisitAction {
        VisitAction::Continue
    }

    fn on_trailers(&mut self, _trailers: &HttpHeaders) {}
}

/// An already-built request source and its response-reading policy.
pub struct SourceRequest {
    pub source: HttpSourceHolder,
    pub reservation: Option<RequestReservation>,
    pub timeout: Duration,
    pub cancellation: CancellationToken,
    pub maximum_response_body: usize,
    pub visitor: Option<Box<dyn ResponseVisitor>>,
    pub logger: ClientLoggerSelection,
}

/// An already-built request source and the source reader that owns its response.
pub struct SourceReaderRequest {
    pub source: HttpSourceHolder,
    pub reservation: Option<RequestReservation>,
    pub timeout: Duration,
    pub cancellation: CancellationToken,
    pub maximum_read_size: u32,
    pub reader: HttpSourceReader,
    pub logger: ClientLoggerSelection,
}

impl SourceReaderRequest {
    pub fn new(source: HttpSourceHolder, reader: HttpSourceReader) -> Self {
        Self {
            source,
            reservation: None,
            timeout: Duration::ZERO,
            cancellation: CancellationToken::never(),
            maximum_read_size: u32::MAX,
            reader,
            logger: ClientLoggerSelection::BuiltIn,
        }
    }
}

impl SourceRequest {
    pub fn new(source: HttpSourceHolder) -> Self {
        Self {
            source,
            reservation: None,
            timeout: Duration::ZERO,
            cancellation: CancellationToken::never(),
            maximum_response_body: DEFAULT_RESPONSE_LIMIT,
            visitor: None,
            logger: ClientLoggerSelection::BuiltIn,
        }
    }
}

/// Convenience request facade mirroring the frozen `HTTPClient` helpers.
pub struct HttpClient;

impl HttpClient {
    pub fn set_default_ca_paths(paths: Vec<PathBuf>) {
        connector::set_default_ca_paths(paths);
    }

    pub fn get_default_ca_paths() -> Vec<PathBuf> {
        connector::get_default_ca_paths()
    }

    pub fn set_default_fizz_cert_verifier(verifier: Option<Arc<dyn TlsCertificateVerifier>>) {
        connector::set_default_cert_verifier(verifier);
    }

    pub fn get_default_cert_verifier() -> Option<Arc<dyn TlsCertificateVerifier>> {
        connector::get_default_cert_verifier()
    }

    pub fn set_default_log_impl(logger: Option<Arc<dyn ClientRequestLogger>>) {
        let old = {
            let mut slot = default_logger_slot()
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            std::mem::replace(&mut *slot, logger)
        };
        let _ = catch_unwind(AssertUnwindSafe(|| drop(old)));
    }

    pub fn get_default_log_impl() -> Option<Arc<dyn ClientRequestLogger>> {
        default_logger_slot()
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn make_default_reader(maximum_body: usize) -> (HttpSourceReader, DefaultResponseHandle) {
        let state = Rc::new(RefCell::new(DefaultResponseState {
            headers: None,
            body: BufQueue::new(maximum_body),
            trailers: None,
            error: None,
            maximum_body,
        }));
        let mut reader = HttpSourceReader::new();
        let header_state = Rc::clone(&state);
        reader
            .on_headers(move |headers, is_final, _| {
                if is_final {
                    header_state.borrow_mut().headers = Some(headers);
                }
                HttpSourceReader::CONTINUE
            })
            .expect("a new reader accepts its header callback");
        let body_state = Rc::clone(&state);
        reader
            .on_body(move |mut body, _| {
                let result = body_state.borrow_mut().body.append(&mut body);
                if let Err(error) = result {
                    body_state.borrow_mut().error = Some(client_error(
                        HttpErrorCode::ENHANCE_YOUR_CALM,
                        error.to_string(),
                    ));
                    return HttpSourceReader::CANCEL;
                }
                HttpSourceReader::CONTINUE
            })
            .expect("a new reader accepts its body callback");
        let trailer_state = Rc::clone(&state);
        reader
            .on_trailers(move |trailers| trailer_state.borrow_mut().trailers = Some(trailers))
            .expect("a new reader accepts its trailer callback");
        reader
            .on_error_result(|context, error| {
                Err(response_error(
                    match context {
                        ErrorContext::Headers => b"headers",
                        ErrorContext::Body => b"body",
                    },
                    error,
                ))
            })
            .expect("a new reader accepts its error callback");
        (reader, DefaultResponseHandle { state })
    }

    pub fn read_response(
        source: HttpSourceHolder,
    ) -> LocalClientFuture<Result<Response, HttpError>> {
        Box::pin(async move {
            let (mut reader, response) = Self::make_default_reader(DEFAULT_RESPONSE_LIMIT);
            reader
                .set_source(Box::new(source))
                .unwrap_or_else(|_| unreachable!("a new reader accepts its first source"));
            reader.read(u32::MAX).await?;
            response.take_response()
        })
    }

    pub fn get(
        context: &WorkerContext,
        url: impl AsRef<[u8]>,
        options: RequestOptions,
    ) -> LocalClientFuture<Result<Response, HttpError>> {
        Self::request_new_session(context, url.as_ref(), HttpMethod::Get, None, options)
    }

    pub fn post(
        context: &WorkerContext,
        url: impl AsRef<[u8]>,
        body: impl AsRef<[u8]>,
        options: RequestOptions,
    ) -> LocalClientFuture<Result<Response, HttpError>> {
        Self::request_new_session(
            context,
            url.as_ref(),
            HttpMethod::Post,
            Some(body.as_ref().to_vec().into_boxed_slice()),
            options,
        )
    }

    pub fn get_with_reader(
        context: &WorkerContext,
        url: impl AsRef<[u8]>,
        options: RequestOptions,
        reader: HttpSourceReader,
    ) -> LocalClientFuture<Result<(), HttpError>> {
        Self::request_new_session_with_reader(
            context,
            url.as_ref(),
            HttpMethod::Get,
            None,
            options,
            reader,
        )
    }

    pub fn post_with_reader(
        context: &WorkerContext,
        url: impl AsRef<[u8]>,
        body: impl AsRef<[u8]>,
        options: RequestOptions,
        reader: HttpSourceReader,
    ) -> LocalClientFuture<Result<(), HttpError>> {
        Self::request_new_session_with_reader(
            context,
            url.as_ref(),
            HttpMethod::Post,
            Some(body.as_ref().to_vec().into_boxed_slice()),
            options,
            reader,
        )
    }

    fn request_new_session(
        context: &WorkerContext,
        url: &[u8],
        method: HttpMethod,
        body: Option<Box<[u8]>>,
        options: RequestOptions,
    ) -> LocalClientFuture<Result<Response, HttpError>> {
        let parsed = Url::parse(url, false, UrlMode::StrictCompat);
        if !parsed.is_valid() || !parsed.has_host() {
            return Box::pin(std::future::ready(Err(client_error(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"invalid URL",
            ))));
        }
        let context = context.clone();
        Box::pin(async move {
            let session = connect_for_url(&context, &parsed, &options).await?;
            let timeout = options.timeout.unwrap_or(DEFAULT_REQUEST_TIMEOUT);
            let mut spec = match method {
                HttpMethod::Get => RequestSpec::get(parsed),
                HttpMethod::Post => RequestSpec::post(parsed, body.as_deref().unwrap_or_default()),
                _ => RequestSpec {
                    method,
                    url: parsed,
                    headers: BTreeMap::new(),
                    body,
                    timeout: Duration::ZERO,
                    cancellation: CancellationToken::never(),
                    maximum_response_body: DEFAULT_RESPONSE_LIMIT,
                    logger: ClientLoggerSelection::BuiltIn,
                },
            };
            spec.headers = options.headers;
            spec.timeout = timeout;
            spec.cancellation = options.cancellation;
            spec.maximum_response_body = options.maximum_response_body;
            spec.logger = options.logger;
            Self::request(&session, None, spec).await
        })
    }

    fn request_new_session_with_reader(
        context: &WorkerContext,
        url: &[u8],
        method: HttpMethod,
        body: Option<Box<[u8]>>,
        options: RequestOptions,
        reader: HttpSourceReader,
    ) -> LocalClientFuture<Result<(), HttpError>> {
        let parsed = Url::parse(url, false, UrlMode::StrictCompat);
        if !parsed.is_valid() || !parsed.has_host() {
            return Box::pin(std::future::ready(Err(client_error(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"invalid URL",
            ))));
        }
        let context = context.clone();
        Box::pin(async move {
            let session = connect_for_url(&context, &parsed, &options).await?;
            let timeout = options.timeout.unwrap_or(DEFAULT_REQUEST_TIMEOUT);
            let mut spec = match method {
                HttpMethod::Get => RequestSpec::get(parsed),
                HttpMethod::Post => RequestSpec::post(parsed, body.as_deref().unwrap_or_default()),
                _ => RequestSpec {
                    method,
                    url: parsed,
                    headers: BTreeMap::new(),
                    body,
                    timeout: Duration::ZERO,
                    cancellation: CancellationToken::never(),
                    maximum_response_body: DEFAULT_RESPONSE_LIMIT,
                    logger: ClientLoggerSelection::BuiltIn,
                },
            };
            spec.headers = options.headers;
            spec.timeout = timeout;
            spec.cancellation = options.cancellation;
            spec.maximum_response_body = options.maximum_response_body;
            spec.logger = options.logger;
            Self::request_with_reader(&session, None, spec, reader).await
        })
    }

    pub fn get_with_session(
        session: &ClientSession,
        url: Url,
        options: RequestOptions,
    ) -> LocalClientFuture<Result<Response, HttpError>> {
        let mut spec = RequestSpec::get(url);
        spec.headers = options.headers;
        spec.timeout = options.timeout.unwrap_or(Duration::ZERO);
        spec.cancellation = options.cancellation;
        spec.maximum_response_body = options.maximum_response_body;
        spec.logger = options.logger;
        Self::request(session, None, spec)
    }

    pub fn post_with_session(
        session: &ClientSession,
        url: Url,
        body: impl AsRef<[u8]>,
        options: RequestOptions,
    ) -> LocalClientFuture<Result<Response, HttpError>> {
        let mut spec = RequestSpec::post(url, body);
        spec.headers = options.headers;
        spec.timeout = options.timeout.unwrap_or(Duration::ZERO);
        spec.cancellation = options.cancellation;
        spec.maximum_response_body = options.maximum_response_body;
        spec.logger = options.logger;
        Self::request(session, None, spec)
    }

    pub fn request(
        session: &ClientSession,
        reservation: Option<RequestReservation>,
        spec: RequestSpec,
    ) -> LocalClientFuture<Result<Response, HttpError>> {
        let session = session.clone();
        Box::pin(async move {
            if !spec.url.is_valid() || !spec.url.has_host() {
                return Err(client_error(
                    HttpErrorCode::INVALID_STATE_TRANSITION,
                    b"invalid URL",
                ));
            }
            let request = SourceRequest {
                source: request_source(&spec)?,
                reservation,
                timeout: spec.timeout,
                cancellation: spec.cancellation,
                maximum_response_body: spec.maximum_response_body,
                visitor: None,
                logger: spec.logger,
            };
            Self::request_source(&session, request).await
        })
    }

    pub fn request_with_visitor(
        session: &ClientSession,
        reservation: Option<RequestReservation>,
        spec: RequestSpec,
        visitor: Box<dyn ResponseVisitor>,
    ) -> LocalClientFuture<Result<Response, HttpError>> {
        let session = session.clone();
        Box::pin(async move {
            validate_request_url(&spec.url)?;
            let request = SourceRequest {
                source: request_source(&spec)?,
                reservation,
                timeout: spec.timeout,
                cancellation: spec.cancellation,
                maximum_response_body: spec.maximum_response_body,
                visitor: Some(visitor),
                logger: spec.logger,
            };
            Self::request_source(&session, request).await
        })
    }

    pub fn request_with_reader(
        session: &ClientSession,
        reservation: Option<RequestReservation>,
        spec: RequestSpec,
        reader: HttpSourceReader,
    ) -> LocalClientFuture<Result<(), HttpError>> {
        let session = session.clone();
        Box::pin(async move {
            validate_request_url(&spec.url)?;
            let request = SourceReaderRequest {
                source: request_source(&spec)?,
                reservation,
                timeout: spec.timeout,
                cancellation: spec.cancellation,
                maximum_read_size: u32::try_from(spec.maximum_response_body).unwrap_or(u32::MAX),
                reader,
                logger: spec.logger,
            };
            Self::request_source_with_reader(&session, request).await
        })
    }

    pub fn request_source(
        session: &ClientSession,
        request: SourceRequest,
    ) -> LocalClientFuture<Result<Response, HttpError>> {
        let session = session.clone();
        Box::pin(async move {
            let logger = ClientRequestLogGuard::new(
                select_logger(request.logger).map(|logger| logger.create(&session)),
            );
            let result = async {
                let source = match logger.get() {
                    Some(logger) => logger.request_filter(request.source)?,
                    None => request.source,
                };
                let response_cancellation = request.cancellation.clone();
                let response = match request.reservation {
                    Some(reservation) => {
                        session
                            .send_reserved_request(source, reservation, request.cancellation)
                            .await
                    }
                    None => session.send_request(source, request.cancellation).await,
                };
                let response = response?;
                let mut response_source = response.source;
                if request.timeout != Duration::ZERO {
                    response_source.set_read_timeout(request.timeout);
                }
                let source = match logger.get() {
                    Some(logger) => logger.response_filter(response_source)?,
                    None => response_source,
                };
                read_response(
                    source,
                    request.maximum_response_body,
                    request.visitor,
                    response_cancellation,
                )
                .await
            }
            .await;
            logger.finish(result).await
        })
    }

    pub fn request_source_with_reader(
        session: &ClientSession,
        request: SourceReaderRequest,
    ) -> LocalClientFuture<Result<(), HttpError>> {
        let session = session.clone();
        Box::pin(async move {
            let logger = ClientRequestLogGuard::new(
                select_logger(request.logger).map(|logger| logger.create(&session)),
            );
            let result = async {
                let source = match logger.get() {
                    Some(logger) => logger.request_filter(request.source)?,
                    None => request.source,
                };
                let response_cancellation = request.cancellation.clone();
                let response = match request.reservation {
                    Some(reservation) => {
                        session
                            .send_reserved_request(source, reservation, request.cancellation)
                            .await
                    }
                    None => session.send_request(source, request.cancellation).await,
                };
                let response = response?;
                let mut response_source = response.source;
                if request.timeout != Duration::ZERO {
                    response_source.set_read_timeout(request.timeout);
                }
                let source = match logger.get() {
                    Some(logger) => logger.response_filter(response_source)?,
                    None => response_source,
                };
                let mut reader = request.reader;
                reader.set_source(Box::new(source)).map_err(|error| {
                    client_error(
                        HttpErrorCode::INVALID_STATE_TRANSITION,
                        error.reason().to_string(),
                    )
                })?;
                reader
                    .read_with_cancellation(request.maximum_read_size, response_cancellation)
                    .await
            }
            .await;
            logger.finish(result).await
        })
    }

    pub fn get_session(
        context: &WorkerContext,
        host: impl Into<Box<str>>,
        port: u16,
        secure: bool,
        timeout: Duration,
        read_timeout: Duration,
        server_address: Option<Box<str>>,
        connection: Option<ConnectionParams>,
        resolver: Option<Rc<dyn DnsResolver>>,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<ClientSession, HttpError>> {
        let context = context.clone();
        let host = host.into();
        Box::pin(async move {
            let resolved = match server_address {
                Some(address) => crate::connector::ResolvedHost {
                    primary: roxygen_core::message::SocketAddress::new(address.as_bytes(), port),
                    fallback: None,
                },
                None => {
                    let mut resolved = CoroDnsResolver::resolve_host(
                        &context,
                        host.clone(),
                        timeout,
                        cancellation.clone(),
                        resolver,
                    )
                    .await?;
                    resolved.primary =
                        roxygen_core::message::SocketAddress::new(resolved.primary.host(), port);
                    resolved.fallback = resolved.fallback.map(|address| {
                        roxygen_core::message::SocketAddress::new(address.host(), port)
                    });
                    resolved
                }
            };
            let connection = connection
                .unwrap_or_default()
                .for_destination(&host, secure);
            let session = SessionParams::http_client_defaults(Some(read_timeout));
            match resolved.fallback {
                Some(fallback) => {
                    HttpConnector::happy_eyeballs_connect(
                        &context,
                        resolved.primary,
                        fallback,
                        timeout,
                        Duration::from_millis(150),
                        connection,
                        session,
                        cancellation,
                    )
                    .await
                }
                None => {
                    HttpConnector::connect(
                        &context,
                        resolved.primary,
                        timeout,
                        connection,
                        session,
                        cancellation,
                    )
                    .await
                }
            }
        })
    }
}

async fn connect_for_url(
    context: &WorkerContext,
    url: &Url,
    options: &RequestOptions,
) -> Result<ClientSession, HttpError> {
    let timeout = options.timeout.unwrap_or(DEFAULT_REQUEST_TIMEOUT);
    let host = String::from_utf8_lossy(url.host())
        .into_owned()
        .into_boxed_str();
    let resolved = match &options.server_address {
        Some(address) => crate::connector::ResolvedHost {
            primary: roxygen_core::message::SocketAddress::new(address.as_bytes(), url.port()),
            fallback: None,
        },
        None => {
            let mut resolved = CoroDnsResolver::resolve_host(
                context,
                host.clone(),
                timeout,
                options.cancellation.clone(),
                options.resolver.clone(),
            )
            .await?;
            resolved.primary =
                roxygen_core::message::SocketAddress::new(resolved.primary.host(), url.port());
            resolved.fallback = resolved.fallback.map(|address| {
                roxygen_core::message::SocketAddress::new(address.host(), url.port())
            });
            resolved
        }
    };
    let connection = options
        .connection
        .clone()
        .unwrap_or_default()
        .for_destination(&host, url.is_secure());
    let session = session_params_for_request(options);
    match resolved.fallback {
        Some(fallback) => {
            HttpConnector::happy_eyeballs_connect(
                context,
                resolved.primary,
                fallback,
                timeout,
                Duration::from_millis(150),
                connection,
                session,
                options.cancellation.clone(),
            )
            .await
        }
        None => {
            HttpConnector::connect(
                context,
                resolved.primary,
                timeout,
                connection,
                session,
                options.cancellation.clone(),
            )
            .await
        }
    }
}

fn session_params_for_request(options: &RequestOptions) -> SessionParams {
    options.session.clone().unwrap_or_else(|| {
        SessionParams::http_client_defaults(Some(
            options.timeout.unwrap_or(DEFAULT_REQUEST_TIMEOUT),
        ))
    })
}

fn request_source(spec: &RequestSpec) -> Result<HttpSourceHolder, HttpError> {
    let mut message = HttpMessage::new();
    message
        .set_url(spec.url.make_relative_url(), false)
        .map_err(|error| {
            client_error(HttpErrorCode::INVALID_STATE_TRANSITION, error.to_string())
        })?;
    message.set_method(spec.method).map_err(|error| {
        client_error(HttpErrorCode::INVALID_STATE_TRANSITION, error.to_string())
    })?;
    message.set_http_version(1, 1);
    let body = match &spec.body {
        Some(body) if !body.is_empty() => {
            let mut queue = BufQueue::new(body.len());
            queue
                .push_bytes(Bytes::copy_from_slice(body))
                .map_err(|error| client_error(HttpErrorCode::INTERNAL_ERROR, error.to_string()))?;
            Some(queue)
        }
        _ => None,
    };
    let mut source = HttpFixedSource::new(message, body);
    let message = source
        .message_mut()
        .expect("a fixed request retains its header message");
    spec.headers
        .iter()
        .for_each(|(name, value)| message.headers_mut().add(name, value));
    if !message.headers().exists(HttpHeaderCode::Host) {
        message
            .headers_mut()
            .add(HttpHeaderCode::Host, spec.url.host_and_port_omit_default());
    }
    message.set_wants_keepalive(true);
    message.set_secure(spec.url.is_secure());
    Ok(HttpSourceHolder::new(Box::new(source)))
}

/// Collects an arbitrary response source using the standard response policy.
pub async fn read_response(
    mut source: HttpSourceHolder,
    maximum_body: usize,
    mut visitor: Option<Box<dyn ResponseVisitor>>,
    cancellation: CancellationToken,
) -> Result<Response, HttpError> {
    let (headers, header_eom) = loop {
        let mut header_event = await_or_cancel(source.read_header_event(), cancellation.clone())
            .await?
            .map_err(|error| response_error(b"headers", error))?;
        let is_final = header_event.is_final().map_err(|error| {
            client_error(HttpErrorCode::INVALID_STATE_TRANSITION, error.to_string())
        })?;
        let header_eom = header_event.eom();
        let headers = header_event.into_headers();
        if visitor.as_mut().is_some_and(|visitor| {
            visitor.on_headers(&headers, is_final, header_eom) == VisitAction::Cancel
        }) {
            return Ok(Response {
                headers,
                body: BufQueue::new(maximum_body),
                trailers: None,
            });
        }
        if is_final {
            break (headers, header_eom);
        }
    };
    let mut response = Response {
        headers,
        body: BufQueue::new(maximum_body),
        trailers: None,
    };
    if header_eom {
        return Ok(response);
    }
    loop {
        let maximum_read = u32::try_from(maximum_body).unwrap_or(u32::MAX);
        let event = await_or_cancel(source.read_body_event(maximum_read), cancellation.clone())
            .await?
            .map_err(|error| response_error(b"body", error))?;
        let eom = event.eom();
        match event.into_kind() {
            HttpBodyEventKind::Body(mut body) => {
                if visitor
                    .as_mut()
                    .is_some_and(|visitor| visitor.on_body(&body, eom) == VisitAction::Cancel)
                {
                    return Ok(response);
                }
                response.body.append(&mut body).map_err(|error| {
                    client_error(HttpErrorCode::ENHANCE_YOUR_CALM, error.to_string())
                })?;
            }
            HttpBodyEventKind::Trailers(trailers) => {
                if let Some(visitor) = &mut visitor {
                    visitor.on_trailers(&trailers);
                }
                response.trailers = Some(trailers);
            }
            HttpBodyEventKind::Suspend(resume) => {
                match await_or_cancel(resume, cancellation.clone()).await? {
                    ResumeStatus::Signalled => continue,
                    ResumeStatus::TimedOut => {
                        return Err(client_error(
                            HttpErrorCode::READ_TIMEOUT,
                            b"response body suspended until timeout",
                        ));
                    }
                    ResumeStatus::Cancelled | ResumeStatus::NotReady => {
                        return Err(client_error(
                            HttpErrorCode::CORO_CANCELLED,
                            b"response body suspension cancelled",
                        ));
                    }
                }
            }
            HttpBodyEventKind::Upgrade
            | HttpBodyEventKind::PushPromise(_)
            | HttpBodyEventKind::Padding(_) => {}
        }
        if eom {
            return Ok(response);
        }
    }
}

fn select_logger(selection: ClientLoggerSelection) -> Option<Arc<dyn ClientRequestLogger>> {
    match selection {
        ClientLoggerSelection::BuiltIn => Some(Arc::new(NoopClientRequestLogger)),
        ClientLoggerSelection::Default => HttpClient::get_default_log_impl(),
        ClientLoggerSelection::Explicit(logger) => Some(logger),
    }
}

fn validate_request_url(url: &Url) -> Result<(), HttpError> {
    if !url.is_valid() || !url.has_host() {
        return Err(client_error(
            HttpErrorCode::INVALID_STATE_TRANSITION,
            b"invalid URL",
        ));
    }
    Ok(())
}

async fn await_or_cancel<T>(
    mut operation: Pin<Box<dyn Future<Output = T> + 'static>>,
    cancellation: CancellationToken,
) -> Result<T, HttpError> {
    if cancellation.is_cancellation_requested() {
        return Err(client_error(
            HttpErrorCode::CORO_CANCELLED,
            b"request cancelled",
        ));
    }
    let baton = CancellableBaton::new();
    let mut cancelled = Box::pin(baton.wait(cancellation));
    poll_fn(|context| {
        if let Poll::Ready(value) = operation.as_mut().poll(context) {
            return Poll::Ready(Ok(value));
        }
        match cancelled.as_mut().poll(context) {
            Poll::Ready(WaitStatus::Cancelled) => Poll::Ready(Err(client_error(
                HttpErrorCode::CORO_CANCELLED,
                b"request cancelled",
            ))),
            Poll::Ready(_) | Poll::Pending => Poll::Pending,
        }
    })
    .await
}

fn response_error(context: &[u8], mut error: HttpError) -> HttpError {
    let mut message = b"Error receiving response ".to_vec();
    message.extend_from_slice(context);
    message.extend_from_slice(b", err=");
    message.extend_from_slice(error.message());
    error.set_message(message);
    error
}

fn client_error(code: HttpErrorCode, message: impl AsRef<[u8]>) -> HttpError {
    HttpError::with_message(code, message)
}

#[cfg(test)]
#[path = "../tests/integration.rs"]
mod integration_tests;

#[cfg(test)]
#[path = "../tests/attachment.rs"]
mod attachment_tests;

#[cfg(test)]
#[path = "../tests/pool_attachment.rs"]
mod pool_attachment_tests;

#[cfg(test)]
#[path = "../tests/native_readiness.rs"]
mod native_readiness_tests;

#[cfg(all(test, target_os = "linux"))]
#[path = "../tests/native_staged_cleanup.rs"]
mod native_staged_cleanup_tests;

#[cfg(test)]
#[path = "../tests/ping_probes.rs"]
mod ping_probes_tests;

#[cfg(test)]
#[path = "../tests/idle_sweep_cost.rs"]
mod idle_sweep_cost_tests;

#[cfg(test)]
#[path = "../tests/pool_idle_timeout.rs"]
mod pool_idle_timeout_tests;
