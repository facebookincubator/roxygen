/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::fs;
use std::io::Read;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use cookie::Cookie;
use cookie::time::OffsetDateTime;
use flate2::read::DeflateDecoder;
use flate2::read::GzDecoder;
use flate2::read::ZlibDecoder;
use futures::StreamExt;
use futures::stream;
use hickory_resolver::TokioResolver;
use hickory_resolver::config::ConnectionConfig;
use hickory_resolver::config::LookupIpStrategy;
use hickory_resolver::config::NameServerConfig;
use hickory_resolver::config::ResolverConfig;
use hickory_resolver::config::ResolverOpts;
use hickory_resolver::net::runtime::TokioRuntimeProvider;
use roxygen_client::Response;
use roxygen_client::connector::ClientTransportOptions;
use roxygen_client::connector::ConnectionParams;
use roxygen_client::connector::DnsResolver;
use roxygen_client::connector::LocalClientFuture;
use roxygen_client::pool::ClientConnectionCache;
use roxygen_client::pool::GetSessionResult;
use roxygen_client::pool::PoolError;
use roxygen_client::pool::PoolParams;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEventKind;
use roxygen_core::events::ResumeStatus;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::http::CodecProtocol;
use roxygen_core::http::Method;
use roxygen_core::message::HttpMessage;
use roxygen_core::message::SocketAddress as RoxygenSocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_core::url::Url as RoxygenUrl;
use roxygen_core::url::UrlMode;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::worker::WorkerContext;
#[cfg(target_os = "linux")]
use roxygen_transport::tcp::TcpSocketOption;
use roxygen_transport::tls::TlsCertificateVerifier;
use roxygen_transport::tls::TlsClientOptions;
use roxygen_transport::tls::TlsVersion;
use url::Url;

use crate::CliError;
use crate::args::HeaderArgument;
use crate::args::HttpVersion;
use crate::args::IpVersion;
use crate::args::MinimumTlsVersion;
use crate::args::Options;
use crate::args::RequestBody;
use crate::args::TransferSpec;

const MAX_REDIRECTS: usize = 50;
const UNBOUNDED_IO_TIMEOUT: Duration = Duration::from_secs(365 * 24 * 60 * 60);

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct CookieStore {
    explicit: BTreeMap<String, String>,
    values: BTreeMap<CookieKey, StoredCookie>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CookieKey {
    domain: String,
    path: String,
    name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StoredCookie {
    value: String,
    host_only: bool,
    secure: bool,
    http_only: bool,
    expires: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NetrcEntry {
    machine: String,
    login: String,
    password: String,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedEnvironment {
    pub cookies: CookieStore,
    pub netrc: Vec<NetrcEntry>,
    pub client_certificate: Option<(Vec<u8>, Vec<u8>)>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FailureKind {
    Resolve,
    Connect,
    Timeout,
    Unsupported,
    Other,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("{message}")]
pub(crate) struct TransferFailure {
    pub kind: FailureKind,
    pub message: String,
}

impl TransferFailure {
    pub fn exit_code(&self) -> i32 {
        match self.kind {
            FailureKind::Resolve => 6,
            FailureKind::Connect => 7,
            FailureKind::Timeout => 28,
            FailureKind::Unsupported | FailureKind::Other => 1,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ExchangeTrace {
    pub request_headers: Vec<u8>,
    pub request_body: Vec<u8>,
    pub response_headers: Vec<u8>,
    pub response_body: Vec<u8>,
}

#[derive(Clone, Debug)]
pub(crate) struct TransferResult {
    pub index: usize,
    pub original_url: String,
    pub effective_url: String,
    pub response_headers: Vec<Vec<u8>>,
    pub body: Vec<u8>,
    pub status: u16,
    pub redirect_url: String,
    pub time_total: Duration,
    pub time_connect: Duration,
    pub downloaded: usize,
    pub traces: Vec<ExchangeTrace>,
    pub cookies: CookieStore,
}

#[derive(Debug)]
pub(crate) struct NetworkRun {
    pub results: Vec<Result<TransferResult, TransferFailure>>,
    pub cookies: CookieStore,
}

#[derive(Debug)]
struct InsecureVerifier;

impl TlsCertificateVerifier for InsecureVerifier {
    fn verify_server_certificate(
        &self,
        _server_name: &str,
        _certificate_chain_der: &[&[u8]],
    ) -> Result<(), HttpError> {
        Ok(())
    }

    fn is_insecure(&self) -> bool {
        true
    }
}

struct CurlDnsResolver {
    servers: Vec<SocketAddr>,
    ip_version: IpVersion,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ConnectionKey {
    host: String,
    port: u16,
    secure: bool,
    proxy: bool,
}

struct ConnectionManager {
    cache: ClientConnectionCache,
    connection_template: ConnectionParams,
    connections: RefCell<BTreeMap<ConnectionKey, ConnectionParams>>,
}

impl ConnectionManager {
    fn new(context: WorkerContext, options: &Options, environment: &PreparedEnvironment) -> Self {
        let maximum_pools = options.transfers.len().saturating_add(MAX_REDIRECTS).max(1);
        let cache = ClientConnectionCache::new(context, maximum_pools);
        cache
            .set_pool_params(PoolParams {
                max_connections: if options.parallel && options.http_version == HttpVersion::Http11
                {
                    u32::try_from(options.transfers.len()).unwrap_or(u32::MAX)
                } else {
                    1
                }
                .max(1),
                max_connection_attempts: 1,
                connect_timeout: options.connect_timeout,
                ..PoolParams::default()
            })
            .expect("a new connection cache accepts pool parameters");
        cache
            .set_dns_resolver(Some(Rc::new(CurlDnsResolver {
                servers: options.dns_servers.clone(),
                ip_version: options.ip_version,
            })))
            .expect("a new connection cache accepts a DNS resolver");
        Self {
            cache,
            connection_template: connection_params(options, environment, ""),
            connections: RefCell::new(BTreeMap::new()),
        }
    }

    async fn acquire(
        &self,
        options: &Options,
        target: &Url,
        proxy: Option<&Url>,
    ) -> Result<GetSessionResult, TransferFailure> {
        let endpoint = proxy.unwrap_or(target);
        let host = endpoint.host_str().ok_or_else(|| TransferFailure {
            kind: FailureKind::Resolve,
            message: format!("URL has no host: {endpoint}"),
        })?;
        let port = endpoint
            .port_or_known_default()
            .ok_or_else(|| TransferFailure {
                kind: FailureKind::Connect,
                message: format!("URL has no usable port: {endpoint}"),
            })?;
        let key = ConnectionKey {
            host: host.to_owned(),
            port,
            secure: endpoint.scheme() == "https",
            proxy: proxy.is_some(),
        };
        let connection = {
            let mut connections = self.connections.borrow_mut();
            connections
                .entry(key.clone())
                .or_insert_with(|| {
                    let mut connection = self.connection_template.clone();
                    if let ClientTransportOptions::Tls(tls) = &mut connection.transport {
                        tls.set_server_name(key.host.as_str());
                    }
                    connection
                })
                .clone()
        };
        let server_address = match resolve_address(options, host, port) {
            Some(Ok(address)) => Some(address.to_string().into_boxed_str()),
            Some(Err(error)) => return Err(error),
            None => None,
        };
        let acquired = self
            .cache
            .get_session_with_connection_params(
                host,
                port,
                key.secure,
                options.connect_timeout,
                server_address,
                Some(connection.clone()),
                CancellationToken::never(),
            )
            .await
            .map_err(pool_failure)?;
        self.tune_automatic_parallel_pool(options, host, port, key.secure, connection, &acquired)?;
        Ok(acquired)
    }

    fn tune_automatic_parallel_pool(
        &self,
        options: &Options,
        host: &str,
        port: u16,
        secure: bool,
        connection: ConnectionParams,
        acquired: &GetSessionResult,
    ) -> Result<(), TransferFailure> {
        if !options.parallel || options.http_version != HttpVersion::Any {
            return Ok(());
        }
        let maximum = match acquired.session.protocol() {
            CodecProtocol::Http2 => 1,
            CodecProtocol::Http11 => u32::try_from(options.transfers.len()).unwrap_or(u32::MAX),
            _ => 1,
        };
        let address = String::from_utf8_lossy(acquired.session.peer_address().host());
        let pool = self
            .cache
            .pool_for_address(host, address.as_ref(), port, secure, Some(connection))
            .map_err(pool_failure)?;
        pool.set_max_connections(maximum.max(1))
            .map_err(pool_failure)
    }

    async fn prepare_parallel(&self, options: &Options) {
        if !options.parallel || options.http_version != HttpVersion::Any {
            return;
        }
        let mut seen = BTreeSet::new();
        let mut endpoints = Vec::new();
        for transfer in &options.transfers {
            let Ok(target) = Url::parse(&transfer.url) else {
                continue;
            };
            let Ok(proxy) = proxy_for_url(options, &target) else {
                continue;
            };
            if proxy.is_some() && target.scheme() == "https" {
                continue;
            }
            let endpoint = proxy.as_ref().unwrap_or(&target);
            let Some(host) = endpoint.host_str() else {
                continue;
            };
            let Some(port) = endpoint.port_or_known_default() else {
                continue;
            };
            let key = ConnectionKey {
                host: host.to_owned(),
                port,
                secure: endpoint.scheme() == "https",
                proxy: proxy.is_some(),
            };
            if seen.insert(key) {
                endpoints.push((target, proxy));
            }
        }
        let concurrency = endpoints.len().max(1);
        stream::iter(endpoints)
            .map(|(target, proxy)| async move {
                self.acquire(options, &target, proxy.as_ref()).await.ok()
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<_>>()
            .await;
    }
}

impl DnsResolver for CurlDnsResolver {
    fn resolve(
        &self,
        hostname: Box<str>,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<Vec<RoxygenSocketAddress>, HttpError>> {
        let servers = self.servers.clone();
        let ip_version = self.ip_version;
        Box::pin(async move {
            if cancellation.is_cancellation_requested() {
                return Err(HttpError::with_message(
                    HttpErrorCode::CORO_CANCELLED,
                    b"DNS resolution cancelled",
                ));
            }
            let mut options = ResolverOpts::default();
            options.ip_strategy = match ip_version {
                IpVersion::Any => LookupIpStrategy::Ipv4thenIpv6,
                IpVersion::V4 => LookupIpStrategy::Ipv4Only,
                IpVersion::V6 => LookupIpStrategy::Ipv6Only,
            };
            let mut builder = if servers.is_empty() {
                TokioResolver::builder_tokio().map_err(dns_error)?
            } else {
                let nameservers = servers
                    .iter()
                    .map(|server| {
                        let mut connection = ConnectionConfig::udp();
                        connection.port = server.port();
                        NameServerConfig::new(server.ip(), true, vec![connection])
                    })
                    .collect();
                TokioResolver::builder_with_config(
                    ResolverConfig::from_parts(None, Vec::new(), nameservers),
                    TokioRuntimeProvider::default(),
                )
            };
            *builder.options_mut() = options;
            let resolver = builder.build().map_err(dns_error)?;
            let lookup = tokio::time::timeout(timeout, resolver.lookup_ip(hostname.as_ref()))
                .await
                .map_err(|_| {
                    HttpError::with_message(
                        HttpErrorCode::READ_TIMEOUT,
                        b"DNS resolution timed out",
                    )
                })?
                .map_err(dns_error)?;
            let addresses = lookup
                .iter()
                .map(|address| RoxygenSocketAddress::new(address.to_string(), 0))
                .collect::<Vec<_>>();
            if addresses.is_empty() {
                return Err(HttpError::with_message(
                    HttpErrorCode::CONNECT_ERROR,
                    b"DNS resolution returned no addresses",
                ));
            }
            Ok(addresses)
        })
    }
}

fn dns_error(error: impl std::fmt::Display) -> HttpError {
    HttpError::with_message(
        HttpErrorCode::CONNECT_ERROR,
        format!("DNS resolution failed: {error}"),
    )
}

pub(crate) fn prepare_environment(options: &Options) -> Result<PreparedEnvironment, CliError> {
    let mut cookies = CookieStore::default();
    for cookie in &options.cookies {
        if cookie.is_empty() {
            continue;
        } else if cookie.contains('=') {
            cookies.add_cookie_header(cookie);
        } else {
            let contents = fs::read_to_string(cookie).map_err(|source| CliError::Input {
                path: cookie.into(),
                source,
            })?;
            cookies.add_cookie_file(&contents);
        }
    }
    let netrc = if options.netrc {
        let path = std::env::var_os("HOME")
            .map(PathBuf::from)
            .map(|home| home.join(".netrc"))
            .ok_or_else(|| CliError::Usage("HOME is unset; cannot read .netrc".to_owned()))?;
        let contents =
            fs::read_to_string(&path).map_err(|source| CliError::Input { path, source })?;
        parse_netrc(&contents)
    } else {
        Vec::new()
    };
    let client_certificate = match (&options.cert, &options.key) {
        (Some(cert), Some(key)) => Some((
            fs::read(cert).map_err(|source| CliError::Input {
                path: cert.clone(),
                source,
            })?,
            fs::read(key).map_err(|source| CliError::Input {
                path: key.clone(),
                source,
            })?,
        )),
        (None, None) => None,
        _ => unreachable!("argument validation requires a certificate and key pair"),
    };
    Ok(PreparedEnvironment {
        cookies,
        netrc,
        client_certificate,
    })
}

pub(crate) async fn execute(
    context: WorkerContext,
    options: Options,
    environment: PreparedEnvironment,
) -> NetworkRun {
    let connections = Rc::new(ConnectionManager::new(
        context.clone(),
        &options,
        &environment,
    ));
    if options.parallel {
        execute_parallel(options, environment, connections).await
    } else {
        execute_sequential(&options, environment, &connections).await
    }
}

async fn execute_sequential(
    options: &Options,
    mut environment: PreparedEnvironment,
    connections: &ConnectionManager,
) -> NetworkRun {
    let mut results = Vec::with_capacity(options.transfers.len());
    for (index, transfer) in options.transfers.iter().enumerate() {
        let result =
            execute_with_retries(options, &environment, connections, index, transfer.clone()).await;
        if let Ok(result) = &result {
            environment.cookies.merge(&result.cookies);
        }
        results.push(result);
    }
    NetworkRun {
        results,
        cookies: environment.cookies,
    }
}

async fn execute_parallel(
    options: Options,
    environment: PreparedEnvironment,
    connections: Rc<ConnectionManager>,
) -> NetworkRun {
    connections.prepare_parallel(&options).await;
    let options = Rc::new(options);
    let environment = Rc::new(environment);
    let concurrency = options.transfers.len().max(1);
    let mut indexed = stream::iter(options.transfers.clone().into_iter().enumerate())
        .map(|(index, transfer)| {
            let options = Rc::clone(&options);
            let environment = Rc::clone(&environment);
            let connections = Rc::clone(&connections);
            async move {
                (
                    index,
                    execute_with_retries(&options, &environment, &connections, index, transfer)
                        .await,
                )
            }
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;
    indexed.sort_by_key(|(index, _)| *index);
    let mut cookies = environment.cookies.clone();
    let results = indexed
        .into_iter()
        .map(|(_, result)| {
            if let Ok(result) = &result {
                cookies.merge(&result.cookies);
            }
            result
        })
        .collect();
    NetworkRun { results, cookies }
}

async fn execute_with_retries(
    options: &Options,
    environment: &PreparedEnvironment,
    connections: &ConnectionManager,
    index: usize,
    transfer: TransferSpec,
) -> Result<TransferResult, TransferFailure> {
    let retry_started = Instant::now();
    let mut attempt = 0_u32;
    loop {
        let future = execute_transfer(options, environment, connections, index, transfer.clone());
        let result = match options
            .max_time
            .map(|maximum| (maximum, maximum.saturating_sub(retry_started.elapsed())))
        {
            Some((maximum, remaining)) => match tokio::time::timeout(remaining, future).await {
                Ok(result) => result,
                Err(_) => Err(TransferFailure {
                    kind: FailureKind::Timeout,
                    message: format!(
                        "operation timed out after {:.3} seconds",
                        maximum.as_secs_f64()
                    ),
                }),
            },
            None => future.await,
        };
        let retryable = match &result {
            Ok(response) => matches!(response.status, 408 | 429 | 500 | 502 | 503 | 504),
            Err(error) => matches!(
                error.kind,
                FailureKind::Resolve | FailureKind::Connect | FailureKind::Timeout
            ),
        };
        if !retryable || attempt >= options.retry {
            return result;
        }
        attempt += 1;
        let delay = if options.retry_delay == Duration::ZERO {
            Duration::from_secs(u64::from(attempt).min(10))
        } else {
            options.retry_delay
        };
        if options
            .max_time
            .is_some_and(|maximum| retry_started.elapsed().saturating_add(delay) >= maximum)
        {
            return Err(TransferFailure {
                kind: FailureKind::Timeout,
                message: format!(
                    "operation timed out after {:.3} seconds",
                    options
                        .max_time
                        .expect("maximum time was checked")
                        .as_secs_f64()
                ),
            });
        }
        if options
            .retry_max_time
            .is_some_and(|maximum| retry_started.elapsed().saturating_add(delay) > maximum)
        {
            return result;
        }
        tokio::time::sleep(delay).await;
    }
}

async fn execute_transfer(
    options: &Options,
    environment: &PreparedEnvironment,
    connections: &ConnectionManager,
    index: usize,
    transfer: TransferSpec,
) -> Result<TransferResult, TransferFailure> {
    let started = Instant::now();
    let mut effective_url = transfer.url.clone();
    let mut method = request_method(options);
    let explicit_method = options.request.is_some();
    let mut body = (!options.get).then(|| options.body.clone()).flatten();
    if options.get
        && let Some(request_body) = &options.body
    {
        effective_url = append_query(&effective_url, &request_body.bytes)?;
    }
    let mut cookies = environment.cookies.clone();
    let mut response_headers = Vec::new();
    let mut traces = Vec::new();
    let mut total_connect = Duration::ZERO;
    let credential_origin = Url::parse(&effective_url).ok().and_then(|url| origin(&url));

    for redirect_count in 0..=MAX_REDIRECTS {
        let request_url = Url::parse(&effective_url).map_err(|error| TransferFailure {
            kind: FailureKind::Other,
            message: format!("invalid URL {effective_url:?}: {error}"),
        })?;
        if !matches!(request_url.scheme(), "http" | "https") {
            return Err(TransferFailure {
                kind: FailureKind::Unsupported,
                message: format!("unsupported URL scheme: {}", request_url.scheme()),
            });
        }
        if method.eq_ignore_ascii_case("CONNECT") {
            return Err(TransferFailure {
                kind: FailureKind::Unsupported,
                message: "CONNECT is not supported by Roxygen".to_owned(),
            });
        }

        let proxy = proxy_for_url(options, &request_url)?;
        if proxy.is_some() && request_url.scheme() == "https" {
            return Err(TransferFailure {
                kind: FailureKind::Unsupported,
                message: "HTTPS proxying requires CONNECT, which Roxygen does not support"
                    .to_owned(),
            });
        }
        let connect_started = Instant::now();
        let acquired = connections
            .acquire(options, &request_url, proxy.as_ref())
            .await?;
        total_connect = total_connect.saturating_add(connect_started.elapsed());
        let protocol = acquired.session.protocol();
        let trace_body =
            options.verbose || options.trace.is_some() || options.trace_ascii.is_some();
        let (source, request_headers, request_body) = request_source(
            options,
            environment,
            &request_url,
            proxy.as_ref(),
            &method,
            body.as_ref(),
            &cookies,
            transfer.resume_offset,
            credential_origin
                .as_ref()
                .is_some_and(|origin| Some(origin.clone()) == self::origin(&request_url)),
            protocol,
            trace_body,
        )?;
        let response = send_request(
            acquired,
            source,
            options.max_time.unwrap_or(UNBOUNDED_IO_TIMEOUT),
            options.limit_rate,
        )
        .await
        .map_err(|error| http_failure(error, false))?;
        let mut headers = response.headers;
        let status = headers.status_code().map_err(message_error)?;
        if transfer.resume_offset.is_some_and(|offset| offset > 0) && status != 206 {
            return Err(TransferFailure {
                kind: FailureKind::Other,
                message: format!(
                    "server did not honor the resumed transfer with a 206 response (received {status})"
                ),
            });
        }
        let header_bytes = serialize_response_headers(&mut headers, protocol)?;
        let mut response_body = flatten_body(response.body);
        let wire_body = if trace_body {
            response_body.clone()
        } else {
            Vec::new()
        };
        cookies.add_response_headers(headers.headers(), &request_url);
        response_headers.push(header_bytes.clone());
        traces.push(ExchangeTrace {
            request_headers,
            request_body,
            response_headers: header_bytes,
            response_body: wire_body,
        });

        let location = header_value(headers.headers(), b"Location")
            .map(|value| String::from_utf8_lossy(&value).into_owned());
        let is_redirect = matches!(status, 301 | 302 | 303 | 307 | 308) && location.is_some();
        let redirect_url = location
            .as_deref()
            .and_then(|location| request_url.join(location).ok())
            .map(|url| url.to_string())
            .unwrap_or_default();
        if options.location && is_redirect {
            if redirect_count == MAX_REDIRECTS {
                return Err(TransferFailure {
                    kind: FailureKind::Other,
                    message: format!("maximum redirects ({MAX_REDIRECTS}) followed"),
                });
            }
            if !explicit_method
                && (status == 303
                    || matches!(status, 301 | 302) && method.eq_ignore_ascii_case("POST"))
            {
                method = "GET".to_owned();
                body = None;
            }
            effective_url = redirect_url;
            continue;
        }
        if options.compressed && !(options.fail && status >= 400) {
            response_body = decompress(headers.headers(), response_body)?;
        }
        let downloaded = response_body.len();
        return Ok(TransferResult {
            index,
            original_url: transfer.url,
            effective_url,
            response_headers,
            body: response_body,
            status,
            redirect_url,
            time_total: started.elapsed(),
            time_connect: total_connect,
            downloaded,
            traces,
            cookies,
        });
    }
    unreachable!("the redirect loop returns at or before its inclusive bound")
}

async fn send_request(
    acquired: GetSessionResult,
    source: HttpSourceHolder,
    timeout: Duration,
    limit_rate: Option<u64>,
) -> Result<Response, HttpError> {
    let response = acquired
        .session
        .send_reserved_request(source, acquired.reservation, CancellationToken::never())
        .await?;
    let mut source = response.source;
    source.set_read_timeout(timeout);
    let (headers, header_eom) = loop {
        let event = source.read_header_event().await?;
        let eom = event.eom();
        let mut headers = event.into_headers();
        let final_response = headers.is_final().map_err(|error| {
            HttpError::with_message(HttpErrorCode::PROTOCOL_ERROR, error.to_string())
        })?;
        if final_response {
            break (headers, eom);
        }
    };
    let mut response = Response {
        headers,
        body: BufQueue::new(u32::MAX as usize),
        trailers: None,
    };
    if header_eom {
        return Ok(response);
    }

    let started = Instant::now();
    let mut received = 0_usize;
    loop {
        let maximum = limit_rate.map_or(u32::MAX, |rate| {
            u32::try_from(rate.clamp(1, 64 * 1024)).unwrap_or(64 * 1024)
        });
        let event = source.read_body_event(maximum).await?;
        let eom = event.eom();
        match event.into_kind() {
            HttpBodyEventKind::Body(mut body) => {
                received += body.chain_length();
                response.body.append(&mut body).map_err(|error| {
                    HttpError::with_message(HttpErrorCode::ENHANCE_YOUR_CALM, error.to_string())
                })?;
                if let Some(rate) = limit_rate {
                    let target = Duration::from_secs_f64(received as f64 / rate as f64);
                    if let Some(delay) = target.checked_sub(started.elapsed()) {
                        tokio::time::sleep(delay).await;
                    }
                }
            }
            HttpBodyEventKind::Trailers(trailers) => response.trailers = Some(trailers),
            HttpBodyEventKind::Suspend(resume) => match resume.await {
                ResumeStatus::Signalled => continue,
                ResumeStatus::TimedOut => {
                    return Err(HttpError::with_message(
                        HttpErrorCode::READ_TIMEOUT,
                        b"response body suspended until timeout",
                    ));
                }
                ResumeStatus::Cancelled | ResumeStatus::NotReady => {
                    return Err(HttpError::with_message(
                        HttpErrorCode::CORO_CANCELLED,
                        b"response body suspension cancelled",
                    ));
                }
            },
            HttpBodyEventKind::Upgrade
            | HttpBodyEventKind::PushPromise(_)
            | HttpBodyEventKind::Padding(_) => {}
        }
        if eom {
            return Ok(response);
        }
    }
}

fn connection_params(
    options: &Options,
    environment: &PreparedEnvironment,
    server_name: &str,
) -> ConnectionParams {
    let mut connection = ConnectionParams {
        plaintext_protocol: match options.http_version {
            HttpVersion::Http2 => CodecProtocol::Http2,
            HttpVersion::Any | HttpVersion::Http11 | HttpVersion::Http3 => CodecProtocol::Http11,
        },
        ..ConnectionParams::default()
    };
    #[cfg(target_os = "linux")]
    connection
        .tcp_options
        .socket_options
        .push(TcpSocketOption::keep_alive(options.keepalive));
    let mut tls = TlsClientOptions::new(server_name);
    if let Some(path) = &options.cacert {
        tls.set_ca_paths([path]);
    }
    if let Some((certificate, key)) = &environment.client_certificate {
        tls.set_client_identity_pem(certificate, key);
    }
    match options.http_version {
        HttpVersion::Http11 => tls.set_alpn_protocols([b"http/1.1".as_slice()]),
        HttpVersion::Http2 => tls.set_alpn_protocols([b"h2".as_slice()]),
        HttpVersion::Any | HttpVersion::Http3 => {}
    }
    match options.minimum_tls_version {
        MinimumTlsVersion::Default | MinimumTlsVersion::Tls12 => {
            tls.set_minimum_version(TlsVersion::Tls12)
        }
        MinimumTlsVersion::Tls13 => tls.set_minimum_version(TlsVersion::Tls13),
    }
    if options.insecure {
        tls.set_certificate_verifier(Some(Arc::new(InsecureVerifier)));
        connection.identity_validation =
            roxygen_client::connector::IdentityValidation::DisabledInsecure;
    } else {
        connection.identity_validation = roxygen_client::connector::IdentityValidation::Enforcing;
    }
    connection.transport = ClientTransportOptions::Tls(tls);
    connection
}

fn pool_failure(error: PoolError) -> TransferFailure {
    match error {
        PoolError::ConnectFailed { source } => http_failure(source, true),
        PoolError::AttachedConnectFailed { source } => {
            http_failure(source.http_error().clone(), true)
        }
        PoolError::ReservationFailed { source } => http_failure(source, false),
        PoolError::Timeout => TransferFailure {
            kind: FailureKind::Timeout,
            message: "timed out waiting for an available connection".to_owned(),
        },
        PoolError::Cancelled => TransferFailure {
            kind: FailureKind::Other,
            message: "connection acquisition was cancelled".to_owned(),
        },
        PoolError::Draining => TransferFailure {
            kind: FailureKind::Other,
            message: "connection pool is draining".to_owned(),
        },
        PoolError::MaxWaiters => TransferFailure {
            kind: FailureKind::Other,
            message: "connection pool has too many waiters".to_owned(),
        },
        PoolError::WrongWorker => TransferFailure {
            kind: FailureKind::Other,
            message: "connection pool belongs to a different worker".to_owned(),
        },
        PoolError::InvalidSessionState => TransferFailure {
            kind: FailureKind::Other,
            message: "connection pool returned an invalid session".to_owned(),
        },
        PoolError::InvalidConnectionParams => TransferFailure {
            kind: FailureKind::Other,
            message: "connection pool rejected its TLS configuration".to_owned(),
        },
        error @ (PoolError::CleanupRegistration { .. }
        | PoolError::TaskJoin { .. }
        | PoolError::SessionJoin { .. }
        | PoolError::CleanupPanicked
        | PoolError::NativeConfigurationOnly
        | PoolError::SecurityMismatch
        | PoolError::OwnershipCapacity) => TransferFailure {
            kind: FailureKind::Other,
            message: error.to_string(),
        },
    }
}

fn resolve_address(
    options: &Options,
    host: &str,
    port: u16,
) -> Option<Result<IpAddr, TransferFailure>> {
    options
        .resolve
        .iter()
        .rev()
        .find(|entry| entry.port == port && entry.host.eq_ignore_ascii_case(host))
        .map(|entry| {
            entry
                .addresses
                .iter()
                .copied()
                .find(|address| match options.ip_version {
                    IpVersion::Any => true,
                    IpVersion::V4 => address.is_ipv4(),
                    IpVersion::V6 => address.is_ipv6(),
                })
                .ok_or_else(|| TransferFailure {
                    kind: FailureKind::Resolve,
                    message: format!(
                        "--resolve has no address matching the selected IP version for {host}:{port}"
                    ),
                })
        })
}

fn request_source(
    options: &Options,
    environment: &PreparedEnvironment,
    url: &Url,
    proxy: Option<&Url>,
    method: &str,
    body: Option<&RequestBody>,
    cookies: &CookieStore,
    resume_offset: Option<u64>,
    allow_explicit_credentials: bool,
    protocol: CodecProtocol,
    trace_body: bool,
) -> Result<(HttpSourceHolder, Vec<u8>, Vec<u8>), TransferFailure> {
    let roxygen_url = RoxygenUrl::parse(url.as_str(), false, UrlMode::StrictCompat);
    if !roxygen_url.is_valid() || !roxygen_url.has_host() {
        return Err(TransferFailure {
            kind: FailureKind::Other,
            message: format!("invalid URL: {url}"),
        });
    }
    let request_target = if proxy.is_some() {
        let mut absolute = url.clone();
        absolute.set_fragment(None);
        absolute.to_string().into_bytes()
    } else {
        roxygen_url.make_relative_url().into_vec()
    };
    let mut message = HttpMessage::new();
    message
        .set_url(&request_target, false)
        .map_err(message_error)?;
    message
        .set_method(Method::from_name(method.to_owned()))
        .map_err(message_error)?;
    match protocol {
        CodecProtocol::Http2 => message.set_http_version(2, 0),
        _ => message.set_http_version(1, 1),
    }
    let headers = message.headers_mut();
    add_default_header(
        options,
        headers,
        b"Host",
        &roxygen_url.host_and_port_omit_default(),
    );
    add_default_header(
        options,
        headers,
        b"User-Agent",
        options
            .user_agent
            .as_deref()
            .unwrap_or("curl/8.10.1-roxygen")
            .as_bytes(),
    );
    add_default_header(
        options,
        headers,
        b"Accept",
        if options.data_has_json() {
            b"application/json"
        } else {
            b"*/*"
        },
    );
    if options.compressed {
        add_default_header(options, headers, b"Accept-Encoding", b"gzip, deflate, br");
    }
    if let Some(referer) = &options.referer {
        add_default_header(options, headers, b"Referer", referer.as_bytes());
    }
    if let Some(content_type) = body.and_then(|body| body.content_type.as_deref()) {
        add_default_header(options, headers, b"Content-Type", content_type);
    }
    if let Some(credentials) =
        credentials_for(options, environment, url, allow_explicit_credentials)
    {
        add_default_header(
            options,
            headers,
            b"Authorization",
            format!("Basic {}", BASE64.encode(credentials)).as_bytes(),
        );
    }
    if let Some(proxy_credentials) = proxy_credentials(options, proxy) {
        add_default_header(
            options,
            headers,
            b"Proxy-Authorization",
            format!("Basic {}", BASE64.encode(proxy_credentials)).as_bytes(),
        );
    }
    let cookie_header = cookies.header_value(url);
    if !cookie_header.is_empty() {
        add_default_header(options, headers, b"Cookie", cookie_header.as_bytes());
    }
    if let Some(range) = resume_offset
        .filter(|offset| *offset > 0)
        .map(|offset| format!("bytes={offset}-"))
        .or_else(|| options.range.as_ref().map(|range| format!("bytes={range}")))
    {
        add_default_header(options, headers, b"Range", range.as_bytes());
    }
    if last_header_value(&options.headers, b"Connection").is_none() {
        headers.add(HttpHeaderCode::Connection, b"");
    }
    options
        .headers
        .iter()
        .enumerate()
        .filter(|(index, header)| {
            !options.headers[index + 1..]
                .iter()
                .any(|later| later.name.eq_ignore_ascii_case(&header.name) && later.value.is_none())
        })
        .map(|(_, header)| header)
        .filter_map(|header| header.value.as_ref().map(|value| (&header.name, value)))
        .for_each(|(name, value)| headers.add(name, value));
    message.set_wants_keepalive(true);
    message.set_secure(url.scheme() == "https");

    let request_body = body.map_or_else(Vec::new, |body| body.bytes.clone());
    let traced_request_body = if trace_body {
        request_body.clone()
    } else {
        Vec::new()
    };
    let queue = if body.is_some() {
        let request_body_length = request_body.len();
        Some(
            BufQueue::from_bytes(Bytes::from(request_body), request_body_length).map_err(
                |error| TransferFailure {
                    kind: FailureKind::Other,
                    message: error.to_string(),
                },
            )?,
        )
    } else {
        None
    };
    let mut source = HttpFixedSource::new(message, queue);
    if header_removed(&options.headers, b"Content-Length") {
        source
            .message_mut()
            .expect("a fixed request retains its message")
            .headers_mut()
            .remove(HttpHeaderCode::ContentLength);
    } else if let Some(value) = last_header_value(&options.headers, b"Content-Length") {
        source
            .message_mut()
            .expect("a fixed request retains its message")
            .headers_mut()
            .set(HttpHeaderCode::ContentLength, value);
    }
    let request_headers = serialize_request_headers(
        source
            .message_mut()
            .expect("a fixed request retains its message"),
        protocol,
    )?;
    Ok((
        HttpSourceHolder::new(Box::new(source)),
        request_headers,
        traced_request_body,
    ))
}

fn request_method(options: &Options) -> String {
    if let Some(method) = &options.request {
        return method.to_ascii_uppercase();
    }
    if options.head {
        return "HEAD".to_owned();
    }
    if options.get {
        return "GET".to_owned();
    }
    options
        .body
        .as_ref()
        .map_or("GET", |body| body.implied_method)
        .to_owned()
}

fn add_default_header(
    options: &Options,
    headers: &mut roxygen_core::headers::HttpHeaders,
    name: &[u8],
    value: &[u8],
) {
    if !options
        .headers
        .iter()
        .any(|header| header.name.eq_ignore_ascii_case(name))
        && !value.is_empty()
    {
        headers.add(name, value);
    }
}

fn header_removed(headers: &[HeaderArgument], name: &[u8]) -> bool {
    headers
        .iter()
        .rev()
        .find(|header| header.name.eq_ignore_ascii_case(name))
        .is_some_and(|header| header.value.is_none())
}

fn last_header_value<'a>(headers: &'a [HeaderArgument], name: &[u8]) -> Option<&'a [u8]> {
    headers
        .iter()
        .rev()
        .find(|header| header.name.eq_ignore_ascii_case(name))
        .and_then(|header| header.value.as_deref())
}

fn credentials_for(
    options: &Options,
    environment: &PreparedEnvironment,
    url: &Url,
    allow_explicit: bool,
) -> Option<Vec<u8>> {
    if allow_explicit && let Some(user) = &options.user {
        let credentials = if user.contains(':') {
            user.clone()
        } else {
            format!("{user}:")
        };
        return Some(credentials.into_bytes());
    }
    let host = url.host_str()?;
    environment
        .netrc
        .iter()
        .find(|entry| entry.machine.eq_ignore_ascii_case(host))
        .or_else(|| {
            environment
                .netrc
                .iter()
                .find(|entry| entry.machine == "default")
        })
        .map(|entry| format!("{}:{}", entry.login, entry.password).into_bytes())
}

fn origin(url: &Url) -> Option<(String, u16, String)> {
    Some((
        url.host_str()?.to_ascii_lowercase(),
        url.port_or_known_default()?,
        url.scheme().to_owned(),
    ))
}

fn proxy_credentials(options: &Options, proxy: Option<&Url>) -> Option<Vec<u8>> {
    if let Some(credentials) = &options.proxy_user {
        return Some(credentials.as_bytes().to_vec());
    }
    let proxy = proxy?;
    if proxy.username().is_empty() {
        return None;
    }
    Some(
        format!(
            "{}:{}",
            proxy.username(),
            proxy.password().unwrap_or_default()
        )
        .into_bytes(),
    )
}

fn proxy_for_url(options: &Options, url: &Url) -> Result<Option<Url>, TransferFailure> {
    let Some(proxy) = &options.proxy else {
        return Ok(None);
    };
    let host = url.host_str().unwrap_or_default();
    if no_proxy_matches(
        options.no_proxy.as_deref(),
        host,
        url.port_or_known_default(),
    ) {
        return Ok(None);
    }
    let proxy = if proxy.contains("://") {
        proxy.clone()
    } else {
        format!("http://{proxy}")
    };
    let parsed = Url::parse(&proxy).map_err(|error| TransferFailure {
        kind: FailureKind::Other,
        message: format!("invalid proxy URL {proxy:?}: {error}"),
    })?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(TransferFailure {
            kind: FailureKind::Unsupported,
            message: format!("unsupported proxy scheme: {}", parsed.scheme()),
        });
    }
    Ok(Some(parsed))
}

fn no_proxy_matches(no_proxy: Option<&str>, host: &str, port: Option<u16>) -> bool {
    no_proxy.is_some_and(|list| {
        list.split(',').map(str::trim).any(|entry| {
            if entry == "*" {
                return true;
            }
            let (candidate, candidate_port) = entry
                .rsplit_once(':')
                .filter(|(_, port)| port.parse::<u16>().is_ok())
                .map_or((entry, None), |(host, port)| (host, port.parse().ok()));
            if candidate_port.is_some() && candidate_port != port {
                return false;
            }
            let candidate = candidate.trim_start_matches('.');
            host.eq_ignore_ascii_case(candidate)
                || host.len() > candidate.len()
                    && host.as_bytes()[host.len() - candidate.len() - 1] == b'.'
                    && host[host.len() - candidate.len()..].eq_ignore_ascii_case(candidate)
        })
    })
}

fn append_query(url: &str, data: &[u8]) -> Result<String, TransferFailure> {
    let mut parsed = Url::parse(url).map_err(|error| TransferFailure {
        kind: FailureKind::Other,
        message: format!("invalid URL {url:?}: {error}"),
    })?;
    let data = String::from_utf8_lossy(data);
    let query = match parsed.query() {
        Some(existing) if !existing.is_empty() => format!("{existing}&{data}"),
        _ => data.into_owned(),
    };
    parsed.set_query(Some(&query));
    Ok(parsed.to_string())
}

fn serialize_request_headers(
    message: &mut HttpMessage,
    protocol: CodecProtocol,
) -> Result<Vec<u8>, TransferFailure> {
    let mut output = Vec::new();
    output.extend_from_slice(message.method_string().map_err(message_error)?.as_bytes());
    output.push(b' ');
    output.extend_from_slice(message.url().map_err(message_error)?);
    output.extend_from_slice(match protocol {
        CodecProtocol::Http2 => b" HTTP/2",
        _ => b" HTTP/1.1",
    });
    output.extend_from_slice(b"\r\n");
    append_headers(
        &mut output,
        message.headers(),
        protocol == CodecProtocol::Http2,
    );
    output.extend_from_slice(b"\r\n");
    Ok(output)
}

fn serialize_response_headers(
    message: &mut HttpMessage,
    protocol: CodecProtocol,
) -> Result<Vec<u8>, TransferFailure> {
    let mut output = Vec::new();
    let version = message.http_version();
    if protocol == CodecProtocol::Http2 {
        output.extend_from_slice(b"HTTP/2 ");
    } else {
        output.extend_from_slice(format!("HTTP/{}.{} ", version.0, version.1).as_bytes());
    }
    output.extend_from_slice(
        message
            .status_code()
            .map_err(message_error)?
            .to_string()
            .as_bytes(),
    );
    if protocol != CodecProtocol::Http2 {
        let reason = message.status_message().map_err(message_error)?;
        if !reason.is_empty() {
            output.push(b' ');
            output.extend_from_slice(reason);
        }
    } else {
        output.push(b' ');
    }
    output.extend_from_slice(b"\r\n");
    append_headers(
        &mut output,
        message.headers(),
        protocol == CodecProtocol::Http2,
    );
    output.extend_from_slice(b"\r\n");
    Ok(output)
}

fn append_headers(
    output: &mut Vec<u8>,
    headers: &roxygen_core::headers::HttpHeaders,
    lowercase_names: bool,
) {
    headers.for_each(|name, value| {
        if name.eq_ignore_ascii_case(b"Connection") && value.is_empty() {
            return;
        }
        if lowercase_names {
            output.extend(name.iter().map(u8::to_ascii_lowercase));
        } else {
            output.extend_from_slice(name);
        }
        output.extend_from_slice(b": ");
        output.extend_from_slice(value);
        output.extend_from_slice(b"\r\n");
    });
}

fn flatten_body(body: BufQueue) -> Vec<u8> {
    let mut flattened = Vec::with_capacity(body.chain_length());
    body.into_bytes_segments()
        .iter()
        .for_each(|segment| flattened.extend_from_slice(segment));
    flattened
}

fn decompress(
    headers: &roxygen_core::headers::HttpHeaders,
    body: Vec<u8>,
) -> Result<Vec<u8>, TransferFailure> {
    let Some(encoding) = header_value(headers, b"Content-Encoding") else {
        return Ok(body);
    };
    let encoding = String::from_utf8_lossy(&encoding).to_ascii_lowercase();
    match encoding.trim() {
        "gzip" | "x-gzip" => read_decoder(GzDecoder::new(body.as_slice())),
        "deflate" => read_decoder(ZlibDecoder::new(body.as_slice()))
            .or_else(|_| read_decoder(DeflateDecoder::new(body.as_slice()))),
        "br" => read_decoder(brotli::Decompressor::new(body.as_slice(), 4096)),
        "identity" | "" => Ok(body),
        other => Err(TransferFailure {
            kind: FailureKind::Unsupported,
            message: format!("unsupported content encoding: {other}"),
        }),
    }
}

fn read_decoder(mut decoder: impl Read) -> Result<Vec<u8>, TransferFailure> {
    let mut decoded = Vec::new();
    decoder
        .read_to_end(&mut decoded)
        .map_err(|error| TransferFailure {
            kind: FailureKind::Other,
            message: format!("failed to decompress response: {error}"),
        })?;
    Ok(decoded)
}

fn header_value(headers: &roxygen_core::headers::HttpHeaders, name: &[u8]) -> Option<Vec<u8>> {
    let value = headers.combine(name, b", ");
    (!value.is_empty()).then(|| value.into_vec())
}

fn message_error(error: impl std::fmt::Display) -> TransferFailure {
    TransferFailure {
        kind: FailureKind::Other,
        message: error.to_string(),
    }
}

fn http_failure(error: HttpError, connecting: bool) -> TransferFailure {
    let message = String::from_utf8_lossy(error.message()).into_owned();
    let kind = if error.code() == HttpErrorCode::READ_TIMEOUT {
        FailureKind::Timeout
    } else if connecting
        && (message.contains("DNS")
            || message.contains("lookup")
            || message.contains("name or service"))
    {
        FailureKind::Resolve
    } else if connecting {
        FailureKind::Connect
    } else {
        FailureKind::Other
    };
    TransferFailure { kind, message }
}

impl CookieStore {
    pub fn header_value(&self, url: &Url) -> String {
        let host = url.host_str().unwrap_or_default();
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let mut matching = self
            .values
            .iter()
            .filter(|(key, value)| {
                (host == key.domain
                    || (!value.host_only && cookie_domain_matches(host, &key.domain)))
                    && cookie_path_matches(url.path(), &key.path)
                    && (!value.secure || url.scheme() == "https")
                    && value.expires.is_none_or(|expires| expires > now)
            })
            .collect::<Vec<_>>();
        matching.sort_by_key(|(key, _)| std::cmp::Reverse(key.path.len()));
        self.explicit
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .chain(
                matching
                    .into_iter()
                    .map(|(key, value)| format!("{}={}", key.name, value.value)),
            )
            .collect::<Vec<_>>()
            .join("; ")
    }

    pub fn add_cookie_header(&mut self, value: &str) {
        value
            .split(';')
            .filter_map(|pair| pair.trim().split_once('='))
            .for_each(|(name, value)| {
                self.explicit.insert(name.to_owned(), value.to_owned());
            });
    }

    fn add_cookie_file(&mut self, contents: &str) {
        for line in contents.lines() {
            let http_only = line.starts_with("#HttpOnly_");
            let line = line.strip_prefix("#HttpOnly_").unwrap_or(line);
            if line.trim_start().starts_with('#') {
                continue;
            }
            let fields = line.split('\t').collect::<Vec<_>>();
            if fields.len() != 7 || fields[0].is_empty() || !fields[2].starts_with('/') {
                continue;
            }
            let Ok(expires) = fields[4].parse::<i64>() else {
                continue;
            };
            let key = CookieKey {
                domain: fields[0].trim_start_matches('.').to_ascii_lowercase(),
                path: fields[2].to_owned(),
                name: fields[5].to_owned(),
            };
            self.values.insert(
                key,
                StoredCookie {
                    value: fields[6].to_owned(),
                    host_only: fields[1] != "TRUE",
                    secure: fields[3] == "TRUE",
                    http_only,
                    expires: (expires != 0).then_some(expires),
                },
            );
        }
    }

    fn add_response_headers(&mut self, headers: &roxygen_core::headers::HttpHeaders, url: &Url) {
        headers.for_each_value(HttpHeaderCode::SetCookie, |value| {
            self.add_response_cookie(&String::from_utf8_lossy(value), url);
            false
        });
    }

    fn add_response_cookie(&mut self, value: &str, url: &Url) {
        let Ok(cookie) = Cookie::parse(value) else {
            return;
        };
        let Some(host) = url.host_str() else {
            return;
        };
        let domain = cookie
            .domain()
            .unwrap_or(host)
            .trim_start_matches('.')
            .to_ascii_lowercase();
        if !cookie_domain_matches(host, &domain) {
            return;
        }
        let mut host_only = cookie.domain().is_none();
        if !host_only && domain.parse::<IpAddr>().is_err() {
            let Ok(parsed_domain) = addr::parse_domain_name(&domain) else {
                return;
            };
            if parsed_domain.root().is_none() {
                if domain != host {
                    return;
                }
                host_only = true;
            }
        }
        let default_path = url
            .path()
            .rsplit_once('/')
            .map(|(path, _)| path)
            .unwrap_or("");
        let path = cookie
            .path()
            .filter(|path| path.starts_with('/'))
            .unwrap_or(if default_path.is_empty() {
                "/"
            } else {
                default_path
            });
        if [cookie.name(), cookie.value(), domain.as_str(), path]
            .iter()
            .any(|value| value.bytes().any(|byte| byte.is_ascii_control()))
        {
            return;
        }
        let secure = cookie.secure().unwrap_or(false);
        if secure && url.scheme() != "https" {
            return;
        }
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let expires = cookie
            .max_age()
            .map(|age| now.saturating_add(age.whole_seconds()))
            .or_else(|| {
                cookie
                    .expires_datetime()
                    .map(|expires| expires.unix_timestamp())
            });
        self.values.insert(
            CookieKey {
                domain,
                path: path.to_owned(),
                name: cookie.name().to_owned(),
            },
            StoredCookie {
                value: cookie.value().to_owned(),
                host_only,
                secure,
                http_only: cookie.http_only().unwrap_or(false),
                expires,
            },
        );
    }

    fn merge(&mut self, other: &Self) {
        self.explicit.extend(other.explicit.clone());
        self.values.extend(other.values.clone());
    }

    pub fn to_netscape(&self) -> String {
        let mut output = "# Netscape HTTP Cookie File\n# Generated by roxygen curl\n".to_owned();
        let now = OffsetDateTime::now_utc().unix_timestamp();
        self.values
            .iter()
            .filter(|(_, value)| value.expires.is_none_or(|expires| expires > now))
            .for_each(|(key, value)| {
                output.push_str(&format!(
                    "{}{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                    if value.http_only { "#HttpOnly_" } else { "" },
                    key.domain,
                    if value.host_only { "FALSE" } else { "TRUE" },
                    key.path,
                    if value.secure { "TRUE" } else { "FALSE" },
                    value.expires.unwrap_or(0),
                    key.name,
                    value.value,
                ));
            });
        output
    }
}

fn cookie_domain_matches(host: &str, domain: &str) -> bool {
    host == domain
        || (host.parse::<IpAddr>().is_err()
            && host
                .strip_suffix(domain)
                .is_some_and(|prefix| prefix.ends_with('.')))
}

fn cookie_path_matches(request: &str, path: &str) -> bool {
    request == path
        || request
            .strip_prefix(path)
            .is_some_and(|suffix| path.ends_with('/') || suffix.starts_with('/'))
}

fn parse_netrc(contents: &str) -> Vec<NetrcEntry> {
    let tokens = contents.split_whitespace().collect::<Vec<_>>();
    let mut entries = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        let machine = match tokens[index] {
            "machine" if index + 1 < tokens.len() => {
                index += 2;
                tokens[index - 1].to_owned()
            }
            "default" => {
                index += 1;
                "default".to_owned()
            }
            _ => {
                index += 1;
                continue;
            }
        };
        let mut login = String::new();
        let mut password = String::new();
        while index < tokens.len() && !matches!(tokens[index], "machine" | "default") {
            match tokens[index] {
                "login" if index + 1 < tokens.len() => {
                    login = tokens[index + 1].to_owned();
                    index += 2;
                }
                "password" if index + 1 < tokens.len() => {
                    password = tokens[index + 1].to_owned();
                    index += 2;
                }
                _ => index += 1,
            }
        }
        if !login.is_empty() {
            entries.push(NetrcEntry {
                machine,
                login,
                password,
            });
        }
    }
    entries
}

trait OptionsExt {
    fn data_has_json(&self) -> bool;
}

impl OptionsExt for Options {
    fn data_has_json(&self) -> bool {
        self.body.as_ref().is_some_and(|body| {
            body.content_type.as_deref() == Some(b"application/json".as_slice())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_proxy_matches_exact_and_domain_suffixes() {
        assert!(no_proxy_matches(
            Some("example.com"),
            "api.example.com",
            Some(80)
        ));
        assert!(no_proxy_matches(
            Some("example.com:443"),
            "example.com",
            Some(443)
        ));
        assert!(!no_proxy_matches(
            Some("example.com:443"),
            "example.com",
            Some(80)
        ));
        assert!(!no_proxy_matches(
            Some("example.com"),
            "notexample.com",
            Some(80)
        ));
    }

    #[test]
    fn cookie_files_and_headers_are_combined() {
        let mut cookies = CookieStore::default();
        cookies.add_cookie_header("a=one; b=two");
        cookies.add_cookie_file("example.test\tFALSE\t/\tFALSE\t0\tc\tthree\n");
        assert_eq!(
            cookies.header_value(&Url::parse("http://example.test/").unwrap()),
            "a=one; b=two; c=three"
        );
        assert_eq!(
            cookies.header_value(&Url::parse("http://other.test/").unwrap()),
            "a=one; b=two"
        );
    }

    #[test]
    fn response_cookies_are_scoped_to_host_path_and_secure_transport() {
        let url = Url::parse("https://api.example.com/account/login").unwrap();
        let mut cookies = CookieStore::default();
        cookies.add_response_cookie("sid=account; Secure; HttpOnly", &url);
        cookies.add_response_cookie("sid=root; Path=/", &url);
        cookies.add_response_cookie("shared=yes; Domain=example.com; Path=/", &url);
        assert_eq!(
            cookies.header_value(&url),
            "sid=account; sid=root; shared=yes"
        );
        assert_eq!(
            cookies.header_value(&Url::parse("https://api.example.com/accounting").unwrap()),
            "sid=root; shared=yes"
        );
        assert_eq!(
            cookies.header_value(&Url::parse("http://api.example.com/account/view").unwrap()),
            "sid=root; shared=yes"
        );
        assert_eq!(
            cookies.header_value(&Url::parse("https://www.example.com/").unwrap()),
            "shared=yes"
        );
        assert!(
            cookies
                .header_value(&Url::parse("https://notexample.com/account").unwrap())
                .is_empty()
        );
        assert!(
            cookies
                .header_value(&Url::parse("https://other.test/account").unwrap())
                .is_empty()
        );
    }

    #[test]
    fn response_cookies_reject_unrelated_and_public_suffix_domains() {
        let url = Url::parse("https://api.example.co.uk/").unwrap();
        let mut cookies = CookieStore::default();
        for value in [
            "a=1; Domain=other.co.uk",
            "b=2; Domain=co.uk",
            "c=3; Domain=uk",
        ] {
            cookies.add_response_cookie(value, &url);
        }
        assert!(cookies.header_value(&url).is_empty());
        cookies.add_response_cookie("valid=yes; Domain=.example.co.uk", &url);
        assert_eq!(
            cookies.header_value(&Url::parse("https://www.example.co.uk/").unwrap()),
            "valid=yes"
        );
        cookies.add_response_cookie(
            "private=no; Domain=github.io",
            &Url::parse("https://tenant.github.io/").unwrap(),
        );
        assert!(
            cookies
                .header_value(&Url::parse("https://other.github.io/").unwrap())
                .is_empty()
        );
    }

    #[test]
    fn cookie_jar_round_trip_preserves_scope_and_expiry() {
        let url = Url::parse("https://api.example.com/account/login").unwrap();
        let mut cookies = CookieStore::default();
        cookies.add_cookie_header("explicit=only");
        cookies.add_response_cookie("sid=account; Secure; HttpOnly", &url);
        cookies.add_response_cookie("shared=yes; Domain=example.com; Path=/; Max-Age=3600", &url);
        cookies.add_response_cookie("expired=no; Expires=Thu, 01 Jan 1970 00:00:01 GMT", &url);
        let jar = cookies.to_netscape();
        assert!(
            jar.contains("#HttpOnly_api.example.com\tFALSE\t/account\tTRUE\t0\tsid\taccount\n")
        );
        assert!(jar.contains("example.com\tTRUE\t/\tFALSE\t"));
        assert!(!jar.contains("expired"));
        assert!(!jar.contains("explicit"));
        let mut imported = CookieStore::default();
        imported.add_cookie_file(&jar);
        assert_eq!(imported.header_value(&url), "sid=account; shared=yes");
        assert_eq!(
            imported.header_value(&Url::parse("https://www.example.com/").unwrap()),
            "shared=yes"
        );
        assert!(
            imported
                .header_value(&Url::parse("https://other.test/").unwrap())
                .is_empty()
        );
    }

    #[test]
    fn cookie_deletion_survives_transfer_merge() {
        let url = Url::parse("https://example.com/").unwrap();
        let mut cookies = CookieStore::default();
        cookies.add_response_cookie("sid=old", &url);
        let mut transfer = cookies.clone();
        transfer.add_response_cookie("sid=deleted; Max-Age=0", &url);
        cookies.merge(&transfer);
        assert!(cookies.header_value(&url).is_empty());
        assert!(!cookies.to_netscape().contains("sid"));
    }

    #[test]
    fn parses_multiple_netrc_machines() {
        let entries = parse_netrc(
            "machine one.test login first password secret machine two.test login second password other",
        );
        assert_eq!(entries.len(), 2, "both machine entries should be retained");
        assert_eq!(entries[1].login, "second");
    }

    #[test]
    fn parses_default_netrc_credentials() {
        let entries = parse_netrc("default login fallback password secret");
        assert_eq!(entries.len(), 1, "the default entry should be retained");
        assert_eq!(entries[0].machine, "default");
        assert_eq!(entries[0].login, "fallback");
    }
}
