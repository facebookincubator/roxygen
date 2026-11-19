/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Fetches one URL with Roxygen's experimental owned client and prints the
//! response.
//!
//! ```sh
//! cargo run --bin hello_server
//! cargo run --bin owned_client -- http://127.0.0.1:3000/
//! ```
//!
//! Pass `--h2c` to use HTTP/2 with prior knowledge, against
//! `hello_server --h2c`. An HTTPS URL negotiates HTTP/2 or HTTP/1.1 with ALPN.
//!
//! The owned client takes a `SocketAddress` and a request head, not a URL, and
//! `OwnedClientSession::connect` dials plaintext TCP only. The example builds
//! the head itself, and for HTTPS it makes the TLS connection and attaches the
//! session to it.

use std::error::Error;
use std::io::Write;
use std::time::Duration;

use roxygen_client::HttpClient;
use roxygen_client::Response;
use roxygen_client::connector::ClientAttachOptions;
use roxygen_client::connector::ConnectionParams;
use roxygen_client::connector::SessionParams;
use roxygen_client::experimental::owned::OwnedClientSession;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::http::CodecProtocol;
use roxygen_core::http::HttpMethod;
use roxygen_core::message::HttpMessage;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_core::url::Url;
use roxygen_core::url::UrlMode;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::tokio_host::ManagedWorker;
use roxygen_runtime::worker::WorkerContext;
use roxygen_transport::byte_stream::ByteStreamLimits;
use roxygen_transport::tls::TlsClientOptions;
use roxygen_transport::tls::TlsTransport;

type BoxError = Box<dyn Error + Send + Sync>;

const TIMEOUT: Duration = Duration::from_secs(10);

fn main() -> Result<(), BoxError> {
    let h2c = std::env::args().any(|arg| arg == "--h2c");
    let url = std::env::args()
        .skip(1)
        .find(|arg| arg != "--h2c")
        .unwrap_or_else(|| "http://127.0.0.1:3000/".to_owned());
    let worker = ManagedWorker::new(16)?;
    let call = worker
        .handle()
        .call_local(move |context| async move { fetch(&context, &url, h2c).await })?;
    let response = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(call)??;
    worker.shutdown()?;

    let head = &response.headers;
    println!("status: {}", head.response_status().unwrap_or_default());
    for (_, name, value) in head.headers().iter() {
        println!(
            "{}: {}",
            String::from_utf8_lossy(name),
            String::from_utf8_lossy(value)
        );
    }
    println!();
    let mut stdout = std::io::stdout().lock();
    for segment in response.body.segments() {
        stdout.write_all(segment)?;
    }
    Ok(())
}

async fn fetch(context: &WorkerContext, url: &str, h2c: bool) -> Result<Response, BoxError> {
    let url = Url::parse(url, false, UrlMode::StrictCompat);
    if !url.has_host() {
        return Err("invalid URL".into());
    }
    let session = connect(context, &url, h2c).await?;

    let mut request = HttpMessage::new();
    request.set_method(HttpMethod::Get)?;
    request.set_url(url.make_relative_url(), false)?;
    request.set_http_version(1, 1);
    request.set_secure(url.is_secure());
    request
        .headers_mut()
        .add("host", url.host_and_port_omit_default());
    let head = HttpHeaderEvent::new(request, true)?;
    let response = session
        .reserve_request()?
        .submit(head, HttpSourceHolder::empty(), CancellationToken::never())
        .await?;

    // As in `HttpClient`, the timeout bounds each read. `HttpResponseSource::split`
    // gives a reader that is polled in place instead of the holder.
    let mut source = response.source.into_source_owner().into_source_holder();
    source.set_read_timeout(TIMEOUT);
    let response = HttpClient::read_response(source).await?;
    // Dropping the session does not close the connection.
    session.close_when_idle()?;
    session.wait_closed().await?;
    Ok(response)
}

async fn connect(
    context: &WorkerContext,
    url: &Url,
    h2c: bool,
) -> Result<OwnedClientSession, HttpError> {
    let address = SocketAddress::new(url.host(), url.port());
    // Owned HTTP/1.1 sessions reject a session timer other than `Some(Duration::ZERO)`.
    let session = SessionParams {
        stream_read_timeout: Some(Duration::ZERO),
        connection_read_timeout: Some(Duration::ZERO),
        write_timeout: Some(Duration::ZERO),
        ..SessionParams::default()
    };
    if !url.is_secure() {
        let connection = ConnectionParams {
            plaintext_protocol: if h2c {
                CodecProtocol::Http2
            } else {
                CodecProtocol::Http11
            },
            ..ConnectionParams::default()
        };
        return OwnedClientSession::connect(
            context,
            address,
            TIMEOUT,
            connection,
            session,
            CancellationToken::never(),
        )
        .await;
    }
    // These options verify the certificate chain and reject certificates issued for other names.
    let tls = TlsClientOptions::new(String::from_utf8_lossy(url.host()));
    // `HttpConnector` uses these read, write and byte-event limits.
    let limits = ByteStreamLimits::new(256 * 1024, 256 * 1024, 1024)?;
    let cancellation = CancellationToken::never();
    let tls = TlsTransport::connect(context, address, tls, limits, TIMEOUT, cancellation).await?;
    let protocol = match tls.connection_info().application_protocol() {
        Some(b"h2") => CodecProtocol::Http2,
        _ => CodecProtocol::Http11,
    };
    let options = ClientAttachOptions {
        session,
        ..ClientAttachOptions::new(protocol, TransportSecurity::Tls)
    };
    OwnedClientSession::attach(context, tls.into_byte_stream(), options)
}
