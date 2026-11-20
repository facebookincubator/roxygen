/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! TCP and TLS byte streams that stay on one worker, with byte events and
//! adoption of connections the application established.
//!
//! A transport is created on a worker thread and is read, written and dropped
//! there. Worker shutdown closes it. The HTTP sessions in `roxygen_session` run
//! over it. Most applications use `roxygen_server` and `roxygen_client`, which
//! accept and connect through this crate. Use it directly to dial or accept
//! connections yourself, or to run a session over a connection your
//! application already established.
//!
//! # Entry points
//!
//! - [`ByteStreamTransport`](byte_stream::ByteStreamTransport) is the interface
//!   that sessions read, write and close through. Every transport here is an
//!   [`OwnedByteStream`](byte_stream::OwnedByteStream) or wraps one.
//! - [`tcp::TcpTransport`] connects a TCP socket, or adopts one returned by
//!   `std::net::TcpListener::accept`.
//! - [`tls::TlsTransport`] connects or accepts TLS over TCP with rustls,
//!   configured by [`tls::TlsClientOptions`] and [`tls::TlsServerOptions`].
//! - [`established::adopt_local`] wraps a Tokio `AsyncRead + AsyncWrite` that
//!   the application connected itself, such as a tunnel or a Unix socket. The
//!   application stays responsible for routing, authentication and keeping the
//!   I/O's reactor alive.
//! - On Linux,
//!   [`OwnedByteStream::enable_zero_copy`](byte_stream::OwnedByteStream::enable_zero_copy)
//!   sends TCP and TLS writes of at least a given size with `MSG_ZEROCOPY`. It
//!   returns `false` where that is unavailable, including on other platforms
//!   and on adopted I/O.
//!
//! # Byte events
//!
//! Sessions use
//! [`AsyncSocketByteEventObserver`](byte_event::AsyncSocketByteEventObserver)
//! to report when a registered byte reaches a
//! [`ByteEventType`](roxygen_core::byte_event::ByteEventType). `TransportWrite`
//! fires when the session hands the byte to the transport, and `KernelWrite`
//! when the write that carries it completes. `NicTx` and `CumulativeAck`
//! require a transport whose
//! [`byte_event_capability`](byte_stream::ByteStreamTransport::byte_event_capability)
//! is `TransportEvents`, but every transport in this crate reports
//! `Unsupported`.
//!
//! # TLS certificate verification
//!
//! A client checks the server's certificate chain with webpki against roots
//! from the first of these sources that applies:
//!
//! 1. Certificates added with
//!    [`add_root_certificate`](tls::TlsClientOptions::add_root_certificate).
//!    They replace the system roots instead of adding to them.
//! 2. The PEM files given to
//!    [`set_ca_paths`](tls::TlsClientOptions::set_ca_paths).
//! 3. The PEM file named by the `PROXYGEN_CORO_CA_PATH` environment variable,
//!    else by `CURL_CA_BUNDLE`, else the path set at build time by
//!    `PROXYGEN_FALLBACK_CA_PATH` ([`tls::get_fallback_ca_path`]).
//! 4. The operating system's trust store, loaded with `rustls-native-certs`.
//!    Calling `set_ca_paths` with an empty list skips this source too.
//!
//! Connecting fails if a CA file cannot be read or holds no certificates, or
//! if the sources yield no roots. A
//! [`TlsCertificateVerifier`](tls::TlsCertificateVerifier) set with
//! `set_certificate_verifier` replaces the chain check.
//!
//! With options from [`TlsClientOptions::new`](tls::TlsClientOptions::new), a
//! certificate that does not match the server name fails the handshake. With
//! [`TlsServerNameValidation`](tls::TlsServerNameValidation) set to `Logging`
//! or `DisabledInsecure`, a name mismatch is accepted as long as the chain
//! verifies.
//!
//! `roxygen_client` changes two of these defaults. It sets the name check from
//! `ConnectionParams::identity_validation`, which defaults to `Logging`, so the
//! client accepts certificates issued for other names unless that field is
//! `Enforcing`. When it builds the TLS options itself for an `https` URL, it
//! calls `set_ca_paths` with its default CA list, which is
//! `/etc/pki/tls/cert.pem` unless changed with
//! `HttpClient::set_default_ca_paths`.
//!
//! Clients offer TLS 1.2 and 1.3 and the ALPN protocols `h2` and `http/1.1` by
//! default. Client options without a provider set, and server options not
//! built from a rustls `ServerConfig` or given a provider with
//! [`TlsServerSettings::with_crypto_provider`], use the process-wide rustls
//! `CryptoProvider` if one is installed, and ring otherwise.
//!
//! # TLS client certificates
//!
//! A server built with
//! [`TlsServerOptions::from_settings`](tls::TlsServerOptions::from_settings)
//! requests client certificates as its [`TlsClientVerification`] says: never,
//! optionally (a client that sends none is accepted, and a chain it sends must
//! verify), or always. A [`TlsClientTrust`] bundle anchors the check at its
//! self-signed certificates; its other certificates complete client chains and
//! have their validity checked like the intermediates a client sends. The
//! accepted connection's peer identity is the client's leaf certificate.

pub mod byte_event;
pub mod byte_stream;
pub mod established;
pub mod tcp;
mod tcp_zero_copy;
pub mod tls;

pub use byte_stream::TransportRetirement;
pub use byte_stream::TransportRetirementError;
pub use tls::TlsAlpnMismatch;
pub use tls::TlsClientTrust;
pub use tls::TlsClientVerification;
pub use tls::TlsHandshakeFailureKind;
pub use tls::TlsServerSettings;
pub use tls::tls_handshake_failure_kind;
pub use tls::tls_handshake_rustls_error;

#[cfg(test)]
static TEST_PROCESS_SPAWN_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
#[path = "../tests/byte_event_delivery.rs"]
mod byte_event_delivery_tests;

#[cfg(test)]
#[path = "../tests/byte_stream.rs"]
mod byte_stream_tests;

#[cfg(test)]
#[path = "../tests/established.rs"]
mod established_tests;

#[cfg(test)]
#[path = "../tests/tcp_tls.rs"]
mod tcp_tls_tests;

#[cfg(test)]
#[path = "../tests/exchange_completion.rs"]
mod exchange_completion_tests;
