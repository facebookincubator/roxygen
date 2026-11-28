/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Rustls adapter for the worker-local byte-stream contract.

use std::cell::Cell;
use std::cell::RefCell;
use std::fmt;
use std::future::Future;
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::io;
use std::io::IoSlice;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use roxygen_core::buf_queue::BufQueue;
use roxygen_core::byte_event::HttpByteEventRegistration;
use roxygen_core::connection::TlsSessionFacts;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::message::SocketAddress;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::worker::WorkerCleanupRegistration;
use roxygen_runtime::worker::WorkerCleanupTaskRegistration;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerState;
use rustls::ClientConfig;
use rustls::DigitallySignedStruct;
use rustls::DistinguishedName;
use rustls::HandshakeKind;
use rustls::NamedGroup;
use rustls::RootCertStore;
use rustls::ServerConfig;
use rustls::SignatureScheme;
use rustls::client::ClientSessionMemoryCache;
use rustls::client::ClientSessionStore;
use rustls::client::Resumption;
use rustls::client::Tls12ClientSessionValue;
use rustls::client::Tls13ClientSessionValue;
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::HandshakeSignatureValid;
use rustls::client::danger::ServerCertVerified;
use rustls::client::danger::ServerCertVerifier;
use rustls::client::verify_server_name;
use rustls::crypto::CryptoProvider;
use rustls::crypto::WebPkiSupportedAlgorithms;
use rustls::crypto::verify_tls12_signature;
use rustls::crypto::verify_tls13_signature;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::PrivateKeyDer;
use rustls::pki_types::ServerName;
use rustls::pki_types::UnixTime;
use rustls::server::Acceptor;
use rustls::server::NoServerSessionStorage;
use rustls::server::ParsedCertificate;
use rustls::server::ProducesTickets;
use rustls::server::ServerSessionMemoryCache;
use rustls::server::StoresServerSessions;
use rustls::server::WebPkiClientVerifier;
use rustls::server::danger::ClientCertVerified;
use rustls::server::danger::ClientCertVerifier;
use tokio::io::AsyncBufRead;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::time::Instant;
use tokio_rustls::LazyConfigAcceptor;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream as ClientTlsStream;
use tokio_rustls::server::TlsStream as ServerTlsStream;

use crate::byte_event::ByteEventWriteFlags;
use crate::byte_stream::ByteEventCapability;
use crate::byte_stream::ByteEventRegistrationError;
use crate::byte_stream::ByteStreamLimits;
use crate::byte_stream::ByteStreamTransport;
use crate::byte_stream::LocalTransportFuture;
use crate::byte_stream::OwnedByteStream;
use crate::byte_stream::PeerIdentity;
use crate::byte_stream::TransportRead;
use crate::byte_stream::TransportRetirement;
use crate::byte_stream::TransportWriteError;
use crate::byte_stream::WriteInfo;
use crate::tcp::AcceptedTcpStream;
use crate::tcp::AwaitError;
use crate::tcp::ConnectDeadline;
use crate::tcp::OwnedRead;
use crate::tcp::TcpConnectOptions;
use crate::tcp::TokioByteStream;
use crate::tcp::await_with_deadline;
use crate::tcp::connect_socket_with_options;
use crate::tcp::owned_from_stream;
use crate::tcp::prepare_accepted_stream;
use crate::tcp::set_congestion_flavor_best_effort;
use crate::tcp::setup_future;

pub(crate) mod backend;

/// Owner-local policy applied to the peer's end-entity certificate after chain
/// verification.
pub trait TlsPeerIdentityVerifier {
    fn verify(&self, certificate_der: &[u8]) -> Result<PeerIdentity, HttpError>;
}

/// Application-supplied certificate-chain policy used in place of WebPKI.
pub trait TlsCertificateVerifier: fmt::Debug + Send + Sync + 'static {
    fn verify_server_certificate(
        &self,
        server_name: &str,
        certificate_chain_der: &[&[u8]],
    ) -> Result<(), HttpError>;

    fn is_insecure(&self) -> bool {
        false
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TlsServerNameValidation {
    DisabledInsecure,
    Enforcing,
    Logging,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TlsServerNameVerification {
    Success,
    Fail,
    SoftFail,
}

pub type TlsServerNameLogFn = Arc<dyn Fn(TlsServerNameVerification, &[u8]) + Send + Sync>;

/// The oldest TLS protocol version a client connection may negotiate.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TlsVersion {
    #[default]
    Tls12,
    Tls13,
}

#[derive(Clone)]
pub struct TlsSessionStore(Arc<dyn ClientSessionStore>);

#[derive(Debug)]
struct IdentityMappedSessionStore {
    inner: Arc<dyn ClientSessionStore>,
    identity: ServerName<'static>,
}

impl ClientSessionStore for IdentityMappedSessionStore {
    fn set_kx_hint(&self, _server_name: ServerName<'static>, group: NamedGroup) {
        self.inner.set_kx_hint(self.identity.clone(), group);
    }

    fn kx_hint(&self, _server_name: &ServerName<'_>) -> Option<NamedGroup> {
        self.inner.kx_hint(&self.identity)
    }

    fn set_tls12_session(&self, _server_name: ServerName<'static>, value: Tls12ClientSessionValue) {
        self.inner.set_tls12_session(self.identity.clone(), value);
    }

    fn tls12_session(&self, _server_name: &ServerName<'_>) -> Option<Tls12ClientSessionValue> {
        self.inner.tls12_session(&self.identity)
    }

    fn remove_tls12_session(&self, _server_name: &ServerName<'static>) {
        self.inner.remove_tls12_session(&self.identity);
    }

    fn insert_tls13_ticket(
        &self,
        _server_name: ServerName<'static>,
        value: Tls13ClientSessionValue,
    ) {
        self.inner.insert_tls13_ticket(self.identity.clone(), value);
    }

    fn take_tls13_ticket(
        &self,
        _server_name: &ServerName<'static>,
    ) -> Option<Tls13ClientSessionValue> {
        self.inner.take_tls13_ticket(&self.identity)
    }
}

impl TlsSessionStore {
    pub fn in_memory(capacity: usize) -> Self {
        Self(Arc::new(ClientSessionMemoryCache::new(capacity)))
    }
}

/// Runtime-neutral client settings for the optional rustls adapter.
pub struct TlsClientOptions {
    server_name: Box<str>,
    root_certificates: Vec<Box<[u8]>>,
    ca_paths: Vec<PathBuf>,
    client_certificate_pem: Option<Box<[u8]>>,
    client_private_key_pem: Option<Box<[u8]>>,
    alpn_protocols: Vec<Box<[u8]>>,
    identity_verifier: Option<Rc<dyn TlsPeerIdentityVerifier>>,
    certificate_verifier: Option<Arc<dyn TlsCertificateVerifier>>,
    server_name_validation: TlsServerNameValidation,
    server_name_log: Option<TlsServerNameLogFn>,
    use_native_roots: bool,
    early_data: bool,
    session_caching: bool,
    session_store: Option<Arc<dyn ClientSessionStore>>,
    session_store_explicit: bool,
    session_cache_identity: Option<Box<str>>,
    minimum_version: TlsVersion,
    crypto_provider: Option<Arc<CryptoProvider>>,
    config: Rc<RefCell<Option<Arc<ClientConfig>>>>,
}

impl Clone for TlsClientOptions {
    fn clone(&self) -> Self {
        Self {
            server_name: self.server_name.clone(),
            root_certificates: self.root_certificates.clone(),
            ca_paths: self.ca_paths.clone(),
            client_certificate_pem: self.client_certificate_pem.clone(),
            client_private_key_pem: self.client_private_key_pem.clone(),
            alpn_protocols: self.alpn_protocols.clone(),
            identity_verifier: self.identity_verifier.clone(),
            certificate_verifier: self.certificate_verifier.clone(),
            server_name_validation: self.server_name_validation,
            server_name_log: self.server_name_log.clone(),
            use_native_roots: self.use_native_roots,
            early_data: self.early_data,
            session_caching: self.session_caching,
            session_store: self.session_store.clone(),
            session_store_explicit: self.session_store_explicit,
            session_cache_identity: self.session_cache_identity.clone(),
            minimum_version: self.minimum_version,
            crypto_provider: self.crypto_provider.clone(),
            config: Rc::clone(&self.config),
        }
    }
}

impl TlsClientOptions {
    pub fn cache_identity(&self) -> usize {
        Rc::as_ptr(&self.config) as usize
    }

    pub fn new(server_name: impl Into<Box<str>>) -> Self {
        Self {
            server_name: server_name.into(),
            root_certificates: Vec::new(),
            ca_paths: Vec::new(),
            client_certificate_pem: None,
            client_private_key_pem: None,
            alpn_protocols: vec![
                b"h2".to_vec().into_boxed_slice(),
                b"http/1.1".to_vec().into_boxed_slice(),
            ],
            identity_verifier: None,
            certificate_verifier: None,
            server_name_validation: TlsServerNameValidation::Enforcing,
            server_name_log: None,
            use_native_roots: true,
            early_data: false,
            session_caching: true,
            session_store: Some(Arc::new(ClientSessionMemoryCache::new(10_000))),
            session_store_explicit: false,
            session_cache_identity: None,
            minimum_version: TlsVersion::Tls12,
            crypto_provider: None,
            config: Rc::new(RefCell::new(None)),
        }
    }

    pub fn add_root_certificate(&mut self, certificate_der: impl AsRef<[u8]>) {
        if self.root_certificates.is_empty() {
            self.use_native_roots = false;
        }
        self.root_certificates
            .push(certificate_der.as_ref().to_vec().into_boxed_slice());
        self.config = Rc::new(RefCell::new(None));
    }

    pub fn set_ca_paths<I, P>(&mut self, paths: I)
    where
        I: IntoIterator<Item = P>,
        P: AsRef<Path>,
    {
        self.ca_paths = paths
            .into_iter()
            .map(|path| path.as_ref().to_path_buf())
            .collect();
        self.root_certificates.clear();
        self.use_native_roots = false;
        self.config = Rc::new(RefCell::new(None));
    }

    pub fn set_client_identity_pem(
        &mut self,
        certificate_chain_pem: impl AsRef<[u8]>,
        private_key_pem: impl AsRef<[u8]>,
    ) {
        self.client_certificate_pem = Some(certificate_chain_pem.as_ref().into());
        self.client_private_key_pem = Some(private_key_pem.as_ref().into());
        self.config = Rc::new(RefCell::new(None));
    }

    pub async fn set_client_identity_paths(
        &mut self,
        certificate_chain_path: impl AsRef<Path>,
        private_key_path: impl AsRef<Path>,
    ) -> Result<(), HttpError> {
        let certificate_chain_pem =
            tokio::fs::read(certificate_chain_path)
                .await
                .map_err(|error| {
                    tls_message_error(b"failed to read TLS client certificate: ", error)
                })?;
        let private_key_pem = tokio::fs::read(private_key_path).await.map_err(|error| {
            tls_message_error(b"failed to read TLS client private key: ", error)
        })?;
        self.set_client_identity_pem(certificate_chain_pem, private_key_pem);
        Ok(())
    }

    pub fn set_alpn_protocols<I, P>(&mut self, protocols: I)
    where
        I: IntoIterator<Item = P>,
        P: AsRef<[u8]>,
    {
        self.alpn_protocols = protocols
            .into_iter()
            .map(|protocol| protocol.as_ref().to_vec().into_boxed_slice())
            .collect();
        self.config = Rc::new(RefCell::new(None));
    }

    pub fn set_identity_verifier<V>(&mut self, verifier: Rc<V>)
    where
        V: TlsPeerIdentityVerifier + 'static,
    {
        self.identity_verifier = Some(verifier);
        self.config = Rc::new(RefCell::new(None));
    }

    pub fn set_certificate_verifier(&mut self, verifier: Option<Arc<dyn TlsCertificateVerifier>>) {
        self.certificate_verifier = verifier;
        self.config = Rc::new(RefCell::new(None));
    }

    pub fn has_certificate_verifier(&self) -> bool {
        self.certificate_verifier.is_some()
    }

    pub fn set_server_name(&mut self, server_name: impl Into<Box<str>>) {
        self.server_name = server_name.into();
    }

    pub fn set_server_name_validation(
        &mut self,
        validation: TlsServerNameValidation,
        log: Option<TlsServerNameLogFn>,
    ) {
        let log_changed = match (&self.server_name_log, &log) {
            (None, None) => false,
            (Some(current), Some(next)) => !Arc::ptr_eq(current, next),
            _ => true,
        };
        if self.server_name_validation != validation || log_changed {
            self.server_name_validation = validation;
            self.server_name_log = log;
            self.config = Rc::new(RefCell::new(None));
        }
    }

    pub fn set_session_caching(&mut self, enabled: bool) {
        if self.session_caching != enabled {
            self.session_caching = enabled;
            self.config = Rc::new(RefCell::new(None));
        }
    }

    pub fn set_early_data(&mut self, enabled: bool) {
        if self.early_data != enabled {
            self.early_data = enabled;
            self.config = Rc::new(RefCell::new(None));
        }
    }

    pub fn set_session_store(&mut self, store: Arc<dyn ClientSessionStore>) {
        self.session_caching = true;
        self.session_store = Some(store);
        self.session_store_explicit = true;
        self.config = Rc::new(RefCell::new(None));
    }

    pub fn set_shared_session_store(&mut self, store: &TlsSessionStore) {
        if self
            .session_store
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &store.0))
            && self.session_caching
        {
            return;
        }
        self.session_caching = true;
        self.session_store = Some(Arc::clone(&store.0));
        self.session_store_explicit = false;
        self.config = Rc::new(RefCell::new(None));
    }

    pub fn set_session_cache_identity(&mut self, identity: Option<Box<str>>) {
        if self.session_cache_identity != identity {
            self.session_cache_identity = identity;
            self.config = Rc::new(RefCell::new(None));
        }
    }

    /// Restricts negotiation to this TLS version or newer.
    pub fn set_minimum_version(&mut self, version: TlsVersion) {
        if self.minimum_version != version {
            self.minimum_version = version;
            self.config = Rc::new(RefCell::new(None));
        }
    }

    pub const fn session_caching_enabled(&self) -> bool {
        self.session_caching
    }

    /// Selects the cryptographic implementation and ordered cipher suites for these options.
    pub fn set_crypto_provider(&mut self, provider: Arc<CryptoProvider>) {
        self.crypto_provider = Some(provider);
        self.config = Rc::new(RefCell::new(None));
    }

    pub const fn has_explicit_session_store(&self) -> bool {
        self.session_store_explicit
    }
}

pub const fn get_fallback_ca_path() -> Option<&'static str> {
    option_env!("PROXYGEN_FALLBACK_CA_PATH")
}

const TLS_CIPHERTEXT_STAGING_LIMIT: usize = 128 * 1024;

/// How a TLS connection closes, shared by its transport, its TLS stream and its ciphertext
/// writer.
#[derive(Default)]
struct TlsCloseMode {
    /// Every reset closes the socket without `SO_LINGER` zero.
    non_abortive: Cell<bool>,
    /// The graceful close sends no close_notify.
    skip_close_notify: Cell<bool>,
    /// Asked once, when the graceful close starts; false skips close_notify.
    close_notify_condition: RefCell<Option<Box<dyn Fn() -> bool>>>,
}

impl TlsCloseMode {
    /// Whether the graceful close that is starting sends close_notify.
    fn close_notify(&self) -> bool {
        let condition = self.close_notify_condition.borrow_mut().take();
        if let Some(established) = condition
            && !established()
        {
            self.skip_close_notify.set(true);
        }
        !self.skip_close_notify.get()
    }
}

/// Controls how one TLS connection's graceful close ends. Clones control the same connection.
#[derive(Clone)]
pub struct TlsCloseControl(Rc<TlsCloseMode>);

impl TlsCloseControl {
    /// Makes the connection's graceful close (`close` or `shutdown_write`) send no
    /// close_notify: once the ciphertext already written is on the socket, the TCP stream is
    /// shut down with FIN. It has no effect on a close that has already started.
    pub fn skip_close_notify(&self) {
        self.0.skip_close_notify.set(true);
    }

    /// Makes the connection's graceful close send close_notify only if `established` returns
    /// true when the close starts; otherwise the close ends as `skip_close_notify` describes.
    /// `established` is called at most once, on the connection's worker, and replaces any
    /// condition set before.
    pub fn skip_close_notify_unless(&self, established: impl Fn() -> bool + 'static) {
        *self.0.close_notify_condition.borrow_mut() = Some(Box::new(established));
    }
}

/// Bounded caller-owned storage for ciphertext emitted by rustls.
pub(crate) struct OwnedCiphertextStream<S: TokioByteStream> {
    state: Rc<RefCell<CiphertextState<S>>>,
    cleanup: Option<WorkerCleanupRegistration>,
    flush_waiting: bool,
    shutdown_waiting: bool,
    #[cfg(test)]
    staged_batches: Rc<Cell<usize>>,
}

#[derive(Debug, thiserror::Error)]
#[error("background TLS ciphertext write timed out")]
pub(crate) struct CiphertextWriteTimeout;

#[derive(Clone)]
enum StoredIoError {
    WriteTimeout,
    Io {
        kind: io::ErrorKind,
        message: String,
    },
}

#[derive(Clone, Copy)]
enum ForegroundRole {
    Read,
    Write,
    Barrier,
}

impl StoredIoError {
    fn new(error: io::Error) -> Self {
        if error
            .get_ref()
            .is_some_and(|cause| cause.is::<CiphertextWriteTimeout>())
        {
            Self::WriteTimeout
        } else {
            Self::Io {
                kind: error.kind(),
                message: error.to_string(),
            }
        }
    }

    fn to_io_error(&self) -> io::Error {
        match self {
            Self::WriteTimeout => io::Error::new(io::ErrorKind::TimedOut, CiphertextWriteTimeout),
            Self::Io { kind, message } => io::Error::new(*kind, message.clone()),
        }
    }
}

/// The bound Tokio's `safe_delay` puts on a `tokio::time::sleep` duration: about 30 years.
const TOKIO_MAX_SLEEP: Duration = Duration::from_secs(86_400 * 365 * 30);

struct CiphertextState<S> {
    stream: Option<S>,
    pending: BufQueue,
    pending_zero_copy: Option<bool>,
    error: Option<StoredIoError>,
    task_waker: Option<std::task::Waker>,
    read_waker: Option<std::task::Waker>,
    write_waker: Option<std::task::Waker>,
    barrier_waker: Option<std::task::Waker>,
    foreground_role: ForegroundRole,
    flush_requested: bool,
    shutdown_requested: bool,
    shutdown_complete: bool,
    plaintext_complete: bool,
    next_timeout: Option<Duration>,
    deadline: Option<Pin<Box<tokio::time::Sleep>>>,
    /// The deadline of a timed write whose ciphertext is not staged yet. The background task reads
    /// the deadline only while ciphertext is staged, so its timer is built only when ciphertext
    /// is first staged (`stage_deadline`). A write that TCP accepts immediately never builds one.
    unstaged_deadline: Option<Instant>,
    stopped: bool,
    close: Rc<TlsCloseMode>,
}

impl<S> CiphertextState<S>
where
    S: TokioByteStream,
{
    fn configure_write_timeout(&mut self, timeout: Duration) {
        if !self.plaintext_complete {
            return;
        }
        self.plaintext_complete = false;
        if timeout.is_zero() {
            if self.pending.is_empty() {
                self.clear_deadline();
            }
            return;
        }
        // `tokio::time::sleep(timeout)`'s deadline, which clamps the timeout to `TOKIO_MAX_SLEEP`.
        let deadline = Instant::now() + timeout.min(TOKIO_MAX_SLEEP);
        if self
            .deadline_instant()
            .is_some_and(|current| deadline >= current)
        {
            return;
        }
        if self.pending.is_empty()
            && !self.flush_requested
            && !self.shutdown_requested
            && !self.shutdown_complete
            && !self.stopped
            && self.error.is_none()
            && self.stream.is_some()
        {
            self.deadline = None;
            self.unstaged_deadline = Some(deadline);
            return;
        }
        self.unstaged_deadline = Some(deadline);
        self.stage_deadline();
        if let Some(waker) = self.task_waker.take() {
            waker.wake();
        }
    }

    /// The current write deadline, whether its timer is built or not.
    fn deadline_instant(&self) -> Option<Instant> {
        self.deadline
            .as_ref()
            .map(|deadline| deadline.deadline())
            .or(self.unstaged_deadline)
    }

    /// Builds the timer of an unstaged deadline, just before ciphertext is staged.
    fn stage_deadline(&mut self) {
        if let Some(deadline) = self.unstaged_deadline.take() {
            #[cfg(test)]
            record_deadline_timer();
            self.deadline = Some(Box::pin(tokio::time::sleep_until(deadline)));
        }
    }

    fn clear_deadline(&mut self) {
        self.deadline = None;
        self.unstaged_deadline = None;
    }

    /// Stores the writer task's waker, keeping a stored one that will wake the
    /// same task.
    fn register_task_waker(&mut self, waker: &std::task::Waker) {
        if self
            .task_waker
            .as_ref()
            .is_none_or(|stored| !stored.will_wake(waker))
        {
            self.task_waker = Some(waker.clone());
        }
    }

    fn foreground_waker_for(&mut self, role: ForegroundRole) -> &mut Option<std::task::Waker> {
        match role {
            ForegroundRole::Read => &mut self.read_waker,
            ForegroundRole::Write => &mut self.write_waker,
            ForegroundRole::Barrier => &mut self.barrier_waker,
        }
    }

    fn register_foreground_waker_for(&mut self, role: ForegroundRole, waker: &std::task::Waker) {
        let slot = self.foreground_waker_for(role);
        if slot
            .as_ref()
            .is_none_or(|registered| !registered.will_wake(waker))
        {
            *slot = Some(waker.clone());
        }
    }

    fn register_foreground_waker(&mut self, waker: &std::task::Waker) {
        self.register_foreground_waker_for(self.foreground_role, waker);
    }

    fn clear_foreground_waker(&mut self) {
        self.foreground_waker_for(self.foreground_role).take();
    }

    fn set_foreground_role(&mut self, role: ForegroundRole) -> ForegroundRole {
        std::mem::replace(&mut self.foreground_role, role)
    }

    fn wake_waiters(&mut self) {
        [
            self.read_waker.take(),
            self.write_waker.take(),
            self.barrier_waker.take(),
        ]
        .into_iter()
        .flatten()
        .for_each(std::task::Waker::wake);
    }

    fn abort(&mut self, reset: bool, error: Option<io::Error>) {
        if self.stopped {
            return;
        }
        self.stopped = true;
        if self.error.is_none()
            && let Some(error) = error
        {
            self.error = Some(StoredIoError::new(error));
        }
        if let Some(stream) = self.stream.as_mut() {
            if reset && !self.close.non_abortive.get() {
                stream.prepare_reset();
            }
            stream.abandon_zero_copy();
        }
        self.stream.take();
        self.pending = BufQueue::new(TLS_CIPHERTEXT_STAGING_LIMIT);
        self.pending_zero_copy = None;
        self.flush_requested = false;
        self.shutdown_requested = false;
        self.next_timeout = None;
        self.clear_deadline();
        self.wake_waiters();
        if let Some(waker) = self.task_waker.take() {
            waker.wake();
        }
    }

    fn fail(&mut self, error: io::Error) {
        self.abort(true, Some(error));
    }

    fn poll_background(&mut self, context: &mut Context<'_>) -> Poll<()> {
        loop {
            if self.stopped {
                return Poll::Ready(());
            }

            if !self.pending.is_empty() {
                if self
                    .deadline
                    .as_mut()
                    .is_some_and(|deadline| deadline.as_mut().poll(context).is_ready())
                {
                    self.fail(io::Error::new(
                        io::ErrorKind::TimedOut,
                        CiphertextWriteTimeout,
                    ));
                    return Poll::Ready(());
                }
                let Some(stream) = self.stream.as_mut() else {
                    self.fail(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "TLS ciphertext transport is closed",
                    ));
                    return Poll::Ready(());
                };
                let zero_copy = *self
                    .pending_zero_copy
                    .get_or_insert_with(|| stream.begin_owned_write(self.pending.chain_length()));
                match stream.poll_write_owned(context, &self.pending, zero_copy) {
                    Poll::Ready(Ok(0)) => {
                        self.fail(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "TLS ciphertext write made no progress",
                        ));
                        return Poll::Ready(());
                    }
                    Poll::Ready(Ok(written)) if written <= self.pending.chain_length() => {
                        self.pending
                            .trim_start(written)
                            .expect("validated ciphertext progress fits the pending queue");
                        self.wake_waiters();
                        continue;
                    }
                    Poll::Ready(Ok(_)) => {
                        self.fail(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "TCP reported excess TLS ciphertext progress",
                        ));
                        return Poll::Ready(());
                    }
                    Poll::Ready(Err(error)) => {
                        self.fail(error);
                        return Poll::Ready(());
                    }
                    Poll::Pending => {
                        self.register_task_waker(context.waker());
                        return Poll::Pending;
                    }
                }
            }

            self.pending_zero_copy = None;
            if self.plaintext_complete {
                self.clear_deadline();
            }
            let Some(stream) = self.stream.as_mut() else {
                return Poll::Ready(());
            };
            if self.shutdown_requested {
                return match Pin::new(stream).poll_shutdown(context) {
                    Poll::Ready(Ok(())) => {
                        self.shutdown_requested = false;
                        self.flush_requested = false;
                        self.shutdown_complete = true;
                        self.wake_waiters();
                        Poll::Ready(())
                    }
                    Poll::Ready(Err(error)) => {
                        self.fail(error);
                        Poll::Ready(())
                    }
                    Poll::Pending => {
                        self.register_task_waker(context.waker());
                        Poll::Pending
                    }
                };
            }
            if self.flush_requested {
                match Pin::new(stream).poll_flush(context) {
                    Poll::Ready(Ok(())) => {
                        self.flush_requested = false;
                        self.wake_waiters();
                        continue;
                    }
                    Poll::Ready(Err(error)) => {
                        self.fail(error);
                        return Poll::Ready(());
                    }
                    Poll::Pending => {
                        self.register_task_waker(context.waker());
                        return Poll::Pending;
                    }
                }
            }

            self.register_task_waker(context.waker());
            return Poll::Pending;
        }
    }
}

fn poll_ciphertext_background<S>(
    state: &Weak<RefCell<CiphertextState<S>>>,
    context: &mut Context<'_>,
) -> Poll<()>
where
    S: TokioByteStream,
{
    let Some(state) = state.upgrade() else {
        return Poll::Ready(());
    };
    state.borrow_mut().poll_background(context)
}

#[cfg(test)]
pub(crate) struct CiphertextBackgroundForTest<S: TokioByteStream> {
    state: Weak<RefCell<CiphertextState<S>>>,
}

#[cfg(test)]
impl<S: TokioByteStream> Clone for CiphertextBackgroundForTest<S> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

#[cfg(test)]
impl<S> CiphertextBackgroundForTest<S>
where
    S: TokioByteStream,
{
    pub(crate) fn poll(&self, context: &mut Context<'_>) -> Poll<()> {
        poll_ciphertext_background(&self.state, context)
    }

    pub(crate) fn has_deadline(&self) -> bool {
        self.state
            .upgrade()
            .is_some_and(|state| state.borrow().deadline_instant().is_some())
    }

    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.state
            .upgrade()
            .and_then(|state| state.borrow().deadline_instant())
    }
}

#[cfg(test)]
std::thread_local! {
    static DEADLINE_TIMERS: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
fn record_deadline_timer() {
    DEADLINE_TIMERS.with(|timers| timers.set(timers.get() + 1));
}

/// The ciphertext write-deadline timers built on this thread.
#[cfg(test)]
pub(crate) fn deadline_timers_for_test() -> usize {
    DEADLINE_TIMERS.with(Cell::get)
}

impl<S> OwnedCiphertextStream<S>
where
    S: TokioByteStream,
{
    pub(crate) fn new(stream: S) -> Self {
        Self {
            state: Rc::new(RefCell::new(CiphertextState {
                stream: Some(stream),
                pending: BufQueue::new(TLS_CIPHERTEXT_STAGING_LIMIT),
                pending_zero_copy: None,
                error: None,
                task_waker: None,
                read_waker: None,
                write_waker: None,
                barrier_waker: None,
                foreground_role: ForegroundRole::Barrier,
                flush_requested: false,
                shutdown_requested: false,
                shutdown_complete: false,
                plaintext_complete: true,
                next_timeout: None,
                deadline: None,
                unstaged_deadline: None,
                stopped: false,
                close: Rc::default(),
            })),
            cleanup: None,
            flush_waiting: false,
            shutdown_waiting: false,
            #[cfg(test)]
            staged_batches: Rc::new(Cell::new(0)),
        }
    }

    fn prepare_reset(&self) {
        if let Some(stream) = self.state.borrow().stream.as_ref() {
            stream.prepare_reset();
        }
    }

    fn close_mode(&self) -> Rc<TlsCloseMode> {
        Rc::clone(&self.state.borrow().close)
    }

    fn set_foreground_role(&self, role: ForegroundRole) -> ForegroundRole {
        self.state.borrow_mut().set_foreground_role(role)
    }

    fn clear_foreground_waker(&self, role: ForegroundRole) {
        let mut state = self.state.borrow_mut();
        let previous = state.set_foreground_role(role);
        state.clear_foreground_waker();
        state.foreground_role = previous;
    }

    fn enable_zero_copy(&self, context: &WorkerContext, threshold: usize) -> io::Result<bool> {
        let mut state = self.state.borrow_mut();
        let Some(stream) = state.stream.as_mut() else {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "TLS ciphertext transport is closed",
            ));
        };
        stream.enable_zero_copy(context, threshold)
    }

    fn set_zero_copy_reenable_threshold(&self, threshold: usize) {
        if let Some(stream) = self.state.borrow_mut().stream.as_mut() {
            stream.set_zero_copy_reenable_threshold(threshold);
        }
    }

    fn begin_plaintext_write(&self) {
        let mut state = self.state.borrow_mut();
        let timeout = state.next_timeout.take().unwrap_or(Duration::ZERO);
        state.configure_write_timeout(timeout);
    }

    fn mark_plaintext_complete(&self) {
        let mut state = self.state.borrow_mut();
        state.plaintext_complete = true;
        if state.pending.is_empty() {
            state.clear_deadline();
        }
    }

    #[cfg(test)]
    fn zero_copy_owner_for_test(&self) -> Option<usize> {
        self.state
            .borrow()
            .stream
            .as_ref()
            .and_then(TokioByteStream::zero_copy_owner_for_test)
    }

    #[cfg(test)]
    fn zero_copy_pending_for_test(&self) -> usize {
        self.state
            .borrow()
            .stream
            .as_ref()
            .map_or(0, TokioByteStream::zero_copy_pending_for_test)
    }

    fn abort(&mut self) {
        self.state.borrow_mut().abort(true, None);
    }

    fn abandon(&mut self) {
        self.state.borrow_mut().abort(false, None);
    }

    #[cfg(test)]
    pub(crate) fn pending_length_for_test(&self) -> usize {
        self.state.borrow().pending.chain_length()
    }

    #[cfg(test)]
    pub(crate) fn pending_segments_for_test(&self) -> Vec<(usize, Vec<u8>)> {
        self.state
            .borrow()
            .pending
            .segments()
            .map(|segment| (segment.as_ptr() as usize, segment.to_vec()))
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn poll_background_for_test(&self, context: &mut Context<'_>) -> Poll<()> {
        poll_ciphertext_background(&Rc::downgrade(&self.state), context)
    }

    #[cfg(test)]
    pub(crate) fn background_for_test(&self) -> CiphertextBackgroundForTest<S> {
        CiphertextBackgroundForTest {
            state: Rc::downgrade(&self.state),
        }
    }

    #[cfg(test)]
    pub(crate) fn staged_batches_for_test(&self) -> usize {
        self.staged_batches.get()
    }

    #[cfg(test)]
    pub(crate) fn set_read_role_for_test(&self) {
        self.state.borrow_mut().foreground_role = ForegroundRole::Read;
    }

    #[cfg(test)]
    pub(crate) fn set_write_role_for_test(&self) {
        self.state.borrow_mut().foreground_role = ForegroundRole::Write;
    }

    fn poll_stage_vectored(
        &mut self,
        context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let (accepted, wake) = {
            let mut state = self.state.borrow_mut();
            if let Some(error) = &state.error {
                let error = error.to_io_error();
                state.clear_foreground_waker();
                return Poll::Ready(Err(error));
            }
            let available = TLS_CIPHERTEXT_STAGING_LIMIT - state.pending.chain_length();
            if available == 0 {
                state.register_foreground_waker(context.waker());
                return Poll::Pending;
            }

            if state.pending.is_empty()
                && !state.flush_requested
                && !state.shutdown_requested
                && !state.shutdown_complete
                && state
                    .deadline_instant()
                    .is_none_or(|deadline| deadline > Instant::now())
                && let Some(offered) = buffers
                    .iter()
                    .try_fold(0usize, |length, buffer| length.checked_add(buffer.len()))
                    .filter(|length| *length != 0 && *length <= available)
                && let Some(stream) = state.stream.as_mut()
            {
                let error = match stream.try_write_borrowed(context, buffers) {
                    Ok(written) if written != 0 && written <= offered => {
                        state.clear_foreground_waker();
                        return Poll::Ready(Ok(written));
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => None,
                    Ok(0) => Some(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "TLS ciphertext write made no progress",
                    )),
                    Ok(_) => Some(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "TCP reported excess TLS ciphertext progress",
                    )),
                    Err(error) => Some(error),
                };
                if let Some(error) = error {
                    let returned = io::Error::new(error.kind(), error.to_string());
                    state.fail(error);
                    return Poll::Ready(Err(returned));
                }
            }

            let mut accepted = 0;
            for buffer in buffers.iter().filter(|buffer| !buffer.is_empty()) {
                let amount = buffer.len().min(available - accepted);
                if amount == 0 {
                    break;
                }
                state
                    .pending
                    .push_copy(&buffer[..amount])
                    .expect("ciphertext is truncated to the staging limit");
                accepted += amount;
            }
            if accepted == 0 {
                state.clear_foreground_waker();
                return Poll::Ready(Ok(0));
            }
            state.stage_deadline();
            #[cfg(test)]
            self.staged_batches.set(self.staged_batches.get() + 1);
            state.clear_foreground_waker();
            (accepted, state.task_waker.take())
        };
        if let Some(waker) = wake {
            waker.wake();
        }
        Poll::Ready(Ok(accepted))
    }

    fn poll_barrier(&mut self, context: &mut Context<'_>, shutdown: bool) -> Poll<io::Result<()>> {
        let (result, wake) = {
            let mut state = self.state.borrow_mut();
            if let Some(error) = &state.error {
                let error = error.to_io_error();
                state.barrier_waker = None;
                return Poll::Ready(Err(error));
            }
            if shutdown && state.shutdown_complete {
                state.barrier_waker = None;
                return Poll::Ready(Ok(()));
            }
            let waiting = if shutdown {
                &mut self.shutdown_waiting
            } else {
                &mut self.flush_waiting
            };
            if *waiting {
                let request_pending = if shutdown {
                    state.shutdown_requested
                } else {
                    state.flush_requested
                };
                if !request_pending {
                    *waiting = false;
                    state.barrier_waker = None;
                    return Poll::Ready(Ok(()));
                }
            } else {
                if shutdown {
                    state.shutdown_requested = true;
                } else {
                    state.flush_requested = true;
                }
                *waiting = true;
            }
            state.register_foreground_waker_for(ForegroundRole::Barrier, context.waker());
            (Poll::Pending, state.task_waker.take())
        };
        if let Some(waker) = wake {
            waker.wake();
        }
        result
    }
}

impl OwnedCiphertextStream<AcceptedTcpStream> {
    /// Starts the ciphertext writer, as a TLS connect or accept does.
    #[cfg(test)]
    pub(crate) fn start_background_for_test(
        &mut self,
        context: &WorkerContext,
    ) -> Result<(), HttpError> {
        self.start_background(context)
    }

    fn start_background(&mut self, context: &WorkerContext) -> Result<(), HttpError> {
        // `register_scope` admits work while the worker quiesces, but a new TLS
        // connection is refused once admission closes.
        if !context.handle().is_accepting() {
            return Err(dropped_error());
        }
        let weak = Rc::downgrade(&self.state);
        let cleanup_weak = weak.clone();
        let cleanup = context
            .register_cleanup(move || {
                if let Some(state) = cleanup_weak.upgrade() {
                    state.borrow_mut().abort(
                        true,
                        Some(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "TLS owner worker was dropped",
                        )),
                    );
                }
            })
            .map_err(|error| {
                HttpError::with_message(
                    HttpErrorCode::INVALID_STATE_TRANSITION,
                    error.to_string().as_bytes(),
                )
            })?;
        let writer = context
            .register_scope(std::future::poll_fn(move |task_context| {
                poll_ciphertext_background(&weak, task_context)
            }))
            .and_then(WorkerCleanupTaskRegistration::activate);
        match writer {
            Ok(writer) => writer.detach(),
            Err(_) => {
                let _ = cleanup.disarm();
                return Err(dropped_error());
            }
        }
        self.cleanup = Some(cleanup);
        Ok(())
    }
}

impl<S: TokioByteStream> Drop for OwnedCiphertextStream<S> {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            let _ = cleanup.disarm();
        }
        self.state.borrow_mut().abort(false, None);
    }
}

impl<S> AsyncRead for OwnedCiphertextStream<S>
where
    S: TokioByteStream,
{
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut state = self.state.borrow_mut();
        if let Some(error) = &state.error {
            return Poll::Ready(Err(error.to_io_error()));
        }
        let Some(stream) = state.stream.as_mut() else {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "TLS ciphertext transport is closed",
            )));
        };
        let result = Pin::new(stream).poll_read(context, buffer);
        if result.is_pending() {
            state.register_foreground_waker(context.waker());
        } else {
            state.clear_foreground_waker();
        }
        result
    }
}

impl<S> AsyncWrite for OwnedCiphertextStream<S>
where
    S: TokioByteStream,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_stage_vectored(context, &[IoSlice::new(buffer)])
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.poll_stage_vectored(context, buffers)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_barrier(context, false)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_barrier(context, true)
    }
}

pub(crate) trait TlsStreamAccess: AsyncBufRead + AsyncWrite + Unpin + 'static {
    type Tcp: TokioByteStream;

    fn transport(&self) -> &OwnedCiphertextStream<Self::Tcp>;

    fn transport_mut(&mut self) -> &mut OwnedCiphertextStream<Self::Tcp>;

    fn wants_write(&self) -> bool;
}

impl<S> TlsStreamAccess for ClientTlsStream<OwnedCiphertextStream<S>>
where
    S: TokioByteStream,
{
    type Tcp = S;

    fn transport(&self) -> &OwnedCiphertextStream<S> {
        self.get_ref().0
    }

    fn transport_mut(&mut self) -> &mut OwnedCiphertextStream<S> {
        self.get_mut().0
    }

    fn wants_write(&self) -> bool {
        self.get_ref().1.wants_write()
    }
}

impl<S> TlsStreamAccess for ServerTlsStream<OwnedCiphertextStream<S>>
where
    S: TokioByteStream,
{
    type Tcp = S;

    fn transport(&self) -> &OwnedCiphertextStream<S> {
        self.get_ref().0
    }

    fn transport_mut(&mut self) -> &mut OwnedCiphertextStream<S> {
        self.get_mut().0
    }

    fn wants_write(&self) -> bool {
        self.get_ref().1.wants_write()
    }
}

/// Reports plaintext progress once ciphertext reaches TCP or the owned staging
/// queue. Queued ciphertext drains on the owner worker.
pub(crate) struct OwnedTlsStream<T> {
    tls: Option<T>,
    write_remaining: usize,
    pending_plaintext: Option<usize>,
    write_active: bool,
    /// Whether the shutdown sends close_notify, decided when it starts.
    shutdown_close_notify: Option<bool>,
    /// Whether a shutdown without close_notify has written the records rustls held.
    shutdown_flushed: bool,
}

impl<T> OwnedTlsStream<T>
where
    T: TlsStreamAccess,
{
    pub(crate) fn new(tls: T) -> Self {
        Self {
            tls: Some(tls),
            write_remaining: 0,
            pending_plaintext: None,
            write_active: false,
            shutdown_close_notify: None,
            shutdown_flushed: false,
        }
    }

    fn closed_error() -> io::Error {
        io::Error::new(
            io::ErrorKind::BrokenPipe,
            "TLS stream is no longer writable",
        )
    }

    fn poison(&mut self) {
        let Some(mut tls) = self.tls.take() else {
            return;
        };
        tls.transport_mut().abort();
        drop(tls);
        self.pending_plaintext = None;
        self.write_remaining = 0;
        self.write_active = false;
    }

    fn record_plaintext_progress(&mut self, written: usize) -> io::Result<usize> {
        let Some(remaining) = self.write_remaining.checked_sub(written) else {
            self.poison();
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "TLS reported excess plaintext progress",
            ));
        };
        self.write_remaining = remaining;
        self.write_active = remaining != 0;
        if remaining == 0
            && let Some(tls) = &self.tls
        {
            tls.transport().mark_plaintext_complete();
        }
        Ok(written)
    }

    fn poll_pending_plaintext(&mut self, context: &mut Context<'_>) -> Poll<io::Result<usize>> {
        let written = self
            .pending_plaintext
            .expect("pending plaintext progress is retained until rustls output is staged");
        let Some(tls) = self.tls.as_mut() else {
            return Poll::Ready(Err(Self::closed_error()));
        };
        let previous = tls.transport().set_foreground_role(ForegroundRole::Write);
        let flush = Pin::new(&mut *tls).poll_flush(context);
        tls.transport().set_foreground_role(previous);
        match flush {
            Poll::Ready(Err(error)) => {
                self.poison();
                Poll::Ready(Err(error))
            }
            Poll::Ready(Ok(())) | Poll::Pending if !tls.wants_write() => {
                self.pending_plaintext = None;
                Poll::Ready(self.record_plaintext_progress(written))
            }
            Poll::Ready(Ok(())) => {
                self.poison();
                Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "rustls completed a flush with unstaged ciphertext",
                )))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    #[cfg(test)]
    pub(crate) fn is_poisoned_for_test(&self) -> bool {
        self.tls.is_none()
    }

    #[cfg(test)]
    pub(crate) fn poll_ciphertext_background_for_test(
        &self,
        context: &mut Context<'_>,
    ) -> Poll<()> {
        let Some(tls) = &self.tls else {
            return Poll::Ready(());
        };
        tls.transport().poll_background_for_test(context)
    }

    #[cfg(test)]
    pub(crate) fn pending_ciphertext_segments_for_test(&self) -> Vec<(usize, Vec<u8>)> {
        self.tls
            .as_ref()
            .map_or_else(Vec::new, |tls| tls.transport().pending_segments_for_test())
    }
}

impl<T> AsyncRead for OwnedTlsStream<T>
where
    T: TlsStreamAccess,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let Some(tls) = self.tls.as_mut() else {
            return Poll::Ready(Err(Self::closed_error()));
        };
        let previous = tls.transport().set_foreground_role(ForegroundRole::Read);
        let result = Pin::new(&mut *tls).poll_read(context, buffer);
        tls.transport().set_foreground_role(previous);
        result
    }
}

impl<T> AsyncWrite for OwnedTlsStream<T>
where
    T: TlsStreamAccess,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let Some(tls) = self.tls.as_mut() else {
            return Poll::Ready(Err(Self::closed_error()));
        };
        let previous = tls.transport().set_foreground_role(ForegroundRole::Write);
        let result = Pin::new(&mut *tls).poll_write(context, buffer);
        tls.transport().set_foreground_role(previous);
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let Some(tls) = self.tls.as_mut() else {
            return Poll::Ready(Err(Self::closed_error()));
        };
        let previous = tls.transport().set_foreground_role(ForegroundRole::Barrier);
        let result = Pin::new(&mut *tls).poll_flush(context);
        tls.transport().set_foreground_role(previous);
        result
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = &mut *self;
        let Some(tls) = this.tls.as_mut() else {
            return Poll::Ready(Err(Self::closed_error()));
        };
        let close_notify = *this
            .shutdown_close_notify
            .get_or_insert_with(|| tls.transport().close_mode().close_notify());
        let previous = tls.transport().set_foreground_role(ForegroundRole::Barrier);
        let result = if close_notify {
            Pin::new(&mut *tls).poll_shutdown(context)
        } else {
            poll_shutdown_without_close_notify(tls, &mut this.shutdown_flushed, context)
        };
        tls.transport().set_foreground_role(previous);
        result
    }
}

/// Writes the records rustls holds and the staged ciphertext, then shuts the TCP stream down,
/// with no close_notify. As tokio-rustls's shutdown does, it treats an unconnected socket as shut
/// down.
fn poll_shutdown_without_close_notify<T: TlsStreamAccess>(
    tls: &mut T,
    flushed: &mut bool,
    context: &mut Context<'_>,
) -> Poll<io::Result<()>> {
    if !*flushed {
        std::task::ready!(Pin::new(&mut *tls).poll_flush(context))?;
        *flushed = true;
    }
    Poll::Ready(
        match std::task::ready!(Pin::new(tls.transport_mut()).poll_shutdown(context)) {
            Err(error) if error.kind() == io::ErrorKind::NotConnected => Ok(()),
            result => result,
        },
    )
}

impl<T> TokioByteStream for OwnedTlsStream<T>
where
    T: TlsStreamAccess,
{
    fn prepare_reset(&self) {
        if let Some(tls) = &self.tls {
            tls.transport().prepare_reset();
        }
    }

    fn prepare_write_timeout(&mut self, timeout: Duration) {
        if let Some(tls) = &self.tls {
            tls.transport().state.borrow_mut().next_timeout = Some(timeout);
        }
    }

    fn poll_read_owned(
        &mut self,
        context: &mut Context<'_>,
        maximum: usize,
        _allocation_size: usize,
        read: &mut OwnedRead,
    ) -> Poll<io::Result<bool>> {
        let Some(tls) = self.tls.as_mut() else {
            return Poll::Ready(Err(Self::closed_error()));
        };
        let previous = tls.transport().set_foreground_role(ForegroundRole::Read);
        let result = match Pin::new(&mut *tls).poll_fill_buf(context) {
            Poll::Ready(Ok(available)) => {
                let amount = maximum.min(available.len());
                read.copy_from_slice(&available[..amount]);
                Pin::new(&mut *tls).consume(amount);
                Poll::Ready(Ok(amount == 0))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        };
        tls.transport().set_foreground_role(previous);
        result
    }

    fn cancel_read(&mut self) {
        if let Some(tls) = &self.tls {
            tls.transport().clear_foreground_waker(ForegroundRole::Read);
        }
    }

    fn enable_zero_copy(&mut self, context: &WorkerContext, threshold: usize) -> io::Result<bool> {
        let Some(tls) = &self.tls else {
            return Err(Self::closed_error());
        };
        tls.transport().enable_zero_copy(context, threshold)
    }

    fn set_zero_copy_reenable_threshold(&mut self, threshold: usize) {
        if let Some(tls) = &self.tls {
            tls.transport().set_zero_copy_reenable_threshold(threshold);
        }
    }

    fn begin_owned_write(&mut self, length: usize) -> bool {
        if let Some(tls) = &self.tls {
            tls.transport().begin_plaintext_write();
        }
        self.write_remaining = length;
        self.pending_plaintext = None;
        self.write_active = length != 0;
        false
    }

    fn poll_write_owned(
        &mut self,
        context: &mut Context<'_>,
        data: &BufQueue,
        _zero_copy: bool,
    ) -> Poll<io::Result<usize>> {
        if self.pending_plaintext.is_some() {
            return self.poll_pending_plaintext(context);
        }
        let Some(tls) = self.tls.as_mut() else {
            return Poll::Ready(Err(Self::closed_error()));
        };
        let buffers = data.iovecs().take(1024).collect::<Vec<IoSlice<'_>>>();
        let previous = tls.transport().set_foreground_role(ForegroundRole::Write);
        let result = Pin::new(&mut *tls).poll_write_vectored(context, &buffers);
        tls.transport().set_foreground_role(previous);
        match result {
            Poll::Ready(Ok(0)) if !data.is_empty() => {
                self.poison();
                Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "TLS plaintext write made no progress",
                )))
            }
            Poll::Ready(Ok(written)) => {
                if tls.wants_write() {
                    self.pending_plaintext = Some(written);
                    self.poll_pending_plaintext(context)
                } else {
                    Poll::Ready(self.record_plaintext_progress(written))
                }
            }
            Poll::Ready(Err(error)) => {
                self.poison();
                Poll::Ready(Err(error))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn abandon_zero_copy(&mut self) {
        if let Some(tls) = self.tls.as_mut() {
            tls.transport_mut().abandon();
        }
    }

    fn cancel_owned_write(&mut self) {
        if self.write_active {
            self.poison();
        }
    }

    #[cfg(test)]
    fn zero_copy_owner_for_test(&self) -> Option<usize> {
        self.tls
            .as_ref()
            .and_then(|tls| tls.transport().zero_copy_owner_for_test())
    }

    #[cfg(test)]
    fn zero_copy_pending_for_test(&self) -> usize {
        self.tls
            .as_ref()
            .map_or(0, |tls| tls.transport().zero_copy_pending_for_test())
    }
}

struct NamePolicyVerifier {
    inner: Option<Arc<WebPkiServerVerifier>>,
    custom: Option<Arc<dyn TlsCertificateVerifier>>,
    signature_algorithms: WebPkiSupportedAlgorithms,
    policy: TlsServerNameValidation,
    log: Option<TlsServerNameLogFn>,
}

impl fmt::Debug for NamePolicyVerifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NamePolicyVerifier")
            .field("has_webpki", &self.inner.is_some())
            .field("has_custom", &self.custom.is_some())
            .field("policy", &self.policy)
            .field("has_log", &self.log.is_some())
            .finish()
    }
}

impl ServerCertVerifier for NamePolicyVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if let Some(verifier) = &self.custom {
            let mut certificate_chain = Vec::with_capacity(intermediates.len() + 1);
            certificate_chain.push(end_entity.as_ref());
            certificate_chain.extend(intermediates.iter().map(AsRef::as_ref));
            verifier
                .verify_server_certificate(server_name.to_str().as_ref(), &certificate_chain)
                .map_err(|error| rustls::Error::General(error.to_string()))?;
            if verifier.is_insecure() {
                return Ok(ServerCertVerified::assertion());
            }
            let certificate = ParsedCertificate::try_from(end_entity)?;
            return match verify_server_name(&certificate, server_name) {
                Ok(()) => {
                    if let Some(log) = &self.log {
                        log(TlsServerNameVerification::Success, b"");
                    }
                    Ok(ServerCertVerified::assertion())
                }
                Err(error) if self.policy != TlsServerNameValidation::Enforcing => {
                    if let Some(log) = &self.log {
                        log(
                            TlsServerNameVerification::SoftFail,
                            error.to_string().as_bytes(),
                        );
                    }
                    Ok(ServerCertVerified::assertion())
                }
                Err(error) => {
                    if let Some(log) = &self.log {
                        log(
                            TlsServerNameVerification::Fail,
                            error.to_string().as_bytes(),
                        );
                    }
                    Err(error)
                }
            };
        }
        match self
            .inner
            .as_ref()
            .expect("a non-custom verifier retains its WebPKI verifier")
            .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
        {
            Ok(verified) => {
                if let Some(log) = &self.log {
                    log(TlsServerNameVerification::Success, b"");
                }
                Ok(verified)
            }
            Err(error) if is_name_mismatch(&error) => {
                let message = error.to_string();
                if let Some(log) = &self.log {
                    log(
                        if self.policy == TlsServerNameValidation::Enforcing {
                            TlsServerNameVerification::Fail
                        } else {
                            TlsServerNameVerification::SoftFail
                        },
                        message.as_bytes(),
                    );
                }
                if self.policy == TlsServerNameValidation::Enforcing {
                    Err(error)
                } else {
                    Ok(ServerCertVerified::assertion())
                }
            }
            Err(error) => Err(error),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(message, certificate, signature, &self.signature_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(message, certificate, signature, &self.signature_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.signature_algorithms.supported_schemes()
    }
}

fn is_name_mismatch(error: &rustls::Error) -> bool {
    matches!(
        error,
        rustls::Error::InvalidCertificate(
            rustls::CertificateError::NotValidForName
                | rustls::CertificateError::NotValidForNameContext { .. }
        )
    )
}

/// Whether a TLS server requests a client certificate, and how it verifies one.
#[derive(Clone, Debug)]
pub enum TlsClientVerification {
    /// Sends no CertificateRequest.
    DoNotRequest,
    /// Requests a certificate. A client that sends none is accepted; a
    /// presented chain that does not verify fails the handshake.
    IfPresented(TlsClientTrust),
    /// Requests a certificate and fails the handshake unless one is presented
    /// and verifies.
    Always(TlsClientTrust),
}

/// A CA bundle that client certificates are verified against, with the
/// semantics of a trusted certificate store.
///
/// The bundle's self-signed certificates are the trust anchors: a certificate
/// is self-signed if it names itself as its issuer and the server's crypto
/// provider verifies its signature with its own public key. Every other bundle
/// certificate is an intermediate that can complete a client's chain, and its
/// validity period and constraints are checked like those of the
/// intermediates the client presents. The server's CertificateRequest names
/// the subject of every bundle certificate.
#[derive(Clone, Debug)]
pub struct TlsClientTrust {
    certificates: Arc<[CertificateDer<'static>]>,
}

impl TlsClientTrust {
    /// Uses the DER certificates in `certificates`. Fails if it is empty or
    /// holds a value whose certificate fields do not parse as DER. The
    /// contents of the Names, the validity, the AlgorithmIdentifiers and the
    /// extensions are not checked here. A certificate with issuer or subject
    /// unique identifiers also fails, as webpki cannot use it.
    pub fn from_der(certificates: Vec<Box<[u8]>>) -> Result<Self, HttpError> {
        if certificates.is_empty() {
            return Err(HttpError::with_message(
                HttpErrorCode::INADEQUATE_SECURITY,
                b"TLS client CA bundle contains no certificates",
            ));
        }
        let certificates = certificates
            .into_iter()
            .map(|certificate| {
                CertificateFields::read(&certificate).ok_or_else(unreadable_client_ca)?;
                Ok(CertificateDer::from(certificate.into_vec()))
            })
            .collect::<Result<_, HttpError>>()?;
        Ok(Self { certificates })
    }

    /// Uses the PEM certificates in the file at `path`. The file is read with
    /// blocking I/O, so call this while building settings, not on a worker.
    /// Fails if the file cannot be read or holds no certificates.
    pub fn from_pem_file(path: &Path) -> Result<Self, HttpError> {
        let pem = std::fs::read(path)
            .map_err(|error| tls_message_error(b"failed to read TLS client CA file: ", error))?;
        let certificates = rustls_pemfile::certs(&mut &*pem)
            .map(|certificate| certificate.map(|certificate| certificate.to_vec().into()))
            .collect::<Result<_, _>>()
            .map_err(|error| tls_message_error(b"invalid TLS client CA file: ", error))?;
        Self::from_der(certificates)
    }

    /// Splits the bundle into the certificates that `provider` verifies as
    /// self-signed, the anchors, and the intermediates.
    fn roles(&self, provider: &CryptoProvider) -> Result<ClientTrustRoles, HttpError> {
        let mut roles = ClientTrustRoles::default();
        for certificate in self.certificates.iter() {
            let fields = CertificateFields::read(certificate).ok_or_else(unreadable_client_ca)?;
            if fields.is_self_signed(&provider.signature_verification_algorithms) {
                roles.anchors.push(certificate.clone());
            } else {
                roles
                    .intermediate_subjects
                    .push(DistinguishedName::in_sequence(fields.subject));
                roles.intermediates.push(certificate.clone());
            }
        }
        Ok(roles)
    }
}

/// A client trust bundle's certificates in the roles one provider gives them.
#[derive(Default)]
struct ClientTrustRoles {
    anchors: Vec<CertificateDer<'static>>,
    intermediates: Vec<CertificateDer<'static>>,
    intermediate_subjects: Vec<DistinguishedName>,
}

impl ClientTrustRoles {
    /// Hashes the anchors and the intermediates each in sorted order, so that
    /// the bundle's order does not matter.
    fn digest(&self) -> TrustDigest {
        let sorted_sha256 = |certificates: &[CertificateDer<'static>]| {
            let mut sorted: Vec<&[u8]> = certificates.iter().map(AsRef::as_ref).collect();
            sorted.sort_unstable();
            certificates_sha256(sorted)
        };
        TrustDigest {
            anchors: sorted_sha256(&self.anchors),
            intermediates: sorted_sha256(&self.intermediates),
        }
    }

    fn into_verifier(
        self,
        provider: &Arc<CryptoProvider>,
        allow_unauthenticated: bool,
    ) -> Result<Arc<dyn ClientCertVerifier>, HttpError> {
        let mut anchors = RootCertStore::empty();
        for anchor in self.anchors {
            anchors
                .add(anchor)
                .map_err(|error| tls_message_error(b"invalid TLS client CA: ", error))?;
        }
        // The builder hints the anchors' subjects; adding the intermediates'
        // hints every bundle subject.
        let mut builder =
            WebPkiClientVerifier::builder_with_provider(Arc::new(anchors), Arc::clone(provider))
                .add_root_hint_subjects(self.intermediate_subjects);
        if allow_unauthenticated {
            builder = builder.allow_unauthenticated();
        }
        let inner = builder
            .build()
            .map_err(|error| tls_message_error(b"invalid TLS client verifier: ", error))?;
        Ok(Arc::new(BundleClientVerifier {
            inner,
            intermediates: self.intermediates,
        }))
    }
}

/// What a TLS server does when a client's ALPN protocols include none of its own.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TlsAlpnMismatch {
    /// Fails the handshake with a `no_application_protocol` alert.
    #[default]
    Reject,
    /// Completes the handshake without selecting a protocol. The server reads
    /// the first ClientHello before it picks a configuration: an offer that
    /// includes one of its protocols negotiates as under `Reject`, and an offer
    /// that includes none continues on a copy of the configuration that
    /// offers no protocols. The copy shares the resumption state, so a session
    /// resumes on either. A client that sends no ALPN gets no protocol under
    /// either choice. One handshake timeout covers both the ClientHello read
    /// and the rest of the handshake.
    ContinueWithoutAlpn,
}

/// Settings for [`TlsServerOptions::from_settings`]: the server's certificate
/// chain and key, and builder methods for its other choices.
///
/// By default the server offers no ALPN protocols, requests no client
/// certificate, and uses the installed process-default crypto provider, else
/// [`builtin_crypto_provider`]. Each set of options issues tickets from its own
/// ticketer on the built-in backend, as `TlsServerOptions::new` does, unless
/// the settings carry a [`TlsServerResumption`].
#[non_exhaustive]
pub struct TlsServerSettings {
    certificate_chain_der: Vec<Box<[u8]>>,
    private_key_der: Box<[u8]>,
    alpn_protocols: Vec<Box<[u8]>>,
    alpn_mismatch: TlsAlpnMismatch,
    client_verification: TlsClientVerification,
    crypto_provider: Option<Arc<CryptoProvider>>,
    resumption: Option<TlsServerResumption>,
}

impl TlsServerSettings {
    /// Takes the server's certificate chain, leaf first, and its private key,
    /// all DER.
    pub fn new(certificate_chain_der: Vec<Box<[u8]>>, private_key_der: Box<[u8]>) -> Self {
        Self {
            certificate_chain_der,
            private_key_der,
            alpn_protocols: Vec::new(),
            alpn_mismatch: TlsAlpnMismatch::Reject,
            client_verification: TlsClientVerification::DoNotRequest,
            crypto_provider: None,
            resumption: None,
        }
    }

    /// Sets the ALPN protocols in the server's order of preference, and what
    /// to do when a client's offer includes none of them (see
    /// [`TlsAlpnMismatch`]).
    pub fn with_alpn(mut self, protocols: Vec<Box<[u8]>>, mismatch: TlsAlpnMismatch) -> Self {
        self.alpn_protocols = protocols;
        self.alpn_mismatch = mismatch;
        self
    }

    /// Sets whether the server requests client certificates and how it
    /// verifies them.
    pub fn with_client_verification(mut self, verification: TlsClientVerification) -> Self {
        self.client_verification = verification;
        self
    }

    /// Sets the provider for the handshake and for verifying client
    /// certificates, instead of the installed process default, else
    /// [`builtin_crypto_provider`].
    pub fn with_crypto_provider(mut self, provider: Arc<CryptoProvider>) -> Self {
        self.crypto_provider = Some(provider);
        self
    }

    /// Uses `resumption`'s ticketer and session cache. Options built with
    /// clones of it share them while their settings keep the same client
    /// verification mode, trust bundle certificates and certificate chain.
    /// When one of those changes, the shared state is replaced with a new
    /// ticketer and an empty cache, so a session does not resume across a
    /// change of client authentication or server identity. The crypto
    /// provider is not compared: a caller that changes which signatures the
    /// provider verifies passes a new `TlsServerResumption`, which starts with
    /// fresh state.
    ///
    /// Without this, each [`TlsServerOptions::from_settings`] call makes its
    /// own ticketer on the built-in backend and a cache of 256 sessions.
    pub fn with_resumption(mut self, resumption: TlsServerResumption) -> Self {
        self.resumption = Some(resumption);
        self
    }
}

type TicketerFactory =
    Arc<dyn Fn() -> Result<Arc<dyn ProducesTickets>, rustls::Error> + Send + Sync>;

/// Session resumption state that TLS server options built with
/// [`TlsServerSettings::with_resumption`] share: a ticketer and an optional
/// session cache. Clones share one state.
#[derive(Clone)]
pub struct TlsServerResumption {
    make_ticketer: Option<TicketerFactory>,
    session_cache_capacity: Option<usize>,
    state: Arc<Mutex<ResumptionState>>,
}

impl TlsServerResumption {
    /// Issues tickets with the built-in backend's ticketer (see
    /// [`builtin_crypto_provider`]), and caches up to `session_cache_capacity`
    /// sessions for TLS 1.2 clients that resume by session ID. Fails if the
    /// backend cannot make the ticketer.
    pub fn new(session_cache_capacity: usize) -> Result<Self, HttpError> {
        Self::with_builtin_ticketer(Some(session_cache_capacity))
    }

    /// Issues tickets with the built-in backend's ticketer and keeps no
    /// session cache, so TLS 1.2 clients cannot resume by session ID. Fails if
    /// the backend cannot make the ticketer.
    pub fn tickets_only() -> Result<Self, HttpError> {
        Self::with_builtin_ticketer(None)
    }

    /// Issues no tickets and keeps no session cache, so sessions do not
    /// resume.
    pub fn disabled() -> Self {
        Self::unbound(None, None, Ok(None))
    }

    /// Issues tickets from `make_ticketer` instead of the built-in backend's
    /// ticketer, for example to keep them on the provider that runs the
    /// handshake. It is called once before this returns, and again by each
    /// [`TlsServerOptions::from_settings`] call that replaces the state
    /// because the settings changed. Each call must return a ticketer with new
    /// keys: returning the same ticketer again would let tickets resume across
    /// those changes. The returned value keeps the session cache choice but
    /// does not share state with `self`.
    ///
    /// If the first call fails, every `from_settings` call with the returned
    /// value fails, and `make_ticketer` is not called again. A later call that
    /// fails fails its `from_settings` call and leaves the state as it was.
    /// On a [`disabled`](Self::disabled) value, which issues no tickets, this
    /// returns `self` and does not call `make_ticketer`.
    pub fn with_ticketer(
        self,
        make_ticketer: Arc<
            dyn Fn() -> Result<Arc<dyn ProducesTickets>, rustls::Error> + Send + Sync,
        >,
    ) -> Self {
        if self.make_ticketer.is_none() {
            return self;
        }
        let first = make_ticketer().map(Some);
        Self::unbound(Some(make_ticketer), self.session_cache_capacity, first)
    }

    fn with_builtin_ticketer(session_cache_capacity: Option<usize>) -> Result<Self, HttpError> {
        let make_ticketer: TicketerFactory = Arc::new(backend::Ticketer::new);
        let first = make_ticketer().map_err(ticket_error)?;
        Ok(Self::unbound(
            Some(make_ticketer),
            session_cache_capacity,
            Ok(Some(first)),
        ))
    }

    fn unbound(
        make_ticketer: Option<TicketerFactory>,
        session_cache_capacity: Option<usize>,
        first_ticketer: Result<Option<Arc<dyn ProducesTickets>>, rustls::Error>,
    ) -> Self {
        Self {
            make_ticketer,
            session_cache_capacity,
            state: Arc::new(Mutex::new(ResumptionState::Unbound(first_ticketer))),
        }
    }

    /// Returns the state bound to `key`. The first call binds the unbound
    /// state; a call with another key replaces it, unless making the new
    /// ticketer fails. If making the first ticketer failed, every call returns
    /// that error.
    fn bind(&self, key: ResumptionKey) -> Result<BoundResumption, HttpError> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let bound = match &*state {
            ResumptionState::Unbound(Err(error)) => return Err(ticket_error(error)),
            ResumptionState::Unbound(Ok(ticketer)) => BoundResumption {
                key,
                ticketer: ticketer.clone(),
                sessions: self.new_sessions(),
            },
            ResumptionState::Bound(bound) if bound.key == key => return Ok(bound.clone()),
            ResumptionState::Bound(_) => BoundResumption {
                key,
                ticketer: self
                    .make_ticketer
                    .as_ref()
                    .map(|make_ticketer| make_ticketer())
                    .transpose()
                    .map_err(ticket_error)?,
                sessions: self.new_sessions(),
            },
        };
        *state = ResumptionState::Bound(bound.clone());
        Ok(bound)
    }

    fn new_sessions(&self) -> Arc<dyn StoresServerSessions> {
        match self.session_cache_capacity {
            Some(capacity) => ServerSessionMemoryCache::new(capacity),
            None => Arc::new(NoServerSessionStorage {}),
        }
    }
}

enum ResumptionState {
    /// Not used yet: the first ticketer, `None` without tickets, or the error
    /// from making it.
    Unbound(Result<Option<Arc<dyn ProducesTickets>>, rustls::Error>),
    Bound(BoundResumption),
}

#[derive(Clone)]
struct BoundResumption {
    key: ResumptionKey,
    ticketer: Option<Arc<dyn ProducesTickets>>,
    sessions: Arc<dyn StoresServerSessions>,
}

/// The settings that resumption state is bound to. rustls requires the
/// configurations that share a ticketer or session cache to authenticate
/// clients and present the server's identity in the same way, and does not
/// check it.
#[derive(Clone, Copy, Eq, PartialEq)]
struct ResumptionKey {
    client_verification: ClientVerificationKey,
    certificate_chain: [u8; 32],
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ClientVerificationKey {
    DoNotRequest,
    IfPresented(TrustDigest),
    Always(TrustDigest),
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct TrustDigest {
    anchors: [u8; 32],
    intermediates: [u8; 32],
}

/// SHA-256 over `certificates`, each preceded by its length as eight
/// big-endian octets.
fn certificates_sha256<'a>(certificates: impl IntoIterator<Item = &'a [u8]>) -> [u8; 32] {
    let mut context = backend::cipher_suite::TLS13_AES_128_GCM_SHA256
        .tls13()
        .expect("should be a TLS 1.3 cipher suite")
        .common
        .hash_provider
        .start();
    for certificate in certificates {
        context.update(&(certificate.len() as u64).to_be_bytes());
        context.update(certificate);
    }
    context
        .finish()
        .as_ref()
        .try_into()
        .expect("should be a 32-byte SHA-256 output")
}

fn ticket_error(error: impl fmt::Display) -> HttpError {
    tls_message_error(b"failed to initialize TLS tickets: ", error)
}

/// Runtime-neutral certificate and ALPN settings for accepted TLS streams.
#[derive(Clone)]
pub struct TlsServerOptions {
    config: Arc<ServerConfig>,
    /// The configuration for a client whose ALPN offer includes none of
    /// `config`'s protocols. Present only for
    /// [`TlsAlpnMismatch::ContinueWithoutAlpn`] with protocols to offer.
    config_without_alpn: Option<Arc<ServerConfig>>,
}

impl TlsServerOptions {
    /// Builds server options from `settings`. Fails if the certificate chain
    /// or key is invalid, if a client trust has no self-signed certificate
    /// that the provider verifies, or if a ticketer cannot be made. Under
    /// [`TlsAlpnMismatch::ContinueWithoutAlpn`] with protocols to offer, it
    /// also builds the configuration copy without ALPN, once per call rather
    /// than once per connection.
    pub fn from_settings(settings: TlsServerSettings) -> Result<Self, HttpError> {
        let provider = settings
            .crypto_provider
            .unwrap_or_else(fallback_crypto_provider);
        let certificate_chain_key =
            certificates_sha256(settings.certificate_chain_der.iter().map(AsRef::as_ref));
        let certificates = settings
            .certificate_chain_der
            .into_iter()
            .map(|certificate| CertificateDer::from(certificate.into_vec()))
            .collect();
        let private_key = PrivateKeyDer::try_from(settings.private_key_der.into_vec())
            .map_err(|error| tls_message_error(b"invalid TLS private key: ", error))?;
        let (verifier, client_verification_key) = match &settings.client_verification {
            TlsClientVerification::DoNotRequest => (
                WebPkiClientVerifier::no_client_auth(),
                ClientVerificationKey::DoNotRequest,
            ),
            TlsClientVerification::IfPresented(trust) => {
                let roles = trust.roles(&provider)?;
                let key = ClientVerificationKey::IfPresented(roles.digest());
                (roles.into_verifier(&provider, true)?, key)
            }
            TlsClientVerification::Always(trust) => {
                let roles = trust.roles(&provider)?;
                let key = ClientVerificationKey::Always(roles.digest());
                (roles.into_verifier(&provider, false)?, key)
            }
        };
        let mut config = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|error| tls_message_error(b"invalid TLS server provider: ", error))?
            .with_client_cert_verifier(verifier)
            .with_single_cert(certificates, private_key)
            .map_err(|error| tls_message_error(b"invalid TLS server certificate: ", error))?;
        config.alpn_protocols = settings
            .alpn_protocols
            .into_iter()
            .map(|protocol| protocol.into_vec())
            .collect();
        match &settings.resumption {
            Some(resumption) => {
                let bound = resumption.bind(ResumptionKey {
                    client_verification: client_verification_key,
                    certificate_chain: certificate_chain_key,
                })?;
                if let Some(ticketer) = bound.ticketer {
                    config.ticketer = ticketer;
                }
                config.session_storage = bound.sessions;
            }
            None => {
                config.ticketer = backend::Ticketer::new().map_err(ticket_error)?;
            }
        }
        let continue_without_alpn = settings.alpn_mismatch == TlsAlpnMismatch::ContinueWithoutAlpn
            && !config.alpn_protocols.is_empty();
        let mut options = Self {
            config: Arc::new(config),
            config_without_alpn: None,
        };
        if continue_without_alpn {
            options.config_without_alpn = Some(options.without_alpn().config);
        }
        Ok(options)
    }

    /// A copy of these options that offers no ALPN protocols. It shares their
    /// ticketer and session cache, so a session resumes on either.
    pub(crate) fn without_alpn(&self) -> Self {
        let mut config = ServerConfig::clone(&self.config);
        config.alpn_protocols.clear();
        Self {
            config: Arc::new(config),
            config_without_alpn: None,
        }
    }

    /// Uses a complete rustls server configuration, including authentication, ALPN and tickets.
    pub fn from_rustls_config(config: ServerConfig) -> Self {
        Self {
            config: Arc::new(config),
            config_without_alpn: None,
        }
    }

    pub fn new(
        certificate_chain_der: Vec<Box<[u8]>>,
        private_key_der: Box<[u8]>,
        alpn_protocols: Vec<Box<[u8]>>,
    ) -> Result<Self, HttpError> {
        let certificates = certificate_chain_der
            .into_iter()
            .map(|certificate| CertificateDer::from(certificate.into_vec()))
            .collect();
        let private_key = PrivateKeyDer::try_from(private_key_der.into_vec())
            .map_err(|error| tls_message_error(b"invalid TLS private key: ", error))?;
        let mut config = ServerConfig::builder_with_provider(fallback_crypto_provider())
            .with_safe_default_protocol_versions()
            .map_err(|error| tls_message_error(b"invalid TLS server provider: ", error))?
            .with_no_client_auth()
            .with_single_cert(certificates, private_key)
            .map_err(|error| tls_message_error(b"invalid TLS server certificate: ", error))?;
        config.ticketer = backend::Ticketer::new()
            .map_err(|error| tls_message_error(b"failed to initialize TLS tickets: ", error))?;
        config.alpn_protocols = alpn_protocols
            .into_iter()
            .map(|protocol| protocol.into_vec())
            .collect();
        Ok(Self {
            config: Arc::new(config),
            config_without_alpn: None,
        })
    }

    pub fn new_with_client_auth(
        certificate_chain_der: Vec<Box<[u8]>>,
        private_key_der: Box<[u8]>,
        alpn_protocols: Vec<Box<[u8]>>,
        client_ca_certificates_der: Vec<Box<[u8]>>,
    ) -> Result<Self, HttpError> {
        let provider = fallback_crypto_provider();
        let certificates = certificate_chain_der
            .into_iter()
            .map(|certificate| CertificateDer::from(certificate.into_vec()))
            .collect();
        let private_key = PrivateKeyDer::try_from(private_key_der.into_vec())
            .map_err(|error| tls_message_error(b"invalid TLS private key: ", error))?;
        let mut client_roots = RootCertStore::empty();
        for certificate in client_ca_certificates_der {
            client_roots
                .add(CertificateDer::from(certificate.into_vec()))
                .map_err(|error| tls_message_error(b"invalid TLS client CA: ", error))?;
        }
        let verifier = WebPkiClientVerifier::builder_with_provider(
            Arc::new(client_roots),
            Arc::clone(&provider),
        )
        .build()
        .map_err(|error| tls_message_error(b"invalid TLS client verifier: ", error))?;
        let mut config = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|error| tls_message_error(b"invalid TLS server provider: ", error))?
            .with_client_cert_verifier(verifier)
            .with_single_cert(certificates, private_key)
            .map_err(|error| tls_message_error(b"invalid TLS server certificate: ", error))?;
        config.ticketer = backend::Ticketer::new()
            .map_err(|error| tls_message_error(b"failed to initialize TLS tickets: ", error))?;
        config.alpn_protocols = alpn_protocols
            .into_iter()
            .map(|protocol| protocol.into_vec())
            .collect();
        Ok(Self {
            config: Arc::new(config),
            config_without_alpn: None,
        })
    }
}

fn unreadable_client_ca() -> HttpError {
    HttpError::with_message(
        HttpErrorCode::INADEQUATE_SECURITY,
        b"invalid TLS client CA: not a DER certificate",
    )
}

/// Verifies client certificates against a bundle's anchors, with the bundle's
/// other certificates as path-building intermediates, so that their validity
/// is checked like that of the intermediates a client presents.
#[derive(Debug)]
struct BundleClientVerifier {
    inner: Arc<dyn ClientCertVerifier>,
    intermediates: Vec<CertificateDer<'static>>,
}

impl ClientCertVerifier for BundleClientVerifier {
    fn offer_client_auth(&self) -> bool {
        self.inner.offer_client_auth()
    }

    fn client_auth_mandatory(&self) -> bool {
        self.inner.client_auth_mandatory()
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.inner.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        let candidates: Vec<_> = intermediates
            .iter()
            .chain(&self.intermediates)
            .map(|certificate| CertificateDer::from(certificate.as_ref()))
            .collect();
        self.inner.verify_client_cert(end_entity, &candidates, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner
            .verify_tls12_signature(message, certificate, signature)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner
            .verify_tls13_signature(message, certificate, signature)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// The parts of a DER certificate that the bundle split reads.
struct CertificateFields<'a> {
    /// The whole TBSCertificate encoding, which the signature covers.
    tbs_certificate: &'a [u8],
    /// The contents of the issuer Name.
    issuer: &'a [u8],
    /// The contents of the subject Name.
    subject: &'a [u8],
    /// The contents of the subject public key's AlgorithmIdentifier.
    public_key_algorithm: &'a [u8],
    /// The subject public key, without its BIT STRING's unused-bits octet.
    public_key: &'a [u8],
    /// The contents of the certificate's signatureAlgorithm.
    signature_algorithm: &'a [u8],
    /// The signature, without its BIT STRING's unused-bits octet.
    signature: &'a [u8],
}

impl<'a> CertificateFields<'a> {
    /// Reads `der`, or returns `None` if it is not a certificate. Every length
    /// must use the shortest form and fit its enclosing element, and the
    /// certificate and its version, subject public key info and extensions
    /// must hold nothing else. Names, the validity and the extension contents
    /// are left to webpki.
    fn read(der: &'a [u8]) -> Option<Self> {
        let certificate = der_only_element(der, DER_SEQUENCE)?;
        let (tbs, rest) = der_element(certificate, DER_SEQUENCE)?;
        let tbs_certificate = &certificate[..certificate.len() - rest.len()];
        let (signature_algorithm, rest) = der_element(rest, DER_SEQUENCE)?;
        let signature = der_only_element(rest, DER_BIT_STRING)?;

        let tbs = match tbs.first() {
            Some(&DER_EXPLICIT_VERSION) => {
                let (version, rest) = der_element(tbs, DER_EXPLICIT_VERSION)?;
                der_only_element(version, DER_INTEGER)?;
                rest
            }
            _ => tbs,
        };
        let (_serial_number, rest) = der_element(tbs, DER_INTEGER)?;
        let (_tbs_signature_algorithm, rest) = der_element(rest, DER_SEQUENCE)?;
        let (issuer, rest) = der_element(rest, DER_SEQUENCE)?;
        let (_validity, rest) = der_element(rest, DER_SEQUENCE)?;
        let (subject, rest) = der_element(rest, DER_SEQUENCE)?;
        let (public_key_info, extensions) = der_element(rest, DER_SEQUENCE)?;
        // Only the extensions may follow: like webpki, the walk accepts no
        // unique identifiers.
        if !extensions.is_empty() {
            der_only_element(
                der_only_element(extensions, DER_EXPLICIT_EXTENSIONS)?,
                DER_SEQUENCE,
            )?;
        }
        let (public_key_algorithm, rest) = der_element(public_key_info, DER_SEQUENCE)?;
        let public_key = der_only_element(rest, DER_BIT_STRING)?;
        Some(Self {
            tbs_certificate,
            issuer,
            subject,
            public_key_algorithm,
            public_key: bit_string_bytes(public_key)?,
            signature_algorithm,
            signature: bit_string_bytes(signature)?,
        })
    }

    /// Whether the certificate is self-signed: it names itself as its issuer,
    /// and one of `algorithms` verifies its signature with its own public key,
    /// matching both AlgorithmIdentifiers as webpki does.
    fn is_self_signed(&self, algorithms: &WebPkiSupportedAlgorithms) -> bool {
        self.issuer == self.subject
            && algorithms.all.iter().any(|algorithm| {
                algorithm.public_key_alg_id().as_ref() == self.public_key_algorithm
                    && algorithm.signature_alg_id().as_ref() == self.signature_algorithm
                    && algorithm
                        .verify_signature(self.public_key, self.tbs_certificate, self.signature)
                        .is_ok()
            })
    }
}

const DER_INTEGER: u8 = 0x02;
const DER_BIT_STRING: u8 = 0x03;
const DER_SEQUENCE: u8 = 0x30;
/// The tag of the TBSCertificate's optional `[0] EXPLICIT` version.
const DER_EXPLICIT_VERSION: u8 = 0xa0;
/// The tag of the TBSCertificate's optional `[3] EXPLICIT` extensions.
const DER_EXPLICIT_EXTENSIONS: u8 = 0xa3;

/// Splits the DER element tagged `tag` off the front of `input`, returning its
/// contents and the bytes after it, or `None` if `input` does not start with
/// such an element.
fn der_element(input: &[u8], tag: u8) -> Option<(&[u8], &[u8])> {
    let (&actual, rest) = input.split_first()?;
    if actual != tag {
        return None;
    }
    let (&first, rest) = rest.split_first()?;
    let (length, rest) = if first < 0x80 {
        (usize::from(first), rest)
    } else {
        // The long form: the low bits count the big-endian length octets.
        let count = usize::from(first & 0x7f);
        if count == 0 || count > 4 {
            return None;
        }
        let (octets, rest) = rest.split_at_checked(count)?;
        let length = octets
            .iter()
            .fold(0, |length, octet| (length << 8) | usize::from(*octet));
        // DER uses the long form only for lengths of 128 or more, with no
        // leading zero octet.
        if length < 0x80 || octets.first() == Some(&0) {
            return None;
        }
        (length, rest)
    };
    rest.split_at_checked(length)
}

/// Returns the contents of the DER element tagged `tag` that makes up all of
/// `input`, or `None` if `input` is not exactly such an element.
fn der_only_element(input: &[u8], tag: u8) -> Option<&[u8]> {
    match der_element(input, tag)? {
        (contents, []) => Some(contents),
        _ => None,
    }
}

/// Returns the bytes of a BIT STRING's contents, or `None` unless it has no
/// unused bits.
fn bit_string_bytes(contents: &[u8]) -> Option<&[u8]> {
    match contents.split_first()? {
        (0, bytes) => Some(bytes),
        _ => None,
    }
}

/// Negotiated TLS metadata, represented without rustls-owned public types.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TlsConnectionInfo {
    requested_server_name: Option<Box<str>>,
    application_protocol: Option<Box<[u8]>>,
    cipher_suite: Option<Box<str>>,
    key_exchange_group: Option<Box<str>>,
    protocol_version: Option<Box<str>>,
    protocol_version_number: Option<u16>,
    session_resumed: bool,
}

impl TlsConnectionInfo {
    /// Returns SNI observed by the server during the completed handshake.
    pub fn requested_server_name(&self) -> Option<&str> {
        self.requested_server_name.as_deref()
    }

    pub fn application_protocol(&self) -> Option<&[u8]> {
        self.application_protocol.as_deref()
    }

    pub fn cipher_suite(&self) -> Option<&str> {
        self.cipher_suite.as_deref()
    }

    /// Returns the negotiated key-exchange group, or `None` when the
    /// handshake did not exchange a key, as in a resumed TLS 1.2 session.
    pub fn key_exchange_group(&self) -> Option<&str> {
        self.key_exchange_group.as_deref()
    }

    pub fn protocol_version(&self) -> Option<&str> {
        self.protocol_version.as_deref()
    }

    pub const fn session_resumed(&self) -> bool {
        self.session_resumed
    }

    /// Returns the negotiated protocol version's wire value and whether the
    /// session was resumed, or `None` if no version was negotiated.
    pub fn session_facts(&self) -> Option<TlsSessionFacts> {
        self.protocol_version_number
            .map(|version| TlsSessionFacts::new(version, self.session_resumed))
    }
}

/// An `OwnedByteStream` plus immutable TLS handshake metadata.
pub struct TlsTransport {
    stream: OwnedByteStream,
    connection_info: TlsConnectionInfo,
    close: Rc<TlsCloseMode>,
}

impl TlsTransport {
    /// Connects TCP, performs a verified TLS handshake, and binds all I/O to
    /// `context`. The adapter owns a ciphertext-drain task, never a session task.
    pub fn connect(
        context: &WorkerContext,
        address: SocketAddress,
        options: TlsClientOptions,
        limits: ByteStreamLimits,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<Self, HttpError>> {
        Self::connect_with_tcp_options(
            context,
            address,
            options,
            limits,
            timeout,
            cancellation,
            TcpConnectOptions::default(),
        )
    }

    pub fn connect_with_tcp_options(
        context: &WorkerContext,
        address: SocketAddress,
        options: TlsClientOptions,
        limits: ByteStreamLimits,
        timeout: Duration,
        cancellation: CancellationToken,
        tcp_options: TcpConnectOptions,
    ) -> LocalTransportFuture<Result<Self, HttpError>> {
        if !context.is_owner_thread() {
            return Box::pin(std::future::ready(Err(HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"TLS connect requires its worker owner thread",
            ))));
        }
        if context.handle().state() != WorkerState::Open {
            return Box::pin(std::future::ready(Err(dropped_error())));
        }
        let setup_context = context.clone();
        let setup = async move {
            let congestion_flavor = tcp_options.congestion_flavor.clone();
            let options = prepare_client_options(options).await?;
            let (config, server_name, identity_verifier) = build_client_config(options)?;
            let deadline = ConnectDeadline::new(timeout);
            let connected =
                connect_socket_with_options(address, &deadline, cancellation.clone(), tcp_options)
                    .await?;
            set_congestion_flavor_best_effort(&connected.stream, congestion_flavor.as_deref());
            let mut wire = OwnedCiphertextStream::new(AcceptedTcpStream::new(connected.stream));
            let close = wire.close_mode();
            wire.start_background(&setup_context)?;
            let tls = match await_handshake_with_deadline(
                TlsConnector::from(config).connect(server_name, wire),
                &deadline,
                cancellation,
            )
            .await
            {
                Ok(result) => result.map_err(tls_error)?,
                Err(AwaitError::Cancelled) => {
                    return Err(HttpError::with_message(
                        HttpErrorCode::CORO_CANCELLED,
                        b"TLS handshake cancelled",
                    ));
                }
                Err(AwaitError::TimedOut) => {
                    return Err(HttpError::with_message(
                        HttpErrorCode::READ_TIMEOUT,
                        b"TLS handshake timed out",
                    ));
                }
            };
            if setup_context.handle().state() != WorkerState::Open {
                return Err(dropped_error());
            }
            let connection_info = connection_info(&tls);
            let peer_certificate = tls
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|certificates| certificates.first());
            let peer_identity = match (peer_certificate, identity_verifier) {
                (Some(certificate), Some(verifier)) => Some(verifier.verify(certificate.as_ref())?),
                (Some(certificate), None) => Some(PeerIdentity::new(certificate.as_ref())),
                (None, _) => None,
            };
            let stream = owned_from_stream(
                &setup_context,
                OwnedTlsStream::new(tls),
                connected.local_address,
                connected.peer_address,
                peer_identity,
                limits,
            )?;
            Ok(Self {
                stream,
                connection_info,
                close,
            })
        };
        setup_future(context, setup)
            .unwrap_or_else(|error| Box::pin(std::future::ready(Err(error))))
    }

    /// Performs a server-side TLS handshake for a newly accepted TCP stream.
    pub fn accept(
        context: &WorkerContext,
        stream: std::net::TcpStream,
        options: TlsServerOptions,
        limits: ByteStreamLimits,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<Self, HttpError>> {
        if !context.is_owner_thread() {
            return Box::pin(std::future::ready(Err(HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"TLS accept requires its worker owner thread",
            ))));
        }
        if context.handle().state() != WorkerState::Open {
            return Box::pin(std::future::ready(Err(dropped_error())));
        }
        let setup_context = context.clone();
        let setup = async move {
            let (stream, local_address, peer_address) = prepare_accepted_stream(stream)?;
            let deadline = ConnectDeadline::new(timeout);
            let mut wire = OwnedCiphertextStream::new(stream);
            let close = wire.close_mode();
            wire.start_background(&setup_context)?;
            let handshake = async move {
                let Some(config_without_alpn) = options.config_without_alpn else {
                    return TlsAcceptor::from(options.config).accept(wire).await;
                };
                let start = LazyConfigAcceptor::new(Acceptor::default(), wire).await?;
                let config = if offers_no_server_protocol(
                    start.client_hello().alpn(),
                    &options.config.alpn_protocols,
                ) {
                    config_without_alpn
                } else {
                    options.config
                };
                start.into_stream(config).await
            };
            let tls = match await_handshake_with_deadline(handshake, &deadline, cancellation).await
            {
                Ok(result) => result.map_err(tls_error)?,
                Err(AwaitError::Cancelled) => {
                    return Err(HttpError::with_message(
                        HttpErrorCode::CORO_CANCELLED,
                        b"TLS handshake cancelled",
                    ));
                }
                Err(AwaitError::TimedOut) => {
                    return Err(HttpError::with_message(
                        HttpErrorCode::READ_TIMEOUT,
                        b"TLS handshake timed out",
                    ));
                }
            };
            if setup_context.handle().state() != WorkerState::Open {
                return Err(dropped_error());
            }
            let connection = tls.get_ref().1;
            let connection_info = TlsConnectionInfo {
                requested_server_name: connection.server_name().map(Box::from),
                application_protocol: connection
                    .alpn_protocol()
                    .map(|protocol| protocol.to_vec().into_boxed_slice()),
                cipher_suite: connection
                    .negotiated_cipher_suite()
                    .map(|suite| format!("{:?}", suite.suite()).into_boxed_str()),
                key_exchange_group: connection
                    .negotiated_key_exchange_group()
                    .map(|group| format!("{:?}", group.name()).into_boxed_str()),
                protocol_version: connection
                    .protocol_version()
                    .map(|version| format!("{version:?}").into_boxed_str()),
                protocol_version_number: connection.protocol_version().map(u16::from),
                session_resumed: matches!(
                    connection.handshake_kind(),
                    Some(HandshakeKind::Resumed)
                ),
            };
            let peer_identity = connection
                .peer_certificates()
                .and_then(|certificates| certificates.first())
                .map(|certificate| PeerIdentity::new(certificate.as_ref()));
            let stream = owned_from_stream(
                &setup_context,
                OwnedTlsStream::new(tls),
                local_address,
                peer_address,
                peer_identity,
                limits,
            )?;
            Ok(Self {
                stream,
                connection_info,
                close,
            })
        };
        setup_future(context, setup)
            .unwrap_or_else(|error| Box::pin(std::future::ready(Err(error))))
    }

    pub fn connection_info(&self) -> &TlsConnectionInfo {
        &self.connection_info
    }

    pub fn enable_zero_copy(
        &self,
        context: &WorkerContext,
        threshold: usize,
    ) -> Result<bool, HttpError> {
        self.stream.enable_zero_copy(context, threshold)
    }

    #[cfg(test)]
    pub(crate) fn zero_copy_owner_for_test(
        &self,
        context: &WorkerContext,
    ) -> Result<Option<usize>, HttpError> {
        self.stream.zero_copy_owner_for_test(context)
    }

    pub fn into_byte_stream(self) -> OwnedByteStream {
        self.stream
    }

    /// The control of how this connection's graceful close ends.
    pub fn close_control(&self) -> TlsCloseControl {
        TlsCloseControl(Rc::clone(&self.close))
    }

    /// Makes every later reset of this connection close its socket without `SO_LINGER` zero,
    /// as `OwnedByteStream::enable_non_abortive_close` does, and without close_notify. This
    /// includes the resets that the ciphertext writer's own failures cause (a socket write error
    /// or a missed write deadline), an abandoned plaintext write, and the worker's teardown.
    pub fn enable_non_abortive_close(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.stream.enable_non_abortive_close(context)?;
        self.close.non_abortive.set(true);
        Ok(())
    }

    /// Returns an observer that reports when the I/O value and close future have
    /// been dropped. It does not report whether TLS shutdown succeeded.
    pub fn retirement(&self) -> Option<TransportRetirement> {
        self.stream.retirement()
    }
}

/// Whether a ClientHello's ALPN offer includes none of `server_protocols`, by
/// the exact byte comparison that rustls makes, so that a configuration
/// offering them would fail the handshake with `no_application_protocol`. A
/// ClientHello without the ALPN extension gets no protocol and no alert.
fn offers_no_server_protocol<'a>(
    offered: Option<impl Iterator<Item = &'a [u8]>>,
    server_protocols: &[Vec<u8>],
) -> bool {
    offered.is_some_and(|mut offered| {
        !offered.any(|protocol| {
            server_protocols
                .iter()
                .any(|server_protocol| server_protocol.as_slice() == protocol)
        })
    })
}

pub(crate) async fn await_handshake_with_deadline<F>(
    future: F,
    deadline: &ConnectDeadline,
    cancellation: CancellationToken,
) -> Result<F::Output, AwaitError>
where
    F: Future,
{
    await_with_deadline(future, deadline, cancellation).await
}

impl ByteStreamTransport for TlsTransport {
    fn read(
        &self,
        context: &WorkerContext,
        maximum: usize,
        allocation_size: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<TransportRead, HttpError>> {
        self.stream
            .read(context, maximum, allocation_size, timeout, cancellation)
    }

    fn write(
        &self,
        context: &WorkerContext,
        data: BufQueue,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        self.stream.write(context, data, timeout, cancellation)
    }

    fn write_with_byte_events(
        &self,
        context: &WorkerContext,
        data: BufQueue,
        flags: ByteEventWriteFlags,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        self.stream
            .write_with_byte_events(context, data, flags, timeout, cancellation)
    }

    fn shutdown_write(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.stream.shutdown_write(context)
    }

    fn close(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.stream.close(context)
    }

    fn close_with_reset(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.stream.close_with_reset(context)
    }

    fn local_address(&self, context: &WorkerContext) -> Result<SocketAddress, HttpError> {
        self.stream.local_address(context)
    }

    fn peer_address(&self, context: &WorkerContext) -> Result<SocketAddress, HttpError> {
        self.stream.peer_address(context)
    }

    fn peer_identity(&self, context: &WorkerContext) -> Result<Option<PeerIdentity>, HttpError> {
        self.stream.peer_identity(context)
    }

    fn byte_event_capability(
        &self,
        context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError> {
        self.stream.byte_event_capability(context)
    }

    fn register_byte_event(
        &self,
        context: &WorkerContext,
        registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError> {
        self.stream.register_byte_event(context, registration)
    }
}

fn build_client_config(
    options: TlsClientOptions,
) -> Result<
    (
        Arc<ClientConfig>,
        ServerName<'static>,
        Option<Rc<dyn TlsPeerIdentityVerifier>>,
    ),
    HttpError,
> {
    let server_name_text: String = options.server_name.clone().into();
    let server_name = ServerName::try_from(server_name_text)
        .map_err(|error| tls_message_error(b"invalid TLS server name: ", error))?;
    let config = if let Some(config) = options.config.borrow().as_ref() {
        Arc::clone(config)
    } else {
        let provider = options
            .crypto_provider
            .unwrap_or_else(fallback_crypto_provider);
        let signature_algorithms = provider.signature_verification_algorithms;
        let verifier = if options.certificate_verifier.is_none() {
            let mut roots = RootCertStore::empty();
            if options.use_native_roots {
                let native = rustls_native_certs::load_native_certs();
                roots.add_parsable_certificates(native.certs);
            }
            for certificate in &options.root_certificates {
                roots
                    .add(CertificateDer::from(certificate.to_vec()))
                    .map_err(|error| tls_message_error(b"invalid TLS root certificate: ", error))?;
            }
            if roots.is_empty() {
                return Err(HttpError::with_message(
                    HttpErrorCode::INADEQUATE_SECURITY,
                    b"no TLS root certificates are available",
                ));
            }
            Some(
                WebPkiServerVerifier::builder_with_provider(Arc::new(roots), Arc::clone(&provider))
                    .build()
                    .map_err(|error| {
                        tls_message_error(b"invalid TLS verifier configuration: ", error)
                    })?,
            )
        } else {
            None
        };
        let versions: &[_] = match options.minimum_version {
            TlsVersion::Tls12 => &[&rustls::version::TLS13, &rustls::version::TLS12],
            TlsVersion::Tls13 => &[&rustls::version::TLS13],
        };
        let builder = ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(versions)
            .map_err(|error| tls_message_error(b"invalid TLS client provider: ", error))?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NamePolicyVerifier {
                inner: verifier,
                custom: options.certificate_verifier.clone(),
                signature_algorithms,
                policy: options.server_name_validation,
                log: options.server_name_log.clone(),
            }));
        let mut config = match (
            options.client_certificate_pem.as_deref(),
            options.client_private_key_pem.as_deref(),
        ) {
            (None, None) => builder.with_no_client_auth(),
            (Some(certificate_pem), Some(private_key_pem)) => {
                let certificates = rustls_pemfile::certs(&mut &*certificate_pem)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| {
                        tls_message_error(b"invalid TLS client certificate: ", error)
                    })?;
                let private_key = rustls_pemfile::private_key(&mut &*private_key_pem)
                    .map_err(|error| tls_message_error(b"invalid TLS client private key: ", error))?
                    .ok_or_else(|| {
                        HttpError::with_message(
                            HttpErrorCode::INADEQUATE_SECURITY,
                            b"TLS client private key file contains no private key",
                        )
                    })?;
                builder
                    .with_client_auth_cert(certificates, private_key)
                    .map_err(|error| tls_message_error(b"invalid TLS client identity: ", error))?
            }
            _ => {
                return Err(HttpError::with_message(
                    HttpErrorCode::INADEQUATE_SECURITY,
                    b"TLS client certificate and private key must be configured together",
                ));
            }
        };
        config.alpn_protocols = options
            .alpn_protocols
            .iter()
            .map(|protocol| protocol.to_vec())
            .collect();
        config.enable_early_data = options.early_data;
        config.resumption = if options.session_caching {
            let store = options
                .session_store
                .expect("enabled TLS session caching retains a session store");
            let store: Arc<dyn ClientSessionStore> = match options.session_cache_identity {
                Some(identity) => Arc::new(IdentityMappedSessionStore {
                    inner: store,
                    identity: session_store_name(&identity),
                }),
                None => store,
            };
            Resumption::store(store)
        } else {
            Resumption::disabled()
        };
        let config = Arc::new(config);
        options.config.replace(Some(Arc::clone(&config)));
        config
    };
    Ok((config, server_name, options.identity_verifier))
}

/// Returns the default crypto provider of the built-in backend, which TLS
/// options use when they have no provider of their own and none is installed
/// for the process. The backend, which also makes the servers' session
/// ticketers, is aws-lc-rs when the `aws-lc-rs` feature is on, as it is by
/// default, even if `ring` is on too, and ring when only `ring` is on. The
/// aws-lc-rs provider offers the post-quantum X25519MLKEM768 key exchange
/// first; ring's does not have it.
pub fn builtin_crypto_provider() -> CryptoProvider {
    backend::default_provider()
}

/// Returns the crypto provider that TLS options use when they have none of
/// their own: the process-default provider if one is installed, else
/// [`builtin_crypto_provider`]. It does not install either as the process
/// default.
pub fn fallback_crypto_provider() -> Arc<CryptoProvider> {
    CryptoProvider::get_default()
        .map(Arc::clone)
        .unwrap_or_else(|| Arc::new(builtin_crypto_provider()))
}

fn session_store_name(identity: &str) -> ServerName<'static> {
    let mut hasher = DefaultHasher::new();
    identity.hash(&mut hasher);
    ServerName::try_from(format!("psk-{:016x}.roxygen.invalid", hasher.finish()))
        .expect("the fixed-format session cache identity is a valid DNS name")
}

async fn prepare_client_options(
    mut options: TlsClientOptions,
) -> Result<TlsClientOptions, HttpError> {
    if options.config.borrow().is_some()
        || !options.root_certificates.is_empty()
        || options.certificate_verifier.is_some()
    {
        return Ok(options);
    }
    let paths = if options.ca_paths.is_empty() {
        select_default_ca_path(
            std::env::var_os("PROXYGEN_CORO_CA_PATH"),
            std::env::var_os("CURL_CA_BUNDLE"),
            get_fallback_ca_path(),
        )
        .into_iter()
        .collect()
    } else {
        std::mem::take(&mut options.ca_paths)
    };
    if paths.is_empty() {
        return Ok(options);
    }
    let mut pending: std::collections::VecDeque<_> = paths.into_iter().collect();
    while let Some(path) = pending.pop_front() {
        let pem = tokio::fs::read(&path)
            .await
            .map_err(|error| tls_message_error(b"failed to read TLS CA file: ", error))?;
        let certificates = rustls_pemfile::certs(&mut &*pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| tls_message_error(b"invalid TLS CA file: ", error))?;
        if certificates.is_empty() {
            return Err(HttpError::with_message(
                HttpErrorCode::INADEQUATE_SECURITY,
                b"TLS CA file contains no certificates",
            ));
        }
        options.root_certificates.extend(
            certificates
                .into_iter()
                .map(|certificate| certificate.as_ref().into()),
        );
    }
    options.use_native_roots = false;
    Ok(options)
}

fn select_default_ca_path(
    proxygen_path: Option<std::ffi::OsString>,
    curl_path: Option<std::ffi::OsString>,
    fallback_path: Option<&str>,
) -> Option<PathBuf> {
    proxygen_path
        .filter(|path| !path.is_empty())
        .or_else(|| curl_path.filter(|path| !path.is_empty()))
        .map(PathBuf::from)
        .or_else(|| {
            fallback_path
                .filter(|path| !path.is_empty())
                .map(PathBuf::from)
        })
}

fn connection_info(
    tls: &ClientTlsStream<OwnedCiphertextStream<AcceptedTcpStream>>,
) -> TlsConnectionInfo {
    let connection = tls.get_ref().1;
    TlsConnectionInfo {
        requested_server_name: None,
        application_protocol: connection
            .alpn_protocol()
            .map(|protocol| protocol.to_vec().into_boxed_slice()),
        cipher_suite: connection
            .negotiated_cipher_suite()
            .map(|suite| format!("{:?}", suite.suite()).into_boxed_str()),
        key_exchange_group: connection
            .negotiated_key_exchange_group()
            .map(|group| format!("{:?}", group.name()).into_boxed_str()),
        protocol_version: connection
            .protocol_version()
            .map(|version| format!("{version:?}").into_boxed_str()),
        protocol_version_number: connection.protocol_version().map(u16::from),
        session_resumed: matches!(connection.handshake_kind(), Some(HandshakeKind::Resumed)),
    }
}

/// Why a TLS handshake failed, as [`tls_handshake_failure_kind`] reports it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TlsHandshakeFailureKind {
    /// The peer presented no certificate where one is required, or rustls's
    /// certificate verification rejected it. On connect, a rejection by an
    /// application [`TlsCertificateVerifier`] reports `Protocol`, and a
    /// [`TlsPeerIdentityVerifier`] rejection is classified like any other
    /// error.
    Certificate,
    /// rustls rejected the handshake for another reason: bytes that are not
    /// TLS, a peer alert, or no shared protocol version or cipher suite.
    Protocol,
    /// The peer closed the connection before the handshake finished.
    Eof,
    /// Reading or writing the socket failed, for example after a reset.
    Io,
    /// The handshake missed its deadline.
    Timeout,
    /// The handshake was cancelled, or its worker stopped.
    Cancelled,
    /// Any other failure, such as a full worker command queue.
    Other,
}

/// Classifies an error returned by [`TlsTransport::accept`] or
/// [`TlsTransport::connect`].
///
/// A failed handshake keeps the I/O error rustls or the socket returned as
/// the error's [`source`](std::error::Error::source), so the kind does not
/// depend on the error's code or message. Errors the transport raises
/// itself are classified by code: `READ_TIMEOUT` is `Timeout`, and
/// `CORO_CANCELLED` and `DROPPED` are `Cancelled`.
///
/// `Certificate` covers only rustls's certificate verification errors. On
/// connect, an application [`TlsCertificateVerifier`] rejection reaches
/// rustls as a general error and reports `Protocol`. A
/// [`TlsPeerIdentityVerifier`] rejection is returned unchanged and is
/// classified like any other error: by an `io::Error` cause if the
/// application attached one, otherwise by its code.
pub fn tls_handshake_failure_kind(error: &HttpError) -> TlsHandshakeFailureKind {
    let Some(cause) = handshake_io_error(error) else {
        return match error.code() {
            HttpErrorCode::READ_TIMEOUT => TlsHandshakeFailureKind::Timeout,
            HttpErrorCode::CORO_CANCELLED | HttpErrorCode::DROPPED => {
                TlsHandshakeFailureKind::Cancelled
            }
            _ => TlsHandshakeFailureKind::Other,
        };
    };
    match rustls_error(cause) {
        Some(
            rustls::Error::InvalidCertificate(_)
            | rustls::Error::NoCertificatesPresented
            | rustls::Error::InvalidCertRevocationList(_),
        ) => TlsHandshakeFailureKind::Certificate,
        Some(_) => TlsHandshakeFailureKind::Protocol,
        None if cause.kind() == io::ErrorKind::UnexpectedEof => TlsHandshakeFailureKind::Eof,
        None => TlsHandshakeFailureKind::Io,
    }
}

/// Returns the rustls error behind a failed handshake, if there is one.
pub fn tls_handshake_rustls_error(error: &HttpError) -> Option<&rustls::Error> {
    rustls_error(handshake_io_error(error)?)
}

fn handshake_io_error(error: &HttpError) -> Option<&io::Error> {
    std::error::Error::source(error)?.downcast_ref::<io::Error>()
}

fn rustls_error(error: &io::Error) -> Option<&rustls::Error> {
    error.get_ref()?.downcast_ref::<rustls::Error>()
}

pub(crate) fn tls_error(error: std::io::Error) -> HttpError {
    HttpError::with_message(
        match error.kind() {
            std::io::ErrorKind::InvalidData | std::io::ErrorKind::InvalidInput => {
                HttpErrorCode::INADEQUATE_SECURITY
            }
            std::io::ErrorKind::UnexpectedEof => HttpErrorCode::TRANSPORT_EOF,
            std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::NotConnected => HttpErrorCode::TRANSPORT_READ_ERROR,
            _ => HttpErrorCode::CONNECT_ERROR,
        },
        error.to_string().as_bytes(),
    )
    .with_cause(Arc::new(error))
}

fn tls_message_error(prefix: &'static [u8], error: impl std::fmt::Display) -> HttpError {
    let mut message = prefix.to_vec();
    message.extend_from_slice(error.to_string().as_bytes());
    HttpError::with_message(HttpErrorCode::INADEQUATE_SECURITY, message)
}

fn dropped_error() -> HttpError {
    HttpError::with_message(HttpErrorCode::DROPPED, b"TLS transport owner was dropped")
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::io::Read;
    use std::path::PathBuf;
    use std::process::Command;
    use std::process::Stdio;
    use std::sync::Arc;
    use std::thread;
    use std::thread::JoinHandle;
    use std::time::Duration;
    use std::time::Instant;

    use rustls::CipherSuite;
    use rustls::NamedGroup;
    use rustls::crypto::CryptoProvider;

    use super::TlsClientOptions;
    use super::TlsServerOptions;
    use super::TlsServerSettings;
    use super::build_client_config;
    use super::builtin_crypto_provider;
    use super::fallback_crypto_provider;
    use super::select_default_ca_path;

    const PROVIDER_CHILD_MARKER: &str = "ROXYGEN_TLS_PROVIDER_CHILD";
    const PROVIDER_CHILD_RECEIPT: &str = "roxygen-tls-provider-child-complete";
    const PROVIDER_CHILD_BOUND: Duration = Duration::from_secs(60);

    fn provider_algorithms(provider: &CryptoProvider) -> (Vec<CipherSuite>, Vec<NamedGroup>) {
        (
            provider
                .cipher_suites
                .iter()
                .map(|suite| suite.suite())
                .collect(),
            provider
                .kx_groups
                .iter()
                .map(|group| group.name())
                .collect(),
        )
    }

    fn builtin_with_only(suite: CipherSuite) -> CryptoProvider {
        let mut provider = builtin_crypto_provider();
        provider
            .cipher_suites
            .retain(|candidate| candidate.suite() == suite);
        assert_eq!(
            provider.cipher_suites.len(),
            1,
            "the built-in provider includes {suite:?}",
        );
        provider
    }

    /// Returns the providers of a client, a server, a mutual-TLS server and a
    /// settings-built server config, built without an explicit provider.
    fn fallback_config_providers() -> [Arc<CryptoProvider>; 4] {
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("test certificate");
        let certificate = generated.cert.der().to_vec().into_boxed_slice();
        let private_key = generated
            .signing_key
            .serialized_der()
            .to_vec()
            .into_boxed_slice();
        let mut client_options = TlsClientOptions::new("localhost");
        client_options.add_root_certificate(&certificate);
        let client = build_client_config(client_options).unwrap().0;
        let server = TlsServerOptions::new(
            vec![certificate.clone()],
            private_key.clone(),
            vec![b"h2".to_vec().into_boxed_slice()],
        )
        .unwrap();
        let mutual = TlsServerOptions::new_with_client_auth(
            vec![certificate.clone()],
            private_key.clone(),
            vec![b"h2".to_vec().into_boxed_slice()],
            vec![certificate.clone()],
        )
        .unwrap();
        let settings =
            TlsServerOptions::from_settings(TlsServerSettings::new(vec![certificate], private_key))
                .unwrap();
        [
            client.crypto_provider(),
            server.config.crypto_provider(),
            mutual.config.crypto_provider(),
            settings.config.crypto_provider(),
        ]
        .map(Arc::clone)
    }

    fn collect_output(mut pipe: impl Read + Send + 'static) -> JoinHandle<String> {
        thread::spawn(move || {
            let mut output = Vec::new();
            let read = pipe.read_to_end(&mut output);
            let mut text = String::from_utf8_lossy(&output).into_owned();
            if let Err(error) = read {
                text.push_str(&format!("\n<output read failed: {error}>"));
            }
            text
        })
    }

    /// Runs `body` in a fresh copy of this test binary filtered to `test`.
    ///
    /// rustls' process-default provider can be installed only once per
    /// process, and every test in this binary shares one process, so tests
    /// that observe or install it run alone in a child.
    fn run_in_child_process(test: &str, body: impl FnOnce()) {
        let module = module_path!();
        let module = module.split_once("::").map_or(module, |(_, path)| path);
        let name = format!("{module}::{test}");
        if std::env::var_os(PROVIDER_CHILD_MARKER).is_some_and(|marker| marker == *name) {
            body();
            println!("{PROVIDER_CHILD_RECEIPT} {name}");
            return;
        }

        let mut child = {
            let _spawn = crate::TEST_PROCESS_SPAWN_LOCK
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            Command::new(std::env::current_exe().expect("test binary path"))
                .args([name.as_str(), "--exact", "--nocapture", "--test-threads=1"])
                .env(PROVIDER_CHILD_MARKER, &name)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("spawn the provider test child")
        };
        let stdout = collect_output(child.stdout.take().expect("piped child stdout"));
        let stderr = collect_output(child.stderr.take().expect("piped child stderr"));
        let started = Instant::now();
        let status = loop {
            if let Some(status) = child.try_wait().expect("poll the provider test child") {
                break Some(status);
            }
            if started.elapsed() > PROVIDER_CHILD_BOUND {
                let _ = child.kill();
                child
                    .wait()
                    .expect("reap the timed-out provider test child");
                break None;
            }
            thread::sleep(Duration::from_millis(10));
        };
        let stdout = stdout.join().expect("child stdout reader");
        let stderr = stderr.join().expect("child stderr reader");
        assert!(
            status.is_some_and(|status| status.success())
                && stdout.contains(&format!("{PROVIDER_CHILD_RECEIPT} {name}")),
            "child for {name} failed, or was killed after {PROVIDER_CHILD_BOUND:?} if the \
             status is None (status {status:?})\n--- child stdout ---\n{stdout}\n--- child stderr ---\n{stderr}",
        );
    }

    #[test]
    fn fallback_configs_use_the_builtin_provider_without_installing_it() {
        run_in_child_process(
            "fallback_configs_use_the_builtin_provider_without_installing_it",
            || {
                assert!(
                    CryptoProvider::get_default().is_none(),
                    "a fresh test process starts without a process-default provider",
                );
                let fallback = fallback_crypto_provider();
                let providers = fallback_config_providers();
                assert!(
                    CryptoProvider::get_default().is_none(),
                    "the fallback provider and fallback TLS configs must not install a \
                     process-default provider",
                );
                let expected = provider_algorithms(&builtin_crypto_provider());
                assert_eq!(
                    provider_algorithms(&fallback),
                    expected,
                    "the fallback provider is the built-in provider",
                );
                for provider in providers {
                    assert_eq!(
                        provider_algorithms(&provider),
                        expected,
                        "fallback configs keep the built-in provider's suites and groups",
                    );
                }
            },
        );
    }

    #[test]
    fn fallback_configs_use_the_installed_process_provider() {
        run_in_child_process(
            "fallback_configs_use_the_installed_process_provider",
            || {
                builtin_with_only(CipherSuite::TLS13_CHACHA20_POLY1305_SHA256)
                    .install_default()
                    .expect("a fresh test process accepts a process-default provider");
                let installed = CryptoProvider::get_default().expect("installed process default");
                assert!(
                    Arc::ptr_eq(&fallback_crypto_provider(), installed),
                    "the fallback provider is the installed process default",
                );
                for provider in fallback_config_providers() {
                    assert!(
                        Arc::ptr_eq(&provider, installed),
                        "fallback configs use the installed process default",
                    );
                    assert_eq!(
                        provider_algorithms(&provider).0,
                        [CipherSuite::TLS13_CHACHA20_POLY1305_SHA256],
                        "the fallback provider keeps the process default's restricted suites",
                    );
                }
            },
        );
    }

    #[cfg(feature = "aws-lc-rs")]
    #[test]
    fn builtin_provider_prefers_post_quantum_key_exchange() {
        assert_eq!(
            provider_algorithms(&builtin_crypto_provider()).1.first(),
            Some(&NamedGroup::X25519MLKEM768),
            "with aws-lc-rs, the built-in provider offers X25519MLKEM768 first",
        );
    }

    #[cfg(all(feature = "aws-lc-rs", feature = "ring"))]
    #[test]
    fn fallback_configs_use_aws_lc_rs_when_both_backends_are_built() {
        run_in_child_process(
            "fallback_configs_use_aws_lc_rs_when_both_backends_are_built",
            || {
                assert!(
                    CryptoProvider::get_default().is_none(),
                    "a fresh test process starts without a process-default provider",
                );
                let expected = provider_algorithms(&rustls::crypto::aws_lc_rs::default_provider());
                assert_eq!(
                    provider_algorithms(&builtin_crypto_provider()),
                    expected,
                    "the built-in provider is aws-lc-rs's when ring is built too",
                );
                for provider in fallback_config_providers() {
                    assert_eq!(
                        provider_algorithms(&provider),
                        expected,
                        "fallback configs use aws-lc-rs's suites and groups",
                    );
                }
            },
        );
    }

    #[cfg(all(feature = "ring", not(feature = "aws-lc-rs")))]
    #[test]
    fn fallback_configs_use_ring_without_aws_lc_rs() {
        run_in_child_process("fallback_configs_use_ring_without_aws_lc_rs", || {
            assert!(
                CryptoProvider::get_default().is_none(),
                "a fresh test process starts without a process-default provider",
            );
            let expected = provider_algorithms(&rustls::crypto::ring::default_provider());
            assert_eq!(
                provider_algorithms(&builtin_crypto_provider()),
                expected,
                "the built-in provider is ring's without aws-lc-rs",
            );
            for provider in fallback_config_providers() {
                assert_eq!(
                    provider_algorithms(&provider),
                    expected,
                    "fallback configs use ring's suites and groups",
                );
            }
        });
    }

    #[test]
    fn explicit_client_provider_overrides_the_installed_process_provider() {
        run_in_child_process(
            "explicit_client_provider_overrides_the_installed_process_provider",
            || {
                builtin_with_only(CipherSuite::TLS13_CHACHA20_POLY1305_SHA256)
                    .install_default()
                    .expect("a fresh test process accepts a process-default provider");
                let explicit = Arc::new(builtin_with_only(CipherSuite::TLS13_AES_128_GCM_SHA256));
                let certificate = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
                    .expect("test certificate");
                let mut options = TlsClientOptions::new("localhost");
                options.add_root_certificate(certificate.cert.der());
                options.set_crypto_provider(Arc::clone(&explicit));
                let client = build_client_config(options).unwrap().0;
                assert!(
                    Arc::ptr_eq(client.crypto_provider(), &explicit),
                    "an explicit client provider is used instead of the process default",
                );
                assert_eq!(
                    provider_algorithms(client.crypto_provider()).0,
                    [CipherSuite::TLS13_AES_128_GCM_SHA256],
                );
            },
        );
    }

    #[test]
    fn changing_crypto_provider_separates_cached_config_from_existing_clones() {
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("test certificate");
        let mut options = TlsClientOptions::new("localhost");
        options.add_root_certificate(certificate.cert.der());
        let retained = options.clone();
        let original = build_client_config(options.clone()).unwrap().0;
        assert!(Arc::ptr_eq(
            &original,
            &build_client_config(retained.clone()).unwrap().0,
        ));

        let mut provider = builtin_crypto_provider();
        provider
            .cipher_suites
            .sort_by_key(|suite| suite.suite() != CipherSuite::TLS13_AES_128_GCM_SHA256);
        let provider = Arc::new(provider);
        options.set_crypto_provider(Arc::clone(&provider));
        let changed = build_client_config(options.clone()).unwrap().0;
        assert!(!Arc::ptr_eq(&original, &changed));
        assert!(Arc::ptr_eq(changed.crypto_provider(), &provider));
        assert!(Arc::ptr_eq(
            &changed,
            &build_client_config(options).unwrap().0,
        ));
        assert!(Arc::ptr_eq(
            &original,
            &build_client_config(retained).unwrap().0,
        ));
    }

    #[test]
    fn default_ca_path_ignores_empty_values_and_preserves_precedence() {
        assert_eq!(
            select_default_ca_path(
                Some(OsString::from("/proxygen")),
                Some(OsString::from("/curl")),
                Some("/fallback"),
            ),
            Some(PathBuf::from("/proxygen")),
        );
        assert_eq!(
            select_default_ca_path(
                Some(OsString::new()),
                Some(OsString::from("/curl")),
                Some("/fallback"),
            ),
            Some(PathBuf::from("/curl")),
        );
        assert_eq!(
            select_default_ca_path(
                Some(OsString::new()),
                Some(OsString::new()),
                Some("/fallback"),
            ),
            Some(PathBuf::from("/fallback")),
        );
        assert_eq!(
            select_default_ca_path(Some(OsString::new()), Some(OsString::new()), Some(""),),
            None,
        );
    }
}
