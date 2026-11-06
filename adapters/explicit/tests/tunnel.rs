//! CONNECT tunnel oracles through a loopback plaintext proxy.

mod tunnel {
    use std::collections::VecDeque;
    use std::io;
    use std::io::Read;
    use std::io::Write;
    use std::net::IpAddr;
    use std::net::Ipv4Addr;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::thread::JoinHandle;
    use std::time::Duration;
    use std::time::Instant;

    use futures::channel::oneshot;
    use http::HeaderName;
    use http::HeaderValue;
    use http::uri::Authority;
    use roxygen_core::error::HttpError;
    use roxygen_core::error::HttpErrorCode;
    use roxygen_core::message::HttpMessage;
    use roxygen_runtime::cancellation::CancellationToken;
    use roxygen_transport::byte_stream::ByteStreamLimits;
    use roxygen_transport::established::EstablishedIo;
    use roxygen_transport::established::adopt_local_with_completion;
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::PrivateKeyDer;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    use tokio::net::tcp::OwnedReadHalf;
    use tokio::net::tcp::OwnedWriteHalf;
    use tokio::sync::Notify;

    use crate::error::ErrorKind;
    use crate::error::ExplicitError;
    use crate::error::Observed;
    use crate::error::Phase;
    use crate::error::Position;
    use crate::exchange::BodyOutcome;
    use crate::exchange::ExchangeHooks;
    use crate::exchange::ExchangeProbe;
    use crate::exchange::Request;
    use crate::exchange::Response;
    use crate::exchange::TeardownWatch;
    use crate::exchange::TestPause;
    use crate::exchange_tests::support::EXPIRY_BUDGET;
    use crate::exchange_tests::support::HANG_GUARD;
    use crate::exchange_tests::support::ReleaseOnDrop;
    use crate::exchange_tests::support::block_on;
    use crate::exchange_tests::support::endpoint;
    use crate::exchange_tests::support::eventually;
    use crate::exchange_tests::support::install_crypto_provider;
    use crate::exchange_tests::support::refusing_address;
    use crate::exchange_tests::support::request;
    use crate::exchange_tests::support::run_on_tokio_driver;
    use crate::exchange_tests::support::start;
    use crate::exchange_tests::support::tls_target;
    use crate::exchange_tests::support::untrusting_client;
    use crate::exchange_tests::support::wait_until;
    use crate::runtime::ExplicitRuntime;
    use crate::runtime::LegSetup;
    use crate::runtime::RuntimeHooks;
    use crate::runtime::Security;
    use crate::runtime::Target;
    use crate::runtime::attach_proxy_leg;
    use crate::runtime::tunnel_cleanup;
    use crate::stats::ExplicitStats;
    use crate::tunnel::ConnectHead;
    use crate::tunnel::Tunnel;
    use crate::tunnel::TunnelHandoff;

    /// The request authority. Nothing resolves it: the proxy dials the origin it was given.
    const DESTINATION: &str = "destination.does-not-exist.invalid";

    const TUNNEL_200: &[u8] =
        b"HTTP/1.1 200 Connection established\r\nX-FB-Fwdproxy-Request-ID: request-1\r\n\r\n";

    const ORIGIN_OK: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";

    /// A head and the first bytes of a body that the origin then holds.
    const ORIGIN_PARTIAL: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\npartial";

    /// A request budget far beyond the hang guard, so an oracle bounded by the guard fails when
    /// something waits for the deadline.
    const LONG_BUDGET: Duration = Duration::from_secs(60);

    const OPEN: Observed = Observed {
        own_cancelled: false,
        worker_open: true,
        deadline_passed: false,
    };

    // ---------------------------------------------------------------------------------------
    // Loopback proxy.

    /// How one side of a connection ended.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum End {
        Eof,
        Error(io::ErrorKind),
    }

    fn end_of(result: &io::Result<usize>) -> Option<End> {
        match result {
            Ok(0) => Some(End::Eof),
            Ok(_) => None,
            Err(error) => Some(End::Error(error.kind())),
        }
    }

    /// One CONNECT head, as the proxy read it.
    #[derive(Clone, Debug)]
    struct RawConnect {
        request_line: String,
        headers: Vec<(String, String)>,
    }

    impl RawConnect {
        fn values(&self, name: &str) -> Vec<&str> {
            self.headers
                .iter()
                .filter(|(header, _)| header.eq_ignore_ascii_case(name))
                .map(|(_, value)| value.as_str())
                .collect()
        }
    }

    /// One accepted proxy connection.
    #[derive(Clone, Debug, Default)]
    struct Connection {
        client_port: u16,
        connect: Option<RawConnect>,
        /// How the client connection's read side ended.
        read_end: Option<End>,
        /// The first proxy write to the client that failed once its read side ended.
        write_failure: Option<io::ErrorKind>,
    }

    impl Connection {
        /// Closed: the proxy read EOF or a reset, and a later write of its own then failed with a
        /// reset or broken pipe, which only a client that no longer holds the socket causes.
        fn closed(&self) -> bool {
            self.read_end.is_some()
                && matches!(
                    self.write_failure,
                    Some(io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe)
                )
        }
    }

    /// What the proxy does with one accepted connection once it has read the CONNECT head. The
    /// proxy never closes or shuts down an accepted connection unless the script says so, and
    /// keeps reading it.
    #[derive(Clone)]
    enum Script {
        /// Answers `head` and relays to `origin`, passing the client's FIN on to the origin as a
        /// write shutdown. With `pass_fin`, it also passes the origin's end on to the client as
        /// a write shutdown; a write fails after that whether or not the client still holds the
        /// socket, so the closure probe never runs on such a connection. Once `reset` is
        /// notified, it resets the client connection.
        Tunnel {
            head: &'static [u8],
            origin: SocketAddr,
            reset: Option<Arc<Notify>>,
            pass_fin: bool,
        },
        /// Answers `bytes` and keeps reading.
        Answer(Vec<u8>),
        /// Reads the CONNECT and never answers.
        Silent,
        /// Answers `bytes`, reads the client's next bytes when `after_client_bytes`, and then
        /// closes the connection, with a reset when `reset`.
        End {
            bytes: &'static [u8],
            after_client_bytes: bool,
            reset: bool,
        },
    }

    impl Script {
        fn tunnel(head: &'static [u8], origin: &Origin) -> Self {
            Self::Tunnel {
                head,
                origin: origin.address,
                reset: None,
                pass_fin: false,
            }
        }

        /// A 200 tunnel to an origin that closes as `close` says.
        fn closing(origin: &Origin, close: Close) -> Self {
            Self::Tunnel {
                head: TUNNEL_200,
                origin: origin.address,
                reset: None,
                pass_fin: close == Close::Fin,
            }
        }
    }

    struct ProxyShared {
        scripts: Mutex<VecDeque<Script>>,
        default: Script,
        connections: Mutex<Vec<Connection>>,
    }

    impl ProxyShared {
        fn update(&self, index: usize, change: impl FnOnce(&mut Connection)) {
            change(&mut self.connections.lock().unwrap()[index]);
        }
    }

    struct Proxy {
        address: SocketAddr,
        shared: Arc<ProxyShared>,
        stop: Option<tokio::sync::oneshot::Sender<()>>,
        thread: Option<JoinHandle<()>>,
    }

    impl Proxy {
        fn start(default: Script) -> Self {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("proxy listener");
            listener
                .set_nonblocking(true)
                .expect("non-blocking listener");
            let address = listener.local_addr().expect("proxy address");
            let shared = Arc::new(ProxyShared {
                scripts: Mutex::new(VecDeque::new()),
                default,
                connections: Mutex::new(Vec::new()),
            });
            let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
            let accepting = Arc::clone(&shared);
            let thread = std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("proxy runtime");
                runtime.block_on(async move {
                    let listener =
                        tokio::net::TcpListener::from_std(listener).expect("tokio listener");
                    let mut stopped = stopped;
                    loop {
                        tokio::select! {
                            _ = &mut stopped => break,
                            accepted = listener.accept() => {
                                let Ok((stream, peer)) = accepted else { continue };
                                let index = {
                                    let mut connections = accepting.connections.lock().unwrap();
                                    connections.push(Connection {
                                        client_port: peer.port(),
                                        ..Connection::default()
                                    });
                                    connections.len() - 1
                                };
                                let script = accepting
                                    .scripts
                                    .lock()
                                    .unwrap()
                                    .pop_front()
                                    .unwrap_or_else(|| accepting.default.clone());
                                tokio::spawn(serve(Arc::clone(&accepting), index, stream, script));
                            }
                        }
                    }
                });
                // Dropping the runtime drops every connection task and closes its sockets.
                drop(runtime);
            });
            Self {
                address,
                shared,
                stop: Some(stop),
                thread: Some(thread),
            }
        }

        /// Scripts the next accepted connection, ahead of the default.
        fn push(&self, script: Script) {
            self.shared.scripts.lock().unwrap().push_back(script);
        }

        fn connections(&self) -> Vec<Connection> {
            self.shared.connections.lock().unwrap().clone()
        }

        fn connection(&self, index: usize) -> Connection {
            self.connections()[index].clone()
        }

        fn wait_closed(&self, index: usize) {
            let closed = eventually(|| {
                self.connections()
                    .get(index)
                    .is_some_and(Connection::closed)
            });
            assert!(
                closed,
                "proxy connection {index} is not closed: {:?}",
                self.connections().get(index)
            );
        }
    }

    impl Drop for Proxy {
        fn drop(&mut self) {
            if let Some(stop) = self.stop.take() {
                let _ = stop.send(());
            }
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }

    async fn serve(
        shared: Arc<ProxyShared>,
        index: usize,
        stream: tokio::net::TcpStream,
        script: Script,
    ) {
        let (mut reader, mut writer) = stream.into_split();
        let mut pending = Vec::new();
        let connect = match read_connect(&mut reader, &mut pending).await {
            Ok(connect) => connect,
            Err(end) => {
                shared.update(index, |connection| connection.read_end = Some(end));
                probe(&shared, index, &mut writer).await;
                return;
            }
        };
        shared.update(index, |connection| connection.connect = Some(connect));
        match script {
            Script::Silent => watch(&shared, index, reader, writer).await,
            Script::Answer(bytes) => {
                let _ = writer.write_all(&bytes).await;
                watch(&shared, index, reader, writer).await;
            }
            Script::End {
                bytes,
                after_client_bytes,
                reset,
            } => {
                let _ = writer.write_all(bytes).await;
                if after_client_bytes && pending.is_empty() {
                    let mut byte = [0; 1];
                    let _ = tokio::time::timeout(HANG_GUARD, reader.read(&mut byte)).await;
                }
                close(reader, writer, reset);
            }
            Script::Tunnel {
                head,
                origin,
                reset,
                pass_fin,
            } => {
                let _ = writer.write_all(head).await;
                let relayed = Relayed {
                    pending,
                    origin,
                    reset,
                    pass_fin,
                };
                relay(&shared, index, reader, writer, relayed).await;
            }
        }
    }

    fn close(reader: OwnedReadHalf, writer: OwnedWriteHalf, reset: bool) {
        if reset {
            let _ = reader.as_ref().set_zero_linger();
            writer.forget();
        } else {
            drop(writer);
        }
        drop(reader);
    }

    async fn read_connect(
        reader: &mut OwnedReadHalf,
        pending: &mut Vec<u8>,
    ) -> Result<RawConnect, End> {
        let mut buffer = [0; 4096];
        loop {
            if let Some(end) = find(pending, b"\r\n\r\n") {
                let head: Vec<u8> = pending.drain(..end + 4).collect();
                let head = String::from_utf8(head).expect("ASCII CONNECT head");
                let mut lines = head.split("\r\n");
                let request_line = lines.next().expect("request line").to_owned();
                let headers = lines
                    .filter(|line| !line.is_empty())
                    .map(|line| {
                        let (name, value) = line.split_once(':').expect("header line");
                        (name.to_owned(), value.trim().to_owned())
                    })
                    .collect();
                return Ok(RawConnect {
                    request_line,
                    headers,
                });
            }
            let read = reader.read(&mut buffer).await;
            if let Some(end) = end_of(&read) {
                return Err(end);
            }
            pending.extend_from_slice(&buffer[..read.expect("bytes")]);
        }
    }

    fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack
            .windows(needle.len())
            .position(|window| window == needle)
    }

    /// Reads until the client's side ends, and then probes.
    async fn watch(
        shared: &ProxyShared,
        index: usize,
        mut reader: OwnedReadHalf,
        mut writer: OwnedWriteHalf,
    ) {
        let mut buffer = [0; 4096];
        let end = loop {
            if let Some(end) = end_of(&reader.read(&mut buffer).await) {
                break end;
            }
        };
        shared.update(index, |connection| connection.read_end = Some(end));
        probe(shared, index, &mut writer).await;
        drop(reader);
    }

    /// Writes one byte at a time until a write fails, within the hang guard.
    async fn probe(shared: &ProxyShared, index: usize, writer: &mut OwnedWriteHalf) {
        let deadline = Instant::now() + HANG_GUARD;
        while Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
            if let Err(error) = writer.write_all(b"x").await {
                shared.update(index, |connection| {
                    connection.write_failure = Some(error.kind());
                });
                return;
            }
        }
    }

    /// What a tunnel relays to, and how.
    struct Relayed {
        /// Client bytes read past the CONNECT head.
        pending: Vec<u8>,
        origin: SocketAddr,
        reset: Option<Arc<Notify>>,
        pass_fin: bool,
    }

    async fn relay(
        shared: &ProxyShared,
        index: usize,
        mut client_reader: OwnedReadHalf,
        mut client_writer: OwnedWriteHalf,
        relayed: Relayed,
    ) {
        let Relayed {
            pending,
            origin,
            reset,
            pass_fin,
        } = relayed;
        let Ok(origin) = tokio::net::TcpStream::connect(origin).await else {
            watch(shared, index, client_reader, client_writer).await;
            return;
        };
        let (mut origin_reader, mut origin_writer) = origin.into_split();
        if !pending.is_empty() {
            let _ = origin_writer.write_all(&pending).await;
        }
        let reset = reset.unwrap_or_default();
        let mut client_buffer = vec![0; 16384];
        let mut origin_buffer = vec![0; 16384];
        let mut client_open = true;
        let mut origin_open = true;
        let mut probe = tokio::time::interval(Duration::from_millis(5));
        loop {
            if pass_fin && !client_open && !origin_open {
                return;
            }
            tokio::select! {
                read = client_reader.read(&mut client_buffer), if client_open => {
                    match end_of(&read) {
                        Some(end) => {
                            shared.update(index, |connection| connection.read_end = Some(end));
                            client_open = false;
                            let _ = origin_writer.shutdown().await;
                        }
                        None => {
                            let read = read.expect("bytes");
                            let _ = origin_writer.write_all(&client_buffer[..read]).await;
                        }
                    }
                }
                read = origin_reader.read(&mut origin_buffer), if origin_open => {
                    match read {
                        Ok(0) | Err(_) => {
                            origin_open = false;
                            if pass_fin {
                                let _ = client_writer.shutdown().await;
                            }
                        }
                        Ok(read) => {
                            if let Err(error) =
                                client_writer.write_all(&origin_buffer[..read]).await
                                && !client_open
                            {
                                shared.update(index, |connection| {
                                    connection.write_failure = Some(error.kind());
                                });
                                return;
                            }
                        }
                    }
                }
                _ = probe.tick(), if !client_open && !pass_fin => {
                    if let Err(error) = client_writer.write_all(b"x").await {
                        shared.update(index, |connection| {
                            connection.write_failure = Some(error.kind());
                        });
                        return;
                    }
                }
                () = reset.notified() => {
                    close(client_reader, client_writer, true);
                    return;
                }
            }
        }
    }

    // ---------------------------------------------------------------------------------------
    // Loopback TLS origin.

    /// How an origin that closes on purpose ends its connection.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Close {
        /// close_notify and then the FIN, which `Script::closing` keeps from the client, so the
        /// client reads the end inside the tunnel's TLS.
        Notify,
        /// The FIN alone, which `Script::closing` passes on, so the client reads EOF from the
        /// tunnel without a close_notify.
        Fin,
    }

    /// What the origin does with one accepted connection.
    #[derive(Clone)]
    enum Serve {
        /// Reads one request, writes `response`, and then reads until the client ends.
        Respond(&'static [u8]),
        /// Reads one request, writes `response`, and closes the connection.
        RespondAndClose(&'static [u8], Close),
        /// Reads one request, writes `first`, writes `rest` once `release` is set, and then
        /// reads until the client ends.
        RespondInTwo {
            first: &'static [u8],
            rest: &'static [u8],
            release: Arc<AtomicBool>,
        },
        /// Reads one request and never answers.
        Hold,
        /// Reads one request and closes before answering.
        CloseBeforeHead(Close),
        /// Reads the client's bytes without a TLS handshake and never answers them.
        Silent,
    }

    #[derive(Clone, Debug, Default)]
    struct OriginConnection {
        peer_port: u16,
        /// Bytes read from a `Serve::Silent` connection.
        raw_bytes: usize,
        sni: Option<String>,
        alpn: Option<Vec<u8>>,
        request: Option<RawConnect>,
        end: Option<End>,
    }

    struct Origin {
        address: SocketAddr,
        connections: Arc<Mutex<Vec<OriginConnection>>>,
        stop: Arc<AtomicBool>,
        acceptor: Option<JoinHandle<()>>,
        handlers: Arc<Mutex<Vec<JoinHandle<()>>>>,
    }

    impl Origin {
        fn start(config: Option<&Arc<rustls::ServerConfig>>, serve: Serve) -> Self {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("origin listener");
            let address = listener.local_addr().expect("origin address");
            let connections = Arc::new(Mutex::new(Vec::new()));
            let stop = Arc::new(AtomicBool::new(false));
            let handlers = Arc::new(Mutex::new(Vec::new()));
            let config = config.cloned();
            let acceptor = {
                let connections = Arc::clone(&connections);
                let stop = Arc::clone(&stop);
                let handlers = Arc::clone(&handlers);
                std::thread::spawn(move || {
                    for stream in listener.incoming() {
                        if stop.load(Ordering::SeqCst) {
                            return;
                        }
                        let Ok(tcp) = stream else { continue };
                        let index = {
                            let mut connections = connections.lock().unwrap();
                            connections.push(OriginConnection {
                                peer_port: tcp.peer_addr().expect("peer").port(),
                                ..OriginConnection::default()
                            });
                            connections.len() - 1
                        };
                        let connections = Arc::clone(&connections);
                        let config = config.clone();
                        let serve = serve.clone();
                        let handler = std::thread::spawn(move || {
                            let record = |change: &dyn Fn(&mut OriginConnection)| {
                                change(&mut connections.lock().unwrap()[index]);
                            };
                            serve_origin(config, serve, tcp, &record);
                        });
                        handlers.lock().unwrap().push(handler);
                    }
                })
            };
            Self {
                address,
                connections,
                stop,
                acceptor: Some(acceptor),
                handlers,
            }
        }

        fn port(&self) -> u16 {
            self.address.port()
        }

        fn connections(&self) -> Vec<OriginConnection> {
            self.connections.lock().unwrap().clone()
        }

        fn requests(&self) -> usize {
            self.connections()
                .iter()
                .filter(|connection| connection.request.is_some())
                .count()
        }
    }

    impl Drop for Origin {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            let _ = std::net::TcpStream::connect(self.address);
            if let Some(acceptor) = self.acceptor.take() {
                let _ = acceptor.join();
            }
            let handlers = std::mem::take(&mut *self.handlers.lock().unwrap());
            handlers.into_iter().for_each(|handler| {
                let _ = handler.join();
            });
        }
    }

    fn serve_origin(
        config: Option<Arc<rustls::ServerConfig>>,
        serve: Serve,
        tcp: std::net::TcpStream,
        record: &dyn Fn(&dyn Fn(&mut OriginConnection)),
    ) {
        tcp.set_read_timeout(Some(HANG_GUARD))
            .expect("read timeout");
        tcp.set_write_timeout(Some(HANG_GUARD))
            .expect("write timeout");
        if let Serve::Silent = serve {
            let mut tcp = tcp;
            let mut buffer = [0; 4096];
            loop {
                let read = tcp.read(&mut buffer);
                if let Some(end) = end_of(&read) {
                    record(&|connection| connection.end = Some(end));
                    return;
                }
                let read = read.expect("bytes");
                record(&|connection| connection.raw_bytes += read);
            }
        }
        let config = config.expect("a TLS origin");
        let mut tls = rustls::StreamOwned::new(
            rustls::ServerConnection::new(config).expect("TLS server"),
            tcp,
        );
        let mut pending = Vec::new();
        let request = loop {
            if let Some(end) = find(&pending, b"\r\n\r\n") {
                let head = String::from_utf8(pending[..end].to_vec()).expect("ASCII request");
                let mut lines = head.split("\r\n");
                let request_line = lines.next().expect("request line").to_owned();
                let headers = lines
                    .map(|line| {
                        let (name, value) = line.split_once(':').expect("header line");
                        (name.to_owned(), value.trim().to_owned())
                    })
                    .collect();
                break RawConnect {
                    request_line,
                    headers,
                };
            }
            let mut buffer = [0; 4096];
            let read = tls.read(&mut buffer);
            if let Some(end) = end_of(&read) {
                record(&|connection| connection.end = Some(end));
                return;
            }
            pending.extend_from_slice(&buffer[..read.expect("bytes")]);
        };
        let sni = tls.conn.server_name().map(str::to_owned);
        let alpn = tls.conn.alpn_protocol().map(<[u8]>::to_vec);
        record(&|connection| {
            connection.sni = sni.clone();
            connection.alpn = alpn.clone();
            connection.request = Some(request.clone());
        });
        let (response, close): (&[u8], _) = match &serve {
            Serve::Respond(response) => (response, None),
            Serve::RespondAndClose(response, close) => (response, Some(*close)),
            Serve::RespondInTwo { first, .. } => (first, None),
            Serve::Hold => (b"", None),
            Serve::CloseBeforeHead(close) => (b"", Some(*close)),
            Serve::Silent => unreachable!("handled above"),
        };
        let _ = tls.write_all(response).and_then(|()| tls.flush());
        if let Some(close) = close {
            if close == Close::Notify {
                tls.conn.send_close_notify();
                let _ = tls.flush();
            }
            return;
        }
        if let Serve::RespondInTwo { rest, release, .. } = &serve {
            if !eventually(|| release.load(Ordering::SeqCst)) {
                return;
            }
            let _ = tls.write_all(rest).and_then(|()| tls.flush());
        }
        let mut buffer = [0; 4096];
        loop {
            let read = tls.read(&mut buffer);
            if let Some(end) = end_of(&read) {
                record(&|connection| connection.end = Some(end));
                return;
            }
        }
    }

    // ---------------------------------------------------------------------------------------
    // TLS identities, tunnels, and sends.

    struct Identity {
        server: Arc<rustls::ServerConfig>,
        client: Arc<rustls::ClientConfig>,
    }

    /// A self-signed origin identity for `names`, which the client trusts. With `alpn`, the
    /// client offers http/1.1, which the origin selects.
    fn identity(names: &[&str], alpn: bool) -> Identity {
        install_crypto_provider();
        let generated = rcgen::generate_simple_self_signed(
            names
                .iter()
                .map(|name| (*name).to_owned())
                .collect::<Vec<_>>(),
        )
        .expect("self-signed certificate");
        let certificate: CertificateDer<'static> = generated.cert.der().clone();
        let key = PrivateKeyDer::try_from(generated.signing_key.serialized_der().to_vec())
            .expect("private key");
        let mut server = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![certificate.clone()], key)
            .expect("server identity");
        server.alpn_protocols = vec![b"http/1.1".to_vec()];
        let mut roots = rustls::RootCertStore::empty();
        roots.add(certificate).expect("trusted root");
        let mut client = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        if alpn {
            client.alpn_protocols = vec![b"http/1.1".to_vec()];
        }
        Identity {
            server: Arc::new(server),
            client: Arc::new(client),
        }
    }

    fn localhost() -> Identity {
        identity(&["localhost", "127.0.0.1"], false)
    }

    fn headers(pairs: &[(&'static str, &'static str)]) -> Vec<(HeaderName, HeaderValue)> {
        pairs
            .iter()
            .map(|(name, value)| {
                (
                    HeaderName::from_static(name),
                    HeaderValue::from_static(value),
                )
            })
            .collect()
    }

    fn try_tunnel(
        authority: &str,
        connect_headers: &[(&'static str, &'static str)],
        config: &Arc<rustls::ClientConfig>,
        tls_host: &str,
    ) -> Result<Tunnel, ExplicitError> {
        Tunnel::new(
            Authority::try_from(authority).expect("authority"),
            headers(connect_headers),
            Arc::clone(config),
            tls_host,
        )
    }

    fn target(proxy: SocketAddr, tunnel: Tunnel) -> Arc<Target> {
        Arc::new(Target {
            address: proxy,
            security: Security::Tunnel(Arc::new(tunnel)),
        })
    }

    /// A tunnel target through `proxy` whose CONNECT names the origin's numeric address and whose
    /// TLS verifies `localhost`.
    fn origin_target(proxy: &Proxy, origin: &Origin, identity: &Identity) -> Arc<Target> {
        let tunnel = try_tunnel(
            &format!("127.0.0.1:{}", origin.port()),
            &[],
            &identity.client,
            "localhost",
        )
        .expect("tunnel");
        target(proxy.address, tunnel)
    }

    fn destination_request(origin: &Origin, budget: Duration) -> Request {
        request(&format!("{DESTINATION}:{}", origin.port()), "/", budget)
    }

    fn send(
        runtime: &ExplicitRuntime,
        target: &Arc<Target>,
        request: Request,
    ) -> Result<Response, ExplicitError> {
        block_on(runtime.client(Arc::clone(target)).send(request))
    }

    fn lines(head: &ConnectHead) -> Vec<(String, String)> {
        head.lines
            .iter()
            .map(|(name, value)| {
                (
                    String::from_utf8(name.to_vec()).expect("ASCII name"),
                    String::from_utf8(value.to_vec()).expect("ASCII value"),
                )
            })
            .collect()
    }

    fn head_status(head: Option<&ConnectHead>) -> Option<u16> {
        head.map(|head| head.status)
    }

    fn assert_complete(response: &Response, bytes: u64) {
        match &response.body {
            BodyOutcome::Complete { bytes: read } => assert_eq!(*read, bytes),
            BodyOutcome::Truncated { cause, .. } => panic!("truncated: {cause}"),
        }
    }

    fn truncated_cause(response: Response) -> (ExplicitError, Option<Box<ConnectHead>>) {
        match response.body {
            BodyOutcome::Truncated { cause, .. } => (cause, response.connect_head),
            BodyOutcome::Complete { bytes } => panic!("complete with {bytes} bytes"),
        }
    }

    fn pending_teardowns(runtime: &ExplicitRuntime) -> u64 {
        runtime.stats().pending_teardowns
    }

    fn wait_for_teardowns(runtime: &ExplicitRuntime) {
        wait_until("both legs retire", || pending_teardowns(runtime) == 0);
        assert_eq!(runtime.stats().unconfirmed_retirements, 0);
    }

    fn probing(probe: &Arc<ExchangeProbe>) -> ExchangeHooks {
        ExchangeHooks {
            probe: Some(Arc::clone(probe)),
            ..ExchangeHooks::default()
        }
    }

    /// Blocks the calling thread until `deadline` has passed.
    fn sleep_past(deadline: Instant) {
        while Instant::now() <= deadline {
            std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
        }
    }

    // ---------------------------------------------------------------------------------------
    // expectation-353: heads, authorities, CONNECT headers, construction, and tunnel TLS.

    #[test]
    fn success_carries_the_200_head_with_its_repeated_lines() {
        const HEAD: &[u8] = b"HTTP/1.1 200 Connection established\r\n\
            X-FB-Fwdproxy-Debug: proxy-ip=\"10.0.0.1\", edge-hop=1\r\n\
            X-Dup: first\r\n\
            Via: 1.1 fwdproxy\r\n\
            X-Dup: second\r\n\r\n";
        for alpn in [false, true] {
            let identity = identity(&["localhost"], alpn);
            let origin = Origin::start(Some(&identity.server), Serve::Respond(ORIGIN_OK));
            let proxy = Proxy::start(Script::tunnel(HEAD, &origin));
            let runtime = start(1, "rtn-ok");
            let response = send(
                &runtime,
                &origin_target(&proxy, &origin, &identity),
                destination_request(&origin, HANG_GUARD),
            )
            .expect("tunnel send");
            assert_eq!(response.status, 200);
            assert_complete(&response, 2);
            let head = response.connect_head.as_deref().expect("connect head");
            assert_eq!(head.status, 200);
            assert_eq!(
                lines(head),
                [
                    ("X-FB-Fwdproxy-Debug", "proxy-ip=\"10.0.0.1\", edge-hop=1"),
                    ("X-Dup", "first"),
                    ("Via", "1.1 fwdproxy"),
                    ("X-Dup", "second"),
                ]
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
            );
            let values: Vec<_> = head.get_all("x-dup").map(|value| &value[..]).collect();
            assert_eq!(values, [&b"first"[..], b"second"]);
            let connections = origin.connections();
            assert_eq!(connections.len(), 1, "{connections:?}");
            assert_eq!(
                connections[0].alpn,
                alpn.then(|| b"http/1.1".to_vec()),
                "alpn {alpn}"
            );
            assert_eq!(response.connection.peer, Some(proxy.address));
            assert!(response.connection.tls_version.is_some());
            assert!(response.timings.tcp_connect.is_some());
            assert!(response.timings.tls_handshake.is_some());
        }
    }

    #[test]
    fn a_raw_headers_client_keeps_the_tunneled_head_as_parsed() {
        const ORIGIN_HEAD: &[u8] =
            b"HTTP/1.1 200 OK\r\nX-Origin: first\r\nx-origin: second\r\nContent-Length: 2\r\n\r\nok";
        let identity = localhost();
        let origin = Origin::start(Some(&identity.server), Serve::Respond(ORIGIN_HEAD));
        let proxy = Proxy::start(Script::tunnel(TUNNEL_200, &origin));
        let runtime = start(1, "rtn-raw");
        let target = origin_target(&proxy, &origin, &identity);
        let response = block_on(
            runtime
                .client(target)
                .with_raw_headers()
                .send(destination_request(&origin, HANG_GUARD)),
        )
        .expect("tunnel send");
        assert_eq!(response.status, 200);
        assert_complete(&response, 2);
        assert!(response.headers.is_empty(), "{:?}", response.headers);
        let raw = response.raw_headers.as_ref().expect("raw headers");
        assert_eq!(raw.get_str("x-origin"), Some("first"));
        assert!(raw.contains_key("content-length"));
        assert_eq!(response.dropped_headers, 0);
        assert_eq!(
            head_status(response.connect_head.as_deref()),
            Some(200),
            "the CONNECT head stays as it was"
        );
    }

    #[test]
    fn connect_head_keeps_each_delivered_name_spelling() {
        const HEAD: &[u8] = b"HTTP/1.1 200 Connection established\r\n\
            proxy-status: fwdproxy; e_proxy=\"edge\"\r\n\
            x-Fb-fwdproxy-DEBUG: proxy-ip=\"10.0.0.1\"\r\n\r\n";
        let identity = localhost();
        let origin = Origin::start(Some(&identity.server), Serve::Respond(ORIGIN_OK));
        let proxy = Proxy::start(Script::tunnel(HEAD, &origin));
        let runtime = start(1, "rtn-case");
        let response = send(
            &runtime,
            &origin_target(&proxy, &origin, &identity),
            destination_request(&origin, HANG_GUARD),
        )
        .expect("tunnel send");
        let head = response.connect_head.as_deref().expect("connect head");
        assert_eq!(
            lines(head),
            [
                ("Proxy-Status", "fwdproxy; e_proxy=\"edge\""),
                ("x-Fb-fwdproxy-DEBUG", "proxy-ip=\"10.0.0.1\""),
            ]
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
        );
        for name in ["proxy-status", "PROXY-STATUS", "Proxy-Status"] {
            assert_eq!(
                head.get(name).map(|value| &value[..]),
                Some(&b"fwdproxy; e_proxy=\"edge\""[..]),
                "{name}"
            );
        }
        for name in ["x-fb-fwdproxy-debug", "X-FB-Fwdproxy-Debug"] {
            assert_eq!(
                head.get(name).map(|value| &value[..]),
                Some(&b"proxy-ip=\"10.0.0.1\""[..]),
                "{name}"
            );
        }
        assert_eq!(head.get("via"), None);
    }

    /// Sends through a tunnel whose CONNECT names `authority`, and returns the CONNECT the proxy
    /// read and the request the origin read.
    fn connect_as(
        authority: &str,
        connect_headers: &[(&'static str, &'static str)],
    ) -> (RawConnect, RawConnect) {
        let identity = localhost();
        let origin = Origin::start(Some(&identity.server), Serve::Respond(ORIGIN_OK));
        let proxy = Proxy::start(Script::tunnel(TUNNEL_200, &origin));
        let runtime = start(1, "rtn-auth");
        let tunnel =
            try_tunnel(authority, connect_headers, &identity.client, "localhost").expect("tunnel");
        let response = send(
            &runtime,
            &target(proxy.address, tunnel),
            destination_request(&origin, HANG_GUARD),
        )
        .expect("tunnel send");
        assert_complete(&response, 2);
        let connections = proxy.connections();
        assert_eq!(connections.len(), 1, "one CONNECT: {connections:?}");
        let origins = origin.connections();
        assert_eq!(origins.len(), 1, "{origins:?}");
        (
            connections[0].connect.clone().expect("CONNECT"),
            origins[0].request.clone().expect("request"),
        )
    }

    #[test]
    fn v4_and_host_port_authorities_reach_the_proxy_as_given() {
        for authority in ["127.0.0.1:8443", "fwd.does-not-exist.invalid:443"] {
            let (connect, inner) = connect_as(authority, &[]);
            assert_eq!(
                connect.request_line,
                format!("CONNECT {authority} HTTP/1.1")
            );
            assert_eq!(connect.values("host"), [authority]);
            let host = inner.values("host");
            assert_eq!(host.len(), 1, "{inner:?}");
            assert!(
                host[0].starts_with(&format!("{DESTINATION}:")),
                "the origin reads the request authority: {inner:?}"
            );
        }
    }

    #[test]
    fn bracketed_v6_authority_reaches_the_proxy_as_given() {
        let (connect, inner) = connect_as("[::1]:8443", &[]);
        assert_eq!(connect.request_line, "CONNECT [::1]:8443 HTTP/1.1");
        assert_eq!(connect.values("host"), ["[::1]:8443"]);
        assert!(
            inner.values("host")[0].starts_with(DESTINATION),
            "{inner:?}"
        );
    }

    #[test]
    fn connect_request_carries_exactly_one_host_line() {
        let (connect, inner) = connect_as(
            "127.0.0.1:8443",
            &[
                ("host", "elsewhere.invalid:443"),
                ("x-probe", "on"),
                ("user-agent", "SyntheticE2E/2.0"),
                ("x-fb-fwdproxy-debug", "True"),
            ],
        );
        assert_eq!(connect.values("host"), ["127.0.0.1:8443"]);
        assert_eq!(connect.values("x-probe"), ["on"]);
        assert_eq!(connect.values("user-agent"), ["SyntheticE2E/2.0"]);
        assert_eq!(connect.values("x-fb-fwdproxy-debug"), ["True"]);
        // Host first, then the tunnel's headers in key order, then the codec's own Connection.
        let names: Vec<_> = connect
            .headers
            .iter()
            .map(|(name, _)| name.to_ascii_lowercase())
            .collect();
        assert_eq!(
            names,
            [
                "host",
                "user-agent",
                "x-fb-fwdproxy-debug",
                "x-probe",
                "connection"
            ],
            "{connect:?}"
        );
        for name in ["x-probe", "x-fb-fwdproxy-debug", "user-agent"] {
            assert!(inner.values(name).is_empty(), "{name} leaked: {inner:?}");
        }
        assert_eq!(inner.values("host").len(), 1, "{inner:?}");
    }

    /// A refused `Tunnel` never reaches a send, so nothing connects to the proxy: only `Ok`
    /// yields the `Tunnel` that `Security::Tunnel` needs, so no runtime check can observe it.
    fn assert_refused(result: Result<Tunnel, ExplicitError>, phase: Phase, what: &str) {
        let error = result.expect_err(what);
        assert_eq!(
            (error.phase, error.kind, error.reached_wire),
            (phase, ErrorKind::Protocol, false),
            "{what}: {error}"
        );
        assert!(error.connect_head.is_none(), "{what}");
    }

    #[test]
    fn tunnel_new_refuses_bad_authorities_headers_and_alpn() {
        let identity = localhost();
        let config = &identity.client;
        // Each parses as an `http::uri::Authority`; `Tunnel::new` refuses it.
        for authority in [
            "example.com",
            "user@example.com:443",
            ":443",
            "[::1]",
            "example.com:",
        ] {
            let parsed = Authority::try_from(authority).expect("a parsed authority");
            let result = Tunnel::new(parsed, Vec::new(), Arc::clone(config), "localhost");
            assert_refused(result, Phase::Submit, authority);
        }
        assert_refused(
            try_tunnel(
                "127.0.0.1:443",
                &[("x-debug", "a"), ("x-debug", "b")],
                config,
                "localhost",
            ),
            Phase::Submit,
            "a repeated name",
        );
        for name in [
            "connection",
            "content-length",
            "transfer-encoding",
            "upgrade",
        ] {
            assert_refused(
                Tunnel::new(
                    Authority::from_static("127.0.0.1:443"),
                    vec![(HeaderName::from_static(name), HeaderValue::from_static("1"))],
                    Arc::clone(config),
                    "localhost",
                ),
                Phase::Submit,
                name,
            );
        }
        for offer in [
            vec![b"h2".to_vec()],
            vec![b"http/1.1".to_vec(), b"h2".to_vec()],
        ] {
            let mut offering = (**config).clone();
            offering.alpn_protocols = offer;
            assert_refused(
                try_tunnel("127.0.0.1:443", &[], &Arc::new(offering), "localhost"),
                Phase::Tls,
                "an ALPN offer other than http/1.1",
            );
        }
        // Accepted: a dropped Host, no ALPN, exactly http/1.1, and an IP-literal TLS host.
        try_tunnel(
            "127.0.0.1:443",
            &[("host", "elsewhere:1")],
            config,
            "localhost",
        )
        .expect("host");
        let mut http11 = (**config).clone();
        http11.alpn_protocols = vec![b"http/1.1".to_vec()];
        try_tunnel("[::1]:443", &[], &Arc::new(http11), "127.0.0.1").expect("http/1.1");
        try_tunnel("h.invalid:443", &[], config, "[::1]").expect("bracketed literal");
    }

    #[test]
    fn authority_with_bytes_past_its_end_is_refused() {
        let identity = localhost();
        let injected = Authority::from_static("example.com:443/ HTTP/1.1\r\nX-Injected:443");
        assert_eq!(
            injected.as_str(),
            "example.com:443/ HTTP/1.1\r\nX-Injected:443"
        );
        let authorities = [
            injected,
            Authority::from_static("example.com:443/path"),
            Authority::from_static("example.com:443?query"),
            Authority::from_static("example.com:443#fragment"),
        ];
        for authority in authorities {
            let shown = authority.as_str().to_owned();
            assert_refused(
                Tunnel::new(
                    authority,
                    Vec::new(),
                    Arc::clone(&identity.client),
                    "localhost",
                ),
                Phase::Submit,
                &shown,
            );
        }
    }

    #[test]
    fn tunnel_tls_verifies_tls_host_and_sends_sni_only_for_names() {
        const CONNECTED_ELSEWHERE: &[u8] =
            b"HTTP/1.1 200 Connection established\r\nX-Connected-To: 127.0.0.9\r\n\r\n";
        let identity = localhost();
        let runtime = start(1, "rtn-sni");
        for (tls_host, sni) in [("localhost", Some("localhost")), ("127.0.0.1", None)] {
            let origin = Origin::start(Some(&identity.server), Serve::Respond(ORIGIN_OK));
            let proxy = Proxy::start(Script::tunnel(CONNECTED_ELSEWHERE, &origin));
            let tunnel = try_tunnel(
                &format!("127.0.0.1:{}", origin.port()),
                &[],
                &identity.client,
                tls_host,
            )
            .expect("tunnel");
            let response = send(
                &runtime,
                &target(proxy.address, tunnel),
                destination_request(&origin, HANG_GUARD),
            )
            .expect("tunnel send");
            assert_complete(&response, 2);
            assert_eq!(origin.connections()[0].sni.as_deref(), sni, "{tls_host}");
        }

        // A certificate that the configuration trusts, issued for another name.
        let other = identity_for_other_name();
        let origin = Origin::start(Some(&other.server), Serve::Respond(ORIGIN_OK));
        let proxy = Proxy::start(Script::tunnel(TUNNEL_200, &origin));
        let error = send(
            &runtime,
            &origin_target(&proxy, &origin, &other),
            destination_request(&origin, HANG_GUARD),
        )
        .expect_err("wrong name");
        assert_eq!(
            (error.phase, error.kind, error.reached_wire),
            (Phase::Tls, ErrorKind::TlsVerification, false),
            "{error}"
        );
        assert_eq!(head_status(error.connect_head.as_deref()), Some(200));
        assert_eq!(origin.requests(), 0);
    }

    fn identity_for_other_name() -> Identity {
        identity(&["other.does-not-exist.invalid"], false)
    }

    // ---------------------------------------------------------------------------------------
    // expectation-354: failures, phases, kinds, and heads.

    fn assert_failure(
        error: &ExplicitError,
        phase: Phase,
        kind: ErrorKind,
        head: Option<u16>,
        what: &str,
    ) {
        assert_eq!(
            (
                error.phase,
                error.kind,
                head_status(error.connect_head.as_deref())
            ),
            (phase, kind, head),
            "{what}: {error}"
        );
    }

    #[test]
    fn refused_proxy_is_proxy_connect_refused() {
        let identity = localhost();
        let runtime = start(1, "rtn-ref");
        let (refusing, _held) = refusing_address();
        let zero_port: SocketAddr = "127.0.0.1:0".parse().expect("address");
        for proxy in [refusing, zero_port] {
            let tunnel =
                try_tunnel("127.0.0.1:443", &[], &identity.client, "localhost").expect("tunnel");
            let error = send(
                &runtime,
                &target(proxy, tunnel),
                request(DESTINATION, "/", HANG_GUARD),
            )
            .expect_err("refused");
            assert_failure(
                &error,
                Phase::ProxyConnect,
                ErrorKind::Refused,
                None,
                "refused",
            );
            assert!(!error.reached_wire);
            assert_eq!(error.io_kind, Some(io::ErrorKind::ConnectionRefused));
        }
    }

    #[test]
    fn silent_proxy_is_tunnel_connect_timeout_without_a_head() {
        let identity = localhost();
        let proxy = Proxy::start(Script::Silent);
        let runtime = start(1, "rtn-sil");
        let tunnel =
            try_tunnel("127.0.0.1:443", &[], &identity.client, "localhost").expect("tunnel");
        let error = send(
            &runtime,
            &target(proxy.address, tunnel),
            request(DESTINATION, "/", EXPIRY_BUDGET),
        )
        .expect_err("timeout");
        assert_failure(
            &error,
            Phase::TunnelConnect,
            ErrorKind::Timeout,
            None,
            "silent",
        );
        assert!(!error.reached_wire);
        assert!(proxy.connection(0).connect.is_some());
    }

    #[test]
    fn proxy_close_reset_or_malformed_head_is_tunnel_connect() {
        let identity = localhost();
        let runtime = start(1, "rtn-bad");
        let cases: [(Script, ErrorKind, HttpErrorCode, Option<io::ErrorKind>); 5] = [
            (
                Script::End {
                    bytes: b"HTTP/1.1 200 Connection established\r\n",
                    after_client_bytes: false,
                    reset: false,
                },
                ErrorKind::RemoteClosed,
                HttpErrorCode::TRANSPORT_EOF,
                None,
            ),
            (
                Script::End {
                    bytes: b"",
                    after_client_bytes: false,
                    reset: false,
                },
                ErrorKind::RemoteClosed,
                HttpErrorCode::TRANSPORT_EOF,
                None,
            ),
            (
                Script::End {
                    bytes: b"",
                    after_client_bytes: false,
                    reset: true,
                },
                ErrorKind::Reset,
                HttpErrorCode::TRANSPORT_READ_ERROR,
                Some(io::ErrorKind::ConnectionReset),
            ),
            (
                Script::Answer(b"HTTP/1.1 200 OK\r\nX-Bad: a\x7fb\r\n\r\n".to_vec()),
                ErrorKind::Protocol,
                HttpErrorCode::PROTOCOL_ERROR,
                None,
            ),
            (
                Script::Answer(b"HTTP/1.1 200 OK\r\nno colon here\r\n\r\n".to_vec()),
                ErrorKind::Other,
                HttpErrorCode::HEADER_PARSE_ERROR,
                None,
            ),
        ];
        for (index, (script, kind, code, io_kind)) in cases.into_iter().enumerate() {
            let proxy = Proxy::start(script);
            let tunnel =
                try_tunnel("127.0.0.1:443", &[], &identity.client, "localhost").expect("tunnel");
            let error = send(
                &runtime,
                &target(proxy.address, tunnel),
                request(DESTINATION, "/", HANG_GUARD),
            )
            .expect_err("tunnel failure");
            let what = format!("case {index}");
            assert_failure(&error, Phase::TunnelConnect, kind, None, &what);
            assert_eq!(error.code, Some(code.as_u16()), "{what}: {error}");
            if io_kind.is_some() {
                assert_eq!(error.io_kind, io_kind, "{what}: {error}");
            }
            assert!(!error.reached_wire, "{what}");
        }
    }

    #[test]
    fn non_2xx_connect_is_tunnel_rejected_with_its_head() {
        let identity = localhost();
        let origin = Origin::start(Some(&identity.server), Serve::Respond(ORIGIN_OK));
        let runtime = start(1, "rtn-rej");
        let rejections: [(&[u8], u16, HttpErrorCode); 5] = [
            (
                b"HTTP/1.1 403 Forbidden\r\nProxy-Status: fwdproxy; error=destination_ip_prohibited\r\nX-FB-Error-From: fwdproxy\r\nContent-Length: 0\r\n\r\n",
                403,
                HttpErrorCode::CONNECT_ERROR,
            ),
            (
                b"HTTP/1.1 403 Forbidden\r\nProxy-Status: fwdproxy; error=destination_ip_prohibited\r\nX-FB-Error-From: fwdproxy\r\n\r\n",
                403,
                HttpErrorCode::CONNECT_ERROR,
            ),
            (
                b"HTTP/1.1 502 Bad Gateway\r\nProxy-Status: fwdproxy; error=dns_error\r\nX-FB-Error-From: fwdproxy\r\nContent-Length: 0\r\n\r\n",
                502,
                HttpErrorCode::CONNECT_ERROR,
            ),
            (
                b"HTTP/1.1 502 Bad Gateway\r\nProxy-Status: fwdproxy; error=dns_error\r\nX-FB-Error-From: fwdproxy\r\n\r\n",
                502,
                HttpErrorCode::CONNECT_ERROR,
            ),
            (
                b"HTTP/1.1 403 Forbidden\r\nProxy-Status: fwdproxy; error=destination_ip_prohibited\r\nX-FB-Error-From: fwdproxy\r\nContent-Length: 19\r\n\r\nnineteen bytes here",
                403,
                HttpErrorCode::CONTENT_LENGTH_MISMATCH,
            ),
        ];
        for (index, (answer, status, code)) in rejections.into_iter().enumerate() {
            let proxy = Proxy::start(Script::Answer(answer.to_vec()));
            let error = send(
                &runtime,
                &origin_target(&proxy, &origin, &identity),
                destination_request(&origin, LONG_BUDGET),
            )
            .expect_err("rejected");
            let what = format!("rejection {index}");
            assert_failure(
                &error,
                Phase::TunnelConnect,
                ErrorKind::TunnelRejected,
                Some(status),
                &what,
            );
            assert_eq!(error.code, Some(code.as_u16()), "{what}");
            assert!(!error.reached_wire, "{what}");
            let head = error.connect_head.as_deref().expect("head");
            assert!(head.get("proxy-status").is_some(), "{what}: {head:?}");
            assert_eq!(
                head.get("x-fb-error-from").map(|value| &value[..]),
                Some(&b"fwdproxy"[..]),
                "{what}"
            );
            wait_for_teardowns(&runtime);
            proxy.wait_closed(0);
        }
        assert!(
            origin.connections().is_empty(),
            "the origin was never dialed"
        );
    }

    #[test]
    fn destination_tls_failure_after_a_200_carries_the_head() {
        let identity = localhost();
        let origin = Origin::start(Some(&identity.server), Serve::Respond(ORIGIN_OK));
        let proxy = Proxy::start(Script::tunnel(TUNNEL_200, &origin));
        let runtime = start(1, "rtn-tls");
        let tunnel = try_tunnel(
            &format!("127.0.0.1:{}", origin.port()),
            &[],
            &untrusting_client(),
            "localhost",
        )
        .expect("tunnel");
        let error = send(
            &runtime,
            &target(proxy.address, tunnel),
            destination_request(&origin, LONG_BUDGET),
        )
        .expect_err("untrusted");
        assert_failure(
            &error,
            Phase::Tls,
            ErrorKind::TlsVerification,
            Some(200),
            "untrusted",
        );
        assert!(!error.reached_wire);
        assert_eq!(proxy.connections().len(), 1);
        assert!(proxy.connection(0).connect.is_some());
        assert_eq!(origin.requests(), 0);
        wait_for_teardowns(&runtime);
        proxy.wait_closed(0);
    }

    /// A listener whose accept queue is full, so a connect to it hangs.
    fn full_accept_queue() -> (SocketAddr, std::net::TcpListener, Vec<std::net::TcpStream>) {
        block_on(async {
            let socket = tokio::net::TcpSocket::new_v4().expect("socket");
            socket
                .bind("127.0.0.1:0".parse().expect("address"))
                .expect("bind");
            let listener = socket.listen(0).expect("listen");
            let address = listener.local_addr().expect("listener address");
            let mut queued = Vec::new();
            while let Ok(Ok(stream)) = tokio::time::timeout(
                Duration::from_millis(200),
                tokio::net::TcpStream::connect(address),
            )
            .await
            {
                queued.push(stream.into_std().expect("std stream"));
                assert!(queued.len() < 16, "the accept queue never filled");
            }
            (address, listener.into_std().expect("std listener"), queued)
        })
    }

    #[test]
    fn deadline_expiry_keeps_each_tunnel_stage() {
        let identity = localhost();
        let runtime = start(1, "rtn-exp");

        // Proxy dial.
        let (full, _listener, _queued) = full_accept_queue();
        let tunnel =
            try_tunnel("127.0.0.1:443", &[], &identity.client, "localhost").expect("tunnel");
        let error = send(
            &runtime,
            &target(full, tunnel),
            request(DESTINATION, "/", EXPIRY_BUDGET),
        )
        .expect_err("proxy dial expiry");
        assert_failure(
            &error,
            Phase::ProxyConnect,
            ErrorKind::Timeout,
            None,
            "dial",
        );
        assert_eq!(error.code, None);

        // Held after the proxy attach until the deadline passed: no CONNECT is sent.
        let proxy = Proxy::start(Script::Silent);
        let pause = TestPause::new();
        let _release = ReleaseOnDrop(pause.clone());
        *runtime.hooks().proxy_attached_pause.lock().unwrap() = Some(pause.clone());
        let send_request = request(DESTINATION, "/", EXPIRY_BUDGET);
        let deadline = send_request.deadline;
        let releaser = std::thread::spawn(move || {
            pause.wait_reached(1);
            sleep_past(deadline);
            pause.release();
        });
        let tunnel =
            try_tunnel("127.0.0.1:443", &[], &identity.client, "localhost").expect("tunnel");
        let error = send(&runtime, &target(proxy.address, tunnel), send_request)
            .expect_err("held past the deadline");
        releaser.join().expect("releaser");
        *runtime.hooks().proxy_attached_pause.lock().unwrap() = None;
        assert_failure(
            &error,
            Phase::TunnelConnect,
            ErrorKind::Timeout,
            None,
            "held",
        );
        assert!(!error.reached_wire);
        proxy.wait_closed(0);
        assert!(proxy.connection(0).connect.is_none(), "no CONNECT was sent");
        wait_for_teardowns(&runtime);

        // Tunnel TLS: the origin never answers the ClientHello.
        let silent = Origin::start(None, Serve::Silent);
        let proxy = Proxy::start(Script::tunnel(TUNNEL_200, &silent));
        let error = send(
            &runtime,
            &origin_target(&proxy, &silent, &identity),
            destination_request(&silent, EXPIRY_BUDGET),
        )
        .expect_err("handshake expiry");
        assert_failure(
            &error,
            Phase::Tls,
            ErrorKind::Timeout,
            Some(200),
            "handshake",
        );
        assert!(!error.reached_wire);

        // The inner exchange: a head held past the deadline, and a body stalled mid-way.
        let holding = Origin::start(Some(&identity.server), Serve::Hold);
        let proxy = Proxy::start(Script::tunnel(TUNNEL_200, &holding));
        let error = send(
            &runtime,
            &origin_target(&proxy, &holding, &identity),
            destination_request(&holding, EXPIRY_BUDGET),
        )
        .expect_err("head expiry");
        assert_failure(&error, Phase::Read, ErrorKind::Timeout, Some(200), "head");
        assert_eq!(error.code, None);
        assert!(error.reached_wire);

        let stalling = Origin::start(Some(&identity.server), Serve::Respond(ORIGIN_PARTIAL));
        let proxy = Proxy::start(Script::tunnel(TUNNEL_200, &stalling));
        let response = send(
            &runtime,
            &origin_target(&proxy, &stalling, &identity),
            destination_request(&stalling, EXPIRY_BUDGET),
        )
        .expect("head arrives");
        let (cause, head) = truncated_cause(response);
        assert_eq!(
            (cause.phase, cause.kind),
            (Phase::Body, ErrorKind::Timeout),
            "{cause}"
        );
        assert_eq!(head_status(head.as_deref()), Some(200));
    }

    #[test]
    fn destination_errors_after_a_200_carry_the_head() {
        const SHORT: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort";
        let identity = localhost();
        let runtime = start(1, "rtn-dst");
        let row = |error: &ExplicitError| (error.phase, error.kind, error.code, error.io_kind);
        let eof = Some(HttpErrorCode::TRANSPORT_EOF.as_u16());
        for close in [Close::Notify, Close::Fin] {
            // rustls reports a FIN without close_notify as UnexpectedEof.
            let io_kind = (close == Close::Fin).then_some(io::ErrorKind::UnexpectedEof);
            for serve in [
                Serve::CloseBeforeHead(close),
                Serve::RespondAndClose(SHORT, close),
            ] {
                let before_head = matches!(serve, Serve::CloseBeforeHead(_));
                let phase = if before_head {
                    Phase::Read
                } else {
                    Phase::Body
                };
                let what = format!("{close:?}, {phase:?}");
                let origin = Origin::start(Some(&identity.server), serve);
                let proxy = Proxy::start(Script::closing(&origin, close));
                let started = Instant::now();
                let (error, head) = ending(
                    send(
                        &runtime,
                        &origin_target(&proxy, &origin, &identity),
                        destination_request(&origin, LONG_BUDGET),
                    ),
                    before_head,
                );
                assert!(started.elapsed() < HANG_GUARD, "{what}: not the deadline");
                assert_eq!(
                    row(&error),
                    (phase, ErrorKind::RemoteClosed, eof, io_kind),
                    "{what}: {error}"
                );
                assert!(error.reached_wire, "{what}");
                assert_eq!(head_status(head.as_deref()), Some(200), "{what}");
                wait_for_teardowns(&runtime);

                // A direct TLS send to the same origin ends the same way.
                let (direct, _) = ending(
                    send(
                        &runtime,
                        &tls_target(origin.address, &identity.client, "localhost"),
                        destination_request(&origin, LONG_BUDGET),
                    ),
                    before_head,
                );
                assert_eq!(row(&direct), row(&error), "{what}: {direct}");
            }
        }
    }

    /// A send's error before the head, or its truncated body's cause after it, with the head.
    fn ending(
        result: Result<Response, ExplicitError>,
        before_head: bool,
    ) -> (ExplicitError, Option<Box<ConnectHead>>) {
        if before_head {
            let error = result.expect_err("an error before the head");
            let head = error.connect_head.clone();
            (error, head)
        } else {
            truncated_cause(result.expect("the head arrives"))
        }
    }

    #[test]
    fn proxy_reset_or_close_after_the_200_keeps_its_kind() {
        let identity = localhost();
        let runtime = start(1, "rtn-pxy");
        for (reset, kind) in [(true, ErrorKind::Reset), (false, ErrorKind::RemoteClosed)] {
            let proxy = Proxy::start(Script::End {
                bytes: TUNNEL_200,
                after_client_bytes: true,
                reset,
            });
            let tunnel =
                try_tunnel("127.0.0.1:443", &[], &identity.client, "localhost").expect("tunnel");
            let error = send(
                &runtime,
                &target(proxy.address, tunnel),
                request(DESTINATION, "/", HANG_GUARD),
            )
            .expect_err("proxy ended the tunnel");
            assert_failure(
                &error,
                Phase::Tls,
                kind,
                Some(200),
                &format!("reset {reset}"),
            );
            assert!(!error.reached_wire);
            if reset {
                assert_eq!(
                    error.code,
                    Some(HttpErrorCode::TRANSPORT_READ_ERROR.as_u16()),
                    "{error}"
                );
                assert_eq!(error.io_kind, Some(io::ErrorKind::ConnectionReset));
            }
        }

        // A reset once the origin's head has been relayed and read.
        let stalling = Origin::start(Some(&identity.server), Serve::Respond(ORIGIN_PARTIAL));
        let trigger = Arc::new(Notify::new());
        let proxy = Proxy::start(Script::Tunnel {
            head: TUNNEL_200,
            origin: stalling.address,
            reset: Some(Arc::clone(&trigger)),
            pass_fin: false,
        });
        let counted = Arc::new(AtomicU64::new(0));
        runtime.set_exchange_hooks(ExchangeHooks {
            body_counted: Some(Arc::clone(&counted)),
            ..ExchangeHooks::default()
        });
        let target = origin_target(&proxy, &stalling, &identity);
        let mut client = runtime.client(target);
        let response = block_on(async {
            let pending = tokio::spawn(async move {
                client
                    .send(destination_request(&stalling, LONG_BUDGET))
                    .await
            });
            while counted.load(Ordering::Acquire) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            trigger.notify_one();
            pending.await.expect("send task")
        })
        .expect("head arrives");
        let (cause, head) = truncated_cause(response);
        assert_eq!(
            (cause.phase, cause.kind, cause.io_kind),
            (
                Phase::Body,
                ErrorKind::Reset,
                Some(io::ErrorKind::ConnectionReset)
            ),
            "{cause}"
        );
        assert_eq!(
            cause.code,
            Some(HttpErrorCode::TRANSPORT_READ_ERROR.as_u16())
        );
        assert_eq!(head_status(head.as_deref()), Some(200));
        runtime.set_exchange_hooks(ExchangeHooks::default());
        wait_for_teardowns(&runtime);
    }

    #[test]
    fn proxy_and_inner_sessions_take_their_timeouts_from_the_deadline() {
        let identity = localhost();
        let origin = Origin::start(Some(&identity.server), Serve::Respond(ORIGIN_OK));
        let proxy = Proxy::start(Script::tunnel(TUNNEL_200, &origin));
        let runtime = start(1, "rtn-par");
        let probe = Arc::new(ExchangeProbe::default());
        runtime.set_exchange_hooks(probing(&probe));
        let budget = Duration::from_secs(30);
        let send_request = destination_request(&origin, budget);
        let deadline = send_request.deadline;
        let response = send(
            &runtime,
            &origin_target(&proxy, &origin, &identity),
            send_request,
        )
        .expect("tunnel send");
        let left_after = deadline.saturating_duration_since(Instant::now());
        assert_complete(&response, 2);
        let records = probe.attach_records.lock().unwrap().clone();
        assert_eq!(
            records.len(),
            2,
            "the proxy attach and the inner attach: {records:?}"
        );
        for record in &records {
            let timeout = record.stream_read_timeout.expect("stream read timeout");
            assert_eq!(record.max_concurrent_outgoing_streams, Some(1));
            assert_eq!(record.connection_read_timeout, Some(timeout));
            assert_eq!(record.write_timeout, Some(timeout));
            assert!(
                left_after <= timeout && timeout <= budget,
                "{timeout:?} is not the time left when attached"
            );
            assert!(timeout > Duration::from_secs(5), "{timeout:?}");
        }
        assert!(records[0].stream_read_timeout >= records[1].stream_read_timeout);
        let limits = ByteStreamLimits::new(16384, 16384, 1).expect("limits");
        assert_eq!(*probe.adoption_limits.lock().unwrap(), [limits, limits]);
        assert_eq!(probe.deadline_sleeps.load(Ordering::SeqCst), 1);
        assert_eq!(probe.validations.load(Ordering::SeqCst), 1);
    }

    /// A real CONNECT that the proxy answers with `answer`, classified on the worker.
    fn classified_connect(answer: &'static [u8], observed: Observed) -> ExplicitError {
        let proxy = Proxy::start(Script::Answer(answer.to_vec()));
        let address = proxy.address;
        install_crypto_provider();
        let tunnel = Arc::new(
            try_tunnel("10.0.0.1:443", &[], &untrusting_client(), "localhost").expect("tunnel"),
        );
        run_on_tokio_driver(move |context, _| async move {
            let tcp = tokio::net::TcpStream::connect(address)
                .await
                .expect("proxy");
            let local = tcp.local_addr().expect("local");
            let limits = ByteStreamLimits::new(16384, 16384, 1).expect("limits");
            let (transport, _control) = adopt_local_with_completion(
                &context,
                EstablishedIo::new(tcp, endpoint(local), endpoint(address), None),
                limits,
            )
            .expect("adopted");
            let session = roxygen_client::connector::ClientSession::attach_transport(
                &context,
                transport,
                roxygen_client::connector::ClientAttachOptions::new(
                    roxygen_core::http::CodecProtocol::Http11,
                    roxygen_core::connection::TransportSecurity::Plaintext,
                ),
            )
            .expect("attached");
            let connect = crate::tunnel::connect(
                &context,
                &session,
                &tunnel,
                Duration::from_secs(5),
                CancellationToken::never(),
            )
            .expect("reserved");
            let error = match connect.await {
                Ok(_) => panic!("the CONNECT succeeded"),
                Err(error) => error,
            };
            session.cancel();
            ExplicitError::from_connect(&error, observed)
        })
    }

    fn response_message(status: u16) -> HttpMessage {
        let mut message = HttpMessage::new();
        message.set_status_code(status).expect("a response");
        message.headers_mut().add("Proxy-Status", "fwdproxy");
        message
    }

    fn wrapped(code: HttpErrorCode, cause: Option<io::ErrorKind>) -> io::Error {
        let mut error = HttpError::with_message(code, b"injected");
        if let Some(kind) = cause {
            error = error.with_cause(Arc::new(io::Error::from(kind)));
        }
        io::Error::other(error)
    }

    fn reset_cause(code: HttpErrorCode) -> HttpError {
        HttpError::with_message(code, b"injected")
            .with_cause(Arc::new(io::Error::from(io::ErrorKind::ConnectionReset)))
    }

    #[test]
    fn tunnel_classification_matrix_keeps_phase_kind_code_and_head() {
        let row = |error: &ExplicitError| {
            (
                error.phase,
                error.kind,
                error.code,
                error.io_kind,
                head_status(error.connect_head.as_deref()),
                error.reached_wire,
            )
        };
        let code = |code: HttpErrorCode| Some(code.as_u16());
        let passed = Observed {
            deadline_passed: true,
            ..OPEN
        };
        let cancelled = Observed {
            own_cancelled: true,
            ..OPEN
        };

        // Proxy dial: the Connect kind rules in ProxyConnect.
        for (io_kind, kind) in [
            (io::ErrorKind::ConnectionRefused, ErrorKind::Refused),
            (io::ErrorKind::ConnectionReset, ErrorKind::Reset),
            (io::ErrorKind::ConnectionAborted, ErrorKind::Reset),
            (io::ErrorKind::TimedOut, ErrorKind::Timeout),
            (io::ErrorKind::AddrNotAvailable, ErrorKind::Other),
        ] {
            let error = ExplicitError::from_connect_io(&io::Error::from(io_kind))
                .in_phase(Phase::ProxyConnect);
            assert_eq!(
                row(&error),
                (Phase::ProxyConnect, kind, None, Some(io_kind), None, false)
            );
        }

        // Proxy leg setup and the inner leg's setup keep a direct attach's kinds.
        for (position, phase) in [
            (Position::ProxyAttach, Phase::ProxyConnect),
            (Position::TunnelAttach, Phase::TunnelConnect),
        ] {
            for (error, kind) in [
                (HttpError::new(HttpErrorCode::DROPPED), ErrorKind::Other),
                (
                    reset_cause(HttpErrorCode::TRANSPORT_READ_ERROR),
                    ErrorKind::Reset,
                ),
                (
                    HttpError::new(HttpErrorCode::TRANSPORT_EOF),
                    ErrorKind::RemoteClosed,
                ),
            ] {
                let classified = ExplicitError::from_http(&error, position, OPEN);
                assert_eq!(
                    (classified.phase, classified.kind, classified.reached_wire),
                    (phase, kind, false)
                );
            }
            let closed = Observed {
                worker_open: false,
                ..OPEN
            };
            let lost =
                ExplicitError::from_http(&HttpError::new(HttpErrorCode::DROPPED), position, closed);
            assert_eq!((lost.phase, lost.kind), (phase, ErrorKind::WorkerLost));
        }
        for phase in [Phase::ProxyConnect, Phase::TunnelConnect] {
            let error = ExplicitError::from_cleanup_registration(
                &roxygen_runtime::worker::CleanupRegistrationError::Closed,
                phase,
            );
            assert_eq!((error.phase, error.kind), (phase, ErrorKind::WorkerLost));
        }

        // CONNECT commit: TunnelConnect, never Write.
        let connect = |error: HttpError, observed| ExplicitError::from_connect(&error, observed);
        assert_eq!(
            row(&connect(HttpError::new(HttpErrorCode::WRITE_TIMEOUT), OPEN)),
            (
                Phase::TunnelConnect,
                ErrorKind::Timeout,
                code(HttpErrorCode::WRITE_TIMEOUT),
                None,
                None,
                false
            )
        );
        assert_eq!(
            row(&connect(
                HttpError::new(HttpErrorCode::WRITE_TIMEOUT),
                passed
            )),
            (
                Phase::TunnelConnect,
                ErrorKind::Timeout,
                None,
                None,
                None,
                false
            )
        );
        assert_eq!(
            row(&connect(
                reset_cause(HttpErrorCode::TRANSPORT_WRITE_ERROR),
                OPEN
            )),
            (
                Phase::TunnelConnect,
                ErrorKind::Reset,
                code(HttpErrorCode::TRANSPORT_WRITE_ERROR),
                Some(io::ErrorKind::ConnectionReset),
                None,
                false
            )
        );
        assert_eq!(
            row(&connect(
                HttpError::new(HttpErrorCode::TRANSPORT_WRITE_ERROR),
                OPEN
            )),
            (
                Phase::TunnelConnect,
                ErrorKind::Other,
                code(HttpErrorCode::TRANSPORT_WRITE_ERROR),
                None,
                None,
                false
            )
        );

        // CONNECT head wait.
        for (error, observed, kind, has_code) in [
            (
                HttpError::new(HttpErrorCode::READ_TIMEOUT),
                OPEN,
                ErrorKind::Timeout,
                true,
            ),
            (
                HttpError::new(HttpErrorCode::READ_TIMEOUT),
                passed,
                ErrorKind::Timeout,
                false,
            ),
            (
                HttpError::new(HttpErrorCode::TRANSPORT_EOF),
                OPEN,
                ErrorKind::RemoteClosed,
                true,
            ),
            (
                reset_cause(HttpErrorCode::TRANSPORT_READ_ERROR),
                OPEN,
                ErrorKind::Reset,
                true,
            ),
            (
                HttpError::new(HttpErrorCode::PROTOCOL_ERROR),
                OPEN,
                ErrorKind::Protocol,
                true,
            ),
            (
                HttpError::new(HttpErrorCode::CORO_CANCELLED),
                cancelled,
                ErrorKind::Cancelled,
                true,
            ),
            (
                HttpError::new(HttpErrorCode::CORO_CANCELLED),
                OPEN,
                ErrorKind::Other,
                true,
            ),
            (
                HttpError::new(HttpErrorCode::INTERNAL_ERROR),
                OPEN,
                ErrorKind::Other,
                true,
            ),
        ] {
            let expected_code = has_code.then(|| error.code().as_u16());
            let classified = connect(error, observed);
            assert_eq!(
                (
                    classified.phase,
                    classified.kind,
                    classified.code,
                    head_status(classified.connect_head.as_deref()),
                    classified.reached_wire
                ),
                (Phase::TunnelConnect, kind, expected_code, None, false),
                "{classified}"
            );
        }

        // Rejections: a typed cause, or the HTTP/1.1 limit with a final non-2xx head.
        let rejected = classified_connect(
            b"HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Status: fwdproxy\r\nContent-Length: 0\r\n\r\n",
            OPEN,
        );
        assert_eq!(
            row(&rejected),
            (
                Phase::TunnelConnect,
                ErrorKind::TunnelRejected,
                code(HttpErrorCode::CONNECT_ERROR),
                None,
                Some(407),
                false
            )
        );
        assert_eq!(
            lines(rejected.connect_head.as_deref().expect("head"))[0].0,
            "Proxy-Status"
        );
        let limited = HttpError::with_message(HttpErrorCode::CONTENT_LENGTH_MISMATCH, b"limit")
            .with_http_message(response_message(403));
        assert_eq!(
            row(&connect(limited, OPEN)),
            (
                Phase::TunnelConnect,
                ErrorKind::TunnelRejected,
                code(HttpErrorCode::CONTENT_LENGTH_MISMATCH),
                None,
                Some(403),
                false
            )
        );
        for message in [Some(response_message(200)), None] {
            let mut error =
                HttpError::with_message(HttpErrorCode::CONTENT_LENGTH_MISMATCH, b"limit");
            error.set_http_message(message);
            assert_eq!(
                row(&connect(error, OPEN)),
                (
                    Phase::TunnelConnect,
                    ErrorKind::Other,
                    code(HttpErrorCode::CONTENT_LENGTH_MISMATCH),
                    None,
                    None,
                    false
                )
            );
        }

        // Proxy-session failures that cross the tunnel as a wrapped HttpError.
        let tls = |error: io::Error, observed| ExplicitError::from_tunnel_tls_io(&error, observed);
        assert_eq!(
            row(&tls(
                wrapped(
                    HttpErrorCode::TRANSPORT_READ_ERROR,
                    Some(io::ErrorKind::ConnectionReset)
                ),
                OPEN
            )),
            (
                Phase::Tls,
                ErrorKind::Reset,
                code(HttpErrorCode::TRANSPORT_READ_ERROR),
                Some(io::ErrorKind::ConnectionReset),
                None,
                false
            )
        );
        assert_eq!(
            row(&tls(wrapped(HttpErrorCode::TRANSPORT_EOF, None), OPEN)),
            (
                Phase::Tls,
                ErrorKind::RemoteClosed,
                code(HttpErrorCode::TRANSPORT_EOF),
                None,
                None,
                false
            )
        );
        assert_eq!(
            row(&tls(wrapped(HttpErrorCode::READ_TIMEOUT, None), OPEN)),
            (
                Phase::Tls,
                ErrorKind::Timeout,
                code(HttpErrorCode::READ_TIMEOUT),
                None,
                None,
                false
            )
        );
        assert_eq!(
            row(&tls(wrapped(HttpErrorCode::READ_TIMEOUT, None), passed)),
            (Phase::Tls, ErrorKind::Timeout, None, None, None, false)
        );
        let inner = |code: HttpErrorCode, cause: io::Error| {
            HttpError::with_message(code, b"inner").with_cause(Arc::new(cause))
        };
        for (position, inner_code, phase) in [
            (
                Position::BeforeHead,
                HttpErrorCode::TRANSPORT_READ_ERROR,
                Phase::Read,
            ),
            (
                Position::BeforeHead,
                HttpErrorCode::TRANSPORT_WRITE_ERROR,
                Phase::Write,
            ),
            (
                Position::AfterHead,
                HttpErrorCode::TRANSPORT_READ_ERROR,
                Phase::Body,
            ),
        ] {
            let error = inner(
                inner_code,
                wrapped(
                    HttpErrorCode::TRANSPORT_READ_ERROR,
                    Some(io::ErrorKind::ConnectionReset),
                ),
            );
            assert_eq!(
                row(&ExplicitError::from_http(&error, position, OPEN)),
                (
                    phase,
                    ErrorKind::Reset,
                    code(inner_code),
                    Some(io::ErrorKind::ConnectionReset),
                    None,
                    true
                )
            );
        }
        let eof = inner(
            HttpErrorCode::TRANSPORT_READ_ERROR,
            wrapped(HttpErrorCode::TRANSPORT_EOF, None),
        );
        assert_eq!(
            ExplicitError::from_http(&eof, Position::AfterHead, OPEN).kind,
            ErrorKind::RemoteClosed
        );
    }

    // ---------------------------------------------------------------------------------------
    // expectation-355: both legs end with the exchange.

    #[test]
    fn proxy_session_closes_after_each_exchange() {
        let identity = localhost();
        let runtime = start(1, "rtn-seq");

        // Sequential sends: one proxy connection, client port, and origin connection each.
        let origin = Origin::start(Some(&identity.server), Serve::Respond(ORIGIN_OK));
        let proxy = Proxy::start(Script::tunnel(TUNNEL_200, &origin));
        let target = origin_target(&proxy, &origin, &identity);
        for index in 0..3 {
            let response =
                send(&runtime, &target, destination_request(&origin, LONG_BUDGET)).expect("send");
            assert_complete(&response, 2);
            proxy.wait_closed(index);
            wait_for_teardowns(&runtime);
        }
        let ports: std::collections::BTreeSet<_> = proxy
            .connections()
            .iter()
            .map(|connection| connection.client_port)
            .collect();
        assert_eq!(ports.len(), 3);
        let origins = origin.connections();
        let origin_ports: std::collections::BTreeSet<_> = origins
            .iter()
            .map(|connection| connection.peer_port)
            .collect();
        assert_eq!(origin_ports.len(), 3);
        wait_until("every origin connection ends", || {
            origin
                .connections()
                .iter()
                .all(|connection| connection.end.is_some())
        });

        // Failures before the inner session.
        let failures: Vec<(Script, Duration)> = vec![
            (
                Script::Answer(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n".to_vec()),
                LONG_BUDGET,
            ),
            (
                Script::Answer(
                    b"HTTP/1.1 403 Forbidden\r\nContent-Length: 19\r\n\r\nnineteen bytes here"
                        .to_vec(),
                ),
                LONG_BUDGET,
            ),
            (Script::Silent, EXPIRY_BUDGET),
        ];
        for (index, (script, budget)) in failures.into_iter().enumerate() {
            let proxy = Proxy::start(script);
            let error = send(
                &runtime,
                &origin_target(&proxy, &origin, &identity),
                destination_request(&origin, budget),
            )
            .expect_err("failure before the inner session");
            assert_eq!(
                error.phase,
                Phase::TunnelConnect,
                "failure {index}: {error}"
            );
            proxy.wait_closed(0);
            wait_for_teardowns(&runtime);
        }

        // Failures after the 200 and before the inner session.
        let untrusted = Proxy::start(Script::tunnel(TUNNEL_200, &origin));
        let tunnel = try_tunnel(
            &format!("127.0.0.1:{}", origin.port()),
            &[],
            &untrusting_client(),
            "localhost",
        )
        .expect("tunnel");
        let error = send(
            &runtime,
            &target_for(&untrusted, tunnel),
            destination_request(&origin, LONG_BUDGET),
        )
        .expect_err("untrusted");
        assert_eq!(error.kind, ErrorKind::TlsVerification);
        untrusted.wait_closed(0);
        wait_for_teardowns(&runtime);

        let silent = Origin::start(None, Serve::Silent);
        let proxy = Proxy::start(Script::tunnel(TUNNEL_200, &silent));
        let error = send(
            &runtime,
            &origin_target(&proxy, &silent, &identity),
            destination_request(&silent, EXPIRY_BUDGET),
        )
        .expect_err("handshake expiry");
        assert_eq!((error.phase, error.kind), (Phase::Tls, ErrorKind::Timeout));
        proxy.wait_closed(0);
        wait_for_teardowns(&runtime);

        // Failures after the inner hand-off that return normally.
        let after_handoff: [(Serve, Duration); 4] = [
            (Serve::CloseBeforeHead(Close::Notify), LONG_BUDGET),
            (
                Serve::RespondAndClose(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort",
                    Close::Notify,
                ),
                LONG_BUDGET,
            ),
            (Serve::Hold, EXPIRY_BUDGET),
            (Serve::Respond(ORIGIN_PARTIAL), EXPIRY_BUDGET),
        ];
        for (index, (serve, budget)) in after_handoff.into_iter().enumerate() {
            let failing = Origin::start(Some(&identity.server), serve);
            let proxy = Proxy::start(Script::tunnel(TUNNEL_200, &failing));
            let result = send(
                &runtime,
                &origin_target(&proxy, &failing, &identity),
                destination_request(&failing, budget),
            );
            match result {
                Ok(response) => {
                    truncated_cause(response);
                }
                Err(error) => assert!(error.reached_wire, "case {index}: {error}"),
            }
            proxy.wait_closed(0);
            wait_for_teardowns(&runtime);
        }

        // Two tunnels in flight on one worker keep their teardowns apart.
        let release = Arc::new(AtomicBool::new(false));
        let releasing = Origin::start(
            Some(&identity.server),
            Serve::RespondInTwo {
                first: b"HTTP/1.1 200 OK\r\nContent-Length: 12\r\n\r\npartial",
                rest: b"-rest",
                release: Arc::clone(&release),
            },
        );
        let proxy = Proxy::start(Script::Answer(
            b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n".to_vec(),
        ));
        proxy.push(Script::tunnel(TUNNEL_200, &releasing));
        let counted = Arc::new(AtomicU64::new(0));
        runtime.set_exchange_hooks(ExchangeHooks {
            body_counted: Some(Arc::clone(&counted)),
            ..ExchangeHooks::default()
        });
        let target = origin_target(&proxy, &releasing, &identity);
        let mut held = runtime.client(Arc::clone(&target));
        let held_request = destination_request(&releasing, LONG_BUDGET);
        let response = block_on(async {
            let pending = tokio::spawn(async move { held.send(held_request).await });
            while counted.load(Ordering::Acquire) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let rejected = runtime
                .client(Arc::clone(&target))
                .send(destination_request(&releasing, LONG_BUDGET))
                .await
                .expect_err("rejected");
            assert_eq!(rejected.kind, ErrorKind::TunnelRejected);
            tokio::task::spawn_blocking({
                let proxy_connections = Arc::clone(&proxy.shared);
                move || {
                    assert!(eventually(|| proxy_connections.connections.lock().unwrap()
                        [1]
                    .closed()));
                }
            })
            .await
            .expect("blocking wait");
            assert!(eventually(|| pending_teardowns(&runtime) == 2));
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert_eq!(pending_teardowns(&runtime), 2);
            assert_eq!(
                proxy.connection(0).read_end,
                None,
                "the held tunnel stays open"
            );
            release.store(true, Ordering::SeqCst);
            pending.await.expect("send task")
        })
        .expect("the held send succeeds");
        assert_complete(&response, 12);
        runtime.set_exchange_hooks(ExchangeHooks::default());
        proxy.wait_closed(0);
        wait_for_teardowns(&runtime);

        // A proxy reset mid-response leaves the proxy no socket to test.
        let stalling = Origin::start(Some(&identity.server), Serve::Respond(ORIGIN_PARTIAL));
        let trigger = Arc::new(Notify::new());
        let proxy = Proxy::start(Script::Tunnel {
            head: TUNNEL_200,
            origin: stalling.address,
            reset: Some(Arc::clone(&trigger)),
            pass_fin: false,
        });
        let counted = Arc::new(AtomicU64::new(0));
        runtime.set_exchange_hooks(ExchangeHooks {
            body_counted: Some(Arc::clone(&counted)),
            ..ExchangeHooks::default()
        });
        let mut client = runtime.client(origin_target(&proxy, &stalling, &identity));
        let response = block_on(async {
            let pending = tokio::spawn(async move {
                client
                    .send(destination_request(&stalling, LONG_BUDGET))
                    .await
            });
            while counted.load(Ordering::Acquire) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            trigger.notify_one();
            pending.await.expect("send task")
        })
        .expect("head arrives");
        truncated_cause(response);
        runtime.set_exchange_hooks(ExchangeHooks::default());
        wait_for_teardowns(&runtime);
    }

    fn target_for(proxy: &Proxy, tunnel: Tunnel) -> Arc<Target> {
        target(proxy.address, tunnel)
    }

    #[test]
    fn successful_send_closes_the_proxy_leg_after_close_notify_and_fin() {
        let identity = localhost();
        let runtime = start(1, "rtn-fin");
        let origin = Origin::start(Some(&identity.server), Serve::Respond(ORIGIN_OK));
        let proxy = Proxy::start(Script::tunnel(TUNNEL_200, &origin));
        let target = origin_target(&proxy, &origin, &identity);

        let response =
            send(&runtime, &target, destination_request(&origin, LONG_BUDGET)).expect("send");
        assert_complete(&response, 2);
        proxy.wait_closed(0);
        let connection = proxy.connection(0);
        assert_eq!(
            connection.read_end,
            Some(End::Eof),
            "a FIN, not a reset: {connection:?}"
        );
        wait_until("the origin's read ends", || {
            origin.connections()[0].end.is_some()
        });
        assert_eq!(
            origin.connections()[0].end,
            Some(End::Eof),
            "the origin's read ends at close_notify"
        );
        wait_for_teardowns(&runtime);

        // A held write shutdown keeps the proxy leg open until it is released.
        let pause = TestPause::new();
        let _release = ReleaseOnDrop(pause.clone());
        *runtime.hooks().proxy_shutdown_pause.lock().unwrap() = Some(pause.clone());
        let response =
            send(&runtime, &target, destination_request(&origin, LONG_BUDGET)).expect("send");
        assert_complete(&response, 2);
        pause.wait_reached(1);
        std::thread::sleep(Duration::from_millis(200));
        assert!(pending_teardowns(&runtime) >= 1);
        assert_eq!(
            proxy.connection(1).read_end,
            None,
            "neither EOF nor a reset while held"
        );
        pause.release();
        proxy.wait_closed(1);
        assert_eq!(proxy.connection(1).read_end, Some(End::Eof));
        wait_for_teardowns(&runtime);

        // Held past a short deadline, the connection closes at the deadline.
        let pause = TestPause::new();
        let _release_late = ReleaseOnDrop(pause.clone());
        *runtime.hooks().proxy_shutdown_pause.lock().unwrap() = Some(pause.clone());
        let send_request = destination_request(&origin, EXPIRY_BUDGET);
        let deadline = send_request.deadline;
        let response = send(&runtime, &target, send_request).expect("send");
        assert_complete(&response, 2);
        pause.wait_reached(1);
        proxy.wait_closed(2);
        assert!(Instant::now() >= deadline, "closed only at the deadline");
        *runtime.hooks().proxy_shutdown_pause.lock().unwrap() = None;
        pause.release();
        wait_for_teardowns(&runtime);
    }

    #[test]
    fn dropped_send_closes_the_proxy_connection_in_every_stage() {
        let identity = localhost();
        let runtime = start(1, "rtn-drop");

        // While the proxy holds the CONNECT head.
        let proxy = Proxy::start(Script::Silent);
        let tunnel =
            try_tunnel("127.0.0.1:443", &[], &identity.client, "localhost").expect("tunnel");
        let mut client = runtime.client(target(proxy.address, tunnel));
        block_on(async {
            let pending =
                tokio::spawn(
                    async move { client.send(request(DESTINATION, "/", LONG_BUDGET)).await },
                );
            let shared = Arc::clone(&proxy.shared);
            tokio::task::spawn_blocking(move || {
                wait_until("the CONNECT", || {
                    shared
                        .connections
                        .lock()
                        .unwrap()
                        .first()
                        .is_some_and(|c| c.connect.is_some())
                });
            })
            .await
            .expect("blocking wait");
            pending.abort();
            assert!(pending.await.expect_err("aborted").is_cancelled());
        });
        proxy.wait_closed(0);

        // While the origin holds the tunnel TLS handshake.
        let silent = Origin::start(None, Serve::Silent);
        let proxy = Proxy::start(Script::tunnel(TUNNEL_200, &silent));
        let mut client = runtime.client(origin_target(&proxy, &silent, &identity));
        let silent_connections = Arc::clone(&silent.connections);
        block_on(async {
            let pending =
                tokio::spawn(
                    async move { client.send(request(DESTINATION, "/", LONG_BUDGET)).await },
                );
            tokio::task::spawn_blocking(move || {
                wait_until("the ClientHello", || {
                    silent_connections
                        .lock()
                        .unwrap()
                        .first()
                        .is_some_and(|c| c.raw_bytes > 0)
                });
            })
            .await
            .expect("blocking wait");
            pending.abort();
            assert!(pending.await.expect_err("aborted").is_cancelled());
        });
        proxy.wait_closed(0);

        // Mid-response.
        let stalling = Origin::start(Some(&identity.server), Serve::Respond(ORIGIN_PARTIAL));
        let proxy = Proxy::start(Script::tunnel(TUNNEL_200, &stalling));
        let counted = Arc::new(AtomicU64::new(0));
        runtime.set_exchange_hooks(ExchangeHooks {
            body_counted: Some(Arc::clone(&counted)),
            ..ExchangeHooks::default()
        });
        let mut client = runtime.client(origin_target(&proxy, &stalling, &identity));
        let stalling_request = destination_request(&stalling, LONG_BUDGET);
        block_on(async {
            let pending = tokio::spawn(async move { client.send(stalling_request).await });
            while counted.load(Ordering::Acquire) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            pending.abort();
            assert!(pending.await.expect_err("aborted").is_cancelled());
        });
        runtime.set_exchange_hooks(ExchangeHooks::default());
        proxy.wait_closed(0);
        wait_for_teardowns(&runtime);

        assert_eq!(runtime.stats().caller_cancellations, 3);
        let jobs = runtime.hooks().job_errors.lock().unwrap().clone();
        let outcomes: Vec<_> = jobs
            .iter()
            .map(|(phase, kind, reached, _)| (*phase, *kind, *reached))
            .collect();
        assert_eq!(outcomes.len(), 3, "{jobs:?}");
        assert_eq!(
            outcomes[0],
            (Phase::TunnelConnect, ErrorKind::Cancelled, false)
        );
        assert_eq!(outcomes[1], (Phase::Tls, ErrorKind::Cancelled, false));
        assert!(
            matches!(
                outcomes[2],
                (Phase::Read | Phase::Body, ErrorKind::Cancelled, true)
            ),
            "{jobs:?}"
        );
    }

    #[test]
    fn panic_after_the_connect_closes_the_proxy_connection() {
        let identity = localhost();
        let runtime = start(1, "rtn-pan");
        let origin = Origin::start(Some(&identity.server), Serve::Hold);

        let proxy = Proxy::start(Script::tunnel(TUNNEL_200, &origin));
        runtime.set_exchange_hooks(ExchangeHooks {
            panic_before_handoff: true,
            ..ExchangeHooks::default()
        });
        let started = Instant::now();
        let error = send(
            &runtime,
            &origin_target(&proxy, &origin, &identity),
            destination_request(&origin, LONG_BUDGET),
        )
        .expect_err("panic");
        assert_eq!(
            (error.phase, error.kind, error.reached_wire),
            (Phase::TunnelConnect, ErrorKind::Panicked, false),
            "{error}"
        );
        assert!(error.connect_head.is_none());
        proxy.wait_closed(0);
        wait_for_teardowns(&runtime);

        let proxy = Proxy::start(Script::tunnel(TUNNEL_200, &origin));
        let pause = TestPause::new();
        let _release = ReleaseOnDrop(pause.clone());
        runtime.set_exchange_hooks(ExchangeHooks {
            before_head: Some(pause.clone()),
            panic_before_head: true,
            ..ExchangeHooks::default()
        });
        let mut client = runtime.client(origin_target(&proxy, &origin, &identity));
        let held_request = destination_request(&origin, LONG_BUDGET);
        let error = block_on(async {
            let pending = tokio::spawn(async move { client.send(held_request).await });
            pause.reached_async(1).await;
            pause.release();
            pending.await.expect("send task")
        })
        .expect_err("panic");
        assert_eq!(
            (error.phase, error.kind, error.reached_wire),
            (Phase::Read, ErrorKind::Panicked, true),
            "{error}"
        );
        proxy.wait_closed(0);
        wait_for_teardowns(&runtime);
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "closed long before the deadline"
        );
        runtime.set_exchange_hooks(ExchangeHooks::default());
        assert_eq!(runtime.stats().panics, 2);
    }

    #[test]
    fn tunnel_cleanup_dropped_unpolled_cancels_the_proxy_session() {
        for send_first in [true, false] {
            let proxy = Proxy::start(Script::Silent);
            let address = proxy.address;
            let shared = Arc::clone(&proxy.shared);
            let closed = run_on_tokio_driver(move |context, _| async move {
                let tcp = tokio::net::TcpStream::connect(address)
                    .await
                    .expect("proxy");
                let deadline = context.host().now() + LONG_BUDGET;
                let hooks = RuntimeHooks::default();
                let setup = LegSetup {
                    context: &context,
                    deadline,
                    cancellation: &CancellationToken::never(),
                    stats: &Arc::new(ExplicitStats::new()),
                    exchange_hooks: &ExchangeHooks::default(),
                    hooks: &hooks,
                    teardown_watch: TeardownWatch::CleanupTask,
                };
                let leg = attach_proxy_leg(&setup, tcp, address).expect("proxy leg");
                let (inner_io, _peer_io) = tokio::io::duplex(1024);
                let limits = ByteStreamLimits::new(16384, 16384, 1).expect("limits");
                let (_inner, inner_control) = adopt_local_with_completion(
                    &context,
                    EstablishedIo::new(inner_io, endpoint(address), endpoint(address), None),
                    limits,
                )
                .expect("inner adopted");
                let (handoff, receipt) = oneshot::channel::<TunnelHandoff>();
                let cleanup = tunnel_cleanup(
                    &context,
                    receipt,
                    leg.mark.clone(),
                    leg.completion.clone(),
                    deadline,
                );
                // A second clone, as `connect_unique`'s is in a live tunnel, keeps the last
                // handle's drop from draining the session, so only the guard's cancel closes it.
                let held = leg.guard.session().clone();
                let handed = TunnelHandoff {
                    inner: inner_control.completion(),
                    guard: leg.guard,
                };
                if send_first {
                    assert!(handoff.send(handed).is_ok());
                    drop(cleanup);
                } else {
                    drop(cleanup);
                    assert!(handoff.send(handed).is_err());
                }
                let shared = Arc::clone(&shared);
                let closed = tokio::task::spawn_blocking(move || {
                    eventually(|| {
                        shared
                            .connections
                            .lock()
                            .unwrap()
                            .first()
                            .is_some_and(Connection::closed)
                    })
                })
                .await
                .expect("blocking wait");
                drop(held);
                closed
            });
            assert!(closed, "{:?}", proxy.connections());
        }
    }

    #[test]
    fn proxy_leg_reports_the_dialed_proxy_address() {
        let proxy = Proxy::start(Script::Silent);
        let address = proxy.address;
        let (leg_local, leg_peer, local) = run_on_tokio_driver(move |context, _| async move {
            let tcp = tokio::net::TcpStream::connect(address)
                .await
                .expect("proxy");
            let local = tcp.local_addr().expect("local address");
            let hooks = RuntimeHooks::default();
            let setup = LegSetup {
                context: &context,
                deadline: context.host().now() + LONG_BUDGET,
                cancellation: &CancellationToken::never(),
                stats: &Arc::new(ExplicitStats::new()),
                exchange_hooks: &ExchangeHooks::default(),
                hooks: &hooks,
                teardown_watch: TeardownWatch::CleanupTask,
            };
            let leg = attach_proxy_leg(&setup, tcp, address).expect("proxy leg");
            (leg.local, leg.peer, local)
        });
        assert_eq!(leg_peer, Some(address));
        assert_eq!(leg_local, Some(local));
    }

    #[test]
    fn proxy_leg_reports_an_unspecified_dialed_proxy_address() {
        let proxy = Proxy::start(Script::Silent);
        // Linux connects a dial of the unspecified address to loopback.
        let dialed = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), proxy.address.port());
        let (leg_peer, kernel_peer) = run_on_tokio_driver(move |context, _| async move {
            let tcp = tokio::net::TcpStream::connect(dialed).await.expect("proxy");
            let kernel_peer = tcp.peer_addr().expect("kernel peer address");
            let hooks = RuntimeHooks::default();
            let setup = LegSetup {
                context: &context,
                deadline: context.host().now() + LONG_BUDGET,
                cancellation: &CancellationToken::never(),
                stats: &Arc::new(ExplicitStats::new()),
                exchange_hooks: &ExchangeHooks::default(),
                hooks: &hooks,
                teardown_watch: TeardownWatch::CleanupTask,
            };
            let leg = attach_proxy_leg(&setup, tcp, dialed).expect("proxy leg");
            (leg.peer, kernel_peer)
        });
        assert_eq!(kernel_peer, proxy.address);
        assert_eq!(leg_peer, Some(dialed));
    }

    #[test]
    fn deadline_closes_the_proxy_connection_when_inner_retirement_stalls() {
        let identity = localhost();
        let runtime = start(1, "rtn-stl");
        let origin = Origin::start(Some(&identity.server), Serve::Respond(ORIGIN_OK));
        let proxy = Proxy::start(Script::tunnel(TUNNEL_200, &origin));
        let pause = TestPause::new();
        let _release = ReleaseOnDrop(pause.clone());
        *runtime.hooks().inner_shutdown_pause.lock().unwrap() = Some(pause.clone());
        let send_request = destination_request(&origin, EXPIRY_BUDGET);
        let deadline = send_request.deadline;
        let response = send(
            &runtime,
            &origin_target(&proxy, &origin, &identity),
            send_request,
        )
        .expect("send");
        assert_complete(&response, 2);
        pause.wait_reached(1);
        assert!(
            pending_teardowns(&runtime) >= 2,
            "both legs are live while the inner stalls"
        );
        proxy.wait_closed(0);
        assert!(Instant::now() >= deadline, "closed at the deadline");
        assert!(
            pending_teardowns(&runtime) >= 1,
            "the stalled inner stream has not retired"
        );
        *runtime.hooks().inner_shutdown_pause.lock().unwrap() = None;
        pause.release();
        wait_for_teardowns(&runtime);
    }

    #[test]
    fn teardown_guards_count_both_legs_and_keep_a_stalled_inner_stream_counted() {
        let identity = localhost();
        let runtime = start(1, "rtn-tgd");
        // A cleanup task told not to confirm would count each leg as unconfirmed; guarded streams
        // register none.
        runtime.set_exchange_hooks(ExchangeHooks {
            unconfirmed_retirement: true,
            ..ExchangeHooks::default()
        });
        let origin = Origin::start(Some(&identity.server), Serve::Respond(ORIGIN_OK));
        let proxy = Proxy::start(Script::tunnel(TUNNEL_200, &origin));
        let pause = TestPause::new();
        let _release = ReleaseOnDrop(pause.clone());
        *runtime.hooks().inner_shutdown_pause.lock().unwrap() = Some(pause.clone());
        let send_request = destination_request(&origin, EXPIRY_BUDGET);
        let deadline = send_request.deadline;
        let mut client = runtime
            .client(origin_target(&proxy, &origin, &identity))
            .with_teardown_guard();
        let response = block_on(client.send(send_request)).expect("send");
        assert_complete(&response, 2);
        pause.wait_reached(1);
        assert_eq!(
            pending_teardowns(&runtime),
            2,
            "both legs are live while the inner stalls"
        );
        proxy.wait_closed(0);
        assert!(Instant::now() >= deadline, "closed at the deadline");
        wait_until("the proxy leg's stream is destroyed", || {
            pending_teardowns(&runtime) == 1
        });
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(
            pending_teardowns(&runtime),
            1,
            "the stalled inner stream stays counted"
        );
        *runtime.hooks().inner_shutdown_pause.lock().unwrap() = None;
        pause.release();
        wait_for_teardowns(&runtime);
        assert_eq!(runtime.stats().close_to_retirement_us.count, 0);
        runtime.set_exchange_hooks(ExchangeHooks::default());
    }

    #[test]
    fn runtime_drop_with_a_live_tunnel_joins_and_closes_the_proxy_connection() {
        let identity = localhost();

        // During the CONNECT head wait.
        let proxy = Proxy::start(Script::Silent);
        let runtime = start(1, "rtn-rd1");
        let tunnel =
            try_tunnel("127.0.0.1:443", &[], &identity.client, "localhost").expect("tunnel");
        let mut client = runtime.client(target(proxy.address, tunnel));
        let shared = Arc::clone(&proxy.shared);
        let error = block_on(async move {
            let pending =
                tokio::spawn(
                    async move { client.send(request(DESTINATION, "/", LONG_BUDGET)).await },
                );
            tokio::task::spawn_blocking(move || {
                wait_until("the CONNECT", || {
                    shared
                        .connections
                        .lock()
                        .unwrap()
                        .first()
                        .is_some_and(|c| c.connect.is_some())
                });
                let started = Instant::now();
                drop(runtime);
                assert!(
                    started.elapsed() < HANG_GUARD,
                    "joined without waiting for the proxy"
                );
            })
            .await
            .expect("blocking drop");
            pending.await.expect("send task")
        })
        .expect_err("worker lost");
        assert_eq!(
            (error.phase, error.kind, error.reached_wire),
            (Phase::TunnelConnect, ErrorKind::WorkerLost, false),
            "{error}"
        );
        proxy.wait_closed(0);

        // Mid-response, after the origin's head.
        let stalling = Origin::start(Some(&identity.server), Serve::Respond(ORIGIN_PARTIAL));
        let proxy = Proxy::start(Script::tunnel(TUNNEL_200, &stalling));
        let runtime = start(1, "rtn-rd2");
        let counted = Arc::new(AtomicU64::new(0));
        runtime.set_exchange_hooks(ExchangeHooks {
            body_counted: Some(Arc::clone(&counted)),
            ..ExchangeHooks::default()
        });
        let mut client = runtime.client(origin_target(&proxy, &stalling, &identity));
        let held_request = destination_request(&stalling, LONG_BUDGET);
        block_on(async move {
            let pending = tokio::spawn(async move { client.send(held_request).await });
            while counted.load(Ordering::Acquire) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            tokio::task::spawn_blocking(move || {
                let started = Instant::now();
                drop(runtime);
                assert!(started.elapsed() < HANG_GUARD, "joined within the bound");
            })
            .await
            .expect("blocking drop");
            let _ = pending.await;
        });
        proxy.wait_closed(0);
    }
}
