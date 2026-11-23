/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A minimal HTTP/3 client for the server's tests: an mvfstr client driver on its own worker,
//! whose connections frame requests with `roxygen_h3::codec` and encode field sections with
//! `roxygen_qpack`. It sends requests whole, buffers responses whole and reports what arrived on
//! the server's control stream and what happened to the connection.

use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use bytes::BytesMut;
use mvfstr_driver::Conn;
use mvfstr_driver::ConnHandle;
use mvfstr_driver::ConnInfo;
use mvfstr_driver::EndpointDriver;
use mvfstr_driver::EndpointHandle;
use mvfstr_driver::QuicApplication;
use mvfstr_driver::QuicSession;
use mvfstr_driver::Refusal;
use mvfstr_driver::SendSink;
use mvfstr_io::MmsgBackend;
use mvfstr_proto::AppCode;
use mvfstr_proto::ClientConfig;
use mvfstr_proto::CloseReason;
use mvfstr_proto::ConnEvent;
use mvfstr_proto::ConnOptions;
use mvfstr_proto::Dir;
use mvfstr_proto::EndpointConfig;
use mvfstr_proto::SendOrder;
use mvfstr_proto::Side;
use mvfstr_proto::StreamId;
use mvfstr_proto::StreamReady;
use mvfstr_tls::ManualTimeProvider;
use mvfstr_tls::ServerVerification;
use roxygen_h3::codec::Frame;
use roxygen_h3::codec::FrameDecoder;
use roxygen_h3::codec::FrameLimits;
use roxygen_h3::codec::FrameType;
use roxygen_h3::codec::Role;
use roxygen_h3::codec::Settings;
use roxygen_h3::codec::StreamKind;
use roxygen_h3::codec::UniStreamHeader;
use roxygen_h3::codec::UniStreamHeaderDecoder;
use roxygen_h3::codec::encode_frame_header;
use roxygen_qpack::FieldLine;
use roxygen_runtime::tokio_host::ManagedWorker;
use roxygen_transport::tls::builtin_crypto_provider;
use rustls::NamedGroup;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::PrivateKeyDer;
use rustls::pki_types::PrivatePkcs8KeyDer;
use rustls::pki_types::ServerName;
use rustls::pki_types::UnixTime;

/// How long the tests wait for anything the server should do promptly.
pub(crate) const WAIT: Duration = Duration::from_secs(20);

/// H3_NO_ERROR, the HTTP/3 error code of a close without error (RFC 9114).
pub(crate) const H3_NO_ERROR: u32 = 0x100;

const REQUEST_ORDER: SendOrder = SendOrder {
    class: 1,
    urgency: 3,
    incremental: false,
    order: 0,
};

const CONTROL_ORDER: SendOrder = SendOrder {
    class: 0,
    urgency: 0,
    incremental: false,
    order: 0,
};

/// Generates a self-signed certificate for `localhost` and its key.
pub(crate) fn localhost_certificate() -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
    let key = rcgen::KeyPair::generate().expect("rcgen makes a key");
    let cert = rcgen::CertificateParams::new(vec!["localhost".to_owned()])
        .and_then(|params| params.self_signed(&key))
        .expect("rcgen makes a certificate");
    (
        vec![cert.der().clone()],
        PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
    )
}

/// A request the client sends whole, with a FIN after its last byte.
#[derive(Clone, Debug)]
pub(crate) struct Request {
    pub(crate) method: &'static str,
    pub(crate) path: String,
    pub(crate) body: Bytes,
    /// Fields after the pseudo-header fields.
    fields: Vec<(String, String)>,
    /// Where the rest of the body comes from, if the request waits for it before its FIN.
    tail: Option<BodyTail>,
}

/// The rest of a request's body, which the test hands over with [`H3Conn::finish_body`].
#[derive(Clone, Debug, Default)]
pub(crate) struct BodyTail(Arc<Mutex<Option<Bytes>>>);

impl Request {
    pub(crate) fn get(path: &str) -> Self {
        Self {
            method: "GET",
            path: path.to_owned(),
            body: Bytes::new(),
            fields: Vec::new(),
            tail: None,
        }
    }

    pub(crate) fn post(path: &str, body: impl Into<Bytes>) -> Self {
        Self {
            method: "POST",
            path: path.to_owned(),
            body: body.into(),
            fields: Vec::new(),
            tail: None,
        }
    }

    /// Returns the request with the field `name: value` added.
    pub(crate) fn with_field(mut self, name: &str, value: &str) -> Self {
        self.fields.push((name.to_owned(), value.to_owned()));
        self
    }

    /// Creates a POST whose body starts with `first` and whose rest, with the FIN, waits for the
    /// test to pass it to [`H3Conn::finish_body`] with the returned tail.
    pub(crate) fn unfinished_post(path: &str, first: impl Into<Bytes>) -> (Self, BodyTail) {
        let tail = BodyTail::default();
        let request = Self {
            tail: Some(tail.clone()),
            ..Self::post(path, first)
        };
        (request, tail)
    }

    fn encode(&self) -> Bytes {
        let mut fields = vec![
            (":method", self.method.to_owned()),
            (":scheme", "https".to_owned()),
            (":authority", "localhost".to_owned()),
            (":path", self.path.clone()),
        ];
        if !self.body.is_empty() && self.tail.is_none() {
            fields.push(("content-length", self.body.len().to_string()));
        }
        let mut block = BytesMut::new();
        roxygen_qpack::Encoder::new().encode_field_section(
            fields
                .iter()
                .map(|(name, value)| (*name, value.as_str()))
                .chain(
                    self.fields
                        .iter()
                        .map(|(name, value)| (name.as_str(), value.as_str())),
                )
                .map(|(name, value)| FieldLine::new(name.as_bytes(), value.as_bytes())),
            &mut block,
        );
        let mut out = BytesMut::new();
        encode_frame_header(&mut out, FrameType::HEADERS, block.len() as u64)
            .expect("a HEADERS header encodes");
        out.extend_from_slice(&block);
        if !self.body.is_empty() {
            encode_frame_header(&mut out, FrameType::DATA, self.body.len() as u64)
                .expect("a DATA header encodes");
            out.extend_from_slice(&self.body);
        }
        out.freeze()
    }
}

/// A response as the client decoded it.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Response {
    pub(crate) status: u16,
    /// The final head's fields other than `:status`, in order.
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Vec<u8>,
}

/// An event on a connection, reported in order.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Seen {
    Connected,
    /// The server's SETTINGS arrived.
    Settings,
    /// The server's GOAWAY with this stream ID arrived.
    Goaway(u64),
    Closed(CloseReason),
    Drained,
}

type Reply = mpsc::Sender<Result<Response, String>>;

/// A worker running one client driver, from which connections open.
pub(crate) struct H3Client {
    worker: ManagedWorker,
    config: Arc<ClientConfig>,
}

thread_local! {
    static DRIVER: RefCell<Option<EndpointHandle<ClientApp, MmsgBackend>>> =
        const { RefCell::new(None) };
}

impl H3Client {
    pub(crate) fn new() -> Self {
        Self::with_alpn(&[b"h3"])
    }

    /// Creates a client whose ClientHello takes two Initial packets: its ALPN list follows `h3`
    /// with protocols the server does not support.
    pub(crate) fn with_large_client_hello() -> Self {
        let padding = [b'x'; 200];
        let mut alpn: Vec<&[u8]> = vec![b"h3"];
        alpn.extend(std::iter::repeat_n(&padding[..], 8));
        Self::with_alpn(&alpn)
    }

    /// Creates a client that offers the ALPN protocols `alpn`.
    pub(crate) fn with_alpn(alpn: &[&[u8]]) -> Self {
        let worker = ManagedWorker::new(64).expect("a client worker");
        let (tx, rx) = mpsc::channel();
        worker
            .handle()
            .submit(move |context| {
                let endpoint = EndpointConfig::builder([9; 32])
                    .build()
                    .expect("an endpoint config");
                let handle = EndpointDriver::<_, MmsgBackend>::builder(endpoint, ClientApp)
                    .spawn(context)
                    .expect("the client driver starts");
                DRIVER.with(|driver| *driver.borrow_mut() = Some(handle));
                let _ = tx.send(());
            })
            .expect("the client worker takes commands");
        rx.recv_timeout(WAIT).expect("the client driver starts");
        let mut provider = builtin_crypto_provider();
        provider
            .kx_groups
            .retain(|group| group.name() == NamedGroup::X25519);
        let tls = mvfstr_tls::client_config(
            provider,
            Arc::new(ManualTimeProvider::new(UnixTime::now())),
            ServerVerification::AcceptAnyCertificate,
            alpn,
        )
        .expect("a client TLS config");
        let config = ClientConfig::builder(Arc::new(tls))
            .build()
            .expect("a client config");
        Self { worker, config }
    }

    /// Opens a connection to `addr`.
    pub(crate) fn connect(&self, addr: SocketAddr) -> H3Conn {
        let shared = Arc::new(Shared::default());
        let (seen_tx, seen) = mpsc::channel();
        let (tx, rx) = mpsc::channel();
        let config = Arc::clone(&self.config);
        let session = ClientSession::new(Arc::clone(&shared), seen_tx);
        self.worker
            .handle()
            .submit(move |_| {
                let conn = DRIVER.with(|driver| {
                    driver
                        .borrow()
                        .as_ref()
                        .expect("the client driver runs")
                        .connect(
                            addr,
                            ServerName::try_from("localhost").expect("a DNS name"),
                            config,
                            ConnOptions::default(),
                            session,
                        )
                        .map_err(|error| error.to_string())
                });
                let _ = tx.send(conn);
            })
            .expect("the client worker takes commands");
        let handle = rx
            .recv_timeout(WAIT)
            .expect("the client worker answers")
            .expect("the connection starts");
        H3Conn {
            handle,
            shared,
            seen,
        }
    }

    pub(crate) fn shutdown(self) {
        self.worker
            .shutdown()
            .expect("the client worker shuts down cleanly");
    }
}

/// The requests a connection has not opened yet, shared with the test thread.
#[derive(Default)]
struct Shared {
    queue: Mutex<VecDeque<(Request, Reply)>>,
}

/// One client connection, driven from the test thread.
pub(crate) struct H3Conn {
    handle: ConnHandle,
    shared: Arc<Shared>,
    seen: mpsc::Receiver<Seen>,
}

impl H3Conn {
    /// Sends `request` and returns the receiver of its response, or of why it failed.
    pub(crate) fn send(&self, request: Request) -> mpsc::Receiver<Result<Response, String>> {
        let (tx, rx) = mpsc::channel();
        self.shared
            .queue
            .lock()
            .expect("the queue lock is sound")
            .push_back((request, tx));
        let _ = self.handle.wake();
        rx
    }

    /// Sends `request` and waits for its response.
    pub(crate) fn fetch(&self, request: Request) -> Response {
        self.send(request)
            .recv_timeout(WAIT)
            .expect("a response arrives")
            .expect("the request succeeds")
    }

    /// Waits for an event that `wanted` accepts, and returns every event up to and including it.
    pub(crate) fn await_seen(&self, wanted: impl Fn(&Seen) -> bool) -> Vec<Seen> {
        let deadline = Instant::now() + WAIT;
        let mut seen = Vec::new();
        loop {
            let next = self
                .seen
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .unwrap_or_else(|_| panic!("still waiting; saw {seen:?}"));
            let done = wanted(&next);
            seen.push(next);
            if done {
                return seen;
            }
        }
    }

    /// Returns the events that have not been returned yet, without waiting.
    pub(crate) fn seen_now(&self) -> Vec<Seen> {
        self.seen.try_iter().collect()
    }

    /// Sends `rest` as the end of the body whose tail is `tail`, then the request's FIN.
    pub(crate) fn finish_body(&self, tail: &BodyTail, rest: impl Into<Bytes>) {
        *tail.0.lock().expect("the tail lock is sound") = Some(rest.into());
        let _ = self.handle.wake();
    }

    /// Closes the connection with H3_NO_ERROR.
    pub(crate) fn close(&self) {
        let _ = self
            .handle
            .close(AppCode::from_u32(H3_NO_ERROR), Bytes::new());
    }
}

struct ClientApp;

impl QuicApplication for ClientApp {
    type Session = ClientSession;

    fn accept(&mut self, _: &ConnInfo, _: &mut ConnOptions) -> Result<ClientSession, Refusal> {
        Err(Refusal::Ignore)
    }
}

/// One request stream.
struct Exchange {
    /// Request bytes the stream has not taken yet.
    out: Bytes,
    /// The rest of the body, until the test hands it over; the FIN waits for it.
    tail: Option<BodyTail>,
    fin_sent: bool,
    decoder: FrameDecoder,
    response: Option<Response>,
    reply: Reply,
}

/// A unidirectional stream the server opened.
enum PeerUni {
    Header(UniStreamHeaderDecoder),
    Control(FrameDecoder),
    Ignored,
}

struct ClientSession {
    shared: Arc<Shared>,
    seen: mpsc::Sender<Seen>,
    connected: bool,
    control: Option<StreamId>,
    control_out: Bytes,
    exchanges: HashMap<StreamId, Exchange>,
    peer_uni: HashMap<StreamId, PeerUni>,
}

impl ClientSession {
    fn new(shared: Arc<Shared>, seen: mpsc::Sender<Seen>) -> Self {
        let mut control_out = BytesMut::new();
        UniStreamHeader::Control
            .encode(&mut control_out)
            .expect("the control stream header encodes");
        Frame::Settings(Settings::new())
            .encode(&mut control_out)
            .expect("SETTINGS encodes");
        Self {
            shared,
            seen,
            connected: false,
            control: None,
            control_out: control_out.freeze(),
            exchanges: HashMap::new(),
            peer_uni: HashMap::new(),
        }
    }

    fn report(&self, seen: Seen) {
        let _ = self.seen.send(seen);
    }

    /// Opens the control stream once, and a request stream for each queued request while the
    /// server's stream limit allows.
    fn open_streams(&mut self, conn: &mut Conn<'_>) {
        if !self.connected {
            return;
        }
        if self.control.is_none()
            && let Ok(id) = conn.open_stream(Dir::Uni, CONTROL_ORDER)
        {
            self.control = Some(id);
            self.flush(conn, id);
        }
        loop {
            let Some((request, reply)) = self
                .shared
                .queue
                .lock()
                .expect("the queue lock is sound")
                .pop_front()
            else {
                return;
            };
            match conn.open_stream(Dir::Bi, REQUEST_ORDER) {
                Ok(id) => {
                    self.exchanges.insert(
                        id,
                        Exchange {
                            out: request.encode(),
                            tail: request.tail,
                            fin_sent: false,
                            decoder: FrameDecoder::new(
                                StreamKind::Request,
                                Role::Client,
                                FrameLimits::default(),
                            ),
                            response: None,
                            reply,
                        },
                    );
                    self.flush(conn, id);
                }
                Err(mvfstr_proto::OpenError::StreamLimit) => {
                    self.shared
                        .queue
                        .lock()
                        .expect("the queue lock is sound")
                        .push_front((request, reply));
                    return;
                }
                Err(error) => {
                    let _ = reply.send(Err(format!("no stream opened: {error}")));
                }
            }
        }
    }

    /// Hands the stream what it accepts of its pending bytes, then its FIN.
    fn flush(&mut self, conn: &mut Conn<'_>, id: StreamId) {
        let (out, fin_sent) = if Some(id) == self.control {
            (&mut self.control_out, None)
        } else if let Some(exchange) = self.exchanges.get_mut(&id) {
            let finished = exchange.tail.is_none();
            (
                &mut exchange.out,
                finished.then_some(&mut exchange.fin_sent),
            )
        } else {
            return;
        };
        let Ok(mut stream) = conn.stream(id) else {
            return;
        };
        if !out.is_empty() {
            let _ = stream.send(out);
        }
        if out.is_empty()
            && let Some(fin_sent) = fin_sent
            && !*fin_sent
            && stream.finish().is_ok()
        {
            *fin_sent = true;
        }
    }

    /// Appends the body tails the test has handed over to their requests' output.
    fn take_tails(&mut self, conn: &mut Conn<'_>) {
        let mut finished = Vec::new();
        for (&id, exchange) in &mut self.exchanges {
            let Some(rest) = exchange
                .tail
                .as_ref()
                .and_then(|tail| tail.0.lock().expect("the tail lock is sound").take())
            else {
                continue;
            };
            let mut out = BytesMut::from(&exchange.out[..]);
            encode_frame_header(&mut out, FrameType::DATA, rest.len() as u64)
                .expect("a DATA header encodes");
            out.extend_from_slice(&rest);
            exchange.out = out.freeze();
            exchange.tail = None;
            finished.push(id);
        }
        for id in finished {
            self.flush(conn, id);
        }
    }

    /// Takes every readable byte of stream `id`.
    fn take_readable(conn: &mut Conn<'_>, id: StreamId) -> Vec<Bytes> {
        let Ok(mut stream) = conn.stream(id) else {
            return Vec::new();
        };
        let segments: Vec<Bytes> = stream.readable().segments().cloned().collect();
        let len = segments.iter().map(Bytes::len).sum();
        if len > 0 {
            let _ = stream.consume(len);
        }
        segments
    }

    fn read(&mut self, conn: &mut Conn<'_>, id: StreamId, fin: bool) {
        let segments = Self::take_readable(conn, id);
        if id.dir() == Dir::Uni {
            if id.initiator() == Side::Server {
                self.read_peer_uni(id, segments);
            }
            return;
        }
        let Some(exchange) = self.exchanges.get_mut(&id) else {
            return;
        };
        for mut segment in segments {
            loop {
                match exchange.decoder.decode(&mut segment) {
                    Ok(Some(Frame::Headers(block))) => {
                        let mut status = None;
                        let mut headers = Vec::new();
                        let decoded =
                            roxygen_qpack::Decoder::new().decode_field_section(&block, |line| {
                                if line.name == b":status" {
                                    status = std::str::from_utf8(line.value)
                                        .ok()
                                        .and_then(|value| value.parse().ok());
                                } else {
                                    headers.push((
                                        String::from_utf8_lossy(line.name).into_owned(),
                                        String::from_utf8_lossy(line.value).into_owned(),
                                    ));
                                }
                            });
                        if decoded.is_err() {
                            let _ = exchange.reply.send(Err("a bad field section".to_owned()));
                            self.exchanges.remove(&id);
                            return;
                        }
                        if exchange.response.is_none() {
                            exchange.response = Some(Response {
                                status: status.unwrap_or(0),
                                headers,
                                body: Vec::new(),
                            });
                        }
                    }
                    Ok(Some(Frame::Data(data))) => {
                        if let Some(response) = exchange.response.as_mut() {
                            response.body.extend_from_slice(&data);
                        }
                    }
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(error) => {
                        let _ = exchange.reply.send(Err(format!("a bad frame: {error:?}")));
                        self.exchanges.remove(&id);
                        return;
                    }
                }
            }
        }
        if fin && let Some(exchange) = self.exchanges.remove(&id) {
            let _ = exchange.reply.send(
                exchange
                    .response
                    .ok_or_else(|| "the stream ended without a response".to_owned()),
            );
        }
    }

    fn read_peer_uni(&mut self, id: StreamId, segments: Vec<Bytes>) {
        let mut seen = Vec::new();
        let state = self
            .peer_uni
            .entry(id)
            .or_insert_with(|| PeerUni::Header(UniStreamHeaderDecoder::new(Role::Client)));
        for mut segment in segments {
            loop {
                match state {
                    PeerUni::Header(decoder) => match decoder.decode(&mut segment) {
                        Ok(Some(UniStreamHeader::Control)) => {
                            *state = PeerUni::Control(FrameDecoder::new(
                                StreamKind::Control,
                                Role::Client,
                                FrameLimits::default(),
                            ));
                        }
                        Ok(Some(_)) | Err(_) => *state = PeerUni::Ignored,
                        Ok(None) => break,
                    },
                    PeerUni::Control(decoder) => match decoder.decode(&mut segment) {
                        Ok(Some(Frame::Settings(_))) => seen.push(Seen::Settings),
                        Ok(Some(Frame::Goaway(id))) => seen.push(Seen::Goaway(id)),
                        Ok(Some(_)) => {}
                        Ok(None) | Err(_) => break,
                    },
                    PeerUni::Ignored => break,
                }
            }
        }
        for seen in seen {
            self.report(seen);
        }
    }
}

impl QuicSession for ClientSession {
    fn on_event(&mut self, conn: &mut Conn<'_>, event: ConnEvent) {
        match event {
            ConnEvent::Connected => {
                self.connected = true;
                self.report(Seen::Connected);
                self.open_streams(conn);
            }
            ConnEvent::StreamsAvailable { .. } => self.open_streams(conn),
            ConnEvent::Closed { reason, .. } => {
                let queued: Vec<_> = self
                    .shared
                    .queue
                    .lock()
                    .expect("the queue lock is sound")
                    .drain(..)
                    .map(|(_, reply)| reply)
                    .collect();
                let opened = self.exchanges.drain().map(|(_, exchange)| exchange.reply);
                for reply in opened.chain(queued) {
                    let _ = reply.send(Err(format!("the connection closed: {reason:?}")));
                }
                self.report(Seen::Closed(reason));
            }
            ConnEvent::Drained => self.report(Seen::Drained),
            _ => {}
        }
    }

    fn on_stream(&mut self, conn: &mut Conn<'_>, id: StreamId, ready: StreamReady) {
        match ready {
            StreamReady::Opened | StreamReady::Readable => self.read(conn, id, false),
            StreamReady::Finished => self.read(conn, id, true),
            StreamReady::Writable => self.flush(conn, id),
            StreamReady::Reset { code, .. } => {
                if let Some(exchange) = self.exchanges.remove(&id) {
                    let _ = exchange
                        .reply
                        .send(Err(format!("the server reset the stream: {code:?}")));
                }
            }
            StreamReady::Stopped { .. } | StreamReady::Closed => {}
        }
    }

    fn poll_send(&mut self, _: &mut Context<'_>, _: StreamId, _: &mut SendSink<'_>) -> Poll<()> {
        Poll::Pending
    }

    fn poll_session(&mut self, _: &mut Context<'_>, conn: &mut Conn<'_>) -> Poll<()> {
        self.take_tails(conn);
        self.open_streams(conn);
        Poll::Pending
    }
}
