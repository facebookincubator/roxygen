/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The HTTP/3 server session: an mvfstr [`QuicSession`] hosting Roxygen's [`OwnedApplication`]
//! unchanged (RFC 9114).
//!
//! [`H3Application`] accepts connections for one endpoint driver and creates an [`H3Session`] for
//! each. The session opens its control stream with SETTINGS, accepts the peer's control and QPACK
//! streams, and turns each request stream into one exchange of the application: it decodes the
//! request head through QPACK, feeds DATA to the exchange's ingress body, and returns stream
//! credit only as the application consumes the body. The body is built when the application
//! takes its completion, or else when the head is decoded, except that a request whose FIN
//! directly follows its HEADERS frame, other than a CONNECT, gets a body that holds no source.
//! Responses go out through the pull path: `poll_send` polls the exchange's head and body with an
//! allowance and pushes frames whose payloads are never copied. A small response that the exchange
//! holds whole when its request is delivered is staged at once, the same way, without waiting for
//! the pull step. The logic is written against the transport seam in [`crate::transport`].

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::mem;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use bytes::BytesMut;
use mvfstr_driver::Conn;
use mvfstr_driver::ConnInfo;
use mvfstr_driver::QuicApplication;
use mvfstr_driver::QuicSession;
use mvfstr_driver::Refusal;
use mvfstr_driver::SendSink;
use mvfstr_driver::SinkError;
use mvfstr_proto::AppCode;
use mvfstr_proto::BufQueue;
use mvfstr_proto::ConfigError;
use mvfstr_proto::ConnEvent;
use mvfstr_proto::ConnOptions;
use mvfstr_proto::Dir;
use mvfstr_proto::Initiator;
use mvfstr_proto::OpenError;
use mvfstr_proto::SendError;
use mvfstr_proto::SendOrder;
use mvfstr_proto::Side;
use mvfstr_proto::StreamId;
use mvfstr_proto::StreamReady;
use mvfstr_proto::TransportConfig;
use mvfstr_proto::VarInt;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEventKind;
use roxygen_core::events::ResumeFuture;
use roxygen_core::events::ResumeStatus;
use roxygen_core::ids::StreamId as HttpStreamId;
use roxygen_core::message::HttpMessage;
use roxygen_core::stream_source::FlowControlState;
use roxygen_core::stream_source::HttpIngressBody;
use roxygen_core::stream_source::HttpStreamSourceDrain;
use roxygen_core::stream_source::HttpStreamSourceProducer;
use roxygen_qpack::QPACK_DECODER_STREAM_ERROR;
use roxygen_qpack::QPACK_DECOMPRESSION_FAILED;
use roxygen_qpack::QPACK_ENCODER_STREAM_ERROR;
use roxygen_runtime::host::Host;
use roxygen_session::owned::OwnedApplication;
use roxygen_session::owned::OwnedExchange;
use thiserror::Error;

use crate::codec::Frame;
use crate::codec::FrameDecoder;
use crate::codec::FrameLimits;
use crate::codec::FrameType;
use crate::codec::H3ErrorCode;
use crate::codec::Role;
use crate::codec::SettingId;
use crate::codec::Settings;
use crate::codec::StreamKind;
use crate::codec::UniStreamHeader;
use crate::codec::UniStreamHeaderDecoder;
use crate::codec::VARINT_MAX;
use crate::codec::encode_frame_header;
use crate::codec::frame_header_len;
use crate::credit::CreditCell;
use crate::credit::IngressSignals;
use crate::credit::WorkQueue;
use crate::fields;
use crate::fields::FieldError;
use crate::transport::PullSink;
use crate::transport::StreamSink;
use crate::transport::Transport;
use crate::transport::TransportStream;

/// The largest body a single `poll_body` may return, the same limit as Roxygen's owned HTTP/2
/// driver.
const MAX_BODY_POLL: usize = 64 << 10;

/// Application events one `poll_send` call handles at most, so that a producer returning empty
/// events cannot hold the driver.
const EVENTS_PER_POLL: usize = 16;

/// The wire order of control and QPACK streams, which go before every request.
const CONTROL_ORDER: SendOrder = SendOrder {
    class: 0,
    urgency: 0,
    incremental: false,
    order: 0,
};

/// Room kept free in the frame-header buffer, so that headers carve from a shared allocation.
const FRAME_BUFFER: usize = 4096;

/// The largest fixed response body staged when its request is delivered: 16 KiB, the driver's
/// low-water mark, the allowance a stream must reach before the driver polls it.
const INLINE_BODY_MAX: usize = 16 << 10;

/// The work that fixed responses staged at delivery may charge a connection in one phase: 1 MiB,
/// the driver's bound on the bytes one connection's pull steps stage in a turn.
const INLINE_STAGING: usize = 1 << 20;

/// The least a fixed response staged at delivery is charged: 16 KiB, the driver's least charge
/// for a poll. A connection so stages at most 64 responses at delivery in one phase.
const INLINE_CHARGE: usize = 16 << 10;

/// The shortest time a quiescing connection stays open after its GOAWAY. CONNECTION_CLOSE stops
/// all stream data at once (RFC 9000) and the transport does not report when the control
/// stream's bytes are acknowledged, so the GOAWAY gets time to arrive, and to be sent again if
/// it is lost, before the connection closes.
const QUIESCE_CLOSE_DELAY: Duration = Duration::from_secs(1);

/// Why [`H3ApplicationBuilder::build`] refused its settings.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum H3ConfigError {
    /// A limit is zero or too large.
    #[error("{field} is out of range")]
    OutOfRange {
        /// The name of the setting.
        field: &'static str,
    },
    /// The default transport settings were rejected.
    #[error("invalid transport settings: {0}")]
    Transport(ConfigError),
}

/// The limits of an HTTP/3 session.
#[derive(Clone, Debug)]
struct Config {
    max_field_section_size: u64,
    max_concurrent_exchanges: usize,
    request_body_buffer: u32,
    transport: Arc<TransportConfig>,
}

/// A builder for an [`H3Application`].
#[derive(Clone, Debug)]
pub struct H3ApplicationBuilder {
    max_field_section_size: u64,
    max_concurrent_exchanges: usize,
    request_body_buffer: u32,
    transport: Option<Arc<TransportConfig>>,
}

impl Default for H3ApplicationBuilder {
    fn default() -> Self {
        Self {
            max_field_section_size: 64 << 10,
            max_concurrent_exchanges: 100,
            request_body_buffer: 64 << 10,
            transport: None,
        }
    }
}

impl H3ApplicationBuilder {
    /// Creates a builder with the default limits.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the largest request head or trailer section accepted, advertised as
    /// SETTINGS_MAX_FIELD_SECTION_SIZE. RFC 9114 counts each field's name and value plus 32 bytes.
    /// 64 KiB by default.
    pub fn max_field_section_size(mut self, bytes: u64) -> Self {
        self.max_field_section_size = bytes;
        self
    }

    /// Sets the most exchanges a connection runs at once; later request streams are rejected
    /// with H3_REQUEST_REJECTED. 100 by default.
    pub fn max_concurrent_exchanges(mut self, exchanges: usize) -> Self {
        self.max_concurrent_exchanges = exchanges;
        self
    }

    /// Sets the request body bytes buffered for an exchange before its application consumes
    /// them, and the default stream credit of request streams. 64 KiB by default.
    pub fn request_body_buffer(mut self, bytes: u32) -> Self {
        self.request_body_buffer = bytes;
        self
    }

    /// Sets the transport settings of accepted connections. By default mvfstr's, with 100
    /// concurrent request streams, 8 unidirectional streams and request stream credit equal to
    /// the request body buffer.
    pub fn transport(mut self, transport: Arc<TransportConfig>) -> Self {
        self.transport = Some(transport);
        self
    }

    /// Builds the application. `make` creates the owned application of each accepted
    /// connection, or refuses the connection; `host` runs the request bodies.
    pub fn build<A, F>(self, host: Host, make: F) -> Result<H3Application<A>, H3ConfigError>
    where
        A: OwnedApplication,
        F: FnMut(&ConnInfo) -> Result<A, Refusal> + 'static,
    {
        Ok(H3Application {
            settings: self.build_settings(host)?,
            make: Box::new(make),
        })
    }

    /// Builds the settings alone, for a server that accepts connections itself and creates each
    /// session with [`H3Settings::session`]; `host` runs the request bodies.
    pub fn build_settings(self, host: Host) -> Result<H3Settings, H3ConfigError> {
        Ok(H3Settings {
            config: Rc::new(self.config()?),
            host,
        })
    }

    /// Checks the settings as [`build_settings`](Self::build_settings) does, for a server that
    /// reports bad settings before it starts the workers that build them.
    pub fn validate(&self) -> Result<(), H3ConfigError> {
        self.clone().config().map(drop)
    }

    fn config(self) -> Result<Config, H3ConfigError> {
        let check = |ok: bool, field| {
            if ok {
                Ok(())
            } else {
                Err(H3ConfigError::OutOfRange { field })
            }
        };
        check(
            (1..=VARINT_MAX).contains(&self.max_field_section_size),
            "max_field_section_size",
        )?;
        check(
            self.max_concurrent_exchanges > 0,
            "max_concurrent_exchanges",
        )?;
        // The ingress body keeps its window as a signed 32-bit value.
        check(
            (1..=i32::MAX as u32).contains(&self.request_body_buffer),
            "request_body_buffer",
        )?;
        let transport = match self.transport {
            Some(transport) => transport,
            None => TransportConfig::builder()
                .max_streams_bidi(100)
                .max_streams_uni(8)
                .max_stream_data_bidi_remote(u64::from(self.request_body_buffer))
                .build()
                .map_err(H3ConfigError::Transport)?,
        };
        // RFC 9114: the peer must be able to open its control and QPACK streams.
        check(
            transport.max_streams_uni() >= 3,
            "transport.max_streams_uni",
        )?;
        check(
            transport.max_streams_bidi() > 0,
            "transport.max_streams_bidi",
        )?;
        Ok(Config {
            max_field_section_size: self.max_field_section_size,
            max_concurrent_exchanges: self.max_concurrent_exchanges,
            request_body_buffer: self.request_body_buffer,
            transport,
        })
    }
}

/// Validated HTTP/3 settings, from which sessions of any owned application are created.
#[derive(Clone)]
pub struct H3Settings {
    config: Rc<Config>,
    host: Host,
}

impl H3Settings {
    /// Creates the session of a connection being accepted, around `application`, and sets the
    /// connection's transport settings in `options`.
    pub fn session<A: OwnedApplication>(
        &self,
        application: A,
        options: &mut ConnOptions,
    ) -> H3Session<A> {
        options.transport = Some(Arc::clone(&self.config.transport));
        H3Session {
            core: Core::new(application, Rc::clone(&self.config), self.host.clone()),
        }
    }
}

/// The application of one endpoint driver serving HTTP/3: it creates an [`H3Session`] around a
/// new owned application for each accepted connection.
pub struct H3Application<A> {
    settings: H3Settings,
    make: Box<dyn FnMut(&ConnInfo) -> Result<A, Refusal>>,
}

impl<A: OwnedApplication> QuicApplication for H3Application<A> {
    type Session = H3Session<A>;

    fn accept(
        &mut self,
        info: &ConnInfo,
        options: &mut ConnOptions,
    ) -> Result<H3Session<A>, Refusal> {
        let application = (self.make)(info)?;
        Ok(self.settings.session(application, options))
    }
}

/// The HTTP/3 state of one connection.
pub struct H3Session<A: OwnedApplication> {
    pub(crate) core: Core<A>,
}

impl<A: OwnedApplication> H3Session<A> {
    /// Returns whether the peer's SETTINGS frame has arrived, which completes the HTTP/3
    /// connection handshake on the server's side.
    pub fn handshake_complete(&self) -> bool {
        self.core.state.peer_settings
    }
}

impl<A: OwnedApplication> QuicSession for H3Session<A> {
    fn on_event(&mut self, conn: &mut Conn<'_>, event: ConnEvent) {
        self.core.on_event(conn, event);
    }

    fn on_stream(&mut self, conn: &mut Conn<'_>, id: StreamId, ready: StreamReady) {
        self.core.on_stream(conn, id, ready);
    }

    fn poll_send(
        &mut self,
        cx: &mut Context<'_>,
        id: StreamId,
        sink: &mut SendSink<'_>,
    ) -> Poll<()> {
        self.core.poll_send(cx, id, sink)
    }

    fn poll_session(&mut self, cx: &mut Context<'_>, conn: &mut Conn<'_>) -> Poll<()> {
        self.core.poll_session(cx, conn)
    }

    fn deadline(&self) -> Option<Instant> {
        self.core.deadline()
    }

    fn on_quiesce(&mut self, conn: &mut Conn<'_>) {
        self.core.on_quiesce(conn);
    }
}

/// The session logic, generic over the transport seam.
pub(crate) struct Core<A: OwnedApplication> {
    application: A,
    state: State,
    /// The live exchanges, sorted by stream ID.
    exchanges: Vec<Entry<A::Exchange>>,
}

/// One request stream's exchange.
struct Entry<E> {
    id: StreamId,
    exchange: Box<Exchange<E>>,
}

/// The connection-level state.
struct State {
    config: Rc<Config>,
    host: Host,
    started: bool,
    closed: bool,
    /// Our control stream, once open, and the bytes it has not yet taken.
    control: Option<StreamId>,
    control_out: Bytes,
    /// Whether opening the control stream waits for the peer's stream limit to rise.
    control_waiting: bool,
    /// Whether a quiesce came while the control stream waited to open. The transport shuts down,
    /// and the close delay starts, once the stream is open: a shut-down transport cannot open it.
    shutdown_owed: bool,
    /// The critical streams the peer has opened: control, QPACK encoder and QPACK decoder.
    peer_control: bool,
    /// Whether the peer's SETTINGS frame has arrived.
    peer_settings: bool,
    peer_encoder: bool,
    peer_decoder: bool,
    /// The peer's unidirectional streams still being read.
    uni: Vec<UniStream>,
    qpack_decoder: roxygen_qpack::Decoder,
    qpack_encoder: roxygen_qpack::Encoder,
    work: Arc<WorkQueue>,
    work_scratch: Vec<StreamId>,
    segments: Vec<Bytes>,
    scratch: Vec<u8>,
    names: Vec<u8>,
    out: BytesMut,
    /// One more than the highest index of a client request stream that has arrived.
    next_request: u64,
    /// The stream ID our GOAWAY carries, once sent.
    goaway: Option<u64>,
    /// The earliest time the quiesced connection closes.
    close_at: Option<Instant>,
    /// The phase reading of the last fixed response staged at delivery, and the work charged to
    /// such responses at that reading.
    inline_at: Option<Instant>,
    inline_work: usize,
}

/// A peer unidirectional stream.
struct UniStream {
    id: StreamId,
    kind: UniKind,
    fin: bool,
}

enum UniKind {
    Header(UniStreamHeaderDecoder),
    Control(FrameDecoder),
    QpackEncoder,
    QpackDecoder,
    /// A reserved or unknown type: stopped and drained.
    Discard,
}

impl UniKind {
    fn critical(&self) -> bool {
        matches!(
            self,
            UniKind::Control(_) | UniKind::QpackEncoder | UniKind::QpackDecoder
        )
    }
}

/// How far a request's ingress has come.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Ingress {
    /// Waiting for the request head.
    Head,
    /// Receiving the body.
    Body,
    /// The trailers arrived; only the FIN remains.
    Trailers,
    /// The whole request arrived.
    Complete,
    /// Reading stopped before the request was complete.
    Abandoned,
}

impl Ingress {
    fn open(self) -> bool {
        matches!(self, Ingress::Head | Ingress::Body | Ingress::Trailers)
    }
}

/// How far a response has come.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Egress {
    Headers,
    Body,
    /// The message ended; the FIN goes after the pending bytes.
    Finish,
    Done,
}

struct Exchange<E> {
    application: Option<E>,
    decoder: FrameDecoder,
    /// The ingress body, once built, until the request is delivered. It is declared before `feed`
    /// so that a dropped exchange drops it before the drain owner, which then stops the draining
    /// body as dropped rather than abandoned.
    body: Option<HttpIngressBody>,
    /// The session's side of the ingress body, once the body is built.
    feed: Option<IngressFeed>,
    /// The decoded request head until it is delivered.
    head: Option<Box<HttpMessage>>,
    delivered: bool,
    ingress: Ingress,
    /// Whether DATA or trailers reached the ingress body.
    body_events: bool,
    content_length: Option<u64>,
    /// Whether the request is a CONNECT.
    connect: bool,
    /// Whether the 2xx response to a CONNECT made the stream a tunnel (RFC 9114).
    tunnel: bool,
    /// Whether the request is a HEAD, so that the application's response body is not sent.
    head_request: bool,
    /// DATA payload bytes received.
    received: u64,
    /// Stream bytes parsed, consumed from the transport, and parsed that are not body data.
    parsed: u64,
    consumed: u64,
    overhead: u64,
    /// Body bytes handed to the ingress body, and those its consumer has read.
    handed: u64,
    app_consumed: u64,
    egress: Egress,
    /// Response bytes that did not fit the allowance.
    pending: BufQueue,
    resume: Option<ResumeFuture>,
    /// The waker of a `poll_send` that came before the request was delivered.
    waker: Option<Waker>,
    response_complete: bool,
    /// Whether STOP_SENDING is due after an early response.
    stop_owed: bool,
    /// A reset due after a failed response.
    reset_owed: Option<H3ErrorCode>,
    /// Whether the application completed. The entry stays until the stream's `Closed` edge.
    retired: bool,
}

impl<E> Exchange<E> {
    /// Returns the body bytes the ingress body can still take.
    fn room(&self, buffer: u32) -> usize {
        let outstanding = self.handed - self.app_consumed;
        u64::from(buffer).saturating_sub(outstanding) as usize
    }

    /// Returns the producer of the ingress body, once the body is built.
    fn producer(&self) -> Option<&HttpStreamSourceProducer> {
        self.feed.as_ref().map(|feed| &feed.producer)
    }
}

/// The session's side of a request's ingress body, built with the body and kept until the exchange
/// goes.
struct IngressFeed {
    /// The cell the body's consumer returns credit through.
    cell: Arc<CreditCell>,
    producer: HttpStreamSourceProducer,
    /// The owner that keeps a body the application dropped draining, with its credit, until the
    /// request ends.
    _drain: HttpStreamSourceDrain,
}

/// Builds the ingress body of request stream `id` and the session's side of it.
fn ingress_body(state: &State, id: StreamId) -> (HttpIngressBody, IngressFeed) {
    let cell = Arc::new(CreditCell::new(id, Arc::clone(&state.work)));
    let signals = Rc::new(IngressSignals {
        cell: Arc::clone(&cell),
    });
    let body = HttpIngressBody::new_shared(
        http_stream_id(id),
        signals,
        state.config.request_body_buffer,
        state.host.clone(),
    )
    .expect("the builder bounds the request body buffer to the body's window range");
    let feed = IngressFeed {
        cell,
        producer: body.producer(),
        _drain: body.drain_on_drop(),
    };
    (body, feed)
}

/// Returns the Roxygen stream ID of request stream `id`. QUIC's first request stream is 0, which
/// Roxygen reserves for "unspecified".
fn http_stream_id(id: StreamId) -> HttpStreamId {
    HttpStreamId::new(id.into_inner() + 1)
}

/// The outcome of a step of ingress processing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Verdict {
    Continue,
    /// A stream error: reset the stream with the code.
    Stream(H3ErrorCode),
    /// A connection error: close the connection with the code.
    Connection(H3ErrorCode),
}

impl<A: OwnedApplication> Core<A> {
    fn new(application: A, config: Rc<Config>, host: Host) -> Self {
        Self {
            application,
            state: State {
                config,
                host,
                started: false,
                closed: false,
                control: None,
                control_out: Bytes::new(),
                control_waiting: false,
                shutdown_owed: false,
                peer_control: false,
                peer_settings: false,
                peer_encoder: false,
                peer_decoder: false,
                uni: Vec::new(),
                qpack_decoder: roxygen_qpack::Decoder::new(),
                qpack_encoder: roxygen_qpack::Encoder::new(),
                work: Arc::new(WorkQueue::default()),
                work_scratch: Vec::new(),
                segments: Vec::new(),
                scratch: Vec::new(),
                names: Vec::new(),
                out: BytesMut::new(),
                next_request: 0,
                goaway: None,
                close_at: None,
                inline_at: None,
                inline_work: 0,
            },
            exchanges: Vec::new(),
        }
    }

    pub(crate) fn on_event<T: Transport>(&mut self, t: &mut T, event: ConnEvent) {
        self.start(t);
        match event {
            ConnEvent::StreamsAvailable { dir: Dir::Uni } if self.state.control_waiting => {
                self.open_control(t);
            }
            ConnEvent::Closed { initiator, .. } => {
                self.state.closed = true;
                let (code, peer) = match initiator {
                    Initiator::Local => (HttpErrorCode::CANCEL, false),
                    Initiator::Internal => (HttpErrorCode::INTERNAL_ERROR, false),
                    _ => (HttpErrorCode::TRANSPORT_EOF, true),
                };
                self.cancel_all(&HttpError::with_message(code, b"connection closed"), peer);
            }
            _ => {}
        }
        self.service(t);
    }

    pub(crate) fn on_stream<T: Transport>(&mut self, t: &mut T, id: StreamId, ready: StreamReady) {
        self.start(t);
        if !self.state.closed {
            match (id.initiator(), id.dir()) {
                (Side::Client, Dir::Bi) => self.request_ready(t, id, ready),
                (Side::Client, Dir::Uni) => self.uni_ready(t, id, ready),
                (Side::Server, Dir::Uni) => self.own_stream_ready(t, id, ready),
                (Side::Server, Dir::Bi) => {}
            }
        }
        self.service(t);
    }

    pub(crate) fn poll_session<T: Transport>(
        &mut self,
        cx: &mut Context<'_>,
        t: &mut T,
    ) -> Poll<()> {
        self.start(t);
        self.state.work.set_waker(cx.waker());
        self.service(t);
        Poll::Pending
    }

    pub(crate) fn on_quiesce<T: Transport>(&mut self, t: &mut T) {
        self.start(t);
        if self.state.closed || self.state.goaway.is_some() {
            return;
        }
        // RFC 9114: requests on this stream ID or later are not processed.
        let goaway = self.state.next_request << 2;
        self.state.goaway = Some(goaway);
        let mut out = BytesMut::new();
        Frame::Goaway(goaway)
            .encode(&mut out)
            .expect("a client stream ID fits a variable-length integer");
        self.queue_control(t, out.freeze());
        if self.state.control.is_some() {
            self.shut_down(t);
        } else {
            self.state.shutdown_owed = true;
        }
        self.service(t);
    }

    /// Shuts the transport down once GOAWAY is queued on the open control stream, and gives the
    /// GOAWAY time to arrive before a connection without exchanges closes.
    fn shut_down<T: Transport>(&mut self, t: &mut T) {
        t.shutdown();
        self.state.close_at = Some(t.now() + QUIESCE_CLOSE_DELAY);
    }

    /// Returns the time a quiesced connection without exchanges closes.
    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.state
            .close_at
            .filter(|_| !self.state.closed && self.exchanges.is_empty())
    }

    pub(crate) fn poll_send<S: PullSink>(
        &mut self,
        cx: &mut Context<'_>,
        id: StreamId,
        sink: &mut S,
    ) -> Poll<()> {
        if self.state.control == Some(id) {
            return self.state.push_control(sink);
        }
        let Some(index) = self.find(id) else {
            return Poll::Pending;
        };
        let state = &mut self.state;
        let exchange = &mut *self.exchanges[index].exchange;
        if exchange.retired || exchange.application.is_none() {
            return Poll::Pending;
        }
        if !exchange.delivered {
            exchange.waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let mut progress = false;
        let produced = produce(state, exchange, cx, sink, &mut progress);
        if settle_response(state, id, exchange, produced) && progress {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    /// Opens the control stream and installs the session waker at the first callback.
    fn start<T: Transport>(&mut self, t: &mut T) {
        if self.state.started {
            return;
        }
        self.state.started = true;
        self.state.work.set_handle(t.session_handle());
        self.open_control(t);
    }

    fn open_control<T: Transport>(&mut self, t: &mut T) {
        match t.open_uni(CONTROL_ORDER) {
            Ok(id) => {
                self.state.control = Some(id);
                self.state.control_waiting = false;
                // A GOAWAY queued while the stream waited for credit goes after SETTINGS.
                let settings = self.state.settings();
                let queued = mem::replace(&mut self.state.control_out, settings);
                if queued.is_empty() {
                    self.flush_control(t);
                } else {
                    self.queue_control(t, queued);
                }
                if mem::take(&mut self.state.shutdown_owed) {
                    self.shut_down(t);
                }
            }
            Err(OpenError::StreamLimit) => self.state.control_waiting = true,
            Err(OpenError::Closed) => {}
        }
    }

    /// Appends `bytes` to the control stream's output and stages what the stream takes.
    fn queue_control<T: Transport>(&mut self, t: &mut T, bytes: Bytes) {
        let state = &mut self.state;
        state.control_out = if state.control_out.is_empty() {
            bytes
        } else {
            let mut joined = BytesMut::from(&state.control_out[..]);
            joined.extend_from_slice(&bytes);
            joined.freeze()
        };
        self.flush_control(t);
    }

    fn flush_control<T: Transport>(&mut self, t: &mut T) {
        let Some(id) = self.state.control else {
            return;
        };
        if self.state.control_out.is_empty() {
            return;
        }
        let stopped = match t.stream(id) {
            Ok(mut stream) => matches!(
                stream.send(&mut self.state.control_out),
                Err(SendError::Stopped { .. })
            ),
            Err(_) => false,
        };
        if stopped {
            self.connection_error(t, H3ErrorCode::CLOSED_CRITICAL_STREAM);
        }
    }

    fn own_stream_ready<T: Transport>(&mut self, t: &mut T, id: StreamId, ready: StreamReady) {
        if self.state.control != Some(id) {
            return;
        }
        match ready {
            StreamReady::Writable => self.flush_control(t),
            // RFC 9114: the peer must not ask the server to close the server's control stream.
            StreamReady::Stopped { .. } | StreamReady::Reset { .. } | StreamReady::Closed => {
                self.connection_error(t, H3ErrorCode::CLOSED_CRITICAL_STREAM);
            }
            _ => {}
        }
    }

    fn find(&self, id: StreamId) -> Option<usize> {
        self.exchanges
            .binary_search_by_key(&id, |entry| entry.id)
            .ok()
    }

    fn request_ready<T: Transport>(&mut self, t: &mut T, id: StreamId, ready: StreamReady) {
        match ready {
            StreamReady::Opened => {
                if self.find(id).is_none() {
                    self.accept_request(t, id);
                }
                self.read_request(t, id);
            }
            // Every peer stream, including one opened implicitly by a higher ID, gets its own
            // Opened edge, and a FIN its own Finished edge. A data edge for an unknown stream
            // below `next_request` therefore belongs to one already ended.
            StreamReady::Readable | StreamReady::Finished => {
                if self.find(id).is_none() && id.index() >= self.state.next_request {
                    self.accept_request(t, id);
                }
                self.read_request(t, id);
            }
            StreamReady::Reset { code, .. } => self.peer_reset(t, id, code),
            StreamReady::Stopped { code } => self.peer_stopped(t, id, code),
            StreamReady::Closed => self.stream_closed(id),
            StreamReady::Writable | StreamReady::Progress => {}
        }
    }

    /// Creates the exchange for a new request stream, or rejects the stream.
    fn accept_request<T: Transport>(&mut self, t: &mut T, id: StreamId) {
        self.state.next_request = self.state.next_request.max(id.index() + 1);
        let live = self
            .exchanges
            .iter()
            .filter(|entry| !entry.exchange.retired)
            .count();
        let going_away = self
            .state
            .goaway
            .is_some_and(|goaway| id.into_inner() >= goaway);
        if going_away || live >= self.state.config.max_concurrent_exchanges {
            abort_stream(t, id, H3ErrorCode::REQUEST_REJECTED);
            return;
        }
        // An application that takes the completion gets the body built at admission.
        let mut built = None;
        let application = self.application.new_exchange_with(http_stream_id(id), || {
            let (body, feed) = ingress_body(&self.state, id);
            let completion = body
                .ingress_completion()
                .expect("a fresh ingress body reports its completion");
            built = Some((body, feed));
            completion
        });
        let (body, feed) = built.unzip();
        let limits = FrameLimits {
            max_field_section_payload: usize::try_from(self.state.config.max_field_section_size)
                .unwrap_or(usize::MAX),
            ..FrameLimits::default()
        };
        let exchange = Box::new(Exchange {
            application: Some(application),
            decoder: FrameDecoder::new(StreamKind::Request, Role::Server, limits),
            body,
            feed,
            head: None,
            delivered: false,
            ingress: Ingress::Head,
            body_events: false,
            content_length: None,
            connect: false,
            tunnel: false,
            head_request: false,
            received: 0,
            parsed: 0,
            consumed: 0,
            overhead: 0,
            handed: 0,
            app_consumed: 0,
            egress: Egress::Headers,
            pending: BufQueue::new(usize::MAX),
            resume: None,
            waker: None,
            response_complete: false,
            stop_owed: false,
            reset_owed: None,
            retired: false,
        });
        let position = self
            .exchanges
            .binary_search_by_key(&id, |entry| entry.id)
            .unwrap_err();
        self.exchanges.insert(position, Entry { id, exchange });
    }

    /// Parses what the request stream has received, applies the credit its application
    /// returned, ends ingress at the FIN, and delivers the request once its head is complete.
    fn read_request<T: Transport>(&mut self, t: &mut T, id: StreamId) {
        let Some(index) = self.find(id) else {
            return;
        };
        let verdict = {
            let state = &mut self.state;
            let exchange = &mut *self.exchanges[index].exchange;
            read_frames(t, state, id, exchange)
        };
        match verdict {
            Verdict::Continue => {}
            Verdict::Stream(code) => return self.fail_stream(t, id, code),
            Verdict::Connection(code) => return self.connection_error(t, code),
        }
        let state = &mut self.state;
        let exchange = &mut *self.exchanges[index].exchange;
        if exchange.head.is_some()
            && let Err(error) = deliver(t, state, id, exchange)
        {
            abort_stream(t, id, H3ErrorCode::INTERNAL_ERROR);
            let mut entry = self.exchanges.remove(index);
            cancel(&mut entry.exchange, &error, false);
            return self.check_quiesced(t);
        }
        if exchange.ingress == Ingress::Complete
            && exchange.response_complete
            && exchange.application.is_some()
        {
            retire(exchange);
        }
    }

    /// Resets the request stream with a stream error and cancels its exchange.
    fn fail_stream<T: Transport>(&mut self, t: &mut T, id: StreamId, code: H3ErrorCode) {
        abort_stream(t, id, code);
        if let Some(index) = self.find(id) {
            let mut entry = self.exchanges.remove(index);
            let error = HttpError::with_message(error_code(code), b"HTTP/3 stream error");
            cancel(&mut entry.exchange, &error, false);
        }
        self.check_quiesced(t);
    }

    fn peer_reset<T: Transport>(&mut self, t: &mut T, id: StreamId, code: AppCode) {
        let Some(index) = self.find(id) else {
            return;
        };
        let exchange = &mut *self.exchanges[index].exchange;
        // The upload is half of a tunnel, so a reset aborts it also after the target closed its
        // side (RFC 9114).
        if exchange.retired || (exchange.response_complete && !exchange.tunnel) {
            // The response is out; the peer only stopped its upload, which ends the body.
            if exchange.ingress.open() {
                exchange.ingress = Ingress::Abandoned;
                if let Some(producer) = exchange.producer() {
                    producer.abort(
                        fields::peer_error_code(code.0.into_inner()),
                        b"peer reset the request stream",
                    );
                }
            }
            return;
        }
        if let Ok(mut stream) = t.stream(id) {
            let _ = stream.reset(app_code(H3ErrorCode::REQUEST_CANCELLED));
        }
        let mut entry = self.exchanges.remove(index);
        let error = HttpError::with_message(
            fields::peer_error_code(code.0.into_inner()),
            b"peer reset the request stream",
        );
        cancel(&mut entry.exchange, &error, true);
        self.check_quiesced(t);
    }

    fn peer_stopped<T: Transport>(&mut self, t: &mut T, id: StreamId, code: AppCode) {
        let Some(index) = self.find(id) else {
            return;
        };
        if self.exchanges[index].exchange.retired {
            return;
        }
        let mut entry = self.exchanges.remove(index);
        if entry.exchange.ingress.open() {
            if let Ok(mut stream) = t.stream(id) {
                let _ = stream.stop_sending(app_code(H3ErrorCode::REQUEST_CANCELLED));
            }
        } else {
            // The cancelled body is not read; its bytes must go for the receive half to end.
            release(t, id);
        }
        let error = HttpError::with_message(
            fields::peer_error_code(code.0.into_inner()),
            b"peer stopped the response",
        );
        cancel(&mut entry.exchange, &error, true);
        self.check_quiesced(t);
    }

    /// Removes the exchange of a stream whose two halves have ended, completing it if its response
    /// is complete and cancelling it otherwise.
    fn stream_closed(&mut self, id: StreamId) {
        let Some(index) = self.find(id) else {
            return;
        };
        let mut entry = self.exchanges.remove(index);
        let exchange = &mut *entry.exchange;
        if exchange.response_complete {
            if let Some(application) = exchange.application.take() {
                application.complete();
            }
        } else {
            let error = HttpError::with_message(HttpErrorCode::STREAM_CLOSED, b"stream closed");
            cancel(exchange, &error, true);
        }
    }

    /// Closes the connection with `code` and cancels every exchange.
    fn connection_error<T: Transport>(&mut self, t: &mut T, code: H3ErrorCode) {
        if self.state.closed {
            return;
        }
        self.state.closed = true;
        t.close(app_code(code), Bytes::new());
        let error = HttpError::with_message(error_code(code), b"HTTP/3 connection error");
        self.cancel_all(&error, false);
    }

    fn cancel_all(&mut self, error: &HttpError, peer_reset: bool) {
        for mut entry in mem::take(&mut self.exchanges) {
            if entry.exchange.retired {
                continue;
            }
            cancel(&mut entry.exchange, error, peer_reset);
        }
    }

    /// Runs the work the exchanges queued: credit their applications returned, STOP_SENDING
    /// after early responses and resets after failed responses. Then closes a quiesced
    /// connection once every exchange is gone.
    fn service<T: Transport>(&mut self, t: &mut T) {
        let mut queued = mem::take(&mut self.state.work_scratch);
        self.state.work.take(&mut queued);
        for id in queued.drain(..) {
            if self.state.closed {
                break;
            }
            let Some(index) = self.find(id) else {
                continue;
            };
            let exchange = &mut *self.exchanges[index].exchange;
            if let Some(code) = exchange.reset_owed.take() {
                self.fail_stream(t, id, code);
                continue;
            }
            if mem::take(&mut exchange.stop_owed) {
                if exchange.ingress.open() {
                    // RFC 9114: once the response is complete, the server may stop the rest of
                    // the request with H3_NO_ERROR.
                    if let Ok(mut stream) = t.stream(id) {
                        let _ = stream.stop_sending(app_code(H3ErrorCode::NO_ERROR));
                    }
                    if let Some(producer) = exchange.producer() {
                        producer.abort(HttpErrorCode::CANCEL, b"response completed early");
                    }
                    exchange.ingress = Ingress::Abandoned;
                    continue;
                }
                if exchange.ingress == Ingress::Complete {
                    retire(exchange);
                    continue;
                }
            }
            self.read_request(t, id);
        }
        self.state.work_scratch = queued;
        self.check_quiesced(t);
    }

    /// After GOAWAY, closes the connection with H3_NO_ERROR once every exchange is gone and the
    /// GOAWAY has had its time to arrive. A retired exchange stays until its stream's `Closed`
    /// edge, so that CONNECTION_CLOSE does not cut off a response that is staged but not yet
    /// acknowledged (RFC 9000).
    fn check_quiesced<T: Transport>(&mut self, t: &mut T) {
        if self.state.closed || !self.exchanges.is_empty() {
            return;
        }
        if self
            .state
            .close_at
            .is_some_and(|close_at| t.now() >= close_at)
        {
            self.state.closed = true;
            t.close(app_code(H3ErrorCode::NO_ERROR), Bytes::new());
        }
    }

    fn uni_index(&self, id: StreamId) -> Option<usize> {
        self.state.uni.iter().position(|stream| stream.id == id)
    }

    fn uni_ready<T: Transport>(&mut self, t: &mut T, id: StreamId, ready: StreamReady) {
        match ready {
            StreamReady::Opened | StreamReady::Readable | StreamReady::Finished => {
                let index = match self.uni_index(id) {
                    Some(index) => index,
                    None => {
                        self.state.uni.push(UniStream {
                            id,
                            kind: UniKind::Header(UniStreamHeaderDecoder::new(Role::Server)),
                            fin: false,
                        });
                        self.state.uni.len() - 1
                    }
                };
                if ready == StreamReady::Finished {
                    self.state.uni[index].fin = true;
                }
                self.read_uni(t, index);
            }
            StreamReady::Reset { .. } | StreamReady::Closed => {
                if let Some(index) = self.uni_index(id) {
                    self.uni_ended(t, index);
                }
            }
            StreamReady::Writable | StreamReady::Progress | StreamReady::Stopped { .. } => {}
        }
    }

    /// Forgets a peer unidirectional stream that was reset or reaped, or closes the connection if
    /// it was a critical stream.
    fn uni_ended<T: Transport>(&mut self, t: &mut T, index: usize) {
        if self.state.uni[index].kind.critical() {
            self.connection_error(t, H3ErrorCode::CLOSED_CRITICAL_STREAM);
        } else {
            self.state.uni.remove(index);
        }
    }

    /// Reads a peer unidirectional stream: its type, then its frames or instructions.
    fn read_uni<T: Transport>(&mut self, t: &mut T, index: usize) {
        loop {
            let id = self.state.uni[index].id;
            let (verdict, retyped) = read_uni_step(t, &mut self.state, index);
            if let Verdict::Connection(code) | Verdict::Stream(code) = verdict {
                return self.connection_error(t, code);
            }
            if !retyped {
                break;
            }
            if matches!(self.state.uni[index].kind, UniKind::Discard) {
                if let Ok(mut stream) = t.stream(id) {
                    let _ = stream.stop_sending(app_code(H3ErrorCode::STREAM_CREATION_ERROR));
                }
            }
        }
        let stream = &self.state.uni[index];
        if !stream.fin {
            return;
        }
        if stream.kind.critical() {
            // RFC 9114 and RFC 9204: critical streams never end, not even inside an instruction
            // the QPACK stream has not finished.
            return self.connection_error(t, H3ErrorCode::CLOSED_CRITICAL_STREAM);
        }
        let drained = t
            .stream(stream.id)
            .map_or(true, |stream| stream.readable().is_empty());
        if drained {
            self.state.uni.remove(index);
        }
    }
}

impl State {
    /// Returns the control stream's header and SETTINGS frame (RFC 9114).
    fn settings(&self) -> Bytes {
        let random = RandomState::new().hash_one(0u8);
        let mut settings = Settings::new();
        let entries = [
            (SettingId::QPACK_MAX_TABLE_CAPACITY, 0),
            (SettingId::QPACK_BLOCKED_STREAMS, 0),
            (
                SettingId::MAX_FIELD_SECTION_SIZE,
                self.config.max_field_section_size,
            ),
            // A reserved setting, which the peer must ignore (RFC 9114).
            (SettingId::grease(random % 64), (random >> 32) & 0xff),
        ];
        for (id, value) in entries {
            settings
                .set(id, value)
                .expect("the settings are defined and their values fit");
        }
        let mut out = BytesMut::new();
        UniStreamHeader::Control
            .encode(&mut out)
            .expect("the control stream header encodes");
        Frame::Settings(settings)
            .encode(&mut out)
            .expect("SETTINGS encodes");
        out.freeze()
    }

    /// Pushes control stream bytes through the pull path.
    fn push_control<S: PullSink>(&mut self, sink: &mut S) -> Poll<()> {
        let take = self.control_out.len().min(sink.allowance());
        if take == 0 {
            return Poll::Pending;
        }
        let chunk = self.control_out.split_to(take);
        match sink.push(chunk.clone()) {
            Ok(()) => Poll::Ready(()),
            Err(_) => {
                let mut rest = BytesMut::from(&chunk[..]);
                rest.extend_from_slice(&self.control_out);
                self.control_out = rest.freeze();
                Poll::Pending
            }
        }
    }

    /// Returns a frame header carved from the shared header buffer.
    fn frame_header(&mut self, frame_type: FrameType, len: usize) -> Bytes {
        if self.out.capacity() < 16 {
            self.out.reserve(FRAME_BUFFER);
        }
        encode_frame_header(&mut self.out, frame_type, len as u64)
            .expect("a payload length fits and the frame type is defined");
        self.out.split().freeze()
    }
}

/// Takes one step of reading a peer unidirectional stream. Returns the verdict and whether this
/// step read the stream's type, in which case the caller reads again.
fn read_uni_step<T: Transport>(t: &mut T, state: &mut State, index: usize) -> (Verdict, bool) {
    let id = state.uni[index].id;
    let Ok(stream) = t.stream(id) else {
        return (Verdict::Continue, false);
    };
    state.segments.clear();
    state.segments.extend(stream.readable().segments().cloned());
    let mut used = 0usize;
    let mut verdict = Verdict::Continue;
    let mut retyped = false;
    match &mut state.uni[index].kind {
        UniKind::Header(decoder) => {
            for segment in &state.segments {
                let mut input = segment.clone();
                let header = decoder.decode(&mut input);
                used += segment.len() - input.len();
                match header {
                    Ok(None) => continue,
                    Ok(Some(header)) => {
                        let kind = match header {
                            UniStreamHeader::Control if !state.peer_control => {
                                state.peer_control = true;
                                UniKind::Control(FrameDecoder::new(
                                    StreamKind::Control,
                                    Role::Server,
                                    FrameLimits::default(),
                                ))
                            }
                            UniStreamHeader::QpackEncoder if !state.peer_encoder => {
                                state.peer_encoder = true;
                                UniKind::QpackEncoder
                            }
                            UniStreamHeader::QpackDecoder if !state.peer_decoder => {
                                state.peer_decoder = true;
                                UniKind::QpackDecoder
                            }
                            // A second control or QPACK stream (RFC 9114, RFC 9204).
                            UniStreamHeader::Control
                            | UniStreamHeader::QpackEncoder
                            | UniStreamHeader::QpackDecoder => {
                                verdict = Verdict::Connection(H3ErrorCode::STREAM_CREATION_ERROR);
                                break;
                            }
                            UniStreamHeader::Push { .. } | UniStreamHeader::Unknown(_) => {
                                UniKind::Discard
                            }
                        };
                        state.uni[index].kind = kind;
                        retyped = true;
                        break;
                    }
                    Err(error) => {
                        verdict = Verdict::Connection(error.code());
                        break;
                    }
                }
            }
        }
        UniKind::Control(decoder) => {
            'segments: for segment in &state.segments {
                let mut input = segment.clone();
                loop {
                    match decoder.decode(&mut input) {
                        Ok(None) => break,
                        // The server never promises a push, so a push ID cannot be cancelled
                        // (RFC 9114).
                        Ok(Some(Frame::CancelPush(_))) => {
                            verdict = Verdict::Connection(H3ErrorCode::ID_ERROR);
                            break 'segments;
                        }
                        // The decoder validated SETTINGS, and the client's settings do not
                        // change what this server sends.
                        Ok(Some(Frame::Settings(_))) => state.peer_settings = true,
                        // A client's GOAWAY limits pushes, and MAX_PUSH_ID permits them; the
                        // server never pushes.
                        Ok(Some(_)) => {}
                        Err(error) => {
                            verdict = Verdict::Connection(error.code());
                            break 'segments;
                        }
                    }
                }
                used += segment.len();
            }
        }
        UniKind::QpackEncoder | UniKind::QpackDecoder => {
            state.scratch.clear();
            for segment in &state.segments {
                state.scratch.extend_from_slice(segment);
            }
            let fed = if matches!(state.uni[index].kind, UniKind::QpackEncoder) {
                state
                    .qpack_decoder
                    .feed_encoder_stream(&state.scratch)
                    .map_err(|_| QPACK_ENCODER_STREAM_ERROR)
            } else {
                state
                    .qpack_encoder
                    .feed_decoder_stream(&state.scratch)
                    .map_err(|_| QPACK_DECODER_STREAM_ERROR)
            };
            match fed {
                Ok(n) => used = n,
                Err(code) => verdict = Verdict::Connection(qpack_code(code)),
            }
        }
        UniKind::Discard => used = state.segments.iter().map(Bytes::len).sum(),
    }
    drop(stream);
    if used > 0
        && let Ok(mut stream) = t.stream(id)
    {
        let _ = stream.consume(used);
    }
    (verdict, retyped)
}

/// Parses the request stream's received bytes into the exchange, within the room its ingress
/// body has, applies the credit its application returned, and ends ingress at the FIN.
fn read_frames<T: Transport, E: OwnedExchange>(
    t: &mut T,
    state: &mut State,
    id: StreamId,
    exchange: &mut Exchange<E>,
) -> Verdict {
    if let Some(feed) = &exchange.feed {
        exchange.app_consumed = (exchange.app_consumed + feed.cell.take()).min(exchange.handed);
    }
    if !exchange.ingress.open() {
        settle_credit(t, id, exchange);
        return Verdict::Continue;
    }
    let Ok(stream) = t.stream(id) else {
        return Verdict::Continue;
    };
    // The transport reports the FIN with the bytes before it, ahead of its Finished edge, so a
    // request whose head and FIN arrive together is delivered complete.
    let fin = stream.peer_finished();
    let readable = stream.readable();
    let end = exchange.consumed + readable.chain_length() as u64;
    let mut skip = (exchange.parsed - exchange.consumed) as usize;
    state.segments.clear();
    for segment in readable.segments() {
        if skip >= segment.len() {
            skip -= segment.len();
            continue;
        }
        state.segments.push(segment.slice(skip..));
        skip = 0;
    }
    drop(stream);

    let buffer = state.config.request_body_buffer;
    // Moved out of the state while the loop runs, since `on_headers` takes the whole state.
    let segments = mem::take(&mut state.segments);
    let mut verdict = Verdict::Continue;
    'segments: for segment in &segments {
        let mut position = 0;
        while position < segment.len() {
            let room = exchange.room(buffer);
            if room == 0 {
                break 'segments;
            }
            let mut input = segment.slice(position..segment.len().min(position + room));
            let before = input.len();
            let frame = exchange.decoder.decode(&mut input);
            let used = before - input.len();
            position += used;
            exchange.parsed += used as u64;
            verdict = match frame {
                Ok(Some(Frame::Data(data))) => {
                    exchange.overhead += (used - data.len()) as u64;
                    on_data(exchange, data)
                }
                Ok(Some(Frame::Headers(block))) => {
                    exchange.overhead += used as u64;
                    let complete = fin && exchange.parsed == end;
                    on_headers(state, id, exchange, &block, complete)
                }
                Ok(_) => {
                    exchange.overhead += used as u64;
                    Verdict::Continue
                }
                Err(error) => Verdict::Connection(error.code()),
            };
            if verdict != Verdict::Continue {
                break 'segments;
            }
        }
    }
    state.segments = segments;
    if verdict != Verdict::Continue {
        return verdict;
    }
    settle_credit(t, id, exchange);
    if fin && exchange.parsed == end {
        return end_ingress(t.now(), exchange);
    }
    Verdict::Continue
}

fn on_data<E>(exchange: &mut Exchange<E>, data: Bytes) -> Verdict {
    match exchange.ingress {
        Ingress::Body => {}
        // RFC 9114 forbids DATA before the head or after the trailers.
        Ingress::Head | Ingress::Trailers => {
            return Verdict::Connection(H3ErrorCode::FRAME_UNEXPECTED);
        }
        Ingress::Complete | Ingress::Abandoned => return Verdict::Continue,
    }
    let len = data.len() as u64;
    exchange.received += len;
    if exchange
        .content_length
        .is_some_and(|expected| exchange.received > expected)
    {
        return Verdict::Stream(H3ErrorCode::MESSAGE_ERROR);
    }
    if len == 0 {
        return Verdict::Continue;
    }
    exchange.handed += len;
    exchange.body_events = true;
    let body = BufQueue::from_bytes(data, len as usize)
        .expect("a queue sized for its one segment accepts it");
    let taken = exchange
        .producer()
        .is_some_and(|producer| producer.body(body, 0, false) != FlowControlState::Error);
    if !taken {
        // The body is not read any more, or does not exist: its bytes count as consumed.
        exchange.app_consumed += len;
    }
    Verdict::Continue
}

/// Decodes a HEADERS frame: the request head, or the trailers. `complete` is whether the peer's
/// FIN directly follows the frame.
fn on_headers<E>(
    state: &mut State,
    id: StreamId,
    exchange: &mut Exchange<E>,
    block: &[u8],
    complete: bool,
) -> Verdict {
    let max_size = state.config.max_field_section_size;
    match exchange.ingress {
        Ingress::Head => match fields::decode_request(&mut state.qpack_decoder, block, max_size) {
            Ok(head) => {
                let mut message = head.message;
                // A request that is complete at its head carries no body event, so its body is
                // built only for an application that took the completion. A CONNECT's body is the
                // upload half of its tunnel and is always built.
                if exchange.feed.is_none() && (!complete || head.connect) {
                    let (body, feed) = ingress_body(state, id);
                    exchange.body = Some(body);
                    exchange.feed = Some(feed);
                }
                if let Some(body) = &exchange.body
                    && !body.initialize_body_only(&mut message, false)
                {
                    return Verdict::Stream(H3ErrorCode::MESSAGE_ERROR);
                }
                exchange.content_length = head.content_length;
                exchange.connect = head.connect;
                exchange.head_request = head.head_request;
                exchange.head = Some(message);
                exchange.ingress = Ingress::Body;
                Verdict::Continue
            }
            Err(error) => field_verdict(error),
        },
        // RFC 9114: a CONNECT stream carries only DATA after the head.
        Ingress::Body if exchange.connect => Verdict::Connection(H3ErrorCode::FRAME_UNEXPECTED),
        Ingress::Body => match fields::decode_trailers(&mut state.qpack_decoder, block, max_size) {
            Ok(trailers) => {
                if exchange
                    .content_length
                    .is_some_and(|expected| expected != exchange.received)
                {
                    return Verdict::Stream(H3ErrorCode::MESSAGE_ERROR);
                }
                exchange.body_events = true;
                exchange.ingress = Ingress::Trailers;
                // The source leaves the body open after trailers; the trailers end the message.
                if let Some(producer) = exchange.producer() {
                    let _ = producer.trailers(trailers) && producer.eom();
                }
                Verdict::Continue
            }
            Err(error) => field_verdict(error),
        },
        // RFC 9114 forbids a HEADERS frame after the trailers.
        Ingress::Trailers => Verdict::Connection(H3ErrorCode::FRAME_UNEXPECTED),
        Ingress::Complete | Ingress::Abandoned => Verdict::Continue,
    }
}

fn field_verdict(error: FieldError) -> Verdict {
    match error {
        FieldError::Malformed => Verdict::Stream(H3ErrorCode::MESSAGE_ERROR),
        FieldError::Qpack(error) if error.is_connection_error() => {
            Verdict::Connection(qpack_code(QPACK_DECOMPRESSION_FAILED))
        }
        FieldError::Qpack(_) => Verdict::Stream(qpack_code(QPACK_DECOMPRESSION_FAILED)),
    }
}

/// Ends a request's ingress once the peer's FIN arrived and every byte before it was parsed.
fn end_ingress<E>(now: Instant, exchange: &mut Exchange<E>) -> Verdict {
    if let Err(error) = exchange.decoder.finish() {
        return Verdict::Connection(error.code());
    }
    match exchange.ingress {
        // RFC 9114: the stream ended before the request was complete.
        Ingress::Head => Verdict::Stream(H3ErrorCode::REQUEST_INCOMPLETE),
        Ingress::Body => {
            if exchange
                .content_length
                .is_some_and(|expected| expected != exchange.received)
            {
                return Verdict::Stream(H3ErrorCode::MESSAGE_ERROR);
            }
            exchange.ingress = Ingress::Complete;
            if let Some(producer) = exchange.producer() {
                if exchange.body_events {
                    producer.eom();
                } else {
                    producer.eom_at(now);
                }
            }
            Verdict::Continue
        }
        Ingress::Trailers => {
            exchange.ingress = Ingress::Complete;
            Verdict::Continue
        }
        Ingress::Complete | Ingress::Abandoned => Verdict::Continue,
    }
}

/// Returns the credit of the bytes parsed that are not body data and of the body bytes the
/// application consumed. The transport frees a prefix of the received bytes; every byte of it
/// was parsed, and body bytes stay shared with the ingress body.
fn settle_credit<T: Transport, E>(t: &mut T, id: StreamId, exchange: &mut Exchange<E>) {
    let target = exchange.overhead + exchange.app_consumed;
    // After STOP_SENDING the transport has released the stream's unread bytes itself.
    if target <= exchange.consumed || exchange.ingress == Ingress::Abandoned {
        return;
    }
    if let Ok(mut stream) = t.stream(id)
        && stream
            .consume((target - exchange.consumed) as usize)
            .is_ok()
    {
        exchange.consumed = target;
    }
}

/// Hands the request to the application, then stages a fixed response at once or wakes the
/// stream for the response.
fn deliver<T: Transport, E: OwnedExchange>(
    t: &mut T,
    state: &mut State,
    id: StreamId,
    exchange: &mut Exchange<E>,
) -> Result<(), HttpError> {
    let (Some(application), Some(head)) = (exchange.application.as_mut(), exchange.head.take())
    else {
        return Ok(());
    };
    let eom = exchange.ingress == Ingress::Complete && !exchange.body_events;
    let body = match exchange.body.take() {
        Some(body) if eom => body.retire_empty_body(&mut Context::from_waker(Waker::noop()))?,
        Some(body) => body,
        None => {
            debug_assert!(eom, "only a request complete at its head has no body");
            HttpIngressBody::header_only()
        }
    };
    if let Ok(mut stream) = t.stream(id) {
        stream.set_order(fields::request_order(&head));
    }
    exchange.delivered = true;
    application.request_body(head, body, eom);
    if let Some(waker) = exchange.waker.take() {
        waker.wake();
    } else if !stage_fixed(t, state, id, exchange) {
        t.stream_waker(id).wake();
    }
    Ok(())
}

/// Stages the response of a just-delivered request whose exchange holds it whole, with a body of
/// at most `INLINE_BODY_MAX` bytes, within the connection's bound for the phase. A CONNECT keeps
/// the pull path. The body is staged only whole, in one DATA frame (see [`WholeBody`]). Returns
/// false if the stream needs its wake, for the whole response or the rest of it.
fn stage_fixed<T: Transport, E: OwnedExchange>(
    t: &mut T,
    state: &mut State,
    id: StreamId,
    exchange: &mut Exchange<E>,
) -> bool {
    let Some(body) = exchange
        .application
        .as_ref()
        .and_then(OwnedExchange::fixed_response_len)
        .filter(|&len| len <= INLINE_BODY_MAX)
    else {
        return false;
    };
    if exchange.connect {
        return false;
    }
    let now = t.now();
    let work = if state.inline_at == Some(now) {
        state.inline_work
    } else {
        0
    };
    if work >= INLINE_STAGING {
        return false;
    }
    let Ok(stream) = t.stream(id) else {
        return false;
    };
    let mut sink = WholeBody {
        sink: StreamSink::new(stream),
        body,
    };
    let mut progress = false;
    // A fixed response's polls do not register a waker, so the context's waker is never used.
    let mut cx = Context::from_waker(Waker::noop());
    let produced = produce(state, exchange, &mut cx, &mut sink, &mut progress);
    state.inline_at = Some(now);
    state.inline_work = work + sink.sink.staged().max(INLINE_CHARGE);
    // A failed response is cancelled and its stream reset through the work queue, so a poll would
    // find nothing to do.
    !settle_response(state, id, exchange, produced) || exchange.egress == Egress::Done
}

/// The sink a fixed response is staged through at delivery. It offers the stream's allowance only
/// while that allowance lets one body poll take the whole body, so the body goes out in the one
/// DATA frame that a pull with room for it stages, or waits for the pull step, which sizes the
/// frame by its own allowance. Bytes that do not fit wait in the exchange's pending bytes.
struct WholeBody<S> {
    sink: S,
    /// The fixed body's length.
    body: usize,
}

impl<S: PullSink> PullSink for WholeBody<S> {
    fn allowance(&self) -> usize {
        let allowance = self.sink.allowance();
        if body_maximum(allowance) >= self.body {
            allowance
        } else {
            0
        }
    }

    fn push(&mut self, data: Bytes) -> Result<(), SinkError> {
        self.sink.push(data)
    }

    fn push_parts(&mut self, header: Bytes, shared: Bytes) -> Result<(), SinkError> {
        self.sink.push_parts(header, shared)
    }

    fn finish(&mut self) -> Result<(), SinkError> {
        self.sink.finish()
    }
}

/// Produces response frames into the sink until the application or the allowance runs out.
fn produce<E: OwnedExchange, S: PullSink>(
    state: &mut State,
    exchange: &mut Exchange<E>,
    cx: &mut Context<'_>,
    sink: &mut S,
    progress: &mut bool,
) -> Result<(), HttpError> {
    for _ in 0..EVENTS_PER_POLL {
        if !exchange.pending.is_empty() {
            match flush(sink, &mut exchange.pending) {
                Ok(pushed) => *progress |= pushed,
                Err(_) => return Ok(()),
            }
            if !exchange.pending.is_empty() {
                return Ok(());
            }
        }
        let application = exchange
            .application
            .as_mut()
            .expect("a producing exchange owns its application");
        match exchange.egress {
            Egress::Done => return Ok(()),
            Egress::Finish => {
                if sink.finish().is_ok() {
                    *progress = true;
                    exchange.egress = Egress::Done;
                }
                return Ok(());
            }
            Egress::Headers => {
                let Poll::Ready(event) = application.poll_headers(cx) else {
                    return Ok(());
                };
                let mut event = event?;
                let status = event.headers().response_status().unwrap_or(0);
                let Some(size) = fields::encode_response(
                    &state.qpack_encoder,
                    event.headers(),
                    &mut state.names,
                    &mut state.out,
                ) else {
                    return Err(HttpError::with_message(
                        HttpErrorCode::INTERNAL_ERROR,
                        b"response head has no status HTTP/3 can carry",
                    ));
                };
                event.on_headers_encoded(size);
                if !stage_headers(state, sink, &mut exchange.pending, progress) {
                    return Ok(());
                }
                if status >= 200 {
                    exchange.tunnel = exchange.connect && (200..300).contains(&status);
                    exchange.egress = if event.eom() {
                        Egress::Finish
                    } else {
                        Egress::Body
                    };
                }
            }
            Egress::Body => {
                if let Some(resume) = &mut exchange.resume {
                    match resume.as_mut().poll(cx) {
                        Poll::Pending => return Ok(()),
                        Poll::Ready(ResumeStatus::Cancelled) => {
                            return Err(HttpError::with_message(
                                HttpErrorCode::CORO_CANCELLED,
                                b"response body resume cancelled",
                            ));
                        }
                        Poll::Ready(_) => exchange.resume = None,
                    }
                }
                let allowance = sink.allowance();
                if allowance == 0 {
                    return Ok(());
                }
                let maximum = body_maximum(allowance);
                let Poll::Ready(event) = application.poll_body(cx, maximum as u32) else {
                    return Ok(());
                };
                let event = event?;
                let eom = event.eom();
                match event.into_kind() {
                    // RFC 9110: a HEAD response does not carry content.
                    HttpBodyEventKind::Body(_) if exchange.head_request => {}
                    HttpBodyEventKind::Body(body) => {
                        if body.chain_length() > maximum {
                            return Err(HttpError::with_message(
                                HttpErrorCode::MESSAGE_ERROR,
                                b"response body event exceeds its allowance",
                            ));
                        }
                        if !body.is_empty() {
                            let header = state.frame_header(FrameType::DATA, body.chain_length());
                            match stage(sink, &mut exchange.pending, header, body) {
                                Ok(pushed) => *progress |= pushed,
                                Err(_) => return Ok(()),
                            }
                        }
                    }
                    // RFC 9114: a tunnel carries only DATA.
                    HttpBodyEventKind::Trailers(_) if exchange.tunnel => {}
                    HttpBodyEventKind::Trailers(trailers) => {
                        fields::encode_trailers(
                            &state.qpack_encoder,
                            &trailers,
                            &mut state.names,
                            &mut state.out,
                        );
                        if !stage_headers(state, sink, &mut exchange.pending, progress) {
                            return Ok(());
                        }
                    }
                    HttpBodyEventKind::Suspend(resume) => exchange.resume = Some(resume),
                    HttpBodyEventKind::Padding(_) => {}
                    HttpBodyEventKind::Upgrade | HttpBodyEventKind::PushPromise(_) => {
                        return Err(HttpError::with_message(
                            HttpErrorCode::PROTOCOL_ERROR,
                            b"HTTP/3 responses carry no upgrade or push",
                        ));
                    }
                }
                if eom {
                    exchange.egress = Egress::Finish;
                }
            }
        }
    }
    // The cap ended the call while every poll returned Ready, so the polls did not register a
    // waker: put the stream back on the pull-ready list.
    cx.waker().wake_by_ref();
    Ok(())
}

/// Applies the outcome of `produce`: a failed response cancels the exchange and queues its stream's
/// reset, and a response whose FIN is staged completes. Returns false if the response failed.
fn settle_response<E: OwnedExchange>(
    state: &State,
    id: StreamId,
    exchange: &mut Exchange<E>,
    produced: Result<(), HttpError>,
) -> bool {
    if let Err(failure) = produced {
        cancel(exchange, &failure, false);
        // RFC 9114: an error in a tunnel's TCP connection is H3_CONNECT_ERROR, and any other
        // response abandoned after partial processing is H3_REQUEST_CANCELLED.
        let code = if exchange.tunnel && failure.code() == HttpErrorCode::CONNECT_ERROR {
            H3ErrorCode::CONNECT_ERROR
        } else {
            H3ErrorCode::REQUEST_CANCELLED
        };
        exchange.reset_owed = Some(code);
        state.work.push(id);
        return false;
    }
    if exchange.egress == Egress::Done && !exchange.response_complete {
        on_fin_staged(state, id, exchange);
    }
    true
}

/// Frames the field section encoded at the start of the state's buffer as HEADERS and stages
/// it. Returns false if the stream's send half ended.
fn stage_headers<S: PullSink>(
    state: &mut State,
    sink: &mut S,
    pending: &mut BufQueue,
    progress: &mut bool,
) -> bool {
    let block = state.out.split().freeze();
    let header = state.frame_header(FrameType::HEADERS, block.len());
    let block = BufQueue::from_bytes(block.clone(), block.len())
        .expect("a queue sized for its one segment accepts it");
    match stage(sink, pending, header, block) {
        Ok(pushed) => {
            *progress |= pushed;
            true
        }
        Err(_) => false,
    }
}

/// Returns the body limit of one `poll_body`: the allowance, at most 64 KiB, less the DATA frame
/// header. With a tiny allowance it is still one byte, whose frame goes out over several polls.
fn body_maximum(allowance: usize) -> usize {
    let limit = allowance.min(MAX_BODY_POLL);
    limit
        .saturating_sub(frame_header_len(FrameType::DATA, limit as u64))
        .max(1)
}

/// Stages a frame header and its payload: as `push_parts` when they fit the allowance, so the
/// payload is not copied, and otherwise through the pending bytes.
fn stage<S: PullSink>(
    sink: &mut S,
    pending: &mut BufQueue,
    header: Bytes,
    mut payload: BufQueue,
) -> Result<bool, SinkError> {
    if pending.is_empty() && header.len() + payload.chain_length() <= sink.allowance() {
        let mut segments = payload.into_segment_iter();
        match segments.next() {
            Some(first) => sink.push_parts(header, first)?,
            None => sink.push(header)?,
        }
        for segment in segments {
            sink.push(segment)?;
        }
        return Ok(true);
    }
    pending
        .push_bytes(header)
        .expect("the pending queue is unbounded");
    pending
        .append(&mut payload)
        .expect("the pending queue is unbounded");
    flush(sink, pending)
}

/// Pushes as many pending bytes as the allowance takes.
fn flush<S: PullSink>(sink: &mut S, pending: &mut BufQueue) -> Result<bool, SinkError> {
    let allowance = sink.allowance();
    if allowance == 0 || pending.is_empty() {
        return Ok(false);
    }
    for segment in pending.split_at_most(allowance).into_segment_iter() {
        sink.push(segment)?;
    }
    Ok(true)
}

/// Marks the response complete once its FIN is staged.
fn on_fin_staged<E: OwnedExchange>(state: &State, id: StreamId, exchange: &mut Exchange<E>) {
    exchange.response_complete = true;
    if let Some(application) = &mut exchange.application {
        application.response_complete();
    }
    match exchange.ingress {
        Ingress::Complete => retire(exchange),
        Ingress::Abandoned => {}
        // The client of a tunnel may go on sending after the target closed (RFC 9114); the
        // exchange retires when the request ends.
        _ if exchange.tunnel => {}
        // An early response: STOP_SENDING needs the stream, which the sink cannot reach.
        Ingress::Head | Ingress::Body | Ingress::Trailers => {
            exchange.stop_owed = true;
            state.work.push(id);
        }
    }
}

/// Completes an exchange whose response is staged and whose request arrived whole, without
/// waiting for its writes (the retirement exception of `roxygen_session::owned`). The entry
/// stays until its stream's `Closed` edge, and the credit of body bytes the application still
/// holds keeps returning through the credit cell.
fn retire<E: OwnedExchange>(exchange: &mut Exchange<E>) {
    if let Some(application) = exchange.application.take() {
        application.complete();
    }
    exchange.retired = true;
}

/// Cancels an exchange and its ingress body. A cancelled exchange is dropped, not completed.
fn cancel<E: OwnedExchange>(exchange: &mut Exchange<E>, error: &HttpError, peer_reset: bool) {
    if let Some(producer) = exchange.producer() {
        producer.abort_with_error(error.clone(), b"HTTP/3 exchange cancelled");
    }
    if let Some(mut application) = exchange.application.take() {
        application.cancel(error, peer_reset);
    }
}

/// Abandons both directions of a request stream (RFC 9114).
fn abort_stream<T: Transport>(t: &mut T, id: StreamId, code: H3ErrorCode) {
    if let Ok(mut stream) = t.stream(id) {
        let _ = stream.reset(app_code(code));
        let _ = stream.stop_sending(app_code(code));
    }
}

/// Returns the credit of every byte left on a stream whose body will not be read.
fn release<T: Transport>(t: &mut T, id: StreamId) {
    if let Ok(mut stream) = t.stream(id) {
        let left = stream.readable().chain_length();
        if left > 0 {
            let _ = stream.consume(left);
        }
    }
}

fn app_code(code: H3ErrorCode) -> AppCode {
    AppCode(
        VarInt::from_u64(code.as_u64()).expect("HTTP/3 error codes fit a variable-length integer"),
    )
}

fn qpack_code(code: u64) -> H3ErrorCode {
    H3ErrorCode::new(code).expect("QPACK error codes fit a variable-length integer")
}

/// Maps a local stream or connection error code to the Roxygen error code an exchange is
/// cancelled with.
fn error_code(code: H3ErrorCode) -> HttpErrorCode {
    match code {
        H3ErrorCode::MESSAGE_ERROR => HttpErrorCode::MESSAGE_ERROR,
        H3ErrorCode::REQUEST_REJECTED => HttpErrorCode::REFUSED_STREAM,
        H3ErrorCode::INTERNAL_ERROR => HttpErrorCode::INTERNAL_ERROR,
        H3ErrorCode::EXCESSIVE_LOAD => HttpErrorCode::ENHANCE_YOUR_CALM,
        H3ErrorCode::NO_ERROR => HttpErrorCode::CANCEL,
        code if code == qpack_code(QPACK_DECOMPRESSION_FAILED) => HttpErrorCode::COMPRESSION_ERROR,
        _ => HttpErrorCode::PROTOCOL_ERROR,
    }
}
