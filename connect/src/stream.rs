//! `HttpConnectStream`, a CONNECT tunnel over a proxy session.

use std::any::Any;
use std::cell::Cell;
use std::cell::OnceCell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt::Write;
use std::future::Future;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use roxygen_core::buf_queue::BufQueue;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::http::HttpMethod;
use roxygen_core::ids::StreamId;
use roxygen_core::ingress_completion::IngressCompletion;
use roxygen_core::message::HttpMessage;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_core::stream_source::FlowControlState;
use roxygen_core::stream_source::HttpStreamSource;
use roxygen_core::stream_source::HttpStreamSourceCallback;
use roxygen_core::stream_source::HttpStreamSourceProducer;
use roxygen_core::stream_source::HttpStreamSourceSharedCallback;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::cancellation::CancellationWakeRegistration;
use roxygen_runtime::panic::discard_panic;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerId;
use roxygen_session::experimental::UniplexResponseAbortExt;
use roxygen_session::experimental::UniplexStreamAbort;
use roxygen_session::session::RequestReservation;
use roxygen_session::uniplex::UniplexResponse;

use crate::experimental::ConnectRejected;
use crate::session::ConnectSession;
use crate::session::LocalConnectFuture;

/// CONNECT request headers, added in key order (`std::map`, HTTPConnectStream.h:41).
pub type RequestHeaderMap = BTreeMap<Box<[u8]>, Box<[u8]>>;

/// The default egress buffer of a tunnel (HTTPConnectStream.h:53, 65).
pub const DEFAULT_EGRESS_BUFFER_SIZE: u32 = 256 * 1024;

type SendFuture = LocalConnectFuture<Result<UniplexResponse, HttpError>>;

/// A CONNECT tunnel established on a proxy session (`HTTPConnectStream`).
///
/// The tunnel's egress is a request body source committed to the proxy
/// session, and its ingress is the CONNECT response source. Dropping the
/// stream closes both directions and, for `connect_unique`, drains the session.
pub struct HttpConnectStream {
    state: Rc<StreamState>,
    session: Option<Box<dyn ConnectSession>>,
    /// The final 2xx CONNECT response head (`DIV-011`).
    pub(crate) connect_response: Option<Box<HttpMessage>>,
    /// The committed stream's abort (`DIV-023`).
    abort: Option<UniplexStreamAbort>,
    /// Until the final head arrives, teardown aborts a committed stream
    /// instead of closing it (`DIV-023`).
    reset_on_cancel: bool,
}

struct StreamState {
    worker_id: WorkerId,
    egress_buffer_size: u32,
    local_address: SocketAddress,
    peer_address: SocketAddress,
    connected_peer_address: OnceCell<SocketAddress>,
    local_endpoint: TransportEndpoint,
    peer_endpoint: TransportEndpoint,
    connect_headers: RefCell<RequestHeaderMap>,
    egress: RefCell<Option<HttpStreamSourceProducer>>,
    egress_eom_seen: Cell<bool>,
    egress_error: OnceCell<HttpError>,
    ingress: RefCell<HttpSourceHolder>,
    user_session_id: OnceCell<Box<[u8]>>,
    callback: RefCell<Option<Weak<dyn HttpStreamSourceSharedCallback>>>,
}

impl StreamState {
    fn callback(&self) -> Option<Rc<dyn HttpStreamSourceSharedCallback>> {
        self.callback.borrow().as_ref().and_then(Weak::upgrade)
    }

    fn egress_producer(&self) -> Option<HttpStreamSourceProducer> {
        self.egress.borrow().clone()
    }

    fn can_read(&self) -> bool {
        self.ingress.borrow().readable()
    }

    fn can_write(&self) -> bool {
        self.egress.borrow().is_some()
            && !self.egress_eom_seen.get()
            && self.egress_error.get().is_none()
    }

    fn stop_ingress(&self, error: Option<HttpErrorCode>) {
        let mut ingress = std::mem::take(&mut *self.ingress.borrow_mut());
        ingress.clear(error);
    }

    fn shutdown_read(&self) {
        if self.can_read() {
            self.stop_ingress(None);
        }
    }

    fn shutdown_write(&self) {
        if self.can_write() {
            self.egress_eom();
        }
    }

    // Producer calls can run egress callbacks inline, so none of them holds a
    // `RefCell` borrow of this state.
    fn egress_body(&self, body: BufQueue) -> FlowControlState {
        let Some(producer) = self.egress_producer() else {
            return FlowControlState::Error;
        };
        producer.body(body, 0, false)
    }

    fn egress_eom(&self) -> bool {
        let Some(producer) = self.egress_producer() else {
            return false;
        };
        self.egress_eom_seen.set(true);
        producer.eom()
    }

    fn egress_abort(&self, error: HttpErrorCode) -> bool {
        let Some(producer) = self.egress_producer() else {
            return false;
        };
        self.egress_eom_seen.set(true);
        producer.abort(error, b"")
    }
}

/// `HTTPStreamSource::Callback` of the egress source. It holds the stream
/// state weakly, so neither the session's egress source nor the installed
/// callback keeps the stream alive.
struct EgressCallbacks {
    stream: Weak<StreamState>,
}

impl EgressCallbacks {
    fn callback(&self) -> Option<Rc<dyn HttpStreamSourceSharedCallback>> {
        self.stream.upgrade()?.callback()
    }
}

impl HttpStreamSourceCallback for EgressCallbacks {
    fn bytes_processed(&mut self, id: StreamId, amount: usize, to_acknowledge: usize) {
        if let Some(callback) = self.callback() {
            callback.bytes_processed(id, amount, to_acknowledge);
        }
    }

    fn window_open(&mut self, id: StreamId) {
        if let Some(callback) = self.callback() {
            callback.window_open(id);
        }
    }

    fn source_complete(&mut self, id: StreamId, error: Option<HttpError>) {
        let Some(stream) = self.stream.upgrade() else {
            return;
        };
        *stream.egress.borrow_mut() = None;
        if let Some(error) = &error {
            let _ = stream.egress_error.set(error.clone());
        }
        if let Some(callback) = stream.callback() {
            callback.source_complete(id, error);
        }
    }
}

fn cancelled() -> HttpError {
    HttpError::with_message(HttpErrorCode::CORO_CANCELLED, b"Cancelled")
}

/// `folly::SocketAddress::setFromIpPort` and `getAddressStr`: the value, read
/// as a C string, resolved by glibc `getaddrinfo(AI_NUMERICHOST)`, and printed
/// by `inet_ntop`. `SocketAddress` has no IPv6 zone index, so a value with one
/// is ignored like an invalid value.
fn connected_peer(value: &[u8], port: u16) -> Option<SocketAddress> {
    let value = &value[..value
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(value.len())];
    let host = match inet_aton_exact(value) {
        Some(address) => address.to_string(),
        None => ipv6_presentation(std::str::from_utf8(value).ok()?.parse::<Ipv6Addr>().ok()?),
    };
    Some(SocketAddress::new(host, port))
}

/// glibc `__inet_aton_exact`: one to four dot-separated C integers, of which
/// each but the last is one byte and the last fills the remaining bytes.
fn inet_aton_exact(text: &[u8]) -> Option<Ipv4Addr> {
    let mut address = 0_u32;
    let mut rest = text;
    for part in 0_u32..4 {
        let (value, after) = c_number(rest)?;
        match after {
            [] if value <= u32::MAX >> (8 * part) => return Some(Ipv4Addr::from(address | value)),
            [b'.', next @ ..] if part < 3 && value <= 0xff => {
                address |= value << (24 - 8 * part);
                rest = next;
            }
            _ => return None,
        }
    }
    None
}

/// `strtoul(text, &end, 0)` where `text` must start with a digit: `0x` hex,
/// `0` octal, or decimal. Values above `u32::MAX` fail, as `inet_aton` rejects
/// them.
fn c_number(text: &[u8]) -> Option<(u32, &[u8])> {
    if !text.first()?.is_ascii_digit() {
        return None;
    }
    let (radix, digits) = match text {
        [b'0', b'x' | b'X', next, ..] if next.is_ascii_hexdigit() => (16, &text[2..]),
        [b'0', ..] => (8, text),
        _ => (10, text),
    };
    let length = digits
        .iter()
        .take_while(|byte| char::from(**byte).is_digit(radix))
        .count();
    let value = digits[..length].iter().try_fold(0_u32, |value, byte| {
        value
            .checked_mul(radix)?
            .checked_add(char::from(*byte).to_digit(radix)?)
    })?;
    Some((value, &digits[length..]))
}

/// glibc `inet_ntop(AF_INET6)`: the first longest run of two or more zero
/// words becomes `::`, and an address whose first six words are zero, or
/// whose first five are zero and sixth is `ffff`, ends in dotted IPv4.
fn ipv6_presentation(address: Ipv6Addr) -> String {
    let words = address.segments();
    let mut best: Option<(usize, usize)> = None;
    let mut run: Option<(usize, usize)> = None;
    for (index, word) in words.iter().enumerate() {
        run = (*word == 0).then(|| run.map_or((index, 1), |(base, length)| (base, length + 1)));
        if let Some((base, length)) = run
            && length >= 2
            && best.is_none_or(|(_, best_length)| length > best_length)
        {
            best = Some((base, length));
        }
    }
    let mut text = String::new();
    for (index, word) in words.iter().enumerate() {
        if let Some((base, length)) = best
            && (base..base + length).contains(&index)
        {
            if index == base {
                text.push(':');
            }
            continue;
        }
        if index != 0 {
            text.push(':');
        }
        if index == 6
            && let Some((0, length)) = best
            && (length == 6 || (length == 5 && words[5] == 0xffff))
        {
            let [.., a, b, c, d] = address.octets();
            write!(text, "{}", Ipv4Addr::new(a, b, c, d)).expect("should write to a String");
            break;
        }
        write!(text, "{word:x}").expect("should write to a String");
    }
    if let Some((base, length)) = best
        && base + length == words.len()
    {
        text.push(':');
    }
    text
}

impl HttpConnectStream {
    /// Establishes a CONNECT tunnel to `authority` on a session that another
    /// owner, such as a pool, keeps (`HTTPConnectStream::connect`).
    ///
    /// The stream keeps no session and never drains it. Because the returned
    /// future cannot borrow `session`, this call reads the session, creates the
    /// egress source, checks `cancellation`, and calls `send_connect_request`
    /// before returning; the future checks `cancellation` again on its first
    /// poll. `cancellation` covers the request commit and the wait for the
    /// final response head, never the established tunnel.
    #[expect(
        clippy::too_many_arguments,
        reason = "ports the frozen HTTPConnectStream::connect parameters one to one"
    )]
    pub fn connect<S: ConnectSession>(
        context: &WorkerContext,
        session: &S,
        reservation: RequestReservation,
        authority: &str,
        timeout: Duration,
        connect_headers: RequestHeaderMap,
        egress_buffer_size: u32,
        cancellation: CancellationToken,
    ) -> LocalConnectFuture<Result<HttpConnectStream, HttpError>> {
        Self::connect_inner(
            context,
            session,
            reservation,
            authority,
            timeout,
            connect_headers,
            egress_buffer_size,
            cancellation,
            false,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "connect's parameters plus the experimental reset_on_cancel"
    )]
    pub(crate) fn connect_inner<S: ConnectSession>(
        context: &WorkerContext,
        session: &S,
        reservation: RequestReservation,
        authority: &str,
        timeout: Duration,
        connect_headers: RequestHeaderMap,
        egress_buffer_size: u32,
        cancellation: CancellationToken,
        reset_on_cancel: bool,
    ) -> LocalConnectFuture<Result<HttpConnectStream, HttpError>> {
        let (mut stream, egress) = match Self::construct(
            context,
            session,
            connect_headers,
            egress_buffer_size,
            reset_on_cancel,
        ) {
            Ok(constructed) => constructed,
            Err(error) => return Box::pin(std::future::ready(Err(error))),
        };
        let (send, upstream_port) = match stream.begin_connect(
            context,
            session,
            egress,
            reservation,
            authority,
            &cancellation,
        ) {
            Ok(started) => started,
            Err(error) => return Box::pin(std::future::ready(Err(error))),
        };
        let authority: Box<str> = authority.into();
        Box::pin(async move {
            if cancellation.is_cancellation_requested() {
                drop(send);
                return Err(cancelled());
            }
            stream
                .finish_connect(send, &authority, upstream_port, timeout, &cancellation)
                .await?;
            Ok(stream)
        })
    }

    /// Establishes a CONNECT tunnel to `authority` and takes ownership of the
    /// session (`HTTPConnectStream::connectUnique`).
    ///
    /// Dropping the stream closes it and then drains the session, after
    /// success and after every failure. Like the frozen lazy `Task`, the
    /// returned future calls no session method until it is first polled.
    /// `cancellation` covers the request commit and the wait for the final
    /// response head, never the established tunnel.
    #[expect(
        clippy::too_many_arguments,
        reason = "ports the frozen HTTPConnectStream::connectUnique parameters one to one"
    )]
    pub fn connect_unique<S: ConnectSession>(
        context: &WorkerContext,
        session: S,
        reservation: RequestReservation,
        authority: &str,
        timeout: Duration,
        connect_headers: RequestHeaderMap,
        egress_buffer_size: u32,
        cancellation: CancellationToken,
    ) -> LocalConnectFuture<Result<HttpConnectStream, HttpError>> {
        Self::connect_unique_inner(
            context,
            session,
            reservation,
            authority,
            timeout,
            connect_headers,
            egress_buffer_size,
            cancellation,
            false,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "connect_unique's parameters plus the experimental reset_on_cancel"
    )]
    pub(crate) fn connect_unique_inner<S: ConnectSession>(
        context: &WorkerContext,
        session: S,
        reservation: RequestReservation,
        authority: &str,
        timeout: Duration,
        connect_headers: RequestHeaderMap,
        egress_buffer_size: u32,
        cancellation: CancellationToken,
        reset_on_cancel: bool,
    ) -> LocalConnectFuture<Result<HttpConnectStream, HttpError>> {
        let context = context.clone();
        let authority: Box<str> = authority.into();
        Box::pin(async move {
            let session: Box<dyn ConnectSession> = Box::new(session);
            let (mut stream, egress) = match Self::construct(
                &context,
                session.as_ref(),
                connect_headers,
                egress_buffer_size,
                reset_on_cancel,
            ) {
                Ok(constructed) => constructed,
                Err(error) => {
                    let _ = session.initiate_drain();
                    return Err(error);
                }
            };
            stream.session = Some(session);
            let session = stream
                .session
                .as_deref()
                .expect("should own the session it was just given");
            let (send, upstream_port) = stream.begin_connect(
                &context,
                session,
                egress,
                reservation,
                &authority,
                &cancellation,
            )?;
            stream
                .finish_connect(send, &authority, upstream_port, timeout, &cancellation)
                .await?;
            Ok(stream)
        })
    }

    /// The constructor (HTTPConnectStream.cpp:14-31).
    fn construct(
        context: &WorkerContext,
        session: &dyn ConnectSession,
        connect_headers: RequestHeaderMap,
        egress_buffer_size: u32,
        reset_on_cancel: bool,
    ) -> Result<(Self, HttpStreamSource), HttpError> {
        let worker_id = session.worker_id();
        let local_address = session.local_address();
        let peer_address = session.peer_address();
        let local_endpoint = session.local_endpoint();
        let peer_endpoint = session.peer_endpoint();
        let egress = HttpStreamSource::new_async_with_host(
            None,
            None,
            egress_buffer_size,
            context.host().clone(),
        )
        .map_err(|error| {
            HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string())
        })?;
        let state = Rc::new(StreamState {
            worker_id,
            egress_buffer_size,
            local_address,
            peer_address,
            connected_peer_address: OnceCell::new(),
            local_endpoint,
            peer_endpoint,
            connect_headers: RefCell::new(connect_headers),
            egress: RefCell::new(Some(egress.producer())),
            egress_eom_seen: Cell::new(false),
            egress_error: OnceCell::new(),
            ingress: RefCell::new(HttpSourceHolder::empty()),
            user_session_id: OnceCell::new(),
            callback: RefCell::new(None),
        });
        egress.set_callback(Some(Box::new(EgressCallbacks {
            stream: Rc::downgrade(&state),
        })));
        Ok((
            Self {
                state,
                session: None,
                connect_response: None,
                abort: None,
                reset_on_cancel,
            },
            egress,
        ))
    }

    /// `connectImpl` up to `sendRequest` (HTTPConnectStream.cpp:73-93).
    fn begin_connect(
        &self,
        context: &WorkerContext,
        session: &dyn ConnectSession,
        egress: HttpStreamSource,
        reservation: RequestReservation,
        authority: &str,
        cancellation: &CancellationToken,
    ) -> Result<(SendFuture, u16), HttpError> {
        assert_eq!(
            context.worker_id(),
            self.state.worker_id,
            "a CONNECT stream must be established on its proxy session's worker"
        );
        if cancellation.is_cancellation_requested() {
            return Err(cancelled());
        }
        let mut request = HttpMessage::new();
        request
            .set_method(HttpMethod::Connect)
            .expect("should set the method of a new message");
        request.set_http_version(1, 1);
        let upstream_port = match request.set_url(authority, false) {
            Ok(url) if url.valid() => url.port(),
            _ => 0,
        };
        let headers = request.headers_mut();
        headers.set(HttpHeaderCode::Host, authority);
        let connect_headers = std::mem::take(&mut *self.state.connect_headers.borrow_mut());
        for (name, value) in connect_headers {
            headers.add(&*name, value);
        }
        if let Some(producer) = self.state.egress_producer() {
            producer.headers(request, false);
        }
        let send = session.send_connect_request(
            HttpSourceHolder::new(Box::new(egress)),
            reservation,
            cancellation.clone(),
        );
        Ok((send, upstream_port))
    }

    /// `connectImpl` from `sendRequest` (HTTPConnectStream.cpp:91-128).
    async fn finish_connect(
        &mut self,
        send: SendFuture,
        authority: &str,
        upstream_port: u16,
        timeout: Duration,
        cancellation: &CancellationToken,
    ) -> Result<(), HttpError> {
        let mut response = send.await?;
        self.abort = response.take_stream_abort();
        let completion = response.source.ingress_completion();
        *self.state.ingress.borrow_mut() = response.source;
        self.state.ingress.borrow_mut().set_read_timeout(timeout);
        let head = HeadWait {
            state: &self.state,
            registration: cancellation.wake_registration(),
            completion,
            read: None,
            parked: false,
        }
        .await?;
        self.reset_on_cancel = false;
        let status = head.headers().response_status().unwrap_or_default();
        if status / 100 != 2 {
            self.state.egress_abort(HttpErrorCode::CONNECT_ERROR);
            let rejected =
                ConnectRejected::new(authority.into(), status, head.into_boxed_headers());
            return Err(HttpError::with_message(
                HttpErrorCode::CONNECT_ERROR,
                format!("CONNECT to {authority} failed status={status}"),
            )
            .with_cause(Arc::new(rejected)));
        }
        let headers = head.headers().headers();
        if let Some(peer) = connected_peer(
            headers.get_single_or_empty(b"X-Connected-To"),
            upstream_port,
        ) {
            let _ = self.state.connected_peer_address.set(peer);
        }
        let user_session_id = headers.get_single_or_empty(b"X-FB-Fwdproxy-Request-ID");
        let _ = self.state.user_session_id.set(user_session_id.into());
        self.connect_response = Some(head.into_boxed_headers());
        Ok(())
    }

    /// Ends both directions: `shutdownRead`, then `shutdownWrite`.
    pub fn close(&mut self) {
        self.shutdown_read();
        self.shutdown_write();
    }

    /// Whether the CONNECT response source is still readable.
    pub fn can_read(&self) -> bool {
        self.state.can_read()
    }

    /// Whether the egress source accepts more tunnel bytes.
    pub fn can_write(&self) -> bool {
        self.state.can_write()
    }

    /// Stops reading the CONNECT response source.
    pub fn shutdown_read(&mut self) {
        self.state.shutdown_read();
    }

    /// Ends the egress with EOM.
    pub fn shutdown_write(&mut self) {
        self.state.shutdown_write();
    }

    /// The `X-FB-Fwdproxy-Request-ID` bytes of the CONNECT response, or empty.
    pub fn extract_user_session_id(&self) -> &[u8] {
        self.state.user_session_id.get().map_or(&[], |id| &**id)
    }

    /// The proxy session's local address.
    pub fn local_address(&self) -> &SocketAddress {
        &self.state.local_address
    }

    /// The single numeric-host `X-Connected-To` of the CONNECT response with
    /// the authority's port, or else the proxy session's peer address.
    pub fn peer_address(&self) -> &SocketAddress {
        self.state
            .connected_peer_address
            .get()
            .unwrap_or(&self.state.peer_address)
    }

    /// The error the egress source completed with, if any.
    pub fn egress_error(&self) -> Option<&HttpError> {
        self.state.egress_error.get()
    }

    /// The worker that owns the stream and its proxy session.
    pub fn worker_id(&self) -> WorkerId {
        self.state.worker_id
    }

    /// The proxy session's local endpoint.
    pub(crate) fn local_endpoint(&self) -> &TransportEndpoint {
        &self.state.local_endpoint
    }

    /// The proxy session's immediate peer endpoint, never `X-Connected-To`.
    pub(crate) fn peer_endpoint(&self) -> &TransportEndpoint {
        &self.state.peer_endpoint
    }

    pub(crate) fn egress_buffer_size(&self) -> u32 {
        self.state.egress_buffer_size
    }

    /// `egressSource_->body(buf, 0, false)`.
    pub(crate) fn egress_body(&self, body: BufQueue) -> FlowControlState {
        self.state.egress_body(body)
    }

    /// `egressSource_->eom()`.
    pub(crate) fn egress_eom(&self) -> bool {
        self.state.egress_eom()
    }

    /// `egressSource_->abort(error)`.
    pub(crate) fn egress_abort(&self, error: HttpErrorCode) -> bool {
        self.state.egress_abort(error)
    }

    /// The proposed `closeWithReset` that resets after egress EOM (`DIV-023`):
    /// the session resets the stream first, so the egress abort and the
    /// stopped ingress that close both directions add no second reset. A panic
    /// of the session abort, such as a lifecycle observer's, is returned after
    /// both directions are closed.
    #[must_use]
    pub(crate) fn abort_with_reset(&mut self) -> Option<Box<dyn Any + Send>> {
        let panic = self
            .abort
            .take()
            .and_then(|abort| catch_unwind(AssertUnwindSafe(|| abort.abort())).err());
        self.state.egress_abort(HttpErrorCode::CANCEL);
        self.state.stop_ingress(Some(HttpErrorCode::CANCEL));
        panic
    }

    /// Runs `operation` on the CONNECT response source holder.
    pub(crate) fn with_ingress_source<R>(
        &self,
        operation: impl FnOnce(&mut HttpSourceHolder) -> R,
    ) -> R {
        operation(&mut self.state.ingress.borrow_mut())
    }

    /// `setHTTPStreamSourceCallback`. The slot does not own the callback.
    pub(crate) fn set_http_stream_source_callback(
        &self,
        callback: Option<Weak<dyn HttpStreamSourceSharedCallback>>,
    ) {
        *self.state.callback.borrow_mut() = callback;
    }
}

impl Drop for HttpConnectStream {
    fn drop(&mut self) {
        let panic = if self.reset_on_cancel && self.abort.is_some() {
            self.abort_with_reset()
        } else {
            self.close();
            None
        };
        if let Some(session) = &self.session {
            let _ = session.initiate_drain();
        }
        if let Some(payload) = panic {
            if std::thread::panicking() {
                discard_panic(payload);
            } else {
                resume_unwind(payload);
            }
        }
    }
}

/// The wait for the final CONNECT response head (HTTPConnectStream.cpp:96-101).
///
/// Each poll first samples the caller's token through a wake registration, so
/// a cancellation from any thread wakes a parked wait, as `TimedBaton::wait`'s
/// cancellation callback signals its baton. A header read that has returned
/// `Pending` is never polled after a sampled cancellation, because polling it
/// could consume a head that cancellation supersedes.
struct HeadWait<'a> {
    state: &'a StreamState,
    registration: CancellationWakeRegistration,
    completion: Option<IngressCompletion>,
    read: Option<LocalSourceFuture<Result<HttpHeaderEvent, HttpError>>>,
    parked: bool,
}

impl HeadWait<'_> {
    fn cancel(&mut self) -> HttpError {
        drop(self.read.take());
        let committed = self
            .completion
            .as_ref()
            .and_then(IngressCompletion::snapshot);
        self.state.stop_ingress(Some(HttpErrorCode::CORO_CANCELLED));
        if let Some(Err(error)) = committed {
            return error;
        }
        let mut error = HttpError::with_message(HttpErrorCode::CORO_CANCELLED, b"Read cancelled");
        if let Some(Err(mut stopped)) = self
            .completion
            .as_ref()
            .and_then(IngressCompletion::snapshot)
        {
            error.set_http_message(stopped.take_http_message());
        }
        error
    }
}

impl Future for HeadWait<'_> {
    type Output = Result<HttpHeaderEvent, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let cancelled = this.registration.update_waker(context.waker());
        loop {
            if cancelled && this.parked {
                return Poll::Ready(Err(this.cancel()));
            }
            let state = this.state;
            let read = this
                .read
                .get_or_insert_with(|| state.ingress.borrow_mut().read_header_event());
            match read.as_mut().poll(context) {
                Poll::Pending => {
                    this.parked = true;
                    if !cancelled {
                        return Poll::Pending;
                    }
                }
                Poll::Ready(result) => {
                    this.read = None;
                    this.parked = false;
                    let mut event = match result {
                        Ok(event) => event,
                        Err(error) => return Poll::Ready(Err(error)),
                    };
                    if event.is_final().unwrap_or(true) {
                        return Poll::Ready(Ok(event));
                    }
                }
            }
        }
    }
}
