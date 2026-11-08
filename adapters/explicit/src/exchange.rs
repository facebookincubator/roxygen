//! Host-agnostic exchange over a worker and an adopted byte stream.

use std::cell::Cell;
use std::cell::RefCell;
use std::future::Future;
use std::future::poll_fn;
use std::future::ready;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
use std::sync::atomic::AtomicU8;
#[cfg(test)]
use std::sync::atomic::AtomicU64;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use futures::FutureExt as _;
use futures::TryFutureExt as _;
use futures::channel::oneshot;
use futures::future::Either;
use http::HeaderMap;
use http::HeaderName;
use http::HeaderValue;
use http::header;
use http::uri::Authority;
use http::uri::PathAndQuery;
use roxygen_client::connector::ClientAttachOptions;
use roxygen_client::connector::ClientSession;
use roxygen_client::connector::SessionParams;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEventKind;
use roxygen_core::events::ResumeStatus;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::headers::HttpHeaders;
use roxygen_core::http::CodecProtocol;
use roxygen_core::http::HttpMethod;
use roxygen_core::message::HttpMessage;
use roxygen_core::message::MessageTypeError;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::host::Host;
use roxygen_runtime::host::LocalFuture;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerState;
use roxygen_transport::byte_stream::ByteStreamLimits;
use roxygen_transport::established::AdoptedIoCompletion;
use roxygen_transport::established::AdoptedIoControl;
use roxygen_transport::established::EstablishedIo;
use roxygen_transport::established::adopt_local_with_completion;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;

use crate::error::ErrorKind;
use crate::error::ExplicitError;
use crate::error::Observed;
use crate::error::Phase;
use crate::error::Position;
use crate::stats::ExplicitStats;
use crate::stats::PendingTeardown;
use crate::stats::TeardownGuard;
use crate::tunnel::ConnectHead;
use crate::tunnel::TunnelLink;

/// Read and write limits of an adopted stream, and the largest body read.
const STREAM_LIMIT: usize = 16 * 1024;

/// One GET request for an origin-form target.
#[derive(Clone, Debug)]
pub struct Request {
    /// The request target. It must begin with `/`.
    pub path_and_query: PathAndQuery,
    /// The `Host` header, which replaces any caller `Host`. It must carry no userinfo.
    pub authority: Authority,
    /// Caller headers, sent in order. `Connection`, `Content-Length`, `Transfer-Encoding`, and
    /// `Upgrade` belong to the adapter and are refused.
    pub headers: Arc<[(HeaderName, HeaderValue)]>,
    /// Bounds dial, TLS, head, and body together, in the worker host's clock domain.
    pub deadline: Instant,
}

/// The final response head and the outcome of draining its body.
#[derive(Debug)]
pub struct Response {
    pub status: u16,
    /// Response headers in wire order; repeated names keep every value. Empty when the exchange
    /// keeps `raw_headers` instead.
    pub headers: HeaderMap,
    /// The response headers as the codec parsed them, for a client made with
    /// `ExplicitClient::with_raw_headers`, which builds no `headers`; `None` otherwise.
    pub raw_headers: Option<RawHeaders>,
    /// Response headers that `HeaderMap` could not represent and that were dropped, or that it
    /// would have dropped for `raw_headers`.
    pub dropped_headers: usize,
    pub body: BodyOutcome,
    pub connection: ConnectionInfo,
    pub timings: Timings,
    /// A tunnel's 2xx CONNECT head; `None` for a direct target.
    pub connect_head: Option<Box<ConnectHead>>,
}

/// How an exchange hands back its response headers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HeaderForm {
    /// Converted into `Response::headers`.
    Map,
    /// Kept as the codec parsed them, in `Response::raw_headers`.
    Raw,
}

/// How an exchange counts its adopted stream's teardown in `ExplicitStats`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TeardownWatch {
    /// A worker cleanup task awaits the stream's retirement, confirms it, and records the time
    /// from `close_when_idle`.
    CleanupTask,
    /// The stream owns a `TeardownGuard`, which the runtime gave it, and no cleanup task waits.
    StreamGuard,
}

impl TeardownWatch {
    /// The guard that a stream of this watch owns.
    pub(crate) fn guard(self, stats: &Arc<ExplicitStats>) -> Option<TeardownGuard> {
        (self == Self::StreamGuard).then(|| TeardownGuard::new(stats))
    }
}

/// The most headers that [`RawHeaders`] keeps as the codec parsed them, well under the 3,276
/// that `convert_headers` could convert without a `HeaderMap` refusing an append. Sized for
/// every header of a head, the map holds them all without growing, and it grows after hash
/// collisions only by doubling a table that has at most five slots per name, which stays within
/// its limit of 2^15 slots for up to 3,276 headers.
pub(crate) const RAW_HEADER_LIMIT: usize = 1024;

/// Response headers as the codec parsed them, read as the `HeaderMap` of `Response::headers`
/// would read them without building it. A name matches in any ASCII case, and a header whose
/// name `HeaderName` refuses, or whose value `HeaderValue` refuses, is absent, as
/// `convert_headers` drops it. A head of more than `RAW_HEADER_LIMIT` (1,024) headers is
/// converted instead, so that it also loses the headers that the map has no room for.
///
/// Lookups go through `HttpHeaders`' own, as Proxygen's `forEachValueOfHeader` does: a common
/// name is compared by its code, and only the other names by their bytes.
#[derive(Debug)]
pub struct RawHeaders(RawForm);

#[derive(Debug)]
enum RawForm {
    /// The parsed headers, and whether the map would hold every one of them.
    Parsed {
        headers: HttpHeaders,
        all_held: bool,
    },
    Converted(Box<HeaderMap>),
}

impl RawHeaders {
    /// Takes `headers`, and returns them with the number that `convert_headers` drops.
    pub(crate) fn new(headers: HttpHeaders) -> (Self, usize) {
        if headers.len() > RAW_HEADER_LIMIT {
            let (converted, dropped) = convert_headers(&headers);
            return (Self::from(converted), dropped);
        }
        let dropped = headers
            .iter()
            .filter(|(code, name, value)| !held(*code, name, value))
            .count();
        let all_held = dropped == 0;
        (Self(RawForm::Parsed { headers, all_held }), dropped)
    }

    /// `HeaderMap::get(name)` as bytes: the first value of `name` that the map would hold.
    pub fn get(&self, name: &str) -> Option<&[u8]> {
        let (headers, all_held) = match &self.0 {
            RawForm::Parsed { headers, all_held } => (headers, *all_held),
            RawForm::Converted(converted) => {
                return converted.get(name).map(HeaderValue::as_bytes);
            }
        };
        let mut first = None;
        headers.for_each_value(name, |value| {
            let held = all_held || valid_value(value);
            if held {
                first = Some(value);
            }
            held
        });
        // Only a header whose name equals `name` in ASCII case matched, and that name is a
        // token exactly when `name` is. So when every name is held, a match proves `name` valid;
        // otherwise a name that `HeaderName` refuses finds nothing, as in the map.
        first.filter(|_| all_held || valid_name(name.as_bytes()))
    }

    /// `HeaderMap::get(name)` read with `HeaderValue::to_str`: the first value of `name`, if it
    /// is visible ASCII.
    pub fn get_str(&self, name: &str) -> Option<&str> {
        let value = self.get(name)?;
        let visible = value
            .iter()
            .fold(true, |visible, byte| visible & visible_ascii(*byte));
        visible.then(|| std::str::from_utf8(value).ok()).flatten()
    }

    /// `HeaderMap::contains_key(name)`.
    pub fn contains_key(&self, name: &str) -> bool {
        self.get(name).is_some()
    }
}

impl From<HeaderMap> for RawHeaders {
    fn from(headers: HeaderMap) -> Self {
        Self(RawForm::Converted(Box::new(headers)))
    }
}

/// Whether `convert_headers` keeps the header. Every common header but the pseudo-headers has a
/// `HeaderName` constant, so only the other names are checked byte by byte, as `header_name`
/// parses only those.
fn held(code: HttpHeaderCode, name: &[u8], value: &[u8]) -> bool {
    let named = match code {
        HttpHeaderCode::None
        | HttpHeaderCode::Other
        | HttpHeaderCode::ColonAuthority
        | HttpHeaderCode::ColonMethod
        | HttpHeaderCode::ColonPath
        | HttpHeaderCode::ColonScheme
        | HttpHeaderCode::ColonStatus
        | HttpHeaderCode::ColonProtocol => valid_name(name),
        _ => true,
    };
    named && valid_value(value)
}

/// Whether `HeaderName::from_bytes` accepts `name`: 1 to 65,535 token characters.
fn valid_name(name: &[u8]) -> bool {
    const MAX_NAME: usize = (1 << 16) - 1;
    (1..=MAX_NAME).contains(&name.len()) && name.iter().all(|byte| TOKEN[usize::from(*byte)])
}

/// Whether `HeaderValue::from_bytes` accepts `value`. It folds rather than stops early, so that
/// it vectorizes, as `HeaderValue`'s own check does.
fn valid_value(value: &[u8]) -> bool {
    value.iter().fold(true, |valid, byte| {
        valid & (*byte >= 32 && *byte != 127 || *byte == b'\t')
    })
}

/// Whether `HeaderValue::to_str` accepts `byte`.
fn visible_ascii(byte: u8) -> bool {
    (32..127).contains(&byte) || byte == b'\t'
}

/// The RFC 9110 token characters, which `HeaderName` accepts in a name in either ASCII case.
const TOKEN: [bool; 256] = {
    let mut token = [false; 256];
    let mut byte = 0;
    while byte < 256 {
        token[byte] = matches!(
            byte as u8,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
                | b'0'..=b'9'
                | b'a'..=b'z'
                | b'A'..=b'Z'
        );
        byte += 1;
    }
    token
};

/// How the response body ended. The body itself is counted and discarded.
#[derive(Debug)]
pub enum BodyOutcome {
    Complete {
        bytes: u64,
    },
    /// A failure after the final head, with the body bytes read before it.
    Truncated {
        bytes: u64,
        cause: ExplicitError,
    },
}

/// Exchange timing. Every offset is measured from the exchange's start on its worker, on the
/// worker host's clock.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Timings {
    /// From `send` to the exchange's start on its worker or, for a send whose deadline passed
    /// before that start, to the send's expiry.
    pub queued: Duration,
    pub tcp_connect: Option<Duration>,
    pub tls_handshake: Option<Duration>,
    /// The first response head, informational heads included.
    pub first_byte: Option<Duration>,
    /// The end of the response body.
    pub last_byte: Option<Duration>,
}

/// The connection that carried an exchange.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConnectionInfo {
    pub local: Option<SocketAddr>,
    /// For an exchange that `ExplicitClient::send` dialed, the address it dialed,
    /// `Target::address` (the proxy's for a tunnel), as folly's `AsyncSocket::getPeerAddress`
    /// reports the address it connected to, not the kernel's `getpeername`. The two differ for an
    /// unspecified destination, which the kernel routes to loopback, and for a destination that a
    /// cgroup connect hook rewrites. For `exchange`, the address of the stream's peer endpoint.
    pub peer: Option<SocketAddr>,
    pub tls_version: Option<rustls::ProtocolVersion>,
}

const NOT_STARTED: u8 = 0;
const STARTED: u8 = 1;
const HANDED: u8 = 2;
const FINISHED: u8 = 3;
const POSITION: u8 = 0b11;
const CLAIMED: u8 = 0b100;
/// Set with `HANDED` and kept by `FINISHED`.
const REACHED: u8 = 0b1000;

/// The exchange's progress, shared by its worker and its caller and changed only by
/// compare-and-swap: not started, started, handed to the session, or finished, plus a claimed
/// mark that the caller sets to give up on an unfinished exchange and a mark, kept once
/// finished, that the request was handed to the session.
#[derive(Debug, Default)]
pub struct ExchangeProgress(AtomicU8);

impl ExchangeProgress {
    pub fn new() -> Self {
        Self::default()
    }

    /// Moves a not-started exchange to started. Fails once the exchange is claimed.
    pub fn try_start(&self) -> bool {
        self.swap(NOT_STARTED, STARTED)
    }

    /// Claims an unfinished exchange. A core caller cancels an exchange only by winning this
    /// claim before it cancels the exchange's token; once the exchange is finished, its token
    /// also resets the connection that `close_when_idle` is closing.
    pub fn claim(&self) -> bool {
        self.update(|word| {
            (word & CLAIMED == 0 && word & POSITION != FINISHED).then_some(word | CLAIMED)
        })
    }

    /// Whether the request was handed to the session.
    pub fn reached_session(&self) -> bool {
        self.0.load(Ordering::Acquire) & REACHED != 0
    }

    pub(crate) fn started(&self) -> bool {
        self.0.load(Ordering::Acquire) & POSITION != NOT_STARTED
    }

    pub(crate) fn claim_unstarted(&self) -> bool {
        self.swap(NOT_STARTED, NOT_STARTED | CLAIMED)
    }

    pub(crate) fn hand_off(&self) -> bool {
        self.swap(STARTED, HANDED | REACHED)
    }

    /// Moves an unclaimed exchange to finished.
    pub(crate) fn finish(&self) -> bool {
        self.update(|word| {
            matches!(word & !REACHED, STARTED | HANDED).then_some(FINISHED | (word & REACHED))
        }) || self.0.load(Ordering::Acquire) & !REACHED == FINISHED
    }

    fn swap(&self, current: u8, new: u8) -> bool {
        self.0
            .compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Applies `next` until it returns `None` or its result is stored. The same loop as
    /// `AtomicU8::try_update`, which is not stable on every supported toolchain.
    fn update(&self, mut next: impl FnMut(u8) -> Option<u8>) -> bool {
        let mut word = self.0.load(Ordering::Acquire);
        while let Some(new) = next(word) {
            match self
                .0
                .compare_exchange_weak(word, new, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return true,
                Err(actual) => word = actual,
            }
        }
        false
    }
}

/// Everything one core exchange takes.
pub struct ExchangeInput<Io> {
    pub worker: WorkerContext,
    /// Established I/O; its endpoints fill `ConnectionInfo`.
    pub io: EstablishedIo<Io>,
    /// The connector's assertion about the immediate hop.
    pub security: TransportSecurity,
    pub request: Request,
    /// Cancels the exchange and, for its whole life, the attached session.
    pub cancellation: CancellationToken,
    /// Must be started, or the exchange fails as `Phase::Submit` and `ErrorKind::Other` before
    /// it adopts the stream; the exchange moves it to handed and then to finished.
    pub progress: Arc<ExchangeProgress>,
    /// The exchange's start, in the worker host's clock domain.
    pub started: Instant,
    pub stats: Arc<ExplicitStats>,
}

/// Runs one HTTP/1.1 GET over `input.io` on its worker, drains and counts the body, and closes
/// the connection with `ClientSession::close_when_idle`.
///
/// A worker cleanup task awaits the connection's retirement, so teardown outlives the result.
/// This catches no panic.
pub async fn exchange<Io>(input: ExchangeInput<Io>) -> Result<Response, ExplicitError>
where
    Io: AsyncRead + AsyncWrite + 'static,
{
    run_exchange(input, &ExchangeHooks::default()).await
}

/// `exchange` with test hooks. Callers await the future at once: the checks before the
/// exchange's first await run when it is called.
pub(crate) fn run_exchange<Io>(
    input: ExchangeInput<Io>,
    hooks: &ExchangeHooks,
) -> impl Future<Output = Result<Response, ExplicitError>>
where
    Io: AsyncRead + AsyncWrite + 'static,
{
    start(input, hooks, None)
}

/// What the runtime's dial lends the exchange over the stream it dialed.
///
/// `ExplicitClient::dispatch` validated the request, which is immutable, so the exchange does
/// not validate it again.
pub(crate) struct Dialed<'a> {
    /// The exchange's one deadline sleep, which the dial's stages raced. None of them finished
    /// it, since an expiry ends the exchange.
    pub(crate) sleep: &'a mut LocalFuture,
    /// The addresses that `ConnectionInfo` reports.
    pub(crate) local: Option<SocketAddr>,
    pub(crate) peer: Option<SocketAddr>,
    /// A tunnel's proxy guard, which the exchange hands to the tunnel cleanup once it has
    /// adopted the stream. When set, the stream's cleanup reservation, adoption, and attach
    /// failures are `Phase::TunnelConnect`.
    pub(crate) tunnel: Option<TunnelLink>,
    pub(crate) header_form: HeaderForm,
    pub(crate) teardown_watch: TeardownWatch,
}

/// `run_exchange` over a stream that the runtime dialed.
pub(crate) fn run_dialed_exchange<'a, Io>(
    input: ExchangeInput<Io>,
    hooks: &'a ExchangeHooks,
    dialed: Dialed<'a>,
) -> impl Future<Output = Result<Response, ExplicitError>> + 'a
where
    Io: AsyncRead + AsyncWrite + 'static,
{
    start(input, hooks, Some(dialed))
}

fn start<'a, Io>(
    input: ExchangeInput<Io>,
    hooks: &'a ExchangeHooks,
    dialed: Option<Dialed<'a>>,
) -> impl Future<Output = Result<Response, ExplicitError>> + 'a
where
    Io: AsyncRead + AsyncWrite + 'static,
{
    let progress = Arc::clone(&input.progress);
    if !progress.started() {
        return Either::Left(ready(Err(ExplicitError::new(
            Phase::Submit,
            ErrorKind::Other,
            "the exchange's progress was not started",
        ))));
    }
    Either::Right(
        attempt(input, hooks, dialed).map(move |result| match result {
            Ok(response) => Ok(response),
            Err(error) if progress.finish() => Err(error),
            Err(error) => Err(ExplicitError {
                kind: ErrorKind::Cancelled,
                ..error
            }),
        }),
    )
}

/// The read and write limits that every adopted stream takes, a tunnel's proxy stream included.
pub(crate) fn stream_limits() -> ByteStreamLimits {
    ByteStreamLimits::new(STREAM_LIMIT, STREAM_LIMIT, 1)
        .expect("should accept non-zero stream limits")
}

/// The parameters of a session attached at `host.now()`: one stream, and read and write
/// timeouts of the time left, at least 1 ms, since Uniplex reads a zero timeout as none.
pub(crate) fn session_params(host: &Host, deadline: Instant) -> SessionParams {
    let remaining = deadline
        .saturating_duration_since(host.now())
        .max(Duration::from_millis(1));
    SessionParams {
        max_concurrent_outgoing_streams: Some(1),
        stream_read_timeout: Some(remaining),
        connection_read_timeout: Some(remaining),
        write_timeout: Some(remaining),
        ..SessionParams::default()
    }
}

/// Refuses input that cannot make an origin-form request or that the HTTP/1.1 codec acts on.
pub(crate) fn validate_request(request: &Request) -> Result<(), ExplicitError> {
    let refuse = |detail: String| {
        Err(ExplicitError::new(
            Phase::Submit,
            ErrorKind::Protocol,
            detail,
        ))
    };
    if !request.path_and_query.as_str().starts_with('/') {
        return refuse(format!(
            "request target {} is not origin-form",
            request.path_and_query
        ));
    }
    if request.authority.as_str().contains('@') {
        return refuse("request authority carries userinfo".to_owned());
    }
    let owned = [
        header::CONNECTION,
        header::CONTENT_LENGTH,
        header::TRANSFER_ENCODING,
        header::UPGRADE,
    ];
    if let Some((name, _)) = request
        .headers
        .iter()
        .find(|(name, _)| owned.contains(name))
    {
        return refuse(format!("request header {name} belongs to the adapter"));
    }
    Ok(())
}

/// What the cleanup task needs once the stream is adopted.
pub(crate) struct Handoff {
    pub(crate) completion: AdoptedIoCompletion,
    pub(crate) teardown: PendingTeardown,
    pub(crate) closed_at: Rc<Cell<Option<Instant>>>,
    pub(crate) host: Host,
}

/// Awaits the adopted stream's retirement and then confirms its pending teardown.
pub(crate) async fn retire(receipt: oneshot::Receiver<Handoff>, confirms: bool) {
    let Ok(Handoff {
        completion,
        teardown,
        closed_at,
        host,
    }) = receipt.await
    else {
        return;
    };
    if completion.wait_retired().await.is_ok() && confirms {
        let since_close = closed_at
            .get()
            .map(|closed| host.now().saturating_duration_since(closed));
        teardown.retired(since_close);
    }
}

/// Cancels the adopted I/O unless the exchange reached `close_when_idle`.
struct ControlGuard(Option<AdoptedIoControl>);

impl ControlGuard {
    fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for ControlGuard {
    fn drop(&mut self) {
        if let Some(control) = self.0.take() {
            control.cancel();
        }
    }
}

enum Raced<T> {
    Done(T),
    Expired,
}

/// Races `work` against the deadline sleep that one exchange creates once for its dial and all
/// its races. `Expired` ends the exchange, so a finished sleep is never polled again.
///
/// A passed deadline wins even over work that is ready, as reqwest's total timeout does. It is
/// read from the clock first, because a host may fire the sleep later than its deadline.
async fn race<Work: Future>(
    observer: &Observer<'_>,
    sleep: &mut LocalFuture,
    work: Work,
) -> Raced<Work::Output> {
    let mut work = pin!(work);
    poll_fn(|context| {
        if observer.deadline_passed() {
            return Poll::Ready(Raced::Expired);
        }
        if let Poll::Ready(output) = work.as_mut().poll(context) {
            return Poll::Ready(Raced::Done(output));
        }
        if sleep.as_mut().poll(context).is_ready() {
            return Poll::Ready(Raced::Expired);
        }
        Poll::Pending
    })
    .await
}

/// State that classifies errors observed after the stream is attached.
struct Observer<'a> {
    worker: &'a WorkerContext,
    cancellation: &'a CancellationToken,
    host: &'a Host,
    deadline: Instant,
}

impl Observer<'_> {
    fn deadline_passed(&self) -> bool {
        self.host.now() >= self.deadline
    }

    fn observed(&self) -> Observed {
        Observed {
            own_cancelled: self.cancellation.is_cancellation_requested(),
            worker_open: self.worker.handle().state() == WorkerState::Open,
            deadline_passed: self.deadline_passed(),
        }
    }

    fn classify(&self, error: &HttpError, position: Position) -> ExplicitError {
        ExplicitError::from_http(error, position, self.observed())
    }

    fn expired(&self, phase: Phase) -> ExplicitError {
        ExplicitError::expired(phase)
            .reached(true)
            .observed(self.observed())
    }
}

/// Uses up the request when it is called, so that the exchange's futures hold only its source.
///
/// The exchange runs as two futures, the second in the first's place: the first adopts the
/// stream and hands the request to its session, and the second, which holds the attached
/// session until the exchange ends, never holds the stream.
fn attempt<'a, Io>(
    input: ExchangeInput<Io>,
    hooks: &'a ExchangeHooks,
    dialed: Option<Dialed<'a>>,
) -> impl Future<Output = Result<Response, ExplicitError>> + 'a
where
    Io: AsyncRead + AsyncWrite + 'static,
{
    let ExchangeInput {
        worker,
        io,
        security,
        request,
        cancellation,
        progress,
        started,
        stats,
    } = input;
    let deadline = request.deadline;
    let secure = security == TransportSecurity::Tls;
    let source = if dialed.is_some() {
        request_source(&request, secure)
    } else {
        hooks.request_validated();
        validate_request(&request).and_then(|()| request_source(&request, secure))
    };
    drop(request);
    let attaching = async move {
        let host = worker.host().clone();
        let source = source?;
        let (local, peer, sleep, tunnel, header_form, teardown_watch) = match dialed {
            Some(Dialed {
                sleep,
                local,
                peer,
                tunnel,
                header_form,
                teardown_watch,
            }) => (
                local,
                peer,
                Some(sleep),
                tunnel,
                header_form,
                teardown_watch,
            ),
            None => (
                socket_address(&io.local_endpoint),
                socket_address(&io.peer_endpoint),
                None,
                None,
                HeaderForm::Map,
                TeardownWatch::CleanupTask,
            ),
        };
        let (attach, attach_phase) = if tunnel.is_some() {
            (Position::TunnelAttach, Phase::TunnelConnect)
        } else {
            (Position::Attach, Phase::Connect)
        };
        let connection = ConnectionInfo {
            local,
            peer,
            tls_version: None,
        };
        // Uniplex reads a zero timeout as none, so a stream with no time left is never attached.
        if host.now() >= deadline {
            let phase = if security == TransportSecurity::Tls {
                Phase::Tls
            } else {
                Phase::Connect
            };
            return Err(ExplicitError::expired(phase));
        }

        let retirement = match teardown_watch {
            TeardownWatch::CleanupTask => {
                let (handoff, receipt) = oneshot::channel();
                let reservation = worker
                    .register_cleanup_task(retire(receipt, hooks.confirms_retirement()))
                    .map_err(|error| {
                        ExplicitError::from_cleanup_registration(&error, attach_phase)
                    })?;
                Some((handoff, reservation))
            }
            TeardownWatch::StreamGuard => None,
        };
        hooks.before_adoption().await;
        let observer = Observer {
            worker: &worker,
            cancellation: &cancellation,
            host: &host,
            deadline,
        };
        let limits = stream_limits();
        hooks.adopting(limits);
        let (transport, control) = match adopt_local_with_completion(&worker, io, limits) {
            Ok(adopted) => adopted,
            Err(error) => {
                if let Some((_, reservation)) = retirement {
                    let _ = reservation.disarm();
                }
                return Err(observer.classify(&error, attach));
            }
        };
        if let Some(tunnel) = tunnel {
            tunnel.hand_off(control.completion());
        }
        let closed_at = retirement.map(|(handoff, reservation)| {
            let closed_at = Rc::new(Cell::new(None));
            let _ = handoff.send(Handoff {
                completion: control.completion(),
                teardown: PendingTeardown::new(Arc::clone(&stats)),
                closed_at: Rc::clone(&closed_at),
                host: host.clone(),
            });
            if let Ok(cleanup) = reservation.activate() {
                cleanup.detach();
            }
            closed_at
        });
        let control = ControlGuard(Some(control));

        // The deadline can pass after the check above, and a zero timeout would disable them.
        let mut options = ClientAttachOptions::new(CodecProtocol::Http11, security);
        options.cancellation = cancellation.clone();
        options.session = session_params(&host, deadline);
        hooks.attaching(&options.session);
        let session = ClientSession::attach_transport(&worker, transport, options)
            .map_err(|error| observer.classify(&error, attach))?;

        hooks.before_handoff();
        if !progress.hand_off() {
            return Err(ExplicitError::new(
                Phase::Read,
                ErrorKind::Cancelled,
                "exchange claimed",
            ));
        }
        Ok(Attached {
            session,
            control,
            closed_at,
            worker,
            cancellation,
            host,
            deadline,
            source,
            connection,
            progress,
            started,
            stats,
            sleep,
            header_form,
        })
    };
    attaching.and_then(move |attached| read_and_close(attached, hooks))
}

/// An exchange whose request is handed to its attached session. Fields drop in declaration
/// order, so the session drops before the guard that cancels its stream.
struct Attached<'a> {
    session: ClientSession,
    control: ControlGuard,
    /// When `close_when_idle` was called, for the cleanup task of `TeardownWatch::CleanupTask`.
    closed_at: Option<Rc<Cell<Option<Instant>>>>,
    worker: WorkerContext,
    cancellation: CancellationToken,
    host: Host,
    deadline: Instant,
    source: HttpSourceHolder,
    connection: ConnectionInfo,
    progress: Arc<ExchangeProgress>,
    started: Instant,
    stats: Arc<ExplicitStats>,
    /// The dial's deadline sleep, if the runtime dialed the stream.
    sleep: Option<&'a mut LocalFuture>,
    header_form: HeaderForm,
}

/// Reads an attached exchange's response, drains its body, and closes its connection.
#[expect(
    clippy::manual_async_fn,
    reason = "an `async fn` keeps a `mut` parameter twice, once as moved into its body"
)]
fn read_and_close<'a>(
    mut attached: Attached<'a>,
    hooks: &'a ExchangeHooks,
) -> impl Future<Output = Result<Response, ExplicitError>> + 'a {
    async move {
        let observer = Observer {
            worker: &attached.worker,
            cancellation: &attached.cancellation,
            host: &attached.host,
            deadline: attached.deadline,
        };
        let mut own;
        let sleep = match attached.sleep.take() {
            Some(sleep) => sleep,
            None => {
                own = attached.host.sleep_until(attached.deadline);
                hooks.deadline_sleep_created();
                &mut own
            }
        };
        let read = read_response(
            &attached.session,
            attached.source,
            &observer,
            sleep,
            attached.started,
            attached.header_form,
            hooks,
        )
        .await;
        let expired = match &read {
            Err(error) => error.is_expiry(),
            Ok(ReadOutcome {
                body: BodyOutcome::Truncated { cause, .. },
                ..
            }) => cause.is_expiry(),
            Ok(_) => false,
        };
        if expired {
            // Session-local: it resets the transport and leaves the exchange's token alone.
            attached.session.cancel();
        }
        let read = read?;
        let response = Response {
            status: read.status,
            headers: read.headers,
            raw_headers: read.raw_headers,
            dropped_headers: read.dropped_headers,
            body: read.body,
            connection: attached.connection,
            timings: Timings {
                first_byte: Some(read.first_byte),
                last_byte: read.last_byte,
                ..Timings::default()
            },
            connect_head: None,
        };
        attached
            .stats
            .count_dropped_headers(response.dropped_headers);
        let truncated = matches!(response.body, BodyOutcome::Truncated { .. });
        if truncated {
            attached.stats.count_truncated_body();
        }

        hooks.before_finish().await;
        if !attached.progress.finish() {
            let mut error =
                ExplicitError::new(Phase::Body, ErrorKind::Cancelled, "exchange claimed")
                    .reached(true);
            *error.timings = response.timings;
            return Err(error);
        }
        if truncated {
            return Ok(response);
        }
        hooks.before_close().await;
        if let Some(closed_at) = &attached.closed_at {
            closed_at.set(Some(attached.host.now()));
        }
        if attached.session.close_when_idle().is_ok() {
            attached.control.disarm();
        }
        Ok(response)
    }
}

struct ReadOutcome {
    status: u16,
    headers: HeaderMap,
    raw_headers: Option<RawHeaders>,
    dropped_headers: usize,
    body: BodyOutcome,
    first_byte: Duration,
    last_byte: Option<Duration>,
}

async fn read_response(
    session: &ClientSession,
    source: HttpSourceHolder,
    observer: &Observer<'_>,
    sleep: &mut LocalFuture,
    started: Instant,
    header_form: HeaderForm,
    hooks: &ExchangeHooks,
) -> Result<ReadOutcome, ExplicitError> {
    let host = observer.host;
    let elapsed = || host.now().saturating_duration_since(started);
    let response = match race(
        observer,
        sleep,
        session.send_request(source, observer.cancellation.clone()),
    )
    .await
    {
        Raced::Expired => return Err(observer.expired(Phase::Read)),
        Raced::Done(Err(error)) => return Err(observer.classify(&error, Position::BeforeHead)),
        Raced::Done(Ok(response)) => response,
    };
    let mut source = response.source;
    hooks.before_head().await;

    let mut first_byte = None;
    let (mut message, ending) = loop {
        let mut event = match race(observer, sleep, source.read_header_event()).await {
            Raced::Expired => return Err(timed(observer.expired(Phase::Read), first_byte)),
            Raced::Done(Err(error)) => match final_head(error) {
                Ok((head, error)) => {
                    first_byte.get_or_insert_with(elapsed);
                    let cause = observer.classify(&error, Position::AfterHead);
                    break (head, HeadEnding::Failed(cause));
                }
                Err(error) => {
                    let error = observer.classify(&error, Position::BeforeHead);
                    return Err(timed(error, first_byte));
                }
            },
            Raced::Done(Ok(event)) => event,
        };
        first_byte.get_or_insert_with(elapsed);
        match event.is_final() {
            Ok(true) => {
                let ending = if event.eom() {
                    HeadEnding::Complete
                } else {
                    HeadEnding::Body
                };
                break (event.into_headers(), ending);
            }
            Ok(false) => {}
            Err(error) => {
                let error = ExplicitError::new(Phase::Read, ErrorKind::Protocol, error.to_string())
                    .reached(true);
                return Err(timed(error, first_byte));
            }
        }
    };
    let first_byte = first_byte.expect("should record the first head before the final head");
    let status = message.status_code().map_err(|error| {
        let error =
            ExplicitError::new(Phase::Read, ErrorKind::Protocol, error.to_string()).reached(true);
        timed(error, Some(first_byte))
    })?;
    let (headers, raw_headers, dropped_headers) = match header_form {
        HeaderForm::Map => {
            let (headers, dropped) = convert_headers(message.headers());
            (headers, None, dropped)
        }
        HeaderForm::Raw => {
            let (raw, dropped) = RawHeaders::new(message.headers_mut().take());
            (HeaderMap::new(), Some(raw), dropped)
        }
    };
    let (mut body, last_byte) = match ending {
        HeadEnding::Complete => (BodyOutcome::Complete { bytes: 0 }, Some(elapsed())),
        HeadEnding::Body => drain_body(&mut source, observer, sleep, &elapsed, hooks).await,
        HeadEnding::Failed(cause) => (BodyOutcome::Truncated { bytes: 0, cause }, None),
    };
    if let BodyOutcome::Truncated { cause, .. } = &mut body {
        cause.timings.first_byte = Some(first_byte);
    }
    Ok(ReadOutcome {
        status,
        headers,
        raw_headers,
        dropped_headers,
        body,
        first_byte,
        last_byte,
    })
}

/// Attaches the first response head's offset, if one arrived, to a failure.
fn timed(mut error: ExplicitError, first_byte: Option<Duration>) -> ExplicitError {
    error.timings.first_byte = first_byte;
    error
}

/// How the final head left the body.
enum HeadEnding {
    /// The head carried the end of the message.
    Complete,
    /// The body follows.
    Body,
    /// The stream failed after the head arrived.
    Failed(ExplicitError),
}

/// Splits off the final head that a failed head read carries. A stream source whose stream fails
/// with its final head still queued moves that head into the error and discards the queued body.
///
/// The codec's own errors also carry the message it was parsing. For a head it rejected, that is
/// the partly parsed head: the HTTP/1.1 codec rejects a head with `HEADER_PARSE_ERROR`, or with
/// `PROTOCOL_ERROR` for invalid header content, so no head on those errors is a final head. For a
/// close inside a head, it is a message whose status line never parsed, which is no head either.
/// Uniplex builds that codec without strict validation (`Http1xCodec::new`), so a trailer that
/// fails validation is a `BODY_PARSE_ERROR`; strict trailer validation would make
/// `PROTOCOL_ERROR` reachable after a head.
fn final_head(mut error: HttpError) -> Result<(HttpMessage, HttpError), HttpError> {
    let rejected = error.code() == HttpErrorCode::HEADER_PARSE_ERROR
        || error.code() == HttpErrorCode::PROTOCOL_ERROR;
    // `is_final` turns a message of no kind into a response with status 0, so check first.
    let is_final = !rejected
        && error.http_message_mut().is_some_and(|head| {
            head.response_status().is_some() && head.is_final().unwrap_or(false)
        });
    if !is_final {
        return Err(error);
    }
    let head = error
        .take_http_message()
        .expect("should hold the final head checked above");
    Ok((head, error))
}

async fn drain_body(
    source: &mut HttpSourceHolder,
    observer: &Observer<'_>,
    sleep: &mut LocalFuture,
    elapsed: &impl Fn() -> Duration,
    hooks: &ExchangeHooks,
) -> (BodyOutcome, Option<Duration>) {
    let mut bytes = 0_u64;
    let truncated = |bytes, cause| (BodyOutcome::Truncated { bytes, cause }, None);
    let maximum = u32::try_from(STREAM_LIMIT).expect("should fit the body read limit in u32");
    loop {
        let event = match race(observer, sleep, source.read_body_event(maximum)).await {
            Raced::Expired => return truncated(bytes, observer.expired(Phase::Body)),
            Raced::Done(Err(error)) => {
                return truncated(bytes, observer.classify(&error, Position::AfterHead));
            }
            Raced::Done(Ok(event)) => event,
        };
        let eom = event.eom();
        match event.into_kind() {
            HttpBodyEventKind::Body(data) => {
                bytes += u64::try_from(data.chain_length()).unwrap_or(u64::MAX);
                hooks.body_counted(bytes);
            }
            HttpBodyEventKind::Suspend(resume) => {
                let failure = match race(observer, sleep, resume).await {
                    Raced::Expired => return truncated(bytes, observer.expired(Phase::Body)),
                    Raced::Done(ResumeStatus::Signalled) => None,
                    Raced::Done(ResumeStatus::TimedOut) => Some(HttpError::with_message(
                        HttpErrorCode::READ_TIMEOUT,
                        b"response body suspended until timeout",
                    )),
                    Raced::Done(ResumeStatus::Cancelled | ResumeStatus::NotReady) => {
                        Some(HttpError::with_message(
                            HttpErrorCode::CORO_CANCELLED,
                            b"response body suspension cancelled",
                        ))
                    }
                };
                if let Some(error) = failure {
                    return truncated(bytes, observer.classify(&error, Position::AfterHead));
                }
            }
            HttpBodyEventKind::Trailers(_)
            | HttpBodyEventKind::Padding(_)
            | HttpBodyEventKind::Upgrade
            | HttpBodyEventKind::PushPromise(_) => {}
        }
        if eom {
            return (BodyOutcome::Complete { bytes }, Some(elapsed()));
        }
    }
}

fn request_source(request: &Request, secure: bool) -> Result<HttpSourceHolder, ExplicitError> {
    let invalid = |error: MessageTypeError| {
        ExplicitError::new(Phase::Submit, ErrorKind::Other, error.to_string())
    };
    let mut message = HttpMessage::new();
    message
        .set_url(request.path_and_query.as_str(), false)
        .map_err(invalid)?;
    message.set_method(HttpMethod::Get).map_err(invalid)?;
    message.set_http_version(1, 1);
    let headers = message.headers_mut();
    request
        .headers
        .iter()
        .for_each(|(name, value)| headers.add(name.as_str(), value.as_bytes()));
    headers.set(HttpHeaderCode::Host, request.authority.as_str());
    message.set_wants_keepalive(true);
    message.set_secure(secure);
    Ok(HttpSourceHolder::new(Box::new(HttpFixedSource::new(
        message, None,
    ))))
}

/// Converts in wire order, dropping and counting what `HeaderMap` cannot represent. Every value
/// is a view of one copy of all the values. Custom names come from a bounded per-thread cache
/// that holds only names.
pub(crate) fn convert_headers(headers: &HttpHeaders) -> (HeaderMap, usize) {
    // `HeaderMap::with_capacity` panics above its maximum size, which a peer's header count can
    // exceed.
    let mut converted = HeaderMap::try_with_capacity(headers.len()).unwrap_or_default();
    let mut values = Vec::with_capacity(headers.iter().map(|(_, _, value)| value.len()).sum());
    headers
        .iter()
        .for_each(|(_, _, value)| values.extend_from_slice(value));
    let values = Bytes::from(values);
    let mut dropped = 0;
    let mut start = 0;
    headers.iter().for_each(|(code, name, value)| {
        let end = start + value.len();
        let appended = header_name(code, name)
            .zip(HeaderValue::from_maybe_shared(values.slice(start..end)).ok())
            .is_some_and(|(name, value)| converted.try_append(name, value).is_ok());
        if !appended {
            dropped += 1;
        }
        start = end;
    });
    (converted, dropped)
}

/// The constant for a common header's code, or `name` as `HeaderName::from_bytes` parses it. A
/// common header's `name` is its table spelling, which parses to that constant.
fn header_name(code: HttpHeaderCode, name: &[u8]) -> Option<HeaderName> {
    let constant = match code {
        HttpHeaderCode::Accept => header::ACCEPT,
        HttpHeaderCode::AcceptEncoding => header::ACCEPT_ENCODING,
        HttpHeaderCode::AcceptLanguage => header::ACCEPT_LANGUAGE,
        HttpHeaderCode::AcceptRanges => header::ACCEPT_RANGES,
        HttpHeaderCode::AccessControlAllowCredentials => header::ACCESS_CONTROL_ALLOW_CREDENTIALS,
        HttpHeaderCode::AccessControlAllowMethods => header::ACCESS_CONTROL_ALLOW_METHODS,
        HttpHeaderCode::AccessControlAllowOrigin => header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HttpHeaderCode::AccessControlExposeHeaders => header::ACCESS_CONTROL_EXPOSE_HEADERS,
        HttpHeaderCode::Age => header::AGE,
        HttpHeaderCode::AltSvc => header::ALT_SVC,
        HttpHeaderCode::Authorization => header::AUTHORIZATION,
        HttpHeaderCode::CacheControl => header::CACHE_CONTROL,
        HttpHeaderCode::ClientTimeout => CLIENT_TIMEOUT,
        HttpHeaderCode::Connection => header::CONNECTION,
        HttpHeaderCode::ContentDigest => CONTENT_DIGEST,
        HttpHeaderCode::ContentDisposition => header::CONTENT_DISPOSITION,
        HttpHeaderCode::ContentEncoding => header::CONTENT_ENCODING,
        HttpHeaderCode::ContentLanguage => header::CONTENT_LANGUAGE,
        HttpHeaderCode::ContentLength => header::CONTENT_LENGTH,
        HttpHeaderCode::ContentRange => header::CONTENT_RANGE,
        HttpHeaderCode::ContentSecurityPolicy => header::CONTENT_SECURITY_POLICY,
        HttpHeaderCode::ContentType => header::CONTENT_TYPE,
        HttpHeaderCode::Cookie => header::COOKIE,
        HttpHeaderCode::CrossOriginEmbedderPolicyReportOnly => {
            CROSS_ORIGIN_EMBEDDER_POLICY_REPORT_ONLY
        }
        HttpHeaderCode::CrossOriginOpenerPolicy => CROSS_ORIGIN_OPENER_POLICY,
        HttpHeaderCode::CrossOriginResourcePolicy => CROSS_ORIGIN_RESOURCE_POLICY,
        HttpHeaderCode::Date => header::DATE,
        HttpHeaderCode::DocumentPolicy => DOCUMENT_POLICY,
        HttpHeaderCode::EdgeControl => EDGE_CONTROL,
        HttpHeaderCode::Etag => header::ETAG,
        HttpHeaderCode::Expires => header::EXPIRES,
        HttpHeaderCode::Host => header::HOST,
        HttpHeaderCode::IfModifiedSince => header::IF_MODIFIED_SINCE,
        HttpHeaderCode::IfNoneMatch => header::IF_NONE_MATCH,
        HttpHeaderCode::KeepAlive => KEEP_ALIVE,
        HttpHeaderCode::LastModified => header::LAST_MODIFIED,
        HttpHeaderCode::Location => header::LOCATION,
        HttpHeaderCode::NoVarySearch => NO_VARY_SEARCH,
        HttpHeaderCode::PermissionsPolicy => PERMISSIONS_POLICY,
        HttpHeaderCode::Pragma => header::PRAGMA,
        HttpHeaderCode::Priority => PRIORITY,
        HttpHeaderCode::ProxyAuthenticate => header::PROXY_AUTHENTICATE,
        HttpHeaderCode::ProxyAuthorization => header::PROXY_AUTHORIZATION,
        HttpHeaderCode::ProxyConnection => PROXY_CONNECTION,
        HttpHeaderCode::ProxyStatus => PROXY_STATUS,
        HttpHeaderCode::QueueTimeout => QUEUE_TIMEOUT,
        HttpHeaderCode::Range => header::RANGE,
        HttpHeaderCode::Referer => header::REFERER,
        HttpHeaderCode::ReportTo => REPORT_TO,
        HttpHeaderCode::ReportingEndpoints => REPORTING_ENDPOINTS,
        HttpHeaderCode::RpcKind => RPC_KIND,
        HttpHeaderCode::SecWebSocketKey => header::SEC_WEBSOCKET_KEY,
        HttpHeaderCode::SecWebSocketAccept => header::SEC_WEBSOCKET_ACCEPT,
        HttpHeaderCode::Server => header::SERVER,
        HttpHeaderCode::SetCookie => header::SET_COOKIE,
        HttpHeaderCode::StrictTransportSecurity => header::STRICT_TRANSPORT_SECURITY,
        HttpHeaderCode::Te => header::TE,
        HttpHeaderCode::TimingAllowOrigin => TIMING_ALLOW_ORIGIN,
        HttpHeaderCode::Trailer => header::TRAILER,
        HttpHeaderCode::TransferEncoding => header::TRANSFER_ENCODING,
        HttpHeaderCode::Upgrade => header::UPGRADE,
        HttpHeaderCode::UserAgent => header::USER_AGENT,
        HttpHeaderCode::Vary => header::VARY,
        HttpHeaderCode::Via => header::VIA,
        HttpHeaderCode::XContentTypeOptions => header::X_CONTENT_TYPE_OPTIONS,
        HttpHeaderCode::XForwardedFor => X_FORWARDED_FOR,
        HttpHeaderCode::XFrameOptions => header::X_FRAME_OPTIONS,
        HttpHeaderCode::XRobotsTag => X_ROBOTS_TAG,
        HttpHeaderCode::XXssProtection => header::X_XSS_PROTECTION,
        HttpHeaderCode::Other => return custom_name(name),
        _ => return parse_name(name),
    };
    Some(constant)
}

// The common headers that `http` has no constant for, spelled as the common-header table's
// lowercase names.
const CLIENT_TIMEOUT: HeaderName = HeaderName::from_static("client_timeout");
const CONTENT_DIGEST: HeaderName = HeaderName::from_static("content-digest");
const CROSS_ORIGIN_EMBEDDER_POLICY_REPORT_ONLY: HeaderName =
    HeaderName::from_static("cross-origin-embedder-policy-report-only");
const CROSS_ORIGIN_OPENER_POLICY: HeaderName =
    HeaderName::from_static("cross-origin-opener-policy");
const CROSS_ORIGIN_RESOURCE_POLICY: HeaderName =
    HeaderName::from_static("cross-origin-resource-policy");
const DOCUMENT_POLICY: HeaderName = HeaderName::from_static("document-policy");
const EDGE_CONTROL: HeaderName = HeaderName::from_static("edge-control");
const KEEP_ALIVE: HeaderName = HeaderName::from_static("keep-alive");
const NO_VARY_SEARCH: HeaderName = HeaderName::from_static("no-vary-search");
const PERMISSIONS_POLICY: HeaderName = HeaderName::from_static("permissions-policy");
const PRIORITY: HeaderName = HeaderName::from_static("priority");
const PROXY_CONNECTION: HeaderName = HeaderName::from_static("proxy-connection");
const PROXY_STATUS: HeaderName = HeaderName::from_static("proxy-status");
const QUEUE_TIMEOUT: HeaderName = HeaderName::from_static("queue_timeout");
const REPORT_TO: HeaderName = HeaderName::from_static("report-to");
const REPORTING_ENDPOINTS: HeaderName = HeaderName::from_static("reporting-endpoints");
const RPC_KIND: HeaderName = HeaderName::from_static("rpckind");
const TIMING_ALLOW_ORIGIN: HeaderName = HeaderName::from_static("timing-allow-origin");
const X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");
const X_ROBOTS_TAG: HeaderName = HeaderName::from_static("x-robots-tag");

/// The most names that a thread's `NameCache` holds.
const NAME_CACHE_CAPACITY: usize = 64;

/// The longest name that a `NameCache` holds.
const CACHED_NAME_LIMIT: usize = 64;

thread_local! {
    /// Custom names that `convert_headers` has parsed on this thread, allocated at the first.
    static NAME_CACHE: RefCell<Option<Box<NameCache>>> = const { RefCell::new(None) };
}

/// A custom header name, from this thread's name cache when the name is short enough and the
/// cache can be reached, and otherwise parsed.
fn custom_name(name: &[u8]) -> Option<HeaderName> {
    if name.len() > CACHED_NAME_LIMIT {
        return parse_name(name);
    }
    NAME_CACHE
        .try_with(|cache| {
            let mut cache = cache.try_borrow_mut().ok()?;
            Some(cache.get_or_insert_with(NameCache::new).name(name))
        })
        .ok()
        .flatten()
        .unwrap_or_else(|| parse_name(name))
}

/// Parsed custom header names. A name is found by a hash that ignores ASCII case and confirmed
/// by an ASCII case-insensitive comparison with the cached lowercase name, which is the name
/// that `HeaderName::from_bytes` gives every case variant. Once full, each new name replaces the
/// oldest.
struct NameCache {
    /// The hash of each of `names`, by index.
    hashes: [u64; NAME_CACHE_CAPACITY],
    names: Vec<HeaderName>,
    /// The entry that the next new name replaces once the cache is full.
    next: usize,
}

impl NameCache {
    fn new() -> Box<Self> {
        Box::new(Self {
            hashes: [0; NAME_CACHE_CAPACITY],
            names: Vec::with_capacity(NAME_CACHE_CAPACITY),
            next: 0,
        })
    }

    /// `name` as `HeaderName::from_bytes` parses it: cached, or parsed and cached if valid.
    fn name(&mut self, name: &[u8]) -> Option<HeaderName> {
        let hash = folded_hash(name);
        let cached = self
            .hashes
            .iter()
            .zip(&self.names)
            .find(|(stored, cached)| {
                **stored == hash && name.eq_ignore_ascii_case(cached.as_str().as_bytes())
            });
        if let Some((_, cached)) = cached {
            return Some(cached.clone());
        }
        let parsed = parse_name(name)?;
        let index = if self.names.len() < NAME_CACHE_CAPACITY {
            self.names.push(parsed.clone());
            self.names.len() - 1
        } else {
            let index = self.next;
            self.next = (index + 1) % NAME_CACHE_CAPACITY;
            self.names[index] = parsed.clone();
            index
        };
        self.hashes[index] = hash;
        Some(parsed)
    }
}

/// A hash of `name` that ASCII case does not change, a word at a time. Setting bit `0x20` of
/// every byte takes each capital letter to its lowercase letter.
fn folded_hash(name: &[u8]) -> u64 {
    const FOLD: u64 = 0x2020_2020_2020_2020;
    const MULTIPLIER: u64 = 0x9e37_79b9_7f4a_7c15;
    let (words, tail) = name.as_chunks::<8>();
    let mut last = [0; 8];
    last[..tail.len()].copy_from_slice(tail);
    words
        .iter()
        .chain([&last])
        .fold(name.len() as u64, |hash, word| {
            (hash ^ (u64::from_le_bytes(*word) | FOLD))
                .wrapping_mul(MULTIPLIER)
                .rotate_left(29)
        })
}

/// `HeaderName::from_bytes`, counted in test builds.
fn parse_name(name: &[u8]) -> Option<HeaderName> {
    #[cfg(test)]
    NAME_PARSES.set(NAME_PARSES.get() + 1);
    HeaderName::from_bytes(name).ok()
}

#[cfg(test)]
thread_local! {
    static NAME_PARSES: Cell<usize> = const { Cell::new(0) };
}

/// How many header names `convert_headers` has parsed on this thread.
#[cfg(test)]
pub(crate) fn name_parses() -> usize {
    NAME_PARSES.get()
}

fn socket_address(endpoint: &TransportEndpoint) -> Option<SocketAddr> {
    let address = endpoint.socket_address()?;
    let ip: IpAddr = std::str::from_utf8(address.host()).ok()?.parse().ok()?;
    Some(SocketAddr::new(ip, address.port()))
}

#[derive(Clone, Default)]
pub(crate) struct ExchangeHooks {
    #[cfg(test)]
    pub(crate) before_adoption: Option<TestPause>,
    #[cfg(test)]
    pub(crate) panic_before_handoff: bool,
    #[cfg(test)]
    pub(crate) before_head: Option<TestPause>,
    #[cfg(test)]
    pub(crate) panic_before_head: bool,
    #[cfg(test)]
    pub(crate) before_finish: Option<TestPause>,
    #[cfg(test)]
    pub(crate) before_close: Option<TestPause>,
    #[cfg(test)]
    pub(crate) unconfirmed_retirement: bool,
    #[cfg(test)]
    pub(crate) body_counted: Option<Arc<AtomicU64>>,
    #[cfg(test)]
    pub(crate) probe: Option<Arc<ExchangeProbe>>,
}

/// Test-only counts and records of the exchanges whose hooks carry it.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct ExchangeProbe {
    /// Deadline sleeps created on the worker.
    pub(crate) deadline_sleeps: AtomicUsize,
    /// Calls to `validate_request`.
    pub(crate) validations: AtomicUsize,
    /// The session parameters of each attach.
    pub(crate) attach_records: Mutex<Vec<AttachRecord>>,
    /// The limits of each adoption.
    pub(crate) adoption_limits: Mutex<Vec<ByteStreamLimits>>,
}

/// The session parameters that an exchange attaches its session with, other than its
/// observers.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AttachRecord {
    pub(crate) max_concurrent_outgoing_streams: Option<u32>,
    pub(crate) stream_read_timeout: Option<Duration>,
    pub(crate) connection_read_timeout: Option<Duration>,
    pub(crate) write_timeout: Option<Duration>,
}

/// Hook points, which only test builds act on.
impl ExchangeHooks {
    async fn before_adoption(&self) {
        #[cfg(test)]
        if let Some(pause) = &self.before_adoption {
            pause.pause().await;
        }
    }

    fn before_handoff(&self) {
        #[cfg(test)]
        if self.panic_before_handoff {
            panic!("injected panic before the request is handed to the session");
        }
    }

    async fn before_head(&self) {
        #[cfg(test)]
        if let Some(pause) = &self.before_head {
            pause.pause().await;
            if self.panic_before_head {
                panic!("injected panic after the request is handed to the session");
            }
        }
    }

    async fn before_finish(&self) {
        #[cfg(test)]
        if let Some(pause) = &self.before_finish {
            pause.pause().await;
        }
    }

    async fn before_close(&self) {
        #[cfg(test)]
        if let Some(pause) = &self.before_close {
            pause.pause().await;
        }
    }

    /// Publishes the body bytes counted so far.
    fn body_counted(&self, bytes: u64) {
        #[cfg(test)]
        if let Some(counted) = &self.body_counted {
            counted.store(bytes, Ordering::Release);
        }
        #[cfg(not(test))]
        let _ = bytes;
    }

    pub(crate) fn deadline_sleep_created(&self) {
        #[cfg(test)]
        if let Some(probe) = &self.probe {
            probe.deadline_sleeps.fetch_add(1, Ordering::SeqCst);
        }
    }

    pub(crate) fn request_validated(&self) {
        #[cfg(test)]
        if let Some(probe) = &self.probe {
            probe.validations.fetch_add(1, Ordering::SeqCst);
        }
    }

    pub(crate) fn adopting(&self, limits: ByteStreamLimits) {
        #[cfg(test)]
        if let Some(probe) = &self.probe {
            probe.adoption_limits.lock().unwrap().push(limits);
        }
        #[cfg(not(test))]
        let _ = limits;
    }

    pub(crate) fn attaching(&self, session: &SessionParams) {
        #[cfg(test)]
        if let Some(probe) = &self.probe {
            probe.attach_records.lock().unwrap().push(AttachRecord {
                max_concurrent_outgoing_streams: session.max_concurrent_outgoing_streams,
                stream_read_timeout: session.stream_read_timeout,
                connection_read_timeout: session.connection_read_timeout,
                write_timeout: session.write_timeout,
            });
        }
        #[cfg(not(test))]
        let _ = session;
    }

    /// Whether the cleanup may confirm a retirement that `wait_retired` reports. A test build can
    /// refuse, as a failed `wait_retired` does.
    pub(crate) fn confirms_retirement(&self) -> bool {
        #[cfg(test)]
        if self.unconfirmed_retirement {
            return false;
        }
        true
    }
}

#[cfg(test)]
pub(crate) use test_pause::TestPause;

#[cfg(test)]
mod test_pause {
    use std::future::poll_fn;
    use std::sync::Arc;
    use std::sync::Condvar;
    use std::sync::Mutex;
    use std::sync::MutexGuard;
    use std::task::Poll;
    use std::task::Waker;
    use std::time::Duration;
    use std::time::Instant;

    const GUARD: Duration = Duration::from_secs(10);

    #[derive(Default)]
    struct PauseState {
        reached: usize,
        released: bool,
        wakers: Vec<Waker>,
    }

    #[derive(Default)]
    struct PauseShared {
        state: Mutex<PauseState>,
        changed: Condvar,
    }

    /// A crate-internal gate that holds code at a hook point until a test releases it.
    #[derive(Clone, Default)]
    pub(crate) struct TestPause {
        shared: Arc<PauseShared>,
    }

    impl TestPause {
        pub(crate) fn new() -> Self {
            Self::default()
        }

        fn lock(&self) -> MutexGuard<'_, PauseState> {
            self.shared
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        }

        fn notify(&self, mut state: MutexGuard<'_, PauseState>) {
            let wakers = std::mem::take(&mut state.wakers);
            drop(state);
            self.shared.changed.notify_all();
            wakers.into_iter().for_each(Waker::wake);
        }

        fn arrive(&self) {
            let mut state = self.lock();
            state.reached += 1;
            self.notify(state);
        }

        /// Records arrival, then waits asynchronously for release.
        pub(crate) async fn pause(&self) {
            self.arrive();
            poll_fn(|context| {
                let mut state = self.lock();
                if state.released {
                    Poll::Ready(())
                } else {
                    state.wakers.push(context.waker().clone());
                    Poll::Pending
                }
            })
            .await;
        }

        /// Records arrival, then blocks the calling thread until release.
        pub(crate) fn pause_blocking(&self) {
            self.arrive();
            let mut state = self.lock();
            while !state.released {
                state = self
                    .shared
                    .changed
                    .wait(state)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        }

        pub(crate) fn release(&self) {
            let mut state = self.lock();
            state.released = true;
            self.notify(state);
        }

        pub(crate) fn reached(&self) -> usize {
            self.lock().reached
        }

        /// Blocks until `count` arrivals, failing after a hang guard.
        pub(crate) fn wait_reached(&self, count: usize) {
            let deadline = Instant::now() + GUARD;
            let mut state = self.lock();
            while state.reached < count {
                let remaining = deadline.saturating_duration_since(Instant::now());
                assert!(!remaining.is_zero(), "hook point not reached");
                state = self
                    .shared
                    .changed
                    .wait_timeout(state, remaining)
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .0;
            }
        }

        /// Waits asynchronously until `count` arrivals.
        pub(crate) async fn reached_async(&self, count: usize) {
            poll_fn(|context| {
                let mut state = self.lock();
                if state.reached >= count {
                    Poll::Ready(())
                } else {
                    state.wakers.push(context.waker().clone());
                    Poll::Pending
                }
            })
            .await;
        }
    }
}
