//! HTTP/1.1 egress batching (`DIV-034`): an HTTP/1.1 server
//! session writes a response head with the body ready with it, body events that
//! are ready together, and a streamed response's last data with its end, in one
//! write each, as hyper's server coalesces them; the bytes are unchanged.

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::io::IoSlice;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

use bytes::Bytes;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::http::CodecProtocol;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::managed::RuntimeConfig;
use roxygen_runtime::managed::RuntimeOwner;
use roxygen_runtime::tokio_host::HostedWorker;
use roxygen_runtime::tokio_host::HostedWorkerOptions;
use roxygen_runtime::worker::WorkerContext;
use roxygen_transport::established::EstablishedIo;
use roxygen_transport::established::adopt_local;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::ReadBuf;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

use crate::acceptor::ServerSessionConfig;
use crate::allocation_count::COUNTS_EVERY_CRATE;
use crate::allocation_count::start_counting;
use crate::allocation_count::stop_counting;
use crate::connection::ServeTransportOptions;
use crate::connection::ServerConnection;
use crate::connection::serve_transport;

const TIMEOUT: Duration = Duration::from_secs(10);

fn wait<Work: Future>(future: Work) -> Work::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("application runtime")
        .block_on(async {
            tokio::time::timeout(TIMEOUT, future)
                .await
                .expect("bounded test completion")
        })
}

/// Runs `test` on a fresh `RuntimeOwner` worker, which must then quiesce without
/// forcing, and again on a `HostedWorker`, whose commands and cleanups are
/// independent Tokio tasks that only their own wakeups poll.
fn on_worker<Test, Work>(test: Test)
where
    Test: FnOnce(WorkerContext) -> Work + Clone + Send + 'static,
    Work: Future<Output = ()> + 'static,
{
    on_runtime_owner(test.clone());
    on_hosted_worker(test);
}

fn on_hosted_worker<Test, Work>(test: Test)
where
    Test: FnOnce(WorkerContext) -> Work + Send + 'static,
    Work: Future<Output = ()> + 'static,
{
    let worker = HostedWorker::spawn(HostedWorkerOptions {
        command_capacity: NonZeroUsize::new(4).expect("nonzero capacity"),
        thread_name: "egress-batching".to_owned(),
        on_owner_thread_stop: None,
    })
    .expect("hosted worker starts");
    wait(worker.handle().call_local(test).expect("test admitted")).expect("test finishes");
    let completion = worker.completion();
    drop(worker);
    assert!(
        matches!(*completion.borrow(), Some(Ok(()))),
        "the hosted worker joins cleanly"
    );
}

fn on_runtime_owner<Test, Work>(test: Test)
where
    Test: FnOnce(WorkerContext) -> Work + Send + 'static,
    Work: Future<Output = ()> + 'static,
{
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 4,
    })
    .expect("owner starts");
    let runtime = owner.handle();
    let worker = runtime.worker(0).expect("worker exists");
    wait(worker.call_local(test).expect("test admitted")).expect("test finishes");
    wait(runtime.quiesce(TIMEOUT)).expect("cleanup joins");
    assert_eq!(owner.join().expect("physical join").workers_forced, 0);
}

const REQUEST: &[u8] = b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n";
/// A fixed `Date`, so that responses compare byte for byte.
const DATE: &[u8] = b"Thu, 05 Nov 2026 00:00:00 GMT";

/// The server side of a loopback TCP connection, recording the bytes of each
/// write the session's transport makes.
struct RecordedWrites {
    inner: TcpStream,
    writes: Rc<RefCell<Vec<Vec<u8>>>>,
}

impl RecordedWrites {
    fn record(&self, written: &Poll<io::Result<usize>>, bytes: impl Iterator<Item = u8>) {
        if let Poll::Ready(Ok(written)) = written
            && *written != 0
        {
            self.writes
                .borrow_mut()
                .push(bytes.take(*written).collect());
        }
    }
}

impl AsyncRead for RecordedWrites {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl AsyncWrite for RecordedWrites {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let written = Pin::new(&mut self.inner).poll_write(context, buffer);
        self.record(&written, buffer.iter().copied());
        written
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let written = Pin::new(&mut self.inner).poll_write_vectored(context, buffers);
        self.record(
            &written,
            buffers.iter().flat_map(|buffer| buffer.iter().copied()),
        );
        written
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

/// One body event of a `ScriptedResponse`: its bytes, its end of message, and
/// whether its source is pending once before it is ready.
#[derive(Clone, Copy)]
struct Step {
    bytes: &'static [u8],
    eom: bool,
    after_pending: bool,
}

const fn ready(bytes: &'static [u8], eom: bool) -> Step {
    Step {
        bytes,
        eom,
        after_pending: false,
    }
}

const fn after_pending(bytes: &'static [u8], eom: bool) -> Step {
    Step {
        bytes,
        eom,
        after_pending: true,
    }
}

/// A 200 response with the fixed `Date`, a `Content-Length` if given (chunked
/// otherwise), and the body events of `steps`.
struct ScriptedResponse {
    header: Option<HttpHeaderEvent>,
    steps: VecDeque<Step>,
}

impl HttpSource for ScriptedResponse {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(self.header.take().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"response header exhausted")
        })))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let step = self.steps.pop_front().expect("a body event is left");
        Box::pin(async move {
            if step.after_pending {
                tokio::task::yield_now().await;
            }
            let body = BufQueue::from_bytes(Bytes::from_static(step.bytes), step.bytes.len())
                .expect("bounded body");
            Ok(HttpBodyEvent::body(body, step.eom))
        })
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

struct ScriptedHandler {
    content_length: Option<usize>,
    steps: &'static [Step],
}

impl HttpHandler for ScriptedHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let mut message = HttpMessage::new();
        message.set_status_code(200).expect("status");
        message
            .set_status_message(HttpMessage::default_reason(200))
            .expect("reason");
        message.set_http_version(1, 1);
        message.headers_mut().set(HttpHeaderCode::Date, DATE);
        if let Some(length) = self.content_length {
            message
                .headers_mut()
                .set(HttpHeaderCode::ContentLength, length.to_string().as_bytes());
        }
        let source = ScriptedResponse {
            header: Some(HttpHeaderEvent::new(message, false).expect("response head")),
            steps: self.steps.iter().copied().collect(),
        };
        Box::pin(std::future::ready(Ok(HttpSourceHolder::new(Box::new(
            source,
        )))))
    }
}

/// Serves one loopback connection with `serve_transport` on this worker and
/// answers one GET with `handler`'s response; returns the server's writes and
/// the bytes the client read up to `response_end`.
async fn serve_one(
    context: WorkerContext,
    handler: ScriptedHandler,
    response_end: &'static [u8],
) -> (Vec<Vec<u8>>, Vec<u8>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let mut client = TcpStream::connect(listener.local_addr().expect("listener address"))
        .await
        .expect("client connects");
    let (accepted, _) = listener.accept().await.expect("connection accepted");
    accepted.set_nodelay(true).expect("server socket option");
    let writes = Rc::new(RefCell::new(Vec::new()));
    let session = ServerSessionConfig {
        stream_read_timeout: Duration::ZERO,
        connection_idle_timeout: Duration::ZERO,
        write_timeout: Duration::ZERO,
        ..ServerSessionConfig::default()
    };
    let transport = adopt_local(
        &context,
        EstablishedIo::new(
            RecordedWrites {
                inner: accepted,
                writes: Rc::clone(&writes),
            },
            TransportEndpoint::Unknown,
            TransportEndpoint::Unknown,
            None,
        ),
        session.byte_stream_limits,
    )
    .expect("I/O adopted");
    let mut options =
        ServeTransportOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext);
    options.session = session;
    let connection =
        serve_transport(&context, transport, Rc::new(handler), options).expect("attached");
    client.write_all(REQUEST).await.expect("request written");
    let mut wire = Vec::new();
    while !wire.ends_with(response_end) {
        let mut chunk = [0; 512];
        let read = client.read(&mut chunk).await.expect("response read");
        assert_ne!(read, 0, "the response arrives before close");
        wire.extend_from_slice(&chunk[..read]);
    }
    drop(client);
    connection
        .wait_closed()
        .await
        .expect("the connection closes cleanly");
    let writes = writes.take();
    (writes, wire)
}

/// Records one response on both managed and hosted workers.
fn recorded_writes(
    content_length: Option<usize>,
    steps: &'static [Step],
    response_end: &'static [u8],
) -> Vec<Vec<Vec<u8>>> {
    let (sender, receiver) = std::sync::mpsc::channel();
    on_worker(move |context| async move {
        let handler = ScriptedHandler {
            content_length,
            steps,
        };
        let (writes, wire) = serve_one(context, handler, response_end).await;
        assert_eq!(writes.concat(), wire, "every write is recorded");
        sender.send(writes).expect("writes recorded");
    });
    let runs: Vec<_> = receiver.try_iter().collect();
    assert_eq!(runs.len(), 2, "both executors ran");
    runs
}

/// The response head the codec writes, in its header order.
fn head(content_length: Option<&str>) -> Vec<u8> {
    let framing = content_length.map_or_else(
        || "Transfer-Encoding: chunked\r\nConnection: keep-alive\r\n".to_owned(),
        |length| format!("Connection: keep-alive\r\nContent-Length: {length}\r\n"),
    );
    format!(
        "HTTP/1.1 200 OK\r\nDate: {}\r\n{framing}\r\n",
        std::str::from_utf8(DATE).expect("ASCII date")
    )
    .into_bytes()
}

#[test]
fn http1_response_head_and_ready_body_share_a_write() {
    const STEPS: &[Step] = &[ready(b"hello", true)];
    for batched in recorded_writes(Some(5), STEPS, b"hello") {
        assert_eq!(batched, [[head(Some("5")), b"hello".to_vec()].concat()]);
    }
}

/// Body events ready together share a write, and so do a streamed response's
/// last data and its end, here a separate empty event as a response stream's
/// end arrives after its last item.
#[test]
fn http1_streamed_response_writes_ready_events_and_its_end_together() {
    const STEPS: &[Step] = &[
        after_pending(b"first", false),
        ready(b"second", false),
        after_pending(b"last", false),
        ready(b"", true),
    ];
    for batched in recorded_writes(None, STEPS, b"0\r\n\r\n") {
        assert_eq!(
            batched,
            [
                head(None),
                b"5\r\nfirst\r\n6\r\nsecond\r\n".to_vec(),
                b"4\r\nlast\r\n0\r\n\r\n".to_vec(),
            ]
        );
    }
}

/// The server side of a loopback TCP connection, counting the session's
/// transport writes without allocating.
struct CountedWrites {
    inner: TcpStream,
    writes: Rc<Cell<usize>>,
}

impl CountedWrites {
    fn count(&self, written: &Poll<io::Result<usize>>) {
        if matches!(written, Poll::Ready(Ok(written)) if *written != 0) {
            self.writes.set(self.writes.get() + 1);
        }
    }
}

impl AsyncRead for CountedWrites {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl AsyncWrite for CountedWrites {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let written = Pin::new(&mut self.inner).poll_write(context, buffer);
        self.count(&written);
        written
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let written = Pin::new(&mut self.inner).poll_write_vectored(context, buffers);
        self.count(&written);
        written
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

static TOKEN: [u8; 16] = [b't'; 16];

/// A streamed 200 of `remaining` 16-byte tokens, each ready after one
/// `Pending` (the source wakes itself), then a ready empty end, as a token
/// stream's end follows its last item.
struct TokenResponse {
    header: Option<HttpHeaderEvent>,
    remaining: usize,
}

impl HttpSource for TokenResponse {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(self.header.take().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"response header exhausted")
        })))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        if self.remaining == 0 {
            return Box::pin(std::future::ready(Ok(HttpBodyEvent::body(
                BufQueue::new(0),
                true,
            ))));
        }
        self.remaining -= 1;
        let mut primed = false;
        Box::pin(std::future::poll_fn(move |context| {
            if !std::mem::replace(&mut primed, true) {
                context.waker().wake_by_ref();
                return Poll::Pending;
            }
            let token = BufQueue::from_bytes(Bytes::from_static(&TOKEN), TOKEN.len())
                .expect("a token fits its queue");
            Poll::Ready(Ok(HttpBodyEvent::body(token, false)))
        }))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

struct TokenHandler(usize);

impl HttpHandler for TokenHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let mut message = HttpMessage::new();
        message.set_status_code(200).expect("status");
        message.set_http_version(1, 1);
        let source = TokenResponse {
            header: Some(HttpHeaderEvent::new(message, false).expect("response head")),
            remaining: self.0,
        };
        Box::pin(std::future::ready(Ok(HttpSourceHolder::new(Box::new(
            source,
        )))))
    }
}

/// Serves one loopback connection with `serve_transport` on this worker with
/// `session`'s settings, counting the server's transport writes; returns the
/// client's socket.
async fn attach_counted(
    context: &WorkerContext,
    handler: impl HttpHandler + 'static,
    session: ServerSessionConfig,
) -> (TcpStream, ServerConnection, Rc<Cell<usize>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let connect = TcpStream::connect(listener.local_addr().expect("address"));
    let (client, accepted) = tokio::join!(connect, listener.accept());
    let client = client.expect("client connects");
    let (accepted, _) = accepted.expect("connection accepted");
    accepted.set_nodelay(true).expect("server socket option");
    let writes = Rc::new(Cell::new(0));
    let transport = adopt_local(
        context,
        EstablishedIo::new(
            CountedWrites {
                inner: accepted,
                writes: Rc::clone(&writes),
            },
            TransportEndpoint::Unknown,
            TransportEndpoint::Unknown,
            None,
        ),
        session.byte_stream_limits,
    )
    .expect("I/O adopted");
    let mut options =
        ServeTransportOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext);
    options.session = session;
    let connection =
        serve_transport(context, transport, Rc::new(handler), options).expect("attached");
    (client, connection, writes)
}

/// Session settings with timers disabled.
pub(crate) fn timers_off() -> ServerSessionConfig {
    ServerSessionConfig {
        stream_read_timeout: Duration::ZERO,
        connection_idle_timeout: Duration::ZERO,
        write_timeout: Duration::ZERO,
        ..ServerSessionConfig::default()
    }
}

/// A token response as `serve_tokens` observed it.
struct ServedTokens {
    /// Heap allocations on the worker thread from the request to the response's end.
    allocations: usize,
    writes: usize,
    body: Vec<u8>,
}

/// Serves one `events`-token response on a `HostedWorker`, whose thread also
/// runs the client.
fn serve_tokens(events: usize) -> ServedTokens {
    let worker = HostedWorker::spawn(HostedWorkerOptions {
        command_capacity: NonZeroUsize::new(4).expect("nonzero capacity"),
        thread_name: "batched-tokens".to_owned(),
        on_owner_thread_stop: None,
    })
    .expect("hosted worker starts");
    let served = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("application runtime")
        .block_on(async {
            let call = worker
                .handle()
                .call_local(move |context| async move {
                    let session = timers_off();
                    let (mut client, connection, writes) =
                        attach_counted(&context, TokenHandler(events), session).await;
                    client.write_all(REQUEST).await.expect("request written");
                    let mut wire = Vec::with_capacity(1 << 20);
                    let mut buffer = vec![0; 1 << 16];
                    start_counting();
                    while !wire.ends_with(b"\r\n0\r\n\r\n") {
                        let read = client.read(&mut buffer).await.expect("response read");
                        assert_ne!(read, 0, "the response ends before the connection");
                        wire.extend_from_slice(&buffer[..read]);
                    }
                    let allocations = stop_counting();
                    drop(client);
                    connection
                        .wait_closed()
                        .await
                        .expect("the connection closes cleanly");
                    (allocations, writes.get(), wire)
                })
                .expect("test admitted");
            tokio::time::timeout(Duration::from_secs(10), call)
                .await
                .expect("bounded response")
                .expect("test finishes")
        });
    drop(worker);
    let (allocations, writes, wire) = served;
    let head_end = wire
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("a response head");
    ServedTokens {
        allocations,
        writes,
        body: wire[head_end + 4..].to_vec(),
    }
}

/// The chunked body of `events` tokens, one 16-byte (0x10) chunk each.
fn token_chunks(events: usize) -> Vec<u8> {
    let mut expected = [b"10\r\n".as_slice(), &TOKEN, b"\r\n"]
        .concat()
        .repeat(events);
    expected.extend_from_slice(b"0\r\n\r\n");
    expected
}

/// Allocations per body event, from the difference between two response
/// lengths, so that per-response costs cancel.
fn token_allocations_per_event() -> usize {
    let short = serve_tokens(200);
    let long = serve_tokens(400);
    assert_eq!(short.body, token_chunks(200));
    assert_eq!(long.body, token_chunks(400));
    let (short, long) = (short.allocations, long.allocations);
    assert_eq!(
        (long - short) % 200,
        0,
        "a whole number of allocations per event ({short}, {long})"
    );
    (long - short) / 200
}

/// Batching tries the next event without waiting while bytes are queued; for a
/// token stream that attempt finds the source pending before each flush, and
/// must not cost an allocation per event.
#[test]
#[cfg_attr(
    fbcode_build,
    ignore = "allocation counts cover only the test crate in Buck's shared-library test builds"
)]
fn a_token_stream_stays_within_its_allocation_budget() {
    let allocations = token_allocations_per_event();
    assert!(
        allocations <= 14,
        "{allocations} allocations per body event"
    );
}

#[test]
fn a_token_stream_writes_its_last_token_with_its_end() {
    const EVENTS: usize = 50;
    let served = serve_tokens(EVENTS);
    assert_eq!(served.body, token_chunks(EVENTS));
    assert_eq!(served.writes, 1 + EVENTS, "head and one write per token");
}

/// The tokens a producer pushed that the response has not taken yet, and the
/// task of the response's pending read.
#[derive(Default)]
pub(crate) struct Feed {
    tokens: usize,
    ended: bool,
    waker: Option<Waker>,
    /// Polls of the response's body reads.
    polls: usize,
}

impl Feed {
    /// One poll of the response's body read: the next pushed 16-byte token,
    /// the empty end once the producer ended the stream, `Pending` otherwise.
    pub(crate) fn poll_token(
        &mut self,
        context: &Context<'_>,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        self.polls += 1;
        if self.tokens != 0 {
            self.tokens -= 1;
            let token = BufQueue::from_bytes(Bytes::from_static(&TOKEN), TOKEN.len())
                .expect("a token fits its queue");
            return Poll::Ready(Ok(HttpBodyEvent::body(token, false)));
        }
        if self.ended {
            return Poll::Ready(Ok(HttpBodyEvent::body(BufQueue::new(0), true)));
        }
        self.waker = Some(context.waker().clone());
        Poll::Pending
    }

    /// Pushes the next token, or the stream's end, and wakes the pending read.
    fn push(feed: &RefCell<Self>, end: bool) {
        let waker = {
            let mut feed = feed.borrow_mut();
            if end {
                feed.ended = true;
            } else {
                feed.tokens += 1;
            }
            feed.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// A streamed 200 of the 16-byte tokens a producer pushes, then an empty end:
/// a body read is `Pending` on every poll until the producer pushes, as a
/// response fed by an upstream stream is.
struct FedResponse {
    header: Option<HttpHeaderEvent>,
    feed: Rc<RefCell<Feed>>,
}

impl HttpSource for FedResponse {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(self.header.take().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"response header exhausted")
        })))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let feed = Rc::clone(&self.feed);
        Box::pin(std::future::poll_fn(move |context| {
            feed.borrow_mut().poll_token(context)
        }))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

/// Makes the response source of a fed token response from its head and feed.
pub(crate) type FedSource = fn(HttpHeaderEvent, Rc<RefCell<Feed>>) -> Box<dyn HttpSource>;

fn fed_response(header: HttpHeaderEvent, feed: Rc<RefCell<Feed>>) -> Box<dyn HttpSource> {
    Box::new(FedResponse {
        header: Some(header),
        feed,
    })
}

struct FedHandler {
    feed: Rc<RefCell<Feed>>,
    source: FedSource,
}

impl HttpHandler for FedHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let mut message = HttpMessage::new();
        message.set_status_code(200).expect("status");
        message.set_http_version(1, 1);
        let header = HttpHeaderEvent::new(message, false).expect("response head");
        Box::pin(std::future::ready(Ok(HttpSourceHolder::new(
            (self.source)(header, Rc::clone(&self.feed)),
        ))))
    }
}

/// A fed token response as `serve_fed_tokens` observed it, from the first
/// token's push to the response's end.
pub(crate) struct FedTokens {
    /// Polls of the response's body reads.
    pub(crate) polls: usize,
    /// Heap allocations on the worker thread.
    pub(crate) allocations: usize,
    pub(crate) body: Vec<u8>,
}

/// One token chunk on the wire: `10\r\n`, 16 bytes, `\r\n`.
const TOKEN_CHUNK: usize = 22;

/// Serves one response of `events` fed tokens from `source` with `session`'s
/// settings on a `HostedWorker`, whose thread also runs the client, which
/// pushes each token once it has read the previous one, and then the end:
/// every token arrives after the session flushed the previous one, as tokens
/// paced by an engine do.
pub(crate) fn serve_fed_tokens(
    events: usize,
    session: ServerSessionConfig,
    source: FedSource,
) -> FedTokens {
    let worker = HostedWorker::spawn(HostedWorkerOptions {
        command_capacity: NonZeroUsize::new(4).expect("nonzero capacity"),
        thread_name: "fed-tokens".to_owned(),
        on_owner_thread_stop: None,
    })
    .expect("hosted worker starts");
    let served = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("application runtime")
        .block_on(async {
            let call = worker
                .handle()
                .call_local(move |context| async move {
                    let feed = Rc::new(RefCell::new(Feed::default()));
                    let handler = FedHandler {
                        feed: Rc::clone(&feed),
                        source,
                    };
                    let (mut client, connection, _) =
                        attach_counted(&context, handler, session).await;
                    client.write_all(REQUEST).await.expect("request written");
                    let mut wire = Vec::with_capacity(1 << 20);
                    let mut buffer = vec![0; 1 << 16];
                    let head_end = loop {
                        let read = client.read(&mut buffer).await.expect("response read");
                        assert_ne!(read, 0, "the head arrives before close");
                        wire.extend_from_slice(&buffer[..read]);
                        if let Some(end) = wire.windows(4).position(|window| window == b"\r\n\r\n")
                        {
                            break end + 4;
                        }
                    };
                    assert_eq!(wire.len(), head_end, "no body before the first token");
                    let polls_before = feed.borrow().polls;
                    start_counting();
                    Feed::push(&feed, false);
                    let mut pushed = 1;
                    while !wire.ends_with(b"\r\n0\r\n\r\n") {
                        let read = client.read(&mut buffer).await.expect("response read");
                        assert_ne!(read, 0, "the response ends before the connection");
                        wire.extend_from_slice(&buffer[..read]);
                        let received = (wire.len() - head_end) / TOKEN_CHUNK;
                        if received == pushed && pushed < events {
                            Feed::push(&feed, false);
                            pushed += 1;
                        } else if received == events && !feed.borrow().ended {
                            Feed::push(&feed, true);
                        }
                    }
                    let allocations = stop_counting();
                    let polls = feed.borrow().polls - polls_before;
                    drop(client);
                    connection
                        .wait_closed()
                        .await
                        .expect("the connection closes cleanly");
                    FedTokens {
                        polls,
                        allocations,
                        body: wire[head_end..].to_vec(),
                    }
                })
                .expect("test admitted");
            tokio::time::timeout(Duration::from_secs(10), call)
                .await
                .expect("bounded response")
                .expect("test finishes")
        });
    drop(worker);
    served
}

/// Source polls and allocations per body event of a fed token response, from
/// the difference between two response lengths, so that per-response costs
/// cancel.
pub(crate) fn fed_token_costs_per_event(
    session: ServerSessionConfig,
    source: FedSource,
) -> (usize, usize) {
    let short = serve_fed_tokens(100, session.clone(), source);
    let long = serve_fed_tokens(300, session, source);
    assert_eq!(short.body, token_chunks(100));
    assert_eq!(long.body, token_chunks(300));
    let per_event = |short: usize, long: usize, what: &str| {
        assert_eq!(
            (long - short) % 200,
            0,
            "a whole number of {what} per event ({short}, {long})"
        );
        (long - short) / 200
    };
    (
        per_event(short.polls, long.polls, "source polls"),
        per_event(short.allocations, long.allocations, "allocations"),
    )
}

/// Each token requires one pending poll before the flush and one ready poll after its wake.
#[test]
fn a_fed_token_stream_polls_its_source_twice_per_event() {
    let (polls, allocations) = fed_token_costs_per_event(timers_off(), fed_response);
    assert_eq!(polls, 2);
    assert!(
        !COUNTS_EVERY_CRATE || allocations <= 14,
        "{allocations} allocations per body event"
    );
}
