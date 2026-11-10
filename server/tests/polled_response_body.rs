//! A response source that polls its body events in place (`DIV-039`), served
//! over loopback TCP, with the worker thread's heap allocations counted
//! (`crate::allocation_count`).

use std::cell::Cell;
use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::http::CodecProtocol;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::tokio_host::HostedWorker;
use roxygen_runtime::tokio_host::HostedWorkerOptions;
use roxygen_transport::tcp::TcpTransport;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

use crate::acceptor::ServerSessionConfig;
use crate::allocation_count::COUNTS_EVERY_CRATE;
use crate::allocation_count::start_counting;
use crate::allocation_count::stop_counting;
use crate::connection::ServeTransportOptions;
use crate::connection::serve_transport;
use crate::eof_abort_tests::h2_request;
use crate::http1_egress_batching_tests::Feed;
use crate::http1_egress_batching_tests::fed_response;
use crate::http1_egress_batching_tests::fed_token_costs_per_event;
use crate::http1_egress_batching_tests::timers_off;

static TOKEN: [u8; 16] = [b't'; 16];
const REQUEST: &[u8] = b"GET /stream HTTP/1.1\r\nHost: localhost\r\n\r\n";

/// A streamed response of `remaining` 16-byte events, each produced after one
/// `Pending`, as a token stream produces them. With `polling`, it serves them
/// only through `poll_body_event`.
struct TokenBody {
    header: Option<HttpHeaderEvent>,
    remaining: usize,
    polling: bool,
    /// The next poll yields an event.
    primed: Rc<Cell<bool>>,
}

impl TokenBody {
    fn poll_token(
        primed: &Cell<bool>,
        context: &mut Context<'_>,
        eom: bool,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        if !primed.replace(false) {
            primed.set(true);
            context.waker().wake_by_ref();
            return Poll::Pending;
        }
        let token = BufQueue::from_bytes(Bytes::from_static(&TOKEN), TOKEN.len())
            .expect("a token fits its queue");
        Poll::Ready(Ok(HttpBodyEvent::body(token, eom)))
    }
}

impl HttpSource for TokenBody {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(self.header.take().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"response header exhausted")
        })))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        assert!(
            !self.polling,
            "a polling source is never read through a future"
        );
        self.remaining -= 1;
        let eom = self.remaining == 0;
        let primed = Rc::clone(&self.primed);
        Box::pin(std::future::poll_fn(move |context| {
            Self::poll_token(&primed, context, eom)
        }))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}

    fn polls_body_events(&self) -> bool {
        self.polling
    }

    fn poll_body_event(
        &mut self,
        context: &mut Context<'_>,
        _maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        let poll = Self::poll_token(&self.primed, context, self.remaining == 1);
        if poll.is_ready() {
            self.remaining -= 1;
        }
        poll
    }
}

struct TokenHandler {
    events: usize,
    polling: bool,
}

impl HttpHandler for TokenHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let mut message = HttpMessage::new();
        message.set_status_code(200).expect("status");
        message.set_http_version(1, 1);
        let source = TokenBody {
            header: Some(HttpHeaderEvent::new(message, false).expect("response head")),
            remaining: self.events,
            polling: self.polling,
            primed: Rc::new(Cell::new(false)),
        };
        Box::pin(std::future::ready(Ok(HttpSourceHolder::new(Box::new(
            source,
        )))))
    }
}

/// Serves one `events`-token response on a `HostedWorker` and returns the
/// heap allocations its worker thread made between the request and the
/// response's end, with the body the client received. `router_settings` adds
/// the router's server options: `abort_exchanges_on_eof` (`DIV-053`) and
/// `non_abortive_close` (`DIV-038`).
fn serve_tokens(events: usize, polling: bool, router_settings: bool) -> (usize, Vec<u8>) {
    serve_tokens_on(events, polling, router_settings, Serving::Uniplex)
}

/// The session that serves a [`serve_tokens_on`] connection.
#[derive(Clone, Copy, PartialEq)]
enum Serving {
    /// `UniplexSession`, over HTTP/1.1.
    Uniplex,
    /// The owned HTTP/1.1 driver (`owned_http1`, `DIV-029`).
    OwnedHttp1,
    /// The owned HTTP/2 driver (`owned_http2`), over h2c with prior knowledge.
    OwnedHttp2,
}

/// [`serve_tokens`], on the session `serving` names. Over HTTP/2 the body
/// returned is the DATA on stream 1.
fn serve_tokens_on(
    events: usize,
    polling: bool,
    router_settings: bool,
    serving: Serving,
) -> (usize, Vec<u8>) {
    let http2 = serving == Serving::OwnedHttp2;
    let worker = HostedWorker::spawn(HostedWorkerOptions {
        command_capacity: NonZeroUsize::new(4).expect("nonzero capacity"),
        thread_name: "polled-body".to_owned(),
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
                    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
                    let connect = TcpStream::connect(listener.local_addr().expect("address"));
                    let (client, accepted) = tokio::join!(connect, listener.accept());
                    let mut client = client.expect("client connects");
                    let (accepted, _) = accepted.expect("connection accepted");
                    let session = ServerSessionConfig {
                        stream_read_timeout: Duration::ZERO,
                        connection_idle_timeout: Duration::ZERO,
                        write_timeout: Duration::ZERO,
                        abort_exchanges_on_eof: router_settings,
                        non_abortive_close: router_settings,
                        owned_http1: serving == Serving::OwnedHttp1,
                        owned_http2: http2,
                        ..ServerSessionConfig::default()
                    };
                    let transport = TcpTransport::from_accepted(
                        &context,
                        accepted.into_std().expect("standard stream"),
                        session.byte_stream_limits,
                    )
                    .expect("transport adopted");
                    let mut options = ServeTransportOptions::new(
                        if http2 {
                            CodecProtocol::Http2
                        } else {
                            CodecProtocol::Http11
                        },
                        TransportSecurity::Plaintext,
                    );
                    options.session = session;
                    let _connection = serve_transport(
                        &context,
                        transport,
                        Rc::new(TokenHandler { events, polling }),
                        options,
                    )
                    .expect("attached");
                    let request = if http2 {
                        h2_request()
                    } else {
                        REQUEST.to_vec()
                    };
                    client.write_all(&request).await.expect("request written");
                    let mut wire = Vec::with_capacity(1 << 20);
                    let mut buffer = vec![0; 1 << 16];
                    start_counting();
                    while !(if http2 {
                        h2_stream_one_data(&wire).1
                    } else {
                        wire.ends_with(b"\r\n0\r\n\r\n")
                    }) {
                        let read = client.read(&mut buffer).await.expect("response read");
                        assert_ne!(read, 0, "the response ends before the connection");
                        wire.extend_from_slice(&buffer[..read]);
                    }
                    (stop_counting(), wire)
                })
                .expect("test admitted");
            tokio::time::timeout(Duration::from_secs(10), call)
                .await
                .expect("bounded response")
                .expect("test finishes")
        });
    drop(worker);
    let (allocations, wire) = served;
    if http2 {
        return (allocations, h2_stream_one_data(&wire).0);
    }
    let head_end = wire
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("a response head");
    (allocations, wire[head_end + 4..].to_vec())
}

/// The DATA payloads on stream 1 in `wire`, the server's frames from the start,
/// and whether one of them ended the stream. A trailing partial frame is left
/// unread.
fn h2_stream_one_data(mut wire: &[u8]) -> (Vec<u8>, bool) {
    let mut data = Vec::new();
    while let Some(header) = wire.get(..9) {
        let length =
            usize::from(header[0]) << 16 | usize::from(header[1]) << 8 | usize::from(header[2]);
        let Some(payload) = wire.get(9..9 + length) else {
            break;
        };
        let stream = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & 0x7fff_ffff;
        // A DATA frame on stream 1; the END_STREAM flag ends the response.
        if stream == 1 && header[3] == 0x0 {
            data.extend_from_slice(payload);
            if header[4] & 0x1 != 0 {
                return (data, true);
            }
        }
        wire = &wire[9 + length..];
    }
    (data, false)
}

/// The chunked body of `events` tokens, one 16-byte (0x10) chunk each.
fn token_chunks(events: usize) -> Vec<u8> {
    let mut expected = [b"10\r\n".as_slice(), &TOKEN, b"\r\n"]
        .concat()
        .repeat(events);
    expected.extend_from_slice(b"0\r\n\r\n");
    expected
}

#[test]
fn a_polling_response_source_streams_without_read_futures() {
    for router_settings in [false, true] {
        let (_, body) = serve_tokens(50, true, router_settings);
        assert_eq!(body, token_chunks(50), "router settings {router_settings}");
    }
}

/// Allocations per body event, from the difference between two response
/// lengths, so that per-request costs cancel.
fn allocations_per_event(polling: bool, router_settings: bool) -> usize {
    allocations_per_event_on(polling, router_settings, Serving::Uniplex)
}

fn allocations_per_event_on(polling: bool, router_settings: bool, serving: Serving) -> usize {
    let (short, short_body) = serve_tokens_on(200, polling, router_settings, serving);
    let (long, long_body) = serve_tokens_on(400, polling, router_settings, serving);
    assert_eq!(short_body, token_chunks(200));
    assert_eq!(long_body, token_chunks(400));
    assert_eq!(
        (long - short) % 200,
        0,
        "polling {polling}, router settings {router_settings}: a whole number of \
         allocations per event ({short}, {long})"
    );
    (long - short) / 200
}

/// Polling saves the source's boxed read future and the session's boxed
/// exchange wrapper around it, two heap allocations per body event.
#[test]
#[cfg_attr(
    fbcode_build,
    ignore = "allocation counts cover only the test crate in Buck's shared-library test builds"
)]
fn a_polling_response_source_saves_two_allocations_per_body_event() {
    let boxed = allocations_per_event(false, false);
    let polled = allocations_per_event(true, false);
    assert_eq!(
        boxed - polled,
        2,
        "{boxed} allocations per body event with read futures, {polled} polled"
    );
}

#[test]
fn a_polling_response_source_streams_over_owned_http1() {
    for polling in [false, true] {
        let (_, body) = serve_tokens_on(50, polling, false, Serving::OwnedHttp1);
        assert_eq!(body, token_chunks(50), "polling {polling}");
    }
}

/// On the owned HTTP/1.1 driver, the exchange polls a polling response source
/// through its holder in place (`HttpSourceHolder::poll_body_event_exclusive`),
/// so polling saves the source's boxed read future and the holder read the
/// exchange boxed around it, two heap allocations per body event. The driver's
/// own allocations are not a whole number per event, so the two modes are
/// compared at each length and the saving differenced across lengths. Both
/// saved allocations are made by code compiled into this test crate, so Buck
/// counts them too.
#[test]
fn a_polling_response_source_saves_two_allocations_per_body_event_over_owned_http1() {
    let saved = |events| {
        let (boxed, boxed_body) = serve_tokens_on(events, false, false, Serving::OwnedHttp1);
        let (polled, polled_body) = serve_tokens_on(events, true, false, Serving::OwnedHttp1);
        assert_eq!(boxed_body, token_chunks(events));
        assert_eq!(polled_body, token_chunks(events));
        boxed - polled
    };
    let (short, long) = (saved(200), saved(400));
    assert_eq!(
        long - short,
        400,
        "{short} allocations saved over 200 body events, {long} over 400"
    );
}

/// The owned HTTP/2 driver shares the owned HTTP/1.1 exchange
/// (`handler_application.rs`), so it too polls a polling response source in
/// place and saves both boxed reads, two heap allocations per body event,
/// counted as in the owned HTTP/1.1 test, after a first connection that takes
/// the HTTP/2 path's one-time allocations (about 100 on the first connection a
/// process serves).
#[test]
fn a_polling_response_source_streams_over_owned_http2_without_read_futures() {
    serve_tokens_on(1, false, false, Serving::OwnedHttp2);
    let saved = |events| {
        let (boxed, boxed_body) = serve_tokens_on(events, false, false, Serving::OwnedHttp2);
        let (polled, polled_body) = serve_tokens_on(events, true, false, Serving::OwnedHttp2);
        assert_eq!(boxed_body, TOKEN.repeat(events), "with read futures");
        assert_eq!(polled_body, TOKEN.repeat(events), "polled");
        boxed - polled
    };
    let (short, long) = (saved(200), saved(400));
    assert_eq!(
        long - short,
        400,
        "{short} allocations saved over 200 body events, {long} over 400"
    );
}

/// Batching (`DIV-034`) polls a token stream's source once without waiting
/// before each flush; with the router's settings that attempt must still cost
/// a polling source no allocation, so polling keeps its saving.
#[test]
#[cfg_attr(
    fbcode_build,
    ignore = "allocation counts cover only the test crate in Buck's shared-library test builds"
)]
fn a_polling_response_source_allocates_no_more_per_body_event_with_the_router_settings() {
    let plain = allocations_per_event(true, false);
    let router = allocations_per_event(true, true);
    assert!(
        router <= plain,
        "{router} allocations per body event with the router's settings, {plain} without"
    );
}

/// A fed token response (`crate::http1_egress_batching_tests::Feed`) served
/// only through `poll_body_event`: every poll is `Pending` until the producer
/// pushes.
struct PolledFedBody {
    header: Option<HttpHeaderEvent>,
    feed: Rc<RefCell<Feed>>,
}

impl HttpSource for PolledFedBody {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(self.header.take().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"response header exhausted")
        })))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        panic!("a polling source is never read through a future");
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}

    fn polls_body_events(&self) -> bool {
        true
    }

    fn poll_body_event(
        &mut self,
        context: &mut Context<'_>,
        _maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        self.feed.borrow_mut().poll_token(context)
    }
}

fn polled_fed_body(header: HttpHeaderEvent, feed: Rc<RefCell<Feed>>) -> Box<dyn HttpSource> {
    Box::new(PolledFedBody {
        header: Some(header),
        feed,
    })
}

/// The router's server settings (`abort_exchanges_on_eof`, `non_abortive_close`,
/// timers off).
fn router_session() -> ServerSessionConfig {
    ServerSessionConfig {
        abort_exchanges_on_eof: true,
        non_abortive_close: true,
        ..timers_off()
    }
}

/// With the router's settings, a polling source fed token by token is polled
/// once pending before each flush and once ready after its wake (`DIV-034`).
#[test]
fn a_polling_fed_token_stream_polls_its_source_twice_per_body_event() {
    let (polls, _) = fed_token_costs_per_event(router_session(), polled_fed_body);
    assert_eq!(
        polls, 2,
        "one pending poll before the flush and one ready poll after the wake"
    );
}

/// With the router's server settings, a fed token stream polled in place costs
/// two heap allocations per body event fewer than the same stream read through
/// futures (the source's read future and the session's exchange wrapper), and
/// no source poll more (`DIV-039`).
#[test]
fn a_polling_fed_token_stream_saves_two_allocations_per_body_event_with_the_router_settings() {
    let (boxed_polls, boxed_allocations) =
        fed_token_costs_per_event(router_session(), fed_response);
    let (polled_polls, polled_allocations) =
        fed_token_costs_per_event(router_session(), polled_fed_body);
    assert_eq!(
        (boxed_polls, polled_polls),
        (2, 2),
        "source polls per body event with read futures and polled"
    );
    if COUNTS_EVERY_CRATE {
        assert_eq!(
            boxed_allocations - polled_allocations,
            2,
            "{boxed_allocations} allocations per body event with read futures, {polled_allocations} polled"
        );
    }
}
