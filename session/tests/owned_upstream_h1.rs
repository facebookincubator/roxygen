/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Owned HTTP/1.1 upstream session tests. Each byte comparison runs a `UniplexSession` HTTP/1.1
// client with the same request, the same zero-timeout `UniplexConfig` and the same origin bytes.

use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::task::Context;
use std::task::Wake;
use std::task::Waker;

use roxygen_core::ids::StreamId;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::stream_source::FlowControlState;
use roxygen_runtime::tokio_host::ManagedWorkerError;
use roxygen_runtime::worker::WorkerPollStats;
use thiserror::Error;

use super::*;
use crate::experimental::OwnedRequestProgressExt;
use crate::external_host_tests::after_forced_teardown;
use crate::external_host_tests::forced_teardown_of_scope;
use crate::external_host_tests::panic_text;
use crate::external_host_tests::poll_once;
use crate::external_host_tests::run_on_host;
use crate::external_host_tests::run_on_host_with;
use crate::owned::PanicSite;
use crate::owned::take_contained_panics;
use crate::owned_h1::Http1DriverConfig;
use crate::owned_h1::OwnedH1Options;
use crate::owned_h1::OwnedH1UpstreamReservation;
use crate::owned_h1::OwnedH1UpstreamSession;
use crate::owned_h1::byte_event_refusals;
use crate::owned_h1::cancel_selections;
use crate::owned_h1::codec_parses;
use crate::owned_h1::driver_turns;
use crate::owned_h1::phase_checks;
use crate::owned_h1::read_start_checks;
use crate::owned_h1::response_deliveries;
use crate::owned_h1::response_events;

const OK_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
/// `OK_RESPONSE` on a connection the origin will not keep open.
const CLOSE_RESPONSE: &[u8] =
    b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 2\r\n\r\nok";

/// The configuration both sessions share in every comparison.
fn upstream_config() -> UniplexConfig {
    UniplexConfig {
        read_timeout: Duration::ZERO,
        stream_read_timeout: Duration::ZERO,
        write_timeout: Duration::ZERO,
        drain_request_body_on_drop: false,
        ..UniplexConfig::default()
    }
}

fn upstream_options(context: WorkerContext, transport: Rc<dyn UniplexTransport>) -> OwnedH1Options {
    OwnedH1Options {
        context,
        transport,
        io: upstream_config(),
        http1: Http1DriverConfig::default(),
    }
}

/// A `POST /upload` head, with `Content-Length: length` when `length` is set.
fn post_head(length: Option<usize>, eom: bool) -> HttpHeaderEvent {
    let mut message = roxygen_core::message::HttpMessage::new();
    message.set_method(HttpMethod::Post).unwrap();
    message.set_url(b"/upload", false).unwrap();
    message.set_http_version(1, 1);
    message.headers_mut().set(HttpHeaderCode::Host, b"origin");
    if let Some(length) = length {
        message
            .headers_mut()
            .set(HttpHeaderCode::ContentLength, length.to_string().as_bytes());
    }
    HttpHeaderEvent::new(message, eom).unwrap()
}

fn get_head() -> HttpHeaderEvent {
    let mut message = roxygen_core::message::HttpMessage::new();
    message.set_method(HttpMethod::Get).unwrap();
    message.set_url(b"/", false).unwrap();
    message.set_http_version(1, 1);
    message.headers_mut().set(HttpHeaderCode::Host, b"origin");
    HttpHeaderEvent::new(message, true).unwrap()
}

/// A scripted request: an optional head (a `UniplexSession` client reads it from the source;
/// an owned submission takes it separately), then body events.
struct RequestSource {
    head: Option<HttpHeaderEvent>,
    body: VecDeque<HttpBodyEvent>,
}

impl RequestSource {
    fn held(head: Option<HttpHeaderEvent>, body: Vec<HttpBodyEvent>) -> HttpSourceHolder {
        HttpSourceHolder::new(Box::new(Self {
            head,
            body: body.into(),
        }))
    }
}

impl HttpSource for RequestSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let head = self.head.take().expect("the request head is read once");
        Box::pin(std::future::ready(Ok(head)))
    }

    /// Yields at most `maximum` bytes of the next body event, keeping the rest queued.
    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let maximum = maximum as usize;
        if let Some(front) = self.body.front_mut()
            && let Some(bytes) = front.as_body_mut()
            && bytes.chain_length() > maximum
        {
            let part = bytes.split_at_most(maximum);
            return Box::pin(std::future::ready(Ok(HttpBodyEvent::body(part, false))));
        }
        let event = self.body.pop_front().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"request body exhausted")
        });
        Box::pin(std::future::ready(event))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {
        self.body.clear();
    }
}

/// A chunked request body followed by a trailer section.
fn trailered_request() -> (HttpHeaderEvent, Vec<HttpBodyEvent>) {
    let mut trailers = HttpHeaders::new();
    trailers.add(b"x-checksum", b"abc");
    (
        post_head(None, false),
        vec![
            HttpBodyEvent::body(queue(b"hello"), false),
            HttpBodyEvent::trailers(trailers),
        ],
    )
}

/// The two request shapes the wire comparison covers.
fn scripted_request(chunked: bool) -> (HttpHeaderEvent, Vec<HttpBodyEvent>) {
    if chunked {
        (
            post_head(None, false),
            vec![
                HttpBodyEvent::body(queue(b"hello "), false),
                HttpBodyEvent::body(queue(b"world"), true),
            ],
        )
    } else {
        (
            post_head(Some(11), false),
            vec![HttpBodyEvent::body(queue(b"hello world"), true)],
        )
    }
}

/// What a response source yielded: each head's status, the body, trailers, and EOM.
#[derive(Debug, Default, PartialEq)]
struct ReadResponse {
    statuses: Vec<u16>,
    body: Vec<u8>,
    trailers: Vec<(Vec<u8>, Vec<u8>)>,
    eom: bool,
    error: Option<HttpErrorCode>,
}

/// Reads `source` to its end.
async fn read_response(source: &mut HttpSourceHolder) -> ReadResponse {
    let mut read = ReadResponse::default();
    loop {
        match source.read_header_event().await {
            Ok(mut head) => {
                read.statuses
                    .push(head.headers_mut().status_code().unwrap());
                if head.eom() {
                    read.eom = true;
                    return read;
                }
                if head.is_final().unwrap() {
                    break;
                }
            }
            Err(error) => {
                read.error = Some(error.code());
                return read;
            }
        }
    }
    loop {
        match source.read_body_event(u32::MAX).await {
            Ok(event) => {
                if let Some(bytes) = event.as_body() {
                    let length = bytes.chain_length();
                    read.body
                        .extend_from_slice(&bytes.coalesce(length).unwrap());
                }
                if let HttpBodyEventKind::Trailers(trailers) = event.kind() {
                    read.trailers.extend(
                        trailers
                            .iter()
                            .map(|(_, name, value)| (name.to_vec(), value.to_vec())),
                    );
                }
                if event.eom() {
                    read.eom = true;
                    return read;
                }
            }
            Err(error) => {
                read.error = Some(error.code());
                return read;
            }
        }
    }
}

/// The bytes an owned upstream session writes for one scripted request.
fn owned_request_wire(chunked: bool) -> Vec<u8> {
    owned_wire_for(move || scripted_request(chunked))
}

/// As `owned_request_wire`, for the request `request` builds.
fn owned_wire_for(
    request: impl FnOnce() -> (HttpHeaderEvent, Vec<HttpBodyEvent>) + Send + 'static,
) -> Vec<u8> {
    owned_exchange_wire(request, OK_RESPONSE, b"ok")
}

/// As `owned_wire_for`, with an origin that responds with `response`, whose body is
/// `response_body`.
fn owned_exchange_wire(
    request: impl FnOnce() -> (HttpHeaderEvent, Vec<HttpBodyEvent>) + Send + 'static,
    response: &'static [u8],
    response_body: &'static [u8],
) -> Vec<u8> {
    owned_exchange_writes(request, response, response_body).concat()
}

/// As `owned_exchange_wire`; returns each write.
fn owned_exchange_writes(
    request: impl FnOnce() -> (HttpHeaderEvent, Vec<HttpBodyEvent>) + Send + 'static,
    response: &'static [u8],
    response_body: &'static [u8],
) -> Vec<Vec<u8>> {
    owned_exchange_run(request, response, response_body).0
}

/// As `owned_exchange_writes`; also returns how many times the driver selected the exchange's
/// cancel cause, and how many byte-event refusal errors it built.
fn owned_exchange_run(
    request: impl FnOnce() -> (HttpHeaderEvent, Vec<HttpBodyEvent>) + Send + 'static,
    response: &'static [u8],
    response_body: &'static [u8],
) -> (Vec<Vec<u8>>, usize, usize) {
    run_on_host(move |context| {
        Box::pin(async move {
            let selections = cancel_selections();
            let refusals = byte_event_refusals();
            let gate = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([
                ReadAction::WaitData(Arc::clone(&gate), response.to_vec()),
                ReadAction::Eof,
            ]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let (head, body) = request();
            let submission = handle.reserve_request().unwrap().submit(
                head,
                RequestSource::held(None, body),
                CancellationToken::never(),
            );
            let (run, read) = futures::join!(session.run(), async {
                let (_stream, mut source, _control) = submission.await.unwrap().into_source_parts();
                gate.notify_one();
                read_response(&mut source).await
            });
            run.unwrap();
            assert_eq!(read.body, response_body);
            (
                state.borrow().writes.clone(),
                cancel_selections() - selections,
                byte_event_refusals() - refusals,
            )
        })
    })
}

/// The bytes a `UniplexSession` HTTP/1.1 client writes for the same request.
fn uniplex_request_wire(chunked: bool) -> Vec<u8> {
    uniplex_wire_for(move || scripted_request(chunked))
}

/// As `uniplex_request_wire`, for the request `request` builds.
fn uniplex_wire_for(
    request: impl FnOnce() -> (HttpHeaderEvent, Vec<HttpBodyEvent>) + Send + 'static,
) -> Vec<u8> {
    uniplex_exchange_wire(request, OK_RESPONSE, b"ok")
}

/// As `uniplex_wire_for`, with an origin that responds with `response`, whose body is
/// `response_body`.
fn uniplex_exchange_wire(
    request: impl FnOnce() -> (HttpHeaderEvent, Vec<HttpBodyEvent>) + Send + 'static,
    response: &'static [u8],
    response_body: &'static [u8],
) -> Vec<u8> {
    let (run, read, writes) = uniplex_exchange_outcome(request, response);
    assert_eq!(run, None);
    assert_eq!(read.body, response_body);
    writes
}

/// A `UniplexSession` HTTP/1.1 client's exchange: its run's error code, if it failed, what the
/// response source yielded, and the bytes it wrote.
fn uniplex_exchange_outcome(
    request: impl FnOnce() -> (HttpHeaderEvent, Vec<HttpBodyEvent>) + Send + 'static,
    response: &'static [u8],
) -> (Option<HttpErrorCode>, ReadResponse, Vec<u8>) {
    run_on_host(move |context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([
                ReadAction::WaitData(Arc::clone(&gate), response.to_vec()),
                ReadAction::Eof,
            ]);
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                transport,
                None,
                upstream_config(),
            )
            .unwrap();
            let (head, body) = request();
            let (run, read) = futures::join!(session.run(), async {
                let mut response = handle
                    .send_request(
                        RequestSource::held(Some(head), body),
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap();
                gate.notify_one();
                read_response(&mut response.source).await
            });
            (
                run.err().map(|error| error.code()),
                read,
                state.borrow().writes.concat(),
            )
        })
    })
}

#[test]
fn owned_h1_upstream_writes_what_uniplex_writes() {
    for chunked in [false, true] {
        let owned = owned_request_wire(chunked);
        let uniplex = uniplex_request_wire(chunked);
        assert_eq!(
            String::from_utf8_lossy(&owned),
            String::from_utf8_lossy(&uniplex),
            "chunked: {chunked}"
        );
        assert!(owned.ends_with(if chunked {
            b"0\r\n\r\n"
        } else {
            b"hello world"
        }));
    }
}

#[test]
fn bodyless_requests_match_uniplex() {
    let head = || {
        let mut head = get_head();
        head.headers_mut().set_method(HttpMethod::Head).unwrap();
        head
    };
    let requests: [(&str, fn() -> HttpHeaderEvent, &[u8], &[u8]); 2] = [
        ("GET", get_head, OK_RESPONSE, b"ok"),
        (
            "HEAD",
            head,
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n",
            b"",
        ),
    ];
    for (name, request, response, body) in requests {
        let owned = owned_exchange_wire(move || (request(), Vec::new()), response, body);
        let uniplex = uniplex_exchange_wire(move || (request(), Vec::new()), response, body);
        assert_eq!(
            String::from_utf8_lossy(&owned),
            String::from_utf8_lossy(&uniplex),
            "{name}"
        );
        assert!(
            owned.ends_with(b"\r\n\r\n"),
            "{name}: the head carries the request's end"
        );
    }
}

#[test]
fn capacity_is_one() {
    run_on_host(|context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            let (transport, _state) = ScriptedTransport::new([
                ReadAction::WaitData(Arc::clone(&gate), OK_RESPONSE.to_vec()),
                ReadAction::Eof,
            ]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            assert_eq!(handle.available_capacity(), 1);
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let (run, ()) = futures::join!(session.run(), async {
                let (_stream, mut source, _control) = submission.await.unwrap().into_source_parts();
                assert_eq!(handle.available_capacity(), 0);
                let refused = handle
                    .reserve_request()
                    .err()
                    .expect("the only slot is in flight");
                assert_eq!(refused.code(), HttpErrorCode::REFUSED_STREAM);
                gate.notify_one();
                assert_eq!(read_response(&mut source).await.body, b"ok");
            });
            run.unwrap();
        })
    });
}

/// Yields to the host until `ready` holds, within a bound.
async fn yield_until(mut ready: impl FnMut() -> bool) {
    for _ in 0..1000 {
        if ready() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("the condition did not hold within 1000 yields");
}

/// Closes a reusable connection once its exchange has retired.
async fn close_once_idle(handle: &crate::owned_h1::OwnedH1UpstreamHandle) {
    yield_until(|| handle.is_idle()).await;
    assert!(handle.close_when_idle().unwrap());
}

#[test]
fn response_interest_resolves_at_the_first_head() {
    run_on_host(|context| {
        Box::pin(async move {
            let head_gate = Arc::new(tokio::sync::Notify::new());
            let body_gate = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([
                ReadAction::WaitData(
                    Arc::clone(&head_gate),
                    b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n".to_vec(),
                ),
                ReadAction::WaitData(Arc::clone(&body_gate), b"body".to_vec()),
                ReadAction::Eof,
            ]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let submission = handle
                .reserve_request()
                .unwrap()
                .submit_with_response_interest(
                    get_head(),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                    None,
                );
            let (run, ()) = futures::join!(session.run(), async {
                let mut submission = std::pin::pin!(submission);
                yield_until(|| !state.borrow().writes.is_empty()).await;
                assert!(
                    crate::external_host_tests::poll_once(submission.as_mut()).is_pending(),
                    "commitment alone does not resolve a response-interest submission"
                );
                head_gate.notify_one();
                let response = submission.await.unwrap();
                let receipt = response.source.ingress_completion().unwrap();
                assert!(receipt.snapshot().is_none(), "the body has not arrived");
                let mut source = response.source.into_source_owner().into_source_holder();
                body_gate.notify_one();
                let read = read_response(&mut source).await;
                assert_eq!(read.statuses, [200]);
                assert_eq!(read.body, b"body");
                assert!(read.eom);
            });
            run.unwrap();
        })
    });
}

#[test]
fn response_interest_selects_a_supplied_producer_failure() {
    for supplied in [true, false] {
        run_on_host(move |context| {
            Box::pin(async move {
                let never = Arc::new(tokio::sync::Notify::new());
                let (transport, state) =
                    ScriptedTransport::new([ReadAction::WaitDataOrCancellation(never, Vec::new())]);
                let mut head = post_head(Some(1), false);
                let upload = roxygen_core::stream_source::HttpIngressBody::new(
                    StreamId::new(1),
                    NoUploadCallback,
                    1,
                    context.host().clone(),
                )
                .unwrap();
                assert!(upload.initialize_body_only(head.headers_mut(), false));
                let producer = upload.producer();
                let completion = supplied.then(|| upload.ingress_completion().unwrap());
                let (session, handle) =
                    OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
                let submission = handle
                    .reserve_request()
                    .unwrap()
                    .submit_with_response_interest(
                        head,
                        upload,
                        CancellationToken::never(),
                        completion,
                    );
                let (run, error) = futures::join!(session.run(), async {
                    yield_until(|| !state.borrow().writes.is_empty()).await;
                    assert!(producer.abort(HttpErrorCode::READ_TIMEOUT, b"downstream failed"));
                    match submission.await {
                        Err(error) => error,
                        Ok(response) => response
                            .source
                            .into_source_owner()
                            .into_source_holder()
                            .read_header_event()
                            .await
                            .err()
                            .expect("the failed upload fails the response"),
                    }
                });
                // Only a supplied producer's failure is selected for the request; otherwise the
                // response carries the upload-failure outcome. Either way the connection closes.
                let expected = if supplied {
                    "HTTP source aborted err=READ_TIMEOUT, details=downstream failed"
                } else {
                    "HTTP source aborted err=CORO_CANCELLED, details=egress source failed"
                };
                assert_eq!(
                    String::from_utf8_lossy(error.message()),
                    expected,
                    "supplied: {supplied}"
                );
                assert!(run.is_err());
                assert_eq!(state.borrow().close, CloseKind::Reset);
            })
        });
    }
}

struct NoUploadCallback;
impl roxygen_core::stream_source::HttpStreamSourceCallback for NoUploadCallback {}

/// One GET through an owned upstream session whose origin responds with `response`, released
/// once the request is committed. The origin then sends EOF.
fn owned_get(response: &'static [u8]) -> (ReadResponse, Wire) {
    let (read, wire, _deliveries) = owned_get_reads(vec![response.to_vec()]);
    (read, wire)
}

/// As `owned_get`, with an origin that sends each of `reads` as one read, the first released
/// once the request is committed. Also returns how many response codec callbacks the driver
/// received, and how many of them came through `dyn HttpCodecCallback`.
fn owned_get_reads(reads: Vec<Vec<u8>>) -> (ReadResponse, Wire, (usize, usize)) {
    run_on_host(move |context| {
        Box::pin(async move {
            let before = response_deliveries();
            let gate = Arc::new(tokio::sync::Notify::new());
            let mut reads = reads.into_iter();
            let first = ReadAction::WaitData(Arc::clone(&gate), reads.next().unwrap());
            let (transport, state) = ScriptedTransport::new(
                std::iter::once(first)
                    .chain(reads.map(ReadAction::Data))
                    .chain([ReadAction::Eof]),
            );
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let (run, read) = futures::join!(session.run(), async {
                let (_stream, mut source, _control) = submission.await.unwrap().into_source_parts();
                gate.notify_one();
                read_response(&mut source).await
            });
            let after = response_deliveries();
            let state = state.borrow();
            (
                read,
                Wire {
                    close: state.close,
                    error: run.err().map(|error| error.code()),
                },
                (after.0 - before.0, after.1 - before.1),
            )
        })
    })
}

struct Wire {
    close: CloseKind,
    error: Option<HttpErrorCode>,
}

/// After a read that drained the transport, the driver polls its reader again only once the
/// reader's waker fires. Each read of a response that arrives in three reads is polled once,
/// after one poll that waited for the first bytes.
#[test]
fn owned_h1_upstream_polls_a_drained_reader_once_per_read() {
    let (read, counts) = run_on_host(move |context| {
        Box::pin(async move {
            let (scripted, _state) = ScriptedTransport::new([
                ReadAction::Data(b"HTTP/1.1 200 OK\r\n".to_vec()),
                ReadAction::Data(b"Content-Length: 2\r\n\r\n".to_vec()),
                ReadAction::Data(b"ok".to_vec()),
                ReadAction::Eof,
            ]);
            let (transport, gate) = GatedReaderTransport::new(scripted);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let turns = driver_turns();
            let ((run, read), counts) = GatedReaderTransport::run_gated(&gate, async {
                futures::join!(session.run(), async {
                    let (_stream, mut source, _control) =
                        submission.await.unwrap().into_source_parts();
                    read_response(&mut source).await
                })
            })
            .await;
            run.unwrap();
            (read, (counts, driver_turns() - turns))
        })
    });
    let (counts, turns) = counts;
    assert_eq!(
        counts,
        (5, 4),
        "(polls, reads): four reads, each polled once, after one poll that waited",
    );
    assert_eq!(
        turns, 8,
        "a drained reader makes no turn due; counting its read start as due would add an empty \
         turn after each drained read (11)",
    );
    assert_eq!(read.statuses, [200]);
    assert_eq!(read.body, b"ok");
    assert!(read.eom);
    assert_eq!(read.error, None);
}

/// The driver reads through the transport's reader when it offers one, with the same outcome as
/// when it reads through `read`.
#[test]
fn owned_h1_upstream_reads_through_the_transport_reader() {
    let (direct, direct_wire) = owned_get(OK_RESPONSE);
    let (read, wire, counts) = run_on_host(move |context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            let (scripted, state) = ScriptedTransport::new([
                ReadAction::WaitData(Arc::clone(&gate), OK_RESPONSE.to_vec()),
                ReadAction::Eof,
            ]);
            let (transport, counts) = ReaderTransport::new(scripted);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let (run, read) = futures::join!(session.run(), async {
                let (_stream, mut source, _control) = submission.await.unwrap().into_source_parts();
                gate.notify_one();
                read_response(&mut source).await
            });
            let state = state.borrow();
            (
                read,
                Wire {
                    close: state.close,
                    error: run.err().map(|error| error.code()),
                },
                (counts.direct.get(), counts.reader.get()),
            )
        })
    });
    assert_eq!(
        counts,
        (0, 2),
        "the response and the EOF are read through the reader"
    );
    assert_eq!(read.statuses, direct.statuses);
    assert_eq!(read.body, direct.body);
    assert_eq!(read.eom, direct.eom);
    assert_eq!(read.error, direct.error);
    assert_eq!(wire.close, direct_wire.close);
    assert_eq!(wire.error, direct_wire.error);
}

#[test]
fn chunked_response_trailers_reach_the_reader() {
    let (read, wire) = owned_get(
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\ngrpc-status: 0\r\n\r\n",
    );
    assert_eq!(read.statuses, [200]);
    assert_eq!(read.body, b"hello");
    assert_eq!(read.trailers, [(b"grpc-status".to_vec(), b"0".to_vec())]);
    assert!(read.eom);
    assert_eq!(read.error, None);
    assert_eq!(wire.error, None);
}

#[test]
fn response_without_length_reads_to_close() {
    let (read, wire) = owned_get(b"HTTP/1.1 200 OK\r\nDate: x\r\n\r\nstreamed until close");
    assert_eq!(read.statuses, [200]);
    assert_eq!(read.body, b"streamed until close");
    assert!(read.eom);
    assert_eq!(read.error, None);
    assert_eq!(wire.error, None);
    assert_eq!(wire.close, CloseKind::ShutdownWrite);
}

#[test]
fn eof_inside_a_response_fails_it() {
    // The response's bytes and the EOF are read before the caller reads, so the abort
    // reaches the caller at its first read.
    let (read, wire) = owned_get(b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\npartial");
    assert_eq!(read.error, Some(HttpErrorCode::TRANSPORT_EOF));
    assert!(!read.eom);
    assert_eq!(wire.error, Some(HttpErrorCode::TRANSPORT_EOF));
    assert_eq!(wire.close, CloseKind::Reset);
}

#[test]
fn informational_heads_precede_the_final_head() {
    let (read, wire) = owned_get(
        b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 103 Early Hints\r\nLink: </a>\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok",
    );
    assert_eq!(read.statuses, [100, 103, 200]);
    assert_eq!(read.body, b"ok");
    assert!(read.eom);
    assert_eq!(wire.error, None);
}

/// Records an upstream observer's notifications, in order.
#[derive(Default)]
struct AdmissionLog(RefCell<Vec<String>>);

impl crate::owned::OwnedUpstreamObserver for AdmissionLog {
    fn capacity_released(&self, stream: StreamId) {
        self.0
            .borrow_mut()
            .push(format!("capacity_released({})", stream.get()));
    }

    fn capacity_released_before_delivery(&self) {
        self.0
            .borrow_mut()
            .push("capacity_released_before_delivery".to_owned());
    }

    fn released_submission_retired(&self) {
        self.0
            .borrow_mut()
            .push("released_submission_retired".to_owned());
    }

    fn capacity_available(&self) {
        self.0.borrow_mut().push("capacity_available".to_owned());
    }

    fn admission_closed(&self) {
        self.0.borrow_mut().push("admission_closed".to_owned());
    }
}

#[test]
fn dropped_submission_after_commitment_closes_the_connection() {
    run_on_host(|context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([ReadAction::WaitData(
                Arc::clone(&gate),
                b"HTTP/1.1 200 OK\r\nContent-".to_vec(),
            )]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let log = Rc::new(AdmissionLog::default());
            handle.set_observer(log.clone()).unwrap();
            let submission = handle
                .reserve_request()
                .unwrap()
                .submit_with_response_interest(
                    get_head(),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                    None,
                );
            let (run, ()) = futures::join!(session.run(), async {
                let mut submission = Box::pin(submission);
                yield_until(|| !state.borrow().writes.is_empty()).await;
                gate.notify_one();
                yield_until(|| state.borrow().reads.is_empty()).await;
                assert!(crate::external_host_tests::poll_once(submission.as_mut()).is_pending());
                drop(submission);
            });
            assert_eq!(
                run.expect_err("the cancelled exchange fails the connection")
                    .code(),
                HttpErrorCode::CANCEL
            );
            assert_eq!(state.borrow().close, CloseKind::Reset);
            assert_eq!(
                *log.0.borrow(),
                ["admission_closed", "capacity_released(1)"]
            );
            assert!(handle.is_finished());
        })
    });
}

#[test]
fn cancelled_request_token_mid_response_closes_the_connection() {
    run_on_host(|context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([ReadAction::WaitData(
                Arc::clone(&gate),
                b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nhalf".to_vec(),
            )]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let token = CancellationSource::new();
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                token.token(),
            );
            let (run, read) = futures::join!(session.run(), async {
                let (_stream, mut source, _control) = submission.await.unwrap().into_source_parts();
                gate.notify_one();
                let mut head = source.read_header_event().await.unwrap();
                assert_eq!(head.headers_mut().status_code().unwrap(), 200);
                token.request_cancellation();
                loop {
                    match source.read_body_event(u32::MAX).await {
                        Ok(event) => assert!(!event.eom(), "the response is unfinished"),
                        Err(error) => break error.code(),
                    }
                }
            });
            assert_eq!(read, HttpErrorCode::CORO_CANCELLED);
            assert_eq!(
                run.expect_err("the cancelled exchange fails the connection")
                    .code(),
                HttpErrorCode::CORO_CANCELLED
            );
            assert_eq!(state.borrow().close, CloseKind::Reset);
        })
    });
}

/// A response deadline that fires on its first poll; its completion stops the response
/// producer with the deadline's code, as a caller's route timeout does.
struct FiringDeadline {
    producer: roxygen_core::stream_source::HttpStreamSourceProducer,
    completed: Rc<RefCell<Vec<Result<(), HttpErrorCode>>>>,
}

impl crate::owned::OwnedResponseObserver for FiringDeadline {
    fn poll_deadline(&mut self, _context: &mut std::task::Context<'_>) -> Poll<HttpError> {
        Poll::Ready(HttpError::with_message(
            HttpErrorCode::READ_TIMEOUT,
            b"route timeout",
        ))
    }

    fn response_complete(self: Box<Self>, completed: Result<std::time::Instant, HttpError>) {
        let completed = completed.map(drop).map_err(|error| error.code());
        self.producer.stop_reading(completed.err());
        self.completed.borrow_mut().push(completed);
    }
}

#[test]
fn response_observer_deadline_cancels_and_closes() {
    run_on_host(|context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([ReadAction::WaitData(
                Arc::clone(&gate),
                b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nhalf".to_vec(),
            )]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let completed = Rc::new(RefCell::new(Vec::new()));
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let (run, read) = futures::join!(session.run(), async {
                let response = submission.await.unwrap();
                let producer = response.source.source().unwrap().producer();
                gate.notify_one();
                yield_until(|| state.borrow().reads.is_empty()).await;
                response
                    .control
                    .observe_response(Box::new(FiringDeadline {
                        producer,
                        completed: Rc::clone(&completed),
                    }))
                    .unwrap();
                let (_stream, mut source, _control) = response.into_source_parts();
                read_response(&mut source).await
            });
            assert_eq!(*completed.borrow(), [Err(HttpErrorCode::READ_TIMEOUT)]);
            assert_eq!(read.error, Some(HttpErrorCode::READ_TIMEOUT));
            assert!(!read.eom);
            assert_eq!(
                run.expect_err("the deadline closes the connection").code(),
                HttpErrorCode::READ_TIMEOUT
            );
            assert_eq!(state.borrow().close, CloseKind::Reset);
        })
    });
}

#[test]
fn returned_slot_follows_the_released_submission_order() {
    // (whether the submitter takes the response before its capacity is released)
    for taken_first in [true, false] {
        run_on_host(move |context| {
            Box::pin(async move {
                let gate = Arc::new(tokio::sync::Notify::new());
                // The origin keeps the connection open: only the caller closes it.
                let (transport, state) = ScriptedTransport::new([ReadAction::WaitData(
                    Arc::clone(&gate),
                    OK_RESPONSE.to_vec(),
                )]);
                let (session, handle) =
                    OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
                let log = Rc::new(AdmissionLog::default());
                handle.set_observer(log.clone()).unwrap();
                let submission = handle.reserve_request().unwrap().submit(
                    get_head(),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                let (run, ()) = futures::join!(session.run(), async {
                    let mut submission = Box::pin(submission);
                    yield_until(|| !state.borrow().writes.is_empty()).await;
                    let response = if taken_first {
                        let response = submission.as_mut().await.unwrap();
                        gate.notify_one();
                        yield_until(|| response.control.capacity_released()).await;
                        response
                    } else {
                        gate.notify_one();
                        yield_until(|| !log.0.borrow().is_empty()).await;
                        submission.as_mut().await.unwrap()
                    };
                    assert!(response.control.capacity_released());
                    assert!(!handle.is_idle(), "the response is still unread");
                    assert_eq!(
                        handle.available_capacity(),
                        0,
                        "the slot returns once the response is read"
                    );
                    let (_stream, mut source, _control) = response.into_source_parts();
                    assert_eq!(read_response(&mut source).await.body, b"ok");
                    drop(source);
                    yield_until(|| handle.is_idle()).await;
                    assert_eq!(handle.available_capacity(), 1);
                    assert!(handle.close_when_idle().unwrap());
                });
                run.unwrap();
                let expected: &[&str] = if taken_first {
                    &[
                        "capacity_released(1)",
                        "capacity_available",
                        "admission_closed",
                    ]
                } else {
                    &[
                        "capacity_released_before_delivery",
                        "capacity_released(1)",
                        "released_submission_retired",
                        "capacity_available",
                        "admission_closed",
                    ]
                };
                assert_eq!(*log.0.borrow(), expected, "taken first: {taken_first}");
                assert_eq!(state.borrow().close, CloseKind::ShutdownWrite);
                assert!(handle.is_finished());
            })
        });
    }
}

/// An exchange's slot is returning once only the driver's retirement still holds it. That is
/// when its request is written and its response is read to its end, on a connection that its
/// codec leaves reusable. The driver's next turn then returns the slot, so a pool can wait for
/// it instead of dialing (see `OwnedH1UpstreamHandle::returning_capacity`).
#[test]
fn a_finished_exchange_s_slot_is_returning_until_the_driver_retires_it() {
    // (case, first response, whether the request's write waits until its response is read,
    // `returning_capacity` once the request is committed, once its response is complete and
    // unread, once it is read and before the driver's next turn, and once the driver retired
    // the exchange)
    let cases: [(&str, &'static [u8], bool, [u32; 4]); 3] = [
        ("keep-alive", OK_RESPONSE, false, [0, 0, 1, 0]),
        ("connection close", CLOSE_RESPONSE, false, [0, 0, 0, 0]),
        ("request unwritten", OK_RESPONSE, true, [0, 0, 0, 0]),
    ];
    for (case, response, held_write, expected) in cases {
        let observed = run_on_host(move |context| {
            Box::pin(async move {
                let respond = Arc::new(tokio::sync::Notify::new());
                let second = Arc::new(tokio::sync::Notify::new());
                let (transport, state) =
                    ScriptedTransport::new(reuse_reads(&respond, &second, response, false));
                let write_gate = Rc::new(tokio::sync::Notify::new());
                if held_write {
                    state.borrow_mut().write_gate = Some((b"GET".to_vec(), Rc::clone(&write_gate)));
                }
                let (session, handle) =
                    OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
                let submission = handle.reserve_request().unwrap().submit(
                    get_head(),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                let (run, observed) = futures::join!(session.run(), async {
                    let response = submission.await.unwrap();
                    let committed = handle.returning_capacity();
                    respond.notify_one();
                    yield_until(|| response.control.capacity_released()).await;
                    let unread = handle.returning_capacity();
                    let (_stream, mut source, _control) = response.into_source_parts();
                    assert_eq!(read_response(&mut source).await.body, b"ok");
                    drop(source);
                    // No yield since the read: the driver has not run.
                    let read = handle.returning_capacity();
                    write_gate.notify_one();
                    yield_until(|| handle.is_idle() || handle.is_finished()).await;
                    let retired = handle.returning_capacity();
                    if handle.is_idle() {
                        assert_eq!(handle.available_capacity(), 1, "{case}: the slot returned");
                        assert!(handle.close_when_idle().unwrap());
                    }
                    [committed, unread, read, retired]
                });
                run.unwrap();
                observed
            })
        });
        assert_eq!(observed, expected, "{case}");
    }
}

/// Once its consumer completes it, a finished exchange's slot admits the next request
/// immediately, before the driver turn that the completion woke retires the exchange. An
/// exchange is finished when its request is written and its response read to its end, on a
/// connection that its codec leaves reusable. `returning_capacity` reports that window. A
/// `Connection: close` response's connection does not admit again, and an unwritten request
/// keeps the slot until the driver writes it and retires the exchange.
#[test]
fn a_consumed_exchange_s_slot_admits_the_next_request_before_the_driver_retires_it() {
    // (case, first response, whether the request's write waits until its response is read,
    // `available_capacity` and `returning_capacity` once the request is committed, once its
    // response is complete and unread, once it is read and before the driver's next turn, and
    // once the driver retired the exchange)
    type Observed = [(u32, u32); 4];
    let cases: [(&str, &'static [u8], bool, Observed); 3] = [
        (
            "keep-alive",
            OK_RESPONSE,
            false,
            [(0, 0), (0, 0), (1, 1), (1, 0)],
        ),
        (
            "connection close",
            CLOSE_RESPONSE,
            false,
            [(0, 0), (0, 0), (0, 0), (0, 0)],
        ),
        (
            "request unwritten",
            OK_RESPONSE,
            true,
            [(0, 0), (0, 0), (0, 0), (1, 0)],
        ),
    ];
    for (case, response, held_write, expected) in cases {
        let observed = run_on_host(move |context| {
            Box::pin(async move {
                let respond = Arc::new(tokio::sync::Notify::new());
                let second = Arc::new(tokio::sync::Notify::new());
                let (transport, state) =
                    ScriptedTransport::new(reuse_reads(&respond, &second, response, false));
                let write_gate = Rc::new(tokio::sync::Notify::new());
                if held_write {
                    state.borrow_mut().write_gate = Some((b"GET".to_vec(), Rc::clone(&write_gate)));
                }
                let (session, handle) =
                    OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
                let submission = handle.reserve_request().unwrap().submit(
                    get_head(),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                let observe = || (handle.available_capacity(), handle.returning_capacity());
                let (run, observed) = futures::join!(session.run(), async {
                    let response = submission.await.unwrap();
                    let committed = observe();
                    respond.notify_one();
                    yield_until(|| response.control.capacity_released()).await;
                    let unread = observe();
                    let (_stream, mut source, _control) = response.into_source_parts();
                    assert_eq!(read_response(&mut source).await.body, b"ok");
                    drop(source);
                    // No yield since the read: the driver has not run.
                    let read = observe();
                    write_gate.notify_one();
                    yield_until(|| handle.is_idle() || handle.is_finished()).await;
                    let retired = observe();
                    if handle.is_idle() {
                        assert!(handle.close_when_idle().unwrap());
                    }
                    [committed, unread, read, retired]
                });
                run.unwrap();
                observed
            })
        });
        assert_eq!(observed, expected, "{case}");
    }
}

/// A request reserved and submitted on a consumed exchange's slot before the driver's next turn
/// is committed by that turn once it retires the exchange, and written in the same poll. That
/// retirement does not announce capacity, since the slot is taken. The second exchange's
/// retirement announces it as usual.
#[test]
fn a_request_reserved_on_a_consumed_exchange_s_slot_is_committed_once_the_exchange_retires() {
    run_on_host(|context| {
        Box::pin(async move {
            let respond = Arc::new(tokio::sync::Notify::new());
            let second = Arc::new(tokio::sync::Notify::new());
            let (transport, state) =
                ScriptedTransport::new(reuse_reads(&respond, &second, OK_RESPONSE, false));
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let log = Rc::new(AdmissionLog::default());
            handle.set_observer(log.clone()).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let (run, (bodies, writes_when_reserved, log_when_committed)) =
                futures::join!(session.run(), async {
                    let (_stream, mut source, _control) =
                        submission.await.unwrap().into_source_parts();
                    respond.notify_one();
                    let first = read_response(&mut source).await.body;
                    drop(source);
                    // No yield since the read: the driver has not retired the exchange.
                    let writes = state.borrow().writes.len();
                    let next = handle
                        .reserve_request()
                        .expect("the consumed exchange's slot admits the next request")
                        .submit(
                            get_head(),
                            HttpSourceHolder::empty(),
                            CancellationToken::never(),
                        );
                    let (_stream, mut source, _control) = next.await.unwrap().into_source_parts();
                    let committed = log.0.borrow().clone();
                    second.notify_one();
                    let second = read_response(&mut source).await.body;
                    drop(source);
                    yield_until(|| handle.is_idle()).await;
                    assert!(handle.close_when_idle().unwrap());
                    ([first, second], writes, committed)
                });
            run.unwrap();
            assert_eq!(bodies, [b"ok".to_vec(), b"ok".to_vec()]);
            assert_eq!(
                writes_when_reserved, 1,
                "only the first request was written"
            );
            assert_eq!(state.borrow().writes.concat(), GET_WIRE.repeat(2));
            assert_eq!(
                log_when_committed,
                ["capacity_released(1)"],
                "the retirement of a slot already taken announces nothing"
            );
            assert_eq!(
                *log.0.borrow(),
                [
                    "capacity_released(1)",
                    "capacity_released(2)",
                    "capacity_available",
                    "admission_closed",
                ]
            );
            assert_eq!(state.borrow().close, CloseKind::ShutdownWrite);
        })
    });
}

/// A reservation taken on a consumed exchange's slot and dropped before the driver's next turn
/// does not announce capacity. The retirement then announces the free slot once.
#[test]
fn a_reservation_dropped_on_a_returning_slot_leaves_one_announcement_to_the_retirement() {
    run_on_host(|context| {
        Box::pin(async move {
            let respond = Arc::new(tokio::sync::Notify::new());
            let second = Arc::new(tokio::sync::Notify::new());
            let (transport, state) =
                ScriptedTransport::new(reuse_reads(&respond, &second, OK_RESPONSE, false));
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let log = Rc::new(AdmissionLog::default());
            handle.set_observer(log.clone()).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let (run, (reserved, dropped, log_when_dropped)) =
                futures::join!(session.run(), async {
                    let (_stream, mut source, _control) =
                        submission.await.unwrap().into_source_parts();
                    respond.notify_one();
                    assert_eq!(read_response(&mut source).await.body, b"ok");
                    drop(source);
                    let reservation = handle
                        .reserve_request()
                        .expect("the consumed exchange's slot admits the next request");
                    let reserved = handle.available_capacity();
                    drop(reservation);
                    let dropped = handle.available_capacity();
                    let logged = log.0.borrow().clone();
                    yield_until(|| handle.is_idle()).await;
                    assert!(handle.close_when_idle().unwrap());
                    (reserved, dropped, logged)
                });
            run.unwrap();
            assert_eq!((reserved, dropped), (0, 1));
            assert_eq!(log_when_dropped, ["capacity_released(1)"]);
            assert_eq!(
                *log.0.borrow(),
                [
                    "capacity_released(1)",
                    "capacity_available",
                    "admission_closed"
                ]
            );
            assert_eq!(state.borrow().close, CloseKind::ShutdownWrite);
        })
    });
}

/// A request admitted on a returning slot fails unwritten, with `request_unprocessed` set, when
/// the connection fails before the driver commits it, as a request submitted from
/// `capacity_available` does. Here the failure is response bytes behind the finished response.
#[test]
fn a_request_on_a_returning_slot_is_refused_unprocessed_when_the_connection_fails_first() {
    run_on_host(|context| {
        Box::pin(async move {
            let respond = Arc::new(tokio::sync::Notify::new());
            let mut response = OK_RESPONSE.to_vec();
            response.extend_from_slice(b"HTTP/1.1 200 OK\r\n");
            let (transport, state) =
                ScriptedTransport::new([ReadAction::WaitData(Arc::clone(&respond), response)]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let (run, (error, unprocessed)) = futures::join!(session.run(), async {
                let (_stream, mut source, _control) = submission.await.unwrap().into_source_parts();
                respond.notify_one();
                assert_eq!(read_response(&mut source).await.body, b"ok");
                drop(source);
                let mut next = Box::pin(
                    handle
                        .reserve_request()
                        .expect("the consumed exchange's slot admits the next request")
                        .submit(
                            get_head(),
                            HttpSourceHolder::empty(),
                            CancellationToken::never(),
                        ),
                );
                let error = next.as_mut().await.err().map(|error| error.code());
                (error, next.request_unprocessed())
            });
            assert_eq!(
                run.expect_err("bytes behind the response fail the connection")
                    .code(),
                HttpErrorCode::PROTOCOL_ERROR
            );
            assert_eq!(
                (error, unprocessed),
                (Some(HttpErrorCode::PROTOCOL_ERROR), true)
            );
            assert_eq!(
                state.borrow().writes.concat(),
                GET_WIRE,
                "the second is unwritten"
            );
            assert_eq!(state.borrow().close, CloseKind::Reset);
        })
    });
}

/// The returning hook a pool installs runs once, at the consumer's completion that makes the
/// slot returning, with `returning_capacity` already 1. It does not run for an exchange whose
/// slot does not become returning there: a `Connection: close` response, a request still
/// unwritten at that completion, or a connection cancelled first.
#[test]
fn the_returning_hook_runs_once_at_the_consumer_s_completion() {
    // (case, first response, whether the request's write waits until its response is read,
    // whether the connection is cancelled before the read, and the hook's runs once the request
    // is committed, once its response is complete and unread, once it is read and dropped, and
    // once the driver retired the exchange or the connection finished)
    let cases: [(&str, &'static [u8], bool, bool, [usize; 4]); 4] = [
        ("keep-alive", OK_RESPONSE, false, false, [0, 0, 1, 1]),
        (
            "connection close",
            CLOSE_RESPONSE,
            false,
            false,
            [0, 0, 0, 0],
        ),
        ("request unwritten", OK_RESPONSE, true, false, [0, 0, 0, 0]),
        ("cancelled", OK_RESPONSE, false, true, [0, 0, 0, 0]),
    ];
    for (case, response, held_write, cancelled, expected) in cases {
        let (observed, capacities) = run_on_host(move |context| {
            Box::pin(async move {
                let respond = Arc::new(tokio::sync::Notify::new());
                let second = Arc::new(tokio::sync::Notify::new());
                let (transport, state) =
                    ScriptedTransport::new(reuse_reads(&respond, &second, response, false));
                let write_gate = Rc::new(tokio::sync::Notify::new());
                if held_write {
                    state.borrow_mut().write_gate = Some((b"GET".to_vec(), Rc::clone(&write_gate)));
                }
                let (session, handle) =
                    OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
                // `returning_capacity` in each run; the probe's handle is taken at the end.
                let probe = Rc::new(RefCell::new(Some(handle.clone())));
                let capacities = Rc::new(RefCell::new(Vec::new()));
                handle.set_returning_hook(Rc::new({
                    let (probe, capacities) = (Rc::clone(&probe), Rc::clone(&capacities));
                    move || {
                        let capacity = probe
                            .borrow()
                            .as_ref()
                            .map(|handle| handle.returning_capacity());
                        capacities.borrow_mut().push(capacity);
                    }
                }));
                let submission = handle.reserve_request().unwrap().submit(
                    get_head(),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                let runs = || capacities.borrow().len();
                let (run, observed) = futures::join!(session.run(), async {
                    let response = submission.await.unwrap();
                    let committed = runs();
                    respond.notify_one();
                    yield_until(|| response.control.capacity_released()).await;
                    let unread = runs();
                    let (_stream, mut source, _control) = response.into_source_parts();
                    if cancelled {
                        handle.cancel();
                        yield_until(|| handle.is_finished()).await;
                    } else {
                        assert_eq!(read_response(&mut source).await.body, b"ok");
                    }
                    drop(source);
                    let read = runs();
                    write_gate.notify_one();
                    yield_until(|| handle.is_idle() || handle.is_finished()).await;
                    let retired = runs();
                    if handle.is_idle() {
                        assert!(handle.close_when_idle().unwrap());
                    }
                    [committed, unread, read, retired]
                });
                assert_eq!(run.is_err(), cancelled, "{case}: the run's outcome");
                probe.take();
                let capacities = capacities.take();
                (observed, capacities)
            })
        });
        assert_eq!(observed, expected, "{case}");
        assert!(
            capacities.iter().all(|capacity| *capacity == Some(1)),
            "{case}: the slot is returning when the hook runs: {capacities:?}"
        );
    }
}

/// A reservation taken on a returning slot and dropped before the driver's next turn leaves the
/// slot returning again without announcing capacity, so the returning hook runs again, with
/// `returning_capacity` 1.
#[test]
fn the_returning_hook_runs_again_when_a_reservation_on_the_returning_slot_is_dropped() {
    let (runs, capacities) = run_on_host(|context| {
        Box::pin(async move {
            let respond = Arc::new(tokio::sync::Notify::new());
            let second = Arc::new(tokio::sync::Notify::new());
            let (transport, _state) =
                ScriptedTransport::new(reuse_reads(&respond, &second, OK_RESPONSE, false));
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let probe = Rc::new(RefCell::new(Some(handle.clone())));
            let capacities = Rc::new(RefCell::new(Vec::new()));
            handle.set_returning_hook(Rc::new({
                let (probe, capacities) = (Rc::clone(&probe), Rc::clone(&capacities));
                move || {
                    let capacity = probe
                        .borrow()
                        .as_ref()
                        .map(|handle| handle.returning_capacity());
                    capacities.borrow_mut().push(capacity);
                }
            }));
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let runs = || capacities.borrow().len();
            let (run, observed) = futures::join!(session.run(), async {
                let (_stream, mut source, _control) = submission.await.unwrap().into_source_parts();
                respond.notify_one();
                assert_eq!(read_response(&mut source).await.body, b"ok");
                drop(source);
                let read = runs();
                let reservation = handle
                    .reserve_request()
                    .expect("the consumed exchange's slot admits the next request");
                let reserved = runs();
                drop(reservation);
                let dropped = runs();
                yield_until(|| handle.is_idle()).await;
                let retired = runs();
                assert!(handle.close_when_idle().unwrap());
                [read, reserved, dropped, retired]
            });
            run.unwrap();
            probe.take();
            let capacities = capacities.take();
            (observed, capacities)
        })
    });
    assert_eq!(
        runs,
        [1, 1, 2, 2],
        "the hook's runs after the read, the reservation, its drop and the retirement"
    );
    assert_eq!(capacities, [Some(1), Some(1)]);
}

/// A reservation that takes a returning slot ends its returning capacity, since the driver's
/// next turn then retires the exchange without announcing capacity. Dropped before that turn,
/// it leaves the slot returning again.
#[test]
fn a_reservation_on_a_returning_slot_ends_its_returning_capacity_until_it_is_dropped() {
    let observed = run_on_host(|context| {
        Box::pin(async move {
            let respond = Arc::new(tokio::sync::Notify::new());
            let second = Arc::new(tokio::sync::Notify::new());
            let (transport, _state) =
                ScriptedTransport::new(reuse_reads(&respond, &second, OK_RESPONSE, false));
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let observe = || (handle.available_capacity(), handle.returning_capacity());
            let (run, observed) = futures::join!(session.run(), async {
                let (_stream, mut source, _control) = submission.await.unwrap().into_source_parts();
                respond.notify_one();
                assert_eq!(read_response(&mut source).await.body, b"ok");
                drop(source);
                // No yield since the read: the driver has not retired the exchange.
                let read = observe();
                let reservation = handle
                    .reserve_request()
                    .expect("the consumed exchange's slot admits the next request");
                let reserved = observe();
                drop(reservation);
                let dropped = observe();
                yield_until(|| handle.is_idle()).await;
                let retired = observe();
                assert!(handle.close_when_idle().unwrap());
                [read, reserved, dropped, retired]
            });
            run.unwrap();
            observed
        })
    });
    assert_eq!(
        observed,
        [(1, 1), (0, 0), (1, 1), (1, 0)],
        "(available_capacity, returning_capacity) after the read, the reservation, its drop and \
         the retirement"
    );
}

#[test]
fn close_when_idle_closes_an_idle_connection() {
    run_on_host(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let log = Rc::new(AdmissionLog::default());
            handle.set_observer(log.clone()).unwrap();
            assert!(handle.is_idle());
            let (run, ()) = futures::join!(session.run(), async {
                assert!(handle.close_when_idle().unwrap());
                assert!(!handle.close_when_idle().unwrap());
                assert_eq!(handle.available_capacity(), 0);
                assert_eq!(
                    handle.reserve_request().err().unwrap().code(),
                    HttpErrorCode::REFUSED_STREAM
                );
            });
            run.unwrap();
            assert_eq!(*log.0.borrow(), ["admission_closed"]);
            assert_eq!(state.borrow().close, CloseKind::ShutdownWrite);
            assert!(handle.is_finished());
            assert!(!handle.close_when_idle().unwrap());
        })
    });
}

/// An idle close parks while its outstanding read has yet to observe cancellation, and finishes
/// once that read completes.
#[test]
fn idle_close_parks_until_the_read_observes_cancel() {
    run_on_host_with(|host, context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            // The read ignores cancellation: it completes only when the test releases it.
            let (transport, state) =
                ScriptedTransport::new([ReadAction::WaitData(Arc::clone(&gate), Vec::new())]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let (run, ()) = futures::join!(session.run(), async {
                assert!(handle.close_when_idle().unwrap());
                for _ in 0..10 {
                    tokio::task::yield_now().await;
                }
                let yields = host.yields();
                for _ in 0..1000 {
                    tokio::task::yield_now().await;
                }
                assert_eq!(
                    host.yields(),
                    yields,
                    "the idle close kept polling while its read was outstanding"
                );
                assert!(!handle.is_finished());
                gate.notify_one();
            });
            run.unwrap();
            assert_eq!(state.borrow().close, CloseKind::ShutdownWrite);
            assert!(handle.is_finished());
        })
    });
}

/// Writes and closes through a [`ScriptedTransport`]. Each read observes its cancellation only
/// when polled, and does not arrange a wake for it.
struct PolledCancellationTransport(Rc<ScriptedTransport>);

impl UniplexTransport for PolledCancellationTransport {
    fn read(
        &self,
        _context: &WorkerContext,
        _maximum: usize,
        _allocation_size: usize,
        _timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        Box::pin(std::future::poll_fn(move |_| {
            if cancellation.is_cancellation_requested() {
                Poll::Ready(Err(HttpError::with_message(
                    HttpErrorCode::CORO_CANCELLED,
                    b"read cancelled",
                )))
            } else {
                Poll::Pending
            }
        }))
    }

    fn write(
        &self,
        context: &WorkerContext,
        data: BufQueue,
        flags: ByteEventWriteFlags,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        self.0.write(context, data, flags, timeout, cancellation)
    }

    fn shutdown_write(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.0.shutdown_write(context)
    }

    fn close(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.0.close(context)
    }

    fn close_with_reset(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.0.close_with_reset(context)
    }

    fn byte_event_capability(
        &self,
        context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError> {
        self.0.byte_event_capability(context)
    }

    fn register_byte_event(
        &self,
        context: &WorkerContext,
        registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError> {
        self.0.register_byte_event(context, registration)
    }
}

/// The idle close polls the outstanding read again after requesting its cancellation, so a read
/// that observes cancellation only when polled still completes the close.
#[test]
fn idle_close_polls_the_read_again_after_cancelling_it() {
    run_on_host(|context| {
        Box::pin(async move {
            let (inner, state) = ScriptedTransport::new([]);
            let transport = Rc::new(PolledCancellationTransport(inner));
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let (run, ()) = futures::join!(session.run(), async {
                assert!(handle.close_when_idle().unwrap());
            });
            run.unwrap();
            assert_eq!(state.borrow().close, CloseKind::ShutdownWrite);
            assert!(handle.is_finished());
        })
    });
}

/// Records an upload source's completion.
struct UploadRetirement(Rc<RefCell<Vec<Option<HttpErrorCode>>>>);
impl roxygen_core::stream_source::HttpStreamSourceCallback for UploadRetirement {
    fn source_complete(&mut self, _id: StreamId, error: Option<HttpError>) {
        self.0.borrow_mut().push(error.map(|error| error.code()));
    }
}

#[test]
fn upstream_teardown_is_clean() {
    run_on_host(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([]);
            let mut head = post_head(Some(6), false);
            let retired = Rc::new(RefCell::new(Vec::new()));
            let upload = roxygen_core::stream_source::HttpIngressBody::new(
                StreamId::new(1),
                UploadRetirement(Rc::clone(&retired)),
                16,
                context.host().clone(),
            )
            .unwrap();
            assert!(upload.initialize_body_only(head.headers_mut(), false));
            let producer = upload.producer();
            assert_eq!(
                producer.body(queue(b"abc"), 0, false),
                FlowControlState::Open
            );
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let log = Rc::new(AdmissionLog::default());
            handle.set_observer(log.clone()).unwrap();
            let submission =
                handle
                    .reserve_request()
                    .unwrap()
                    .submit(head, upload, CancellationToken::never());
            let (run, read) = futures::join!(session.run(), async {
                let (_stream, mut source, _control) = submission.await.unwrap().into_source_parts();
                yield_until(|| state.borrow().writes.concat().ends_with(b"abc")).await;
                handle.cancel();
                assert_eq!(handle.available_capacity(), 0);
                read_response(&mut source).await
            });
            assert_eq!(read.error, Some(HttpErrorCode::CORO_CANCELLED));
            assert_eq!(
                run.expect_err("cancel fails the connection").code(),
                HttpErrorCode::CORO_CANCELLED
            );
            assert_eq!(state.borrow().close, CloseKind::Reset);
            assert_eq!(*retired.borrow(), [Some(HttpErrorCode::CORO_CANCELLED)]);
            assert_eq!(
                *log.0.borrow(),
                ["admission_closed", "capacity_released(1)"]
            );
            assert!(handle.is_finished());
            drop(producer);
        })
    });
}

/// A worker whose teardown deadline has passed destroys an owned upstream with a request in
/// flight before its native I/O cleanup joined. The driver still resets the transport, and the
/// worker reports `TeardownDeadlineExceeded` instead of a panic.
#[test]
fn a_forced_worker_teardown_abandons_an_owned_upstream_mid_request() {
    let (inspected_tx, inspected_rx) = std::sync::mpsc::channel();
    let completion = forced_teardown_of_scope(move |context| {
        let (transport, state) = ScriptedTransport::new([]);
        let (session, handle) =
            OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
        let submission = handle.reserve_request().unwrap().submit(
            get_head(),
            HttpSourceHolder::empty(),
            CancellationToken::never(),
        );
        after_forced_teardown(move || {
            let state = state.borrow();
            inspected_tx
                .send((state.close, state.writes.concat()))
                .unwrap();
        });
        Box::pin(async move {
            let _ = futures::join!(session.run(), async move {
                let response = submission.await;
                std::future::pending::<()>().await;
                drop((handle, response));
            });
        })
    });
    assert_eq!(
        completion,
        Err(ManagedWorkerError::TeardownDeadlineExceeded),
        "the forced destruction of the unjoined driver is a clean overrun"
    );
    let (close, writes) = inspected_rx.recv().unwrap();
    assert!(
        writes.starts_with(b"GET / HTTP/1.1\r\n"),
        "the request was in flight: {:?}",
        String::from_utf8_lossy(&writes)
    );
    assert_eq!(
        close,
        CloseKind::Reset,
        "the abandoned driver reset its transport"
    );
}

/// Outside a forced teardown, dropping a started owned upstream before its native I/O cleanup
/// joined still asserts.
#[test]
fn dropping_a_started_owned_upstream_before_its_cleanup_joined_asserts() {
    let message = run_on_host(|context| {
        Box::pin(async move {
            let (transport, _state) = ScriptedTransport::new([]);
            let (session, _handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let mut run = Box::pin(session.run());
            assert!(poll_once(run.as_mut()).is_pending());
            panic_text(catch_unwind(AssertUnwindSafe(|| drop(run))).unwrap_err())
        })
    });
    assert_eq!(
        message,
        "started owned H1 upstream runner dropped before native I/O cleanup joined"
    );
}

/// One chunked response read either through `submit_exclusive`'s fresh source and its
/// exclusive reader, or through `submit` and `into_source_parts`.
fn exclusive_or_parts(exclusive: bool) -> (StreamId, ReadResponse, bool) {
    run_on_host(move |context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            let (transport, _state) = ScriptedTransport::new([ReadAction::WaitData(
                Arc::clone(&gate),
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\n"
                    .to_vec(),
            )]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let reservation = handle.reserve_request().unwrap();
            let (run, outcome) = futures::join!(session.run(), async {
                if exclusive {
                    let response = reservation
                        .submit_exclusive(
                            get_head(),
                            HttpSourceHolder::empty(),
                            CancellationToken::never(),
                        )
                        .await
                        .unwrap();
                    gate.notify_one();
                    let (owner, mut reader) = response.source.split();
                    let mut read = ReadResponse::default();
                    reader.arm_header(&owner).unwrap();
                    let mut head = std::future::poll_fn(|context| reader.poll_header(context))
                        .await
                        .unwrap();
                    read.statuses
                        .push(head.headers_mut().status_code().unwrap());
                    while !read.eom {
                        reader.arm_body(&owner, u32::MAX).unwrap();
                        let event = std::future::poll_fn(|context| reader.poll_body(context))
                            .await
                            .unwrap();
                        if let Some(bytes) = event.as_body() {
                            let length = bytes.chain_length();
                            read.body
                                .extend_from_slice(&bytes.coalesce(length).unwrap());
                        }
                        read.eom = event.eom();
                    }
                    let released = response.control.capacity_released();
                    drop((owner, reader));
                    close_once_idle(&handle).await;
                    (response.stream, read, released)
                } else {
                    let response = reservation
                        .submit(
                            get_head(),
                            HttpSourceHolder::empty(),
                            CancellationToken::never(),
                        )
                        .await
                        .unwrap();
                    gate.notify_one();
                    let (stream, mut source, control) = response.into_source_parts();
                    let read = read_response(&mut source).await;
                    drop(source);
                    close_once_idle(&handle).await;
                    (stream, read, control.capacity_released())
                }
            });
            run.unwrap();
            outcome
        })
    })
}

#[test]
fn submit_exclusive_and_source_parts_match_h2_contracts() {
    let exclusive = exclusive_or_parts(true);
    let parts = exclusive_or_parts(false);
    assert_eq!(exclusive.0, StreamId::new(1));
    assert_eq!(exclusive.0, parts.0);
    assert_eq!(exclusive.1, parts.1);
    assert_eq!(exclusive.1.body, b"abc");
    assert!(exclusive.1.eom);
    assert!(
        exclusive.2 && parts.2,
        "each control observes the released capacity"
    );
}

#[test]
fn owned_h1_upstream_rejects_unsupported_config() {
    type Change = fn(&mut OwnedH1Options);
    let cases: [(&str, Change, ByteEventCapability); 12] = [
        (
            "read_timeout",
            |options| options.io.read_timeout = Duration::from_secs(1),
            ByteEventCapability::Unsupported,
        ),
        (
            "stream_read_timeout",
            |options| options.io.stream_read_timeout = Duration::from_secs(1),
            ByteEventCapability::Unsupported,
        ),
        (
            "write_timeout",
            |options| options.io.write_timeout = Duration::from_secs(1),
            ByteEventCapability::Unsupported,
        ),
        (
            "drain_request_body_on_drop",
            |options| options.io.drain_request_body_on_drop = true,
            ByteEventCapability::Unsupported,
        ),
        (
            "wait_for_http1_response_on_drain",
            |options| options.io.wait_for_http1_response_on_drain = true,
            ByteEventCapability::Unsupported,
        ),
        (
            "abort_exchanges_on_eof",
            |options| options.io.abort_exchanges_on_eof = true,
            ByteEventCapability::Unsupported,
        ),
        (
            "exact_method_tokens",
            |options| options.io.exact_method_tokens = true,
            ByteEventCapability::Unsupported,
        ),
        (
            "byte-event-capable transport",
            |_| {},
            ByteEventCapability::TransportEvents,
        ),
        (
            "zero maximum_write_size",
            |options| options.io.maximum_write_size = 0,
            ByteEventCapability::Unsupported,
        ),
        (
            "maximum_write_size above buffer_limit",
            |options| options.io.maximum_write_size = options.io.buffer_limit + 1,
            ByteEventCapability::Unsupported,
        ),
        (
            "maximum_write_size with no room beside chunk framing",
            |options| options.io.maximum_write_size = 17,
            ByteEventCapability::Unsupported,
        ),
        (
            "a cancelled context",
            |options| {
                assert!(options.context.handle().begin_shutdown());
            },
            ByteEventCapability::Unsupported,
        ),
    ];
    for (name, change, capability) in cases {
        let rejected = run_on_host(move |context| {
            Box::pin(async move {
                let (transport, _state) = ScriptedTransport::with_capability([], capability);
                let mut options = upstream_options(context, transport);
                change(&mut options);
                match OwnedH1UpstreamSession::new(options) {
                    Ok(_) => None,
                    Err(error) => Some(error.code()),
                }
            })
        });
        assert_eq!(
            rejected,
            Some(HttpErrorCode::INVALID_STATE_TRANSITION),
            "{name} is rejected"
        );
    }
}

#[test]
fn request_trailers_match_uniplex() {
    let owned = owned_wire_for(trailered_request);
    let uniplex = uniplex_wire_for(trailered_request);
    assert_eq!(
        String::from_utf8_lossy(&owned),
        String::from_utf8_lossy(&uniplex)
    );
    assert!(
        owned.ends_with(b"0\r\nx-checksum: abc\r\n\r\n"),
        "{:?}",
        String::from_utf8_lossy(&owned)
    );
}

/// A request body with a padding event between two body events.
fn padded_request(chunked: bool) -> (HttpHeaderEvent, Vec<HttpBodyEvent>) {
    (
        post_head((!chunked).then_some(5), false),
        vec![
            HttpBodyEvent::body(queue(b"hel"), false),
            HttpBodyEvent::padding(4, false),
            HttpBodyEvent::body(queue(b"lo"), true),
        ],
    )
}

#[test]
fn request_padding_matches_uniplex() {
    for chunked in [false, true] {
        let owned = owned_wire_for(move || padded_request(chunked));
        let uniplex = uniplex_wire_for(move || padded_request(chunked));
        assert_eq!(
            String::from_utf8_lossy(&owned),
            String::from_utf8_lossy(&uniplex),
            "chunked: {chunked}"
        );
    }
}

/// A request whose last event is padding that carries EOM.
fn padding_eom_request(chunked: bool) -> (HttpHeaderEvent, Vec<HttpBodyEvent>) {
    (
        post_head((!chunked).then_some(5), false),
        vec![
            HttpBodyEvent::body(queue(b"hello"), false),
            HttpBodyEvent::padding(2, true),
        ],
    )
}

#[test]
fn padding_with_eom_ends_the_request() {
    for chunked in [false, true] {
        let owned = owned_wire_for(move || padding_eom_request(chunked));
        let ending: &[u8] = if chunked {
            b"5\r\nhello\r\n0\r\n\r\n"
        } else {
            b"Content-Length: 5\r\n\r\nhello"
        };
        assert!(
            owned.ends_with(ending),
            "chunked: {chunked}: {:?}",
            String::from_utf8_lossy(&owned)
        );
        // Uniplex passes the event to the codec as padding alone, so the codec's message never
        // ends and the exchange fails at EOF.
        let (run, _read, uniplex) =
            uniplex_exchange_outcome(move || padding_eom_request(chunked), OK_RESPONSE);
        assert_eq!(
            run,
            Some(HttpErrorCode::TRANSPORT_EOF),
            "chunked: {chunked}"
        );
        if chunked {
            assert_eq!(
                String::from_utf8_lossy(&uniplex),
                String::from_utf8_lossy(&owned[..owned.len() - 5])
            );
        } else {
            assert_eq!(
                String::from_utf8_lossy(&uniplex),
                String::from_utf8_lossy(&owned)
            );
        }
    }
}

/// Records request byte-event outcomes: `None` if one fired, else its cancel code.
#[derive(Default)]
struct ByteEventLog(RefCell<Vec<Option<HttpErrorCode>>>);

impl HttpByteEventCallback for ByteEventLog {
    fn on_byte_event(&self, _event: HttpByteEvent) {
        self.0.borrow_mut().push(None);
    }

    fn on_byte_event_cancelled(&self, _event: HttpByteEvent, error: HttpError) {
        self.0.borrow_mut().push(Some(error.code()));
    }
}

#[test]
fn request_head_size_is_reported_and_byte_events_are_refused() {
    let (wire, events, sizes) = run_on_host(move |context| {
        Box::pin(async move {
            let log = Rc::new(ByteEventLog::default());
            let encoded = Rc::new(RefCell::new(Vec::new()));
            let gate = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([ReadAction::WaitData(
                Arc::clone(&gate),
                OK_RESPONSE.to_vec(),
            )]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let write = ByteEventType::TransportWrite.bit();
            let (mut head, mut body) = scripted_request(false);
            let sizes = Rc::clone(&encoded);
            head.set_egress_headers_callback(move |size| {
                sizes.borrow_mut().push(size.compressed);
            });
            head.byte_event_registrations_mut()
                .push(HttpByteEventRegistration::new(None, write, &log));
            body[0]
                .byte_event_registrations_mut()
                .push(HttpByteEventRegistration::new(None, write, &log));
            let submission = handle.reserve_request().unwrap().submit(
                head,
                RequestSource::held(None, body),
                CancellationToken::never(),
            );
            let (run, ()) = futures::join!(session.run(), async {
                let (_stream, mut source, _control) = submission.await.unwrap().into_source_parts();
                gate.notify_one();
                assert_eq!(read_response(&mut source).await.body, b"ok");
                drop(source);
                close_once_idle(&handle).await;
            });
            run.unwrap();
            let wire = state.borrow().writes.concat();
            (wire, log.0.take(), encoded.take())
        })
    });
    let head = wire
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("a request head")
        + 4;
    assert_eq!(sizes, vec![head as u32]);
    assert_eq!(
        events,
        vec![Some(HttpErrorCode::INVALID_STATE_TRANSITION); 2],
        "the head's and the body's registrations are each cancelled with the documented error"
    );
}

/// Records request byte-event outcomes: `None` if one fired, else its cancel code and message.
#[derive(Default)]
struct ByteEventCancels(RefCell<Vec<Option<(HttpErrorCode, Vec<u8>)>>>);

impl HttpByteEventCallback for ByteEventCancels {
    fn on_byte_event(&self, _event: HttpByteEvent) {
        self.0.borrow_mut().push(None);
    }

    fn on_byte_event_cancelled(&self, _event: HttpByteEvent, error: HttpError) {
        self.0
            .borrow_mut()
            .push(Some((error.code(), error.message().to_vec())));
    }
}

#[test]
fn byte_event_refusals_are_built_only_for_events_that_register() {
    let (cancels, refusals) = run_on_host(move |context| {
        Box::pin(async move {
            let log = Rc::new(ByteEventCancels::default());
            let gate = Arc::new(tokio::sync::Notify::new());
            let (transport, _state) = ScriptedTransport::new([ReadAction::WaitData(
                Arc::clone(&gate),
                OK_RESPONSE.to_vec(),
            )]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let write = ByteEventType::TransportWrite.bit();
            let (mut head, mut body) = scripted_request(true);
            for _ in 0..2 {
                head.byte_event_registrations_mut()
                    .push(HttpByteEventRegistration::new(None, write, &log));
            }
            body[1]
                .byte_event_registrations_mut()
                .push(HttpByteEventRegistration::new(None, write, &log));
            let refusals = byte_event_refusals();
            let submission = handle.reserve_request().unwrap().submit(
                head,
                RequestSource::held(None, body),
                CancellationToken::never(),
            );
            let (run, ()) = futures::join!(session.run(), async {
                let (_stream, mut source, _control) = submission.await.unwrap().into_source_parts();
                gate.notify_one();
                assert_eq!(read_response(&mut source).await.body, b"ok");
                drop(source);
                close_once_idle(&handle).await;
            });
            run.unwrap();
            (log.0.take(), byte_event_refusals() - refusals)
        })
    });
    let refused = Some((
        HttpErrorCode::INVALID_STATE_TRANSITION,
        b"owned H1 delivers no byte events".to_vec(),
    ));
    assert_eq!(
        cancels,
        vec![refused; 3],
        "the head's two registrations and the last body event's one are each cancelled with the \
         documented error"
    );
    assert_eq!(
        refusals, 2,
        "one refusal error for the head and one for the last body event; the first body event \
         registers nothing"
    );
}

#[test]
fn head_response_declares_a_length_without_a_body() {
    run_on_host(|context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            let (transport, _state) = ScriptedTransport::new([ReadAction::WaitData(
                Arc::clone(&gate),
                b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n".to_vec(),
            )]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let mut head = get_head();
            head.headers_mut().set_method(HttpMethod::Head).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                head,
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let (run, read) = futures::join!(session.run(), async {
                let (_stream, mut source, _control) = submission.await.unwrap().into_source_parts();
                gate.notify_one();
                let read = read_response(&mut source).await;
                drop(source);
                close_once_idle(&handle).await;
                read
            });
            assert_eq!(read.statuses, [200]);
            assert_eq!(read.error, None);
            assert!(read.eom);
            run.unwrap();
        })
    });
}

#[test]
fn response_bytes_without_a_request_fail_the_connection() {
    run_on_host(|context| {
        Box::pin(async move {
            let (transport, state) =
                ScriptedTransport::new([ReadAction::Data(OK_RESPONSE.to_vec())]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let log = Rc::new(AdmissionLog::default());
            handle.set_observer(log.clone()).unwrap();
            let run = session.run().await;
            assert_eq!(
                run.expect_err("an unsolicited response fails the connection")
                    .code(),
                HttpErrorCode::PROTOCOL_ERROR
            );
            assert_eq!(state.borrow().close, CloseKind::Reset);
            assert_eq!(*log.0.borrow(), ["admission_closed"]);
        })
    });
}

#[test]
fn upgrade_requests_are_refused_before_any_byte() {
    let connect = || {
        let mut message = roxygen_core::message::HttpMessage::new();
        message.set_method(HttpMethod::Connect).unwrap();
        message.set_url(b"origin:443", false).unwrap();
        message.set_http_version(1, 1);
        HttpHeaderEvent::new(message, false).unwrap()
    };
    let websocket = || {
        let mut head = get_head();
        head.headers_mut()
            .headers_mut()
            .set(HttpHeaderCode::Upgrade, b"websocket");
        head.headers_mut()
            .headers_mut()
            .set(HttpHeaderCode::Connection, b"Upgrade");
        head
    };
    // A message flagged for an egress websocket upgrade: the codec writes the `Upgrade` and
    // `Sec-WebSocket-Key` lines itself, so the head does not carry an `Upgrade` header.
    let flagged = || {
        let mut head = get_head();
        head.headers_mut().set_egress_websocket_upgrade();
        head
    };
    let heads: [(&str, fn() -> HttpHeaderEvent); 3] = [
        ("CONNECT", connect),
        ("Upgrade", websocket),
        ("websocket flag", flagged),
    ];
    for (name, head) in heads {
        run_on_host(move |context| {
            Box::pin(async move {
                let (transport, state) = ScriptedTransport::new([]);
                let (session, handle) =
                    OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
                let submission = handle.reserve_request().unwrap().submit(
                    head(),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                let (run, refused) = futures::join!(session.run(), async {
                    let refused = submission
                        .await
                        .err()
                        .unwrap_or_else(|| panic!("the {name} request is refused"));
                    assert_eq!(handle.available_capacity(), 1, "the slot returns");
                    assert!(handle.close_when_idle().unwrap());
                    refused
                });
                assert_eq!(refused.code(), HttpErrorCode::HEADER_PARSE_ERROR, "{name}");
                run.unwrap();
                assert!(state.borrow().writes.is_empty(), "{name}");
            })
        });
    }
}

#[test]
fn cancel_before_the_response_is_claimed_fails_the_submission() {
    // (the explicit cause, or the request's cancellation token)
    for explicit in [true, false] {
        run_on_host(move |context| {
            Box::pin(async move {
                let never = Arc::new(tokio::sync::Notify::new());
                let (transport, state) =
                    ScriptedTransport::new([ReadAction::WaitDataOrCancellation(never, Vec::new())]);
                let (session, handle) =
                    OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
                let token = CancellationSource::new();
                let submission = handle.reserve_request().unwrap().submit(
                    get_head(),
                    HttpSourceHolder::empty(),
                    token.token(),
                );
                let (run, claimed) = futures::join!(session.run(), async {
                    let submission = std::pin::pin!(submission);
                    yield_until(|| !state.borrow().writes.is_empty()).await;
                    // The request is committed and its response is ready to claim; the
                    // cancel is selected before the submitter polls again.
                    if explicit {
                        submission.cancel(HttpError::with_message(
                            HttpErrorCode::READ_TIMEOUT,
                            b"route timeout",
                        ));
                    } else {
                        token.request_cancellation();
                    }
                    submission.await.err().map(|error| error.code())
                });
                let expected = if explicit {
                    HttpErrorCode::READ_TIMEOUT
                } else {
                    HttpErrorCode::CORO_CANCELLED
                };
                assert_eq!(claimed, Some(expected), "explicit: {explicit}");
                assert_eq!(
                    run.expect_err("the cancel closes the connection").code(),
                    expected,
                    "explicit: {explicit}"
                );
                assert_eq!(state.borrow().close, CloseKind::Reset);
            })
        });
    }
}

/// The request keeps its first explicit cause: a later cause and a later cancellation of its
/// token change neither the connection's error nor the response's.
#[test]
fn repeated_request_cancellation_preserves_the_first_error() {
    for cancellable in [false, true] {
        run_on_host(move |context| {
            Box::pin(async move {
                let never = Arc::new(tokio::sync::Notify::new());
                let (transport, state) =
                    ScriptedTransport::new([ReadAction::WaitDataOrCancellation(never, Vec::new())]);
                let (session, handle) =
                    OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
                let cancellation = CancellationSource::new();
                let submission = handle.reserve_request().unwrap().submit(
                    get_head(),
                    HttpSourceHolder::empty(),
                    if cancellable {
                        cancellation.token()
                    } else {
                        CancellationToken::never()
                    },
                );
                let (run, read) = futures::join!(session.run(), async {
                    let (_, mut source, control) = submission.await.unwrap().into_source_parts();
                    control.cancel(HttpError::with_message(
                        HttpErrorCode::READ_TIMEOUT,
                        b"first explicit error",
                    ));
                    control.cancel(HttpError::with_message(
                        HttpErrorCode::WRITE_TIMEOUT,
                        b"later explicit error",
                    ));
                    cancellation.request_cancellation();
                    source
                        .read_header_event()
                        .await
                        .err()
                        .expect("cancellation fails the response before its head")
                });
                let run_error = run.expect_err("cancellation resets the connection");
                assert_eq!(
                    run_error.code(),
                    HttpErrorCode::READ_TIMEOUT,
                    "{cancellable}"
                );
                assert_eq!(
                    run_error.message(),
                    b"first explicit error",
                    "{cancellable}"
                );
                assert_eq!(read.code(), HttpErrorCode::READ_TIMEOUT, "{cancellable}");
                assert_eq!(
                    read.message(),
                    b"HTTP source aborted err=READ_TIMEOUT, details=owned H1 upstream exchange cancelled",
                    "{cancellable}"
                );
                assert_eq!(state.borrow().close, CloseKind::Reset);
            })
        });
    }
}

thread_local! {
    static REJECTED_CAUSE_DROP: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
}

/// Clears `REJECTED_CAUSE_DROP` when the test ends, whether or not the callback ran.
struct RejectedCauseDropGuard;

impl Drop for RejectedCauseDropGuard {
    fn drop(&mut self) {
        REJECTED_CAUSE_DROP.with_borrow_mut(Option::take);
    }
}

/// A cause whose destructor runs the thread's `REJECTED_CAUSE_DROP` callback once.
#[derive(Debug, Error)]
#[error("rejected request error cause")]
struct RejectedRequestCause {
    owner: ThreadId,
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl Drop for RejectedRequestCause {
    fn drop(&mut self) {
        assert_eq!(std::thread::current().id(), self.owner);
        self.events.lock().unwrap().push("drop-start");
        let reenter = REJECTED_CAUSE_DROP
            .with_borrow_mut(Option::take)
            .expect("the rejected cause has one destructor callback");
        reenter();
        self.events.lock().unwrap().push("drop-end");
    }
}

/// Records each wake while `recording` is set.
struct RequestErrorWake {
    recording: AtomicBool,
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl Wake for RequestErrorWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.recording.load(Ordering::Acquire) {
            self.events.lock().unwrap().push("wake");
        }
    }
}

/// A cause the request rejects is destroyed before the request wakes the driver, and its
/// destructor may cancel the request again.
#[test]
fn rejected_request_cause_can_reenter_before_the_cancellation_wake() {
    run_on_host(|context| {
        Box::pin(async move {
            let never = Arc::new(tokio::sync::Notify::new());
            let (transport, state) =
                ScriptedTransport::new([ReadAction::WaitDataOrCancellation(never, Vec::new())]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let events = Arc::new(Mutex::new(Vec::new()));
            let wake = Arc::new(RequestErrorWake {
                recording: AtomicBool::new(false),
                events: Arc::clone(&events),
            });
            let waker = Waker::from(Arc::clone(&wake));
            let mut run = std::pin::pin!(session.run());
            assert!(
                run.as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            let (_, mut source, control) = submission.await.unwrap().into_source_parts();
            control.cancel(HttpError::with_message(
                HttpErrorCode::READ_TIMEOUT,
                b"first explicit error",
            ));
            let _drop_guard = RejectedCauseDropGuard;
            let reentrant_control = control.clone();
            let reentrant_events = Arc::clone(&events);
            REJECTED_CAUSE_DROP.with_borrow_mut(|slot| {
                assert!(slot.is_none());
                *slot = Some(Box::new(move || {
                    reentrant_control.cancel(HttpError::new(HttpErrorCode::CANCEL));
                    reentrant_events.lock().unwrap().push("reentered");
                }));
            });
            let rejected = HttpError::new(HttpErrorCode::WRITE_TIMEOUT).with_cause(Arc::new(
                RejectedRequestCause {
                    owner: std::thread::current().id(),
                    events: Arc::clone(&events),
                },
            ));
            wake.recording.store(true, Ordering::Release);
            control.cancel(rejected);
            wake.recording.store(false, Ordering::Release);
            assert_eq!(
                *events.lock().unwrap(),
                ["drop-start", "wake", "reentered", "drop-end", "wake"],
                "the outer cancellation wakes after the rejected cause finishes reentry"
            );
            let run_error = run
                .await
                .expect_err("the first cancellation resets the connection");
            let response_error = source
                .read_header_event()
                .await
                .err()
                .expect("the retained first error reaches the response");
            assert_eq!(run_error.code(), HttpErrorCode::READ_TIMEOUT);
            assert_eq!(run_error.message(), b"first explicit error");
            assert_eq!(response_error.code(), HttpErrorCode::READ_TIMEOUT);
            assert_eq!(
                response_error.message(),
                b"HTTP source aborted err=READ_TIMEOUT, details=owned H1 upstream exchange cancelled"
            );
            assert_eq!(state.borrow().close, CloseKind::Reset);
            assert!(REJECTED_CAUSE_DROP.with_borrow(|slot| slot.is_none()));
        })
    });
}

/// A write queue whose writes complete immediately. Its capacity check runs `hook` once, its
/// first completion runs `complete_hook` once, and its idle check runs `idle_hook` once.
struct HookedQueue {
    state: Rc<RefCell<TransportState>>,
    hook: Rc<RefCell<Option<Box<dyn Fn()>>>>,
    complete_hook: Rc<RefCell<Option<Box<dyn Fn()>>>>,
    idle_hook: Rc<RefCell<Option<Box<dyn Fn()>>>>,
    receipts: VecDeque<usize>,
}

impl crate::uniplex::UniplexWriteQueue for HookedQueue {
    fn can_write(&self, _bytes: usize) -> bool {
        let hook = self.hook.borrow_mut().take();
        if let Some(hook) = hook {
            hook();
        }
        true
    }

    fn start_write(&mut self, data: BufQueue) -> Result<(), HttpError> {
        let length = data.chain_length();
        let bytes = data.coalesce(length).unwrap().to_vec();
        self.state.borrow_mut().writes.push(bytes);
        self.receipts.push_back(length);
        Ok(())
    }

    fn poll_complete(
        &mut self,
        _context: &mut std::task::Context<'_>,
    ) -> Poll<crate::uniplex::UniplexWriteCompletion> {
        match self.receipts.pop_front() {
            Some(requested_bytes) => {
                let hook = self.complete_hook.borrow_mut().take();
                if let Some(hook) = hook {
                    hook();
                }
                Poll::Ready(crate::uniplex::UniplexWriteCompletion {
                    requested_bytes,
                    result: Ok(WriteInfo {
                        bytes_written: requested_bytes,
                    }),
                })
            }
            None => Poll::Pending,
        }
    }

    fn is_idle(&self) -> bool {
        let hook = self.idle_hook.borrow_mut().take();
        if let Some(hook) = hook {
            hook();
        }
        self.receipts.is_empty()
    }
}

/// Reads and closes through a [`ScriptedTransport`]; writes go through a [`HookedQueue`].
struct QueuedTransport {
    inner: Rc<ScriptedTransport>,
    state: Rc<RefCell<TransportState>>,
    hook: Rc<RefCell<Option<Box<dyn Fn()>>>>,
    complete_hook: Rc<RefCell<Option<Box<dyn Fn()>>>>,
    idle_hook: Rc<RefCell<Option<Box<dyn Fn()>>>>,
}

impl UniplexTransport for QueuedTransport {
    fn try_write_queue(
        &self,
        _context: &WorkerContext,
        _maximum_write_size: usize,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> Result<Option<Box<dyn crate::uniplex::UniplexWriteQueue>>, HttpError> {
        Ok(Some(Box::new(HookedQueue {
            state: Rc::clone(&self.state),
            hook: Rc::clone(&self.hook),
            complete_hook: Rc::clone(&self.complete_hook),
            idle_hook: Rc::clone(&self.idle_hook),
            receipts: VecDeque::new(),
        })))
    }

    fn read(
        &self,
        context: &WorkerContext,
        maximum: usize,
        allocation_size: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        self.inner
            .read(context, maximum, allocation_size, timeout, cancellation)
    }

    fn write(
        &self,
        _context: &WorkerContext,
        _data: BufQueue,
        _flags: ByteEventWriteFlags,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        unreachable!("every write goes through the write queue")
    }

    fn shutdown_write(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.inner.shutdown_write(context)
    }

    fn close(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.inner.close(context)
    }

    fn close_with_reset(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.inner.close_with_reset(context)
    }

    fn byte_event_capability(
        &self,
        context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError> {
        self.inner.byte_event_capability(context)
    }

    fn register_byte_event(
        &self,
        context: &WorkerContext,
        registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError> {
        self.inner.register_byte_event(context, registration)
    }
}

#[test]
fn cancel_from_the_write_queue_capacity_check_writes_nothing() {
    run_on_host(|context| {
        Box::pin(async move {
            let never = Arc::new(tokio::sync::Notify::new());
            let (inner, state) =
                ScriptedTransport::new([ReadAction::WaitDataOrCancellation(never, Vec::new())]);
            let hook = Rc::new(RefCell::new(None));
            let transport = Rc::new(QueuedTransport {
                inner,
                state: Rc::clone(&state),
                hook: Rc::clone(&hook),
                complete_hook: Rc::default(),
                idle_hook: Rc::default(),
            });
            // The request head is longer than one write, so the turn that commits it also
            // writes it, and its first capacity check is the one that cancels.
            let mut options = upstream_options(context, transport);
            options.io.maximum_write_size = 24;
            let (session, handle) = OwnedH1UpstreamSession::new(options).unwrap();
            let canceller = handle.clone();
            *hook.borrow_mut() = Some(Box::new(move || canceller.cancel()) as Box<dyn Fn()>);
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let (run, read) = futures::join!(session.run(), async {
                match submission.await {
                    Ok(response) => {
                        let (_stream, mut source, _control) = response.into_source_parts();
                        read_response(&mut source).await.error
                    }
                    Err(error) => Some(error.code()),
                }
            });
            assert!(hook.borrow().is_none(), "the capacity check ran");
            assert!(
                state.borrow().writes.is_empty(),
                "nothing is written after the cancel: {:?}",
                state.borrow().writes
            );
            assert_eq!(read, Some(HttpErrorCode::CORO_CANCELLED));
            assert_eq!(
                run.expect_err("cancel fails the connection").code(),
                HttpErrorCode::CORO_CANCELLED
            );
            assert_eq!(state.borrow().close, CloseKind::Reset);
        })
    });
}

#[test]
fn returned_slot_leaves_the_connection_idle() {
    // (how the slot returns uncommitted)
    for case in ["dropped reservation", "refused upgrade", "cancelled token"] {
        run_on_host(move |context| {
            Box::pin(async move {
                let (transport, state) = ScriptedTransport::new([]);
                let (session, handle) =
                    OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
                let (run, ()) = futures::join!(session.run(), async {
                    let reservation = handle.reserve_request().unwrap();
                    assert!(!handle.is_idle(), "{case}: the slot is taken");
                    match case {
                        "dropped reservation" => drop(reservation),
                        "refused upgrade" => {
                            let mut head = get_head();
                            head.headers_mut()
                                .headers_mut()
                                .set(HttpHeaderCode::Upgrade, b"websocket");
                            let submission = reservation.submit(
                                head,
                                HttpSourceHolder::empty(),
                                CancellationToken::never(),
                            );
                            assert!(submission.await.is_err(), "{case}");
                        }
                        _ => {
                            let token = CancellationSource::new();
                            token.request_cancellation();
                            let submission = reservation.submit(
                                get_head(),
                                HttpSourceHolder::empty(),
                                token.token(),
                            );
                            assert!(submission.await.is_err(), "{case}");
                        }
                    }
                    assert_eq!(handle.available_capacity(), 1, "{case}: the slot returns");
                    yield_until(|| handle.is_idle()).await;
                    assert!(handle.close_when_idle().unwrap());
                });
                run.unwrap();
                assert!(state.borrow().writes.is_empty(), "{case}");
            })
        });
    }
}

fn patterned(length: usize) -> Vec<u8> {
    (0..length).map(|index| (index % 251) as u8).collect()
}

fn large_request() -> (HttpHeaderEvent, Vec<HttpBodyEvent>) {
    let body = patterned(256 << 10);
    let events = body
        .chunks(48 << 10)
        .enumerate()
        .map(|(index, chunk)| {
            HttpBodyEvent::body(queue(chunk), (index + 1) * (48 << 10) >= body.len())
        })
        .collect();
    (post_head(Some(body.len()), false), events)
}

#[test]
fn large_upload_matches_uniplex() {
    let owned = owned_wire_for(large_request);
    let uniplex = uniplex_wire_for(large_request);
    assert_eq!(owned.len(), uniplex.len());
    assert!(
        owned == uniplex,
        "the owned request bytes equal the Uniplex client's"
    );
    assert!(owned.ends_with(&patterned(256 << 10)));
}

#[test]
fn large_response_body_pauses_parsing_until_read() {
    let body = patterned(256 << 10);
    let mut response =
        format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).into_bytes();
    response.extend_from_slice(&body);
    let read = run_on_host(move |context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            let mut chunks = response.chunks(16 << 10).map(<[u8]>::to_vec);
            let first = chunks.next().unwrap();
            let reads: Vec<ReadAction> =
                std::iter::once(ReadAction::WaitData(Arc::clone(&gate), first))
                    .chain(chunks.map(ReadAction::Data))
                    .collect();
            let (transport, _state) = ScriptedTransport::new(reads);
            let mut options = upstream_options(context, transport);
            options.io.ingress_buffer_limit = 16 << 10;
            let (session, handle) = OwnedH1UpstreamSession::new(options).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let (run, read) = futures::join!(session.run(), async {
                let (_stream, mut source, _control) = submission.await.unwrap().into_source_parts();
                gate.notify_one();
                let mut head = source.read_header_event().await.unwrap();
                assert_eq!(head.headers_mut().status_code().unwrap(), 200);
                let mut body = Vec::new();
                let body = loop {
                    // A slow reader: the driver must stop parsing at the ingress limit and
                    // resume as the reader drains.
                    tokio::task::yield_now().await;
                    let event = source.read_body_event(4 << 10).await.unwrap();
                    if let Some(bytes) = event.as_body() {
                        let length = bytes.chain_length();
                        body.extend_from_slice(&bytes.coalesce(length).unwrap());
                    }
                    if event.eom() {
                        break body;
                    }
                };
                drop(source);
                close_once_idle(&handle).await;
                body
            });
            run.unwrap();
            read
        })
    });
    assert!(
        read == patterned(256 << 10),
        "the response body arrives intact"
    );
}

struct PanickingObserver;

impl crate::owned::OwnedUpstreamObserver for PanickingObserver {
    fn capacity_released(&self, _stream: StreamId) {
        panic!("observer panicked");
    }
}

/// The contained panics `worker` counted since `start`, by scope: exchange, connection, driver.
fn contained_panic_counts(worker: &WorkerHandle, start: &WorkerPollStats) -> (u64, u64, u64) {
    let delta = worker.poll_stats().since(start);
    (
        delta.contained_exchange_panics,
        delta.contained_connection_panics,
        delta.contained_driver_panics,
    )
}

/// The observer's panic while the driver polls is counted at connection scope and drains the
/// connection. Admission closes with `REFUSED_STREAM`, the exchange in flight completes, and
/// the run ends `Ok` once the connection is idle. The drain does not record a caller drain
/// request, so a later `close_when_idle` still reports a change.
#[cfg(panic = "unwind")]
#[test]
fn an_upstream_observer_panic_drains_the_connection() {
    run_on_host(|context| {
        Box::pin(async move {
            take_contained_panics();
            let worker = context.handle().clone();
            let start = worker.poll_stats();
            let gate = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([ReadAction::WaitData(
                Arc::clone(&gate),
                OK_RESPONSE.to_vec(),
            )]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            handle.set_observer(Rc::new(PanickingObserver)).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let (run, (read, refused, changed)) = futures::join!(session.run(), async {
                let (_stream, mut source, _control) = submission.await.unwrap().into_source_parts();
                gate.notify_one();
                let read = read_response(&mut source).await;
                let refused = handle.reserve_request().err().map(|error| error.code());
                let changed = handle.close_when_idle().unwrap();
                drop(source);
                (read, refused, changed)
            });
            run.unwrap();
            assert_eq!((read.body.as_slice(), read.eom), (b"ok".as_slice(), true));
            assert_eq!(refused, Some(HttpErrorCode::REFUSED_STREAM));
            assert!(changed, "the drain is not the caller's");
            assert_eq!(state.borrow().close, CloseKind::ShutdownWrite);
            assert!(handle.is_finished());
            assert_eq!(
                take_contained_panics(),
                [(
                    PanicSite::UpstreamObserver,
                    Some(StreamId::new(1)),
                    Some("observer panicked".to_owned())
                )]
            );
            assert_eq!(contained_panic_counts(&worker, &start), (0, 1, 0));
        })
    });
}

/// A request body whose one event is released by `gate`; its read runs `cancel` as it returns
/// the event.
struct GatedUpload {
    gate: Arc<tokio::sync::Notify>,
    cancel: Option<Box<dyn FnOnce()>>,
    body: Option<HttpBodyEvent>,
}

impl GatedUpload {
    fn held(
        gate: &Arc<tokio::sync::Notify>,
        cancel: Option<Box<dyn FnOnce()>>,
    ) -> HttpSourceHolder {
        HttpSourceHolder::new(Box::new(Self {
            gate: Arc::clone(gate),
            cancel,
            body: Some(HttpBodyEvent::body(queue(b"hello"), true)),
        }))
    }
}

impl HttpSource for GatedUpload {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        unreachable!("an owned submission takes the head separately")
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let gate = Arc::clone(&self.gate);
        let cancel = self.cancel.take();
        let body = self.body.take();
        Box::pin(async move {
            gate.notified().await;
            if let Some(cancel) = cancel {
                cancel();
            }
            body.ok_or_else(|| {
                HttpError::with_message(HttpErrorCode::DROPPED, b"request body exhausted")
            })
        })
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {
        self.body = None;
    }
}

/// A request body read that panics fails its request as a failed upload does. The response
/// fails with `CORO_CANCELLED` ("egress source failed"), the connection resets, the run returns
/// `Err(INTERNAL_ERROR)`, and the panic is counted once, at exchange scope.
#[cfg(panic = "unwind")]
#[test]
fn owned_h1_upstream_upload_panic_fails_the_request_and_resets() {
    run_on_host(|context| {
        Box::pin(async move {
            // Discards reports left on this thread by an earlier test.
            take_contained_panics();
            let worker = context.handle().clone();
            let start = worker.poll_stats();
            let upload_gate = Arc::new(tokio::sync::Notify::new());
            upload_gate.notify_one();
            let (transport, state) = ScriptedTransport::new([ReadAction::WaitDataOrCancellation(
                Arc::new(tokio::sync::Notify::new()),
                OK_RESPONSE.to_vec(),
            )]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                post_head(Some(5), false),
                GatedUpload::held(
                    &upload_gate,
                    Some(Box::new(|| panic!("request body panicked"))),
                ),
                CancellationToken::never(),
            );
            let (run, read) = futures::join!(session.run(), async {
                let (_stream, mut source, _control) = submission.await.unwrap().into_source_parts();
                read_response(&mut source).await.error
            });
            assert_eq!(read, Some(HttpErrorCode::CORO_CANCELLED));
            assert_eq!(
                run.err().map(|error| error.code()),
                Some(HttpErrorCode::INTERNAL_ERROR)
            );
            assert_eq!(state.borrow().close, CloseKind::Reset);
            assert!(handle.is_finished());
            assert_eq!(
                take_contained_panics(),
                [(
                    PanicSite::UploadBody,
                    Some(StreamId::new(1)),
                    Some("request body panicked".to_owned())
                )]
            );
            assert_eq!(contained_panic_counts(&worker, &start), (1, 0, 0));
        })
    });
}

/// A response observer whose completion cancels its request and releases the request body.
struct CancelOnComplete {
    control: crate::owned_h1::OwnedH1UpstreamRequest,
    upload: Arc<tokio::sync::Notify>,
}

impl crate::owned::OwnedResponseObserver for CancelOnComplete {
    fn poll_deadline(&mut self, _context: &mut std::task::Context<'_>) -> Poll<HttpError> {
        Poll::Pending
    }

    fn response_complete(self: Box<Self>, _completed: Result<std::time::Instant, HttpError>) {
        self.control.cancel(HttpError::with_message(
            HttpErrorCode::READ_TIMEOUT,
            b"route timeout",
        ));
        self.upload.notify_one();
    }
}

#[test]
fn cancel_from_the_response_observer_serializes_no_more_request() {
    run_on_host(|context| {
        Box::pin(async move {
            let response_gate = Arc::new(tokio::sync::Notify::new());
            let upload_gate = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([ReadAction::WaitData(
                Arc::clone(&response_gate),
                OK_RESPONSE.to_vec(),
            )]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                post_head(Some(5), false),
                GatedUpload::held(&upload_gate, None),
                CancellationToken::never(),
            );
            let (run, ()) = futures::join!(session.run(), async {
                let response = submission.await.unwrap();
                // The early response ends while the request body is still unread; the
                // observer's completion cancels the request in the same driver turn as the
                // body becomes readable.
                response
                    .control
                    .observe_response(Box::new(CancelOnComplete {
                        control: response.control.clone(),
                        upload: Arc::clone(&upload_gate),
                    }))
                    .unwrap();
                response_gate.notify_one();
                let (_stream, mut source, _control) = response.into_source_parts();
                read_response(&mut source).await;
            });
            let writes = state.borrow().writes.concat();
            assert!(
                !writes.ends_with(b"hello"),
                "no request byte is serialized after the cancel: {:?}",
                String::from_utf8_lossy(&writes)
            );
            assert_eq!(
                run.expect_err("the cancel closes the connection").code(),
                HttpErrorCode::READ_TIMEOUT
            );
            assert_eq!(state.borrow().close, CloseKind::Reset);
        })
    });
}

#[test]
fn cancel_from_the_request_body_read_serializes_no_more_request() {
    run_on_host(|context| {
        Box::pin(async move {
            let response_gate = Arc::new(tokio::sync::Notify::new());
            let upload_gate = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([ReadAction::WaitData(
                Arc::clone(&response_gate),
                OK_RESPONSE.to_vec(),
            )]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            // A token cancelled during the read would already fail the transfer, so the read
            // cancels through the request handle, which the driver itself must observe.
            let control = Rc::new(RefCell::new(
                None::<crate::owned_h1::OwnedH1UpstreamRequest>,
            ));
            let canceller = Rc::clone(&control);
            let cancel = Box::new(move || {
                canceller
                    .borrow()
                    .as_ref()
                    .expect("the response was claimed")
                    .cancel(HttpError::with_message(
                        HttpErrorCode::READ_TIMEOUT,
                        b"route timeout",
                    ));
            }) as Box<dyn FnOnce()>;
            let submission = handle.reserve_request().unwrap().submit(
                post_head(Some(5), false),
                GatedUpload::held(&upload_gate, Some(cancel)),
                CancellationToken::never(),
            );
            let (run, ()) = futures::join!(session.run(), async {
                let (_stream, mut source, request) = submission.await.unwrap().into_source_parts();
                *control.borrow_mut() = Some(request);
                // The early response is read to its end; then the request body's read cancels
                // the request as it returns the body's last bytes.
                response_gate.notify_one();
                assert!(read_response(&mut source).await.eom);
                upload_gate.notify_one();
            });
            let writes = state.borrow().writes.concat();
            assert!(
                !writes.ends_with(b"hello"),
                "no request byte is serialized after the cancel: {:?}",
                String::from_utf8_lossy(&writes)
            );
            assert_eq!(
                run.expect_err("the cancel closes the connection").code(),
                HttpErrorCode::READ_TIMEOUT
            );
            assert_eq!(state.borrow().close, CloseKind::Reset);
            control.borrow_mut().take();
        })
    });
}

#[test]
fn cancel_from_a_failing_request_body_read_selects_the_cancel() {
    run_on_host(|context| {
        Box::pin(async move {
            let response_gate = Arc::new(tokio::sync::Notify::new());
            let upload_gate = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([ReadAction::WaitData(
                Arc::clone(&response_gate),
                OK_RESPONSE.to_vec(),
            )]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let control = Rc::new(RefCell::new(
                None::<crate::owned_h1::OwnedH1UpstreamRequest>,
            ));
            let canceller = Rc::clone(&control);
            let cancel = Box::new(move || {
                canceller
                    .borrow()
                    .as_ref()
                    .expect("the response was claimed")
                    .cancel(HttpError::with_message(
                        HttpErrorCode::READ_TIMEOUT,
                        b"route timeout",
                    ));
            }) as Box<dyn FnOnce()>;
            // The read cancels the request and then fails with its own error.
            let upload = HttpSourceHolder::new(Box::new(GatedUpload {
                gate: Arc::clone(&upload_gate),
                cancel: Some(cancel),
                body: None,
            }));
            let submission = handle.reserve_request().unwrap().submit(
                post_head(Some(5), false),
                upload,
                CancellationToken::never(),
            );
            let (run, ()) = futures::join!(session.run(), async {
                let (_stream, mut source, request) = submission.await.unwrap().into_source_parts();
                *control.borrow_mut() = Some(request);
                response_gate.notify_one();
                assert!(read_response(&mut source).await.eom);
                upload_gate.notify_one();
            });
            assert_eq!(
                run.expect_err("the cancel closes the connection").code(),
                HttpErrorCode::READ_TIMEOUT,
                "the first cause, not the source's later failure, closes the connection"
            );
            assert_eq!(state.borrow().close, CloseKind::Reset);
            control.borrow_mut().take();
        })
    });
}

#[test]
fn request_cancel_from_the_write_queue_capacity_check_writes_nothing() {
    run_on_host(|context| {
        Box::pin(async move {
            let never = Arc::new(tokio::sync::Notify::new());
            let (inner, state) =
                ScriptedTransport::new([ReadAction::WaitDataOrCancellation(never, Vec::new())]);
            let hook = Rc::new(RefCell::new(None));
            let transport = Rc::new(QueuedTransport {
                inner,
                state: Rc::clone(&state),
                hook: Rc::clone(&hook),
                complete_hook: Rc::default(),
                idle_hook: Rc::default(),
            });
            // As `cancel_from_the_write_queue_capacity_check_writes_nothing`, with the request's
            // own token cancelled instead of the connection.
            let mut options = upstream_options(context, transport);
            options.io.maximum_write_size = 24;
            let (session, handle) = OwnedH1UpstreamSession::new(options).unwrap();
            let token = Rc::new(CancellationSource::new());
            let canceller = Rc::clone(&token);
            *hook.borrow_mut() = Some(Box::new(move || {
                canceller.request_cancellation();
            }) as Box<dyn Fn()>);
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                token.token(),
            );
            let (run, read) = futures::join!(session.run(), async {
                match submission.await {
                    Ok(response) => {
                        let (_stream, mut source, _control) = response.into_source_parts();
                        read_response(&mut source).await.error
                    }
                    Err(error) => Some(error.code()),
                }
            });
            assert!(hook.borrow().is_none(), "the capacity check ran");
            assert!(
                state.borrow().writes.is_empty(),
                "nothing is written after the cancel: {:?}",
                state.borrow().writes
            );
            assert_eq!(read, Some(HttpErrorCode::CORO_CANCELLED));
            assert_eq!(
                run.expect_err("the cancel fails the connection").code(),
                HttpErrorCode::CORO_CANCELLED
            );
            assert_eq!(state.borrow().close, CloseKind::Reset);
        })
    });
}

#[test]
fn bytes_behind_the_response_fail_the_connection_at_eof() {
    // The response is complete; the bytes behind it arrive with it, and EOF follows.
    let (read, wire) =
        owned_get(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nokHTTP/1.1 200 OK\r\n");
    assert_eq!(read.body, b"ok");
    assert!(read.eom);
    assert_eq!(wire.error, Some(HttpErrorCode::PROTOCOL_ERROR));
    assert_eq!(wire.close, CloseKind::Reset);
}

/// A `TRACE` request with a `Content-Length` body.
fn trace_request() -> (HttpHeaderEvent, Vec<HttpBodyEvent>) {
    let mut head = post_head(Some(5), false);
    head.headers_mut().set_method(HttpMethod::Trace).unwrap();
    (head, vec![HttpBodyEvent::body(queue(b"hello"), true)])
}

#[test]
fn trace_request_with_a_body_matches_uniplex() {
    let owned = owned_wire_for(trace_request);
    let uniplex = uniplex_wire_for(trace_request);
    assert_eq!(
        String::from_utf8_lossy(&owned),
        String::from_utf8_lossy(&uniplex)
    );
    assert!(owned.starts_with(b"TRACE /upload HTTP/1.1\r\n"));
    assert!(owned.ends_with(b"Content-Length: 5\r\n\r\nhello"));
}

/// What one owned exchange produced: `run`'s error code, the submission's or response's error
/// code, the bytes written, and how the connection closed. A rejected submission closes the
/// idle connection before the origin responds, which cancels the read waiting for it.
struct OwnedOutcome {
    run: Option<HttpErrorCode>,
    request: Option<HttpErrorCode>,
    wire: Vec<u8>,
    close: CloseKind,
}

fn owned_exchange_outcome(
    request: impl FnOnce() -> (HttpHeaderEvent, Vec<HttpBodyEvent>) + Send + 'static,
) -> OwnedOutcome {
    run_on_host(move |context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([
                ReadAction::WaitDataOrCancellation(Arc::clone(&gate), OK_RESPONSE.to_vec()),
                ReadAction::Eof,
            ]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let (head, body) = request();
            let submission = handle.reserve_request().unwrap().submit(
                head,
                RequestSource::held(None, body),
                CancellationToken::never(),
            );
            let (run, request) = futures::join!(session.run(), async {
                match submission.await {
                    Ok(response) => {
                        let (_stream, mut source, _control) = response.into_source_parts();
                        gate.notify_one();
                        read_response(&mut source).await.error
                    }
                    Err(error) => {
                        assert!(handle.close_when_idle().unwrap());
                        Some(error.code())
                    }
                }
            });
            let state = state.borrow();
            OwnedOutcome {
                run: run.err().map(|error| error.code()),
                request,
                wire: state.writes.concat(),
                close: state.close,
            }
        })
    })
}

/// The bytes of a `POST /upload` declaring `length`, through its head.
fn declared_head_wire(length: usize) -> Vec<u8> {
    format!(
        "POST /upload HTTP/1.1\r\nHost: origin\r\nConnection: keep-alive\r\nContent-Length: {length}\r\n\r\n"
    )
    .into_bytes()
}

#[test]
fn request_body_longer_than_its_content_length_fails() {
    // (the excess in the body event that reaches the length, or in a later event)
    let requests: [(&str, fn() -> (HttpHeaderEvent, Vec<HttpBodyEvent>)); 2] = [
        ("one event", || {
            (
                post_head(Some(5), false),
                vec![HttpBodyEvent::body(queue(b"hello world"), true)],
            )
        }),
        ("two events", || {
            (
                post_head(Some(5), false),
                vec![
                    HttpBodyEvent::body(queue(b"hello"), false),
                    HttpBodyEvent::body(queue(b" world"), true),
                ],
            )
        }),
    ];
    for (name, request) in requests {
        let owned = owned_exchange_outcome(request);
        assert_eq!(
            owned.run,
            Some(HttpErrorCode::CONTENT_LENGTH_MISMATCH),
            "{name}"
        );
        assert_eq!(
            owned.request,
            Some(HttpErrorCode::CONTENT_LENGTH_MISMATCH),
            "{name}"
        );
        assert_eq!(owned.close, CloseKind::Reset, "{name}");
        let declared = [declared_head_wire(5), b"hello".to_vec()].concat();
        assert!(
            declared.starts_with(&owned.wire),
            "{name}: no byte past the declared length is written: {:?}",
            String::from_utf8_lossy(&owned.wire)
        );
        // `UniplexSession` records the mismatch, writes every byte and completes the exchange.
        let (run, read, uniplex) = uniplex_exchange_outcome(request, OK_RESPONSE);
        assert_eq!((run, read.error), (None, None), "{name}");
        assert_eq!(
            uniplex,
            [declared_head_wire(5), b"hello world".to_vec()].concat(),
            "{name}"
        );
    }
}

#[test]
fn request_body_shorter_than_its_content_length_fails() {
    // (EOM on the last body event, or on an empty body event after it)
    let requests: [(&str, fn() -> (HttpHeaderEvent, Vec<HttpBodyEvent>)); 2] = [
        ("body EOM", || {
            (
                post_head(Some(11), false),
                vec![HttpBodyEvent::body(queue(b"hello"), true)],
            )
        }),
        ("empty EOM", || {
            (
                post_head(Some(11), false),
                vec![
                    HttpBodyEvent::body(queue(b"hello"), false),
                    HttpBodyEvent::body(queue(b""), true),
                ],
            )
        }),
    ];
    for (name, request) in requests {
        let owned = owned_exchange_outcome(request);
        assert_eq!(
            owned.run,
            Some(HttpErrorCode::CONTENT_LENGTH_MISMATCH),
            "{name}"
        );
        assert_eq!(
            owned.request,
            Some(HttpErrorCode::CONTENT_LENGTH_MISMATCH),
            "{name}"
        );
        assert_eq!(owned.close, CloseKind::Reset, "{name}");
        let declared = [declared_head_wire(11), b"hello".to_vec()].concat();
        assert!(
            declared.starts_with(&owned.wire),
            "{name}: {:?}",
            String::from_utf8_lossy(&owned.wire)
        );
        // `UniplexSession` records the mismatch, writes the short body and completes.
        let (run, read, uniplex) = uniplex_exchange_outcome(request, OK_RESPONSE);
        assert_eq!((run, read.error), (None, None), "{name}");
        assert!(uniplex.starts_with(&declared), "{name}");
    }
}

#[test]
fn header_only_request_declaring_a_length_is_refused_before_any_byte() {
    let request = || (post_head(Some(5), true), Vec::new());
    let owned = owned_exchange_outcome(request);
    assert_eq!(owned.request, Some(HttpErrorCode::CONTENT_LENGTH_MISMATCH));
    assert_eq!(
        owned.run, None,
        "the refused request leaves the connection idle"
    );
    assert!(
        owned.wire.is_empty(),
        "{:?}",
        String::from_utf8_lossy(&owned.wire)
    );
    // `UniplexSession` records the mismatch, writes the head and completes the exchange.
    let (run, read, uniplex) = uniplex_exchange_outcome(request, OK_RESPONSE);
    assert_eq!((run, read.error), (None, None));
    assert_eq!(uniplex, declared_head_wire(5));
    // A declared length of zero is a complete request.
    let zero = owned_exchange_outcome(|| (post_head(Some(0), true), Vec::new()));
    assert_eq!((zero.run, zero.request), (None, None));
    assert_eq!(zero.wire, declared_head_wire(0));
}

#[test]
fn trailers_on_a_content_length_request_end_it_without_them() {
    let request = || {
        let mut trailers = HttpHeaders::new();
        trailers.add(b"x-checksum", b"abc");
        (
            post_head(Some(5), false),
            vec![
                HttpBodyEvent::body(queue(b"hello"), false),
                HttpBodyEvent::trailers(trailers),
            ],
        )
    };
    let declared = [declared_head_wire(5), b"hello".to_vec()].concat();
    // A `Content-Length` message cannot carry trailers. The codec does not write them, and the
    // request ends with its body.
    let owned = owned_exchange_outcome(request);
    assert_eq!((owned.run, owned.request), (None, None));
    assert_eq!(
        String::from_utf8_lossy(&owned.wire),
        String::from_utf8_lossy(&declared)
    );
    assert_eq!(owned.close, CloseKind::ShutdownWrite);
    // Uniplex passes the trailers to the codec alone, so the codec's message never ends and the
    // exchange fails at EOF.
    let (run, _read, uniplex) = uniplex_exchange_outcome(request, OK_RESPONSE);
    assert_eq!(run, Some(HttpErrorCode::TRANSPORT_EOF));
    assert_eq!(uniplex, declared);
}

/// An upstream observer that cancels the connection from one named callback. The test takes
/// `handle` back after `run`, which breaks the handle -> observer -> handle cycle.
struct CancelFromObserver {
    handle: RefCell<Option<crate::owned_h1::OwnedH1UpstreamHandle>>,
    callback: &'static str,
}

impl CancelFromObserver {
    fn cancel(&self, callback: &str) {
        if self.callback == callback
            && let Some(handle) = &*self.handle.borrow()
        {
            handle.cancel();
        }
    }
}

impl crate::owned::OwnedUpstreamObserver for CancelFromObserver {
    fn capacity_released(&self, _stream: StreamId) {
        self.cancel("capacity_released");
    }

    fn capacity_available(&self) {
        self.cancel("capacity_available");
    }

    fn admission_closed(&self) {
        self.cancel("admission_closed");
    }
}

#[test]
fn cancel_from_a_settle_callback_fails_the_connection() {
    // The callbacks that a reusable connection runs as its exchange retires, then the callback
    // that replaces `capacity_available` when the connection is not reused.
    for (callback, response) in [
        ("capacity_released", OK_RESPONSE),
        ("capacity_available", OK_RESPONSE),
        ("admission_closed", CLOSE_RESPONSE),
    ] {
        run_on_host(move |context| {
            Box::pin(async move {
                let response_gate = Arc::new(tokio::sync::Notify::new());
                let empty_gate = Arc::new(tokio::sync::Notify::new());
                // The empty read completes in the turn that retires the exchange, so the slot
                // returns without an outstanding read.
                let (transport, state) = ScriptedTransport::new([
                    ReadAction::WaitData(Arc::clone(&response_gate), response.to_vec()),
                    ReadAction::WaitDataOrCancellation(Arc::clone(&empty_gate), Vec::new()),
                ]);
                let (session, handle) =
                    OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
                let observer = Rc::new(CancelFromObserver {
                    handle: RefCell::new(Some(handle.clone())),
                    callback,
                });
                handle.set_observer(observer.clone()).unwrap();
                let submission = handle.reserve_request().unwrap().submit(
                    get_head(),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                let (run, ()) = futures::join!(session.run(), async {
                    let (_stream, mut source, _control) =
                        submission.await.unwrap().into_source_parts();
                    response_gate.notify_one();
                    read_response(&mut source).await;
                    drop(source);
                    empty_gate.notify_one();
                });
                observer.handle.take();
                assert_eq!(
                    run.map_err(|error| error.code()).err(),
                    Some(HttpErrorCode::CORO_CANCELLED),
                    "the cancel from {callback} fails the connection"
                );
                assert_eq!(state.borrow().close, CloseKind::Reset, "{callback}");
                assert!(handle.is_finished(), "{callback}");
            })
        });
    }
}

#[test]
fn cancel_from_admission_closed_at_eof_fails_the_connection() {
    run_on_host(|context| {
        Box::pin(async move {
            let eof = Arc::new(tokio::sync::Notify::new());
            let (transport, state) =
                ScriptedTransport::new([ReadAction::WaitEof(Arc::clone(&eof))]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let observer = Rc::new(CancelFromObserver {
                handle: RefCell::new(Some(handle.clone())),
                callback: "admission_closed",
            });
            handle.set_observer(observer.clone()).unwrap();
            // Peer EOF on an idle connection closes admission, and the observer cancels there.
            let (run, ()) = futures::join!(session.run(), async {
                eof.notify_one();
            });
            observer.handle.take();
            assert_eq!(
                run.map_err(|error| error.code()).err(),
                Some(HttpErrorCode::CORO_CANCELLED),
                "the cancel from admission_closed at EOF fails the connection"
            );
            assert_eq!(state.borrow().close, CloseKind::Reset);
            assert!(handle.is_finished());
        })
    });
}

#[test]
fn request_cancel_from_a_write_completion_closes_the_connection() {
    run_on_host(|context| {
        Box::pin(async move {
            let response_gate = Arc::new(tokio::sync::Notify::new());
            let (inner, state) = ScriptedTransport::new([ReadAction::WaitDataOrCancellation(
                Arc::clone(&response_gate),
                OK_RESPONSE.to_vec(),
            )]);
            let complete_hook = Rc::new(RefCell::new(None));
            let transport = Rc::new(QueuedTransport {
                inner,
                state: Rc::clone(&state),
                hook: Rc::default(),
                complete_hook: Rc::clone(&complete_hook),
                idle_hook: Rc::default(),
            });
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let token = Rc::new(CancellationSource::new());
            let canceller = Rc::clone(&token);
            let release = Arc::clone(&response_gate);
            // The request head's write completion cancels the request and releases the whole
            // response, which the driver reads in the same turn.
            *complete_hook.borrow_mut() = Some(Box::new(move || {
                canceller.request_cancellation();
                release.notify_one();
            }) as Box<dyn Fn()>);
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                token.token(),
            );
            let (run, read) = futures::join!(session.run(), async {
                match submission.await {
                    Ok(response) => {
                        let (_stream, mut source, _control) = response.into_source_parts();
                        read_response(&mut source).await.error
                    }
                    Err(error) => Some(error.code()),
                }
            });
            assert!(complete_hook.borrow().is_none(), "the write completed");
            assert_eq!(
                (run.map_err(|error| error.code()).err(), read),
                (
                    Some(HttpErrorCode::CORO_CANCELLED),
                    Some(HttpErrorCode::CORO_CANCELLED)
                ),
                "a cancel selected before the response is parsed closes the connection"
            );
            assert_eq!(state.borrow().close, CloseKind::Reset);
        })
    });
}

#[test]
fn cancel_from_a_request_head_callback_reads_no_request_body() {
    run_on_host(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let upload_gate = Arc::new(tokio::sync::Notify::new());
            upload_gate.notify_one();
            let body_read = Rc::new(Cell::new(false));
            let read = Rc::clone(&body_read);
            // The head's serialization callback cancels the connection during commitment. The
            // request body is ready, but it is not read after the cancel.
            let mut head = post_head(Some(5), false);
            let canceller = handle.clone();
            head.set_egress_headers_callback(move |_size| canceller.cancel());
            let submission = handle.reserve_request().unwrap().submit(
                head,
                GatedUpload::held(&upload_gate, Some(Box::new(move || read.set(true)))),
                CancellationToken::never(),
            );
            let (run, _response) = futures::join!(session.run(), submission);
            assert!(
                !body_read.get(),
                "the request body is read after the cancel"
            );
            assert_eq!(
                run.map_err(|error| error.code()).err(),
                Some(HttpErrorCode::CORO_CANCELLED)
            );
            assert!(state.borrow().writes.is_empty());
            assert_eq!(state.borrow().close, CloseKind::Reset);
        })
    });
}

/// A response observer whose completion cancels the connection and releases the request body.
struct CancelConnectionOnComplete {
    handle: crate::owned_h1::OwnedH1UpstreamHandle,
    upload: Arc<tokio::sync::Notify>,
}

impl crate::owned::OwnedResponseObserver for CancelConnectionOnComplete {
    fn poll_deadline(&mut self, _context: &mut std::task::Context<'_>) -> Poll<HttpError> {
        Poll::Pending
    }

    fn response_complete(self: Box<Self>, _completed: Result<std::time::Instant, HttpError>) {
        self.handle.cancel();
        self.upload.notify_one();
    }
}

#[test]
fn connection_cancel_from_the_response_observer_reads_no_more_request() {
    run_on_host(|context| {
        Box::pin(async move {
            let response_gate = Arc::new(tokio::sync::Notify::new());
            let upload_gate = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([ReadAction::WaitData(
                Arc::clone(&response_gate),
                OK_RESPONSE.to_vec(),
            )]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let body_read = Rc::new(Cell::new(false));
            let read = Rc::clone(&body_read);
            let submission = handle.reserve_request().unwrap().submit(
                post_head(Some(5), false),
                GatedUpload::held(&upload_gate, Some(Box::new(move || read.set(true)))),
                CancellationToken::never(),
            );
            let (run, ()) = futures::join!(session.run(), async {
                let response = submission.await.unwrap();
                // As `cancel_from_the_response_observer_serializes_no_more_request`, with the
                // connection cancelled instead of the request.
                response
                    .control
                    .observe_response(Box::new(CancelConnectionOnComplete {
                        handle: handle.clone(),
                        upload: Arc::clone(&upload_gate),
                    }))
                    .unwrap();
                response_gate.notify_one();
                let (_stream, mut source, _control) = response.into_source_parts();
                read_response(&mut source).await;
            });
            assert!(
                !body_read.get(),
                "the request body is read after the cancel"
            );
            assert!(!state.borrow().writes.concat().ends_with(b"hello"));
            assert_eq!(
                run.map_err(|error| error.code()).err(),
                Some(HttpErrorCode::CORO_CANCELLED)
            );
            assert_eq!(state.borrow().close, CloseKind::Reset);
        })
    });
}

/// A response observer whose completion cancels the worker, then releases the upload.
struct CancelWorkerOnComplete {
    worker: roxygen_runtime::worker::WorkerHandle,
    upload: Arc<tokio::sync::Notify>,
}

impl crate::owned::OwnedResponseObserver for CancelWorkerOnComplete {
    fn poll_deadline(&mut self, _context: &mut std::task::Context<'_>) -> Poll<HttpError> {
        Poll::Pending
    }

    fn response_complete(self: Box<Self>, _completed: Result<std::time::Instant, HttpError>) {
        assert!(self.worker.begin_shutdown());
        self.upload.notify_one();
    }
}

/// As `connection_cancel_from_the_response_observer_reads_no_more_request`, with the worker
/// cancelled instead of the connection. The driver observes it at the end of that phase and
/// stops reading the request.
#[test]
fn worker_cancel_from_the_response_observer_reads_no_more_request() {
    run_on_host(|context| {
        Box::pin(async move {
            let response_gate = Arc::new(tokio::sync::Notify::new());
            let upload_gate = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([ReadAction::WaitData(
                Arc::clone(&response_gate),
                OK_RESPONSE.to_vec(),
            )]);
            let worker = context.handle().clone();
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let body_read = Rc::new(Cell::new(false));
            let read = Rc::clone(&body_read);
            let submission = handle.reserve_request().unwrap().submit(
                post_head(Some(5), false),
                GatedUpload::held(&upload_gate, Some(Box::new(move || read.set(true)))),
                CancellationToken::never(),
            );
            let (run, ()) = futures::join!(session.run(), async {
                let response = submission.await.unwrap();
                response
                    .control
                    .observe_response(Box::new(CancelWorkerOnComplete {
                        worker,
                        upload: Arc::clone(&upload_gate),
                    }))
                    .unwrap();
                response_gate.notify_one();
                let (_stream, mut source, _control) = response.into_source_parts();
                read_response(&mut source).await;
            });
            assert!(
                !body_read.get(),
                "the request body is read after the cancel"
            );
            assert!(!state.borrow().writes.concat().ends_with(b"hello"));
            assert_eq!(
                run.map_err(|error| error.code()).err(),
                Some(HttpErrorCode::CORO_CANCELLED)
            );
            assert_eq!(state.borrow().close, CloseKind::Reset);
        })
    });
}

/// A response-interest waiter that cancels its request's token, then wakes the task.
struct CancelOnWake {
    source: CancellationSource,
    task: std::sync::Mutex<Option<std::task::Waker>>,
}

impl std::task::Wake for CancelOnWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.source.request_cancellation();
        let task = self.task.lock().unwrap().take();
        if let Some(task) = task {
            task.wake();
        }
    }
}

#[test]
fn request_cancel_from_the_response_head_publication_closes_the_connection() {
    run_on_host(|context| {
        Box::pin(async move {
            let response_gate = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([ReadAction::WaitDataOrCancellation(
                Arc::clone(&response_gate),
                OK_RESPONSE.to_vec(),
            )]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let source = CancellationSource::new();
            let token = source.token();
            let wake = Arc::new(CancelOnWake {
                source,
                task: std::sync::Mutex::new(None),
            });
            let submission = handle
                .reserve_request()
                .unwrap()
                .submit_with_response_interest(get_head(), HttpSourceHolder::empty(), token, None);
            let (run, claimed) = futures::join!(session.run(), async {
                let mut submission = std::pin::pin!(submission);
                yield_until(|| !state.borrow().writes.is_empty()).await;
                // The whole response arrives in one read: publishing its head runs this waker,
                // which cancels the request before the codec reaches the response's end.
                let mut release = Some(response_gate);
                std::future::poll_fn(|task| {
                    *wake.task.lock().unwrap() = Some(task.waker().clone());
                    let waker = std::task::Waker::from(Arc::clone(&wake));
                    let poll = submission
                        .as_mut()
                        .poll(&mut std::task::Context::from_waker(&waker));
                    if let Some(gate) = release.take() {
                        gate.notify_one();
                    }
                    poll
                })
                .await
                .err()
                .map(|error| error.code())
            });
            assert_eq!(
                (run.map_err(|error| error.code()).err(), claimed),
                (
                    Some(HttpErrorCode::CORO_CANCELLED),
                    Some(HttpErrorCode::CORO_CANCELLED)
                ),
                "a cancel selected before the response ends closes the connection"
            );
            assert_eq!(state.borrow().close, CloseKind::Reset);
        })
    });
}

#[test]
fn cancel_from_the_write_queue_idle_check_fails_the_connection() {
    run_on_host(|context| {
        Box::pin(async move {
            let response_gate = Arc::new(tokio::sync::Notify::new());
            let empty_gate = Arc::new(tokio::sync::Notify::new());
            // As in `cancel_from_a_settle_callback_fails_the_connection`, a read is not
            // outstanding when the retired connection's write side would shut down.
            let (inner, state) = ScriptedTransport::new([
                ReadAction::WaitData(Arc::clone(&response_gate), OK_RESPONSE.to_vec()),
                ReadAction::WaitDataOrCancellation(Arc::clone(&empty_gate), Vec::new()),
            ]);
            let idle_hook = Rc::new(RefCell::new(None));
            let transport = Rc::new(QueuedTransport {
                inner,
                state: Rc::clone(&state),
                hook: Rc::default(),
                complete_hook: Rc::default(),
                idle_hook: Rc::clone(&idle_hook),
            });
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let (run, ()) = futures::join!(session.run(), async {
                let (_stream, mut source, _control) = submission.await.unwrap().into_source_parts();
                response_gate.notify_one();
                read_response(&mut source).await;
                // The request's write completed long ago, so the next idle check is the one the
                // driver makes before closing the retired connection.
                let canceller = handle.clone();
                *idle_hook.borrow_mut() =
                    Some(Box::new(move || canceller.cancel()) as Box<dyn Fn()>);
                drop(source);
                empty_gate.notify_one();
            });
            assert!(idle_hook.borrow().is_none(), "the idle check ran");
            assert_eq!(
                run.map_err(|error| error.code()).err(),
                Some(HttpErrorCode::CORO_CANCELLED),
                "the cancel from the idle check fails the connection"
            );
            assert_eq!(state.borrow().close, CloseKind::Reset);
        })
    });
}

/// A request body that suspends first, with a resume future that runs `on_resume`; the read
/// after the resume records itself and returns the body.
struct SuspendingUpload {
    on_resume: Option<Box<dyn FnOnce()>>,
    read_after_resume: Rc<Cell<bool>>,
}

impl HttpSource for SuspendingUpload {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        unreachable!("an owned submission takes the head separately")
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        if let Some(on_resume) = self.on_resume.take() {
            let resume: roxygen_core::events::ResumeFuture = Box::pin(async move {
                on_resume();
                ResumeStatus::Signalled
            });
            return Box::pin(std::future::ready(Ok(HttpBodyEvent::suspend(resume))));
        }
        self.read_after_resume.set(true);
        Box::pin(std::future::ready(Ok(HttpBodyEvent::body(
            queue(b"hello"),
            true,
        ))))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

#[test]
fn connection_cancel_from_a_resume_future_reads_no_more_request() {
    run_on_host(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let read_after_resume = Rc::new(Cell::new(false));
            // The upload's resume future cancels the connection and completes. The body is not
            // read after it.
            let canceller = handle.clone();
            let upload = HttpSourceHolder::new(Box::new(SuspendingUpload {
                on_resume: Some(Box::new(move || canceller.cancel())),
                read_after_resume: Rc::clone(&read_after_resume),
            }));
            let submission = handle.reserve_request().unwrap().submit(
                post_head(None, false),
                upload,
                CancellationToken::never(),
            );
            let (run, _response) = futures::join!(session.run(), submission);
            assert!(
                !read_after_resume.get(),
                "the request body is read after the cancel"
            );
            assert_eq!(
                run.map_err(|error| error.code()).err(),
                Some(HttpErrorCode::CORO_CANCELLED)
            );
            assert_eq!(state.borrow().close, CloseKind::Reset);
        })
    });
}

/// What a slow reader got from a 24 KiB response whose bytes all arrive before a failed read,
/// with the ingress limit below the body: the run's error, the body, EOM, and the error that
/// ended the body early, if any.
#[derive(Debug, PartialEq)]
struct ReadErrorOutcome {
    run: Option<HttpErrorCode>,
    body: Vec<u8>,
    eom: bool,
    read: Option<HttpErrorCode>,
}

/// Runs that exchange through an owned upstream session, or a `UniplexSession` client.
fn paused_read_error_outcome(owned: bool) -> ReadErrorOutcome {
    let body = patterned(24 << 10);
    let mut first =
        format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).into_bytes();
    first.extend_from_slice(&body[..12 << 10]);
    let second = body[12 << 10..].to_vec();
    run_on_host(move |context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            let (transport, _state) = ScriptedTransport::new([
                ReadAction::WaitData(Arc::clone(&gate), first),
                ReadAction::Data(second),
                ReadAction::Error(HttpErrorCode::TRANSPORT_READ_ERROR),
            ]);
            let mut config = upstream_config();
            config.ingress_buffer_limit = 16 << 10;
            let read_body = |mut source: HttpSourceHolder| async move {
                let mut outcome = ReadErrorOutcome {
                    run: None,
                    body: Vec::new(),
                    eom: false,
                    read: None,
                };
                match source.read_header_event().await {
                    Ok(mut head) => assert_eq!(head.headers_mut().status_code().unwrap(), 200),
                    Err(error) => {
                        outcome.read = Some(error.code());
                        return outcome;
                    }
                }
                loop {
                    // A slow reader, so parsing waits on the ingress limit.
                    tokio::task::yield_now().await;
                    match source.read_body_event(4 << 10).await {
                        Ok(event) => {
                            if let Some(bytes) = event.as_body() {
                                let length = bytes.chain_length();
                                outcome
                                    .body
                                    .extend_from_slice(&bytes.coalesce(length).unwrap());
                            }
                            if event.eom() {
                                outcome.eom = true;
                                return outcome;
                            }
                        }
                        Err(error) => {
                            outcome.read = Some(error.code());
                            return outcome;
                        }
                    }
                }
            };
            let (run, mut outcome) = if owned {
                let mut options = upstream_options(context, transport);
                options.io = config;
                let (session, handle) = OwnedH1UpstreamSession::new(options).unwrap();
                let submission = handle.reserve_request().unwrap().submit(
                    get_head(),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                futures::join!(session.run(), async {
                    let (_stream, source, _control) = submission.await.unwrap().into_source_parts();
                    gate.notify_one();
                    read_body(source).await
                })
            } else {
                let (session, handle) = UniplexSession::new(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    config,
                )
                .unwrap();
                futures::join!(session.run(), async {
                    let response = handle
                        .send_request(
                            RequestSource::held(Some(get_head()), Vec::new()),
                            CancellationToken::never(),
                        )
                        .await
                        .unwrap();
                    gate.notify_one();
                    read_body(response.source).await
                })
            };
            outcome.run = run.err().map(|error| error.code());
            outcome
        })
    })
}

#[test]
fn bytes_read_before_a_read_error_reach_the_response_as_uniplex() {
    let uniplex = paused_read_error_outcome(false);
    assert!(
        uniplex.eom && uniplex.body == patterned(24 << 10),
        "Uniplex parses every byte read before the error: {} bytes, eom {}",
        uniplex.body.len(),
        uniplex.eom
    );
    let owned = paused_read_error_outcome(true);
    assert!(
        owned.eom && owned.body == uniplex.body,
        "every byte read before the error reaches the response: {} bytes, eom {}, read error {:?}",
        owned.body.len(),
        owned.eom,
        owned.read
    );
    assert_eq!(
        (owned.run, owned.read),
        (uniplex.run, uniplex.read),
        "the read error fails the connection after the response"
    );
}

/// What the caller observed of [`resume_threshold_outcome`]'s response.
struct ResumeThresholdOutcome {
    run: Option<HttpErrorCode>,
    body: Vec<u8>,
    /// `bytes_processed` callbacks the response source gave its session.
    callbacks: usize,
    /// The `record_buffered_read_bytes` deltas of a `UniplexSession` that reports stats.
    buffered_read_deltas: Vec<i64>,
}

/// A 22 KiB response body, between `ingress_buffer_limit` (16 KiB) and `buffer_limit`
/// (64 KiB), read 4 KiB at a time by an owned upstream session's caller, or a `UniplexSession`
/// client's. The first transport read carries 20 KiB of it, which pause parsing; the last 2 KiB
/// wait behind that pause, and with them the response's end. Every read arrives as one buffer,
/// so parsing pauses once. With `stats`, the `UniplexSession` reports session stats.
fn resume_threshold_outcome(owned: bool, stats: bool) -> ResumeThresholdOutcome {
    let body = patterned(22 << 10);
    let mut first = format!(
        "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    first.extend_from_slice(&body[..20 << 10]);
    let second = body[20 << 10..].to_vec();
    run_on_host(move |context| {
        Box::pin(async move {
            super::owned_h1::take_ingress_credit_callbacks(owned);
            let deltas = Rc::new(super::owned_h1::BufferedReadDeltas::default());
            let gate = Arc::new(tokio::sync::Notify::new());
            let (transport, _state) = ScriptedTransport::new([
                ReadAction::WaitData(Arc::clone(&gate), first),
                ReadAction::Data(second),
            ]);
            let config = UniplexConfig {
                buffer_limit: 64 << 10,
                ingress_buffer_limit: 16 << 10,
                maximum_read_size: 64 << 10,
                read_allocation_size: 64 << 10,
                ..upstream_config()
            };
            let read_body = |mut source: HttpSourceHolder| async move {
                let mut head = source.read_header_event().await.unwrap();
                assert_eq!(head.headers_mut().status_code().unwrap(), 200);
                let mut body = Vec::new();
                loop {
                    let event = source.read_body_event(4 << 10).await.unwrap();
                    if let Some(bytes) = event.as_body() {
                        let length = bytes.chain_length();
                        body.extend_from_slice(&bytes.coalesce(length).unwrap());
                    }
                    if event.eom() {
                        return body;
                    }
                }
            };
            let (run, body) = if owned {
                let mut options = upstream_options(context, transport);
                options.io = config;
                let (session, handle) = OwnedH1UpstreamSession::new(options).unwrap();
                let submission = handle.reserve_request().unwrap().submit(
                    get_head(),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                futures::join!(session.run(), async {
                    let (_stream, source, _control) = submission.await.unwrap().into_source_parts();
                    gate.notify_one();
                    read_body(source).await
                })
            } else {
                let (session, handle) = if stats {
                    UniplexSession::new_with_stats(
                        context,
                        TransportDirection::Upstream,
                        transport,
                        None,
                        config,
                        Rc::clone(&deltas) as Rc<dyn crate::http2::stats::Http2SessionStats>,
                    )
                } else {
                    UniplexSession::new(
                        context,
                        TransportDirection::Upstream,
                        transport,
                        None,
                        config,
                    )
                }
                .unwrap();
                futures::join!(session.run(), async {
                    let response = handle
                        .send_request(
                            RequestSource::held(Some(get_head()), Vec::new()),
                            CancellationToken::never(),
                        )
                        .await
                        .unwrap();
                    gate.notify_one();
                    read_body(response.source).await
                })
            };
            ResumeThresholdOutcome {
                run: run.err().map(|error| error.code()),
                body,
                callbacks: super::owned_h1::take_ingress_credit_callbacks(owned),
                buffered_read_deltas: deltas.0.take(),
            }
        })
    })
}

/// Asserts that the caller read the whole response after one pause and the one credit callback
/// that resumed parsing.
fn assert_resumed_once_below_the_threshold(outcome: &ResumeThresholdOutcome) {
    assert_eq!(outcome.run, None);
    assert!(
        outcome.body == patterned(22 << 10),
        "parsing resumed and the body arrived intact through its end: {} bytes",
        outcome.body.len()
    );
    assert_eq!(
        outcome.callbacks, 1,
        "only the read that takes the unread body below the threshold reports credit"
    );
}

#[test]
fn uniplex_client_resumes_below_the_ingress_threshold() {
    assert_resumed_once_below_the_threshold(&resume_threshold_outcome(false, false));
}

#[test]
fn owned_client_resumes_below_the_ingress_threshold() {
    assert_resumed_once_below_the_threshold(&resume_threshold_outcome(true, false));
}

/// HTTP/1.1 session stats count buffered response bytes down on every read, so a Uniplex
/// client that reports them keeps a callback per read.
#[test]
fn uniplex_client_with_stats_keeps_a_callback_per_read() {
    let outcome = resume_threshold_outcome(false, true);
    assert_eq!(outcome.run, None);
    assert!(outcome.body == patterned(22 << 10));
    let deltas = &outcome.buffered_read_deltas;
    let reads: Vec<i64> = deltas.iter().copied().filter(|delta| *delta < 0).collect();
    assert_eq!(
        reads,
        [-4096, -4096, -4096, -4096, -4096, -2048],
        "one delta per 4 KiB read of the 22 KiB body; all deltas: {deltas:?}"
    );
    assert_eq!(
        deltas.iter().sum::<i64>(),
        0,
        "every buffered byte is counted out"
    );
    assert_eq!(outcome.callbacks, 6);
}

/// With 400 bytes of a 700-byte response window unread and the pause at 500, both clients parse
/// the next 400-byte read only up to the window and pause there instead of failing the
/// response. Once the caller reads, it gets all 850 bytes.
#[test]
fn h1_response_body_events_stop_at_the_window() {
    let body = patterned(850);
    for owned in [true, false] {
        let head = format!(
            "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes();
        let chunks: Vec<_> = body.chunks(400).map(<[u8]>::to_vec).collect();
        let (run, parsed, received) = run_on_host(move |context| {
            Box::pin(async move {
                let gate = Arc::new(tokio::sync::Notify::new());
                // Each body read yields first, so the client parses the bytes before it within
                // its 700-byte buffer.
                let (transport, _state) = ScriptedTransport::new(
                    std::iter::once(ReadAction::WaitData(Arc::clone(&gate), head))
                        .chain(chunks.into_iter().map(ReadAction::YieldData)),
                );
                let config = UniplexConfig {
                    buffer_limit: 700,
                    ingress_buffer_limit: 500,
                    maximum_read_size: 700,
                    read_allocation_size: 700,
                    maximum_write_size: 700,
                    ..upstream_config()
                };
                let read_body = |mut source: HttpSourceHolder| async move {
                    source.read_header_event().await.unwrap();
                    let completion = source.ingress_completion().unwrap();
                    yield_until(|| {
                        completion.body_bytes_received() >= 700 || completion.snapshot().is_some()
                    })
                    .await;
                    let parsed = completion.body_bytes_received();
                    let mut received = Vec::new();
                    loop {
                        let event = source.read_body_event(u32::MAX).await.unwrap();
                        if let Some(bytes) = event.as_body() {
                            let length = bytes.chain_length();
                            received.extend_from_slice(&bytes.coalesce(length).unwrap());
                        }
                        if event.eom() {
                            return (parsed, received);
                        }
                    }
                };
                let (run, (parsed, received)) = if owned {
                    let mut options = upstream_options(context, transport);
                    options.io = config;
                    let (session, handle) = OwnedH1UpstreamSession::new(options).unwrap();
                    let submission = handle.reserve_request().unwrap().submit(
                        get_head(),
                        HttpSourceHolder::empty(),
                        CancellationToken::never(),
                    );
                    futures::join!(session.run(), async {
                        let (_stream, source, _control) =
                            submission.await.unwrap().into_source_parts();
                        gate.notify_one();
                        read_body(source).await
                    })
                } else {
                    let (session, handle) = UniplexSession::new(
                        context,
                        TransportDirection::Upstream,
                        transport,
                        None,
                        config,
                    )
                    .unwrap();
                    futures::join!(session.run(), async {
                        let response = handle
                            .send_request(
                                RequestSource::held(Some(get_head()), Vec::new()),
                                CancellationToken::never(),
                            )
                            .await
                            .unwrap();
                        gate.notify_one();
                        read_body(response.source).await
                    })
                };
                (run.err().map(|error| error.code()), parsed, received)
            })
        });
        assert_eq!(
            parsed, 700,
            "owned {owned}: the client paused at the window"
        );
        assert!(
            received == body,
            "owned {owned}: {} body bytes arrived",
            received.len()
        );
        assert_eq!(run, None, "owned {owned}");
    }
}

/// A scripted request whose last body event waits for `gate`.
struct GatedTail {
    head: Option<HttpHeaderEvent>,
    body: VecDeque<HttpBodyEvent>,
    gate: Arc<tokio::sync::Notify>,
}

impl HttpSource for GatedTail {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let head = self.head.take().expect("the request head is read once");
        Box::pin(std::future::ready(Ok(head)))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let event = self.body.pop_front().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"request body exhausted")
        });
        if !self.body.is_empty() {
            return Box::pin(std::future::ready(event));
        }
        let gate = Arc::clone(&self.gate);
        Box::pin(async move {
            gate.notified().await;
            event
        })
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {
        self.body.clear();
    }
}

/// The request source both sessions read: with `tail`, its last body event waits for it.
fn reuse_request_source(
    head: Option<HttpHeaderEvent>,
    body: Vec<HttpBodyEvent>,
    tail: Option<&Arc<tokio::sync::Notify>>,
) -> HttpSourceHolder {
    match tail {
        Some(gate) => HttpSourceHolder::new(Box::new(GatedTail {
            head,
            body: body.into(),
            gate: Arc::clone(gate),
        })),
        None => RequestSource::held(head, body),
    }
}

/// Whether and how a connection was reused after its first exchange, with an origin that keeps
/// it open.
#[derive(Debug)]
struct ReuseOutcome {
    first: ReadResponse,
    /// The connection admitted a second request.
    reused: bool,
    /// The bytes the second request wrote, when the connection admitted one.
    second_wire: Vec<u8>,
    /// Every byte the connection wrote.
    wire: Vec<u8>,
    /// The upstream observer's notifications when the decision was read (owned sessions only).
    log: Vec<String>,
    run: Option<HttpErrorCode>,
    close: CloseKind,
}

/// The origin's reads for a reuse comparison: `response`, then, unless `eof` ends the
/// connection, `OK_RESPONSE` for a second request.
fn reuse_reads(
    first: &Arc<tokio::sync::Notify>,
    second: &Arc<tokio::sync::Notify>,
    response: &'static [u8],
    eof: bool,
) -> Vec<ReadAction> {
    let mut reads = vec![ReadAction::WaitData(Arc::clone(first), response.to_vec())];
    reads.push(if eof {
        ReadAction::Eof
    } else {
        // A connection that is not reused cancels this read when it closes.
        ReadAction::WaitDataOrCancellation(Arc::clone(second), OK_RESPONSE.to_vec())
    });
    reads
}

/// One exchange through an owned upstream session, then a `GET /` when the connection
/// returns its slot. With `tail`, the request's last body event waits until the response has
/// been read.
fn owned_reuse_outcome(
    request: impl FnOnce() -> (HttpHeaderEvent, Vec<HttpBodyEvent>) + Send + 'static,
    response: &'static [u8],
    eof: bool,
    tail: Option<Arc<tokio::sync::Notify>>,
) -> ReuseOutcome {
    owned_reuse_outcome_with(upstream_config(), request, response, eof, tail)
}

/// As `owned_reuse_outcome`, with `config` for the session.
fn owned_reuse_outcome_with(
    config: UniplexConfig,
    request: impl FnOnce() -> (HttpHeaderEvent, Vec<HttpBodyEvent>) + Send + 'static,
    response: &'static [u8],
    eof: bool,
    tail: Option<Arc<tokio::sync::Notify>>,
) -> ReuseOutcome {
    run_on_host(move |context| {
        Box::pin(async move {
            let first_ready = Arc::new(tokio::sync::Notify::new());
            let second_ready = Arc::new(tokio::sync::Notify::new());
            let (transport, state) =
                ScriptedTransport::new(reuse_reads(&first_ready, &second_ready, response, eof));
            let mut options = upstream_options(context, transport);
            options.io = config;
            let (session, handle) = OwnedH1UpstreamSession::new(options).unwrap();
            let log = Rc::new(AdmissionLog::default());
            handle.set_observer(log.clone()).unwrap();
            let (head, body) = request();
            let submission = handle.reserve_request().unwrap().submit(
                head,
                reuse_request_source(None, body, tail.as_ref()),
                CancellationToken::never(),
            );
            let (run, (first, reused, second_wire, decided)) =
                futures::join!(session.run(), async {
                    let (_stream, mut source, _control) =
                        submission.await.unwrap().into_source_parts();
                    first_ready.notify_one();
                    let first = read_response(&mut source).await;
                    drop(source);
                    if let Some(tail) = &tail {
                        // The read response does not return the slot while the upload runs.
                        for _ in 0..64 {
                            tokio::task::yield_now().await;
                        }
                        assert_eq!(handle.available_capacity(), 0, "the upload is unfinished");
                        assert!(!handle.is_idle(), "the upload is unfinished");
                        tail.notify_one();
                    }
                    yield_until(|| handle.is_idle() || handle.is_finished()).await;
                    let reused = handle.available_capacity() == 1;
                    let decided = log.0.borrow().clone();
                    if !reused {
                        return (first, false, Vec::new(), decided);
                    }
                    let written = state.borrow().writes.concat().len();
                    let second = handle.reserve_request().unwrap().submit(
                        get_head(),
                        HttpSourceHolder::empty(),
                        CancellationToken::never(),
                    );
                    let (_stream, mut source, _control) = second.await.unwrap().into_source_parts();
                    second_ready.notify_one();
                    assert_eq!(read_response(&mut source).await.body, b"ok");
                    drop(source);
                    yield_until(|| handle.is_idle()).await;
                    assert!(handle.close_when_idle().unwrap());
                    let second_wire = state.borrow().writes.concat()[written..].to_vec();
                    (first, true, second_wire, decided)
                });
            let state = state.borrow();
            ReuseOutcome {
                first,
                reused,
                second_wire,
                wire: state.writes.concat(),
                log: decided,
                run: run.err().map(|error| error.code()),
                close: state.close,
            }
        })
    })
}

/// As `owned_reuse_outcome`, through a `UniplexSession` HTTP/1.1 client. Its connection
/// finishes on its own when it is not reused, so the decision is read once the session is
/// idle or finished and a bounded number of turns has passed.
fn uniplex_reuse_outcome(
    request: impl FnOnce() -> (HttpHeaderEvent, Vec<HttpBodyEvent>) + Send + 'static,
    response: &'static [u8],
    eof: bool,
    tail: Option<Arc<tokio::sync::Notify>>,
) -> ReuseOutcome {
    uniplex_reuse_outcome_with(upstream_config(), request, response, eof, tail)
}

/// As `uniplex_reuse_outcome`, with `config` for the session.
fn uniplex_reuse_outcome_with(
    config: UniplexConfig,
    request: impl FnOnce() -> (HttpHeaderEvent, Vec<HttpBodyEvent>) + Send + 'static,
    response: &'static [u8],
    eof: bool,
    tail: Option<Arc<tokio::sync::Notify>>,
) -> ReuseOutcome {
    run_on_host(move |context| {
        Box::pin(async move {
            let first_ready = Arc::new(tokio::sync::Notify::new());
            let second_ready = Arc::new(tokio::sync::Notify::new());
            let (transport, state) =
                ScriptedTransport::new(reuse_reads(&first_ready, &second_ready, response, eof));
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                transport,
                None,
                config,
            )
            .unwrap();
            let (head, body) = request();
            let (run, (first, reused, second_wire)) = futures::join!(session.run(), async {
                let mut response = handle
                    .send_request(
                        reuse_request_source(Some(head), body, tail.as_ref()),
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap();
                first_ready.notify_one();
                let first = read_response(&mut response.source).await;
                drop(response);
                if let Some(tail) = &tail {
                    for _ in 0..64 {
                        tokio::task::yield_now().await;
                    }
                    tail.notify_one();
                }
                yield_until(|| handle.is_idle() || handle.is_finished()).await;
                for _ in 0..64 {
                    tokio::task::yield_now().await;
                }
                if handle.is_finished() {
                    return (first, false, Vec::new());
                }
                let written = state.borrow().writes.concat().len();
                let mut response = handle
                    .send_request(
                        RequestSource::held(Some(get_head()), Vec::new()),
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap();
                second_ready.notify_one();
                assert_eq!(read_response(&mut response.source).await.body, b"ok");
                drop(response);
                yield_until(|| handle.is_idle()).await;
                assert!(handle.close_when_idle().unwrap());
                let second_wire = state.borrow().writes.concat()[written..].to_vec();
                (first, true, second_wire)
            });
            let state = state.borrow();
            ReuseOutcome {
                first,
                reused,
                second_wire,
                wire: state.writes.concat(),
                log: Vec::new(),
                run: run.err().map(|error| error.code()),
                close: state.close,
            }
        })
    })
}

#[test]
fn reusable_connection_returns_capacity_after_the_exchange() {
    let request = || scripted_request(false);
    let uniplex = uniplex_reuse_outcome(request, OK_RESPONSE, false, None);
    assert!(uniplex.reused, "Uniplex reuses a keep-alive connection");
    let owned = owned_reuse_outcome(request, OK_RESPONSE, false, None);
    assert_eq!(owned.first.body, b"ok");
    assert_eq!(owned.log, ["capacity_released(1)", "capacity_available"]);
    assert!(owned.reused, "the connection returns its slot and is idle");
    assert_eq!(
        String::from_utf8_lossy(&owned.second_wire),
        String::from_utf8_lossy(&uniplex.second_wire)
    );
    assert_eq!((owned.run, owned.close), (None, CloseKind::ShutdownWrite));
    assert_eq!(
        (uniplex.run, uniplex.close),
        (None, CloseKind::ShutdownWrite)
    );
}

/// Asserts that a connection whose first response is `response` is not reused, under both
/// sessions, and that the owned one closes admission and then shuts down its write side.
fn assert_not_reused(response: &'static [u8]) {
    let request = || scripted_request(false);
    let uniplex = uniplex_reuse_outcome(request, response, false, None);
    assert!(!uniplex.reused, "Uniplex does not reuse the connection");
    let owned = owned_reuse_outcome(request, response, false, None);
    assert_eq!(owned.first.body, b"ok");
    assert!(owned.first.eom);
    assert_eq!(owned.log, ["capacity_released(1)", "admission_closed"]);
    assert!(!owned.reused);
    assert_eq!((owned.run, owned.close), (None, CloseKind::ShutdownWrite));
    assert_eq!(
        (uniplex.run, uniplex.close),
        (None, CloseKind::ShutdownWrite)
    );
}

#[test]
fn connection_close_response_closes_admission() {
    assert_not_reused(CLOSE_RESPONSE);
}

#[test]
fn http10_response_closes_admission() {
    assert_not_reused(b"HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\nok");
}

#[test]
fn cancel_mid_response_closes_the_connection() {
    run_on_host(|context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            // A keep-alive response whose body is still arriving.
            let (transport, state) = ScriptedTransport::new([ReadAction::WaitData(
                Arc::clone(&gate),
                b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nhalf".to_vec(),
            )]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let log = Rc::new(AdmissionLog::default());
            handle.set_observer(log.clone()).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let (run, read) = futures::join!(session.run(), async {
                let (_stream, mut source, control) = submission.await.unwrap().into_source_parts();
                gate.notify_one();
                let mut head = source.read_header_event().await.unwrap();
                assert_eq!(head.headers_mut().status_code().unwrap(), 200);
                let event = source.read_body_event(u32::MAX).await.unwrap();
                assert!(!event.eom(), "the response is unfinished");
                // The caller's route timeout.
                control.cancel(HttpError::with_message(
                    HttpErrorCode::READ_TIMEOUT,
                    b"route timeout",
                ));
                loop {
                    match source.read_body_event(u32::MAX).await {
                        Ok(event) => assert!(!event.eom(), "the response is unfinished"),
                        Err(error) => break error.code(),
                    }
                }
            });
            assert_eq!(read, HttpErrorCode::READ_TIMEOUT);
            assert_eq!(
                run.expect_err("the cancel closes the connection").code(),
                HttpErrorCode::READ_TIMEOUT
            );
            assert_eq!(state.borrow().close, CloseKind::Reset);
            assert_eq!(
                *log.0.borrow(),
                ["admission_closed", "capacity_released(1)"],
                "no reuse: the slot never becomes available"
            );
            assert!(handle.is_finished());
        })
    });
}

#[test]
fn unfinished_upload_at_response_end_follows_uniplex() {
    // The origin responds after the first body bytes. The rest of the body follows once the
    // response has been read.
    let request = || scripted_request(true);
    let tail = || Some(Arc::new(tokio::sync::Notify::new()));
    let uniplex = uniplex_reuse_outcome(request, OK_RESPONSE, false, tail());
    assert!(
        uniplex.reused,
        "Uniplex finishes the upload and reuses the connection"
    );
    let owned = owned_reuse_outcome(request, OK_RESPONSE, false, tail());
    assert_eq!(owned.first, uniplex.first);
    assert_eq!(
        owned.reused, uniplex.reused,
        "the reuse decision equals Uniplex's"
    );
    assert_eq!(
        String::from_utf8_lossy(&owned.wire),
        String::from_utf8_lossy(&uniplex.wire),
        "both requests, the first one's late tail included"
    );
    assert_eq!((owned.run, owned.close), (uniplex.run, uniplex.close));
    if owned.reused {
        assert_eq!(owned.log, ["capacity_released(1)", "capacity_available"]);
    }
}

#[test]
fn close_when_idle_finishes_after_the_in_flight_exchange() {
    run_on_host(|context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            // A keep-alive origin that keeps the connection open.
            let (transport, state) = ScriptedTransport::new([ReadAction::WaitData(
                Arc::clone(&gate),
                OK_RESPONSE.to_vec(),
            )]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let log = Rc::new(AdmissionLog::default());
            handle.set_observer(log.clone()).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let (run, read) = futures::join!(session.run(), async {
                let (_stream, mut source, _control) = submission.await.unwrap().into_source_parts();
                assert!(handle.close_when_idle().unwrap());
                assert_eq!(*log.0.borrow(), ["admission_closed"]);
                assert_eq!(
                    handle.reserve_request().err().unwrap().code(),
                    HttpErrorCode::REFUSED_STREAM
                );
                assert!(!handle.is_finished(), "the exchange is in flight");
                gate.notify_one();
                read_response(&mut source).await
            });
            assert_eq!((read.body.as_slice(), read.eom), (b"ok".as_slice(), true));
            run.unwrap();
            assert_eq!(
                *log.0.borrow(),
                ["admission_closed", "capacity_released(1)"]
            );
            assert_eq!(state.borrow().close, CloseKind::ShutdownWrite);
            assert!(handle.is_finished());
        })
    });
}

#[test]
fn reuse_decisions_match_uniplex() {
    // (name, first response, whether the origin then sends EOF)
    let corpus: [(&str, &'static [u8], bool); 8] = [
        ("keep-alive", OK_RESPONSE, false),
        (
            "explicit keep-alive",
            b"HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 2\r\n\r\nok",
            false,
        ),
        ("close", CLOSE_RESPONSE, false),
        (
            "HTTP/1.0",
            b"HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\nok",
            false,
        ),
        (
            "HTTP/1.0 keep-alive",
            b"HTTP/1.0 200 OK\r\nConnection: keep-alive\r\nContent-Length: 2\r\n\r\nok",
            false,
        ),
        ("no length", b"HTTP/1.1 200 OK\r\n\r\nok", true),
        (
            "chunked",
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nok\r\n0\r\n\r\n",
            false,
        ),
        (
            "1xx then 200",
            b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok",
            false,
        ),
    ];
    for (name, response, eof) in corpus {
        let request = || scripted_request(false);
        let uniplex = uniplex_reuse_outcome(request, response, eof, None);
        let owned = owned_reuse_outcome(request, response, eof, None);
        assert_eq!(owned.first, uniplex.first, "{name}");
        assert_eq!(owned.first.body, b"ok", "{name}");
        assert_eq!(owned.reused, uniplex.reused, "{name}: the reuse decision");
        assert_eq!(
            owned.reused,
            owned.log.iter().any(|event| event == "capacity_available"),
            "{name}: {:?}",
            owned.log
        );
        assert_eq!(
            String::from_utf8_lossy(&owned.wire),
            String::from_utf8_lossy(&uniplex.wire),
            "{name}"
        );
        assert_eq!(
            (owned.run, owned.close),
            (uniplex.run, uniplex.close),
            "{name}"
        );
    }
}

#[test]
fn trailered_content_length_request_leaves_the_connection_reusable() {
    let request = || {
        let mut trailers = HttpHeaders::new();
        trailers.add(b"x-checksum", b"abc");
        (
            post_head(Some(5), false),
            vec![
                HttpBodyEvent::body(queue(b"hello"), false),
                HttpBodyEvent::trailers(trailers),
            ],
        )
    };
    let owned = owned_reuse_outcome(request, OK_RESPONSE, false, None);
    assert_eq!(owned.log, ["capacity_released(1)", "capacity_available"]);
    assert!(
        owned.reused,
        "the request ended with its body, so the codec is between messages"
    );
    // The trailers are not written, so the first request is its head and body.
    let first = [declared_head_wire(5), b"hello".to_vec()].concat();
    assert_eq!(
        String::from_utf8_lossy(&owned.wire[..owned.wire.len() - owned.second_wire.len()]),
        String::from_utf8_lossy(&first)
    );
    // Uniplex fails this request at EOF
    // (`trailers_on_a_content_length_request_end_it_without_them`), so the second request is
    // compared with Uniplex's after an untrailered first request.
    let uniplex = uniplex_reuse_outcome(|| scripted_request(false), OK_RESPONSE, false, None);
    assert_eq!(
        String::from_utf8_lossy(&owned.second_wire),
        String::from_utf8_lossy(&uniplex.second_wire)
    );
    assert_eq!((owned.run, owned.close), (None, CloseKind::ShutdownWrite));
}

#[test]
fn unread_response_holds_the_slot_unlike_uniplex() {
    // Uniplex releases a keep-alive connection's capacity once the response's EOM is parsed, so
    // a second request can go out while the first response is unread.
    let uniplex_available = run_on_host(|context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            let (transport, _state) = ScriptedTransport::new([ReadAction::WaitData(
                Arc::clone(&gate),
                OK_RESPONSE.to_vec(),
            )]);
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                transport,
                None,
                upstream_config(),
            )
            .unwrap();
            let (run, available) = futures::join!(session.run(), async {
                let response = handle
                    .send_request(
                        RequestSource::held(Some(get_head()), Vec::new()),
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap();
                gate.notify_one();
                response
                    .source
                    .ingress_completion()
                    .unwrap()
                    .wait()
                    .await
                    .unwrap();
                let available = handle.outgoing_transaction_activity().available();
                drop(response);
                yield_until(|| handle.is_idle()).await;
                assert!(handle.close_when_idle().unwrap());
                available
            });
            run.unwrap();
            available
        })
    });
    assert_eq!(uniplex_available, 1);
    // The owned driver returns the slot once the response is read: its connection carries one
    // exchange at a time.
    run_on_host(|context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            let (transport, _state) = ScriptedTransport::new([ReadAction::WaitData(
                Arc::clone(&gate),
                OK_RESPONSE.to_vec(),
            )]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let (run, ()) = futures::join!(session.run(), async {
                let response = submission.await.unwrap();
                gate.notify_one();
                yield_until(|| response.control.capacity_released()).await;
                for _ in 0..64 {
                    tokio::task::yield_now().await;
                }
                assert_eq!(handle.available_capacity(), 0, "the response is unread");
                let (_stream, mut source, _control) = response.into_source_parts();
                assert_eq!(read_response(&mut source).await.body, b"ok");
                drop(source);
                close_once_idle(&handle).await;
            });
            run.unwrap();
        })
    });
}

/// An upstream observer that submits a `GET /` from `capacity_available`, as a pool that
/// dispatches a waiting request the moment a connection frees does.
struct SubmitOnCapacity {
    handle: RefCell<Option<crate::owned_h1::OwnedH1UpstreamHandle>>,
    submission: RefCell<Option<crate::owned_h1::OwnedH1UpstreamSubmission>>,
}

impl crate::owned::OwnedUpstreamObserver for SubmitOnCapacity {
    fn capacity_available(&self) {
        let handle = self.handle.borrow().clone();
        if let Some(handle) = handle {
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            *self.submission.borrow_mut() = Some(submission);
        }
    }
}

#[test]
fn bytes_behind_a_response_are_not_read_as_the_next_response() {
    run_on_host(|context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            // One request, but the origin sends two responses in one read.
            let (transport, state) = ScriptedTransport::new([ReadAction::WaitData(
                Arc::clone(&gate),
                [OK_RESPONSE, OK_RESPONSE].concat(),
            )]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let observer = Rc::new(SubmitOnCapacity {
                handle: RefCell::new(Some(handle.clone())),
                submission: RefCell::new(None),
            });
            handle.set_observer(observer.clone()).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let (run, ()) = futures::join!(session.run(), async {
                let (_stream, mut source, _control) = submission.await.unwrap().into_source_parts();
                gate.notify_one();
                let read = read_response(&mut source).await;
                assert_eq!((read.body.as_slice(), read.eom), (b"ok".as_slice(), true));
            });
            observer.handle.take();
            assert_eq!(
                run.expect_err("bytes behind the response fail the connection")
                    .code(),
                HttpErrorCode::PROTOCOL_ERROR
            );
            let wire = state.borrow().writes.concat();
            assert_eq!(
                wire.windows(4).filter(|window| window == b"GET ").count(),
                1,
                "no second request is written: {}",
                String::from_utf8_lossy(&wire)
            );
            if let Some(second) = observer.submission.take() {
                assert!(second.await.is_err(), "the second request gets no response");
            }
            assert_eq!(state.borrow().close, CloseKind::Reset);
        })
    });
}

/// The unread body source of a bodyless request, dropped when its exchange retires: the drop
/// panics.
struct PanicOnRetirement;

impl HttpSource for PanicOnRetirement {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        unreachable!("an owned submission takes the head separately")
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        unreachable!("a bodyless request reads no body")
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

impl Drop for PanicOnRetirement {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            panic!("request source panicked at retirement");
        }
    }
}

/// A panic as a finished exchange retires is counted at exchange scope, and its keep-alive
/// connection still returns its slot and serves the next request.
#[cfg(panic = "unwind")]
#[test]
fn a_retirement_panic_keeps_a_reusable_connection() {
    run_on_host(|context| {
        Box::pin(async move {
            take_contained_panics();
            let worker = context.handle().clone();
            let start = worker.poll_stats();
            let first_ready = Arc::new(tokio::sync::Notify::new());
            let second_ready = Arc::new(tokio::sync::Notify::new());
            // A keep-alive origin that keeps the connection open.
            let (transport, state) = ScriptedTransport::new(reuse_reads(
                &first_ready,
                &second_ready,
                OK_RESPONSE,
                false,
            ));
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let log = Rc::new(AdmissionLog::default());
            handle.set_observer(log.clone()).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::new(Box::new(PanicOnRetirement)),
                CancellationToken::never(),
            );
            let (run, (first, second)) = futures::join!(session.run(), async {
                let (_stream, mut source, _control) = submission.await.unwrap().into_source_parts();
                first_ready.notify_one();
                let first = read_response(&mut source).await;
                drop(source);
                yield_until(|| handle.is_idle()).await;
                let submission = handle.reserve_request().unwrap().submit(
                    get_head(),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                let (_stream, mut source, _control) = submission.await.unwrap().into_source_parts();
                second_ready.notify_one();
                let second = read_response(&mut source).await;
                drop(source);
                close_once_idle(&handle).await;
                (first, second)
            });
            run.unwrap();
            assert_eq!((first.body.as_slice(), first.eom), (b"ok".as_slice(), true));
            assert_eq!(
                (second.body.as_slice(), second.eom),
                (b"ok".as_slice(), true)
            );
            assert_eq!(
                *log.0.borrow(),
                [
                    "capacity_released(1)",
                    "capacity_available",
                    "capacity_released(2)",
                    "capacity_available",
                    "admission_closed"
                ],
                "the slot is returned after the retirement panic"
            );
            assert_eq!(state.borrow().close, CloseKind::ShutdownWrite);
            assert_eq!(
                take_contained_panics(),
                [(
                    PanicSite::ExchangeRetirement,
                    Some(StreamId::new(1)),
                    Some("request source panicked at retirement".to_owned())
                )]
            );
            assert_eq!(contained_panic_counts(&worker, &start), (1, 0, 0));
        })
    });
}

/// Where the origin's EOF falls in a request that gets no response byte.
#[derive(Clone, Copy, Debug)]
enum EofPoint {
    /// The origin's first read is EOF, before the request is committed.
    BeforeCommit,
    /// A bodyless request's head is written.
    AfterHead,
    /// A `Content-Length` request's head and body are written.
    AfterBody,
    /// A chunked request's first body bytes are written; its last body event never comes.
    MidUpload,
}

impl EofPoint {
    /// The request's head, and a source of its body whose last event waits for `tail` in
    /// `MidUpload`; with `head_in_source`, the source yields the head instead (a
    /// `UniplexSession` client reads it from there).
    fn request(
        self,
        head_in_source: bool,
        tail: &Arc<tokio::sync::Notify>,
    ) -> (Option<HttpHeaderEvent>, HttpSourceHolder) {
        let (head, body) = match self {
            Self::BeforeCommit | Self::AfterHead => (get_head(), Vec::new()),
            Self::AfterBody => scripted_request(false),
            Self::MidUpload => scripted_request(true),
        };
        let tail = matches!(self, Self::MidUpload).then_some(tail);
        if head_in_source {
            (None, reuse_request_source(Some(head), body, tail))
        } else {
            (Some(head), reuse_request_source(None, body, tail))
        }
    }

    /// The origin's reads: EOF, released by `gate` unless it is the first read.
    fn reads(self, gate: &Arc<tokio::sync::Notify>) -> Vec<ReadAction> {
        match self {
            Self::BeforeCommit => vec![ReadAction::Eof],
            _ => vec![
                ReadAction::WaitData(Arc::clone(gate), Vec::new()),
                ReadAction::Eof,
            ],
        }
    }

    /// The bytes on the wire once the EOF is released.
    fn written(self) -> &'static [u8] {
        match self {
            Self::BeforeCommit | Self::AfterHead => b"\r\n\r\n",
            Self::AfterBody => b"hello world",
            Self::MidUpload => b"hello ",
        }
    }
}

/// The turns a session gets to finish after the origin's EOF.
const EOF_TURNS: usize = 10_000;

/// What a session did with a request that got EOF and no response byte.
#[derive(Debug, PartialEq)]
struct EofOutcome {
    run: Option<HttpErrorCode>,
    /// What the response source yielded, or the submission's error.
    read: ReadResponse,
    wire: Vec<u8>,
}

/// Waits, a bounded number of turns, until `state`'s writes contain `written`, then releases
/// the origin's EOF.
async fn release_eof_once_written(
    state: &Rc<RefCell<TransportState>>,
    written: &[u8],
    gate: &tokio::sync::Notify,
) {
    for _ in 0..EOF_TURNS {
        if state
            .borrow()
            .writes
            .concat()
            .windows(written.len())
            .any(|window| window == written)
        {
            gate.notify_one();
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("the request's bytes were never written");
}

/// An owned upstream session's outcome for `point`, or `None` if it had not finished
/// `EOF_TURNS` turns after the EOF (it is then cancelled, so the test does not hang).
fn owned_eof_outcome(point: EofPoint) -> Option<EofOutcome> {
    run_on_host(move |context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            let tail = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new(point.reads(&gate));
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let (head, source) = point.request(false, &tail);
            let submission = handle.reserve_request().unwrap().submit(
                head.expect("an owned submission takes the head separately"),
                source,
                CancellationToken::never(),
            );
            let released = Rc::new(Cell::new(false));
            let work = futures::future::join(session.run(), {
                let released = Rc::clone(&released);
                let state = Rc::clone(&state);
                async move {
                    let response = match submission.await {
                        Ok(response) => response,
                        Err(error) => {
                            released.set(true);
                            return ReadResponse {
                                error: Some(error.code()),
                                ..ReadResponse::default()
                            };
                        }
                    };
                    let (_stream, mut source, _control) = response.into_source_parts();
                    release_eof_once_written(&state, point.written(), &gate).await;
                    released.set(true);
                    read_response(&mut source).await
                }
            });
            let bound = async {
                while !released.get() {
                    tokio::task::yield_now().await;
                }
                for _ in 0..EOF_TURNS {
                    tokio::task::yield_now().await;
                }
            };
            futures::pin_mut!(work);
            futures::pin_mut!(bound);
            match futures::future::select(work, bound).await {
                futures::future::Either::Left(((run, read), _)) => Some(EofOutcome {
                    run: run.err().map(|error| error.code()),
                    read,
                    wire: state.borrow().writes.concat(),
                }),
                futures::future::Either::Right((_, work)) => {
                    handle.cancel();
                    let (_run, _read) = work.await;
                    None
                }
            }
        })
    })
}

/// A `UniplexSession` HTTP/1.1 client's outcome for `point`.
fn uniplex_eof_outcome(point: EofPoint) -> EofOutcome {
    run_on_host(move |context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            let tail = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new(point.reads(&gate));
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                transport,
                None,
                upstream_config(),
            )
            .unwrap();
            let (_head, source) = point.request(true, &tail);
            let (run, read) = futures::join!(session.run(), async {
                match handle
                    .send_request(source, CancellationToken::never())
                    .await
                {
                    Ok(mut response) => {
                        release_eof_once_written(&state, point.written(), &gate).await;
                        read_response(&mut response.source).await
                    }
                    Err(error) => ReadResponse {
                        error: Some(error.code()),
                        ..ReadResponse::default()
                    },
                }
            });
            EofOutcome {
                run: run.err().map(|error| error.code()),
                read,
                wire: state.borrow().writes.concat(),
            }
        })
    })
}

#[test]
fn eof_before_a_response_fails_the_request_as_uniplex() {
    for point in [
        EofPoint::AfterHead,
        EofPoint::AfterBody,
        EofPoint::MidUpload,
    ] {
        let owned = owned_eof_outcome(point).unwrap_or_else(|| {
            panic!("{point:?}: the owned session had not finished {EOF_TURNS} turns after EOF")
        });
        let uniplex = uniplex_eof_outcome(point);
        assert_eq!(
            owned.read.error,
            Some(HttpErrorCode::TRANSPORT_EOF),
            "{point:?}: the request fails with TRANSPORT_EOF"
        );
        assert_eq!(
            (owned.run, &owned.read),
            (uniplex.run, &uniplex.read),
            "{point:?}: the run and the response match Uniplex"
        );
        assert_eq!(
            String::from_utf8_lossy(&owned.wire),
            String::from_utf8_lossy(&uniplex.wire),
            "{point:?}"
        );
    }
    // Before commitment, EOF closes admission, so the submission fails and the session does not
    // write. `UniplexSession` differs here (it writes the request after the EOF, with
    // `Connection: close`), so this case is checked on its own, not compared.
    let owned = owned_eof_outcome(EofPoint::BeforeCommit).expect("the owned session finishes");
    assert_eq!(
        owned,
        EofOutcome {
            run: None,
            read: ReadResponse {
                error: Some(HttpErrorCode::TRANSPORT_EOF),
                ..ReadResponse::default()
            },
            wire: Vec::new(),
        }
    );
}

/// `upstream_options` under `Http1UpgradePolicy::AsOrdinaryRequest`.
fn ordinary_options(context: WorkerContext, transport: Rc<dyn UniplexTransport>) -> OwnedH1Options {
    let mut options = upstream_options(context, transport);
    options.http1.upgrade_policy = crate::owned_h1::Http1UpgradePolicy::AsOrdinaryRequest;
    options
}

/// A request head with `headers` in order, marked chunked when `chunked`.
fn request_head(
    method: HttpMethod,
    url: &[u8],
    minor: u8,
    headers: &[(&str, &str)],
    chunked: bool,
    eom: bool,
) -> HttpHeaderEvent {
    let mut message = roxygen_core::message::HttpMessage::new();
    message.set_method(method).unwrap();
    message.set_url(url, false).unwrap();
    message.set_http_version(1, minor);
    for (name, value) in headers {
        message.headers_mut().add(name.as_bytes(), value.as_bytes());
    }
    message.set_chunked(chunked);
    HttpHeaderEvent::new(message, eom).unwrap()
}

/// A `CONNECT h:443`, with `extra` after its `Host`.
fn ordinary_connect(extra: &[(&str, &str)], chunked: bool, eom: bool) -> HttpHeaderEvent {
    let headers = [&[("Host", "h:443")], extra].concat();
    request_head(HttpMethod::Connect, b"h:443", 1, &headers, chunked, eom)
}

/// A WebSocket GET, with `extra` after its own headers.
fn ordinary_websocket(extra: &[(&str, &str)], chunked: bool, eom: bool) -> HttpHeaderEvent {
    let own: &[(&str, &str)] = &[
        ("Host", "c1"),
        ("Upgrade", "websocket"),
        ("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ=="),
        ("sec-websocket-version", "13"),
        ("Connection", "upgrade"),
    ];
    let headers = [own, extra].concat();
    request_head(HttpMethod::Get, b"/ws", 1, &headers, chunked, eom)
}

/// The bytes the origin reads for `ordinary_connect(&[], false, true)`.
const ORDINARY_CONNECT_WIRE: &[u8] =
    b"CONNECT h:443 HTTP/1.1\r\nHost: h:443\r\nConnection: keep-alive\r\n\r\n";
/// The bytes the origin reads for `ordinary_websocket(&[], false, true)`.
const ORDINARY_WEBSOCKET_WIRE: &[u8] = b"GET /ws HTTP/1.1\r\nHost: c1\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nsec-websocket-version: 13\r\nConnection: upgrade, keep-alive\r\n\r\n";

type Request = fn() -> (HttpHeaderEvent, Vec<HttpBodyEvent>);

/// What `ordinary_exchanges` observed.
struct OrdinaryRun {
    run: Option<HttpErrorCode>,
    outcomes: Vec<Result<ReadResponse, HttpErrorCode>>,
    wire: Vec<u8>,
    close: CloseKind,
}

/// Submits each request in turn, once the connection is idle, to one owned upstream connection
/// under `Http1UpgradePolicy::AsOrdinaryRequest`, then closes it once idle. The origin responds
/// to each committed request with the next given response. A request that is expected to be
/// rejected has `None`.
fn ordinary_exchanges(exchanges: Vec<(Request, Option<&'static [u8]>)>) -> OrdinaryRun {
    run_on_host(move |context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            let reads: Vec<ReadAction> = exchanges
                .iter()
                .filter_map(|(_, response)| {
                    response.map(|bytes| {
                        ReadAction::WaitDataOrCancellation(Arc::clone(&gate), bytes.to_vec())
                    })
                })
                .collect();
            let (transport, state) = ScriptedTransport::new(reads);
            let (session, handle) =
                OwnedH1UpstreamSession::new(ordinary_options(context, transport)).unwrap();
            let (run, outcomes) = futures::join!(session.run(), async {
                let mut outcomes = Vec::new();
                for (request, _) in exchanges {
                    yield_until(|| handle.is_idle()).await;
                    let (head, body) = request();
                    let submission = handle.reserve_request().unwrap().submit(
                        head,
                        RequestSource::held(None, body),
                        CancellationToken::never(),
                    );
                    outcomes.push(match submission.await {
                        Ok(response) => {
                            let (_stream, mut source, _control) = response.into_source_parts();
                            gate.notify_one();
                            Ok(read_response(&mut source).await)
                        }
                        Err(error) => Err(error.code()),
                    });
                }
                yield_until(|| handle.is_idle()).await;
                assert!(handle.close_when_idle().unwrap());
                outcomes
            });
            let state = state.borrow();
            OrdinaryRun {
                run: run.err().map(|error| error.code()),
                outcomes,
                wire: state.writes.concat(),
                close: state.close,
            }
        })
    })
}

/// A complete `200` response with body `ok`, as the reader receives it.
fn ok_read() -> ReadResponse {
    ReadResponse {
        statuses: vec![200],
        body: b"ok".to_vec(),
        eom: true,
        ..ReadResponse::default()
    }
}

#[test]
fn upgrade_as_ordinary_request_reaches_the_origin() {
    const UPGRADE_OK: &[u8] =
        b"HTTP/1.1 200 OK\r\nUpgrade: websocket\r\nContent-Length: 2\r\n\r\nok";
    let exchanges: Vec<(&str, Request, &'static [u8], &[u8])> = vec![
        (
            "the consumer's CONNECT",
            || (ordinary_connect(&[], false, true), Vec::new()),
            OK_RESPONSE,
            ORDINARY_CONNECT_WIRE,
        ),
        (
            "the consumer's WebSocket GET, answered with an Upgrade header",
            || (ordinary_websocket(&[], false, true), Vec::new()),
            UPGRADE_OK,
            ORDINARY_WEBSOCKET_WIRE,
        ),
        (
            "a CONNECT with a Content-Length body",
            || {
                (
                    ordinary_connect(&[("Content-Length", "5")], false, false),
                    vec![HttpBodyEvent::body(queue(b"hello"), true)],
                )
            },
            OK_RESPONSE,
            b"CONNECT h:443 HTTP/1.1\r\nHost: h:443\r\nConnection: keep-alive\r\nContent-Length: 5\r\n\r\nhello",
        ),
        (
            "a chunked CONNECT",
            || {
                (
                    ordinary_connect(&[("Transfer-Encoding", "chunked")], true, false),
                    vec![HttpBodyEvent::body(queue(b"hello"), true)],
                )
            },
            OK_RESPONSE,
            b"CONNECT h:443 HTTP/1.1\r\nHost: h:443\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n5\r\nhello\r\n0\r\n\r\n",
        ),
        (
            "a WebSocket GET with a Content-Length body",
            || {
                (
                    ordinary_websocket(&[("Content-Length", "2")], false, false),
                    vec![HttpBodyEvent::body(queue(b"hi"), true)],
                )
            },
            OK_RESPONSE,
            b"GET /ws HTTP/1.1\r\nHost: c1\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nsec-websocket-version: 13\r\nConnection: upgrade, keep-alive\r\nContent-Length: 2\r\n\r\nhi",
        ),
        (
            "a POST with an Upgrade header, which the codec chunks",
            || {
                (
                    request_head(
                        HttpMethod::Post,
                        b"/up",
                        1,
                        &[("Host", "origin"), ("Upgrade", "h2c"), ("Connection", "Upgrade")],
                        false,
                        false,
                    ),
                    vec![HttpBodyEvent::body(queue(b"abc"), true)],
                )
            },
            OK_RESPONSE,
            b"POST /up HTTP/1.1\r\nHost: origin\r\nUpgrade: h2c\r\nTransfer-Encoding: chunked\r\nConnection: Upgrade, keep-alive\r\n\r\n3\r\nabc\r\n0\r\n\r\n",
        ),
        (
            "a GET on the same connection",
            || (get_head(), Vec::new()),
            OK_RESPONSE,
            b"GET / HTTP/1.1\r\nHost: origin\r\nConnection: keep-alive\r\n\r\n",
        ),
    ];
    let expected_wire: Vec<u8> = exchanges
        .iter()
        .flat_map(|(.., wire)| wire.to_vec())
        .collect();
    let owned = ordinary_exchanges(
        exchanges
            .iter()
            .map(|(_, request, response, _)| (*request, Some(*response)))
            .collect(),
    );
    for ((name, ..), outcome) in exchanges.iter().zip(&owned.outcomes) {
        assert_eq!(outcome, &Ok(ok_read()), "{name}");
    }
    assert_eq!(owned.outcomes.len(), exchanges.len());
    assert_eq!(owned.run, None);
    assert_eq!(
        String::from_utf8_lossy(&owned.wire),
        String::from_utf8_lossy(&expected_wire)
    );
    assert_eq!(
        owned.close,
        CloseKind::ShutdownWrite,
        "the connection stayed reusable"
    );
}

/// What `ordinary_outcome` observed: the response the caller read, or the submission's error.
#[derive(Debug, PartialEq)]
struct OrdinaryOutcome {
    submitted: Result<ReadResponse, HttpErrorCode>,
    run: Option<HttpErrorCode>,
    wire: Vec<u8>,
    close: CloseKind,
}

/// One request through an owned upstream connection under
/// `Http1UpgradePolicy::AsOrdinaryRequest`. The origin responds with `response` once the
/// request is committed, then sends EOF. A rejected request leaves the connection to be closed
/// once idle.
fn ordinary_outcome(request: Request, response: &'static [u8]) -> OrdinaryOutcome {
    run_on_host(move |context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([
                ReadAction::WaitDataOrCancellation(Arc::clone(&gate), response.to_vec()),
                ReadAction::Eof,
            ]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(ordinary_options(context, transport)).unwrap();
            let (head, body) = request();
            let submission = handle.reserve_request().unwrap().submit(
                head,
                RequestSource::held(None, body),
                CancellationToken::never(),
            );
            let (run, submitted) = futures::join!(session.run(), async {
                match submission.await {
                    Ok(response) => {
                        let (_stream, mut source, _control) = response.into_source_parts();
                        gate.notify_one();
                        Ok(read_response(&mut source).await)
                    }
                    Err(error) => {
                        assert_eq!(handle.available_capacity(), 1, "the slot returns");
                        assert!(handle.close_when_idle().unwrap());
                        Err(error.code())
                    }
                }
            });
            let state = state.borrow();
            OrdinaryOutcome {
                submitted,
                run: run.err().map(|error| error.code()),
                wire: state.writes.concat(),
                close: state.close,
            }
        })
    })
}

#[test]
fn upgrade_as_ordinary_refuses_a_101() {
    let h2c: Request = || {
        (
            request_head(
                HttpMethod::Get,
                b"/",
                1,
                &[
                    ("Host", "origin"),
                    ("Connection", "Upgrade"),
                    ("Upgrade", "h2c"),
                ],
                false,
                true,
            ),
            Vec::new(),
        )
    };
    let cases: [(&str, Request, &[u8], &[u8]); 3] = [
        (
            "the consumer's CONNECT",
            || (ordinary_connect(&[], false, true), Vec::new()),
            b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\nraw",
            ORDINARY_CONNECT_WIRE,
        ),
        (
            "the consumer's WebSocket GET",
            || (ordinary_websocket(&[], false, true), Vec::new()),
            b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\nraw",
            ORDINARY_WEBSOCKET_WIRE,
        ),
        (
            "an h2c GET",
            h2c,
            b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: h2c\r\nConnection: Upgrade\r\n\r\nraw",
            b"GET / HTTP/1.1\r\nHost: origin\r\nUpgrade: h2c\r\nConnection: Upgrade, keep-alive\r\n\r\n",
        ),
    ];
    for (name, request, response, wire) in cases {
        let owned = ordinary_outcome(request, response);
        assert_eq!(
            owned,
            OrdinaryOutcome {
                submitted: Ok(ReadResponse {
                    error: Some(HttpErrorCode::HEADER_PARSE_ERROR),
                    ..ReadResponse::default()
                }),
                run: Some(HttpErrorCode::HEADER_PARSE_ERROR),
                wire: wire.to_vec(),
                close: CloseKind::Reset,
            },
            "{name}: the 101 fails the response and the connection"
        );
    }
    // Interim responses before the final one are read as for any request.
    let owned = ordinary_outcome(
        || (ordinary_connect(&[], false, true), Vec::new()),
        b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 103 Early Hints\r\nLink: </a>\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok",
    );
    assert_eq!(
        owned.submitted,
        Ok(ReadResponse {
            statuses: vec![100, 103, 200],
            ..ok_read()
        })
    );
    assert_eq!(owned.run, None);
}

/// The bytes the origin reads for `get_head`.
const GET_WIRE: &[u8] = b"GET / HTTP/1.1\r\nHost: origin\r\nConnection: keep-alive\r\n\r\n";

/// Asserts that, under `AsOrdinaryRequest`, `refused` fails with `code` before any byte is
/// written, the slot returns, and a GET then completes on the same connection.
fn assert_ordinary_refusal(name: &str, refused: Request, code: HttpErrorCode) {
    let owned = ordinary_exchanges(vec![
        (refused, None),
        (|| (get_head(), Vec::new()), Some(OK_RESPONSE)),
    ]);
    assert_eq!(owned.outcomes, [Err(code), Ok(ok_read())], "{name}");
    assert_eq!(owned.run, None, "{name}");
    assert_eq!(
        String::from_utf8_lossy(&owned.wire),
        String::from_utf8_lossy(GET_WIRE),
        "{name}: no byte of the refused request"
    );
    assert_eq!(owned.close, CloseKind::ShutdownWrite, "{name}");
}

#[test]
fn upgrade_as_ordinary_still_refuses_the_egress_websocket_flag() {
    let cases: [(&str, Request); 3] = [
        ("the flag", || {
            let mut head = get_head();
            head.headers_mut().set_egress_websocket_upgrade();
            (head, Vec::new())
        }),
        ("the flag with an Upgrade header", || {
            let mut head = ordinary_websocket(&[], false, true);
            head.headers_mut().set_egress_websocket_upgrade();
            (head, Vec::new())
        }),
        ("the flag on a request with a body", || {
            let mut head = post_head(Some(2), false);
            head.headers_mut().set_egress_websocket_upgrade();
            (head, vec![HttpBodyEvent::body(queue(b"hi"), true)])
        }),
    ];
    for (name, request) in cases {
        assert_ordinary_refusal(name, request, HttpErrorCode::HEADER_PARSE_ERROR);
    }
}

/// A body that holds a last chunk and then a whole request.
const SMUGGLED_BODY: &[u8] = b"0\r\n\r\nGET /smuggled HTTP/1.1\r\nHost: origin\r\n\r\n";

#[test]
fn upgrade_as_ordinary_refuses_an_unframed_connect_body() {
    let cases: [(&str, Request, HttpErrorCode); 8] = [
        (
            "a body with neither framing",
            || {
                (
                    ordinary_connect(&[], false, false),
                    vec![HttpBodyEvent::body(queue(b"hello"), true)],
                )
            },
            HttpErrorCode::HEADER_PARSE_ERROR,
        ),
        (
            "Transfer-Encoding: chunked on a message not marked chunked",
            || {
                (
                    ordinary_connect(&[("Transfer-Encoding", "chunked")], false, false),
                    vec![HttpBodyEvent::body(queue(SMUGGLED_BODY), true)],
                )
            },
            HttpErrorCode::HEADER_PARSE_ERROR,
        ),
        (
            "Content-Length: 0 then Content-Length: 5 on a head that ends the request",
            || {
                (
                    ordinary_connect(
                        &[("Content-Length", "0"), ("Content-Length", "5")],
                        false,
                        true,
                    ),
                    Vec::new(),
                )
            },
            HttpErrorCode::HEADER_PARSE_ERROR,
        ),
        (
            "two agreeing Content-Length lines",
            || {
                (
                    ordinary_connect(
                        &[("Content-Length", "5"), ("Content-Length", "5")],
                        false,
                        false,
                    ),
                    vec![HttpBodyEvent::body(queue(b"hello"), true)],
                )
            },
            HttpErrorCode::HEADER_PARSE_ERROR,
        ),
        (
            "a Content-Length that is not a decimal length",
            || {
                (
                    ordinary_connect(&[("Content-Length", "5x")], false, false),
                    vec![HttpBodyEvent::body(queue(b"hello"), true)],
                )
            },
            HttpErrorCode::HEADER_PARSE_ERROR,
        ),
        (
            "Content-Length: 5 on a head that ends the request, as for every request",
            || {
                (
                    ordinary_connect(&[("Content-Length", "5")], false, true),
                    Vec::new(),
                )
            },
            HttpErrorCode::CONTENT_LENGTH_MISMATCH,
        ),
        (
            "a chunked HTTP/1.0 CONNECT, which the codec cannot chunk",
            || {
                (
                    request_head(
                        HttpMethod::Connect,
                        b"h:443",
                        0,
                        &[("Host", "h:443"), ("Transfer-Encoding", "chunked")],
                        true,
                        false,
                    ),
                    vec![HttpBodyEvent::body(queue(b"hello"), true)],
                )
            },
            HttpErrorCode::HEADER_PARSE_ERROR,
        ),
        (
            "an HTTP/1.0 CONNECT whose written gzip line sits beside its Content-Length",
            || {
                (
                    request_head(
                        HttpMethod::Connect,
                        b"h:443",
                        0,
                        &[
                            ("Host", "h:443"),
                            ("Transfer-Encoding", "chunked"),
                            ("Transfer-Encoding", "gzip"),
                            ("Content-Length", "5"),
                        ],
                        true,
                        false,
                    ),
                    vec![HttpBodyEvent::body(queue(b"hello"), true)],
                )
            },
            HttpErrorCode::HEADER_PARSE_ERROR,
        ),
    ];
    for (name, request, code) in cases {
        assert_ordinary_refusal(name, request, code);
    }
}

#[test]
fn upgrade_as_ordinary_refuses_an_unframed_upgrade_body() {
    let cases: [(&str, Request, HttpErrorCode); 8] = [
        (
            "a body with neither framing",
            || {
                (
                    ordinary_websocket(&[], false, false),
                    vec![HttpBodyEvent::body(queue(b"hello"), true)],
                )
            },
            HttpErrorCode::HEADER_PARSE_ERROR,
        ),
        (
            "Transfer-Encoding: chunked on a message not marked chunked",
            || {
                (
                    ordinary_websocket(&[("Transfer-Encoding", "chunked")], false, false),
                    vec![HttpBodyEvent::body(queue(SMUGGLED_BODY), true)],
                )
            },
            HttpErrorCode::HEADER_PARSE_ERROR,
        ),
        (
            "a Transfer-Encoding: chunked head that ends the request",
            || {
                (
                    ordinary_websocket(&[("Transfer-Encoding", "chunked")], false, true),
                    Vec::new(),
                )
            },
            HttpErrorCode::HEADER_PARSE_ERROR,
        ),
        (
            "two agreeing Content-Length: 2 lines over a longer body",
            || {
                (
                    ordinary_websocket(
                        &[("Content-Length", "2"), ("Content-Length", "2")],
                        false,
                        false,
                    ),
                    vec![HttpBodyEvent::body(
                        queue(b"hiGET /smuggled HTTP/1.1\r\n\r\n"),
                        true,
                    )],
                )
            },
            HttpErrorCode::HEADER_PARSE_ERROR,
        ),
        (
            "a second written Transfer-Encoding line",
            || {
                (
                    ordinary_websocket(
                        &[
                            ("Transfer-Encoding", "chunked"),
                            ("Transfer-Encoding", "gzip"),
                        ],
                        true,
                        false,
                    ),
                    vec![HttpBodyEvent::body(queue(b"hello"), true)],
                )
            },
            HttpErrorCode::HEADER_PARSE_ERROR,
        ),
        (
            "a Content-Length beside a chunked line",
            || {
                (
                    ordinary_websocket(
                        &[("Transfer-Encoding", "chunked"), ("Content-Length", "5")],
                        true,
                        false,
                    ),
                    vec![HttpBodyEvent::body(queue(b"hello"), true)],
                )
            },
            HttpErrorCode::HEADER_PARSE_ERROR,
        ),
        (
            "an HTTP/1.0 POST with an Upgrade header and no length, which the codec cannot chunk",
            || {
                (
                    request_head(
                        HttpMethod::Post,
                        b"/up",
                        0,
                        &[
                            ("Host", "origin"),
                            ("Upgrade", "h2c"),
                            ("Connection", "Upgrade"),
                        ],
                        false,
                        false,
                    ),
                    vec![HttpBodyEvent::body(queue(b"abc"), true)],
                )
            },
            HttpErrorCode::HEADER_PARSE_ERROR,
        ),
        (
            "Content-Length: 2 on a head that ends the request, as for every request",
            || {
                (
                    ordinary_websocket(&[("Content-Length", "2")], false, true),
                    Vec::new(),
                )
            },
            HttpErrorCode::CONTENT_LENGTH_MISMATCH,
        ),
    ];
    for (name, request, code) in cases {
        assert_ordinary_refusal(name, request, code);
    }
}

/// The turns a session gets to take an origin's bytes, and the end behind them, before the
/// reader in a `deliver_ingress_before_abort` comparison starts.
const SETTLE_TURNS: usize = 1_000;

async fn settle() {
    for _ in 0..SETTLE_TURNS {
        tokio::task::yield_now().await;
    }
}

/// How the origin's response to a GET ends in the `deliver_ingress_before_abort` comparisons.
#[derive(Clone, Copy, Debug)]
enum ResponseEnd {
    /// A `Content-Length: 9` body cut short by EOF after `first`.
    LengthBodyEof,
    /// A chunked body cut short by EOF after its `first` chunk.
    ChunkedBodyEof,
    /// A head declaring a body, then EOF.
    HeadEof,
    /// A `Content-Length: 9` body cut short after `first` by a reset connection's failed read.
    LengthBodyReset,
    /// A chunked body whose second chunk size does not parse.
    ChunkedBodyParseError,
    /// A complete response, then EOF.
    Complete,
}

impl ResponseEnd {
    const ALL: [Self; 6] = [
        Self::LengthBodyEof,
        Self::ChunkedBodyEof,
        Self::HeadEof,
        Self::LengthBodyReset,
        Self::ChunkedBodyParseError,
        Self::Complete,
    ];

    /// The origin's reads: the response's bytes, released by `gate`, then how they end.
    fn reads(self, gate: &Arc<tokio::sync::Notify>) -> Vec<ReadAction> {
        let (bytes, end): (&[u8], ReadAction) = match self {
            Self::LengthBodyEof => (
                b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\nfirst",
                ReadAction::Eof,
            ),
            Self::ChunkedBodyEof => (
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n",
                ReadAction::Eof,
            ),
            Self::HeadEof => (
                b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\n",
                ReadAction::Eof,
            ),
            Self::LengthBodyReset => (
                b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\nfirst",
                ReadAction::Error(HttpErrorCode::TRANSPORT_READ_ERROR),
            ),
            Self::ChunkedBodyParseError => (
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\nzz\r\n",
                ReadAction::Eof,
            ),
            Self::Complete => (
                b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfirst",
                ReadAction::Eof,
            ),
        };
        vec![ReadAction::WaitData(Arc::clone(gate), bytes.to_vec()), end]
    }

    /// What a reader gets with `deliver_ingress_before_abort` on: every event received before
    /// the end, then the end's error.
    fn delivered(self) -> ReadResponse {
        let truncated = |error| ReadResponse {
            statuses: vec![200],
            body: b"first".to_vec(),
            error: Some(error),
            ..ReadResponse::default()
        };
        match self {
            Self::LengthBodyEof | Self::ChunkedBodyEof => truncated(HttpErrorCode::TRANSPORT_EOF),
            Self::HeadEof => ReadResponse {
                statuses: vec![200],
                error: Some(HttpErrorCode::TRANSPORT_EOF),
                ..ReadResponse::default()
            },
            Self::LengthBodyReset => truncated(HttpErrorCode::TRANSPORT_READ_ERROR),
            Self::ChunkedBodyParseError => truncated(HttpErrorCode::BODY_PARSE_ERROR),
            Self::Complete => ReadResponse {
                statuses: vec![200],
                body: b"first".to_vec(),
                eom: true,
                ..ReadResponse::default()
            },
        }
    }
}

/// A session's run error, if it failed, and what its response source yielded.
#[derive(Debug, PartialEq)]
struct ResponseEndOutcome {
    run: Option<HttpErrorCode>,
    read: ReadResponse,
}

/// Runs a GET whose response ends as `end` through an owned upstream session, or a
/// `UniplexSession` client, with `deliver_ingress_before_abort` set to `deliver`. The reader
/// starts `SETTLE_TURNS` turns after the origin's bytes are released, so the session has taken
/// the bytes and their end by then.
fn response_end_outcome(end: ResponseEnd, owned: bool, deliver: bool) -> ResponseEndOutcome {
    run_on_host(move |context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            let (transport, _state) = ScriptedTransport::new(end.reads(&gate));
            let mut config = upstream_config();
            config.deliver_ingress_before_abort = deliver;
            let read_late = |mut source: HttpSourceHolder| {
                let gate = Arc::clone(&gate);
                async move {
                    gate.notify_one();
                    settle().await;
                    read_response(&mut source).await
                }
            };
            let (run, read) = if owned {
                let mut options = upstream_options(context, transport);
                options.io = config;
                let (session, handle) = OwnedH1UpstreamSession::new(options).unwrap();
                let submission = handle.reserve_request().unwrap().submit(
                    get_head(),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                futures::join!(session.run(), async {
                    let (_stream, source, _control) = submission.await.unwrap().into_source_parts();
                    read_late(source).await
                })
            } else {
                let (session, handle) = UniplexSession::new(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    config,
                )
                .unwrap();
                futures::join!(session.run(), async {
                    let response = handle
                        .send_request(
                            RequestSource::held(Some(get_head()), Vec::new()),
                            CancellationToken::never(),
                        )
                        .await
                        .unwrap();
                    read_late(response.source).await
                })
            };
            ResponseEndOutcome {
                run: run.err().map(|error| error.code()),
                read,
            }
        })
    })
}

#[test]
fn received_response_events_precede_an_ingress_abort_as_uniplex() {
    for end in ResponseEnd::ALL {
        let uniplex = response_end_outcome(end, false, true);
        let owned = response_end_outcome(end, true, true);
        assert_eq!(
            owned.read,
            end.delivered(),
            "{end:?}: deliver_ingress_before_abort delivers the head and bytes before the error"
        );
        assert_eq!(
            owned, uniplex,
            "{end:?}: the owned session matches Uniplex with deliver_ingress_before_abort on"
        );
        let uniplex = response_end_outcome(end, false, false);
        let owned = response_end_outcome(end, true, false);
        assert_eq!(
            owned, uniplex,
            "{end:?}: the owned session matches Uniplex with deliver_ingress_before_abort off"
        );
    }
}

#[test]
fn an_ingress_abort_discards_the_received_response_by_default() {
    // By default, the error reaches the late reader first, carrying the queued head.
    let owned = response_end_outcome(ResponseEnd::LengthBodyEof, true, false);
    assert_eq!(
        owned.read,
        ReadResponse {
            error: Some(HttpErrorCode::TRANSPORT_EOF),
            ..ReadResponse::default()
        }
    );
}

/// A real loopback origin responds to a GET with a head and the first bytes of a longer body,
/// then resets the connection (`SO_LINGER` 0). The owned session runs with
/// `deliver_ingress_before_abort` on, and its response is read only once its run has ended.
/// Returns the run's error and what was read.
fn owned_loopback_reset_mid_body() -> ResponseEndOutcome {
    run_on_io_worker(|context| {
        Box::pin(async move {
            let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
                .await
                .expect("loopback listener");
            let (accepted, client) = tokio::join!(
                listener.accept(),
                tokio::net::TcpStream::connect(listener.local_addr().unwrap()),
            );
            let mut peer = accepted.expect("accepted loopback peer").0;
            let transport = roxygen_transport::established::adopt_local(
                &context,
                roxygen_transport::established::EstablishedIo::new(
                    client.expect("connected loopback client"),
                    roxygen_transport::byte_stream::TransportEndpoint::Unknown,
                    roxygen_transport::byte_stream::TransportEndpoint::Unknown,
                    None,
                ),
                roxygen_transport::byte_stream::ByteStreamLimits::new(64 << 10, 64 << 10, 64)
                    .unwrap(),
            )
            .expect("adopted loopback I/O");
            let mut options = upstream_options(context, Rc::new(transport));
            options.io.deliver_ingress_before_abort = true;
            let (session, handle) = OwnedH1UpstreamSession::new(options).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let origin = async move {
                let mut input = Vec::new();
                let mut buffer = [0; 1024];
                while !input.windows(4).any(|window| window == b"\r\n\r\n") {
                    let read = tokio::io::AsyncReadExt::read(&mut peer, &mut buffer)
                        .await
                        .expect("origin reads the request");
                    assert_ne!(read, 0, "the request arrives");
                    input.extend_from_slice(&buffer[..read]);
                }
                tokio::io::AsyncWriteExt::write_all(
                    &mut peer,
                    b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\nfirst",
                )
                .await
                .expect("origin answers");
                peer.set_zero_linger().expect("zero linger");
                drop(peer);
            };
            let reader = async {
                let (_stream, mut source, _control) = submission.await.unwrap().into_source_parts();
                tokio::time::timeout(Duration::from_secs(5), async {
                    while !handle.is_finished() {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                })
                .await
                .expect("the session's run ends after the reset");
                read_response(&mut source).await
            };
            let (run, read, ()) = futures::join!(session.run(), reader, origin);
            ResponseEndOutcome {
                run: run.err().map(|error| error.code()),
                read,
            }
        })
    })
}

#[test]
fn a_connection_reset_mid_body_yields_the_received_bytes_first() {
    assert_eq!(
        owned_loopback_reset_mid_body(),
        ResponseEndOutcome {
            run: Some(HttpErrorCode::TRANSPORT_READ_ERROR),
            read: ResponseEnd::LengthBodyReset.delivered(),
        }
    );
}

/// How the caller cancels an owned upstream exchange in the `deliver_ingress_before_abort`
/// cancel tests, once its response's head and `first` are queued.
#[derive(Clone, Copy, Debug)]
enum OwnedCancel {
    /// `OwnedH1UpstreamRequest::cancel`, with the error `UniplexStreamAbort` selects.
    Request,
    /// The request's cancellation token.
    Token,
    /// `OwnedH1UpstreamHandle::cancel`: the connection, not the request.
    Connection,
}

/// The origin's reads in the `deliver_ingress_before_abort` cancel tests. A head and `first` of
/// a 9-byte body are released by `gate`, and the connection then stays open.
fn unfinished_response(gate: &Arc<tokio::sync::Notify>) -> Vec<ReadAction> {
    vec![ReadAction::WaitData(
        Arc::clone(gate),
        b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\nfirst".to_vec(),
    )]
}

/// Runs a GET through an owned upstream session with `deliver_ingress_before_abort` set to
/// `deliver`, cancels it as `cancel` once the session has queued the response's head and
/// `first`, and reads the response once the session has taken the cancel.
fn owned_cancel_outcome(cancel: OwnedCancel, deliver: bool) -> ResponseEndOutcome {
    run_on_host(move |context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            let (transport, _state) = ScriptedTransport::new(unfinished_response(&gate));
            let mut options = upstream_options(context, transport);
            options.io.deliver_ingress_before_abort = deliver;
            let (session, handle) = OwnedH1UpstreamSession::new(options).unwrap();
            let token = CancellationSource::new();
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                token.token(),
            );
            let (run, read) = futures::join!(session.run(), async {
                let (_stream, mut source, control) = submission.await.unwrap().into_source_parts();
                gate.notify_one();
                settle().await;
                match cancel {
                    OwnedCancel::Request => control.cancel(HttpError::with_message(
                        HttpErrorCode::CANCEL,
                        b"stream aborted",
                    )),
                    OwnedCancel::Token => {
                        token.request_cancellation();
                    }
                    OwnedCancel::Connection => handle.cancel(),
                }
                settle().await;
                read_response(&mut source).await
            });
            ResponseEndOutcome {
                run: run.err().map(|error| error.code()),
                read,
            }
        })
    })
}

/// As `owned_cancel_outcome` for a `UniplexSession` client whose caller aborts the stream
/// with `UniplexStreamAbort`.
fn uniplex_stream_abort_outcome(deliver: bool) -> ResponseEndOutcome {
    use crate::experimental::UniplexResponseAbortExt;

    run_on_host(move |context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            let (transport, _state) = ScriptedTransport::new(unfinished_response(&gate));
            let mut config = upstream_config();
            config.deliver_ingress_before_abort = deliver;
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                transport,
                None,
                config,
            )
            .unwrap();
            let (run, read) = futures::join!(session.run(), async {
                let mut response = handle
                    .send_request(
                        RequestSource::held(Some(get_head()), Vec::new()),
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap();
                gate.notify_one();
                settle().await;
                response
                    .take_stream_abort()
                    .expect("a live stream's abort")
                    .abort();
                settle().await;
                read_response(&mut response.source).await
            });
            ResponseEndOutcome {
                run: run.err().map(|error| error.code()),
                read,
            }
        })
    })
}

#[test]
fn a_request_cancel_discards_the_received_response_as_a_uniplex_stream_abort() {
    for deliver in [true, false] {
        let owned = owned_cancel_outcome(OwnedCancel::Request, deliver);
        let uniplex = uniplex_stream_abort_outcome(deliver);
        assert_eq!(
            owned.read,
            ReadResponse {
                error: Some(HttpErrorCode::CANCEL),
                ..ReadResponse::default()
            },
            "deliver {deliver}: the consumer's cancel discards the queued head and bytes"
        );
        assert_eq!(
            owned.read, uniplex.read,
            "deliver {deliver}: the owned reader gets what a Uniplex stream abort leaves"
        );
        // The runs differ. A Uniplex stream abort leaves the session to finish, while a
        // cancel of an unfinished owned exchange closes its connection.
        assert_eq!(owned.run, Some(HttpErrorCode::CANCEL));
    }
}

#[test]
fn a_cancelled_request_token_discards_the_received_response() {
    assert_eq!(
        owned_cancel_outcome(OwnedCancel::Token, true),
        ResponseEndOutcome {
            run: Some(HttpErrorCode::CORO_CANCELLED),
            read: ReadResponse {
                error: Some(HttpErrorCode::CORO_CANCELLED),
                ..ReadResponse::default()
            },
        }
    );
}

#[test]
fn a_connection_cancel_keeps_the_received_response() {
    // A connection cancel does not cancel the request. Like an ingress abort, it keeps the
    // received response.
    assert_eq!(
        owned_cancel_outcome(OwnedCancel::Connection, true),
        ResponseEndOutcome {
            run: Some(HttpErrorCode::CORO_CANCELLED),
            read: ReadResponse {
                statuses: vec![200],
                body: b"first".to_vec(),
                error: Some(HttpErrorCode::CORO_CANCELLED),
                ..ReadResponse::default()
            },
        }
    );
}

/// `upstream_config` with `unframed_bodiless_http1_requests` set to `unframed`.
fn unframed_config(unframed: bool) -> UniplexConfig {
    UniplexConfig {
        unframed_bodiless_http1_requests: unframed,
        ..upstream_config()
    }
}

type RequestFn = fn() -> (HttpHeaderEvent, Vec<HttpBodyEvent>);

/// A `POST /upload` whose head ends the request, with no framing header.
fn bodiless_post() -> (HttpHeaderEvent, Vec<HttpBodyEvent>) {
    (post_head(None, true), Vec::new())
}

/// As `bodiless_post`, declaring `Content-Length: 0`.
fn zero_length_post() -> (HttpHeaderEvent, Vec<HttpBodyEvent>) {
    (post_head(Some(0), true), Vec::new())
}

/// As `bodiless_post`, an HTTP/1.0 request requesting keep-alive, on which the codec cannot
/// chunk.
fn bodiless_http10_post() -> (HttpHeaderEvent, Vec<HttpBodyEvent>) {
    let mut head = post_head(None, true);
    head.headers_mut().set_http_version(1, 0);
    head.headers_mut()
        .headers_mut()
        .set(HttpHeaderCode::Connection, b"keep-alive");
    (head, Vec::new())
}

/// As `bodiless_post`, carrying the caller's own chunked `Transfer-Encoding`.
fn caller_chunked_bodiless_post() -> (HttpHeaderEvent, Vec<HttpBodyEvent>) {
    let mut head = post_head(None, true);
    head.headers_mut()
        .headers_mut()
        .set(HttpHeaderCode::TransferEncoding, b"chunked");
    (head, Vec::new())
}

/// Sends `request`, then a `GET /` when the connection admits one, through an owned upstream
/// session and a `UniplexSession` client that share `unframed_config(unframed)`; asserts that
/// they read, write, reuse and close alike, and returns the owned session's first request's
/// bytes and whether it reused the connection.
fn unframed_outcome(name: &str, request: RequestFn, unframed: bool) -> (Vec<u8>, bool) {
    let config = unframed_config(unframed);
    let uniplex = uniplex_reuse_outcome_with(config, request, OK_RESPONSE, false, None);
    let owned = owned_reuse_outcome_with(config, request, OK_RESPONSE, false, None);
    assert_eq!(owned.first, uniplex.first, "{name}");
    assert_eq!(owned.first.body, b"ok", "{name}");
    assert_eq!(
        String::from_utf8_lossy(&owned.wire),
        String::from_utf8_lossy(&uniplex.wire),
        "{name}: every byte both connections wrote"
    );
    assert_eq!(owned.reused, uniplex.reused, "{name}: the reuse decision");
    assert_eq!(
        (owned.run, owned.close),
        (uniplex.run, uniplex.close),
        "{name}"
    );
    let first = owned.wire[..owned.wire.len() - owned.second_wire.len()].to_vec();
    (first, owned.reused)
}

#[test]
fn bodiless_requests_go_out_unframed_as_uniplex_when_enabled() {
    // (name, request, its bytes with unframed bodiless requests on, whether the connection is
    // then reused)
    let cases: [(&str, RequestFn, &[u8], bool); 6] = [
        (
            "bodiless POST",
            bodiless_post,
            b"POST /upload HTTP/1.1\r\nHost: origin\r\nConnection: keep-alive\r\n\r\n",
            true,
        ),
        (
            "Content-Length: 0 POST",
            zero_length_post,
            b"POST /upload HTTP/1.1\r\nHost: origin\r\nConnection: keep-alive\r\nContent-Length: 0\r\n\r\n",
            true,
        ),
        (
            // Unframed, the codec does not disable keep-alive on a connection that cannot chunk.
            "bodiless HTTP/1.0 POST",
            bodiless_http10_post,
            b"POST /upload HTTP/1.0\r\nHost: origin\r\nConnection: keep-alive\r\n\r\n",
            true,
        ),
        (
            // The caller's framing is kept: no last chunk, so the connection closes.
            "bodiless POST with the caller's chunked framing",
            caller_chunked_bodiless_post,
            b"POST /upload HTTP/1.1\r\nHost: origin\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
            false,
        ),
        (
            "POST with a body",
            || scripted_request(true),
            b"POST /upload HTTP/1.1\r\nHost: origin\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n6\r\nhello \r\n5\r\nworld\r\n0\r\n\r\n",
            true,
        ),
        (
            "GET",
            || (get_head(), Vec::new()),
            b"GET / HTTP/1.1\r\nHost: origin\r\nConnection: keep-alive\r\n\r\n",
            true,
        ),
    ];
    for (name, request, expected, reused) in cases {
        let (first, owned_reused) = unframed_outcome(name, request, true);
        assert_eq!(
            String::from_utf8_lossy(&first),
            String::from_utf8_lossy(expected),
            "{name}"
        );
        assert_eq!(owned_reused, reused, "{name}: the reuse decision");
    }
}

#[test]
fn bodiless_posts_are_framed_as_uniplex_by_default() {
    // (name, request, its bytes with unframed bodiless requests off, whether the connection is
    // then reused)
    let cases: [(&str, RequestFn, &[u8], bool); 2] = [
        (
            "bodiless POST",
            bodiless_post,
            b"POST /upload HTTP/1.1\r\nHost: origin\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n0\r\n\r\n",
            true,
        ),
        (
            "bodiless HTTP/1.0 POST",
            bodiless_http10_post,
            b"POST /upload HTTP/1.0\r\nHost: origin\r\nConnection: close\r\n\r\n",
            false,
        ),
    ];
    for (name, request, expected, reused) in cases {
        let (first, owned_reused) = unframed_outcome(name, request, false);
        assert_eq!(
            String::from_utf8_lossy(&first),
            String::from_utf8_lossy(expected),
            "{name}"
        );
        assert_eq!(owned_reused, reused, "{name}: the reuse decision");
    }
}

/// A finished request's error and progress: `request_unprocessed()` and `sequence_number()`.
#[derive(Debug, Eq, PartialEq)]
struct RequestProgress {
    error: Option<HttpErrorCode>,
    unprocessed: bool,
    sequence: Option<u64>,
}

impl RequestProgress {
    fn of(error: Option<HttpErrorCode>, progress: &impl OwnedRequestProgressExt) -> Self {
        Self {
            error,
            unprocessed: progress.request_unprocessed(),
            sequence: progress.sequence_number(),
        }
    }
}

/// The owned submission kinds, which all report progress.
#[derive(Clone, Copy, Debug)]
enum SubmitKind {
    Plain,
    ResponseInterest,
    Exclusive,
    /// On a session run with `run_pull_through`.
    PullThrough,
}

enum Submitted {
    Plain(crate::owned_h1::OwnedH1UpstreamSubmission),
    Exclusive(crate::owned_h1::OwnedH1ExclusiveSubmission),
    PullThrough(crate::owned_h1::OwnedH1PullThroughSubmission),
}

impl Submitted {
    fn new(kind: SubmitKind, reservation: OwnedH1UpstreamReservation) -> Self {
        let (head, body, token) = (
            get_head(),
            HttpSourceHolder::empty(),
            CancellationToken::never(),
        );
        match kind {
            SubmitKind::Plain => Self::Plain(reservation.submit(head, body, token)),
            SubmitKind::ResponseInterest => {
                Self::Exclusive(reservation.submit_with_response_interest(head, body, token, None))
            }
            SubmitKind::Exclusive => {
                Self::Exclusive(reservation.submit_exclusive(head, body, token))
            }
            SubmitKind::PullThrough => {
                Self::PullThrough(reservation.submit_pull_through(head, body, token))
            }
        }
    }

    /// Awaits the submission, then reads its progress.
    async fn progress(&mut self) -> RequestProgress {
        match self {
            Self::Plain(submission) => {
                let error = (&mut *submission).await.err().map(|error| error.code());
                RequestProgress::of(error, submission)
            }
            Self::Exclusive(submission) => {
                let error = (&mut *submission).await.err().map(|error| error.code());
                RequestProgress::of(error, submission)
            }
            Self::PullThrough(submission) => {
                let error = (&mut *submission).await.err().map(|error| error.code());
                RequestProgress::of(error, submission)
            }
        }
    }
}

/// When the FIN of a stale reused connection's origin reaches the owned driver, relative to the
/// connection's next request.
#[derive(Clone, Copy, Debug)]
enum StaleFin {
    /// The driver processes the FIN between the reservation and the submission, which then
    /// meets closed admission.
    BeforeSubmission,
    /// The FIN becomes readable, then the request is submitted, before the driver runs.
    ReadableThenSubmitted,
    /// The request is submitted, then the FIN becomes readable, before the driver runs.
    SubmittedThenReadable,
}

/// The outcome of two requests on a stale reused connection: the first gets a response, and
/// the second is submitted around the origin's FIN.
#[derive(Debug, Eq, PartialEq)]
struct StaleOutcome {
    first: RequestProgress,
    second: RequestProgress,
    written_after_first: Vec<u8>,
    run: Option<HttpErrorCode>,
    close: CloseKind,
}

/// A `GET /` that gets `OK_RESPONSE` on a keep-alive connection, then a second `GET /`
/// submitted as `kind` around the origin's FIN, as `fin` orders them. A pull-through kind runs
/// the session with `run_pull_through`.
fn owned_stale_outcome(fin: StaleFin, kind: SubmitKind) -> StaleOutcome {
    run_on_host(move |context| {
        Box::pin(async move {
            let first_ready = Arc::new(tokio::sync::Notify::new());
            let fin_ready = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([
                ReadAction::WaitData(Arc::clone(&first_ready), OK_RESPONSE.to_vec()),
                ReadAction::WaitEof(Arc::clone(&fin_ready)),
            ]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let log = Rc::new(AdmissionLog::default());
            handle.set_observer(log.clone()).unwrap();
            let run: Pin<
                Box<dyn Future<Output = Result<crate::uniplex::UniplexRunSummary, HttpError>>>,
            > = if matches!(kind, SubmitKind::PullThrough) {
                Box::pin(session.run_pull_through())
            } else {
                Box::pin(session.run())
            };
            let (run, (first, second, written_after_first)) = futures::join!(run, async {
                let (_stream, mut source, control) = handle
                    .reserve_request()
                    .unwrap()
                    .submit(
                        get_head(),
                        HttpSourceHolder::empty(),
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap()
                    .into_source_parts();
                first_ready.notify_one();
                assert_eq!(read_response(&mut source).await.body, b"ok");
                drop(source);
                yield_until(|| handle.is_idle()).await;
                assert_eq!(handle.available_capacity(), 1, "the connection is reused");
                let first = RequestProgress::of(None, &control);
                let written = state.borrow().writes.concat().len();
                let reservation = handle.reserve_request().unwrap();
                match fin {
                    StaleFin::BeforeSubmission => {
                        fin_ready.notify_one();
                        yield_until(|| {
                            log.0.borrow().iter().any(|note| note == "admission_closed")
                        })
                        .await;
                    }
                    StaleFin::ReadableThenSubmitted => fin_ready.notify_one(),
                    StaleFin::SubmittedThenReadable => {}
                }
                let mut submitted = Submitted::new(kind, reservation);
                if matches!(fin, StaleFin::SubmittedThenReadable) {
                    fin_ready.notify_one();
                }
                let second = submitted.progress().await;
                let written_after_first = state.borrow().writes.concat()[written..].to_vec();
                (first, second, written_after_first)
            });
            let close = state.borrow().close;
            StaleOutcome {
                first,
                second,
                written_after_first,
                run: run.err().map(|error| error.code()),
                close,
            }
        })
    })
}

#[test]
fn a_stale_reused_connection_rejects_its_next_request_unprocessed_in_every_schedule() {
    for fin in [
        StaleFin::BeforeSubmission,
        StaleFin::ReadableThenSubmitted,
        StaleFin::SubmittedThenReadable,
    ] {
        for kind in [
            SubmitKind::Plain,
            SubmitKind::ResponseInterest,
            SubmitKind::Exclusive,
            SubmitKind::PullThrough,
        ] {
            assert_eq!(
                owned_stale_outcome(fin, kind),
                StaleOutcome {
                    first: RequestProgress {
                        error: None,
                        unprocessed: false,
                        sequence: Some(0),
                    },
                    // Refused before commit, with one earlier request on the connection.
                    second: RequestProgress {
                        error: Some(HttpErrorCode::TRANSPORT_EOF),
                        unprocessed: true,
                        sequence: Some(1),
                    },
                    written_after_first: Vec::new(),
                    run: None,
                    close: CloseKind::ShutdownWrite,
                },
                "{fin:?}, {kind:?}"
            );
        }
    }
}

/// A FIN the driver processes before the next reservation closes admission, so the
/// reservation fails and the session does not write.
#[test]
fn a_stale_reused_connection_refuses_a_reservation_after_its_fin() {
    run_on_host(|context| {
        Box::pin(async move {
            let first_ready = Arc::new(tokio::sync::Notify::new());
            let fin_ready = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([
                ReadAction::WaitData(Arc::clone(&first_ready), OK_RESPONSE.to_vec()),
                ReadAction::WaitEof(Arc::clone(&fin_ready)),
            ]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let (run, refused) = futures::join!(session.run(), async {
                let (_stream, mut source, _control) = handle
                    .reserve_request()
                    .unwrap()
                    .submit(
                        get_head(),
                        HttpSourceHolder::empty(),
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap()
                    .into_source_parts();
                first_ready.notify_one();
                assert_eq!(read_response(&mut source).await.body, b"ok");
                drop(source);
                yield_until(|| handle.is_idle()).await;
                fin_ready.notify_one();
                yield_until(|| handle.is_finished()).await;
                handle.reserve_request().err().map(|error| error.code())
            });
            assert_eq!(refused, Some(HttpErrorCode::REFUSED_STREAM));
            run.unwrap();
            assert_eq!(
                state.borrow().writes.concat(),
                b"GET / HTTP/1.1\r\nHost: origin\r\nConnection: keep-alive\r\n\r\n",
                "only the first request is written"
            );
        })
    });
}

#[cfg(target_os = "linux")]
mod idle_peer_close {
    use super::*;

    /// The head the session writes for `get_head()`.
    const GET: &str = "GET / HTTP/1.1\r\nHost: origin\r\nConnection: keep-alive\r\n\r\n";
    /// What an origin may write on an idle connection before it closes it.
    const REQUEST_TIMEOUT: &[u8] =
        b"HTTP/1.1 408 Request Timeout\r\nConnection: close\r\nContent-Length: 0\r\n\r\n";

    /// What the peer does on an idle connection before the next submission.
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum IdlePeer {
        /// Keeps it open and responds to the request.
        Open,
        /// Half-closes it (FIN).
        Fin,
        /// Writes `REQUEST_TIMEOUT`, then half-closes it.
        BytesThenFin,
        /// Half-closes it, and the worker's I/O pass then wakes the session's read for the FIN.
        FinAfterIoPass,
        /// As `FinAfterIoPass`, and the session's next poll starts with the task's coop budget
        /// spent.
        FinWithBudgetSpent,
        /// Resets it (`SO_LINGER` 0, then close).
        Reset,
    }

    fn refused(code: HttpErrorCode, sequence: u64) -> RequestProgress {
        RequestProgress {
            error: Some(code),
            unprocessed: true,
            sequence: Some(sequence),
        }
    }

    fn served(sequence: u64) -> RequestProgress {
        RequestProgress {
            error: None,
            unprocessed: false,
            sequence: Some(sequence),
        }
    }

    #[derive(Debug, PartialEq)]
    struct IdleReuse {
        /// One request per step, in order.
        requests: Vec<RequestProgress>,
        run: Option<HttpErrorCode>,
        /// Every byte the peer read.
        received: String,
    }

    /// Request progress on a real loopback TCP connection. For each step the peer acts on the
    /// idle connection and one request is submitted. After a close, the request is submitted
    /// once the client's socket reports the close but before the worker's next I/O pass, which
    /// alone would wake the session's parked read.
    /// The session runs in the same task: `futures::join!` polls it right after the steps.
    fn idle_reuse(steps: &'static [IdlePeer]) -> IdleReuse {
        crate::uniplex_tests::run_on_hosted_worker(move |context| {
            Box::pin(async move {
                use std::io::Read;
                use std::io::Write;
                use std::os::fd::AsRawFd;

                let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
                    .expect("loopback listener");
                let client = std::net::TcpStream::connect(listener.local_addr().unwrap())
                    .expect("loopback client");
                let (peer, _) = listener.accept().expect("loopback peer");
                peer.set_nonblocking(true).unwrap();
                let mut peer = Some(peer);
                let client_fd = client.as_raw_fd();
                let transport = roxygen_transport::tcp::TcpTransport::from_accepted(
                    &context,
                    client,
                    roxygen_transport::byte_stream::ByteStreamLimits::new(
                        256 << 10,
                        256 << 10,
                        1024,
                    )
                    .unwrap(),
                )
                .expect("adopted loopback client");
                let (session, handle) =
                    OwnedH1UpstreamSession::new(upstream_options(context, Rc::new(transport)))
                        .unwrap();
                let submit = || {
                    handle.reserve_request().unwrap().submit(
                        get_head(),
                        HttpSourceHolder::empty(),
                        CancellationToken::never(),
                    )
                };
                let wait_idle = || {
                    tokio::time::timeout(Duration::from_secs(5), async {
                        while !handle.is_idle() {
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    })
                };
                let mut received = Vec::new();
                let (requests, run) = futures::join!(
                    async {
                        let mut requests = Vec::new();
                        for &step in steps {
                            let socket = peer
                                .as_mut()
                                .expect("the peer resets only in the last step");
                            let mut buffer = [0; 4096];
                            loop {
                                match socket.read(&mut buffer) {
                                    Ok(read) => {
                                        assert_ne!(read, 0, "the client keeps its side open");
                                        received.extend_from_slice(&buffer[..read]);
                                    }
                                    Err(error)
                                        if error.kind() == std::io::ErrorKind::WouldBlock =>
                                    {
                                        break;
                                    }
                                    Err(error) => panic!("peer read: {error}"),
                                }
                            }
                            if step == IdlePeer::BytesThenFin {
                                socket.write_all(REQUEST_TIMEOUT).unwrap();
                            }
                            match step {
                                IdlePeer::Open => {}
                                IdlePeer::Fin
                                | IdlePeer::BytesThenFin
                                | IdlePeer::FinAfterIoPass
                                | IdlePeer::FinWithBudgetSpent => {
                                    socket.shutdown(std::net::Shutdown::Write).unwrap();
                                }
                                IdlePeer::Reset => {
                                    let stream =
                                        tokio::net::TcpStream::from_std(peer.take().unwrap())
                                            .unwrap();
                                    #[expect(
                                        deprecated,
                                        reason = "Tokio's only linger-zero reset setter"
                                    )]
                                    let linger = stream.set_linger(Some(Duration::ZERO));
                                    linger.unwrap();
                                    drop(stream);
                                }
                            }
                            if step != IdlePeer::Open {
                                // Blocks the worker until the client's socket reports the close,
                                // so that an I/O pass cannot run before the submission.
                                // `POLLRDHUP` waits for the FIN behind any bytes.
                                let mut client_socket = libc::pollfd {
                                    fd: client_fd,
                                    events: libc::POLLRDHUP,
                                    revents: 0,
                                };
                                // SAFETY: `poll` reads and writes one `pollfd` at the pointer,
                                // `client_socket`, which is initialized and outlives the call.
                                let ready = unsafe { libc::poll(&mut client_socket, 1, 5000) };
                                assert_eq!(ready, 1, "the client's socket shows {step:?}");
                            }
                            if matches!(
                                step,
                                IdlePeer::FinAfterIoPass | IdlePeer::FinWithBudgetSpent
                            ) {
                                // The I/O pass this yield waits for wakes the session's read.
                                tokio::task::yield_now().await;
                            }
                            let mut submission = submit();
                            if step == IdlePeer::FinWithBudgetSpent {
                                // Tokio's and Roxygen's TCP reads then pend without reading.
                                while tokio::task::coop::has_budget_remaining() {
                                    tokio::task::consume_budget().await;
                                }
                            }
                            let resolved =
                                tokio::time::timeout(Duration::from_secs(5), &mut submission)
                                    .await
                                    .expect("the request is committed or refused");
                            let error = match resolved {
                                Ok(response) => {
                                    let (_stream, mut source, _control) =
                                        response.into_source_parts();
                                    if step == IdlePeer::Open {
                                        peer.as_mut().unwrap().write_all(OK_RESPONSE).unwrap();
                                    }
                                    tokio::time::timeout(
                                        Duration::from_secs(5),
                                        read_response(&mut source),
                                    )
                                    .await
                                    .expect("the response or its failure arrives")
                                    .error
                                }
                                Err(error) => Some(error.code()),
                            };
                            requests.push(RequestProgress::of(error, &submission));
                            if step == IdlePeer::Open {
                                wait_idle().await.expect("the session goes idle");
                            }
                        }
                        if steps.last() == Some(&IdlePeer::Open) {
                            let socket = peer.as_ref().unwrap();
                            socket.shutdown(std::net::Shutdown::Write).unwrap();
                        }
                        requests
                    },
                    tokio::time::timeout(Duration::from_secs(5), session.run()),
                );
                if let Some(socket) = peer {
                    let mut socket = tokio::net::TcpStream::from_std(socket).unwrap();
                    let read = tokio::io::AsyncReadExt::read_to_end(&mut socket, &mut received);
                    let _ = tokio::time::timeout(Duration::from_secs(5), read)
                        .await
                        .expect("the client closes the connection");
                }
                IdleReuse {
                    requests,
                    run: run
                        .expect("the session ends")
                        .err()
                        .map(|error| error.code()),
                    received: String::from_utf8_lossy(&received).into_owned(),
                }
            })
        })
    }

    /// The session reads the peer's EOF before it commits the request, which fails unprocessed
    /// and is never written, so a caller can resend it.
    #[test]
    fn a_request_on_an_idle_connection_its_peer_closed_is_refused_unprocessed() {
        assert_eq!(
            idle_reuse(&[IdlePeer::Open, IdlePeer::Fin]),
            IdleReuse {
                requests: vec![served(0), refused(HttpErrorCode::TRANSPORT_EOF, 1)],
                run: None,
                received: GET.to_owned(),
            }
        );
    }

    /// Bytes ahead of the FIN, here an unsolicited `408`, are not a response to a request
    /// committed after them. The session reads through to the EOF, which fails the request
    /// unprocessed, and the bytes, a response without a request, then fail the connection.
    #[test]
    fn a_close_behind_unasked_bytes_still_refuses_the_request_unprocessed() {
        assert_eq!(
            idle_reuse(&[IdlePeer::Open, IdlePeer::BytesThenFin]),
            IdleReuse {
                requests: vec![served(0), refused(HttpErrorCode::TRANSPORT_EOF, 1)],
                run: Some(HttpErrorCode::PROTOCOL_ERROR),
                received: GET.to_owned(),
            }
        );
    }

    /// The reset's read error fails the request unprocessed. The peer does not read after its
    /// reset, so only the request's unprocessed failure, not `received`, shows that the head
    /// was not written.
    #[test]
    fn a_request_on_an_idle_connection_its_peer_reset_is_refused_unprocessed() {
        assert_eq!(
            idle_reuse(&[IdlePeer::Open, IdlePeer::Reset]),
            IdleReuse {
                requests: vec![served(0), refused(HttpErrorCode::TRANSPORT_READ_ERROR, 1)],
                run: Some(HttpErrorCode::TRANSPORT_READ_ERROR),
                received: GET.to_owned(),
            }
        );
    }

    /// The read that would return the EOF pends for coop budget without reading, and the check
    /// still holds the request until a later poll reads the EOF and fails it unprocessed.
    #[test]
    fn a_request_on_an_idle_connection_its_peer_closed_is_refused_with_the_budget_spent() {
        assert_eq!(
            idle_reuse(&[IdlePeer::Open, IdlePeer::FinWithBudgetSpent]),
            IdleReuse {
                requests: vec![served(0), refused(HttpErrorCode::TRANSPORT_EOF, 1)],
                run: None,
                received: GET.to_owned(),
            }
        );
    }

    /// Control for the spent-budget test. With coop budget left, the read that the yield's I/O
    /// pass woke returns the EOF before the commit, so the check is not needed. This shows that
    /// the yield does run an I/O pass.
    #[test]
    fn a_request_on_an_idle_connection_its_peer_closed_is_refused_with_the_budget_left() {
        assert_eq!(
            idle_reuse(&[IdlePeer::Open, IdlePeer::FinAfterIoPass]),
            IdleReuse {
                requests: vec![served(0), refused(HttpErrorCode::TRANSPORT_EOF, 1)],
                run: None,
                received: GET.to_owned(),
            }
        );
    }

    /// After a served reuse, the next request cannot commit onto the closed connection.
    #[test]
    fn a_close_after_a_served_reuse_refuses_the_next_request_unprocessed() {
        assert_eq!(
            idle_reuse(&[IdlePeer::Open, IdlePeer::Open, IdlePeer::Fin]),
            IdleReuse {
                requests: vec![
                    served(0),
                    served(1),
                    refused(HttpErrorCode::TRANSPORT_EOF, 2)
                ],
                run: None,
                received: GET.repeat(2),
            }
        );
    }

    /// An open peer does not hold the commit.
    #[test]
    fn a_request_on_an_idle_connection_its_peer_kept_open_is_served() {
        assert_eq!(
            idle_reuse(&[IdlePeer::Open, IdlePeer::Open]),
            IdleReuse {
                requests: vec![served(0), served(1)],
                run: None,
                received: GET.repeat(2),
            }
        );
    }
}

#[test]
fn committed_requests_report_how_many_requests_their_connection_committed_before() {
    run_on_host(|context| {
        Box::pin(async move {
            let ready = [
                Arc::new(tokio::sync::Notify::new()),
                Arc::new(tokio::sync::Notify::new()),
            ];
            let (transport, _state) = ScriptedTransport::new(
                ready
                    .iter()
                    .map(|ready| ReadAction::WaitData(Arc::clone(ready), OK_RESPONSE.to_vec())),
            );
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let (run, progress) = futures::join!(session.run(), async {
                let mut progress = Vec::new();
                for ready in &ready {
                    let mut submission = handle.reserve_request().unwrap().submit(
                        get_head(),
                        HttpSourceHolder::empty(),
                        CancellationToken::never(),
                    );
                    let response = (&mut submission).await.unwrap();
                    progress.push(RequestProgress::of(None, &submission));
                    let (_stream, mut source, control) = response.into_source_parts();
                    ready.notify_one();
                    assert_eq!(read_response(&mut source).await.body, b"ok");
                    drop(source);
                    progress.push(RequestProgress::of(None, &control));
                    yield_until(|| handle.is_idle()).await;
                }
                assert!(handle.close_when_idle().unwrap());
                progress
            });
            run.unwrap();
            // The submission and its response's request control read the same progress.
            let committed = |sequence| RequestProgress {
                error: None,
                unprocessed: false,
                sequence: Some(sequence),
            };
            assert_eq!(
                progress,
                [committed(0), committed(0), committed(1), committed(1)]
            );
        })
    });
}

/// How a request fails on a fresh connection for a cause other than its connection's refusal.
#[derive(Clone, Copy, Debug)]
enum NotUnprocessed {
    /// The driver rejects the request itself at commit (a CONNECT).
    RefusedRequest,
    /// The submission's consumer cancels it before the driver commits it.
    CancelledBeforeCommit,
    /// The consumer cancels the queued submission, then the peer's EOF closes admission.
    CancelledThenEof,
    /// The request is committed and written, then the peer's EOF fails its response.
    EofAfterCommit,
}

fn not_unprocessed_outcome(case: NotUnprocessed) -> RequestProgress {
    run_on_host(move |context| {
        Box::pin(async move {
            let fin_ready = Arc::new(tokio::sync::Notify::new());
            let reads = match case {
                NotUnprocessed::RefusedRequest | NotUnprocessed::CancelledBeforeCommit => vec![],
                NotUnprocessed::CancelledThenEof | NotUnprocessed::EofAfterCommit => {
                    vec![ReadAction::WaitEof(Arc::clone(&fin_ready))]
                }
            };
            let (transport, state) = ScriptedTransport::new(reads);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let head = if matches!(case, NotUnprocessed::RefusedRequest) {
                let mut message = roxygen_core::message::HttpMessage::new();
                message.set_method(HttpMethod::Connect).unwrap();
                message.set_url(b"origin:443", false).unwrap();
                message.set_http_version(1, 1);
                HttpHeaderEvent::new(message, false).unwrap()
            } else {
                get_head()
            };
            let mut submission = handle.reserve_request().unwrap().submit(
                head,
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            if matches!(
                case,
                NotUnprocessed::CancelledBeforeCommit | NotUnprocessed::CancelledThenEof
            ) {
                submission.cancel(HttpError::with_message(
                    HttpErrorCode::READ_TIMEOUT,
                    b"route timeout",
                ));
            }
            if matches!(case, NotUnprocessed::CancelledThenEof) {
                fin_ready.notify_one();
            }
            let (_run, progress) = futures::join!(session.run(), async {
                let progress = match (&mut submission).await {
                    Err(error) => RequestProgress::of(Some(error.code()), &submission),
                    Ok(response) => {
                        let (_stream, mut source, control) = response.into_source_parts();
                        yield_until(|| !state.borrow().writes.is_empty()).await;
                        fin_ready.notify_one();
                        let read = read_response(&mut source).await;
                        RequestProgress::of(read.error, &control)
                    }
                };
                handle.cancel();
                progress
            });
            progress
        })
    })
}

#[test]
fn failures_a_request_or_its_consumer_causes_are_not_unprocessed() {
    // (case, its error, its sequence number: fixed only for a committed request)
    let cases = [
        (
            NotUnprocessed::RefusedRequest,
            HttpErrorCode::HEADER_PARSE_ERROR,
            None,
        ),
        (
            NotUnprocessed::CancelledBeforeCommit,
            HttpErrorCode::READ_TIMEOUT,
            None,
        ),
        (
            NotUnprocessed::CancelledThenEof,
            HttpErrorCode::READ_TIMEOUT,
            None,
        ),
        (
            NotUnprocessed::EofAfterCommit,
            HttpErrorCode::TRANSPORT_EOF,
            Some(0),
        ),
    ];
    for (case, error, sequence) in cases {
        assert_eq!(
            not_unprocessed_outcome(case),
            RequestProgress {
                error: Some(error),
                unprocessed: false,
                sequence,
            },
            "{case:?}"
        );
    }
}

/// A cancellation that lands right after a connection refusal has sampled the request's cause,
/// as one from another thread can, follows the refusal. The request reports the refusal's error
/// with `request_unprocessed` set, not the cancellation's error with the refusal's flag.
#[test]
fn a_cancellation_racing_a_refusal_follows_it() {
    let (progress, written) = run_on_host(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([ReadAction::Eof]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let token = CancellationSource::new();
            let mut submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                token.token(),
            );
            crate::experimental::after_next_refusal_cause_sample(move || {
                token.request_cancellation();
            });
            let (_run, progress) = futures::join!(session.run(), async {
                let error = (&mut submission).await.err().map(|error| error.code());
                RequestProgress::of(error, &submission)
            });
            let written = state.borrow().writes.concat();
            (progress, written)
        })
    });
    assert_eq!(
        progress,
        RequestProgress {
            error: Some(HttpErrorCode::TRANSPORT_EOF),
            unprocessed: true,
            sequence: Some(0),
        }
    );
    assert!(written.is_empty(), "nothing is written");
}

/// How the first fenced write completes; later writes complete in full.
#[derive(Clone, Copy, Debug)]
enum FirstWrite {
    Full,
    /// Failure with `WRITE_TIMEOUT` and no bytes written.
    Fail,
}

/// Holds every write's completion until `open`, as a slow peer's socket does.
struct RequestFence {
    open: Cell<bool>,
    first: Cell<Option<FirstWrite>>,
    wakers: RefCell<Vec<std::task::Waker>>,
}

impl RequestFence {
    fn open(&self) {
        self.open.set(true);
        let wakers = std::mem::take(&mut *self.wakers.borrow_mut());
        for waker in wakers {
            waker.wake();
        }
    }

    fn poll_open(&self, context: &mut std::task::Context<'_>) -> Poll<()> {
        if self.open.get() {
            return Poll::Ready(());
        }
        self.wakers.borrow_mut().push(context.waker().clone());
        Poll::Pending
    }

    /// A completed write of `requested` bytes: the first write's outcome, then full writes.
    fn result(&self, requested: usize) -> Result<WriteInfo, TransportWriteError> {
        match self.first.take() {
            Some(FirstWrite::Fail) => Err(TransportWriteError::new(
                HttpError::with_message(HttpErrorCode::WRITE_TIMEOUT, b"fenced write failure"),
                0,
            )),
            _ => Ok(WriteInfo {
                bytes_written: requested,
            }),
        }
    }
}

fn record_request_write(state: &RefCell<TransportState>, data: BufQueue) -> usize {
    let length = data.chain_length();
    let bytes = data.coalesce(length).unwrap().to_vec();
    state.borrow_mut().writes.push(bytes);
    length
}

/// A write queue with one write in flight, as a single write is, whose completion waits for
/// the fence.
struct RequestFenceQueue {
    fence: Rc<RequestFence>,
    state: Rc<RefCell<TransportState>>,
    in_flight: Option<usize>,
}

impl crate::uniplex::UniplexWriteQueue for RequestFenceQueue {
    fn can_write(&self, _bytes: usize) -> bool {
        self.in_flight.is_none()
    }

    fn start_write(&mut self, data: BufQueue) -> Result<(), HttpError> {
        self.in_flight = Some(record_request_write(&self.state, data));
        Ok(())
    }

    fn poll_complete(
        &mut self,
        context: &mut std::task::Context<'_>,
    ) -> Poll<crate::uniplex::UniplexWriteCompletion> {
        let Some(requested_bytes) = self.in_flight else {
            return Poll::Pending;
        };
        if self.fence.poll_open(context).is_pending() {
            return Poll::Pending;
        }
        self.in_flight = None;
        Poll::Ready(crate::uniplex::UniplexWriteCompletion {
            requested_bytes,
            result: self.fence.result(requested_bytes),
        })
    }

    fn is_idle(&self) -> bool {
        self.in_flight.is_none()
    }
}

/// Reads and closes through a [`ScriptedTransport`]; writes wait for a [`RequestFence`],
/// directly or, when `queued`, through a [`RequestFenceQueue`]. `UniplexSession` does not use a
/// write queue, so it writes directly either way.
struct RequestFenceTransport {
    inner: Rc<ScriptedTransport>,
    state: Rc<RefCell<TransportState>>,
    fence: Rc<RequestFence>,
    queued: bool,
}

impl UniplexTransport for RequestFenceTransport {
    fn try_write_queue(
        &self,
        _context: &WorkerContext,
        _maximum_write_size: usize,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> Result<Option<Box<dyn crate::uniplex::UniplexWriteQueue>>, HttpError> {
        Ok(self.queued.then(|| {
            Box::new(RequestFenceQueue {
                fence: Rc::clone(&self.fence),
                state: Rc::clone(&self.state),
                in_flight: None,
            }) as Box<dyn crate::uniplex::UniplexWriteQueue>
        }))
    }

    fn read(
        &self,
        context: &WorkerContext,
        maximum: usize,
        allocation_size: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        self.inner
            .read(context, maximum, allocation_size, timeout, cancellation)
    }

    fn write(
        &self,
        _context: &WorkerContext,
        data: BufQueue,
        _flags: ByteEventWriteFlags,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        let length = record_request_write(&self.state, data);
        let fence = Rc::clone(&self.fence);
        Box::pin(std::future::poll_fn(move |context| {
            fence.poll_open(context).map(|()| fence.result(length))
        }))
    }

    fn shutdown_write(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.inner.shutdown_write(context)
    }

    fn close(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.inner.close(context)
    }

    fn close_with_reset(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.inner.close_with_reset(context)
    }

    fn byte_event_capability(
        &self,
        context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError> {
        self.inner.byte_event_capability(context)
    }

    fn register_byte_event(
        &self,
        context: &WorkerContext,
        registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError> {
        self.inner.register_byte_event(context, registration)
    }
}

fn written_text(state: &TransportState) -> String {
    String::from_utf8_lossy(&state.writes.concat()).into_owned()
}

/// What a read that failed during a request's first write left: how the transport was closed
/// and what was written while that write was held, every byte written, the final close, the
/// run's error, the error the request's response yielded, and the scripted reads never issued.
#[derive(Debug, PartialEq)]
struct ReadErrorDuringWrite {
    held_close: CloseKind,
    held_written: String,
    written: String,
    close: CloseKind,
    run: Option<HttpErrorCode>,
    response: Option<HttpErrorCode>,
    unread: usize,
}

/// Sends `request` through an owned upstream session (`owned`) or a `UniplexSession` client,
/// with `maximum_write_size`, holding every write at a fence. Once the first write starts, the
/// connection's read fails; the fence opens after the session has had turns to act on the
/// failure, and the first write completes as `first`. Response bytes follow the failed read in
/// the script, for a session that would read again.
fn read_error_during_request_write(
    owned: bool,
    queued: bool,
    first: FirstWrite,
    maximum_write_size: usize,
    request: fn() -> (HttpHeaderEvent, Vec<HttpBodyEvent>),
) -> ReadErrorDuringWrite {
    run_on_host(move |context| {
        Box::pin(async move {
            let failed = Arc::new(tokio::sync::Notify::new());
            let (inner, state) = ScriptedTransport::new([
                ReadAction::WaitError(Arc::clone(&failed), HttpErrorCode::TRANSPORT_READ_ERROR),
                ReadAction::Data(OK_RESPONSE.to_vec()),
            ]);
            let fence = Rc::new(RequestFence {
                open: Cell::new(false),
                first: Cell::new(Some(first)),
                wakers: RefCell::default(),
            });
            let transport = Rc::new(RequestFenceTransport {
                inner,
                state: Rc::clone(&state),
                fence: Rc::clone(&fence),
                queued,
            });
            let mut config = upstream_config();
            config.maximum_write_size = maximum_write_size;
            let script = async {
                yield_until(|| !state.borrow().writes.is_empty()).await;
                failed.notify_one();
                // Yields for the session turns in which it observes the failed read.
                for _ in 0..8 {
                    context.host().yield_to_io().await;
                }
                let held = (state.borrow().close, written_text(&state.borrow()));
                fence.open();
                held
            };
            let (head, body) = request();
            let (run, response, (held_close, held_written)) = if owned {
                let mut options = upstream_options(context.clone(), transport);
                options.io = config;
                let (session, handle) = OwnedH1UpstreamSession::new(options).unwrap();
                let submission = handle.reserve_request().unwrap().submit(
                    head,
                    RequestSource::held(None, body),
                    CancellationToken::never(),
                );
                futures::join!(
                    session.run(),
                    async {
                        match submission.await {
                            Ok(response) => {
                                let (_stream, mut source, _control) = response.into_source_parts();
                                read_response(&mut source).await.error
                            }
                            Err(error) => Some(error.code()),
                        }
                    },
                    script
                )
            } else {
                let (session, handle) = UniplexSession::new(
                    context.clone(),
                    TransportDirection::Upstream,
                    transport,
                    None,
                    config,
                )
                .unwrap();
                futures::join!(
                    session.run(),
                    async {
                        match handle
                            .send_request(
                                RequestSource::held(Some(head), body),
                                CancellationToken::never(),
                            )
                            .await
                        {
                            Ok(mut response) => read_response(&mut response.source).await.error,
                            Err(error) => Some(error.code()),
                        }
                    },
                    script
                )
            };
            let state = state.borrow();
            ReadErrorDuringWrite {
                held_close,
                held_written,
                written: written_text(&state),
                close: state.close,
                run: run.err().map(|error| error.code()),
                response,
                unread: state.reads.len(),
            }
        })
    })
}

/// A read that fails while a request's write is outstanding fails the connection as
/// `UniplexSession` does (`reset_after_draining_writes`). The response fails and the session
/// stops reading. The transport resets only once the already serialized request bytes are
/// written. Here the head is longer than one write, so part of it waits behind the outstanding
/// write. The read error stays the run's error even if that write then fails too.
#[test]
fn a_read_error_during_a_request_write_writes_the_serialized_request_as_uniplex() {
    let head = "GET / HTTP/1.1\r\nHost: origin\r\nConnection: keep-alive\r\n\r\n";
    for queued in [false, true] {
        for first in [FirstWrite::Full, FirstWrite::Fail] {
            let case = format!("queued {queued}, first write {first:?}");
            let request = || (get_head(), Vec::<HttpBodyEvent>::new());
            let owned = read_error_during_request_write(true, queued, first, 24, request);
            let uniplex = read_error_during_request_write(false, queued, first, 24, request);
            assert_eq!(owned, uniplex, "{case}");
            assert_eq!(
                owned,
                ReadErrorDuringWrite {
                    held_close: CloseKind::Open,
                    held_written: head[..24].to_owned(),
                    written: match first {
                        FirstWrite::Full => head.to_owned(),
                        FirstWrite::Fail => head[..24].to_owned(),
                    },
                    close: CloseKind::Reset,
                    run: Some(HttpErrorCode::TRANSPORT_READ_ERROR),
                    response: Some(HttpErrorCode::TRANSPORT_READ_ERROR),
                    unread: 1,
                },
                "{case}"
            );
        }
    }
}

/// As `a_read_error_during_a_request_write_writes_the_serialized_request_as_uniplex`, with a
/// request body. Both sessions serialize the ready body with its head before the read fails,
/// then finish writing those bytes before resetting the transport.
#[test]
fn a_read_error_during_a_request_write_writes_the_serialized_body() {
    let maximum_write_size = UniplexConfig::default().maximum_write_size;
    for queued in [false, true] {
        let case = format!("queued {queued}");
        let request = || scripted_request(false);
        let owned = read_error_during_request_write(
            true,
            queued,
            FirstWrite::Full,
            maximum_write_size,
            request,
        );
        let uniplex = read_error_during_request_write(
            false,
            queued,
            FirstWrite::Full,
            maximum_write_size,
            request,
        );
        let head_and_body = "POST /upload HTTP/1.1\r\nHost: origin\r\nConnection: keep-alive\r\nContent-Length: 11\r\n\r\nhello world";
        assert_eq!(owned, uniplex, "{case}");
        assert_eq!(
            owned,
            ReadErrorDuringWrite {
                held_close: CloseKind::Open,
                held_written: head_and_body.to_owned(),
                written: head_and_body.to_owned(),
                close: CloseKind::Reset,
                run: Some(HttpErrorCode::TRANSPORT_READ_ERROR),
                response: Some(HttpErrorCode::TRANSPORT_READ_ERROR),
                unread: 1,
            },
            "{case}: the serialized head and body drain before the reset"
        );
    }
}

/// When a failed read reaches the owned driver, relative to the connection's queued request.
#[derive(Clone, Copy, Debug)]
enum ReadFailure {
    /// The driver applies the failure between the reservation and the submission, which then
    /// meets closed admission.
    BeforeSubmission,
    /// The read fails, then the request is submitted, before the driver runs.
    FailedThenSubmitted,
    /// The request is submitted, then the read fails, before the driver runs.
    SubmittedThenFailed,
}

/// A failed read the driver has received before it commits a queued request closes admission
/// first, as a peer EOF does. The request fails unprocessed, the session does not write,
/// and the connection resets, as `UniplexSession` fails a request whose head it has not
/// serialized when it processes a read error.
#[test]
fn a_read_error_received_before_commit_refuses_the_request_unprocessed() {
    for failure in [
        ReadFailure::BeforeSubmission,
        ReadFailure::FailedThenSubmitted,
        ReadFailure::SubmittedThenFailed,
    ] {
        for kind in [
            SubmitKind::Plain,
            SubmitKind::ResponseInterest,
            SubmitKind::Exclusive,
            SubmitKind::PullThrough,
        ] {
            let outcome = run_on_host(move |context| {
                Box::pin(async move {
                    let failed = Arc::new(tokio::sync::Notify::new());
                    let (transport, state) = ScriptedTransport::new([ReadAction::WaitError(
                        Arc::clone(&failed),
                        HttpErrorCode::TRANSPORT_READ_ERROR,
                    )]);
                    let (session, handle) =
                        OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
                    let run: Pin<
                        Box<
                            dyn Future<
                                Output = Result<crate::uniplex::UniplexRunSummary, HttpError>,
                            >,
                        >,
                    > = if matches!(kind, SubmitKind::PullThrough) {
                        Box::pin(session.run_pull_through())
                    } else {
                        Box::pin(session.run())
                    };
                    let (run, progress) = futures::join!(run, async {
                        let reservation = handle.reserve_request().unwrap();
                        match failure {
                            ReadFailure::BeforeSubmission => {
                                failed.notify_one();
                                yield_until(|| handle.is_finished()).await;
                            }
                            ReadFailure::FailedThenSubmitted => failed.notify_one(),
                            ReadFailure::SubmittedThenFailed => {}
                        }
                        let mut submitted = Submitted::new(kind, reservation);
                        if matches!(failure, ReadFailure::SubmittedThenFailed) {
                            failed.notify_one();
                        }
                        submitted.progress().await
                    });
                    let state = state.borrow();
                    (
                        progress,
                        written_text(&state),
                        run.err().map(|error| error.code()),
                        state.close,
                    )
                })
            });
            assert_eq!(
                outcome,
                (
                    RequestProgress {
                        error: Some(HttpErrorCode::TRANSPORT_READ_ERROR),
                        unprocessed: true,
                        sequence: Some(0),
                    },
                    String::new(),
                    Some(HttpErrorCode::TRANSPORT_READ_ERROR),
                    CloseKind::Reset,
                ),
                "{failure:?}, {kind:?}"
            );
        }
    }
}

/// A panic as a failed read cancels the live exchange (here the observer's capacity release) is
/// counted at connection scope, and the failed read keeps its path. The transport resets only
/// once the outstanding request write completes, and the run fails with the read's error.
#[cfg(panic = "unwind")]
#[test]
fn an_observer_panic_on_a_read_error_during_a_request_write_waits_for_the_write() {
    for queued in [false, true] {
        let outcome = run_on_host(move |context| {
            Box::pin(async move {
                take_contained_panics();
                let worker = context.handle().clone();
                let start = worker.poll_stats();
                let failed = Arc::new(tokio::sync::Notify::new());
                let (inner, state) = ScriptedTransport::new([ReadAction::WaitError(
                    Arc::clone(&failed),
                    HttpErrorCode::TRANSPORT_READ_ERROR,
                )]);
                let fence = Rc::new(RequestFence {
                    open: Cell::new(false),
                    first: Cell::new(None),
                    wakers: RefCell::default(),
                });
                let transport = Rc::new(RequestFenceTransport {
                    inner,
                    state: Rc::clone(&state),
                    fence: Rc::clone(&fence),
                    queued,
                });
                let (session, handle) =
                    OwnedH1UpstreamSession::new(upstream_options(context.clone(), transport))
                        .unwrap();
                handle.set_observer(Rc::new(PanickingObserver)).unwrap();
                let submission = handle.reserve_request().unwrap().submit(
                    get_head(),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                let script = async {
                    yield_until(|| !state.borrow().writes.is_empty()).await;
                    failed.notify_one();
                    // Yields for the session turns in which it observes the failed read.
                    for _ in 0..8 {
                        context.host().yield_to_io().await;
                    }
                    let held_close = state.borrow().close;
                    fence.open();
                    held_close
                };
                let (run, response, held_close) = futures::join!(
                    session.run(),
                    async {
                        match submission.await {
                            Ok(response) => {
                                let (_stream, mut source, _control) = response.into_source_parts();
                                read_response(&mut source).await.error
                            }
                            Err(error) => Some(error.code()),
                        }
                    },
                    script
                );
                (
                    held_close,
                    run.err().map(|error| error.code()),
                    response,
                    state.borrow().close,
                    handle.is_finished(),
                    take_contained_panics(),
                    contained_panic_counts(&worker, &start),
                )
            })
        });
        assert_eq!(
            outcome,
            (
                CloseKind::Open,
                Some(HttpErrorCode::TRANSPORT_READ_ERROR),
                Some(HttpErrorCode::TRANSPORT_READ_ERROR),
                CloseKind::Reset,
                true,
                vec![(
                    PanicSite::UpstreamObserver,
                    Some(StreamId::new(1)),
                    Some("observer panicked".to_owned())
                )],
                (0, 1, 0),
            ),
            "queued {queued}"
        );
    }
}

/// How the read futures of a [`PoisonedReads`] transport poll once it is armed.
#[derive(Clone, Copy, Debug)]
enum ReadPanics {
    /// Every poll panics.
    Always,
    /// A panic, then a pending poll that wakes itself, in turn.
    Alternate,
}

/// Writes and closes through a [`ScriptedTransport`]. Its reads stay pending until it is armed,
/// then poll as `mode` specifies, counting each armed poll in `polls` and each read future's
/// drop in `drops`.
struct PoisonedReads {
    inner: Rc<ScriptedTransport>,
    mode: ReadPanics,
    armed: Rc<Cell<bool>>,
    waker: Rc<RefCell<Option<Waker>>>,
    polls: Rc<Cell<usize>>,
    drops: Rc<Cell<usize>>,
}

/// Counts its drop.
struct CountsDrop(Rc<Cell<usize>>);

impl Drop for CountsDrop {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

impl PoisonedReads {
    fn new(mode: ReadPanics, armed: bool) -> (Rc<Self>, Rc<RefCell<TransportState>>) {
        let (inner, state) = ScriptedTransport::new([]);
        let transport = Rc::new(Self {
            inner,
            mode,
            armed: Rc::new(Cell::new(armed)),
            waker: Rc::default(),
            polls: Rc::default(),
            drops: Rc::default(),
        });
        (transport, state)
    }

    /// Lets the read in flight panic from its next poll.
    fn arm(&self) {
        self.armed.set(true);
        let waker = self.waker.borrow_mut().take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl UniplexTransport for PoisonedReads {
    fn read(
        &self,
        _context: &WorkerContext,
        _maximum: usize,
        _allocation_size: usize,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        let mode = self.mode;
        let armed = Rc::clone(&self.armed);
        let waker = Rc::clone(&self.waker);
        let polls = Rc::clone(&self.polls);
        let dropped = CountsDrop(Rc::clone(&self.drops));
        Box::pin(poll_fn(move |context| {
            let _dropped = &dropped;
            if !armed.get() {
                *waker.borrow_mut() = Some(context.waker().clone());
                return Poll::Pending;
            }
            let poll = polls.get() + 1;
            polls.set(poll);
            if matches!(mode, ReadPanics::Alternate) && poll.is_multiple_of(2) {
                context.waker().wake_by_ref();
                return Poll::Pending;
            }
            std::panic::panic_any(format!("native read poll {poll}"))
        }))
    }

    fn write(
        &self,
        context: &WorkerContext,
        data: BufQueue,
        flags: ByteEventWriteFlags,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        self.inner
            .write(context, data, flags, timeout, cancellation)
    }

    fn shutdown_write(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.inner.shutdown_write(context)
    }

    fn close(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.inner.close(context)
    }

    fn close_with_reset(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.inner.close_with_reset(context)
    }

    fn byte_event_capability(
        &self,
        context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError> {
        self.inner.byte_event_capability(context)
    }

    fn register_byte_event(
        &self,
        context: &WorkerContext,
        registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError> {
        self.inner.register_byte_event(context, registration)
    }
}

/// The panic reports of a run whose read panics from its first poll: one for the driver turn,
/// then one for each of the three panicking teardown polls that drop the read. `panicked` gives
/// the poll number in each report.
fn poisoned_read_reports(
    panicked: [usize; 4],
) -> Vec<(PanicSite, Option<StreamId>, Option<String>)> {
    let message = |poll: usize| Some(format!("native read poll {poll}"));
    vec![
        (PanicSite::DriverTurn, None, message(panicked[0])),
        (PanicSite::Teardown, None, message(panicked[1])),
        (PanicSite::Teardown, None, message(panicked[2])),
        (PanicSite::Teardown, None, message(panicked[3])),
    ]
}

/// A native read whose every poll panics is dropped unfinished at its third panicking teardown
/// poll, and the run fails within its deadline. So is one whose panics alternate with
/// self-woken pending polls, since teardown panics count in total. The read is not polled
/// again, so its panics add exactly four reports. The deadline leaves room for each panic's
/// backtrace under ASan.
#[cfg(panic = "unwind")]
#[test]
fn owned_h1_upstream_poisoned_read_is_abandoned_after_bounded_teardown_polls() {
    // (which polls panic, the read's polls, the polls that panicked)
    for (mode, polls, panicked) in [
        (ReadPanics::Always, 4, [1, 2, 3, 4]),
        (ReadPanics::Alternate, 7, [1, 3, 5, 7]),
    ] {
        run_on_worker(move |context| {
            Box::pin(async move {
                take_contained_panics();
                let worker = context.handle().clone();
                let start = worker.poll_stats();
                let (transport, state) = PoisonedReads::new(mode, true);
                let (session, handle) =
                    OwnedH1UpstreamSession::new(upstream_options(context, transport.clone()))
                        .unwrap();
                let error = tokio::time::timeout(Duration::from_secs(30), session.run())
                    .await
                    .expect("a poisoned read is dropped within the deadline")
                    .expect_err("a Driver-scope panic fails the run");
                assert_eq!(error.code(), HttpErrorCode::INTERNAL_ERROR, "{mode:?}");
                assert_eq!(transport.polls.get(), polls, "{mode:?}");
                assert_eq!(transport.drops.get(), 1, "{mode:?}");
                assert_eq!(
                    take_contained_panics(),
                    poisoned_read_reports(panicked),
                    "{mode:?}: dropping the read adds no report of its own"
                );
                assert_eq!(contained_panic_counts(&worker, &start), (0, 0, 4));
                assert_eq!(state.borrow().close, CloseKind::Reset);
                assert!(handle.is_finished());
            })
        });
    }
}

/// After a Driver-scope panic the run joins its native I/O before it fails. With the request's
/// write held, a read whose polls panic is dropped at its third panicking teardown poll, and
/// the run stays pending until the write completes, then returns `Err(INTERNAL_ERROR)`.
#[cfg(panic = "unwind")]
#[test]
fn owned_h1_upstream_driver_panic_joins_a_held_write_before_failing_the_run() {
    run_on_worker(|context| {
        Box::pin(async move {
            take_contained_panics();
            let worker = context.handle().clone();
            let start = worker.poll_stats();
            let (transport, state) = PoisonedReads::new(ReadPanics::Always, false);
            let release = Rc::new(tokio::sync::Notify::new());
            state.borrow_mut().write_gate = Some((b"GET ".to_vec(), Rc::clone(&release)));
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport.clone())).unwrap();
            let _submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let mut run = Box::pin(session.run());
            assert!(
                poll_fn(|context| Poll::Ready(run.as_mut().poll(context)))
                    .await
                    .is_pending()
            );
            assert_eq!(
                state.borrow().writes.len(),
                1,
                "the request's write is held"
            );
            transport.arm();
            // The turn's read poll panics, then each teardown poll polls the read once; three more
            // polls drop it, and later ones wait for the write.
            for poll in 0..6 {
                assert!(
                    poll_fn(|context| Poll::Ready(run.as_mut().poll(context)))
                        .await
                        .is_pending(),
                    "poll {poll}: the held write keeps the run pending"
                );
            }
            assert_eq!(transport.polls.get(), 4, "the read is polled no more");
            assert_eq!(transport.drops.get(), 1, "the read is dropped unfinished");
            assert_eq!(state.borrow().close, CloseKind::Reset);
            assert!(!handle.is_finished());
            release.notify_one();
            let error = tokio::time::timeout(Duration::from_secs(5), run)
                .await
                .expect("the run completes once the write does")
                .expect_err("a Driver-scope panic fails the run");
            assert_eq!(error.code(), HttpErrorCode::INTERNAL_ERROR);
            assert!(handle.is_finished());
            assert_eq!(take_contained_panics(), poisoned_read_reports([1, 2, 3, 4]));
            assert_eq!(contained_panic_counts(&worker, &start), (0, 0, 4));
        })
    });
}

/// A panic committing a request, where only Roxygen code runs, is counted once at Driver scope.
/// The submission fails with `INTERNAL_ERROR`, the session does not write, and the connection
/// tears down and fails the run without counting the panic again.
#[cfg(panic = "unwind")]
#[test]
fn owned_h1_upstream_commit_panic_tears_down_with_one_report() {
    run_on_host(|context| {
        Box::pin(async move {
            take_contained_panics();
            let worker = context.handle().clone();
            let start = worker.poll_stats();
            let (transport, state) = ScriptedTransport::new([]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            crate::owned_h1::fault_next_upstream_commit();
            let (run, submitted) = futures::join!(session.run(), submission);
            assert_eq!(
                submitted
                    .err()
                    .map(|error| (error.code(), error.message().to_vec())),
                Some((
                    HttpErrorCode::INTERNAL_ERROR,
                    b"upstream request commitment panicked".to_vec()
                ))
            );
            assert_eq!(
                run.err().map(|error| error.code()),
                Some(HttpErrorCode::INTERNAL_ERROR)
            );
            assert!(state.borrow().writes.is_empty());
            assert_eq!(state.borrow().close, CloseKind::Reset);
            assert_eq!(
                take_contained_panics(),
                [(
                    PanicSite::Commit,
                    None,
                    Some("owned H1 upstream commit fault".to_owned())
                )]
            );
            assert_eq!(contained_panic_counts(&worker, &start), (0, 0, 1));
        })
    });
}

/// A response observer whose deadline panics, logging each call with the error code of its
/// outcome.
struct PanickingDeadline(Rc<RefCell<Vec<(&'static str, Option<HttpErrorCode>)>>>);

impl crate::owned::OwnedResponseObserver for PanickingDeadline {
    fn poll_deadline(&mut self, _context: &mut Context<'_>) -> Poll<HttpError> {
        self.0.borrow_mut().push(("poll_deadline", None));
        panic!("deadline panicked");
    }

    fn response_complete(self: Box<Self>, completed: Result<std::time::Instant, HttpError>) {
        let code = completed.err().map(|error| error.code());
        self.0.borrow_mut().push(("response_complete", code));
    }
}

/// A panic in a request's response-observer deadline fails the request, which closes its
/// connection, and the observer is not polled again. It gets `response_complete` once, with
/// `INTERNAL_ERROR`. The panic is counted once, at exchange scope.
#[cfg(panic = "unwind")]
#[test]
fn owned_h1_upstream_response_observer_deadline_panic_fails_its_request_once() {
    run_on_host(|context| {
        Box::pin(async move {
            take_contained_panics();
            let worker = context.handle().clone();
            let start = worker.poll_stats();
            let (transport, state) = ScriptedTransport::new([ReadAction::WaitDataOrCancellation(
                Arc::new(tokio::sync::Notify::new()),
                OK_RESPONSE.to_vec(),
            )]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let log = Rc::new(RefCell::new(Vec::new()));
            let observed = Rc::clone(&log);
            let (run, read) = futures::join!(session.run(), async move {
                let (_stream, mut source, control) = submission.await.unwrap().into_source_parts();
                control
                    .observe_response(Box::new(PanickingDeadline(observed)))
                    .unwrap();
                read_response(&mut source).await.error
            });
            assert_eq!(read, Some(HttpErrorCode::INTERNAL_ERROR));
            assert_eq!(
                run.err().map(|error| error.code()),
                Some(HttpErrorCode::INTERNAL_ERROR)
            );
            assert_eq!(
                *log.borrow(),
                [
                    ("poll_deadline", None),
                    ("response_complete", Some(HttpErrorCode::INTERNAL_ERROR))
                ]
            );
            assert_eq!(state.borrow().close, CloseKind::Reset);
            assert_eq!(
                take_contained_panics(),
                [(
                    PanicSite::ResponseObserver,
                    Some(StreamId::new(1)),
                    Some("deadline panicked".to_owned())
                )]
            );
            assert_eq!(contained_panic_counts(&worker, &start), (1, 0, 0));
        })
    });
}

/// Records each byte-event cancellation's code and message, then panics.
#[derive(Default)]
struct PanickingCancels(RefCell<Vec<(HttpErrorCode, Vec<u8>)>>);

impl HttpByteEventCallback for PanickingCancels {
    fn on_byte_event(&self, _event: HttpByteEvent) {
        unreachable!("owned H1 fires no byte event");
    }

    fn on_byte_event_cancelled(&self, _event: HttpByteEvent, error: HttpError) {
        self.0
            .borrow_mut()
            .push((error.code(), error.message().to_vec()));
        panic!("byte event cancel panicked");
    }
}

/// Each request byte-event registration, of the head and of a body event, is cancelled under
/// its own unwind boundary. Every one gets the driver's refusal, even after another's
/// callback panicked, each panic is counted at exchange scope, and the exchange completes.
#[cfg(panic = "unwind")]
#[test]
fn owned_h1_upstream_byte_event_cancel_panics_are_contained_per_registration() {
    run_on_host(|context| {
        Box::pin(async move {
            take_contained_panics();
            let worker = context.handle().clone();
            let start = worker.poll_stats();
            let log = Rc::new(PanickingCancels::default());
            let gate = Arc::new(tokio::sync::Notify::new());
            let (transport, _state) = ScriptedTransport::new([ReadAction::WaitData(
                Arc::clone(&gate),
                OK_RESPONSE.to_vec(),
            )]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let write = ByteEventType::TransportWrite.bit();
            let (mut head, mut body) = scripted_request(false);
            for _ in 0..2 {
                head.byte_event_registrations_mut()
                    .push(HttpByteEventRegistration::new(None, write, &log));
                body[0]
                    .byte_event_registrations_mut()
                    .push(HttpByteEventRegistration::new(None, write, &log));
            }
            let submission = handle.reserve_request().unwrap().submit(
                head,
                RequestSource::held(None, body),
                CancellationToken::never(),
            );
            let (run, read) = futures::join!(session.run(), async {
                let (_stream, mut source, _control) = submission.await.unwrap().into_source_parts();
                gate.notify_one();
                let read = read_response(&mut source).await;
                drop(source);
                close_once_idle(&handle).await;
                read
            });
            run.unwrap();
            assert_eq!((read.body.as_slice(), read.eom), (b"ok".as_slice(), true));
            let refusal = (
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"owned H1 delivers no byte events".to_vec(),
            );
            assert_eq!(*log.0.borrow(), vec![refusal; 4]);
            let report = (
                PanicSite::ByteEvent,
                Some(StreamId::new(1)),
                Some("byte event cancel panicked".to_owned()),
            );
            assert_eq!(take_contained_panics(), vec![report; 4]);
            assert_eq!(contained_panic_counts(&worker, &start), (4, 0, 0));
        })
    });
}

/// A request head's encoded-headers callback that panics is counted at exchange scope. On a
/// head that ends its request, the exchange continues and completes. On a head with a body to
/// follow, the request fails with `INTERNAL_ERROR` before any byte is written, and the
/// connection resets. Each outcome is the submission's error, the response's body, the run's
/// error, the bytes written, the close, and the worker's contained panics by scope.
#[cfg(panic = "unwind")]
#[test]
fn owned_h1_upstream_encoded_headers_panic_fails_only_a_request_with_a_body() {
    type Outcome = (
        Option<HttpErrorCode>,
        Vec<u8>,
        Option<HttpErrorCode>,
        bool,
        CloseKind,
        (u64, u64, u64),
    );
    for ends_request in [true, false] {
        let outcome: Outcome = run_on_host(move |context| {
            Box::pin(async move {
                take_contained_panics();
                let worker = context.handle().clone();
                let start = worker.poll_stats();
                let gate = Arc::new(tokio::sync::Notify::new());
                let (transport, state) =
                    ScriptedTransport::new([ReadAction::WaitDataOrCancellation(
                        Arc::clone(&gate),
                        OK_RESPONSE.to_vec(),
                    )]);
                let (session, handle) =
                    OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
                let (mut head, body) = if ends_request {
                    (get_head(), Vec::new())
                } else {
                    scripted_request(false)
                };
                head.set_egress_headers_callback(|_size| panic!("encoded headers panicked"));
                let submission = handle.reserve_request().unwrap().submit(
                    head,
                    RequestSource::held(None, body),
                    CancellationToken::never(),
                );
                let (run, (submitted, body)) = futures::join!(session.run(), async {
                    match submission.await {
                        Ok(response) => {
                            let (_stream, mut source, _control) = response.into_source_parts();
                            gate.notify_one();
                            let read = read_response(&mut source).await;
                            drop(source);
                            close_once_idle(&handle).await;
                            (None, read.body)
                        }
                        Err(error) => (Some(error.code()), Vec::new()),
                    }
                });
                let reports = take_contained_panics();
                assert_eq!(
                    reports,
                    [(
                        PanicSite::HeadersEncoded,
                        Some(StreamId::new(1)),
                        Some("encoded headers panicked".to_owned())
                    )],
                    "ends request: {ends_request}"
                );
                let state = state.borrow();
                (
                    submitted,
                    body,
                    run.err().map(|error| error.code()),
                    state.writes.is_empty(),
                    state.close,
                    contained_panic_counts(&worker, &start),
                )
            })
        });
        let expected: Outcome = if ends_request {
            (
                None,
                b"ok".to_vec(),
                None,
                false,
                CloseKind::ShutdownWrite,
                (1, 0, 0),
            )
        } else {
            (
                Some(HttpErrorCode::INTERNAL_ERROR),
                Vec::new(),
                Some(HttpErrorCode::INTERNAL_ERROR),
                true,
                CloseKind::Reset,
                (1, 0, 0),
            )
        };
        assert_eq!(outcome, expected, "ends request: {ends_request}");
    }
}

/// A run whose poll unwinds, which only a fault in the run's own code can cause, is poisoned,
/// under `run` and `run_pull_through` alike. Its next poll returns `Err(INTERNAL_ERROR)`
/// without polling the driver, and dropping it abandons the driver, resetting the transport and
/// releasing it, without the join assertion and without a report.
#[cfg(panic = "unwind")]
#[test]
fn owned_h1_upstream_run_whose_poll_unwinds_is_poisoned() {
    for mode in [pull_through::Mode::Run, pull_through::Mode::PullThrough] {
        run_on_host(move |context| {
            Box::pin(async move {
                take_contained_panics();
                let mut fixture = pull_through::Fixture::new(context, mode, |_| {});
                crate::owned_h1::fault_next_upstream_run_poll();
                let unwound = catch_unwind(AssertUnwindSafe(|| fixture.poll_run()));
                let Err(panic) = unwound else {
                    panic!("{mode:?}: the faulted poll unwinds");
                };
                assert_eq!(panic_text(panic), "owned H1 upstream run fault");
                let starts = fixture.origin.starts.get();
                assert_eq!(starts, 1, "{mode:?}: the faulted poll started the read");
                let Poll::Ready(Err(error)) = fixture.poll_run() else {
                    panic!("{mode:?}: a poisoned run is ready with an error");
                };
                assert_eq!(error.code(), HttpErrorCode::INTERNAL_ERROR);
                assert_eq!(
                    fixture.origin.starts.get(),
                    starts,
                    "{mode:?}: the poisoned poll ran no driver code"
                );
                assert_eq!(fixture.state.borrow().close, CloseKind::Open);
                assert!(
                    catch_unwind(AssertUnwindSafe(|| fixture.drop_run())).is_ok(),
                    "{mode:?}: a poisoned run's abandonment does not assert its join"
                );
                assert_eq!(fixture.state.borrow().close, CloseKind::Reset);
                assert!(
                    fixture.origin.dropped.get(),
                    "{mode:?}: the driver is released"
                );
                assert!(take_contained_panics().is_empty());
            })
        });
    }
}

/// Polling a run after it completed returns `Err(INVALID_STATE_TRANSITION)`, under `run` and
/// `run_pull_through` alike.
#[test]
fn polling_a_finished_owned_upstream_run_returns_an_error() {
    for mode in [pull_through::Mode::Run, pull_through::Mode::PullThrough] {
        run_on_host(move |context| {
            Box::pin(async move {
                let mut fixture = pull_through::Fixture::new(context, mode, |_| {});
                assert!(fixture.poll_run().is_pending());
                assert!(fixture.handle.close_when_idle().unwrap());
                fixture
                    .settle_run()
                    .expect("the idle connection closes")
                    .unwrap();
                let Poll::Ready(Err(error)) = fixture.poll_run() else {
                    panic!("{mode:?}: a finished run is ready with an error");
                };
                assert_eq!(
                    error.code(),
                    HttpErrorCode::INVALID_STATE_TRANSITION,
                    "{mode:?}"
                );
            })
        });
    }
}

/// An upstream observer whose drop panics.
struct PanicsOnDrop;

impl crate::owned::OwnedUpstreamObserver for PanicsOnDrop {}

impl Drop for PanicsOnDrop {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            panic!("observer drop panicked");
        }
    }
}

/// A run that completes releases the driver field by field. A destructor that panics there,
/// here the observer's as the driver drops the connection's last reference, is counted as a
/// teardown step, and the run still returns its result.
#[cfg(panic = "unwind")]
#[test]
fn a_panicking_drop_after_owned_upstream_completion_is_reported() {
    run_on_host(|context| {
        Box::pin(async move {
            take_contained_panics();
            let worker = context.handle().clone();
            let start = worker.poll_stats();
            let (transport, state) = ScriptedTransport::new([]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            handle.set_observer(Rc::new(PanicsOnDrop)).unwrap();
            assert!(handle.close_when_idle().unwrap());
            drop(handle);
            session.run().await.unwrap();
            assert_eq!(state.borrow().close, CloseKind::ShutdownWrite);
            assert_eq!(
                take_contained_panics(),
                [(
                    PanicSite::Teardown,
                    None,
                    Some("observer drop panicked".to_owned())
                )]
            );
            assert_eq!(contained_panic_counts(&worker, &start), (0, 0, 1));
        })
    });
}

/// Bytes, or read failures, pushed by the test, which `PushedReadTransport` reads deliver as they
/// arrive.
#[derive(Default)]
struct PushedReads {
    chunks: RefCell<VecDeque<Result<Bytes, HttpErrorCode>>>,
    waker: RefCell<Option<Waker>>,
    /// How many times the transport was closed with a reset.
    resets: Arc<AtomicUsize>,
}

impl PushedReads {
    fn push(&self, bytes: &[u8]) {
        self.push_read(Ok(Bytes::copy_from_slice(bytes)));
    }

    /// Pushes a read (bytes, delivered as they are, or a failure).
    fn push_read(&self, read: Result<Bytes, HttpErrorCode>) {
        self.chunks.borrow_mut().push_back(read);
        let waker = self.waker.borrow_mut().take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// Writes and closes through a [`ScriptedTransport`]; each read takes the next pushed bytes,
/// waiting for them, and observes its cancellation when polled.
struct PushedReadTransport {
    inner: Rc<ScriptedTransport>,
    reads: Rc<PushedReads>,
}

impl UniplexTransport for PushedReadTransport {
    fn read(
        &self,
        _context: &WorkerContext,
        _maximum: usize,
        _allocation_size: usize,
        _timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        let reads = Rc::clone(&self.reads);
        Box::pin(std::future::poll_fn(move |context| {
            if let Some(read) = reads.chunks.borrow_mut().pop_front() {
                return Poll::Ready(match read {
                    Ok(bytes) => {
                        let length = bytes.len();
                        Ok(UniplexRead::new(
                            BufQueue::from_bytes(bytes, length).unwrap(),
                            false,
                        ))
                    }
                    Err(code) => Err(HttpError::with_message(code, b"pushed read failure")),
                });
            }
            if cancellation.is_cancellation_requested() {
                return Poll::Ready(Err(HttpError::with_message(
                    HttpErrorCode::CORO_CANCELLED,
                    b"pushed read cancelled",
                )));
            }
            *reads.waker.borrow_mut() = Some(context.waker().clone());
            Poll::Pending
        }))
    }

    fn write(
        &self,
        context: &WorkerContext,
        data: BufQueue,
        flags: ByteEventWriteFlags,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        self.inner
            .write(context, data, flags, timeout, cancellation)
    }

    fn shutdown_write(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.inner.shutdown_write(context)
    }

    fn close(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.inner.close(context)
    }

    fn close_with_reset(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.reads.resets.fetch_add(1, Ordering::SeqCst);
        self.inner.close_with_reset(context)
    }

    fn byte_event_capability(
        &self,
        context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError> {
        self.inner.byte_event_capability(context)
    }

    fn register_byte_event(
        &self,
        context: &WorkerContext,
        registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError> {
        self.inner.register_byte_event(context, registration)
    }
}

/// Sends a GET whose chunked response head the origin sends immediately, then, for each of
/// `chunks`, pushes it as one read, polls the driver, reads the chunk from the response source
/// and polls the driver again. Returns the driver turns, codec parses, cancel cause selections
/// and response events each chunk took.
fn streamed_chunk_turns(chunks: &'static [&'static [u8]]) -> Vec<(usize, usize, usize, usize)> {
    run_on_host(move |context| {
        Box::pin(async move {
            let (inner, _state) = ScriptedTransport::new([]);
            let reads = Rc::new(PushedReads::default());
            let transport = Rc::new(PushedReadTransport {
                inner,
                reads: Rc::clone(&reads),
            });
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let mut poll = Context::from_waker(Waker::noop());
            let mut run = std::pin::pin!(session.run());
            assert!(
                run.as_mut().poll(&mut poll).is_pending(),
                "the GET waits for its response"
            );
            let mut submission = std::pin::pin!(submission);
            let Poll::Ready(response) = submission.as_mut().poll(&mut poll) else {
                panic!("the GET is committed in the first poll");
            };
            let (_stream, mut source, _control) = response.unwrap().into_source_parts();
            reads.push(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
            assert!(run.as_mut().poll(&mut poll).is_pending());
            let Poll::Ready(head) = source.read_header_event().as_mut().poll(&mut poll) else {
                panic!("the response head is readable");
            };
            assert_eq!(head.unwrap().headers_mut().status_code().unwrap(), 200);
            let mut turns = Vec::new();
            for chunk in chunks {
                let before = (
                    driver_turns(),
                    codec_parses(),
                    cancel_selections(),
                    response_events(),
                );
                let mut wire = format!("{:x}\r\n", chunk.len()).into_bytes();
                wire.extend_from_slice(chunk);
                wire.extend_from_slice(b"\r\n");
                reads.push(&wire);
                assert!(
                    run.as_mut().poll(&mut poll).is_pending(),
                    "the read is parsed"
                );
                let Poll::Ready(event) = source.read_body_event(u32::MAX).as_mut().poll(&mut poll)
                else {
                    panic!("the chunk is readable");
                };
                let event = event.unwrap();
                let body = event.as_body().expect("a body event");
                assert_eq!(body.coalesce(body.chain_length()).unwrap(), *chunk);
                assert!(
                    run.as_mut().poll(&mut poll).is_pending(),
                    "the read chunk is acknowledged"
                );
                turns.push((
                    driver_turns() - before.0,
                    codec_parses() - before.1,
                    cancel_selections() - before.2,
                    response_events() - before.3,
                ));
            }
            reads.push(b"0\r\n\r\n");
            let (run, ()) = futures::join!(run, async {
                let rest = read_response_body(&mut source).await;
                assert!(rest.eom && rest.body.is_empty(), "{rest:?}");
                drop(source);
                close_once_idle(&handle).await;
            });
            run.unwrap();
            turns
        })
    })
}

/// Reads `source`'s remaining body events, after its final head, to its end.
async fn read_response_body(source: &mut HttpSourceHolder) -> ReadResponse {
    let mut read = ReadResponse::default();
    loop {
        match source.read_body_event(u32::MAX).await {
            Ok(event) => {
                if let Some(bytes) = event.as_body() {
                    read.body
                        .extend_from_slice(&bytes.coalesce(bytes.chain_length()).unwrap());
                }
                if event.eom() {
                    read.eom = true;
                    return read;
                }
            }
            Err(error) => {
                read.error = Some(error.code());
                return read;
            }
        }
    }
}

/// Each chunk of a streamed response takes two driver turns and one parse. The turn whose read
/// carries it parses it whole, and the turn that notes its consumption does not call the parser,
/// since no input arrived. Every turn has work to do.
#[test]
fn a_streamed_response_chunk_takes_two_driver_turns_and_one_parse() {
    let turns: Vec<_> = streamed_chunk_turns(&[b"one", b"two", b"three"])
        .iter()
        .map(|&(turns, parses, ..)| (turns, parses))
        .collect();
    assert_eq!(turns, [(2, 1); 3], "(driver turns, codec parses) per chunk");
}

/// Without a cancel, the driver's cancel checks, at its phase boundaries and before each codec
/// callback, report no pending cancellation without selecting a cause. A streamed response's
/// chunks do not build or look up a cancel error.
#[test]
fn a_streamed_response_chunk_selects_no_cancel_cause() {
    let selections: Vec<_> = streamed_chunk_turns(&[b"one", b"two", b"three"])
        .iter()
        .map(|&(turns, _, selections, ..)| (turns, selections))
        .collect();
    assert_eq!(
        selections,
        [(2, 0); 3],
        "(driver turns, cancel cause selections) per chunk"
    );
}

/// The response ignores a chunk's boundaries, its size line and its terminator, so of a chunk's
/// three codec callbacks only the body's is handled as an event.
#[test]
fn a_streamed_response_chunk_handles_only_its_body_event() {
    let counts: Vec<_> = streamed_chunk_turns(&[b"one", b"two", b"three"])
        .iter()
        .map(|&(turns, _, _, events)| (turns, events))
        .collect();
    assert_eq!(
        counts,
        [(2, 1); 3],
        "(driver turns, response events) per chunk"
    );
}

/// The driver hands the codec its own callback type, so the codec calls it directly for every
/// callback that parsing delivers. Only `on_ingress_eof`, which ends a close-delimited
/// response, still delivers its completion through `dyn HttpCodecCallback`.
#[test]
fn parsed_response_callbacks_are_not_delivered_through_dyn() {
    let responses: [(&str, &[u8]); 3] = [
        // The message's begin, head, body and completion.
        ("content-length", OK_RESPONSE),
        // The begin, head, chunk size line, body, chunk terminator and completion: the last
        // chunk has neither a size callback nor a terminator callback.
        (
            "chunked",
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n",
        ),
        // The begin, head and body; the completion at EOF.
        (
            "close-delimited",
            b"HTTP/1.1 200 OK\r\nDate: x\r\n\r\nstreamed until close",
        ),
    ];
    let deliveries: Vec<_> = responses
        .into_iter()
        .map(|(name, response)| {
            let (read, wire, deliveries) = owned_get_reads(vec![response.to_vec()]);
            assert!(read.eom, "{name}: {read:?}");
            assert_eq!(read.error, None, "{name}");
            assert_eq!(wire.error, None, "{name}");
            (name, deliveries)
        })
        .collect();
    assert_eq!(
        deliveries,
        [
            ("content-length", (4, 0)),
            ("chunked", (6, 0)),
            ("close-delimited", (4, 1)),
        ],
        "(response callbacks, of them through dyn) per response"
    );
}

/// A response read one byte at a time, so that the codec stops and resumes inside every head,
/// chunk size line, chunk and trailer, reads as it does in one read.
#[test]
fn a_response_read_one_byte_at_a_time_reads_as_in_one_read() {
    const RESPONSE: &[u8] = b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n6\r\n world\r\n0\r\ngrpc-status: 0\r\n\r\n";
    let (whole, whole_wire, _) = owned_get_reads(vec![RESPONSE.to_vec()]);
    let (bytes, bytes_wire, _) = owned_get_reads(RESPONSE.iter().map(|&byte| vec![byte]).collect());
    let expected = ReadResponse {
        statuses: vec![100, 200],
        body: b"hello world".to_vec(),
        trailers: vec![(b"grpc-status".to_vec(), b"0".to_vec())],
        eom: true,
        error: None,
    };
    assert_eq!(whole, expected);
    assert_eq!(bytes, expected);
    assert_eq!(whole_wire.error, None);
    assert_eq!(bytes_wire.error, None);
    assert_eq!(bytes_wire.close, whole_wire.close);
}

/// An exchange that is not cancelled never selects a cancel cause, from its commitment to its
/// retirement, whatever its request body.
#[test]
fn an_uncancelled_exchange_selects_no_cancel_cause() {
    type Request = fn() -> (HttpHeaderEvent, Vec<HttpBodyEvent>);
    let requests: [(&str, Request); 4] = [
        ("content-length", || scripted_request(false)),
        ("chunked", || scripted_request(true)),
        ("large", large_request),
        ("get", || (get_head(), Vec::new())),
    ];
    let selections: Vec<(&str, usize)> = requests
        .into_iter()
        .map(|(name, request)| (name, owned_exchange_run(request, OK_RESPONSE, b"ok").1))
        .collect();
    assert_eq!(
        selections,
        [
            ("content-length", 0),
            ("chunked", 0),
            ("large", 0),
            ("get", 0)
        ],
        "cancel cause selections per exchange"
    );
}

#[test]
fn an_unregistered_request_builds_no_byte_event_refusal() {
    type Request = fn() -> (HttpHeaderEvent, Vec<HttpBodyEvent>);
    let requests: [(&str, Request); 4] = [
        ("content-length", || scripted_request(false)),
        ("chunked", || scripted_request(true)),
        ("large", large_request),
        ("get", || (get_head(), Vec::new())),
    ];
    let refusals: Vec<(&str, usize)> = requests
        .into_iter()
        .map(|(name, request)| (name, owned_exchange_run(request, OK_RESPONSE, b"ok").2))
        .collect();
    assert_eq!(
        refusals,
        [
            ("content-length", 0),
            ("chunked", 0),
            ("large", 0),
            ("get", 0)
        ],
        "byte-event refusal errors per exchange whose events register nothing"
    );
}

/// Submits the request `request` builds on a new connection and polls the driver once, which
/// commits and writes it while the origin is silent. Returns the driver turns and phase checks
/// that poll took, and its writes. The origin then responds with `OK_RESPONSE`.
fn committed_request_turns(
    request: fn() -> (HttpHeaderEvent, Vec<HttpBodyEvent>),
) -> ((usize, usize), Vec<Vec<u8>>) {
    run_on_host(move |context| {
        Box::pin(async move {
            let (inner, state) = ScriptedTransport::new([]);
            let reads = Rc::new(PushedReads::default());
            let transport = Rc::new(PushedReadTransport {
                inner,
                reads: Rc::clone(&reads),
            });
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let (head, body) = request();
            let submission = handle.reserve_request().unwrap().submit(
                head,
                RequestSource::held(None, body),
                CancellationToken::never(),
            );
            let mut poll = Context::from_waker(Waker::noop());
            let mut run = std::pin::pin!(session.run());
            let before = (driver_turns(), phase_checks());
            assert!(
                run.as_mut().poll(&mut poll).is_pending(),
                "the request waits for its response"
            );
            let counts = (driver_turns() - before.0, phase_checks() - before.1);
            let writes = state.borrow().writes.clone();
            reads.push(OK_RESPONSE);
            let (run, ()) = futures::join!(run, async {
                let (_stream, mut source, _control) = submission.await.unwrap().into_source_parts();
                assert_eq!(read_response(&mut source).await.body, b"ok");
                drop(source);
                close_once_idle(&handle).await;
            });
            run.unwrap();
            (counts, writes)
        })
    })
}

/// The driver checks at most once per turn whether a phase before the write would act. A turn
/// that could start a write either has such a phase due, which holds the write back and stays
/// due in the next turn, or it starts the write and marks the write's signal. Either way the
/// next turn is due without a second check. So the poll that commits and writes a request
/// checks once per turn: at the write in each turn that serializes, and at the end of the turn
/// that takes the write's completion.
#[test]
fn a_committed_request_checks_the_phases_once_per_turn() {
    type Request = fn() -> (HttpHeaderEvent, Vec<HttpBodyEvent>);
    let requests: [(&str, Request); 3] = [
        ("get", || (get_head(), Vec::new())),
        ("content-length", || scripted_request(false)),
        ("chunked", || scripted_request(true)),
    ];
    let checks: Vec<(&str, (usize, usize))> = requests
        .into_iter()
        .map(|(name, request)| {
            let (counts, writes) = committed_request_turns(request);
            assert_eq!(writes.len(), 1, "{name}: the request is written at once");
            (name, counts)
        })
        .collect();
    assert_eq!(
        checks,
        [
            ("get", (2, 2)),
            ("content-length", (3, 3)),
            ("chunked", (4, 4))
        ],
        "(driver turns, phase checks) of the committing poll"
    );
}

/// A request body whose first read waits for `gate`, then yields once, waking its task; the reads
/// after it are ready immediately.
struct ResumedUpload {
    gate: Option<Arc<tokio::sync::Notify>>,
    body: VecDeque<HttpBodyEvent>,
}

impl HttpSource for ResumedUpload {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        unreachable!("an owned submission takes the head separately")
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let gate = self.gate.take();
        let event = self.body.pop_front().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"request body exhausted")
        });
        Box::pin(async move {
            if let Some(gate) = gate {
                gate.notified().await;
                let mut yielded = false;
                std::future::poll_fn(|context| {
                    if std::mem::replace(&mut yielded, true) {
                        Poll::Ready(())
                    } else {
                        context.waker().wake_by_ref();
                        Poll::Pending
                    }
                })
                .await;
            }
            event
        })
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {
        self.body.clear();
    }
}

/// A request body that becomes readable after its head is written, through a read that yields
/// once, goes out whole and in one piece in the poll that resumed it: the read's wake makes the
/// exchange due again within that poll, and each body event it serializes holds the write back
/// until the body ends.
#[test]
fn a_resumed_request_body_is_written_in_one_piece() {
    run_on_host(|context| {
        Box::pin(async move {
            let (inner, state) = ScriptedTransport::new([]);
            let reads = Rc::new(PushedReads::default());
            let transport = Rc::new(PushedReadTransport {
                inner,
                reads: Rc::clone(&reads),
            });
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let gate = Arc::new(tokio::sync::Notify::new());
            let upload = ResumedUpload {
                gate: Some(Arc::clone(&gate)),
                body: VecDeque::from([
                    HttpBodyEvent::body(queue(b"hello "), false),
                    HttpBodyEvent::body(queue(b"world"), true),
                ]),
            };
            let submission = handle.reserve_request().unwrap().submit(
                post_head(None, false),
                HttpSourceHolder::new(Box::new(upload)),
                CancellationToken::never(),
            );
            let mut poll = Context::from_waker(Waker::noop());
            let mut run = std::pin::pin!(session.run());
            assert!(run.as_mut().poll(&mut poll).is_pending());
            assert_eq!(state.borrow().writes.len(), 1, "the head is written first");
            gate.notify_one();
            assert!(run.as_mut().poll(&mut poll).is_pending());
            assert_eq!(
                state.borrow().writes[1..],
                [b"6\r\nhello \r\n5\r\nworld\r\n0\r\n\r\n".to_vec()],
                "the body is written in one piece"
            );
            reads.push(OK_RESPONSE);
            let (run, ()) = futures::join!(run, async {
                let (_stream, mut source, _control) = submission.await.unwrap().into_source_parts();
                assert_eq!(read_response(&mut source).await.body, b"ok");
                drop(source);
                close_once_idle(&handle).await;
            });
            run.unwrap();
        })
    });
}

/// A waker that cancels the worker.
struct CancelWorkerOnWake(roxygen_runtime::worker::WorkerHandle);

impl std::task::Wake for CancelWorkerOnWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        assert!(self.0.begin_shutdown());
    }
}

/// A worker cancel from code that a parse runs ends the connection within the poll that parsed
/// it. Here the code is the resume waker of a suspended body read, woken by the parse's last
/// event. The phases after the parse do not observe cancels, and a worker cancel does not mark a
/// signal, so the end of the turn detects the cancel and runs the turn that observes it.
#[test]
fn worker_cancel_from_a_body_read_wake_ends_the_connection_in_that_poll() {
    run_on_host(|context| {
        Box::pin(async move {
            let (inner, state) = ScriptedTransport::new([]);
            let reads = Rc::new(PushedReads::default());
            let transport = Rc::new(PushedReadTransport {
                inner,
                reads: Rc::clone(&reads),
            });
            let worker = context.handle().clone();
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let mut poll = Context::from_waker(Waker::noop());
            let mut run = std::pin::pin!(session.run());
            assert!(
                run.as_mut().poll(&mut poll).is_pending(),
                "the GET waits for its response"
            );
            let mut submission = std::pin::pin!(submission);
            let Poll::Ready(response) = submission.as_mut().poll(&mut poll) else {
                panic!("the GET is committed in the first poll");
            };
            let (_stream, mut source, _control) = response.unwrap().into_source_parts();
            reads.push(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\n");
            assert!(run.as_mut().poll(&mut poll).is_pending());
            let Poll::Ready(head) = source.read_header_event().as_mut().poll(&mut poll) else {
                panic!("the response head is readable");
            };
            assert_eq!(head.unwrap().headers_mut().status_code().unwrap(), 200);
            let Poll::Ready(Ok(event)) = source.read_body_event(u32::MAX).as_mut().poll(&mut poll)
            else {
                panic!("a read before the body suspends");
            };
            let HttpBodyEventKind::Suspend(mut resume) = event.into_kind() else {
                panic!("no body has arrived");
            };
            let waker = Waker::from(Arc::new(CancelWorkerOnWake(worker)));
            assert!(
                resume
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            reads.push(b"one");
            let Poll::Ready(run) = run.as_mut().poll(&mut poll) else {
                panic!("the worker cancel ends the connection in the poll that parsed the body");
            };
            assert_eq!(
                run.map_err(|error| error.code()).err(),
                Some(HttpErrorCode::CORO_CANCELLED)
            );
            assert_eq!(state.borrow().close, CloseKind::Reset);
        })
    });
}

/// What one poll of the driver did, and what the response source yielded after it.
#[derive(Debug, PartialEq)]
struct PublicationCancel {
    /// The poll's result: `None` while the run is pending.
    run: Option<Result<(), HttpErrorCode>>,
    /// The body the response source yields before it fails.
    body: Vec<u8>,
    /// The response source's failure.
    error: HttpErrorCode,
    close: CloseKind,
}

/// Sends a GET whose chunked response head is read first. A body read then suspends, and its
/// resume future is polled with a waker that cancels the request's token, as a caller that
/// cancels when the body arrives. Then the origin sends `reads` (bytes, or a failed read) and the
/// driver is polled once for them. Returns that poll's result, then what the response source
/// yields.
fn cancel_from_a_body_publication(
    reads: &'static [Result<&'static [u8], HttpErrorCode>],
) -> PublicationCancel {
    let reads = reads
        .iter()
        .map(|read| read.map(Bytes::from_static))
        .collect();
    cancel_from_a_body_publication_with(reads, Arc::default())
}

/// As `cancel_from_a_body_publication`, with reads that the test owns, on a transport that counts
/// its resets in `resets`.
fn cancel_from_a_body_publication_with(
    reads: Vec<Result<Bytes, HttpErrorCode>>,
    resets: Arc<AtomicUsize>,
) -> PublicationCancel {
    run_on_host(move |context| {
        Box::pin(async move {
            let (inner, state) = ScriptedTransport::new([]);
            let pushed = Rc::new(PushedReads {
                resets,
                ..PushedReads::default()
            });
            let transport = Rc::new(PushedReadTransport {
                inner,
                reads: Rc::clone(&pushed),
            });
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let source = CancellationSource::new();
            let token = source.token();
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                token,
            );
            let mut poll = Context::from_waker(Waker::noop());
            let mut run = std::pin::pin!(session.run());
            assert!(
                run.as_mut().poll(&mut poll).is_pending(),
                "the GET waits for its response"
            );
            let mut submission = std::pin::pin!(submission);
            let Poll::Ready(response) = submission.as_mut().poll(&mut poll) else {
                panic!("the GET is committed in the first poll");
            };
            let (_stream, mut response, _control) = response.unwrap().into_source_parts();
            pushed.push(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
            assert!(run.as_mut().poll(&mut poll).is_pending());
            let Poll::Ready(head) = response.read_header_event().as_mut().poll(&mut poll) else {
                panic!("the response head is readable");
            };
            assert_eq!(head.unwrap().headers_mut().status_code().unwrap(), 200);
            let Poll::Ready(Ok(event)) =
                response.read_body_event(u32::MAX).as_mut().poll(&mut poll)
            else {
                panic!("a read before the body suspends");
            };
            let HttpBodyEventKind::Suspend(mut resume) = event.into_kind() else {
                panic!("no body has arrived");
            };
            let waker = Waker::from(Arc::new(CancelOnWake {
                source,
                task: std::sync::Mutex::new(None),
            }));
            assert!(
                resume
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            for read in reads {
                pushed.push_read(read);
            }
            let run = match run.as_mut().poll(&mut poll) {
                Poll::Ready(run) => Some(run.map(drop).map_err(|error| error.code())),
                Poll::Pending => None,
            };
            drop(resume);
            let mut body = Vec::new();
            let error = loop {
                let Poll::Ready(read) = response.read_body_event(u32::MAX).as_mut().poll(&mut poll)
                else {
                    panic!("the failed response is readable");
                };
                match read {
                    Ok(event) => {
                        assert!(!event.eom(), "the response is unfinished");
                        let bytes = event.as_body().expect("a body event");
                        body.extend_from_slice(&bytes.coalesce(bytes.chain_length()).unwrap());
                    }
                    Err(error) => break error.code(),
                }
            };
            let close = state.borrow().close;
            PublicationCancel {
                run,
                body,
                error,
                close,
            }
        })
    })
}

/// A cancel that a chunk's body publication raises stops the parse before any later event acts:
/// the response fails, though the same read carries its next chunk and its end, and the
/// buffered body is discarded.
#[test]
fn request_cancel_from_a_chunk_publication_fails_the_response_ending_in_that_read() {
    assert_eq!(
        cancel_from_a_body_publication(&[Ok(b"3\r\none\r\n3\r\ntwo\r\n0\r\n\r\n")]),
        PublicationCancel {
            run: Some(Err(HttpErrorCode::CORO_CANCELLED)),
            body: Vec::new(),
            error: HttpErrorCode::CORO_CANCELLED,
            close: CloseKind::Reset,
        }
    );
}

/// A cancel that a chunk's body publication raises, when the read ends with the chunk, is
/// observed by that parse, and so selected before a failure of the read after it.
#[test]
fn request_cancel_from_a_chunk_publication_precedes_a_held_read_failure() {
    assert_eq!(
        cancel_from_a_body_publication(&[
            Ok(b"3\r\none\r\n"),
            Err(HttpErrorCode::TRANSPORT_READ_ERROR)
        ]),
        PublicationCancel {
            run: Some(Err(HttpErrorCode::CORO_CANCELLED)),
            body: Vec::new(),
            error: HttpErrorCode::CORO_CANCELLED,
            close: CloseKind::Reset,
        }
    );
}

/// A read buffer that notes, when it is released, how many times its transport had been reset.
struct ResetObservedBytes {
    bytes: &'static [u8],
    resets: Arc<AtomicUsize>,
    /// `usize::MAX` until the buffer is released.
    resets_at_release: Arc<AtomicUsize>,
}

impl AsRef<[u8]> for ResetObservedBytes {
    fn as_ref(&self) -> &[u8] {
        self.bytes
    }
}

impl Drop for ResetObservedBytes {
    fn drop(&mut self) {
        self.resets_at_release
            .store(self.resets.load(Ordering::SeqCst), Ordering::SeqCst);
    }
}

/// A cancel that a chunk's body publication raises stops the parse at that chunk's end: the
/// next chunk's size line, read separately in the same turn, stays unparsed, so its buffer is
/// released with the driver, after the abort reset the transport, not before the abort.
#[test]
fn request_cancel_from_a_chunk_publication_leaves_a_later_read_unparsed() {
    let resets = Arc::new(AtomicUsize::new(0));
    let resets_at_release = Arc::new(AtomicUsize::new(usize::MAX));
    let size_line = Bytes::from_owner(ResetObservedBytes {
        bytes: b"3\r\n",
        resets: Arc::clone(&resets),
        resets_at_release: Arc::clone(&resets_at_release),
    });
    assert_eq!(
        cancel_from_a_body_publication_with(
            vec![Ok(Bytes::from_static(b"3\r\none\r\n")), Ok(size_line)],
            Arc::clone(&resets),
        ),
        PublicationCancel {
            run: Some(Err(HttpErrorCode::CORO_CANCELLED)),
            body: Vec::new(),
            error: HttpErrorCode::CORO_CANCELLED,
            close: CloseKind::Reset,
        }
    );
    assert_eq!(
        resets_at_release.load(Ordering::SeqCst),
        1,
        "the unparsed size line is released after the transport reset"
    );
}

/// A chunk's body publication that fails, as the caller dropped the response source, stops the
/// parse at that chunk's end: the next chunk's size line, read separately in the same turn,
/// stays unparsed, so its buffer is released with the driver, after the abort reset the
/// transport, not before the abort.
#[test]
fn a_failed_chunk_publication_leaves_a_later_read_unparsed() {
    let resets = Arc::new(AtomicUsize::new(0));
    let resets_at_release = Arc::new(AtomicUsize::new(usize::MAX));
    let size_line = Bytes::from_owner(ResetObservedBytes {
        bytes: b"3\r\n",
        resets: Arc::clone(&resets),
        resets_at_release: Arc::clone(&resets_at_release),
    });
    let (run, close) = run_on_host(move |context| {
        Box::pin(async move {
            let (inner, state) = ScriptedTransport::new([]);
            let pushed = Rc::new(PushedReads {
                resets,
                ..PushedReads::default()
            });
            let transport = Rc::new(PushedReadTransport {
                inner,
                reads: Rc::clone(&pushed),
            });
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let mut poll = Context::from_waker(Waker::noop());
            let mut run = std::pin::pin!(session.run());
            assert!(
                run.as_mut().poll(&mut poll).is_pending(),
                "the GET waits for its response"
            );
            let mut submission = std::pin::pin!(submission);
            let Poll::Ready(response) = submission.as_mut().poll(&mut poll) else {
                panic!("the GET is committed in the first poll");
            };
            let (_stream, mut response, control) = response.unwrap().into_source_parts();
            pushed.push(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
            assert!(run.as_mut().poll(&mut poll).is_pending());
            let Poll::Ready(head) = response.read_header_event().as_mut().poll(&mut poll) else {
                panic!("the response head is readable");
            };
            assert_eq!(head.unwrap().headers_mut().status_code().unwrap(), 200);
            drop((response, control));
            pushed.push(b"3\r\none\r\n");
            pushed.push_read(Ok(size_line));
            let run = match run.as_mut().poll(&mut poll) {
                Poll::Ready(run) => Some(run.map(drop).map_err(|error| error.code())),
                Poll::Pending => None,
            };
            (run, state.borrow().close)
        })
    });
    assert_eq!(
        (run, close),
        (
            Some(Err(HttpErrorCode::FLOW_CONTROL_ERROR)),
            CloseKind::Reset
        ),
        "the body publication fails the run"
    );
    assert_eq!(
        resets_at_release.load(Ordering::SeqCst),
        1,
        "the unparsed size line is released after the transport reset"
    );
}

/// A cancel that the body publication raises when the body is the read's last event is not
/// observed by that parse: a failure of the read after it, held until those bytes are parsed,
/// is applied first.
#[test]
fn a_held_read_failure_precedes_a_cancel_from_the_last_body_publication() {
    assert_eq!(
        cancel_from_a_body_publication(&[
            Ok(b"3\r\none"),
            Err(HttpErrorCode::TRANSPORT_READ_ERROR)
        ]),
        PublicationCancel {
            run: Some(Err(HttpErrorCode::TRANSPORT_READ_ERROR)),
            body: Vec::new(),
            error: HttpErrorCode::TRANSPORT_READ_ERROR,
            close: CloseKind::Reset,
        }
    );
}

/// The owned upstream driver writes each request in the recorded pieces. Skipping phases that
/// have no work must not change them.
#[test]
fn owned_upstream_h1_writes_in_the_recorded_pieces() {
    let recorded: &[(&str, &[usize])] = &[
        ("content-length", &[94]),
        ("chunked", &[117]),
        ("trailered", &[123]),
        ("padded content-length", &[87]),
        ("padded chunked", &[111]),
        ("padding eom content-length", &[87]),
        ("padding eom chunked", &[106]),
        ("large", &[65519, 65519, 65519, 65519, 155]),
        ("trace", &[88]),
        ("get", &[56]),
    ];
    type Request = fn() -> (HttpHeaderEvent, Vec<HttpBodyEvent>);
    let requests: [(&str, Request); 10] = [
        ("content-length", || scripted_request(false)),
        ("chunked", || scripted_request(true)),
        ("trailered", trailered_request),
        ("padded content-length", || padded_request(false)),
        ("padded chunked", || padded_request(true)),
        ("padding eom content-length", || padding_eom_request(false)),
        ("padding eom chunked", || padding_eom_request(true)),
        ("large", large_request),
        ("trace", trace_request),
        ("get", || (get_head(), Vec::new())),
    ];
    let actual: Vec<String> = requests
        .into_iter()
        .map(|(name, request)| {
            let writes: Vec<usize> = owned_exchange_writes(request, OK_RESPONSE, b"ok")
                .iter()
                .map(Vec::len)
                .collect();
            format!("(\"{name}\", &{writes:?}),")
        })
        .collect();
    let expected: Vec<String> = recorded
        .iter()
        .map(|(name, writes)| format!("(\"{name}\", &{writes:?}),"))
        .collect();
    assert_eq!(actual, expected, "write lengths:\n{}", actual.join("\n"));
}

/// A submission the driver rejects returns its slot, and the poll that rejected it notes the
/// connection idle before returning, as the quiescent turn it ends with always has. An owner
/// that polls the driver only when woken observes the connection idle immediately.
#[test]
fn a_refused_submission_leaves_the_connection_idle_in_that_poll() {
    for case in ["refused upgrade", "cancelled token"] {
        run_on_host(move |context| {
            Box::pin(async move {
                let (transport, state) = ScriptedTransport::new([]);
                let (session, handle) =
                    OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
                let mut poll = Context::from_waker(Waker::noop());
                let mut run = std::pin::pin!(session.run());
                assert!(run.as_mut().poll(&mut poll).is_pending(), "{case}");
                assert!(handle.is_idle(), "{case}: a new connection is idle");
                let reservation = handle.reserve_request().unwrap();
                let token = CancellationSource::new();
                let mut head = get_head();
                if case == "refused upgrade" {
                    head.headers_mut()
                        .headers_mut()
                        .set(HttpHeaderCode::Upgrade, b"websocket");
                } else {
                    token.request_cancellation();
                }
                let mut submission = std::pin::pin!(reservation.submit(
                    head,
                    HttpSourceHolder::empty(),
                    token.token(),
                ));
                assert!(!handle.is_idle(), "{case}: the slot is taken");
                assert!(run.as_mut().poll(&mut poll).is_pending(), "{case}");
                assert!(
                    handle.is_idle(),
                    "{case}: the poll that returned the slot noted the connection idle"
                );
                assert!(
                    matches!(submission.as_mut().poll(&mut poll), Poll::Ready(Err(_))),
                    "{case}: the submission is refused"
                );
                assert!(handle.close_when_idle().unwrap());
                run.await.unwrap();
                assert!(state.borrow().writes.is_empty(), "{case}");
            })
        });
    }
}

/// A request body of one-byte chunks, each ready immediately. The `cancel_at`th read cancels
/// the connection through `handle`.
struct BurstUpload {
    reads: usize,
    cancel_at: usize,
    handle: crate::owned_h1::OwnedH1UpstreamHandle,
}

impl HttpSource for BurstUpload {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        unreachable!("an owned submission takes the head separately")
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.reads += 1;
        if self.reads == self.cancel_at {
            self.handle.cancel();
        }
        Box::pin(std::future::ready(Ok(HttpBodyEvent::body(
            queue(b"x"),
            false,
        ))))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

/// The write lengths of the owned upstream driver that, under a 64-byte write size and with
/// writes that complete immediately, sends a chunked POST whose body chunks are one byte each
/// and ready immediately, until the `cancel_at`th body read cancels the connection.
fn upstream_burst_writes(cancel_at: usize) -> Vec<usize> {
    run_on_host(move |context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([]);
            let mut options = upstream_options(context, transport);
            options.io.maximum_write_size = 64;
            let (session, handle) = OwnedH1UpstreamSession::new(options).unwrap();
            let upload = HttpSourceHolder::new(Box::new(BurstUpload {
                reads: 0,
                cancel_at,
                handle: handle.clone(),
            }));
            let submission = handle.reserve_request().unwrap().submit(
                post_head(None, false),
                upload,
                CancellationToken::never(),
            );
            let (run, _response) = futures::join!(session.run(), submission);
            assert_eq!(
                run.map_err(|error| error.code()).err(),
                Some(HttpErrorCode::CORO_CANCELLED)
            );
            state.borrow().writes.iter().map(Vec::len).collect()
        })
    })
}

/// A burst of ready request body events that runs past the turn budget, and a cancel that
/// discards what is not yet written, write the recorded pieces. Do not change them. A turn that
/// is skipped because no phase has work still counts against the budget, so the write that the
/// budget forces at its last turn holds the recorded bytes.
#[test]
fn a_cancelled_request_burst_past_the_turn_budget_writes_the_recorded_pieces() {
    let recorded: &[(usize, &[usize])] = &[
        (55, &[64, 51, 48, 48, 48, 48, 48, 42]),
        (56, &[64, 51, 48, 48, 48, 48, 48, 42]),
        (57, &[64, 51, 48, 48, 48, 48, 48, 42]),
        (60, &[64, 51, 48, 48, 48, 48, 48, 42, 48]),
        (61, &[64, 51, 48, 48, 48, 48, 48, 42, 48]),
        (62, &[64, 51, 48, 48, 48, 48, 48, 42, 48]),
        (
            110,
            &[64, 51, 48, 48, 48, 48, 48, 42, 48, 48, 48, 48, 48, 48, 24],
        ),
        (
            118,
            &[
                64, 51, 48, 48, 48, 48, 48, 42, 48, 48, 48, 48, 48, 48, 24, 48,
            ],
        ),
        (
            119,
            &[
                64, 51, 48, 48, 48, 48, 48, 42, 48, 48, 48, 48, 48, 48, 24, 48,
            ],
        ),
        (
            120,
            &[
                64, 51, 48, 48, 48, 48, 48, 42, 48, 48, 48, 48, 48, 48, 24, 48, 48,
            ],
        ),
    ];
    let actual: Vec<String> = [55, 56, 57, 60, 61, 62, 110, 118, 119, 120]
        .into_iter()
        .map(|cancel_at| format!("({cancel_at}, &{:?}),", upstream_burst_writes(cancel_at)))
        .collect();
    let expected: Vec<String> = recorded
        .iter()
        .map(|(cancel_at, writes)| format!("({cancel_at}, &{writes:?}),"))
        .collect();
    assert_eq!(actual, expected, "write lengths:\n{}", actual.join("\n"));
}

/// Many times what the TCP connection holds while the origin does not read.
const TCP_UPLOAD: usize = 16 << 20;

/// The send and receive buffer size that both ends of the TCP connection set. Fixed buffers do
/// not autotune, so what the connection holds while the origin does not read stays far below
/// `TCP_UPLOAD` whatever the host's TCP buffer settings.
const PINNED_SOCKET_BUFFER: u32 = 64 << 10;

/// A patterned request body in events of at most 64 KiB, recording how many bytes the driver
/// has pulled.
struct CountedUpload {
    body: Rc<Vec<u8>>,
    pulled: Rc<Cell<usize>>,
}

impl HttpSource for CountedUpload {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        panic!("an owned submission takes the request head separately")
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let start = self.pulled.get();
        let end = self
            .body
            .len()
            .min(start + (maximum as usize).min(64 << 10));
        self.pulled.set(end);
        Box::pin(std::future::ready(Ok(HttpBodyEvent::body(
            queue(&self.body[start..end]),
            end == self.body.len(),
        ))))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

/// Sleeps until `ready` holds.
async fn sleep_until(mut ready: impl FnMut() -> bool) {
    while !ready() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Owned HTTP/1.1 upstream over a real TCP `OwnedByteStream`, which offers the native write
/// queue. A GET, then a POST, run on one connection. The POST's body pends against an origin
/// that does not read, until the origin reads both requests whole. The session then closes the
/// idle connection cleanly.
#[test]
fn owned_h1_upstream_runs_sequential_exchanges_over_a_tcp_byte_stream() {
    use roxygen_transport::tcp::TcpConnectOptions;
    use roxygen_transport::tcp::TcpSocketOption;
    use roxygen_transport::tcp::TcpSocketOptionPhase;
    use roxygen_transport::tcp::TcpSocketOptionValue;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    const GET_WIRE: &[u8] = b"GET / HTTP/1.1\r\nHost: origin\r\nConnection: keep-alive\r\n\r\n";
    let (received, responses, stalled_at) = Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(LocalSet::new().run_until(async {
            let listener = tokio::net::TcpSocket::new_v4().unwrap();
            listener.set_send_buffer_size(PINNED_SOCKET_BUFFER).unwrap();
            listener.set_recv_buffer_size(PINNED_SOCKET_BUFFER).unwrap();
            listener.bind("127.0.0.1:0".parse().unwrap()).unwrap();
            // Accepted sockets inherit the listener's buffers.
            let listener = listener.listen(1).unwrap();
            let port = listener.local_addr().unwrap().port();
            let (worker, driver) = WorkerDriver::new(16).unwrap();
            let call = worker
                .call_local(move |context| async move {
                    let accept = async { listener.accept().await.unwrap().0 };
                    let pinned_buffers = [libc::SO_SNDBUF, libc::SO_RCVBUF]
                        .map(|name| TcpSocketOption {
                            level: libc::SOL_SOCKET,
                            name,
                            value: TcpSocketOptionValue::Integer(PINNED_SOCKET_BUFFER as i32),
                            phase: TcpSocketOptionPhase::PreConnect,
                        })
                        .into();
                    let connect = roxygen_transport::tcp::TcpTransport::connect_with_options(
                        &context,
                        roxygen_core::message::SocketAddress::new("127.0.0.1", port),
                        roxygen_transport::byte_stream::ByteStreamLimits::new(
                            64 << 10,
                            64 << 10,
                            8,
                        )
                        .unwrap(),
                        Duration::from_secs(5),
                        CancellationToken::never(),
                        TcpConnectOptions {
                            socket_options: pinned_buffers,
                            ..Default::default()
                        },
                    );
                    let (mut origin, transport) = tokio::join!(accept, connect);
                    let transport: Rc<dyn UniplexTransport> = Rc::new(transport.unwrap());
                    assert!(
                        transport
                            .try_write_queue(
                                &context,
                                64 << 10,
                                Duration::ZERO,
                                CancellationToken::never()
                            )
                            .unwrap()
                            .is_some(),
                        "the TCP transport offers its native write queue"
                    );
                    let (session, handle) =
                        OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
                    // The origin and the submitter run in their own task, so their wakes never
                    // poll the session.
                    let script = tokio::task::spawn_local(async move {
                        let body = Rc::new(patterned(TCP_UPLOAD));
                        let pulled = Rc::new(Cell::new(0));
                        let mut received = vec![0; GET_WIRE.len()];
                        let first = handle.reserve_request().unwrap().submit(
                            get_head(),
                            HttpSourceHolder::empty(),
                            CancellationToken::never(),
                        );
                        let (_stream, mut source, _control) =
                            first.await.unwrap().into_source_parts();
                        origin.read_exact(&mut received).await.unwrap();
                        origin.write_all(OK_RESPONSE).await.unwrap();
                        let first = read_response(&mut source).await;
                        drop(source);
                        sleep_until(|| handle.is_idle()).await;
                        let second = handle.reserve_request().unwrap().submit(
                            post_head(Some(TCP_UPLOAD), false),
                            HttpSourceHolder::new(Box::new(CountedUpload {
                                body: Rc::clone(&body),
                                pulled: Rc::clone(&pulled),
                            })),
                            CancellationToken::never(),
                        );
                        let (_stream, mut source, _control) =
                            second.await.unwrap().into_source_parts();
                        // The origin does not read until the driver stops pulling the body.
                        let mut previous = usize::MAX;
                        while pulled.get() != previous {
                            previous = pulled.get();
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                        let stalled_at = pulled.get();
                        let mut rest = vec![0; declared_head_wire(TCP_UPLOAD).len() + TCP_UPLOAD];
                        origin.read_exact(&mut rest).await.unwrap();
                        received.extend_from_slice(&rest);
                        origin.write_all(OK_RESPONSE).await.unwrap();
                        let second = read_response(&mut source).await;
                        drop(source);
                        sleep_until(|| handle.is_idle()).await;
                        assert!(handle.close_when_idle().unwrap());
                        let mut trailing = Vec::new();
                        origin.read_to_end(&mut trailing).await.unwrap();
                        assert!(trailing.is_empty(), "the session wrote past its requests");
                        (received, [first, second], stalled_at)
                    });
                    let (run, script) =
                        match futures::future::select(Box::pin(session.run()), script).await {
                            futures::future::Either::Left((run, script)) => (run, script.await),
                            futures::future::Either::Right((Err(failure), _)) => {
                                std::panic::resume_unwind(failure.into_panic())
                            }
                            futures::future::Either::Right((script, run)) => (run.await, script),
                        };
                    run.expect("the session closes the idle connection cleanly");
                    script.unwrap_or_else(|failure| std::panic::resume_unwind(failure.into_panic()))
                })
                .unwrap();
            let driver_task = tokio::task::spawn_local(driver);
            let result = tokio::time::timeout(Duration::from_secs(30), call)
                .await
                .expect("the TCP exchanges finish")
                .unwrap();
            worker.begin_shutdown();
            driver_task.await.unwrap();
            result
        }));
    assert!(
        stalled_at < TCP_UPLOAD,
        "a write pends against the origin that does not read, so the driver stops pulling the \
         body: it stopped at {stalled_at} bytes"
    );
    let mut expected = GET_WIRE.to_vec();
    expected.extend(declared_head_wire(TCP_UPLOAD));
    expected.extend(patterned(TCP_UPLOAD));
    assert!(
        received == expected,
        "the origin receives the GET, then the POST head and its whole body, in order"
    );
    for response in responses {
        assert_eq!(
            response,
            ReadResponse {
                statuses: vec![200],
                body: b"ok".to_vec(),
                trailers: Vec::new(),
                eom: true,
                error: None,
            }
        );
    }
}

/// Pull-through exchanges, whose response body drives its connection's driver from inside its
/// reads.
mod pull_through {
    use roxygen_core::stream_source::HttpResponseReader;
    use roxygen_core::stream_source::HttpResponseSource;
    use roxygen_core::stream_source::HttpResponseSourceOwner;
    use roxygen_runtime::host::LocalHost;

    use super::*;
    use crate::owned_h1::OwnedH1PullThroughBody;

    /// One response's reads: a pull-through body's, or a suspend-free exclusive reader's, as a
    /// consumer of a plain owned submission reads.
    pub(super) trait Reads {
        fn poll_header(
            &mut self,
            context: &mut Context<'_>,
        ) -> Poll<Result<HttpHeaderEvent, HttpError>>;

        fn poll_body(
            &mut self,
            context: &mut Context<'_>,
            maximum: u32,
        ) -> Poll<Result<HttpBodyEvent, HttpError>>;
    }

    impl Reads for OwnedH1PullThroughBody {
        fn poll_header(
            &mut self,
            context: &mut Context<'_>,
        ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
            OwnedH1PullThroughBody::poll_header(self, context)
        }

        fn poll_body(
            &mut self,
            context: &mut Context<'_>,
            maximum: u32,
        ) -> Poll<Result<HttpBodyEvent, HttpError>> {
            OwnedH1PullThroughBody::poll_body(self, context, maximum)
        }
    }

    /// A suspend-free exclusive reader of a plain submission's response.
    pub(super) struct Waiting {
        reader: HttpResponseReader,
        owner: HttpResponseSourceOwner,
    }

    impl Waiting {
        pub(super) fn new(source: HttpResponseSource) -> Self {
            let (owner, mut reader) = source.split();
            reader.set_suspend_free(true);
            Self { reader, owner }
        }
    }

    impl Reads for Waiting {
        fn poll_header(
            &mut self,
            context: &mut Context<'_>,
        ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
            if !self.reader.is_active()
                && let Err(error) = self.reader.arm_header(&self.owner)
            {
                return Poll::Ready(Err(error));
            }
            self.reader.poll_header(context)
        }

        fn poll_body(
            &mut self,
            context: &mut Context<'_>,
            maximum: u32,
        ) -> Poll<Result<HttpBodyEvent, HttpError>> {
            if !self.reader.is_active()
                && let Err(error) = self.reader.arm_body(&self.owner, maximum)
            {
                return Poll::Ready(Err(error));
            }
            self.reader.poll_body(context)
        }
    }

    /// Reads a response to its end, `maximum` body bytes at a time; a slow reader yields to the
    /// host before each body read.
    pub(super) async fn read_all(reads: &mut impl Reads, maximum: u32, slow: bool) -> ReadResponse {
        let mut read = ReadResponse::default();
        loop {
            match poll_fn(|context| reads.poll_header(context)).await {
                Ok(mut head) => {
                    read.statuses
                        .push(head.headers_mut().status_code().unwrap());
                    if head.eom() {
                        read.eom = true;
                        return read;
                    }
                    if head.is_final().unwrap() {
                        break;
                    }
                }
                Err(error) => {
                    read.error = Some(error.code());
                    return read;
                }
            }
        }
        loop {
            if slow {
                tokio::task::yield_now().await;
            }
            match poll_fn(|context| reads.poll_body(context, maximum)).await {
                Ok(event) => {
                    if let Some(bytes) = event.as_body() {
                        let length = bytes.chain_length();
                        read.body
                            .extend_from_slice(&bytes.coalesce(length).unwrap());
                    }
                    if let HttpBodyEventKind::Trailers(trailers) = event.kind() {
                        read.trailers.extend(
                            trailers
                                .iter()
                                .map(|(_, name, value)| (name.to_vec(), value.to_vec())),
                        );
                    }
                    if event.eom() {
                        read.eom = true;
                        return read;
                    }
                }
                Err(error) => {
                    read.error = Some(error.code());
                    return read;
                }
            }
        }
    }

    /// How an exchange runs: a plain submission under `run`, read with a suspend-free reader;
    /// a pull-through submission under `run_pull_through`, read through its body; or a plain
    /// submission under `run_pull_through`.
    #[derive(Clone, Copy, Debug, PartialEq)]
    pub(super) enum Mode {
        Run,
        PullThrough,
        PlainOnPullThrough,
    }

    /// The gates a case's origin waits on: `response` opens once the request is committed,
    /// `after` once the reader has read the response and dropped its reads.
    pub(super) struct Gates {
        pub(super) response: Arc<tokio::sync::Notify>,
        pub(super) after: Arc<tokio::sync::Notify>,
    }

    /// One response-reading exchange of the comparison matrix.
    #[derive(Clone, Copy)]
    pub(super) struct Case {
        pub(super) name: &'static str,
        pub(super) request: fn() -> (HttpHeaderEvent, Vec<HttpBodyEvent>),
        pub(super) reads: fn(&Gates) -> Vec<ReadAction>,
        pub(super) configure: fn(&mut UniplexConfig),
        pub(super) maximum: u32,
        /// The request body is "hello", released once the reader has read the response.
        pub(super) gated_upload: bool,
        pub(super) slow: bool,
        /// The connection observer callback that cancels the connection.
        pub(super) cancel_from: Option<&'static str>,
    }

    impl Case {
        /// A GET whose origin responds with `reads`.
        pub(super) fn get(name: &'static str, reads: fn(&Gates) -> Vec<ReadAction>) -> Self {
            Self {
                name,
                request: || (get_head(), Vec::new()),
                reads,
                configure: |_| {},
                maximum: u32::MAX,
                gated_upload: false,
                slow: false,
                cancel_from: None,
            }
        }
    }

    /// The origin responds with `response` once the request is committed, then sends EOF.
    pub(super) fn answer(gates: &Gates, response: &[u8]) -> Vec<ReadAction> {
        vec![
            ReadAction::WaitData(Arc::clone(&gates.response), response.to_vec()),
            ReadAction::Eof,
        ]
    }

    /// What one exchange produced: what the reader read, `run`'s error code, the bytes written
    /// and how the connection closed.
    #[derive(Debug, PartialEq)]
    pub(super) struct Outcome {
        pub(super) read: ReadResponse,
        pub(super) run: Option<HttpErrorCode>,
        pub(super) wire: Vec<u8>,
        pub(super) close: CloseKind,
    }

    /// Waits until the connection finished, or closes it once it is idle.
    pub(super) async fn finish(handle: &crate::owned_h1::OwnedH1UpstreamHandle) {
        yield_until(|| handle.is_finished() || handle.is_idle()).await;
        if !handle.is_finished() {
            handle.close_when_idle().unwrap();
        }
    }

    pub(super) fn exchange_outcome(mode: Mode, case: Case) -> Outcome {
        run_on_host(move |context| {
            Box::pin(async move {
                let gates = Gates {
                    response: Arc::new(tokio::sync::Notify::new()),
                    after: Arc::new(tokio::sync::Notify::new()),
                };
                let upload_gate = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = ScriptedTransport::new((case.reads)(&gates));
                let mut options = upstream_options(context, transport);
                (case.configure)(&mut options.io);
                let (session, handle) = OwnedH1UpstreamSession::new(options).unwrap();
                let observer = case.cancel_from.map(|callback| {
                    let observer = Rc::new(CancelFromObserver {
                        handle: RefCell::new(Some(handle.clone())),
                        callback,
                    });
                    handle.set_observer(observer.clone()).unwrap();
                    observer
                });
                let (head, body) = (case.request)();
                let upload = if case.gated_upload {
                    GatedUpload::held(&upload_gate, None)
                } else {
                    RequestSource::held(None, body)
                };
                let reservation = handle.reserve_request().unwrap();
                let run: Pin<
                    Box<dyn Future<Output = Result<crate::uniplex::UniplexRunSummary, HttpError>>>,
                > = if mode == Mode::Run {
                    Box::pin(session.run())
                } else {
                    Box::pin(session.run_pull_through())
                };
                let (run, read) = futures::join!(run, async {
                    let read = if mode == Mode::PullThrough {
                        let mut response = reservation
                            .submit_pull_through(head, upload, CancellationToken::never())
                            .await
                            .unwrap();
                        gates.response.notify_one();
                        read_all(&mut response.body, case.maximum, case.slow).await
                    } else {
                        let response = reservation
                            .submit_exclusive(head, upload, CancellationToken::never())
                            .await
                            .unwrap();
                        gates.response.notify_one();
                        let mut reads = Waiting::new(response.source);
                        read_all(&mut reads, case.maximum, case.slow).await
                    };
                    upload_gate.notify_one();
                    gates.after.notify_one();
                    finish(&handle).await;
                    read
                });
                if let Some(observer) = observer {
                    observer.handle.take();
                }
                let state = state.borrow();
                Outcome {
                    read,
                    run: run.err().map(|error| error.code()),
                    wire: state.writes.concat(),
                    close: state.close,
                }
            })
        })
    }

    /// Runs `case` under `run` and as a pull-through exchange, and compares their outcomes.
    fn assert_pull_through_matches_run(case: Case) -> Outcome {
        let run = exchange_outcome(Mode::Run, case);
        let pull_through = exchange_outcome(Mode::PullThrough, case);
        assert_eq!(pull_through, run, "{}", case.name);
        pull_through
    }

    #[test]
    fn matches_run_content_length() {
        let outcome = assert_pull_through_matches_run(Case::get("content-length", |gates| {
            answer(gates, OK_RESPONSE)
        }));
        assert_eq!(
            (outcome.read.body.as_slice(), outcome.read.eom),
            (b"ok".as_slice(), true)
        );
    }

    #[test]
    fn matches_run_chunked_with_trailers() {
        let outcome = assert_pull_through_matches_run(Case::get("chunked", |gates| {
            answer(
                gates,
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nok\r\n3\r\n!!!\r\n0\r\nx-checksum: abc\r\n\r\n",
            )
        }));
        assert_eq!(outcome.read.body, b"ok!!!");
        assert_eq!(
            outcome.read.trailers,
            [(b"x-checksum".to_vec(), b"abc".to_vec())]
        );
    }

    #[test]
    fn matches_run_close_delimited() {
        let outcome = assert_pull_through_matches_run(Case::get("close-delimited", |gates| {
            answer(gates, b"HTTP/1.1 200 OK\r\n\r\nok")
        }));
        assert_eq!(
            (outcome.read.body.as_slice(), outcome.read.eom),
            (b"ok".as_slice(), true)
        );
    }

    #[test]
    fn matches_run_eof_inside_a_response() {
        let outcome = assert_pull_through_matches_run(Case::get("eof inside", |gates| {
            answer(gates, b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nok")
        }));
        assert!(outcome.read.error.is_some(), "{outcome:?}");
    }

    #[test]
    fn matches_run_informational_heads() {
        let outcome = assert_pull_through_matches_run(Case::get("informational", |gates| {
            answer(
                gates,
                b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 103 Early Hints\r\nLink: </a>\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok",
            )
        }));
        assert_eq!(outcome.read.statuses, [100, 103, 200]);
    }

    #[test]
    fn matches_run_bodiless_responses() {
        let mut head = Case::get("HEAD", |gates| {
            answer(gates, b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n")
        });
        head.request = || {
            let mut head = get_head();
            head.headers_mut().set_method(HttpMethod::Head).unwrap();
            (head, Vec::new())
        };
        for case in [
            head,
            Case::get("204", |gates| {
                answer(gates, b"HTTP/1.1 204 No Content\r\n\r\n")
            }),
            Case::get("304", |gates| {
                answer(
                    gates,
                    b"HTTP/1.1 304 Not Modified\r\nContent-Length: 5\r\n\r\n",
                )
            }),
        ] {
            let outcome = assert_pull_through_matches_run(case);
            assert!(
                outcome.read.body.is_empty() && outcome.read.eom,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn matches_run_connection_close() {
        for case in [
            Case::get("Connection: close", |gates| answer(gates, CLOSE_RESPONSE)),
            Case::get("HTTP/1.0", |gates| {
                answer(gates, b"HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\nok")
            }),
        ] {
            let outcome = assert_pull_through_matches_run(case);
            assert_eq!(outcome.read.body, b"ok", "{}", case.name);
        }
    }

    #[test]
    fn matches_run_bytes_behind_a_response() {
        let outcome = assert_pull_through_matches_run(Case::get("bytes behind", |gates| {
            answer(
                gates,
                b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nokHTTP/1.1 200 OK\r\n",
            )
        }));
        assert_eq!(outcome.run, Some(HttpErrorCode::PROTOCOL_ERROR));
    }

    /// A 256 KiB response in 16 KiB reads, the first released once the request is committed.
    pub(super) fn large_response_reads(gates: &Gates) -> Vec<ReadAction> {
        let body = patterned(256 << 10);
        let mut response =
            format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).into_bytes();
        response.extend_from_slice(&body);
        let mut chunks = response.chunks(16 << 10).map(<[u8]>::to_vec);
        let first = chunks.next().unwrap();
        std::iter::once(ReadAction::WaitData(Arc::clone(&gates.response), first))
            .chain(chunks.map(ReadAction::Data))
            .chain([ReadAction::Eof])
            .collect()
    }

    #[test]
    fn matches_run_large_body_with_a_flow_pause() {
        let mut case = Case::get("flow pause", large_response_reads);
        case.configure = |config| config.ingress_buffer_limit = 16 << 10;
        case.maximum = 4 << 10;
        case.slow = true;
        let outcome = assert_pull_through_matches_run(case);
        assert!(outcome.read.body == patterned(256 << 10) && outcome.read.eom);
    }

    #[test]
    fn matches_run_early_response_with_an_unfinished_upload() {
        let case = Case {
            request: || (post_head(Some(5), false), Vec::new()),
            gated_upload: true,
            // The origin responds immediately, before the request body, and keeps the
            // connection.
            ..Case::get("early response", |gates| {
                vec![ReadAction::WaitData(
                    Arc::clone(&gates.response),
                    OK_RESPONSE.to_vec(),
                )]
            })
        };
        let outcome = assert_pull_through_matches_run(case);
        assert!(outcome.wire.ends_with(b"hello"), "{outcome:?}");
    }

    #[test]
    fn matches_run_large_upload() {
        let case = Case {
            request: large_request,
            ..Case::get("large upload", |gates| answer(gates, OK_RESPONSE))
        };
        let outcome = assert_pull_through_matches_run(case);
        assert!(outcome.wire.ends_with(&patterned(256 << 10)));
    }

    /// As `cancel_from_a_settle_callback_fails_the_connection`'s origin: the response, then an
    /// empty read that completes once the reader is done, in the turn that retires the exchange.
    fn settle_reads<const CLOSE: bool>(gates: &Gates) -> Vec<ReadAction> {
        let response = if CLOSE { CLOSE_RESPONSE } else { OK_RESPONSE };
        vec![
            ReadAction::WaitData(Arc::clone(&gates.response), response.to_vec()),
            ReadAction::WaitDataOrCancellation(Arc::clone(&gates.after), Vec::new()),
        ]
    }

    #[test]
    fn matches_run_cancel_from_a_settle_callback() {
        for (callback, response) in [
            ("capacity_released", OK_RESPONSE),
            ("capacity_available", OK_RESPONSE),
            ("admission_closed", CLOSE_RESPONSE),
        ] {
            let mut case = Case::get(callback, settle_reads::<false>);
            if response == CLOSE_RESPONSE {
                case.reads = settle_reads::<true>;
            }
            case.cancel_from = Some(callback);
            let outcome = assert_pull_through_matches_run(case);
            assert_eq!(
                outcome.run,
                Some(HttpErrorCode::CORO_CANCELLED),
                "{callback}"
            );
        }
    }

    #[test]
    fn matches_run_read_error_after_a_flow_pause() {
        let mut case = Case::get("read error", |gates| {
            let body = patterned(24 << 10);
            let mut first =
                format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).into_bytes();
            first.extend_from_slice(&body[..12 << 10]);
            vec![
                ReadAction::WaitData(Arc::clone(&gates.response), first),
                ReadAction::Data(body[12 << 10..].to_vec()),
                ReadAction::Error(HttpErrorCode::TRANSPORT_READ_ERROR),
            ]
        });
        case.configure = |config| config.ingress_buffer_limit = 16 << 10;
        case.maximum = 4 << 10;
        case.slow = true;
        let outcome = assert_pull_through_matches_run(case);
        assert!(outcome.read.body == patterned(24 << 10) && outcome.read.eom);
        assert_eq!(outcome.run, Some(HttpErrorCode::TRANSPORT_READ_ERROR));
    }

    /// Two exchanges on one keep-alive connection, each plain or pull-through; under `run`
    /// when neither is pull-through and `pull_through_session` is false. Returns what each
    /// reader read, `run`'s error code, the bytes written and how the connection closed.
    fn reuse_outcome(
        pull_through_session: bool,
        kinds: [bool; 2],
    ) -> (Vec<ReadResponse>, Option<HttpErrorCode>, Vec<u8>, CloseKind) {
        run_on_host(move |context| {
            Box::pin(async move {
                let first_gate = Arc::new(tokio::sync::Notify::new());
                let second_gate = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = ScriptedTransport::new([
                    ReadAction::WaitData(Arc::clone(&first_gate), OK_RESPONSE.to_vec()),
                    ReadAction::WaitData(
                        Arc::clone(&second_gate),
                        b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\ntwo".to_vec(),
                    ),
                ]);
                let (session, handle) =
                    OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
                let run: Pin<
                    Box<dyn Future<Output = Result<crate::uniplex::UniplexRunSummary, HttpError>>>,
                > = if pull_through_session {
                    Box::pin(session.run_pull_through())
                } else {
                    Box::pin(session.run())
                };
                let (run, reads) = futures::join!(run, async {
                    let mut reads = Vec::new();
                    for (pull_through, gate) in kinds.into_iter().zip([&first_gate, &second_gate]) {
                        yield_until(|| handle.available_capacity() == 1).await;
                        let reservation = handle.reserve_request().unwrap();
                        let read = if pull_through {
                            let mut response = reservation
                                .submit_pull_through(
                                    get_head(),
                                    HttpSourceHolder::empty(),
                                    CancellationToken::never(),
                                )
                                .await
                                .unwrap();
                            gate.notify_one();
                            read_all(&mut response.body, u32::MAX, false).await
                        } else {
                            let response = reservation
                                .submit_exclusive(
                                    get_head(),
                                    HttpSourceHolder::empty(),
                                    CancellationToken::never(),
                                )
                                .await
                                .unwrap();
                            gate.notify_one();
                            read_all(&mut Waiting::new(response.source), u32::MAX, false).await
                        };
                        reads.push(read);
                    }
                    close_once_idle(&handle).await;
                    reads
                });
                let state = state.borrow();
                (
                    reads,
                    run.err().map(|error| error.code()),
                    state.writes.concat(),
                    state.close,
                )
            })
        })
    }

    #[test]
    fn matches_run_keep_alive_reuse() {
        let run = reuse_outcome(false, [false, false]);
        assert_eq!(run.0[1].body, b"two");
        for kinds in [[true, true], [true, false], [false, true]] {
            assert_eq!(reuse_outcome(true, kinds), run, "{kinds:?}");
        }
    }

    /// Every case of the comparison matrix.
    fn matrix() -> Vec<Case> {
        let mut head = Case::get("HEAD", |gates| {
            answer(gates, b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n")
        });
        head.request = || {
            let mut head = get_head();
            head.headers_mut().set_method(HttpMethod::Head).unwrap();
            (head, Vec::new())
        };
        let mut flow = Case::get("flow pause", large_response_reads);
        flow.configure = |config| config.ingress_buffer_limit = 16 << 10;
        flow.maximum = 4 << 10;
        flow.slow = true;
        vec![
            Case::get("content-length", |gates| answer(gates, OK_RESPONSE)),
            Case::get("chunked", |gates| {
                answer(
                    gates,
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nok\r\n0\r\nx-checksum: abc\r\n\r\n",
                )
            }),
            Case::get("close-delimited", |gates| {
                answer(gates, b"HTTP/1.1 200 OK\r\n\r\nok")
            }),
            Case::get("eof inside", |gates| {
                answer(gates, b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nok")
            }),
            head,
            Case::get("204", |gates| {
                answer(gates, b"HTTP/1.1 204 No Content\r\n\r\n")
            }),
            Case::get("Connection: close", |gates| answer(gates, CLOSE_RESPONSE)),
            Case::get("bytes behind", |gates| {
                answer(
                    gates,
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nokHTTP/1.1 200 OK\r\n",
                )
            }),
            flow,
            Case {
                request: large_request,
                ..Case::get("large upload", |gates| answer(gates, OK_RESPONSE))
            },
        ]
    }

    #[test]
    fn plain_submissions_on_a_pull_through_session_behave_as_run() {
        for case in matrix() {
            assert_eq!(
                exchange_outcome(Mode::PlainOnPullThrough, case),
                exchange_outcome(Mode::Run, case),
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn submit_pull_through_is_refused_on_a_session_run_with_run() {
        run_on_host(|context| {
            Box::pin(async move {
                let (transport, state) = ScriptedTransport::new([]);
                let (session, handle) =
                    OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
                let mut submission = handle.reserve_request().unwrap().submit_pull_through(
                    get_head(),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                // First polled before the session's run is chosen: it waits for commitment.
                assert!(poll_once(Pin::new(&mut submission)).is_pending());
                let (run, refused) = futures::join!(session.run(), async {
                    let refused = (&mut submission).await.err();
                    close_once_idle(&handle).await;
                    refused
                });
                let refused = refused.expect("a pull-through submission is refused under run");
                assert_eq!(refused.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
                assert!(
                    !submission.request_unprocessed(),
                    "the refusal is not a stale connection's: a retry would repeat it"
                );
                assert!(
                    state.borrow().writes.is_empty(),
                    "no byte of the refused request is written"
                );
                run.unwrap();
            })
        });
    }

    /// A waker that counts its wakes and does not wake a task.
    #[derive(Default)]
    pub(super) struct Count(AtomicUsize);

    impl Count {
        pub(super) fn get(&self) -> usize {
            self.0.load(Ordering::SeqCst)
        }
    }

    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// One read an [`Origin`] delivers.
    pub(super) enum Pushed {
        Bytes(Vec<u8>),
        Eof,
        Error(HttpErrorCode),
    }

    /// The origin side of a [`PtTransport`]: the test pushes reads, and each transport read
    /// takes the next one once its future is polled.
    #[derive(Default)]
    pub(super) struct Origin {
        reads: RefCell<VecDeque<Pushed>>,
        waker: RefCell<Option<Waker>>,
        /// `UniplexTransport::read` calls.
        pub(super) starts: Cell<usize>,
        /// Reads that delivered bytes, EOF or a failure.
        pub(super) delivered: Cell<usize>,
        /// Runs in the next `UniplexTransport::read` call, inside whatever polls the driver.
        pub(super) on_read_start: RefCell<Option<Box<dyn FnOnce()>>>,
        /// Runs in the next `UniplexTransport::close_with_reset` call, after the reset.
        pub(super) on_reset: RefCell<Option<Box<dyn FnOnce()>>>,
        pub(super) dropped: Cell<bool>,
    }

    impl Origin {
        pub(super) fn push(&self, read: Pushed) {
            self.reads.borrow_mut().push_back(read);
            let waker = self.waker.borrow_mut().take();
            if let Some(waker) = waker {
                waker.wake();
            }
        }

        pub(super) fn send(&self, bytes: &[u8]) {
            self.push(Pushed::Bytes(bytes.to_vec()));
        }
    }

    /// Writes and closes through a [`ScriptedTransport`]; reads come from an [`Origin`], through
    /// an [`OriginReader`] when `drains` is set.
    pub(super) struct PtTransport {
        inner: Rc<ScriptedTransport>,
        origin: Rc<Origin>,
        drains: bool,
    }

    /// One pushed read's result.
    fn delivered(read: Pushed) -> Result<crate::uniplex::UniplexRead, HttpError> {
        match read {
            Pushed::Bytes(bytes) => Ok(crate::uniplex::UniplexRead::new(queue(&bytes), false)),
            Pushed::Eof => Ok(crate::uniplex::UniplexRead::new(BufQueue::new(0), true)),
            Pushed::Error(code) => Err(HttpError::with_message(code, b"pushed read failure")),
        }
    }

    /// Reads an [`Origin`]'s pushed reads. A read that takes the last pushed bytes reports the
    /// transport drained and keeps its poll's waker for the next push, as the native TCP reader
    /// does.
    struct OriginReader {
        origin: Rc<Origin>,
        cancellation: CancellationToken,
        drained: bool,
    }

    impl UniplexReader for OriginReader {
        fn start_read(
            &mut self,
            _maximum: usize,
            _allocation_size: usize,
        ) -> Result<(), HttpError> {
            self.origin.starts.set(self.origin.starts.get() + 1);
            let hook = self.origin.on_read_start.borrow_mut().take();
            if let Some(hook) = hook {
                hook();
            }
            self.drained = false;
            Ok(())
        }

        fn poll_read(
            &mut self,
            context: &mut Context<'_>,
        ) -> Poll<Result<crate::uniplex::UniplexRead, HttpError>> {
            let read = self.origin.reads.borrow_mut().pop_front();
            let Some(read) = read else {
                if self.cancellation.is_cancellation_requested() {
                    return Poll::Ready(Err(HttpError::with_message(
                        HttpErrorCode::CORO_CANCELLED,
                        b"pushed read cancelled",
                    )));
                }
                *self.origin.waker.borrow_mut() = Some(context.waker().clone());
                return Poll::Pending;
            };
            self.origin.delivered.set(self.origin.delivered.get() + 1);
            self.drained =
                matches!(read, Pushed::Bytes(_)) && self.origin.reads.borrow().is_empty();
            if self.drained {
                *self.origin.waker.borrow_mut() = Some(context.waker().clone());
            }
            Poll::Ready(delivered(read))
        }

        fn is_drained(&self) -> bool {
            self.drained
        }
    }

    impl Drop for PtTransport {
        fn drop(&mut self) {
            self.origin.dropped.set(true);
        }
    }

    impl UniplexTransport for PtTransport {
        fn try_reader(
            &self,
            _context: &WorkerContext,
            _timeout: Duration,
            cancellation: CancellationToken,
        ) -> Result<Option<Box<dyn UniplexReader>>, HttpError> {
            Ok(self.drains.then(|| {
                Box::new(OriginReader {
                    origin: Rc::clone(&self.origin),
                    cancellation,
                    drained: false,
                }) as Box<dyn UniplexReader>
            }))
        }

        fn read(
            &self,
            _context: &WorkerContext,
            _maximum: usize,
            _allocation_size: usize,
            _timeout: Duration,
            cancellation: CancellationToken,
        ) -> LocalTransportFuture<Result<crate::uniplex::UniplexRead, HttpError>> {
            self.origin.starts.set(self.origin.starts.get() + 1);
            let hook = self.origin.on_read_start.borrow_mut().take();
            if let Some(hook) = hook {
                hook();
            }
            let origin = Rc::clone(&self.origin);
            Box::pin(poll_fn(move |context| {
                let read = origin.reads.borrow_mut().pop_front();
                if let Some(read) = read {
                    origin.delivered.set(origin.delivered.get() + 1);
                    return Poll::Ready(delivered(read));
                }
                if cancellation.is_cancellation_requested() {
                    return Poll::Ready(Err(HttpError::with_message(
                        HttpErrorCode::CORO_CANCELLED,
                        b"pushed read cancelled",
                    )));
                }
                *origin.waker.borrow_mut() = Some(context.waker().clone());
                Poll::Pending
            }))
        }

        fn write(
            &self,
            context: &WorkerContext,
            data: BufQueue,
            flags: ByteEventWriteFlags,
            timeout: Duration,
            cancellation: CancellationToken,
        ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
            self.inner
                .write(context, data, flags, timeout, cancellation)
        }

        fn shutdown_write(&self, context: &WorkerContext) -> Result<(), HttpError> {
            self.inner.shutdown_write(context)
        }

        fn close(&self, context: &WorkerContext) -> Result<(), HttpError> {
            self.inner.close(context)
        }

        fn close_with_reset(&self, context: &WorkerContext) -> Result<(), HttpError> {
            let reset = self.inner.close_with_reset(context);
            let hook = self.origin.on_reset.borrow_mut().take();
            if let Some(hook) = hook {
                hook();
            }
            reset
        }

        fn byte_event_capability(
            &self,
            context: &WorkerContext,
        ) -> Result<ByteEventCapability, HttpError> {
            self.inner.byte_event_capability(context)
        }

        fn register_byte_event(
            &self,
            context: &WorkerContext,
            registration: HttpByteEventRegistration,
        ) -> Result<(), ByteEventRegistrationError> {
            self.inner.register_byte_event(context, registration)
        }
    }

    /// A response as a test reads it: through a pull-through body, or a plain exclusive
    /// response's suspend-free reader.
    pub(super) enum Response {
        PullThrough(crate::owned_h1::OwnedH1PullThroughResponse),
        Plain(Waiting, crate::owned_h1::OwnedH1UpstreamRequest),
    }

    impl Response {
        pub(super) fn body(&mut self) -> &mut OwnedH1PullThroughBody {
            match self {
                Self::PullThrough(response) => &mut response.body,
                Self::Plain(..) => panic!("a plain response has no pull-through body"),
            }
        }

        pub(super) fn control(&self) -> &crate::owned_h1::OwnedH1UpstreamRequest {
            match self {
                Self::PullThrough(response) => &response.control,
                Self::Plain(_, control) => control,
            }
        }
    }

    impl Reads for Response {
        fn poll_header(
            &mut self,
            context: &mut Context<'_>,
        ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
            match self {
                Self::PullThrough(response) => response.body.poll_header(context),
                Self::Plain(reads, _) => reads.poll_header(context),
            }
        }

        fn poll_body(
            &mut self,
            context: &mut Context<'_>,
            maximum: u32,
        ) -> Poll<Result<HttpBodyEvent, HttpError>> {
            match self {
                Self::PullThrough(response) => response.body.poll_body(context, maximum),
                Self::Plain(reads, _) => reads.poll_body(context, maximum),
            }
        }
    }

    pub(super) const CHUNKED_HEAD: &[u8] = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";

    /// `bytes` as one chunk of a chunked body.
    pub(super) fn chunk(bytes: &[u8]) -> Vec<u8> {
        let mut wire = format!("{:x}\r\n", bytes.len()).into_bytes();
        wire.extend_from_slice(bytes);
        wire.extend_from_slice(b"\r\n");
        wire
    }

    pub(super) fn body_bytes(event: &HttpBodyEvent) -> Vec<u8> {
        event.as_body().map_or_else(Vec::new, |bytes| {
            bytes.coalesce(bytes.chain_length()).unwrap().to_vec()
        })
    }

    pub(super) type RunResult = Result<crate::uniplex::UniplexRunSummary, HttpError>;

    /// One owned upstream connection over an [`Origin`], under `run` (`Mode::Run`) or
    /// `run_pull_through`, whose run future and response reads the test polls by hand: the run
    /// with `run_waker`, as its root would, the reads with `holder_waker`, as a downstream
    /// exchange would. Both wakers count their wakes and do not wake a task.
    pub(super) struct Fixture {
        pub(super) mode: Mode,
        pub(super) origin: Rc<Origin>,
        pub(super) state: Rc<RefCell<TransportState>>,
        pub(super) handle: crate::owned_h1::OwnedH1UpstreamHandle,
        run: Pin<Box<dyn Future<Output = RunResult>>>,
        pub(super) run_wake: Arc<Count>,
        run_waker: Waker,
        /// `run_wake`'s count when the run was last polled.
        run_seen: usize,
        /// How many times the test polled the run.
        pub(super) run_polls: usize,
        pub(super) holder_wake: Arc<Count>,
        pub(super) holder_waker: Waker,
    }

    impl Fixture {
        pub(super) fn new(
            context: WorkerContext,
            mode: Mode,
            configure: impl FnOnce(&mut UniplexConfig),
        ) -> Self {
            Self::build(context, mode, configure, false)
        }

        /// As `new`, over a transport whose reader reports it drained once a read took the last
        /// pushed bytes (an [`OriginReader`]).
        pub(super) fn draining(context: WorkerContext, mode: Mode) -> Self {
            Self::build(context, mode, |_| {}, true)
        }

        fn build(
            context: WorkerContext,
            mode: Mode,
            configure: impl FnOnce(&mut UniplexConfig),
            drains: bool,
        ) -> Self {
            let (inner, state) = ScriptedTransport::new([]);
            let origin = Rc::new(Origin::default());
            let transport = Rc::new(PtTransport {
                inner,
                origin: Rc::clone(&origin),
                drains,
            });
            let mut options = upstream_options(context, transport);
            configure(&mut options.io);
            let (session, handle) = OwnedH1UpstreamSession::new(options).unwrap();
            let run: Pin<Box<dyn Future<Output = RunResult>>> = if mode == Mode::Run {
                Box::pin(session.run())
            } else {
                Box::pin(session.run_pull_through())
            };
            let run_wake = Arc::new(Count::default());
            let holder_wake = Arc::new(Count::default());
            Self {
                mode,
                origin,
                state,
                handle,
                run,
                run_waker: Waker::from(Arc::clone(&run_wake)),
                run_wake,
                run_seen: 0,
                run_polls: 0,
                holder_waker: Waker::from(Arc::clone(&holder_wake)),
                holder_wake,
            }
        }

        pub(super) fn poll_run(&mut self) -> Poll<RunResult> {
            self.run_seen = self.run_wake.get();
            self.run_polls += 1;
            self.run
                .as_mut()
                .poll(&mut Context::from_waker(&self.run_waker))
        }

        /// Whether the run's waker was woken since its last poll.
        pub(super) fn run_woken(&self) -> bool {
            self.run_wake.get() != self.run_seen
        }

        /// Drops the run future, as a forced teardown of its scope would.
        pub(super) fn drop_run(&mut self) {
            self.run = Box::pin(std::future::pending());
        }

        /// Polls the run each time its waker was woken since its last poll, as its root
        /// would. Returns its result if it completes.
        pub(super) fn settle_run(&mut self) -> Option<RunResult> {
            for _ in 0..100 {
                if self.run_wake.get() == self.run_seen {
                    return None;
                }
                if let Poll::Ready(result) = self.poll_run() {
                    return Some(result);
                }
            }
            panic!("the run woke itself 100 times");
        }

        /// Submits a request, a pull-through one unless under `run`, then polls the run, which
        /// commits it and writes what it can.
        pub(super) fn submit(
            &mut self,
            head: HttpHeaderEvent,
            upload: HttpSourceHolder,
            token: CancellationToken,
        ) -> Response {
            let reservation = self.handle.reserve_request().unwrap();
            let mut noop = Context::from_waker(Waker::noop());
            if self.mode == Mode::Run {
                let mut submission = reservation.submit_exclusive(head, upload, token);
                assert!(self.poll_run().is_pending());
                assert!(self.settle_run().is_none());
                let Poll::Ready(response) = Pin::new(&mut submission).poll(&mut noop) else {
                    panic!("the request is committed");
                };
                let response = response.unwrap();
                Response::Plain(Waiting::new(response.source), response.control)
            } else {
                let mut submission = reservation.submit_pull_through(head, upload, token);
                assert!(self.poll_run().is_pending());
                assert!(self.settle_run().is_none());
                let Poll::Ready(response) = Pin::new(&mut submission).poll(&mut noop) else {
                    panic!("the request is committed");
                };
                Response::PullThrough(response.unwrap())
            }
        }

        pub(super) fn header(
            &self,
            response: &mut impl Reads,
        ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
            response.poll_header(&mut Context::from_waker(&self.holder_waker))
        }

        pub(super) fn body(
            &self,
            response: &mut impl Reads,
            maximum: u32,
        ) -> Poll<Result<HttpBodyEvent, HttpError>> {
            response.poll_body(&mut Context::from_waker(&self.holder_waker), maximum)
        }

        /// Reads the next header. A read that waits lets the run progress, then reads again.
        pub(super) fn next_header(&mut self, response: &mut Response) -> HttpHeaderEvent {
            for _ in 0..4 {
                if let Poll::Ready(head) = self.header(response) {
                    return head.unwrap();
                }
                self.settle_run();
            }
            panic!("no head arrived");
        }

        /// Reads the next body event, as `next_header` reads a head.
        pub(super) fn next_event(
            &mut self,
            response: &mut Response,
            maximum: u32,
        ) -> Result<HttpBodyEvent, HttpError> {
            for _ in 0..4 {
                if let Poll::Ready(event) = self.body(response, maximum) {
                    return event;
                }
                self.settle_run();
            }
            panic!("no body event arrived");
        }

        /// Submits a pull-through GET and reads its chunked response head through the body,
        /// which takes the drive.
        pub(super) fn held(&mut self) -> Response {
            assert_eq!(self.mode, Mode::PullThrough);
            let mut response = self.submit(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            self.origin.send(CHUNKED_HEAD);
            let Poll::Ready(head) = self.header(&mut response) else {
                panic!("the head is taken in the read that takes the drive");
            };
            assert_eq!(head.unwrap().headers_mut().status_code().unwrap(), 200);
            assert!(
                response.body().holds_the_drive(),
                "the body holds the drive"
            );
            response
        }

        /// Ends the chunked response, reads it to its end, then closes the idle connection.
        pub(super) fn finish(&mut self, response: &mut Response) -> RunResult {
            self.origin.send(b"0\r\n\r\n");
            while !self.next_event(response, u32::MAX).unwrap().eom() {}
            assert!(self.settle_run().is_none());
            assert!(self.handle.is_idle(), "the exchange retired");
            assert!(self.handle.close_when_idle().unwrap());
            self.settle_run().expect("the idle connection closes")
        }
    }

    #[test]
    fn each_event_is_taken_in_the_read_that_parses_it() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let mut response = fixture.held();
                for piece in [b"one".as_slice(), b"two", b"three"] {
                    let holder = fixture.holder_wake.get();
                    let run = fixture.run_wake.get();
                    fixture.origin.send(&chunk(piece));
                    assert_eq!(
                        (
                            fixture.holder_wake.get() - holder,
                            fixture.run_wake.get() - run
                        ),
                        (1, 0),
                        "the read wakes the holder, not the run"
                    );
                    let Poll::Ready(event) = fixture.body(&mut response, u32::MAX) else {
                        panic!("the chunk is taken in the read that parses it");
                    };
                    assert_eq!(body_bytes(&event.unwrap()), piece);
                    assert!(
                        fixture.body(&mut response, u32::MAX).is_pending(),
                        "the re-read finds nothing"
                    );
                    assert_eq!(
                        (
                            fixture.holder_wake.get() - holder,
                            fixture.run_wake.get() - run
                        ),
                        (1, 0),
                        "parsing and taking the event wake nothing: no waiter is registered"
                    );
                }
                fixture.finish(&mut response).unwrap();
            })
        });
    }

    #[test]
    fn run_future_does_not_poll_a_held_driver() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let mut response = fixture.held();
                let turns = driver_turns();
                assert!(fixture.poll_run().is_pending());
                fixture.origin.send(&chunk(b"one"));
                assert!(fixture.poll_run().is_pending());
                assert_eq!(driver_turns(), turns, "the run future polls no held driver");
                let Poll::Ready(event) = fixture.body(&mut response, u32::MAX) else {
                    panic!("the holder's read drives");
                };
                assert_eq!(body_bytes(&event.unwrap()), b"one");
                fixture.finish(&mut response).unwrap();
            })
        });
    }

    #[test]
    fn the_run_is_polled_only_after_a_wake() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let mut response = fixture.held();
                let turns = driver_turns();
                assert!(fixture.body(&mut response, u32::MAX).is_pending());
                assert!(fixture.body(&mut response, u32::MAX).is_pending());
                assert_eq!(driver_turns(), turns, "a quiescent wait needs no poll");
                fixture.origin.send(&chunk(b"one"));
                assert!(
                    fixture.body(&mut response, u32::MAX).is_ready(),
                    "a wake delivered to the holder makes its next read poll"
                );
                assert!(driver_turns() > turns);
                let turns = driver_turns();
                let other = Arc::new(Count::default());
                let other_waker = Waker::from(Arc::clone(&other));
                assert!(
                    response
                        .poll_body(&mut Context::from_waker(&other_waker), u32::MAX)
                        .is_pending()
                );
                assert!(driver_turns() > turns, "another waker always polls");
                fixture.origin.send(&chunk(b"two"));
                assert_eq!(other.get(), 1, "the driver's wakes reach the new waker");
                let Poll::Ready(event) =
                    response.poll_body(&mut Context::from_waker(&other_waker), u32::MAX)
                else {
                    panic!("the new waker's read drives");
                };
                assert_eq!(body_bytes(&event.unwrap()), b"two");
                fixture.finish(&mut response).unwrap();
            })
        });
    }

    #[test]
    fn acquisition_retires_a_pending_waiting_read() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let upload = Arc::new(tokio::sync::Notify::new());
                let mut response = fixture.submit(
                    post_head(Some(5), false),
                    GatedUpload::held(&upload, None),
                    CancellationToken::never(),
                );
                // The request body is not written: the head read waits, registered with the
                // source.
                assert!(fixture.header(&mut response).is_pending());
                assert!(!response.body().holds_the_drive());
                upload.notify_one();
                assert!(fixture.settle_run().is_none());
                assert!(
                    fixture.state.borrow().writes.concat().ends_with(b"hello"),
                    "the run wrote the request body"
                );
                fixture.origin.send(CHUNKED_HEAD);
                let Poll::Ready(head) = fixture.header(&mut response) else {
                    panic!("the acquiring read drives");
                };
                let mut head =
                    head.expect("the try read succeeds: the pending waiting read was retired");
                assert_eq!(head.headers_mut().status_code().unwrap(), 200);
                assert!(response.body().holds_the_drive());
                let holder = fixture.holder_wake.get();
                fixture.origin.send(&chunk(b"one"));
                let Poll::Ready(event) = fixture.body(&mut response, u32::MAX) else {
                    panic!("the chunk is taken");
                };
                assert_eq!(body_bytes(&event.unwrap()), b"one");
                assert_eq!(
                    fixture.holder_wake.get() - holder,
                    1,
                    "the chunk wakes the holder once, through the driver: the retired wait is gone"
                );
                fixture.finish(&mut response).unwrap();
            })
        });
    }

    #[test]
    fn a_read_nested_in_the_run_s_drive_waits_without_taking_the_drive() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let response = Rc::new(RefCell::new(fixture.submit(
                    get_head(),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                )));
                let (nested, holder_waker) = (Rc::clone(&response), fixture.holder_waker.clone());
                let nested_read = Rc::new(Cell::new(None));
                let read = Rc::clone(&nested_read);
                // A read start inside the run's drive reads the body, after the request is
                // written and before its head is complete.
                *fixture.origin.on_read_start.borrow_mut() = Some(Box::new(move || {
                    let mut response = nested.borrow_mut();
                    let poll = response.poll_header(&mut Context::from_waker(&holder_waker));
                    read.set(Some((poll.is_pending(), response.body().holds_the_drive())));
                }));
                fixture.origin.send(b"HTTP/1.1 200 OK\r\n");
                assert!(fixture.settle_run().is_none());
                let Some((pending, held)) = nested_read.get() else {
                    panic!("the run's drive started a read after the partial head");
                };
                assert!(pending, "the head is incomplete");
                let holder = fixture.holder_wake.get();
                fixture.origin.send(b"Transfer-Encoding: chunked\r\n\r\n");
                assert!(fixture.settle_run().is_none());
                assert_eq!(
                    fixture.holder_wake.get() - holder,
                    1,
                    "the run produces the head, which wakes the nested read's waker"
                );
                assert!(
                    !held,
                    "the nested read waited without taking the drive the run was polling"
                );
                let mut response = response.borrow_mut();
                let Poll::Ready(head) = fixture.header(&mut *response) else {
                    panic!("the woken read takes the head");
                };
                assert_eq!(head.unwrap().headers_mut().status_code().unwrap(), 200);
                assert!(
                    response.body().holds_the_drive(),
                    "a read outside the run's drive takes the drive"
                );
                fixture.finish(&mut response).unwrap();
            })
        });
    }

    #[test]
    fn an_unfinished_upload_keeps_the_run_driving_until_the_request_is_written() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let upload = Arc::new(tokio::sync::Notify::new());
                let mut response = fixture.submit(
                    post_head(Some(5), false),
                    GatedUpload::held(&upload, None),
                    CancellationToken::never(),
                );
                // The origin responds before the request body is sent.
                fixture
                    .origin
                    .send(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
                assert!(
                    fixture.header(&mut response).is_pending(),
                    "the read waits, and the run reads the response"
                );
                assert!(fixture.settle_run().is_none());
                let Poll::Ready(head) = fixture.header(&mut response) else {
                    panic!("the run's parse woke the waiting read");
                };
                assert_eq!(head.unwrap().headers_mut().status_code().unwrap(), 200);
                assert!(
                    !response.body().holds_the_drive(),
                    "until the request is written, the run keeps the drive"
                );
                // The holder does not read again until the request is sent. The run writes it.
                upload.notify_one();
                assert!(fixture.settle_run().is_none());
                assert!(fixture.state.borrow().writes.concat().ends_with(b"hello"));
                let mut body = Vec::new();
                loop {
                    let event = fixture.next_event(&mut response, u32::MAX).unwrap();
                    body.extend(body_bytes(&event));
                    if event.eom() {
                        break;
                    }
                }
                assert_eq!(body, b"ok");
                assert!(fixture.settle_run().is_none());
                assert_eq!(fixture.handle.available_capacity(), 1, "the slot returns");
                drop(response);
                assert!(fixture.handle.close_when_idle().unwrap());
                fixture
                    .settle_run()
                    .expect("the idle connection closes")
                    .unwrap();
            })
        });
    }

    #[test]
    fn a_fatal_abort_while_held_and_unread_completes_the_run() {
        run_on_host(|context| {
            Box::pin(async move {
                take_contained_panics();
                let mut fixture = Fixture::new(context, Mode::PullThrough, |config| {
                    config.deliver_ingress_before_abort = true;
                });
                let mut response = fixture.held();
                fixture.origin.send(&chunk(b"abcdef"));
                let Poll::Ready(event) = fixture.body(&mut response, 2) else {
                    panic!("the chunk is taken in part");
                };
                assert_eq!(body_bytes(&event.unwrap()), b"ab");
                *fixture.origin.on_read_start.borrow_mut() =
                    Some(Box::new(|| panic!("read start panicked")));
                fixture.origin.send(&chunk(b"ghi"));
                // The read that drives reads the chunk, then the next read start panics: the
                // connection fails and keeps the queued events.
                let read = catch_unwind(AssertUnwindSafe(|| fixture.body(&mut response, 2)));
                let Ok(Poll::Ready(event)) = read else {
                    panic!("the holder's read neither panics nor waits");
                };
                assert_eq!(body_bytes(&event.unwrap()), b"cd", "a kept event");
                // The holder stalls. The run finishes its teardown on its own wakes and fails.
                let run = fixture
                    .settle_run()
                    .expect("the run completes on its wakes");
                assert_eq!(
                    run.err().map(|error| error.code()),
                    Some(HttpErrorCode::INTERNAL_ERROR)
                );
                assert_eq!(
                    take_contained_panics(),
                    [(
                        PanicSite::DriverTurn,
                        None,
                        Some("read start panicked".to_owned())
                    )]
                );
                assert!(fixture.handle.is_finished());
                assert_eq!(fixture.state.borrow().close, CloseKind::Reset);
                let event = fixture.next_event(&mut response, 2).unwrap();
                assert_eq!(body_bytes(&event), b"ef");
                let error = fixture.next_event(&mut response, 2).err();
                assert_eq!(
                    error.map(|error| error.code()),
                    Some(HttpErrorCode::INTERNAL_ERROR)
                );
            })
        });
    }

    #[test]
    fn an_inline_completion_leaves_the_driver_to_the_run() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let mut response = fixture.held();
                fixture.origin.push(Pushed::Eof);
                // The read that drives parses an EOF inside the response: the connection fails,
                // and its run completes in that drive.
                let Poll::Ready(read) = fixture.body(&mut response, u32::MAX) else {
                    panic!("the EOF is applied in the read");
                };
                assert_eq!(
                    read.err().map(|error| error.code()),
                    Some(HttpErrorCode::TRANSPORT_EOF)
                );
                assert!(
                    !fixture.handle.is_finished(),
                    "only the run future marks the run finished"
                );
                assert!(
                    !fixture.origin.dropped.get(),
                    "the driver, which owns the transport, is left to the run future"
                );
                let run = fixture
                    .settle_run()
                    .expect("the run future takes the completion on its wake");
                assert_eq!(run.unwrap_err().code(), HttpErrorCode::TRANSPORT_EOF);
                assert!(fixture.handle.is_finished());
                assert!(
                    fixture.origin.dropped.get(),
                    "the run future dropped the driver"
                );
            })
        });
    }

    /// What a reader observed of a response whose origin sends a chunk and EOF while it does not
    /// read: its events after that point, `run`'s error code, and whether admission closed
    /// before it read again.
    fn eof_while_unread(mode: Mode, deliver: bool) -> (Vec<String>, Option<HttpErrorCode>, bool) {
        run_on_host(move |context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, mode, |config| {
                    config.deliver_ingress_before_abort = deliver;
                });
                let log = Rc::new(AdmissionLog::default());
                fixture.handle.set_observer(log.clone()).unwrap();
                let mut response = fixture.submit(
                    get_head(),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                fixture.origin.send(CHUNKED_HEAD);
                fixture.next_header(&mut response);
                fixture.origin.send(&chunk(b"one"));
                let event = fixture.next_event(&mut response, u32::MAX).unwrap();
                assert_eq!(body_bytes(&event), b"one");
                // The reader stops reading; the origin sends a chunk, then EOF.
                fixture.origin.send(&chunk(b"two"));
                fixture.origin.push(Pushed::Eof);
                let mut run = fixture.settle_run();
                let closed_before_read =
                    log.0.borrow().iter().any(|note| note == "admission_closed");
                let mut events = Vec::new();
                loop {
                    match fixture.next_event(&mut response, u32::MAX) {
                        Ok(event) => {
                            events.push(String::from_utf8(body_bytes(&event)).unwrap());
                            if event.eom() {
                                break;
                            }
                        }
                        Err(error) => {
                            events.push(format!("error {:?}", error.code()));
                            break;
                        }
                    }
                }
                run = run.or_else(|| fixture.settle_run());
                let run = run.expect("the run completes");
                (
                    events,
                    run.err().map(|error| error.code()),
                    closed_before_read,
                )
            })
        })
    }

    #[test]
    fn eof_while_held_and_unread_is_applied_at_the_next_read() {
        for deliver in [false, true] {
            let (run_events, run_error, run_closed) = eof_while_unread(Mode::Run, deliver);
            let (events, error, closed) = eof_while_unread(Mode::PullThrough, deliver);
            assert!(run_closed, "under run, the EOF is applied at once");
            assert!(
                !closed,
                "while held and unread, the EOF waits for the holder's next read"
            );
            assert_eq!(
                (events, error),
                (run_events, run_error),
                "the reader then gets run's events and error (deliver_ingress_before_abort: {deliver})"
            );
        }
    }

    /// How many transport reads completed while a reader that read the head and a first chunk
    /// stopped reading and its origin sent three more chunks.
    fn reads_while_unread(mode: Mode) -> usize {
        run_on_host(move |context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, mode, |_| {});
                let mut response = fixture.submit(
                    get_head(),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                fixture.origin.send(CHUNKED_HEAD);
                fixture.next_header(&mut response);
                fixture.origin.send(&chunk(b"one"));
                fixture.next_event(&mut response, u32::MAX).unwrap();
                let delivered = fixture.origin.delivered.get();
                for piece in [b"two".as_slice(), b"three", b"four"] {
                    fixture.origin.send(&chunk(piece));
                    assert!(fixture.settle_run().is_none());
                }
                let unread = fixture.origin.delivered.get() - delivered;
                let mut body = Vec::new();
                loop {
                    let event = fixture.next_event(&mut response, u32::MAX).unwrap();
                    body.extend(body_bytes(&event));
                    if body.len() == b"twothreefour".len() {
                        break;
                    }
                }
                assert_eq!(body, b"twothreefour");
                fixture.origin.send(b"0\r\n\r\n");
                while !fixture.next_event(&mut response, u32::MAX).unwrap().eom() {}
                assert!(fixture.settle_run().is_none());
                assert!(fixture.handle.close_when_idle().unwrap());
                fixture
                    .settle_run()
                    .expect("the idle connection closes")
                    .unwrap();
                unread
            })
        })
    }

    #[test]
    fn an_unread_body_stops_the_upstream_reading() {
        assert_eq!(
            reads_while_unread(Mode::Run),
            3,
            "under run, the connection reads whether or not its reader does"
        );
        assert!(
            reads_while_unread(Mode::PullThrough) <= 1,
            "while held and unread, at most one transport read completes"
        );
    }

    #[test]
    fn the_eom_read_wakes_only_the_run() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let mut response = fixture.held();
                fixture.origin.send(&chunk(b"one"));
                let Poll::Ready(event) = fixture.body(&mut response, u32::MAX) else {
                    panic!("the chunk is taken");
                };
                assert_eq!(body_bytes(&event.unwrap()), b"one");
                fixture.origin.send(b"0\r\n\r\n");
                let holder = fixture.holder_wake.get();
                let run = fixture.run_wake.get();
                let Poll::Ready(event) = fixture.body(&mut response, u32::MAX) else {
                    panic!("the EOM is taken in the read that parses it");
                };
                assert!(event.unwrap().eom());
                assert_eq!(
                    fixture.holder_wake.get(),
                    holder,
                    "neither the release in the drive that parses EOM nor the source's completion \
                     wakes the holder"
                );
                assert!(fixture.run_wake.get() > run, "they wake the run");
                assert!(!response.body().holds_the_drive());
                let polls = fixture.run_polls;
                assert!(fixture.settle_run().is_none());
                assert_eq!(
                    fixture.run_polls - polls,
                    1,
                    "one run poll retires the exchange"
                );
                assert_eq!(
                    fixture.handle.available_capacity(),
                    1,
                    "and returns the slot"
                );
                assert!(fixture.handle.close_when_idle().unwrap());
                fixture
                    .settle_run()
                    .expect("the idle connection closes")
                    .unwrap();
            })
        });
    }

    /// The source's completion at the EOM read admits the next request immediately, before the
    /// run retires the exchange. One poll of the run then retires the exchange, commits the
    /// pull-through request submitted meanwhile and writes it.
    #[test]
    fn the_eom_read_admits_the_next_request_before_the_run_retires_the_exchange() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let mut response = fixture.held();
                fixture.origin.send(b"0\r\n\r\n");
                let Poll::Ready(event) = fixture.body(&mut response, u32::MAX) else {
                    panic!("the EOM is taken in the read that parses it");
                };
                assert!(event.unwrap().eom());
                assert_eq!(
                    (
                        fixture.handle.available_capacity(),
                        fixture.handle.returning_capacity()
                    ),
                    (1, 1),
                    "the completed source's slot admits before the run polls"
                );
                drop(response);
                let writes = fixture.state.borrow().writes.len();
                let mut submission = fixture
                    .handle
                    .reserve_request()
                    .unwrap()
                    .submit_pull_through(
                        get_head(),
                        HttpSourceHolder::empty(),
                        CancellationToken::never(),
                    );
                assert!(fixture.poll_run().is_pending());
                assert_eq!(
                    fixture.state.borrow().writes.len(),
                    writes + 1,
                    "one run poll retires the exchange, then commits and writes the next request"
                );
                let Poll::Ready(next) =
                    Pin::new(&mut submission).poll(&mut Context::from_waker(Waker::noop()))
                else {
                    panic!("the next request is committed");
                };
                let mut next = Response::PullThrough(next.unwrap());
                fixture.origin.send(CHUNKED_HEAD);
                let mut head = fixture.next_header(&mut next);
                assert_eq!(head.headers_mut().status_code().unwrap(), 200);
                fixture.finish(&mut next).unwrap();
            })
        });
    }

    #[test]
    fn a_source_completion_while_held_wakes_only_the_run() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let mut response = fixture.held();
                fixture.origin.send(&chunk(b"one"));
                let Poll::Ready(event) = fixture.body(&mut response, u32::MAX) else {
                    panic!("the chunk is taken");
                };
                assert_eq!(body_bytes(&event.unwrap()), b"one");
                let holder = fixture.holder_wake.get();
                let run = fixture.run_wake.get();
                response.body().stop(None);
                assert_eq!(
                    fixture.holder_wake.get(),
                    holder,
                    "the drive returns before the source's completion wakes the driver"
                );
                assert!(fixture.run_wake.get() > run);
                let closed = fixture
                    .settle_run()
                    .expect("the run closes the connection whose response was not read to its end");
                assert!(closed.is_err());
                assert_eq!(fixture.state.borrow().close, CloseKind::Reset);
            })
        });
    }

    /// The poll that parses a response releases the exchange's capacity, and that poll's turns
    /// take the release's mark, so the release does not wake the run. Reading the response to
    /// its end wakes the run once, and one poll then retires the exchange.
    #[test]
    fn a_release_in_the_run_s_poll_does_not_wake_the_run() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::Run, |_| {});
                let mut response = fixture.submit(
                    get_head(),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                fixture.origin.send(OK_RESPONSE);
                assert!(fixture.poll_run().is_pending());
                assert!(
                    response.control().capacity_released(),
                    "the poll that parsed the response released its capacity"
                );
                assert!(!fixture.run_woken(), "the release did not wake the run");
                let mut head = fixture.next_header(&mut response);
                assert_eq!(head.headers_mut().status_code().unwrap(), 200);
                let run = fixture.run_wake.get();
                while !fixture.next_event(&mut response, u32::MAX).unwrap().eom() {}
                assert_eq!(
                    fixture.run_wake.get() - run,
                    1,
                    "the source's completion wakes the run once"
                );
                let polls = fixture.run_polls;
                assert!(fixture.settle_run().is_none());
                assert_eq!(
                    (
                        fixture.run_polls - polls,
                        fixture.handle.available_capacity()
                    ),
                    (1, 1),
                    "one poll retires the exchange and returns the slot"
                );
                assert!(fixture.handle.close_when_idle().unwrap());
                fixture
                    .settle_run()
                    .expect("the idle connection closes")
                    .unwrap();
            })
        });
    }

    /// Between the driver's polls, a capacity release or an admission close wakes the last
    /// registered waker once: the run's, or the holder's while a body holds the drive.
    #[test]
    fn a_release_or_close_between_polls_wakes_the_registered_waker_once() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::Run, |_| {});
                assert!(fixture.poll_run().is_pending());
                let run = fixture.run_wake.get();
                drop(fixture.handle.reserve_request().unwrap());
                assert_eq!(
                    fixture.run_wake.get() - run,
                    1,
                    "the release wakes the run once"
                );
                assert!(fixture.settle_run().is_none());
                let run = fixture.run_wake.get();
                assert!(fixture.handle.close_when_idle().unwrap());
                assert_eq!(
                    fixture.run_wake.get() - run,
                    1,
                    "the close wakes the run once"
                );
                fixture
                    .settle_run()
                    .expect("the idle connection closes")
                    .unwrap();
            })
        });
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let mut response = fixture.held();
                let (holder, run) = (fixture.holder_wake.get(), fixture.run_wake.get());
                assert!(fixture.handle.close_when_idle().unwrap());
                assert_eq!(
                    (
                        fixture.holder_wake.get() - holder,
                        fixture.run_wake.get() - run
                    ),
                    (1, 0),
                    "while the body holds the drive, the close wakes the holder once"
                );
                fixture.origin.send(b"0\r\n\r\n");
                while !fixture.next_event(&mut response, u32::MAX).unwrap().eom() {}
                fixture
                    .settle_run()
                    .expect("the connection closes once its exchange retires")
                    .unwrap();
            })
        });
    }

    /// An admission close from a read start in the holder's drive does not wake either waker,
    /// because that drive's turns take its mark. It leaves the wake record set, so the holder's
    /// next read drives the connection, and the read after that does not.
    #[test]
    fn an_owner_thread_wake_in_the_holder_s_drive_only_sets_the_wake_record() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let mut response = fixture.held();
                let handle = fixture.handle.clone();
                *fixture.origin.on_read_start.borrow_mut() = Some(Box::new(move || {
                    assert!(handle.close_when_idle().unwrap());
                }));
                let (holder, run) = (fixture.holder_wake.get(), fixture.run_wake.get());
                fixture.origin.send(&chunk(b"one"));
                let Poll::Ready(event) = fixture.body(&mut response, u32::MAX) else {
                    panic!("the chunk is taken in the read that parses it");
                };
                assert_eq!(body_bytes(&event.unwrap()), b"one");
                assert!(
                    fixture.origin.on_read_start.borrow().is_none(),
                    "the drive started the next read, which closed admission"
                );
                assert_eq!(
                    (
                        fixture.holder_wake.get() - holder,
                        fixture.run_wake.get() - run
                    ),
                    (1, 0),
                    "only the chunk's arrival woke the holder"
                );
                let turns = driver_turns();
                assert!(fixture.body(&mut response, u32::MAX).is_pending());
                assert!(driver_turns() > turns, "the holder's next read drives");
                let turns = driver_turns();
                assert!(fixture.body(&mut response, u32::MAX).is_pending());
                assert_eq!(driver_turns(), turns, "the read after it does not");
                fixture.origin.send(b"0\r\n\r\n");
                while !fixture.next_event(&mut response, u32::MAX).unwrap().eom() {}
                fixture
                    .settle_run()
                    .expect("the connection closes once its exchange retires")
                    .unwrap();
            })
        });
    }

    /// The holder's drive that parses the response's end returns the drive to the run. That
    /// drive's turns take the release's mark, and its end still wakes the run once, since the run
    /// polls the driver from then on. The holder is not woken.
    #[test]
    fn a_drive_returned_in_the_holder_s_drive_wakes_the_run_once_at_its_end() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let mut response = fixture.held();
                fixture
                    .origin
                    .send(&[chunk(b"one"), b"0\r\n\r\n".to_vec()].concat());
                let (holder, run) = (fixture.holder_wake.get(), fixture.run_wake.get());
                // The read takes one byte, so the source does not complete in it.
                let Poll::Ready(event) = fixture.body(&mut response, 1) else {
                    panic!("a byte is taken in the read that parses the response");
                };
                assert_eq!(body_bytes(&event.unwrap()), b"o");
                assert!(!response.body().holds_the_drive());
                assert_eq!(
                    (
                        fixture.holder_wake.get() - holder,
                        fixture.run_wake.get() - run
                    ),
                    (0, 1),
                    "the drive's end wakes only the run, once"
                );
                let mut rest = Vec::new();
                loop {
                    let event = fixture.next_event(&mut response, u32::MAX).unwrap();
                    rest.extend(body_bytes(&event));
                    if event.eom() {
                        break;
                    }
                }
                assert_eq!(rest, b"ne");
                assert!(fixture.settle_run().is_none());
                assert_eq!(
                    fixture.handle.available_capacity(),
                    1,
                    "the exchange retired"
                );
                assert!(fixture.handle.close_when_idle().unwrap());
                fixture
                    .settle_run()
                    .expect("the idle connection closes")
                    .unwrap();
            })
        });
    }

    /// Inside nested marks of the driver's poll, a release only marks the driver's signal. The
    /// inner exit does not wake, and the outermost exit finds the mark and wakes the run once.
    #[test]
    fn nested_polling_marks_wake_only_at_the_outermost_exit() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::Run, |_| {});
                assert!(fixture.poll_run().is_pending());
                let (handle, run_wake) = (fixture.handle.clone(), Arc::clone(&fixture.run_wake));
                let run = run_wake.get();
                let inner = handle.as_if_polled(|| {
                    handle.as_if_polled(|| drop(handle.reserve_request().unwrap()));
                    run_wake.get() - run
                });
                assert_eq!(
                    (inner, run_wake.get() - run),
                    (0, 1),
                    "the inner exit does not wake the run, and the outer exit wakes it once"
                );
                assert!(fixture.settle_run().is_none());
                assert!(fixture.handle.close_when_idle().unwrap());
                fixture
                    .settle_run()
                    .expect("the idle connection closes")
                    .unwrap();
            })
        });
    }

    /// The outermost exit of the driver's poll contains a panic of the waker that it wakes and
    /// reports it at the transport site. The connection then serves a request under a waker that
    /// does not panic.
    #[cfg(panic = "unwind")]
    #[test]
    fn a_waker_that_panics_at_the_poll_s_exit_is_reported() {
        struct PanickingWake;

        impl Wake for PanickingWake {
            fn wake(self: Arc<Self>) {
                panic!("waker panicked");
            }
        }

        run_on_host(|context| {
            Box::pin(async move {
                take_contained_panics();
                let mut fixture = Fixture::new(context, Mode::Run, |_| {});
                let panicking = Waker::from(Arc::new(PanickingWake));
                let mut panicking = Context::from_waker(&panicking);
                assert!(fixture.run.as_mut().poll(&mut panicking).is_pending());
                let handle = fixture.handle.clone();
                let exit = catch_unwind(AssertUnwindSafe(|| {
                    handle.as_if_polled(|| drop(handle.reserve_request().unwrap()))
                }));
                assert!(exit.is_ok(), "the waker's panic does not leave the exit");
                assert_eq!(
                    take_contained_panics(),
                    [(
                        PanicSite::Transport,
                        None,
                        Some("waker panicked".to_owned())
                    )]
                );
                assert!(fixture.poll_run().is_pending());
                let mut response = fixture.submit(
                    get_head(),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                fixture.origin.send(OK_RESPONSE);
                let mut head = fixture.next_header(&mut response);
                assert_eq!(head.headers_mut().status_code().unwrap(), 200);
                while !fixture.next_event(&mut response, u32::MAX).unwrap().eom() {}
                assert!(fixture.settle_run().is_none());
                assert!(fixture.handle.close_when_idle().unwrap());
                fixture
                    .settle_run()
                    .expect("the idle connection closes")
                    .unwrap();
                assert!(take_contained_panics().is_empty());
            })
        });
    }

    #[test]
    fn a_body_read_after_its_response_ended_does_not_take_the_drive() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let upload = Arc::new(tokio::sync::Notify::new());
                let mut response = fixture.submit(
                    post_head(Some(5), false),
                    GatedUpload::held(&upload, None),
                    CancellationToken::never(),
                );
                // An early response, all of it parsed by the run before the upload ends.
                fixture
                    .origin
                    .send(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
                let mut head = fixture.next_header(&mut response);
                assert_eq!(head.headers_mut().status_code().unwrap(), 200);
                upload.notify_one();
                assert!(fixture.settle_run().is_none());
                assert!(fixture.state.borrow().writes.concat().ends_with(b"hello"));
                let turns = driver_turns();
                let Poll::Ready(event) = fixture.body(&mut response, 1) else {
                    panic!("a queued byte is taken");
                };
                assert_eq!(body_bytes(&event.unwrap()), b"o");
                assert_eq!(
                    driver_turns(),
                    turns,
                    "after its response's end, a read does not drive the connection"
                );
                assert!(
                    !response.body().holds_the_drive(),
                    "after its response's end, a read does not take the drive"
                );
                let mut rest = Vec::new();
                loop {
                    let event = fixture.next_event(&mut response, 1).unwrap();
                    rest.extend(body_bytes(&event));
                    if event.eom() {
                        break;
                    }
                }
                assert_eq!(rest, b"k");
                assert!(fixture.settle_run().is_none());
                assert_eq!(
                    fixture.handle.available_capacity(),
                    1,
                    "the exchange retires and the slot returns while the body lives"
                );
                drop(response);
                assert!(fixture.handle.close_when_idle().unwrap());
                fixture
                    .settle_run()
                    .expect("the idle connection closes")
                    .unwrap();
            })
        });
    }

    #[test]
    fn eof_after_the_response_end_is_applied_immediately() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let log = Rc::new(AdmissionLog::default());
                fixture.handle.set_observer(log.clone()).unwrap();
                let mut response = fixture.held();
                fixture
                    .origin
                    .send(&[chunk(b"abc"), b"0\r\n\r\n".to_vec()].concat());
                let Poll::Ready(event) = fixture.body(&mut response, 1) else {
                    panic!("the drive parses the response's end");
                };
                assert_eq!(body_bytes(&event.unwrap()), b"a");
                assert!(
                    !response.body().holds_the_drive(),
                    "the drive returned at the release"
                );
                fixture.origin.push(Pushed::Eof);
                assert!(fixture.settle_run().is_none());
                assert!(
                    log.0.borrow().iter().any(|note| note == "admission_closed"),
                    "the run applies the EOF at once, with the response's events unread"
                );
                let mut rest = Vec::new();
                loop {
                    let event = fixture.next_event(&mut response, 1).unwrap();
                    rest.extend(body_bytes(&event));
                    if event.eom() {
                        break;
                    }
                }
                assert_eq!(rest, b"bc");
                fixture
                    .settle_run()
                    .expect("the connection closes once the exchange retires")
                    .unwrap();
            })
        });
    }

    #[test]
    fn a_producer_abort_completes_the_source_immediately_and_returns_the_drive() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let mut response = fixture.held();
                // A malformed chunk size fails the response in the drive that parses it.
                fixture.origin.send(b"zz\r\n");
                let holder = fixture.holder_wake.get();
                let Poll::Ready(read) = fixture.body(&mut response, u32::MAX) else {
                    panic!("the failure is read in the read that parses it");
                };
                let read = read.err().map(|error| error.code());
                assert!(read.is_some());
                assert_eq!(
                    fixture.holder_wake.get(),
                    holder,
                    "the drive wakes only the run"
                );
                assert!(
                    !response.body().holds_the_drive(),
                    "the abort completed the source at once, which returned the drive"
                );
                let run = fixture.settle_run().expect("the run completes on its wake");
                assert_eq!(run.err().map(|error| error.code()), read);
            })
        });
    }

    #[test]
    fn an_immediately_ready_budget_yield_is_not_lost() {
        run_on_host_with(|host, context| {
            Box::pin(async move {
                host.set_yield_mode(crate::external_host_tests::YieldMode::Immediate);
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let mut response = fixture.held();
                // More immediately ready reads than one poll's turn budget: the drive yields,
                // and the yield is ready on its first poll.
                for _ in 0..100 {
                    fixture.origin.send(&chunk(b"a"));
                }
                let mut body = Vec::new();
                let mut seen = fixture.holder_wake.get();
                // As a downstream task, the holder reads again only after a wake.
                for _ in 0..1000 {
                    match fixture.body(&mut response, u32::MAX) {
                        Poll::Ready(event) => {
                            body.extend(body_bytes(&event.unwrap()));
                            if body.len() == 100 {
                                break;
                            }
                        }
                        Poll::Pending => {
                            let wakes = fixture.holder_wake.get();
                            assert!(
                                wakes > seen,
                                "the holder waits with no wake to come: {} of 100 bytes read",
                                body.len()
                            );
                            seen = wakes;
                        }
                    }
                }
                assert_eq!(body, vec![b'a'; 100]);
                fixture.finish(&mut response).unwrap();
            })
        });
    }

    #[test]
    fn a_connection_cancel_while_held_and_unread_is_applied_by_the_run() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let mut response = fixture.held();
                fixture.origin.send(&chunk(b"one"));
                assert!(fixture.body(&mut response, u32::MAX).is_ready());
                // The holder stops reading, as a downstream whose client does not read.
                let holder = fixture.holder_wake.get();
                fixture.handle.cancel();
                let run = fixture
                    .settle_run()
                    .expect("the run applies the cancel without the holder");
                assert_eq!(
                    run.err().map(|error| error.code()),
                    Some(HttpErrorCode::CORO_CANCELLED)
                );
                assert_eq!(fixture.state.borrow().close, CloseKind::Reset);
                assert!(
                    fixture.holder_wake.get() > holder,
                    "the holder is woken to read the outcome"
                );
                let Poll::Ready(read) = fixture.body(&mut response, u32::MAX) else {
                    panic!("the holder's next read returns the outcome");
                };
                assert_eq!(
                    read.err().map(|error| error.code()),
                    Some(HttpErrorCode::CORO_CANCELLED)
                );
            })
        });
    }

    #[test]
    fn a_third_party_request_cancel_while_held_is_applied_by_the_run() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let mut response = fixture.held();
                fixture.origin.send(&chunk(b"one"));
                assert!(fixture.body(&mut response, u32::MAX).is_ready());
                response.control().cancel(HttpError::with_message(
                    HttpErrorCode::CANCEL,
                    b"route timeout",
                ));
                let run = fixture
                    .settle_run()
                    .expect("the run applies the request's cancel without the holder");
                assert_eq!(
                    run.err().map(|error| error.code()),
                    Some(HttpErrorCode::CANCEL)
                );
                assert_eq!(fixture.state.borrow().close, CloseKind::Reset);
                let Poll::Ready(read) = fixture.body(&mut response, u32::MAX) else {
                    panic!("the holder's next read returns the outcome");
                };
                assert_eq!(
                    read.err().map(|error| error.code()),
                    Some(HttpErrorCode::CANCEL)
                );
            })
        });
    }

    /// The run's last poll ends at a budget yield that completes only at the host's I/O phase,
    /// as an event-loop host's does. A connection or request cancel then lands, and the body
    /// reads before the run runs. The run applies the cancel without the holder's reads.
    fn cancel_before_acquisition_with_a_pending_yield(request_cancel: bool, code: HttpErrorCode) {
        run_on_host_with(move |host, context| {
            Box::pin(async move {
                host.set_yield_mode(crate::external_host_tests::YieldMode::Deferred);
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let mut response = fixture.submit(
                    get_head(),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                fixture.origin.send(CHUNKED_HEAD);
                // More ready reads than one poll's turn budget.
                for _ in 0..100 {
                    fixture.origin.send(&chunk(b"a"));
                }
                let yields = host.yields();
                assert!(fixture.settle_run().is_none());
                assert!(
                    host.yields() > yields,
                    "the run's poll ends at a pending budget yield"
                );
                if request_cancel {
                    response.control().cancel(HttpError::with_message(
                        HttpErrorCode::CANCEL,
                        b"route timeout",
                    ));
                } else {
                    fixture.handle.cancel();
                }
                let _ = fixture.header(&mut response);
                let held = response.body().holds_the_drive();
                let mut run = fixture.settle_run();
                if run.is_none() {
                    // The host idles, and its I/O phase completes the yield.
                    LocalHost::sleep_until(&*host, LocalHost::now(&*host) + Duration::from_secs(1))
                        .await;
                    run = fixture.settle_run();
                }
                let run = run.unwrap_or_else(|| {
                    panic!(
                        "the run applies the cancel without the holder's reads (the read took the drive: {held})"
                    )
                });
                assert_eq!(run.err().map(|error| error.code()), Some(code));
                assert_eq!(fixture.state.borrow().close, CloseKind::Reset);
                let outcome = (0..200)
                    .find_map(|_| match fixture.body(&mut response, u32::MAX) {
                        Poll::Ready(Ok(_)) => None,
                        Poll::Ready(Err(error)) => Some(error.code()),
                        Poll::Pending => panic!("the holder's reads return the outcome"),
                    })
                    .expect("the holder's reads end with the outcome");
                assert_eq!(outcome, code);
            })
        });
    }

    #[test]
    fn a_connection_cancel_before_acquisition_with_a_pending_yield_is_applied_by_the_run() {
        cancel_before_acquisition_with_a_pending_yield(false, HttpErrorCode::CORO_CANCELLED);
    }

    #[test]
    fn a_request_cancel_before_acquisition_with_a_pending_yield_is_applied_by_the_run() {
        cancel_before_acquisition_with_a_pending_yield(true, HttpErrorCode::CANCEL);
    }

    #[test]
    fn a_token_cancel_while_held_and_unread_is_applied_by_the_run() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let token = CancellationSource::new();
                let mut response =
                    fixture.submit(get_head(), HttpSourceHolder::empty(), token.token());
                fixture.origin.send(CHUNKED_HEAD);
                let Poll::Ready(head) = fixture.header(&mut response) else {
                    panic!("the head is taken in the read that takes the drive");
                };
                assert_eq!(head.unwrap().headers_mut().status_code().unwrap(), 200);
                assert!(response.body().holds_the_drive());
                fixture.origin.send(&chunk(b"one"));
                assert!(fixture.body(&mut response, u32::MAX).is_ready());
                // Cancelled from another thread while the holder does not read.
                std::thread::scope(|scope| {
                    scope.spawn(|| assert!(token.request_cancellation()));
                });
                let run = fixture
                    .settle_run()
                    .expect("the run applies the token's cancel without the holder");
                assert_eq!(
                    run.err().map(|error| error.code()),
                    Some(HttpErrorCode::CORO_CANCELLED)
                );
                assert_eq!(fixture.state.borrow().close, CloseKind::Reset);
                let Poll::Ready(read) = fixture.body(&mut response, u32::MAX) else {
                    panic!("the holder's next read returns the outcome");
                };
                assert_eq!(
                    read.err().map(|error| error.code()),
                    Some(HttpErrorCode::CORO_CANCELLED)
                );
            })
        });
    }

    #[test]
    fn a_cancel_from_an_inline_callback_revokes_without_borrowing_the_driver() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let mut response = fixture.held();
                let handle = fixture.handle.clone();
                *fixture.origin.on_read_start.borrow_mut() =
                    Some(Box::new(move || handle.cancel()));
                fixture.origin.send(&chunk(b"one"));
                let holder = fixture.holder_wake.get();
                // The drive reads the chunk, and the next read start cancels the connection.
                let read = catch_unwind(AssertUnwindSafe(|| fixture.body(&mut response, u32::MAX)));
                let Ok(Poll::Ready(read)) = read else {
                    panic!("the holder's read neither panics nor waits");
                };
                assert_eq!(
                    read.err().map(|error| error.code()),
                    Some(HttpErrorCode::CORO_CANCELLED),
                    "the drive applied the cancel at its next phase boundary"
                );
                assert_eq!(
                    fixture.holder_wake.get(),
                    holder,
                    "a revocation inside the holder's drive does not wake the holder"
                );
                let run = fixture
                    .settle_run()
                    .expect("the run completes on its wakes");
                assert_eq!(
                    run.err().map(|error| error.code()),
                    Some(HttpErrorCode::CORO_CANCELLED),
                    "with the cancel: the revocation borrowed no driver"
                );
                assert_eq!(fixture.state.borrow().close, CloseKind::Reset);
            })
        });
    }

    #[test]
    fn worker_cancellation_while_held_completes_the_run_on_its_scope() {
        run_on_host(|context| {
            Box::pin(async move {
                let worker = context.handle().clone();
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let mut response = fixture.held();
                fixture.origin.send(&chunk(b"one"));
                assert!(fixture.body(&mut response, u32::MAX).is_ready());
                assert!(worker.begin_shutdown());
                // The root polls the run at worker cancellation.
                let run = match fixture.poll_run() {
                    Poll::Ready(run) => run,
                    Poll::Pending => fixture
                        .settle_run()
                        .expect("the run completes on its scope, without the holder"),
                };
                assert_eq!(
                    run.err().map(|error| error.code()),
                    Some(HttpErrorCode::CORO_CANCELLED)
                );
                assert_eq!(fixture.state.borrow().close, CloseKind::Reset);
                let Poll::Ready(read) = fixture.body(&mut response, u32::MAX) else {
                    panic!("the holder's next read returns the outcome");
                };
                assert!(read.is_err());
            })
        });
    }

    #[test]
    fn dropping_the_run_while_held_revokes_and_aborts() {
        let (inspected_tx, inspected_rx) = std::sync::mpsc::channel();
        let completion = forced_teardown_of_scope(move |context| {
            let gate = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([ReadAction::WaitData(
                Arc::clone(&gate),
                CHUNKED_HEAD.to_vec(),
            )]);
            let (session, handle) =
                OwnedH1UpstreamSession::new(upstream_options(context, transport)).unwrap();
            let submission = handle.reserve_request().unwrap().submit_pull_through(
                get_head(),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let slot: Rc<RefCell<Option<OwnedH1PullThroughBody>>> = Rc::new(RefCell::new(None));
            let held = Rc::new(Cell::new(false));
            let (body, held_at_teardown) = (Rc::clone(&slot), Rc::clone(&held));
            after_forced_teardown(move || {
                let mut body = body.borrow_mut().take().expect("the body outlives the run");
                let holds = body.holds_the_drive();
                let Poll::Ready(read) = body.poll_header(&mut Context::from_waker(Waker::noop()))
                else {
                    panic!("the abort is read at once");
                };
                inspected_tx
                    .send((
                        held_at_teardown.get(),
                        holds,
                        read.err().map(|error| error.code()),
                        state.borrow().close,
                    ))
                    .unwrap();
            });
            Box::pin(async move {
                let _ = futures::join!(session.run_pull_through(), async move {
                    let response = submission.await.unwrap();
                    gate.notify_one();
                    *slot.borrow_mut() = Some(response.body);
                    // The head read takes the drive, then waits for the head.
                    let _ = poll_fn(|context| {
                        let mut body = slot.borrow_mut();
                        let body = body.as_mut().expect("the body is stored");
                        let poll = body.poll_header(context);
                        held.set(body.holds_the_drive());
                        poll
                    })
                    .await;
                    std::future::pending::<()>().await;
                    drop(handle);
                });
            })
        });
        assert_eq!(
            completion,
            Err(ManagedWorkerError::TeardownDeadlineExceeded),
            "the forced destruction of the unjoined run is a clean overrun"
        );
        let (held, holds, read, close) = inspected_rx.recv().unwrap();
        assert!(held, "the body held the drive when the run was dropped");
        assert!(!holds, "dropping the run took the drive back");
        assert_eq!(
            read,
            Some(HttpErrorCode::CORO_CANCELLED),
            "the body reads the abort"
        );
        assert_eq!(
            close,
            CloseKind::Reset,
            "the abandoned driver reset its transport"
        );
    }

    /// Dropping the run future with a completion an inline drive stashed finishes the run: no
    /// abort, no join assertion, and a panic the driver contained in that drive (here the
    /// transport's reset) is counted, not resumed.
    #[test]
    fn dropping_the_run_with_a_stashed_completion_neither_aborts_nor_asserts() {
        for panics in [false, true] {
            let (dropped, reports) = run_on_host(move |context| {
                Box::pin(async move {
                    take_contained_panics();
                    let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                    let mut response = fixture.held();
                    if panics {
                        *fixture.origin.on_reset.borrow_mut() =
                            Some(Box::new(|| panic!("reset panicked")));
                    }
                    // An EOF inside the response completes the run in the holder's drive.
                    fixture.origin.push(Pushed::Eof);
                    assert!(fixture.body(&mut response, u32::MAX).is_ready());
                    assert!(!fixture.handle.is_finished());
                    let dropped = catch_unwind(AssertUnwindSafe(|| fixture.drop_run()))
                        .err()
                        .map(panic_text);
                    assert!(
                        fixture.handle.is_finished(),
                        "the drop marked the run finished"
                    );
                    assert!(fixture.origin.dropped.get(), "and dropped the driver");
                    assert_eq!(fixture.state.borrow().close, CloseKind::Reset);
                    (dropped, take_contained_panics())
                })
            });
            assert_eq!(dropped, None, "the drop neither aborts nor asserts");
            let expected = if panics {
                vec![(
                    PanicSite::Transport,
                    None,
                    Some("reset panicked".to_owned()),
                )]
            } else {
                Vec::new()
            };
            assert_eq!(reports, expected, "panics {panics}");
        }
    }

    /// A response observer whose deadline never fires.
    struct NoDeadline;

    impl crate::owned::OwnedResponseObserver for NoDeadline {
        fn poll_deadline(&mut self, _context: &mut Context<'_>) -> Poll<HttpError> {
            Poll::Pending
        }

        fn response_complete(self: Box<Self>, _completed: Result<std::time::Instant, HttpError>) {}
    }

    #[test]
    fn response_observation_is_refused_on_a_pull_through_exchange() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let mut response = fixture.held();
                let refused = response
                    .control()
                    .observe_response(Box::new(NoDeadline))
                    .err()
                    .map(|error| error.code());
                assert_eq!(
                    refused,
                    Some(HttpErrorCode::INVALID_STATE_TRANSITION),
                    "its deadline would be processed only on the holder's reads"
                );
                fixture.finish(&mut response).unwrap();
            })
        });
    }

    #[test]
    fn a_second_pull_through_exchange_is_refused() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let mut response = fixture.held();
                let refused = fixture
                    .handle
                    .reserve_request()
                    .err()
                    .map(|error| error.code());
                assert_eq!(
                    refused,
                    Some(HttpErrorCode::REFUSED_STREAM),
                    "capacity one: no second exchange while one holds the drive"
                );
                fixture.finish(&mut response).unwrap();
            })
        });
    }

    #[test]
    fn a_drain_while_held_finishes_the_exchange_then_closes() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let mut response = fixture.held();
                assert!(fixture.handle.close_when_idle().unwrap());
                assert!(
                    response.body().holds_the_drive(),
                    "a drain does not take the drive back"
                );
                fixture
                    .origin
                    .send(&[chunk(b"one"), b"0\r\n\r\n".to_vec()].concat());
                let mut body = Vec::new();
                loop {
                    let event = fixture.next_event(&mut response, u32::MAX).unwrap();
                    body.extend(body_bytes(&event));
                    if event.eom() {
                        break;
                    }
                }
                assert_eq!(body, b"one");
                fixture
                    .settle_run()
                    .expect("the drained connection closes once the exchange retires")
                    .unwrap();
                assert_eq!(fixture.state.borrow().close, CloseKind::ShutdownWrite);
            })
        });
    }

    /// What one read after a stall observed: the transport reads completed by then, and the size of
    /// its event. The origin sends a 256 KiB body in 4 KiB reads while the reader does not read.
    fn first_read_after_a_stall(mode: Mode) -> (usize, usize) {
        run_on_host(move |context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, mode, |config| {
                    config.ingress_buffer_limit = 16 << 10;
                });
                let mut response = fixture.submit(
                    get_head(),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                let body = patterned(256 << 10);
                fixture.origin.send(
                    format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).as_bytes(),
                );
                fixture.next_header(&mut response);
                for piece in body.chunks(4 << 10) {
                    fixture.origin.send(piece);
                }
                fixture.settle_run();
                let first = body_bytes(&fixture.next_event(&mut response, u32::MAX).unwrap());
                let delivered = fixture.origin.delivered.get();
                let mut read = first.clone();
                loop {
                    let event = fixture.next_event(&mut response, u32::MAX).unwrap();
                    read.extend(body_bytes(&event));
                    if event.eom() {
                        break;
                    }
                }
                assert!(read == body, "the body arrives intact");
                assert!(fixture.settle_run().is_none());
                assert!(fixture.handle.close_when_idle().unwrap());
                fixture
                    .settle_run()
                    .expect("the idle connection closes")
                    .unwrap();
                (delivered, first.len())
            })
        })
    }

    #[test]
    fn one_holder_read_buffers_at_most_the_ingress_limit() {
        let run = first_read_after_a_stall(Mode::Run);
        let pull_through = first_read_after_a_stall(Mode::PullThrough);
        assert_eq!(
            pull_through, run,
            "one read after a stall reads and buffers what run reads and buffers"
        );
        assert!(
            pull_through.1 <= 16 << 10,
            "the source holds at most the ingress limit"
        );
    }

    #[test]
    fn a_slow_reader_gets_the_whole_body_after_flow_pauses() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::PullThrough, |config| {
                    config.ingress_buffer_limit = 16 << 10;
                });
                let mut response = fixture.submit(
                    get_head(),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                let body = patterned(256 << 10);
                fixture.origin.send(
                    format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).as_bytes(),
                );
                fixture.next_header(&mut response);
                for piece in body.chunks(16 << 10) {
                    fixture.origin.send(piece);
                }
                // The holder reads 4 KiB at a time, and again only after a wake. The resume
                // credit wakes it once parsing may resume.
                let mut read = Vec::new();
                let mut seen = fixture.holder_wake.get();
                for _ in 0..10_000 {
                    match fixture.body(&mut response, 4 << 10) {
                        Poll::Ready(event) => {
                            let event = event.unwrap();
                            read.extend(body_bytes(&event));
                            if event.eom() {
                                break;
                            }
                        }
                        Poll::Pending => {
                            let wakes = fixture.holder_wake.get();
                            assert!(
                                wakes > seen,
                                "the holder waits with no wake to come: {} bytes read",
                                read.len()
                            );
                            seen = wakes;
                        }
                    }
                }
                assert!(read == body, "the whole body arrives: {} bytes", read.len());
                assert!(fixture.settle_run().is_none());
                assert!(fixture.handle.close_when_idle().unwrap());
                fixture
                    .settle_run()
                    .expect("the idle connection closes")
                    .unwrap();
            })
        });
    }

    /// Whether an EOF that arrives once the connection is idle, after an exchange, closes
    /// admission and ends the run immediately, and the run's error code.
    fn eof_while_idle(mode: Mode) -> (bool, Option<Option<HttpErrorCode>>) {
        run_on_host(move |context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, mode, |_| {});
                let log = Rc::new(AdmissionLog::default());
                fixture.handle.set_observer(log.clone()).unwrap();
                let mut response = fixture.submit(
                    get_head(),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                fixture.origin.send(OK_RESPONSE);
                fixture.next_header(&mut response);
                while !fixture.next_event(&mut response, u32::MAX).unwrap().eom() {}
                assert!(fixture.settle_run().is_none());
                assert!(fixture.handle.is_idle());
                fixture.origin.push(Pushed::Eof);
                let run = fixture.settle_run();
                (
                    log.0.borrow().iter().any(|note| note == "admission_closed"),
                    run.map(|run| run.err().map(|error| error.code())),
                )
            })
        })
    }

    #[test]
    fn eof_while_idle_is_applied_immediately() {
        let pull_through = eof_while_idle(Mode::PullThrough);
        assert_eq!(pull_through, eof_while_idle(Mode::Run));
        assert!(pull_through.0, "admission closes at once");
        assert!(pull_through.1.is_some(), "and the run ends at once");
    }

    /// What a reader read of a response whose connection fails with a read error after a chunk,
    /// with `deliver_ingress_before_abort`, and `run`'s error code.
    fn reset_mid_response(mode: Mode) -> (Vec<String>, Option<HttpErrorCode>) {
        run_on_host(move |context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, mode, |config| {
                    config.deliver_ingress_before_abort = true;
                });
                let mut response = fixture.submit(
                    get_head(),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                fixture.origin.send(CHUNKED_HEAD);
                fixture.next_header(&mut response);
                fixture.origin.send(&chunk(b"one"));
                fixture
                    .origin
                    .push(Pushed::Error(HttpErrorCode::TRANSPORT_READ_ERROR));
                let mut run = fixture.settle_run();
                let mut events = Vec::new();
                loop {
                    match fixture.next_event(&mut response, u32::MAX) {
                        Ok(event) => events.push(String::from_utf8(body_bytes(&event)).unwrap()),
                        Err(error) => {
                            events.push(format!("error {:?}", error.code()));
                            break;
                        }
                    }
                }
                run = run.or_else(|| fixture.settle_run());
                (
                    events,
                    run.expect("the run completes")
                        .err()
                        .map(|error| error.code()),
                )
            })
        })
    }

    #[test]
    fn an_upstream_reset_mid_response_delivers_kept_events_then_the_error() {
        let pull_through = reset_mid_response(Mode::PullThrough);
        assert_eq!(pull_through, reset_mid_response(Mode::Run));
        assert_eq!(
            pull_through.0,
            [
                "one".to_owned(),
                format!("error {:?}", HttpErrorCode::TRANSPORT_READ_ERROR)
            ]
        );
    }

    /// Where a reader drops its response: with a body read pending, or between body events.
    #[derive(Clone, Copy, Debug)]
    enum DropPoint {
        Pending,
        Armed,
    }

    /// Drops the response mid-body at `point` and returns `run`'s error code and the
    /// connection's close kind. With `gated`, the request body is never released, so a
    /// pull-through body reads with waiting reads, as a plain response's reader does. Otherwise
    /// it holds the drive.
    fn dropped_mid_response(
        mode: Mode,
        gated: bool,
        point: DropPoint,
    ) -> (Option<HttpErrorCode>, CloseKind) {
        run_on_host(move |context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, mode, |_| {});
                let upload = Arc::new(tokio::sync::Notify::new());
                let mut response = if gated {
                    fixture.submit(
                        post_head(Some(5), false),
                        GatedUpload::held(&upload, None),
                        CancellationToken::never(),
                    )
                } else {
                    fixture.submit(
                        get_head(),
                        HttpSourceHolder::empty(),
                        CancellationToken::never(),
                    )
                };
                fixture.origin.send(CHUNKED_HEAD);
                fixture.next_header(&mut response);
                fixture.origin.send(&chunk(b"one"));
                assert_eq!(
                    body_bytes(&fixture.next_event(&mut response, u32::MAX).unwrap()),
                    b"one"
                );
                if let DropPoint::Pending = point {
                    assert!(fixture.body(&mut response, u32::MAX).is_pending());
                }
                if mode == Mode::PullThrough {
                    assert_eq!(response.body().holds_the_drive(), !gated);
                }
                drop(response);
                let run = fixture.settle_run().expect("the run closes the connection");
                (
                    run.err().map(|error| error.code()),
                    fixture.state.borrow().close,
                )
            })
        })
    }

    #[test]
    fn dropping_the_body_mid_response_closes_the_connection_as_run_does() {
        for gated in [false, true] {
            for point in [DropPoint::Pending, DropPoint::Armed] {
                let run = dropped_mid_response(Mode::Run, gated, point);
                assert!(run.0.is_some(), "{point:?}: the connection fails");
                assert_eq!(
                    dropped_mid_response(Mode::PullThrough, gated, point),
                    run,
                    "gated {gated}, {point:?}"
                );
            }
        }
    }

    #[test]
    fn dropping_the_body_from_an_inline_callback_does_not_poll_the_run() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut a = Fixture::new(context.clone(), Mode::PullThrough, |_| {});
                let mut b = Fixture::new(context, Mode::PullThrough, |_| {});
                let mut a_response = a.held();
                let b_response = Rc::new(RefCell::new(Some(b.held())));
                let dropped = Rc::clone(&b_response);
                // A callback inside A's drive drops B's held body.
                *a.origin.on_read_start.borrow_mut() =
                    Some(Box::new(move || drop(dropped.borrow_mut().take())));
                a.origin.send(&chunk(b"one"));
                let b_run = b.run_wake.get();
                let Poll::Ready(event) = a.body(&mut a_response, u32::MAX) else {
                    panic!("A's read drives");
                };
                assert_eq!(body_bytes(&event.unwrap()), b"one");
                assert!(
                    b_response.borrow().is_none(),
                    "B's body dropped inside A's drive"
                );
                assert_eq!(
                    b.state.borrow().close,
                    CloseKind::Open,
                    "B's driver was not polled: its connection is not yet closed"
                );
                assert!(b.run_wake.get() > b_run, "B's run is woken instead");
                let b_closed = b
                    .settle_run()
                    .expect("B's run closes its connection on its wake");
                assert!(b_closed.is_err());
                assert_eq!(b.state.borrow().close, CloseKind::Reset);
                a.finish(&mut a_response).unwrap();
            })
        });
    }

    #[test]
    fn a_body_dropped_while_unwinding_does_not_poll_the_run() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let mut response = fixture.held();
                fixture.origin.send(&chunk(b"one"));
                assert!(fixture.body(&mut response, u32::MAX).is_ready());
                let run = fixture.run_wake.get();
                let unwound = catch_unwind(AssertUnwindSafe(move || {
                    let _response = response;
                    panic!("unwinding");
                }));
                assert_eq!(
                    panic_text(unwound.unwrap_err()),
                    "unwinding",
                    "the body's drop neither panics again nor aborts"
                );
                assert_eq!(
                    fixture.state.borrow().close,
                    CloseKind::Open,
                    "the run was not polled during the unwind"
                );
                assert!(fixture.run_wake.get() > run, "the run is woken");
                let closed = fixture.settle_run().expect("the run closes the connection");
                assert!(closed.is_err());
                assert_eq!(fixture.state.borrow().close, CloseKind::Reset);
            })
        });
    }

    /// Records the runtime context each `capacity_released` callback runs under.
    #[derive(Default)]
    struct ContextProbe(RefCell<Vec<Option<roxygen_runtime::context::RuntimeContext>>>);

    impl crate::owned::OwnedUpstreamObserver for ContextProbe {
        fn capacity_released(&self, _stream: StreamId) {
            self.0
                .borrow_mut()
                .push(roxygen_runtime::context::RuntimeContext::current());
        }
    }

    #[test]
    fn inline_drives_run_under_the_holder_s_runtime_context() {
        run_on_host(|context| {
            Box::pin(async move {
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let probe = Rc::new(ContextProbe::default());
                fixture.handle.set_observer(probe.clone()).unwrap();
                let mut response = fixture.held();
                fixture
                    .origin
                    .send(&[chunk(b"one"), b"0\r\n\r\n".to_vec()].concat());
                let holder = roxygen_runtime::context::RuntimeContext::new();
                let read = {
                    let _entered = holder.enter();
                    fixture.body(&mut response, u32::MAX)
                };
                let Poll::Ready(event) = read else {
                    panic!("the drive parses the response's end");
                };
                assert_eq!(
                    probe.0.borrow().as_slice(),
                    [Some(holder)],
                    "the release in the holder's drive runs under the holder's context"
                );
                let mut eom = event.unwrap().eom();
                while !eom {
                    eom = fixture.next_event(&mut response, u32::MAX).unwrap().eom();
                }
                assert!(fixture.settle_run().is_none());
                assert!(fixture.handle.close_when_idle().unwrap());
                fixture
                    .settle_run()
                    .expect("the idle connection closes")
                    .unwrap();
            })
        });
    }

    /// A Driver-scope panic inside an inline drive tears the connection down; the run future
    /// finishes the teardown and fails with `INTERNAL_ERROR`, and the panic is counted once.
    #[test]
    fn a_callback_panic_in_an_inline_drive_fails_the_run() {
        run_on_host(|context| {
            Box::pin(async move {
                take_contained_panics();
                let mut fixture = Fixture::new(context, Mode::PullThrough, |_| {});
                let mut response = fixture.held();
                *fixture.origin.on_read_start.borrow_mut() =
                    Some(Box::new(|| panic!("read start panicked")));
                fixture.origin.send(&chunk(b"one"));
                let read = catch_unwind(AssertUnwindSafe(|| fixture.body(&mut response, u32::MAX)));
                let Ok(Poll::Ready(read)) = read else {
                    panic!("the holder's read neither panics nor waits");
                };
                assert_eq!(
                    read.err().map(|error| error.code()),
                    Some(HttpErrorCode::INTERNAL_ERROR),
                    "the holder reads the connection's error"
                );
                let run = fixture
                    .settle_run()
                    .expect("the run completes on its wakes");
                assert_eq!(
                    run.err().map(|error| error.code()),
                    Some(HttpErrorCode::INTERNAL_ERROR)
                );
                assert_eq!(
                    take_contained_panics(),
                    [(
                        PanicSite::DriverTurn,
                        None,
                        Some("read start panicked".to_owned())
                    )]
                );
                assert!(fixture.handle.is_finished());
                assert_eq!(fixture.state.borrow().close, CloseKind::Reset);
            })
        });
    }

    /// A downstream exchange that forwards one upstream response, a fresh head and then each
    /// body event, as a proxy's exchange does.
    struct Forward(Rc<RefCell<Option<Response>>>);

    impl crate::owned::OwnedExchange for Forward {
        fn request(
            &mut self,
            _head: Box<roxygen_core::message::HttpMessage>,
            _body: roxygen_core::stream_source::HttpStreamSourceOwner,
            _eom: bool,
        ) {
        }

        fn poll_headers(
            &mut self,
            context: &mut Context<'_>,
        ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
            let mut upstream = self.0.borrow_mut();
            let response = upstream.as_mut().expect("the upstream response is live");
            match response.poll_header(context) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                Poll::Ready(Ok(_)) => {
                    let mut message = roxygen_core::message::HttpMessage::new();
                    message.set_status_code(200).unwrap();
                    message.set_http_version(1, 1);
                    Poll::Ready(Ok(HttpHeaderEvent::new(message, false).unwrap()))
                }
            }
        }

        fn poll_body(
            &mut self,
            context: &mut Context<'_>,
            maximum: u32,
        ) -> Poll<Result<HttpBodyEvent, HttpError>> {
            let mut upstream = self.0.borrow_mut();
            upstream
                .as_mut()
                .expect("the upstream response is live")
                .poll_body(context, maximum)
        }

        fn cancel(&mut self, _error: &HttpError, _peer_reset: bool) {
            let upstream = self.0.borrow_mut().take();
            drop(upstream);
        }

        fn response_complete(&mut self) {}

        fn complete(self) {}
    }

    struct ForwardApp(Rc<RefCell<Option<Response>>>);

    impl crate::owned::OwnedApplication for ForwardApp {
        type Exchange = Forward;

        fn new_exchange(
            &self,
            _stream: StreamId,
            _ingress: roxygen_core::ingress_completion::IngressCompletion,
        ) -> Forward {
            Forward(Rc::clone(&self.0))
        }
    }

    /// The downstream wire, the downstream run's and the upstream run's error codes, and the
    /// upstream's close kind, when a callback the upstream runs while it parses a chunk
    /// cancels the downstream connection that forwards the response.
    fn downstream_cancel_outcome(
        mode: Mode,
    ) -> (
        Vec<u8>,
        Option<HttpErrorCode>,
        Option<HttpErrorCode>,
        CloseKind,
    ) {
        run_on_host(move |context| {
            Box::pin(async move {
                let mut upstream = Fixture::new(context.clone(), mode, |_| {});
                let response = upstream.submit(
                    get_head(),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                let slot = Rc::new(RefCell::new(Some(response)));
                let (down_transport, down_state) = ScriptedTransport::new([ReadAction::Data(
                    b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec(),
                )]);
                let options = OwnedH1Options {
                    context,
                    transport: down_transport,
                    io: UniplexConfig {
                        drain_request_body_on_drop: true,
                        ..upstream_config()
                    },
                    http1: Http1DriverConfig::default(),
                };
                let (session, down_handle) =
                    crate::owned_h1::OwnedH1Session::new(ForwardApp(Rc::clone(&slot)), options)
                        .unwrap();
                let mut downstream: Pin<Box<dyn Future<Output = RunResult>>> =
                    Box::pin(session.run());
                let down_wake = Arc::new(Count::default());
                let down_waker = Waker::from(Arc::clone(&down_wake));
                let mut down_seen = usize::MAX;
                let mut down_done = None;
                let mut up_done = None;
                // Polls each run that was woken since its last poll, until neither was.
                let mut pump = |upstream: &mut Fixture,
                                down_done: &mut Option<RunResult>,
                                up_done: &mut Option<RunResult>| {
                    for _ in 0..100 {
                        let mut polled = false;
                        if up_done.is_none() && upstream.run_woken() {
                            polled = true;
                            *up_done = upstream.settle_run();
                        }
                        if down_done.is_none() && down_wake.get() != down_seen {
                            polled = true;
                            down_seen = down_wake.get();
                            if let Poll::Ready(result) = downstream
                                .as_mut()
                                .poll(&mut Context::from_waker(&down_waker))
                            {
                                *down_done = Some(result);
                            }
                        }
                        if !polled {
                            return;
                        }
                    }
                    panic!("the runs kept waking each other");
                };
                upstream.origin.send(CHUNKED_HEAD);
                pump(&mut upstream, &mut down_done, &mut up_done);
                upstream.origin.send(&chunk(b"zero"));
                pump(&mut upstream, &mut down_done, &mut up_done);
                assert!(
                    down_state
                        .borrow()
                        .writes
                        .concat()
                        .windows(4)
                        .any(|window| window == b"zero"),
                    "the first chunk is forwarded"
                );
                let handle = down_handle.clone();
                *upstream.origin.on_read_start.borrow_mut() =
                    Some(Box::new(move || handle.cancel()));
                upstream.origin.send(&chunk(b"one"));
                pump(&mut upstream, &mut down_done, &mut up_done);
                let down = down_done.expect("the downstream run ends");
                let up = up_done.expect("the upstream run ends");
                let wire = down_state.borrow().writes.concat();
                (
                    wire,
                    down.err().map(|error| error.code()),
                    up.err().map(|error| error.code()),
                    upstream.state.borrow().close,
                )
            })
        })
    }

    #[test]
    fn a_downstream_cancel_from_an_inline_callback_serializes_nothing_more() {
        let run = downstream_cancel_outcome(Mode::Run);
        let pull_through = downstream_cancel_outcome(Mode::PullThrough);
        assert!(
            !run.0.windows(3).any(|window| window == b"one"),
            "under run, the cancelled downstream forwards nothing after the cancel"
        );
        assert_eq!(
            pull_through, run,
            "the event taken in the drive is dropped unserialized: the wire and both outcomes \
             are run's"
        );
    }

    /// A chunked response whose head a draining reader read, after which the reader is parked:
    /// read by a suspend-free reader of a plain submission under `run`, or through the body of
    /// a pull-through exchange, which holds the drive.
    fn parked_response(fixture: &mut Fixture) -> Response {
        if fixture.mode == Mode::PullThrough {
            return fixture.held();
        }
        let mut response = fixture.submit(
            get_head(),
            HttpSourceHolder::empty(),
            CancellationToken::never(),
        );
        fixture.origin.send(CHUNKED_HEAD);
        let mut head = fixture.next_header(&mut response);
        assert_eq!(head.headers_mut().status_code().unwrap(), 200);
        response
    }

    /// While a drained reader waits for its waker ("parked"), each driver turn leaves the read
    /// phase without evaluating a read start. Each chunk's read is then the only read start
    /// evaluated for it, under `run` and in a pull-through body's inline drive. Without this
    /// fast exit, the turns after each read would evaluate one each. The chunk's arrival
    /// wakes the parked reader's waker, which reaches the run future under `run` and the holder
    /// while it holds the drive.
    #[test]
    fn owned_h1_upstream_a_parked_reader_evaluates_one_read_start_per_read() {
        let counts = [Mode::Run, Mode::PullThrough].map(|mode| {
            let counts = run_on_host(move |context| {
                Box::pin(async move {
                    let mut fixture = Fixture::draining(context, mode);
                    let mut response = parked_response(&mut fixture);
                    let counts = [b"one".as_slice(), b"two", b"three"]
                        .map(|piece| {
                            let wakes = (fixture.holder_wake.get(), fixture.run_wake.get());
                            let before = read_start_checks();
                            fixture.origin.send(&chunk(piece));
                            let woken = (
                                fixture.holder_wake.get() - wakes.0,
                                fixture.run_wake.get() - wakes.1,
                            );
                            let event = fixture.next_event(&mut response, u32::MAX).unwrap();
                            assert_eq!(body_bytes(&event), piece, "{mode:?}");
                            (woken, read_start_checks() - before)
                        })
                        .to_vec();
                    fixture.finish(&mut response).unwrap();
                    counts
                })
            });
            (mode, counts)
        });
        assert_eq!(
            counts,
            [
                (Mode::Run, vec![((0, 1), 1); 3]),
                (Mode::PullThrough, vec![((1, 0), 1); 3])
            ],
            "(mode, per chunk ((holder wakes, run wakes) at its arrival, read starts evaluated))"
        );
    }

    /// What a reader read after a parked reader's connection was ended by `end`, mid-response
    /// (or, for `Close`, after the response's end): its last read's error, `run`'s error, and
    /// how the transport closed.
    fn parked_upstream_end(
        mode: Mode,
        end: ParkedEnd,
    ) -> (Option<HttpErrorCode>, Option<HttpErrorCode>, CloseKind) {
        run_on_host(move |context| {
            Box::pin(async move {
                let mut fixture = Fixture::draining(context, mode);
                let mut response = parked_response(&mut fixture);
                fixture.origin.send(&chunk(b"one"));
                let event = fixture.next_event(&mut response, u32::MAX).unwrap();
                assert_eq!(body_bytes(&event), b"one");
                match end {
                    ParkedEnd::PeerEof => fixture.origin.push(Pushed::Eof),
                    ParkedEnd::ReadError(code) => fixture.origin.push(Pushed::Error(code)),
                    ParkedEnd::Close => fixture.origin.send(b"0\r\n\r\n"),
                    ParkedEnd::Cancel => fixture.handle.cancel(),
                }
                // Reads to the response's end or error, letting the run progress while a read
                // waits.
                let mut run = None;
                let mut read = None;
                for _ in 0..8 {
                    match fixture.body(&mut response, u32::MAX) {
                        Poll::Ready(Ok(event)) if !event.eom() => {}
                        Poll::Ready(result) => {
                            read = Some(result.err().map(|error| error.code()));
                            break;
                        }
                        Poll::Pending => {
                            if let Some(result) = fixture.settle_run() {
                                run = Some(result);
                            }
                        }
                    }
                }
                let read = read.expect("the response ends");
                if matches!(end, ParkedEnd::Close) {
                    assert!(fixture.settle_run().is_none());
                    assert!(fixture.handle.is_idle(), "the exchange retired");
                    assert!(fixture.handle.close_when_idle().unwrap());
                }
                let run = run
                    .or_else(|| fixture.settle_run())
                    .expect("the parked connection ends");
                (
                    read,
                    run.err().map(|error| error.code()),
                    fixture.state.borrow().close,
                )
            })
        })
    }

    /// A parked reader still ends, under `run` and with a pull-through body holding the drive.
    /// The next read's EOF or failure wakes it, and the driver's own idle close and abort mark
    /// its read wake in the same call (`close_idle`, `reset_transport`). A connection cancel
    /// revokes a held drive first. The fast exit does not change how any connection ends.
    #[test]
    fn owned_h1_upstream_a_parked_reader_still_ends_on_its_wake_close_and_abort() {
        let read_error = HttpErrorCode::TRANSPORT_READ_ERROR;
        let dropped = HttpErrorCode::DROPPED;
        let cancelled = HttpErrorCode::CORO_CANCELLED;
        for mode in [Mode::Run, Mode::PullThrough] {
            for (end, outcome) in [
                (
                    ParkedEnd::PeerEof,
                    (
                        Some(HttpErrorCode::TRANSPORT_EOF),
                        Some(HttpErrorCode::TRANSPORT_EOF),
                        CloseKind::Reset,
                    ),
                ),
                (
                    ParkedEnd::ReadError(read_error),
                    (Some(read_error), Some(read_error), CloseKind::Reset),
                ),
                (
                    ParkedEnd::ReadError(dropped),
                    (Some(dropped), Some(dropped), CloseKind::Reset),
                ),
                (ParkedEnd::Close, (None, None, CloseKind::ShutdownWrite)),
                (
                    ParkedEnd::Cancel,
                    (Some(cancelled), Some(cancelled), CloseKind::Reset),
                ),
            ] {
                assert_eq!(
                    parked_upstream_end(mode, end),
                    outcome,
                    "{mode:?} {end:?}: (read error, run error, close)"
                );
            }
        }
    }
}
