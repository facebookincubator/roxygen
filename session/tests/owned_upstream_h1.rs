// Owned HTTP/1.1 upstream session tests. Each byte comparison runs a `UniplexSession` HTTP/1.1
// client with the same request, the same zero-timeout `UniplexConfig` and the same origin bytes.

use roxygen_core::ids::StreamId;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::stream_source::FlowControlState;

use super::*;
use crate::experimental::OwnedRequestProgressExt;
use crate::external_host_tests::run_on_host;
use crate::external_host_tests::run_on_host_with;
use crate::owned_h1::Http1DriverConfig;
use crate::owned_h1::OwnedH1Options;
use crate::owned_h1::OwnedH1UpstreamReservation;
use crate::owned_h1::OwnedH1UpstreamSession;

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

/// A `POST /upload` head; with `length`, it declares that `Content-Length`.
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

/// As `owned_wire_for`, with an origin that answers `response`, whose body is `response_body`.
fn owned_exchange_wire(
    request: impl FnOnce() -> (HttpHeaderEvent, Vec<HttpBodyEvent>) + Send + 'static,
    response: &'static [u8],
    response_body: &'static [u8],
) -> Vec<u8> {
    run_on_host(move |context| {
        Box::pin(async move {
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
            state.borrow().writes.concat()
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

/// As `uniplex_wire_for`, with an origin that answers `response`, whose body is `response_body`.
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

/// One GET through an owned upstream session whose origin answers with `response`, released
/// once the request is committed; the origin then sends EOF.
fn owned_get(response: &'static [u8]) -> (ReadResponse, Wire) {
    run_on_host(move |context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([
                ReadAction::WaitData(Arc::clone(&gate), response.to_vec()),
                ReadAction::Eof,
            ]);
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
            )
        })
    })
}

struct Wire {
    close: CloseKind,
    error: Option<HttpErrorCode>,
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

/// Writes and closes through a [`ScriptedTransport`]; each read observes its cancellation only
/// when polled, and arranges no wake for it.
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
    let cases: [(&str, Change, ByteEventCapability); 11] = [
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
    // `Sec-WebSocket-Key` lines itself, so the head carries no `Upgrade` header.
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

/// A write queue whose writes complete at once; its capacity check runs `hook` once, its
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

#[test]
fn observer_panic_resets_the_connection_and_resumes_from_run() {
    run_on_host(|context| {
        Box::pin(async move {
            use futures::FutureExt;
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
            let (run, ()) = futures::join!(
                std::panic::AssertUnwindSafe(session.run()).catch_unwind(),
                async {
                    let (_stream, _source, _control) =
                        submission.await.unwrap().into_source_parts();
                    gate.notify_one();
                }
            );
            let panic = run.expect_err("run resumes the observer's panic");
            assert_eq!(panic.downcast_ref::<&str>(), Some(&"observer panicked"));
            assert_eq!(state.borrow().close, CloseKind::Reset);
            assert!(handle.is_finished());
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
            // A token cancelled during the read already fails the transfer; the request's
            // control is the cancel the driver itself must observe.
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

/// What one owned exchange showed: `run`'s error code, the submission's or response's error
/// code, the bytes written, and how the connection closed. A refused submission closes the
/// idle connection before the origin answers, which cancels the read waiting for it.
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
    // A `Content-Length` message has no place for trailers: the codec writes nothing for them,
    // as Proxygen's `generateTrailers` does, and the request ends with its body.
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
    // A reusable connection's settle callbacks, then the one a connection that is not reused
    // runs instead of `capacity_available`.
    for (callback, response) in [
        ("capacity_released", OK_RESPONSE),
        ("capacity_available", OK_RESPONSE),
        ("admission_closed", CLOSE_RESPONSE),
    ] {
        run_on_host(move |context| {
            Box::pin(async move {
                let response_gate = Arc::new(tokio::sync::Notify::new());
                let empty_gate = Arc::new(tokio::sync::Notify::new());
                // The empty read completes in the turn that retires the exchange, so no read is
                // outstanding when the slot returns.
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
            // The head's serialization callback cancels the connection during commitment; the
            // request body is ready, but no read follows the cancel.
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
            // As `cancel_from_a_settle_callback_fails_the_connection`: no read is outstanding
            // when the retired connection's write side would shut down.
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
            // The upload's resume future cancels the connection and completes; no body read
            // follows it.
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

/// What a connection decided after its first exchange, with an origin that keeps it open.
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
    // The origin answers after the first body bytes; the rest of the body follows once the
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
    // The trailers write nothing, so the first request is its head and body.
    let first = [declared_head_wire(5), b"hello".to_vec()].concat();
    assert_eq!(
        String::from_utf8_lossy(&owned.wire[..owned.wire.len() - owned.second_wire.len()]),
        String::from_utf8_lossy(&first)
    );
    // Uniplex fails this request at EOF (`trailers_on_a_content_length_request_end_it_without_them`),
    // so the second request is compared with Uniplex's after an untrailered first request.
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

#[test]
fn retirement_panic_fails_a_reusable_connection() {
    run_on_host(|context| {
        Box::pin(async move {
            use futures::FutureExt;
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
            let (run, read) = futures::join!(
                std::panic::AssertUnwindSafe(session.run()).catch_unwind(),
                async {
                    let (_stream, mut source, _control) =
                        submission.await.unwrap().into_source_parts();
                    first_ready.notify_one();
                    read_response(&mut source).await
                }
            );
            assert_eq!((read.body.as_slice(), read.eom), (b"ok".as_slice(), true));
            let panic = run.expect_err("run resumes the retirement panic");
            assert_eq!(
                panic.downcast_ref::<&str>(),
                Some(&"request source panicked at retirement")
            );
            assert_eq!(
                *log.0.borrow(),
                ["capacity_released(1)", "admission_closed"],
                "the slot is not returned"
            );
            assert_eq!(state.borrow().close, CloseKind::Reset);
            assert!(handle.is_finished());
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
    // Before commitment, EOF closes admission: the submission is refused and nothing is
    // written. `UniplexSession` has no such state here (it writes the request after the EOF,
    // with `Connection: close`), so this is pinned, not compared.
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

/// The consumer's `CONNECT h:443`, with `extra` after its `Host`.
fn ordinary_connect(extra: &[(&str, &str)], chunked: bool, eom: bool) -> HttpHeaderEvent {
    let headers = [&[("Host", "h:443")], extra].concat();
    request_head(HttpMethod::Connect, b"h:443", 1, &headers, chunked, eom)
}

/// The consumer's WebSocket GET, with `extra` after its own headers.
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

/// The bytes the consumer's origin reads for `ordinary_connect(&[], false, true)`.
const ORDINARY_CONNECT_WIRE: &[u8] =
    b"CONNECT h:443 HTTP/1.1\r\nHost: h:443\r\nConnection: keep-alive\r\n\r\n";
/// The bytes the consumer's origin reads for `ordinary_websocket(&[], false, true)`.
const ORDINARY_WEBSOCKET_WIRE: &[u8] = b"GET /ws HTTP/1.1\r\nHost: c1\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nsec-websocket-version: 13\r\nConnection: upgrade, keep-alive\r\n\r\n";

type Request = fn() -> (HttpHeaderEvent, Vec<HttpBodyEvent>);

/// What `ordinary_exchanges` saw.
struct OrdinaryRun {
    run: Option<HttpErrorCode>,
    outcomes: Vec<Result<ReadResponse, HttpErrorCode>>,
    wire: Vec<u8>,
    close: CloseKind,
}

/// Submits each request in turn, once the connection is idle, to one owned upstream connection
/// under `Http1UpgradePolicy::AsOrdinaryRequest`, then closes it once idle. The origin answers
/// each committed request with the next given response; a request expected to be refused has
/// none.
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

/// A complete `200` response with body `ok`, as the reader sees it.
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

/// What `ordinary_outcome` saw: the response the caller read, or the submission's refusal.
#[derive(Debug, PartialEq)]
struct OrdinaryOutcome {
    submitted: Result<ReadResponse, HttpErrorCode>,
    run: Option<HttpErrorCode>,
    wire: Vec<u8>,
    close: CloseKind,
}

/// One request through an owned upstream connection under
/// `Http1UpgradePolicy::AsOrdinaryRequest`; the origin answers it with `response` once it is
/// committed, then sends EOF. A refused request leaves the connection to be closed once idle.
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

/// Asserts that, under `AsOrdinaryRequest`, `refused` fails with `code` before any byte, the
/// slot returns, and a GET then completes on the same connection.
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

/// The turns a session gets to take an origin's bytes, and the end behind them, before a
/// `DIV-033` comparison's reader starts.
const SETTLE_TURNS: usize = 1_000;

async fn settle() {
    for _ in 0..SETTLE_TURNS {
        tokio::task::yield_now().await;
    }
}

/// How the origin's response to a GET ends in the `DIV-033` comparisons.
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

    /// What a reader gets with `DIV-033` on: every event received before the end, then the
    /// end's error.
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
            "{end:?}: with DIV-033 on, the received head and bytes precede the error"
        );
        assert_eq!(
            owned, uniplex,
            "{end:?}: the owned session matches Uniplex with DIV-033 on"
        );
        let uniplex = response_end_outcome(end, false, false);
        let owned = response_end_outcome(end, true, false);
        assert_eq!(
            owned, uniplex,
            "{end:?}: the owned session matches Uniplex with DIV-033 off"
        );
    }
}

#[test]
fn an_ingress_abort_discards_the_received_response_by_default() {
    // The frozen order: the error reaches the late reader first, carrying the queued head.
    let owned = response_end_outcome(ResponseEnd::LengthBodyEof, true, false);
    assert_eq!(
        owned.read,
        ReadResponse {
            error: Some(HttpErrorCode::TRANSPORT_EOF),
            ..ReadResponse::default()
        }
    );
}

/// A real loopback origin answers a GET with a head and the first bytes of a longer body, then
/// resets the connection (`SO_LINGER` 0). The owned session runs with `DIV-033` on, and its
/// response is read only once its run has ended. Returns the run's error and what was read.
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

/// How the consumer cancels an owned upstream exchange in the `DIV-033` cancel tests, once its
/// response's head and `first` are queued.
#[derive(Clone, Copy, Debug)]
enum OwnedCancel {
    /// `OwnedH1UpstreamRequest::cancel`, with the error `UniplexStreamAbort` selects.
    Request,
    /// The request's cancellation token.
    Token,
    /// `OwnedH1UpstreamHandle::cancel`: the connection, not the request.
    Connection,
}

/// The origin's reads in the `DIV-033` cancel tests: a head and `first` of a 9-byte body,
/// released by `gate`; the connection then stays open.
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

/// As `owned_cancel_outcome` for a `UniplexSession` client whose consumer aborts the stream
/// with `UniplexStreamAbort` (`DIV-023`).
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
        // The runs differ as `DIV-029` records: a Uniplex stream abort leaves the session to
        // finish, while a cancel of an unfinished owned exchange closes its connection.
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
    // The connection's cancel is no cancel of the request: as an ingress abort, it keeps.
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

/// `upstream_config` with `DIV-036`'s `unframed_bodiless_http1_requests` set to `unframed`.
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

/// As `bodiless_post`, an HTTP/1.0 request asking for keep-alive, on which the codec cannot chunk.
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
    // (name, request, its bytes with `DIV-036` on, whether the connection is then reused)
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
            // Unframed, the codec no longer disables keep-alive on a connection that cannot chunk.
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
    // (name, request, its bytes with `DIV-036` off, whether the connection is then reused)
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

/// A finished request's error and progress (`DIV-035`): `request_unprocessed()` and
/// `sequence_number()`.
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
}

enum Submitted {
    Plain(crate::owned_h1::OwnedH1UpstreamSubmission),
    Exclusive(crate::owned_h1::OwnedH1ExclusiveSubmission),
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

/// A stale reused connection's two requests: the first, answered, and the second, submitted
/// around the origin's FIN; every byte written after the first request; the run and the close.
#[derive(Debug, Eq, PartialEq)]
struct StaleOutcome {
    first: RequestProgress,
    second: RequestProgress,
    written_after_first: Vec<u8>,
    run: Option<HttpErrorCode>,
    close: CloseKind,
}

/// A `GET /` answered with `OK_RESPONSE` on a keep-alive connection, then a second `GET /`
/// submitted as `kind` around the origin's FIN, as `fin` orders them.
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
            let (run, (first, second, written_after_first)) =
                futures::join!(session.run(), async {
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

/// Control (passes before and after `DIV-035`'s owned progress): a FIN the driver processes
/// before the next reservation closes admission, so no request is taken and nothing is written.
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
    /// The driver refuses the request itself at commit (a CONNECT).
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

/// A cancellation that lands right after a connection refusal samples its request's cause, as
/// another thread's can, follows the refusal: the request reports the refusal's error and
/// unprocessed, not the cancellation's error with the refusal's flag (`DIV-035`).
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
/// directly or, when `queued`, through a [`RequestFenceQueue`]. `UniplexSession` takes no write
/// queue, so it writes directly either way.
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
                // Leaves the session turns in which it observes the failed read.
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
/// `UniplexSession` does (`reset_after_draining_writes`): the response fails, nothing more is
/// read, the transport resets only once the request bytes already serialized are written (here
/// a head longer than one write, so part of it waits behind the outstanding write), and the
/// read error stays the run's error when the outstanding write then fails too.
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
/// request body. Both sessions serialize the ready body with its head before the
/// read fails, then finish writing those bytes before resetting the transport.
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
/// first, as a peer EOF does: the request is refused unprocessed, nothing is written and the
/// connection resets, as `UniplexSession` fails a request whose head it has not serialized when
/// it processes a read error (`DIV-035`).
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
                    let (run, progress) = futures::join!(session.run(), async {
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

/// A panic in the teardown of a failed read (here the observer's capacity release as the live
/// exchange is cancelled) resets the transport at once, without waiting for the outstanding
/// request write, and `run` resumes it, as a teardown panic in any other abort does.
#[test]
fn a_teardown_panic_on_a_read_error_during_a_request_write_resets_at_once() {
    for queued in [false, true] {
        let outcome = run_on_host(move |context| {
            Box::pin(async move {
                use futures::FutureExt;
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
                    // Leaves the session turns in which it observes the failed read.
                    for _ in 0..8 {
                        context.host().yield_to_io().await;
                    }
                    let held_close = state.borrow().close;
                    fence.open();
                    held_close
                };
                let (run, response, held_close) = futures::join!(
                    std::panic::AssertUnwindSafe(session.run()).catch_unwind(),
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
                let panic = run.expect_err("run resumes the observer's panic");
                (
                    held_close,
                    panic.downcast_ref::<&str>().copied(),
                    response,
                    state.borrow().close,
                    handle.is_finished(),
                )
            })
        });
        assert_eq!(
            outcome,
            (
                CloseKind::Reset,
                Some("observer panicked"),
                Some(HttpErrorCode::TRANSPORT_READ_ERROR),
                CloseKind::Reset,
                true,
            ),
            "queued {queued}"
        );
    }
}
