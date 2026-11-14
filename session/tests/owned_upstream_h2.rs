use std::thread;
use std::thread::ThreadId;

use roxygen_codec::CodecSetting;
use roxygen_core::source_filter::HttpSourceFilter;
use roxygen_core::source_filter::HttpSourceFilterHooks;
use roxygen_core::stream_source::HttpIngressBody;
use roxygen_core::stream_source::HttpResponseReader;
use roxygen_core::stream_source::HttpResponseSourceOwner;
use roxygen_core::stream_source::HttpStreamSourceCallback;
use roxygen_core::stream_source::HttpStreamSourceProducer;
use roxygen_runtime::host::Host;
use roxygen_runtime::tokio_host::ManagedWorkerError;

use super::*;
use crate::experimental::OwnedRequestProgressExt;
use crate::external_host_tests::after_forced_teardown;
use crate::external_host_tests::forced_teardown_of_scope;
use crate::owned_h2::OwnedH2ExclusiveSubmission;
use crate::owned_h2::OwnedH2Options;
use crate::owned_h2::OwnedH2UpstreamHandle;
use crate::owned_h2::OwnedH2UpstreamObserver;
use crate::owned_h2::OwnedH2UpstreamReservation;
use crate::owned_h2::OwnedH2UpstreamSession;
use crate::owned_h2::OwnedH2UpstreamSubmission;
use crate::uniplex::UniplexRunSummary;

#[derive(Default)]
struct UploadProbe {
    reads: Cell<usize>,
    produced: Cell<usize>,
    bytes: Cell<usize>,
    stops: RefCell<Vec<Option<HttpErrorCode>>>,
    drops: Cell<usize>,
    lifecycle: RefCell<Vec<&'static str>>,
    expected_retirement_thread: Cell<Option<ThreadId>>,
}

struct Upload {
    remaining: usize,
    chunk: usize,
    tail: VecDeque<HttpBodyEvent>,
    gate: Option<(usize, Arc<tokio::sync::Notify>)>,
    failure: Option<(Arc<tokio::sync::Notify>, HttpErrorCode)>,
    body_read: Option<LocalSourceFuture<Result<HttpBodyEvent, HttpError>>>,
    probe: Rc<UploadProbe>,
}

impl Upload {
    fn new(remaining: usize, chunk: usize) -> Self {
        Self {
            remaining,
            chunk,
            tail: VecDeque::new(),
            gate: None,
            failure: None,
            body_read: None,
            probe: Rc::new(UploadProbe::default()),
        }
    }

    fn held(self) -> (HttpSourceHolder, Rc<UploadProbe>) {
        let probe = Rc::clone(&self.probe);
        (HttpSourceHolder::new(Box::new(self)), probe)
    }
}

impl HttpSource for Upload {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        panic!("a prepared upload must not reread its header")
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.probe.reads.set(self.probe.reads.get() + 1);
        if let Some(read) = self.body_read.take() {
            return read;
        }
        if let Some((ready, code)) = self.failure.take() {
            return Box::pin(async move {
                ready.notified().await;
                Err(HttpError::new(code))
            });
        }
        let event = if self.remaining != 0 {
            let size = self.remaining.min(self.chunk).min(maximum as usize);
            assert!(size != 0);
            self.remaining -= size;
            HttpBodyEvent::body(
                queue(&vec![b'u'; size]),
                self.remaining == 0 && self.tail.is_empty(),
            )
        } else if let Some(event) = self.tail.pop_front() {
            event
        } else {
            return Box::pin(std::future::pending());
        };
        let gate = if self
            .gate
            .as_ref()
            .is_some_and(|(after, _)| self.probe.bytes.get() >= *after)
        {
            self.gate.take().map(|(_, gate)| gate)
        } else {
            None
        };
        let probe = Rc::clone(&self.probe);
        Box::pin(async move {
            if let Some(gate) = gate {
                gate.notified().await;
            }
            probe.produced.set(probe.produced.get() + 1);
            probe
                .bytes
                .set(probe.bytes.get() + event.as_body().map_or(0, BufQueue::chain_length));
            Ok(event)
        })
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        if let Some(expected) = self.probe.expected_retirement_thread.get() {
            assert_eq!(thread::current().id(), expected);
        }
        self.probe.stops.borrow_mut().push(error);
        self.probe.lifecycle.borrow_mut().push("source.stop");
    }
}

impl Drop for Upload {
    fn drop(&mut self) {
        if let Some(expected) = self.probe.expected_retirement_thread.get() {
            assert_eq!(thread::current().id(), expected);
        }
        self.probe.drops.set(self.probe.drops.get() + 1);
        self.probe.lifecycle.borrow_mut().push("source.drop");
    }
}

struct UploadReadDrop(Rc<UploadProbe>);

impl Drop for UploadReadDrop {
    fn drop(&mut self) {
        self.0.lifecycle.borrow_mut().push("read.drop");
    }
}

#[derive(Default)]
struct AdmissionProbe {
    released: RefCell<Vec<StreamId>>,
    available: Cell<usize>,
    closed: Cell<usize>,
}

impl OwnedH2UpstreamObserver for AdmissionProbe {
    fn capacity_released(&self, stream: StreamId) {
        self.released.borrow_mut().push(stream);
    }

    fn capacity_available(&self) {
        self.available.set(self.available.get() + 1);
    }

    fn admission_closed(&self) {
        self.closed.set(self.closed.get() + 1);
    }
}

fn options(context: WorkerContext, transport: Rc<dyn UniplexTransport>) -> OwnedH2Options {
    let unlimited = RateLimitConfig::new(u64::MAX, Duration::from_millis(100), 0);
    OwnedH2Options {
        context,
        transport,
        io: UniplexConfig {
            read_timeout: Duration::ZERO,
            stream_read_timeout: Duration::ZERO,
            write_timeout: Duration::ZERO,
            drain_request_body_on_drop: false,
            ingress_buffer_limit: 1 << 20,
            ..UniplexConfig::default()
        },
        http2: Http2SessionConfig {
            settings: vec![roxygen_codec::CodecSetting {
                identifier: SettingsId::EnablePush.as_u16(),
                value: 0,
            }],
            double_goaway_drain: false,
            goaway_timeout: Duration::ZERO,
            rate_limits: Http2RateLimitConfig {
                control_frames: unlimited,
                header_frames: unlimited,
                resets: unlimited,
                direct_errors: unlimited,
            },
            ..Http2SessionConfig::default()
        },
    }
}

/// `exact_method_tokens` (`DIV-040`) reads downstream request methods. The owned upstream
/// driver refuses it, as the owned HTTP/1.1 upstream driver does, instead of ignoring it.
#[test]
fn owned_upstream_h2_refuses_exact_method_tokens() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = FixtureTransport::new([]);
            let mut config = options(context, transport);
            config.io.exact_method_tokens = true;
            let error = match OwnedH2UpstreamSession::new(config) {
                Err(error) => error,
                Ok(_) => panic!("exact_method_tokens was silently accepted"),
            };
            assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
            assert_eq!(
                error.message(),
                b"owned H2 upstream does not support exact_method_tokens"
            );
            assert!(state.borrow().writes.is_empty());
        })
    });
}

/// `abort_streams_on_eof` (`DIV-055`) judges a downstream peer's EOF. The owned upstream
/// driver refuses it, as the owned HTTP/1.1 upstream driver refuses `abort_exchanges_on_eof`,
/// instead of ignoring it.
#[test]
fn owned_upstream_h2_refuses_abort_streams_on_eof() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = FixtureTransport::new([]);
            let mut config = options(context, transport);
            config.http2.abort_streams_on_eof = true;
            let error = match OwnedH2UpstreamSession::new(config) {
                Err(error) => error,
                Ok(_) => panic!("abort_streams_on_eof was silently accepted"),
            };
            assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
            assert!(state.borrow().writes.is_empty());
        })
    });
}

/// The owned downstream driver applies `verbatim_http2_header_values` (`DIV-041`); the owned
/// upstream driver still refuses it instead of ignoring it.
#[test]
fn owned_upstream_h2_refuses_verbatim_http2_header_values() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = FixtureTransport::new([]);
            let mut config = options(context, transport);
            config.io.verbatim_http2_header_values = true;
            let error = match OwnedH2UpstreamSession::new(config) {
                Err(error) => error,
                Ok(_) => panic!("verbatim_http2_header_values was silently accepted"),
            };
            assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
            assert_eq!(
                error.message(),
                b"owned H2 upstream does not support verbatim_http2_header_values"
            );
            assert!(state.borrow().writes.is_empty());
        })
    });
}

fn request_head(method: HttpMethod, eom: bool, length: Option<usize>) -> HttpHeaderEvent {
    let mut headers = committed_test_headers(b"/owned-upstream", eom);
    headers.headers_mut().set_method(method).unwrap();
    if let Some(length) = length {
        headers
            .headers_mut()
            .headers_mut()
            .set(HttpHeaderCode::ContentLength, length.to_string().as_bytes());
    }
    headers
}

fn response_head(
    peer: &mut Http2Codec,
    stream: StreamId,
    status: u16,
    eom: bool,
    length: Option<usize>,
) -> Vec<u8> {
    let mut message = HttpMessage::new();
    message.set_status_code(status).unwrap();
    message.set_http_version(1, 1);
    if let Some(length) = length {
        message
            .headers_mut()
            .set(HttpHeaderCode::ContentLength, length.to_string().as_bytes());
    }
    eligibility_frame(
        peer,
        EgressFrame::Headers {
            stream,
            message: &message,
            end_stream: eom,
            extra_headers: None,
        },
    )
}

async fn drive(
    session: OwnedH2UpstreamSession,
    handle: &OwnedH2UpstreamHandle,
    gates: &[Arc<tokio::sync::Notify>],
    operation: impl Future<Output = ()>,
) -> Result<UniplexRunSummary, HttpError> {
    let exercise = async {
        let outcome =
            eligibility_catch_unwind(tokio::time::timeout(Duration::from_secs(5), operation)).await;
        gates.iter().for_each(|gate| gate.notify_one());
        handle.cancel();
        outcome
    };
    let (run, outcome) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(session.run(), exercise)
    })
    .await
    .expect("upstream run joins after releasing fixture gates");
    match outcome {
        Ok(result) => result.expect("upstream fixture makes progress"),
        Err(panic) => resume_unwind(panic),
    }
    run
}

fn cancelled(run: Result<UniplexRunSummary, HttpError>) {
    assert_eq!(
        run.expect_err("fixture ends by explicit cancellation")
            .code(),
        HttpErrorCode::CORO_CANCELLED
    );
}

/// `DIV-043`: a worker whose teardown deadline has passed destroys an owned HTTP/2 upstream
/// with a request in flight before its native I/O cleanup joined. That destruction is an
/// abandonment: the driver still resets the transport, and the worker reports a clean overrun
/// instead of a panic.
#[test]
fn a_forced_worker_teardown_abandons_an_owned_h2_upstream_mid_request() {
    let (inspected_tx, inspected_rx) = std::sync::mpsc::channel();
    let completion = forced_teardown_of_scope(move |context| {
        let (transport, state) = FixtureTransport::new([]);
        let (session, handle) = OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
        let submission = handle.reserve_request().unwrap().submit(
            request_head(HttpMethod::Get, true, None),
            HttpSourceHolder::empty(),
            CancellationToken::never(),
        );
        after_forced_teardown(move || {
            inspected_tx
                .send((state.borrow().close, header_streams(&state)))
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
    let (close, header_streams) = inspected_rx.recv().unwrap();
    assert_eq!(
        header_streams,
        [StreamId::new(1)],
        "the request was in flight"
    );
    assert_eq!(
        completion,
        Err(ManagedWorkerError::TeardownDeadlineExceeded),
        "the forced destruction of the unjoined driver is a clean overrun"
    );
    assert_eq!(
        close,
        FixtureClose::Reset,
        "the abandoned driver reset its transport"
    );
}

#[test]
fn producer_eom_releases_one_hundred_unread_responses_and_settings_still_gate_admission() {
    run_on_worker(|context| {
        Box::pin(async move {
            let respond = Arc::new(tokio::sync::Notify::new());
            let zero = Arc::new(tokio::sync::Notify::new());
            let reopen = Arc::new(tokio::sync::Notify::new());
            let finish = Arc::new(tokio::sync::Notify::new());
            let (transport, wire) = FixtureTransport::new([
                FixtureRead::Data(server_settings_bytes(100)),
                FixtureRead::WaitData(
                    Arc::clone(&respond),
                    server_empty_responses_bytes(
                        (0..100).map(|index| StreamId::new(1 + 2 * index)),
                    ),
                ),
                FixtureRead::WaitData(Arc::clone(&zero), server_settings_bytes(0)),
                FixtureRead::WaitData(Arc::clone(&reopen), server_settings_bytes(100)),
                FixtureRead::WaitData(
                    Arc::clone(&finish),
                    server_empty_responses_bytes([StreamId::new(201)]),
                ),
            ]);
            let (session, handle) =
                OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
            let observer = Rc::new(AdmissionProbe::default());
            handle.set_observer(observer.clone()).unwrap();
            drop(handle.reserve_request().unwrap());
            assert_eq!(handle.available_capacity(), 100);
            let submissions: Vec<_> = (0..100)
                .map(|_| {
                    handle.reserve_request().unwrap().submit(
                        request_head(HttpMethod::Get, true, None),
                        HttpSourceHolder::empty(),
                        CancellationToken::never(),
                    )
                })
                .collect();
            assert_eq!(handle.available_capacity(), 0);
            assert!(handle.reserve_request().is_err());
            cancelled(
                drive(
                    session,
                    &handle,
                    &[
                        Arc::clone(&respond),
                        Arc::clone(&zero),
                        Arc::clone(&reopen),
                        Arc::clone(&finish),
                    ],
                    async {
                        let mut responses = Vec::new();
                        for submission in submissions {
                            responses.push(submission.await.unwrap().into_source_parts());
                        }
                        complete_credit_wait(|| {
                            decode_client_frames(&wire.borrow().writes.concat())
                                .header_streams
                                .len()
                                == 100
                        })
                        .await;
                        respond.notify_one();
                        for response in &responses {
                            response
                                .1
                                .ingress_completion()
                                .unwrap()
                                .wait()
                                .await
                                .unwrap();
                            complete_credit_wait(|| response.2.capacity_released()).await;
                            assert!(response.1.readable());
                        }
                        assert_eq!(handle.available_capacity(), 100);
                        assert_eq!(observer.released.borrow().len(), 100);
                        assert!(!handle.is_idle());
                        zero.notify_one();
                        complete_credit_wait(|| handle.available_capacity() == 0).await;
                        assert!(handle.reserve_request().is_err());
                        reopen.notify_one();
                        complete_credit_wait(|| handle.available_capacity() == 100).await;
                        let next = handle
                            .reserve_request()
                            .unwrap()
                            .submit(
                                request_head(HttpMethod::Get, true, None),
                                HttpSourceHolder::empty(),
                                CancellationToken::never(),
                            )
                            .await
                            .unwrap()
                            .into_source_parts();
                        assert_eq!(next.0, StreamId::new(201));
                        finish.notify_one();
                        next.1.ingress_completion().unwrap().wait().await.unwrap();
                        complete_credit_wait(|| next.2.capacity_released()).await;
                        assert_eq!(handle.available_capacity(), 100);
                        let sticky = next.2.clone();
                        drop((responses, next));
                        complete_credit_wait(|| handle.is_idle()).await;
                        assert!(sticky.capacity_released());
                        assert_eq!(observer.released.borrow().len(), 101);
                        let decoded = decode_client_frames(&wire.borrow().writes.concat());
                        assert_eq!(decoded.header_streams.len(), 101);
                        assert_eq!(decoded.header_streams[0], StreamId::new(1));
                        assert!(decoded.resets.is_empty());
                        assert!(decoded.errors.is_empty());
                    },
                )
                .await,
            );
        })
    });
}

#[test]
fn an_early_response_preserves_upload_progress_and_holds_capacity_until_request_eom() {
    run_on_worker(|context| {
        Box::pin(async move {
            let respond = Arc::new(tokio::sync::Notify::new());
            let upload_ready = Arc::new(tokio::sync::Notify::new());
            let (transport, wire) = FixtureTransport::new([
                FixtureRead::Data(server_settings_bytes(1)),
                FixtureRead::WaitData(
                    Arc::clone(&respond),
                    server_empty_responses_bytes([StreamId::new(1)]),
                ),
            ]);
            let mut config = options(context, transport);
            config.http2.max_concurrent_outgoing_streams = 1;
            let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
            let mut upload = Upload::new(1, 1);
            upload.gate = Some((0, Arc::clone(&upload_ready)));
            let (upload, probe) = upload.held();
            let submission = handle.reserve_request().unwrap().submit(
                request_head(HttpMethod::Post, false, Some(1)),
                upload,
                CancellationToken::never(),
            );
            cancelled(
                drive(
                    session,
                    &handle,
                    &[Arc::clone(&respond), Arc::clone(&upload_ready)],
                    async {
                        let response = submission.await.unwrap().into_source_parts();
                        respond.notify_one();
                        response
                            .1
                            .ingress_completion()
                            .unwrap()
                            .wait()
                            .await
                            .unwrap();
                        assert!(!response.2.capacity_released());
                        assert_eq!(handle.available_capacity(), 0);
                        assert!(handle.reserve_request().is_err());
                        assert_eq!(probe.bytes.get(), 0);
                        assert_eq!(probe.drops.get(), 0);
                        assert!(probe.stops.borrow().is_empty());
                        upload_ready.notify_one();
                        complete_credit_wait(|| response.2.capacity_released()).await;
                        assert_eq!(probe.bytes.get(), 1);
                        assert_eq!(probe.drops.get(), 1);
                        assert!(probe.stops.borrow().is_empty());
                        assert_eq!(handle.available_capacity(), 1);
                        assert!(response.1.readable());
                        drop(response);
                        complete_credit_wait(|| handle.is_idle()).await;
                        let decoded = decode_client_frames(&wire.borrow().writes.concat());
                        assert_eq!(decoded.body_data, b"u");
                        assert!(decoded.resets.is_empty());
                    },
                )
                .await,
            );
        })
    });
}

async fn upload_pending_event_case(context: WorkerContext, empty_tail: bool) {
    let stream = StreamId::new(1);
    let mut peer = Http2Codec::new(TransportDirection::Downstream);
    let initial = eligibility_frame(
        &mut peer,
        EgressFrame::Settings {
            settings: &[
                roxygen_codec::CodecSetting {
                    identifier: SettingsId::InitialWindowSize.as_u16(),
                    value: 0,
                },
                roxygen_codec::CodecSetting {
                    identifier: SettingsId::MaxConcurrentStreams.as_u16(),
                    value: 1,
                },
            ],
        },
    );
    let ping = Arc::new(tokio::sync::Notify::new());
    let credit = Arc::new(tokio::sync::Notify::new());
    let respond = Arc::new(tokio::sync::Notify::new());
    let release_write = Arc::new(tokio::sync::Notify::new());
    let size = if empty_tail { 1 } else { 65_536 };
    let mut update = eligibility_frame(
        &mut peer,
        EgressFrame::WindowUpdate {
            stream,
            amount: size as u32,
        },
    );
    update.extend(eligibility_frame(
        &mut peer,
        EgressFrame::WindowUpdate {
            stream: StreamId::UNSPECIFIED,
            amount: 1,
        },
    ));
    let (transport, wire) = FixtureTransport::new([
        FixtureRead::Data(initial),
        FixtureRead::WaitData(
            Arc::clone(&ping),
            eligibility_frame(&mut peer, EgressFrame::PingRequest { data: Some(91) }),
        ),
        FixtureRead::WaitData(Arc::clone(&credit), update),
        FixtureRead::WaitData(
            Arc::clone(&respond),
            response_head(&mut peer, stream, 200, true, Some(0)),
        ),
    ]);
    wire.borrow_mut().write_gate = Some(Arc::clone(&release_write));
    let (session, handle) = OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
    let mut upload = Upload::new(size, 1024);
    if empty_tail {
        for ordinal in 0..300 {
            upload.tail.push_back(if ordinal % 2 == 0 {
                HttpBodyEvent::padding(0, false)
            } else {
                HttpBodyEvent::body(BufQueue::new(0), false)
            });
        }
        upload
            .tail
            .push_back(HttpBodyEvent::body(BufQueue::new(0), true));
    }
    let (upload, probe) = upload.held();
    cancelled(
        drive(
            session,
            &handle,
            &[
                Arc::clone(&ping),
                Arc::clone(&credit),
                Arc::clone(&respond),
                Arc::clone(&release_write),
            ],
            async {
                complete_credit_wait(|| handle.available_capacity() == 1).await;
                let response = handle
                    .reserve_request()
                    .unwrap()
                    .submit(
                        request_head(HttpMethod::Post, false, Some(size)),
                        upload,
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap()
                    .into_source_parts();
                complete_credit_wait(|| {
                    probe.produced.get() >= 1 && wire.borrow().write_gate_pending.is_some()
                })
                .await;
                for _ in 0..4 {
                    tokio::task::yield_now().await;
                }
                assert_eq!(probe.reads.get(), 1);
                assert_eq!(probe.produced.get(), 1);
                assert_eq!(probe.bytes.get(), if empty_tail { 1 } else { 1024 });
                assert_eq!(probe.drops.get(), 0);
                assert!(wire.borrow().writes.is_empty());
                release_write.notify_one();
                ping.notify_one();
                complete_credit_wait(|| {
                    decode_client_frames(&wire.borrow().writes.concat()).ping_replies == [91]
                })
                .await;
                assert_eq!(probe.reads.get(), 1);
                assert_eq!(probe.produced.get(), 1);
                assert_eq!(
                    decode_client_frames(&wire.borrow().writes.concat()).body_bytes,
                    0
                );
                assert!(!response.2.capacity_released());
                credit.notify_one();
                complete_credit_wait(|| complete_credit_wire(true, &wire).completed == [stream])
                    .await;
                assert_eq!(probe.produced.get(), if empty_tail { 302 } else { 64 });
                assert_eq!(probe.bytes.get(), size);
                assert_eq!(probe.drops.get(), 1);
                assert!(probe.stops.borrow().is_empty());
                respond.notify_one();
                response
                    .1
                    .ingress_completion()
                    .unwrap()
                    .wait()
                    .await
                    .unwrap();
                complete_credit_wait(|| response.2.capacity_released()).await;
                drop(response);
                complete_credit_wait(|| handle.is_idle()).await;
                let decoded = decode_client_frames(&wire.borrow().writes.concat());
                assert_eq!(decoded.body_data, vec![b'u'; size]);
                assert!(decoded.resets.is_empty());
                assert!(decoded.errors.is_empty());
            },
        )
        .await,
    );
}

#[test]
fn held_write_and_zero_credit_hold_one_upload_event() {
    run_on_worker(|context| Box::pin(upload_pending_event_case(context, false)));
}

#[test]
fn held_write_and_zero_credit_do_not_read_past_one_upload_event_into_empty_tail() {
    run_on_worker(|context| Box::pin(upload_pending_event_case(context, true)));
}

async fn partial_upload_case(context: WorkerContext, cancel_pending: bool) {
    let stream = StreamId::new(1);
    let sibling_stream = StreamId::new(3);
    let mut peer = Http2Codec::new(TransportDirection::Downstream);
    let initial = eligibility_frame(
        &mut peer,
        EgressFrame::Settings {
            settings: &[
                CodecSetting {
                    identifier: SettingsId::InitialWindowSize.as_u16(),
                    value: 1,
                },
                CodecSetting {
                    identifier: SettingsId::MaxConcurrentStreams.as_u16(),
                    value: 2,
                },
            ],
        },
    );
    let ping = Arc::new(tokio::sync::Notify::new());
    let first_credit = Arc::new(tokio::sync::Notify::new());
    let last_credit = Arc::new(tokio::sync::Notify::new());
    let respond = Arc::new(tokio::sync::Notify::new());
    let mut reads = vec![
        FixtureRead::Data(initial),
        FixtureRead::WaitData(
            Arc::clone(&ping),
            eligibility_frame(&mut peer, EgressFrame::PingRequest { data: Some(92) }),
        ),
    ];
    for (ready, amount) in [(&first_credit, 1), (&last_credit, 6)] {
        reads.push(FixtureRead::WaitData(
            Arc::clone(ready),
            eligibility_frame(&mut peer, EgressFrame::WindowUpdate { stream, amount }),
        ));
    }
    let reply_streams = if cancel_pending {
        vec![sibling_stream]
    } else {
        vec![stream, sibling_stream]
    };
    let replies = reply_streams
        .into_iter()
        .flat_map(|stream| response_head(&mut peer, stream, 200, true, Some(0)))
        .collect();
    reads.push(FixtureRead::WaitData(Arc::clone(&respond), replies));
    let (transport, wire) = FixtureTransport::new(reads);
    let (session, handle) = OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
    let byte_events = Rc::new(ByteEventProbe::default());
    let observed_events = Rc::clone(&byte_events);
    let source_wake = Rc::new(RefCell::new(None));
    let observed_wake = Rc::clone(&source_wake);
    let mut upload = Upload::new(4, 4);
    let observed = Rc::clone(&upload.probe);
    upload.body_read = Some(Box::pin(poll_fn(move |context| {
        *observed_wake.borrow_mut() = Some(context.waker().clone());
        observed.produced.set(observed.produced.get() + 1);
        observed.bytes.set(observed.bytes.get() + 4);
        let mut event = HttpBodyEvent::body(queue(b"uuuu"), false);
        event
            .byte_event_registrations_mut()
            .push(HttpByteEventRegistration::new(
                Some(StreamId::new(7)),
                ByteEventType::TransportWrite.bit(),
                &observed_events,
            ));
        Poll::Ready(Ok(event))
    })));
    let (upload, probe) = upload.held();
    cancelled(
        drive(
            session,
            &handle,
            &[
                Arc::clone(&ping),
                Arc::clone(&first_credit),
                Arc::clone(&last_credit),
                Arc::clone(&respond),
            ],
            async {
                complete_credit_wait(|| handle.available_capacity() == 2).await;
                let mut response = handle
                    .reserve_request()
                    .unwrap()
                    .submit(
                        request_head(HttpMethod::Post, false, Some(8)),
                        upload,
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap()
                    .into_source_parts();
                complete_credit_wait(|| complete_credit_wire(true, &wire).frames.body_bytes == 1)
                    .await;
                let (sibling_upload, sibling_probe) = Upload::new(1, 1).held();
                let sibling = handle
                    .reserve_request()
                    .unwrap()
                    .submit(
                        request_head(HttpMethod::Post, false, Some(1)),
                        sibling_upload,
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap()
                    .into_source_parts();
                complete_credit_wait(|| {
                    complete_credit_wire(true, &wire).completed == [sibling_stream]
                })
                .await;
                for _ in 0..4 {
                    source_wake.borrow().as_ref().unwrap().wake_by_ref();
                    tokio::task::yield_now().await;
                }
                ping.notify_one();
                complete_credit_wait(|| {
                    complete_credit_wire(true, &wire).frames.ping_replies == [92]
                })
                .await;
                assert_eq!(probe.reads.get(), 1);
                assert_eq!(probe.produced.get(), 1);
                assert_eq!(probe.bytes.get(), 4);
                assert_eq!(probe.drops.get(), 0);
                assert_eq!(sibling_probe.drops.get(), 1);
                assert_eq!(complete_credit_wire(true, &wire).frames.body_bytes, 2);
                assert!(!response.2.capacity_released());
                first_credit.notify_one();
                complete_credit_wait(|| complete_credit_wire(true, &wire).frames.body_bytes == 3)
                    .await;
                for _ in 0..4 {
                    source_wake.borrow().as_ref().unwrap().wake_by_ref();
                    tokio::task::yield_now().await;
                }
                assert_eq!(probe.reads.get(), 1);
                assert_eq!(probe.produced.get(), 1);
                assert_eq!(probe.bytes.get(), 4);
                assert_eq!(probe.drops.get(), 0);
                assert!(probe.stops.borrow().is_empty());
                assert!(byte_events.delivered.borrow().is_empty());
                assert!(byte_events.cancelled.borrow().is_empty());
                if cancel_pending {
                    response
                        .2
                        .cancel(HttpError::new(HttpErrorCode::CORO_CANCELLED));
                    let error = response
                        .1
                        .read_header_event()
                        .await
                        .err()
                        .expect("cancellation retires the retained upload remainder");
                    assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                    drop(response);
                    last_credit.notify_one();
                    respond.notify_one();
                    sibling
                        .1
                        .ingress_completion()
                        .unwrap()
                        .wait()
                        .await
                        .unwrap();
                    complete_credit_wait(|| sibling.2.capacity_released()).await;
                    drop(sibling);
                    complete_credit_wait(|| handle.is_idle()).await;
                    assert_eq!(probe.reads.get(), 1);
                    assert_eq!(probe.produced.get(), 1);
                    assert_eq!(probe.bytes.get(), 4);
                    assert_eq!(probe.drops.get(), 1);
                    assert_eq!(
                        probe.stops.borrow().as_slice(),
                        &[Some(HttpErrorCode::CORO_CANCELLED)]
                    );
                    assert!(byte_events.delivered.borrow().is_empty());
                    let events = byte_events.cancelled.borrow();
                    assert_eq!(events.len(), 1);
                    assert_eq!(events[0].0.stream_id, Some(StreamId::new(7)));
                    assert_eq!(events[0].0.event_type, ByteEventType::TransportWrite);
                    assert_eq!(events[0].1, HttpErrorCode::CORO_CANCELLED);
                    let decoded = complete_credit_wire(true, &wire);
                    assert_eq!(decoded.frames.body_data, b"uuu");
                    assert_eq!(decoded.completed, [sibling_stream]);
                    assert_eq!(decoded.frames.resets, [(stream, H2ErrorCode::CANCEL)]);
                    assert!(decoded.frames.errors.is_empty());
                    assert_eq!(handle.available_capacity(), 2);
                    return;
                }
                last_credit.notify_one();
                complete_credit_wait(|| {
                    complete_credit_wire(true, &wire).completed == [sibling_stream, stream]
                })
                .await;
                assert_eq!(probe.reads.get(), 2);
                assert_eq!(probe.produced.get(), 2);
                assert_eq!(probe.bytes.get(), 8);
                assert_eq!(probe.drops.get(), 1);
                assert!(probe.stops.borrow().is_empty());
                respond.notify_one();
                for response in [response, sibling] {
                    response
                        .1
                        .ingress_completion()
                        .unwrap()
                        .wait()
                        .await
                        .unwrap();
                    complete_credit_wait(|| response.2.capacity_released()).await;
                }
                complete_credit_wait(|| handle.is_idle()).await;
                assert_eq!(handle.available_capacity(), 2);
                assert_eq!(byte_events.delivered.borrow().len(), 1);
                assert!(byte_events.cancelled.borrow().is_empty());
                let decoded = complete_credit_wire(true, &wire);
                assert_eq!(decoded.frames.body_data, b"uuuuuuuuu");
                assert_eq!(decoded.completed, [sibling_stream, stream]);
                assert!(decoded.frames.resets.is_empty());
                assert!(decoded.frames.errors.is_empty());
            },
        )
        .await,
    );
    assert_eq!(
        byte_events.cancelled.borrow().len(),
        usize::from(cancel_pending)
    );
    assert_eq!(
        byte_events.delivered.borrow().len(),
        usize::from(!cancel_pending)
    );
}

#[test]
fn partial_upload_retains_one_event_across_wakes_and_allows_sibling_progress() {
    run_on_worker(|context| Box::pin(partial_upload_case(context, false)));
}

#[test]
fn cancellation_retires_a_split_upload_registration_once_with_its_original_stream() {
    run_on_worker(|context| Box::pin(partial_upload_case(context, true)));
}

#[test]
fn no_error_response_abandonment_cancels_an_unfinished_upload_without_draining() {
    run_on_worker(|context| {
        Box::pin(async move {
            let respond = Arc::new(tokio::sync::Notify::new());
            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            let (transport, wire) = FixtureTransport::new([
                FixtureRead::Data(server_settings_bytes(1)),
                FixtureRead::WaitData(
                    Arc::clone(&respond),
                    response_head(&mut peer, StreamId::new(1), 200, false, Some(1)),
                ),
            ]);
            let mut config = options(context, transport);
            config.http2.max_concurrent_outgoing_streams = 1;
            let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
            let (upload, probe) = Upload::new(0, 1).held();
            let submission = handle.reserve_request().unwrap().submit(
                request_head(HttpMethod::Post, false, None),
                upload,
                CancellationToken::never(),
            );
            cancelled(
                drive(session, &handle, &[Arc::clone(&respond)], async {
                    let mut response = submission.await.unwrap().into_source_parts();
                    respond.notify_one();
                    let header = response.1.read_header_event().await.unwrap();
                    assert!(!header.eom());
                    response.1.stop_reading(Some(HttpErrorCode::NO_ERROR));
                    complete_credit_wait(|| probe.drops.get() == 1 && handle.is_idle()).await;
                    assert_eq!(
                        probe.stops.borrow().as_slice(),
                        &[Some(HttpErrorCode::CANCEL)]
                    );
                    assert_eq!(handle.available_capacity(), 1);
                    assert!(response.2.capacity_released());
                    assert_eq!(
                        decode_client_frames(&wire.borrow().writes.concat()).resets,
                        [(StreamId::new(1), H2ErrorCode::CANCEL)]
                    );
                    drop(response);
                })
                .await,
            );
        })
    });
}

#[test]
fn submission_drop_before_and_after_codec_commit_retires_each_upload_once() {
    for (committed, interest) in [(false, false), (true, false), (false, true), (true, true)] {
        run_on_worker(move |context| {
            Box::pin(async move {
                let (transport, wire) =
                    FixtureTransport::new([FixtureRead::Data(server_settings_bytes(1))]);
                let mut config = options(context, transport);
                config.http2.max_concurrent_outgoing_streams = 1;
                let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
                let (upload, probe) = Upload::new(0, 1).held();
                let encoded = Rc::new(Cell::new(false));
                let mut header = request_head(HttpMethod::Post, false, None);
                let observed = Rc::clone(&encoded);
                header.set_egress_headers_callback(move |_| observed.set(true));
                let reservation = handle.reserve_request().unwrap();
                let mut submission: Option<Box<dyn std::any::Any>> = Some(if interest {
                    Box::new(reservation.submit_with_response_interest(
                        header,
                        upload,
                        CancellationToken::never(),
                        None,
                    ))
                } else {
                    Box::new(reservation.submit(header, upload, CancellationToken::never()))
                });
                // A committed response-interest submission is dropped as an ordinary
                // caller's response owner is, which cancels with CANCEL.
                let stop = if committed && interest {
                    HttpErrorCode::CANCEL
                } else {
                    HttpErrorCode::CORO_CANCELLED
                };
                if !committed {
                    drop(submission.take());
                }
                cancelled(
                    drive(session, &handle, &[], async {
                        if committed {
                            complete_credit_wait(|| encoded.get()).await;
                            drop(submission.take());
                        }
                        complete_credit_wait(|| probe.drops.get() == 1 && handle.is_idle()).await;
                        assert_eq!(probe.stops.borrow().as_slice(), &[Some(stop)]);
                        assert_eq!(handle.available_capacity(), 1);
                        let decoded = decode_client_frames(&wire.borrow().writes.concat());
                        if committed {
                            assert_eq!(decoded.header_streams, [StreamId::new(1)]);
                            assert_eq!(decoded.resets, [(StreamId::new(1), H2ErrorCode::CANCEL)]);
                        } else {
                            assert!(!encoded.get());
                            assert_eq!(probe.reads.get(), 0);
                            assert!(decoded.header_streams.is_empty());
                            assert!(decoded.resets.is_empty());
                        }
                        assert_eq!(decoded.body_bytes, 0);
                        assert!(decoded.errors.is_empty());
                    })
                    .await,
                );
            })
        });
    }
}

#[test]
fn initially_cancelled_submission_retires_on_owner_without_request_bytes() {
    run_on_worker(|context| {
        Box::pin(async move {
            let owner = thread::current().id();
            let release_read = Arc::new(tokio::sync::Notify::new());
            let (transport, wire) = FixtureTransport::new([FixtureRead::WaitDataEof(
                Arc::clone(&release_read),
                Vec::new(),
            )]);
            let mut config = options(context, transport);
            config.http2.max_concurrent_outgoing_streams = 1;
            let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
            let observer = Rc::new(AdmissionProbe::default());
            handle.set_observer(observer.clone()).unwrap();
            let (upload, probe) = Upload::new(1, 1).held();
            probe.expected_retirement_thread.set(Some(owner));
            let encoded = Rc::new(Cell::new(false));
            let observed = Rc::clone(&encoded);
            let mut header = request_head(HttpMethod::Post, false, Some(1));
            header.set_egress_headers_callback(move |_| observed.set(true));
            let cancellation = CancellationSource::new();
            assert!(cancellation.request_cancellation());
            let submission =
                handle
                    .reserve_request()
                    .unwrap()
                    .submit(header, upload, cancellation.token());
            assert_eq!(handle.available_capacity(), 0);
            assert!(probe.lifecycle.borrow().is_empty());

            let assert_retired = || {
                assert_eq!(probe.reads.get(), 0);
                assert_eq!(probe.produced.get(), 0);
                assert_eq!(probe.bytes.get(), 0);
                assert_eq!(
                    probe.stops.borrow().as_slice(),
                    &[Some(HttpErrorCode::CORO_CANCELLED)]
                );
                assert_eq!(probe.drops.get(), 1);
                assert_eq!(
                    probe.lifecycle.borrow().as_slice(),
                    &["source.stop", "source.drop"]
                );
                assert!(!encoded.get());
            };
            cancelled(
                drive(session, &handle, &[Arc::clone(&release_read)], async {
                    let error = submission
                        .await
                        .err()
                        .expect("initial cancellation rejects request commitment");
                    assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                    complete_credit_wait(|| handle.is_idle() && wire.borrow().reads.is_empty())
                        .await;
                    assert_retired();
                    assert_eq!(handle.available_capacity(), 1);
                    assert_eq!(observer.available.get(), 1);
                    assert!(observer.released.borrow().is_empty());
                })
                .await,
            );
            assert_retired();
            assert!(wire.borrow().transport_dropped);
            let written = wire.borrow().writes.concat();
            assert!(written.len() > CONNECTION_PREFACE.len());
            let decoded = decode_client_frames(&written);
            assert!(decoded.header_streams.is_empty());
            assert!(decoded.body_streams.is_empty());
            assert_eq!(decoded.body_bytes, 0);
            assert!(decoded.resets.is_empty());
            assert!(decoded.errors.is_empty());
        })
    });
}

#[test]
fn shared_token_queued_submissions_keep_survivor_wake_after_first_unregisters() {
    use std::sync::atomic::AtomicBool;
    use std::task::Context;
    use std::task::Wake;
    use std::task::Waker;

    struct RecordingWake(AtomicBool);

    impl Wake for RecordingWake {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    run_on_worker(|context| {
        Box::pin(async move {
            let owner = thread::current().id();
            let release_write = Arc::new(tokio::sync::Notify::new());
            let (transport, wire) = FixtureTransport::new([]);
            wire.borrow_mut().write_gate = Some(Arc::clone(&release_write));
            let mut config = options(context, transport);
            config.http2.max_concurrent_outgoing_streams = 2;
            config.io.maximum_write_size = FRAME_HEADER_SIZE + 1;
            let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
            let cancellation = Arc::new(CancellationSource::new());
            let token = cancellation.token();
            let (first_upload, first_probe) = Upload::new(1, 1).held();
            let (survivor_upload, survivor_probe) = Upload::new(1, 1).held();
            for probe in [&first_probe, &survivor_probe] {
                probe.expected_retirement_thread.set(Some(owner));
            }
            let first_encoded = Rc::new(Cell::new(false));
            let survivor_encoded = Rc::new(Cell::new(false));
            let snapshot = |probe: &UploadProbe| {
                (
                    probe.reads.get(),
                    probe.produced.get(),
                    probe.bytes.get(),
                    probe.stops.borrow().clone(),
                    probe.drops.get(),
                    probe.lifecycle.borrow().clone(),
                )
            };
            let retired = (
                0,
                0,
                0,
                vec![Some(HttpErrorCode::CORO_CANCELLED)],
                1,
                vec!["source.stop", "source.drop"],
            );
            let wake = Arc::new(RecordingWake(AtomicBool::new(false)));
            let waker = Waker::from(Arc::clone(&wake));
            let mut run = Box::pin(session.run());
            let mut finished = None;
            let outcome = catch_unwind(AssertUnwindSafe(|| {
                let mut poll_to_quiescence = || {
                    for _ in 0..16 {
                        wake.0.store(false, Ordering::SeqCst);
                        if let Poll::Ready(result) =
                            run.as_mut().poll(&mut Context::from_waker(&waker))
                        {
                            finished = Some(result);
                            panic!("the pending native read keeps the session running");
                        }
                        if !wake.0.load(Ordering::SeqCst) {
                            return;
                        }
                    }
                    panic!("held I/O must allow incidental readiness to settle");
                };
                poll_to_quiescence();
                assert_eq!(
                    wire.borrow().write_gate_pending,
                    Some(FRAME_HEADER_SIZE + 1)
                );

                let mut header = request_head(HttpMethod::Post, false, None);
                let observed = Rc::clone(&first_encoded);
                header.set_egress_headers_callback(move |_| observed.set(true));
                let first =
                    handle
                        .reserve_request()
                        .unwrap()
                        .submit(header, first_upload, token.clone());
                let mut header = request_head(HttpMethod::Post, false, None);
                let observed = Rc::clone(&survivor_encoded);
                header.set_egress_headers_callback(move |_| observed.set(true));
                let mut survivor = handle.reserve_request().unwrap().submit(
                    header,
                    survivor_upload,
                    token.clone(),
                );
                assert_eq!(handle.available_capacity(), 0);
                assert!(!first_encoded.get() && !survivor_encoded.get());
                assert_eq!(first_probe.reads.get(), 0);
                assert_eq!(survivor_probe.reads.get(), 0);

                drop(first);
                poll_to_quiescence();
                let first_retired = snapshot(&first_probe);
                assert_eq!(first_retired, retired);
                assert!(!token.is_cancellation_requested());
                assert_eq!(handle.available_capacity(), 1);
                assert!(
                    Pin::new(&mut survivor)
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending()
                );
                assert_eq!(
                    snapshot(&survivor_probe),
                    (0, 0, 0, Vec::new(), 0, Vec::new())
                );
                assert!(!first_encoded.get() && !survivor_encoded.get());

                poll_to_quiescence();
                wake.0.store(false, Ordering::SeqCst);
                let cancel = Arc::clone(&cancellation);
                assert!(
                    thread::spawn(move || cancel.request_cancellation())
                        .join()
                        .expect("foreign-thread cancellation joins")
                );
                assert!(
                    wake.0.load(Ordering::SeqCst),
                    "the surviving registration must wake the owner before any repoll or write release"
                );
                assert_eq!(
                    snapshot(&survivor_probe),
                    (0, 0, 0, Vec::new(), 0, Vec::new())
                );

                poll_to_quiescence();
                let Poll::Ready(Err(error)) =
                    Pin::new(&mut survivor).poll(&mut Context::from_waker(&waker))
                else {
                    panic!("the surviving queued request observes token cancellation");
                };
                assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                assert_eq!(snapshot(&survivor_probe), retired);
                assert_eq!(snapshot(&first_probe), first_retired);
                assert_eq!(handle.available_capacity(), 2);
                assert!(!first_encoded.get() && !survivor_encoded.get());
                assert!(wire.borrow().writes.is_empty());

                release_write.notify_one();
                poll_to_quiescence();
                assert!(handle.is_idle());
                let written = wire.borrow().writes.concat();
                assert!(written.len() > CONNECTION_PREFACE.len());
                let decoded = decode_client_frames(&written);
                assert!(decoded.header_streams.is_empty());
                assert_eq!(decoded.body_bytes, 0);
                assert!(decoded.resets.is_empty());
                assert!(decoded.errors.is_empty());
            }));
            release_write.notify_one();
            handle.cancel();
            let result = match finished {
                Some(result) => result,
                None => tokio::time::timeout(Duration::from_secs(5), run.as_mut())
                    .await
                    .expect("queued cancellation joins both native operations"),
            };
            drop(run);
            if let Err(payload) = outcome {
                resume_unwind(payload);
            }
            cancelled(result);
            assert_eq!(snapshot(&first_probe), retired);
            assert_eq!(snapshot(&survivor_probe), retired);
            assert!(wire.borrow().transport_dropped);
        })
    });
}

#[test]
fn released_capacity_does_not_replace_raw_response_or_native_write_retirement() {
    for read_first in [false, true] {
        run_on_worker(move |context| {
            Box::pin(async move {
                let stream = StreamId::new(1);
                let mut peer = Http2Codec::new(TransportDirection::Downstream);
                let response_headers = Arc::new(tokio::sync::Notify::new());
                let response_eom = Arc::new(tokio::sync::Notify::new());
                let upload_ready = Arc::new(tokio::sync::Notify::new());
                let release_write = Arc::new(tokio::sync::Notify::new());
                let (transport, wire) = FixtureTransport::new([
                    FixtureRead::Data(server_settings_bytes(1)),
                    FixtureRead::WaitData(
                        Arc::clone(&response_headers),
                        response_head(&mut peer, stream, 200, false, Some(1)),
                    ),
                    FixtureRead::WaitData(
                        Arc::clone(&response_eom),
                        eligibility_frame(
                            &mut peer,
                            EgressFrame::Body {
                                stream,
                                body: queue(b"r"),
                                padding: None,
                                end_stream: true,
                            },
                        ),
                    ),
                ]);
                let mut config = options(context, transport);
                config.http2.max_concurrent_outgoing_streams = 1;
                let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
                let mut upload = Upload::new(1, 1);
                upload.gate = Some((0, Arc::clone(&upload_ready)));
                let (upload, probe) = upload.held();
                let submission = handle.reserve_request().unwrap().submit(
                    request_head(HttpMethod::Post, false, Some(1)),
                    upload,
                    CancellationToken::never(),
                );
                cancelled(drive(session, &handle, &[Arc::clone(&response_headers), Arc::clone(&response_eom), Arc::clone(&upload_ready), Arc::clone(&release_write)], async {
                let mut response = submission.await.unwrap().into_source_parts();
                complete_credit_wait(|| decode_client_frames(&wire.borrow().writes.concat()).header_streams == [stream]).await;
                wire.borrow_mut().write_gate = Some(Arc::clone(&release_write));
                upload_ready.notify_one();
                complete_credit_wait(|| wire.borrow().write_gate_pending.is_some() && probe.drops.get() == 1).await;
                response_headers.notify_one();
                assert!(!response.1.read_header_event().await.unwrap().eom());
                let receipt = response.1.ingress_completion().unwrap();
                match response.1.read_body_event(1).await.unwrap().into_kind() {
                    HttpBodyEventKind::Suspend(resume) => drop(resume),
                    _ => panic!("an empty response first yields a suspend event"),
                }
                let mut raw = response.1.read_body_event(1);
                poll_fn(|context| {
                    assert!(raw.as_mut().poll(context).is_pending());
                    Poll::Ready(())
                }).await;
                response_eom.notify_one();
                receipt.wait().await.unwrap();
                complete_credit_wait(|| response.2.capacity_released()).await;
                assert_eq!(handle.available_capacity(), 1);
                assert!(!handle.is_idle());
                assert_eq!(decode_client_frames(&wire.borrow().writes.concat()).body_bytes, 0);
                if !read_first {
                    release_write.notify_one();
                    complete_credit_wait(|| decode_client_frames(&wire.borrow().writes.concat()).body_bytes == 1).await;
                    assert!(!handle.is_idle(), "the raw response read still owns source retirement");
                }
                let event = raw.await.unwrap();
                assert!(event.eom());
                assert_eq!(event.as_body().unwrap().coalesce(1).unwrap().as_ref(), b"r");
                drop(response);
                if read_first {
                    for _ in 0..4 { tokio::task::yield_now().await; }
                    assert!(!handle.is_idle(), "the request's native write remains owned after raw response consumption");
                    release_write.notify_one();
                }
                complete_credit_wait(|| handle.is_idle()).await;
                assert_eq!(probe.drops.get(), 1);
                assert!(probe.stops.borrow().is_empty());
                assert!(decode_client_frames(&wire.borrow().writes.concat()).resets.is_empty());
            }).await);
            })
        });
    }
}

#[test]
fn empty_terminal_data_and_trailers_survive_zero_and_negative_stream_credit() {
    for (negative, trailers) in [(false, false), (false, true), (true, false), (true, true)] {
        run_on_worker(move |context| {
            Box::pin(async move {
                let stream = StreamId::new(1);
                let mut peer = Http2Codec::new(TransportDirection::Downstream);
                let initial = eligibility_frame(
                    &mut peer,
                    EgressFrame::Settings {
                        settings: &[
                            roxygen_codec::CodecSetting {
                                identifier: SettingsId::InitialWindowSize.as_u16(),
                                value: u64::from(negative),
                            },
                            roxygen_codec::CodecSetting {
                                identifier: SettingsId::MaxConcurrentStreams.as_u16(),
                                value: 1,
                            },
                        ],
                    },
                );
                let change_window = Arc::new(tokio::sync::Notify::new());
                let release_terminal = Arc::new(tokio::sync::Notify::new());
                let respond = Arc::new(tokio::sync::Notify::new());
                let mut update = eligibility_frame(
                    &mut peer,
                    EgressFrame::Settings {
                        settings: &[roxygen_codec::CodecSetting {
                            identifier: SettingsId::InitialWindowSize.as_u16(),
                            value: 0,
                        }],
                    },
                );
                update.extend(eligibility_frame(
                    &mut peer,
                    EgressFrame::PingRequest { data: Some(44) },
                ));
                let (transport, wire) = FixtureTransport::new([
                    FixtureRead::Data(initial),
                    FixtureRead::WaitData(Arc::clone(&change_window), update),
                    FixtureRead::WaitData(
                        Arc::clone(&respond),
                        response_head(&mut peer, stream, 200, true, Some(0)),
                    ),
                ]);
                let (session, handle) =
                    OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
                let size = usize::from(negative);
                let mut upload = Upload::new(size, 1);
                upload.tail.push_back(if trailers {
                    let mut headers = HttpHeaders::new();
                    headers.add("x-source-finish", "exact");
                    HttpBodyEvent::trailers(headers)
                } else {
                    HttpBodyEvent::body(BufQueue::new(0), true)
                });
                upload.gate = Some((size, Arc::clone(&release_terminal)));
                let (upload, probe) = upload.held();
                cancelled(
                    drive(
                        session,
                        &handle,
                        &[
                            Arc::clone(&change_window),
                            Arc::clone(&release_terminal),
                            Arc::clone(&respond),
                        ],
                        async {
                            complete_credit_wait(|| handle.available_capacity() == 1).await;
                            let response = handle
                                .reserve_request()
                                .unwrap()
                                .submit(
                                    request_head(HttpMethod::Post, false, Some(size)),
                                    upload,
                                    CancellationToken::never(),
                                )
                                .await
                                .unwrap()
                                .into_source_parts();
                            complete_credit_wait(|| {
                                decode_client_frames(&wire.borrow().writes.concat()).body_bytes
                                    == size
                            })
                            .await;
                            change_window.notify_one();
                            complete_credit_wait(|| {
                                decode_client_frames(&wire.borrow().writes.concat()).ping_replies
                                    == [44]
                            })
                            .await;
                            assert!(!response.2.capacity_released());
                            release_terminal.notify_one();
                            complete_credit_wait(|| {
                                complete_credit_wire(true, &wire).completed == [stream]
                            })
                            .await;
                            assert_eq!(probe.drops.get(), 1);
                            assert!(probe.stops.borrow().is_empty());
                            let complete = complete_credit_wire(true, &wire);
                            assert_eq!(complete.frames.body_bytes, size);
                            assert_eq!(
                                complete.trailers,
                                if trailers {
                                    vec![(stream, b"exact".to_vec())]
                                } else {
                                    vec![]
                                }
                            );
                            assert!(complete.frames.resets.is_empty());
                            respond.notify_one();
                            response
                                .1
                                .ingress_completion()
                                .unwrap()
                                .wait()
                                .await
                                .unwrap();
                            complete_credit_wait(|| response.2.capacity_released()).await;
                            drop(response);
                            complete_credit_wait(|| handle.is_idle()).await;
                        },
                    )
                    .await,
                );
            })
        });
    }
}

#[test]
fn reservations_survive_settings_zero_but_cannot_commit_after_goaway() {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            let mut initial = server_settings_bytes(0);
            initial.extend(eligibility_frame(
                &mut peer,
                EgressFrame::PingRequest { data: Some(70) },
            ));
            let respond = Arc::new(tokio::sync::Notify::new());
            let reopen = Arc::new(tokio::sync::Notify::new());
            let goaway = Arc::new(tokio::sync::Notify::new());
            let (transport, wire) = FixtureTransport::new([
                FixtureRead::Data(initial),
                FixtureRead::WaitData(
                    Arc::clone(&respond),
                    response_head(&mut peer, StreamId::new(1), 200, true, Some(0)),
                ),
                FixtureRead::WaitData(Arc::clone(&reopen), server_settings_bytes(1)),
                FixtureRead::WaitData(
                    Arc::clone(&goaway),
                    eligibility_frame(
                        &mut peer,
                        EgressFrame::Goaway {
                            last_stream: StreamId::new(1),
                            code: H2ErrorCode::NO_ERROR,
                            debug_data: None,
                        },
                    ),
                ),
            ]);
            let mut config = options(context, transport);
            config.http2.max_concurrent_outgoing_streams = 1;
            let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
            drop(handle.reserve_request().unwrap());
            assert_eq!(handle.available_capacity(), 1);
            let reserved = handle.reserve_request().unwrap();
            let run = drive(
                session,
                &handle,
                &[
                    Arc::clone(&respond),
                    Arc::clone(&reopen),
                    Arc::clone(&goaway),
                ],
                async {
                    complete_credit_wait(|| {
                        decode_client_frames(&wire.borrow().writes.concat()).ping_replies == [70]
                    })
                    .await;
                    assert_eq!(handle.available_capacity(), 0);
                    assert!(handle.reserve_request().is_err());
                    let response = reserved
                        .submit(
                            request_head(HttpMethod::Get, true, None),
                            HttpSourceHolder::empty(),
                            CancellationToken::never(),
                        )
                        .await
                        .unwrap()
                        .into_source_parts();
                    assert_eq!(response.0, StreamId::new(1));
                    respond.notify_one();
                    response
                        .1
                        .ingress_completion()
                        .unwrap()
                        .wait()
                        .await
                        .unwrap();
                    complete_credit_wait(|| response.2.capacity_released()).await;
                    assert_eq!(handle.available_capacity(), 0);
                    drop(response);
                    reopen.notify_one();
                    complete_credit_wait(|| handle.available_capacity() == 1).await;
                    let reserved = handle.reserve_request().unwrap();
                    goaway.notify_one();
                    complete_credit_wait(|| handle.is_finished()).await;
                    assert_eq!(handle.available_capacity(), 0);
                    let (upload, probe) = Upload::new(0, 1).held();
                    let error = reserved
                        .submit(
                            request_head(HttpMethod::Post, false, None),
                            upload,
                            CancellationToken::never(),
                        )
                        .await
                        .err()
                        .expect("GOAWAY refuses a previously reserved request");
                    assert_eq!(error.code(), HttpErrorCode::REFUSED_STREAM);
                    assert_eq!(probe.reads.get(), 0);
                    assert_eq!(probe.drops.get(), 1);
                    assert_eq!(
                        probe.stops.borrow().as_slice(),
                        &[Some(HttpErrorCode::REFUSED_STREAM)]
                    );
                    let decoded = decode_client_frames(&wire.borrow().writes.concat());
                    assert_eq!(decoded.header_streams, [StreamId::new(1)]);
                    assert!(decoded.resets.is_empty());
                },
            )
            .await;
            run.unwrap();
        })
    });
}

#[test]
fn eof_and_cancellation_resolve_queued_submission_acknowledgments() {
    for eof in [false, true] {
        run_on_worker(move |context| {
            Box::pin(async move {
                let ready_eof = Arc::new(tokio::sync::Notify::new());
                ready_eof.notify_one();
                let reads = if eof {
                    vec![FixtureRead::WaitDataEof(Arc::clone(&ready_eof), Vec::new())]
                } else {
                    vec![]
                };
                let (transport, wire) = FixtureTransport::new(reads);
                let (session, handle) =
                    OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
                let (upload, probe) = Upload::new(0, 1).held();
                let submission = handle.reserve_request().unwrap().submit(
                    request_head(HttpMethod::Post, false, None),
                    upload,
                    CancellationToken::never(),
                );
                if !eof {
                    handle.cancel();
                }
                let run = drive(session, &handle, &[Arc::clone(&ready_eof)], async {
                    let error = submission
                        .await
                        .err()
                        .expect("shutdown resolves the pending submission with its cause");
                    let expected = if eof {
                        HttpErrorCode::TRANSPORT_EOF
                    } else {
                        HttpErrorCode::CORO_CANCELLED
                    };
                    assert_eq!(error.code(), expected);
                    assert_eq!(probe.reads.get(), 0);
                    assert_eq!(probe.drops.get(), 1);
                    assert_eq!(probe.stops.borrow().as_slice(), &[Some(expected)]);
                    assert_eq!(handle.available_capacity(), 0);
                })
                .await;
                if eof {
                    run.unwrap();
                } else {
                    cancelled(run);
                }
                assert!(handle.is_finished());
                assert!(
                    decode_client_frames(&wire.borrow().writes.concat())
                        .header_streams
                        .is_empty()
                );
            })
        });
    }
}

#[test]
fn informational_headers_and_head_content_length_preserve_response_semantics() {
    for head in [false, true] {
        run_on_worker(move |context| {
            Box::pin(async move {
                let stream = StreamId::new(1);
                let mut peer = Http2Codec::new(TransportDirection::Downstream);
                let mut response_bytes = response_head(&mut peer, stream, 103, false, None);
                response_bytes.extend(response_head(
                    &mut peer,
                    stream,
                    200,
                    head,
                    Some(if head { 999 } else { 1 }),
                ));
                if !head {
                    response_bytes.extend(eligibility_frame(
                        &mut peer,
                        EgressFrame::Body {
                            stream,
                            body: queue(b"r"),
                            padding: None,
                            end_stream: true,
                        },
                    ));
                }
                let respond = Arc::new(tokio::sync::Notify::new());
                let (transport, wire) = FixtureTransport::new([
                    FixtureRead::Data(server_settings_bytes(1)),
                    FixtureRead::WaitData(Arc::clone(&respond), response_bytes),
                ]);
                let (session, handle) =
                    OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
                let submission = handle.reserve_request().unwrap().submit(
                    request_head(
                        if head {
                            HttpMethod::Head
                        } else {
                            HttpMethod::Get
                        },
                        true,
                        None,
                    ),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                cancelled(
                    drive(session, &handle, &[Arc::clone(&respond)], async {
                        let mut response = submission.await.unwrap().into_source_parts();
                        let receipt = response.1.ingress_completion().unwrap();
                        respond.notify_one();
                        let mut informational = response.1.read_header_event().await.unwrap();
                        assert_eq!(informational.headers_mut().status_code().unwrap(), 103);
                        assert!(!informational.is_final().unwrap());
                        assert!(!informational.eom());
                        let mut final_head = response.1.read_header_event().await.unwrap();
                        assert_eq!(final_head.headers_mut().status_code().unwrap(), 200);
                        assert!(final_head.is_final().unwrap());
                        assert_eq!(final_head.eom(), head);
                        if !head {
                            let event = response.1.read_body_event(1).await.unwrap();
                            assert!(event.eom());
                            assert_eq!(
                                event.as_body().unwrap().coalesce(1).unwrap().as_ref(),
                                b"r"
                            );
                        }
                        receipt.wait().await.unwrap();
                        assert_eq!(receipt.body_bytes_received(), usize::from(!head) as u64);
                        complete_credit_wait(|| response.2.capacity_released()).await;
                        drop(response);
                        complete_credit_wait(|| handle.is_idle()).await;
                        let decoded = decode_client_frames(&wire.borrow().writes.concat());
                        assert_eq!(decoded.header_streams, [stream]);
                        assert!(decoded.header_statuses.is_empty());
                        assert!(decoded.resets.is_empty());
                        assert!(decoded.errors.is_empty());
                    })
                    .await,
                );
            })
        });
    }
}

#[test]
fn response_parse_errors_and_eof_never_synthesize_downstream_error_responses() {
    for eof in [false, true] {
        run_on_worker(move |context| {
            Box::pin(async move {
                let mut peer = Http2Codec::new(TransportDirection::Downstream);
                let fail = Arc::new(tokio::sync::Notify::new());
                let sibling = Arc::new(tokio::sync::Notify::new());
                let mut reads = vec![FixtureRead::Data(server_settings_bytes(100))];
                if eof {
                    reads.push(FixtureRead::WaitDataEof(
                        Arc::clone(&fail),
                        response_head(&mut peer, StreamId::new(1), 200, false, Some(1)),
                    ));
                } else {
                    reads.push(FixtureRead::WaitData(
                        Arc::clone(&fail),
                        server_duplicate_content_length_response(),
                    ));
                    reads.push(FixtureRead::WaitData(
                        Arc::clone(&sibling),
                        response_head(&mut peer, StreamId::new(3), 200, true, Some(0)),
                    ));
                }
                let (transport, wire) = FixtureTransport::new(reads);
                let (session, handle) =
                    OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
                let submission = handle.reserve_request().unwrap().submit(
                    request_head(HttpMethod::Get, true, None),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                let run = drive(
                    session,
                    &handle,
                    &[Arc::clone(&fail), Arc::clone(&sibling)],
                    async {
                        let mut response = submission.await.unwrap().into_source_parts();
                        fail.notify_one();
                        let error = response.1.read_header_event().await.err().expect(
                            "the upstream response fails instead of returning a synthetic head",
                        );
                        assert_eq!(
                            error.code(),
                            if eof {
                                HttpErrorCode::TRANSPORT_EOF
                            } else {
                                HttpErrorCode::PROTOCOL_ERROR
                            }
                        );
                        drop(response);
                        if !eof {
                            let mut next = handle
                                .reserve_request()
                                .unwrap()
                                .submit(
                                    request_head(HttpMethod::Get, true, None),
                                    HttpSourceHolder::empty(),
                                    CancellationToken::never(),
                                )
                                .await
                                .unwrap()
                                .into_source_parts();
                            assert_eq!(next.0, StreamId::new(3));
                            sibling.notify_one();
                            let mut header = next.1.read_header_event().await.unwrap();
                            assert_eq!(header.headers_mut().status_code().unwrap(), 200);
                            assert!(header.eom());
                            drop(next);
                            complete_credit_wait(|| handle.is_idle()).await;
                        } else {
                            complete_credit_wait(|| handle.is_finished()).await;
                        }
                    },
                )
                .await;
                if eof {
                    run.unwrap();
                } else {
                    cancelled(run);
                }
                let decoded = decode_client_frames(&wire.borrow().writes.concat());
                assert_eq!(
                    decoded.header_streams,
                    if eof {
                        vec![StreamId::new(1)]
                    } else {
                        vec![StreamId::new(1), StreamId::new(3)]
                    }
                );
                assert!(decoded.header_statuses.is_empty());
                assert!(decoded.errors.is_empty());
                if !eof {
                    assert_eq!(decoded.resets.len(), 1);
                }
            })
        });
    }
}

#[test]
fn goaway_immediately_excludes_refused_uploads_and_preserves_earlier_causes() {
    for goaway_error in [false, true] {
        run_on_worker(move |context| {
            Box::pin(async move {
                let mut peer = Http2Codec::new(TransportDirection::Downstream);
                let initial = eligibility_frame(
                    &mut peer,
                    EgressFrame::Settings {
                        settings: &[
                            roxygen_codec::CodecSetting {
                                identifier: SettingsId::InitialWindowSize.as_u16(),
                                value: 65_535,
                            },
                            roxygen_codec::CodecSetting {
                                identifier: SettingsId::MaxConcurrentStreams.as_u16(),
                                value: 3,
                            },
                        ],
                    },
                );
                let refuse = Arc::new(tokio::sync::Notify::new());
                let credit = Arc::new(tokio::sync::Notify::new());
                let respond = Arc::new(tokio::sync::Notify::new());
                let mut refused = eligibility_frame(
                    &mut peer,
                    EgressFrame::Reset {
                        stream: StreamId::new(3),
                        code: H2ErrorCode::CANCEL,
                    },
                );
                refused.extend(eligibility_frame(
                    &mut peer,
                    EgressFrame::Goaway {
                        last_stream: StreamId::new(1),
                        code: if goaway_error {
                            H2ErrorCode::INTERNAL_ERROR
                        } else {
                            H2ErrorCode::NO_ERROR
                        },
                        debug_data: None,
                    },
                ));
                for stream in [StreamId::UNSPECIFIED, StreamId::new(5)] {
                    refused.extend(eligibility_frame(
                        &mut peer,
                        EgressFrame::WindowUpdate { stream, amount: 1 },
                    ));
                }
                let (transport, wire) = FixtureTransport::new([
                    FixtureRead::Data(initial),
                    FixtureRead::WaitData(Arc::clone(&refuse), refused),
                    FixtureRead::WaitData(
                        Arc::clone(&credit),
                        eligibility_frame(
                            &mut peer,
                            EgressFrame::WindowUpdate {
                                stream: StreamId::new(1),
                                amount: 1,
                            },
                        ),
                    ),
                    FixtureRead::WaitData(
                        Arc::clone(&respond),
                        response_head(&mut peer, StreamId::new(1), 200, true, Some(0)),
                    ),
                ]);
                let (session, handle) =
                    OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
                let run = drive(
                    session,
                    &handle,
                    &[
                        Arc::clone(&refuse),
                        Arc::clone(&credit),
                        Arc::clone(&respond),
                    ],
                    async {
                        complete_credit_wait(|| handle.available_capacity() == 3).await;
                        let (upload, first) = Upload::new(65_536, 1024).held();
                        let mut survivor = handle
                            .reserve_request()
                            .unwrap()
                            .submit(
                                request_head(HttpMethod::Post, false, Some(65_536)),
                                upload,
                                CancellationToken::never(),
                            )
                            .await
                            .unwrap()
                            .into_source_parts();
                        complete_credit_wait(|| {
                            decode_client_frames(&wire.borrow().writes.concat()).body_bytes
                                == 65_535
                                && first.bytes.get() == 65_536
                                && first.drops.get() == 1
                        })
                        .await;
                        let mut responses = Vec::new();
                        let mut probes = vec![first];
                        for _ in 0..2 {
                            let (upload, probe) = Upload::new(2, 1).held();
                            responses.push(
                                handle
                                    .reserve_request()
                                    .unwrap()
                                    .submit(
                                        request_head(HttpMethod::Post, false, Some(2)),
                                        upload,
                                        CancellationToken::never(),
                                    )
                                    .await
                                    .unwrap()
                                    .into_source_parts(),
                            );
                            probes.push(probe);
                        }
                        complete_credit_wait(|| {
                            decode_client_frames(&wire.borrow().writes.concat()).header_streams
                                == [StreamId::new(1), StreamId::new(3), StreamId::new(5)]
                                && probes[1..]
                                    .iter()
                                    .all(|probe| probe.bytes.get() == 1 && probe.drops.get() == 0)
                        })
                        .await;
                        for _ in 0..4 {
                            tokio::task::yield_now().await;
                        }
                        assert_eq!(
                            decode_client_frames(&wire.borrow().writes.concat()).body_bytes,
                            65_535
                        );
                        refuse.notify_one();
                        for (response, expected) in responses
                            .iter_mut()
                            .zip([HttpErrorCode::CANCEL, HttpErrorCode::REFUSED_STREAM])
                        {
                            let error = response
                                .1
                                .read_header_event()
                                .await
                                .err()
                                .expect("peer refusal must fail the unread response");
                            assert_eq!(error.code(), expected);
                        }
                        let refused_wire = decode_client_frames(&wire.borrow().writes.concat());
                        assert_eq!(refused_wire.body_bytes, 65_535);
                        assert!(refused_wire.resets.is_empty());
                        drop(responses);
                        assert_eq!(handle.available_capacity(), 0);
                        if goaway_error {
                            let error = survivor
                                .1
                                .read_header_event()
                                .await
                                .err()
                                .expect("error GOAWAY must fail the surviving response");
                            assert_eq!(error.code(), HttpErrorCode::INTERNAL_ERROR);
                        } else {
                            // The survivor needs the sole connection byte reopened with GOAWAY.
                            credit.notify_one();
                            complete_credit_wait(|| {
                                complete_credit_wire(true, &wire).completed == [StreamId::new(1)]
                            })
                            .await;
                            respond.notify_one();
                            let mut headers = survivor.1.read_header_event().await.unwrap();
                            assert_eq!(headers.headers_mut().status_code().unwrap(), 200);
                            assert!(headers.eom());
                        }
                        drop(survivor);
                        complete_credit_wait(|| handle.is_finished()).await;
                        let final_wire = decode_client_frames(&wire.borrow().writes.concat());
                        assert_eq!(
                            final_wire.body_data,
                            vec![b'u'; if goaway_error { 65_535 } else { 65_536 }]
                        );
                        assert!(
                            final_wire
                                .body_streams
                                .iter()
                                .all(|stream| *stream == StreamId::new(1))
                        );
                        assert!(final_wire.resets.is_empty());
                        assert!(final_wire.errors.is_empty());
                        assert_eq!(probes[0].drops.get(), 1);
                        assert!(probes[0].stops.borrow().is_empty());
                        for (probe, expected) in probes[1..]
                            .iter()
                            .zip([HttpErrorCode::CANCEL, HttpErrorCode::REFUSED_STREAM])
                        {
                            assert_eq!(probe.reads.get(), 1);
                            assert_eq!(probe.produced.get(), 1);
                            assert_eq!(probe.bytes.get(), 1);
                            assert_eq!(probe.drops.get(), 1);
                            assert_eq!(probe.stops.borrow().as_slice(), &[Some(expected)]);
                        }
                    },
                )
                .await;
                if goaway_error {
                    assert_eq!(
                        run.expect_err("error GOAWAY fails the session").code(),
                        HttpErrorCode::INTERNAL_ERROR
                    );
                } else {
                    run.unwrap();
                }
            })
        });
    }
}

#[test]
fn a_committed_upload_error_preserves_its_ack_while_explicit_cancellation_keeps_its_cause() {
    for cancellation in [None, Some(false), Some(true)] {
        run_on_worker(move |context| {
            Box::pin(async move {
                let failed = Arc::new(tokio::sync::Notify::new());
                let (transport, wire) =
                    FixtureTransport::new([FixtureRead::Data(server_settings_bytes(1))]);
                let mut config = options(context, transport);
                config.http2.max_concurrent_outgoing_streams = 1;
                let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
                let mut upload = Upload::new(0, 1);
                if cancellation.is_none() {
                    upload.failure = Some((Arc::clone(&failed), HttpErrorCode::READ_TIMEOUT));
                }
                let (upload, probe) = upload.held();
                let encoded = Rc::new(Cell::new(false));
                let mut header = request_head(HttpMethod::Post, false, None);
                let observed = Rc::clone(&encoded);
                header.set_egress_headers_callback(move |_| observed.set(true));
                let submission = handle.reserve_request().unwrap().submit(
                    header,
                    upload,
                    CancellationToken::never(),
                );
                cancelled(
                    drive(session, &handle, &[Arc::clone(&failed)], async {
                        complete_credit_wait(|| encoded.get() && probe.reads.get() == 1).await;
                        match cancellation {
                            None => {
                                failed.notify_one();
                                complete_credit_wait(|| {
                                    probe.drops.get() == 1
                                        && !decode_client_frames(&wire.borrow().writes.concat())
                                            .resets
                                            .is_empty()
                                })
                                .await;
                                let mut response = submission
                                    .await
                                    .expect("a later upload error preserves the committed acknowledgment").into_source_parts();
                                let error = response
                                    .1
                                    .read_header_event()
                                    .await
                                    .err()
                                    .expect("the original response source observes upload cancellation");
                                assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                                drop(response);
                                assert!(probe.stops.borrow().is_empty());
                            }
                            Some(false) => {
                                submission.cancel(HttpError::new(HttpErrorCode::READ_TIMEOUT));
                                submission.cancel(HttpError::new(HttpErrorCode::CORO_CANCELLED));
                                let Err(error) = submission.await else {
                                    panic!("explicit submission cancellation rejects the unclaimed acknowledgment");
                                };
                                assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
                            }
                            Some(true) => {
                                let mut response = submission.await.unwrap().into_source_parts();
                                response.2.cancel(HttpError::new(HttpErrorCode::READ_TIMEOUT));
                                response.2.cancel(HttpError::new(HttpErrorCode::CORO_CANCELLED));
                                let error = response
                                    .1
                                    .read_header_event()
                                    .await
                                    .err()
                                    .expect("explicit request cancellation reaches the response source");
                                assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
                                drop(response);
                            }
                        }
                        complete_credit_wait(|| probe.drops.get() == 1 && handle.is_idle()).await;
                        assert_eq!(probe.reads.get(), 1);
                        assert_eq!(probe.produced.get(), 0);
                        if cancellation.is_some() {
                            assert_eq!(probe.stops.borrow().as_slice(), &[Some(HttpErrorCode::READ_TIMEOUT)]);
                        }
                        assert_eq!(handle.available_capacity(), 1);
                        let decoded = decode_client_frames(&wire.borrow().writes.concat());
                        assert_eq!(decoded.header_streams, [StreamId::new(1)]);
                        assert_eq!(
                            decoded.resets,
                            [(StreamId::new(1), if cancellation.is_some() {
                                H2ErrorCode::CANCEL
                            } else {
                                H2ErrorCode::INTERNAL_ERROR
                            })]
                        );
                        assert_eq!(decoded.body_bytes, 0);
                        assert!(decoded.errors.is_empty());
                    })
                    .await,
                );
            })
        });
    }
}

#[test]
fn buffered_responses_survive_peer_reset_and_native_write_failure_until_consumed() {
    for write_failure in [false, true] {
        run_on_worker(move |context| {
            Box::pin(async move {
                let mut peer = Http2Codec::new(TransportDirection::Downstream);
                let respond = Arc::new(tokio::sync::Notify::new());
                let stop = Arc::new(tokio::sync::Notify::new());
                let mut complete = response_head(&mut peer, StreamId::new(1), 200, false, Some(1));
                complete.extend(eligibility_frame(
                    &mut peer,
                    EgressFrame::Body {
                        stream: StreamId::new(1),
                        body: queue(b"r"),
                        padding: None,
                        end_stream: true,
                    },
                ));
                let stopped = eligibility_frame(
                    &mut peer,
                    if write_failure {
                        EgressFrame::PingRequest { data: Some(73) }
                    } else {
                        EgressFrame::Reset {
                            stream: StreamId::new(1),
                            code: H2ErrorCode::NO_ERROR,
                        }
                    },
                );
                let (transport, wire) = FixtureTransport::new([
                    FixtureRead::Data(server_settings_bytes(1)),
                    FixtureRead::WaitData(Arc::clone(&respond), complete),
                    FixtureRead::WaitData(Arc::clone(&stop), stopped),
                ]);
                let mut config = options(context, transport);
                config.http2.max_concurrent_outgoing_streams = 1;
                let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
                let observer = Rc::new(AdmissionProbe::default());
                handle.set_observer(observer.clone()).unwrap();
                let (upload, probe) = Upload::new(0, 1).held();
                let submission = handle.reserve_request().unwrap().submit(
                    request_head(HttpMethod::Post, false, None),
                    upload,
                    CancellationToken::never(),
                );
                let run = drive(
                    session,
                    &handle,
                    &[Arc::clone(&respond), Arc::clone(&stop)],
                    async {
                        let mut response = submission.await.unwrap().into_source_parts();
                        complete_credit_wait(|| probe.reads.get() == 1).await;
                        let receipt = response.1.ingress_completion().unwrap();
                        respond.notify_one();
                        receipt.wait().await.unwrap();
                        assert!(!response.2.capacity_released());
                        assert_eq!(handle.available_capacity(), 0);
                        if write_failure {
                            wire.borrow_mut().write_error =
                                Some(HttpErrorCode::TRANSPORT_WRITE_ERROR);
                        }
                        stop.notify_one();
                        complete_credit_wait(|| probe.drops.get() == 1).await;
                        if write_failure {
                            complete_credit_wait(|| wire.borrow().close == FixtureClose::Reset)
                                .await;
                        }
                        // A peer RST_STREAM(NO_ERROR) after the whole response stops the
                        // upload with NO_ERROR (`DIV-050`).
                        assert_eq!(
                            probe.stops.borrow().as_slice(),
                            &[Some(if write_failure {
                                HttpErrorCode::TRANSPORT_WRITE_ERROR
                            } else {
                                HttpErrorCode::NO_ERROR
                            })]
                        );
                        assert!(response.1.readable());
                        assert!(matches!(receipt.snapshot(), Some(Ok(_))));
                        assert_eq!(receipt.body_bytes_received(), 1);
                        assert!(!handle.is_idle());
                        assert!(!handle.is_finished());
                        assert!(!response.2.capacity_released());
                        assert!(observer.released.borrow().is_empty());
                        let mut headers = response.1.read_header_event().await.unwrap();
                        assert_eq!(headers.headers_mut().status_code().unwrap(), 200);
                        assert!(!headers.eom());
                        let body = response.1.read_body_event(1).await.unwrap();
                        assert!(body.eom());
                        assert_eq!(body.as_body().unwrap().coalesce(1).unwrap().as_ref(), b"r");
                        let control = response.2.clone();
                        drop(response);
                        if write_failure {
                            complete_credit_wait(|| handle.is_finished()).await;
                        } else {
                            complete_credit_wait(|| handle.is_idle()).await;
                            assert_eq!(handle.available_capacity(), 1);
                        }
                        assert!(control.capacity_released());
                        assert_eq!(observer.released.borrow().as_slice(), &[StreamId::new(1)]);
                        assert_eq!(probe.drops.get(), 1);
                        let decoded = decode_client_frames(&wire.borrow().writes.concat());
                        assert_eq!(decoded.header_streams, [StreamId::new(1)]);
                        assert_eq!(decoded.body_bytes, 0);
                        assert!(decoded.resets.is_empty());
                        assert!(decoded.errors.is_empty());
                    },
                )
                .await;
                if write_failure {
                    assert_eq!(
                        run.expect_err("native write failure reaches the joined runner")
                            .code(),
                        HttpErrorCode::TRANSPORT_WRITE_ERROR
                    );
                } else {
                    cancelled(run);
                }
            })
        });
    }
}

/// What a submitter sees when the origin resets its stream with RST_STREAM(NO_ERROR)
/// while the request upload is still open.
struct NoErrorReset {
    /// The upload source's `stop_reading` codes.
    stops: Vec<Option<HttpErrorCode>>,
    /// The response receipt once the upload is retired.
    receipt: Option<Result<(), HttpErrorCode>>,
    /// The response status and its body events through EOM, or the first read error.
    read: Result<(u16, Vec<ReadEvent>), HttpErrorCode>,
    /// RST_STREAM frames the session wrote.
    resets: Vec<(StreamId, H2ErrorCode)>,
}

/// Submits a POST whose upload stays open. Once the upload is being read, the origin
/// sends `response`, whose body is one byte, in one read. Once that byte has arrived
/// (and, if `complete`, the response has ended), the origin resets the stream with
/// RST_STREAM(NO_ERROR) in a separate read.
fn no_error_reset_case(
    complete: bool,
    response: impl FnOnce(&mut Http2Codec, StreamId) -> Vec<u8> + Send + 'static,
) -> NoErrorReset {
    run_on_worker(move |context| {
        Box::pin(async move {
            let stream = StreamId::new(1);
            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            let respond = Arc::new(tokio::sync::Notify::new());
            let reset = Arc::new(tokio::sync::Notify::new());
            let response = response(&mut peer, stream);
            let reset_bytes = eligibility_frame(
                &mut peer,
                EgressFrame::Reset {
                    stream,
                    code: H2ErrorCode::NO_ERROR,
                },
            );
            let (transport, wire) = FixtureTransport::new([
                FixtureRead::Data(server_settings_bytes(1)),
                FixtureRead::WaitData(Arc::clone(&respond), response),
                FixtureRead::WaitData(Arc::clone(&reset), reset_bytes),
            ]);
            let mut config = options(context, transport);
            config.http2.max_concurrent_outgoing_streams = 1;
            let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
            let (upload, probe) = Upload::new(0, 1).held();
            let submission = handle.reserve_request().unwrap().submit(
                request_head(HttpMethod::Post, false, None),
                upload,
                CancellationToken::never(),
            );
            let outcome = RefCell::new(None);
            let run = drive(
                session,
                &handle,
                &[Arc::clone(&respond), Arc::clone(&reset)],
                async {
                    let (_, mut source, _request) = submission.await.unwrap().into_source_parts();
                    complete_credit_wait(|| probe.reads.get() == 1).await;
                    let receipt = source.ingress_completion().unwrap();
                    respond.notify_one();
                    if complete {
                        receipt.wait().await.unwrap();
                    } else {
                        complete_credit_wait(|| receipt.body_bytes_received() == 1).await;
                    }
                    assert_eq!(probe.drops.get(), 0, "the upload is open before the reset");
                    reset.notify_one();
                    complete_credit_wait(|| probe.drops.get() == 1).await;
                    let stops = probe.stops.borrow().clone();
                    let receipt = receipt
                        .snapshot()
                        .map(|result| result.map(drop).map_err(|error| error.code()));
                    let read = async {
                        let mut head = source.read_header_event().await?;
                        let status = head.headers_mut().status_code().unwrap();
                        let mut events = Vec::new();
                        let mut eom = head.eom();
                        while !eom {
                            let event = source.read_body_event(u32::MAX).await?;
                            eom = event.eom();
                            match event.into_kind() {
                                HttpBodyEventKind::Body(bytes) => {
                                    let length = bytes.chain_length();
                                    let bytes = bytes.coalesce(length).unwrap().to_vec();
                                    events.push(("body", bytes, eom));
                                }
                                HttpBodyEventKind::Trailers(trailers) => events.push((
                                    "trailers",
                                    trailers.get_single_or_empty(b"grpc-status").to_vec(),
                                    eom,
                                )),
                                _ => panic!("unexpected response body event"),
                            }
                        }
                        Ok::<_, HttpError>((status, events))
                    }
                    .await
                    .map_err(|error| error.code());
                    drop(source);
                    complete_credit_wait(|| handle.is_idle()).await;
                    let resets = decode_client_frames(&wire.borrow().writes.concat()).resets;
                    *outcome.borrow_mut() = Some(NoErrorReset {
                        stops,
                        receipt,
                        read,
                        resets,
                    });
                },
            )
            .await;
            cancelled(run);
            outcome.into_inner().unwrap()
        })
    })
}

#[test]
fn peer_no_error_reset_after_response_trailers_stops_the_upload_with_no_error() {
    let outcome = no_error_reset_case(true, |peer, stream| {
        let mut wire = response_head(peer, stream, 200, false, None);
        wire.extend(eligibility_frame(
            peer,
            EgressFrame::Body {
                stream,
                body: queue(b"r"),
                padding: None,
                end_stream: false,
            },
        ));
        let mut trailers = HttpHeaders::new();
        trailers.add("grpc-status", "0");
        wire.extend(eligibility_frame(
            peer,
            EgressFrame::Trailers {
                stream,
                trailers: &trailers,
            },
        ));
        wire
    });
    assert_eq!(
        outcome.stops,
        [Some(HttpErrorCode::NO_ERROR)],
        "a reset after the trailers ended the response stops the upload with NO_ERROR"
    );
    assert_eq!(outcome.receipt, Some(Ok(())));
    assert_eq!(
        outcome.read,
        Ok((
            200,
            vec![
                ("body", b"r".to_vec(), false),
                ("trailers", b"0".to_vec(), true),
            ]
        )),
        "the whole response, trailers included, stays readable"
    );
    assert!(outcome.resets.is_empty(), "a peer reset is not echoed");
}

#[test]
fn peer_no_error_reset_mid_response_stops_the_upload_with_cancel() {
    let outcome = no_error_reset_case(false, |peer, stream| {
        let mut wire = response_head(peer, stream, 200, false, None);
        wire.extend(eligibility_frame(
            peer,
            EgressFrame::Body {
                stream,
                body: queue(b"r"),
                padding: None,
                end_stream: false,
            },
        ));
        wire
    });
    assert_eq!(
        outcome.stops,
        [Some(HttpErrorCode::CANCEL)],
        "a reset before the response ended stops the upload with CANCEL"
    );
    assert_eq!(outcome.receipt, Some(Err(HttpErrorCode::CANCEL)));
    assert_eq!(
        outcome.read,
        Err(HttpErrorCode::CANCEL),
        "the response fails"
    );
    assert!(outcome.resets.is_empty(), "a peer reset is not echoed");
}

#[test]
fn large_window_zero_timeout_settings_accept_finite_rates_and_close_with_one_terminal_goaway() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, wire) = FixtureTransport::new([]);
            let config = OwnedH2Options {
                context,
                transport,
                io: UniplexConfig {
                    buffer_limit: 16 << 20,
                    ingress_buffer_limit: 16 << 20,
                    read_timeout: Duration::ZERO,
                    stream_read_timeout: Duration::ZERO,
                    write_timeout: Duration::ZERO,
                    ..UniplexConfig::default()
                },
                http2: Http2SessionConfig {
                    settings: vec![
                        roxygen_codec::CodecSetting {
                            identifier: SettingsId::InitialWindowSize.as_u16(),
                            value: 16 << 20,
                        },
                        roxygen_codec::CodecSetting {
                            identifier: SettingsId::EnablePush.as_u16(),
                            value: 0,
                        },
                    ],
                    stream_receive_window: 16 << 20,
                    connection_receive_window: 128 << 20,
                    max_concurrent_outgoing_streams: 100,
                    ..Http2SessionConfig::default()
                },
            };
            let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
            assert_eq!(handle.available_capacity(), 100);
            assert!(handle.close_when_idle().unwrap());
            assert!(!handle.close_when_idle().unwrap());
            assert!(handle.reserve_request().is_err());
            let run = drive(session, &handle, &[], async {
                complete_credit_wait(|| handle.is_finished()).await;
            })
            .await
            .unwrap();
            assert!(!run.reset);
            let decoded = decode_client_frames(&wire.borrow().writes.concat());
            assert_eq!(
                decoded.goaways,
                [(StreamId::UNSPECIFIED, H2ErrorCode::NO_ERROR)]
            );
            assert!(decoded.header_streams.is_empty());
            assert!(decoded.resets.is_empty());
            assert!(decoded.errors.is_empty());
        })
    });
}

#[test]
fn owned_upstream_h2_ignores_rate_limits() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, _) = FixtureTransport::new([]);
            let mut config = options(context, transport);
            let tight = RateLimitConfig::new(0, Duration::from_secs(60), 0);
            config.http2.rate_limits = Http2RateLimitConfig {
                control_frames: tight,
                header_frames: tight,
                resets: tight,
                direct_errors: tight,
            };
            let (session, _) = OwnedH2UpstreamSession::new(config)
                .expect("the upstream driver accepts any rate limits");
            assert!(
                !session.limits_rates(),
                "an upstream session counts nothing, as UniplexSession upstream does"
            );
        })
    });
}

#[test]
fn upload_resume_cancellation_prevents_admitting_another_body_read() {
    struct CancelResume {
        ready: LocalSourceFuture<()>,
        cancel: Option<Box<dyn FnOnce()>>,
        on_drop: bool,
    }

    impl Future for CancelResume {
        type Output = ResumeStatus;

        fn poll(
            mut self: Pin<&mut Self>,
            context: &mut std::task::Context<'_>,
        ) -> Poll<ResumeStatus> {
            if self.ready.as_mut().poll(context).is_pending() {
                return Poll::Pending;
            }
            if !self.on_drop {
                self.cancel.take().unwrap()();
            }
            Poll::Ready(ResumeStatus::Signalled)
        }
    }

    impl Drop for CancelResume {
        fn drop(&mut self) {
            if self.on_drop
                && let Some(cancel) = self.cancel.take()
            {
                cancel();
            }
        }
    }

    for (connection, on_drop) in [(false, false), (false, true), (true, false), (true, true)] {
        run_on_worker(move |context| {
            Box::pin(async move {
                let ready = Arc::new(tokio::sync::Notify::new());
                let (transport, wire) =
                    FixtureTransport::new([FixtureRead::Data(server_settings_bytes(1))]);
                let mut config = options(context, transport);
                config.http2.max_concurrent_outgoing_streams = 1;
                let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
                let request: Rc<RefCell<Option<crate::owned_h2::OwnedH2UpstreamRequest>>> =
                    Rc::new(RefCell::new(None));
                let cancel_handle = handle.clone();
                let cancel_request = Rc::clone(&request);
                let gate = Arc::clone(&ready);
                let mut upload = Upload::new(0, 1);
                upload
                    .tail
                    .push_back(HttpBodyEvent::suspend(Box::pin(CancelResume {
                        ready: Box::pin(async move { gate.notified().await }),
                        cancel: Some(Box::new(move || {
                            if connection {
                                cancel_handle.cancel();
                            } else {
                                cancel_request
                                    .borrow()
                                    .as_ref()
                                    .unwrap()
                                    .cancel(HttpError::new(HttpErrorCode::CORO_CANCELLED));
                            }
                        })),
                        on_drop,
                    })));
                upload
                    .tail
                    .push_back(HttpBodyEvent::body(queue(b"u"), true));
                let (upload, probe) = upload.held();
                let submission = handle.reserve_request().unwrap().submit(
                    request_head(HttpMethod::Post, false, Some(1)),
                    upload,
                    CancellationToken::never(),
                );
                let run = drive(session, &handle, &[Arc::clone(&ready)], async {
                    let mut response = submission.await.unwrap().into_source_parts();
                    *request.borrow_mut() = Some(response.2.clone());
                    complete_credit_wait(|| probe.produced.get() == 1).await;
                    assert_eq!(probe.reads.get(), 1);
                    ready.notify_one();
                    let error = response
                        .1
                        .read_header_event()
                        .await
                        .err()
                        .expect("resume callback cancellation reaches the response source");
                    assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                    drop(response);
                    if connection {
                        complete_credit_wait(|| handle.is_finished()).await;
                    } else {
                        complete_credit_wait(|| handle.is_idle()).await;
                    }
                    assert_eq!(
                        probe.reads.get(),
                        1,
                        "cancellation must be rechecked before constructing the next body read"
                    );
                    assert_eq!(probe.produced.get(), 1);
                    assert_eq!(probe.bytes.get(), 0);
                    assert_eq!(probe.drops.get(), 1);
                    assert_eq!(
                        probe.stops.borrow().as_slice(),
                        &[Some(HttpErrorCode::CORO_CANCELLED)]
                    );
                    let decoded = decode_client_frames(&wire.borrow().writes.concat());
                    assert_eq!(decoded.body_bytes, 0);
                    if !connection {
                        assert_eq!(decoded.resets, [(StreamId::new(1), H2ErrorCode::CANCEL)]);
                    }
                    assert!(decoded.errors.is_empty());
                })
                .await;
                cancelled(run);
            })
        });
    }
}

#[test]
fn ready_upload_preserves_selected_timeout_and_retires_its_registration_after_source() {
    struct ReadyEventCallback {
        upload: Rc<UploadProbe>,
        events: Rc<ByteEventProbe>,
    }

    impl HttpByteEventCallback for ReadyEventCallback {
        fn on_byte_event(&self, event: HttpByteEvent) {
            self.events.on_byte_event(event);
        }

        fn on_byte_event_cancelled(&self, event: HttpByteEvent, error: HttpError) {
            self.upload.lifecycle.borrow_mut().push("event.cancel");
            self.events.on_byte_event_cancelled(event, error);
        }
    }

    for registered_stream in [None, Some(StreamId::new(7))] {
        run_on_worker(move |context| {
            Box::pin(async move {
                let stream = StreamId::new(1);
                let ready = Arc::new(tokio::sync::Notify::new());
                let request: Rc<RefCell<Option<crate::owned_h2::OwnedH2UpstreamRequest>>> =
                    Rc::new(RefCell::new(None));
                let (transport, wire) =
                    FixtureTransport::new([FixtureRead::Data(server_settings_bytes(1))]);
                let mut config = options(context, transport);
                config.http2.max_concurrent_outgoing_streams = 1;
                let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
                let mut upload = Upload::new(0, 1);
                let byte_events = Rc::new(ByteEventProbe::default());
                let callback = Rc::new(ReadyEventCallback {
                    upload: Rc::clone(&upload.probe),
                    events: Rc::clone(&byte_events),
                });
                let mut event = HttpBodyEvent::body(queue(b"u"), false);
                event
                    .byte_event_registrations_mut()
                    .push(HttpByteEventRegistration::new(
                        registered_stream,
                        ByteEventType::TransportWrite.bit(),
                        &callback,
                    ));
                let mut event = Some(event);
                let observed = Rc::clone(&upload.probe);
                let drop_read = UploadReadDrop(Rc::clone(&observed));
                let cancel_request = Rc::clone(&request);
                let gate = Arc::clone(&ready);
                let mut gate: LocalSourceFuture<()> =
                    Box::pin(async move { gate.notified().await });
                upload.body_read = Some(Box::pin(poll_fn(move |context| {
                    let _read_owner = &drop_read;
                    if gate.as_mut().poll(context).is_pending() {
                        return Poll::Pending;
                    }
                    observed.lifecycle.borrow_mut().push("read.poll");
                    let request = cancel_request.borrow();
                    let request = request.as_ref().unwrap();
                    request.cancel(HttpError::new(HttpErrorCode::READ_TIMEOUT));
                    request.cancel(HttpError::new(HttpErrorCode::CORO_CANCELLED));
                    observed.produced.set(observed.produced.get() + 1);
                    observed.bytes.set(observed.bytes.get() + 1);
                    Poll::Ready(Ok(event.take().expect("the upload read completes once")))
                })));
                let (upload, probe) = upload.held();
                let submission = handle.reserve_request().unwrap().submit(
                    request_head(HttpMethod::Post, false, Some(1)),
                    upload,
                    CancellationToken::never(),
                );
                cancelled(
                    drive(session, &handle, &[Arc::clone(&ready)], async {
                        let mut response = submission.await.unwrap().into_source_parts();
                        *request.borrow_mut() = Some(response.2.clone());
                        complete_credit_wait(|| probe.reads.get() == 1).await;
                        ready.notify_one();
                        let error =
                            response.1.read_header_event().await.err().expect(
                                "the ready upload keeps the first selected cancellation cause",
                            );
                        assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
                        drop(response);
                        complete_credit_wait(|| handle.is_idle()).await;
                        assert_eq!(probe.reads.get(), 1);
                        assert_eq!(probe.produced.get(), 1);
                        assert_eq!(probe.bytes.get(), 1);
                        assert_eq!(probe.drops.get(), 1);
                        assert_eq!(
                            probe.stops.borrow().as_slice(),
                            &[Some(HttpErrorCode::READ_TIMEOUT)]
                        );
                        assert_eq!(
                            probe.lifecycle.borrow().as_slice(),
                            [
                                "read.poll",
                                "read.drop",
                                "source.stop",
                                "source.drop",
                                "event.cancel",
                            ]
                        );
                        assert!(byte_events.delivered.borrow().is_empty());
                        let events = byte_events.cancelled.borrow();
                        assert_eq!(events.len(), 1);
                        assert_eq!(events[0].0.stream_id, registered_stream.or(Some(stream)));
                        assert_eq!(events[0].0.event_type, ByteEventType::TransportWrite);
                        assert_eq!(events[0].1, HttpErrorCode::READ_TIMEOUT);
                        assert_eq!(handle.available_capacity(), 1);
                        let decoded = decode_client_frames(&wire.borrow().writes.concat());
                        assert_eq!(decoded.body_bytes, 0);
                        assert_eq!(decoded.resets, [(stream, H2ErrorCode::CANCEL)]);
                        assert!(decoded.errors.is_empty());
                    })
                    .await,
                );
                assert_eq!(byte_events.cancelled.borrow().len(), 1);
                assert!(byte_events.delivered.borrow().is_empty());
            })
        });
    }
}

#[test]
fn selected_request_timeout_survives_peer_reset_before_the_next_owner_poll() {
    run_on_worker(|context| {
        Box::pin(async move {
            let reset = Arc::new(tokio::sync::Notify::new());
            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            let (transport, wire) = FixtureTransport::new([
                FixtureRead::Data(server_settings_bytes(1)),
                FixtureRead::WaitData(
                    Arc::clone(&reset),
                    eligibility_frame(
                        &mut peer,
                        EgressFrame::Reset {
                            stream: StreamId::new(1),
                            code: H2ErrorCode::CANCEL,
                        },
                    ),
                ),
            ]);
            let mut config = options(context, transport);
            config.http2.max_concurrent_outgoing_streams = 1;
            let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
            let (upload, probe) = Upload::new(0, 1).held();
            let submission = handle.reserve_request().unwrap().submit(
                request_head(HttpMethod::Post, false, None),
                upload,
                CancellationToken::never(),
            );
            cancelled(
                drive(session, &handle, &[Arc::clone(&reset)], async {
                    let mut response = submission.await.unwrap().into_source_parts();
                    complete_credit_wait(|| {
                        probe.reads.get() == 1
                            && wire.borrow().reads.is_empty()
                            && decode_client_frames(&wire.borrow().writes.concat()).header_streams
                                == [StreamId::new(1)]
                    })
                    .await;
                    response
                        .2
                        .cancel(HttpError::new(HttpErrorCode::READ_TIMEOUT));
                    reset.notify_one();
                    let error = response
                        .1
                        .read_header_event()
                        .await
                        .err()
                        .expect("the earlier local cause survives the queued peer reset");
                    assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
                    drop(response);
                    complete_credit_wait(|| probe.drops.get() == 1 && handle.is_idle()).await;
                    assert_eq!(probe.reads.get(), 1);
                    assert_eq!(probe.produced.get(), 0);
                    assert_eq!(
                        probe.stops.borrow().as_slice(),
                        &[Some(HttpErrorCode::READ_TIMEOUT)]
                    );
                    assert_eq!(handle.available_capacity(), 1);
                    let decoded = decode_client_frames(&wire.borrow().writes.concat());
                    assert_eq!(decoded.header_streams, [StreamId::new(1)]);
                    assert_eq!(decoded.body_bytes, 0);
                    assert!(decoded.resets.is_empty());
                    assert!(decoded.errors.is_empty());
                })
                .await,
            );
        })
    });
}

#[test]
fn pending_or_suspended_upload_flushes_headers_and_control_without_body_readiness() {
    for suspended in [false, true] {
        run_on_worker(move |context| {
            Box::pin(async move {
                let body_ready = Arc::new(tokio::sync::Notify::new());
                let ping = Arc::new(tokio::sync::Notify::new());
                let respond = Arc::new(tokio::sync::Notify::new());
                let mut peer = Http2Codec::new(TransportDirection::Downstream);
                let (transport, wire) = FixtureTransport::new([
                    FixtureRead::Data(server_settings_bytes(1)),
                    FixtureRead::WaitData(
                        Arc::clone(&ping),
                        eligibility_frame(&mut peer, EgressFrame::PingRequest { data: Some(83) }),
                    ),
                    FixtureRead::WaitData(
                        Arc::clone(&respond),
                        response_head(&mut peer, StreamId::new(1), 200, true, Some(0)),
                    ),
                ]);
                let mut config = options(context, transport);
                config.http2.max_concurrent_outgoing_streams = 1;
                let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
                let mut upload = Upload::new(usize::from(!suspended), 1);
                if suspended {
                    let ready = Arc::clone(&body_ready);
                    upload
                        .tail
                        .push_back(HttpBodyEvent::suspend(Box::pin(async move {
                            ready.notified().await;
                            ResumeStatus::Signalled
                        })));
                    upload
                        .tail
                        .push_back(HttpBodyEvent::body(queue(b"u"), true));
                } else {
                    upload.gate = Some((0, Arc::clone(&body_ready)));
                }
                let (upload, probe) = upload.held();
                let submission = handle.reserve_request().unwrap().submit(
                    request_head(HttpMethod::Post, false, Some(1)),
                    upload,
                    CancellationToken::never(),
                );
                cancelled(
                    drive(
                        session,
                        &handle,
                        &[
                            Arc::clone(&body_ready),
                            Arc::clone(&ping),
                            Arc::clone(&respond),
                        ],
                        async {
                            let mut response = submission.await.unwrap().into_source_parts();
                            ping.notify_one();
                            complete_credit_wait(|| {
                                let decoded = decode_client_frames(&wire.borrow().writes.concat());
                                decoded.header_streams == [StreamId::new(1)]
                                    && decoded.ping_replies == [83]
                                    && probe.reads.get() == 1
                            })
                            .await;
                            assert_eq!(probe.produced.get(), usize::from(suspended));
                            assert_eq!(probe.bytes.get(), 0);
                            assert_eq!(probe.drops.get(), 0);
                            assert!(probe.stops.borrow().is_empty());
                            assert_eq!(
                                decode_client_frames(&wire.borrow().writes.concat()).body_bytes,
                                0
                            );
                            body_ready.notify_one();
                            complete_credit_wait(|| {
                                complete_credit_wire(true, &wire).completed == [StreamId::new(1)]
                                    && probe.drops.get() == 1
                            })
                            .await;
                            respond.notify_one();
                            assert!(response.1.read_header_event().await.unwrap().eom());
                            drop(response);
                            complete_credit_wait(|| handle.is_idle()).await;
                            assert_eq!(probe.drops.get(), 1);
                            assert!(probe.stops.borrow().is_empty());
                            let decoded = decode_client_frames(&wire.borrow().writes.concat());
                            assert_eq!(decoded.body_data, b"u");
                            assert!(decoded.resets.is_empty());
                            assert!(decoded.errors.is_empty());
                        },
                    )
                    .await,
                );
            })
        });
    }
}

#[test]
fn upload_pending_token_cancellation_and_explicit_status_keep_their_retirement_order() {
    for token_cancel in [false, true] {
        run_on_worker(move |context| {
            Box::pin(async move {
                let ready = Arc::new(tokio::sync::Notify::new());
                let cancellation = Rc::new(CancellationSource::new());
                let request: Rc<RefCell<Option<crate::owned_h2::OwnedH2UpstreamRequest>>> =
                    Rc::new(RefCell::new(None));
                let (transport, wire) =
                    FixtureTransport::new([FixtureRead::Data(server_settings_bytes(1))]);
                let (session, handle) =
                    OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
                let mut upload = Upload::new(0, 1);
                let observed = Rc::clone(&upload.probe);
                let drop_read = UploadReadDrop(Rc::clone(&observed));
                let cancel = Rc::clone(&cancellation);
                let cancel_request = Rc::clone(&request);
                let gate = Arc::clone(&ready);
                upload.body_read = Some(Box::pin(async move {
                    let _drop_read = drop_read;
                    gate.notified().await;
                    poll_fn(move |_| {
                        observed.lifecycle.borrow_mut().push("read.poll");
                        if token_cancel {
                            cancel.request_cancellation();
                        } else {
                            let request = cancel_request.borrow();
                            let request = request.as_ref().unwrap();
                            request.cancel(HttpError::new(HttpErrorCode::READ_TIMEOUT));
                            request.cancel(HttpError::new(HttpErrorCode::CORO_CANCELLED));
                        }
                        Poll::<Result<HttpBodyEvent, HttpError>>::Pending
                    })
                    .await
                }));
                let (upload, probe) = upload.held();
                let submission = handle.reserve_request().unwrap().submit(
                    request_head(HttpMethod::Post, false, None),
                    upload,
                    cancellation.token(),
                );
                cancelled(
                    drive(session, &handle, &[Arc::clone(&ready)], async {
                        let mut response = submission.await.unwrap().into_source_parts();
                        *request.borrow_mut() = Some(response.2.clone());
                        complete_credit_wait(|| probe.reads.get() == 1).await;
                        ready.notify_one();
                        let error =
                            response.1.read_header_event().await.err().expect(
                                "body-poll cancellation reaches the original response source",
                            );
                        let expected = if token_cancel {
                            HttpErrorCode::CORO_CANCELLED
                        } else {
                            HttpErrorCode::READ_TIMEOUT
                        };
                        assert_eq!(error.code(), expected);
                        drop(response);
                        complete_credit_wait(|| probe.drops.get() == 1 && handle.is_idle()).await;
                        assert_eq!(probe.reads.get(), 1);
                        assert_eq!(probe.produced.get(), 0);
                        assert_eq!(probe.stops.borrow().as_slice(), &[Some(expected)]);
                        assert_eq!(
                            cancellation.token().is_cancellation_requested(),
                            token_cancel
                        );
                        let expected_order = if token_cancel {
                            ["read.poll", "read.drop", "source.stop", "source.drop"]
                        } else {
                            ["read.poll", "source.stop", "source.drop", "read.drop"]
                        };
                        assert_eq!(probe.lifecycle.borrow().as_slice(), expected_order);
                        let decoded = decode_client_frames(&wire.borrow().writes.concat());
                        assert_eq!(decoded.body_bytes, 0);
                        assert_eq!(decoded.resets, [(StreamId::new(1), H2ErrorCode::CANCEL)]);
                        assert!(decoded.errors.is_empty());
                    })
                    .await,
                );
            })
        });
    }
}

#[test]
fn upload_body_poll_panic_retires_the_read_and_joins_native_io_before_resuming() {
    run_on_worker(|context| {
        Box::pin(async move {
            let release_callback = Arc::new(tokio::sync::Notify::new());
            let release_write = Arc::new(tokio::sync::Notify::new());
            let release_read = Arc::new(tokio::sync::Notify::new());
            let (transport, wire) = FixtureTransport::new([
                FixtureRead::Data(server_settings_bytes(1)),
                FixtureRead::WaitDataEof(Arc::clone(&release_read), Vec::new()),
            ]);
            wire.borrow_mut().write_gate = Some(Arc::clone(&release_write));
            let (session, handle) =
                OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
            let mut upload = Upload::new(0, 1);
            let drop_read = UploadReadDrop(Rc::clone(&upload.probe));
            let ready = Arc::clone(&release_callback);
            upload.body_read = Some(Box::pin(async move {
                let _drop_read = drop_read;
                ready.notified().await;
                panic_any("upstream body poll");
            }));
            let (upload, probe) = upload.held();
            let submission = handle.reserve_request().unwrap().submit(
                request_head(HttpMethod::Post, false, None),
                upload,
                CancellationToken::never(),
            );
            let resumed = Cell::new(false);
            let run = async {
                let result = eligibility_catch_unwind(session.run()).await;
                resumed.set(true);
                result
            };
            let scenario = async {
                let outcome =
                    eligibility_catch_unwind(tokio::time::timeout(Duration::from_secs(5), async {
                        complete_credit_wait(|| {
                            let wire = wire.borrow();
                            wire.write_gate_pending.is_some_and(|length| length > 0)
                                && wire.reads.is_empty()
                                && probe.reads.get() == 1
                        })
                        .await;
                        release_callback.notify_one();
                        complete_credit_wait(|| probe.drops.get() == 1).await;
                        assert_eq!(
                            probe.lifecycle.borrow().as_slice(),
                            ["read.drop", "source.drop"]
                        );
                        assert!(probe.stops.borrow().is_empty());
                        drop(submission);
                        assert!(!resumed.get(), "held I/O retains the source panic");
                        assert!(wire.borrow().writes.is_empty());
                        release_write.notify_one();
                        complete_credit_wait(|| wire.borrow().writes.len() == 1).await;
                        assert!(!resumed.get(), "the admitted read must also finish");
                    }))
                    .await;
                release_callback.notify_one();
                release_write.notify_one();
                release_read.notify_one();
                if !matches!(&outcome, Ok(Ok(()))) {
                    handle.cancel();
                }
                outcome
            };
            let (result, outcome) = tokio::time::timeout(Duration::from_secs(10), async {
                tokio::join!(run, scenario)
            })
            .await
            .expect("source unwind joins the admitted read and write");
            match outcome {
                Ok(result) => result.expect("source panic reaches joined cleanup"),
                Err(payload) => resume_unwind(payload),
            }
            let panic = result.expect_err("the original source panic is resumed");
            assert_eq!(panic.downcast_ref::<&str>(), Some(&"upstream body poll"));
            assert_eq!(probe.reads.get(), 1);
            assert_eq!(probe.produced.get(), 0);
            assert_eq!(probe.drops.get(), 1);
            let wire = wire.borrow();
            assert_eq!(wire.writes.len(), 1);
            assert_eq!(Some(wire.writes[0].len()), wire.write_gate_pending);
            assert!(wire.transport_dropped);
            assert_eq!(wire.close, FixtureClose::Reset);
        })
    });
}

#[test]
fn an_owned_upload_polls_the_submitted_filter_before_serializing_its_body() {
    struct ObserveBody(Rc<Cell<usize>>);

    impl HttpSourceFilterHooks for ObserveBody {
        fn filter_name(&self) -> &'static str {
            "ObserveUpload"
        }

        fn read_body_event(
            &mut self,
            downstream: LocalSourceFuture<Result<HttpBodyEvent, HttpError>>,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            let reads = Rc::clone(&self.0);
            Box::pin(async move {
                let event = downstream.await?;
                reads.set(reads.get() + 1);
                Ok(event)
            })
        }
    }

    run_on_worker(|context| {
        Box::pin(async move {
            let respond = Arc::new(tokio::sync::Notify::new());
            let (transport, wire) = FixtureTransport::new([
                FixtureRead::Data(server_settings_bytes(1)),
                FixtureRead::WaitData(
                    Arc::clone(&respond),
                    server_empty_responses_bytes([StreamId::new(1)]),
                ),
            ]);
            let (session, handle) =
                OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
            let upload = Upload::new(2, 1);
            let probe = Rc::clone(&upload.probe);
            let filtered = Rc::new(Cell::new(0));
            let mut filter = HttpSourceFilter::new(ObserveBody(Rc::clone(&filtered)));
            assert!(filter.set_source(Box::new(upload)).is_ok());
            let submission = handle.reserve_request().unwrap().submit(
                request_head(HttpMethod::Post, false, Some(2)),
                HttpSourceHolder::new(Box::new(filter)),
                CancellationToken::never(),
            );
            cancelled(
                drive(session, &handle, &[Arc::clone(&respond)], async {
                    let mut response = submission.await.unwrap().into_source_parts();
                    complete_credit_wait(|| {
                        complete_credit_wire(true, &wire).completed == [StreamId::new(1)]
                    })
                    .await;
                    assert_eq!(filtered.get(), 2);
                    assert_eq!(probe.reads.get(), 2);
                    assert_eq!(probe.produced.get(), 2);
                    assert_eq!(probe.drops.get(), 1);
                    assert!(probe.stops.borrow().is_empty());
                    let decoded = decode_client_frames(&wire.borrow().writes.concat());
                    assert_eq!(decoded.body_data, b"uu");
                    assert!(decoded.resets.is_empty());
                    respond.notify_one();
                    assert!(response.1.read_header_event().await.unwrap().eom());
                    drop(response);
                    complete_credit_wait(|| handle.is_idle()).await;
                })
                .await,
            );
        })
    });
}

#[test]
fn unclaimed_acknowledgment_drop_retires_response_and_preserves_first_panic() {
    struct AckDropPanic(Arc<()>);

    struct ArmedWake {
        delegate: std::task::Waker,
        armed: Arc<std::sync::atomic::AtomicBool>,
        marker: Arc<()>,
    }

    impl std::task::Wake for ArmedWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            if self.armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
                std::panic::panic_any(AckDropPanic(Arc::clone(&self.marker)));
            }
            self.delegate.wake_by_ref();
        }
    }

    for deliver_ack in [false, true] {
        run_on_worker(move |context| {
            Box::pin(async move {
                let stream = StreamId::new(1);
                let mut peer = Http2Codec::new(TransportDirection::Downstream);
                let respond = Arc::new(tokio::sync::Notify::new());
                let mut response_bytes = response_head(&mut peer, stream, 200, false, Some(8));
                response_bytes.extend(eligibility_frame(
                    &mut peer,
                    EgressFrame::Body {
                        stream,
                        body: queue(b"body"),
                        padding: None,
                        end_stream: false,
                    },
                ));
                response_bytes.extend(eligibility_frame(
                    &mut peer,
                    EgressFrame::PingRequest { data: Some(93) },
                ));
                let (transport, wire) = FixtureTransport::new([
                    FixtureRead::Data(server_settings_bytes(1)),
                    FixtureRead::WaitData(Arc::clone(&respond), response_bytes),
                ]);
                let mut config = options(context, transport);
                config.http2.max_concurrent_outgoing_streams = 1;
                let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
                let observer = Rc::new(AdmissionProbe::default());
                handle.set_observer(observer.clone()).unwrap();
                let (upload, probe) = Upload::new(0, 1).held();
                let encoded = Rc::new(Cell::new(false));
                let observed = Rc::clone(&encoded);
                let mut head = request_head(HttpMethod::Post, false, None);
                head.set_egress_headers_callback(move |_| observed.set(true));
                let mut submission = Some(handle.reserve_request().unwrap().submit(
                    head,
                    upload,
                    CancellationToken::never(),
                ));
                let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let marker = Arc::new(());
                let operation = async {
                    complete_credit_wait(|| encoded.get() && probe.reads.get() == 1).await;
                    let mut response = if deliver_ack {
                        Some(submission.take().unwrap().await.unwrap())
                    } else {
                        None
                    };
                    respond.notify_one();
                    complete_credit_wait(|| {
                        decode_client_frames(&wire.borrow().writes.concat()).ping_replies == [93]
                    })
                    .await;
                    let producer = response.as_ref().map(|response| {
                        let source = response.source.source().unwrap();
                        assert_eq!(source.body_bytes_buffered(), 4);
                        source.producer()
                    });
                    assert!(!handle.is_idle());
                    armed.store(true, std::sync::atomic::Ordering::SeqCst);
                    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        if deliver_ack {
                            drop(response.take());
                        } else {
                            drop(submission.take());
                        }
                    }))
                    .expect_err("the original retirement notification panic must escape");
                    assert!(Arc::ptr_eq(
                        &panic.downcast_ref::<AckDropPanic>().unwrap().0,
                        &marker,
                    ));
                    drop(panic);
                    assert!(!armed.load(std::sync::atomic::Ordering::SeqCst));
                    if let Some(producer) = producer {
                        assert!(
                            !producer.is_live(),
                            "the original source is destroyed after stop panics"
                        );
                    }
                    complete_credit_wait(|| probe.drops.get() == 1 && handle.is_idle()).await;
                    assert_eq!(probe.reads.get(), 1);
                    assert_eq!(
                        probe.stops.borrow().as_slice(),
                        &[Some(if deliver_ack {
                            HttpErrorCode::CANCEL
                        } else {
                            HttpErrorCode::CORO_CANCELLED
                        })],
                    );
                    assert_eq!(handle.available_capacity(), 1);
                    assert_eq!(observer.released.borrow().as_slice(), &[stream]);
                    let decoded = decode_client_frames(&wire.borrow().writes.concat());
                    assert_eq!(decoded.header_streams, [stream]);
                    assert_eq!(decoded.body_bytes, 0);
                    assert_eq!(decoded.resets, [(stream, H2ErrorCode::CANCEL)]);
                    assert!(decoded.errors.is_empty());
                };
                let exercise = async {
                    let outcome = eligibility_catch_unwind(tokio::time::timeout(
                        Duration::from_secs(5),
                        operation,
                    ))
                    .await;
                    armed.store(false, std::sync::atomic::Ordering::SeqCst);
                    respond.notify_one();
                    handle.cancel();
                    outcome
                };
                let mut run = Box::pin(session.run());
                let (result, outcome) = tokio::time::timeout(Duration::from_secs(10), async {
                    tokio::join!(
                        poll_fn(|context| {
                            let waker = std::task::Waker::from(Arc::new(ArmedWake {
                                delegate: context.waker().clone(),
                                armed: Arc::clone(&armed),
                                marker: Arc::clone(&marker),
                            }));
                            run.as_mut()
                                .poll(&mut std::task::Context::from_waker(&waker))
                        }),
                        exercise,
                    )
                })
                .await
                .expect("retirement panic does not prevent native cleanup from joining");
                match outcome {
                    Ok(result) => result.expect("acknowledgment ownership makes progress"),
                    Err(panic) => resume_unwind(panic),
                }
                cancelled(result);
            })
        });
    }
}

#[derive(Default)]
struct FreshUploadProbe {
    bytes: Cell<usize>,
    acknowledged: Cell<usize>,
    completed: RefCell<Vec<Option<HttpErrorCode>>>,
    drops: Cell<usize>,
}

struct FreshUploadCallback(Rc<FreshUploadProbe>);
impl HttpStreamSourceCallback for FreshUploadCallback {
    fn bytes_processed(&mut self, _: StreamId, amount: usize, acknowledge: usize) {
        self.0.bytes.set(self.0.bytes.get() + amount);
        self.0
            .acknowledged
            .set(self.0.acknowledged.get() + acknowledge);
    }
    fn source_complete(&mut self, _: StreamId, error: Option<HttpError>) {
        self.0
            .completed
            .borrow_mut()
            .push(error.map(|error| error.code()));
    }
}
impl Drop for FreshUploadCallback {
    fn drop(&mut self) {
        self.0.drops.set(self.0.drops.get() + 1);
    }
}

fn fresh_upload(
    head: &mut HttpHeaderEvent,
    window: u32,
    host: Host,
) -> (
    HttpIngressBody,
    HttpStreamSourceProducer,
    Rc<FreshUploadProbe>,
) {
    let probe = Rc::new(FreshUploadProbe::default());
    let source = HttpIngressBody::new(
        StreamId::new(1),
        FreshUploadCallback(Rc::clone(&probe)),
        window,
        host,
    )
    .unwrap();
    assert!(source.initialize_body_only(head.headers_mut(), false));
    let producer = source.producer();
    (source, producer, probe)
}

#[test]
fn fresh_upload_holds_one_event_with_held_write_and_zero_credit() {
    for fragmented in [false, true] {
        run_on_worker(move |context| {
            Box::pin(async move {
                let mut peer = Http2Codec::new(TransportDirection::Downstream);
                let initial = eligibility_frame(
                    &mut peer,
                    EgressFrame::Settings {
                        settings: &[
                            roxygen_codec::CodecSetting {
                                identifier: SettingsId::InitialWindowSize.as_u16(),
                                value: 0,
                            },
                            roxygen_codec::CodecSetting {
                                identifier: SettingsId::MaxConcurrentStreams.as_u16(),
                                value: 1,
                            },
                        ],
                    },
                );
                let ping = Arc::new(tokio::sync::Notify::new());
                let credit = Arc::new(tokio::sync::Notify::new());
                let respond = Arc::new(tokio::sync::Notify::new());
                let release_write = Arc::new(tokio::sync::Notify::new());
                let stream = StreamId::new(1);
                let mut update = eligibility_frame(
                    &mut peer,
                    EgressFrame::WindowUpdate {
                        stream,
                        amount: 65_536,
                    },
                );
                update.extend(eligibility_frame(
                    &mut peer,
                    EgressFrame::WindowUpdate {
                        stream: StreamId::UNSPECIFIED,
                        amount: 1,
                    },
                ));
                let (transport, wire) = FixtureTransport::new([
                    FixtureRead::Data(initial),
                    FixtureRead::WaitData(
                        Arc::clone(&ping),
                        eligibility_frame(&mut peer, EgressFrame::PingRequest { data: Some(94) }),
                    ),
                    FixtureRead::WaitData(Arc::clone(&credit), update),
                    FixtureRead::WaitData(
                        Arc::clone(&respond),
                        response_head(&mut peer, stream, 200, true, Some(0)),
                    ),
                ]);
                wire.borrow_mut().write_gate = Some(Arc::clone(&release_write));
                let size = if fragmented { 300 } else { 65_536 };
                let mut head = request_head(HttpMethod::Post, false, Some(size));
                let (upload, producer, probe) =
                    fresh_upload(&mut head, 1 << 20, context.host().clone());
                if fragmented {
                    for _ in 0..size {
                        producer.body(queue(b"u"), 1, false);
                        assert!(producer.padding(1));
                    }
                    producer.body(BufQueue::new(0), 0, true);
                } else {
                    producer.body(queue(&vec![b'u'; size]), 0, true);
                }
                let (session, handle) =
                    OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
                cancelled(
                    drive(
                        session,
                        &handle,
                        &[
                            Arc::clone(&ping),
                            Arc::clone(&credit),
                            Arc::clone(&respond),
                            Arc::clone(&release_write),
                        ],
                        async {
                            complete_credit_wait(|| handle.available_capacity() == 1).await;
                            let response = handle
                                .reserve_request()
                                .unwrap()
                                .submit(head, upload, CancellationToken::never())
                                .await
                                .unwrap()
                                .into_source_parts();
                            let consumed = if fragmented { 1 } else { 65_535 };
                            complete_credit_wait(|| {
                                probe.bytes.get() >= consumed
                                    && wire.borrow().write_gate_pending.is_some()
                            })
                            .await;
                            for _ in 0..4 {
                                tokio::task::yield_now().await;
                            }
                            assert_eq!(probe.bytes.get(), consumed);
                            assert_eq!(
                                producer.body_bytes_buffered(),
                                Some((size - consumed) as u32)
                            );
                            assert!(probe.completed.borrow().is_empty());
                            assert_eq!(probe.drops.get(), 0);
                            assert!(wire.borrow().writes.is_empty());
                            release_write.notify_one();
                            ping.notify_one();
                            complete_credit_wait(|| {
                                decode_client_frames(&wire.borrow().writes.concat()).ping_replies
                                    == [94]
                            })
                            .await;
                            assert_eq!(probe.bytes.get(), consumed);
                            assert_eq!(
                                decode_client_frames(&wire.borrow().writes.concat()).body_bytes,
                                0
                            );
                            assert!(!response.2.capacity_released());
                            credit.notify_one();
                            complete_credit_wait(|| {
                                complete_credit_wire(true, &wire).completed == [stream]
                            })
                            .await;
                            assert_eq!(probe.bytes.get(), size);
                            assert_eq!(probe.completed.borrow().as_slice(), &[None]);
                            assert_eq!(probe.drops.get(), 1);
                            assert!(!producer.is_live());
                            respond.notify_one();
                            response
                                .1
                                .ingress_completion()
                                .unwrap()
                                .wait()
                                .await
                                .unwrap();
                            complete_credit_wait(|| response.2.capacity_released()).await;
                            drop(response);
                            complete_credit_wait(|| handle.is_idle()).await;
                            let decoded = decode_client_frames(&wire.borrow().writes.concat());
                            assert_eq!(decoded.body_data, vec![b'u'; size]);
                            assert!(decoded.resets.is_empty());
                            assert!(decoded.errors.is_empty());
                        },
                    )
                    .await,
                );
            })
        });
    }
}

#[test]
fn fresh_upload_wait_preserves_control_early_response_and_resume_cancellation() {
    use roxygen_runtime::host::LocalFuture;
    use roxygen_runtime::host::LocalHost;
    use roxygen_runtime::host::TokioHost;

    struct Timer {
        sleep: LocalFuture,
        cancel: Rc<RefCell<Option<crate::owned_h2::OwnedH2UpstreamRequest>>>,
        retired: Rc<Cell<usize>>,
    }
    impl Future for Timer {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<()> {
            self.sleep.as_mut().poll(cx)
        }
    }
    impl Drop for Timer {
        fn drop(&mut self) {
            self.retired.set(self.retired.get() + 1);
            if let Some(request) = self.cancel.borrow_mut().take() {
                request.cancel(HttpError::new(HttpErrorCode::READ_TIMEOUT));
            }
        }
    }
    struct TimerHost {
        cancel: Rc<RefCell<Option<crate::owned_h2::OwnedH2UpstreamRequest>>>,
        registered: Rc<Cell<usize>>,
        retired: Rc<Cell<usize>>,
    }
    impl LocalHost for TimerHost {
        fn spawn_local(&self, future: LocalFuture) {
            TokioHost.spawn_local(future);
        }
        fn now(&self) -> std::time::Instant {
            TokioHost.now()
        }
        fn sleep_until(&self, deadline: std::time::Instant) -> LocalFuture {
            self.registered.set(self.registered.get() + 1);
            Box::pin(Timer {
                sleep: TokioHost.sleep_until(deadline),
                cancel: Rc::clone(&self.cancel),
                retired: Rc::clone(&self.retired),
            })
        }
        fn yield_to_io(&self) -> LocalFuture {
            TokioHost.yield_to_io()
        }
    }

    for cancel_on_resume in [false, true] {
        run_on_worker(move |context| {
            Box::pin(async move {
                let ping = Arc::new(tokio::sync::Notify::new());
                let respond = Arc::new(tokio::sync::Notify::new());
                let mut peer = Http2Codec::new(TransportDirection::Downstream);
                let stream = StreamId::new(1);
                let (transport, wire) = FixtureTransport::new([
                    FixtureRead::Data(server_settings_bytes(1)),
                    FixtureRead::WaitData(
                        Arc::clone(&ping),
                        eligibility_frame(&mut peer, EgressFrame::PingRequest { data: Some(95) }),
                    ),
                    FixtureRead::WaitData(
                        Arc::clone(&respond),
                        response_head(&mut peer, stream, 200, true, Some(0)),
                    ),
                ]);
                let cancel = Rc::new(RefCell::new(None));
                let registered = Rc::new(Cell::new(0));
                let retired = Rc::new(Cell::new(0));
                let host = Host::new(Rc::new(TimerHost {
                    cancel: Rc::clone(&cancel),
                    registered: Rc::clone(&registered),
                    retired: Rc::clone(&retired),
                }));
                let mut head = request_head(HttpMethod::Post, false, Some(1));
                let (upload, producer, probe) = fresh_upload(&mut head, 1, host);
                let receipt = upload.ingress_completion().unwrap();
                assert!(producer.set_read_timeout(Duration::from_secs(60)));
                let mut config = options(context, transport);
                config.http2.max_concurrent_outgoing_streams = 1;
                let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
                let submission = handle.reserve_request().unwrap().submit(
                    head,
                    upload,
                    CancellationToken::never(),
                );
                cancelled(drive(session, &handle, &[Arc::clone(&ping), Arc::clone(&respond)], async {
                let response = submission.await.unwrap().into_source_parts();
                complete_credit_wait(|| registered.get() == 1).await;
                ping.notify_one();
                complete_credit_wait(|| {
                    let decoded = decode_client_frames(&wire.borrow().writes.concat());
                    decoded.header_streams == [stream] && decoded.ping_replies == [95]
                }).await;
                respond.notify_one();
                response.1.ingress_completion().unwrap().wait().await.unwrap();
                assert!(!response.2.capacity_released());
                assert_eq!(handle.available_capacity(), 0);
                assert_eq!(probe.bytes.get(), 0);
                assert!(probe.completed.borrow().is_empty());
                assert!(receipt.snapshot().is_none());
                assert!(producer.is_live());
                if cancel_on_resume { *cancel.borrow_mut() = Some(response.2.clone()); }
                producer.body(queue(b"u"), 0, !cancel_on_resume);
                complete_credit_wait(|| probe.drops.get() == 1).await;
                assert_eq!(registered.get(), 1);
                assert_eq!(retired.get(), 1);
                assert!(!producer.is_live());
                if cancel_on_resume {
                    assert_eq!(receipt.snapshot().unwrap().unwrap_err().code(), HttpErrorCode::READ_TIMEOUT);
                    assert_eq!(probe.completed.borrow().as_slice(), &[Some(HttpErrorCode::READ_TIMEOUT)]);
                    assert_eq!(probe.acknowledged.get(), 0, "cancellation after retiring the wait precedes another source dequeue");
                } else {
                    assert!(receipt.snapshot().unwrap().is_ok());
                    assert_eq!(probe.completed.borrow().as_slice(), &[None]);
                    complete_credit_wait(|| response.2.capacity_released()).await;
                }
                drop(response);
                complete_credit_wait(|| handle.is_idle()).await;
                let decoded = decode_client_frames(&wire.borrow().writes.concat());
                if cancel_on_resume {
                    assert_eq!(decoded.body_bytes, 0);
                    assert_eq!(decoded.resets, [(stream, H2ErrorCode::CANCEL)]);
                } else {
                    assert_eq!(decoded.body_data, b"u");
                    assert!(decoded.resets.is_empty());
                }
                assert!(decoded.errors.is_empty());
            }).await);
            })
        });
    }
}

#[test]
fn fresh_upload_callback_panic_joins_native_io_before_resuming() {
    struct Callback {
        probe: Rc<FreshUploadProbe>,
        producer: Rc<RefCell<Option<HttpStreamSourceProducer>>>,
        panicked: bool,
    }
    impl HttpStreamSourceCallback for Callback {
        fn bytes_processed(&mut self, _: StreamId, amount: usize, _: usize) {
            self.probe.bytes.set(self.probe.bytes.get() + amount);
            self.producer
                .borrow()
                .as_ref()
                .unwrap()
                .stop_reading(Some(HttpErrorCode::READ_TIMEOUT));
            self.panicked = true;
            panic_any("fresh upload credit");
        }
        fn source_complete(&mut self, _: StreamId, error: Option<HttpError>) {
            self.probe
                .completed
                .borrow_mut()
                .push(error.map(|error| error.code()));
            if self.panicked {
                panic_any("secondary fresh completion");
            }
        }
    }
    impl Drop for Callback {
        fn drop(&mut self) {
            self.probe.drops.set(self.probe.drops.get() + 1);
        }
    }

    run_on_worker(|context| {
        Box::pin(async move {
            let release_write = Arc::new(tokio::sync::Notify::new());
            let release_read = Arc::new(tokio::sync::Notify::new());
            let (transport, wire) = FixtureTransport::new([
                FixtureRead::Data(server_settings_bytes(1)),
                FixtureRead::WaitDataEof(Arc::clone(&release_read), Vec::new()),
            ]);
            wire.borrow_mut().write_gate = Some(Arc::clone(&release_write));
            let probe = Rc::new(FreshUploadProbe::default());
            let callback_producer = Rc::new(RefCell::new(None));
            let upload = HttpIngressBody::new(
                StreamId::new(1),
                Callback {
                    probe: Rc::clone(&probe),
                    producer: Rc::clone(&callback_producer),
                    panicked: false,
                },
                1,
                context.host().clone(),
            )
            .unwrap();
            let producer = upload.producer();
            *callback_producer.borrow_mut() = Some(producer.clone());
            let receipt = upload.ingress_completion().unwrap();
            let mut head = request_head(HttpMethod::Post, false, None);
            assert!(upload.initialize_body_only(head.headers_mut(), false));
            let (session, handle) =
                OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
            let submission =
                handle
                    .reserve_request()
                    .unwrap()
                    .submit(head, upload, CancellationToken::never());
            let resumed = Cell::new(false);
            let run = async {
                let result = eligibility_catch_unwind(session.run()).await;
                resumed.set(true);
                result
            };
            let scenario = async {
                let outcome =
                    eligibility_catch_unwind(tokio::time::timeout(Duration::from_secs(5), async {
                        complete_credit_wait(|| {
                            wire.borrow()
                                .write_gate_pending
                                .is_some_and(|length| length > 0)
                                && wire.borrow().reads.is_empty()
                        })
                        .await;
                        producer.body(queue(b"u"), 0, false);
                        complete_credit_wait(|| probe.drops.get() == 1).await;
                        assert_eq!(probe.bytes.get(), 1);
                        assert_eq!(
                            probe.completed.borrow().as_slice(),
                            &[Some(HttpErrorCode::READ_TIMEOUT)]
                        );
                        assert_eq!(
                            receipt.snapshot().unwrap().unwrap_err().code(),
                            HttpErrorCode::READ_TIMEOUT
                        );
                        assert!(!producer.is_live());
                        drop(submission);
                        assert!(!resumed.get(), "held native I/O retains the callback panic");
                        assert!(wire.borrow().writes.is_empty());
                        release_write.notify_one();
                        complete_credit_wait(|| wire.borrow().writes.len() == 1).await;
                        assert!(!resumed.get(), "the admitted native read must also finish");
                    }))
                    .await;
                release_write.notify_one();
                release_read.notify_one();
                if !matches!(&outcome, Ok(Ok(()))) {
                    handle.cancel();
                }
                outcome
            };
            let (result, outcome) = tokio::time::timeout(Duration::from_secs(10), async {
                tokio::join!(run, scenario)
            })
            .await
            .expect("fresh callback unwind joins both native operations");
            match outcome {
                Ok(result) => result.expect("fresh callback panic reaches joined cleanup"),
                Err(payload) => resume_unwind(payload),
            }
            let panic = result.expect_err("the original fresh callback panic is resumed");
            assert_eq!(panic.downcast_ref::<&str>(), Some(&"fresh upload credit"));
            assert_eq!(probe.completed.borrow().len(), 1);
            assert_eq!(probe.drops.get(), 1);
            let wire = wire.borrow();
            assert_eq!(wire.writes.len(), 1);
            assert_eq!(Some(wire.writes[0].len()), wire.write_gate_pending);
            assert!(wire.transport_dropped);
            assert_eq!(wire.close, FixtureClose::Reset);
        })
    });
}

#[test]
fn request_header_priorities_preserve_buffered_upload_wire_order() {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            let initial = eligibility_frame(
                &mut peer,
                EgressFrame::Settings {
                    settings: &[
                        CodecSetting {
                            identifier: SettingsId::InitialWindowSize.as_u16(),
                            value: 0,
                        },
                        CodecSetting {
                            identifier: SettingsId::MaxConcurrentStreams.as_u16(),
                            value: 3,
                        },
                    ],
                },
            );
            let first_credit = Arc::new(tokio::sync::Notify::new());
            let last_credit = Arc::new(tokio::sync::Notify::new());
            let respond = Arc::new(tokio::sync::Notify::new());
            let mut reads = vec![FixtureRead::Data(initial)];
            for (ready, value) in [(&first_credit, 1), (&last_credit, 2)] {
                reads.push(FixtureRead::WaitData(
                    Arc::clone(ready),
                    eligibility_frame(
                        &mut peer,
                        EgressFrame::Settings {
                            settings: &[CodecSetting {
                                identifier: SettingsId::InitialWindowSize.as_u16(),
                                value,
                            }],
                        },
                    ),
                ));
            }
            let replies = [StreamId::new(1), StreamId::new(3), StreamId::new(5)]
                .into_iter()
                .flat_map(|stream| response_head(&mut peer, stream, 200, true, Some(0)))
                .collect();
            reads.push(FixtureRead::WaitData(Arc::clone(&respond), replies));
            let (transport, wire) = FixtureTransport::new(reads);
            let (session, handle) =
                OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
            cancelled(
                drive(
                    session,
                    &handle,
                    &[
                        Arc::clone(&first_credit),
                        Arc::clone(&last_credit),
                        Arc::clone(&respond),
                    ],
                    async {
                        complete_credit_wait(|| handle.available_capacity() == 3).await;
                        let mut responses = Vec::new();
                        let mut probes = Vec::new();
                        for priority in [
                            HttpPriority::from_urgency_and_incremental(6, true),
                            HttpPriority::from_urgency_and_incremental(2, true),
                            HttpPriority::from_urgency_and_incremental(2, false),
                        ] {
                            let (upload, probe) = Upload::new(2, 2).held();
                            let mut headers = request_head(HttpMethod::Post, false, Some(2));
                            headers.headers_mut().set_http_priority(priority);
                            responses.push(
                                handle
                                    .reserve_request()
                                    .unwrap()
                                    .submit(headers, upload, CancellationToken::never())
                                    .await
                                    .unwrap()
                                    .into_source_parts(),
                            );
                            probes.push(probe);
                        }
                        complete_credit_wait(|| {
                            probes
                                .iter()
                                .all(|probe| probe.bytes.get() == 2 && probe.drops.get() == 1)
                        })
                        .await;
                        let before = complete_credit_wire(true, &wire);
                        assert_eq!(before.frames.body_bytes, 0);
                        assert!(before.completed.is_empty());
                        first_credit.notify_one();
                        complete_credit_wait(|| {
                            complete_credit_wire(true, &wire).frames.body_bytes == 3
                        })
                        .await;
                        let order = [StreamId::new(5), StreamId::new(3), StreamId::new(1)];
                        let partial = complete_credit_wire(true, &wire);
                        assert_eq!(partial.frames.body_streams, order);
                        assert_eq!(partial.frames.body_data, b"uuu");
                        assert!(partial.completed.is_empty());
                        assert!(
                            responses
                                .iter()
                                .all(|response| !response.2.capacity_released())
                        );
                        last_credit.notify_one();
                        complete_credit_wait(|| {
                            complete_credit_wire(true, &wire).completed.len() == 3
                        })
                        .await;
                        let sent = complete_credit_wire(true, &wire);
                        assert_eq!(sent.frames.body_streams, order.repeat(2));
                        assert_eq!(sent.frames.body_data, b"uuuuuu");
                        assert_eq!(sent.completed, order);
                        respond.notify_one();
                        for mut response in responses {
                            let mut headers = response.1.read_header_event().await.unwrap();
                            assert_eq!(headers.headers_mut().status_code().unwrap(), 200);
                            assert!(headers.eom());
                        }
                        complete_credit_wait(|| handle.is_idle()).await;
                        for probe in probes {
                            assert_eq!(probe.produced.get(), 1);
                            assert_eq!(probe.bytes.get(), 2);
                            assert_eq!(probe.drops.get(), 1);
                            assert!(probe.stops.borrow().is_empty());
                        }
                        let sent = complete_credit_wire(true, &wire);
                        assert_eq!(sent.frames.body_streams, order.repeat(2));
                        assert_eq!(sent.completed, order);
                        assert!(sent.frames.resets.is_empty());
                        assert!(sent.frames.errors.is_empty());
                    },
                )
                .await,
            );
        })
    });
}

#[test]
fn completed_upload_keeps_token_cancellation_wake_while_write_is_held() {
    completed_upload_keeps_cancellation_wake_while_write_is_held(false);
}

#[test]
fn completed_upload_keeps_direct_cancellation_wake_while_write_is_held() {
    completed_upload_keeps_cancellation_wake_while_write_is_held(true);
}

fn completed_upload_keeps_cancellation_wake_while_write_is_held(direct_control: bool) {
    use std::sync::atomic::AtomicBool;
    use std::task::Context;
    use std::task::Wake;
    use std::task::Waker;

    struct RecordingWake(AtomicBool);

    impl Wake for RecordingWake {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    run_on_worker(move |context| {
        Box::pin(async move {
            let stream = StreamId::new(1);
            let release_upload = Arc::new(tokio::sync::Notify::new());
            let release_write = Arc::new(tokio::sync::Notify::new());
            let release_read = Arc::new(tokio::sync::Notify::new());
            let (transport, wire) = FixtureTransport::new([
                FixtureRead::SegmentedData(queue(&server_settings_bytes(1))),
                FixtureRead::WaitDataEof(Arc::clone(&release_read), Vec::new()),
            ]);
            let mut config = options(context, transport);
            config.http2.max_concurrent_outgoing_streams = 1;
            let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
            let observer = Rc::new(AdmissionProbe::default());
            handle.set_observer(observer.clone()).unwrap();
            let cancellation = CancellationSource::new();
            let expected_error = if direct_control {
                HttpErrorCode::READ_TIMEOUT
            } else {
                HttpErrorCode::CORO_CANCELLED
            };
            let mut upload = Upload::new(1, 1);
            upload.gate = Some((0, Arc::clone(&release_upload)));
            let (upload, probe) = upload.held();
            let wake = Arc::new(RecordingWake(AtomicBool::new(false)));
            let waker = Waker::from(Arc::clone(&wake));
            let mut run = Box::pin(session.run());
            let mut finished = None;
            let outcome = catch_unwind(AssertUnwindSafe(|| {
                let mut poll_to_quiescence = || {
                    for _ in 0..16 {
                        wake.0.store(false, Ordering::SeqCst);
                        if let Poll::Ready(result) =
                            run.as_mut().poll(&mut Context::from_waker(&waker))
                        {
                            finished = Some(result);
                            panic!("the held native read keeps the session running");
                        }
                        if !wake.0.load(Ordering::SeqCst) {
                            return;
                        }
                    }
                    panic!("held I/O must allow incidental readiness to settle");
                };
                let mut submission = handle.reserve_request().unwrap().submit(
                    request_head(HttpMethod::Post, false, Some(1)),
                    upload,
                    if direct_control {
                        CancellationToken::never()
                    } else {
                        cancellation.token()
                    },
                );
                poll_to_quiescence();
                let Poll::Ready(Ok(response)) =
                    Pin::new(&mut submission).poll(&mut Context::from_waker(&waker))
                else {
                    panic!("the committed request acknowledgment is ready");
                };
                let mut response = response.into_source_parts();
                assert_eq!(
                    decode_client_frames(&wire.borrow().writes.concat()).header_streams,
                    [stream]
                );
                assert_eq!(probe.reads.get(), 1);
                assert_eq!(probe.produced.get(), 0);
                assert!(wire.borrow().reads.is_empty());
                let mut header = response.1.read_header_event();
                assert!(
                    header
                        .as_mut()
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending()
                );
                wire.borrow_mut().write_gate = Some(Arc::clone(&release_write));
                release_upload.notify_one();
                poll_to_quiescence();
                assert_eq!(probe.produced.get(), 1);
                assert_eq!(probe.drops.get(), 1);
                assert!(probe.stops.borrow().is_empty());
                assert_eq!(
                    wire.borrow().write_gate_pending,
                    Some(FRAME_HEADER_SIZE + 1)
                );
                assert_eq!(
                    decode_client_frames(&wire.borrow().writes.concat()).body_bytes,
                    0
                );
                assert!(!handle.is_idle());
                assert!(observer.released.borrow().is_empty());

                wake.0.store(false, Ordering::SeqCst);
                if direct_control {
                    response.2.cancel(HttpError::new(expected_error));
                } else {
                    cancellation.request_cancellation();
                }
                assert!(
                    wake.0.load(Ordering::SeqCst),
                    "terminal upload cancellation must wake the parent before any repoll or I/O release"
                );

                poll_to_quiescence();
                let Poll::Ready(Err(error)) =
                    header.as_mut().poll(&mut Context::from_waker(&waker))
                else {
                    panic!("cancellation reaches the original response source");
                };
                assert_eq!(error.code(), expected_error);
                drop(header);
                drop(response);
                poll_to_quiescence();
                assert!(
                    !handle.is_idle(),
                    "the terminal native write is still owned"
                );
                assert!(observer.released.borrow().is_empty());
                release_write.notify_one();
                poll_to_quiescence();
                assert!(handle.is_idle());
                assert_eq!(handle.available_capacity(), 1);
                assert_eq!(observer.released.borrow().as_slice(), &[stream]);
                let sent = complete_credit_wire(true, &wire);
                assert_eq!(sent.frames.body_data, b"u");
                assert_eq!(sent.completed, [stream]);
                assert_eq!(sent.frames.resets, [(stream, H2ErrorCode::CANCEL)]);
                assert!(sent.frames.errors.is_empty());
            }));
            release_upload.notify_one();
            release_write.notify_one();
            release_read.notify_one();
            handle.cancel();
            let result = match finished {
                Some(result) => result,
                None => tokio::time::timeout(Duration::from_secs(5), run.as_mut())
                    .await
                    .expect("terminal upload cancellation joins both native operations"),
            };
            drop(run);
            if let Err(payload) = outcome {
                resume_unwind(payload);
            }
            cancelled(result);
            assert_eq!(probe.reads.get(), 1);
            assert_eq!(probe.produced.get(), 1);
            assert_eq!(probe.drops.get(), 1);
            assert!(probe.stops.borrow().is_empty());
            assert_eq!(observer.released.borrow().as_slice(), &[stream]);
            assert!(wire.borrow().transport_dropped);
        })
    });
}

#[test]
fn response_observer_terminal_cleanup_joins_held_native_io() {
    use std::task::Context;
    use std::time::Instant;

    use crate::owned_h2::OwnedH2ResponseObserver;

    #[derive(Clone, Copy)]
    enum Terminal {
        PeerReset,
        RequestCancel,
        Shutdown,
        PollPanic,
    }

    struct Observer {
        producer: HttpStreamSourceProducer,
        polled: Rc<Cell<usize>>,
        completed: Rc<RefCell<Vec<Result<Instant, HttpErrorCode>>>>,
        dropped: Rc<Cell<usize>>,
        panic: bool,
        poll_panic: bool,
    }
    impl OwnedH2ResponseObserver for Observer {
        fn poll_deadline(&mut self, _: &mut Context<'_>) -> Poll<HttpError> {
            self.polled.set(self.polled.get() + 1);
            if self.poll_panic {
                panic_any("response observer deadline");
            }
            Poll::Pending
        }

        fn response_complete(self: Box<Self>, completed: Result<Instant, HttpError>) {
            let completed = completed.map_err(|error| error.code());
            let code = completed.as_ref().err().copied();
            self.completed.borrow_mut().push(completed);
            self.producer.stop_reading(code);
            if self.panic {
                panic_any("response observer completion");
            }
        }
    }
    impl Drop for Observer {
        fn drop(&mut self) {
            self.dropped.set(self.dropped.get() + 1);
        }
    }

    for terminal in [
        Terminal::PeerReset,
        Terminal::RequestCancel,
        Terminal::Shutdown,
        Terminal::PollPanic,
    ] {
        run_on_worker(move |context| {
            Box::pin(async move {
                let stream = StreamId::new(1);
                let release_write = Arc::new(tokio::sync::Notify::new());
                let release_read = Arc::new(tokio::sync::Notify::new());
                let reset = Arc::new(tokio::sync::Notify::new());
                let mut reads = vec![FixtureRead::Data(server_settings_bytes(1))];
                if matches!(terminal, Terminal::PeerReset) {
                    let mut peer = Http2Codec::new(TransportDirection::Downstream);
                    reads.push(FixtureRead::WaitData(
                        Arc::clone(&reset),
                        eligibility_frame(
                            &mut peer,
                            EgressFrame::Reset {
                                stream,
                                code: H2ErrorCode::CANCEL,
                            },
                        ),
                    ));
                }
                reads.push(FixtureRead::WaitDataEof(
                    Arc::clone(&release_read),
                    Vec::new(),
                ));
                let (transport, wire) = FixtureTransport::new(reads);
                wire.borrow_mut().write_gate = Some(Arc::clone(&release_write));
                let (session, handle) =
                    OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
                let submission = handle.reserve_request().unwrap().submit(
                    request_head(HttpMethod::Get, true, None),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                let polled = Rc::new(Cell::new(0));
                let completed = Rc::new(RefCell::new(Vec::new()));
                let dropped = Rc::new(Cell::new(0));
                let resumed = Cell::new(false);
                let run = async {
                    let result = eligibility_catch_unwind(session.run()).await;
                    resumed.set(true);
                    result
                };
                let scenario = async {
                    let outcome = eligibility_catch_unwind(tokio::time::timeout(
                        Duration::from_secs(5),
                        async {
                            let response = submission.await.unwrap();
                            let producer = response.source.source().unwrap().producer();
                            let receipt = response
                                .source
                                .source()
                                .unwrap()
                                .ingress_completion()
                                .unwrap();
                            complete_credit_wait(|| {
                                let wire = wire.borrow();
                                wire.write_gate_pending.is_some()
                                    && wire.reads.len()
                                        == usize::from(matches!(terminal, Terminal::PeerReset))
                            })
                            .await;
                            response
                                .control
                                .observe_response(Box::new(Observer {
                                    producer: producer.clone(),
                                    polled: Rc::clone(&polled),
                                    completed: Rc::clone(&completed),
                                    dropped: Rc::clone(&dropped),
                                    panic: matches!(
                                        terminal,
                                        Terminal::RequestCancel | Terminal::PollPanic
                                    ),
                                    poll_panic: matches!(terminal, Terminal::PollPanic),
                                }))
                                .unwrap();
                            match terminal {
                                Terminal::Shutdown => handle.cancel(),
                                Terminal::PollPanic => {
                                    complete_credit_wait(|| polled.get() > 0).await;
                                }
                                Terminal::PeerReset => {
                                    complete_credit_wait(|| polled.get() > 0).await;
                                    reset.notify_one();
                                }
                                Terminal::RequestCancel => {
                                    complete_credit_wait(|| polled.get() > 0).await;
                                    response.control.cancel(HttpError::with_message(
                                        HttpErrorCode::READ_TIMEOUT,
                                        b"selected observer cause",
                                    ));
                                }
                            }
                            complete_credit_wait(|| {
                                dropped.get() == 1 && wire.borrow().reads.is_empty()
                            })
                            .await;
                            assert_eq!(completed.borrow().len(), 1);
                            let selected = receipt.snapshot().unwrap().unwrap_err().code();
                            assert_eq!(completed.borrow().as_slice(), &[Err(selected)]);
                            if matches!(terminal, Terminal::PeerReset) {
                                assert_eq!(selected, HttpErrorCode::CANCEL);
                            }
                            if matches!(terminal, Terminal::RequestCancel) {
                                assert_eq!(selected, HttpErrorCode::READ_TIMEOUT);
                            }
                            if matches!(terminal, Terminal::PollPanic) {
                                assert_eq!(selected, HttpErrorCode::INTERNAL_ERROR);
                            }
                            if matches!(terminal, Terminal::Shutdown) {
                                assert_eq!(
                                    polled.get(),
                                    0,
                                    "teardown drains an observer still in the attachment handoff"
                                );
                                assert_eq!(selected, HttpErrorCode::CORO_CANCELLED);
                            }
                            if matches!(terminal, Terminal::PollPanic) {
                                assert_eq!(
                                    polled.get(),
                                    1,
                                    "a panicked deadline is retired without repolling"
                                );
                            }
                            assert!(response.source.source().unwrap().source_complete());
                            drop(response);
                            assert!(!producer.is_live());
                            assert!(
                                !resumed.get(),
                                "observer completion cannot abandon the native write"
                            );
                            assert!(wire.borrow().writes.is_empty());
                            release_write.notify_one();
                            complete_credit_wait(|| !wire.borrow().writes.is_empty()).await;
                            assert!(
                                !resumed.get(),
                                "the admitted native read remains independently owned"
                            );
                        },
                    ))
                    .await;
                    reset.notify_one();
                    release_write.notify_one();
                    release_read.notify_one();
                    handle.cancel();
                    outcome
                };
                let (result, outcome) = tokio::time::timeout(Duration::from_secs(10), async {
                    tokio::join!(run, scenario)
                })
                .await
                .expect("observer terminal cleanup joins both native operations");
                match outcome {
                    Ok(result) => result.expect("terminal observation makes progress"),
                    Err(panic) => resume_unwind(panic),
                }
                if matches!(terminal, Terminal::RequestCancel | Terminal::PollPanic) {
                    let panic =
                        result.expect_err("the original observer panic is resumed after joining");
                    let expected = if matches!(terminal, Terminal::PollPanic) {
                        "response observer deadline"
                    } else {
                        "response observer completion"
                    };
                    assert_eq!(panic.downcast_ref::<&str>(), Some(&expected));
                } else {
                    cancelled(result.expect("ordinary terminal observation does not panic"));
                }
                assert_eq!(completed.borrow().len(), 1);
                assert_eq!(dropped.get(), 1);
                assert!(wire.borrow().transport_dropped);
                assert_eq!(wire.borrow().close, FixtureClose::Reset);
            })
        });
    }
}

struct OncePanicWake {
    armed: std::sync::atomic::AtomicBool,
    message: &'static str,
}

impl std::task::Wake for OncePanicWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if self.armed.swap(false, Ordering::SeqCst) {
            panic_any(self.message);
        }
    }
}

#[test]
fn submission_publication_panic_retires_the_unreturned_request() {
    run_on_worker(move |context| {
        Box::pin(async move {
            let release_read = Arc::new(tokio::sync::Notify::new());
            let (transport, wire) = FixtureTransport::new([
                FixtureRead::Data(server_settings_bytes(1)),
                FixtureRead::WaitDataEof(Arc::clone(&release_read), Vec::new()),
            ]);
            let mut config = options(context, transport);
            config.http2.max_concurrent_outgoing_streams = 1;
            let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
            let (upload, probe) = Upload::new(0, 1).held();
            let wake = Arc::new(OncePanicWake {
                armed: std::sync::atomic::AtomicBool::new(false),
                message: "submission publication",
            });
            let waker = std::task::Waker::from(Arc::clone(&wake));
            let mut run = Box::pin(session.run());
            let setup = catch_unwind(AssertUnwindSafe(|| {
                assert!(
                    run.as_mut()
                        .poll(&mut std::task::Context::from_waker(&waker))
                        .is_pending()
                );
                wake.armed.store(true, Ordering::SeqCst);
                let result = catch_unwind(AssertUnwindSafe(|| {
                    handle.reserve_request().unwrap().submit(
                        request_head(HttpMethod::Post, false, None),
                        upload,
                        CancellationToken::never(),
                    )
                }));
                let panic = result.err().expect("publication invokes the armed wake");
                assert_eq!(
                    panic.downcast_ref::<&str>(),
                    Some(&"submission publication")
                );
            }));
            let scenario = async {
                let outcome =
                    eligibility_catch_unwind(tokio::time::timeout(Duration::from_secs(5), async {
                        if let Err(panic) = setup {
                            resume_unwind(panic);
                        }
                        complete_credit_wait(|| probe.drops.get() == 1 && handle.is_idle()).await;
                        assert_eq!(probe.reads.get(), 0);
                        assert_eq!(
                            probe.stops.borrow().as_slice(),
                            &[Some(HttpErrorCode::CORO_CANCELLED)]
                        );
                        assert_eq!(handle.available_capacity(), 1);
                        let decoded = decode_client_frames(&wire.borrow().writes.concat());
                        assert!(decoded.header_streams.is_empty());
                        assert!(decoded.resets.is_empty());
                    }))
                    .await;
                wake.armed.store(false, Ordering::SeqCst);
                release_read.notify_one();
                handle.cancel();
                outcome
            };
            let (result, outcome) = tokio::time::timeout(Duration::from_secs(10), async {
                tokio::join!(run, scenario)
            })
            .await
            .expect("publication panic cleanup joins");
            match outcome {
                Ok(result) => result.expect("the unreturned request retires"),
                Err(panic) => resume_unwind(panic),
            }
            cancelled(result);
            assert_eq!(probe.drops.get(), 1);
            assert!(wire.borrow().transport_dropped);
        })
    });
}

#[test]
fn response_interest_remembers_final_head_before_request_commitment() {
    use std::task::Context;
    use std::task::Wake;
    use std::task::Waker;

    struct InterestWake(AtomicUsize);
    impl Wake for InterestWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    run_on_worker(|context| {
        Box::pin(async move {
            let upload_ready = Arc::new(tokio::sync::Notify::new());
            let respond = Arc::new(tokio::sync::Notify::new());
            let release_write = Arc::new(tokio::sync::Notify::new());
            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            let response_drops = Arc::new(AtomicUsize::new(0));
            let response = Bytes::from_owner(DropTrackedBytes {
                bytes: response_head(&mut peer, StreamId::new(3), 200, true, Some(0)),
                drops: Arc::clone(&response_drops),
            });
            let (transport, wire) = FixtureTransport::new([
                FixtureRead::Data(server_settings_bytes(100)),
                FixtureRead::WaitSegmentedData(
                    Arc::clone(&respond),
                    BufQueue::from_bytes(response, 16 << 10).unwrap(),
                ),
            ]);
            let mut config = options(context, transport);
            config.io.maximum_write_size = 16 << 10;
            let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
            let submitted = Rc::new(RefCell::new(None));
            let wakes = Arc::new(InterestWake(AtomicUsize::new(0)));
            let waker = Waker::from(Arc::clone(&wakes));
            let mut upload = Upload::new(0, 0);
            let ready = Arc::clone(&upload_ready);
            let submit_handle = handle.clone();
            let result = Rc::clone(&submitted);
            let interest_waker = waker.clone();
            upload.body_read = Some(Box::pin(async move {
                ready.notified().await;
                let mut submission = submit_handle
                    .reserve_request()
                    .unwrap()
                    .submit_with_response_interest(
                        request_head(HttpMethod::Get, true, None),
                        HttpSourceHolder::empty(),
                        CancellationToken::never(),
                        None,
                    );
                assert!(
                    Pin::new(&mut submission)
                        .poll(&mut Context::from_waker(&interest_waker))
                        .is_pending()
                );
                *result.borrow_mut() = Some(submission);
                Ok(HttpBodyEvent::body(queue(&vec![b'u'; 65_535]), true))
            }));
            let (upload, upload_probe) = upload.held();
            let first = handle.reserve_request().unwrap().submit(
                request_head(HttpMethod::Post, false, Some(65_535)),
                upload,
                CancellationToken::never(),
            );
            cancelled(
                drive(
                    session,
                    &handle,
                    &[
                        Arc::clone(&upload_ready),
                        Arc::clone(&respond),
                        Arc::clone(&release_write),
                    ],
                    async {
                        let _first_response = first.await.unwrap();
                        complete_credit_wait(|| {
                            upload_probe.reads.get() == 1
                                && decode_client_frames(&wire.borrow().writes.concat())
                                    .header_streams
                                    == [StreamId::new(1)]
                        })
                        .await;
                        wire.borrow_mut().write_gate = Some(Arc::clone(&release_write));
                        // Queue the new request during the older owner's poll, so its body
                        // precedes the new headers in the serializer's scheduler.
                        upload_ready.notify_one();
                        complete_credit_wait(|| wire.borrow().write_gate_pending == Some(16 << 10))
                            .await;
                        let mut submission = submitted.borrow_mut().take().unwrap();
                        respond.notify_one();
                        complete_credit_wait(|| response_drops.load(Ordering::SeqCst) == 1).await;
                        assert_eq!(
                            wakes.0.load(Ordering::SeqCst),
                            0,
                            "final-head publication alone cannot acknowledge an uncommitted request"
                        );
                        assert!(
                            Pin::new(&mut submission)
                                .poll(&mut Context::from_waker(&waker))
                                .is_pending()
                        );
                        release_write.notify_one();
                        let response = submission.await.unwrap();
                        assert_eq!(response.stream, StreamId::new(3));
                        let mut source = response.source.into_source_owner().into_source_holder();
                        let mut head = source.read_header_event().await.unwrap();
                        assert_eq!(head.headers_mut().status_code().unwrap(), 200);
                        assert!(head.eom());
                        assert!(response.control.capacity_released());
                        complete_credit_wait(|| {
                            decode_client_frames(&wire.borrow().writes.concat()).header_streams
                                == [StreamId::new(1), StreamId::new(3)]
                        })
                        .await;
                    },
                )
                .await,
            );
        })
    });
}

#[test]
fn response_interest_selects_a_supplied_producer_failure_only_before_delivery() {
    // (supply the request producer's completion, take the response before the upload fails,
    // end the response before the upload fails)
    for (supplied, delivered, complete) in [
        (true, false, false),
        (false, false, false),
        (true, true, false),
        (true, false, true),
    ] {
        run_on_worker(move |context| {
            Box::pin(async move {
                let respond = Arc::new(tokio::sync::Notify::new());
                let mut peer = Http2Codec::new(TransportDirection::Downstream);
                let stream = StreamId::new(1);
                let response_drops = Arc::new(AtomicUsize::new(0));
                let response = Bytes::from_owner(DropTrackedBytes {
                    bytes: response_head(&mut peer, stream, 200, complete, None),
                    drops: Arc::clone(&response_drops),
                });
                let (transport, wire) = FixtureTransport::new([
                    FixtureRead::Data(server_settings_bytes(1)),
                    FixtureRead::WaitSegmentedData(
                        Arc::clone(&respond),
                        BufQueue::from_bytes(response, 16 << 10).unwrap(),
                    ),
                ]);
                let mut head = request_head(HttpMethod::Post, false, Some(1));
                let (upload, producer, _) = fresh_upload(&mut head, 1, context.host().clone());
                let completion = supplied.then(|| upload.ingress_completion().unwrap());
                let (session, handle) =
                    OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
                let submission = handle
                    .reserve_request()
                    .unwrap()
                    .submit_with_response_interest(
                        head,
                        upload,
                        CancellationToken::never(),
                        completion,
                    );
                cancelled(
                    drive(session, &handle, &[Arc::clone(&respond)], async {
                        complete_credit_wait(|| {
                            decode_client_frames(&wire.borrow().writes.concat()).header_streams
                                == [stream]
                        })
                        .await;
                        let taken = if delivered {
                            respond.notify_one();
                            let response = submission.await.expect("the head releases the waiter");
                            let mut source =
                                response.source.into_source_owner().into_source_holder();
                            let mut head = source.read_header_event().await.unwrap();
                            assert_eq!(head.headers_mut().status_code().unwrap(), 200);
                            Ok(source)
                        } else {
                            if complete {
                                respond.notify_one();
                                complete_credit_wait(|| response_drops.load(Ordering::SeqCst) == 1)
                                    .await;
                            }
                            Err(submission)
                        };
                        assert!(producer.abort(HttpErrorCode::READ_TIMEOUT, b"downstream failed"));
                        complete_credit_wait(|| {
                            !decode_client_frames(&wire.borrow().writes.concat())
                                .resets
                                .is_empty()
                        })
                        .await;
                        let error = match taken {
                            Ok(mut source) => source.read_body_event(u32::MAX).await.err(),
                            Err(submission) if complete => {
                                let response = submission
                                    .await
                                    .expect("the ended response survives the failed upload");
                                let mut source =
                                    response.source.into_source_owner().into_source_holder();
                                let mut head = source.read_header_event().await.unwrap();
                                assert_eq!(head.headers_mut().status_code().unwrap(), 200);
                                assert!(head.eom());
                                None
                            }
                            Err(submission) if supplied => submission.await.err(),
                            Err(submission) => submission
                                .await
                                .expect("the session publishes its upload-failure outcome")
                                .source
                                .into_source_owner()
                                .into_source_holder()
                                .read_header_event()
                                .await
                                .err(),
                        };
                        // Only a response that the caller has not taken and that has not ended
                        // takes the supplied producer's error and its cancellation reset
                        // (READ_TIMEOUT -> CANCEL).
                        let (message, reset): (Option<&[u8]>, _) = if complete {
                            (None, H2ErrorCode::INTERNAL_ERROR)
                        } else if supplied && !delivered {
                            (
                                Some(b"HTTP source aborted err=READ_TIMEOUT, details=downstream failed"),
                                H2ErrorCode::CANCEL,
                            )
                        } else {
                            (
                                Some(b"HTTP source aborted err=CORO_CANCELLED, details=egress source failed"),
                                H2ErrorCode::INTERNAL_ERROR,
                            )
                        };
                        assert_eq!(error.as_ref().map(HttpError::message), message);
                        complete_credit_wait(|| handle.is_idle()).await;
                        let decoded = decode_client_frames(&wire.borrow().writes.concat());
                        assert_eq!(decoded.resets, [(stream, reset)]);
                        assert!(decoded.errors.is_empty());
                    })
                    .await,
                );
            })
        });
    }
}

#[test]
fn response_interest_producer_failure_sends_only_the_request_body_read_before_it() {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            let initial = eligibility_frame(
                &mut peer,
                EgressFrame::Settings {
                    settings: &[
                        roxygen_codec::CodecSetting {
                            identifier: SettingsId::InitialWindowSize.as_u16(),
                            value: 0,
                        },
                        roxygen_codec::CodecSetting {
                            identifier: SettingsId::MaxConcurrentStreams.as_u16(),
                            value: 1,
                        },
                    ],
                },
            );
            let credit = Arc::new(tokio::sync::Notify::new());
            let stream = StreamId::new(1);
            let (transport, wire) = FixtureTransport::new([
                FixtureRead::Data(initial),
                FixtureRead::WaitData(
                    Arc::clone(&credit),
                    eligibility_frame(
                        &mut peer,
                        EgressFrame::WindowUpdate {
                            stream,
                            amount: 65_536,
                        },
                    ),
                ),
            ]);
            let mut head = request_head(HttpMethod::Post, false, None);
            let (upload, producer, probe) =
                fresh_upload(&mut head, 1 << 20, context.host().clone());
            let completion = upload.ingress_completion().unwrap();
            let (session, handle) =
                OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
            cancelled(
                drive(session, &handle, &[Arc::clone(&credit)], async {
                    complete_credit_wait(|| handle.available_capacity() == 1).await;
                    let submission = handle
                        .reserve_request()
                        .unwrap()
                        .submit_with_response_interest(
                            head,
                            upload,
                            CancellationToken::never(),
                            Some(completion),
                        );
                    producer.body(queue(b"held"), 0, false);
                    complete_credit_wait(|| {
                        probe.bytes.get() == 4
                            && decode_client_frames(&wire.borrow().writes.concat()).header_streams
                                == [stream]
                    })
                    .await;
                    producer.body(queue(b"queued"), 0, false);
                    assert_eq!(producer.body_bytes_buffered(), Some(6));
                    assert!(producer.abort(HttpErrorCode::READ_TIMEOUT, b"downstream failed"));
                    credit.notify_one();
                    let error = submission.await.err();
                    assert_eq!(
                        error.as_ref().map(HttpError::message),
                        Some(
                            &b"HTTP source aborted err=READ_TIMEOUT, details=downstream failed"[..]
                        )
                    );
                    complete_credit_wait(|| handle.is_idle()).await;
                    let decoded = decode_client_frames(&wire.borrow().writes.concat());
                    // The source discards its queued body before it publishes the failure.
                    assert_eq!(decoded.body_data, b"held");
                    assert_eq!(decoded.resets, [(stream, H2ErrorCode::CANCEL)]);
                    assert!(decoded.errors.is_empty());
                })
                .await,
            );
        })
    });
}

#[test]
fn owned_upstream_round_trip_runs_without_tokio_runtime() {
    crate::external_host_tests::run_on_host(|context| {
        Box::pin(async move {
            assert!(tokio::runtime::Handle::try_current().is_err());
            let release_eof = Arc::new(tokio::sync::Notify::new());
            let (transport, wire) = FixtureTransport::new([
                FixtureRead::Data(server_empty_responses_bytes([StreamId::new(1)])),
                FixtureRead::WaitDataEof(Arc::clone(&release_eof), Vec::new()),
            ]);
            let config = OwnedH2Options {
                context,
                transport,
                io: UniplexConfig {
                    buffer_limit: 16 << 20,
                    ingress_buffer_limit: 16 << 20,
                    read_timeout: Duration::ZERO,
                    stream_read_timeout: Duration::ZERO,
                    write_timeout: Duration::ZERO,
                    ..UniplexConfig::default()
                },
                http2: Http2SessionConfig {
                    settings: vec![
                        roxygen_codec::CodecSetting {
                            identifier: SettingsId::InitialWindowSize.as_u16(),
                            value: 16 << 20,
                        },
                        roxygen_codec::CodecSetting {
                            identifier: SettingsId::EnablePush.as_u16(),
                            value: 0,
                        },
                    ],
                    stream_receive_window: 16 << 20,
                    connection_receive_window: 128 << 20,
                    max_concurrent_outgoing_streams: 100,
                    ..Http2SessionConfig::default()
                },
            };
            let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                request_head(HttpMethod::Get, true, None),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let (run, ()) = tokio::join!(session.run(), async {
                let (stream, mut source, _control) = submission
                    .await
                    .expect("the origin response resolves the submission")
                    .into_source_parts();
                assert_eq!(stream, StreamId::new(1));
                let mut head = source.read_header_event().await.unwrap();
                assert_eq!(head.headers_mut().status_code().unwrap(), 200);
                assert!(head.eom(), "the origin sent HEADERS with END_STREAM");
                drop(source);
                complete_credit_wait(|| handle.is_idle()).await;
                release_eof.notify_one();
            });
            let run = run.expect("origin EOF after the only exchange closes cleanly");
            assert!(!run.reset);
            let decoded = decode_client_frames(&wire.borrow().writes.concat());
            assert_eq!(decoded.header_streams, [StreamId::new(1)]);
            assert!(decoded.resets.is_empty());
            assert!(decoded.errors.is_empty());
        })
    });
}

/// Unconsumed receipts the writer-arming fixtures' queue holds before it refuses a write.
const ARMING_CAPACITY: usize = 8;

/// HTTP/2 frame flags the writer-arming fixtures look for.
const ARMING_ACK: u8 = 0x1;
const ARMING_END_STREAM: u8 = 0x1;

/// An origin's SETTINGS; then, for each of streams 1, 3 and 5, a 200 with
/// `content-length: 128` and 128 B of `r` that end the stream.
fn arming_origin() -> (Vec<u8>, [Vec<u8>; 3]) {
    let mut peer = Http2Codec::new(TransportDirection::Downstream);
    let settings = arming_origin_settings(&mut peer);
    let responses = [0, 1, 2].map(|index| {
        let stream = StreamId::new(1 + 2 * index);
        let mut response = response_head(&mut peer, stream, 200, false, Some(128));
        response.extend(eligibility_frame(
            &mut peer,
            EgressFrame::Body {
                stream,
                body: queue(&[b'r'; 128]),
                padding: None,
                end_stream: true,
            },
        ));
        response
    });
    (settings, responses)
}

fn arming_origin_settings(peer: &mut Http2Codec) -> Vec<u8> {
    let mut output = BufQueue::new(1 << 20);
    peer.generate_configured_settings(&mut output, &mut IgnoreCodecEvents)
        .unwrap();
    output.coalesce(output.chain_length()).unwrap().to_vec()
}

/// A fetched response's status and body, or why it failed.
type Fetched = Result<(u16, Vec<u8>), HttpErrorCode>;

fn fetched_ok() -> Option<Fetched> {
    Some(Ok((200, vec![b'r'; 128])))
}

/// Runs an upstream session over `transport` beside `script`, polling the session only on
/// its own wakes. Then cancels it, opens `gates` and finishes every write so the session
/// joins, and returns what the script observed.
async fn run_upstream_arming<T>(
    context: WorkerContext,
    transport: Rc<super::owned::ScriptedWriteTransport>,
    configure: impl FnOnce(&mut OwnedH2Options),
    gates: &[Arc<tokio::sync::Notify>],
    script: impl AsyncFnOnce(
        &OwnedH2UpstreamHandle,
        &RefCell<super::owned::ScriptedWriteState>,
        &Arc<super::owned::WakeCounts>,
    ) -> T,
) -> T {
    let state = Rc::clone(&transport.state);
    let mut config = options(context, transport);
    configure(&mut config);
    let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
    let (run, counts) = super::owned::CountedWakes::new(session.run());
    let script = async {
        let observed = script(&handle, &state, &counts).await;
        handle.cancel();
        gates.iter().for_each(|gate| gate.notify_one());
        super::owned::finish_scripted_writes(&state);
        observed
    };
    let (_run, observed) = tokio::time::timeout(Duration::from_secs(10), async {
        let joined = tokio::join!(run, script);
        super::owned::join_scripted_tasks(&state).await;
        joined
    })
    .await
    .expect("the cancelled session joins its transport operations and the fixture's tasks");
    observed
}

/// Waits until the origin's SETTINGS are acknowledged and the session settles.
async fn arming_settings_settle(state: &RefCell<super::owned::ScriptedWriteState>) -> bool {
    let acknowledged = super::owned::writer_drive_until(|| {
        state.borrow().frames(FrameType::SETTINGS, ARMING_ACK) == 1
    })
    .await;
    super::owned::writer_settle().await;
    acknowledged
}

/// Submits a POST with a 128-byte body as the `index`th request and waits until it is
/// written; `None` if it never is.
async fn arming_request(
    handle: &OwnedH2UpstreamHandle,
    state: &RefCell<super::owned::ScriptedWriteState>,
    index: usize,
) -> Option<crate::owned_h2::OwnedH2UpstreamSubmission> {
    let submission = handle.reserve_request().ok()?.submit(
        request_head(HttpMethod::Post, false, Some(128)),
        HttpSourceHolder::new(Box::new(HttpFixedSource::make_fixed_body(queue(
            &[b'u'; 128],
        )))),
        CancellationToken::never(),
    );
    let written = super::owned::writer_drive_until(|| {
        state.borrow().frames(FrameType::DATA, ARMING_END_STREAM) == index + 1
    })
    .await;
    super::owned::writer_settle().await;
    written.then_some(submission)
}

/// Reads the whole response to `submission`.
async fn arming_fetch(submission: crate::owned_h2::OwnedH2UpstreamSubmission) -> Fetched {
    let response = submission.await.map_err(|error| error.code())?;
    let (_, mut source, _request) = response.into_source_parts();
    read_fetched(&mut source).await
}

/// Reads a whole response from its source.
async fn read_fetched(source: &mut HttpSourceHolder) -> Fetched {
    let mut head = source
        .read_header_event()
        .await
        .map_err(|error| error.code())?;
    let status = head.headers_mut().status_code().unwrap();
    let mut body = Vec::new();
    let mut eom = head.eom();
    while !eom {
        let event = source
            .read_body_event(u32::MAX)
            .await
            .map_err(|error| error.code())?;
        if let Some(bytes) = event.as_body() {
            body.extend(bytes.segments().flat_map(|segment| segment.iter().copied()));
        }
        eom = event.eom();
    }
    Ok((status, body))
}

/// Opens `gate`, behind which the origin's response waits, reads the whole response, and lets
/// the session settle.
async fn arming_respond(
    submission: Option<crate::owned_h2::OwnedH2UpstreamSubmission>,
    gate: &tokio::sync::Notify,
) -> Option<Fetched> {
    let submission = submission?;
    gate.notify_one();
    let fetched = arming_fetch(submission).await;
    super::owned::writer_settle().await;
    Some(fetched)
}

async fn arming_exchange(
    handle: &OwnedH2UpstreamHandle,
    state: &RefCell<super::owned::ScriptedWriteState>,
    index: usize,
    gate: &tokio::sync::Notify,
) -> Option<Fetched> {
    let submission = arming_request(handle, state, index).await;
    arming_respond(submission, gate).await
}

/// The origin's SETTINGS, then its responses for streams 1, 3 and 5, each behind its gate.
fn arming_origin_reads() -> (Vec<FixtureRead>, [Arc<tokio::sync::Notify>; 3]) {
    let (settings, responses) = arming_origin();
    let gates: [_; 3] = std::array::from_fn(|_| Arc::new(tokio::sync::Notify::new()));
    let mut reads = vec![FixtureRead::Data(settings)];
    for (gate, response) in gates.iter().zip(responses) {
        reads.push(FixtureRead::WaitData(Arc::clone(gate), response));
    }
    (reads, gates)
}

/// A session whose writes all complete inline over the origin's reads.
fn inline_arming_transport(
    reads: Vec<FixtureRead>,
    capacity: usize,
) -> Rc<super::owned::ScriptedWriteTransport> {
    super::owned::ScriptedWriteTransport::new(
        reads,
        [],
        super::owned::ScriptedWrite::Complete,
        capacity,
        None,
    )
}

#[test]
fn completed_upstream_write_does_not_repoll_the_session() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (reads, gates) = arming_origin_reads();
            let transport = inline_arming_transport(reads, ARMING_CAPACITY);
            let (settled, fetched, polls, wakes) = run_upstream_arming(
                context,
                transport,
                |_| {},
                &gates,
                async |handle, state, counts| {
                    let settled = arming_settings_settle(state).await;
                    let mut fetched = Vec::new();
                    for (index, gate) in gates.iter().enumerate().take(2) {
                        fetched.push(arming_exchange(handle, state, index, gate).await);
                    }
                    let (polls, wakes) = (counts.polls(), counts.wakes());
                    fetched.push(arming_exchange(handle, state, 2, &gates[2]).await);
                    (
                        settled,
                        fetched,
                        counts.polls() - polls,
                        counts.wakes() - wakes,
                    )
                },
            )
            .await;
            assert!(settled, "the session acknowledges the origin's SETTINGS");
            assert_eq!(fetched, vec![fetched_ok(); 3], "every request succeeds");
            assert_eq!(
                polls, 3,
                "session polls for the third request: neither the write's completion nor the \
                 response's capacity release inside the session's own poll wakes the session \
                 (wakes {wakes})"
            );
        })
    });
}

#[test]
fn consumed_receipt_leaves_no_armed_writer_waker() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (reads, gates) = arming_origin_reads();
            let transport = inline_arming_transport(reads, ARMING_CAPACITY);
            let (settled, first, at_write, after, second) = run_upstream_arming(
                context,
                transport,
                |_| {},
                &gates,
                async |handle, state, counts| {
                    let settled = arming_settings_settle(state).await;
                    let first = arming_exchange(handle, state, 0, &gates[0]).await;
                    let at_write = Rc::new(Cell::new(None));
                    let recorded = Rc::clone(&at_write);
                    let wakes = Arc::clone(counts);
                    state.borrow_mut().on_write = Some(Box::new(move || {
                        if recorded.get().is_none() {
                            recorded.set(Some(wakes.wakes()));
                        }
                    }));
                    let submission = arming_request(handle, state, 1).await;
                    let after = counts.wakes();
                    let second = arming_respond(submission, &gates[1]).await;
                    (settled, first, at_write.get(), after, second)
                },
            )
            .await;
            assert!(settled, "the session acknowledges the origin's SETTINGS");
            assert_eq!([first, second], [fetched_ok(), fetched_ok()]);
            let at_write = at_write.expect("the second request reached the writer");
            assert_eq!(
                after - at_write,
                0,
                "the first request's receipt was consumed, so the second request's write \
                 completion finds no writer waker to wake"
            );
        })
    });
}

#[test]
fn full_write_queue_arms_the_writer() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (reads, gates) = arming_origin_reads();
            let transport = inline_arming_transport(reads, 1);
            let (settled, writes, fetched) = run_upstream_arming(
                context,
                transport,
                |config| config.io.maximum_write_size = 64,
                &gates,
                async |handle, state, _counts| {
                    let settled = arming_settings_settle(state).await;
                    let before = state.borrow().writes.len();
                    let submission = arming_request(handle, state, 0).await;
                    let writes = state.borrow().writes.len() - before;
                    let fetched = arming_respond(submission, &gates[0]).await;
                    (settled, writes, fetched)
                },
            )
            .await;
            assert!(settled, "the session acknowledges the origin's SETTINGS");
            assert!(
                fetched.is_some(),
                "the request is written: a full queue arms the writer"
            );
            assert_eq!(fetched, fetched_ok(), "the request succeeds");
            assert!(
                writes >= 3,
                "the request took several 64 B writes, each behind the one before ({writes})"
            );
        })
    });
}

/// What ends an upstream session that has a write in flight.
#[derive(Clone, Copy, Debug)]
enum SessionEnd {
    CloseWhenIdle,
    Cancel,
    PeerGoaway,
    PeerEof,
}

#[test]
fn drain_waits_for_write_completion() {
    run_on_worker(|context| {
        Box::pin(async move {
            for end in [
                SessionEnd::CloseWhenIdle,
                SessionEnd::Cancel,
                SessionEnd::PeerGoaway,
                SessionEnd::PeerEof,
            ] {
                let mut peer = Http2Codec::new(TransportDirection::Downstream);
                let settings = arming_origin_settings(&mut peer);
                let gate = Arc::new(tokio::sync::Notify::new());
                let mut reads = vec![FixtureRead::Data(settings)];
                match end {
                    SessionEnd::PeerGoaway => reads.push(FixtureRead::WaitData(
                        Arc::clone(&gate),
                        eligibility_frame(
                            &mut peer,
                            EgressFrame::Goaway {
                                last_stream: StreamId::new(0),
                                code: H2ErrorCode::NO_ERROR,
                                debug_data: None,
                            },
                        ),
                    )),
                    SessionEnd::PeerEof => {
                        reads.push(FixtureRead::WaitDataEof(Arc::clone(&gate), Vec::new()))
                    }
                    SessionEnd::CloseWhenIdle | SessionEnd::Cancel => {}
                }
                let transport = super::owned::ScriptedWriteTransport::new(
                    reads,
                    [super::owned::ScriptedWrite::Pending],
                    super::owned::ScriptedWrite::Complete,
                    ARMING_CAPACITY,
                    None,
                );
                let (pending, waiting, finished) = run_upstream_arming(
                    context.clone(),
                    transport,
                    |_| {},
                    &[Arc::clone(&gate)],
                    async |handle, state, _counts| {
                        super::owned::writer_settle().await;
                        let pending = state.borrow().pending();
                        match end {
                            SessionEnd::CloseWhenIdle => {
                                handle
                                    .close_when_idle()
                                    .expect("a live session starts draining");
                            }
                            SessionEnd::Cancel => handle.cancel(),
                            SessionEnd::PeerGoaway | SessionEnd::PeerEof => gate.notify_one(),
                        }
                        super::owned::writer_settle().await;
                        let waiting = !handle.is_finished();
                        super::owned::complete_scripted_writes(state, 1);
                        let finished =
                            super::owned::writer_drive_until(|| handle.is_finished()).await;
                        (pending, waiting, finished)
                    },
                )
                .await;
                assert_eq!(pending, 1, "{end:?}: the session's first write is pending");
                assert!(waiting, "{end:?}: the session waits for its pending write");
                assert!(
                    finished,
                    "{end:?}: the session finishes once its write completes"
                );
            }
        })
    });
}

#[test]
fn idle_is_published_after_a_write_at_idle() {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            let settings = arming_origin_settings(&mut peer);
            let ping = eligibility_frame(
                &mut peer,
                EgressFrame::PingRequest {
                    data: Some(0x0102_0304_0506_0708),
                },
            );
            let gate = Arc::new(tokio::sync::Notify::new());
            let transport = inline_arming_transport(
                vec![
                    FixtureRead::Data(settings),
                    FixtureRead::WaitData(Arc::clone(&gate), ping),
                ],
                ARMING_CAPACITY,
            );
            let (idle, pinged, idle_again) = run_upstream_arming(
                context,
                transport,
                |_| {},
                &[Arc::clone(&gate)],
                async |handle, state, _counts| {
                    let idle = super::owned::writer_drive_until(|| handle.is_idle()).await;
                    let before = state.borrow().writes.len();
                    state
                        .borrow_mut()
                        .set_after_script(super::owned::ScriptedWrite::Pending);
                    gate.notify_one();
                    super::owned::writer_settle().await;
                    let pinged = (
                        state.borrow().writes.len() - before,
                        state.borrow().pending(),
                        state.borrow().frames(FrameType::PING, ARMING_ACK),
                        handle.is_idle(),
                    );
                    super::owned::complete_scripted_writes(state, 1);
                    let idle_again = super::owned::writer_drive_until(|| handle.is_idle()).await;
                    (idle, pinged, idle_again)
                },
            )
            .await;
            assert!(idle, "the session is idle after the SETTINGS exchange");
            assert_eq!(
                pinged,
                (1, 1, 1, false),
                "the PING ACK's write is pending, and a session that owns a pending write is \
                 not idle"
            );
            assert!(
                idle_again,
                "the session publishes idleness once the write completes"
            );
        })
    });
}

/// Fetches `submission` on its own fixture task, as a separately scheduled client does, into
/// `outcome`.
fn spawn_arming_fetch(
    state: &RefCell<super::owned::ScriptedWriteState>,
    submission: crate::owned_h2::OwnedH2UpstreamSubmission,
    outcome: &Rc<RefCell<Option<Fetched>>>,
) {
    let outcome = Rc::clone(outcome);
    super::owned::spawn_scripted_task(state, async move {
        let fetched = arming_fetch(submission).await;
        *outcome.borrow_mut() = Some(fetched);
    });
}

#[test]
fn silently_failed_write_is_observed() {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            let settings = arming_origin_settings(&mut peer);
            let read_failure = Arc::new(tokio::sync::Notify::new());
            let transport = super::owned::ScriptedWriteTransport::new(
                [FixtureRead::Data(settings)],
                [],
                super::owned::ScriptedWrite::Complete,
                ARMING_CAPACITY,
                Some((
                    Arc::clone(&read_failure),
                    HttpErrorCode::TRANSPORT_READ_ERROR,
                )),
            );
            let outcome = Rc::new(RefCell::new(None));
            let (settled, pending, ended) = run_upstream_arming(
                context,
                transport,
                |_| {},
                &[Arc::clone(&read_failure)],
                async |handle, state, _counts| {
                    let settled = arming_settings_settle(state).await;
                    state
                        .borrow_mut()
                        .set_after_script(super::owned::ScriptedWrite::Pending);
                    if let Some(submission) = arming_request(handle, state, 0).await {
                        spawn_arming_fetch(state, submission, &outcome);
                    }
                    let pending = state.borrow().pending();
                    super::owned::fail_scripted_write(
                        state,
                        HttpError::with_message(
                            HttpErrorCode::TRANSPORT_WRITE_ERROR,
                            b"upstream write failed",
                        ),
                    );
                    super::owned::writer_settle().await;
                    read_failure.notify_one();
                    let ended = super::owned::writer_drive_until(|| {
                        outcome.borrow().is_some() && handle.is_finished()
                    })
                    .await;
                    (settled, pending, ended)
                },
            )
            .await;
            assert!(settled, "the session acknowledges the origin's SETTINGS");
            assert_eq!(
                pending, 1,
                "the request's write is pending while its response is awaited"
            );
            assert!(ended, "the request fails and the session finishes");
            assert!(
                outcome.borrow().as_ref().is_some_and(Result::is_err),
                "the in-flight request fails"
            );
        })
    });
}

#[test]
fn in_poll_reset_after_an_unarmed_write_still_retires() {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            let settings = arming_origin_settings(&mut peer);
            let transport = inline_arming_transport(vec![FixtureRead::Data(settings)], 64);
            let outcome = Rc::new(RefCell::new(None));
            let (idle, reset, retired) = run_upstream_arming(
                context,
                transport,
                // A 16 B write limit starts the request's write in the turn that serializes its
                // head, before the turn that polls its upload.
                |config| config.io.maximum_write_size = 16,
                &[],
                async |handle, state, _counts| {
                    let idle = super::owned::writer_drive_until(|| handle.is_idle()).await;
                    let before = state.borrow().writes.len();
                    state
                        .borrow_mut()
                        .set_after_script(super::owned::ScriptedWrite::Pending);
                    let submission = handle.reserve_request().unwrap().submit(
                        request_head(HttpMethod::Post, false, Some(128)),
                        HttpSourceHolder::new(Box::new(
                            roxygen_core::fixed_source::HttpErrorSource::new(
                                HttpError::with_message(
                                    HttpErrorCode::TRANSPORT_READ_ERROR,
                                    b"the upload failed",
                                ),
                            ),
                        )),
                        CancellationToken::never(),
                    );
                    spawn_arming_fetch(state, submission, &outcome);
                    super::owned::writer_drive_until(|| {
                        state.borrow().frames(FrameType::RST_STREAM, 0) == 1
                    })
                    .await;
                    super::owned::writer_settle().await;
                    let reset = {
                        let state = state.borrow();
                        (
                            state.pending() == state.writes.len() - before,
                            state.frames(FrameType::HEADERS, 0),
                            state.frames(FrameType::RST_STREAM, 0),
                            handle.is_idle(),
                        )
                    };
                    state
                        .borrow_mut()
                        .set_after_script(super::owned::ScriptedWrite::Complete);
                    let pending = state.borrow().pending();
                    super::owned::complete_scripted_writes(state, pending);
                    let retired = super::owned::writer_drive_until(|| {
                        outcome.borrow().is_some() && handle.is_idle()
                    })
                    .await;
                    (idle, reset, retired)
                },
            )
            .await;
            assert!(idle, "the session is idle after the SETTINGS exchange");
            assert_eq!(
                reset,
                (true, 1, 1, false),
                "the request's head and the owner's reset are pending writes, and the reset \
                 stream waits for them"
            );
            assert!(retired, "the reset stream retires once its writes complete");
            assert!(
                outcome.borrow().as_ref().is_some_and(Result::is_err),
                "the reset request fails"
            );
        })
    });
}

#[test]
fn two_writes_in_one_iteration_flush_once() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (reads, gates) = arming_origin_reads();
            let transport = inline_arming_transport(reads, ARMING_CAPACITY);
            let (settled, writes, fetched) = run_upstream_arming(
                context,
                transport,
                |config| config.io.maximum_write_size = 64,
                &gates,
                async |handle, state, counts| {
                    let settled = arming_settings_settle(state).await;
                    let polls = Rc::new(RefCell::new(Vec::new()));
                    let recorded = Rc::clone(&polls);
                    let counted = Arc::clone(counts);
                    state.borrow_mut().on_write = Some(Box::new(move || {
                        recorded.borrow_mut().push(counted.polls());
                    }));
                    let before = state.borrow().writes.len();
                    let submission = arming_request(handle, state, 0).await;
                    let sizes: Vec<usize> = state.borrow().writes[before..]
                        .iter()
                        .map(Vec::len)
                        .collect();
                    let writes = (sizes, polls.borrow().clone());
                    let fetched = arming_respond(submission, &gates[0]).await;
                    (settled, writes, fetched)
                },
            )
            .await;
            assert!(settled, "the session acknowledges the origin's SETTINGS");
            assert_eq!(fetched, fetched_ok(), "the request succeeds");
            let (sizes, polls) = writes;
            assert!(
                sizes.len() > 1 && sizes.iter().all(|size| *size <= 64),
                "the request takes more than one 64 B session write: {sizes:?}"
            );
            assert!(
                polls.iter().all(|poll| *poll == polls[0]),
                "the request's session writes all start in one session poll, so the \
                 transport flushes them together: {polls:?}"
            );
        })
    });
}

#[test]
fn upstream_arming_harness_joins_fixture_tasks() {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            let settings = arming_origin_settings(&mut peer);
            let transport =
                inline_arming_transport(vec![FixtureRead::Data(settings)], ARMING_CAPACITY);
            let outcome = Rc::new(RefCell::new(None));
            let written = run_upstream_arming(
                context,
                transport,
                |_| {},
                &[],
                async |handle, state, _counts| {
                    let settled = arming_settings_settle(state).await;
                    let submission = arming_request(handle, state, 0).await;
                    let written = submission.is_some();
                    if let Some(submission) = submission {
                        let outcome = Rc::clone(&outcome);
                        let watched = handle.clone();
                        super::owned::spawn_scripted_task(state, async move {
                            let fetched = arming_fetch(submission).await;
                            let finished =
                                super::owned::writer_drive_until(|| watched.is_finished()).await;
                            *outcome.borrow_mut() = Some((fetched, finished));
                        });
                    }
                    settled && written
                },
            )
            .await;
            assert!(
                written,
                "the request is written after the SETTINGS exchange"
            );
            assert!(
                outcome
                    .borrow()
                    .as_ref()
                    .is_some_and(|(fetched, finished)| fetched.is_err() && *finished),
                "the harness joins the request's task, which fails once the session is \
                 cancelled and runs until it sees the session finished"
            );
        })
    });
}

/// A request body that never produces an event; `stop_reading` logs its
/// stream in a log shared across streams.
struct RefusalOrderUpload {
    stream: u64,
    log: Rc<RefCell<Vec<(u64, Option<HttpErrorCode>)>>>,
}

impl HttpSource for RefusalOrderUpload {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        panic!("a prepared upload must not reread its header")
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(std::future::pending())
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.log.borrow_mut().push((self.stream, error));
    }
}

#[test]
fn goaway_refuses_streams_in_id_order() {
    run_on_worker(|context| {
        Box::pin(async move {
            const STREAMS: [u64; 6] = [1, 3, 5, 7, 9, 11];
            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            let settings = eligibility_frame(&mut peer, EgressFrame::Settings { settings: &[] });
            let goaway = eligibility_frame(
                &mut peer,
                EgressFrame::Goaway {
                    last_stream: StreamId::new(3),
                    code: H2ErrorCode::NO_ERROR,
                    debug_data: None,
                },
            );
            let refuse = Arc::new(tokio::sync::Notify::new());
            let (transport, wire) = FixtureTransport::new([
                FixtureRead::Data(settings),
                FixtureRead::WaitData(Arc::clone(&refuse), goaway),
            ]);
            super::owned::assert_seed_reorders(&STREAMS);
            let (session, handle) =
                OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
            let session = session.with_stream_seed(super::owned::ORDER_SEED);
            let log = Rc::new(RefCell::new(Vec::new()));
            let run = drive(session, &handle, &[Arc::clone(&refuse)], async {
                complete_credit_wait(|| handle.available_capacity() >= STREAMS.len() as u32).await;
                let mut responses = Vec::new();
                for stream in STREAMS {
                    let upload = RefusalOrderUpload {
                        stream,
                        log: Rc::clone(&log),
                    };
                    let response = handle
                        .reserve_request()
                        .unwrap()
                        .submit(
                            request_head(HttpMethod::Post, false, None),
                            HttpSourceHolder::new(Box::new(upload)),
                            CancellationToken::never(),
                        )
                        .await
                        .unwrap()
                        .into_source_parts();
                    assert_eq!(response.0, StreamId::new(stream));
                    responses.push(response);
                }
                complete_credit_wait(|| {
                    decode_client_frames(&wire.borrow().writes.concat()).header_streams
                        == STREAMS.map(StreamId::new)
                })
                .await;
                refuse.notify_one();
                complete_credit_wait(|| log.borrow().len() == 4).await;
                assert_eq!(
                    log.borrow().as_slice(),
                    [5, 7, 9, 11].map(|stream| (stream, Some(HttpErrorCode::REFUSED_STREAM))),
                    "GOAWAY refuses the streams above 3 in id order"
                );
                for response in &mut responses[2..] {
                    let error = response
                        .1
                        .read_header_event()
                        .await
                        .err()
                        .expect("GOAWAY refuses the response");
                    assert_eq!(error.code(), HttpErrorCode::REFUSED_STREAM);
                }
                assert!(
                    decode_client_frames(&wire.borrow().writes.concat())
                        .resets
                        .is_empty(),
                    "a refused stream sends no RST_STREAM"
                );
            })
            .await;
            cancelled(run);
        })
    });
}

/// Reserves and submits one GET from inside the capacity release that makes room for it,
/// once a handle is armed.
#[derive(Default)]
struct SubmitOnCapacity {
    handle: RefCell<Option<OwnedH2UpstreamHandle>>,
    submission: RefCell<Option<Box<dyn std::any::Any>>>,
}

impl OwnedH2UpstreamObserver for SubmitOnCapacity {
    fn capacity_available(&self) {
        let handle = self.handle.borrow_mut().take();
        if let Some(handle) = handle {
            let submission = handle.reserve_request().unwrap().submit(
                request_head(HttpMethod::Get, true, None),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            *self.submission.borrow_mut() = Some(Box::new(submission));
        }
    }
}

fn header_streams(wire: &RefCell<FixtureTransportState>) -> Vec<StreamId> {
    decode_client_frames(&wire.borrow().writes.concat()).header_streams
}

#[test]
fn queued_submission_proceeds_after_an_in_poll_release() {
    let (written, woken) = run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            let respond = Arc::new(tokio::sync::Notify::new());
            let (transport, wire) = FixtureTransport::new([
                FixtureRead::Data(server_settings_bytes(1)),
                FixtureRead::WaitData(
                    Arc::clone(&respond),
                    response_head(&mut peer, StreamId::new(1), 200, true, None),
                ),
            ]);
            let (session, handle) =
                OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
            let observer = Rc::new(SubmitOnCapacity::default());
            handle.set_observer(observer.clone()).unwrap();
            let first = handle.reserve_request().unwrap().submit(
                request_head(HttpMethod::Get, true, None),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let run = super::owned::OwnWakes::new(session.run());
            let wakes = run.wake_count();
            let script = async {
                let response = first.await.unwrap().into_source_parts();
                complete_credit_wait(|| {
                    header_streams(&wire) == [StreamId::new(1)] && handle.available_capacity() == 0
                })
                .await;
                *observer.handle.borrow_mut() = Some(handle.clone());
                let before = wakes.get();
                respond.notify_one();
                let written = super::owned::drive_until(super::owned::DRIVE_LIMIT, || {
                    header_streams(&wire) == [StreamId::new(1), StreamId::new(3)]
                })
                .await;
                let woken = wakes.get() - before;
                observer.handle.borrow_mut().take();
                drop(observer.submission.borrow_mut().take());
                drop(response);
                handle.cancel();
                (written, woken)
            };
            let (run, outcome) =
                tokio::time::timeout(Duration::from_secs(10), async { tokio::join!(run, script) })
                    .await
                    .expect("the cancelled session joins");
            cancelled(run);
            outcome
        })
    });
    assert!(
        written,
        "the submission made inside the in-poll release is written within the bound"
    );
    assert_eq!(
        woken, 2,
        "session wakes between the response and the second HEADERS: the response read and the \
         second submission's own wake; the in-poll release adds none (3 when it wakes)"
    );
}

#[test]
fn idle_is_published_after_an_in_poll_release() {
    let (idle, reset) = run_on_worker(|context| {
        Box::pin(async move {
            let (transport, wire) =
                FixtureTransport::new([FixtureRead::Data(server_settings_bytes(1))]);
            let (session, handle) =
                OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
            let first = handle.reserve_request().unwrap().submit(
                request_head(HttpMethod::Get, true, None),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let script = async {
                let response = first.await.unwrap().into_source_parts();
                complete_credit_wait(|| header_streams(&wire) == [StreamId::new(1)]).await;
                // Abandoning the response resets the stream; the poll that retires it releases
                // its capacity, and no input follows.
                drop(response);
                let idle =
                    super::owned::drive_until(super::owned::DRIVE_LIMIT, || handle.is_idle()).await;
                let reset = decode_client_frames(&wire.borrow().writes.concat()).resets;
                handle.cancel();
                (idle, reset)
            };
            let (run, outcome) = tokio::time::timeout(Duration::from_secs(10), async {
                tokio::join!(super::owned::OwnWakes::new(session.run()), script)
            })
            .await
            .expect("the cancelled session joins");
            cancelled(run);
            outcome
        })
    });
    assert_eq!(reset, [(StreamId::new(1), H2ErrorCode::CANCEL)]);
    assert!(
        idle,
        "the poll that ends the last stream and releases its capacity publishes idleness"
    );
}

#[test]
fn release_outside_the_poll_still_wakes() {
    let woken = run_on_worker(|context| {
        Box::pin(async move {
            let (transport, _wire) =
                FixtureTransport::new([FixtureRead::Data(server_settings_bytes(1))]);
            let (session, handle) =
                OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
            let run = super::owned::OwnWakes::new(session.run());
            let wakes = run.wake_count();
            let script = async {
                complete_credit_wait(|| handle.is_idle()).await;
                let reservation = handle.reserve_request().unwrap();
                let before = wakes.get();
                drop(reservation);
                let woken = wakes.get() - before;
                handle.cancel();
                woken
            };
            let (run, woken) =
                tokio::time::timeout(Duration::from_secs(10), async { tokio::join!(run, script) })
                    .await
                    .expect("the cancelled session joins");
            cancelled(run);
            woken
        })
    });
    assert_eq!(
        woken, 1,
        "dropping a reservation outside the session's poll wakes the session, as before"
    );
}

/// One admission notification and the session poll it ran in.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReleaseNote {
    Released {
        stream: StreamId,
        poll: usize,
        in_poll: bool,
    },
    Available {
        poll: usize,
        in_poll: bool,
        capacity: u32,
    },
}

/// Records each capacity notification with the session poll it ran in and whether it ran
/// inside that poll.
struct ReleasePollProbe {
    counts: Arc<super::owned::WakeCounts>,
    handle: RefCell<Option<OwnedH2UpstreamHandle>>,
    notes: RefCell<Vec<ReleaseNote>>,
}

impl OwnedH2UpstreamObserver for ReleasePollProbe {
    fn capacity_released(&self, stream: StreamId) {
        self.notes.borrow_mut().push(ReleaseNote::Released {
            stream,
            poll: self.counts.polls(),
            in_poll: self.counts.polling(),
        });
    }

    fn capacity_available(&self) {
        let capacity = self
            .handle
            .borrow()
            .as_ref()
            .map_or(0, OwnedH2UpstreamHandle::available_capacity);
        self.notes.borrow_mut().push(ReleaseNote::Available {
            poll: self.counts.polls(),
            in_poll: self.counts.polling(),
            capacity,
        });
    }
}

/// The session's polls, wakes and wakes inside its own poll, so far.
fn poll_wakes(counts: &super::owned::WakeCounts) -> [usize; 3] {
    [counts.polls(), counts.wakes(), counts.in_poll_wakes()]
}

fn poll_wakes_since(counts: &super::owned::WakeCounts, before: [usize; 3]) -> [usize; 3] {
    let now = poll_wakes(counts);
    std::array::from_fn(|index| now[index] - before[index])
}

#[test]
fn release_inside_own_poll_defers_no_wake() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (reads, gates) = arming_origin_reads();
            let transport = inline_arming_transport(reads, ARMING_CAPACITY);
            let (settled, fetched, (inside, inside_notes), (outside, outside_notes)) =
                run_upstream_arming(
                    context,
                    transport,
                    |_| {},
                    &gates,
                    async |handle, state, counts| {
                        let settled = arming_settings_settle(state).await;
                        let mut fetched = Vec::new();
                        for (index, gate) in gates.iter().enumerate().take(2) {
                            fetched.push(arming_exchange(handle, state, index, gate).await);
                        }
                        let probe = Rc::new(ReleasePollProbe {
                            counts: Arc::clone(counts),
                            handle: RefCell::new(Some(handle.clone())),
                            notes: RefCell::default(),
                        });
                        handle.set_observer(probe.clone()).unwrap();
                        let before = poll_wakes(counts);
                        fetched.push(arming_exchange(handle, state, 2, &gates[2]).await);
                        let inside = (poll_wakes_since(counts, before), probe.notes.take());
                        let reservation = handle.reserve_request().unwrap();
                        let before = poll_wakes(counts);
                        drop(reservation);
                        let outside = (poll_wakes_since(counts, before), probe.notes.take());
                        probe.handle.take();
                        (settled, fetched, inside, outside)
                    },
                )
                .await;
            assert!(settled, "the session acknowledges the origin's SETTINGS");
            assert_eq!(fetched, vec![fetched_ok(); 3], "every request succeeds");
            let [polls, wakes, in_poll_wakes] = inside;
            assert_eq!(
                in_poll_wakes, 0,
                "the response's END_STREAM releases capacity inside the session's own poll, which \
                 runs the submission phase again instead of waking itself (polls {polls}, wakes \
                 {wakes})"
            );
            assert_eq!(
                polls, 3,
                "session polls for the third request: the poll that retires the response follows \
                 the consumer's wake, and the release adds none (wakes {wakes})"
            );
            match inside_notes[..] {
                [
                    ReleaseNote::Released {
                        stream,
                        poll: released,
                        in_poll: true,
                    },
                    ReleaseNote::Available {
                        poll: available,
                        in_poll: true,
                        capacity,
                    },
                ] => {
                    assert_eq!(stream, StreamId::new(5));
                    assert_eq!(
                        released, available,
                        "the release and the capacity it frees are observed in the same poll"
                    );
                    assert!(capacity > 0, "capacity is available again in that poll");
                }
                _ => panic!(
                    "the observer sees capacity_released then capacity_available inside one \
                     session poll: {inside_notes:?}"
                ),
            }
            let [polls, wakes, in_poll_wakes] = outside;
            assert_eq!(
                (wakes, in_poll_wakes, polls),
                (1, 0, 0),
                "dropping a reservation outside the session's poll wakes the session once"
            );
            assert!(
                matches!(
                    outside_notes[..],
                    [ReleaseNote::Available { in_poll: false, capacity, .. }] if capacity > 0
                ),
                "the release outside the poll is observed there: {outside_notes:?}"
            );
        })
    });
}

#[test]
fn upstream_sessions_never_query_saturation() {
    let ((read, written, run), idle_before_ping, queries) = run_on_worker(|context| {
        Box::pin(super::owned::unarmed_upstream_transcripts_with(
            context, true,
        ))
    });
    assert_eq!(queries, 0, "the upstream exchange never queries saturation");
    assert_eq!(
        [read.digest(), written.digest()],
        [
            (9, 3_188_096_049_290_309_849),
            (10, 18_252_902_388_575_904_042)
        ],
        "the unarmed upstream transcripts recorded at da1228fe4a74"
    );
    super::owned::assert_transcript_scenario(&read, &written);
    assert!(idle_before_ping, "the session is idle before the PING");
    assert_eq!(run, Ok(()), "the upstream session closes cleanly at EOF");

    run_on_worker(|context| {
        Box::pin(async move {
            let stream = StreamId::new(1);
            let respond = Arc::new(tokio::sync::Notify::new());
            let upload_ready = Arc::new(tokio::sync::Notify::new());
            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            let mut response = response_head(&mut peer, stream, 200, false, Some(4));
            response.extend(eligibility_frame(
                &mut peer,
                EgressFrame::Body {
                    stream,
                    body: queue(b"done"),
                    padding: None,
                    end_stream: true,
                },
            ));
            let transport = super::owned::QueuedFixtureTransport::reporting(
                [
                    FixtureRead::Data(server_settings_bytes(1)),
                    FixtureRead::WaitData(Arc::clone(&respond), response),
                ],
                Some(Rc::new(Cell::new(false))),
                true,
            );
            transport.probe.borrow_mut().complete_inline = true;
            let queries = Rc::clone(&transport.saturation_queries);
            let mut config = options(context, transport);
            config.http2.max_concurrent_outgoing_streams = 1;
            let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
            let mut upload = Upload::new(1, 1);
            upload.gate = Some((0, Arc::clone(&upload_ready)));
            let (upload, probe) = upload.held();
            let submission = handle.reserve_request().unwrap().submit(
                request_head(HttpMethod::Post, false, Some(1)),
                upload,
                CancellationToken::never(),
            );
            cancelled(
                drive(
                    session,
                    &handle,
                    &[Arc::clone(&respond), Arc::clone(&upload_ready)],
                    async {
                        let (_, mut source, request) =
                            submission.await.unwrap().into_source_parts();
                        respond.notify_one();
                        source.read_header_event().await.unwrap();
                        while !source.read_body_event(u32::MAX).await.unwrap().eom() {}
                        assert_eq!(probe.bytes.get(), 0, "the request body is still gated");
                        upload_ready.notify_one();
                        complete_credit_wait(|| request.capacity_released()).await;
                        assert_eq!(probe.bytes.get(), 1, "the request completed");
                        assert_eq!(probe.drops.get(), 1);
                        drop((source, request));
                        complete_credit_wait(|| handle.is_idle()).await;
                    },
                )
                .await,
            );
            assert_eq!(
                queries.get(),
                0,
                "a request whose END_STREAM is serialized after its complete response never \
                 queries saturation"
            );
        })
    });
}

/// One response event as `(kind, bytes, eom)`: body bytes, or the trailers' `grpc-status`.
type ReadEvent = (&'static str, Vec<u8>, bool);

/// A response head as `(status, grpc-status, eom)`.
type ReadHead = (u16, Vec<u8>, bool);

/// Reads the response head, then, unless it ends the message, body events through EOM;
/// suspends are awaited and not reported.
async fn read_exclusive_response(
    owner: &HttpResponseSourceOwner,
    reader: &mut HttpResponseReader,
) -> (ReadHead, Vec<ReadEvent>) {
    reader.arm_header(owner).unwrap();
    let mut head = poll_fn(|context| reader.poll_header(context))
        .await
        .expect("the response head reads cleanly");
    let mut eom = head.eom();
    let head = (
        head.headers_mut().status_code().unwrap(),
        head.headers()
            .headers()
            .get_single_or_empty(b"grpc-status")
            .to_vec(),
        eom,
    );
    let mut events = Vec::new();
    while !eom {
        reader.arm_body(owner, u32::MAX).unwrap();
        let event = poll_fn(|context| reader.poll_body(context))
            .await
            .expect("the response body reads cleanly");
        eom = event.eom();
        match event.into_kind() {
            HttpBodyEventKind::Body(bytes) => {
                let length = bytes.chain_length();
                let bytes = if length == 0 {
                    Vec::new()
                } else {
                    bytes.coalesce(length).unwrap().to_vec()
                };
                events.push(("body", bytes, eom));
            }
            HttpBodyEventKind::Trailers(trailers) => {
                assert_eq!(trailers.len(), 1, "the trailers carry one field");
                events.push((
                    "trailers",
                    trailers.get_single_or_empty(b"grpc-status").to_vec(),
                    eom,
                ));
            }
            HttpBodyEventKind::Suspend(resume) => {
                resume.await;
            }
            _ => panic!("unexpected response body event"),
        }
    }
    (head, events)
}

/// Submits one GET to an origin limited to one stream, feeds `response` once the
/// submission resolves, and reads it through the exclusive reader. Checks that the
/// request's capacity is released by the time the read ends and the handle's available
/// capacity is restored.
fn exclusive_response_case(
    response: impl FnOnce(&mut Http2Codec, StreamId) -> Vec<u8> + Send + 'static,
) -> (ReadHead, Vec<ReadEvent>) {
    run_on_worker(move |context| {
        Box::pin(async move {
            let stream = StreamId::new(1);
            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            let respond = Arc::new(tokio::sync::Notify::new());
            let (transport, wire) = FixtureTransport::new([
                FixtureRead::Data(server_settings_bytes(1)),
                FixtureRead::WaitData(Arc::clone(&respond), response(&mut peer, stream)),
            ]);
            let mut config = options(context, transport);
            config.http2.max_concurrent_outgoing_streams = 1;
            let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
            let read = RefCell::new(None);
            cancelled(
                drive(session, &handle, &[Arc::clone(&respond)], async {
                    complete_credit_wait(|| handle.available_capacity() == 1).await;
                    let before = handle.available_capacity();
                    let response = handle
                        .reserve_request()
                        .unwrap()
                        .submit_exclusive(
                            request_head(HttpMethod::Get, true, None),
                            HttpSourceHolder::empty(),
                            CancellationToken::never(),
                        )
                        .await
                        .unwrap();
                    assert_eq!(response.stream, stream);
                    assert_eq!(handle.available_capacity(), before - 1);
                    assert!(!response.control.capacity_released());
                    let (owner, mut reader) = response.source.split();
                    respond.notify_one();
                    let result = read_exclusive_response(&owner, &mut reader).await;
                    assert!(
                        response.control.capacity_released(),
                        "the response's end released the request's capacity"
                    );
                    assert_eq!(handle.available_capacity(), before);
                    drop((owner, reader));
                    complete_credit_wait(|| handle.is_idle()).await;
                    let decoded = decode_client_frames(&wire.borrow().writes.concat());
                    assert_eq!(decoded.header_streams, [stream]);
                    assert!(decoded.resets.is_empty());
                    *read.borrow_mut() = Some(result);
                })
                .await,
            );
            read.into_inner().unwrap()
        })
    })
}

#[test]
fn response_trailers_reach_the_reader_with_eom_and_release_capacity() {
    let (head, events) = exclusive_response_case(|peer, stream| {
        let mut wire = response_head(peer, stream, 200, false, None);
        wire.extend(eligibility_frame(
            peer,
            EgressFrame::Body {
                stream,
                body: queue(b"reply"),
                padding: None,
                end_stream: false,
            },
        ));
        let mut trailers = HttpHeaders::new();
        trailers.add("grpc-status", "0");
        wire.extend(eligibility_frame(
            peer,
            EgressFrame::Trailers {
                stream,
                trailers: &trailers,
            },
        ));
        wire
    });
    assert_eq!(head, (200, Vec::new(), false));
    assert_eq!(
        events,
        [
            ("body", b"reply".to_vec(), false),
            ("trailers", b"0".to_vec(), true),
        ],
        "the head, the body, then the trailers carrying EOM"
    );
}

#[test]
fn trailers_only_response_is_a_final_head() {
    let (head, events) = exclusive_response_case(|peer, stream| {
        let mut message = HttpMessage::new();
        message.set_status_code(200).unwrap();
        message.set_http_version(1, 1);
        message.headers_mut().add("grpc-status", "0");
        eligibility_frame(
            peer,
            EgressFrame::Headers {
                stream,
                message: &message,
                end_stream: true,
                extra_headers: None,
            },
        )
    });
    assert_eq!(
        head,
        (200, b"0".to_vec(), true),
        "one final head carrying the status fields and EOM"
    );
    assert!(events.is_empty(), "no body or trailers event follows");
}

/// Records admission notifications through the protocol-neutral observer contract.
#[derive(Default)]
struct GenericAdmission {
    notes: RefCell<Vec<&'static str>>,
}

impl crate::owned::OwnedUpstreamObserver for GenericAdmission {
    fn capacity_released(&self, stream: StreamId) {
        assert_eq!(stream, StreamId::new(1));
        self.notes.borrow_mut().push("capacity_released");
    }

    fn capacity_available(&self) {
        self.notes.borrow_mut().push("capacity_available");
    }
}

#[test]
fn generic_upstream_observer_installs_on_owned_h2() {
    run_on_worker(|context| {
        Box::pin(async move {
            let respond = Arc::new(tokio::sync::Notify::new());
            let (transport, _) = FixtureTransport::new([
                FixtureRead::Data(server_settings_bytes(100)),
                FixtureRead::WaitData(
                    Arc::clone(&respond),
                    server_empty_responses_bytes([StreamId::new(1)]),
                ),
            ]);
            let (session, handle) =
                OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
            let observer = Rc::new(GenericAdmission::default());
            let generic: Rc<dyn crate::owned::OwnedUpstreamObserver> = observer.clone();
            handle.set_observer(generic).unwrap();
            cancelled(
                drive(session, &handle, &[Arc::clone(&respond)], async {
                    let response = handle
                        .reserve_request()
                        .unwrap()
                        .submit(
                            request_head(HttpMethod::Get, true, None),
                            HttpSourceHolder::empty(),
                            CancellationToken::never(),
                        )
                        .await
                        .unwrap()
                        .into_source_parts();
                    assert!(observer.notes.borrow().is_empty());
                    respond.notify_one();
                    response
                        .1
                        .ingress_completion()
                        .unwrap()
                        .wait()
                        .await
                        .unwrap();
                    complete_credit_wait(|| response.2.capacity_released()).await;
                    assert_eq!(
                        observer.notes.borrow().as_slice(),
                        ["capacity_released", "capacity_available"],
                        "the one request's release precedes the capacity it frees"
                    );
                })
                .await,
            );
        })
    });
}

/// An idle close parks while its outstanding read has yet to complete, and finishes once that
/// read completes.
#[test]
fn idle_close_parks_until_the_read_completes() {
    crate::external_host_tests::run_on_host_with(|host, context| {
        Box::pin(async move {
            let gate = Arc::new(tokio::sync::Notify::new());
            // The read ignores cancellation: it completes only when the test releases it.
            let (transport, wire) =
                FixtureTransport::new([FixtureRead::WaitData(Arc::clone(&gate), Vec::new())]);
            let (session, handle) =
                OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
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
                assert_eq!(wire.borrow().close, FixtureClose::Open);
                gate.notify_one();
            });
            assert!(!run.unwrap().reset);
            assert!(handle.is_finished());
            assert_eq!(wire.borrow().close, FixtureClose::Closed);
            let decoded = decode_client_frames(&wire.borrow().writes.concat());
            assert_eq!(
                decoded.goaways,
                [(StreamId::UNSPECIFIED, H2ErrorCode::NO_ERROR)]
            );
        })
    });
}

/// An idle close polls the outstanding read again after requesting its cancellation, so a read
/// that observes cancellation only when polled still completes the close.
#[test]
fn idle_close_polls_the_read_again_after_cancelling_it() {
    crate::external_host_tests::run_on_host(|context| {
        Box::pin(async move {
            let (inner, wire) = FixtureTransport::new([]);
            let transport = Rc::new(super::owned::PolledCancellationTransport(inner));
            let (session, handle) =
                OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
            let (run, ()) = futures::join!(session.run(), async {
                assert!(handle.close_when_idle().unwrap());
            });
            assert!(!run.unwrap().reset);
            assert!(handle.is_finished());
            assert_eq!(wire.borrow().close, FixtureClose::Closed);
        })
    });
}

/// What one GET observes once its whole response has arrived and released the request's
/// capacity, before the response source is read.
struct ReleasedExchange {
    /// The request's capacity was released before the source was read.
    released: bool,
    /// Session polls, wakes and in-poll wakes over a settle with nothing to do.
    quiet: [usize; 3],
    /// The same over a settle that starts with the request's cancel, when `cancel` is set.
    after_cancel: [usize; 3],
    /// The response, read after that settle.
    read: Fetched,
    /// The session is idle once the read response is dropped.
    idle: bool,
    /// Session polls, wakes and in-poll wakes over a settle at idle that starts with a
    /// second cancel, when `cancel` is set.
    idle_cancel: [usize; 3],
    /// The session is still idle after that settle.
    still_idle: bool,
    /// Every byte the session wrote.
    written: Vec<u8>,
    /// The streams the session reset, with their codes.
    resets: Vec<(StreamId, H2ErrorCode)>,
}

/// Submits one GET to an origin limited to one stream, whose 200 with a 4-byte body ends the
/// stream, and waits until that response releases the request's capacity; then, with
/// `cancel`, cancels the request through its control. Then reads the response, drops it, waits
/// for idleness and, with `cancel`, cancels again. The session is polled only on its own
/// wakes, as a host polls a separately scheduled task.
fn released_exchange(cancel: bool) -> ReleasedExchange {
    run_on_worker(move |context| {
        Box::pin(async move {
            let stream = StreamId::new(1);
            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            let respond = Arc::new(tokio::sync::Notify::new());
            let mut response = response_head(&mut peer, stream, 200, false, Some(4));
            response.extend(eligibility_frame(
                &mut peer,
                EgressFrame::Body {
                    stream,
                    body: queue(b"done"),
                    padding: None,
                    end_stream: true,
                },
            ));
            let (transport, wire) = FixtureTransport::new([
                FixtureRead::Data(server_settings_bytes(1)),
                FixtureRead::WaitData(Arc::clone(&respond), response),
            ]);
            let mut config = options(context, transport);
            config.http2.max_concurrent_outgoing_streams = 1;
            let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                request_head(HttpMethod::Get, true, None),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let (run, counts) = super::owned::CountedWakes::new(session.run());
            let script = async {
                let (_, mut source, control) = submission.await.unwrap().into_source_parts();
                respond.notify_one();
                let released = super::owned::drive_until(super::owned::DRIVE_LIMIT, || {
                    control.capacity_released()
                })
                .await;
                super::owned::writer_settle().await;
                let before = poll_wakes(&counts);
                super::owned::writer_settle().await;
                let quiet = poll_wakes_since(&counts, before);
                let before = poll_wakes(&counts);
                if cancel {
                    control.cancel(HttpError::new(HttpErrorCode::CANCEL));
                }
                super::owned::writer_settle().await;
                let after_cancel = poll_wakes_since(&counts, before);
                let read = read_fetched(&mut source).await;
                drop(source);
                let idle =
                    super::owned::drive_until(super::owned::DRIVE_LIMIT, || handle.is_idle()).await;
                super::owned::writer_settle().await;
                let before = poll_wakes(&counts);
                if cancel {
                    control.cancel(HttpError::new(HttpErrorCode::CANCEL));
                }
                super::owned::writer_settle().await;
                let idle_cancel = poll_wakes_since(&counts, before);
                let still_idle = handle.is_idle();
                let written = wire.borrow().writes.concat();
                let resets = decode_client_frames(&written).resets;
                drop(control);
                handle.cancel();
                ReleasedExchange {
                    released,
                    quiet,
                    after_cancel,
                    read,
                    idle,
                    idle_cancel,
                    still_idle,
                    written,
                    resets,
                }
            };
            let (run, observed) =
                tokio::time::timeout(Duration::from_secs(10), async { tokio::join!(run, script) })
                    .await
                    .expect("the cancelled session joins");
            cancelled(run);
            observed
        })
    })
}

#[test]
fn cancel_after_release_schedules_no_wake() {
    let uncancelled = released_exchange(false);
    let cancelled = released_exchange(true);
    for exchange in [&uncancelled, &cancelled] {
        assert!(
            exchange.released,
            "the response's end releases the request's capacity before its source is read"
        );
        assert_eq!(
            exchange.quiet,
            [0, 0, 0],
            "a settled session holding an unread, ended response is neither polled nor woken \
             (polls, wakes, in-poll wakes)"
        );
    }
    assert_eq!(
        uncancelled.after_cancel,
        [0, 0, 0],
        "without a cancel the session stays unpolled (polls, wakes, in-poll wakes)"
    );
    assert_eq!(
        cancelled.after_cancel,
        [0, 0, 0],
        "cancelling a request whose capacity is released neither wakes nor polls the session \
         (polls, wakes, in-poll wakes)"
    );
}

#[test]
fn cancel_after_release_changes_no_outcome() {
    let uncancelled = released_exchange(false);
    let cancelled = released_exchange(true);
    assert_eq!(
        cancelled.read,
        Ok((200, b"done".to_vec())),
        "the already-ended response reads whole after the cancel"
    );
    assert_eq!(
        cancelled.read, uncancelled.read,
        "the response reads exactly as without the cancel"
    );
    for exchange in [&uncancelled, &cancelled] {
        assert!(
            exchange.idle,
            "dropping the read response leaves the session idle within the bound"
        );
        assert!(
            exchange.resets.is_empty(),
            "no RST_STREAM is written: {:?}",
            exchange.resets
        );
    }
    assert!(
        cancelled.written == uncancelled.written,
        "the cancels add nothing to the wire ({} bytes written with them, {} without)",
        cancelled.written.len(),
        uncancelled.written.len()
    );
    assert_eq!(
        cancelled.idle_cancel,
        [0, 0, 0],
        "a cancel of the retired request neither wakes nor polls the idle session (polls, \
         wakes, in-poll wakes)"
    );
    assert!(
        cancelled.still_idle,
        "the session stays idle after a cancel of its retired request"
    );
}

#[test]
fn cancel_before_release_still_resets() {
    let (head, released_before, reset, body, idle, released_after, resets) =
        run_on_worker(|context| {
            Box::pin(async move {
                let stream = StreamId::new(1);
                let mut peer = Http2Codec::new(TransportDirection::Downstream);
                let respond = Arc::new(tokio::sync::Notify::new());
                let (transport, wire) = FixtureTransport::new([
                    FixtureRead::Data(server_settings_bytes(1)),
                    FixtureRead::WaitData(
                        Arc::clone(&respond),
                        response_head(&mut peer, stream, 200, false, Some(4)),
                    ),
                ]);
                let mut config = options(context, transport);
                config.http2.max_concurrent_outgoing_streams = 1;
                let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
                let submission = handle.reserve_request().unwrap().submit(
                    request_head(HttpMethod::Get, true, None),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                let script = async {
                    let (_, mut source, control) = submission.await.unwrap().into_source_parts();
                    respond.notify_one();
                    let mut head = source.read_header_event().await.unwrap();
                    let head = (head.headers_mut().status_code().unwrap(), head.eom());
                    let released_before = control.capacity_released();
                    control.cancel(HttpError::new(HttpErrorCode::CANCEL));
                    let reset = super::owned::drive_until(super::owned::DRIVE_LIMIT, || {
                        !decode_client_frames(&wire.borrow().writes.concat())
                            .resets
                            .is_empty()
                    })
                    .await;
                    // Without the reset the body never arrives, so it is read only after one.
                    let body = if reset {
                        Some(
                            source
                                .read_body_event(u32::MAX)
                                .await
                                .map(|event| event.eom())
                                .map_err(|error| error.code()),
                        )
                    } else {
                        None
                    };
                    drop(source);
                    let idle =
                        super::owned::drive_until(super::owned::DRIVE_LIMIT, || handle.is_idle())
                            .await;
                    let released_after = control.capacity_released();
                    let resets = decode_client_frames(&wire.borrow().writes.concat()).resets;
                    drop(control);
                    handle.cancel();
                    (
                        head,
                        released_before,
                        reset,
                        body,
                        idle,
                        released_after,
                        resets,
                    )
                };
                let (run, observed) = tokio::time::timeout(Duration::from_secs(10), async {
                    tokio::join!(super::owned::OwnWakes::new(session.run()), script)
                })
                .await
                .expect("the cancelled session joins");
                cancelled(run);
                observed
            })
        });
    assert_eq!(
        head,
        (200, false),
        "the response head arrives without ending the stream"
    );
    assert!(
        !released_before,
        "mid-response, the request still holds its capacity"
    );
    assert!(
        reset,
        "a cancel before the release resets the stream within the bound"
    );
    assert_eq!(
        resets,
        [(StreamId::new(1), H2ErrorCode::CANCEL)],
        "one RST_STREAM with CANCEL"
    );
    assert_eq!(
        body,
        Some(Err(HttpErrorCode::CANCEL)),
        "the response source observes the cancel's cause"
    );
    assert!(idle, "the reset stream retires and the session is idle");
    assert!(released_after, "the retired stream releases its capacity");
}

/// An idle upstream connection drains at its read timeout, not a millisecond earlier, as
/// Proxygen's read timeout calls `initiateDrain`: one GOAWAY, admission closes, and with no
/// stream left the connection closes.
#[test]
fn upstream_idle_connection_drains_at_its_read_timeout() {
    super::owned::on_both_hosts(|host, context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            let settings = arming_origin_settings(&mut peer);
            let (fixture, wire) = FixtureTransport::new([FixtureRead::Data(settings)]);
            let mut config = options(context, Rc::new(super::owned::UntimedReads(fixture)));
            config.io.read_timeout = super::owned::IDLE;
            let (session, handle) =
                OwnedH2UpstreamSession::new(config).expect("timed owned upstream H2");
            let (run, ()) = futures::join!(session.run(), async {
                super::owned::settle_turns(32).await;
                host.advance(super::owned::IDLE - Duration::from_millis(1));
                super::owned::settle_turns(32).await;
                assert!(
                    decode_client_frames(&wire.borrow().writes.concat())
                        .goaways
                        .is_empty(),
                    "the drain came early"
                );
                assert_eq!(wire.borrow().close, FixtureClose::Open);
                host.advance(Duration::from_millis(1));
            });
            run.expect("an idle drain closes cleanly");
            assert_eq!(
                decode_client_frames(&wire.borrow().writes.concat()).goaways,
                [(StreamId::UNSPECIFIED, H2ErrorCode::NO_ERROR)]
            );
            assert_eq!(wire.borrow().close, FixtureClose::Closed);
            assert!(
                handle.reserve_request().is_err(),
                "the drain closed admission"
            );
        })
    });
}

/// What an upstream driver did while its connection idled past its read timeout: each GOAWAY
/// with the millisecond of paused time it was first seen at, the response it delivered, and how
/// the session ended.
#[derive(Debug, Eq, PartialEq)]
struct UpstreamIdleWire {
    goaways: Vec<(u64, StreamId, H2ErrorCode)>,
    /// The response's status and the millisecond it was read, for a request sent at the start.
    response: Option<(u16, u64)>,
    result: Result<(), HttpErrorCode>,
}

/// Runs an upstream connection with `super::owned::IDLE` as its read timeout on the owned driver
/// or on `UniplexSession`. With `response_at`, a GET goes out at the start and the origin answers
/// it at that instant.
async fn upstream_idle_wire(
    context: WorkerContext,
    owned: bool,
    response_at: Option<Duration>,
) -> UpstreamIdleWire {
    let mut peer = Http2Codec::new(TransportDirection::Downstream);
    let mut script = VecDeque::from([(None, arming_origin_settings(&mut peer))]);
    let gate = Arc::new(tokio::sync::Notify::new());
    if response_at.is_some() {
        script.push_back((
            Some(Arc::clone(&gate)),
            response_head(&mut peer, StreamId::new(1), 200, true, None),
        ));
    }
    let (inner, wire) = FixtureTransport::new([]);
    let transport = Rc::new(super::owned::GatedReads {
        inner,
        script: Rc::new(RefCell::new(script)),
    });
    let begun = tokio::time::Instant::now();
    let done = Rc::new(Cell::new(false));
    let response = Rc::new(Cell::new(None));
    type Run = Pin<Box<dyn Future<Output = Result<UniplexRunSummary, HttpError>>>>;
    type Exchange = Pin<Box<dyn Future<Output = ()>>>;
    let record = {
        let response = Rc::clone(&response);
        move |status: u16| response.set(Some((status, begun.elapsed().as_millis() as u64)))
    };
    let (session, exchange): (Run, Exchange) = if owned {
        let mut config = options(context, transport);
        config.io.read_timeout = super::owned::IDLE;
        let (session, handle) = OwnedH2UpstreamSession::new(config).expect("owned upstream H2");
        let exchange: Exchange = if response_at.is_some() {
            let submission = handle.reserve_request().unwrap().submit(
                request_head(HttpMethod::Get, true, None),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            Box::pin(async move {
                let (_, mut source, _control) = submission
                    .await
                    .expect("the origin answers")
                    .into_source_parts();
                let mut head = source.read_header_event().await.expect("response head");
                record(head.headers_mut().status_code().unwrap());
            })
        } else {
            Box::pin(async {})
        };
        (Box::pin(session.run()), exchange)
    } else {
        let config = UniplexConfig {
            read_timeout: super::owned::IDLE,
            stream_read_timeout: Duration::ZERO,
            write_timeout: Duration::ZERO,
            ..UniplexConfig::default()
        };
        let http2 = Http2SessionConfig {
            settings: vec![roxygen_codec::CodecSetting {
                identifier: SettingsId::EnablePush.as_u16(),
                value: 0,
            }],
            double_goaway_drain: false,
            goaway_timeout: Duration::ZERO,
            ..Http2SessionConfig::default()
        };
        let (session, handle) = UniplexSession::new_http2(
            context,
            TransportDirection::Upstream,
            transport,
            None,
            config,
            http2,
        )
        .expect("Uniplex upstream H2");
        let exchange: Exchange = if response_at.is_some() {
            Box::pin(async move {
                let request =
                    HttpFixedSource::make_get_request(b"/owned-upstream", None).expect("fixed GET");
                let mut source = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationSource::new().token(),
                    )
                    .await
                    .expect("request admitted")
                    .source;
                let mut head = source.read_header_event().await.expect("response head");
                record(head.headers_mut().status_code().unwrap());
            })
        } else {
            Box::pin(async {})
        };
        (Box::pin(session.run()), exchange)
    };
    let monitor = async {
        let mut goaways = Vec::new();
        loop {
            let at = begun.elapsed();
            if response_at.is_some_and(|response_at| at >= response_at) {
                gate.notify_one();
            }
            let ms = at.as_millis() as u64;
            let written = decode_client_frames(&wire.borrow().writes.concat()).goaways;
            goaways.extend(
                written[goaways.len()..]
                    .iter()
                    .map(|(stream, code)| (ms, *stream, *code)),
            );
            if done.get() {
                return goaways;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    let run = async {
        let result = session.await;
        done.set(true);
        result
    };
    let (result, goaways, ()) = tokio::time::timeout(Duration::from_secs(60), async {
        tokio::join!(run, monitor, exchange)
    })
    .await
    .expect("the idle upstream connection ends");
    UpstreamIdleWire {
        goaways,
        response: response.get(),
        result: result.map(|_| ()).map_err(|error| error.code()),
    }
}

/// Upstream read-timeout drains, owned against `UniplexSession` on the same paused clock: an idle
/// connection, and one whose request is still unanswered at the timeout, send the same GOAWAY at
/// the same instant; the late response is still delivered, and both connections then close
/// cleanly.
#[test]
fn owned_upstream_idle_drain_matches_uniplex() {
    let idle = super::owned::IDLE.as_millis() as u64;
    for response_at in [None, Some(super::owned::IDLE + Duration::from_millis(500))] {
        let (owned, uniplex) = super::owned::run_on_paused_worker(move |context| {
            Box::pin(async move {
                let owned = upstream_idle_wire(context.clone(), true, response_at).await;
                let uniplex = upstream_idle_wire(context, false, response_at).await;
                (owned, uniplex)
            })
        });
        assert_eq!(
            owned
                .goaways
                .iter()
                .map(|(_, stream, code)| (*stream, *code))
                .collect::<Vec<_>>(),
            uniplex
                .goaways
                .iter()
                .map(|(_, stream, code)| (*stream, *code))
                .collect::<Vec<_>>(),
            "owned {owned:?}, uniplex {uniplex:?}"
        );
        for ((owned_at, ..), (uniplex_at, ..)) in owned.goaways.iter().zip(&uniplex.goaways) {
            assert!(
                owned_at.abs_diff(*uniplex_at) <= 1,
                "owned {owned:?}, uniplex {uniplex:?}"
            );
        }
        assert_eq!(
            owned
                .goaways
                .iter()
                .map(|(_, stream, code)| (*stream, *code))
                .collect::<Vec<_>>(),
            [(StreamId::UNSPECIFIED, H2ErrorCode::NO_ERROR)]
        );
        assert!(owned.goaways[0].0.abs_diff(idle) <= 1, "owned {owned:?}");
        assert_eq!(
            owned.response.map(|(status, _)| status),
            response_at.map(|_| 200)
        );
        assert_eq!(
            owned.response.map(|(status, _)| status),
            uniplex.response.map(|(status, _)| status)
        );
        assert_eq!(owned.result, Ok(()));
        assert_eq!(owned.result, uniplex.result);
    }
}

/// An upload blocked on its stream's send window is reset with FLOW_CONTROL_ERROR when the write
/// timeout passes with no activity, not a millisecond earlier, as Proxygen's
/// `handleWriteEventTimeout` resets every flow-blocked stream, and its response fails with it.
#[test]
fn upstream_flow_control_timeout_resets_the_blocked_upload() {
    super::owned::on_both_hosts(|host, context| {
        Box::pin(async move {
            const UPLOAD: usize = 96 << 10;
            const WRITE: Duration = Duration::from_secs(1);
            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            let mut input = arming_origin_settings(&mut peer);
            input.extend(eligibility_frame(
                &mut peer,
                EgressFrame::WindowUpdate {
                    stream: StreamId::UNSPECIFIED,
                    amount: 65_535,
                },
            ));
            let (fixture, wire) = FixtureTransport::new([FixtureRead::Data(input)]);
            let mut config = options(context, Rc::new(super::owned::UntimedReads(fixture)));
            config.io.write_timeout = WRITE;
            let (session, handle) =
                OwnedH2UpstreamSession::new(config).expect("write-timed owned upstream H2");
            let (upload, _probe) = Upload::new(UPLOAD, 4096).held();
            let submission = handle.reserve_request().unwrap().submit(
                request_head(HttpMethod::Post, false, Some(UPLOAD)),
                upload,
                CancellationToken::never(),
            );
            let body_bytes = || decode_client_frames(&wire.borrow().writes.concat()).body_bytes;
            let resets = || decode_client_frames(&wire.borrow().writes.concat()).resets;
            let (run, ()) = futures::join!(session.run(), async {
                let (_, mut source, _control) = submission
                    .await
                    .expect("the upload is committed")
                    .into_source_parts();
                complete_credit_wait(|| body_bytes() == 65_535).await;
                super::owned::settle_turns(32).await;
                host.advance(WRITE - Duration::from_millis(1));
                super::owned::settle_turns(32).await;
                assert!(resets().is_empty(), "the flow-control timeout came early");
                host.advance(Duration::from_millis(1));
                let error = match source.read_header_event().await {
                    Ok(_) => panic!("the blocked upload got a response"),
                    Err(error) => error,
                };
                assert_eq!(error.code(), HttpErrorCode::FLOW_CONTROL_ERROR);
                assert_eq!(
                    resets(),
                    [(StreamId::new(1), H2ErrorCode::FLOW_CONTROL_ERROR)]
                );
                assert_eq!(body_bytes(), 65_535);
                assert!(handle.close_when_idle().unwrap());
            });
            run.expect("the drained connection closes cleanly");
        })
    });
}

/// Under a write timeout the writer's flow-control wait observes every queued write's
/// completion. Over a capacity-only queue that still has room, the write that exhausts an
/// upload's stream window completes while the upload awaits its response, which is no wake for
/// an untimed session; the timed session still resets the upload at its flow-control timeout,
/// not a millisecond later, and is polled only on its own wakes meanwhile.
#[test]
fn queued_upload_exhausting_its_window_is_reset_at_its_flow_control_timeout() {
    const UPLOAD: usize = 96 << 10;
    const WRITE: Duration = Duration::from_secs(1);
    let (write_timeout, reset_at, error, resets) = super::owned::run_on_paused_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            let mut input = arming_origin_settings(&mut peer);
            input.extend(eligibility_frame(
                &mut peer,
                EgressFrame::WindowUpdate {
                    stream: StreamId::UNSPECIFIED,
                    amount: 65_535,
                },
            ));
            let transport =
                inline_arming_transport(vec![FixtureRead::Data(input)], ARMING_CAPACITY);
            let begun = tokio::time::Instant::now();
            run_upstream_arming(
                context,
                transport,
                |config| config.io.write_timeout = WRITE,
                &[],
                async |handle, state, _counts| {
                    let (upload, _probe) = Upload::new(UPLOAD, 4096).held();
                    let submission = handle.reserve_request().unwrap().submit(
                        request_head(HttpMethod::Post, false, Some(UPLOAD)),
                        upload,
                        CancellationToken::never(),
                    );
                    let (_, mut source, _control) = submission
                        .await
                        .expect("the upload is committed")
                        .into_source_parts();
                    let error = tokio::time::timeout(4 * WRITE, source.read_header_event())
                        .await
                        .map(|head| head.err().map(|error| error.code()));
                    let reset_at = begun.elapsed();
                    let resets = decode_client_frames(&state.borrow().writes.concat()).resets;
                    (state.borrow().write_timeout, reset_at, error, resets)
                },
            )
            .await
        })
    });
    assert_eq!(write_timeout, WRITE);
    assert_eq!(
        error,
        Ok(Some(HttpErrorCode::FLOW_CONTROL_ERROR)),
        "the blocked upload was not reset"
    );
    assert_eq!(reset_at, WRITE);
    assert_eq!(
        resets,
        [(StreamId::new(1), H2ErrorCode::FLOW_CONTROL_ERROR)]
    );
}

/// What an upstream driver did while an upload waited on its stream's send window beside a
/// header-only response the client consumed later: each RST_STREAM with the millisecond of
/// paused time it was first seen at, and how the upload's response failed.
#[derive(Debug)]
struct RetirementFlowWire {
    resets: Vec<(u64, StreamId, H2ErrorCode)>,
    upload: Option<HttpErrorCode>,
}

/// Runs upload A (stream 1), blocked on its stream window, beside GET B (stream 3), on the owned
/// driver or on `UniplexSession`, both with `write_timeout` as their write timeout. The origin
/// answers B with a header-only response at `respond_at`; the client consumes it at
/// `consume_at`, which retires B without a byte read or written. The client drains the connection
/// at `drain_at`.
async fn retirement_flow_wire(
    context: WorkerContext,
    owned: bool,
    write_timeout: Duration,
    respond_at: Duration,
    consume_at: Duration,
    drain_at: Duration,
) -> RetirementFlowWire {
    const UPLOAD: usize = 96 << 10;
    let mut peer = Http2Codec::new(TransportDirection::Downstream);
    let mut input = arming_origin_settings(&mut peer);
    input.extend(eligibility_frame(
        &mut peer,
        EgressFrame::WindowUpdate {
            stream: StreamId::UNSPECIFIED,
            amount: 65_535,
        },
    ));
    let gate = Arc::new(tokio::sync::Notify::new());
    let script = VecDeque::from([
        (None, input),
        (
            Some(Arc::clone(&gate)),
            response_head(&mut peer, StreamId::new(3), 200, true, None),
        ),
    ]);
    let (inner, wire) = FixtureTransport::new([]);
    let transport = Rc::new(super::owned::GatedReads {
        inner,
        script: Rc::new(RefCell::new(script)),
    });
    let begun = tokio::time::Instant::now();
    let done = Rc::new(Cell::new(false));
    type Run = Pin<Box<dyn Future<Output = Result<UniplexRunSummary, HttpError>>>>;
    type Response = Pin<Box<dyn Future<Output = Result<HttpSourceHolder, HttpError>>>>;
    let (session, upload, fetch, drain): (Run, Response, Response, Box<dyn Fn()>) = if owned {
        let mut config = options(context, transport);
        config.io.write_timeout = write_timeout;
        let (session, handle) = OwnedH2UpstreamSession::new(config).expect("owned upstream H2");
        let (body, _probe) = Upload::new(UPLOAD, 4096).held();
        let upload = handle.reserve_request().unwrap().submit(
            request_head(HttpMethod::Post, false, Some(UPLOAD)),
            body,
            CancellationToken::never(),
        );
        let fetch = handle.reserve_request().unwrap().submit(
            request_head(HttpMethod::Get, true, None),
            HttpSourceHolder::empty(),
            CancellationToken::never(),
        );
        let response = |submission: crate::owned_h2::OwnedH2UpstreamSubmission| -> Response {
            Box::pin(async move {
                let (_, source, _request) = submission.await?.into_source_parts();
                Ok(source)
            })
        };
        (
            Box::pin(session.run()),
            response(upload),
            response(fetch),
            Box::new(move || assert!(handle.close_when_idle().unwrap())),
        )
    } else {
        let config = UniplexConfig {
            read_timeout: Duration::ZERO,
            stream_read_timeout: Duration::ZERO,
            write_timeout,
            ..UniplexConfig::default()
        };
        let http2 = Http2SessionConfig {
            settings: vec![roxygen_codec::CodecSetting {
                identifier: SettingsId::EnablePush.as_u16(),
                value: 0,
            }],
            double_goaway_drain: false,
            goaway_timeout: Duration::ZERO,
            ..Http2SessionConfig::default()
        };
        let (session, handle) = UniplexSession::new_http2(
            context,
            TransportDirection::Upstream,
            transport,
            None,
            config,
            http2,
        )
        .expect("Uniplex upstream H2");
        let handle = Rc::new(handle);
        let ordered = Rc::new(tokio::sync::Notify::new());
        let upload: Response = {
            let (handle, ordered) = (Rc::clone(&handle), Rc::clone(&ordered));
            Box::pin(async move {
                let request = HttpFixedSource::make_fixed_request(
                    b"/owned-upstream",
                    HttpMethod::Post,
                    Some(queue(&vec![b'u'; UPLOAD])),
                )
                .expect("fixed POST");
                let sent = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationSource::new().token(),
                    )
                    .await;
                ordered.notify_one();
                Ok(sent?.source)
            })
        };
        let fetch: Response = {
            let handle = Rc::clone(&handle);
            Box::pin(async move {
                ordered.notified().await;
                let request =
                    HttpFixedSource::make_get_request(b"/owned-upstream", None).expect("fixed GET");
                Ok(handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationSource::new().token(),
                    )
                    .await?
                    .source)
            })
        };
        (
            Box::pin(session.run()),
            upload,
            fetch,
            Box::new(move || assert!(handle.initiate_drain().unwrap())),
        )
    };
    let upload = async {
        let mut source = upload.await.expect("the upload is admitted");
        source
            .read_header_event()
            .await
            .err()
            .map(|error| error.code())
    };
    let fetch = async {
        let source = fetch.await.expect("the GET is admitted");
        tokio::time::sleep_until(begun + consume_at).await;
        let mut source = source;
        let head = source.read_header_event().await.expect("B's response head");
        assert!(head.eom(), "B's response ends with its head");
    };
    let monitor = async {
        let mut resets = Vec::new();
        let mut drained = false;
        loop {
            let at = begun.elapsed();
            if at >= respond_at {
                gate.notify_one();
            }
            if !drained && !done.get() && at >= drain_at {
                drained = true;
                drain();
            }
            let ms = at.as_millis() as u64;
            let written = decode_client_frames(&wire.borrow().writes.concat()).resets;
            resets.extend(
                written[resets.len()..]
                    .iter()
                    .map(|(stream, code)| (ms, *stream, *code)),
            );
            if done.get() {
                return resets;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    let run = async {
        let result = session.await;
        done.set(true);
        result
    };
    let (_, resets, upload, ()) = tokio::time::timeout(Duration::from_secs(60), async {
        tokio::join!(run, monitor, upload, fetch)
    })
    .await
    .expect("the upstream connection ends");
    RetirementFlowWire { resets, upload }
}

/// A stream that retires while an upload waits on flow control restarts the upload's
/// flow-control timeout, as Proxygen's `checkForDetach` signals the writer and Uniplex records the
/// detach as flow activity: B's header-only response, read when it arrives, is consumed halfway
/// through the write timeout with no byte read or written, and both drivers reset the upload a
/// full write timeout after that, at the same instant.
#[test]
fn owned_upstream_retirement_restarts_the_flow_control_timeout_like_uniplex() {
    const WRITE: Duration = Duration::from_secs(1);
    let respond_at = Duration::from_millis(100);
    let consume_at = Duration::from_millis(500);
    let drain_at = 3 * WRITE;
    let (owned, uniplex) = super::owned::run_on_paused_worker(move |context| {
        Box::pin(async move {
            let owned = retirement_flow_wire(
                context.clone(),
                true,
                WRITE,
                respond_at,
                consume_at,
                drain_at,
            )
            .await;
            let uniplex =
                retirement_flow_wire(context, false, WRITE, respond_at, consume_at, drain_at).await;
            (owned, uniplex)
        })
    });
    let expected = [(
        (consume_at + WRITE).as_millis() as u64,
        StreamId::new(1),
        H2ErrorCode::FLOW_CONTROL_ERROR,
    )];
    for (driver, wire) in [("uniplex", &uniplex), ("owned", &owned)] {
        assert_eq!(
            wire.resets
                .iter()
                .map(|(_, stream, code)| (*stream, *code))
                .collect::<Vec<_>>(),
            expected
                .iter()
                .map(|(_, stream, code)| (*stream, *code))
                .collect::<Vec<_>>(),
            "{driver}: owned {owned:?}, uniplex {uniplex:?}"
        );
        assert!(
            wire.resets[0].0.abs_diff(expected[0].0) <= 1,
            "{driver}: owned {owned:?}, uniplex {uniplex:?}"
        );
        assert_eq!(
            wire.upload,
            Some(HttpErrorCode::FLOW_CONTROL_ERROR),
            "{driver}"
        );
    }
}

/// The stream read timeout of the upstream stream-read-timeout tests.
const STREAM_READ: Duration = Duration::from_secs(1);

/// An upstream session over the origin's SETTINGS followed by `reads`, with `STREAM_READ` as
/// its stream read timeout and every other timeout zero.
fn stream_read_timed_upstream(
    context: WorkerContext,
    reads: impl IntoIterator<Item = FixtureRead>,
) -> (
    OwnedH2UpstreamSession,
    OwnedH2UpstreamHandle,
    Rc<RefCell<FixtureTransportState>>,
) {
    let mut peer = Http2Codec::new(TransportDirection::Downstream);
    let settings = arming_origin_settings(&mut peer);
    let (fixture, wire) =
        FixtureTransport::new(std::iter::once(FixtureRead::Data(settings)).chain(reads));
    let mut config = options(context, Rc::new(super::owned::UntimedReads(fixture)));
    config.io.stream_read_timeout = STREAM_READ;
    let (session, handle) =
        OwnedH2UpstreamSession::new(config).expect("stream-read-timed owned upstream H2");
    (session, handle, wire)
}

fn written_resets(wire: &RefCell<FixtureTransportState>) -> Vec<(StreamId, H2ErrorCode)> {
    decode_client_frames(&wire.borrow().writes.concat()).resets
}

/// A response that stalls after the request's EOM fails its `HttpResponseReader` read with "Read
/// timeout after 1000" at the stream read timeout, not a millisecond earlier, and the stream is
/// reset with CANCEL.
#[test]
fn upstream_stall_after_eom_times_out_the_response_read() {
    super::owned::on_both_hosts(|host, context| {
        Box::pin(async move {
            let (session, handle, wire) = stream_read_timed_upstream(context, []);
            let (run, ()) = futures::join!(session.run(), async {
                let response = handle
                    .reserve_request()
                    .unwrap()
                    .submit_exclusive(
                        request_head(HttpMethod::Get, true, None),
                        HttpSourceHolder::empty(),
                        CancellationToken::never(),
                    )
                    .await
                    .expect("the request is committed");
                let (owner, mut reader) = response.source.split();
                reader.arm_header(&owner).unwrap();
                let mut read = std::pin::pin!(poll_fn(|context| reader.poll_header(context)));
                assert!(futures::poll!(read.as_mut()).is_pending());
                super::owned::settle_turns(32).await;
                host.advance(STREAM_READ - Duration::from_millis(1));
                super::owned::settle_turns(32).await;
                assert!(still_pending(&mut read).await, "the read timed out early");
                host.advance(Duration::from_millis(1));
                let Err(error) = read.await else {
                    panic!("the stalled response read must time out");
                };
                assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
                assert_eq!(error.message(), b"Read timeout after 1000");
                super::owned::settle_turns(32).await;
                assert_eq!(
                    written_resets(&wire),
                    [(StreamId::new(1), H2ErrorCode::CANCEL)]
                );
                drop((owner, reader));
                assert!(handle.close_when_idle().unwrap());
            });
            run.expect("the drained upstream connection closes cleanly");
        })
    });
}

/// What an upstream driver did with one request whose response stalls.
#[derive(Debug)]
struct UpstreamStallWire {
    /// `(milliseconds since start, stream, code)` of each RST_STREAM written.
    resets: Vec<(u64, StreamId, H2ErrorCode)>,
    /// How the response head read ended, and when.
    read: (u64, Result<u16, (HttpErrorCode, Vec<u8>)>),
    result: Result<(), HttpErrorCode>,
}

/// Sends a GET on the owned driver or on `UniplexSession`, both with `STREAM_READ` as the
/// stream read timeout and every other timeout zero, to an origin that sends only its SETTINGS
/// (or, with `response_at`, answers 200 at that instant), then closes the connection once the
/// response read has ended.
async fn upstream_stall_wire(
    context: WorkerContext,
    owned: bool,
    response_at: Option<Duration>,
) -> UpstreamStallWire {
    let mut peer = Http2Codec::new(TransportDirection::Downstream);
    let mut script = VecDeque::from([(None, arming_origin_settings(&mut peer))]);
    let gate = Arc::new(tokio::sync::Notify::new());
    if response_at.is_some() {
        script.push_back((
            Some(Arc::clone(&gate)),
            response_head(&mut peer, StreamId::new(1), 200, true, None),
        ));
    }
    let (inner, wire) = FixtureTransport::new([]);
    let transport = Rc::new(super::owned::GatedReads {
        inner,
        script: Rc::new(RefCell::new(script)),
    });
    let begun = tokio::time::Instant::now();
    let done = Rc::new(Cell::new(false));
    type Run = Pin<Box<dyn Future<Output = Result<UniplexRunSummary, HttpError>>>>;
    type Exchange = Pin<Box<dyn Future<Output = (u64, Result<u16, (HttpErrorCode, Vec<u8>)>)>>>;
    let read_head = async move |mut source: HttpSourceHolder| {
        let result = match source.read_header_event().await {
            Ok(mut head) => Ok(head.headers_mut().status_code().unwrap()),
            Err(error) => Err((error.code(), error.message().to_vec())),
        };
        (begun.elapsed().as_millis() as u64, result)
    };
    let (session, exchange, close): (Run, Exchange, Box<dyn Fn()>) = if owned {
        let mut config = options(context, transport);
        config.io.stream_read_timeout = STREAM_READ;
        let (session, handle) = OwnedH2UpstreamSession::new(config).expect("owned upstream H2");
        let submission = handle.reserve_request().unwrap().submit(
            request_head(HttpMethod::Get, true, None),
            HttpSourceHolder::empty(),
            CancellationToken::never(),
        );
        let exchange: Exchange = Box::pin(async move {
            let (_, source, _request) = submission
                .await
                .expect("the request is committed")
                .into_source_parts();
            read_head(source).await
        });
        (
            Box::pin(session.run()),
            exchange,
            Box::new(move || assert!(handle.close_when_idle().unwrap())),
        )
    } else {
        let config = UniplexConfig {
            read_timeout: Duration::ZERO,
            stream_read_timeout: STREAM_READ,
            write_timeout: Duration::ZERO,
            ..UniplexConfig::default()
        };
        let http2 = Http2SessionConfig {
            settings: vec![roxygen_codec::CodecSetting {
                identifier: SettingsId::EnablePush.as_u16(),
                value: 0,
            }],
            double_goaway_drain: false,
            goaway_timeout: Duration::ZERO,
            ..Http2SessionConfig::default()
        };
        let (session, handle) = UniplexSession::new_http2(
            context,
            TransportDirection::Upstream,
            transport,
            None,
            config,
            http2,
        )
        .expect("Uniplex upstream H2");
        let sender = handle.clone();
        let exchange: Exchange = Box::pin(async move {
            let request =
                HttpFixedSource::make_get_request(b"/owned-upstream", None).expect("fixed GET");
            let source = sender
                .send_request(
                    HttpSourceHolder::new(Box::new(request)),
                    CancellationSource::new().token(),
                )
                .await
                .expect("request admitted")
                .source;
            read_head(source).await
        });
        (
            Box::pin(session.run()),
            exchange,
            Box::new(move || assert!(handle.close_when_idle().unwrap())),
        )
    };
    let exchange = async {
        let read = exchange.await;
        close();
        read
    };
    let monitor = async {
        let mut resets = Vec::new();
        loop {
            let at = begun.elapsed();
            if response_at.is_some_and(|response_at| at >= response_at) {
                gate.notify_one();
            }
            let ms = at.as_millis() as u64;
            let written = written_resets(&wire);
            resets.extend(
                written[resets.len()..]
                    .iter()
                    .map(|(stream, code)| (ms, *stream, *code)),
            );
            if done.get() {
                return resets;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    let run = async {
        let result = session.await;
        done.set(true);
        result
    };
    let (result, resets, read) = tokio::time::timeout(Duration::from_secs(60), async {
        tokio::join!(run, monitor, exchange)
    })
    .await
    .expect("the upstream connection ends");
    UpstreamStallWire {
        resets,
        read,
        result: result.map(|_| ()).map_err(|error| error.code()),
    }
}

/// An upstream response that stalls after the request's EOM, owned against `UniplexSession` on
/// the same paused clock: both reads fail with "Read timeout after 1000" at the stream read
/// timeout, both write RST_STREAM(CANCEL) then, and both connections then close cleanly; a
/// response that arrives within the timeout is delivered by both.
#[test]
fn owned_upstream_stream_read_timeout_matches_uniplex() {
    for response_at in [None, Some(STREAM_READ / 2)] {
        let (owned, uniplex) = super::owned::run_on_paused_worker(move |context| {
            Box::pin(async move {
                let uniplex = upstream_stall_wire(context.clone(), false, response_at).await;
                let owned = upstream_stall_wire(context, true, response_at).await;
                (owned, uniplex)
            })
        });
        let expected_read = match response_at {
            None => (
                STREAM_READ.as_millis() as u64,
                Err((
                    HttpErrorCode::READ_TIMEOUT,
                    b"Read timeout after 1000".to_vec(),
                )),
            ),
            Some(at) => (at.as_millis() as u64, Ok(200)),
        };
        for wire in [&owned, &uniplex] {
            assert_eq!(
                wire.read.1, expected_read.1,
                "owned {owned:?}, uniplex {uniplex:?}"
            );
            assert!(
                wire.read.0.abs_diff(expected_read.0) <= 1,
                "owned {owned:?}, uniplex {uniplex:?}"
            );
        }
        let resets = |wire: &UpstreamStallWire| -> Vec<(StreamId, H2ErrorCode)> {
            wire.resets
                .iter()
                .map(|(_, stream, code)| (*stream, *code))
                .collect()
        };
        assert_eq!(
            resets(&owned),
            resets(&uniplex),
            "owned {owned:?}, uniplex {uniplex:?}"
        );
        for ((owned_at, ..), (uniplex_at, ..)) in owned.resets.iter().zip(&uniplex.resets) {
            assert!(
                owned_at.abs_diff(*uniplex_at) <= 1,
                "owned {owned:?}, uniplex {uniplex:?}"
            );
        }
        assert_eq!(owned.result, Ok(()), "owned {owned:?}");
        assert_eq!(owned.result, uniplex.result);
    }
}

/// Whether `read` is still pending after two polls: a timed wait commits its timeout in one poll
/// and returns it from the next.
async fn still_pending<F: Future + Unpin>(read: &mut F) -> bool {
    futures::poll!(&mut *read).is_pending() && futures::poll!(&mut *read).is_pending()
}

/// Submits a POST whose 128-byte body waits for `upload_gate`, takes its response source once
/// the head is committed, and polls the response head read once.
async fn gated_post_read(
    handle: &OwnedH2UpstreamHandle,
    upload_gate: &Arc<tokio::sync::Notify>,
) -> (
    HttpSourceHolder,
    LocalSourceFuture<Result<HttpHeaderEvent, HttpError>>,
    crate::owned_h2::OwnedH2UpstreamRequest,
) {
    let mut upload = Upload::new(128, 128);
    upload.gate = Some((0, Arc::clone(upload_gate)));
    let submission = handle.reserve_request().unwrap().submit(
        request_head(HttpMethod::Post, false, Some(128)),
        upload.held().0,
        CancellationToken::never(),
    );
    let (_, mut source, request) = submission
        .await
        .expect("the request head is committed")
        .into_source_parts();
    let mut read = source.read_header_event();
    assert!(futures::poll!(&mut read).is_pending());
    (source, read, request)
}

/// The response read is not timed while the request uploads, however long that takes: the
/// timeout starts when the request's EOM is serialized, and the read fails a full stream read
/// timeout after it.
#[test]
fn upstream_response_read_is_untimed_during_the_upload() {
    super::owned::on_both_hosts(|host, context| {
        Box::pin(async move {
            let (session, handle, wire) = stream_read_timed_upstream(context, []);
            let upload_gate = Arc::new(tokio::sync::Notify::new());
            let (run, ()) = futures::join!(session.run(), async {
                let (source, mut read, _request) = gated_post_read(&handle, &upload_gate).await;
                super::owned::settle_turns(32).await;
                host.advance(3 * STREAM_READ);
                super::owned::settle_turns(32).await;
                assert!(
                    still_pending(&mut read).await,
                    "the read was timed during the upload"
                );
                upload_gate.notify_one();
                super::owned::settle_turns(64).await;
                assert_eq!(
                    decode_client_frames(&wire.borrow().writes.concat()).body_bytes,
                    128,
                    "the upload ended"
                );
                host.advance(STREAM_READ - Duration::from_millis(1));
                super::owned::settle_turns(32).await;
                assert!(
                    still_pending(&mut read).await,
                    "the read timed out before a full timeout after the request's EOM"
                );
                host.advance(Duration::from_millis(1));
                let Err(error) = read.await else {
                    panic!("the stalled response read must time out");
                };
                assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
                assert_eq!(error.message(), b"Read timeout after 1000");
                drop(source);
                super::owned::settle_turns(32).await;
                assert!(handle.close_when_idle().unwrap());
            });
            run.expect("the drained upstream connection closes cleanly");
        })
    });
}

/// A read timeout the caller sets on the response before the request's EOM replaces the
/// session's and, like it, starts at the EOM (Uniplex's `DeferUpdates`).
#[test]
fn caller_read_timeout_before_the_eom_wins() {
    super::owned::on_both_hosts(|host, context| {
        Box::pin(async move {
            let (session, handle, _wire) = stream_read_timed_upstream(context, []);
            let upload_gate = Arc::new(tokio::sync::Notify::new());
            let (run, ()) = futures::join!(session.run(), async {
                let (mut source, mut read, _request) = gated_post_read(&handle, &upload_gate).await;
                host.advance(2 * STREAM_READ);
                source.set_read_timeout(3 * STREAM_READ);
                super::owned::settle_turns(32).await;
                upload_gate.notify_one();
                super::owned::settle_turns(64).await;
                host.advance(3 * STREAM_READ - Duration::from_millis(1));
                super::owned::settle_turns(32).await;
                assert!(
                    still_pending(&mut read).await,
                    "the caller's timeout did not replace the session's, or started early"
                );
                host.advance(Duration::from_millis(1));
                let Err(error) = read.await else {
                    panic!("the stalled response read must time out");
                };
                assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
                assert_eq!(error.message(), b"Read timeout after 3000");
                drop(source);
                super::owned::settle_turns(32).await;
                assert!(handle.close_when_idle().unwrap());
            });
            run.expect("the drained upstream connection closes cleanly");
        })
    });
}

/// A response that the driver hands to the source at the read's deadline, before the waiting
/// reader is polled again, beats the timeout: the reader gets the response.
#[test]
fn response_published_before_the_reader_polls_beats_the_timeout() {
    super::owned::on_both_hosts(|host, context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            let _ = arming_origin_settings(&mut peer);
            let gate = Arc::new(tokio::sync::Notify::new());
            let response = response_head(&mut peer, StreamId::new(1), 200, true, None);
            let (session, handle, wire) = stream_read_timed_upstream(
                context,
                [FixtureRead::WaitData(Arc::clone(&gate), response)],
            );
            let (run, ()) = futures::join!(session.run(), async {
                let submission = handle.reserve_request().unwrap().submit(
                    request_head(HttpMethod::Get, true, None),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                let (_, source, _request) = submission
                    .await
                    .expect("the request is committed")
                    .into_source_parts();
                let mut source = source;
                let mut read = source.read_header_event();
                assert!(futures::poll!(&mut read).is_pending());
                super::owned::settle_turns(32).await;
                host.advance(STREAM_READ);
                gate.notify_one();
                super::owned::settle_turns(64).await;
                let Poll::Ready(Ok(mut head)) = futures::poll!(&mut read) else {
                    panic!("the published response must beat the timeout");
                };
                assert_eq!(head.headers_mut().status_code().unwrap(), 200);
                assert!(written_resets(&wire).is_empty());
                drop(source);
                super::owned::settle_turns(32).await;
                assert!(handle.close_when_idle().unwrap());
            });
            run.expect("the drained upstream connection closes cleanly");
        })
    });
}

/// Each wait for response data is timed afresh from the request's EOM on: a response whose head
/// and body chunks each arrive within the stream read timeout is delivered whole, however long
/// it takes in all. On a wheel host each armed wait is a store on the stream baton's one slot,
/// with no host sleep; without a wheel each armed wait takes one host sleep, as before. The stream
/// read timeout takes no connection slot.
#[test]
fn trickling_response_is_timed_per_wait() {
    super::owned::on_both_hosts(|host, context| {
        Box::pin(async move {
            let wheel = context.host().wheel_timer().is_some();
            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            let _ = arming_origin_settings(&mut peer);
            let stream = StreamId::new(1);
            let frames: Vec<_> =
                std::iter::once(response_head(&mut peer, stream, 200, false, Some(3)))
                    .chain([(b"a", false), (b"b", false), (b"c", true)].map(
                        |(chunk, end_stream)| {
                            eligibility_frame(
                                &mut peer,
                                EgressFrame::Body {
                                    stream,
                                    body: queue(chunk),
                                    padding: None,
                                    end_stream,
                                },
                            )
                        },
                    ))
                    .collect();
            let gates: Vec<_> = frames
                .iter()
                .map(|_| Arc::new(tokio::sync::Notify::new()))
                .collect();
            let (session, handle, wire) = stream_read_timed_upstream(
                context,
                gates
                    .iter()
                    .zip(frames)
                    .map(|(gate, frame)| FixtureRead::WaitData(Arc::clone(gate), frame)),
            );
            let (run, ()) = futures::join!(session.run(), async {
                let submission = handle.reserve_request().unwrap().submit(
                    request_head(HttpMethod::Get, true, None),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                let (_, mut source, _request) = submission
                    .await
                    .expect("the request is committed")
                    .into_source_parts();
                let fetched = {
                    let mut fetch = std::pin::pin!(read_fetched(&mut source));
                    for gate in &gates {
                        super::owned::settle_turns(32).await;
                        assert!(still_pending(&mut fetch).await, "the response ended early");
                        host.advance(STREAM_READ * 3 / 4);
                        super::owned::settle_turns(32).await;
                        assert!(
                            still_pending(&mut fetch).await,
                            "a read whose data came within the timeout timed out"
                        );
                        gate.notify_one();
                    }
                    fetch.await
                };
                assert_eq!(fetched, Ok((200, b"abc".to_vec())));
                assert!(written_resets(&wire).is_empty());
                drop(source);
                super::owned::settle_turns(32).await;
                assert!(handle.close_when_idle().unwrap());
            });
            run.expect("the drained upstream connection closes cleanly");
            if wheel {
                let stats = host.wheel_stats();
                assert_eq!(
                    (
                        host.sleeps(),
                        stats.scheduled,
                        stats.relinked,
                        stats.live_slots
                    ),
                    (0, gates.len() as u64, 1, 0),
                    "(host sleeps, schedules, links, slots left): each armed wait for response data is a \
                     store on one slot"
                );
            } else {
                assert_eq!(
                    host.sleeps(),
                    gates.len(),
                    "one host sleep per armed wait for response data"
                );
            }
        })
    });
}

/// The clock reads of a GET whose response arrives after 20 s, then a close, with every timeout
/// zero, measured on the parent commit, which did not honor stream read timeouts.
const ZERO_CONFIG_LATE_RESPONSE_NOW_READS: usize = 1;

/// With a zero stream read timeout an upstream response read arms nothing at the request's EOM:
/// no slot, no host sleep, and no clock read beyond the parent's, however long the origin takes.
#[test]
fn zero_stream_read_timeout_upstream_takes_no_slot_sleep_or_clock_read() {
    super::owned::on_both_hosts(|host, context| {
        Box::pin(async move {
            let wheel = context.host().wheel_timer().is_some();
            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            let settings = arming_origin_settings(&mut peer);
            let gate = Arc::new(tokio::sync::Notify::new());
            let response = response_head(&mut peer, StreamId::new(1), 200, true, None);
            let (fixture, wire) = FixtureTransport::new([
                FixtureRead::Data(settings),
                FixtureRead::WaitData(Arc::clone(&gate), response),
            ]);
            let config = options(context, Rc::new(super::owned::UntimedReads(fixture)));
            let reads_before = host.now_reads();
            let (session, handle) = OwnedH2UpstreamSession::new(config).expect("owned upstream H2");
            let (run, ()) = futures::join!(session.run(), async {
                let submission = handle.reserve_request().unwrap().submit(
                    request_head(HttpMethod::Get, true, None),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                );
                let (_, mut source, _request) = submission
                    .await
                    .expect("the request is committed")
                    .into_source_parts();
                let mut read = source.read_header_event();
                assert!(still_pending(&mut read).await);
                super::owned::settle_turns(32).await;
                host.advance(20 * STREAM_READ);
                super::owned::settle_turns(32).await;
                assert!(still_pending(&mut read).await);
                gate.notify_one();
                let Ok(mut head) = read.await else {
                    panic!("the late response arrives");
                };
                assert_eq!(head.headers_mut().status_code().unwrap(), 200);
                drop(source);
                super::owned::settle_turns(32).await;
                assert!(handle.close_when_idle().unwrap());
            });
            run.expect("the closed upstream connection ends cleanly");
            assert_eq!(
                host.now_reads() - reads_before,
                ZERO_CONFIG_LATE_RESPONSE_NOW_READS
            );
            assert_eq!(host.sleeps(), 0, "a zero-config session made a host sleep");
            if wheel {
                let stats = host.wheel_stats();
                assert_eq!(stats.scheduled, 0);
                assert_eq!(stats.live_slots, 0);
            }
            assert!(written_resets(&wire).is_empty());
        })
    });
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
    Plain(OwnedH2UpstreamSubmission),
    Exclusive(OwnedH2ExclusiveSubmission),
}

impl Submitted {
    fn new(kind: SubmitKind, reservation: OwnedH2UpstreamReservation) -> Self {
        let (head, body, token) = (
            request_head(HttpMethod::Get, true, None),
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
/// around the origin's FIN; the streams whose HEADERS were written; the run.
#[derive(Debug, Eq, PartialEq)]
struct StaleOutcome {
    first: RequestProgress,
    second: RequestProgress,
    header_streams: Vec<StreamId>,
    run: Option<HttpErrorCode>,
}

/// A GET answered with a 200 that ends stream 1, then a second GET submitted as `kind` around
/// the origin's FIN, as `fin` orders them.
fn owned_stale_outcome(fin: StaleFin, kind: SubmitKind) -> StaleOutcome {
    run_on_worker(move |context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            let first_ready = Arc::new(tokio::sync::Notify::new());
            let fin_ready = Arc::new(tokio::sync::Notify::new());
            let (transport, wire) = FixtureTransport::new([
                FixtureRead::Data(server_settings_bytes(100)),
                FixtureRead::WaitData(
                    Arc::clone(&first_ready),
                    response_head(&mut peer, StreamId::new(1), 200, true, Some(0)),
                ),
                FixtureRead::WaitDataEof(Arc::clone(&fin_ready), Vec::new()),
            ]);
            let (session, handle) =
                OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
            let probe = Rc::new(AdmissionProbe::default());
            handle.set_observer(probe.clone()).unwrap();
            let (run, (first, second)) = tokio::join!(session.run(), async {
                complete_credit_wait(|| handle.available_capacity() == 100).await;
                let (_stream, mut source, control) = handle
                    .reserve_request()
                    .unwrap()
                    .submit(
                        request_head(HttpMethod::Get, true, None),
                        HttpSourceHolder::empty(),
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap()
                    .into_source_parts();
                first_ready.notify_one();
                let mut head = source.read_header_event().await.unwrap();
                assert_eq!(head.headers_mut().status_code().unwrap(), 200);
                assert!(head.eom());
                drop(source);
                complete_credit_wait(|| handle.is_idle()).await;
                assert_eq!(handle.available_capacity(), 100, "the connection is reused");
                let first = RequestProgress::of(None, &control);
                let reservation = handle.reserve_request().unwrap();
                match fin {
                    StaleFin::BeforeSubmission => {
                        fin_ready.notify_one();
                        complete_credit_wait(|| probe.closed.get() == 1).await;
                    }
                    StaleFin::ReadableThenSubmitted => fin_ready.notify_one(),
                    StaleFin::SubmittedThenReadable => {}
                }
                let mut submitted = Submitted::new(kind, reservation);
                if matches!(fin, StaleFin::SubmittedThenReadable) {
                    fin_ready.notify_one();
                }
                let second = submitted.progress().await;
                complete_credit_wait(|| handle.is_finished()).await;
                (first, second)
            });
            let header_streams =
                decode_client_frames(&wire.borrow().writes.concat()).header_streams;
            StaleOutcome {
                first,
                second,
                header_streams,
                run: run.err().map(|error| error.code()),
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
                    header_streams: vec![StreamId::new(1)],
                    run: None,
                },
                "{fin:?}, {kind:?}"
            );
        }
    }
}

#[test]
fn peer_refusals_before_a_response_head_report_the_request_unprocessed() {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            // Stream 1 gets a response head, then REFUSED_STREAM; stream 3 REFUSED_STREAM;
            // stream 5 CANCEL; a GOAWAY whose last-stream-id is 5 refuses stream 7.
            let mut refusals = response_head(&mut peer, StreamId::new(1), 200, false, None);
            for (stream, code) in [
                (1, H2ErrorCode::REFUSED_STREAM),
                (3, H2ErrorCode::REFUSED_STREAM),
                (5, H2ErrorCode::CANCEL),
            ] {
                refusals.extend(eligibility_frame(
                    &mut peer,
                    EgressFrame::Reset {
                        stream: StreamId::new(stream),
                        code,
                    },
                ));
            }
            refusals.extend(eligibility_frame(
                &mut peer,
                EgressFrame::Goaway {
                    last_stream: StreamId::new(5),
                    code: H2ErrorCode::NO_ERROR,
                    debug_data: None,
                },
            ));
            let refuse = Arc::new(tokio::sync::Notify::new());
            let (transport, wire) = FixtureTransport::new([
                FixtureRead::Data(server_settings_bytes(100)),
                FixtureRead::WaitData(Arc::clone(&refuse), refusals),
            ]);
            let (session, handle) =
                OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
            let probe = Rc::new(AdmissionProbe::default());
            handle.set_observer(probe.clone()).unwrap();
            let (run, (committed, refused)) = tokio::join!(session.run(), async {
                complete_credit_wait(|| handle.available_capacity() == 100).await;
                let mut responses = Vec::new();
                for _ in 0..4 {
                    responses.push(
                        handle
                            .reserve_request()
                            .unwrap()
                            .submit(
                                request_head(HttpMethod::Get, true, None),
                                HttpSourceHolder::empty(),
                                CancellationToken::never(),
                            )
                            .await
                            .unwrap()
                            .into_source_parts(),
                    );
                }
                let reserved = handle.reserve_request().unwrap();
                complete_credit_wait(|| {
                    decode_client_frames(&wire.borrow().writes.concat()).header_streams
                        == [1, 3, 5, 7].map(StreamId::new)
                })
                .await;
                refuse.notify_one();
                complete_credit_wait(|| probe.closed.get() == 1).await;
                let committed: Vec<_> = responses
                    .iter()
                    .map(|(_stream, _source, control)| RequestProgress::of(None, control))
                    .collect();
                // Reserved before the GOAWAY and submitted after it: refused before commit.
                let refused = Submitted::new(SubmitKind::Plain, reserved).progress().await;
                drop(responses);
                complete_credit_wait(|| handle.is_finished()).await;
                (committed, refused)
            });
            let progress = |unprocessed, sequence| RequestProgress {
                error: None,
                unprocessed,
                sequence: Some(sequence),
            };
            assert_eq!(
                committed,
                [
                    // A response head arrived before the refusal.
                    progress(false, 0),
                    progress(true, 1),
                    // CANCEL is no refusal.
                    progress(false, 2),
                    // Above the GOAWAY's last-stream-id.
                    progress(true, 3),
                ]
            );
            assert_eq!(
                refused,
                RequestProgress {
                    error: Some(HttpErrorCode::REFUSED_STREAM),
                    unprocessed: true,
                    sequence: Some(4),
                }
            );
            run.unwrap();
        })
    });
}

/// A peer refusal that reaches a stream whose earlier peer reset the driver already applied, in an
/// earlier turn, is not the stream's first reset: the request keeps that reset's error and is not
/// unprocessed (`DIV-035`). The held response sources retain the reset streams.
#[test]
fn a_peer_refusal_after_an_applied_peer_reset_leaves_the_request_processed() {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            let mut cancels = Vec::new();
            for stream in [1, 3] {
                cancels.extend(eligibility_frame(
                    &mut peer,
                    EgressFrame::Reset {
                        stream: StreamId::new(stream),
                        code: H2ErrorCode::CANCEL,
                    },
                ));
            }
            // Stream 1 REFUSED_STREAM; a GOAWAY whose last-stream-id is 1 refuses stream 3.
            let mut refusals = eligibility_frame(
                &mut peer,
                EgressFrame::Reset {
                    stream: StreamId::new(1),
                    code: H2ErrorCode::REFUSED_STREAM,
                },
            );
            refusals.extend(eligibility_frame(
                &mut peer,
                EgressFrame::Goaway {
                    last_stream: StreamId::new(1),
                    code: H2ErrorCode::NO_ERROR,
                    debug_data: None,
                },
            ));
            let cancel = Arc::new(tokio::sync::Notify::new());
            let refuse = Arc::new(tokio::sync::Notify::new());
            let (transport, wire) = FixtureTransport::new([
                FixtureRead::Data(server_settings_bytes(100)),
                FixtureRead::WaitData(Arc::clone(&cancel), cancels),
                FixtureRead::WaitData(Arc::clone(&refuse), refusals),
            ]);
            let (session, handle) =
                OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
            let probe = Rc::new(AdmissionProbe::default());
            handle.set_observer(probe.clone()).unwrap();
            let (run, outcomes) = tokio::join!(session.run(), async {
                complete_credit_wait(|| handle.available_capacity() == 100).await;
                let mut responses = Vec::new();
                for _ in 0..2 {
                    responses.push(
                        handle
                            .reserve_request()
                            .unwrap()
                            .submit(
                                request_head(HttpMethod::Get, true, None),
                                HttpSourceHolder::empty(),
                                CancellationToken::never(),
                            )
                            .await
                            .unwrap()
                            .into_source_parts(),
                    );
                }
                complete_credit_wait(|| header_streams(&wire) == [1, 3].map(StreamId::new)).await;
                cancel.notify_one();
                // The driver applies both CANCELs, which complete the responses' ingress.
                for (_stream, source, _control) in &responses {
                    let receipt = source.ingress_completion().unwrap();
                    assert_eq!(
                        receipt.wait().await.err().map(|error| error.code()),
                        Some(HttpErrorCode::CANCEL)
                    );
                }
                refuse.notify_one();
                complete_credit_wait(|| probe.closed.get() == 1).await;
                let mut outcomes = Vec::new();
                for (_stream, mut source, control) in responses {
                    let error = source
                        .read_header_event()
                        .await
                        .err()
                        .map(|error| error.code());
                    outcomes.push(RequestProgress::of(error, &control));
                }
                complete_credit_wait(|| handle.is_finished()).await;
                outcomes
            });
            let cancelled = |sequence| RequestProgress {
                error: Some(HttpErrorCode::CANCEL),
                unprocessed: false,
                sequence: Some(sequence),
            };
            assert_eq!(outcomes, [cancelled(0), cancelled(1)]);
            run.unwrap();
        })
    });
}

/// The progress of a GET whose stream the peer refuses, with `REFUSED_STREAM` or, with `goaway`,
/// a GOAWAY whose last-stream-id is below it, in the driver turn that would apply the stream read
/// timeout that has just failed its response head read.
async fn refused_after_a_stream_read_timeout(
    host: Rc<crate::external_host_tests::ManualHost>,
    context: WorkerContext,
    goaway: bool,
) -> RequestProgress {
    let mut peer = Http2Codec::new(TransportDirection::Downstream);
    let refusal = if goaway {
        EgressFrame::Goaway {
            last_stream: StreamId::UNSPECIFIED,
            code: H2ErrorCode::NO_ERROR,
            debug_data: None,
        }
    } else {
        EgressFrame::Reset {
            stream: StreamId::new(1),
            code: H2ErrorCode::REFUSED_STREAM,
        }
    };
    let refuse = Arc::new(tokio::sync::Notify::new());
    let (session, handle, wire) = stream_read_timed_upstream(
        context,
        [FixtureRead::WaitData(
            Arc::clone(&refuse),
            eligibility_frame(&mut peer, refusal),
        )],
    );
    let (run, progress) = futures::join!(session.run(), async {
        let (_stream, mut source, control) = handle
            .reserve_request()
            .unwrap()
            .submit(
                request_head(HttpMethod::Get, true, None),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            )
            .await
            .expect("the request is committed")
            .into_source_parts();
        let mut read = source.read_header_event();
        assert!(futures::poll!(read.as_mut()).is_pending());
        super::owned::settle_turns(32).await;
        assert_eq!(header_streams(&wire), [StreamId::new(1)]);
        host.advance(STREAM_READ);
        // A timed wait commits its timeout in one poll and returns it from the next.
        let _ = futures::poll!(read.as_mut());
        let Poll::Ready(Err(error)) = futures::poll!(read.as_mut()) else {
            panic!("the response head read times out");
        };
        drop(read);
        // Readable before the driver runs again, so its next turn reads the refusal before it
        // applies the timeout.
        refuse.notify_one();
        super::owned::settle_turns(32).await;
        assert!(
            written_resets(&wire).is_empty(),
            "the refusal reset the stream before the driver applied the timeout"
        );
        let progress = RequestProgress::of(Some(error.code()), &control);
        drop((source, control));
        let _ = handle.close_when_idle();
        progress
    });
    run.expect("the upstream connection closes cleanly");
    progress
}

/// A peer refusal that the driver reads in the turn it would apply a stream read timeout that
/// already failed the response, before any response head, leaves the request processed: the
/// timeout is the request's failure (`DIV-035`).
#[test]
fn a_peer_refusal_after_a_stream_read_timeout_failed_the_response_leaves_the_request_processed() {
    const TIMED_OUT: RequestProgress = RequestProgress {
        error: Some(HttpErrorCode::READ_TIMEOUT),
        unprocessed: false,
        sequence: Some(0),
    };
    super::owned::on_both_hosts(|host, context| {
        Box::pin(async move {
            assert_eq!(
                refused_after_a_stream_read_timeout(host, context, false).await,
                TIMED_OUT,
                "REFUSED_STREAM"
            );
        })
    });
    super::owned::on_both_hosts(|host, context| {
        Box::pin(async move {
            assert_eq!(
                refused_after_a_stream_read_timeout(host, context, true).await,
                TIMED_OUT,
                "GOAWAY"
            );
        })
    });
}

/// More streams than the driver polls stream owners in one turn budget.
const BEYOND_A_TURN_BUDGET: usize = 100;

/// Counts the response completions the driver delivers, which it does as it polls a stream's
/// owner.
struct Completions(Rc<Cell<usize>>);

impl crate::owned_h2::OwnedH2ResponseObserver for Completions {
    fn poll_deadline(&mut self, _: &mut std::task::Context<'_>) -> Poll<HttpError> {
        Poll::Pending
    }

    fn response_complete(self: Box<Self>, _: Result<std::time::Instant, HttpError>) {
        self.0.set(self.0.get() + 1);
    }
}

/// The progress of `BEYOND_A_TURN_BUDGET` GETs whose streams the peer refuses in one read, each
/// with `REFUSED_STREAM` or, with `goaway`, all with one GOAWAY, and whose response head reads
/// time out once the driver has yielded at its turn budget with refusals still to apply; and how
/// many of the refusals the driver applied before the timeout.
async fn timed_out_before_the_driver_applies_its_refusals(
    host: Rc<crate::external_host_tests::ManualHost>,
    context: WorkerContext,
    goaway: bool,
) -> (usize, Vec<RequestProgress>) {
    let mut peer = Http2Codec::new(TransportDirection::Downstream);
    let refusals = if goaway {
        eligibility_frame(
            &mut peer,
            EgressFrame::Goaway {
                last_stream: StreamId::UNSPECIFIED,
                code: H2ErrorCode::NO_ERROR,
                debug_data: None,
            },
        )
    } else {
        (0..BEYOND_A_TURN_BUDGET as u64)
            .flat_map(|index| {
                eligibility_frame(
                    &mut peer,
                    EgressFrame::Reset {
                        stream: StreamId::new(2 * index + 1),
                        code: H2ErrorCode::REFUSED_STREAM,
                    },
                )
            })
            .collect()
    };
    let refuse = Arc::new(tokio::sync::Notify::new());
    let (session, handle, wire) = stream_read_timed_upstream(
        context,
        [FixtureRead::WaitData(Arc::clone(&refuse), refusals)],
    );
    let (run, (applied, outcomes)) = futures::join!(session.run(), async {
        let completed = Rc::new(Cell::new(0));
        let mut requests = Vec::new();
        for _ in 0..BEYOND_A_TURN_BUDGET {
            let (_stream, source, control) = handle
                .reserve_request()
                .unwrap()
                .submit(
                    request_head(HttpMethod::Get, true, None),
                    HttpSourceHolder::empty(),
                    CancellationToken::never(),
                )
                .await
                .expect("the request is committed")
                .into_source_parts();
            control
                .observe_response(Box::new(Completions(Rc::clone(&completed))))
                .unwrap();
            requests.push((source, control));
        }
        complete_credit_wait(|| header_streams(&wire).len() == BEYOND_A_TURN_BUDGET).await;
        let mut reads: Vec<_> = requests
            .iter_mut()
            .map(|(source, _control)| source.read_header_event())
            .collect();
        for read in &mut reads {
            assert!(futures::poll!(read.as_mut()).is_pending());
        }
        super::owned::settle_turns(32).await;
        assert_eq!(completed.get(), 0);
        let yields = host.yields();
        refuse.notify_one();
        // The driver reads the refusals, polls one stream owner per turn and yields at its turn
        // budget before it has polled every refused stream's owner.
        assert!(
            super::owned::drive_until(64, || host.yields() > yields).await,
            "the driver yields at its turn budget"
        );
        let applied = completed.get();
        assert!(
            applied < BEYOND_A_TURN_BUDGET,
            "the driver applied every refusal before it yielded"
        );
        host.advance(STREAM_READ);
        let mut errors = Vec::new();
        for read in &mut reads {
            // A timed wait commits its timeout in one poll and returns it from the next.
            let result = match futures::poll!(read.as_mut()) {
                Poll::Ready(result) => result,
                Poll::Pending => {
                    let Poll::Ready(result) = futures::poll!(read.as_mut()) else {
                        panic!("the response head read neither failed nor timed out");
                    };
                    result
                }
            };
            errors.push(result.err().map(|error| error.code()));
        }
        drop(reads);
        super::owned::settle_turns(32).await;
        assert!(written_resets(&wire).is_empty(), "no stream was reset");
        let outcomes = errors
            .into_iter()
            .zip(&requests)
            .map(|(error, (_source, control))| RequestProgress::of(error, control))
            .collect();
        drop(requests);
        let _ = handle.close_when_idle();
        (applied, outcomes)
    });
    run.expect("the upstream connection closes cleanly");
    (applied, outcomes)
}

/// Refusals of more streams than the driver polls stream owners in one turn budget, read at once
/// (`REFUSED_STREAM` for every stream, or one GOAWAY), leave refusals recorded but not yet applied
/// when the driver yields. A stream read timeout that expires then does not fail a request that its
/// refusal failed unprocessed: every response head read fails with `REFUSED_STREAM`, and every
/// request reports unprocessed (`DIV-035`).
#[test]
fn a_stream_read_timeout_before_the_driver_applies_a_peer_refusal_keeps_the_refusal() {
    fn assert_refused((applied, outcomes): (usize, Vec<RequestProgress>), refusal: &str) {
        let mismatched: Vec<_> = outcomes
            .iter()
            .enumerate()
            .filter(|(sequence, progress)| {
                **progress
                    != RequestProgress {
                        error: Some(HttpErrorCode::REFUSED_STREAM),
                        unprocessed: true,
                        sequence: Some(*sequence as u64),
                    }
            })
            .collect();
        assert!(
            mismatched.is_empty(),
            "{refusal}: {} of {BEYOND_A_TURN_BUDGET} requests, {applied} refusals applied before \
             the timeout: {mismatched:?}",
            mismatched.len()
        );
    }
    super::owned::on_both_hosts(|host, context| {
        Box::pin(async move {
            assert_refused(
                timed_out_before_the_driver_applies_its_refusals(host, context, false).await,
                "REFUSED_STREAM",
            );
        })
    });
    super::owned::on_both_hosts(|host, context| {
        Box::pin(async move {
            assert_refused(
                timed_out_before_the_driver_applies_its_refusals(host, context, true).await,
                "GOAWAY",
            );
        })
    });
}

/// A cancellation that lands right after a connection refusal samples its request's cause, as
/// another thread's can, follows the refusal: the request reports the refusal's error and
/// unprocessed, not the cancellation's error with the refusal's flag (`DIV-035`).
#[test]
fn a_cancellation_racing_a_refusal_follows_it() {
    run_on_worker(|context| {
        Box::pin(async move {
            let fin = Arc::new(tokio::sync::Notify::new());
            let (transport, wire) = FixtureTransport::new([
                FixtureRead::Data(server_settings_bytes(100)),
                FixtureRead::WaitDataEof(Arc::clone(&fin), Vec::new()),
            ]);
            let (session, handle) =
                OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
            let probe = Rc::new(AdmissionProbe::default());
            handle.set_observer(probe.clone()).unwrap();
            let (run, progress) = tokio::join!(session.run(), async {
                complete_credit_wait(|| handle.available_capacity() == 100).await;
                let reservation = handle.reserve_request().unwrap();
                fin.notify_one();
                complete_credit_wait(|| probe.closed.get() == 1).await;
                let cancellation = CancellationSource::new();
                let token = cancellation.token();
                crate::experimental::after_next_refusal_cause_sample(move || {
                    cancellation.request_cancellation();
                });
                let mut submission = reservation.submit(
                    request_head(HttpMethod::Get, true, None),
                    HttpSourceHolder::empty(),
                    token,
                );
                let error = (&mut submission).await.err().map(|error| error.code());
                let progress = RequestProgress::of(error, &submission);
                complete_credit_wait(|| handle.is_finished()).await;
                progress
            });
            assert_eq!(
                progress,
                RequestProgress {
                    error: Some(HttpErrorCode::TRANSPORT_EOF),
                    unprocessed: true,
                    sequence: Some(0),
                }
            );
            assert!(header_streams(&wire).is_empty(), "no request is written");
            run.unwrap();
        })
    });
}

impl Submitted {
    fn cancel(&self, error: HttpError) {
        match self {
            Self::Plain(submission) => submission.cancel(error),
            Self::Exclusive(submission) => submission.cancel(error),
        }
    }
}

/// How the peer refuses a committed request's stream 1.
#[derive(Clone, Copy, Debug)]
enum PeerRefusal {
    /// RST_STREAM(`REFUSED_STREAM`), then a GOAWAY whose last-stream-id is 1, which refuses
    /// nothing more and lets the connection finish.
    RefusedStream,
    /// A GOAWAY whose last-stream-id is 0.
    Goaway,
}

/// Whether and when the consumer cancels a refused request whose response it has not claimed.
#[derive(Clone, Copy, Debug)]
enum RefusalCancel {
    None,
    /// After the driver applied the refusal, the submission's `cancel` with a deadline's error.
    Explicit,
    /// After the driver applied the refusal, the request's cancellation token.
    Token,
    /// The token, from a test hook right after the refusal samples the request's cause, where
    /// another thread's cancellation can land.
    RacingTheSample,
}

/// A POST on stream 1, submitted as `kind`, whose upload holds its body back, refused by the
/// peer as `refusal`, cancelled as `cancel`, then claimed: the upload's stops and the request's
/// claim error and progress.
fn unclaimed_peer_refusal(
    refusal: PeerRefusal,
    kind: SubmitKind,
    cancel: RefusalCancel,
) -> (Vec<Option<HttpErrorCode>>, RequestProgress) {
    run_on_worker(move |context| {
        Box::pin(async move {
            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            let goaway = |last_stream| EgressFrame::Goaway {
                last_stream: StreamId::new(last_stream),
                code: H2ErrorCode::NO_ERROR,
                debug_data: None,
            };
            let frames = match refusal {
                PeerRefusal::RefusedStream => {
                    let mut frames = eligibility_frame(
                        &mut peer,
                        EgressFrame::Reset {
                            stream: StreamId::new(1),
                            code: H2ErrorCode::REFUSED_STREAM,
                        },
                    );
                    frames.extend(eligibility_frame(&mut peer, goaway(1)));
                    frames
                }
                PeerRefusal::Goaway => eligibility_frame(&mut peer, goaway(0)),
            };
            let refuse = Arc::new(tokio::sync::Notify::new());
            let (transport, wire) = FixtureTransport::new([
                FixtureRead::Data(server_settings_bytes(100)),
                FixtureRead::WaitData(Arc::clone(&refuse), frames),
            ]);
            let (session, handle) =
                OwnedH2UpstreamSession::new(options(context, transport)).unwrap();
            let (run, outcome) = tokio::join!(session.run(), async {
                complete_credit_wait(|| handle.available_capacity() == 100).await;
                let mut cancellation = Some(CancellationSource::new());
                let token = cancellation.as_ref().unwrap().token();
                let (upload, probe) = Upload::new(0, 1).held();
                let head = request_head(HttpMethod::Post, false, None);
                let reservation = handle.reserve_request().unwrap();
                let mut submitted = match kind {
                    SubmitKind::Plain => Submitted::Plain(reservation.submit(head, upload, token)),
                    SubmitKind::ResponseInterest => Submitted::Exclusive(
                        reservation.submit_with_response_interest(head, upload, token, None),
                    ),
                    SubmitKind::Exclusive => {
                        Submitted::Exclusive(reservation.submit_exclusive(head, upload, token))
                    }
                };
                complete_credit_wait(|| header_streams(&wire) == [StreamId::new(1)]).await;
                let fired = Rc::new(Cell::new(false));
                if matches!(cancel, RefusalCancel::RacingTheSample) {
                    let (cancellation, fired) = (cancellation.take().unwrap(), Rc::clone(&fired));
                    crate::experimental::after_next_refusal_cause_sample(move || {
                        cancellation.request_cancellation();
                        fired.set(true);
                    });
                }
                refuse.notify_one();
                // Applying the refusal stops the upload.
                complete_credit_wait(|| !probe.stops.borrow().is_empty()).await;
                match cancel {
                    RefusalCancel::None => {}
                    RefusalCancel::Explicit => {
                        submitted.cancel(HttpError::new(HttpErrorCode::READ_TIMEOUT));
                    }
                    RefusalCancel::Token => {
                        cancellation.take().unwrap().request_cancellation();
                    }
                    RefusalCancel::RacingTheSample => {
                        assert!(fired.get(), "the refusal sampled the request's cause");
                    }
                }
                let progress = submitted.progress().await;
                complete_credit_wait(|| handle.is_finished()).await;
                let stops = probe.stops.borrow().clone();
                (stops, progress)
            });
            run.unwrap();
            outcome
        })
    })
}

/// Each case's outcome beside the refused request's: the upload stopped with the refusal, and
/// the claim fails with it, unprocessed, sequence 0 (`DIV-035`).
fn peer_refusal_table(
    cases: &[(PeerRefusal, SubmitKind, RefusalCancel)],
) -> [Vec<(String, Vec<Option<HttpErrorCode>>, RequestProgress)>; 2] {
    let refused = || RequestProgress {
        error: Some(HttpErrorCode::REFUSED_STREAM),
        unprocessed: true,
        sequence: Some(0),
    };
    let mut outcomes = Vec::new();
    let mut expected = Vec::new();
    for &(refusal, kind, cancel) in cases {
        let case = format!("{refusal:?}, {kind:?}, {cancel:?}");
        let (stops, progress) = unclaimed_peer_refusal(refusal, kind, cancel);
        outcomes.push((case.clone(), stops, progress));
        expected.push((case, vec![Some(HttpErrorCode::REFUSED_STREAM)], refused()));
    }
    [outcomes, expected]
}

/// Every refusal and submission kind, with `cancels`.
fn peer_refusal_cases(
    kinds: &[SubmitKind],
    cancels: &[RefusalCancel],
) -> Vec<(PeerRefusal, SubmitKind, RefusalCancel)> {
    let mut cases = Vec::new();
    for refusal in [PeerRefusal::RefusedStream, PeerRefusal::Goaway] {
        for &kind in kinds {
            for &cancel in cancels {
                cases.push((refusal, kind, cancel));
            }
        }
    }
    cases
}

const ALL_SUBMIT_KINDS: [SubmitKind; 3] = [
    SubmitKind::Plain,
    SubmitKind::ResponseInterest,
    SubmitKind::Exclusive,
];

/// A consumer's cancel after the driver applied a peer refusal, before the consumer claimed the
/// response, does not replace the refusal: the request stays unprocessed with the refusal's error,
/// so the resend rule never resends a request its consumer cancelled (`DIV-035`).
#[test]
fn a_consumer_cancel_after_an_applied_peer_refusal_keeps_the_refusal() {
    let [outcomes, expected] = peer_refusal_table(&peer_refusal_cases(
        &ALL_SUBMIT_KINDS,
        &[RefusalCancel::Explicit, RefusalCancel::Token],
    ));
    assert_eq!(outcomes, expected);
}

/// A peer refusal is the unprocessed request's selected cause, so its unclaimed submission fails
/// with it, as one its connection refused before commit does (`DIV-035`).
#[test]
fn an_unclaimed_peer_refused_submission_fails_with_the_refusal() {
    let [outcomes, expected] = peer_refusal_table(&peer_refusal_cases(
        &ALL_SUBMIT_KINDS,
        &[RefusalCancel::None],
    ));
    assert_eq!(outcomes, expected);
}

/// A cancellation that lands right after a peer refusal samples its request's cause follows the
/// refusal: one sample decides both the flag and the error (`DIV-035`).
#[test]
fn a_cancellation_racing_a_peer_refusal_follows_it() {
    let [outcomes, expected] = peer_refusal_table(&peer_refusal_cases(
        &[SubmitKind::Plain],
        &[RefusalCancel::RacingTheSample],
    ));
    assert_eq!(outcomes, expected);
}

/// The turns a session gets to apply an origin's bytes, and the end behind them, before a
/// `DIV-033` comparison's reader starts.
const LATE_READ_TURNS: usize = 1_000;

async fn settle_late_read() {
    for _ in 0..LATE_READ_TURNS {
        tokio::task::yield_now().await;
    }
}

/// How the origin's response to a GET ends in the `DIV-033` comparisons.
#[derive(Clone, Copy, Debug)]
enum StreamEnd {
    /// A head and `first`, then RST_STREAM(CANCEL).
    ResetMidBody,
    /// A head and `first`, then RST_STREAM(NO_ERROR), which a response that has not ended
    /// reads as `CANCEL`.
    NoErrorResetMidBody,
    /// A head, then RST_STREAM(INTERNAL_ERROR).
    ResetAfterHead,
    /// A head and `first`, then EOF.
    EofMidBody,
    /// A head and `first`, then a failed read.
    ReadErrorMidBody,
    /// A head and `first`, then a DATA frame on stream 0, a connection error.
    ConnectionErrorMidBody,
    /// A head and `first` with END_STREAM, then RST_STREAM(NO_ERROR).
    CompleteThenNoErrorReset,
}

impl StreamEnd {
    const ALL: [Self; 7] = [
        Self::ResetMidBody,
        Self::NoErrorResetMidBody,
        Self::ResetAfterHead,
        Self::EofMidBody,
        Self::ReadErrorMidBody,
        Self::ConnectionErrorMidBody,
        Self::CompleteThenNoErrorReset,
    ];

    /// The origin's reads: SETTINGS, the response released by `respond`, how it ends, and, on
    /// a connection that stays open, an EOF released by `finish` once the reader is done.
    fn reads(
        self,
        respond: &Arc<tokio::sync::Notify>,
        finish: &Arc<tokio::sync::Notify>,
    ) -> Vec<FixtureRead> {
        let stream = StreamId::new(1);
        let mut peer = Http2Codec::new(TransportDirection::Downstream);
        let mut response = response_head(&mut peer, stream, 200, false, None);
        if !matches!(self, Self::ResetAfterHead) {
            response.extend(eligibility_frame(
                &mut peer,
                EgressFrame::Body {
                    stream,
                    body: queue(b"first"),
                    padding: None,
                    end_stream: matches!(self, Self::CompleteThenNoErrorReset),
                },
            ));
        }
        let reset = |peer: &mut Http2Codec, code| {
            eligibility_frame(peer, EgressFrame::Reset { stream, code })
        };
        let open_end = FixtureRead::WaitDataEof(Arc::clone(finish), Vec::new());
        let mut reads = vec![FixtureRead::Data(server_settings_bytes(1))];
        match self {
            Self::ResetMidBody => response.extend(reset(&mut peer, H2ErrorCode::CANCEL)),
            Self::NoErrorResetMidBody | Self::CompleteThenNoErrorReset => {
                response.extend(reset(&mut peer, H2ErrorCode::NO_ERROR));
            }
            Self::ResetAfterHead => response.extend(reset(&mut peer, H2ErrorCode::INTERNAL_ERROR)),
            // DATA (type 0) with a one-byte payload on stream 0.
            Self::ConnectionErrorMidBody => response.extend([0, 0, 1, 0, 0, 0, 0, 0, 0, b'x']),
            Self::EofMidBody | Self::ReadErrorMidBody => {}
        }
        match self {
            Self::EofMidBody => reads.push(FixtureRead::WaitDataEof(Arc::clone(respond), response)),
            Self::ReadErrorMidBody => {
                reads.push(FixtureRead::WaitData(Arc::clone(respond), response));
                reads.push(FixtureRead::Error(HttpErrorCode::TRANSPORT_READ_ERROR));
            }
            _ => {
                reads.push(FixtureRead::WaitData(Arc::clone(respond), response));
                reads.push(open_end);
            }
        }
        reads
    }

    /// What a late reader gets with `DIV-033` on: every event received before the end, then
    /// the end's error.
    fn delivered(self) -> LateRead {
        let truncated = |error| LateRead {
            status: Some(200),
            body: b"first".to_vec(),
            error: Some(error),
            eom: false,
        };
        match self {
            Self::ResetMidBody | Self::NoErrorResetMidBody => truncated(HttpErrorCode::CANCEL),
            Self::ResetAfterHead => LateRead {
                status: Some(200),
                error: Some(HttpErrorCode::INTERNAL_ERROR),
                ..LateRead::default()
            },
            Self::EofMidBody => truncated(HttpErrorCode::TRANSPORT_EOF),
            Self::ReadErrorMidBody => truncated(HttpErrorCode::TRANSPORT_READ_ERROR),
            Self::ConnectionErrorMidBody => truncated(HttpErrorCode::PROTOCOL_ERROR),
            Self::CompleteThenNoErrorReset => LateRead {
                status: Some(200),
                body: b"first".to_vec(),
                error: None,
                eom: true,
            },
        }
    }
}

/// What a reader that starts after a response ended got: the head's status, the body bytes,
/// and the error that ended the read, or EOM.
#[derive(Debug, Default, PartialEq)]
struct LateRead {
    status: Option<u16>,
    body: Vec<u8>,
    error: Option<HttpErrorCode>,
    eom: bool,
}

async fn read_late(source: &mut HttpSourceHolder) -> LateRead {
    let mut read = LateRead::default();
    let head = match source.read_header_event().await {
        Ok(head) => head,
        Err(error) => {
            read.error = Some(error.code());
            return read;
        }
    };
    read.status = head.headers().response_status();
    let mut eom = head.eom();
    while !eom {
        match source.read_body_event(u32::MAX).await {
            Ok(event) => {
                if let Some(bytes) = event.as_body()
                    && !bytes.is_empty()
                {
                    let length = bytes.chain_length();
                    read.body
                        .extend_from_slice(&bytes.coalesce(length).unwrap());
                }
                eom = event.eom();
            }
            Err(error) => {
                read.error = Some(error.code());
                break;
            }
        }
    }
    read.eom = eom;
    read
}

/// Releases the origin's response once the request is committed, waits for the response's
/// ingress to end and for the session to apply what followed, then reads it.
async fn read_after_the_end(
    mut source: HttpSourceHolder,
    respond: &tokio::sync::Notify,
    finish: &tokio::sync::Notify,
) -> LateRead {
    let receipt = source
        .ingress_completion()
        .expect("a fresh response has an ingress receipt");
    respond.notify_one();
    let _ = receipt.wait().await;
    settle_late_read().await;
    let read = read_late(&mut source).await;
    drop(source);
    finish.notify_one();
    read
}

/// Runs a GET whose response ends as `end` through an owned upstream HTTP/2 session, or a
/// `UniplexSession` HTTP/2 client, with `deliver_ingress_before_abort` set to `deliver`, and
/// reads the response only once its ingress has ended.
fn late_read(end: StreamEnd, owned: bool, deliver: bool) -> LateRead {
    run_on_worker(move |context| {
        Box::pin(async move {
            let respond = Arc::new(tokio::sync::Notify::new());
            let finish = Arc::new(tokio::sync::Notify::new());
            let (transport, _wire) = FixtureTransport::new(end.reads(&respond, &finish));
            let mut config = options(context.clone(), transport.clone());
            config.io.deliver_ingress_before_abort = deliver;
            let scenario = async {
                if owned {
                    let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
                    let submission = handle.reserve_request().unwrap().submit(
                        request_head(HttpMethod::Get, true, None),
                        HttpSourceHolder::empty(),
                        CancellationToken::never(),
                    );
                    let (_run, read) = tokio::join!(session.run(), async {
                        let (_, source, _request) = submission.await.unwrap().into_source_parts();
                        read_after_the_end(source, &respond, &finish).await
                    });
                    read
                } else {
                    let (session, handle) = UniplexSession::new_http2(
                        context,
                        TransportDirection::Upstream,
                        transport,
                        None,
                        config.io,
                        config.http2,
                    )
                    .unwrap();
                    let request = HttpFixedSource::make_get_request(b"/", None).unwrap();
                    let (_run, read) = tokio::join!(session.run(), async {
                        let response = handle
                            .send_request(
                                HttpSourceHolder::new(Box::new(request)),
                                CancellationToken::never(),
                            )
                            .await
                            .unwrap();
                        read_after_the_end(response.source, &respond, &finish).await
                    });
                    read
                }
            };
            tokio::time::timeout(Duration::from_secs(10), scenario)
                .await
                .expect("the session's run and the late read end")
        })
    })
}

/// `DIV-033`: with the option on, a response whose stream or connection fails after part of
/// it arrived yields the head and bytes received, then the error, as through a
/// `UniplexSession` HTTP/2 client; with it off, both discard them.
#[test]
fn received_response_events_precede_a_stream_abort_as_uniplex() {
    for end in StreamEnd::ALL {
        let owned = late_read(end, true, true);
        assert_eq!(
            owned,
            end.delivered(),
            "{end:?}: with DIV-033 on, the received head and bytes precede the error"
        );
        assert_eq!(
            owned,
            late_read(end, false, true),
            "{end:?}: the owned session matches Uniplex with DIV-033 on"
        );
        assert_eq!(
            late_read(end, true, false),
            late_read(end, false, false),
            "{end:?}: the owned session matches Uniplex with DIV-033 off"
        );
    }
}

/// How the consumer ends an owned upstream HTTP/2 exchange in the `DIV-033` cancel tests, once
/// its response's head and `first` are queued.
#[derive(Clone, Copy, Debug)]
enum H2Cancel {
    /// `OwnedH2UpstreamRequest::cancel`, with the error `UniplexStreamAbort` selects.
    Request,
    /// The request's cancellation token.
    Token,
    /// `OwnedH2UpstreamHandle::cancel`: the connection, not the request.
    Connection,
}

/// The origin's reads in the `DIV-033` cancel tests: SETTINGS, then a head and `first`, released
/// by `respond`, of a response that does not end; the connection then stays open.
fn unfinished_h2_response(respond: &Arc<tokio::sync::Notify>) -> Vec<FixtureRead> {
    let stream = StreamId::new(1);
    let mut peer = Http2Codec::new(TransportDirection::Downstream);
    let mut response = response_head(&mut peer, stream, 200, false, None);
    response.extend(eligibility_frame(
        &mut peer,
        EgressFrame::Body {
            stream,
            body: queue(b"first"),
            padding: None,
            end_stream: false,
        },
    ));
    vec![
        FixtureRead::Data(server_settings_bytes(1)),
        FixtureRead::WaitData(Arc::clone(respond), response),
    ]
}

/// Runs a GET through an owned upstream HTTP/2 session with `deliver_ingress_before_abort` set
/// to `deliver`, ends it as `cancel` once the session has queued its response's head and
/// `first`, and reads the response once the session has taken the cancel.
fn owned_h2_cancel_read(cancel: H2Cancel, deliver: bool) -> LateRead {
    run_on_worker(move |context| {
        Box::pin(async move {
            let respond = Arc::new(tokio::sync::Notify::new());
            let (transport, _wire) = FixtureTransport::new(unfinished_h2_response(&respond));
            let mut config = options(context, transport);
            config.io.deliver_ingress_before_abort = deliver;
            let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
            let token = CancellationSource::new();
            let submission = handle.reserve_request().unwrap().submit(
                request_head(HttpMethod::Get, true, None),
                HttpSourceHolder::empty(),
                token.token(),
            );
            let scenario = async {
                let (_run, read) = tokio::join!(session.run(), async {
                    let (_, mut source, request) = submission.await.unwrap().into_source_parts();
                    let receipt = source
                        .ingress_completion()
                        .expect("a fresh response has an ingress receipt");
                    respond.notify_one();
                    complete_credit_wait(|| receipt.body_bytes_received() == 5).await;
                    settle_late_read().await;
                    match cancel {
                        H2Cancel::Request => request.cancel(HttpError::with_message(
                            HttpErrorCode::CANCEL,
                            b"stream aborted",
                        )),
                        H2Cancel::Token => {
                            token.request_cancellation();
                        }
                        H2Cancel::Connection => handle.cancel(),
                    }
                    settle_late_read().await;
                    let read = read_late(&mut source).await;
                    drop(source);
                    handle.cancel();
                    read
                });
                read
            };
            tokio::time::timeout(Duration::from_secs(10), scenario)
                .await
                .expect("the session's run and the read end")
        })
    })
}

/// As `owned_h2_cancel_read` for a `UniplexSession` HTTP/2 client whose consumer aborts the
/// stream with `UniplexStreamAbort` (`DIV-023`).
fn uniplex_h2_stream_abort_read(deliver: bool) -> LateRead {
    use crate::experimental::UniplexResponseAbortExt;

    run_on_worker(move |context| {
        Box::pin(async move {
            let respond = Arc::new(tokio::sync::Notify::new());
            let (transport, _wire) = FixtureTransport::new(unfinished_h2_response(&respond));
            let mut config = options(context.clone(), transport.clone());
            config.io.deliver_ingress_before_abort = deliver;
            let (session, handle) = UniplexSession::new_http2(
                context,
                TransportDirection::Upstream,
                transport,
                None,
                config.io,
                config.http2,
            )
            .unwrap();
            let request = HttpFixedSource::make_get_request(b"/", None).unwrap();
            let scenario = async {
                let (_run, read) = tokio::join!(session.run(), async {
                    let mut response = handle
                        .send_request(
                            HttpSourceHolder::new(Box::new(request)),
                            CancellationToken::never(),
                        )
                        .await
                        .unwrap();
                    let receipt = response
                        .source
                        .ingress_completion()
                        .expect("a fresh response has an ingress receipt");
                    respond.notify_one();
                    complete_credit_wait(|| receipt.body_bytes_received() == 5).await;
                    settle_late_read().await;
                    response
                        .take_stream_abort()
                        .expect("a live stream's abort")
                        .abort();
                    settle_late_read().await;
                    let read = read_late(&mut response.source).await;
                    drop(response);
                    handle.close_when_idle().unwrap();
                    read
                });
                read
            };
            tokio::time::timeout(Duration::from_secs(10), scenario)
                .await
                .expect("the session's run and the read end")
        })
    })
}

/// `DIV-033`: a cancel the request's consumer selects discards the response's received events,
/// as a `UniplexStreamAbort` does, with the option on or off.
#[test]
fn an_owned_h2_request_cancel_discards_the_received_response_as_a_uniplex_stream_abort() {
    for deliver in [true, false] {
        let owned = owned_h2_cancel_read(H2Cancel::Request, deliver);
        assert_eq!(
            owned,
            LateRead {
                error: Some(HttpErrorCode::CANCEL),
                ..LateRead::default()
            },
            "deliver {deliver}: the consumer's cancel discards the queued head and bytes"
        );
        assert_eq!(
            owned,
            uniplex_h2_stream_abort_read(deliver),
            "deliver {deliver}: the owned reader gets what a Uniplex stream abort leaves"
        );
    }
}

/// `DIV-033`: the request's cancellation token is the consumer's cancel too, so it discards.
#[test]
fn an_owned_h2_request_token_cancel_discards_the_received_response() {
    assert_eq!(
        owned_h2_cancel_read(H2Cancel::Token, true),
        LateRead {
            error: Some(HttpErrorCode::CORO_CANCELLED),
            ..LateRead::default()
        }
    );
}

/// `DIV-033`: the connection's cancel is no cancel of the request: as an ingress abort, it
/// keeps the received head and bytes ahead of its error.
#[test]
fn an_owned_h2_connection_cancel_keeps_the_received_response() {
    assert_eq!(
        owned_h2_cancel_read(H2Cancel::Connection, true),
        LateRead {
            status: Some(200),
            body: b"first".to_vec(),
            error: Some(HttpErrorCode::CORO_CANCELLED),
            eom: false,
        }
    );
}

/// What happens to the DATA a peer's reset left unread in the owned connection-credit test.
#[derive(Clone, Copy, Debug)]
enum KeptH2Data {
    /// The reader takes it, then the reset's error.
    Read,
    /// The reader drops the response unread.
    Dropped,
}

/// Connection credit returned on the wire, in WINDOW_UPDATEs on stream 0.
fn connection_credit(wire: &RefCell<FixtureTransportState>) -> u64 {
    decode_client_frames(&wire.borrow().writes.concat())
        .window_updates
        .iter()
        .filter(|(stream, _)| stream.is_unspecified())
        .map(|&(_, amount)| u64::from(amount))
        .sum()
}

/// What the owned connection-credit test observed: the connection credit on the wire and the
/// session's available capacity once the reset is applied, then the same once the reader has
/// read or dropped the response; the stream credit; the body bytes read; and the read's error.
type KeptCredit = ((u64, u32), (u64, u32), u64, usize, Option<HttpErrorCode>);

/// The origin, which allows one stream, answers a GET with a head and 40,000 bytes of DATA, more
/// than half the 65,535-byte connection window (the threshold at which connection credit is
/// returned), then RST_STREAM(CANCEL).
fn peer_reset_connection_credit(deliver: bool, kept: KeptH2Data) -> KeptCredit {
    const UNREAD: usize = 40_000;
    run_on_worker(move |context| {
        Box::pin(async move {
            let stream = StreamId::new(1);
            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            let mut response = response_head(&mut peer, stream, 200, false, None);
            for chunk in vec![b'k'; UNREAD].chunks(16_384) {
                response.extend(eligibility_frame(
                    &mut peer,
                    EgressFrame::Body {
                        stream,
                        body: queue(chunk),
                        padding: None,
                        end_stream: false,
                    },
                ));
            }
            response.extend(eligibility_frame(
                &mut peer,
                EgressFrame::Reset {
                    stream,
                    code: H2ErrorCode::CANCEL,
                },
            ));
            let respond = Arc::new(tokio::sync::Notify::new());
            let (transport, wire) = FixtureTransport::new([
                FixtureRead::Data(server_settings_bytes(1)),
                FixtureRead::WaitData(Arc::clone(&respond), response),
            ]);
            let mut config = options(context, transport);
            config.io.deliver_ingress_before_abort = deliver;
            let (session, handle) = OwnedH2UpstreamSession::new(config).unwrap();
            let submission = handle.reserve_request().unwrap().submit(
                request_head(HttpMethod::Get, true, None),
                HttpSourceHolder::empty(),
                CancellationToken::never(),
            );
            let scenario = async {
                let (_run, outcome) = tokio::join!(session.run(), async {
                    let (_, mut source, _request) = submission.await.unwrap().into_source_parts();
                    let receipt = source
                        .ingress_completion()
                        .expect("a fresh response has an ingress receipt");
                    respond.notify_one();
                    let _ = receipt.wait().await;
                    settle_late_read().await;
                    let at_reset = (connection_credit(&wire), handle.available_capacity());
                    let read = match kept {
                        KeptH2Data::Read => read_late(&mut source).await,
                        KeptH2Data::Dropped => LateRead::default(),
                    };
                    drop(source);
                    complete_credit_wait(|| {
                        connection_credit(&wire) >= UNREAD as u64
                            && handle.available_capacity() == 1
                    })
                    .await;
                    settle_late_read().await;
                    let credit = (connection_credit(&wire), handle.available_capacity());
                    let decoded = decode_client_frames(&wire.borrow().writes.concat());
                    let stream_credit = decoded
                        .window_updates
                        .iter()
                        .filter(|(id, _)| *id == stream)
                        .map(|&(_, amount)| u64::from(amount))
                        .sum();
                    assert!(decoded.goaways.is_empty(), "the connection stays healthy");
                    handle.cancel();
                    (at_reset, credit, stream_credit, read.body.len(), read.error)
                });
                outcome
            };
            tokio::time::timeout(Duration::from_secs(10), scenario)
                .await
                .expect("the session's run and the read end")
        })
    })
}

/// By default a peer's reset discards the unread DATA and returns its connection credit at once.
/// With `DIV-033` on, the kept DATA holds that credit until its reader takes it or drops the
/// response, either of which returns all of it, as in `UniplexSession`
/// (`h2_peer_reset_returns_connection_credit_for_kept_data_as_it_is_read`). Either way the stream
/// keeps its admission slot until its source completes: once the reader has taken the error, and
/// any kept events before it, or dropped the response. The reset stream sends no WINDOW_UPDATE of
/// its own.
#[test]
fn an_owned_h2_peer_reset_holds_the_connection_credit_of_kept_data_until_it_is_read() {
    assert_eq!(
        peer_reset_connection_credit(false, KeptH2Data::Read),
        ((40_000, 0), (40_000, 1), 0, 0, Some(HttpErrorCode::CANCEL)),
        "default: the reset discards the DATA and returns its credit; the read takes the error"
    );
    assert_eq!(
        peer_reset_connection_credit(true, KeptH2Data::Read),
        ((0, 0), (40_000, 1), 0, 40_000, Some(HttpErrorCode::CANCEL)),
        "kept DATA read: credit after the read, then the reset's error"
    );
    assert_eq!(
        peer_reset_connection_credit(true, KeptH2Data::Dropped),
        ((0, 0), (40_000, 1), 0, 0, None),
        "kept DATA dropped unread: credit at the drop"
    );
}
