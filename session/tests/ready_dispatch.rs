/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use futures::FutureExt;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::headers::HttpHeaders;
use roxygen_core::http::HttpMethod;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::LocalSourceFuture;

use super::*;
use crate::uniplex_tests::ready_dispatch_transport;
use crate::uniplex_tests::run_on_worker;

struct CountedBodySource {
    source: HttpFixedSource,
    reads: Rc<Cell<usize>>,
    pending: bool,
}

impl HttpSource for CountedBodySource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        self.source.read_header_event()
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.reads.set(self.reads.get() + 1);
        if self.pending {
            Box::pin(std::future::pending())
        } else {
            self.source.read_body_event(maximum)
        }
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.source.stop_reading(error);
    }
}

#[derive(Clone, Copy)]
enum Attempt {
    Callback,
    SessionCancellation,
    BudgetExhausted,
    ZeroCredit,
    WriteWakeReady,
    WriteWakePending,
}

fn check_attempt(attempt: Attempt, direct: bool) {
    run_on_worker(move |context| {
        Box::pin(async move {
            let (session, handle) = UniplexSession::new_http2(
                context.clone(),
                TransportDirection::Upstream,
                ready_dispatch_transport(),
                None,
                UniplexConfig::default(),
                Http2SessionConfig::default(),
            )
            .expect("H2 fixture starts");
            session.state.borrow_mut().ready_dispatch = direct;
            let reads = Rc::new(Cell::new(0));
            let body = BufQueue::from_vec(vec![b'x'; 4096], 4096).expect("fixture body fits");
            let mut fixed = HttpFixedSource::make_fixed_request(b"/", HttpMethod::Post, Some(body))
                .expect("fixture request is valid");
            fixed
                .message_mut()
                .expect("fixed request has a header")
                .headers_mut()
                .set(b"host", b"example.test");
            let mut source = HttpSourceHolder::new(Box::new(CountedBodySource {
                source: fixed,
                reads: Rc::clone(&reads),
                pending: matches!(attempt, Attempt::WriteWakePending),
            }));
            let headers = source
                .read_header_event()
                .await
                .expect("request header is ready");
            let prepared = UniplexHandle::prepare_request_with_headers(
                headers,
                source,
                handle
                    .reserve_request()
                    .expect("request reservation succeeds"),
            );
            let mut response = Some(
                handle
                    .send_prepared_request(prepared)
                    .expect("request starts"),
            );
            drive_core_callbacks(&session.state, &handle.read_wake).expect("callbacks run");
            let stream = session
                .state
                .borrow()
                .http2
                .as_ref()
                .expect("H2 state exists")
                .egress_order
                .front()
                .expect("request retains its body transfer")
                .stream;
            let cancellation = CancellationSource::new();
            match attempt {
                Attempt::Callback => {
                    drop(response.take());
                    assert!(session.state.borrow().core().has_pending_events());
                }
                Attempt::SessionCancellation => {
                    cancellation.request_cancellation();
                }
                Attempt::BudgetExhausted => {
                    while tokio::task::coop::has_budget_remaining() {
                        tokio::task::consume_budget().await;
                    }
                }
                Attempt::ZeroCredit => {
                    let mut state = session.state.borrow_mut();
                    let credit = state.codec.connection_send_credit() as usize;
                    let mut encoded = BufQueue::new(credit + 1024);
                    state
                        .codec
                        .generate(
                            &mut encoded,
                            EgressFrame::Body {
                                stream,
                                body: BufQueue::from_vec(vec![0; credit], credit)
                                    .expect("credit-sized body fits"),
                                padding: None,
                                end_stream: false,
                            },
                            &mut NoopCodecCallback,
                        )
                        .expect("connection credit is consumed");
                    assert_eq!(state.codec.connection_send_credit(), 0);
                    assert!(
                        state
                            .http2
                            .as_ref()
                            .expect("H2 state exists")
                            .stream_send_credit(stream)
                            > 0
                    );
                }
                Attempt::WriteWakeReady | Attempt::WriteWakePending => {
                    handle.write_wake.notify_one();
                }
            }
            let work = session
                .state
                .borrow_mut()
                .pop_ready_egress_work(&BTreeSet::new())
                .expect("initial transfer is eligible");
            let observer = session
                .observer
                .upgrade()
                .expect("session retains observer");
            let outcome = next_egress_event(
                &session.state,
                work,
                &observer,
                &context,
                &handle.read_wake,
                &handle.write_wake,
                cancellation.token(),
                false,
            )
            .await
            .expect("dispatch returns a normal outcome");
            match attempt {
                Attempt::Callback | Attempt::SessionCancellation => {
                    assert!(matches!(outcome, NextEgressEvent::Interrupted));
                    assert_eq!(reads.get(), 0, "control work precedes source construction");
                }
                Attempt::BudgetExhausted => {
                    assert!(matches!(outcome, NextEgressEvent::Pending));
                    assert_eq!(
                        reads.get(),
                        0,
                        "exhausted budget defers source construction"
                    );
                    tokio::task::yield_now().await;
                    assert!(tokio::task::coop::has_budget_remaining());
                    let work = session
                        .state
                        .borrow_mut()
                        .pop_ready_egress_work(&BTreeSet::new())
                        .expect("deferred transfer remains eligible");
                    let resumed = next_egress_event(
                        &session.state,
                        work,
                        &observer,
                        &context,
                        &handle.read_wake,
                        &handle.write_wake,
                        cancellation.token(),
                        false,
                    )
                    .await
                    .expect("dispatch resumes after the cooperative yield");
                    assert!(matches!(resumed, NextEgressEvent::Ready(_, _)));
                    assert_eq!(reads.get(), 1);
                }
                Attempt::ZeroCredit => {
                    assert!(matches!(outcome, NextEgressEvent::Pending));
                    assert_eq!(reads.get(), 1);
                    assert!(
                        session
                            .state
                            .borrow_mut()
                            .pop_ready_egress_work(&BTreeSet::new())
                            .is_none()
                    );
                    let state = session.state.borrow();
                    assert!(
                        state
                            .http2
                            .as_ref()
                            .expect("H2 state exists")
                            .queued_transfers()
                            .any(|(_, work)| work.transfer.has_buffered_body_bytes())
                    );
                }
                Attempt::WriteWakeReady => {
                    assert!(matches!(outcome, NextEgressEvent::Ready(_, _)));
                    assert_eq!(
                        reads.get(),
                        1,
                        "ready source wins before write notification"
                    );
                }
                Attempt::WriteWakePending => {
                    assert!(matches!(outcome, NextEgressEvent::Interrupted));
                    assert_eq!(
                        reads.get(),
                        1,
                        "write notification follows a pending source poll"
                    );
                }
            }
            drop(outcome);
            drop(response);
            let _ = session.run().await;
            assert!(handle.is_finished());
        })
    });
}

#[test]
fn ready_dispatch_preserves_first_attempt_arbitration() {
    for direct in [false, true] {
        for attempt in [
            Attempt::Callback,
            Attempt::SessionCancellation,
            Attempt::BudgetExhausted,
            Attempt::ZeroCredit,
            Attempt::WriteWakeReady,
            Attempt::WriteWakePending,
        ] {
            check_attempt(attempt, direct);
        }
    }
}

#[test]
fn direct_readiness_preserves_synchronous_self_wake() {
    let mut readiness =
        EgressSourceReadiness::new(Arc::new(tokio::sync::Notify::new()), None, None, None);
    assert!(
        readiness
            .poll_with(|context| {
                context.waker().wake_by_ref();
                Poll::Pending
            })
            .is_pending()
    );
    assert!(
        readiness.is_ready(),
        "a Pending result must retain a synchronous self-wake"
    );
    assert!(readiness.poll_with(|_| Poll::Pending).is_pending());
    assert!(!readiness.is_ready());
    assert!(readiness.poll_with(|_| Poll::Ready(Ok(None))).is_ready());
    assert!(readiness.is_ready());
}

#[test]
fn direct_readiness_cancellation_wakes_and_preempts_source() {
    run_on_worker(|_| {
        Box::pin(async {
            let cancellation = CancellationSource::new();
            let mut readiness = EgressSourceReadiness::new(
                Arc::new(tokio::sync::Notify::new()),
                Some(cancellation.token()),
                None,
                None,
            );
            assert!(readiness.poll_with(|_| Poll::Pending).is_pending());
            assert!(!readiness.is_ready());
            cancellation.request_cancellation();
            assert!(readiness.is_ready());
            let result = readiness.poll_with(|_| panic!("cancelled source must not be polled"));
            assert!(
                matches!(result, Poll::Ready(Err(error)) if error.code() == HttpErrorCode::CORO_CANCELLED)
            );
        })
    });
}

struct OwnershipSource {
    source: HttpFixedSource,
    on_body: Option<Box<dyn FnOnce()>>,
    on_drop: Box<dyn Fn()>,
}

impl HttpSource for OwnershipSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        self.source.read_header_event()
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        if let Some(callback) = self.on_body.take() {
            callback();
        }
        self.source.read_body_event(maximum)
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.source.stop_reading(error);
    }
}

impl Drop for OwnershipSource {
    fn drop(&mut self) {
        (self.on_drop)();
    }
}

fn ownership_source(
    on_body: Option<Box<dyn FnOnce()>>,
    on_drop: Box<dyn Fn()>,
) -> HttpSourceHolder {
    let body = BufQueue::from_vec(vec![b'x'; 4096], 4096).expect("fixture body fits");
    let mut source = HttpFixedSource::make_fixed_request(b"/", HttpMethod::Post, Some(body))
        .expect("fixture request is valid");
    source
        .message_mut()
        .expect("fixed request has a header")
        .headers_mut()
        .set(b"host", b"example.test");
    HttpSourceHolder::new(Box::new(OwnershipSource {
        source,
        on_body,
        on_drop,
    }))
}

#[test]
fn ready_source_return_after_detach_retains_an_unscheduled_owner() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle) = UniplexSession::new_http2(
                context.clone(),
                TransportDirection::Upstream,
                ready_dispatch_transport(),
                None,
                UniplexConfig::default(),
                Http2SessionConfig::default(),
            )
            .expect("H2 fixture starts");
            session.state.borrow_mut().ready_dispatch = true;
            let stream = StreamId::new(1);
            let state = Rc::downgrade(&session.state);
            let drop_state = state.clone();
            let reads = Rc::new(Cell::new(0));
            let body_reads = Rc::clone(&reads);
            let drops = Rc::new(Cell::new(0));
            let source_drops = Rc::clone(&drops);
            let source = ownership_source(
                Some(Box::new(move || {
                    body_reads.set(body_reads.get() + 1);
                    let state = state.upgrade().expect("driver is live during source poll");
                    let mut state = state.borrow_mut();
                    assert!(state.http2.as_ref().unwrap().egress_order.is_empty());
                    assert!(
                        apply_stream_update_locked(
                            &mut state,
                            StreamUpdate::new(stream, StreamAction::None, true),
                        )
                        .is_none()
                    );
                })),
                Box::new(move || {
                    if let Some(state) = drop_state.upgrade() {
                        assert!(
                            state.try_borrow_mut().is_ok(),
                            "drop runs outside driver borrow"
                        );
                    }
                    source_drops.set(source_drops.get() + 1);
                }),
            );
            let cancellation = CancellationSource::new();
            let response = handle
                .send_request(source, cancellation.token())
                .await
                .unwrap();
            drive_core_callbacks(&session.state, &handle.read_wake).unwrap();
            let work = session
                .state
                .borrow_mut()
                .pop_ready_egress_work(&BTreeSet::new())
                .unwrap();
            let observer = session.observer.upgrade().unwrap();
            let outcome = next_egress_event(
                &session.state,
                work,
                &observer,
                &context,
                &handle.read_wake,
                &handle.write_wake,
                cancellation.token(),
                false,
            )
            .await
            .unwrap();
            assert!(matches!(outcome, NextEgressEvent::Ready(id, _) if id == stream));
            assert_eq!(reads.get(), 1);
            let queued = {
                let mut state = session.state.borrow_mut();
                let http2 = state.http2.as_ref().unwrap();
                assert!(!http2.streams.contains_key(&stream));
                assert_eq!(http2.orphaned_egress[&stream].len(), 1);
                assert!(state.pop_ready_egress_work(&BTreeSet::new()).is_none());
                state.take_egress_work()
            };
            assert_eq!(queued.len(), 1);
            drop(queued);
            assert_eq!(drops.get(), 1);
            assert!(
                session
                    .state
                    .borrow()
                    .http2
                    .as_ref()
                    .unwrap()
                    .orphaned_egress
                    .is_empty()
            );
            drop(outcome);
            drop(response);
            let _ = session.run().await;
            assert!(handle.is_finished());
            assert_eq!(drops.get(), 1);
        })
    });
}

#[test]
fn an_empty_h2_egress_order_skips_the_stream_scan() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle) = UniplexSession::new_http2(
                context.clone(),
                TransportDirection::Upstream,
                ready_dispatch_transport(),
                None,
                UniplexConfig::default(),
                Http2SessionConfig::default(),
            )
            .expect("H2 fixture starts");
            let cancellation = CancellationSource::new();
            let mut responses = Vec::new();
            for _ in 0..3 {
                let source = ownership_source(None, Box::new(|| {}));
                let response = handle.send_request(source, cancellation.token()).await;
                responses.push(response.expect("request starts"));
            }
            drive_core_callbacks(&session.state, &handle.read_wake).expect("callbacks run");
            let mut queued = session.state.borrow_mut().take_egress_work();
            assert_eq!(queued.len(), 3);
            {
                let state = session.state.borrow();
                let http2 = state.http2.as_ref().expect("H2 state exists");
                assert!(http2.egress_order.is_empty());
                // Streams stay scheduled after their transfers leave the queue, until they end.
                assert_eq!(http2.scheduler.urgency_queue_entries_for_test(), 3);
            }
            take_h2_egress_scan_evaluations_for_test();
            assert!(
                session
                    .state
                    .borrow()
                    .ready_egress_work(&BTreeSet::new())
                    .is_none()
            );
            assert_eq!(take_h2_egress_scan_evaluations_for_test(), 0);

            let Some(EgressWork::Transfer {
                stream,
                transfer,
                cancellation: transfer_cancellation,
                readiness,
            }) = queued.pop_front()
            else {
                panic!("requests queue transfers");
            };
            let key = EgressTransferKey {
                stream,
                generation: transfer.generation(),
            };
            session.state.borrow_mut().return_egress_transfer(
                stream,
                QueuedEgressTransfer {
                    transfer,
                    readiness,
                    cancellation: transfer_cancellation,
                },
            );
            assert!(matches!(
                session.state.borrow().ready_egress_work(&BTreeSet::new()),
                Some(ReadyEgressWork::Http2(selected)) if selected == key
            ));
            assert!(take_h2_egress_scan_evaluations_for_test() > 0);
            drop(queued);
            drop(responses);
            let _ = session.run().await;
            assert!(handle.is_finished());
        })
    });
}

#[test]
fn stream_local_owners_preserve_global_return_and_retirement_order() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle) = UniplexSession::new_http2(
                context,
                TransportDirection::Upstream,
                ready_dispatch_transport(),
                None,
                UniplexConfig::default(),
                Http2SessionConfig::default(),
            )
            .expect("H2 fixture starts");
            let dropped = Rc::new(RefCell::new(Vec::new()));
            let mut responses = Vec::new();
            for id in [1, 3, 5] {
                let state = Rc::downgrade(&session.state);
                let drops = Rc::clone(&dropped);
                let source = ownership_source(
                    None,
                    Box::new(move || {
                        if let Some(state) = state.upgrade() {
                            assert!(
                                state.try_borrow_mut().is_ok(),
                                "drop runs outside driver borrow"
                            );
                        }
                        drops.borrow_mut().push(id);
                    }),
                );
                responses.push(
                    handle
                        .send_request(source, CancellationSource::new().token())
                        .await
                        .unwrap(),
                );
            }
            drive_core_callbacks(&session.state, &handle.read_wake).unwrap();
            let removed = {
                let mut state = session.state.borrow_mut();
                let skipped = state
                    .http2
                    .as_ref()
                    .unwrap()
                    .egress_order
                    .iter()
                    .filter(|key| key.stream != StreamId::new(5))
                    .map(|key| (key.stream, key.generation))
                    .collect();
                let work = state.pop_ready_egress_work(&skipped).unwrap();
                assert!(
                    matches!(&work, EgressWork::Transfer { stream, .. } if *stream == StreamId::new(5))
                );
                let EgressWork::Transfer {
                    stream,
                    transfer,
                    cancellation,
                    readiness,
                } = work
                else {
                    panic!("HTTP/2 owns transfers");
                };
                state.return_egress_transfer(
                    stream,
                    QueuedEgressTransfer {
                        transfer,
                        cancellation,
                        readiness,
                    },
                );
                assert_eq!(
                    state
                        .http2
                        .as_ref()
                        .unwrap()
                        .queued_transfers()
                        .map(|(stream, _)| stream)
                        .collect::<Vec<_>>(),
                    [StreamId::new(5), StreamId::new(1), StreamId::new(3)]
                );
                state.extract_egress_work(|stream| stream != StreamId::new(3))
            };
            assert!(dropped.borrow().is_empty());
            drop(removed);
            assert_eq!(*dropped.borrow(), [5, 1]);
            let remaining = session.state.borrow_mut().take_egress_work();
            drop(remaining);
            assert_eq!(*dropped.borrow(), [5, 1, 3]);
            drop(responses);
            let _ = session.run().await;
            assert!(handle.is_finished());
            assert_eq!(*dropped.borrow(), [5, 1, 3]);
        })
    });
}

#[test]
fn body_publication_snapshot_preserves_racing_wake_and_retirement() {
    for cancel in [false, true] {
        run_on_worker(move |_| {
            Box::pin(async move {
                let publications = Arc::new(BodySourcePublications::default());
                let owner = Arc::downgrade(&publications);
                let key = EgressTransferKey {
                    stream: StreamId::new(1),
                    generation: 1,
                };
                let cancellation = Arc::new(CancellationSource::new());
                let mut readiness = EgressSourceReadiness::new(
                    Arc::new(tokio::sync::Notify::new()),
                    cancel.then(|| cancellation.token()),
                    Some(BodySourcePublication {
                        key,
                        owner: owner.clone(),
                    }),
                    None,
                );
                readiness.wake.publish();
                assert!(readiness.poll_with(|_| Poll::Pending).is_pending());
                let snapshot = publications.take();
                assert_eq!(snapshot.len(), 1);
                assert!(!readiness.is_ready());
                let late_waker = Waker::from(Arc::clone(&readiness.wake));
                let wake = late_waker.clone();
                std::thread::scope(|scope| {
                    scope
                        .spawn(move || {
                            if cancel {
                                cancellation.request_cancellation();
                            } else {
                                wake.wake();
                            }
                        })
                        .join()
                        .unwrap();
                });
                drop(snapshot);
                assert_eq!(publications.candidates().len(), 1);
                assert!(readiness.is_ready());
                let polls = Cell::new(0);
                let result = readiness.poll_with(|_| {
                    polls.set(polls.get() + 1);
                    Poll::Ready(Ok(None))
                });
                if cancel {
                    assert!(matches!(result, Poll::Ready(Err(error))
                        if error.code() == HttpErrorCode::CORO_CANCELLED));
                    assert_eq!(polls.get(), 0);
                } else {
                    assert!(matches!(result, Poll::Ready(Ok(None))));
                    assert_eq!(polls.get(), 1);
                }
                drop(readiness);
                let replacement = EgressSourceReadiness::new(
                    Arc::new(tokio::sync::Notify::new()),
                    None,
                    Some(BodySourcePublication {
                        key,
                        owner: owner.clone(),
                    }),
                    None,
                );
                replacement.wake.publish();
                late_waker.wake_by_ref();
                assert!(std::ptr::eq(
                    publications.candidates().get(&key).unwrap().as_ptr(),
                    Arc::as_ptr(&replacement.wake),
                ));
                drop(replacement);
                for _ in 0..4 {
                    late_waker.wake_by_ref();
                }
                assert!(publications.candidates().is_empty());
                let intrinsic = CancellationSource::new();
                let unwinding = EgressSourceReadiness::new(
                    Arc::new(tokio::sync::Notify::new()),
                    None,
                    Some(BodySourcePublication {
                        key,
                        owner: owner.clone(),
                    }),
                    Some(intrinsic.token()),
                );
                let unwind_waker = Waker::from(Arc::clone(&unwinding.wake));
                let unwind = catch_unwind(AssertUnwindSafe(move || {
                    let _readiness = unwinding;
                    intrinsic.request_cancellation();
                    panic!("publication retirement unwind");
                }));
                assert!(unwind.is_err());
                assert!(publications.candidates().is_empty());
                unwind_waker.wake_by_ref();
                assert!(publications.candidates().is_empty());
                drop(publications);
                assert!(owner.upgrade().is_none());
                late_waker.wake_by_ref();
            })
        });
    }
}

struct PublicationProbe {
    reads: Cell<usize>,
    retired_reads: Cell<usize>,
    resumes: Cell<usize>,
    retired_resumes: Cell<usize>,
    drops: Cell<usize>,
    outside_borrow: Cell<bool>,
    completions: RefCell<Vec<Option<HttpErrorCode>>>,
    ready_order: Rc<RefCell<Vec<StreamId>>>,
    driver: Weak<RefCell<DriverState>>,
}

struct PublicationCompletion(Rc<PublicationProbe>);

impl roxygen_core::stream_source::HttpStreamSourceCallback for PublicationCompletion {
    fn source_complete(&mut self, _stream: StreamId, error: Option<HttpError>) {
        self.0
            .completions
            .borrow_mut()
            .push(error.map(|error| error.code()));
    }
}

struct PublicationReadRetirement(Rc<PublicationProbe>);

impl Drop for PublicationReadRetirement {
    fn drop(&mut self) {
        self.0.retired_reads.set(self.0.retired_reads.get() + 1);
    }
}

struct PublicationResumeRetirement(Rc<PublicationProbe>);

impl Drop for PublicationResumeRetirement {
    fn drop(&mut self) {
        self.0.retired_resumes.set(self.0.retired_resumes.get() + 1);
    }
}

struct PublicationSource {
    source: roxygen_core::stream_source::HttpStreamSource,
    probe: Rc<PublicationProbe>,
    on_read: Option<Box<dyn FnOnce()>>,
}

impl HttpSource for PublicationSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        self.source.read_header_event()
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        if let Some(on_read) = self.on_read.take() {
            on_read();
        }
        self.probe.reads.set(self.probe.reads.get() + 1);
        let read = self.source.read_body_event(maximum);
        let stream = self.source.id();
        let retirement = PublicationReadRetirement(Rc::clone(&self.probe));
        Box::pin(async move {
            let result = read.await.map(|mut event| {
                if !matches!(event.kind(), HttpBodyEventKind::Suspend(_)) {
                    retirement.0.ready_order.borrow_mut().push(stream);
                    return event;
                }
                let registrations = event.take_byte_event_registrations();
                let HttpBodyEventKind::Suspend(resume) = event.into_kind() else {
                    unreachable!("the event is a suspend");
                };
                retirement.0.resumes.set(retirement.0.resumes.get() + 1);
                let resume_retirement = PublicationResumeRetirement(Rc::clone(&retirement.0));
                let mut event = HttpBodyEvent::suspend(Box::pin(async move {
                    let status = resume.await;
                    drop(resume_retirement);
                    status
                }));
                *event.byte_event_registrations_mut() = registrations;
                event
            });
            drop(retirement);
            result
        })
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.source.stop_reading(error);
    }

    fn get_stream_id(&self) -> Option<StreamId> {
        self.source.get_stream_id()
    }

    fn ingress_completion(&self) -> Option<roxygen_core::ingress_completion::IngressCompletion> {
        self.source.ingress_completion()
    }

    fn set_read_timeout(&mut self, timeout: Duration) {
        self.source.set_read_timeout(timeout);
    }
}

impl Drop for PublicationSource {
    fn drop(&mut self) {
        let outside = self
            .probe
            .driver
            .upgrade()
            .is_none_or(|driver| driver.try_borrow_mut().is_ok());
        self.probe
            .outside_borrow
            .set(self.probe.outside_borrow.get() && outside);
        self.probe.drops.set(self.probe.drops.get() + 1);
    }
}

fn publication_source(
    stream: StreamId,
    probe: &Rc<PublicationProbe>,
    on_read: Option<Box<dyn FnOnce()>>,
) -> (
    HttpSourceHolder,
    roxygen_core::stream_source::HttpStreamSourceProducer,
) {
    let source = roxygen_core::stream_source::HttpStreamSource::new_session(
        stream,
        PublicationCompletion(Rc::clone(probe)),
        65_535,
        Duration::ZERO,
    )
    .unwrap();
    let producer = source.producer();
    let mut header = HttpMessage::new();
    header.set_method(HttpMethod::Post).unwrap();
    header.set_url(b"/publication", false).unwrap();
    header.set_http_version(1, 1);
    header.headers_mut().set(b"host", b"example.test");
    assert!(producer.headers(header, false));
    (
        HttpSourceHolder::new(Box::new(PublicationSource {
            source,
            probe: Rc::clone(probe),
            on_read,
        })),
        producer,
    )
}

fn publication_probe(
    state: &Rc<RefCell<DriverState>>,
    ready_order: &Rc<RefCell<Vec<StreamId>>>,
) -> Rc<PublicationProbe> {
    Rc::new(PublicationProbe {
        reads: Cell::new(0),
        retired_reads: Cell::new(0),
        resumes: Cell::new(0),
        retired_resumes: Cell::new(0),
        drops: Cell::new(0),
        outside_borrow: Cell::new(true),
        completions: RefCell::new(Vec::new()),
        ready_order: Rc::clone(ready_order),
        driver: Rc::downgrade(state),
    })
}

#[test]
fn intrinsic_cancellation_and_dequeue_publish_capacity_blocked_owners() {
    for (terminal, reopen) in [(false, false), (true, false), (false, true)] {
        run_on_worker(move |context| {
            Box::pin(async move {
                let (session, handle) = UniplexSession::new_http2(
                    context.clone(),
                    TransportDirection::Upstream,
                    ready_dispatch_transport(),
                    None,
                    UniplexConfig::default(),
                    Http2SessionConfig::default(),
                )
                .unwrap();
                let order = Rc::new(RefCell::new(Vec::new()));
                let probe = publication_probe(&session.state, &order);
                let stream = StreamId::new(1);
                let (mut source, producer) = publication_source(stream, &probe, None);
                let header = source.read_header_event().await.unwrap();
                let mut response = Some(
                    handle
                        .send_prepared_request(UniplexHandle::prepare_request_with_headers(
                            header,
                            source,
                            handle.reserve_request().unwrap(),
                        ))
                        .unwrap(),
                );
                let mut update = None;
                let outcome = AssertUnwindSafe(async {
                    for offset in (0..65_535).step_by(16 * 1024) {
                        let size = (65_535 - offset).min(16 * 1024);
                        assert_eq!(
                            producer.body(
                                BufQueue::from_vec(vec![b'x'; size], size).unwrap(),
                                0,
                                terminal && offset + size == 65_535,
                            ),
                            if offset + size == 65_535 {
                                roxygen_core::stream_source::FlowControlState::Closed
                            } else {
                                roxygen_core::stream_source::FlowControlState::Open
                            }
                        );
                    }
                    let mut cursor = None;
                    assert!(
                        poll_fn(|context| Poll::Ready(poll_body_sources(
                            &session.state,
                            &mut cursor,
                            context,
                        )))
                        .await
                    );
                    assert_eq!((probe.reads.get(), probe.retired_reads.get()), (1, 1));
                    assert_eq!((probe.resumes.get(), probe.retired_resumes.get()), (0, 0));
                    assert_eq!(probe.drops.get(), usize::from(terminal));
                    assert!(!has_ready_body_source(&session.state.borrow()));
                    {
                        let state = session.state.borrow();
                        let http2 = state.http2.as_ref().unwrap();
                        assert_eq!(http2.egress_order.len(), 1);
                        let transfer = &http2.egress_queue(stream).unwrap()[0].transfer;
                        assert!(!transfer.can_read_source());
                        assert!(!transfer.is_complete());
                        assert!(transfer.has_buffered_body_bytes());
                        assert!(http2.body_source_publications.candidates().is_empty());
                    }
                    if reopen {
                        drive_core_callbacks(&session.state, &handle.read_wake).unwrap();
                        let work = session
                            .state
                            .borrow_mut()
                            .pop_ready_egress_work(&BTreeSet::new())
                            .unwrap();
                        let observer = session.observer.upgrade().unwrap();
                        let event = next_egress_event(
                            &session.state,
                            work,
                            &observer,
                            &context,
                            &handle.read_wake,
                            &handle.write_wake,
                            CancellationToken::never(),
                            false,
                        )
                        .await
                        .unwrap();
                        let NextEgressEvent::Ready(id, EgressEvent::Body(event)) = event else {
                            panic!("the full queue dequeues its DATA");
                        };
                        assert_eq!(id, stream);
                        assert_eq!(event.as_body().unwrap().chain_length(), 65_535);
                        assert!(!event.eom());
                        drop(event);
                        assert!(has_ready_body_source(&session.state.borrow()));
                        assert!(
                            poll_fn(|context| Poll::Ready(poll_body_sources(
                                &session.state,
                                &mut cursor,
                                context,
                            )))
                            .await
                        );
                        assert_eq!((probe.reads.get(), probe.retired_reads.get()), (2, 2));
                        assert_eq!((probe.resumes.get(), probe.retired_resumes.get()), (1, 0));
                        assert!(!has_ready_body_source(&session.state.borrow()));
                        assert_eq!(
                            producer.body(
                                BufQueue::from_vec(b"after".to_vec(), 5).unwrap(),
                                0,
                                true
                            ),
                            roxygen_core::stream_source::FlowControlState::Open
                        );
                        assert!(
                            poll_fn(|context| Poll::Ready(poll_body_sources(
                                &session.state,
                                &mut cursor,
                                context,
                            )))
                            .await
                        );
                        assert_eq!((probe.reads.get(), probe.retired_reads.get()), (3, 3));
                        assert_eq!((probe.resumes.get(), probe.retired_resumes.get()), (1, 1));
                        assert!(!has_ready_body_source(&session.state.borrow()));
                    }
                    let mut core = DetachedSessionCore::take(&session.state);
                    update = Some(
                        core.core_mut()
                            .abort_stream_without_wire(
                                stream,
                                HttpError::with_message(HttpErrorCode::CANCEL, b"intrinsic only"),
                            )
                            .unwrap(),
                    );
                    core.restore();
                    assert_eq!(
                        session
                            .state
                            .borrow()
                            .http2
                            .as_ref()
                            .unwrap()
                            .egress_order
                            .len(),
                        1
                    );
                    assert!(has_ready_body_source(&session.state.borrow()));
                    assert!(
                        poll_fn(|context| Poll::Ready(poll_body_sources(
                            &session.state,
                            &mut cursor,
                            context,
                        )))
                        .await
                    );
                    assert!(
                        session
                            .state
                            .borrow()
                            .http2
                            .as_ref()
                            .unwrap()
                            .egress_order
                            .is_empty()
                    );
                    let reads = if reopen { 3 } else { 1 };
                    assert_eq!(
                        (probe.reads.get(), probe.retired_reads.get()),
                        (reads, reads)
                    );
                    let resumes = usize::from(reopen);
                    assert_eq!(
                        (probe.resumes.get(), probe.retired_resumes.get()),
                        (resumes, resumes)
                    );
                })
                .catch_unwind()
                .await;
                if let Some(update) = update {
                    apply_stream_update(&session.state, update, &handle.read_wake);
                }
                let work = session.state.borrow_mut().take_egress_work();
                drop_items_individually(work);
                drop(response.take());
                let run = AssertUnwindSafe(session.run()).catch_unwind().await;
                if let Err(payload) = outcome {
                    std::panic::resume_unwind(payload);
                }
                if let Err(payload) = run {
                    std::panic::resume_unwind(payload);
                }
                assert!(handle.is_finished());
                assert_eq!(probe.drops.get(), 1);
                assert!(probe.outside_borrow.get());
                assert_eq!(
                    *probe.completions.borrow(),
                    [if terminal || reopen {
                        None
                    } else {
                        Some(HttpErrorCode::CANCEL)
                    }]
                );
            })
        });
    }
}

#[test]
fn pending_body_orphan_republishes_without_restoring_protocol_membership() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle) = UniplexSession::new_http2(
                context,
                TransportDirection::Upstream,
                ready_dispatch_transport(),
                None,
                UniplexConfig::default(),
                Http2SessionConfig::default(),
            )
            .unwrap();
            let stream = StreamId::new(1);
            let order = Rc::new(RefCell::new(Vec::new()));
            let probe = publication_probe(&session.state, &order);
            let driver = Rc::downgrade(&session.state);
            let (mut source, producer) = publication_source(
                stream,
                &probe,
                Some(Box::new(move || {
                    let driver = driver.upgrade().unwrap();
                    assert!(
                        apply_stream_update_locked(
                            &mut driver.borrow_mut(),
                            StreamUpdate::new(stream, StreamAction::None, true),
                        )
                        .is_none()
                    );
                })),
            );
            let header = source.read_header_event().await.unwrap();
            let response = handle
                .send_prepared_request(UniplexHandle::prepare_request_with_headers(
                    header,
                    source,
                    handle.reserve_request().unwrap(),
                ))
                .unwrap();
            let outcome = AssertUnwindSafe(async {
                let mut cursor = None;
                assert!(
                    poll_fn(|context| Poll::Ready(poll_body_sources(
                        &session.state,
                        &mut cursor,
                        context,
                    )))
                    .await
                );
                assert_eq!((probe.reads.get(), probe.retired_reads.get()), (1, 1));
                assert_eq!((probe.resumes.get(), probe.retired_resumes.get()), (1, 0));
                let (publications, snapshot, late_waker) = {
                    let state = session.state.borrow();
                    let http2 = state.http2.as_ref().unwrap();
                    let work = &http2.orphaned_egress[&stream][0];
                    assert!(!body_source_is_ready(work));
                    let readiness = work.readiness.as_ref().unwrap();
                    (
                        Arc::clone(&http2.body_source_publications),
                        http2.body_source_publications.take(),
                        Waker::from(Arc::clone(&readiness.wake)),
                    )
                };
                assert_eq!(snapshot.len(), 1);
                assert_eq!(
                    producer.body(BufQueue::from_vec(b"wake".to_vec(), 4).unwrap(), 0, true),
                    roxygen_core::stream_source::FlowControlState::Open
                );
                let wake = late_waker.clone();
                std::thread::scope(|scope| scope.spawn(move || wake.wake()).join().unwrap());
                drop(snapshot);
                assert_eq!(publications.candidates().len(), 1);
                assert!(has_ready_body_source(&session.state.borrow()));
                assert!(
                    poll_fn(|context| Poll::Ready(poll_body_sources(
                        &session.state,
                        &mut cursor,
                        context,
                    )))
                    .await
                );
                assert_eq!((probe.reads.get(), probe.retired_reads.get()), (2, 2));
                assert_eq!((probe.resumes.get(), probe.retired_resumes.get()), (1, 1));
                let work = {
                    let mut state = session.state.borrow_mut();
                    let http2 = state.http2.as_ref().unwrap();
                    assert!(!http2.streams.contains_key(&stream));
                    assert_eq!(http2.orphaned_egress[&stream].len(), 1);
                    assert!(!has_ready_body_source(&state));
                    assert!(state.pop_ready_egress_work(&BTreeSet::new()).is_none());
                    state.take_egress_work().pop_front().unwrap()
                };
                let EgressWork::Transfer {
                    mut transfer,
                    readiness,
                    ..
                } = work
                else {
                    panic!("orphan retains its transfer");
                };
                let Some(EgressEvent::Body(event)) =
                    transfer.next_event_with_limit(4).await.unwrap()
                else {
                    panic!("orphan retains its body");
                };
                assert!(event.eom());
                assert_eq!(event.as_body().unwrap().front().unwrap().as_ref(), b"wake");
                assert!(transfer.is_complete());
                drop(event);
                drop(transfer);
                drop(readiness);
                late_waker.wake_by_ref();
                assert!(publications.candidates().is_empty());
            })
            .catch_unwind()
            .await;
            let work = session.state.borrow_mut().take_egress_work();
            drop_items_individually(work);
            drop(response);
            let run = AssertUnwindSafe(session.run()).catch_unwind().await;
            if let Err(payload) = outcome {
                std::panic::resume_unwind(payload);
            }
            if let Err(payload) = run {
                std::panic::resume_unwind(payload);
            }
            assert!(handle.is_finished());
            assert_eq!(probe.drops.get(), 1);
            assert!(probe.outside_borrow.get());
            assert_eq!(*probe.completions.borrow(), [None]);
        })
    });
}

#[test]
fn header_checkin_publishes_body_after_a_negative_query() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle) = UniplexSession::new_http2(
                context.clone(),
                TransportDirection::Upstream,
                ready_dispatch_transport(),
                None,
                UniplexConfig::default(),
                Http2SessionConfig::default(),
            )
            .unwrap();
            let old = ownership_source(None, Box::new(|| {}));
            let response = handle
                .send_request(old, CancellationToken::never())
                .await
                .unwrap();
            let stream = response.stream;
            let order = Rc::new(RefCell::new(Vec::new()));
            let probe = publication_probe(&session.state, &order);
            let (source, producer) = publication_source(stream, &probe, None);
            let outcome = AssertUnwindSafe(async {
                let old = session.state.borrow_mut().take_egress_work();
                let mut core = DetachedSessionCore::take(&session.state);
                let transfer = core.core_mut().install_egress_source(stream, source, true).unwrap();
                core.restore();
                drop_items_individually(old);
                drive_core_callbacks(&session.state, &handle.read_wake).unwrap();
                session.state.borrow_mut().enqueue_transfer(stream, transfer, None);
                assert!(!has_ready_body_source(&session.state.borrow()));
                let work = session.state.borrow_mut()
                    .pop_ready_egress_work(&BTreeSet::new()).unwrap();
                let observer = session.observer.upgrade().unwrap();
                let event = next_egress_event(
                    &session.state,
                    work,
                    &observer,
                    &context,
                    &handle.read_wake,
                    &handle.write_wake,
                    CancellationToken::never(),
                    false,
                )
                .await
                .unwrap();
                assert!(matches!(event, NextEgressEvent::Ready(id, EgressEvent::Headers(_)) if id == stream));
                assert_eq!(probe.reads.get(), 0);
                assert!(has_ready_body_source(&session.state.borrow()));
                let mut cursor = None;
                assert!(poll_fn(|context| Poll::Ready(poll_body_sources(
                    &session.state, &mut cursor, context,
                )))
                .await);
                assert_eq!((probe.reads.get(), probe.retired_reads.get()), (1, 1));
                assert_eq!((probe.resumes.get(), probe.retired_resumes.get()), (1, 0));
                assert!(!has_ready_body_source(&session.state.borrow()));
                assert_eq!(
                    producer.body(BufQueue::from_vec(vec![b'x'], 1).unwrap(), 0, true),
                    roxygen_core::stream_source::FlowControlState::Open
                );
                assert!(poll_fn(|context| Poll::Ready(poll_body_sources(
                    &session.state, &mut cursor, context,
                )))
                .await);
                assert_eq!((probe.reads.get(), probe.retired_reads.get()), (2, 2));
                assert_eq!((probe.resumes.get(), probe.retired_resumes.get()), (1, 1));
            })
            .catch_unwind()
            .await;
            let work = session.state.borrow_mut().take_egress_work();
            drop_items_individually(work);
            drop(response);
            let run = AssertUnwindSafe(session.run()).catch_unwind().await;
            if let Err(payload) = outcome {
                std::panic::resume_unwind(payload);
            }
            if let Err(payload) = run {
                std::panic::resume_unwind(payload);
            }
            assert!(handle.is_finished());
            assert_eq!(probe.drops.get(), 1);
            assert!(probe.outside_borrow.get());
            assert_eq!(*probe.completions.borrow(), [None]);
        })
    });
}

#[test]
fn published_body_generations_keep_circular_order_and_the_turn_bound() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle) = UniplexSession::new_http2(
                context,
                TransportDirection::Upstream,
                ready_dispatch_transport(),
                None,
                UniplexConfig::default(),
                Http2SessionConfig::default(),
            )
            .unwrap();
            let order = Rc::new(RefCell::new(Vec::new()));
            let mut responses = Vec::new();
            let mut producers = Vec::new();
            let mut probes = Vec::new();
            let outcome = AssertUnwindSafe(async {
                for ordinal in 0..67 {
                    let stream = StreamId::new(2 * ordinal + 1);
                    let probe = publication_probe(&session.state, &order);
                    let driver = Rc::downgrade(&session.state);
                    let on_read = (ordinal == 0).then(|| {
                        Box::new(move || {
                            let driver = driver.upgrade().unwrap();
                            let removed = driver
                                .borrow_mut()
                                .extract_egress_work(|stream| stream == StreamId::new(3));
                            drop_items_individually(removed);
                        }) as Box<dyn FnOnce()>
                    });
                    let (mut source, producer) = publication_source(stream, &probe, on_read);
                    let header = source.read_header_event().await.unwrap();
                    responses.push(
                        handle
                            .send_prepared_request(UniplexHandle::prepare_request_with_headers(
                                header,
                                source,
                                handle.reserve_request().unwrap(),
                            ))
                            .unwrap(),
                    );
                    producers.push(producer);
                    probes.push(probe);
                }
                let mut cursor = None;
                for _ in 0..2 {
                    assert!(
                        poll_fn(|context| Poll::Ready(poll_body_sources(
                            &session.state,
                            &mut cursor,
                            context,
                        )))
                        .await
                    );
                }
                for (ordinal, probe) in probes.iter().enumerate() {
                    let reads = usize::from(ordinal != 1);
                    assert_eq!(
                        (probe.reads.get(), probe.retired_reads.get()),
                        (reads, reads)
                    );
                    assert_eq!(
                        (probe.resumes.get(), probe.retired_resumes.get()),
                        (reads, 0)
                    );
                }
                assert_eq!(probes[1].drops.get(), 1);
                assert_eq!(cursor.unwrap().stream, StreamId::new(133));
                let removed = session
                    .state
                    .borrow_mut()
                    .extract_egress_work(|stream| stream == StreamId::new(133));
                drop_items_individually(removed);
                assert_eq!(
                    (probes[66].resumes.get(), probes[66].retired_resumes.get()),
                    (1, 1)
                );
                assert!(!has_ready_body_source(&session.state.borrow()));
                for (ordinal, producer) in producers.iter().enumerate().rev() {
                    if ordinal == 1 || ordinal == 66 {
                        continue;
                    }
                    assert_eq!(
                        producer.body(BufQueue::from_vec(vec![b'x'], 1).unwrap(), 0, true),
                        roxygen_core::stream_source::FlowControlState::Open
                    );
                }
                let expected = {
                    let mut state = session.state.borrow_mut();
                    let skipped = state
                        .http2
                        .as_ref()
                        .unwrap()
                        .egress_order
                        .iter()
                        .filter(|key| key.stream != StreamId::new(65))
                        .map(|key| (key.stream, key.generation))
                        .collect();
                    let EgressWork::Transfer {
                        stream,
                        transfer,
                        cancellation,
                        readiness,
                    } = state.pop_ready_egress_work(&skipped).unwrap()
                    else {
                        panic!("selected Body transfer");
                    };
                    state.return_egress_transfer(
                        stream,
                        QueuedEgressTransfer {
                            transfer,
                            cancellation,
                            readiness,
                        },
                    );
                    state
                        .http2
                        .as_ref()
                        .unwrap()
                        .egress_order
                        .iter()
                        .map(|key| key.stream)
                        .collect::<Vec<_>>()
                };
                assert_eq!(expected[0], StreamId::new(65));
                assert_eq!(expected.len(), 65);
                assert_eq!(expected[64], StreamId::new(131));
                assert!(
                    poll_fn(|context| Poll::Ready(poll_body_sources(
                        &session.state,
                        &mut cursor,
                        context,
                    )))
                    .await
                );
                assert_eq!(order.borrow().as_slice(), &expected[..64]);
                assert_eq!(
                    (probes[65].reads.get(), probes[65].retired_reads.get()),
                    (1, 1)
                );
                assert_eq!(
                    (probes[65].resumes.get(), probes[65].retired_resumes.get()),
                    (1, 0)
                );
                assert!(has_ready_body_source(&session.state.borrow()));
                assert!(
                    poll_fn(|context| Poll::Ready(poll_body_sources(
                        &session.state,
                        &mut cursor,
                        context,
                    )))
                    .await
                );
                assert_eq!(*order.borrow(), expected);
                assert!(!has_ready_body_source(&session.state.borrow()));
                for (ordinal, probe) in probes.iter().enumerate() {
                    let reads = if ordinal == 1 {
                        0
                    } else if ordinal == 66 {
                        1
                    } else {
                        2
                    };
                    assert_eq!(
                        (probe.reads.get(), probe.retired_reads.get()),
                        (reads, reads)
                    );
                    let resumes = usize::from(ordinal != 1);
                    assert_eq!(
                        (probe.resumes.get(), probe.retired_resumes.get()),
                        (resumes, resumes)
                    );
                }
                let state = session.state.borrow();
                assert_eq!(
                    state
                        .http2
                        .as_ref()
                        .unwrap()
                        .egress_order
                        .iter()
                        .map(|key| key.stream)
                        .collect::<Vec<_>>(),
                    expected
                );
            })
            .catch_unwind()
            .await;
            let work = session.state.borrow_mut().take_egress_work();
            drop_items_individually(work);
            drop(responses);
            let run = AssertUnwindSafe(session.run()).catch_unwind().await;
            if let Err(payload) = outcome {
                std::panic::resume_unwind(payload);
            }
            if let Err(payload) = run {
                std::panic::resume_unwind(payload);
            }
            assert!(handle.is_finished());
            for (ordinal, probe) in probes.iter().enumerate() {
                assert_eq!(probe.drops.get(), 1);
                assert!(probe.outside_borrow.get());
                assert_eq!(
                    *probe.completions.borrow(),
                    [if ordinal == 1 || ordinal == 66 {
                        Some(HttpErrorCode::CANCEL)
                    } else {
                        None
                    }]
                );
            }
        })
    });
}

struct UnusedHandler;

impl HttpHandler for UnusedHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        panic!("reconciliation fixture must not dispatch requests");
    }
}

fn initial_ingress_meta(complete: bool) -> Box<IngressMeta> {
    Box::new(IngressMeta {
        complete,
        discard: false,
        expected_content_length: None,
        received_body_length: 0,
        request_get: false,
        get_body_recorded: false,
    })
}

fn assert_no_callback_updates(state: &Rc<RefCell<DriverState>>) {
    let mut state = state.borrow_mut();
    let processed = state
        .core_mut()
        .process_pending_events_for_driver()
        .unwrap();
    assert!(processed.updates.is_empty());
    assert!(!state.core().has_pending_events());
}

#[test]
fn rejected_push_after_clean_sweep_reconciles_without_callback_updates() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle) = UniplexSession::new_http2(
                context,
                TransportDirection::Upstream,
                ready_dispatch_transport(),
                None,
                UniplexConfig::default(),
                Http2SessionConfig {
                    settings: vec![CodecSetting {
                        identifier: SettingsId::MaxConcurrentStreams.as_u16(),
                        value: 0,
                    }],
                    ..Http2SessionConfig::default()
                },
            )
            .unwrap();
            let request = HttpFixedSource::make_get_request(b"/", None).unwrap();
            let response = handle
                .send_request(
                    HttpSourceHolder::new(Box::new(request)),
                    CancellationToken::never(),
                )
                .await
                .unwrap();
            let parent = response.stream;
            let child = StreamId::new(2);
            drive_core_callbacks(&session.state, &handle.read_wake).unwrap();
            assert!(
                !session
                    .state
                    .borrow()
                    .core()
                    .ingress_reconciliation_pending()
            );
            assert_no_callback_updates(&session.state);

            {
                let mut state = session.state.borrow_mut();
                let DriverState {
                    core,
                    http2,
                    http1_stats,
                    handler,
                    ingress_meta,
                    pending_pushes,
                    stream_read_timeout,
                    ingress_buffer_limit,
                    http1_ingress_body_ceiling,
                    flow_control_paused,
                    anti_pipeline_paused,
                    upgrade_paused,
                    close_after_ingress,
                    abort_exchanges_on_eof,
                    eof_watch,
                    ingress_close_stream,
                    reads_closed,
                    stream_byte_event_cancellations,
                    ..
                } = &mut *state;
                let mut callback = SessionCodecCallback {
                    core: core.as_deref_mut().unwrap(),
                    http2,
                    http1_stats,
                    handler: handler.clone(),
                    ingress_meta,
                    pending_pushes,
                    stream_read_timeout: *stream_read_timeout,
                    ingress_buffer_limit: *ingress_buffer_limit,
                    http1_ingress_body_ceiling: *http1_ingress_body_ceiling,
                    flow_control_paused,
                    anti_pipeline_paused,
                    upgrade_paused,
                    close_after_ingress,
                    abort_exchanges_on_eof: *abort_exchanges_on_eof,
                    eof_watch,
                    ingress_close_stream,
                    reads_closed,
                    stream_byte_event_cancellations,
                    failures: Vec::new(),
                    stream_updates: Vec::new(),
                    upstream_message_complete: false,
                    upgrade_completed: false,
                    rejected_body: false,
                    drain_trigger: None,
                    peer_goaway_generation: CallbackGoawayGeneration::NotRequested,
                };
                callback.begin_push(child, parent, None);
                assert!(callback.failures.is_empty());
                assert!(callback.stream_updates.is_empty());
                assert!(callback.core.stream(child).is_err());
                assert!(callback.core.stream(parent).is_ok());
                assert!(!callback.ingress_meta.get(&child).unwrap().discard);
                assert!(callback.pending_pushes.contains_key(&child));
                assert!(!callback.core.has_pending_events());
            }
            assert_no_callback_updates(&session.state);
            drive_core_callbacks(&session.state, &handle.read_wake).unwrap();
            let mut rejected = {
                let mut state = session.state.borrow_mut();
                assert!(state.ingress_meta.get(&child).unwrap().discard);
                assert!(state.core().stream(parent).is_ok());
                assert!(!state.core().ingress_reconciliation_pending());
                state.pending_pushes.remove(&child).unwrap().source
            };
            let error = match rejected.read_header_event().await {
                Ok(_) => panic!("over-limit push produced response headers"),
                Err(error) => error,
            };
            assert_eq!(error.code(), HttpErrorCode::REFUSED_STREAM);
            drop(rejected);
            drop(response);
            drop(session);
        })
    });
}

#[test]
fn source_completion_without_action_survives_callback_consumption() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                ready_dispatch_transport(),
                Some(Rc::new(UnusedHandler)),
                UniplexConfig::default(),
            )
            .unwrap();
            let id = StreamId::new(1);
            let request = {
                let mut state = session.state.borrow_mut();
                let request = state.core_mut().begin_incoming_stream(id).unwrap();
                state.ingress_meta.insert(id, initial_ingress_meta(false));
                request
            };
            drive_core_callbacks(&session.state, &handle.read_wake).unwrap();
            assert!(
                !session
                    .state
                    .borrow()
                    .core()
                    .ingress_reconciliation_pending()
            );
            drop(request);
            {
                let mut state = session.state.borrow_mut();
                let processed = state
                    .core_mut()
                    .process_pending_events_for_driver()
                    .unwrap();
                assert_eq!(processed.updates.len(), 1);
                let update = &processed.updates[0];
                assert_eq!(update.stream, id);
                assert_eq!(update.action, StreamAction::None);
                assert!(!update.detached);
                let stream = state.core().stream(id).unwrap();
                assert!(stream.stream_source_complete());
                assert!(!stream.ingress_eom_seen());
                assert!(!state.ingress_meta.get(&id).unwrap().discard);
                assert!(state.core().ingress_reconciliation_pending());
            }
            assert_no_callback_updates(&session.state);
            drive_core_callbacks(&session.state, &handle.read_wake).unwrap();
            {
                let state = session.state.borrow();
                assert!(state.ingress_meta.get(&id).unwrap().discard);
                assert!(state.core().stream(id).is_ok());
                assert!(!state.core().ingress_reconciliation_pending());
            }
            drop(session);
        })
    });
}

#[test]
fn deferred_removal_after_acknowledgment_requires_the_next_sweep() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                ready_dispatch_transport(),
                Some(Rc::new(UnusedHandler)),
                UniplexConfig::default(),
            )
            .unwrap();
            let id = StreamId::new(1);
            let request = {
                let mut state = session.state.borrow_mut();
                let request = state.core_mut().begin_incoming_stream(id).unwrap();
                let mut message = HttpMessage::new();
                message.set_method(HttpMethod::Get).unwrap();
                message.set_url(b"/", false).unwrap();
                message.set_http_version(1, 1);
                message
                    .headers_mut()
                    .set(HttpHeaderCode::Host, b"example.com");
                assert!(state.core_mut().ingress_headers(id, message, true).unwrap());
                state.ingress_meta.insert(id, initial_ingress_meta(true));
                request
            };
            drop(request);
            drive_core_callbacks(&session.state, &handle.read_wake).unwrap();
            {
                let state = session.state.borrow();
                let stream = state.core().stream(id).unwrap();
                assert!(stream.stream_source_complete());
                assert!(stream.ingress_eom_seen());
                assert!(!stream.egress_complete());
                assert!(!state.ingress_meta.get(&id).unwrap().discard);
                assert!(!state.core().ingress_reconciliation_pending());
            }
            let observed = Rc::new(Cell::new(false));
            let retained_request = {
                let mut state = session.state.borrow_mut();
                let request = state
                    .core_mut()
                    .begin_incoming_stream(StreamId::new(3))
                    .unwrap();
                assert!(state.core().ingress_reconciliation_pending());
                let state_ref = Rc::downgrade(&session.state);
                let drop_observed = Rc::clone(&observed);
                state.deferred_push_drops.push(ownership_source(
                    None,
                    Box::new(move || {
                        let state = state_ref.upgrade().unwrap();
                        let mut state = state.borrow_mut();
                        assert!(!state.core().ingress_reconciliation_pending());
                        let update = state.core_mut().egress_finished(id).unwrap();
                        assert_eq!(update.action, StreamAction::None);
                        assert!(update.detached);
                        assert!(state.core().stream(id).is_err());
                        assert!(!state.ingress_meta.get(&id).unwrap().discard);
                        drop_observed.set(true);
                    }),
                ));
                request
            };
            assert_no_callback_updates(&session.state);
            drive_core_callbacks(&session.state, &handle.read_wake).unwrap();
            assert!(observed.get());
            assert!(
                session
                    .state
                    .borrow()
                    .core()
                    .ingress_reconciliation_pending()
            );
            assert!(
                !session
                    .state
                    .borrow()
                    .ingress_meta
                    .get(&id)
                    .unwrap()
                    .discard
            );
            assert_no_callback_updates(&session.state);
            drive_core_callbacks(&session.state, &handle.read_wake).unwrap();
            {
                let state = session.state.borrow();
                assert!(state.core().stream(id).is_err());
                assert!(state.ingress_meta.get(&id).unwrap().discard);
                assert!(!state.core().ingress_reconciliation_pending());
            }
            drop(retained_request);
            drop(session);
        })
    });
}

enum ResponseEnding {
    TrailerFin,
    TrailerWithoutFin,
    ShortTrailerFin,
    DataWithoutFin,
}

fn response_terminal_action(
    ending: ResponseEnding,
) -> (
    Vec<(StreamAction, bool)>,
    Option<HttpErrorCode>,
    Option<HttpErrorCode>,
) {
    run_on_worker(move |context| {
        Box::pin(async move {
            let short_content_length = matches!(ending, ResponseEnding::ShortTrailerFin);
            let (session, handle) = UniplexSession::new_http2(
                context,
                TransportDirection::Upstream,
                ready_dispatch_transport(),
                None,
                UniplexConfig::default(),
                Http2SessionConfig::default(),
            )
            .unwrap();
            let mut request = HttpFixedSource::make_get_request(b"/", None).unwrap();
            request
                .message_mut()
                .unwrap()
                .headers_mut()
                .set(HttpHeaderCode::Host, b"example.test");
            let mut response = handle
                .send_request(
                    HttpSourceHolder::new(Box::new(request)),
                    CancellationToken::never(),
                )
                .await
                .unwrap();
            let stream = response.stream;
            assert!(
                session
                    .state
                    .borrow()
                    .core()
                    .stream(stream)
                    .unwrap()
                    .egress_complete()
            );

            let mut peer = Http2Codec::new(TransportDirection::Downstream);
            let mut callback = NoopCodecCallback;
            let mut wire = BufQueue::new(4096);
            peer.generate_configured_settings(&mut wire, &mut callback)
                .unwrap();
            let mut message = HttpMessage::new();
            message.set_status_code(200).unwrap();
            message.set_http_version(1, 1);
            if short_content_length {
                message
                    .headers_mut()
                    .set(HttpHeaderCode::ContentLength, b"1");
            }
            peer.generate(
                &mut wire,
                EgressFrame::Headers {
                    stream,
                    message: &message,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut callback,
            )
            .unwrap();
            session
                .state
                .borrow_mut()
                .ingress
                .append(&mut wire)
                .unwrap();
            assert!(matches!(
                parse_one_prefix(&session.state),
                Ok(ParseOutcome::Consumed)
            ));
            assert!(session.state.borrow().ingress.is_empty());
            let headers = response.source.read_header_event().await.unwrap();
            assert!(!headers.eom());
            assert_eq!(headers.headers().response_status(), Some(200));
            drop(headers);
            drive_core_callbacks(&session.state, &handle.read_wake).unwrap();

            if matches!(ending, ResponseEnding::DataWithoutFin) {
                let mut body = BufQueue::new(16);
                body.push_vec(b"x".to_vec()).unwrap();
                peer.generate(
                    &mut wire,
                    EgressFrame::Body {
                        stream,
                        body,
                        padding: None,
                        end_stream: false,
                    },
                    &mut callback,
                )
                .unwrap();
            } else {
                let mut trailers = HttpHeaders::new();
                trailers.set(b"x-trailer", b"done");
                peer.generate(
                    &mut wire,
                    EgressFrame::Trailers {
                        stream,
                        trailers: &trailers,
                    },
                    &mut callback,
                )
                .unwrap();
            }
            let length = wire.chain_length();
            let mut bytes = wire.coalesce(length).unwrap().to_vec();
            if matches!(ending, ResponseEnding::TrailerWithoutFin) {
                bytes[4] &= !0x1;
            }
            session.state.borrow_mut().ingress.push_vec(bytes).unwrap();
            let parse_error = match parse_one_prefix(&session.state) {
                Ok(ParseOutcome::Consumed) => None,
                Ok(ParseOutcome::Blocked) => panic!("complete terminal frame was not consumed"),
                Err(failure) => {
                    let code = failure.error.code();
                    handle_codec_failure(&session.state, &handle.write_wake, failure);
                    Some(code)
                }
            };
            assert!(session.state.borrow().ingress.is_empty());
            let error = if short_content_length {
                match response.source.read_body_event(4096).await {
                    Err(error) => Some(error.code()),
                    Ok(_) => panic!("invalid response produced a body event"),
                }
            } else {
                None
            };
            drop(response);

            let mut core = DetachedSessionCore::take(&session.state);
            let processed = core.core_mut().process_pending_events_for_driver().unwrap();
            core.restore();
            let updates = processed
                .updates
                .iter()
                .map(|update| {
                    assert_eq!(update.stream, stream);
                    (update.action, update.detached)
                })
                .collect();
            for update in processed.updates {
                apply_stream_update(&session.state, update, &handle.read_wake);
            }
            drive_core_callbacks(&session.state, &handle.read_wake).unwrap();
            let _ = session.run().await;
            assert!(handle.is_finished());
            (updates, error, parse_error)
        })
    })
}

#[test]
fn received_trailer_fin_does_not_reset_an_abandoned_response() {
    let (updates, error, parse_error) = response_terminal_action(ResponseEnding::TrailerFin);
    let (short_updates, short_error, short_parse_error) =
        response_terminal_action(ResponseEnding::ShortTrailerFin);
    assert_eq!(error, None);
    assert_eq!(parse_error, None);
    assert_eq!(short_error, Some(HttpErrorCode::CONTENT_LENGTH_MISMATCH));
    assert_eq!(short_parse_error, None);
    assert!(short_updates.iter().any(|(_, detached)| *detached));
    assert_eq!(updates, vec![(StreamAction::None, true)]);
}

#[test]
fn body_without_fin_still_cancels_an_abandoned_response() {
    let (updates, error, parse_error) = response_terminal_action(ResponseEnding::DataWithoutFin);
    assert_eq!(error, None);
    assert_eq!(parse_error, None);
    assert_eq!(
        updates,
        vec![(
            StreamAction::Reset {
                code: HttpErrorCode::CANCEL,
                origin: ResetOrigin::Ingress,
            },
            true,
        )]
    );
}

/// Trailers that do not end the stream fail it with PROTOCOL_ERROR, which resets it, so the
/// abandoned response detaches without a second reset.
#[test]
fn trailers_without_fin_fail_an_abandoned_response() {
    let (updates, error, parse_error) = response_terminal_action(ResponseEnding::TrailerWithoutFin);
    assert_eq!(updates, vec![(StreamAction::None, true)]);
    assert_eq!(error, None);
    assert_eq!(parse_error, Some(HttpErrorCode::PROTOCOL_ERROR));
}

#[test]
fn retained_peer_reset_ignores_later_empty_data_eom() {
    struct RetainRequest(RefCell<Option<tokio::sync::oneshot::Sender<HttpSourceHolder>>>);

    impl HttpHandler for RetainRequest {
        fn handle_request(
            &self,
            _context: HandlerSessionContext,
            request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            assert!(self.0.borrow_mut().take().unwrap().send(request).is_ok());
            Box::pin(std::future::pending())
        }
    }

    let (error, parse_error) = run_on_worker(|context| {
        Box::pin(async move {
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let (session, handle) = UniplexSession::new_http2(
                context,
                TransportDirection::Downstream,
                ready_dispatch_transport(),
                Some(Rc::new(RetainRequest(RefCell::new(Some(sender))))),
                UniplexConfig::default(),
                Http2SessionConfig::default(),
            )
            .unwrap();
            let mut peer = Http2Codec::new(TransportDirection::Upstream);
            let mut callback = NoopCodecCallback;
            let mut wire = BufQueue::new(4096);
            peer.generate(&mut wire, EgressFrame::ConnectionPreface, &mut callback)
                .unwrap();
            peer.generate_configured_settings(&mut wire, &mut callback)
                .unwrap();
            let stream = peer.create_stream();
            let mut message = HttpMessage::new();
            message.set_method(HttpMethod::Post).unwrap();
            message.set_url(b"/", false).unwrap();
            message.set_http_version(1, 1);
            message
                .headers_mut()
                .set(HttpHeaderCode::Host, b"example.test");
            message
                .headers_mut()
                .set(HttpHeaderCode::ContentLength, b"1");
            peer.generate(
                &mut wire,
                EgressFrame::Headers {
                    stream,
                    message: &message,
                    end_stream: false,
                    extra_headers: None,
                },
                &mut callback,
            )
            .unwrap();
            session
                .state
                .borrow_mut()
                .ingress
                .append(&mut wire)
                .unwrap();
            assert!(matches!(
                parse_one_prefix(&session.state),
                Ok(ParseOutcome::Consumed)
            ));
            assert!(session.state.borrow().ingress.is_empty());
            let mut request = receiver.await.unwrap();
            assert!(!request.read_header_event().await.unwrap().eom());
            drive_core_callbacks(&session.state, &handle.read_wake).unwrap();
            peer.generate(
                &mut wire,
                EgressFrame::Reset {
                    stream,
                    code: H2ErrorCode::CANCEL,
                },
                &mut callback,
            )
            .unwrap();
            session
                .state
                .borrow_mut()
                .ingress
                .append(&mut wire)
                .unwrap();
            assert!(matches!(
                parse_one_prefix(&session.state),
                Ok(ParseOutcome::Consumed)
            ));
            {
                let state = session.state.borrow();
                let row = state.core().stream(stream).unwrap();
                assert!(row.ingress_cancel_requested());
                assert!(!row.stream_source_complete());
                assert_eq!(state.ingress_meta[&stream].expected_content_length, Some(1));
            }
            peer.generate(
                &mut wire,
                EgressFrame::Body {
                    stream,
                    body: BufQueue::new(0),
                    padding: None,
                    end_stream: true,
                },
                &mut callback,
            )
            .unwrap();
            session
                .state
                .borrow_mut()
                .ingress
                .append(&mut wire)
                .unwrap();
            let parse_error = match parse_one_prefix(&session.state) {
                Ok(ParseOutcome::Consumed) => None,
                Ok(ParseOutcome::Blocked) => panic!("complete DATA frame was not consumed"),
                Err(failure) => {
                    let code = failure.error.code();
                    handle_codec_failure(&session.state, &handle.write_wake, failure);
                    Some(code)
                }
            };
            assert!(session.state.borrow().ingress.is_empty());
            let error = match request.read_body_event(4096).await {
                Err(error) => error.code(),
                Ok(_) => panic!("reset request produced a body event"),
            };
            drop(request);
            let _ = session.run().await;
            assert!(handle.is_finished());
            (error, parse_error)
        })
    });
    assert_eq!(error, HttpErrorCode::CANCEL);
    assert_eq!(parse_error, None);
}

#[test]
fn lifecycle_only_callback_work_delivers_reentrant_drain() {
    struct ReentrantObserver {
        handle: UniplexHandle,
        events: Rc<RefCell<Vec<LifecycleEvent>>>,
    }

    impl LifecycleObserver for ReentrantObserver {
        fn on_event(&self, event: LifecycleEvent) {
            self.events.borrow_mut().push(event);
            if event == (LifecycleEvent::Http2Write { bytes: 1 }) {
                self.handle
                    .initiate_drain()
                    .expect("reentrant drain succeeds");
            }
        }
    }

    run_on_worker(move |context| {
        Box::pin(async move {
            let (session, handle) = UniplexSession::new_http2(
                context,
                TransportDirection::Upstream,
                ready_dispatch_transport(),
                None,
                UniplexConfig::default(),
                Http2SessionConfig::default(),
            )
            .expect("H2 fixture starts");
            drive_core_callbacks(&session.state, &handle.read_wake).unwrap();
            assert!(!session.state.borrow().has_pending_callback_work());
            let events = Rc::new(RefCell::new(Vec::new()));
            session
                .add_lifecycle_observer(Rc::new(ReentrantObserver {
                    handle: handle.clone(),
                    events: Rc::clone(&events),
                }))
                .unwrap();
            session
                .state
                .borrow()
                .core()
                .observe_lifecycle(LifecycleEvent::Http2Write { bytes: 1 });
            assert!(!session.state.borrow().core().has_pending_events());
            assert!(events.borrow().is_empty());
            drive_core_callbacks(&session.state, &handle.read_wake).unwrap();
            assert_eq!(
                events.borrow().as_slice(),
                &[
                    LifecycleEvent::Http2Write { bytes: 1 },
                    LifecycleEvent::DrainStarted(DrainTrigger::Local),
                ]
            );
            session.run().await.expect("drained session finishes");
        })
    });
}

struct ReleasedBodySource {
    header: Option<HttpHeaderEvent>,
    release: Rc<tokio::sync::Notify>,
    served: Rc<Cell<bool>>,
}

impl HttpSource for ReleasedBodySource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let header = self.header.take().expect("one request header");
        Box::pin(std::future::ready(Ok(header)))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let release = Rc::clone(&self.release);
        let served = Rc::clone(&self.served);
        Box::pin(async move {
            release.notified().await;
            served.set(true);
            Ok(HttpBodyEvent::body(
                BufQueue::from_vec(b"payload".to_vec(), 7).expect("fixture body fits"),
                true,
            ))
        })
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

fn released_body_request(
    release: &Rc<tokio::sync::Notify>,
    served: &Rc<Cell<bool>>,
) -> HttpSourceHolder {
    let mut message = HttpMessage::new();
    message.set_method(HttpMethod::Post).unwrap();
    message.set_url(b"/", false).unwrap();
    message.set_http_version(1, 1);
    message.headers_mut().set(b"host", b"example.test");
    HttpSourceHolder::new(Box::new(ReleasedBodySource {
        header: Some(HttpHeaderEvent::new(message, false).unwrap()),
        release: Rc::clone(release),
        served: Rc::clone(served),
    }))
}

fn wrote_payload(writes: &[Vec<u8>]) -> bool {
    writes
        .iter()
        .any(|write| write.windows(7).any(|window| window == b"payload"))
}

#[test]
fn http2_source_readiness_still_wakes_the_writer() {
    use super::shutdown_progress_tests as writer_waits;

    for pending_write in [false, true] {
        writer_waits::run_on_worker(move |context| {
            Box::pin(async move {
                let (transport, state, _) = writer_waits::ProbeTransport::new([]);
                let write_gate = Rc::new(tokio::sync::Notify::new());
                if pending_write {
                    state.borrow_mut().write_gate = Some(Rc::clone(&write_gate));
                }
                let (session, handle) = writer_waits::upstream_http2(&context, transport);
                let release = Rc::new(tokio::sync::Notify::new());
                let served = Rc::new(Cell::new(false));
                let mut run = session.run();
                let send = handle.send_request(
                    released_body_request(&release, &served),
                    CancellationToken::never(),
                );
                let response = tokio::select! {
                    biased;
                    _ = &mut run => panic!("the session ended before the request"),
                    response = send => response.expect("request starts"),
                };
                let control = async {
                    writer_waits::settle().await;
                    assert!(!served.get());
                    assert_eq!(state.borrow().write_pending, pending_write);
                    release.notify_one();
                    writer_waits::settle().await;
                    assert!(served.get(), "source readiness woke the writer");
                    if pending_write {
                        assert!(
                            state.borrow().write_pending,
                            "write_one_batch serviced the source while its write was pending"
                        );
                        write_gate.notify_one();
                    }
                    writer_waits::until(|| wrote_payload(&state.borrow().writes)).await;
                };
                tokio::select! {
                    biased;
                    _ = &mut run => panic!("the session ended before its body"),
                    () = control => {}
                }
                drop(run);
                writer_waits::settle().await;
                drop(response);
            })
        });
    }
}

struct FlowWindowEvents(Rc<RefCell<Vec<LifecycleEvent>>>);

impl LifecycleObserver for FlowWindowEvents {
    fn on_event(&self, event: LifecycleEvent) {
        if matches!(
            event,
            LifecycleEvent::Http2FlowWindowOpen | LifecycleEvent::Http2FlowWindowClosed
        ) {
            self.0.borrow_mut().push(event);
        }
    }
}

#[test]
fn http2_connection_window_transition_still_flushes() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle) = UniplexSession::new_http2(
                context.clone(),
                TransportDirection::Upstream,
                ready_dispatch_transport(),
                None,
                UniplexConfig::default(),
                Http2SessionConfig::default(),
            )
            .expect("H2 fixture starts");
            let events = Rc::new(RefCell::new(Vec::new()));
            session
                .add_lifecycle_observer(Rc::new(FlowWindowEvents(Rc::clone(&events))))
                .unwrap();
            let body = BufQueue::from_vec(vec![b'x'; 16], 16).expect("fixture body fits");
            let mut fixed = HttpFixedSource::make_fixed_request(b"/", HttpMethod::Post, Some(body))
                .expect("fixture request is valid");
            fixed
                .message_mut()
                .expect("fixed request has a header")
                .headers_mut()
                .set(b"host", b"example.test");
            let mut source = HttpSourceHolder::new(Box::new(fixed));
            let headers = source.read_header_event().await.unwrap();
            let prepared = UniplexHandle::prepare_request_with_headers(
                headers,
                source,
                handle.reserve_request().unwrap(),
            );
            let response = handle.send_prepared_request(prepared).unwrap();
            let stream = response.stream;
            let transition = session
                .state
                .borrow()
                .connection_window_transition
                .clone()
                .expect("HTTP/2 tracks connection window transitions");

            // Exhausting the connection window publishes a closed transition.
            {
                let mut state = session.state.borrow_mut();
                let credit = state.codec.connection_send_credit() as usize;
                let mut encoded = BufQueue::new(credit + 1024);
                state
                    .codec
                    .generate(
                        &mut encoded,
                        EgressFrame::Body {
                            stream,
                            body: BufQueue::from_vec(vec![0; credit], credit)
                                .expect("credit-sized body fits"),
                            padding: None,
                            end_stream: false,
                        },
                        &mut NoopCodecCallback,
                    )
                    .expect("connection credit is consumed");
                assert_eq!(state.codec.connection_send_credit(), 0);
            }
            assert_eq!(transition.get(), Some(false));
            flush_http2_flow_transition(&session.state);
            assert_eq!(*events.borrow(), [LifecycleEvent::Http2FlowWindowClosed]);
            assert_eq!(transition.get(), None);
            flush_http2_flow_transition(&session.state);
            assert_eq!(events.borrow().len(), 1, "a transition flushes once");

            transition.set(Some(true));
            flush_http2_flow_transition(&session.state);
            assert_eq!(
                *events.borrow(),
                [
                    LifecycleEvent::Http2FlowWindowClosed,
                    LifecycleEvent::Http2FlowWindowOpen
                ]
            );
            drop(response);
            drop(session);
        })
    });
}

#[test]
fn http1_driver_state_holds_no_http2_only_state() {
    assert_eq!(
        std::mem::size_of::<Option<Box<Http2DriverState>>>(),
        std::mem::size_of::<usize>()
    );
    run_on_worker(|context| {
        Box::pin(async move {
            let (http1, _http1_handle) = UniplexSession::new(
                context.clone(),
                TransportDirection::Upstream,
                ready_dispatch_transport(),
                None,
                UniplexConfig::default(),
            )
            .unwrap();
            {
                let state = http1.state.borrow();
                assert!(state.http2.is_none());
                assert!(state.connection_window_transition.is_none());
            }
            flush_http2_flow_transition(&http1.state);

            let (http2, _http2_handle) = UniplexSession::new_http2(
                context.clone(),
                TransportDirection::Upstream,
                ready_dispatch_transport(),
                None,
                UniplexConfig::default(),
                Http2SessionConfig::default(),
            )
            .unwrap();
            let state = http2.state.borrow();
            let driver = state.http2.as_ref().expect("HTTP/2 driver state");
            assert!(state.connection_window_transition.is_some());
            let source_ready = Arc::clone(&driver.body_source_publications.source_ready);
            assert!(Arc::strong_count(&source_ready) >= 2);
            drop(state);
            drop(http1);
            drop(http2);
        })
    });
}
