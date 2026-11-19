/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::future::Future;
use std::future::poll_fn;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;

use bytes::Bytes;
use futures::FutureExt;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::byte_event::ByteEventType;
use roxygen_core::byte_event::HttpByteEvent;
use roxygen_core::byte_event::HttpByteEventCallback;
use roxygen_core::byte_event::HttpByteEventRegistration;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::http::HttpMethod;
use roxygen_core::http::TransportDirection;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::cancellation::CancellationWakeRegistration;
use roxygen_runtime::driver::WorkerDriver;
use roxygen_runtime::host::Host;
use roxygen_runtime::wait::CancellableBaton;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerState;
use roxygen_transport::byte_event::AsyncSocketByteEventObserver;
use roxygen_transport::byte_event::ByteEventWriteFlags;
use roxygen_transport::byte_stream::ByteEventCapability;
use roxygen_transport::byte_stream::ByteEventRegistrationError;
use roxygen_transport::byte_stream::LocalTransportFuture;
use roxygen_transport::byte_stream::TransportWriteError;
use roxygen_transport::byte_stream::WriteInfo;
use tokio::runtime::Builder;
use tokio::sync::Notify;
use tokio::task::LocalSet;

use super::WriteBatchContext;
use super::WriteWakeReason;
use super::WriterWaitHooks;
use super::catch_future_unwind;
use super::collect_close_wait_panic;
use super::h1_idle_wait;
use super::poll_byte_event_timeouts;
use super::write_one_batch;
use super::writer_probe;
use crate::http2::Http2SessionConfig;
use crate::uniplex::UniplexConfig;
use crate::uniplex::UniplexHandle;
use crate::uniplex::UniplexProtocolConfig;
use crate::uniplex::UniplexRead;
use crate::uniplex::UniplexRunOwner;
use crate::uniplex::UniplexRunSummary;
use crate::uniplex::UniplexSession;
use crate::uniplex::UniplexTransport;

#[test]
fn closed_observer_error_is_not_reported_as_ready_work() {
    let runtime = Builder::new_current_thread().enable_time().build().unwrap();
    LocalSet::new().block_on(&runtime, async {
        let (worker, driver) = WorkerDriver::new(4).unwrap();
        let call = worker
            .call_local(|context| async move {
                let observer = Rc::new(AsyncSocketByteEventObserver::new(&context).unwrap());
                assert!(context.handle().begin_shutdown());
                let expected = observer
                    .poll_timeouts(&context, &mut Context::from_waker(Waker::noop()))
                    .expect_err("observer cannot poll a closing worker");
                let actual = poll_byte_event_timeouts(observer, context)
                    .await
                    .expect_err("the writer must receive the observer error");
                assert_eq!(actual.code(), expected.code());
                assert_eq!(actual.message(), expected.message());
            })
            .unwrap();
        let (_, result) = tokio::join!(driver, call);
        result.expect("observer error was handled without suspending the command");
        assert_eq!(worker.state(), WorkerState::Closed);
    });
}

const RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";

/// Runs `test` on a Tokio-hosted worker, whose tasks are polled only when woken,
/// and returns any panic that the worker driver resumes.
pub(crate) fn run_hosted<T: Send + 'static>(
    test: impl FnOnce(WorkerContext) -> Pin<Box<dyn Future<Output = T>>> + Send + 'static,
) -> (Option<T>, Option<Box<dyn std::any::Any + Send>>) {
    let runtime = Builder::new_current_thread().enable_time().build().unwrap();
    LocalSet::new().block_on(&runtime, async move {
        let (worker, driver) = WorkerDriver::with_host(16, Host::tokio()).unwrap();
        let call = worker
            .call_local(move |context| {
                let worker = context.handle().clone();
                async move {
                    let result = test(context).await;
                    worker.begin_shutdown();
                    result
                }
            })
            .unwrap();
        let (driver, result) = tokio::join!(AssertUnwindSafe(driver).catch_unwind(), call);
        (result.ok(), driver.err())
    })
}

pub(crate) fn run_on_worker<T: Send + 'static>(
    test: impl FnOnce(WorkerContext) -> Pin<Box<dyn Future<Output = T>>> + Send + 'static,
) -> T {
    match run_hosted(test) {
        (Some(result), None) => result,
        (_, Some(payload)) => std::panic::resume_unwind(payload),
        (None, None) => panic!("the hosted worker call did not complete"),
    }
}

type EventLog = Arc<Mutex<Vec<&'static str>>>;

fn record(log: &EventLog, event: &'static str) {
    log.lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(event);
}

fn recorded(log: &EventLog) -> Vec<&'static str> {
    log.lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

pub(crate) enum ProbeRead {
    /// Waits for `release` alone, ignoring cancellation, then returns `data`.
    Release(Rc<Notify>, &'static [u8], bool),
    /// Ends only when its cancellation token is requested.
    Cancelled,
    /// Returns EOF after yielding the task the given number of times.
    EofAfter(usize),
    /// Fails after yielding the task the given number of times.
    ErrorAfter(usize),
    /// Registers a waker that panics on wake, then ends on cancellation.
    PanickingCancellationWaker,
    /// As `PanickingCancellationWaker`, panicking with a [`LoggedPayload`].
    LoggedPanicCancellationWaker,
}

pub(crate) struct ProbeState {
    reads: VecDeque<ProbeRead>,
    pub(crate) writes: Vec<Vec<u8>>,
    pub(crate) write_gate: Option<Rc<Notify>>,
    pub(crate) write_pending: bool,
    write_polls: usize,
    write_delay: Duration,
    on_write: Option<Box<dyn FnMut(usize)>>,
    transport_events: bool,
    registrations: Vec<HttpByteEventRegistration>,
    log: EventLog,
}

pub(crate) struct ProbeTransport {
    state: Rc<RefCell<ProbeState>>,
}

impl ProbeTransport {
    pub(crate) fn new(
        reads: impl IntoIterator<Item = ProbeRead>,
    ) -> (Rc<dyn UniplexTransport>, Rc<RefCell<ProbeState>>, EventLog) {
        let log = EventLog::default();
        let state = Rc::new(RefCell::new(ProbeState {
            reads: reads.into_iter().collect(),
            writes: Vec::new(),
            write_gate: None,
            write_pending: false,
            write_polls: 0,
            write_delay: Duration::ZERO,
            on_write: None,
            transport_events: false,
            registrations: Vec::new(),
            log: Arc::clone(&log),
        }));
        (
            Rc::new(Self {
                state: Rc::clone(&state),
            }),
            state,
            log,
        )
    }
}

struct OperationDrop {
    log: EventLog,
    label: &'static str,
}

impl Drop for OperationDrop {
    fn drop(&mut self) {
        record(&self.log, self.label);
    }
}

struct PanickingWake;

impl Wake for PanickingWake {
    fn wake(self: Arc<Self>) {
        panic!("cancellation waker panic");
    }
}

/// A panic payload that records its drop.
struct LoggedPayload(EventLog);

impl Drop for LoggedPayload {
    fn drop(&mut self) {
        record(&self.0, "payload-drop");
    }
}

struct LoggedPanicWake(EventLog);

impl Wake for LoggedPanicWake {
    fn wake(self: Arc<Self>) {
        std::panic::panic_any(LoggedPayload(Arc::clone(&self.0)));
    }
}

/// Registers `waker` on `cancellation`, then ends once cancellation is
/// requested, logging each poll.
async fn read_until_cancelled_after_registering(
    cancellation: CancellationToken,
    waker: Waker,
    log: EventLog,
) -> Result<UniplexRead, HttpError> {
    let mut registration = cancellation.wake_registration();
    assert!(!registration.update_waker(&waker));
    poll_fn(|_| {
        if cancellation.is_cancellation_requested() {
            record(&log, "read-poll-cancelled");
            Poll::Ready(())
        } else {
            record(&log, "read-poll");
            Poll::Pending
        }
    })
    .await;
    drop(registration);
    Err(cancelled_read())
}

fn cancelled_read() -> HttpError {
    HttpError::with_message(HttpErrorCode::CORO_CANCELLED, b"probe read cancelled")
}

impl UniplexTransport for ProbeTransport {
    fn read(
        &self,
        _context: &WorkerContext,
        _maximum: usize,
        _allocation_size: usize,
        _timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        let action = self
            .state
            .borrow_mut()
            .reads
            .pop_front()
            .unwrap_or(ProbeRead::Cancelled);
        let log = Arc::clone(&self.state.borrow().log);
        let guard = OperationDrop {
            log: Arc::clone(&log),
            label: "read-op-drop",
        };
        Box::pin(async move {
            let _guard = guard;
            match action {
                ProbeRead::Release(release, data, eof) => {
                    release.notified().await;
                    let data = BufQueue::from_bytes(Bytes::from_static(data), data.len())
                        .expect("bounded probe read");
                    Ok(UniplexRead::new(data, eof))
                }
                ProbeRead::Cancelled => {
                    let _ = CancellableBaton::new().wait(cancellation).await;
                    Err(cancelled_read())
                }
                ProbeRead::EofAfter(yields) => {
                    for _ in 0..yields {
                        tokio::task::yield_now().await;
                    }
                    Ok(UniplexRead::new(BufQueue::new(0), true))
                }
                ProbeRead::ErrorAfter(yields) => {
                    for _ in 0..yields {
                        tokio::task::yield_now().await;
                    }
                    Err(HttpError::with_message(
                        HttpErrorCode::TRANSPORT_READ_ERROR,
                        b"probe read failure",
                    ))
                }
                ProbeRead::PanickingCancellationWaker => {
                    let waker = Waker::from(Arc::new(PanickingWake));
                    read_until_cancelled_after_registering(cancellation, waker, log).await
                }
                ProbeRead::LoggedPanicCancellationWaker => {
                    let waker = Waker::from(Arc::new(LoggedPanicWake(Arc::clone(&log))));
                    read_until_cancelled_after_registering(cancellation, waker, log).await
                }
            }
        })
    }

    fn write(
        &self,
        _context: &WorkerContext,
        data: BufQueue,
        _flags: ByteEventWriteFlags,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        let length = data.chain_length();
        let bytes = data.coalesce(length).expect("bounded probe write").to_vec();
        let index = {
            let mut state = self.state.borrow_mut();
            state.writes.push(bytes);
            state.writes.len()
        };
        let hook = self.state.borrow_mut().on_write.take();
        if let Some(mut hook) = hook {
            hook(index);
            self.state.borrow_mut().on_write = Some(hook);
        }
        let delay = self.state.borrow().write_delay;
        if !delay.is_zero() {
            std::thread::sleep(delay);
        }
        let gate = self.state.borrow_mut().write_gate.take();
        let guard = OperationDrop {
            log: Arc::clone(&self.state.borrow().log),
            label: "write-op-drop",
        };
        let state = Rc::clone(&self.state);
        let mut write = Box::pin(async move {
            let _guard = guard;
            if let Some(gate) = gate {
                state.borrow_mut().write_pending = true;
                gate.notified().await;
                state.borrow_mut().write_pending = false;
            }
            Ok(WriteInfo {
                bytes_written: length,
            })
        });
        let state = Rc::clone(&self.state);
        Box::pin(poll_fn(move |context| {
            state.borrow_mut().write_polls += 1;
            write.as_mut().poll(context)
        }))
    }

    fn shutdown_write(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        record(&self.state.borrow().log, "shutdown-write");
        Ok(())
    }

    fn close(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        Ok(())
    }

    fn close_with_reset(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        record(&self.state.borrow().log, "reset");
        Ok(())
    }

    fn byte_event_capability(
        &self,
        _context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError> {
        Ok(if self.state.borrow().transport_events {
            ByteEventCapability::TransportEvents
        } else {
            ByteEventCapability::Unsupported
        })
    }

    fn register_byte_event(
        &self,
        _context: &WorkerContext,
        registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError> {
        let mut state = self.state.borrow_mut();
        if !state.transport_events {
            return Err(ByteEventRegistrationError::Unsupported);
        }
        state
            .registrations
            .extend(registration.into_event_registrations());
        Ok(())
    }
}

struct ScriptedRequest {
    header: Option<HttpHeaderEvent>,
    bodies: VecDeque<HttpBodyEvent>,
}

impl HttpSource for ScriptedRequest {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(self.header.take().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"no header")
        })))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(std::future::ready(self.bodies.pop_front().ok_or_else(
            || HttpError::with_message(HttpErrorCode::DROPPED, b"no body"),
        )))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

fn request(
    method: HttpMethod,
    bodies: impl IntoIterator<Item = HttpBodyEvent>,
    header_events: Option<(u8, &Rc<ByteEventLog>)>,
) -> HttpSourceHolder {
    let bodies: VecDeque<_> = bodies.into_iter().collect();
    let mut message = HttpMessage::new();
    message.set_method(method).unwrap();
    message.set_url(b"/", false).unwrap();
    message.set_http_version(1, 1);
    message.headers_mut().set(b"host", b"example.test");
    let mut header = HttpHeaderEvent::new(message, bodies.is_empty()).unwrap();
    if let Some((events, log)) = header_events {
        header
            .byte_event_registrations_mut()
            .push(HttpByteEventRegistration::new(None, events, log));
    }
    HttpSourceHolder::new(Box::new(ScriptedRequest {
        header: Some(header),
        bodies,
    }))
}

fn body(bytes: &'static [u8], eom: bool) -> HttpBodyEvent {
    HttpBodyEvent::body(
        BufQueue::from_bytes(Bytes::from_static(bytes), bytes.len()).unwrap(),
        eom,
    )
}

#[derive(Default)]
struct ByteEventLog {
    fired: Cell<usize>,
    cancelled: RefCell<Vec<HttpErrorCode>>,
    writer_at_cancel: RefCell<Vec<writer_probe::WriterCounts>>,
}

impl HttpByteEventCallback for ByteEventLog {
    fn on_byte_event(&self, _event: HttpByteEvent) {
        self.fired.set(self.fired.get() + 1);
    }

    fn on_byte_event_cancelled(&self, _event: HttpByteEvent, error: HttpError) {
        self.cancelled.borrow_mut().push(error.code());
        self.writer_at_cancel
            .borrow_mut()
            .push(writer_probe::snapshot());
    }
}

pub(crate) async fn settle() {
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
}

pub(crate) async fn until(mut ready: impl FnMut() -> bool) {
    for _ in 0..20_000 {
        if ready() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("session did not reach the expected state");
}

pub(crate) fn upstream(
    context: &WorkerContext,
    transport: Rc<dyn UniplexTransport>,
) -> (UniplexSession, UniplexHandle) {
    UniplexSession::new(
        context.clone(),
        TransportDirection::Upstream,
        transport,
        None,
        UniplexConfig::default(),
    )
    .unwrap()
}

pub(crate) fn upstream_http2(
    context: &WorkerContext,
    transport: Rc<dyn UniplexTransport>,
) -> (UniplexSession, UniplexHandle) {
    UniplexSession::new_http2(
        context.clone(),
        TransportDirection::Upstream,
        transport,
        None,
        UniplexConfig::default(),
        Http2SessionConfig::default(),
    )
    .unwrap()
}

#[test]
fn idle_writer_exits_when_write_cancellation_is_its_only_wake() {
    run_on_worker(|context| {
        Box::pin(async move {
            let release = Rc::new(Notify::new());
            let (transport, _, log) =
                ProbeTransport::new([ProbeRead::Release(Rc::clone(&release), b"", true)]);
            let (session, handle) = upstream(&context, transport);
            let run = session.run();
            let control = async {
                settle().await;
                assert!(!recorded(&log).contains(&"shutdown-write"));
                assert!(handle.request_write_cancellation_for_test());
                settle().await;
                assert!(
                    recorded(&log).contains(&"shutdown-write"),
                    "the writer left its idle wait on its cancellation registration alone"
                );
                release.notify_one();
            };
            let (result, ()) = tokio::join!(run, control);
            result.unwrap();
        })
    });
}

#[test]
fn callback_pushed_before_the_idle_wait_consumes_its_permit_once() {
    run_on_worker(|context| {
        Box::pin(async move {
            writer_probe::reset();
            let (transport, _, _) = ProbeTransport::new([]);
            let (session, handle) = upstream(&context, transport);
            let callbacks = handle.callback_queue_for_test();
            let pushed = Rc::clone(&callbacks);
            handle.set_writer_wait_hooks_for_test(WriterWaitHooks {
                before: Some(Box::new(move || pushed.wake_retirement())),
                after: None,
            });
            let run = session.run();
            let control = async {
                settle().await;
                let counts = writer_probe::snapshot();
                assert_eq!(counts.idle_wakes, 1, "the pushed callback woke one wait");
                assert_eq!(counts.idle_waits, 2, "the next wait parked");
                assert!(!callbacks.has_pending());
                assert!(!callbacks.has_stored_wake_for_test());
                handle.initiate_drain().unwrap();
            };
            let (result, ()) = tokio::join!(run, control);
            result.unwrap();
        })
    });
}

#[test]
fn idle_writer_keeps_waiting_after_the_reader_drains_the_callbacks() {
    run_on_worker(|context| {
        Box::pin(async move {
            writer_probe::reset();
            let (transport, _, _) = ProbeTransport::new([]);
            let (session, handle) = upstream(&context, transport);
            let callbacks = handle.callback_queue_for_test();
            let run = session.run();
            let control = async {
                settle().await;
                let before = writer_probe::snapshot();
                callbacks.push(crate::stream_state::StreamCallbackEvent::WindowOpen(
                    roxygen_core::ids::StreamId::new(1),
                ));
                assert!(callbacks.pop_front().is_some());
                settle().await;
                let after = writer_probe::snapshot();
                assert_eq!(after.idle_wakes, before.idle_wakes);
                assert_eq!(after.idle_waits, before.idle_waits);
                assert!(!callbacks.has_stored_wake_for_test());

                callbacks.wake_retirement();
                settle().await;
                let woken = writer_probe::snapshot();
                assert_eq!(woken.idle_wakes, before.idle_wakes + 1);
                assert_eq!(woken.idle_waits, before.idle_waits + 1);
                handle.initiate_drain().unwrap();
            };
            let (result, ()) = tokio::join!(run, control);
            result.unwrap();
        })
    });
}

#[test]
fn callback_during_a_pending_transport_write_runs_in_write_one_batch() {
    run_on_worker(|context| {
        Box::pin(async move {
            writer_probe::reset();
            let read_release = Rc::new(Notify::new());
            let (transport, state, _) = ProbeTransport::new([ProbeRead::Release(
                Rc::clone(&read_release),
                RESPONSE,
                false,
            )]);
            let write_gate = Rc::new(Notify::new());
            state.borrow_mut().write_gate = Some(Rc::clone(&write_gate));
            let (session, handle) = upstream(&context, transport);
            let callbacks = handle.callback_queue_for_test();
            let run = session.run();
            let control = async {
                let mut response = handle
                    .send_request(
                        request(HttpMethod::Get, [], None),
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap();
                until(|| state.borrow().write_pending).await;
                settle().await;
                let before = writer_probe::snapshot();
                callbacks.wake_retirement();
                settle().await;
                assert!(
                    state.borrow().write_pending,
                    "the transport write is still pending"
                );
                assert!(
                    !callbacks.has_pending(),
                    "write_one_batch serviced the callback during the write"
                );
                assert!(writer_probe::snapshot().batch_selects > before.batch_selects);
                write_gate.notify_one();
                until(|| !state.borrow().write_pending).await;
                read_release.notify_one();
                assert!(response.source.read_header_event().await.unwrap().eom());
                drop(response);
                handle.initiate_drain().unwrap();
            };
            let (result, ()) = tokio::join!(run, control);
            result.unwrap();
        })
    });
}

#[test]
fn post_select_callback_push_does_not_self_wake_the_session_task() {
    for http2 in [false, true] {
        run_on_worker(move |context| {
            Box::pin(async move {
                writer_probe::reset();
                let (transport, _, _) = ProbeTransport::new([]);
                let (session, handle) = if http2 {
                    upstream_http2(&context, transport)
                } else {
                    upstream(&context, transport)
                };
                let callbacks = handle.callback_queue_for_test();
                let run = session.run();
                let control = async {
                    settle().await;
                    let stored = Rc::new(Cell::new(None));
                    let observed = Rc::clone(&stored);
                    let pushed = Rc::clone(&callbacks);
                    handle.set_writer_wait_hooks_for_test(WriterWaitHooks {
                        before: None,
                        after: Some(Box::new(move || {
                            pushed.wake_retirement();
                            observed.set(Some(pushed.has_stored_wake_for_test()));
                        })),
                    });
                    let before = writer_probe::snapshot();
                    handle.wake_writer_for_test();
                    settle().await;
                    assert_eq!(
                        stored.get(),
                        Some(true),
                        "no callback waiter outlived the completed wait"
                    );
                    let after = writer_probe::snapshot();
                    assert_eq!(after.idle_wakes, before.idle_wakes + 1);
                    assert_eq!(after.idle_waits, before.idle_waits + 1);
                    handle.initiate_drain().unwrap();
                };
                let (result, ()) = tokio::join!(run, control);
                result.unwrap();
            })
        });
    }
}

#[test]
fn h1_byte_event_timeout_fires_while_the_writer_is_idle() {
    run_on_worker(|context| {
        Box::pin(async move {
            writer_probe::reset();
            let read_release = Rc::new(Notify::new());
            let (transport, state, _) = ProbeTransport::new([ProbeRead::Release(
                Rc::clone(&read_release),
                RESPONSE,
                false,
            )]);
            state.borrow_mut().transport_events = true;
            let (session, handle) = upstream(&context, transport);
            session
                .set_byte_event_timeout(Duration::from_millis(20))
                .unwrap();
            let events = Rc::new(ByteEventLog::default());
            let run = session.run();
            let control = async {
                let mut response = handle
                    .send_request(
                        request(
                            HttpMethod::Get,
                            [],
                            Some((ByteEventType::NicTx.bit(), &events)),
                        ),
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap();
                tokio::time::timeout(Duration::from_secs(5), async {
                    while events.cancelled.borrow().is_empty() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("the timeout fires while the writer is idle");
                assert_eq!(*events.cancelled.borrow(), [HttpErrorCode::READ_TIMEOUT]);
                let at_cancel = events.writer_at_cancel.borrow()[0];
                assert_eq!(
                    at_cancel.idle_waits,
                    at_cancel.idle_wakes + 1,
                    "the writer was inside an idle wait"
                );
                read_release.notify_one();
                assert!(response.source.read_header_event().await.unwrap().eom());
                drop(response);
                handle.initiate_drain().unwrap();
            };
            let (result, ()) = tokio::join!(run, control);
            result.unwrap();
            assert_eq!(events.fired.get(), 0);
            assert_eq!(events.cancelled.borrow().len(), 1);
        })
    });
}

#[test]
fn h1_byte_event_timeout_during_first_poll_writes_fires_by_the_next_wait() {
    run_on_worker(|context| {
        Box::pin(async move {
            writer_probe::reset();
            let read_release = Rc::new(Notify::new());
            let (transport, state, _) = ProbeTransport::new([ProbeRead::Release(
                Rc::clone(&read_release),
                RESPONSE,
                false,
            )]);
            let writes_at = Rc::new(RefCell::new(Vec::new()));
            {
                let mut state = state.borrow_mut();
                state.transport_events = true;
                state.write_delay = Duration::from_millis(5);
                let writes_at = Rc::clone(&writes_at);
                state.on_write = Some(Box::new(move |_| {
                    writes_at.borrow_mut().push(writer_probe::snapshot());
                }));
            }
            let (session, handle) = upstream(&context, transport);
            session
                .set_byte_event_timeout(Duration::from_millis(1))
                .unwrap();
            let events = Rc::new(ByteEventLog::default());
            let body_events = Rc::new(ByteEventLog::default());
            let mut first = body(b"first", false);
            first
                .byte_event_registrations_mut()
                .push(HttpByteEventRegistration::new(
                    None,
                    ByteEventType::NicTx.bit(),
                    &body_events,
                ));
            let run = session.run();
            let control = async {
                let mut response = handle
                    .send_request(
                        request(
                            HttpMethod::Post,
                            [first, body(b"second", true)],
                            Some((ByteEventType::NicTx.bit(), &events)),
                        ),
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap();
                tokio::time::timeout(Duration::from_secs(5), async {
                    while events.cancelled.borrow().is_empty()
                        || body_events.cancelled.borrow().is_empty()
                    {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("deferred timeouts fire");
                read_release.notify_one();
                assert!(response.source.read_header_event().await.unwrap().eom());
                drop(response);
                handle.initiate_drain().unwrap();
            };
            let (result, ()) = tokio::join!(run, control);
            result.unwrap();
            let writes_at = writes_at.borrow();
            assert!(writes_at.len() >= 2, "the request took several writes");
            let last_write = writes_at[writes_at.len() - 1];
            for log in [&events, &body_events] {
                assert_eq!(*log.cancelled.borrow(), [HttpErrorCode::READ_TIMEOUT]);
                assert_eq!(log.fired.get(), 0);
                let at_cancel = log.writer_at_cancel.borrow()[0];
                assert!(
                    at_cancel.idle_waits == at_cancel.idle_wakes + 1
                        || at_cancel.idle_waits == last_write.idle_waits,
                    "the deferred timeout fired from an idle or source wait: \
                     {at_cancel:?} {last_write:?}"
                );
            }
        })
    });
}

#[test]
fn first_poll_write_still_runs_pending_callbacks_before_the_next_write() {
    run_on_worker(|context| {
        Box::pin(async move {
            writer_probe::reset();
            let read_release = Rc::new(Notify::new());
            let (transport, state, _) = ProbeTransport::new([ProbeRead::Release(
                Rc::clone(&read_release),
                RESPONSE,
                false,
            )]);
            let (session, handle) = UniplexSession::new(
                context.clone(),
                TransportDirection::Upstream,
                transport,
                None,
                UniplexConfig {
                    maximum_write_size: 16,
                    ..UniplexConfig::default()
                },
            )
            .unwrap();
            let callbacks = handle.callback_queue_for_test();
            let pending_at_write = Rc::new(RefCell::new(Vec::new()));
            {
                let callbacks = Rc::clone(&callbacks);
                let pending_at_write = Rc::clone(&pending_at_write);
                state.borrow_mut().on_write = Some(Box::new(move |index| {
                    pending_at_write.borrow_mut().push(callbacks.has_pending());
                    if index == 1 {
                        callbacks.wake_retirement();
                    }
                }));
            }
            let run = session.run();
            let control = async {
                let mut response = handle
                    .send_request(
                        request(
                            HttpMethod::Post,
                            [body(b"first", false), body(b"second", true)],
                            None,
                        ),
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap();
                until(|| state.borrow().writes.len() >= 2).await;
                read_release.notify_one();
                assert!(response.source.read_header_event().await.unwrap().eom());
                drop(response);
                handle.initiate_drain().unwrap();
            };
            let (result, ()) = tokio::join!(run, control);
            result.unwrap();
            let pending_at_write = pending_at_write.borrow();
            assert!(pending_at_write.len() >= 2);
            assert!(
                !pending_at_write[1],
                "callbacks pushed during the first write ran before the second"
            );
            let state = state.borrow();
            assert_eq!(
                state.write_polls,
                state.writes.len(),
                "every write completed on its first poll"
            );
        })
    });
}

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl WakeCount {
    fn get(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// One poll made with a counting waker, and the wakes that waker received
/// during the poll and by the task's next turn.
struct FirstPoll<T> {
    poll: Poll<T>,
    synchronous_wakes: usize,
    wakes_by_next_turn: usize,
}

/// Polls `future` once with a counting waker, after spending the task's coop
/// budget when `spend_budget`, then yields the task to the scheduler once.
async fn first_poll<F: Future>(future: Pin<&mut F>, spend_budget: bool) -> FirstPoll<F::Output> {
    let wakes = Arc::new(WakeCount::default());
    let waker = Waker::from(Arc::clone(&wakes));
    if spend_budget {
        while tokio::task::coop::has_budget_remaining() {
            tokio::task::consume_budget().await;
        }
    }
    let poll = future.poll(&mut Context::from_waker(&waker));
    let synchronous_wakes = wakes.get();
    tokio::task::yield_now().await;
    FirstPoll {
        poll,
        synchronous_wakes,
        wakes_by_next_turn: wakes.get(),
    }
}

/// One HTTP/1.1 `write_one_batch` of five staged bytes: its first poll, the
/// transport write polls during that poll and in total, and the writer counts
/// around the batch.
struct H1BatchRun {
    first: FirstPoll<Result<(), HttpError>>,
    write_polls_in_first_poll: usize,
    write_polls: usize,
    before: writer_probe::WriterCounts,
    after: writer_probe::WriterCounts,
}

/// Runs one HTTP/1.1 `write_one_batch` to completion on a fresh upstream
/// session, making its first poll through [`first_poll`].
async fn run_h1_write_batch(context: &WorkerContext, spend_budget: bool) -> H1BatchRun {
    let (transport, transport_state, _) = ProbeTransport::new([]);
    let (session, handle) = upstream(context, transport);
    let state = session.state.clone();
    let observer = session.observer.upgrade().unwrap();
    state
        .borrow_mut()
        .egress
        .append(&mut BufQueue::from_bytes(Bytes::from_static(b"bytes"), 5).unwrap())
        .unwrap();
    let erased: Rc<dyn UniplexTransport> = Rc::new(ProbeTransport {
        state: Rc::clone(&transport_state),
    });
    let mut cursor = None;
    let before = writer_probe::snapshot();
    let (first, write_polls_in_first_poll) = {
        let mut batch = pin!(write_one_batch(
            WriteBatchContext {
                state: &state,
                observer: &observer,
                context,
                read_wake: &handle.shared.read_wake,
                write_wake: &handle.shared.write_wake,
                body_source_cursor: &mut cursor,
            },
            &erased,
            UniplexConfig::default(),
            CancellationToken::never(),
        ));
        let first = first_poll(batch.as_mut(), spend_budget).await;
        let write_polls_in_first_poll = transport_state.borrow().write_polls;
        if first.poll.is_pending() {
            tokio::time::timeout(Duration::from_secs(5), batch.as_mut())
                .await
                .expect("the deferred wake repolls the batch")
                .unwrap();
        }
        (first, write_polls_in_first_poll)
    };
    let after = writer_probe::snapshot();
    let write_polls = transport_state.borrow().write_polls;
    drop(session);
    drop(handle);
    H1BatchRun {
        first,
        write_polls_in_first_poll,
        write_polls,
        before,
        after,
    }
}

#[test]
fn h1_idle_wait_and_first_poll_write_keep_the_coop_yield() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, _, _) = ProbeTransport::new([]);
            let (session, handle) = upstream(&context, transport);
            let state = session.state.clone();
            let observer = session.observer.upgrade().unwrap();
            {
                let ready = Notify::new();
                ready.notify_one();
                let callbacks = state.borrow().core().callback_queue();
                let mut registration = CancellationToken::never().wake_registration();
                let mut notified = pin!(ready.notified());
                let mut callback_wait =
                    pin!(callbacks.wait_registered(|waker| registration.update_waker(waker)));
                let mut idle = pin!(h1_idle_wait(
                    &ready,
                    notified.as_mut(),
                    false,
                    callback_wait.as_mut(),
                    &state,
                    &observer,
                    &context,
                    &handle.shared.read_wake,
                ));
                let first = first_poll(idle.as_mut(), true).await;
                assert!(
                    first.poll.is_pending(),
                    "a spent budget yields before the ready wake source is polled"
                );
                assert_eq!(
                    first.synchronous_wakes, 0,
                    "the yield does not wake the task from inside its poll"
                );
                assert_eq!(
                    first.wakes_by_next_turn, 1,
                    "the scheduler runs the deferred wake"
                );
                let reason = tokio::time::timeout(Duration::from_secs(5), idle.as_mut())
                    .await
                    .expect("the deferred wake repolls the idle wait");
                assert!(matches!(reason, WriteWakeReason::Work), "{reason:?}");
            }
            drop(session);
            drop(handle);

            let ready = run_h1_write_batch(&context, false).await;
            assert!(matches!(ready.first.poll, Poll::Ready(Ok(()))));
            assert_eq!(ready.first.synchronous_wakes, 0);
            assert_eq!(ready.write_polls, 1);

            let yielded = run_h1_write_batch(&context, true).await;
            assert!(yielded.first.poll.is_pending(), "a spent budget yields");
            assert_eq!(
                yielded.write_polls_in_first_poll, 0,
                "the yield precedes the transport write"
            );
            assert_eq!(
                yielded.first.synchronous_wakes, 0,
                "the yield does not wake the task from inside its poll"
            );
            assert!(
                yielded.first.wakes_by_next_turn >= 1,
                "the scheduler runs the deferred wake"
            );
            assert_eq!(yielded.write_polls, 1);
        })
    });
}

/// Runs an upstream session whose only read ends, by EOF or by `failed`,
/// after yielding `yields` times, and returns the writer counts.
fn read_end_counts(failed: bool, yields: usize) -> writer_probe::WriterCounts {
    run_on_worker(move |context| {
        Box::pin(async move {
            writer_probe::reset();
            let read = if failed {
                ProbeRead::ErrorAfter(yields)
            } else {
                ProbeRead::EofAfter(yields)
            };
            let (transport, _, _) = ProbeTransport::new([read]);
            let (session, _handle) = upstream(&context, transport);
            let result = session.run().await;
            assert_eq!(result.is_err(), failed);
            writer_probe::snapshot()
        })
    })
}

/// Join passes alternate which loop goes first, starting with the read loop.
fn is_read_first_pass(pass: usize) -> bool {
    pass % 2 == 1
}

#[test]
fn read_loop_end_is_seen_by_the_write_loop_in_the_same_pass() {
    let mut parities = BTreeSet::new();
    for (failed, yields) in [(false, 0), (false, 1), (true, 0), (true, 1)] {
        let counts = read_end_counts(failed, yields);
        let read_done = counts.read_done_pass.expect("the read loop ended");
        let write_done = counts.write_done_pass.expect("the write loop ended");
        parities.insert(is_read_first_pass(read_done));
        if is_read_first_pass(read_done) {
            assert_eq!(
                write_done, read_done,
                "the writer observed the read loop's end in the same pass: {counts:?}"
            );
        } else {
            assert!(
                (read_done..=read_done + 1).contains(&write_done),
                "the writer observed the read loop's end by the next pass: {counts:?}"
            );
        }
    }
    assert_eq!(parities.len(), 2, "both pass parities were exercised");
}

#[test]
fn read_loop_end_on_a_write_first_pass_is_seen_in_that_pass() {
    for failed in [false, true] {
        let counts = read_end_counts(failed, 1);
        let read_done = counts.read_done_pass.expect("the read loop ended");
        assert!(
            !is_read_first_pass(read_done),
            "the read loop ended in a write-first pass: {counts:?}"
        );
        assert_eq!(
            counts.write_done_pass,
            Some(read_done),
            "the writer was polled again in the pass in which the read loop ended: {counts:?}"
        );
    }
}

#[test]
fn mid_run_drop_retires_the_read_operation_before_the_write_operation() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state, log) = ProbeTransport::new([]);
            let write_gate = Rc::new(Notify::new());
            state.borrow_mut().write_gate = Some(Rc::clone(&write_gate));
            let (session, handle) = upstream(&context, transport);
            let mut run = session.run();
            let send = handle.send_request(
                request(HttpMethod::Get, [], None),
                CancellationToken::never(),
            );
            let response = tokio::select! {
                biased;
                _ = &mut run => panic!("the session ended before the request"),
                response = send => response.unwrap(),
            };
            let _ = tokio::time::timeout(Duration::from_millis(50), &mut run).await;
            assert!(state.borrow().write_pending);
            assert!(!recorded(&log).contains(&"read-op-drop"));
            drop(run);
            settle().await;
            let log = recorded(&log);
            let read = log.iter().position(|event| *event == "read-op-drop");
            let write = log.iter().position(|event| *event == "write-op-drop");
            assert!(read.is_some() && write.is_some(), "{log:?}");
            assert!(read < write, "{log:?}");
            drop(response);
        })
    });
}

#[test]
fn panicking_waker_in_terminal_read_cancellation_still_reaches_abort() {
    let log = EventLog::default();
    let shared_log = Arc::clone(&log);
    let (result, driver_panic) = run_hosted(move |context| {
        Box::pin(async move {
            let (transport, state, _) =
                ProbeTransport::new([ProbeRead::PanickingCancellationWaker]);
            state.borrow_mut().log = shared_log;
            let (session, handle) = upstream(&context, transport);
            let run = session.run();
            let control = async {
                settle().await;
                assert!(handle.request_write_cancellation_for_test());
            };
            let (result, ()) = tokio::join!(run, control);
            result.map(|_| ()).map_err(|error| error.code())
        })
    });
    let payload = driver_panic.expect("the cleanup task resumes the waker panic");
    assert_eq!(
        payload.downcast_ref::<&str>(),
        Some(&"cancellation waker panic")
    );
    let events = recorded(&log);
    assert!(
        events.contains(&"reset"),
        "the abort path reset the transport: {events:?}"
    );
    assert!(events.contains(&"read-poll"), "{events:?}");
    assert!(
        !events.contains(&"read-poll-cancelled"),
        "the read loop was not repolled after the terminal request: {events:?}"
    );
    if let Some(result) = result {
        assert!(result.is_err(), "an aborted run reports its failure");
    }
}

struct TeardownFuture {
    drops: Rc<Cell<usize>>,
    ready: bool,
    poll_panic: bool,
    drop_panic: bool,
}

impl Future for TeardownFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _context: &mut std::task::Context<'_>) -> Poll<()> {
        if self.poll_panic {
            panic!("teardown poll");
        }
        if self.ready {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Drop for TeardownFuture {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
        if self.drop_panic {
            panic!("teardown drop");
        }
    }
}

fn teardown(
    drops: &Rc<Cell<usize>>,
    ready: bool,
    poll_panic: bool,
    drop_panic: bool,
) -> TeardownFuture {
    TeardownFuture {
        drops: Rc::clone(drops),
        ready,
        poll_panic,
        drop_panic,
    }
}

fn payload_text(payload: &(dyn std::any::Any + Send)) -> Option<&'static str> {
    payload.downcast_ref::<&'static str>().copied()
}

#[test]
fn teardown_future_with_a_panicking_drop_is_dropped_once_and_recorded() {
    let drops = Rc::new(Cell::new(0));
    let result = futures::executor::block_on(catch_future_unwind(pin!(Some(teardown(
        &drops, true, false, true
    )))));
    let payload = result.expect_err("the drop panic is recorded");
    assert_eq!(payload_text(payload.as_ref()), Some("teardown drop"));
    assert_eq!(drops.get(), 1);

    let drops = Rc::new(Cell::new(0));
    let result = futures::executor::block_on(catch_future_unwind(pin!(Some(teardown(
        &drops, true, false, false
    )))));
    assert!(result.is_ok());
    assert_eq!(drops.get(), 1);

    let drops = Rc::new(Cell::new(0));
    let mut first = None;
    futures::executor::block_on(collect_close_wait_panic(
        &mut first,
        pin!(Some(teardown(&drops, true, false, true))),
    ));
    assert_eq!(
        first.as_deref().and_then(payload_text),
        Some("teardown drop")
    );
    assert_eq!(drops.get(), 1);
}

#[test]
fn teardown_poll_panic_keeps_its_payload_over_a_drop_panic() {
    let drops = Rc::new(Cell::new(0));
    let result = futures::executor::block_on(catch_future_unwind(pin!(Some(teardown(
        &drops, true, true, true
    )))));
    let payload = result.expect_err("the poll panic is recorded");
    assert_eq!(payload_text(payload.as_ref()), Some("teardown poll"));
    assert_eq!(
        drops.get(),
        1,
        "the panicking future is dropped exactly once"
    );

    let drops = Rc::new(Cell::new(0));
    let mut first = None;
    futures::executor::block_on(collect_close_wait_panic(
        &mut first,
        pin!(Some(teardown(&drops, true, true, true))),
    ));
    assert_eq!(
        first.as_deref().and_then(payload_text),
        Some("teardown poll")
    );
    assert_eq!(drops.get(), 1);
}

#[test]
fn suspended_teardown_wait_drops_its_inner_future_once() {
    let drops = Rc::new(Cell::new(0));
    let mut caught = Box::pin(async {
        let _ = catch_future_unwind(pin!(Some(teardown(&drops, false, false, false)))).await;
    });
    assert!(
        caught
            .as_mut()
            .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
            .is_pending()
    );
    assert_eq!(drops.get(), 0);
    drop(caught);
    assert_eq!(drops.get(), 1);

    let drops = Rc::new(Cell::new(0));
    let mut collected = Box::pin(async {
        let mut first = None;
        collect_close_wait_panic(
            &mut first,
            pin!(Some(teardown(&drops, false, false, false))),
        )
        .await;
    });
    assert!(
        collected
            .as_mut()
            .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
            .is_pending()
    );
    assert_eq!(drops.get(), 0);
    drop(collected);
    assert_eq!(drops.get(), 1);
}

#[test]
fn retained_handle_cannot_reach_the_cleanup_control_after_the_run() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, _, _) = ProbeTransport::new([]);
            let (session, handle) = upstream(&context, transport);
            let retained = handle.clone();
            let run = session.run();
            assert!(retained.cleanup_reachable_for_test());
            let control = async {
                settle().await;
                handle.initiate_drain().unwrap();
            };
            let (result, ()) = tokio::join!(run, control);
            result.unwrap();
            drop(handle);
            settle().await;
            assert!(
                !retained.cleanup_reachable_for_test(),
                "a retained handle holds the cleanup control only weakly"
            );
            assert!(retained.is_finished());
            assert_eq!(
                retained
                    .reserve_request()
                    .err()
                    .expect("a finished session admits nothing")
                    .code(),
                HttpErrorCode::DROPPED
            );
            retained.wake_writer_for_test();
        })
    });

    // A cleanup panic recorded by the run future's drop stays in the cleanup
    // control while the cleanup task waits for a held response; dropping the
    // driver retires that task, which drops the payload before the retained
    // handle drops.
    let log = EventLog::default();
    let block_log = Arc::clone(&log);
    let shared_log = Arc::clone(&log);
    let runtime = Builder::new_current_thread().enable_time().build().unwrap();
    LocalSet::new().block_on(&runtime, async move {
        let (worker, driver) = WorkerDriver::with_host(16, Host::tokio()).unwrap();
        let mut driver = Box::pin(driver);
        let call = worker
            .call_local(move |context| async move {
                let (transport, state, _) =
                    ProbeTransport::new([ProbeRead::LoggedPanicCancellationWaker]);
                state.borrow_mut().log = shared_log;
                let (session, handle) = upstream(&context, transport);
                let mut run = session.run();
                let response = tokio::select! {
                    biased;
                    _ = &mut run => panic!("the session ended before the request"),
                    response = handle.send_request(
                        request(HttpMethod::Get, [], None),
                        CancellationToken::never(),
                    ) => response.unwrap(),
                };
                settle().await;
                drop(run);
                settle().await;
                assert!(
                    handle.cleanup_reachable_for_test(),
                    "the cleanup task waits for the held response"
                );
                RETAINED.with(|slot| *slot.borrow_mut() = Some((handle, Box::new(response))));
            })
            .unwrap();
        tokio::select! {
            biased;
            result = call => result.unwrap(),
            () = driver.as_mut() => panic!("the driver ended before the call"),
        }
        assert!(!recorded(&block_log).contains(&"payload-drop"));
        // Retiring a session mid-teardown also trips its own drop invariants,
        // which the driver resumes; the logged payload must not be among them.
        if let Err(teardown_panic) = catch_unwind(AssertUnwindSafe(|| drop(driver))) {
            assert!(!teardown_panic.is::<LoggedPayload>());
        }
        settle().await;
        assert!(
            recorded(&block_log).contains(&"payload-drop"),
            "retiring the cleanup task dropped the recorded payload"
        );
        let (handle, response) = RETAINED
            .with(|slot| slot.borrow_mut().take())
            .expect("the call retained its handle");
        assert!(!handle.cleanup_reachable_for_test());
        drop(response);
        record(&block_log, "retained-handle-drop");
        drop(handle);
    });
    let events = recorded(&log);
    let payload = events.iter().position(|event| *event == "payload-drop");
    let handle = events
        .iter()
        .position(|event| *event == "retained-handle-drop");
    assert!(
        payload.is_some() && payload < handle,
        "the runtime's teardown dropped the payload before the handle: {events:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| **event == "payload-drop")
            .count(),
        1
    );
}

thread_local! {
    static RETAINED: RefCell<Option<(UniplexHandle, Box<dyn std::any::Any>)>> =
        const { RefCell::new(None) };
}

#[test]
fn idle_waits_build_no_baton_and_register_write_cancellation_once() {
    run_on_worker(|context| {
        Box::pin(async move {
            writer_probe::reset();
            let read_release = Rc::new(Notify::new());
            let (transport, _, _) = ProbeTransport::new([ProbeRead::Release(
                Rc::clone(&read_release),
                RESPONSE,
                false,
            )]);
            let (session, handle) = upstream(&context, transport);
            let callbacks = handle.callback_queue_for_test();
            let run = session.run();
            let control = async {
                let mut response = handle
                    .send_request(
                        request(HttpMethod::Get, [], None),
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap();
                settle().await;
                callbacks.wake_retirement();
                settle().await;
                read_release.notify_one();
                assert!(response.source.read_header_event().await.unwrap().eom());
                drop(response);
                settle().await;
                handle.initiate_drain().unwrap();
            };
            let (result, ()) = tokio::join!(run, control);
            result.unwrap();
            let counts = writer_probe::snapshot();
            assert!(counts.idle_waits >= 3, "{counts:?}");
            assert_eq!(
                counts.cancellation_registry_updates, 1,
                "one listener install serves every idle wait: {counts:?}"
            );
            assert_eq!(
                counts.idle_pending_event_waits, 0,
                "no idle wait built a boxed callback wait: {counts:?}"
            );
        })
    });
}

#[test]
fn h1_idle_wake_with_both_wakes_ready_runs_once() {
    run_on_worker(|context| {
        Box::pin(async move {
            writer_probe::reset();
            let (transport, _, _) = ProbeTransport::new([]);
            let (session, handle) = upstream(&context, transport);
            let callbacks = handle.callback_queue_for_test();
            let run = session.run();
            let control = async {
                settle().await;
                let before = writer_probe::snapshot();
                callbacks.wake_retirement();
                handle.wake_writer_for_test();
                settle().await;
                let after = writer_probe::snapshot();
                assert_eq!(after.idle_wakes, before.idle_wakes + 1, "one iteration");
                assert_eq!(
                    after.idle_waits,
                    before.idle_waits + 1,
                    "the next wait parked"
                );
                assert!(!callbacks.has_pending());
                settle().await;
                let later = writer_probe::snapshot();
                assert_eq!(
                    (later.idle_waits, later.idle_wakes),
                    (after.idle_waits, after.idle_wakes),
                    "no wake was left behind"
                );
                handle.initiate_drain().unwrap();
            };
            let (result, ()) = tokio::join!(run, control);
            result.unwrap();
        })
    });
}

#[test]
fn h1_idle_wait_and_ready_write_skip_http2_only_setup() {
    let http1 = run_on_worker(|context| {
        Box::pin(async move {
            writer_probe::reset();
            let read_release = Rc::new(Notify::new());
            let (transport, _, _) = ProbeTransport::new([ProbeRead::Release(
                Rc::clone(&read_release),
                RESPONSE,
                false,
            )]);
            let (session, handle) = upstream(&context, transport);
            let run = session.run();
            let control = async {
                let mut response = handle
                    .send_request(
                        request(HttpMethod::Post, [body(b"payload", true)], None),
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap();
                settle().await;
                read_release.notify_one();
                assert!(response.source.read_header_event().await.unwrap().eom());
                drop(response);
                handle.initiate_drain().unwrap();
            };
            let (result, ()) = tokio::join!(run, control);
            result.unwrap();
            writer_probe::snapshot()
        })
    });
    assert!(http1.idle_waits >= 1);
    assert!(http1.first_poll_writes >= 1);
    assert_eq!(http1.http2_setup, 0, "{http1:?}");
    assert_eq!(http1.batch_selects, 0, "{http1:?}");

    let http2 = run_on_worker(|context| {
        Box::pin(async move {
            writer_probe::reset();
            let (transport, _, _) = ProbeTransport::new([]);
            let (session, handle) = upstream_http2(&context, transport);
            let run = session.run();
            let control = async {
                settle().await;
                handle.initiate_drain().unwrap();
            };
            let (result, ()) = tokio::join!(run, control);
            result.unwrap();
            writer_probe::snapshot()
        })
    });
    assert!(http2.http2_setup > 0, "{http2:?}");
    assert!(http2.batch_selects > 0, "{http2:?}");

    let (ready, yielded) = run_on_worker(|context| {
        Box::pin(async move {
            let ready = run_h1_write_batch(&context, false).await;
            let yielded = run_h1_write_batch(&context, true).await;
            ((ready.before, ready.after), (yielded.before, yielded.after))
        })
    });
    let (before, after) = ready;
    assert_eq!(after.first_poll_writes, before.first_poll_writes + 1);
    assert_eq!(
        after.batch_selects, before.batch_selects,
        "a first-poll write builds no callback wait or select"
    );
    assert_eq!(after.http2_setup, before.http2_setup);
    let (before, after) = yielded;
    assert_eq!(after.first_poll_writes, before.first_poll_writes);
    assert_eq!(
        after.batch_selects,
        before.batch_selects + 1,
        "a spent budget takes the select path"
    );
}

#[test]
fn idle_drain_completes_the_cleanup_task_in_the_write_exit_poll() {
    for extra_wake in [false, true] {
        let counts = run_on_worker(move |context| {
            Box::pin(async move {
                writer_probe::reset();
                let (transport, _, _) = ProbeTransport::new([]);
                let (session, handle) = upstream(&context, transport);
                let run = session.run();
                let control = async {
                    settle().await;
                    if extra_wake {
                        handle.wake_writer_for_test();
                        settle().await;
                    }
                    handle.initiate_drain().unwrap();
                };
                let (result, ()) = tokio::join!(run, control);
                result.unwrap();
                writer_probe::snapshot()
            })
        });
        assert!(counts.write_done_cleanup_poll.is_some(), "{counts:?}");
        assert_eq!(
            counts.cleanup_done_poll, counts.write_done_cleanup_poll,
            "the cleanup task finished in the poll in which the writer exited: {counts:?}"
        );
        assert_eq!(counts.read_done_pass, counts.write_done_pass, "{counts:?}");
    }
}

#[test]
fn h1_writer_wakes_stored_before_a_pass_take_that_one_pass() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, _, _) = ProbeTransport::new([]);
            let (session, handle) = upstream(&context, transport);
            let run = session.run();
            let control = async {
                settle().await;
                let before = writer_probe::snapshot();
                handle.wake_writer_for_test();
                handle.wake_writer_for_test();
                settle().await;
                let after = writer_probe::snapshot();
                assert_eq!(
                    after.loop_passes,
                    before.loop_passes + 1,
                    "the second wake was stored before the woken pass, which answered it"
                );
                assert_eq!(after.idle_wakes, before.idle_wakes + 1);
                assert_eq!(after.idle_waits, before.idle_waits + 1);
                handle.initiate_drain().unwrap();
            };
            let (result, ()) = tokio::join!(run, control);
            result.unwrap();
        })
    });
}

#[test]
fn h1_idle_wait_discards_only_a_wake_stored_before_its_unsuspended_first_poll() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, _, _) = ProbeTransport::new([]);
            let (session, handle) = upstream(&context, transport);
            let state = session.state.clone();
            let observer = session.observer.upgrade().unwrap();
            let callbacks = state.borrow().core().callback_queue();
            for spend_budget in [false, true] {
                let wake = Notify::new();
                wake.notify_one();
                let mut registration = CancellationToken::never().wake_registration();
                let mut notified = pin!(wake.notified());
                let mut callback_wait =
                    pin!(callbacks.wait_registered(|waker| registration.update_waker(waker)));
                let mut idle = pin!(h1_idle_wait(
                    &wake,
                    notified.as_mut(),
                    true,
                    callback_wait.as_mut(),
                    &state,
                    &observer,
                    &context,
                    &handle.shared.read_wake,
                ));
                let first = first_poll(idle.as_mut(), spend_budget).await;
                assert!(first.poll.is_pending());
                if spend_budget {
                    let reason = tokio::time::timeout(Duration::from_secs(5), idle.as_mut())
                        .await
                        .expect("a coop yield is a suspension, so the stored wake survives it");
                    assert!(matches!(reason, WriteWakeReason::Work), "{reason:?}");
                } else {
                    assert_eq!(
                        first.wakes_by_next_turn, 0,
                        "the stored wake was answered by the checks before the wait"
                    );
                    wake.notify_one();
                    let reason = tokio::time::timeout(Duration::from_secs(5), idle.as_mut())
                        .await
                        .expect("a wake after the wait registered ends it");
                    assert!(matches!(reason, WriteWakeReason::Work), "{reason:?}");
                }
            }
            drop(callbacks);
            drop(session);
            drop(handle);
        })
    });
}

#[test]
fn one_shot_h1_get_writes_its_head_in_one_batch_and_parks_once() {
    run_on_worker(|context| {
        Box::pin(async move {
            writer_probe::reset();
            let release = Rc::new(Notify::new());
            let (transport, state, log) = ProbeTransport::new([ProbeRead::Release(
                Rc::clone(&release),
                b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nbody",
                false,
            )]);
            let (session, handle) = upstream(&context, transport);
            let run = session.run();
            let control = async {
                let mut response = handle
                    .send_request(
                        request(HttpMethod::Get, [], None),
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap();
                settle().await;
                let sent = writer_probe::snapshot();
                assert_eq!(state.borrow().writes.len(), 1);
                assert_eq!(sent.first_poll_writes, 1);
                assert_eq!(
                    sent.loop_passes, 3,
                    "one pass serializes the queued head, one writes it, one parks"
                );
                assert_eq!(
                    (sent.idle_waits, sent.idle_wakes),
                    (1, 0),
                    "the wake that announced the request was answered before the writer parked"
                );
                release.notify_one();
                assert!(!response.source.read_header_event().await.unwrap().eom());
                assert!(
                    response
                        .source
                        .read_body_event(1 << 16)
                        .await
                        .unwrap()
                        .eom()
                );
                drop(response);
                handle.close_when_idle().unwrap();
            };
            let (result, ()) = tokio::join!(run, control);
            result.unwrap();
            let done = writer_probe::snapshot();
            assert_eq!(
                done.loop_passes, 4,
                "the detach wake ends the writer in one pass"
            );
            assert_eq!((done.idle_waits, done.idle_wakes), (1, 1));
            assert!(state.borrow().writes[0].starts_with(b"GET / HTTP/1.1\r\n"));
            assert!(recorded(&log).contains(&"shutdown-write"));
        })
    });
}

/// Proxygen's `readLoop` does not signal `writeEvent_` after `onIngress`. After
/// a `Connection: close` response, `shouldContinueWriteLooping` turns false
/// once the stream detaches, and `checkForDetach` signals the writer, which then
/// shuts the write side down. Parsing the response alone leaves the writer
/// parked.
#[test]
fn h1_close_response_parse_leaves_the_writer_parked_until_the_detach() {
    run_on_worker(|context| {
        Box::pin(async move {
            writer_probe::reset();
            let release = Rc::new(Notify::new());
            let (transport, state, log) = ProbeTransport::new([ProbeRead::Release(
                Rc::clone(&release),
                b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 4\r\n\r\nbody",
                false,
            )]);
            let (session, handle) = upstream(&context, transport);
            let run = session.run();
            let control = async {
                let mut response = handle
                    .send_request(
                        request(HttpMethod::Get, [], None),
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap();
                settle().await;
                assert_eq!(state.borrow().writes.len(), 1);
                let parked = writer_probe::snapshot();
                release.notify_one();
                settle().await;
                let parsed = writer_probe::snapshot();
                assert_eq!(
                    parsed.loop_passes, parked.loop_passes,
                    "parsing the response did not wake the writer"
                );
                assert!(
                    !recorded(&log).contains(&"shutdown-write"),
                    "the write side stays open until the stream detaches"
                );
                let headers = response.source.read_header_event().await.unwrap();
                assert!(!headers.eom());
                let body = response.source.read_body_event(1 << 16).await.unwrap();
                assert!(body.eom());
                drop(response);
            };
            let (result, ()) = tokio::join!(run, control);
            result.unwrap();
            let done = writer_probe::snapshot();
            assert_eq!(
                (done.idle_waits, done.idle_wakes),
                (1, 1),
                "one wake, the detach, ended the writer"
            );
            assert!(recorded(&log).contains(&"shutdown-write"));
        })
    });
}

/// What a [`ProbeOwner`] saw, shared with the test across the worker thread.
#[derive(Default)]
struct OwnerRecord {
    polls: AtomicUsize,
    polls_after_completion: AtomicUsize,
    stops: AtomicUsize,
    drops: AtomicUsize,
    /// The run's outcome, as `finish` received it.
    outcome: Mutex<Option<Result<UniplexRunSummary, HttpError>>>,
    /// Strong references to the transport when `finish` ran, the owner's included.
    transport_references_at_finish: Mutex<Option<usize>>,
}

/// An owner that drains its session when `stop` is cancelled.
struct ProbeOwner {
    record: Arc<OwnerRecord>,
    handle: UniplexHandle,
    transport: Rc<dyn UniplexTransport>,
    stop: CancellationWakeRegistration,
    stopped: bool,
    panic_on_poll: bool,
}

impl UniplexRunOwner for ProbeOwner {
    fn poll_stop(&mut self, context: &mut Context<'_>) {
        self.record.polls.fetch_add(1, Ordering::SeqCst);
        if self.handle.is_finished() {
            self.record
                .polls_after_completion
                .fetch_add(1, Ordering::SeqCst);
        }
        if self.panic_on_poll {
            panic!("owner poll panic");
        }
        if !self.stopped && self.stop.update_waker(context.waker()) {
            self.stopped = true;
            self.record.stops.fetch_add(1, Ordering::SeqCst);
            self.handle.initiate_drain().unwrap();
        }
    }

    fn finish(self: Box<Self>, outcome: Result<UniplexRunSummary, HttpError>) {
        *self.record.transport_references_at_finish.lock().unwrap() =
            Some(Rc::strong_count(&self.transport));
        *self.record.outcome.lock().unwrap() = Some(outcome);
    }
}

impl Drop for ProbeOwner {
    fn drop(&mut self) {
        self.record.drops.fetch_add(1, Ordering::SeqCst);
    }
}

/// Starts an upstream HTTP/1 session on `transport` for a [`ProbeOwner`] that
/// records into `record` and drains the session when `stop` is cancelled.
/// Only the session and the owner keep `transport`.
fn start_owned(
    context: &WorkerContext,
    transport: Rc<dyn UniplexTransport>,
    stop: &CancellationSource,
    record: &Arc<OwnerRecord>,
    panic_on_poll: bool,
) -> UniplexHandle {
    let (session, handle) = UniplexSession::new_owned(
        context.clone(),
        TransportDirection::Upstream,
        Rc::clone(&transport),
        None,
        UniplexConfig::default(),
        UniplexProtocolConfig::Http1 { stats: None },
    )
    .unwrap();
    session
        .start(Box::new(ProbeOwner {
            record: Arc::clone(record),
            handle: handle.clone(),
            transport,
            stop: stop.token().wake_registration(),
            stopped: false,
            panic_on_poll,
        }))
        .unwrap();
    handle
}

#[test]
fn owned_session_serves_its_owner_on_its_run_task_and_hands_it_the_outcome_last() {
    let record = Arc::new(OwnerRecord::default());
    let owned = Arc::clone(&record);
    let log = run_on_worker(move |context| {
        Box::pin(async move {
            let (transport, _, log) = ProbeTransport::new([]);
            let stop = CancellationSource::new();
            let handle = start_owned(&context, transport, &stop, &owned, false);
            settle().await;
            assert!(owned.polls.load(Ordering::SeqCst) >= 1);
            assert_eq!(owned.stops.load(Ordering::SeqCst), 0);
            assert!(!handle.is_finished());

            stop.request_cancellation();
            until(|| owned.outcome.lock().unwrap().is_some()).await;
            assert_eq!(
                owned.stops.load(Ordering::SeqCst),
                1,
                "cancelling the owner's token woke the run task, which polled the owner"
            );
            assert!(handle.is_finished());
            log
        })
    });
    assert!(recorded(&log).contains(&"shutdown-write"));
    assert!(
        !recorded(&log).contains(&"reset"),
        "a drained session closes gracefully"
    );
    let outcome = record.outcome.lock().unwrap().take().unwrap();
    assert_eq!(outcome.unwrap(), UniplexRunSummary::default());
    assert_eq!(
        *record.transport_references_at_finish.lock().unwrap(),
        Some(1),
        "the run task released its own transport before its owner took the outcome"
    );
    assert!(
        record.polls_after_completion.load(Ordering::SeqCst) >= 1,
        "the owner was polled once more after the run completed"
    );
    assert_eq!(record.drops.load(Ordering::SeqCst), 1);
}

#[test]
fn owned_session_polls_its_owner_only_after_the_owners_waker_fires() {
    let record = Arc::new(OwnerRecord::default());
    let owned = Arc::clone(&record);
    run_on_worker(move |context| {
        Box::pin(async move {
            let release = Rc::new(Notify::new());
            let (transport, state, _) = ProbeTransport::new([
                ProbeRead::Release(
                    Rc::clone(&release),
                    b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nbody",
                    false,
                ),
                ProbeRead::Cancelled,
            ]);
            let stop = CancellationSource::new();
            let handle = start_owned(&context, transport, &stop, &owned, false);
            settle().await;
            assert_eq!(
                owned.polls.load(Ordering::SeqCst),
                1,
                "the run's first poll polls its owner"
            );

            let mut response = handle
                .send_request(
                    request(HttpMethod::Get, [], None),
                    CancellationToken::never(),
                )
                .await
                .unwrap();
            settle().await;
            assert_eq!(
                state.borrow().writes.len(),
                1,
                "the run task wrote the head"
            );
            release.notify_one();
            assert!(!response.source.read_header_event().await.unwrap().eom());
            assert!(
                response
                    .source
                    .read_body_event(1 << 16)
                    .await
                    .unwrap()
                    .eom()
            );
            drop(response);
            settle().await;
            assert_eq!(
                owned.polls.load(Ordering::SeqCst),
                1,
                "the run's own wakes leave its owner unpolled"
            );

            stop.request_cancellation();
            until(|| owned.outcome.lock().unwrap().is_some()).await;
            assert_eq!(
                owned.stops.load(Ordering::SeqCst),
                1,
                "the owner's waker woke the run task, which polled the owner"
            );
            assert!(handle.is_finished());
        })
    });
    assert!(
        record.polls_after_completion.load(Ordering::SeqCst) >= 1,
        "the owner was polled once more after the run completed"
    );
    assert_eq!(record.drops.load(Ordering::SeqCst), 1);
}

#[test]
fn owned_session_run_dropped_before_completion_aborts_and_drops_its_owner_unfinished() {
    let log = EventLog::default();
    let shared_log = Arc::clone(&log);
    let record = Arc::new(OwnerRecord::default());
    let owned = Arc::clone(&record);
    let runtime = Builder::new_current_thread().enable_time().build().unwrap();
    LocalSet::new().block_on(&runtime, async move {
        let (worker, driver) = WorkerDriver::with_host(16, Host::tokio()).unwrap();
        let mut driver = Box::pin(driver);
        let call = worker
            .call_local(move |context| async move {
                let (transport, state, _) = ProbeTransport::new([ProbeRead::Cancelled]);
                state.borrow_mut().log = shared_log;
                let stop = CancellationSource::new();
                let handle = start_owned(&context, transport, &stop, &owned, false);
                settle().await;
                assert!(owned.polls.load(Ordering::SeqCst) >= 1);
                assert!(!handle.is_finished());
                RETAINED.with(|slot| *slot.borrow_mut() = Some((handle, Box::new(()))));
            })
            .unwrap();
        tokio::select! {
            biased;
            result = call => result.unwrap(),
            () = driver.as_mut() => panic!("the driver ended before the call"),
        }
        let (handle, _) = RETAINED
            .with(|slot| slot.borrow_mut().take())
            .expect("the call retained its handle");
        assert_eq!(record.drops.load(Ordering::SeqCst), 0);
        // Forced retirement drops the run task mid-run.
        let _ = catch_unwind(AssertUnwindSafe(|| drop(driver)));
        settle().await;
        assert!(!handle.is_finished());
        drop(handle);
        assert_eq!(
            record.drops.load(Ordering::SeqCst),
            1,
            "the dropped run task dropped its owner"
        );
        assert!(
            record.outcome.lock().unwrap().is_none(),
            "an owner gets no outcome from a run that did not complete"
        );
    });
    assert!(
        recorded(&log).contains(&"reset"),
        "dropping the run task started abort cleanup, as dropping a run future does: {:?}",
        recorded(&log)
    );
    assert!(!recorded(&log).contains(&"shutdown-write"));
}

#[test]
fn owned_session_owner_panic_aborts_the_run_and_resumes_after_its_cleanup() {
    let record = Arc::new(OwnerRecord::default());
    let owned = Arc::clone(&record);
    let log = EventLog::default();
    let shared_log = Arc::clone(&log);
    let (_, panic) = run_hosted(move |context| {
        Box::pin(async move {
            let (transport, state, _) = ProbeTransport::new([ProbeRead::Cancelled]);
            state.borrow_mut().log = shared_log;
            let stop = CancellationSource::new();
            let _handle = start_owned(&context, transport, &stop, &owned, true);
            // The owner's panic begins worker shutdown, which retires this call.
            std::future::pending::<()>().await;
        })
    });
    let panic = panic.expect("the worker resumed the owner's panic");
    assert_eq!(payload_text(panic.as_ref()), Some("owner poll panic"));
    assert_eq!(
        record.polls.load(Ordering::SeqCst),
        1,
        "a panicked owner is not polled again"
    );
    assert_eq!(record.drops.load(Ordering::SeqCst), 1);
    assert!(
        record.outcome.lock().unwrap().is_none(),
        "a panicked owner gets no outcome"
    );
    assert!(
        recorded(&log).contains(&"reset"),
        "the owner's panic aborted the run: {:?}",
        recorded(&log)
    );
    assert!(!recorded(&log).contains(&"shutdown-write"));
}
