use std::cell::Cell;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::future::poll_fn;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread::ThreadId;
use std::time::Duration;

use bytes::Bytes;
use roxygen_codec::CodecEvent;
use roxygen_codec::EgressFrame;
use roxygen_codec::HttpCodec;
use roxygen_codec::HttpCodecCallback;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::byte_event::ByteEventType;
use roxygen_core::byte_event::HttpByteEvent;
use roxygen_core::byte_event::HttpByteEventCallback;
use roxygen_core::byte_event::HttpByteEventRegistration;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpBodyEventType;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::exchange::ExchangeCompletion;
use roxygen_core::exchange::ExchangeCompletionError;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::http::CodecProtocol;
use roxygen_core::http::HttpMethod;
use roxygen_core::http::TransportDirection;
use roxygen_core::ids::StreamId;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_http2::Http2Codec;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::driver::WorkerDriver;
use roxygen_runtime::keep_alive::KeepAliveOwner;
use roxygen_runtime::wait::CancellableBaton;
use roxygen_runtime::wait::WaitStatus;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerHandle;
use roxygen_runtime::worker::WorkerState;
use roxygen_transport::byte_event::ByteEventWriteFlags;
use roxygen_transport::byte_stream::ByteEventCapability;
use roxygen_transport::byte_stream::ByteEventRegistrationError;
use roxygen_transport::byte_stream::LocalTransportFuture;
use roxygen_transport::byte_stream::TransportWriteError;
use roxygen_transport::byte_stream::WriteInfo;
use tokio::runtime::Builder;
use tokio::sync::Notify;
use tokio::task::LocalSet;
use tokio::time;

use crate::exchange::ExchangeOwner;
use crate::http2::Http2SessionConfig;
use crate::session::LifecycleEvent;
use crate::session::LifecycleObserver;
use crate::session::LocalSessionFuture;
use crate::session::SessionCore;
use crate::stream_state::EgressEvent;
use crate::stream_state::EgressTransfer;
use crate::stream_state::StreamCallbacks;
use crate::uniplex::UniplexConfig;
use crate::uniplex::UniplexRead;
use crate::uniplex::UniplexRunSummary;
use crate::uniplex::UniplexSession;
use crate::uniplex::UniplexTransport;

fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

fn pending(completion: &ExchangeCompletion) {
    assert!(poll_once(completion.wait().as_mut()).is_pending());
}

async fn completed(completion: &ExchangeCompletion) -> Result<(), ExchangeCompletionError> {
    time::timeout(Duration::from_secs(2), completion.wait())
        .await
        .expect("exchange retirement")
}

fn run_local(test: impl Future<Output = ()> + 'static) {
    Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(LocalSet::new().run_until(test));
}

fn new_session(protocol: CodecProtocol) -> (SessionCore, WorkerDriver) {
    let (worker, driver) = WorkerDriver::new(32).unwrap();
    (
        SessionCore::new(TransportDirection::Downstream, protocol, worker).unwrap(),
        driver,
    )
}

fn request_message() -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_method(HttpMethod::Get).unwrap();
    message.set_url(b"/", false).unwrap();
    message.set_http_version(1, 1);
    message.headers_mut().set(b"host", b"example.test");
    message
}

fn response() -> HttpSourceHolder {
    HttpSourceHolder::new(Box::new(
        HttpFixedSource::make_fixed_response(200, None).unwrap(),
    ))
}

async fn drain_transfer(
    session: &mut SessionCore,
    stream: StreamId,
    transfer: &mut EgressTransfer,
) {
    while let Some(event) = transfer.next_event().await.unwrap() {
        match event {
            EgressEvent::Headers(headers) => {
                session
                    .response_headers(stream, true, headers.eom())
                    .unwrap();
            }
            EgressEvent::Body(body) => {
                if body.eom() {
                    session.egress_finished(stream).unwrap();
                }
            }
        }
    }
}

struct PendingSource {
    dropped: Rc<Cell<bool>>,
    read_dropped: Rc<Cell<bool>>,
    stopped: Rc<Cell<usize>>,
    panic_stop: bool,
    ready: bool,
    owner: ThreadId,
}

struct PendingRead {
    dropped: Rc<Cell<bool>>,
    ready: bool,
    owner: ThreadId,
}

impl Future for PendingRead {
    type Output = Result<HttpBodyEvent, HttpError>;

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        assert_eq!(self.owner, std::thread::current().id());
        if self.ready {
            Poll::Ready(Ok(HttpBodyEvent::body(BufQueue::new(0), true)))
        } else {
            Poll::Pending
        }
    }
}

impl Drop for PendingRead {
    fn drop(&mut self) {
        assert_eq!(self.owner, std::thread::current().id());
        self.dropped.set(true);
    }
}

impl HttpSource for PendingSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::pending())
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(PendingRead {
            dropped: Rc::clone(&self.read_dropped),
            ready: self.ready,
            owner: self.owner,
        })
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {
        assert_eq!(self.owner, std::thread::current().id());
        self.stopped.set(self.stopped.get() + 1);
        assert!(!self.panic_stop, "scripted source stop panic");
    }
}

impl Drop for PendingSource {
    fn drop(&mut self) {
        assert_eq!(self.owner, std::thread::current().id());
        self.dropped.set(true);
    }
}

struct WaitingHandler {
    started: Rc<Notify>,
    release: Rc<Notify>,
    observed: Rc<Cell<bool>>,
}

impl HttpHandler for WaitingHandler {
    fn handle_request(
        &self,
        context: HandlerSessionContext,
        mut source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let started = Rc::clone(&self.started);
        let release = Rc::clone(&self.release);
        self.observed.set(context.exchange_completion().is_some());
        pending(context.exchange_completion().unwrap());
        Box::pin(async move {
            source.read_header_event().await?;
            drop(source);
            started.notify_one();
            release.notified().await;
            drop(context);
            Ok(response())
        })
    }
}

#[test]
fn prepared_bodyless_request_panic_preserves_exchange_failure() {
    run_local(async {
        let (worker, _driver) = WorkerDriver::new(32).unwrap();
        let mut session =
            SessionCore::new(TransportDirection::Upstream, CodecProtocol::Http2, worker).unwrap();
        let dropped = Rc::new(Cell::new(false));
        let stopped = Rc::new(Cell::new(0));
        let source = HttpSourceHolder::new(Box::new(PendingSource {
            dropped: Rc::clone(&dropped),
            read_dropped: Rc::new(Cell::new(false)),
            stopped: Rc::clone(&stopped),
            panic_stop: true,
            ready: false,
            owner: std::thread::current().id(),
        }));
        let prepared = SessionCore::prepare_request_with_headers(
            HttpHeaderEvent::new(request_message(), true).unwrap(),
            source,
            session.reserve_request().unwrap(),
        );
        let id = StreamId::new(1);
        assert!(
            catch_unwind(AssertUnwindSafe(
                || session.send_prepared_request(prepared, id)
            ))
            .is_err()
        );
        assert!(dropped.get());
        assert_eq!(stopped.get(), 1);
        let receipt = session.exchange_tracker(id).unwrap().completion();
        pending(&receipt);
        session
            .abort_stream_without_wire(id, HttpError::new(HttpErrorCode::CANCEL))
            .unwrap();
        session.process_pending_events().unwrap();
        assert_eq!(
            completed(&receipt).await,
            Err(ExchangeCompletionError::CleanupUnconfirmed)
        );
        session.finalize().await.unwrap();
    });
}

#[test]
fn prepared_bodyless_request_source_drop_panic_preserves_exchange_failure() {
    run_local(async {
        let (worker, _driver) = WorkerDriver::new(32).unwrap();
        let mut session =
            SessionCore::new(TransportDirection::Upstream, CodecProtocol::Http2, worker).unwrap();
        let dropped = Rc::new(Cell::new(false));
        let source = HttpSourceHolder::new(Box::new(PanickingSource {
            dropped: Rc::clone(&dropped),
            read_dropped: Rc::new(Cell::new(false)),
            panic_poll: false,
            owner: std::thread::current().id(),
        }));
        let prepared = SessionCore::prepare_request_with_headers(
            HttpHeaderEvent::new(request_message(), true).unwrap(),
            source,
            session.reserve_request().unwrap(),
        );
        let id = StreamId::new(1);
        let panic = catch_unwind(AssertUnwindSafe(|| {
            session.send_prepared_request(prepared, id)
        }))
        .err()
        .expect("source destructor panics");
        assert!(panic.is::<PanickingPayload>());
        roxygen_runtime::panic::discard_panic(panic);
        assert!(dropped.get());
        let receipt = session.exchange_tracker(id).unwrap().completion();
        pending(&receipt);
        session
            .abort_stream_without_wire(id, HttpError::new(HttpErrorCode::CANCEL))
            .unwrap();
        session.process_pending_events().unwrap();
        assert_eq!(
            completed(&receipt).await,
            Err(ExchangeCompletionError::CleanupUnconfirmed)
        );
        session.finalize().await.unwrap();
    });
}

#[test]
fn exchange_retirement_wakes_suspended_callback_and_receipt_waiters() {
    struct WakeFlag(AtomicBool);

    impl Wake for WakeFlag {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    for detach_before_zero in [false, true] {
        for driver_before_zero in [false, true] {
            let callbacks = Rc::new(StreamCallbacks::new());
            let callback_queue = Rc::downgrade(&callbacks);
            let owner = ExchangeOwner::new(
                Rc::new(move || {
                    if let Some(callbacks) = callback_queue.upgrade() {
                        callbacks.wake_retirement();
                    }
                }),
                true,
            );
            let tracker = owner.tracker();
            let first = tracker.guard();
            let last = tracker.guard();
            let callback_wake = Arc::new(WakeFlag(AtomicBool::new(false)));
            let callback_waker = Waker::from(Arc::clone(&callback_wake));
            let mut callback_context = Context::from_waker(&callback_waker);
            let receipt_wake = Arc::new(WakeFlag(AtomicBool::new(false)));
            let receipt_waker = Waker::from(Arc::clone(&receipt_wake));
            let mut receipt_context = Context::from_waker(&receipt_waker);
            let mut receipt = owner.completion().wait();
            assert!(receipt.as_mut().poll(&mut receipt_context).is_pending());

            if detach_before_zero {
                let mut detached = Box::pin(callbacks.wait(CancellationToken::never()));
                assert!(detached.as_mut().poll(&mut callback_context).is_pending());
                tracker.mark_detached();
                assert!(callback_wake.0.swap(false, Ordering::SeqCst));
                assert_eq!(
                    detached.as_mut().poll(&mut callback_context),
                    Poll::Ready(WaitStatus::Signalled)
                );
                callbacks.acknowledge_retirement();
            }
            if driver_before_zero {
                tracker.finish_driver();
            }
            drop(first);
            assert!(!owner.ready());
            assert!(receipt.as_mut().poll(&mut receipt_context).is_pending());
            if callbacks.has_pending() {
                assert_eq!(
                    poll_once(Box::pin(callbacks.wait(CancellationToken::never())).as_mut()),
                    Poll::Ready(WaitStatus::Signalled)
                );
                callbacks.acknowledge_retirement();
            }

            let mut released = Box::pin(callbacks.wait(CancellationToken::never()));
            assert!(released.as_mut().poll(&mut callback_context).is_pending());
            callback_wake.0.store(false, Ordering::SeqCst);
            drop(last);
            assert!(callback_wake.0.swap(false, Ordering::SeqCst));
            assert_eq!(
                released.as_mut().poll(&mut callback_context),
                Poll::Ready(WaitStatus::Signalled)
            );
            callbacks.acknowledge_retirement();
            assert!(receipt.as_mut().poll(&mut receipt_context).is_pending());

            if !detach_before_zero {
                assert!(!owner.ready());
                let mut detached = Box::pin(callbacks.wait(CancellationToken::never()));
                assert!(detached.as_mut().poll(&mut callback_context).is_pending());
                tracker.mark_detached();
                assert!(callback_wake.0.swap(false, Ordering::SeqCst));
                assert_eq!(
                    detached.as_mut().poll(&mut callback_context),
                    Poll::Ready(WaitStatus::Signalled)
                );
                callbacks.acknowledge_retirement();
            }
            if !driver_before_zero {
                assert!(!owner.ready());
                tracker.finish_driver();
            }
            assert!(owner.ready());
            receipt_wake.0.store(false, Ordering::SeqCst);
            owner.publish();
            assert!(receipt_wake.0.load(Ordering::SeqCst));
            assert_eq!(
                receipt.as_mut().poll(&mut receipt_context),
                Poll::Ready(Ok(()))
            );
        }
    }
}

#[test]
fn exchange_completion_supports_multiple_and_cancelled_waiters() {
    struct WakeFlag(AtomicBool);

    impl Wake for WakeFlag {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    for publish in [false, true] {
        let owner = ExchangeOwner::new(Rc::new(|| {}), false);
        let tracker = owner.tracker();
        let completion = owner.completion();
        let mut cancelled = completion.wait();
        assert!(poll_once(cancelled.as_mut()).is_pending());
        drop(cancelled);

        let first_wake = Arc::new(WakeFlag(AtomicBool::new(false)));
        let second_wake = Arc::new(WakeFlag(AtomicBool::new(false)));
        let first_waker = Waker::from(Arc::clone(&first_wake));
        let second_waker = Waker::from(Arc::clone(&second_wake));
        let mut first = completion.wait();
        let mut second = completion.clone().wait();
        let mut unpolled = completion.wait();
        assert!(
            first
                .as_mut()
                .poll(&mut Context::from_waker(&first_waker))
                .is_pending()
        );
        assert!(
            second
                .as_mut()
                .poll(&mut Context::from_waker(&second_waker))
                .is_pending()
        );
        let expected = if publish {
            tracker.mark_detached();
            owner.publish();
            Ok(())
        } else {
            drop(owner);
            Err(ExchangeCompletionError::CleanupUnconfirmed)
        };
        assert!(first_wake.0.load(Ordering::SeqCst));
        assert!(second_wake.0.load(Ordering::SeqCst));
        assert_eq!(poll_once(first.as_mut()), Poll::Ready(expected));
        assert_eq!(poll_once(second.as_mut()), Poll::Ready(expected));
        assert_eq!(poll_once(unpolled.as_mut()), Poll::Ready(expected));
        assert_eq!(poll_once(completion.wait().as_mut()), Poll::Ready(expected));
    }
}

#[test]
fn exchange_completion_can_be_observed_during_notification() {
    thread_local! {
        static RECEIPT: RefCell<Option<ExchangeCompletion>> = const { RefCell::new(None) };
        static OBSERVED: Cell<Option<Result<(), ExchangeCompletionError>>> = const { Cell::new(None) };
        static PANICKED: Cell<bool> = const { Cell::new(false) };
    }

    struct ReentrantWake;

    impl Wake for ReentrantWake {
        fn wake(self: Arc<Self>) {
            let result = catch_unwind(AssertUnwindSafe(|| {
                let receipt =
                    RECEIPT.with(|slot| slot.borrow().as_ref().expect("test receipt").clone());
                let Poll::Ready(result) = poll_once(receipt.wait().as_mut()) else {
                    panic!("publication must be visible while waking a waiter");
                };
                OBSERVED.with(|observed| observed.set(Some(result)));
            }));
            PANICKED.with(|panicked| panicked.set(panicked.get() || result.is_err()));
        }
    }

    for publish in [false, true] {
        let owner = ExchangeOwner::new(Rc::new(|| {}), false);
        let completion = owner.completion();
        RECEIPT.with(|slot| *slot.borrow_mut() = Some(completion.clone()));
        OBSERVED.with(|observed| observed.set(None));
        PANICKED.with(|panicked| panicked.set(false));
        let waker = Waker::from(Arc::new(ReentrantWake));
        let mut receipt = completion.wait();
        assert!(
            receipt
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let expected = if publish {
            owner.tracker().mark_detached();
            owner.publish();
            Ok(())
        } else {
            drop(owner);
            Err(ExchangeCompletionError::CleanupUnconfirmed)
        };
        RECEIPT.with(|slot| slot.borrow_mut().take());
        PANICKED.with(|panicked| assert!(!panicked.get(), "observer reentry panicked"));
        OBSERVED.with(|observed| assert_eq!(observed.get(), Some(expected)));
        assert_eq!(poll_once(receipt.as_mut()), Poll::Ready(expected));
    }
}

#[test]
fn exchange_completion_waits_for_retirement() {
    run_local(async {
        let owner = ExchangeOwner::new(Rc::new(|| {}), false);
        let tracker = owner.tracker();
        let receipt = owner.completion();
        let dropped = Rc::new(Cell::new(false));
        let read_dropped = Rc::new(Cell::new(false));
        let stopped = Rc::new(Cell::new(0));
        let mut source = tracker.source(HttpSourceHolder::new(Box::new(PendingSource {
            dropped: Rc::clone(&dropped),
            read_dropped: Rc::clone(&read_dropped),
            stopped: Rc::clone(&stopped),
            panic_stop: false,
            ready: false,
            owner: std::thread::current().id(),
        })));
        let mut read = source.read_body_event(1);
        assert!(poll_once(read.as_mut()).is_pending());
        tracker.mark_detached();
        drop(source);
        assert!(dropped.get());
        assert_eq!(stopped.get(), 1);
        assert!(!owner.ready());
        pending(&receipt);
        drop(read);
        assert!(read_dropped.get());
        assert!(owner.ready());
        owner.publish();
        assert_eq!(completed(&receipt).await, Ok(()));
        assert_eq!(completed(&receipt.clone()).await, Ok(()));

        let owner = ExchangeOwner::new(Rc::new(|| {}), false);
        let tracker = owner.tracker();
        let receipt = owner.completion();
        let read_dropped = Rc::new(Cell::new(false));
        let mut source = tracker.source(HttpSourceHolder::new(Box::new(PendingSource {
            dropped: Rc::new(Cell::new(false)),
            read_dropped: Rc::clone(&read_dropped),
            stopped: Rc::new(Cell::new(0)),
            panic_stop: false,
            ready: true,
            owner: std::thread::current().id(),
        })));
        let mut read = source.read_body_event(1);
        tracker.mark_detached();
        assert!(poll_once(read.as_mut()).is_ready());
        assert!(read_dropped.get());
        assert!(owner.ready());
        owner.publish();
        assert_eq!(completed(&receipt).await, Ok(()));
        drop(read);
        drop(source);

        let lost_owner = ExchangeOwner::new(Rc::new(|| {}), false);
        let lost = lost_owner.completion();
        let still_owned = lost_owner.tracker().guard();
        drop(lost_owner);
        assert_eq!(
            completed(&lost).await,
            Err(ExchangeCompletionError::CleanupUnconfirmed)
        );
        drop(still_owned);

        let (mut session, _driver) = new_session(CodecProtocol::Http2);
        let stream = StreamId::new(1);
        let mut request = session.begin_incoming_stream(stream).unwrap();
        let receipt = request.completion().clone();
        session
            .ingress_headers(stream, request_message(), true)
            .unwrap();
        request.source_mut().read_header_event().await.unwrap();
        drop(request);
        let old = session
            .install_egress_source(stream, response(), true)
            .unwrap();
        let mut transfer = session
            .install_egress_source(stream, response(), true)
            .unwrap();
        drain_transfer(&mut session, stream, &mut transfer).await;
        session.process_pending_events().unwrap();
        pending(&receipt);
        drop(old);
        session.process_pending_events().unwrap();
        assert_eq!(session.num_transactions(), 0);
        pending(&receipt);
        drop(transfer);
        session.process_pending_events().unwrap();
        assert_eq!(completed(&receipt).await, Ok(()));

        let stream = StreamId::new(3);
        let request = session.begin_incoming_stream(stream).unwrap();
        let receipt = request.completion().clone();
        session
            .ingress_headers(stream, request_message(), true)
            .unwrap();
        let started = Rc::new(Notify::new());
        let release = Rc::new(Notify::new());
        let observed = Rc::new(Cell::new(false));
        session
            .start_request_handler(
                request,
                Rc::new(WaitingHandler {
                    started: Rc::clone(&started),
                    release: Rc::clone(&release),
                    observed: Rc::clone(&observed),
                }),
            )
            .unwrap();
        time::timeout(Duration::from_secs(2), started.notified())
            .await
            .unwrap();
        session.egress_finished(stream).unwrap();
        session.process_pending_events().unwrap();
        pending(&receipt);
        assert!(observed.get());
        assert_eq!(session.pending_handler_task_count(), 1);
        release.notify_one();
        time::timeout(Duration::from_secs(2), async {
            while !session.has_pending_handler_completion() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        pending(&receipt);
        assert_eq!(session.pending_handler_task_count(), 1);
        session.process_pending_events().unwrap();
        assert_eq!(session.pending_handler_task_count(), 0);
        assert_eq!(completed(&receipt).await, Ok(()));
        session.finalize().await.unwrap();
    });
}

struct EchoTransport {
    incoming: Rc<RefCell<VecDeque<BufQueue>>>,
    wake: Rc<Notify>,
    closed: Cell<bool>,
    writes: Cell<usize>,
}

impl UniplexTransport for EchoTransport {
    fn read(
        &self,
        _context: &WorkerContext,
        _maximum: usize,
        _allocation: usize,
        _timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        let incoming = Rc::clone(&self.incoming);
        let wake = Rc::clone(&self.wake);
        Box::pin(async move {
            let baton = CancellableBaton::new();
            loop {
                let notified = wake.notified();
                if let Some(bytes) = incoming.borrow_mut().pop_front() {
                    return Ok(UniplexRead::new(bytes, false));
                }
                tokio::select! {
                    _ = notified => {},
                    _ = baton.wait(cancellation.clone()) => {
                        return Err(HttpError::with_message(HttpErrorCode::CORO_CANCELLED, b"test read cancelled"));
                    }
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
        let bytes_written = data.chain_length();
        if bytes_written != 0 {
            self.writes.set(self.writes.get() + 1);
            let response = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
            let queue =
                BufQueue::from_bytes(Bytes::copy_from_slice(response), response.len()).unwrap();
            assert!(self.incoming.borrow().len() < 2);
            self.incoming.borrow_mut().push_back(queue);
            self.wake.notify_one();
        }
        Box::pin(std::future::ready(Ok(WriteInfo { bytes_written })))
    }

    fn shutdown_write(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        self.closed.set(true);
        Ok(())
    }

    fn close(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.shutdown_write(context)
    }

    fn close_with_reset(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.shutdown_write(context)
    }

    fn byte_event_capability(
        &self,
        _context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError> {
        Ok(ByteEventCapability::Unsupported)
    }

    fn register_byte_event(
        &self,
        _context: &WorkerContext,
        _registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError> {
        Err(ByteEventRegistrationError::Unsupported)
    }
}

#[test]
fn exchange_completion_is_stream_local() {
    run_local(async {
        let (mut session, _driver) = new_session(CodecProtocol::Http2);
        let first = StreamId::new(1);
        let second = StreamId::new(3);
        let mut request = session.begin_incoming_stream(first).unwrap();
        let receipt = request.completion().clone();
        let parked = session.begin_incoming_stream(second).unwrap();
        let parked_receipt = parked.completion().clone();
        session
            .ingress_headers(first, request_message(), true)
            .unwrap();
        request.source_mut().read_header_event().await.unwrap();
        drop(request);
        session.egress_finished(first).unwrap();
        session.process_pending_events().unwrap();
        assert_eq!(completed(&receipt).await, Ok(()));
        pending(&parked_receipt);
        assert_eq!(session.num_transactions(), 1);
        session
            .abort_stream_without_wire(
                second,
                HttpError::with_message(HttpErrorCode::CANCEL, b"release parked stream"),
            )
            .unwrap();
        drop(parked);
        session.process_pending_events().unwrap();
        assert_eq!(completed(&parked_receipt).await, Ok(()));
        session.finalize().await.unwrap();

        let (worker, driver) = WorkerDriver::new(32).unwrap();
        let call = worker
            .call_local(|context| async move {
                let transport = Rc::new(EchoTransport {
                    incoming: Rc::new(RefCell::new(VecDeque::new())),
                    wake: Rc::new(Notify::new()),
                    closed: Cell::new(false),
                    writes: Cell::new(0),
                });
                let (session, handle) = UniplexSession::new(
                    context,
                    TransportDirection::Upstream,
                    transport.clone(),
                    None,
                    UniplexConfig::default(),
                )
                .unwrap();
                let requests = async {
                    for _ in 0..2 {
                        let source = HttpSourceHolder::new(Box::new(HttpFixedSource::new(
                            request_message(),
                            Some(BufQueue::new(0)),
                        )));
                        let mut response = handle
                            .send_request(source, CancellationToken::never())
                            .await
                            .unwrap();
                        pending(&response.completion);
                        let mut headers = response.source.read_header_event().await.unwrap();
                        assert_eq!(headers.headers_mut().status_code().unwrap(), 200);
                        if !headers.eom() {
                            assert!(response.source.read_body_event(1).await.unwrap().eom());
                        }
                        assert_eq!(completed(&response.completion).await, Ok(()));
                        assert!(!transport.closed.get());
                    }
                    assert_eq!(transport.writes.get(), 2);
                    handle.initiate_drain().unwrap();
                };
                let (run, ()) = time::timeout(Duration::from_secs(5), async {
                    tokio::join!(session.run(), requests)
                })
                .await
                .unwrap();
                run.unwrap();
            })
            .unwrap();
        let driver = tokio::task::spawn_local(driver);
        call.await.unwrap();
        worker.begin_shutdown();
        driver.await.unwrap();
    });
}

struct DetachObserver {
    receipt: ExchangeCompletion,
    calls: Rc<Cell<usize>>,
    panic: bool,
}

struct FailingHandler(bool);

impl HttpHandler for FailingHandler {
    fn handle_request(
        &self,
        context: HandlerSessionContext,
        source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let panic = self.0;
        Box::pin(async move {
            drop(source);
            drop(context);
            assert!(!panic, "scripted handler panic");
            Err(HttpError::with_message(
                HttpErrorCode::INTERNAL_ERROR,
                b"scripted handler failure",
            ))
        })
    }
}

impl LifecycleObserver for DetachObserver {
    fn on_event(&self, event: LifecycleEvent) {
        if matches!(event, LifecycleEvent::TransactionDetached { .. }) {
            pending(&self.receipt);
            self.calls.set(self.calls.get() + 1);
            assert!(!self.panic, "scripted detach panic");
        }
    }
}

#[derive(Default)]
struct DriverFixture {
    incoming: RefCell<VecDeque<BufQueue>>,
    read_wake: Notify,
    hold_source: Cell<bool>,
    source_waiting: Cell<bool>,
    source_release: Notify,
    hold_write: Cell<bool>,
    write_waiting: Cell<bool>,
    write_release: Notify,
    write_created: Cell<usize>,
    write_dropped: Cell<usize>,
    shutdown_on_write: Cell<bool>,
    shutdown_write_created: Cell<usize>,
    shutdown_write_polls: Cell<usize>,
    shutdown_write_dropped: Cell<usize>,
    written: Cell<u64>,
    write_error: Cell<Option<HttpErrorCode>>,
    registrations: RefCell<Vec<HttpByteEventRegistration>>,
    receipts: RefCell<Vec<ExchangeCompletion>>,
    callback_calls: Cell<usize>,
    callback_error: Cell<Option<HttpErrorCode>>,
    source_dropped: Cell<usize>,
    closed: Cell<bool>,
}

impl DriverFixture {
    fn receive(&self, bytes: BufQueue) {
        assert!(self.incoming.borrow().len() < 2);
        self.incoming.borrow_mut().push_back(bytes);
        self.read_wake.notify_one();
    }

    fn receipt(&self, index: usize) -> ExchangeCompletion {
        self.receipts.borrow()[index].clone()
    }

    fn acknowledge(&self) {
        let mut registration = self.registrations.borrow_mut().pop().unwrap();
        let mut event = HttpByteEvent::new(ByteEventType::CumulativeAck);
        event.transport_offset = self.written.get();
        registration.fire(event);
    }
}

struct DriverTransport(Rc<DriverFixture>);

struct WriteRetirement {
    fixture: Rc<DriverFixture>,
    owner: ThreadId,
}

impl Drop for WriteRetirement {
    fn drop(&mut self) {
        assert_eq!(self.owner, std::thread::current().id());
        self.fixture
            .write_dropped
            .set(self.fixture.write_dropped.get() + 1);
    }
}

struct ShutdownWrite {
    data: BufQueue,
    worker: WorkerHandle,
    retirement: WriteRetirement,
}

impl Future for ShutdownWrite {
    type Output = Result<WriteInfo, TransportWriteError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        assert_eq!(self.retirement.owner, std::thread::current().id());
        assert!(!self.data.is_empty());
        let fixture = &self.retirement.fixture;
        let polls = fixture.shutdown_write_polls.get();
        assert!(
            polls < 32,
            "terminal observer error spun inside the write wait"
        );
        assert_eq!(fixture.shutdown_write_created.get(), 1);
        assert_eq!(fixture.shutdown_write_dropped.get(), 0);
        fixture.shutdown_write_polls.set(polls + 1);
        if polls == 0 {
            pending(&fixture.receipt(1));
            assert!(self.worker.begin_shutdown());
            context.waker().wake_by_ref();
        }
        Poll::Pending
    }
}

impl Drop for ShutdownWrite {
    fn drop(&mut self) {
        assert_eq!(self.retirement.owner, std::thread::current().id());
        assert_eq!(self.worker.state(), WorkerState::Closing);
        let fixture = &self.retirement.fixture;
        assert!(fixture.shutdown_write_polls.get() > 0);
        assert_eq!(fixture.shutdown_write_dropped.replace(1), 0);
    }
}

impl UniplexTransport for DriverTransport {
    fn read(
        &self,
        context: &WorkerContext,
        _maximum: usize,
        _allocation: usize,
        _timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        assert!(context.is_owner_thread());
        let fixture = Rc::clone(&self.0);
        Box::pin(async move {
            let baton = CancellableBaton::new();
            loop {
                let notified = fixture.read_wake.notified();
                if let Some(data) = fixture.incoming.borrow_mut().pop_front() {
                    return Ok(UniplexRead::new(data, false));
                }
                tokio::select! {
                    _ = notified => {},
                    _ = baton.wait(cancellation.clone()) => {
                        return Err(HttpError::new(HttpErrorCode::CORO_CANCELLED));
                    },
                }
            }
        })
    }

    fn write(
        &self,
        context: &WorkerContext,
        data: BufQueue,
        _flags: ByteEventWriteFlags,
        _timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        assert!(context.is_owner_thread());
        let fixture = Rc::clone(&self.0);
        fixture.write_created.set(fixture.write_created.get() + 1);
        let retirement = WriteRetirement {
            fixture: Rc::clone(&fixture),
            owner: std::thread::current().id(),
        };
        if fixture.shutdown_on_write.get() {
            fixture
                .shutdown_write_created
                .set(fixture.shutdown_write_created.get() + 1);
            return Box::pin(ShutdownWrite {
                data,
                worker: context.handle().clone(),
                retirement,
            });
        }
        Box::pin(async move {
            let _retirement = retirement;
            if fixture.hold_write.get() {
                fixture.write_waiting.set(true);
                let baton = CancellableBaton::new();
                tokio::select! {
                    _ = fixture.write_release.notified() => {},
                    _ = baton.wait(cancellation) => {
                        return Err(TransportWriteError::new(HttpError::new(HttpErrorCode::CORO_CANCELLED), 0));
                    },
                }
            }
            if let Some(code) = fixture.write_error.take() {
                return Err(TransportWriteError::new(HttpError::new(code), 0));
            }
            let bytes_written = data.chain_length();
            fixture
                .written
                .set(fixture.written.get() + bytes_written as u64);
            Ok(WriteInfo { bytes_written })
        })
    }

    fn shutdown_write(&self, context: &WorkerContext) -> Result<(), HttpError> {
        assert!(context.is_owner_thread());
        self.0.closed.set(true);
        Ok(())
    }

    fn close(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.shutdown_write(context)
    }

    fn close_with_reset(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.shutdown_write(context)
    }

    fn byte_event_capability(
        &self,
        context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError> {
        assert!(context.is_owner_thread());
        Ok(ByteEventCapability::TransportEvents)
    }

    fn register_byte_event(
        &self,
        context: &WorkerContext,
        registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError> {
        assert!(context.is_owner_thread());
        assert!(self.0.registrations.borrow().is_empty());
        self.0.registrations.borrow_mut().push(registration);
        Ok(())
    }
}

struct ReceiptCallback {
    fixture: Rc<DriverFixture>,
    owner: ThreadId,
}

impl ReceiptCallback {
    fn called(&self, event: HttpByteEvent, error: Option<HttpErrorCode>) {
        assert_eq!(self.owner, std::thread::current().id());
        if event.stream_id == Some(StreamId::new(3)) {
            pending(&self.fixture.receipt(1));
            return;
        }
        assert_eq!(event.stream_id, Some(StreamId::new(1)));
        pending(&self.fixture.receipt(0));
        self.fixture
            .callback_calls
            .set(self.fixture.callback_calls.get() + 1);
        self.fixture.callback_error.set(error);
    }
}

impl HttpByteEventCallback for ReceiptCallback {
    fn on_byte_event(&self, event: HttpByteEvent) {
        self.called(event, None);
    }

    fn on_byte_event_cancelled(&self, event: HttpByteEvent, error: HttpError) {
        self.called(event, Some(error.code()));
    }
}

struct DriverResponse {
    inner: Rc<RefCell<HttpFixedSource>>,
    fixture: Rc<DriverFixture>,
    callback: Option<Rc<ReceiptCallback>>,
    event_type: ByteEventType,
    blocked: bool,
    owner: ThreadId,
}

impl HttpSource for DriverResponse {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let inner = Rc::clone(&self.inner);
        let fixture = Rc::clone(&self.fixture);
        let callback = self.callback.clone();
        let event_type = self.event_type;
        let blocked = self.blocked;
        Box::pin(async move {
            if blocked && fixture.hold_source.get() {
                fixture.source_waiting.set(true);
                fixture.source_release.notified().await;
            }
            let read = inner.borrow_mut().read_header_event();
            let mut header = read.await?;
            if let Some(callback) = callback {
                header
                    .byte_event_registrations_mut()
                    .push(HttpByteEventRegistration::new(
                        None,
                        event_type.bit(),
                        &callback,
                    ));
            }
            Ok(header)
        })
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.inner.borrow_mut().read_body_event(maximum)
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.inner.borrow_mut().stop_reading(error);
    }
}

impl Drop for DriverResponse {
    fn drop(&mut self) {
        assert_eq!(self.owner, std::thread::current().id());
        self.fixture
            .source_dropped
            .set(self.fixture.source_dropped.get() + 1);
    }
}

struct DriverHandler {
    fixture: Rc<DriverFixture>,
    callback: Rc<ReceiptCallback>,
}

impl HttpHandler for DriverHandler {
    fn handle_request(
        &self,
        context: HandlerSessionContext,
        mut source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let fixture = Rc::clone(&self.fixture);
        let first = fixture.receipts.borrow().is_empty();
        assert!(fixture.receipts.borrow().len() < 2);
        fixture
            .receipts
            .borrow_mut()
            .push(context.exchange_completion().unwrap().clone());
        let callback = Some(Rc::clone(&self.callback));
        Box::pin(async move {
            assert!(source.read_header_event().await?.eom());
            drop(source);
            drop(context);
            Ok(HttpSourceHolder::new(Box::new(DriverResponse {
                inner: Rc::new(RefCell::new(
                    HttpFixedSource::make_fixed_response(200, None).unwrap(),
                )),
                blocked: !first && fixture.hold_source.get(),
                fixture,
                callback,
                event_type: if first {
                    ByteEventType::CumulativeAck
                } else {
                    ByteEventType::KernelWrite
                },
                owner: std::thread::current().id(),
            })))
        })
    }
}

struct IgnorePeerEvents;

impl HttpCodecCallback for IgnorePeerEvents {
    fn on_event(&mut self, _event: CodecEvent<'_>) {}
}

fn peer_request(codec: &mut Http2Codec, preface: bool) -> BufQueue {
    let mut output = BufQueue::new(4096);
    if preface {
        codec
            .generate(
                &mut output,
                EgressFrame::ConnectionPreface,
                &mut IgnorePeerEvents,
            )
            .unwrap();
        codec
            .generate_configured_settings(&mut output, &mut IgnorePeerEvents)
            .unwrap();
    }
    let stream = codec.create_stream();
    codec
        .generate(
            &mut output,
            EgressFrame::Headers {
                stream,
                message: &request_message(),
                end_stream: true,
                extra_headers: None,
            },
            &mut IgnorePeerEvents,
        )
        .unwrap();
    output
}

async fn until(mut ready: impl FnMut() -> bool) {
    for _ in 0..10_000 {
        if ready() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("native driver did not reach fixture boundary");
}

#[test]
fn exchange_completion_h2_progress_inside_source_and_write_waits() {
    run_local(async {
        let (worker, driver) = WorkerDriver::new(32).unwrap();
        let call = worker
            .call_local(|context| async move {
                time::pause();
                for source_wait in [true, false] {
                    for timeout in [false, true] {
                        let fixture = Rc::new(DriverFixture::default());
                        let callback = Rc::new(ReceiptCallback {
                            fixture: Rc::clone(&fixture),
                            owner: std::thread::current().id(),
                        });
                        let transport = Rc::new(DriverTransport(Rc::clone(&fixture)));
                        let handler = Rc::new(DriverHandler {
                            fixture: Rc::clone(&fixture),
                            callback,
                        });
                        let (session, handle) = UniplexSession::new_http2(
                            context.clone(),
                            TransportDirection::Downstream,
                            transport,
                            Some(handler),
                            UniplexConfig::default(),
                            Http2SessionConfig {
                                double_goaway_drain: false,
                                ..Http2SessionConfig::default()
                            },
                        )
                        .unwrap();
                        session
                            .set_byte_event_timeout(Duration::from_millis(10))
                            .unwrap();
                        let mut codec = Http2Codec::new(TransportDirection::Upstream);
                        fixture.receive(peer_request(&mut codec, true));
                        let control = async {
                            until(|| {
                                fixture.source_dropped.get() == 1
                                    && !fixture.registrations.borrow().is_empty()
                                    && fixture.write_created.get() == fixture.write_dropped.get()
                            })
                            .await;
                            let first = fixture.receipt(0);
                            pending(&first);
                            assert_eq!(fixture.callback_calls.get(), 0);
                            fixture.hold_source.set(source_wait);
                            fixture.hold_write.set(!source_wait);
                            fixture.receive(peer_request(&mut codec, false));
                            until(|| {
                                if source_wait {
                                    fixture.source_waiting.get()
                                } else {
                                    fixture.write_waiting.get()
                                }
                            })
                            .await;
                            let second = fixture.receipt(1);
                            pending(&first);
                            pending(&second);
                            let created = fixture.write_created.get();
                            let dropped = fixture.write_dropped.get();
                            if timeout {
                                time::advance(Duration::from_millis(20)).await;
                            } else {
                                fixture.acknowledge();
                            }
                            assert_eq!(completed(&first).await, Ok(()));
                            assert_eq!(fixture.callback_calls.get(), 1);
                            if timeout {
                                assert_eq!(
                                    fixture.callback_error.get(),
                                    Some(HttpErrorCode::READ_TIMEOUT)
                                );
                            } else {
                                assert_eq!(fixture.callback_error.get(), None);
                            }
                            pending(&second);
                            assert!(!fixture.closed.get());
                            if !source_wait {
                                assert_eq!(fixture.write_created.get(), created);
                                assert_eq!(fixture.write_dropped.get(), dropped);
                                assert_eq!(created, dropped + 1);
                            }
                            fixture.hold_write.set(false);
                            fixture.hold_source.set(false);
                            fixture.source_release.notify_one();
                            fixture.write_release.notify_one();
                            assert_eq!(completed(&second).await, Ok(()));
                            assert_eq!(fixture.source_dropped.get(), 2);
                            handle.initiate_drain().unwrap();
                        };
                        let (run, ()) = time::timeout(Duration::from_secs(120), async {
                            tokio::join!(session.run(), control)
                        })
                        .await
                        .unwrap();
                        run.unwrap();
                    }
                }
            })
            .unwrap();
        let driver = tokio::task::spawn_local(driver);
        call.await.unwrap();
        worker.begin_shutdown();
        driver.await.unwrap();
    });
}

struct ReturnedSourceHandler {
    response: RefCell<Option<HttpSourceHolder>>,
    request: Rc<RefCell<Option<HttpSourceHolder>>>,
}

#[derive(Clone, Copy)]
enum DriverFinish {
    WriteFailure,
    CancelBeforeAck,
    AckBeforeCancel,
}

#[test]
fn exchange_completion_native_failure_and_final_response_cancel_race() {
    run_local(async {
        let (worker, driver) = WorkerDriver::new(32).unwrap();
        let call = worker.call_local(|context| async move {
            for finish in [DriverFinish::WriteFailure, DriverFinish::CancelBeforeAck, DriverFinish::AckBeforeCancel] {
                let fixture = Rc::new(DriverFixture::default());
                let callback = Rc::new(ReceiptCallback { fixture: Rc::clone(&fixture), owner: std::thread::current().id() });
                let handler = Rc::new(DriverHandler { fixture: Rc::clone(&fixture), callback });
                let (session, handle) = UniplexSession::new_http2(
                    context.clone(), TransportDirection::Downstream,
                    Rc::new(DriverTransport(Rc::clone(&fixture))), Some(handler),
                    UniplexConfig::default(), Http2SessionConfig { double_goaway_drain: false, ..Http2SessionConfig::default() },
                ).unwrap();
                let mut codec = Http2Codec::new(TransportDirection::Upstream);
                fixture.receive(peer_request(&mut codec, true));
                let mut run = session.run();
                let control = async {
                    until(|| fixture.source_dropped.get() == 1 && !fixture.registrations.borrow().is_empty() && fixture.write_created.get() == fixture.write_dropped.get()).await;
                    let first = fixture.receipt(0);
                    pending(&first);
                    fixture.hold_write.set(matches!(finish, DriverFinish::WriteFailure));
                    fixture.hold_source.set(!matches!(finish, DriverFinish::WriteFailure));
                    fixture.receive(peer_request(&mut codec, false));
                    until(|| fixture.source_waiting.get() || fixture.write_waiting.get()).await;
                    let second = fixture.receipt(1);
                    pending(&first);
                    pending(&second);
                    (first, second)
                };
                let (first, second) = tokio::select! {
                    result = &mut run => panic!("session finished before held exchange: {result:?}"),
                    receipts = control => receipts,
                };
                match finish {
                    DriverFinish::WriteFailure => {
                        fixture.write_error.set(Some(HttpErrorCode::TRANSPORT_WRITE_ERROR));
                        fixture.hold_write.set(false);
                        fixture.write_release.notify_one();
                        pending(&first);
                        pending(&second);
                        let error = time::timeout(Duration::from_secs(2), run).await.unwrap().err().unwrap();
                        assert_eq!(error.code(), HttpErrorCode::TRANSPORT_WRITE_ERROR);
                        assert_eq!(fixture.callback_error.get(), Some(HttpErrorCode::TRANSPORT_WRITE_ERROR));
                    }
                    DriverFinish::CancelBeforeAck | DriverFinish::AckBeforeCancel => {
                        if matches!(finish, DriverFinish::AckBeforeCancel) {
                            fixture.acknowledge();
                            pending(&first);
                        }
                        drop(run);
                        pending(&first);
                        pending(&second);
                        assert_eq!(completed(&first).await, Ok(()));
                        if matches!(finish, DriverFinish::CancelBeforeAck) {
                            assert_eq!(fixture.callback_error.get(), Some(HttpErrorCode::CORO_CANCELLED));
                        } else {
                            assert_eq!(fixture.callback_error.get(), None);
                        }
                    }
                }
                assert_eq!(completed(&first).await, Ok(()));
                assert_eq!(completed(&second).await, Ok(()));
                until(|| handle.is_finished()).await;
                assert_eq!(fixture.callback_calls.get(), 1);
                assert_eq!(fixture.source_dropped.get(), 2);
                assert_eq!(fixture.write_created.get(), fixture.write_dropped.get());
                assert!(fixture.closed.get());
            }
        }).unwrap();
        let driver = tokio::task::spawn_local(driver);
        call.await.unwrap();
        worker.begin_shutdown();
        driver.await.unwrap();
    });
}

#[test]
fn exchange_completion_native_finalization_reports_unconfirmed_retained_read() {
    run_local(async {
        let (mut session, _driver) = new_session(CodecProtocol::Http2);
        let stream = StreamId::new(1);
        let mut request = session.begin_incoming_stream(stream).unwrap();
        let receipt = request.completion().clone();
        session
            .ingress_headers(stream, request_message(), false)
            .unwrap();
        request.source_mut().read_header_event().await.unwrap();
        let read = request.source_mut().read_body_event(1);
        pending(&receipt);
        session
            .abort_stream_without_wire(stream, HttpError::new(HttpErrorCode::CANCEL))
            .unwrap();
        drop(request);
        session.process_pending_events().unwrap();
        assert_eq!(session.num_transactions(), 0);
        pending(&receipt);
        session.finalize().await.unwrap();
        assert_eq!(
            completed(&receipt).await,
            Err(ExchangeCompletionError::CleanupUnconfirmed)
        );
        assert_eq!(read.await.err().unwrap().code(), HttpErrorCode::CANCEL);
        assert_eq!(
            completed(&receipt).await,
            Err(ExchangeCompletionError::CleanupUnconfirmed)
        );
    });
}

struct ShutdownRun {
    fixture: Rc<DriverFixture>,
    run: LocalSessionFuture<Result<UniplexRunSummary, HttpError>>,
}

std::thread_local! {
    static SHUTDOWN_RUN: RefCell<Option<ShutdownRun>> = const { RefCell::new(None) };
}

#[test]
fn exchange_completion_shutdown_inside_pending_write_poll_joins() {
    run_local(async {
        for _ in 0..8 {
            let (worker, driver) = WorkerDriver::new(32).unwrap();
            let call = worker
                .call_local(|context| async move {
                    let fixture = Rc::new(DriverFixture::default());
                    let callback = Rc::new(ReceiptCallback {
                        fixture: Rc::clone(&fixture),
                        owner: std::thread::current().id(),
                    });
                    let handler = Rc::new(DriverHandler {
                        fixture: Rc::clone(&fixture),
                        callback,
                    });
                    let (session, _) = UniplexSession::new_http2(
                        context,
                        TransportDirection::Downstream,
                        Rc::new(DriverTransport(Rc::clone(&fixture))),
                        Some(handler),
                        UniplexConfig::default(),
                        Http2SessionConfig {
                            double_goaway_drain: false,
                            ..Http2SessionConfig::default()
                        },
                    )
                    .unwrap();
                    let mut codec = Http2Codec::new(TransportDirection::Upstream);
                    fixture.receive(peer_request(&mut codec, true));
                    let mut run = session.run();
                    poll_fn(|task_context| {
                        assert!(run.as_mut().poll(task_context).is_pending());
                        Poll::Ready(())
                    })
                    .await;
                    until(|| {
                        fixture.source_dropped.get() == 1
                            && !fixture.registrations.borrow().is_empty()
                            && fixture.write_created.get() == fixture.write_dropped.get()
                    })
                    .await;
                    let first = fixture.receipt(0);
                    pending(&first);
                    fixture.acknowledge();
                    assert_eq!(completed(&first).await, Ok(()));
                    fixture.shutdown_on_write.set(true);
                    fixture.receive(peer_request(&mut codec, false));
                    SHUTDOWN_RUN.with(|slot| {
                        assert!(slot.borrow().is_none());
                        *slot.borrow_mut() = Some(ShutdownRun { fixture, run });
                    });
                })
                .unwrap();
            let (_, result) =
                time::timeout(Duration::from_secs(5), async { tokio::join!(driver, call) })
                    .await
                    .expect("shutdown joins instead of spinning in the retained write");
            result.expect("session setup completes before shutdown in transport polling");
            assert_eq!(worker.state(), WorkerState::Closed);
            let ShutdownRun { fixture, run } =
                SHUTDOWN_RUN.with(|slot| slot.borrow_mut().take().unwrap());
            assert_eq!(fixture.shutdown_write_created.get(), 1);
            assert!(fixture.shutdown_write_polls.get() > 0);
            assert!(fixture.shutdown_write_polls.get() < 32);
            assert_eq!(fixture.shutdown_write_dropped.get(), 1);
            assert_eq!(fixture.write_created.get(), fixture.write_dropped.get());
            assert_eq!(fixture.source_dropped.get(), 2);
            assert!(fixture.closed.get());
            assert_eq!(completed(&fixture.receipt(0)).await, Ok(()));
            assert_eq!(completed(&fixture.receipt(1)).await, Ok(()));
            assert!(run.await.is_err());
            assert_eq!(fixture.shutdown_write_dropped.get(), 1);
        }
    });
}

impl HttpHandler for ReturnedSourceHandler {
    fn handle_request(
        &self,
        context: HandlerSessionContext,
        source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let response = self.response.borrow_mut().take().unwrap();
        let request = Rc::clone(&self.request);
        Box::pin(async move {
            *request.borrow_mut() = Some(source);
            drop(context);
            Ok(response)
        })
    }
}

#[test]
fn exchange_completion_discarded_handler_source_is_fenced() {
    run_local(async {
        let (mut session, _driver) = new_session(CodecProtocol::Http2);
        let stream = StreamId::new(1);
        let request = session.begin_incoming_stream(stream).unwrap();
        let receipt = request.completion().clone();
        session
            .ingress_headers(stream, request_message(), true)
            .unwrap();
        let dropped = Rc::new(Cell::new(false));
        let stopped = Rc::new(Cell::new(0));
        let response = HttpSourceHolder::new(Box::new(PendingSource {
            dropped: Rc::clone(&dropped),
            read_dropped: Rc::new(Cell::new(false)),
            stopped: Rc::clone(&stopped),
            panic_stop: true,
            ready: false,
            owner: std::thread::current().id(),
        }));
        let held_request = Rc::new(RefCell::new(None));
        session
            .start_request_handler(
                request,
                Rc::new(ReturnedSourceHandler {
                    response: RefCell::new(Some(response)),
                    request: Rc::clone(&held_request),
                }),
            )
            .unwrap();
        session.egress_finished(stream).unwrap();
        until(|| session.has_pending_handler_completion()).await;
        pending(&receipt);
        assert!(!dropped.get());
        assert!(catch_unwind(AssertUnwindSafe(|| session.process_pending_events())).is_err());
        assert!(dropped.get());
        assert_eq!(stopped.get(), 1);
        pending(&receipt);
        session
            .abort_stream_without_wire(stream, HttpError::new(HttpErrorCode::CANCEL))
            .unwrap();
        drop(held_request.borrow_mut().take());
        session.process_pending_events().unwrap();
        assert_eq!(session.pending_handler_task_count(), 0);
        assert_eq!(
            completed(&receipt).await,
            Err(ExchangeCompletionError::CleanupUnconfirmed)
        );
        session.finalize().await.unwrap();
    });
}

struct PanickingPayload;

impl Drop for PanickingPayload {
    fn drop(&mut self) {
        panic!("suppressed panic payload destructor");
    }
}

struct PanickingRead {
    dropped: Rc<Cell<bool>>,
    panic_poll: bool,
    owner: ThreadId,
}

impl Future for PanickingRead {
    type Output = Result<HttpBodyEvent, HttpError>;

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.panic_poll {
            std::panic::panic_any(PanickingPayload);
        }
        Poll::Pending
    }
}

impl Drop for PanickingRead {
    fn drop(&mut self) {
        assert_eq!(self.owner, std::thread::current().id());
        self.dropped.set(true);
        std::panic::panic_any(PanickingPayload);
    }
}

struct PanickingSource {
    dropped: Rc<Cell<bool>>,
    read_dropped: Rc<Cell<bool>>,
    panic_poll: bool,
    owner: ThreadId,
}

impl HttpSource for PanickingSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::pending())
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(PanickingRead {
            dropped: Rc::clone(&self.read_dropped),
            panic_poll: self.panic_poll,
            owner: self.owner,
        })
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

impl Drop for PanickingSource {
    fn drop(&mut self) {
        assert_eq!(self.owner, std::thread::current().id());
        self.dropped.set(true);
        std::panic::panic_any(PanickingPayload);
    }
}

#[test]
fn exchange_completion_panic_payloads_cannot_abandon_retirement() {
    run_local(async {
        let wake_panics = Rc::new(Cell::new(false));
        let wake = Rc::clone(&wake_panics);
        let owner = ExchangeOwner::new(
            Rc::new(move || {
                if wake.get() {
                    std::panic::panic_any(PanickingPayload);
                }
            }),
            false,
        );
        let tracker = owner.tracker();
        let receipt = owner.completion();
        let retirement = tracker.guard();
        tracker.mark_detached();
        wake_panics.set(true);
        pending(&receipt);
        let panic = catch_unwind(AssertUnwindSafe(|| drop(retirement))).unwrap_err();
        assert!(panic.is::<PanickingPayload>());
        roxygen_runtime::panic::discard_panic(panic);
        assert!(owner.ready());
        owner.publish();
        assert_eq!(
            completed(&receipt).await,
            Err(ExchangeCompletionError::CleanupUnconfirmed)
        );
        for unwinding in [false, true] {
            for panic_poll in [false, true] {
                let wake_panics = Rc::new(Cell::new(false));
                let wake = Rc::clone(&wake_panics);
                let owner = ExchangeOwner::new(
                    Rc::new(move || {
                        if wake.get() {
                            std::panic::panic_any(PanickingPayload);
                        }
                    }),
                    false,
                );
                let tracker = owner.tracker();
                let receipt = owner.completion();
                tracker.mark_detached();
                wake_panics.set(true);
                let dropped = Rc::new(Cell::new(false));
                let read_dropped = Rc::new(Cell::new(false));
                let mut source = tracker.source(HttpSourceHolder::new(Box::new(PanickingSource {
                    dropped: Rc::clone(&dropped),
                    read_dropped: Rc::clone(&read_dropped),
                    panic_poll,
                    owner: std::thread::current().id(),
                })));
                let mut read = source.read_body_event(1);
                pending(&receipt);
                if unwinding {
                    let panic = catch_unwind(AssertUnwindSafe(move || {
                        let _source = source;
                        let _read = read;
                        panic!("primary unwind");
                    }))
                    .unwrap_err();
                    assert_eq!(panic.downcast_ref::<&str>(), Some(&"primary unwind"));
                } else {
                    if panic_poll {
                        let panic = catch_unwind(AssertUnwindSafe(|| poll_once(read.as_mut())))
                            .err()
                            .unwrap();
                        assert!(panic.is::<PanickingPayload>());
                        roxygen_runtime::panic::discard_panic(panic);
                        drop(read);
                    } else {
                        assert!(poll_once(read.as_mut()).is_pending());
                        let panic = catch_unwind(AssertUnwindSafe(|| drop(read))).unwrap_err();
                        assert!(panic.is::<PanickingPayload>());
                        roxygen_runtime::panic::discard_panic(panic);
                    }
                    pending(&receipt);
                    let panic = catch_unwind(AssertUnwindSafe(|| drop(source))).unwrap_err();
                    assert!(panic.is::<PanickingPayload>());
                    roxygen_runtime::panic::discard_panic(panic);
                }
                assert!(dropped.get());
                assert!(read_dropped.get());
                assert!(owner.ready());
                owner.publish();
                assert_eq!(
                    completed(&receipt).await,
                    Err(ExchangeCompletionError::CleanupUnconfirmed)
                );
            }
        }
    });
}

#[test]
fn exchange_completion_cancellation_matrix() {
    run_local(async {
        for panic in [false, true] {
            let (mut session, _driver) = new_session(CodecProtocol::Http2);
            let stream = StreamId::new(1);
            let request = session.begin_incoming_stream(stream).unwrap();
            let receipt = request.completion().clone();
            session
                .ingress_headers(stream, request_message(), true)
                .unwrap();
            session
                .start_request_handler(request, Rc::new(FailingHandler(panic)))
                .unwrap();
            time::timeout(Duration::from_secs(2), async {
                while !session.has_pending_handler_completion() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let mut updates = session.process_pending_events().unwrap();
            let mut transfer = updates
                .iter_mut()
                .find_map(|update| update.replacement.take())
                .unwrap();
            pending(&receipt);
            let EgressEvent::Headers(mut headers) = transfer.next_event().await.unwrap().unwrap()
            else {
                panic!("expected native error response headers");
            };
            assert_eq!(headers.headers_mut().status_code().unwrap(), 500);
            session
                .response_headers(stream, true, headers.eom())
                .unwrap();
            drop(headers);
            drain_transfer(&mut session, stream, &mut transfer).await;
            drop(transfer);
            session.process_pending_events().unwrap();
            assert_eq!(session.pending_handler_task_count(), 0);
            assert_eq!(completed(&receipt).await, Ok(()));
            session.finalize().await.unwrap();
        }
        for code in [
            HttpErrorCode::CANCEL,
            HttpErrorCode::READ_TIMEOUT,
            HttpErrorCode::TRANSPORT_WRITE_ERROR,
        ] {
            let (mut session, _driver) = new_session(CodecProtocol::Http2);
            let stream = StreamId::new(1);
            let mut request = session.begin_incoming_stream(stream).unwrap();
            let receipt = request.completion().clone();
            session
                .ingress_headers(stream, request_message(), false)
                .unwrap();
            request.source_mut().read_header_event().await.unwrap();
            let suspend = request.source_mut().read_body_event(1).await.unwrap();
            assert_eq!(suspend.event_type(), HttpBodyEventType::Suspend);
            assert!(!suspend.eom());
            drop(suspend);
            let mut read = request.source_mut().read_body_event(1);
            assert!(poll_once(read.as_mut()).is_pending());
            session
                .abort_stream_without_wire(
                    stream,
                    HttpError::with_message(code, b"scripted cancellation"),
                )
                .unwrap();
            drop(request);
            pending(&receipt);
            assert_eq!(read.await.err().unwrap().code(), code);
            session.process_pending_events().unwrap();
            assert_eq!(completed(&receipt).await, Ok(()));
            session.finalize().await.unwrap();
        }

        let (mut session, _driver) = new_session(CodecProtocol::Http2);
        let stream = StreamId::new(1);
        let request = session.begin_incoming_stream(stream).unwrap();
        let receipt = request.completion().clone();
        session
            .ingress_headers(stream, request_message(), false)
            .unwrap();
        let mut transfer = session
            .install_egress_source(stream, response(), true)
            .unwrap();
        drop(request);
        drop(receipt.wait());
        drain_transfer(&mut session, stream, &mut transfer).await;
        drop(transfer);
        session.process_pending_events().unwrap();
        assert_eq!(completed(&receipt).await, Ok(()));
        session.finalize().await.unwrap();

        let (mut session, _driver) = new_session(CodecProtocol::Http11);
        let stream = StreamId::new(1);
        let mut request = session.begin_incoming_stream(stream).unwrap();
        let receipt = request.completion().clone();
        let calls = Rc::new(Cell::new(0));
        session.add_lifecycle_observer(Rc::new(DetachObserver {
            receipt: receipt.clone(),
            calls: Rc::clone(&calls),
            panic: false,
        }));
        session.add_lifecycle_observer(Rc::new(DetachObserver {
            receipt: receipt.clone(),
            calls: Rc::clone(&calls),
            panic: true,
        }));
        session
            .ingress_headers(stream, request_message(), true)
            .unwrap();
        request.source_mut().read_header_event().await.unwrap();
        drop(request);
        session.egress_finished(stream).unwrap();
        assert!(catch_unwind(AssertUnwindSafe(|| session.process_pending_events())).is_err());
        assert_eq!(calls.get(), 2);
        pending(&receipt);
        session.process_pending_events().unwrap();
        assert_eq!(
            completed(&receipt).await,
            Err(ExchangeCompletionError::CleanupUnconfirmed)
        );
        session.finalize().await.unwrap();

        let owner = ExchangeOwner::new(Rc::new(|| {}), false);
        let tracker = owner.tracker();
        let receipt = owner.completion();
        let dropped = Rc::new(Cell::new(false));
        let source = tracker.source(HttpSourceHolder::new(Box::new(PendingSource {
            dropped: Rc::clone(&dropped),
            read_dropped: Rc::new(Cell::new(false)),
            stopped: Rc::new(Cell::new(0)),
            panic_stop: true,
            ready: false,
            owner: std::thread::current().id(),
        })));
        tracker.mark_detached();
        assert!(catch_unwind(AssertUnwindSafe(|| drop(source))).is_err());
        assert!(dropped.get());
        assert!(owner.ready());
        owner.publish();
        assert_eq!(
            completed(&receipt).await,
            Err(ExchangeCompletionError::CleanupUnconfirmed)
        );
    });
}

#[test]
fn held_completion_keeps_its_exchange_identity_and_retirement_wake() {
    run_local(async {
        let mut keep_alive = KeepAliveOwner::new(());
        let target = Rc::new(Cell::new(0));
        let weak_target = Rc::downgrade(&target);
        let owner = ExchangeOwner::new(
            Rc::new(move || {
                if let Some(target) = weak_target.upgrade() {
                    target.set(target.get() + 1);
                }
            }),
            false,
        );
        let tracker = owner.tracker();
        let first = owner.completion();
        let second = tracker.completion();
        let context = HandlerSessionContext::acquire(&keep_alive)
            .unwrap()
            .with_exchange_completion(first.clone())
            .unwrap();
        assert!(
            context
                .clone()
                .with_exchange_completion(second.clone())
                .is_ok(),
            "every completion of one exchange is the same exchange"
        );
        let other = ExchangeOwner::new(Rc::new(|| {}), false);
        assert!(
            context
                .with_exchange_completion(other.completion())
                .is_err()
        );
        drop(other);

        let guard = tracker.guard();
        tracker.mark_detached();
        assert_eq!(target.get(), 1);
        drop(guard);
        assert_eq!(target.get(), 2);
        assert!(owner.ready());
        owner.publish();
        drop(tracker);
        assert_eq!(poll_once(first.wait().as_mut()), Poll::Ready(Ok(())));
        assert_eq!(poll_once(second.wait().as_mut()), Poll::Ready(Ok(())));
        assert_eq!(
            Rc::strong_count(&target),
            1,
            "the retained wake closure holds only a weak capture"
        );
        drop(target);
        assert_eq!(poll_once(first.wait().as_mut()), Poll::Ready(Ok(())));
        keep_alive.finalize().await;
    });
}

#[test]
fn exchange_owner_block_is_freed_after_owner_trackers_and_completions_drop() {
    for publish in [false, true] {
        let owner = ExchangeOwner::new(Rc::new(|| {}), false);
        let block = owner.allocation_for_test();
        let tracker = owner.tracker();
        let completions = [owner.completion(), tracker.completion()];
        let mut waiting = completions[0].wait();
        assert!(poll_once(waiting.as_mut()).is_pending());
        if publish {
            tracker.mark_detached();
            owner.publish();
        } else {
            drop(owner);
        }
        assert!(block.upgrade().is_some());
        drop(tracker);
        assert!(block.upgrade().is_some(), "completions retain the exchange");
        drop(completions);
        assert!(
            block.upgrade().is_none(),
            "no reference cycle retains the exchange"
        );
        let expected = if publish {
            Ok(())
        } else {
            Err(ExchangeCompletionError::CleanupUnconfirmed)
        };
        assert_eq!(poll_once(waiting.as_mut()), Poll::Ready(expected));
    }
}
