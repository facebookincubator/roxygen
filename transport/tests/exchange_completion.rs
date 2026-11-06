use std::cell::Cell;
use std::cell::RefCell;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;

use roxygen_core::byte_event::ByteEventType;
use roxygen_core::byte_event::HttpByteEvent;
use roxygen_core::byte_event::HttpByteEventCallback;
use roxygen_core::byte_event::HttpByteEventRegistration;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::ids::StreamId;
use roxygen_runtime::driver::WorkerDriver;
use roxygen_runtime::wait::WaitStatus;
use roxygen_runtime::worker::WorkerContext;
use tokio::runtime::Builder;
use tokio::task::LocalSet;
use tokio::time;

use crate::byte_event::AsyncSocketByteEventObserver;
use crate::byte_event::ByteEventOffsets;
use crate::byte_event::SocketByteEventType;

struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct Callback {
    observer: Weak<AsyncSocketByteEventObserver>,
    context: WorkerContext,
    calls: Cell<usize>,
    panic: Cell<bool>,
    waker: Waker,
}

impl Callback {
    fn called(&self, stream: StreamId) {
        self.calls.set(self.calls.get() + 1);
        assert!(
            self.observer
                .upgrade()
                .unwrap()
                .poll_stream_events_drained(
                    &self.context,
                    stream,
                    &mut Context::from_waker(&self.waker),
                )
                .is_pending()
        );
        assert!(self.context.is_owner_thread());
        assert!(!self.panic.get(), "scripted callback panic");
    }
}

impl HttpByteEventCallback for Callback {
    fn on_byte_event(&self, event: HttpByteEvent) {
        self.called(event.stream_id.unwrap());
    }

    fn on_byte_event_cancelled(&self, event: HttpByteEvent, _error: HttpError) {
        self.called(event.stream_id.unwrap());
    }
}

fn run_worker<F, Fut>(test: F)
where
    F: FnOnce(WorkerContext) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + 'static,
{
    Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(LocalSet::new().run_until(async move {
            time::pause();
            let (worker, driver) = WorkerDriver::new(32).unwrap();
            let call = worker.call_local(test).unwrap();
            let driver = tokio::task::spawn_local(driver);
            call.await.unwrap();
            worker.begin_shutdown();
            driver.await.unwrap();
        }));
}

fn register<C: HttpByteEventCallback + 'static>(
    observer: &AsyncSocketByteEventObserver,
    context: &WorkerContext,
    stream: StreamId,
    offset: u64,
    kind: ByteEventType,
    callback: &Rc<C>,
) {
    observer
        .register_byte_events(
            context,
            ByteEventOffsets {
                stream_id: stream,
                session_byte_offset: offset,
                session_bytes_scheduled: 0,
                stream_offset: offset,
                body_offset: offset,
            },
            None,
            vec![HttpByteEventRegistration::new(
                Some(stream),
                kind.bit(),
                callback,
            )],
            true,
        )
        .unwrap();
}

#[test]
fn stream_events_drained_waits_for_callbacks() {
    run_worker(|context| async move {
        let observer = Rc::new(AsyncSocketByteEventObserver::new(&context).unwrap());
        let wake = Arc::new(WakeCount(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wake));
        let callback = Rc::new(Callback {
            observer: Rc::downgrade(&observer),
            context: context.clone(),
            calls: Cell::new(0),
            panic: Cell::new(false),
            waker: waker.clone(),
        });
        let first = StreamId::new(1);
        let second = StreamId::new(3);
        let mut task_context = Context::from_waker(&waker);
        register(
            &observer,
            &context,
            first,
            10,
            ByteEventType::TransportWrite,
            &callback,
        );
        register(
            &observer,
            &context,
            second,
            20,
            ByteEventType::TransportWrite,
            &callback,
        );
        assert!(
            observer
                .poll_stream_events_drained(&context, first, &mut task_context)
                .is_pending()
        );
        observer.transport_write(&context, 10).unwrap();
        assert_eq!(callback.calls.get(), 1);
        assert!(matches!(
            observer.poll_stream_events_drained(&context, first, &mut task_context),
            Poll::Ready(Ok(()))
        ));
        assert!(
            observer
                .poll_stream_events_drained(&context, second, &mut task_context)
                .is_pending()
        );
        let error = HttpError::with_message(HttpErrorCode::CANCEL, b"test cancellation");
        callback.panic.set(true);
        assert!(
            catch_unwind(AssertUnwindSafe(
                || observer.cancel_stream_events(&context, second, &error)
            ))
            .is_err()
        );
        callback.panic.set(false);
        assert!(matches!(
            observer.poll_stream_events_drained(&context, second, &mut task_context),
            Poll::Ready(Ok(()))
        ));
        observer.set_tx_ack_events_enabled(&context, true).unwrap();
        observer
            .set_byte_event_timeout(&context, Duration::from_millis(10))
            .unwrap();
        register(
            &observer,
            &context,
            first,
            30,
            ByteEventType::CumulativeAck,
            &callback,
        );
        let boundary = observer.next_tx_ack_event(&context).unwrap().unwrap();
        assert!(
            observer
                .poll_stream_events_drained(&context, first, &mut task_context)
                .is_pending()
        );
        observer
            .socket_byte_event(&context, SocketByteEventType::Write, 100)
            .unwrap();
        observer
            .transport_write_complete(&context, 30, Some(boundary))
            .unwrap();
        assert_eq!(observer.pending_timeout_refs(&context).unwrap(), 1);
        assert!(
            observer
                .poll_stream_events_drained(&context, first, &mut task_context)
                .is_pending()
        );
        time::advance(Duration::from_millis(20)).await;
        assert_eq!(
            observer.poll_timeouts(&context, &mut task_context).unwrap(),
            1
        );
        assert_eq!(observer.pending_timeout_refs(&context).unwrap(), 0);
        assert!(matches!(
            observer.poll_stream_events_drained(&context, first, &mut task_context),
            Poll::Ready(Ok(()))
        ));
        register(
            &observer,
            &context,
            second,
            40,
            ByteEventType::TransportWrite,
            &callback,
        );
        assert!(
            observer
                .poll_stream_events_drained(&context, second, &mut task_context)
                .is_pending()
        );
        drop(callback);
        observer.transport_write(&context, 40).unwrap();
        assert!(wake.0.load(Ordering::SeqCst) > 0);
        assert!(matches!(
            observer.poll_stream_events_drained(&context, second, &mut task_context),
            Poll::Ready(Ok(()))
        ));
    });
}

struct PanickingPayload;

impl Drop for PanickingPayload {
    fn drop(&mut self) {
        panic!("retirement wake payload destructor");
    }
}

struct PanickingWake(AtomicUsize);

impl Wake for PanickingWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
        std::panic::panic_any(PanickingPayload);
    }
}

struct RetirementCallback {
    fired: Cell<usize>,
    cancelled: Cell<usize>,
    owner: std::thread::ThreadId,
}

impl HttpByteEventCallback for RetirementCallback {
    fn on_byte_event(&self, _event: HttpByteEvent) {
        assert_eq!(self.owner, std::thread::current().id());
        self.fired.set(self.fired.get() + 1);
    }

    fn on_byte_event_cancelled(&self, _event: HttpByteEvent, _error: HttpError) {
        assert_eq!(self.owner, std::thread::current().id());
        self.cancelled.set(self.cancelled.get() + 1);
    }
}

#[test]
fn stream_retirement_waker_panic_payloads_preserve_cleanup() {
    run_worker(|context| async move {
        for unwinding in [false, true] {
            let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
            observer.set_tx_ack_events_enabled(&context, true).unwrap();
            let wake = Arc::new(PanickingWake(AtomicUsize::new(0)));
            let waker = Waker::from(Arc::clone(&wake));
            let mut task_context = Context::from_waker(&waker);
            let callback = Rc::new(RetirementCallback {
                fired: Cell::new(0),
                cancelled: Cell::new(0),
                owner: std::thread::current().id(),
            });
            for (stream, offset) in [(StreamId::new(1), 10), (StreamId::new(3), 20)] {
                register(
                    &observer,
                    &context,
                    stream,
                    offset,
                    if unwinding {
                        ByteEventType::CumulativeAck
                    } else {
                        ByteEventType::TransportWrite
                    },
                    &callback,
                );
                assert!(
                    observer
                        .poll_stream_events_drained(&context, stream, &mut task_context)
                        .is_pending()
                );
            }
            if unwinding {
                let first = observer.next_tx_ack_event(&context).unwrap().unwrap();
                let second = observer.next_tx_ack_event(&context).unwrap().unwrap();
                let panic = catch_unwind(AssertUnwindSafe(move || {
                    let _first = first;
                    let _second = second;
                    panic!("primary unwind");
                }))
                .unwrap_err();
                assert_eq!(panic.downcast_ref::<&str>(), Some(&"primary unwind"));
                assert_eq!(callback.cancelled.get(), 2);
                assert_eq!(callback.fired.get(), 0);
            } else {
                let panic =
                    catch_unwind(AssertUnwindSafe(|| observer.transport_write(&context, 20)))
                        .unwrap_err();
                assert!(panic.is::<PanickingPayload>());
                roxygen_runtime::panic::discard_panic(panic);
                assert_eq!(callback.fired.get(), 2);
                assert_eq!(callback.cancelled.get(), 0);
            }
            assert_eq!(wake.0.load(Ordering::SeqCst), 2);
            for stream in [StreamId::new(1), StreamId::new(3)] {
                assert!(matches!(
                    observer.poll_stream_events_drained(&context, stream, &mut task_context),
                    Poll::Ready(Ok(()))
                ));
            }
            assert_eq!(observer.pending_timeout_refs(&context).unwrap(), 0);
        }
    });
}

struct QueuePanicPayload {
    stream: StreamId,
    drops: Arc<AtomicUsize>,
}

impl Drop for QueuePanicPayload {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        panic!("cross-queue panic payload destructor");
    }
}

struct QueueRetirementWake {
    stream: StreamId,
    calls: AtomicUsize,
    payload_drops: Arc<AtomicUsize>,
}

impl Wake for QueueRetirementWake {
    fn wake(self: Arc<Self>) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        std::panic::panic_any(QueuePanicPayload {
            stream: self.stream,
            drops: Arc::clone(&self.payload_drops),
        });
    }
}

struct QueueCancellationCallback {
    streams: RefCell<Vec<StreamId>>,
    owner: std::thread::ThreadId,
}

impl HttpByteEventCallback for QueueCancellationCallback {
    fn on_byte_event(&self, _event: HttpByteEvent) {
        panic!("mixed queues must be cancelled, not fired");
    }

    fn on_byte_event_cancelled(&self, event: HttpByteEvent, error: HttpError) {
        assert_eq!(self.owner, std::thread::current().id());
        assert_eq!(error.code(), HttpErrorCode::CANCEL);
        let mut streams = self.streams.borrow_mut();
        assert!(streams.len() < 6);
        streams.push(event.stream_id.unwrap());
    }
}

#[test]
fn stream_retirement_mixed_queue_cancellation_preserves_first_panic() {
    run_worker(|context| async move {
        for unwinding in [false, true] {
            let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
            observer.set_tx_ack_events_enabled(&context, true).unwrap();
            let callback = Rc::new(QueueCancellationCallback {
                streams: RefCell::new(Vec::new()),
                owner: std::thread::current().id(),
            });
            observer
                .socket_byte_event(&context, SocketByteEventType::Write, 20)
                .unwrap();
            for (stream, offset, kind) in [
                (StreamId::new(9), 10, ByteEventType::CumulativeAck),
                (StreamId::new(11), 20, ByteEventType::NicTx),
            ] {
                register(&observer, &context, stream, offset, kind, &callback);
                let boundary = observer.next_tx_ack_event(&context).unwrap().unwrap();
                observer
                    .transport_write_complete(&context, offset, Some(boundary))
                    .unwrap();
            }
            assert_eq!(observer.pending_timeout_refs(&context).unwrap(), 2);
            register(
                &observer,
                &context,
                StreamId::new(7),
                130,
                ByteEventType::CumulativeAck,
                &callback,
            );
            let in_flight = observer.next_tx_ack_event(&context).unwrap().unwrap();
            for (stream, offset, kind) in [
                (StreamId::new(1), 100, ByteEventType::TransportWrite),
                (StreamId::new(3), 120, ByteEventType::KernelWrite),
                (StreamId::new(5), 140, ByteEventType::NicTx),
            ] {
                register(&observer, &context, stream, offset, kind, &callback);
            }
            assert_eq!(observer.pending_events(&context).unwrap(), 6);
            let payload_drops = Arc::new(AtomicUsize::new(0));
            let wakes = [1, 3, 5, 7, 9, 11].map(|stream| {
                Arc::new(QueueRetirementWake {
                    stream: StreamId::new(stream),
                    calls: AtomicUsize::new(0),
                    payload_drops: Arc::clone(&payload_drops),
                })
            });
            for wake in &wakes {
                let waker = Waker::from(Arc::clone(wake));
                assert!(
                    observer
                        .poll_stream_events_drained(
                            &context,
                            wake.stream,
                            &mut Context::from_waker(&waker),
                        )
                        .is_pending()
                );
            }
            let zero_refs = observer.zero_refs(&context).unwrap();
            if unwinding {
                let panic = catch_unwind(AssertUnwindSafe(move || {
                    let _observer = observer;
                    panic!("primary mixed-queue unwind");
                }))
                .unwrap_err();
                assert_eq!(
                    panic.downcast_ref::<&str>(),
                    Some(&"primary mixed-queue unwind")
                );
            } else {
                let panic = catch_unwind(AssertUnwindSafe(|| {
                    observer.cancel_events(&context, &HttpError::new(HttpErrorCode::CANCEL))
                }))
                .expect_err("first retirement wake panic");
                assert_eq!(
                    panic.downcast_ref::<QueuePanicPayload>().unwrap().stream,
                    StreamId::new(1)
                );
                roxygen_runtime::panic::discard_panic(panic);
                assert_eq!(observer.pending_events(&context).unwrap(), 0);
                assert_eq!(observer.pending_timeout_refs(&context).unwrap(), 0);
                for wake in &wakes {
                    assert!(matches!(
                        observer.poll_stream_events_drained(
                            &context,
                            wake.stream,
                            &mut Context::from_waker(Waker::noop()),
                        ),
                        Poll::Ready(Ok(()))
                    ));
                }
            }
            drop(in_flight);
            assert_eq!(
                zero_refs.await,
                if unwinding {
                    WaitStatus::Cancelled
                } else {
                    WaitStatus::Signalled
                }
            );
            callback.streams.borrow_mut().sort();
            assert_eq!(
                *callback.streams.borrow(),
                [1, 3, 5, 7, 9, 11].map(StreamId::new)
            );
            for wake in &wakes {
                assert_eq!(wake.calls.load(Ordering::SeqCst), 1);
            }
            assert_eq!(payload_drops.load(Ordering::SeqCst), 6);
        }
    });
}

struct ActionRetirementCallback {
    observer: Weak<AsyncSocketByteEventObserver>,
    context: WorkerContext,
    wakes: [Arc<QueueRetirementWake>; 2],
    streams: RefCell<Vec<StreamId>>,
}

impl HttpByteEventCallback for ActionRetirementCallback {
    fn on_byte_event(&self, _event: HttpByteEvent) {
        panic!("disabled TX/ACK registrations must be cancelled");
    }

    fn on_byte_event_cancelled(&self, event: HttpByteEvent, error: HttpError) {
        assert!(self.context.is_owner_thread());
        assert_eq!(error.code(), HttpErrorCode::CANCEL);
        let stream = event.stream_id.unwrap();
        let wake = self
            .wakes
            .iter()
            .find(|wake| wake.stream == stream)
            .unwrap();
        let waker = Waker::from(Arc::clone(wake));
        assert!(self.observer.upgrade().unwrap().poll_stream_events_drained(
            &self.context, stream, &mut Context::from_waker(&waker),
        ).is_pending());
        self.streams.borrow_mut().push(stream);
    }
}

#[test]
fn stream_retirement_registration_actions_preserve_first_panic() {
    run_worker(|context| async move {
        let observer = Rc::new(AsyncSocketByteEventObserver::new(&context).unwrap());
        let payload_drops = Arc::new(AtomicUsize::new(0));
        let callback = Rc::new(ActionRetirementCallback {
            observer: Rc::downgrade(&observer),
            context: context.clone(),
            wakes: [1, 3].map(|stream| {
                Arc::new(QueueRetirementWake {
                    stream: StreamId::new(stream),
                    calls: AtomicUsize::new(0),
                    payload_drops: Arc::clone(&payload_drops),
                })
            }),
            streams: RefCell::new(Vec::new()),
        });
        let panic = catch_unwind(AssertUnwindSafe(|| {
            observer.register_byte_events(
                &context,
                ByteEventOffsets {
                    stream_id: StreamId::new(1),
                    session_byte_offset: 10,
                    session_bytes_scheduled: 0,
                    stream_offset: 10,
                    body_offset: 0,
                },
                None,
                [1, 3]
                    .map(|stream| {
                        HttpByteEventRegistration::new(
                            Some(StreamId::new(stream)),
                            ByteEventType::NicTx.bit(),
                            &callback,
                        )
                    })
                    .into_iter()
                    .collect(),
                true,
            )
        }))
        .expect_err("first immediate-registration retirement panic");
        assert_eq!(
            panic.downcast_ref::<QueuePanicPayload>().unwrap().stream,
            StreamId::new(1)
        );
        roxygen_runtime::panic::discard_panic(panic);
        assert_eq!(
            *callback.streams.borrow(),
            [StreamId::new(1), StreamId::new(3)]
        );
        for wake in &callback.wakes {
            assert_eq!(wake.calls.load(Ordering::SeqCst), 1);
            assert!(matches!(
                observer.poll_stream_events_drained(
                    &context,
                    wake.stream,
                    &mut Context::from_waker(Waker::noop()),
                ),
                Poll::Ready(Ok(()))
            ));
        }
        assert_eq!(observer.pending_events(&context).unwrap(), 0);
        assert_eq!(observer.pending_timeout_refs(&context).unwrap(), 0);
        assert_eq!(payload_drops.load(Ordering::SeqCst), 2);
    });
}
