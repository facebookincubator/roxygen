use std::cell::Cell;
use std::cell::RefCell;
use std::future::Future;
use std::future::poll_fn;
use std::mem;
use std::pin::Pin;
use std::rc::Rc;
use std::rc::Weak;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use futures::executor::LocalPool;
use futures::executor::LocalSpawner;
use futures::task::LocalSpawnExt;
use roxygen_core::body_event_queue::BodyEventQueueCallback;
use roxygen_core::body_event_queue::HttpBodyEventQueue;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::byte_event::ByteEventType;
use roxygen_core::byte_event::HttpByteEvent;
use roxygen_core::byte_event::HttpByteEventCallback;
use roxygen_core::byte_event::HttpByteEventRegistration;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::ids::StreamId;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::stream_source::FlowControlState;
use roxygen_core::stream_source::HttpStreamSource;
use roxygen_core::stream_source::HttpStreamSourceCallback;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::driver::WorkerDriver;
use roxygen_runtime::host::Host;
use roxygen_runtime::host::LocalFuture;
use roxygen_runtime::host::LocalHost;
use roxygen_runtime::worker::WorkerContext;
use roxygen_transport::byte_event::AsyncSocketByteEventObserver;
use roxygen_transport::byte_event::ByteEventOffsets;
use roxygen_transport::byte_event::SocketByteEventType;

struct Timer {
    deadline: Instant,
    waker: RefCell<Option<Waker>>,
}

/// How a `ManualHost` completes the futures its `yield_to_io` returns.
#[derive(Clone, Copy)]
pub(crate) enum YieldMode {
    /// Wakes on the first poll and completes on the next.
    Wake,
    /// Completes on the first poll without waking.
    Immediate,
    /// Completes once the host has no ready task left, as an event loop processes I/O between
    /// iterations.
    Deferred,
}

#[derive(Default)]
struct HostYield {
    complete: Cell<bool>,
    waker: RefCell<Option<Waker>>,
}

pub(crate) struct ManualHost {
    spawner: LocalSpawner,
    now: Rc<Cell<Instant>>,
    timers: RefCell<Vec<Weak<Timer>>>,
    yields: Rc<Cell<usize>>,
    yield_mode: Cell<YieldMode>,
    deferred_yields: RefCell<Vec<Weak<HostYield>>>,
    completed_yields: Rc<Cell<usize>>,
    spawns: Cell<usize>,
    sleeps: Cell<usize>,
    now_reads: Cell<usize>,
}

impl ManualHost {
    pub(crate) fn create() -> (LocalPool, Rc<Self>, Host) {
        assert!(tokio::runtime::Handle::try_current().is_err());
        let pool = LocalPool::new();
        let implementation = Rc::new(Self {
            spawner: pool.spawner(),
            now: Rc::new(Cell::new(Instant::now() + Duration::from_secs(3600))),
            timers: RefCell::new(Vec::new()),
            yields: Rc::new(Cell::new(0)),
            yield_mode: Cell::new(YieldMode::Wake),
            deferred_yields: RefCell::new(Vec::new()),
            completed_yields: Rc::new(Cell::new(0)),
            spawns: Cell::new(0),
            sleeps: Cell::new(0),
            now_reads: Cell::new(0),
        });
        let host = Host::new(implementation.clone());
        (pool, implementation, host)
    }

    pub(crate) fn advance(&self, duration: Duration) {
        self.now.set(self.now.get() + duration);
        let ready: Vec<_> = self
            .timers
            .borrow()
            .iter()
            .filter_map(Weak::upgrade)
            .filter(|timer| timer.deadline <= self.now.get())
            .filter_map(|timer| timer.waker.borrow_mut().take())
            .collect();
        ready.into_iter().for_each(Waker::wake);
    }

    pub(crate) fn yields(&self) -> usize {
        self.yields.get()
    }

    pub(crate) fn completed_yields(&self) -> usize {
        self.completed_yields.get()
    }

    /// How many futures this host has been asked to spawn.
    pub(crate) fn spawns(&self) -> usize {
        self.spawns.get()
    }

    /// How many timers this host has created.
    pub(crate) fn timers_created(&self) -> usize {
        self.sleeps.get()
    }

    /// How many times this host's clock has been read.
    pub(crate) fn now_reads(&self) -> usize {
        self.now_reads.get()
    }

    /// Applies to `yield_to_io` futures created after this call.
    pub(crate) fn set_yield_mode(&self, mode: YieldMode) {
        self.yield_mode.set(mode);
    }

    /// Returns whether any live `YieldMode::Deferred` yield was outstanding.
    fn complete_deferred_yields(&self) -> bool {
        let outstanding: Vec<_> = self
            .deferred_yields
            .borrow_mut()
            .drain(..)
            .filter_map(|host_yield| host_yield.upgrade())
            .collect();
        for host_yield in &outstanding {
            host_yield.complete.set(true);
            if let Some(waker) = host_yield.waker.borrow_mut().take() {
                waker.wake();
            }
        }
        !outstanding.is_empty()
    }
}

impl LocalHost for ManualHost {
    fn spawn_local(&self, future: LocalFuture) {
        self.spawns.set(self.spawns.get() + 1);
        self.spawner.spawn_local(future).unwrap();
    }

    fn now(&self) -> Instant {
        self.now_reads.set(self.now_reads.get() + 1);
        self.now.get()
    }

    fn sleep_until(&self, deadline: Instant) -> LocalFuture {
        self.sleeps.set(self.sleeps.get() + 1);
        let timer = Rc::new(Timer {
            deadline,
            waker: RefCell::new(None),
        });
        let mut timers = self.timers.borrow_mut();
        timers.retain(|timer| timer.strong_count() != 0);
        timers.push(Rc::downgrade(&timer));
        let now = Rc::clone(&self.now);
        Box::pin(poll_fn(move |context| {
            if now.get() >= timer.deadline {
                Poll::Ready(())
            } else {
                *timer.waker.borrow_mut() = Some(context.waker().clone());
                Poll::Pending
            }
        }))
    }

    fn yield_to_io(&self) -> LocalFuture {
        let mode = self.yield_mode.get();
        let host_yield = Rc::new(HostYield::default());
        if let YieldMode::Deferred = mode {
            self.deferred_yields
                .borrow_mut()
                .push(Rc::downgrade(&host_yield));
        }
        let yields = Rc::clone(&self.yields);
        let completed = Rc::clone(&self.completed_yields);
        let mut polled = false;
        Box::pin(poll_fn(move |context| {
            if !mem::replace(&mut polled, true) {
                yields.set(yields.get() + 1);
                match mode {
                    YieldMode::Wake => {
                        host_yield.complete.set(true);
                        context.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                    YieldMode::Immediate => host_yield.complete.set(true),
                    YieldMode::Deferred => {}
                }
            }
            if host_yield.complete.get() {
                completed.set(completed.get() + 1);
                Poll::Ready(())
            } else {
                *host_yield.waker.borrow_mut() = Some(context.waker().clone());
                Poll::Pending
            }
        }))
    }
}

thread_local! {
    static RUNNING_HOST: RefCell<Option<Rc<ManualHost>>> = const { RefCell::new(None) };
}

pub(crate) fn run_on_host<T: Send + 'static>(
    test: impl FnOnce(WorkerContext) -> Pin<Box<dyn Future<Output = T>>> + Send + 'static,
) -> T {
    run_on_host_with(|_, context| test(context))
}

pub(crate) fn run_on_host_with<T: Send + 'static>(
    test: impl FnOnce(Rc<ManualHost>, WorkerContext) -> Pin<Box<dyn Future<Output = T>>>
    + Send
    + 'static,
) -> T {
    let (mut pool, implementation, host) = ManualHost::create();
    // The command factory must be `Send`, so the `!Send` host reaches it through this thread.
    RUNNING_HOST.set(Some(Rc::clone(&implementation)));
    let (worker, driver) = WorkerDriver::with_host(16, host).unwrap();
    let call = worker
        .call_local(move |context| {
            let worker = context.handle().clone();
            let host = RUNNING_HOST
                .take()
                .expect("the hosted worker runs commands on the hosting thread");
            async move {
                let result = test(host, context).await;
                worker.begin_shutdown();
                result
            }
        })
        .unwrap();
    let result = Rc::new(RefCell::new(None));
    let completed = Rc::clone(&result);
    pool.spawner()
        .spawn_local(async move {
            let (_, result) = tokio::join!(driver, call);
            *completed.borrow_mut() = Some(result.unwrap());
        })
        .unwrap();
    for _ in 0..100 {
        pool.run_until_stalled();
        if let Some(result) = result.borrow_mut().take() {
            assert!(
                implementation
                    .timers
                    .borrow()
                    .iter()
                    .all(|timer| timer.strong_count() == 0)
            );
            return result;
        }
        if implementation.complete_deferred_yields() {
            continue;
        }
        let deadline = implementation
            .timers
            .borrow()
            .iter()
            .filter_map(Weak::upgrade)
            .map(|timer| timer.deadline)
            .min()
            .expect("stalled host must have an armed timer");
        implementation.advance(deadline.saturating_duration_since(implementation.now()));
    }
    panic!("hosted worker did not complete within 100 timer turns");
}

pub(crate) fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

fn body(bytes: &'static [u8]) -> BufQueue {
    BufQueue::from_bytes(Bytes::from_static(bytes), bytes.len()).unwrap()
}

struct NoopCallback;
impl HttpStreamSourceCallback for NoopCallback {}
impl BodyEventQueueCallback for NoopCallback {
    fn on_egress_bytes_buffered(&self, _bytes: i64) {}
    fn on_content_length_mismatch(&self, _expected: u64, _observed: u64) {}
}

#[test]
fn source_timeout_and_eom_use_external_clock() {
    let (mut pool, clock, host) = ManualHost::create();
    let mut source = HttpStreamSource::new_session_with_host(
        StreamId::new(1),
        NoopCallback,
        8,
        Duration::from_secs(2),
        host.clone(),
    )
    .unwrap();
    let mut read = source.read_header_event();
    assert!(poll_once(read.as_mut()).is_pending());
    clock.advance(Duration::from_secs(1));
    assert!(poll_once(read.as_mut()).is_pending());
    clock.advance(Duration::from_secs(1));
    assert_eq!(
        pool.run_until(read).err().expect("read timeout").code(),
        HttpErrorCode::READ_TIMEOUT
    );

    let source = HttpStreamSource::new_async_with_host(None, None, 8, host.clone()).unwrap();
    source.skip_content_length_validation();
    assert!(source.initialize_body_only(&mut HttpMessage::new(), false));
    let completion = source.ingress_completion().unwrap();
    assert_eq!(
        source.producer().body(body(b"abc"), 0, true),
        FlowControlState::Open
    );
    assert_eq!(completion.snapshot().unwrap().unwrap(), host.now());
    assert_eq!(completion.body_bytes_received(), 3);
    assert_eq!(source.body_bytes_buffered(), 3);
    drop(source);
    assert_eq!(completion.snapshot().unwrap().unwrap(), host.now());
    assert_eq!(completion.body_bytes_received(), 3);
}

#[test]
fn capacity_timer_keeps_host_after_both_reset_paths() {
    let (mut pool, clock, host) = ManualHost::create();
    let callback = Rc::new(NoopCallback);
    let mut queue = HttpBodyEventQueue::with_configuration_and_host(
        StreamId::new(1),
        &callback,
        1,
        Duration::from_secs(2),
        host,
    );
    for generation in 0..3 {
        let source = HttpFixedSource::make_fixed_response(200, Some(body(b"xx"))).unwrap();
        assert!(queue.set_source(Box::new(source)).is_ok());
        pool.run_until(queue.read_header_event()).unwrap();
        pool.run_until(queue.read_body_event(1, CancellationToken::never()))
            .unwrap();
        let mut blocked = queue.read_body_event(1, CancellationToken::never());
        assert!(poll_once(blocked.as_mut()).is_pending());
        clock.advance(Duration::from_secs(1));
        assert!(poll_once(blocked.as_mut()).is_pending());
        clock.advance(Duration::from_secs(1));
        let error = pool.run_until(blocked).err().expect("capacity timeout");
        assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
        assert_eq!(error.message(), b"timed out waiting for buffer space");
        let error = HttpError::new(HttpErrorCode::CANCEL);
        if generation == 0 {
            queue.stop_reading(error.code());
            queue.clear(&error);
        } else {
            queue.prepare_stop_and_clear(error).retire().unwrap();
        }
    }
}

#[derive(Default)]
struct ByteEvents(RefCell<Vec<HttpErrorCode>>);
impl HttpByteEventCallback for ByteEvents {
    fn on_byte_event(&self, _event: HttpByteEvent) {
        panic!("timed-out event must not fire");
    }

    fn on_byte_event_cancelled(&self, _event: HttpByteEvent, error: HttpError) {
        self.0.borrow_mut().push(error.code());
    }
}

#[test]
fn byte_event_timeout_uses_external_clock_and_retires_once() {
    run_on_host(|context| {
        Box::pin(async move {
            let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
            observer.set_tx_ack_events_enabled(&context, true).unwrap();
            observer
                .set_byte_event_timeout(&context, Duration::from_secs(2))
                .unwrap();
            observer
                .socket_byte_event(&context, SocketByteEventType::Write, 100)
                .unwrap();
            let events = Rc::new(ByteEvents::default());
            observer
                .register_byte_events(
                    &context,
                    ByteEventOffsets {
                        stream_id: StreamId::new(1),
                        session_byte_offset: 100,
                        session_bytes_scheduled: 100,
                        stream_offset: 100,
                        body_offset: 0,
                    },
                    None,
                    vec![HttpByteEventRegistration::new(
                        None,
                        ByteEventType::NicTx.bit(),
                        &events,
                    )],
                    false,
                )
                .unwrap();
            let start = context.host().now();
            assert_eq!(observer.pending_timeout_refs(&context).unwrap(), 1);
            poll_fn(|task_context| {
                if observer.poll_timeouts(&context, task_context).unwrap() == 0 {
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            })
            .await;
            assert_eq!(context.host().now() - start, Duration::from_secs(2));
            assert_eq!(&*events.0.borrow(), &[HttpErrorCode::READ_TIMEOUT]);
            assert_eq!(observer.pending_timeout_refs(&context).unwrap(), 0);
            assert_eq!(
                observer
                    .socket_byte_event(&context, SocketByteEventType::Tx, 100)
                    .unwrap(),
                0
            );
            drop(observer);
        })
    });
}

#[test]
fn manual_host_counts_yields() {
    run_on_host_with(|host, context| {
        Box::pin(async move {
            assert_eq!(host.yields(), 0);
            context.host().yield_to_io().await;
            context.host().yield_to_io().await;
            assert_eq!(host.yields(), 2);
        })
    });
}
