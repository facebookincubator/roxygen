mod byte_stream {
    use std::cell::RefCell;
    use std::future::Future;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::RawWaker;
    use std::task::RawWakerVTable;
    use std::task::Wake;
    use std::task::Waker;
    use std::thread::ThreadId;
    use std::time::Duration;

    use roxygen_core::buf_queue::BufQueue;
    use roxygen_core::byte_event::ByteEventType;
    use roxygen_core::byte_event::HttpByteEvent;
    use roxygen_core::byte_event::HttpByteEventCallback;
    use roxygen_core::byte_event::HttpByteEventRegistration;
    use roxygen_core::error::HttpError;
    use roxygen_core::error::HttpErrorCode;
    use roxygen_core::ids::StreamId;
    use roxygen_core::message::SocketAddress;
    use roxygen_runtime::cancellation::CancellationSource;
    use roxygen_runtime::cancellation::CancellationToken;
    use roxygen_runtime::driver::WorkerDriver;
    use roxygen_runtime::worker::WorkerContext;
    use roxygen_runtime::worker::WorkerHandle;
    use roxygen_runtime::worker::WorkerState;
    use tokio::runtime::Builder;
    use tokio::task::LocalSet;
    use tokio::time;

    use crate::byte_event::ByteEventWriteFlags;
    use crate::byte_stream::ByteEventCapability;
    use crate::byte_stream::ByteEventRegistrationError;
    use crate::byte_stream::ByteStreamIo;
    use crate::byte_stream::ByteStreamLimits;
    use crate::byte_stream::ByteStreamTransport;
    use crate::byte_stream::LocalTransportFuture;
    use crate::byte_stream::OwnedByteStream;
    use crate::byte_stream::PeerIdentity;
    use crate::byte_stream::TransportRead;
    use crate::byte_stream::TransportWriteError;
    use crate::byte_stream::WriteInfo;
    use crate::byte_stream::WriteProgress;
    use crate::byte_stream::close_path_errors_for_test;
    use crate::byte_stream::read_queue_rebuilds_for_test;
    use crate::byte_stream::write_deadline_batons_for_test;

    const BUFFER_LIMIT: usize = 1 << 20;

    std::thread_local! {
        static CAPTURED_CONTEXT: RefCell<Option<WorkerContext>> = const { RefCell::new(None) };
        static ESCAPED_LIVE_TRANSPORT: RefCell<Option<LiveTransportOperations>> =
            const { RefCell::new(None) };
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum IoAction {
        CancelRead(HttpErrorCode, ThreadId),
        CancelWrite(HttpErrorCode, ThreadId),
        ShutdownWrite(ThreadId),
        Close(ThreadId),
        Reset(ThreadId),
        Drop(ThreadId),
    }

    struct FakeState {
        owner: ThreadId,
        inbound: BufQueue,
        bound_reads_at_maximum: bool,
        read_eof: bool,
        read_error: Option<HttpError>,
        read_waker: Option<Waker>,
        panic_next_read_poll: bool,
        reads_started: usize,
        read_sizes: Vec<(usize, usize)>,
        block_writes: bool,
        pending_write_progress: usize,
        write_error: Option<HttpError>,
        complete_write_at: Option<usize>,
        excess_write_progress: Option<usize>,
        cancel_write_progress: Option<usize>,
        on_next_write_poll: Option<Box<dyn FnOnce()>>,
        active_write_progress: Option<WriteProgress>,
        write_waker: Option<Waker>,
        writes_started: usize,
        writes: Vec<Vec<Vec<u8>>>,
        write_pointers: Vec<Vec<usize>>,
        actions: Vec<IoAction>,
    }

    impl FakeState {
        fn new() -> Self {
            Self {
                owner: std::thread::current().id(),
                inbound: BufQueue::new(BUFFER_LIMIT),
                bound_reads_at_maximum: false,
                read_eof: false,
                read_error: None,
                read_waker: None,
                panic_next_read_poll: false,
                reads_started: 0,
                read_sizes: Vec::new(),
                block_writes: false,
                pending_write_progress: 0,
                write_error: None,
                complete_write_at: None,
                excess_write_progress: None,
                cancel_write_progress: None,
                on_next_write_poll: None,
                active_write_progress: None,
                write_waker: None,
                writes_started: 0,
                writes: Vec::new(),
                write_pointers: Vec::new(),
                actions: Vec::new(),
            }
        }
    }

    struct FakeIo {
        state: Rc<RefCell<FakeState>>,
    }

    struct ShutdownProbeIo {
        actions: Arc<Mutex<Vec<IoAction>>>,
        owner: ThreadId,
        worker: WorkerHandle,
    }

    impl ShutdownProbeIo {
        fn record(&self, action: IoAction) {
            assert_eq!(std::thread::current().id(), self.owner);
            self.actions.lock().unwrap().push(action);
        }
    }

    impl ByteStreamIo for ShutdownProbeIo {
        fn read(
            &self,
            _maximum: usize,
            _allocation_size: usize,
        ) -> LocalTransportFuture<Result<TransportRead, HttpError>> {
            Box::pin(std::future::pending::<Result<TransportRead, HttpError>>())
        }

        fn write(
            &self,
            _data: BufQueue,
            progress: WriteProgress,
            _flags: crate::byte_event::ByteEventWriteFlags,
            _byte_events: Option<crate::byte_stream::ByteEventWriteState>,
        ) -> LocalTransportFuture<Result<(), HttpError>> {
            Box::pin(async move {
                progress.record(2)?;
                std::future::pending::<Result<(), HttpError>>().await
            })
        }

        fn cancel_read(&self, error: HttpErrorCode) {
            self.record(IoAction::CancelRead(error, std::thread::current().id()));
        }

        fn cancel_write(&self, error: HttpErrorCode) {
            self.record(IoAction::CancelWrite(error, std::thread::current().id()));
        }

        fn shutdown_write(&self, _context: &WorkerContext) -> Result<(), HttpError> {
            self.record(IoAction::ShutdownWrite(std::thread::current().id()));
            Ok(())
        }

        fn close(&self, _context: &WorkerContext, drain_write: bool) -> Result<(), HttpError> {
            if drain_write {
                self.shutdown_write(_context)
            } else {
                self.record(IoAction::Close(std::thread::current().id()));
                Ok(())
            }
        }

        fn drop_transport(&self) {
            let mut actions = self.actions.lock().unwrap();
            if !actions
                .iter()
                .any(|action| matches!(action, IoAction::Close(_)))
            {
                actions.push(IoAction::Close(std::thread::current().id()));
            }
        }

        fn close_with_reset(&self) {
            self.record(IoAction::Reset(std::thread::current().id()));
        }
    }

    impl Drop for ShutdownProbeIo {
        fn drop(&mut self) {
            assert_eq!(self.worker.state(), WorkerState::Closing);
            self.record(IoAction::Drop(std::thread::current().id()));
        }
    }

    struct LiveTransportOperations {
        transport: Option<OwnedByteStream>,
        read: Option<LocalTransportFuture<Result<TransportRead, HttpError>>>,
        write: Option<LocalTransportFuture<Result<WriteInfo, TransportWriteError>>>,
    }

    #[derive(Default)]
    struct WakeSignal {
        woken: AtomicBool,
        notify: tokio::sync::Notify,
    }

    impl WakeSignal {
        fn record(&self) {
            self.woken.store(true, Ordering::Release);
            self.notify.notify_one();
        }

        fn take(&self) -> bool {
            self.woken.swap(false, Ordering::AcqRel)
        }

        async fn wait(&self) {
            while !self.take() {
                self.notify.notified().await;
            }
        }
    }

    impl Wake for WakeSignal {
        fn wake(self: Arc<Self>) {
            self.record();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.record();
        }
    }

    /// Counts the clones and wakes of the wakers made by [`WakerCounts::waker`]. Each waker
    /// holds a strong count, and all of them share the counter's address as their data, so
    /// `Waker::will_wake` holds between them and not with another counter's wakers.
    #[derive(Default)]
    struct WakerCounts {
        clones: AtomicUsize,
        wakes: AtomicUsize,
    }

    static COUNTED_WAKER: RawWakerVTable = RawWakerVTable::new(
        clone_counted_waker,
        wake_counted_waker,
        wake_counted_waker_by_ref,
        drop_counted_waker,
    );

    fn clone_counted_waker(data: *const ()) -> RawWaker {
        // SAFETY: every counted waker's `data` is `Arc::into_raw` of a `WakerCounts` and owns
        // one of its strong counts (`WakerCounts::waker`, and this clone's increment), so the
        // allocation is live while the cloned waker is.
        let counts = unsafe {
            Arc::increment_strong_count(data.cast::<WakerCounts>());
            &*data.cast::<WakerCounts>()
        };
        counts.clones.fetch_add(1, Ordering::Relaxed);
        RawWaker::new(data, &COUNTED_WAKER)
    }

    fn wake_counted_waker(data: *const ()) {
        wake_counted_waker_by_ref(data);
        drop_counted_waker(data);
    }

    fn wake_counted_waker_by_ref(data: *const ()) {
        // SAFETY: the woken waker owns a strong count of the `WakerCounts` at `data`, as in
        // `clone_counted_waker`, so the allocation is live.
        let counts = unsafe { &*data.cast::<WakerCounts>() };
        counts.wakes.fetch_add(1, Ordering::Relaxed);
    }

    fn drop_counted_waker(data: *const ()) {
        // SAFETY: `data` is `Arc::into_raw` of a `WakerCounts`, and the dropped (or consumed)
        // waker owns the strong count released here, which nothing else releases.
        unsafe { Arc::decrement_strong_count(data.cast::<WakerCounts>()) };
    }

    impl WakerCounts {
        fn waker(self: &Arc<Self>) -> Waker {
            let data = Arc::into_raw(Arc::clone(self)).cast::<()>();
            // SAFETY: `data` owns the strong count `into_raw` leaked, and `COUNTED_WAKER`'s
            // functions keep `RawWakerVTable`'s contract for such a `data`: a clone takes its
            // own count, a drop or a consuming wake releases the waker's, and a wake by
            // reference only reads the counter. `WakerCounts` is `Send + Sync`.
            unsafe { Waker::from_raw(RawWaker::new(data, &COUNTED_WAKER)) }
        }

        fn clones(&self) -> usize {
            self.clones.load(Ordering::Relaxed)
        }

        fn wakes(&self) -> usize {
            self.wakes.load(Ordering::Relaxed)
        }

        /// The wakers made from this counter that are alive.
        fn live(self: &Arc<Self>) -> usize {
            Arc::strong_count(self) - 1
        }
    }

    impl Drop for LiveTransportOperations {
        fn drop(&mut self) {
            drop(self.transport.take());
            drop(self.read.take());
            drop(self.write.take());
        }
    }

    #[derive(Clone)]
    struct FakeControl {
        state: Rc<RefCell<FakeState>>,
    }

    impl FakeControl {
        fn feed(&self, mut data: BufQueue, eof: bool) {
            let wake = {
                let mut state = self.state.borrow_mut();
                state.inbound.append(&mut data).unwrap();
                state.read_eof |= eof;
                state.read_waker.take()
            };
            if let Some(waker) = wake {
                waker.wake();
            }
        }

        /// Makes the backend return each read's bytes in a queue bounded at that read's
        /// maximum, as the TCP backend does, instead of in a split of its inbound queue.
        fn bound_reads_at_maximum(&self) {
            self.state.borrow_mut().bound_reads_at_maximum = true;
        }

        /// Makes the next poll of a backend read panic before it looks at its bytes.
        fn panic_on_next_read_poll(&self) {
            self.state.borrow_mut().panic_next_read_poll = true;
        }

        fn eof(&self) {
            let wake = {
                let mut state = self.state.borrow_mut();
                state.read_eof = true;
                state.read_waker.take()
            };
            if let Some(waker) = wake {
                waker.wake();
            }
        }

        fn fail_read(&self, code: HttpErrorCode) {
            let wake = {
                let mut state = self.state.borrow_mut();
                state.read_error = Some(HttpError::with_message(code, b"scripted read error"));
                state.read_waker.take()
            };
            if let Some(waker) = wake {
                waker.wake();
            }
        }

        fn block_writes(&self, progress: usize) {
            let mut state = self.state.borrow_mut();
            state.block_writes = true;
            state.pending_write_progress = progress;
        }

        /// Runs `hook` inside the next backend write poll, before its result.
        fn on_next_write_poll(&self, hook: impl FnOnce() + 'static) {
            self.state.borrow_mut().on_next_write_poll = Some(Box::new(hook));
        }

        fn report_write_progress_on_cancel(&self, progress: usize) {
            self.state.borrow_mut().cancel_write_progress = Some(progress);
        }

        fn fail_write(&self, code: HttpErrorCode) {
            let wake = {
                let mut state = self.state.borrow_mut();
                state.block_writes = false;
                state.write_error = Some(HttpError::with_message(code, b"scripted write error"));
                state.write_waker.take()
            };
            if let Some(waker) = wake {
                waker.wake();
            }
        }

        fn unblock_write(&self) {
            let wake = {
                let mut state = self.state.borrow_mut();
                state.block_writes = false;
                state.write_waker.take()
            };
            if let Some(waker) = wake {
                waker.wake();
            }
        }

        fn complete_write_at(&self, progress: usize) {
            self.state.borrow_mut().complete_write_at = Some(progress);
        }

        fn report_excess_write_progress(&self, progress: usize) {
            self.state.borrow_mut().excess_write_progress = Some(progress);
        }

        fn has_backend_waiters(&self) -> bool {
            let state = self.state.borrow();
            state.read_waker.is_some() || state.write_waker.is_some()
        }

        fn reads_started(&self) -> usize {
            self.state.borrow().reads_started
        }

        fn read_sizes(&self) -> Vec<(usize, usize)> {
            self.state.borrow().read_sizes.clone()
        }

        fn writes_started(&self) -> usize {
            self.state.borrow().writes_started
        }

        fn writes(&self) -> Vec<Vec<Vec<u8>>> {
            self.state.borrow().writes.clone()
        }

        fn write_pointers(&self) -> Vec<Vec<usize>> {
            self.state.borrow().write_pointers.clone()
        }

        fn actions(&self) -> Vec<IoAction> {
            self.state.borrow().actions.clone()
        }
    }

    impl ByteStreamIo for FakeIo {
        fn read(
            &self,
            maximum: usize,
            allocation_size: usize,
        ) -> LocalTransportFuture<Result<TransportRead, HttpError>> {
            {
                let mut state = self.state.borrow_mut();
                state.reads_started += 1;
                state.read_sizes.push((maximum, allocation_size));
            }
            let state = Rc::clone(&self.state);
            Box::pin(std::future::poll_fn(move |context| {
                let mut state = state.borrow_mut();
                assert!(
                    !std::mem::take(&mut state.panic_next_read_poll),
                    "scripted read poll panic"
                );
                if let Some(error) = state.read_error.take() {
                    state.read_waker = None;
                    return Poll::Ready(Err(error));
                }
                if !state.inbound.is_empty() {
                    let mut data = state.inbound.split_at_most(maximum);
                    if state.bound_reads_at_maximum {
                        let mut bounded = BufQueue::new(maximum);
                        bounded.append(&mut data).unwrap();
                        data = bounded;
                    }
                    let eof = state.read_eof && state.inbound.is_empty();
                    state.read_waker = None;
                    return Poll::Ready(Ok(TransportRead::from_io(data, eof)));
                }
                if state.read_eof {
                    state.read_waker = None;
                    return Poll::Ready(Ok(TransportRead::from_io(BufQueue::new(0), true)));
                }
                state.read_waker = Some(context.waker().clone());
                Poll::Pending
            }))
        }

        fn write(
            &self,
            data: BufQueue,
            progress: WriteProgress,
            _flags: crate::byte_event::ByteEventWriteFlags,
            _byte_events: Option<crate::byte_stream::ByteEventWriteState>,
        ) -> LocalTransportFuture<Result<(), HttpError>> {
            {
                let mut state = self.state.borrow_mut();
                state.writes_started += 1;
                state.active_write_progress = Some(progress.clone());
            }
            let state = Rc::clone(&self.state);
            let total = data.chain_length();
            let mut segments = Some(
                data.segments()
                    .map(|segment| segment.as_ref().to_vec())
                    .collect::<Vec<_>>(),
            );
            let mut pointers = Some(
                data.segments()
                    .map(|segment| segment.as_ptr() as usize)
                    .collect::<Vec<_>>(),
            );
            let mut recorded = 0;
            Box::pin(std::future::poll_fn(move |context| {
                let hook = state.borrow_mut().on_next_write_poll.take();
                if let Some(hook) = hook {
                    hook();
                }
                let mut state = state.borrow_mut();
                if recorded == 0 {
                    if let Some(excess) = state.excess_write_progress.take() {
                        state.write_waker = None;
                        let result = progress.record(excess);
                        state.active_write_progress = None;
                        return Poll::Ready(result);
                    }
                    if let Some(partial) = state.complete_write_at.take() {
                        state.write_waker = None;
                        let result = progress.record(partial.min(total));
                        state.active_write_progress = None;
                        return Poll::Ready(result);
                    }
                }
                if state.block_writes {
                    if recorded == 0 {
                        let partial = state.pending_write_progress.min(total);
                        if let Err(error) = progress.record(partial) {
                            return Poll::Ready(Err(error));
                        }
                        recorded = partial;
                    }
                    state.write_waker = Some(context.waker().clone());
                    return Poll::Pending;
                }
                if let Some(error) = state.write_error.take() {
                    state.write_waker = None;
                    state.active_write_progress = None;
                    return Poll::Ready(Err(error));
                }
                if let Err(error) = progress.record(total - recorded) {
                    state.write_waker = None;
                    state.active_write_progress = None;
                    return Poll::Ready(Err(error));
                }
                state.write_waker = None;
                state.active_write_progress = None;
                state
                    .writes
                    .push(segments.take().expect("a fake write completes once"));
                state
                    .write_pointers
                    .push(pointers.take().expect("a fake write completes once"));
                Poll::Ready(Ok(()))
            }))
        }

        fn cancel_read(&self, error: HttpErrorCode) {
            let mut state = self.state.borrow_mut();
            state.read_waker = None;
            let owner = state.owner;
            state
                .actions
                .push(IoAction::CancelRead(error, std::thread::current().id()));
            assert_eq!(std::thread::current().id(), owner);
        }

        fn cancel_write(&self, error: HttpErrorCode) {
            let mut state = self.state.borrow_mut();
            state.write_waker = None;
            let progress = state.active_write_progress.take();
            let amount = state.cancel_write_progress.take();
            if let (Some(progress), Some(amount)) = (progress, amount) {
                progress
                    .record(amount)
                    .expect("scripted cancel progress remains within the write bound");
            }
            let owner = state.owner;
            state
                .actions
                .push(IoAction::CancelWrite(error, std::thread::current().id()));
            assert_eq!(std::thread::current().id(), owner);
        }

        fn shutdown_write(&self, _context: &WorkerContext) -> Result<(), HttpError> {
            let mut state = self.state.borrow_mut();
            let owner = state.owner;
            state
                .actions
                .push(IoAction::ShutdownWrite(std::thread::current().id()));
            assert_eq!(std::thread::current().id(), owner);
            Ok(())
        }

        fn close(&self, _context: &WorkerContext, drain_write: bool) -> Result<(), HttpError> {
            if drain_write {
                return self.shutdown_write(_context);
            }
            let mut state = self.state.borrow_mut();
            state.read_waker = None;
            state.write_waker = None;
            let owner = state.owner;
            state
                .actions
                .push(IoAction::Close(std::thread::current().id()));
            assert_eq!(std::thread::current().id(), owner);
            Ok(())
        }

        fn drop_transport(&self) {
            let mut state = self.state.borrow_mut();
            state.read_waker = None;
            state.write_waker = None;
            let owner = state.owner;
            if !state
                .actions
                .iter()
                .any(|action| matches!(action, IoAction::Close(_)))
            {
                state
                    .actions
                    .push(IoAction::Close(std::thread::current().id()));
            }
            assert_eq!(std::thread::current().id(), owner);
        }

        fn close_with_reset(&self) {
            let mut state = self.state.borrow_mut();
            state.read_waker = None;
            state.write_waker = None;
            let owner = state.owner;
            state
                .actions
                .push(IoAction::Reset(std::thread::current().id()));
            assert_eq!(std::thread::current().id(), owner);
        }
    }

    impl Drop for FakeIo {
        fn drop(&mut self) {
            let mut state = self.state.borrow_mut();
            let owner = state.owner;
            assert!(state.read_waker.is_none());
            assert!(state.write_waker.is_none());
            state
                .actions
                .push(IoAction::Drop(std::thread::current().id()));
            assert_eq!(std::thread::current().id(), owner);
        }
    }

    #[derive(Default)]
    struct ByteEventLog {
        fired: RefCell<Vec<ByteEventType>>,
        cancelled: RefCell<Vec<(ByteEventType, HttpErrorCode, ThreadId)>>,
    }

    impl HttpByteEventCallback for ByteEventLog {
        fn on_byte_event(&self, event: HttpByteEvent) {
            self.fired.borrow_mut().push(event.event_type);
        }

        fn on_byte_event_cancelled(&self, event: HttpByteEvent, error: HttpError) {
            self.cancelled.borrow_mut().push((
                event.event_type,
                error.code(),
                std::thread::current().id(),
            ));
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
            .expect("current-thread runtime")
            .block_on(LocalSet::new().run_until(async move {
                time::pause();
                let (worker, driver) = WorkerDriver::new(32).unwrap();
                let call = worker.call_local(test).unwrap();
                let driver_task = tokio::task::spawn_local(driver);
                call.await.unwrap();
                worker.begin_shutdown();
                driver_task.await.unwrap();
            }));
    }

    fn queue(segments: &[&[u8]], limit: usize) -> BufQueue {
        let mut queue = BufQueue::new(limit);
        for segment in segments {
            queue.push_bytes(segment.to_vec().into()).unwrap();
        }
        queue
    }

    fn repeated(byte: u8, length: usize) -> Vec<u8> {
        vec![byte; length]
    }

    fn flatten(queue: &BufQueue) -> Vec<u8> {
        queue
            .segments()
            .flat_map(|segment| segment.as_ref().iter().copied())
            .collect()
    }

    fn transport(
        context: &WorkerContext,
        byte_events: ByteEventCapability,
        peer_identity: Option<PeerIdentity>,
    ) -> (OwnedByteStream, FakeControl) {
        let state = Rc::new(RefCell::new(FakeState::new()));
        let control = FakeControl {
            state: Rc::clone(&state),
        };
        let transport = OwnedByteStream::new(
            FakeIo { state },
            context.handle().clone(),
            context,
            ByteStreamLimits::new(131_072, 262_144, 2).unwrap(),
            SocketAddress::new(b"127.0.0.1", 8080),
            SocketAddress::new(b"127.0.0.2", 443),
            peer_identity,
            byte_events,
        )
        .unwrap();
        (transport, control)
    }

    fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    fn poll_once_with_waker<F: Future + ?Sized>(
        future: Pin<&mut F>,
        waker: &Waker,
    ) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(waker))
    }

    #[test]
    fn simple_read_write() {
        run_worker(|context| async move {
            let (transport, control) = transport(&context, ByteEventCapability::Unsupported, None);
            let payload = repeated(b'a', 65_536);
            let write = transport
                .write(
                    &context,
                    queue(&[&payload], payload.len()),
                    Duration::from_millis(100),
                    CancellationToken::never(),
                )
                .await
                .unwrap();
            assert_eq!(write.bytes_written, payload.len());
            assert_eq!(control.writes(), vec![vec![payload.clone()]]);

            control.feed(queue(&[&payload], payload.len()), false);
            let read = transport
                .read(
                    &context,
                    payload.len(),
                    payload.len(),
                    Duration::ZERO,
                    CancellationToken::never(),
                )
                .await
                .unwrap();
            assert_eq!(read.bytes_read(), payload.len());
            assert_eq!(flatten(read.data()), payload);
            assert!(!read.is_eof());

            let reads_started = control.reads_started();
            let empty = transport
                .read(&context, 0, 0, Duration::ZERO, CancellationToken::never())
                .await
                .unwrap();
            assert_eq!(empty.bytes_read(), 0);
            assert!(!empty.is_eof());
            assert_eq!(control.reads_started(), reads_started);
            transport.close(&context).unwrap();
            drop(transport);
        });
    }

    #[test]
    fn read_preserves_owned_backing() {
        run_worker(|context| async move {
            let (transport, control) = transport(&context, ByteEventCapability::Unsupported, None);
            let allocation = vec![b'a', b'b', b'c', b'd', b'e', b'f'];
            let pointer = allocation.as_ptr();
            let mut inbound = BufQueue::new(allocation.len());
            inbound
                .push_bytes(allocation.into())
                .expect("owned input fits its exact limit");
            control.feed(inbound, false);

            let read = transport
                .read(&context, 6, 3, Duration::ZERO, CancellationToken::never())
                .await
                .expect("owned read succeeds");
            let segments = read
                .data()
                .segments()
                .map(|segment| (segment.as_ptr(), segment.len()))
                .collect::<Vec<_>>();
            assert_eq!(segments, vec![(pointer, 6)]);

            transport.close(&context).expect("transport closes");
        });
    }

    #[test]
    fn bounded_read_preserves_ready_segments_and_backing_after_close() {
        run_worker(|context| async move {
            for known_capacity in [true, false] {
                let (transport, control) =
                    transport(&context, ByteEventCapability::Unsupported, None);
                let mut first = Vec::with_capacity(4096);
                first.extend_from_slice(b"abc");
                let mut second = Vec::with_capacity(2048);
                second.extend_from_slice(b"de");
                let pointers = [first.as_ptr(), second.as_ptr()];
                let capacity = first.capacity() + second.capacity();
                let mut inbound = BufQueue::new(32);
                for segment in [first, second] {
                    if known_capacity {
                        inbound.push_vec(segment).unwrap();
                    } else {
                        inbound.push_bytes(segment.into()).unwrap();
                    }
                }
                control.feed(inbound, true);

                let read = transport
                    .read(&context, 8, 3, Duration::ZERO, CancellationToken::never())
                    .await
                    .unwrap();
                assert!(!read.is_eof());
                let eof = transport
                    .read(&context, 8, 3, Duration::ZERO, CancellationToken::never())
                    .await
                    .unwrap();
                assert!(eof.is_eof());
                assert_eq!(eof.bytes_read(), 0);
                transport.close(&context).unwrap();
                drop(transport);
                drop(control);

                assert_eq!(read.bytes_read(), 5);
                assert_eq!(read.data().limit(), 8);
                assert_eq!(read.data().remaining_capacity(), 3);
                assert_eq!(
                    read.data().backing_capacity_upper_bound(),
                    known_capacity.then_some(capacity)
                );
                assert_eq!(
                    read.data()
                        .segments()
                        .map(|segment| (segment.as_ptr(), segment.len()))
                        .collect::<Vec<_>>(),
                    vec![(pointers[0], 3), (pointers[1], 2)]
                );
                assert_eq!(flatten(read.data()), b"abcde");
            }
        });
    }

    /// N3c: a read whose backend queue is already bounded at the read's maximum returns that
    /// queue instead of moving its bytes into a new one. The result keeps the bound, remaining
    /// capacity, backing bound and segment views that the moved copy had, and data with EOF
    /// still defers the EOF to the next read.
    #[test]
    fn a_read_bounded_at_its_maximum_keeps_the_backends_queue() {
        run_worker(|context| async move {
            let (transport, control) = transport(&context, ByteEventCapability::Unsupported, None);
            control.bound_reads_at_maximum();
            let mut first = Vec::with_capacity(4096);
            first.extend_from_slice(b"abc");
            let mut second = Vec::with_capacity(2048);
            second.extend_from_slice(b"de");
            let pointers = [first.as_ptr(), second.as_ptr()];
            let capacity = first.capacity() + second.capacity();
            let mut inbound = BufQueue::new(32);
            inbound.push_vec(first).unwrap();
            inbound.push_vec(second).unwrap();
            control.feed(inbound, true);

            let rebuilds = read_queue_rebuilds_for_test();
            let read = transport
                .read(&context, 8, 3, Duration::ZERO, CancellationToken::never())
                .await
                .unwrap();
            assert!(
                !read.is_eof(),
                "EOF after data is deferred to the next read"
            );
            assert_eq!(read.bytes_read(), 5);
            assert_eq!(read.data().limit(), 8);
            assert_eq!(read.data().remaining_capacity(), 3);
            assert_eq!(read.data().backing_capacity_upper_bound(), Some(capacity));
            assert_eq!(
                read.data()
                    .segments()
                    .map(|segment| (segment.as_ptr(), segment.len()))
                    .collect::<Vec<_>>(),
                vec![(pointers[0], 3), (pointers[1], 2)]
            );
            let eof = transport
                .read(&context, 8, 3, Duration::ZERO, CancellationToken::never())
                .await
                .unwrap();
            assert!(eof.is_eof());
            assert_eq!(eof.bytes_read(), 0);
            assert_eq!(
                read_queue_rebuilds_for_test(),
                rebuilds,
                "a read already bounded at its maximum keeps the backend's queue",
            );
            transport.close(&context).unwrap();
        });
    }

    /// `DIV-058`: a backend queue bounded at the read's maximum that holds a segment longer than
    /// the allocation size is returned as it is, with that segment whole and its backing bound.
    #[test]
    fn a_read_bounded_at_its_maximum_keeps_segments_over_the_allocation_size() {
        run_worker(|context| async move {
            let (transport, control) = transport(&context, ByteEventCapability::Unsupported, None);
            control.bound_reads_at_maximum();
            let allocation = b"abcdef".to_vec();
            let start = allocation.as_ptr();
            let capacity = allocation.capacity();
            let mut inbound = BufQueue::new(allocation.len());
            inbound.push_vec(allocation).unwrap();
            control.feed(inbound, false);

            let rebuilds = read_queue_rebuilds_for_test();
            let read = transport
                .read(&context, 6, 3, Duration::ZERO, CancellationToken::never())
                .await
                .unwrap();
            assert_eq!(read.data().limit(), 6);
            assert_eq!(
                read.data()
                    .segments()
                    .map(|segment| (segment.as_ptr(), segment.len()))
                    .collect::<Vec<_>>(),
                vec![(start, 6)]
            );
            assert_eq!(read.data().backing_capacity_upper_bound(), Some(capacity));
            assert_eq!(flatten(read.data()), b"abcdef");
            assert_eq!(read_queue_rebuilds_for_test(), rebuilds);
            transport.close(&context).unwrap();
        });
    }

    /// A backend queue with another bound is moved into a queue bounded at the read's
    /// maximum, keeping its segment views and backing bound.
    #[test]
    fn a_read_whose_backend_queue_has_another_bound_is_rebounded_at_its_maximum() {
        run_worker(|context| async move {
            let (transport, control) = transport(&context, ByteEventCapability::Unsupported, None);
            let allocation = Vec::from(&b"abcde"[..]);
            let start = allocation.as_ptr();
            let capacity = allocation.capacity();
            let mut inbound = BufQueue::new(32);
            inbound.push_vec(allocation).unwrap();
            control.feed(inbound, false);

            let rebuilds = read_queue_rebuilds_for_test();
            let read = transport
                .read(&context, 8, 8, Duration::ZERO, CancellationToken::never())
                .await
                .unwrap();
            assert_eq!(
                read.data().limit(),
                8,
                "the backend's queue is bounded at 1 MiB"
            );
            assert_eq!(read.data().remaining_capacity(), 3);
            assert_eq!(read.data().backing_capacity_upper_bound(), Some(capacity));
            assert_eq!(
                read.data()
                    .segments()
                    .map(|segment| (segment.as_ptr(), segment.len()))
                    .collect::<Vec<_>>(),
                vec![(start, 5)]
            );
            assert_eq!(read_queue_rebuilds_for_test(), rebuilds + 1);
            transport.close(&context).unwrap();
        });
    }

    #[test]
    fn write_accepts_owned_segments() {
        run_worker(|context| async move {
            let (transport, control) = transport(&context, ByteEventCapability::Unsupported, None);
            let first = vec![b'a'; 8];
            let second = vec![b'b'; 5];
            let expected_pointers = vec![first.as_ptr() as usize, second.as_ptr() as usize];
            let mut data = BufQueue::new(first.len() + second.len());
            data.push_bytes(first.into())
                .expect("first owned segment fits the write bound");
            data.push_bytes(second.into())
                .expect("second owned segment fits the write bound");

            let written = transport
                .write(&context, data, Duration::ZERO, CancellationToken::never())
                .await
                .expect("owned write succeeds");
            assert_eq!(written.bytes_written, 13);
            assert_eq!(control.write_pointers(), vec![expected_pointers]);

            transport.close(&context).expect("transport closes");
        });
    }

    #[test]
    fn bounded_segmented_read_and_clean_eof() {
        run_worker(|context| async move {
            let (transport, control) = transport(&context, ByteEventCapability::Unsupported, None);
            let segment = repeated(b'a', 1_184);
            let segments = (0..55).map(|_| segment.as_slice()).collect::<Vec<_>>();
            let expected = segments.concat();
            control.feed(queue(&segments, expected.len()), true);

            let mut received = BufQueue::new(expected.len());
            while received.chain_length() < expected.len() {
                let read = transport
                    .read(
                        &context,
                        1_000,
                        257,
                        Duration::ZERO,
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap();
                assert!(read.bytes_read() <= 1_000);
                assert!(!read.is_eof());
                assert_eq!(
                    read.data().limit(),
                    1_000,
                    "the read's queue keeps its bound"
                );
                let mut data = read.into_data();
                received.append(&mut data).unwrap();
            }
            assert_eq!(flatten(&received), expected);

            let eof = transport
                .read(
                    &context,
                    1_000,
                    257,
                    Duration::from_millis(50),
                    CancellationToken::never(),
                )
                .await
                .unwrap();
            assert_eq!(eof.bytes_read(), 0);
            assert!(eof.is_eof());
            transport.close(&context).unwrap();
            drop(transport);
        });
    }

    #[test]
    fn vectored_write_preserves_segments() {
        run_worker(|context| async move {
            let (transport, control) = transport(&context, ByteEventCapability::Unsupported, None);
            let first = repeated(b'a', 65_536);
            let second = repeated(b'b', 65_536);
            let write = transport
                .write(
                    &context,
                    queue(&[&first, &second], first.len() + second.len()),
                    Duration::from_millis(500),
                    CancellationToken::never(),
                )
                .await
                .unwrap();
            assert_eq!(write.bytes_written, first.len() + second.len());
            assert_eq!(control.writes(), vec![vec![first, second]]);
            transport.close(&context).unwrap();
            drop(transport);
        });
    }

    #[test]
    fn read_cancellation_before_and_during_wait() {
        run_worker(|context| async move {
            let (transport, control) = transport(&context, ByteEventCapability::Unsupported, None);
            let before = CancellationSource::new();
            before.request_cancellation();
            assert_eq!(
                transport
                    .read(&context, 1_024, 1_024, Duration::ZERO, before.token())
                    .await
                    .err()
                    .unwrap()
                    .code(),
                HttpErrorCode::CORO_CANCELLED
            );
            assert_eq!(control.reads_started(), 0);

            let during = CancellationSource::new();
            let mut read = transport.read(&context, 1_024, 1_024, Duration::ZERO, during.token());
            assert!(poll_once(read.as_mut()).is_pending());
            assert!(transport.has_pending_read(&context).unwrap());
            during.request_cancellation();
            assert_eq!(
                read.await.err().unwrap().code(),
                HttpErrorCode::CORO_CANCELLED
            );
            assert!(!transport.has_pending_read(&context).unwrap());
            assert_eq!(
                control.actions(),
                vec![IoAction::CancelRead(
                    HttpErrorCode::CORO_CANCELLED,
                    std::thread::current().id(),
                )]
            );

            let partial_cancel = CancellationSource::new();
            let mut partial =
                transport.read(&context, 2, 2, Duration::ZERO, partial_cancel.token());
            assert!(poll_once(partial.as_mut()).is_pending());
            control.feed(queue(&[b"ok"], 2), true);
            partial_cancel.request_cancellation();
            let partial = partial.await.unwrap();
            assert_eq!(flatten(partial.data()), b"ok");
            assert!(!partial.is_eof());
            assert!(
                transport
                    .read(&context, 2, 2, Duration::ZERO, CancellationToken::never(),)
                    .await
                    .unwrap()
                    .is_eof()
            );
            assert_eq!(
                control.actions(),
                vec![IoAction::CancelRead(
                    HttpErrorCode::CORO_CANCELLED,
                    std::thread::current().id(),
                )]
            );
            transport.close(&context).unwrap();
            drop(transport);
        });
    }

    #[test]
    fn read_timeout() {
        run_worker(|context| async move {
            let (transport, control) = transport(&context, ByteEventCapability::Unsupported, None);
            let mut read = transport.read(
                &context,
                1_024,
                1_024,
                Duration::from_millis(50),
                CancellationToken::never(),
            );
            assert!(poll_once(read.as_mut()).is_pending());
            time::advance(Duration::from_millis(50)).await;
            assert_eq!(
                read.await.err().unwrap().code(),
                HttpErrorCode::READ_TIMEOUT
            );
            assert_eq!(
                control.actions(),
                vec![IoAction::CancelRead(
                    HttpErrorCode::READ_TIMEOUT,
                    std::thread::current().id(),
                )]
            );
            assert!(!transport.has_pending_read(&context).unwrap());
            transport.close(&context).unwrap();
            drop(transport);
        });
    }

    #[test]
    fn read_data_observed_before_timeout_commitment_wins() {
        run_worker(|context| async move {
            let (transport, control) = transport(&context, ByteEventCapability::Unsupported, None);
            let mut read = transport.read(
                &context,
                4,
                4,
                Duration::from_millis(25),
                CancellationToken::never(),
            );
            assert!(poll_once(read.as_mut()).is_pending());
            control.feed(queue(&[b"data"], 4), true);
            time::advance(Duration::from_millis(25)).await;
            let data = read.await.unwrap();
            assert_eq!(flatten(data.data()), b"data");
            assert!(!data.is_eof());
            assert!(
                transport
                    .read(&context, 4, 4, Duration::ZERO, CancellationToken::never(),)
                    .await
                    .unwrap()
                    .is_eof()
            );
            assert!(control.actions().is_empty());
            transport.close(&context).unwrap();
            drop(transport);
        });
    }

    #[test]
    fn read_timeout_commitment_precedes_later_ready_data() {
        run_worker(|context| async move {
            let owner = std::thread::current().id();
            let (transport, control) = transport(&context, ByteEventCapability::Unsupported, None);
            let mut read = transport.read(
                &context,
                4,
                4,
                Duration::from_millis(25),
                CancellationToken::never(),
            );
            let timer_signal = Arc::new(WakeSignal::default());
            let timer_waker = Waker::from(Arc::clone(&timer_signal));
            assert!(poll_once_with_waker(read.as_mut(), &timer_waker).is_pending());
            assert!(!timer_signal.take());
            time::advance(Duration::from_millis(25)).await;
            timer_signal.wait().await;

            let commitment_signal = Arc::new(WakeSignal::default());
            let commitment_waker = Waker::from(Arc::clone(&commitment_signal));
            assert!(poll_once_with_waker(read.as_mut(), &commitment_waker).is_pending());
            assert!(
                commitment_signal.take(),
                "the timeout poll must commit and self-wake before data is supplied",
            );
            control.feed(queue(&[b"late"], 4), true);

            assert_eq!(
                read.await.err().unwrap().code(),
                HttpErrorCode::READ_TIMEOUT,
            );
            assert_eq!(
                control.actions(),
                vec![IoAction::CancelRead(HttpErrorCode::READ_TIMEOUT, owner)],
            );
            transport.close(&context).unwrap();
            drop(transport);
        });
    }

    #[test]
    fn write_timeout_commitment_precedes_later_ready_io() {
        run_worker(|context| async move {
            let owner = std::thread::current().id();
            let (transport, control) = transport(&context, ByteEventCapability::Unsupported, None);
            control.block_writes(3);
            let mut write = transport.write(
                &context,
                queue(&[b"later"], 5),
                Duration::from_millis(25),
                CancellationToken::never(),
            );
            let timer_signal = Arc::new(WakeSignal::default());
            let timer_waker = Waker::from(Arc::clone(&timer_signal));
            assert!(poll_once_with_waker(write.as_mut(), &timer_waker).is_pending());
            assert!(!timer_signal.take());
            time::advance(Duration::from_millis(25)).await;
            timer_signal.wait().await;

            let commitment_signal = Arc::new(WakeSignal::default());
            let commitment_waker = Waker::from(Arc::clone(&commitment_signal));
            assert!(poll_once_with_waker(write.as_mut(), &commitment_waker).is_pending());
            assert!(
                commitment_signal.take(),
                "the timeout poll must commit and self-wake before write readiness",
            );
            control.unblock_write();

            let error = write.await.err().unwrap();
            assert_eq!(error.code(), HttpErrorCode::WRITE_TIMEOUT);
            assert_eq!(error.bytes_written(), 3);
            assert_eq!(
                control.actions(),
                vec![IoAction::CancelWrite(HttpErrorCode::WRITE_TIMEOUT, owner)],
            );
            transport.close(&context).unwrap();
            drop(transport);
        });
    }

    #[test]
    fn read_passes_clamped_allocation_size_to_io() {
        run_worker(|context| async move {
            // `transport` caps reads at a 131_072-byte maximum_read_size.
            let (transport, control) = transport(&context, ByteEventCapability::Unsupported, None);
            for (maximum, allocation_size) in [
                (8_192, 1_024),
                (100, 4_000),
                (200_000, 150_000),
                (200_000, 4_000),
            ] {
                control.feed(queue(&[b"x"], 1), false);
                let read = transport
                    .read(
                        &context,
                        maximum,
                        allocation_size,
                        Duration::ZERO,
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap();
                assert_eq!(flatten(read.data()), b"x");
            }
            assert_eq!(
                control.read_sizes(),
                vec![
                    (8_192, 1_024),
                    (100, 100),
                    (131_072, 131_072),
                    (131_072, 4_000),
                ],
                "the I/O receives the maximum clamped to the limit and the \
                 allocation size clamped to that maximum",
            );
            transport.close(&context).unwrap();
            drop(transport);
        });
    }

    #[test]
    fn read_error_is_not_eof() {
        run_worker(|context| async move {
            let (transport, control) = transport(&context, ByteEventCapability::Unsupported, None);
            control.fail_read(HttpErrorCode::TRANSPORT_READ_ERROR);
            let error = transport
                .read(
                    &context,
                    1_024,
                    1_024,
                    Duration::ZERO,
                    CancellationToken::never(),
                )
                .await
                .err()
                .unwrap();
            assert_eq!(error.code(), HttpErrorCode::TRANSPORT_READ_ERROR);

            control.eof();
            let eof = transport
                .read(
                    &context,
                    1_024,
                    1_024,
                    Duration::ZERO,
                    CancellationToken::never(),
                )
                .await
                .unwrap();
            assert!(eof.is_eof());
            assert_eq!(eof.bytes_read(), 0);
            transport.close(&context).unwrap();
            drop(transport);
        });
    }

    #[test]
    fn write_cancellation_before_and_during_wait() {
        run_worker(|context| async move {
            let owner = std::thread::current().id();
            let (before_transport, before_control) =
                transport(&context, ByteEventCapability::Unsupported, None);
            let mut before_read =
                before_transport.read(&context, 8, 8, Duration::ZERO, CancellationToken::never());
            assert!(poll_once(before_read.as_mut()).is_pending());
            let before = CancellationSource::new();
            before.request_cancellation();
            let error = before_transport
                .write(
                    &context,
                    queue(&[b"cancel"], 6),
                    Duration::ZERO,
                    before.token(),
                )
                .await
                .err()
                .unwrap();
            assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
            assert_eq!(error.bytes_written(), 0);
            assert_eq!(before_control.writes_started(), 0);
            assert_eq!(
                before_read.await.err().unwrap().code(),
                HttpErrorCode::TRANSPORT_READ_ERROR,
            );
            assert_eq!(
                before_control.actions(),
                vec![
                    IoAction::CancelRead(HttpErrorCode::TRANSPORT_READ_ERROR, owner),
                    IoAction::Reset(owner),
                ],
            );
            assert_eq!(
                before_transport
                    .write(
                        &context,
                        queue(&[b"closed"], 6),
                        Duration::ZERO,
                        CancellationToken::never(),
                    )
                    .await
                    .err()
                    .unwrap()
                    .code(),
                HttpErrorCode::TRANSPORT_WRITE_ERROR,
            );
            drop(before_transport);
            assert_eq!(
                before_control.actions(),
                vec![
                    IoAction::CancelRead(HttpErrorCode::TRANSPORT_READ_ERROR, owner),
                    IoAction::Reset(owner),
                    IoAction::Drop(owner),
                ],
            );

            let (transport, control) =
                transport(&context, ByteEventCapability::TransportEvents, None);
            let log = Rc::new(ByteEventLog::default());
            transport
                .register_byte_event(
                    &context,
                    HttpByteEventRegistration::new(None, ByteEventType::TransportWrite.bit(), &log),
                )
                .unwrap();
            let mut pending_read =
                transport.read(&context, 8, 8, Duration::ZERO, CancellationToken::never());
            assert!(poll_once(pending_read.as_mut()).is_pending());
            control.block_writes(3);
            control.report_write_progress_on_cancel(2);
            let during = CancellationSource::new();
            let mut write = transport.write(
                &context,
                queue(&[b"blocked"], 7),
                Duration::ZERO,
                during.token(),
            );
            assert!(poll_once(write.as_mut()).is_pending());
            assert!(transport.has_pending_write(&context).unwrap());
            during.request_cancellation();
            let error = write.await.err().unwrap();
            assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
            assert_eq!(error.bytes_written(), 5);
            assert!(!transport.has_pending_write(&context).unwrap());
            assert_eq!(
                pending_read.await.err().unwrap().code(),
                HttpErrorCode::TRANSPORT_READ_ERROR,
            );
            assert_eq!(
                control.actions(),
                vec![
                    IoAction::CancelRead(HttpErrorCode::TRANSPORT_READ_ERROR, owner),
                    IoAction::CancelWrite(HttpErrorCode::TRANSPORT_WRITE_ERROR, owner),
                    IoAction::Reset(owner),
                ],
            );
            assert_eq!(
                *log.cancelled.borrow(),
                vec![(
                    ByteEventType::TransportWrite,
                    HttpErrorCode::TRANSPORT_WRITE_ERROR,
                    owner,
                )],
            );
            assert_eq!(
                transport
                    .write(
                        &context,
                        queue(&[b"closed"], 6),
                        Duration::ZERO,
                        CancellationToken::never(),
                    )
                    .await
                    .err()
                    .unwrap()
                    .code(),
                HttpErrorCode::TRANSPORT_WRITE_ERROR,
            );
            drop(transport);
            assert_eq!(log.cancelled.borrow().len(), 1);
            assert_eq!(
                control.actions(),
                vec![
                    IoAction::CancelRead(HttpErrorCode::TRANSPORT_READ_ERROR, owner),
                    IoAction::CancelWrite(HttpErrorCode::TRANSPORT_WRITE_ERROR, owner),
                    IoAction::Reset(owner),
                    IoAction::Drop(owner),
                ],
            );
        });
    }

    #[test]
    fn abandoning_pending_write_resets_transport_once() {
        run_worker(|context| async move {
            let owner = std::thread::current().id();
            let (transport, control) =
                transport(&context, ByteEventCapability::TransportEvents, None);
            let log = Rc::new(ByteEventLog::default());
            transport
                .register_byte_event(
                    &context,
                    HttpByteEventRegistration::new(None, ByteEventType::TransportWrite.bit(), &log),
                )
                .unwrap();
            let mut read =
                transport.read(&context, 8, 8, Duration::ZERO, CancellationToken::never());
            control.block_writes(2);
            let mut write = transport.write(
                &context,
                queue(&[b"abandon"], 7),
                Duration::ZERO,
                CancellationToken::never(),
            );
            assert!(poll_once(read.as_mut()).is_pending());
            assert!(poll_once(write.as_mut()).is_pending());

            drop(write);
            assert_eq!(
                read.await.err().unwrap().code(),
                HttpErrorCode::TRANSPORT_READ_ERROR,
            );
            let expected = vec![
                IoAction::CancelRead(HttpErrorCode::TRANSPORT_READ_ERROR, owner),
                IoAction::CancelWrite(HttpErrorCode::TRANSPORT_WRITE_ERROR, owner),
                IoAction::Reset(owner),
            ];
            assert_eq!(control.actions(), expected);
            assert_eq!(
                *log.cancelled.borrow(),
                vec![(
                    ByteEventType::TransportWrite,
                    HttpErrorCode::TRANSPORT_WRITE_ERROR,
                    owner,
                )],
            );
            assert_eq!(
                transport
                    .read(&context, 8, 8, Duration::ZERO, CancellationToken::never(),)
                    .await
                    .err()
                    .unwrap()
                    .code(),
                HttpErrorCode::TRANSPORT_READ_ERROR,
            );
            drop(transport);
            assert_eq!(log.cancelled.borrow().len(), 1);
            assert_eq!(
                control.actions(),
                vec![
                    IoAction::CancelRead(HttpErrorCode::TRANSPORT_READ_ERROR, owner),
                    IoAction::CancelWrite(HttpErrorCode::TRANSPORT_WRITE_ERROR, owner),
                    IoAction::Reset(owner),
                    IoAction::Drop(owner),
                ],
            );
        });
    }

    #[test]
    fn shutdown_write_preserves_read_half() {
        run_worker(|context| async move {
            let (transport, control) = transport(&context, ByteEventCapability::Unsupported, None);
            let mut pending_read =
                transport.read(&context, 8, 4, Duration::ZERO, CancellationToken::never());
            assert!(poll_once(pending_read.as_mut()).is_pending());
            transport.shutdown_write(&context).unwrap();
            assert!(poll_once(pending_read.as_mut()).is_pending());
            assert_eq!(
                transport
                    .write(
                        &context,
                        queue(&[b"x"], 1),
                        Duration::ZERO,
                        CancellationToken::never(),
                    )
                    .await
                    .err()
                    .unwrap()
                    .code(),
                HttpErrorCode::TRANSPORT_WRITE_ERROR
            );
            control.feed(queue(&[b"response"], 8), true);
            let data = pending_read.await.unwrap();
            assert_eq!(flatten(data.data()), b"response");
            assert!(!data.is_eof());
            assert!(
                transport
                    .read(&context, 8, 4, Duration::ZERO, CancellationToken::never(),)
                    .await
                    .unwrap()
                    .is_eof()
            );
            assert!(
                control
                    .actions()
                    .contains(&IoAction::ShutdownWrite(std::thread::current().id()))
            );
            transport.close(&context).unwrap();
            drop(transport);
        });
    }

    #[test]
    fn reset_close_is_abortive() {
        run_worker(|context| async move {
            let owner = std::thread::current().id();
            let (transport, control) = transport(&context, ByteEventCapability::Unsupported, None);
            control.block_writes(0);
            let mut read = transport.read(
                &context,
                1_024,
                1_024,
                Duration::ZERO,
                CancellationToken::never(),
            );
            let mut write = transport.write(
                &context,
                queue(&[b"blocked"], 7),
                Duration::ZERO,
                CancellationToken::never(),
            );
            assert!(poll_once(read.as_mut()).is_pending());
            assert!(poll_once(write.as_mut()).is_pending());
            transport.close_with_reset(&context).unwrap();
            assert!(!control.has_backend_waiters());
            assert_eq!(
                read.await.err().unwrap().code(),
                HttpErrorCode::TRANSPORT_READ_ERROR
            );
            assert_eq!(
                write.await.err().unwrap().code(),
                HttpErrorCode::TRANSPORT_WRITE_ERROR
            );
            assert!(!transport.has_pending_read(&context).unwrap());
            assert!(!transport.has_pending_write(&context).unwrap());
            assert_eq!(
                control.actions(),
                vec![
                    IoAction::CancelRead(HttpErrorCode::TRANSPORT_READ_ERROR, owner,),
                    IoAction::CancelWrite(HttpErrorCode::TRANSPORT_WRITE_ERROR, owner,),
                    IoAction::Reset(owner),
                ]
            );
            drop(transport);
            assert_eq!(
                control.actions(),
                vec![
                    IoAction::CancelRead(HttpErrorCode::TRANSPORT_READ_ERROR, owner),
                    IoAction::CancelWrite(HttpErrorCode::TRANSPORT_WRITE_ERROR, owner),
                    IoAction::Reset(owner),
                    IoAction::Drop(owner),
                ]
            );
        });
    }

    #[test]
    fn close_and_drop_own_pending_operation_cleanup() {
        run_worker(|context| async move {
            let owner = std::thread::current().id();
            let (closed, closed_control) =
                transport(&context, ByteEventCapability::Unsupported, None);
            closed_control.block_writes(0);
            let mut read = closed.read(&context, 8, 8, Duration::ZERO, CancellationToken::never());
            let mut write = closed.write(
                &context,
                queue(&[b"pending"], 7),
                Duration::ZERO,
                CancellationToken::never(),
            );
            assert!(poll_once(read.as_mut()).is_pending());
            assert!(poll_once(write.as_mut()).is_pending());
            assert!(closed_control.has_backend_waiters());

            closed.close(&context).unwrap();
            assert!(closed_control.has_backend_waiters());
            assert!(read.await.unwrap().is_eof());
            closed_control.unblock_write();
            assert_eq!(write.await.unwrap().bytes_written, 7);
            assert!(!closed_control.has_backend_waiters());
            assert_eq!(
                closed_control.actions(),
                vec![
                    IoAction::CancelRead(HttpErrorCode::TRANSPORT_EOF, owner),
                    IoAction::ShutdownWrite(owner),
                    IoAction::Close(owner),
                ]
            );
            drop(closed);
            assert_eq!(
                closed_control.actions(),
                vec![
                    IoAction::CancelRead(HttpErrorCode::TRANSPORT_EOF, owner),
                    IoAction::ShutdownWrite(owner),
                    IoAction::Close(owner),
                    IoAction::Drop(owner),
                ]
            );
            assert_eq!(context.handle().state(), WorkerState::Open);

            let (dropped, dropped_control) =
                transport(&context, ByteEventCapability::Unsupported, None);
            dropped_control.block_writes(0);
            let mut read = dropped.read(&context, 8, 8, Duration::ZERO, CancellationToken::never());
            let mut write = dropped.write(
                &context,
                queue(&[b"pending"], 7),
                Duration::ZERO,
                CancellationToken::never(),
            );
            assert!(poll_once(read.as_mut()).is_pending());
            assert!(poll_once(write.as_mut()).is_pending());

            drop(dropped);
            assert!(!dropped_control.has_backend_waiters());
            let actions_after_transport_drop = vec![
                IoAction::CancelRead(HttpErrorCode::DROPPED, owner),
                IoAction::CancelWrite(HttpErrorCode::DROPPED, owner),
                IoAction::Close(owner),
                IoAction::Drop(owner),
            ];
            assert_eq!(dropped_control.actions(), actions_after_transport_drop);
            assert_eq!(context.handle().state(), WorkerState::Open);
            assert_eq!(read.await.err().unwrap().code(), HttpErrorCode::DROPPED);
            drop(write);
            assert_eq!(dropped_control.actions(), actions_after_transport_drop);
        });
    }

    #[test]
    fn write_terminals_preserve_progress_and_cancel_events() {
        run_worker(|context| async move {
            let owner = std::thread::current().id();

            let (timed, timed_control) =
                transport(&context, ByteEventCapability::TransportEvents, None);
            let timed_log = Rc::new(ByteEventLog::default());
            for event_type in [ByteEventType::TransportWrite, ByteEventType::KernelWrite] {
                timed
                    .register_byte_event(
                        &context,
                        HttpByteEventRegistration::new(None, event_type.bit(), &timed_log),
                    )
                    .unwrap();
            }
            assert_eq!(
                timed.register_byte_event(
                    &context,
                    HttpByteEventRegistration::new(None, ByteEventType::NicTx.bit(), &timed_log,),
                ),
                Err(ByteEventRegistrationError::CapacityExhausted)
            );
            timed_control.block_writes(3);
            let mut write = timed.write(
                &context,
                queue(&[b"timeout"], 7),
                Duration::from_millis(50),
                CancellationToken::never(),
            );
            assert!(poll_once(write.as_mut()).is_pending());
            time::advance(Duration::from_millis(50)).await;
            let error = write.await.err().unwrap();
            assert_eq!(error.code(), HttpErrorCode::WRITE_TIMEOUT);
            assert_eq!(error.bytes_written(), 3);
            assert_eq!(
                timed_control.actions(),
                vec![IoAction::CancelWrite(HttpErrorCode::WRITE_TIMEOUT, owner)]
            );
            assert_eq!(
                *timed_log.cancelled.borrow(),
                vec![
                    (
                        ByteEventType::NicTx,
                        HttpErrorCode::ENHANCE_YOUR_CALM,
                        owner,
                    ),
                    (
                        ByteEventType::TransportWrite,
                        HttpErrorCode::WRITE_TIMEOUT,
                        owner,
                    ),
                    (
                        ByteEventType::KernelWrite,
                        HttpErrorCode::WRITE_TIMEOUT,
                        owner,
                    ),
                ]
            );
            timed.close(&context).unwrap();
            drop(timed);
            assert_eq!(timed_log.cancelled.borrow().len(), 3);

            let (failed, failed_control) =
                transport(&context, ByteEventCapability::TransportEvents, None);
            let failed_log = Rc::new(ByteEventLog::default());
            failed
                .register_byte_event(
                    &context,
                    HttpByteEventRegistration::new(
                        None,
                        ByteEventType::TransportWrite.bit(),
                        &failed_log,
                    ),
                )
                .unwrap();
            failed_control.block_writes(2);
            let mut write = failed.write(
                &context,
                queue(&[b"failed"], 6),
                Duration::ZERO,
                CancellationToken::never(),
            );
            assert!(poll_once(write.as_mut()).is_pending());
            failed_control.fail_write(HttpErrorCode::TRANSPORT_WRITE_ERROR);
            let error = write.await.err().unwrap();
            assert_eq!(error.code(), HttpErrorCode::TRANSPORT_WRITE_ERROR);
            assert_eq!(error.bytes_written(), 2);
            assert_eq!(
                *failed_log.cancelled.borrow(),
                vec![(
                    ByteEventType::TransportWrite,
                    HttpErrorCode::TRANSPORT_WRITE_ERROR,
                    owner,
                )]
            );
            failed.close(&context).unwrap();
            drop(failed);
            assert_eq!(failed_log.cancelled.borrow().len(), 1);

            let (partial, partial_control) =
                transport(&context, ByteEventCapability::TransportEvents, None);
            let partial_log = Rc::new(ByteEventLog::default());
            partial
                .register_byte_event(
                    &context,
                    HttpByteEventRegistration::new(
                        None,
                        ByteEventType::TransportWrite.bit(),
                        &partial_log,
                    ),
                )
                .unwrap();
            partial_control.complete_write_at(2);
            let error = partial
                .write(
                    &context,
                    queue(&[b"partial"], 7),
                    Duration::ZERO,
                    CancellationToken::never(),
                )
                .await
                .err()
                .unwrap();
            assert_eq!(error.code(), HttpErrorCode::TRANSPORT_WRITE_ERROR);
            assert_eq!(error.bytes_written(), 2);
            assert_eq!(
                *partial_log.cancelled.borrow(),
                vec![(
                    ByteEventType::TransportWrite,
                    HttpErrorCode::TRANSPORT_WRITE_ERROR,
                    owner,
                )]
            );
            partial.close(&context).unwrap();
            drop(partial);
            assert_eq!(partial_log.cancelled.borrow().len(), 1);

            let (excess, excess_control) =
                transport(&context, ByteEventCapability::TransportEvents, None);
            let excess_log = Rc::new(ByteEventLog::default());
            excess
                .register_byte_event(
                    &context,
                    HttpByteEventRegistration::new(
                        None,
                        ByteEventType::TransportWrite.bit(),
                        &excess_log,
                    ),
                )
                .unwrap();
            excess_control.report_excess_write_progress(8);
            let error = excess
                .write(
                    &context,
                    queue(&[b"excess!"], 7),
                    Duration::ZERO,
                    CancellationToken::never(),
                )
                .await
                .err()
                .unwrap();
            assert_eq!(error.code(), HttpErrorCode::TRANSPORT_WRITE_ERROR);
            assert_eq!(error.bytes_written(), 0);
            assert_eq!(
                *excess_log.cancelled.borrow(),
                vec![(
                    ByteEventType::TransportWrite,
                    HttpErrorCode::TRANSPORT_WRITE_ERROR,
                    owner,
                )]
            );
            excess.close(&context).unwrap();
            drop(excess);
            assert_eq!(excess_log.cancelled.borrow().len(), 1);

            let (shutdown, shutdown_control) =
                transport(&context, ByteEventCapability::TransportEvents, None);
            let shutdown_log = Rc::new(ByteEventLog::default());
            shutdown
                .register_byte_event(
                    &context,
                    HttpByteEventRegistration::new(
                        None,
                        ByteEventType::TransportWrite.bit(),
                        &shutdown_log,
                    ),
                )
                .unwrap();
            assert_eq!(
                shutdown
                    .write(
                        &context,
                        queue(&[b"written"], 7),
                        Duration::ZERO,
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap()
                    .bytes_written,
                7
            );
            assert!(shutdown_log.cancelled.borrow().is_empty());
            shutdown.shutdown_write(&context).unwrap();
            shutdown.shutdown_write(&context).unwrap();
            assert_eq!(
                shutdown_control.actions(),
                vec![IoAction::ShutdownWrite(owner)]
            );
            assert_eq!(
                *shutdown_log.cancelled.borrow(),
                vec![(
                    ByteEventType::TransportWrite,
                    HttpErrorCode::TRANSPORT_WRITE_ERROR,
                    owner,
                )]
            );
            shutdown.close(&context).unwrap();
            drop(shutdown);
            assert_eq!(shutdown_log.cancelled.borrow().len(), 1);
        });
    }

    #[test]
    fn driver_shutdown_drops_live_transport_and_operations() {
        Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("current-thread runtime")
            .block_on(LocalSet::new().run_until(async move {
                let owner = std::thread::current().id();
                let actions = Arc::new(Mutex::new(Vec::new()));
                let (worker, driver) = WorkerDriver::new(4).unwrap();
                let (started_tx, started_rx) = tokio::sync::oneshot::channel();
                let task_actions = Arc::clone(&actions);
                worker
                    .spawn_local(move |context| async move {
                        let transport = OwnedByteStream::new(
                            ShutdownProbeIo {
                                actions: task_actions,
                                owner,
                                worker: context.handle().clone(),
                            },
                            context.handle().clone(),
                            &context,
                            ByteStreamLimits::new(8, 8, 1).unwrap(),
                            SocketAddress::new(b"127.0.0.1", 1),
                            SocketAddress::new(b"127.0.0.1", 2),
                            None,
                            ByteEventCapability::Unsupported,
                        )
                        .unwrap();
                        let read = transport.read(
                            &context,
                            8,
                            8,
                            Duration::ZERO,
                            CancellationToken::never(),
                        );
                        let write = transport.write(
                            &context,
                            queue(&[b"pending"], 7),
                            Duration::ZERO,
                            CancellationToken::never(),
                        );
                        let mut live = LiveTransportOperations {
                            transport: Some(transport),
                            read: Some(read),
                            write: Some(write),
                        };
                        assert!(poll_once(live.read.as_mut().unwrap().as_mut()).is_pending());
                        assert!(poll_once(live.write.as_mut().unwrap().as_mut()).is_pending());
                        started_tx.send(()).unwrap();
                        std::future::pending::<()>().await;
                        drop(live);
                    })
                    .unwrap();
                let driver_task = tokio::task::spawn_local(driver);
                started_rx.await.unwrap();
                assert!(actions.lock().unwrap().is_empty());
                assert!(worker.begin_shutdown());
                driver_task.await.unwrap();
                assert_eq!(worker.state(), WorkerState::Closed);
                assert_eq!(
                    *actions.lock().unwrap(),
                    vec![
                        IoAction::CancelRead(HttpErrorCode::DROPPED, owner),
                        IoAction::CancelWrite(HttpErrorCode::DROPPED, owner),
                        IoAction::Close(owner),
                        IoAction::Drop(owner),
                    ]
                );
            }));
    }

    #[test]
    fn driver_shutdown_cleans_transport_and_operations_that_escape_the_task() {
        Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("current-thread runtime")
            .block_on(LocalSet::new().run_until(async move {
                let owner = std::thread::current().id();
                let actions = Arc::new(Mutex::new(Vec::new()));
                let (worker, driver) = WorkerDriver::new(4).unwrap();
                let (started_tx, started_rx) = tokio::sync::oneshot::channel();
                let task_actions = Arc::clone(&actions);
                worker
                    .spawn_local(move |context| async move {
                        let transport = OwnedByteStream::new(
                            ShutdownProbeIo {
                                actions: task_actions,
                                owner,
                                worker: context.handle().clone(),
                            },
                            context.handle().clone(),
                            &context,
                            ByteStreamLimits::new(8, 8, 1).unwrap(),
                            SocketAddress::new(b"127.0.0.1", 1),
                            SocketAddress::new(b"127.0.0.1", 2),
                            None,
                            ByteEventCapability::Unsupported,
                        )
                        .unwrap();
                        let read = transport.read(
                            &context,
                            8,
                            8,
                            Duration::ZERO,
                            CancellationToken::never(),
                        );
                        let write = transport.write(
                            &context,
                            queue(&[b"pending"], 7),
                            Duration::ZERO,
                            CancellationToken::never(),
                        );
                        let mut live = LiveTransportOperations {
                            transport: Some(transport),
                            read: Some(read),
                            write: Some(write),
                        };
                        assert!(poll_once(live.read.as_mut().unwrap().as_mut()).is_pending());
                        assert!(poll_once(live.write.as_mut().unwrap().as_mut()).is_pending());
                        CAPTURED_CONTEXT.with(|captured| {
                            assert!(captured.replace(Some(context.clone())).is_none());
                        });
                        ESCAPED_LIVE_TRANSPORT.with(|escaped| {
                            assert!(escaped.replace(Some(live)).is_none());
                        });
                        started_tx.send(()).unwrap();
                    })
                    .unwrap();
                let driver_task = tokio::task::spawn_local(driver);
                started_rx.await.unwrap();
                assert!(actions.lock().unwrap().is_empty());

                assert!(worker.begin_shutdown());
                driver_task.await.unwrap();
                assert_eq!(worker.state(), WorkerState::Closed);
                let expected = vec![
                    IoAction::CancelRead(HttpErrorCode::DROPPED, owner),
                    IoAction::CancelWrite(HttpErrorCode::DROPPED, owner),
                    IoAction::Close(owner),
                    IoAction::Drop(owner),
                ];
                assert_eq!(*actions.lock().unwrap(), expected);

                ESCAPED_LIVE_TRANSPORT.with(|escaped| drop(escaped.replace(None)));
                CAPTURED_CONTEXT.with(|captured| drop(captured.replace(None)));
                assert_eq!(*actions.lock().unwrap(), expected);
            }));
    }

    #[test]
    fn exact_worker_context_and_open_worker_are_required() {
        run_worker(|context| async move {
            let owner = std::thread::current().id();
            let (wrong_worker, wrong_driver) = WorkerDriver::new(2).unwrap();
            let capture = wrong_worker
                .call_local(|wrong_context| async move {
                    CAPTURED_CONTEXT.with(|captured| captured.replace(Some(wrong_context)));
                })
                .unwrap();
            let wrong_driver_task = tokio::task::spawn_local(wrong_driver);
            capture.await.unwrap();
            let wrong_context = CAPTURED_CONTEXT.with(|captured| {
                captured
                    .borrow_mut()
                    .take()
                    .expect("wrong worker context was captured")
            });
            let wrong_state = Rc::new(RefCell::new(FakeState::new()));
            let wrong_control = FakeControl {
                state: Rc::clone(&wrong_state),
            };
            let error = OwnedByteStream::new(
                FakeIo { state: wrong_state },
                wrong_worker.clone(),
                &context,
                ByteStreamLimits::new(8, 8, 1).unwrap(),
                SocketAddress::new(b"127.0.0.1", 1),
                SocketAddress::new(b"127.0.0.1", 2),
                None,
                ByteEventCapability::Unsupported,
            )
            .err()
            .unwrap();
            assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
            assert_eq!(wrong_control.actions(), vec![IoAction::Drop(owner)]);

            let (transport, control) = transport(&context, ByteEventCapability::Unsupported, None);
            assert_eq!(
                transport
                    .read(
                        &wrong_context,
                        1,
                        1,
                        Duration::ZERO,
                        CancellationToken::never(),
                    )
                    .await
                    .err()
                    .unwrap()
                    .code(),
                HttpErrorCode::INVALID_STATE_TRANSITION
            );
            assert_eq!(control.reads_started(), 0);
            let wrong_log = Rc::new(ByteEventLog::default());
            assert_eq!(
                transport.register_byte_event(
                    &wrong_context,
                    HttpByteEventRegistration::new(
                        None,
                        ByteEventType::TransportWrite.bit(),
                        &wrong_log,
                    ),
                ),
                Err(ByteEventRegistrationError::WrongWorker)
            );
            assert_eq!(
                *wrong_log.cancelled.borrow(),
                vec![(
                    ByteEventType::TransportWrite,
                    HttpErrorCode::INVALID_STATE_TRANSITION,
                    owner,
                )]
            );
            assert!(wrong_log.fired.borrow().is_empty());
            assert!(control.actions().is_empty());
            wrong_worker.begin_shutdown();
            wrong_driver_task.await.unwrap();

            assert!(context.handle().begin_shutdown());
            assert_eq!(context.handle().state(), WorkerState::Closing);
            assert_eq!(
                transport.register_byte_event(
                    &wrong_context,
                    HttpByteEventRegistration::new(
                        None,
                        ByteEventType::KernelWrite.bit(),
                        &wrong_log,
                    ),
                ),
                Err(ByteEventRegistrationError::WrongWorker)
            );
            assert_eq!(
                *wrong_log.cancelled.borrow(),
                vec![
                    (
                        ByteEventType::TransportWrite,
                        HttpErrorCode::INVALID_STATE_TRANSITION,
                        owner,
                    ),
                    (
                        ByteEventType::KernelWrite,
                        HttpErrorCode::INVALID_STATE_TRANSITION,
                        owner,
                    ),
                ]
            );
            assert!(wrong_log.fired.borrow().is_empty());
            assert!(control.actions().is_empty());
            drop(wrong_context);
            let closed_state = Rc::new(RefCell::new(FakeState::new()));
            let closed_control = FakeControl {
                state: Rc::clone(&closed_state),
            };
            let error = OwnedByteStream::new(
                FakeIo {
                    state: closed_state,
                },
                context.handle().clone(),
                &context,
                ByteStreamLimits::new(8, 8, 1).unwrap(),
                SocketAddress::new(b"127.0.0.1", 1),
                SocketAddress::new(b"127.0.0.1", 2),
                None,
                ByteEventCapability::Unsupported,
            )
            .err()
            .unwrap();
            assert_eq!(error.code(), HttpErrorCode::DROPPED);
            assert_eq!(closed_control.actions(), vec![IoAction::Drop(owner)]);
            assert_eq!(
                transport
                    .read(&context, 1, 1, Duration::ZERO, CancellationToken::never(),)
                    .await
                    .err()
                    .unwrap()
                    .code(),
                HttpErrorCode::DROPPED
            );
            assert_eq!(control.reads_started(), 0);
            drop(transport);
            assert_eq!(
                control.actions(),
                vec![IoAction::Close(owner), IoAction::Drop(owner)]
            );
            assert_eq!(wrong_log.cancelled.borrow().len(), 2);
            assert!(wrong_log.fired.borrow().is_empty());
        });
    }

    #[test]
    fn local_and_peer_addresses_are_owned() {
        run_worker(|context| async move {
            let (transport, _control) = transport(&context, ByteEventCapability::Unsupported, None);
            let local = transport.local_address(&context).unwrap();
            let peer = transport.peer_address(&context).unwrap();
            assert_eq!(local, SocketAddress::new(b"127.0.0.1", 8080));
            assert_eq!(peer, SocketAddress::new(b"127.0.0.2", 443));
            transport.close(&context).unwrap();
            drop(transport);
            assert_eq!(local.host(), b"127.0.0.1");
            assert_eq!(peer.host(), b"127.0.0.2");
        });
    }

    #[test]
    fn peer_identity_is_optional_and_owned() {
        run_worker(|context| async move {
            let (identified, _identified_control) = transport(
                &context,
                ByteEventCapability::Unsupported,
                Some(PeerIdentity::new(b"peer-certificate-identity")),
            );
            let identity = identified.peer_identity(&context).unwrap().unwrap();
            identified.close(&context).unwrap();
            drop(identified);
            assert_eq!(identity.as_bytes(), b"peer-certificate-identity");

            let (anonymous, _anonymous_control) =
                transport(&context, ByteEventCapability::Unsupported, None);
            assert!(anonymous.peer_identity(&context).unwrap().is_none());
            anonymous.close(&context).unwrap();
            drop(anonymous);
        });
    }

    #[test]
    fn optional_byte_event_capability_is_explicit() {
        run_worker(|context| async move {
            let (unsupported, _control) =
                transport(&context, ByteEventCapability::Unsupported, None);
            let unsupported_log = Rc::new(ByteEventLog::default());
            let bits = ByteEventType::TransportWrite.bit() | ByteEventType::KernelWrite.bit();
            assert_eq!(
                unsupported.register_byte_event(
                    &context,
                    HttpByteEventRegistration::new(None, bits, &unsupported_log),
                ),
                Err(ByteEventRegistrationError::Unsupported)
            );
            assert_eq!(
                *unsupported_log.cancelled.borrow(),
                vec![
                    (
                        ByteEventType::TransportWrite,
                        HttpErrorCode::INVALID_STATE_TRANSITION,
                        std::thread::current().id(),
                    ),
                    (
                        ByteEventType::KernelWrite,
                        HttpErrorCode::INVALID_STATE_TRANSITION,
                        std::thread::current().id(),
                    ),
                ]
            );
            unsupported.close(&context).unwrap();
            drop(unsupported);

            let (supported, _control) =
                transport(&context, ByteEventCapability::TransportEvents, None);
            let supported_log = Rc::new(ByteEventLog::default());
            assert_eq!(
                supported.byte_event_capability(&context).unwrap(),
                ByteEventCapability::TransportEvents
            );
            supported
                .register_byte_event(
                    &context,
                    HttpByteEventRegistration::new(
                        None,
                        ByteEventType::TransportWrite.bit(),
                        &supported_log,
                    ),
                )
                .unwrap();
            let mut registrations = supported.take_byte_event_registrations(&context).unwrap();
            assert_eq!(registrations.len(), 1);
            assert!(registrations[0].fire(HttpByteEvent::new(ByteEventType::TransportWrite)));
            assert_eq!(
                *supported_log.fired.borrow(),
                vec![ByteEventType::TransportWrite]
            );
            assert!(supported_log.cancelled.borrow().is_empty());
            supported.close(&context).unwrap();
            drop(supported);
            drop(registrations);
        });
    }

    #[test]
    fn cancellation_and_destruction_run_on_owner_worker() {
        run_worker(|context| async move {
            let owner = std::thread::current().id();
            let (transport, control) =
                transport(&context, ByteEventCapability::TransportEvents, None);
            assert_eq!(transport.worker_id(&context).unwrap(), context.worker_id());
            let byte_event_log = Rc::new(ByteEventLog::default());
            transport
                .register_byte_event(
                    &context,
                    HttpByteEventRegistration::new(
                        None,
                        ByteEventType::TransportWrite.bit(),
                        &byte_event_log,
                    ),
                )
                .unwrap();
            control.block_writes(0);
            let read_cancel = CancellationSource::new();
            let write_cancel = CancellationSource::new();
            let mut read = transport.read(&context, 8, 8, Duration::ZERO, read_cancel.token());
            let mut write = transport.write(
                &context,
                queue(&[b"pending"], 7),
                Duration::ZERO,
                write_cancel.token(),
            );
            assert!(poll_once(read.as_mut()).is_pending());
            assert!(poll_once(write.as_mut()).is_pending());

            std::thread::spawn(move || {
                read_cancel.request_cancellation();
                write_cancel.request_cancellation();
            })
            .join()
            .unwrap();
            assert_eq!(
                read.await.err().unwrap().code(),
                HttpErrorCode::CORO_CANCELLED
            );
            assert_eq!(
                write.await.err().unwrap().code(),
                HttpErrorCode::CORO_CANCELLED
            );

            assert!(!transport.has_pending_read(&context).unwrap());
            assert!(!transport.has_pending_write(&context).unwrap());
            assert_eq!(
                transport.register_byte_event(
                    &context,
                    HttpByteEventRegistration::new(
                        None,
                        ByteEventType::KernelWrite.bit(),
                        &byte_event_log,
                    ),
                ),
                Err(ByteEventRegistrationError::Closed),
            );
            drop(transport);
            assert_eq!(
                control.actions(),
                vec![
                    IoAction::CancelRead(HttpErrorCode::CORO_CANCELLED, owner),
                    IoAction::CancelWrite(HttpErrorCode::TRANSPORT_WRITE_ERROR, owner),
                    IoAction::Reset(owner),
                    IoAction::Drop(owner),
                ]
            );
            assert_eq!(
                *byte_event_log.cancelled.borrow(),
                vec![
                    (
                        ByteEventType::TransportWrite,
                        HttpErrorCode::TRANSPORT_WRITE_ERROR,
                        owner,
                    ),
                    (ByteEventType::KernelWrite, HttpErrorCode::DROPPED, owner),
                ]
            );
        });
    }

    #[derive(Default)]
    struct WakeCount(AtomicUsize);

    impl WakeCount {
        fn count(&self) -> usize {
            self.0.load(Ordering::SeqCst)
        }
    }

    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    type CancelledEvent = (Option<StreamId>, ByteEventType, HttpErrorCode, Vec<u8>);

    #[derive(Default)]
    struct MessageLog {
        cancelled: RefCell<Vec<CancelledEvent>>,
    }

    impl HttpByteEventCallback for MessageLog {
        fn on_byte_event(&self, _event: HttpByteEvent) {}

        fn on_byte_event_cancelled(&self, event: HttpByteEvent, error: HttpError) {
            self.cancelled.borrow_mut().push((
                event.stream_id,
                event.event_type,
                error.code(),
                error.message().to_vec(),
            ));
        }
    }

    fn cancelled(
        stream: u64,
        event_type: ByteEventType,
        code: HttpErrorCode,
        message: &[u8],
    ) -> CancelledEvent {
        (
            Some(StreamId::new(stream)),
            event_type,
            code,
            message.to_vec(),
        )
    }

    fn register(
        transport: &OwnedByteStream,
        context: &WorkerContext,
        stream: u64,
        events: u8,
        log: &Rc<MessageLog>,
    ) {
        transport
            .register_byte_event(
                context,
                HttpByteEventRegistration::new(Some(StreamId::new(stream)), events, log),
            )
            .unwrap();
    }

    fn owned_transport<I: ByteStreamIo>(
        context: &WorkerContext,
        io: I,
        byte_events: ByteEventCapability,
    ) -> OwnedByteStream {
        OwnedByteStream::new(
            io,
            context.handle().clone(),
            context,
            ByteStreamLimits::new(131_072, 262_144, 8).unwrap(),
            SocketAddress::new(b"127.0.0.1", 8080),
            SocketAddress::new(b"127.0.0.2", 443),
            None,
            byte_events,
        )
        .unwrap()
    }

    fn event_transport(context: &WorkerContext) -> (OwnedByteStream, FakeControl) {
        let state = Rc::new(RefCell::new(FakeState::new()));
        let control = FakeControl {
            state: Rc::clone(&state),
        };
        let transport = owned_transport(
            context,
            FakeIo { state },
            ByteEventCapability::TransportEvents,
        );
        (transport, control)
    }

    struct CancelOnPollState {
        owner: ThreadId,
        source: Option<CancellationSource>,
        progress_on_first_poll: usize,
        panic_on_first_poll: bool,
        writes_started: usize,
        write_polls: usize,
        actions: Vec<IoAction>,
    }

    /// A backend whose write requests the write's cancellation inside its
    /// poll, or panics on its first poll, and stays pending.
    struct CancelOnPollIo(Rc<RefCell<CancelOnPollState>>);

    impl CancelOnPollIo {
        fn record(&self, action: IoAction) {
            let mut state = self.0.borrow_mut();
            assert_eq!(std::thread::current().id(), state.owner);
            state.actions.push(action);
        }
    }

    impl ByteStreamIo for CancelOnPollIo {
        fn read(
            &self,
            _maximum: usize,
            _allocation_size: usize,
        ) -> LocalTransportFuture<Result<TransportRead, HttpError>> {
            Box::pin(std::future::pending::<Result<TransportRead, HttpError>>())
        }

        fn write(
            &self,
            _data: BufQueue,
            progress: WriteProgress,
            _flags: ByteEventWriteFlags,
            _byte_events: Option<crate::byte_stream::ByteEventWriteState>,
        ) -> LocalTransportFuture<Result<(), HttpError>> {
            self.0.borrow_mut().writes_started += 1;
            let state = Rc::clone(&self.0);
            Box::pin(std::future::poll_fn(move |_| {
                let source = {
                    let mut state = state.borrow_mut();
                    state.write_polls += 1;
                    let panics = state.write_polls == 1 && state.panic_on_first_poll;
                    if panics {
                        drop(state);
                        panic!("backend write panicked on its first poll");
                    }
                    if state.write_polls == 1
                        && let Err(error) = progress.record(state.progress_on_first_poll)
                    {
                        return Poll::Ready(Err(error));
                    }
                    state.source.take()
                };
                if let Some(source) = source {
                    source.request_cancellation();
                }
                Poll::Pending
            }))
        }

        fn cancel_read(&self, error: HttpErrorCode) {
            self.record(IoAction::CancelRead(error, std::thread::current().id()));
        }

        fn cancel_write(&self, error: HttpErrorCode) {
            self.record(IoAction::CancelWrite(error, std::thread::current().id()));
        }

        fn shutdown_write(&self, _context: &WorkerContext) -> Result<(), HttpError> {
            self.record(IoAction::ShutdownWrite(std::thread::current().id()));
            Ok(())
        }

        fn close(&self, _context: &WorkerContext, _drain_write: bool) -> Result<(), HttpError> {
            self.record(IoAction::Close(std::thread::current().id()));
            Ok(())
        }

        fn drop_transport(&self) {
            self.record(IoAction::Close(std::thread::current().id()));
        }

        fn close_with_reset(&self) {
            self.record(IoAction::Reset(std::thread::current().id()));
        }
    }

    fn cancel_on_poll_transport(
        context: &WorkerContext,
        source: Option<CancellationSource>,
        progress_on_first_poll: usize,
    ) -> (OwnedByteStream, Rc<RefCell<CancelOnPollState>>) {
        let state = Rc::new(RefCell::new(CancelOnPollState {
            owner: std::thread::current().id(),
            source,
            progress_on_first_poll,
            panic_on_first_poll: false,
            writes_started: 0,
            write_polls: 0,
            actions: Vec::new(),
        }));
        let transport = owned_transport(
            context,
            CancelOnPollIo(Rc::clone(&state)),
            ByteEventCapability::Unsupported,
        );
        (transport, state)
    }

    #[test]
    fn pending_write_times_out_exactly_its_timeout_after_the_first_poll() {
        run_worker(|context| async move {
            let owner = std::thread::current().id();
            let (transport, control) = transport(&context, ByteEventCapability::Unsupported, None);
            control.block_writes(3);
            let timeout = Duration::from_millis(40);
            let mut write = transport.write(
                &context,
                queue(&[b"pending"], 7),
                timeout,
                CancellationToken::never(),
            );
            // A deadline anchored when the write began would expire before the
            // poll that is one millisecond short of the first poll's deadline.
            time::advance(Duration::from_millis(25)).await;
            let first_poll = time::Instant::now();
            assert!(poll_once(write.as_mut()).is_pending());

            time::advance(timeout - Duration::from_millis(1)).await;
            let signal = Arc::new(WakeSignal::default());
            let waker = Waker::from(Arc::clone(&signal));
            assert!(poll_once_with_waker(write.as_mut(), &waker).is_pending());
            assert!(
                !signal.take(),
                "the deadline has not committed one millisecond before it"
            );

            let error = write.await.err().unwrap();
            let elapsed = time::Instant::now() - first_poll;
            assert!(
                elapsed >= timeout && elapsed <= timeout + Duration::from_millis(1),
                "the write times out at the timer tick of its first poll plus its timeout, \
                 not {elapsed:?} after it",
            );
            assert_eq!(error.code(), HttpErrorCode::WRITE_TIMEOUT);
            assert_eq!(error.error().message(), b"byte-stream write timed out");
            assert_eq!(error.bytes_written(), 3);
            assert_eq!(
                control.actions(),
                vec![IoAction::CancelWrite(HttpErrorCode::WRITE_TIMEOUT, owner)],
            );
            transport.close(&context).unwrap();
            drop(transport);
        });
    }

    #[test]
    fn write_completed_on_its_first_poll_never_times_out() {
        run_worker(|context| async move {
            let (transport, control) = transport(&context, ByteEventCapability::Unsupported, None);
            let timeout = Duration::from_millis(5);
            let mut write = transport.write(
                &context,
                queue(&[b"ready"], 5),
                timeout,
                CancellationToken::never(),
            );
            time::advance(timeout * 4).await;
            match poll_once(write.as_mut()) {
                Poll::Ready(Ok(info)) => assert_eq!(info.bytes_written, 5),
                other => panic!("a write that completes on its first poll succeeds: {other:?}"),
            }
            drop(write);
            time::advance(timeout * 4).await;

            let signal = Arc::new(WakeSignal::default());
            let waker = Waker::from(Arc::clone(&signal));
            let mut immediate = transport.write(
                &context,
                queue(&[b"again"], 5),
                timeout,
                CancellationToken::never(),
            );
            match poll_once_with_waker(immediate.as_mut(), &waker) {
                Poll::Ready(Ok(info)) => assert_eq!(info.bytes_written, 5),
                other => panic!("a write that completes on its first poll succeeds: {other:?}"),
            }
            drop(immediate);
            assert!(signal.take());
            time::advance(timeout * 4).await;
            tokio::task::yield_now().await;
            assert!(!signal.take(), "a completed write arms no timer");

            assert!(control.actions().is_empty());
            assert_eq!(
                control.writes(),
                vec![vec![b"ready".to_vec()], vec![b"again".to_vec()]],
            );
            transport.close(&context).unwrap();
            drop(transport);
        });
    }

    #[test]
    fn zero_write_timeout_stays_disabled_for_a_pending_write() {
        run_worker(|context| async move {
            let owner = std::thread::current().id();
            let (transport, control) = transport(&context, ByteEventCapability::Unsupported, None);
            control.block_writes(2);
            let mut write = transport.write(
                &context,
                queue(&[b"blocked"], 7),
                Duration::ZERO,
                CancellationToken::never(),
            );
            assert!(poll_once(write.as_mut()).is_pending());
            time::advance(Duration::from_secs(3_600)).await;
            let signal = Arc::new(WakeSignal::default());
            let waker = Waker::from(Arc::clone(&signal));
            assert!(poll_once_with_waker(write.as_mut(), &waker).is_pending());
            assert!(!signal.take(), "a zero timeout never commits a timeout");
            control.unblock_write();
            assert!(signal.take());
            assert_eq!(write.await.unwrap().bytes_written, 7);
            assert!(control.actions().is_empty());

            control.block_writes(1);
            let source = CancellationSource::new();
            let mut cancelled = transport.write(
                &context,
                queue(&[b"cancel"], 6),
                Duration::ZERO,
                source.token(),
            );
            assert!(poll_once(cancelled.as_mut()).is_pending());
            time::advance(Duration::from_secs(3_600)).await;
            assert!(poll_once(cancelled.as_mut()).is_pending());
            source.request_cancellation();
            let error = cancelled.await.err().unwrap();
            assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
            assert_eq!(error.error().message(), b"byte-stream write cancelled");
            assert_eq!(error.bytes_written(), 1);
            assert_eq!(
                control.actions(),
                vec![
                    IoAction::CancelWrite(HttpErrorCode::TRANSPORT_WRITE_ERROR, owner),
                    IoAction::Reset(owner),
                ],
            );
            drop(transport);
        });
    }

    #[test]
    fn cancellation_inside_a_pending_backend_write_resets_on_the_next_poll() {
        run_worker(|context| async move {
            let owner = std::thread::current().id();
            let source = CancellationSource::new();
            let token = source.token();
            let (transport, state) = cancel_on_poll_transport(&context, Some(source), 4);
            let mut write = transport.write(
                &context,
                queue(&[b"cancel"], 6),
                Duration::from_millis(50),
                token,
            );
            let signal = Arc::new(WakeSignal::default());
            let waker = Waker::from(Arc::clone(&signal));
            assert!(poll_once_with_waker(write.as_mut(), &waker).is_pending());
            assert!(
                signal.take(),
                "the poll that observes the cancellation wakes its task"
            );
            assert_eq!(state.borrow().write_polls, 1);
            assert!(state.borrow().actions.is_empty());
            match poll_once_with_waker(write.as_mut(), &waker) {
                Poll::Ready(Err(error)) => {
                    assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                    assert_eq!(error.error().message(), b"byte-stream write cancelled");
                    assert_eq!(error.bytes_written(), 4);
                }
                other => panic!("the next poll resets the transport: {other:?}"),
            }
            assert_eq!(state.borrow().write_polls, 1);
            assert_eq!(
                state.borrow().actions,
                vec![
                    IoAction::CancelWrite(HttpErrorCode::TRANSPORT_WRITE_ERROR, owner),
                    IoAction::Reset(owner),
                ],
            );
            drop(write);
            drop(transport);
            assert_eq!(state.borrow().actions.len(), 2);

            let source = CancellationSource::new();
            let token = source.token();
            let (before_poll, before_poll_state) = cancel_on_poll_transport(&context, None, 4);
            let mut write = before_poll.write(
                &context,
                queue(&[b"cancel"], 6),
                Duration::from_millis(50),
                token,
            );
            source.request_cancellation();
            match poll_once(write.as_mut()) {
                Poll::Ready(Err(error)) => {
                    assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                    assert_eq!(error.error().message(), b"byte-stream write cancelled");
                    assert_eq!(error.bytes_written(), 0);
                }
                other => panic!("cancellation before the first poll resets: {other:?}"),
            }
            assert_eq!(before_poll_state.borrow().writes_started, 1);
            assert_eq!(before_poll_state.borrow().write_polls, 0);
            assert_eq!(
                before_poll_state.borrow().actions,
                vec![
                    IoAction::CancelWrite(HttpErrorCode::TRANSPORT_WRITE_ERROR, owner),
                    IoAction::Reset(owner),
                ],
            );
            drop(write);
            drop(before_poll);

            let source = CancellationSource::new();
            source.request_cancellation();
            let (before_entry, before_entry_state) = cancel_on_poll_transport(&context, None, 4);
            let error = before_entry
                .write(
                    &context,
                    queue(&[b"cancel"], 6),
                    Duration::from_millis(50),
                    source.token(),
                )
                .await
                .err()
                .unwrap();
            assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
            assert_eq!(
                error.error().message(),
                b"byte-stream write cancelled before entry"
            );
            assert_eq!(error.bytes_written(), 0);
            assert_eq!(before_entry_state.borrow().writes_started, 0);
            assert_eq!(
                before_entry_state.borrow().actions,
                vec![IoAction::Reset(owner)],
            );
            drop(before_entry);
        });
    }

    #[test]
    fn write_completed_on_its_first_poll_wakes_its_task_once() {
        run_worker(|context| async move {
            let (transport, _control) = transport(&context, ByteEventCapability::Unsupported, None);
            let wakes = Arc::new(WakeCount::default());
            let waker = Waker::from(Arc::clone(&wakes));
            let mut write = transport.write(
                &context,
                queue(&[b"once"], 4),
                Duration::from_millis(50),
                CancellationToken::never(),
            );
            match poll_once_with_waker(write.as_mut(), &waker) {
                Poll::Ready(Ok(info)) => assert_eq!(info.bytes_written, 4),
                other => panic!("the write completes on its first poll: {other:?}"),
            }
            assert_eq!(wakes.count(), 1, "the completed write wakes its task once");
            drop(write);
            tokio::task::yield_now().await;
            time::advance(Duration::from_millis(100)).await;
            assert_eq!(wakes.count(), 1);

            let source = CancellationSource::new();
            let mut cancellable = transport.write(
                &context,
                queue(&[b"zero"], 4),
                Duration::ZERO,
                source.token(),
            );
            match poll_once_with_waker(cancellable.as_mut(), &waker) {
                Poll::Ready(Ok(info)) => assert_eq!(info.bytes_written, 4),
                other => panic!("the write completes on its first poll: {other:?}"),
            }
            assert_eq!(wakes.count(), 2);
            drop(cancellable);
            source.request_cancellation();
            tokio::task::yield_now().await;
            assert_eq!(
                wakes.count(),
                2,
                "a completed write leaves no cancellation registration"
            );
            transport.close(&context).unwrap();
            drop(transport);
        });
    }

    struct PanickingCancellation;

    impl HttpByteEventCallback for PanickingCancellation {
        fn on_byte_event(&self, _event: HttpByteEvent) {}

        fn on_byte_event_cancelled(&self, _event: HttpByteEvent, _error: HttpError) {
            panic!("byte-event cancellation callback panicked");
        }
    }

    #[test]
    fn first_poll_write_error_wakes_its_task_once_when_a_cancel_callback_panics() {
        let tx_flags = ByteEventWriteFlags {
            timestamp_write: false,
            timestamp_tx: true,
            timestamp_ack: false,
        };
        run_worker(move |context| async move {
            for partial in [false, true] {
                let (transport, control) = event_transport(&context);
                let callback = Rc::new(PanickingCancellation);
                transport
                    .register_byte_event(
                        &context,
                        HttpByteEventRegistration::new(
                            Some(StreamId::new(1)),
                            ByteEventType::NicTx.bit(),
                            &callback,
                        ),
                    )
                    .unwrap();
                if partial {
                    control.complete_write_at(2);
                } else {
                    control.fail_write(HttpErrorCode::TRANSPORT_WRITE_ERROR);
                }
                let mut write = transport.write_with_byte_events(
                    &context,
                    queue(&[b"panic"], 5),
                    tx_flags,
                    Duration::from_millis(50),
                    CancellationToken::never(),
                );
                let wakes = Arc::new(WakeCount::default());
                let waker = Waker::from(Arc::clone(&wakes));
                let panic = catch_unwind(AssertUnwindSafe(|| {
                    poll_once_with_waker(write.as_mut(), &waker)
                }))
                .expect_err("the cancellation callback's panic leaves the write's poll");
                assert_eq!(
                    *panic.downcast::<&'static str>().unwrap(),
                    "byte-event cancellation callback panicked",
                );
                assert_eq!(
                    wakes.count(),
                    1,
                    "partial={partial}: the unwinding write still wakes its task once",
                );
                drop(write);
                tokio::task::yield_now().await;
                time::advance(Duration::from_millis(100)).await;
                assert_eq!(wakes.count(), 1, "partial={partial}");
                drop(transport);
            }
        });
    }

    #[derive(Debug, PartialEq)]
    enum UnwindStep {
        Cancelled(ByteEventType, HttpErrorCode, String),
        Wake,
    }

    #[derive(Default)]
    struct UnwindLog(Mutex<Vec<UnwindStep>>);

    impl UnwindLog {
        fn push(&self, step: UnwindStep) {
            self.0.lock().unwrap().push(step);
        }

        fn take(&self) -> Vec<UnwindStep> {
            std::mem::take(&mut *self.0.lock().unwrap())
        }
    }

    impl Wake for UnwindLog {
        fn wake(self: Arc<Self>) {
            self.push(UnwindStep::Wake);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.push(UnwindStep::Wake);
        }
    }

    struct PanickingNicTxCancellation(Arc<UnwindLog>);

    impl HttpByteEventCallback for PanickingNicTxCancellation {
        fn on_byte_event(&self, _event: HttpByteEvent) {}

        fn on_byte_event_cancelled(&self, event: HttpByteEvent, error: HttpError) {
            self.0.push(UnwindStep::Cancelled(
                event.event_type,
                error.code(),
                String::from_utf8_lossy(error.message()).into_owned(),
            ));
            if event.event_type == ByteEventType::NicTx {
                panic!("byte-event cancellation callback panicked");
            }
        }
    }

    #[test]
    fn first_poll_write_error_cancels_bound_kinds_before_waking_when_a_cancel_callback_panics() {
        let tx_flags = ByteEventWriteFlags {
            timestamp_write: false,
            timestamp_tx: true,
            timestamp_ack: false,
        };
        run_worker(move |context| async move {
            for partial in [false, true] {
                let (transport, control) = event_transport(&context);
                let log = Arc::new(UnwindLog::default());
                let callback = Rc::new(PanickingNicTxCancellation(Arc::clone(&log)));
                transport
                    .register_byte_event(
                        &context,
                        HttpByteEventRegistration::new(
                            Some(StreamId::new(1)),
                            ByteEventType::NicTx.bit() | ByteEventType::TransportWrite.bit(),
                            &callback,
                        ),
                    )
                    .unwrap();
                let write_error = if partial {
                    control.complete_write_at(2);
                    "byte-stream adapter completed a partial write"
                } else {
                    control.fail_write(HttpErrorCode::TRANSPORT_WRITE_ERROR);
                    "scripted write error"
                };
                let mut write = transport.write_with_byte_events(
                    &context,
                    queue(&[b"panic"], 5),
                    tx_flags,
                    Duration::from_millis(50),
                    CancellationToken::never(),
                );
                let waker = Waker::from(Arc::clone(&log));
                let panic = catch_unwind(AssertUnwindSafe(|| {
                    poll_once_with_waker(write.as_mut(), &waker)
                }))
                .expect_err("the NicTx cancellation callback's panic leaves the write's poll");
                assert_eq!(
                    *panic.downcast::<&'static str>().unwrap(),
                    "byte-event cancellation callback panicked",
                );
                assert_eq!(
                    log.take(),
                    vec![
                        UnwindStep::Cancelled(
                            ByteEventType::NicTx,
                            HttpErrorCode::TRANSPORT_WRITE_ERROR,
                            write_error.to_owned(),
                        ),
                        UnwindStep::Cancelled(
                            ByteEventType::TransportWrite,
                            HttpErrorCode::CANCEL,
                            "ByteEvent registration cancelled".to_owned(),
                        ),
                        UnwindStep::Wake,
                    ],
                    "partial={partial}: the unwind cancels the write's other kinds, then wakes",
                );
                drop(write);
                tokio::task::yield_now().await;
                time::advance(Duration::from_millis(100)).await;
                assert_eq!(log.take(), Vec::new(), "partial={partial}");
                drop(transport);
            }
        });
    }

    #[test]
    fn backend_panic_on_the_first_poll_keeps_the_write_deadline_and_cancellation() {
        run_worker(|context| async move {
            let owner = std::thread::current().id();
            let timeout = Duration::from_millis(40);

            let (transport, state) = cancel_on_poll_transport(&context, None, 0);
            state.borrow_mut().panic_on_first_poll = true;
            let mut write = transport.write(
                &context,
                queue(&[b"panic"], 5),
                timeout,
                CancellationToken::never(),
            );
            let wakes = Arc::new(WakeCount::default());
            let waker = Waker::from(Arc::clone(&wakes));
            let panic = catch_unwind(AssertUnwindSafe(|| {
                poll_once_with_waker(write.as_mut(), &waker)
            }))
            .expect_err("the backend's panic leaves the write's poll");
            assert_eq!(
                *panic.downcast::<&'static str>().unwrap(),
                "backend write panicked on its first poll",
            );
            time::advance(timeout - Duration::from_millis(1)).await;
            tokio::task::yield_now().await;
            assert_eq!(wakes.count(), 0, "the deadline has not passed");
            time::advance(Duration::from_millis(2)).await;
            tokio::task::yield_now().await;
            assert_eq!(wakes.count(), 1, "the write deadline wakes the task");
            let error = write.await.err().unwrap();
            assert_eq!(error.code(), HttpErrorCode::WRITE_TIMEOUT);
            assert_eq!(error.error().message(), b"byte-stream write timed out");
            assert_eq!(error.bytes_written(), 0);
            assert_eq!(
                state.borrow().write_polls,
                1,
                "a passed deadline commits without polling the backend again",
            );
            assert_eq!(
                state.borrow().actions,
                vec![IoAction::CancelWrite(HttpErrorCode::WRITE_TIMEOUT, owner)],
            );
            drop(transport);

            let source = CancellationSource::new();
            let (transport, state) = cancel_on_poll_transport(&context, None, 0);
            state.borrow_mut().panic_on_first_poll = true;
            let mut write =
                transport.write(&context, queue(&[b"panic"], 5), timeout, source.token());
            let wakes = Arc::new(WakeCount::default());
            let waker = Waker::from(Arc::clone(&wakes));
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    poll_once_with_waker(write.as_mut(), &waker)
                }))
                .is_err()
            );
            assert_eq!(wakes.count(), 0);
            source.request_cancellation();
            tokio::task::yield_now().await;
            assert_eq!(wakes.count(), 1, "the cancellation wakes the task");
            let error = write.await.err().unwrap();
            assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
            assert_eq!(error.error().message(), b"byte-stream write cancelled");
            assert_eq!(error.bytes_written(), 0);
            assert_eq!(state.borrow().write_polls, 1);
            assert_eq!(
                state.borrow().actions,
                vec![
                    IoAction::CancelWrite(HttpErrorCode::TRANSPORT_WRITE_ERROR, owner),
                    IoAction::Reset(owner),
                ],
            );
            drop(transport);
        });
    }

    #[test]
    fn close_transitions_cancel_byte_events_with_unchanged_codes_and_messages() {
        const RESET: &[u8] = b"byte-stream reset cancelled byte event";
        const CLOSED: &[u8] = b"byte-stream closed before byte event";
        const DROPPED: &[u8] = b"byte-stream transport was dropped";
        let tx_flags = ByteEventWriteFlags {
            timestamp_write: true,
            timestamp_tx: true,
            timestamp_ack: false,
        };
        run_worker(move |context| async move {
            // Reset with an active write: its TX notification, then the
            // registrations bound to the write, in registration order.
            let (reset, reset_control) = event_transport(&context);
            let reset_log = Rc::new(MessageLog::default());
            register(
                &reset,
                &context,
                1,
                ByteEventType::TransportWrite.bit() | ByteEventType::NicTx.bit(),
                &reset_log,
            );
            reset_control.block_writes(0);
            let mut write = reset.write_with_byte_events(
                &context,
                queue(&[b"reset"], 5),
                tx_flags,
                Duration::ZERO,
                CancellationToken::never(),
            );
            assert!(poll_once(write.as_mut()).is_pending());
            register(
                &reset,
                &context,
                3,
                ByteEventType::KernelWrite.bit(),
                &reset_log,
            );
            reset.close_with_reset(&context).unwrap();
            let expected = vec![
                cancelled(
                    1,
                    ByteEventType::NicTx,
                    HttpErrorCode::TRANSPORT_WRITE_ERROR,
                    RESET,
                ),
                cancelled(
                    1,
                    ByteEventType::TransportWrite,
                    HttpErrorCode::TRANSPORT_WRITE_ERROR,
                    RESET,
                ),
                cancelled(
                    3,
                    ByteEventType::KernelWrite,
                    HttpErrorCode::TRANSPORT_WRITE_ERROR,
                    RESET,
                ),
            ];
            assert_eq!(*reset_log.cancelled.borrow(), expected);
            assert_eq!(
                write.await.err().unwrap().code(),
                HttpErrorCode::TRANSPORT_WRITE_ERROR
            );
            drop(reset);
            assert_eq!(*reset_log.cancelled.borrow(), expected);

            // Graceful close without a write: each registration and each kind
            // in order.
            let (graceful, _graceful_control) = event_transport(&context);
            let graceful_log = Rc::new(MessageLog::default());
            register(
                &graceful,
                &context,
                1,
                ByteEventType::TransportWrite.bit(),
                &graceful_log,
            );
            register(
                &graceful,
                &context,
                3,
                ByteEventType::NicTx.bit() | ByteEventType::CumulativeAck.bit(),
                &graceful_log,
            );
            graceful.close(&context).unwrap();
            let expected = vec![
                cancelled(
                    1,
                    ByteEventType::TransportWrite,
                    HttpErrorCode::CORO_CANCELLED,
                    CLOSED,
                ),
                cancelled(
                    3,
                    ByteEventType::NicTx,
                    HttpErrorCode::CORO_CANCELLED,
                    CLOSED,
                ),
                cancelled(
                    3,
                    ByteEventType::CumulativeAck,
                    HttpErrorCode::CORO_CANCELLED,
                    CLOSED,
                ),
            ];
            assert_eq!(*graceful_log.cancelled.borrow(), expected);
            drop(graceful);
            assert_eq!(*graceful_log.cancelled.borrow(), expected);

            // Graceful close keeps the draining write's notification, and the
            // drop that follows cancels it.
            let (draining, draining_control) = event_transport(&context);
            let draining_log = Rc::new(MessageLog::default());
            register(
                &draining,
                &context,
                1,
                ByteEventType::TransportWrite.bit() | ByteEventType::NicTx.bit(),
                &draining_log,
            );
            draining_control.block_writes(0);
            let mut write = draining.write_with_byte_events(
                &context,
                queue(&[b"drain"], 5),
                tx_flags,
                Duration::ZERO,
                CancellationToken::never(),
            );
            assert!(poll_once(write.as_mut()).is_pending());
            draining.close(&context).unwrap();
            assert_eq!(
                *draining_log.cancelled.borrow(),
                vec![cancelled(
                    1,
                    ByteEventType::TransportWrite,
                    HttpErrorCode::CORO_CANCELLED,
                    CLOSED,
                )],
            );
            drop(draining);
            let expected = vec![
                cancelled(
                    1,
                    ByteEventType::TransportWrite,
                    HttpErrorCode::CORO_CANCELLED,
                    CLOSED,
                ),
                cancelled(1, ByteEventType::NicTx, HttpErrorCode::DROPPED, DROPPED),
            ];
            assert_eq!(*draining_log.cancelled.borrow(), expected);
            drop(write);
            assert_eq!(*draining_log.cancelled.borrow(), expected);

            // Drop with an active write: its TX notification, then the
            // registrations bound to the write, in registration order.
            let (dropped, dropped_control) = event_transport(&context);
            let dropped_log = Rc::new(MessageLog::default());
            register(
                &dropped,
                &context,
                1,
                ByteEventType::NicTx.bit() | ByteEventType::KernelWrite.bit(),
                &dropped_log,
            );
            dropped_control.block_writes(0);
            let mut write = dropped.write_with_byte_events(
                &context,
                queue(&[b"drop"], 4),
                tx_flags,
                Duration::ZERO,
                CancellationToken::never(),
            );
            assert!(poll_once(write.as_mut()).is_pending());
            register(
                &dropped,
                &context,
                3,
                ByteEventType::TransportWrite.bit(),
                &dropped_log,
            );
            drop(dropped);
            let expected = vec![
                cancelled(1, ByteEventType::NicTx, HttpErrorCode::DROPPED, DROPPED),
                cancelled(
                    1,
                    ByteEventType::KernelWrite,
                    HttpErrorCode::DROPPED,
                    DROPPED,
                ),
                cancelled(
                    3,
                    ByteEventType::TransportWrite,
                    HttpErrorCode::DROPPED,
                    DROPPED,
                ),
            ];
            assert_eq!(*dropped_log.cancelled.borrow(), expected);
            assert_eq!(write.await.err().unwrap().code(), HttpErrorCode::DROPPED);
            assert_eq!(*dropped_log.cancelled.borrow(), expected);

            let (idle, _idle_control) = event_transport(&context);
            let idle_log = Rc::new(MessageLog::default());
            register(
                &idle,
                &context,
                5,
                ByteEventType::KernelWrite.bit() | ByteEventType::CumulativeAck.bit(),
                &idle_log,
            );
            drop(idle);
            assert_eq!(
                *idle_log.cancelled.borrow(),
                vec![
                    cancelled(
                        5,
                        ByteEventType::KernelWrite,
                        HttpErrorCode::DROPPED,
                        DROPPED
                    ),
                    cancelled(
                        5,
                        ByteEventType::CumulativeAck,
                        HttpErrorCode::DROPPED,
                        DROPPED,
                    ),
                ],
            );
        });
    }

    #[test]
    fn shutdown_write_cancels_byte_events_with_unchanged_code_and_message() {
        const SHUTDOWN: &[u8] = b"byte-stream write side was shut down before byte event";
        run_worker(|context| async move {
            let owner = std::thread::current().id();
            let (transport, control) = event_transport(&context);
            let log = Rc::new(MessageLog::default());
            assert_eq!(
                transport
                    .write(
                        &context,
                        queue(&[b"written"], 7),
                        Duration::ZERO,
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap()
                    .bytes_written,
                7,
            );
            register(
                &transport,
                &context,
                1,
                ByteEventType::TransportWrite.bit() | ByteEventType::KernelWrite.bit(),
                &log,
            );
            register(&transport, &context, 3, ByteEventType::NicTx.bit(), &log);
            transport.shutdown_write(&context).unwrap();
            let expected = vec![
                cancelled(
                    1,
                    ByteEventType::TransportWrite,
                    HttpErrorCode::TRANSPORT_WRITE_ERROR,
                    SHUTDOWN,
                ),
                cancelled(
                    1,
                    ByteEventType::KernelWrite,
                    HttpErrorCode::TRANSPORT_WRITE_ERROR,
                    SHUTDOWN,
                ),
                cancelled(
                    3,
                    ByteEventType::NicTx,
                    HttpErrorCode::TRANSPORT_WRITE_ERROR,
                    SHUTDOWN,
                ),
            ];
            assert_eq!(*log.cancelled.borrow(), expected);
            transport.shutdown_write(&context).unwrap();
            assert_eq!(control.actions(), vec![IoAction::ShutdownWrite(owner)]);
            transport.close(&context).unwrap();
            drop(transport);
            assert_eq!(*log.cancelled.borrow(), expected);
        });
    }

    #[test]
    fn write_deadline_baton_is_built_only_for_a_pending_write() {
        run_worker(|context| async move {
            let (transport, control) = transport(&context, ByteEventCapability::Unsupported, None);
            let before = write_deadline_batons_for_test();
            assert_eq!(
                transport
                    .write(
                        &context,
                        queue(&[b"ready"], 5),
                        Duration::from_millis(50),
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap()
                    .bytes_written,
                5,
            );
            assert_eq!(
                write_deadline_batons_for_test(),
                before,
                "a write that completes on its first poll builds no baton",
            );

            control.block_writes(1);
            let mut pending = transport.write(
                &context,
                queue(&[b"pending"], 7),
                Duration::from_millis(50),
                CancellationToken::never(),
            );
            assert_eq!(write_deadline_batons_for_test(), before);
            assert!(poll_once(pending.as_mut()).is_pending());
            assert_eq!(write_deadline_batons_for_test(), before + 1);
            assert!(poll_once(pending.as_mut()).is_pending());
            control.unblock_write();
            assert_eq!(pending.await.unwrap().bytes_written, 7);
            assert_eq!(
                write_deadline_batons_for_test(),
                before + 1,
                "a pending write builds its baton once",
            );

            control.block_writes(0);
            let mut zero = transport.write(
                &context,
                queue(&[b"zero"], 4),
                Duration::ZERO,
                CancellationToken::never(),
            );
            assert!(poll_once(zero.as_mut()).is_pending());
            assert_eq!(write_deadline_batons_for_test(), before + 2);
            control.unblock_write();
            assert_eq!(zero.await.unwrap().bytes_written, 4);

            let source = CancellationSource::new();
            let mut cancelled = transport.write(
                &context,
                queue(&[b"cancel"], 6),
                Duration::from_millis(50),
                source.token(),
            );
            source.request_cancellation();
            assert!(matches!(
                poll_once(cancelled.as_mut()),
                Poll::Ready(Err(error)) if error.code() == HttpErrorCode::CORO_CANCELLED
            ));
            assert_eq!(write_deadline_batons_for_test(), before + 2);
            drop(cancelled);
            drop(transport);
        });
    }

    #[test]
    fn close_paths_without_byte_events_build_no_error() {
        fn notification(transport: &OwnedByteStream, context: &WorkerContext) -> Rc<MessageLog> {
            let log = Rc::new(MessageLog::default());
            register(transport, context, 1, ByteEventType::NicTx.bit(), &log);
            log
        }

        run_worker(|context| async move {
            let errors = close_path_errors_for_test;

            let (reset, reset_control) =
                transport(&context, ByteEventCapability::TransportEvents, None);
            reset_control.block_writes(0);
            let mut write = reset.write(
                &context,
                queue(&[b"reset"], 5),
                Duration::ZERO,
                CancellationToken::never(),
            );
            assert!(poll_once(write.as_mut()).is_pending());
            let before = errors();
            reset.close_with_reset(&context).unwrap();
            drop(write);
            drop(reset);
            assert_eq!(errors(), before, "reset without byte events");

            let (dropped, dropped_control) =
                transport(&context, ByteEventCapability::TransportEvents, None);
            dropped_control.block_writes(0);
            let mut write = dropped.write(
                &context,
                queue(&[b"drop"], 4),
                Duration::ZERO,
                CancellationToken::never(),
            );
            assert!(poll_once(write.as_mut()).is_pending());
            let before = errors();
            drop(dropped);
            drop(write);
            assert_eq!(errors(), before, "drop without byte events");

            let (graceful, _graceful_control) =
                transport(&context, ByteEventCapability::TransportEvents, None);
            let before = errors();
            graceful.close(&context).unwrap();
            drop(graceful);
            assert_eq!(errors(), before, "graceful close without byte events");

            let (draining, draining_control) =
                transport(&context, ByteEventCapability::TransportEvents, None);
            draining_control.block_writes(0);
            let mut write = draining.write(
                &context,
                queue(&[b"drain"], 5),
                Duration::ZERO,
                CancellationToken::never(),
            );
            assert!(poll_once(write.as_mut()).is_pending());
            let before = errors();
            draining.close(&context).unwrap();
            drop(draining);
            drop(write);
            assert_eq!(
                errors(),
                before,
                "graceful close then drop without byte events"
            );

            let (shutdown, _shutdown_control) =
                transport(&context, ByteEventCapability::TransportEvents, None);
            let before = errors();
            shutdown.shutdown_write(&context).unwrap();
            assert_eq!(errors(), before, "shutdown_write without byte events");
            shutdown.close(&context).unwrap();
            drop(shutdown);

            // With byte events present, each call builds its error once.
            let (reset, _reset_control) =
                transport(&context, ByteEventCapability::TransportEvents, None);
            let _log = notification(&reset, &context);
            let before = errors();
            reset.close_with_reset(&context).unwrap();
            assert_eq!(errors(), before + 1);
            drop(reset);
            assert_eq!(errors(), before + 1);

            let (dropped, _dropped_control) =
                transport(&context, ByteEventCapability::TransportEvents, None);
            let _log = notification(&dropped, &context);
            let before = errors();
            drop(dropped);
            assert_eq!(errors(), before + 1);

            let (graceful, _graceful_control) =
                transport(&context, ByteEventCapability::TransportEvents, None);
            let _log = notification(&graceful, &context);
            let before = errors();
            graceful.close(&context).unwrap();
            assert_eq!(errors(), before + 1);
            drop(graceful);
            assert_eq!(errors(), before + 1);

            let (shutdown, _shutdown_control) =
                transport(&context, ByteEventCapability::TransportEvents, None);
            let _log = notification(&shutdown, &context);
            let before = errors();
            shutdown.shutdown_write(&context).unwrap();
            assert_eq!(errors(), before + 1);
            shutdown.close(&context).unwrap();
            drop(shutdown);
            assert_eq!(errors(), before + 1);

            let (draining, draining_control) =
                transport(&context, ByteEventCapability::TransportEvents, None);
            let _log = notification(&draining, &context);
            draining_control.block_writes(0);
            let mut write = draining.write_with_byte_events(
                &context,
                queue(&[b"drain"], 5),
                ByteEventWriteFlags {
                    timestamp_write: true,
                    timestamp_tx: true,
                    timestamp_ack: false,
                },
                Duration::ZERO,
                CancellationToken::never(),
            );
            assert!(poll_once(write.as_mut()).is_pending());
            let before = errors();
            draining.close(&context).unwrap();
            assert_eq!(
                errors(),
                before,
                "the draining write keeps its notification through the graceful close",
            );
            drop(draining);
            assert_eq!(errors(), before + 1);
            drop(write);
            assert_eq!(errors(), before + 1);
        });
    }

    mod manual_host {
        use std::cell::Cell;
        use std::future::poll_fn;
        use std::rc::Weak;
        use std::time::Instant;

        use roxygen_runtime::host::Host;
        use roxygen_runtime::host::LocalFuture;
        use roxygen_runtime::host::LocalHost;

        use super::*;

        struct ManualTimer {
            deadline: Instant,
            waker: RefCell<Option<Waker>>,
        }

        /// A host on the running `LocalSet` whose clock moves only when a test
        /// advances it, and which counts its clock reads. Its clock runs a day
        /// ahead of Tokio's, which these tests leave unpaused, so a Tokio timer
        /// armed for a test's timeout never fires during it.
        struct ManualHost {
            now: Rc<Cell<Instant>>,
            reads: Cell<usize>,
            timers: RefCell<Vec<Weak<ManualTimer>>>,
        }

        impl ManualHost {
            fn advance(&self, duration: Duration) {
                self.now.set(self.now.get() + duration);
                let now = self.now.get();
                let ready: Vec<Waker> = self
                    .timers
                    .borrow()
                    .iter()
                    .filter_map(Weak::upgrade)
                    .filter(|timer| timer.deadline <= now)
                    .filter_map(|timer| timer.waker.borrow_mut().take())
                    .collect();
                ready.into_iter().for_each(Waker::wake);
            }

            fn reads(&self) -> usize {
                self.reads.get()
            }
        }

        impl LocalHost for ManualHost {
            fn spawn_local(&self, future: LocalFuture) {
                tokio::task::spawn_local(future);
            }

            fn now(&self) -> Instant {
                self.reads.set(self.reads.get() + 1);
                self.now.get()
            }

            fn sleep_until(&self, deadline: Instant) -> LocalFuture {
                let timer = Rc::new(ManualTimer {
                    deadline,
                    waker: RefCell::new(None),
                });
                {
                    let mut timers = self.timers.borrow_mut();
                    timers.retain(|timer| timer.strong_count() != 0);
                    timers.push(Rc::downgrade(&timer));
                }
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
                Box::pin(tokio::task::yield_now())
            }
        }

        std::thread_local! {
            static MANUAL_HOST: RefCell<Option<Rc<ManualHost>>> = const { RefCell::new(None) };
        }

        /// The running test's host. A worker command must be `Send`, so the
        /// `!Send` host reaches the test through its thread.
        fn manual_host() -> Rc<ManualHost> {
            MANUAL_HOST.with_borrow(|host| {
                Rc::clone(host.as_ref().expect("the test runs on its manual host"))
            })
        }

        fn run_manual_worker<F, Fut>(test: F)
        where
            F: FnOnce(WorkerContext) -> Fut + Send + 'static,
            Fut: Future<Output = ()> + 'static,
        {
            Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("current-thread runtime")
                .block_on(LocalSet::new().run_until(async move {
                    let manual = Rc::new(ManualHost {
                        now: Rc::new(Cell::new(Instant::now() + Duration::from_secs(86_400))),
                        reads: Cell::new(0),
                        timers: RefCell::new(Vec::new()),
                    });
                    MANUAL_HOST.set(Some(Rc::clone(&manual)));
                    let (worker, driver) = WorkerDriver::with_host(32, Host::new(manual)).unwrap();
                    let call = worker.call_local(test).unwrap();
                    let driver_task = tokio::task::spawn_local(driver);
                    call.await.unwrap();
                    worker.begin_shutdown();
                    driver_task.await.unwrap();
                    MANUAL_HOST.set(None);
                }));
        }

        #[test]
        fn a_pending_write_times_out_on_its_hosts_manual_clock() {
            run_manual_worker(|context| async move {
                let owner = std::thread::current().id();
                let manual = manual_host();
                let (transport, control) =
                    transport(&context, ByteEventCapability::Unsupported, None);
                control.block_writes(3);
                let timeout = Duration::from_secs(60);
                let mut write = transport.write(
                    &context,
                    queue(&[b"pending"], 7),
                    timeout,
                    CancellationToken::never(),
                );
                let signal = Arc::new(WakeSignal::default());
                let waker = Waker::from(Arc::clone(&signal));
                assert!(poll_once_with_waker(write.as_mut(), &waker).is_pending());

                manual.advance(timeout - Duration::from_millis(1));
                assert!(
                    !signal.take(),
                    "the write is not woken one millisecond short of its deadline",
                );
                manual.advance(Duration::from_millis(1));
                assert!(
                    signal.take(),
                    "the host's clock passing the deadline wakes the write",
                );
                let error = write.await.err().unwrap();
                assert_eq!(error.code(), HttpErrorCode::WRITE_TIMEOUT);
                assert_eq!(error.bytes_written(), 3);
                assert_eq!(
                    control.actions(),
                    vec![IoAction::CancelWrite(HttpErrorCode::WRITE_TIMEOUT, owner)],
                );
                transport.close(&context).unwrap();
            });
        }

        #[test]
        fn a_pending_read_times_out_on_its_hosts_manual_clock() {
            run_manual_worker(|context| async move {
                let owner = std::thread::current().id();
                let manual = manual_host();
                let (transport, control) =
                    transport(&context, ByteEventCapability::Unsupported, None);
                let timeout = Duration::from_secs(60);
                let mut read =
                    transport.read(&context, 1_024, 1_024, timeout, CancellationToken::never());
                let signal = Arc::new(WakeSignal::default());
                let waker = Waker::from(Arc::clone(&signal));
                assert!(poll_once_with_waker(read.as_mut(), &waker).is_pending());

                manual.advance(timeout - Duration::from_millis(1));
                assert!(
                    !signal.take(),
                    "the read is not woken one millisecond short of its deadline",
                );
                manual.advance(Duration::from_millis(1));
                assert!(
                    signal.take(),
                    "the host's clock passing the deadline wakes the read",
                );
                assert_eq!(
                    read.await.err().unwrap().code(),
                    HttpErrorCode::READ_TIMEOUT
                );
                assert_eq!(
                    control.actions(),
                    vec![IoAction::CancelRead(HttpErrorCode::READ_TIMEOUT, owner)],
                );
                transport.close(&context).unwrap();
            });
        }

        #[test]
        fn a_write_reads_its_hosts_clock_only_for_a_nonzero_timeout() {
            run_manual_worker(|context| async move {
                let manual = manual_host();
                let (transport, control) =
                    transport(&context, ByteEventCapability::Unsupported, None);
                let timed = Duration::from_secs(60);

                for (timeout, reads, why) in [
                    (
                        Duration::ZERO,
                        0,
                        "an immediate write reads no clock at a zero timeout",
                    ),
                    (
                        timed,
                        1,
                        "an immediate timed write reads its first poll's time",
                    ),
                ] {
                    let before = manual.reads();
                    let mut write = transport.write(
                        &context,
                        queue(&[b"ready"], 5),
                        timeout,
                        CancellationToken::never(),
                    );
                    assert!(matches!(
                        poll_once(write.as_mut()),
                        Poll::Ready(Ok(info)) if info.bytes_written == 5
                    ));
                    drop(write);
                    assert_eq!(manual.reads() - before, reads, "{why}");
                }

                for (timeout, first_poll_reads, why) in [
                    (
                        Duration::ZERO,
                        0,
                        "a pending write reads no clock at a zero timeout",
                    ),
                    (
                        timed,
                        3,
                        "a pending timed write reads its first poll's time, its remaining time \
                         and its deadline's registration time",
                    ),
                ] {
                    control.block_writes(1);
                    let before = manual.reads();
                    let mut write = transport.write(
                        &context,
                        queue(&[b"pending"], 7),
                        timeout,
                        CancellationToken::never(),
                    );
                    assert!(poll_once(write.as_mut()).is_pending());
                    assert_eq!(manual.reads() - before, first_poll_reads, "{why}");
                    assert!(poll_once(write.as_mut()).is_pending());
                    control.unblock_write();
                    assert!(matches!(
                        poll_once(write.as_mut()),
                        Poll::Ready(Ok(info)) if info.bytes_written == 7
                    ));
                    drop(write);
                    assert_eq!(
                        manual.reads() - before,
                        first_poll_reads,
                        "a re-poll and the completion read no clock",
                    );
                }
                transport.close(&context).unwrap();
            });
        }

        #[test]
        fn a_write_deadline_is_anchored_at_its_first_polls_start() {
            run_manual_worker(|context| async move {
                let owner = std::thread::current().id();
                let manual = manual_host();
                let (transport, control) =
                    transport(&context, ByteEventCapability::Unsupported, None);
                control.block_writes(2);
                let timeout = Duration::from_secs(60);
                let backend_poll = Duration::from_secs(10);
                let polled_host = Rc::clone(&manual);
                control.on_next_write_poll(move || polled_host.advance(backend_poll));
                let mut write = transport.write(
                    &context,
                    queue(&[b"pending"], 7),
                    timeout,
                    CancellationToken::never(),
                );
                let signal = Arc::new(WakeSignal::default());
                let waker = Waker::from(Arc::clone(&signal));
                assert!(poll_once_with_waker(write.as_mut(), &waker).is_pending());

                // The first backend poll took `backend_poll`. A TLS backend
                // arms its ciphertext deadline inside that poll, so a write
                // deadline anchored at the pending result would fire after it.
                manual.advance(timeout - backend_poll - Duration::from_millis(1));
                assert!(
                    !signal.take(),
                    "the write is not woken one millisecond before its first poll's start \
                     plus its timeout",
                );
                manual.advance(Duration::from_millis(1));
                assert!(
                    signal.take(),
                    "the deadline runs from the start of the first poll, not its pending result",
                );
                let error = write.await.err().unwrap();
                assert_eq!(error.code(), HttpErrorCode::WRITE_TIMEOUT);
                assert_eq!(error.bytes_written(), 2);
                assert_eq!(
                    control.actions(),
                    vec![IoAction::CancelWrite(HttpErrorCode::WRITE_TIMEOUT, owner)],
                );
                transport.close(&context).unwrap();
            });
        }
    }

    /// A pending read polled again with the waker it holds keeps that clone: three polls with
    /// one waker clone it once for the transport's read, once for the deadline's wait, and once
    /// per pending poll in the fake backend, which keeps each pending poll's waker. The read's
    /// never-cancellable token clones none.
    #[test]
    fn a_read_polled_again_with_its_waker_clones_it_once() {
        run_worker(|context| async move {
            let (transport, control) = transport(&context, ByteEventCapability::Unsupported, None);
            let counts = Arc::new(WakerCounts::default());
            let waker = counts.waker();
            let mut read = transport.read(
                &context,
                1_024,
                1_024,
                Duration::ZERO,
                CancellationToken::never(),
            );
            for _ in 0..3 {
                assert!(poll_once_with_waker(read.as_mut(), &waker).is_pending());
            }
            assert_eq!(
                counts.clones(),
                1 + 1 + 3,
                "the read and its wait each clone the waker once; the backend clones it per poll",
            );
            control.feed(queue(&[b"ok"], 2), false);
            assert_eq!(counts.wakes(), 1, "the bytes wake the read");
            let Poll::Ready(Ok(result)) = poll_once_with_waker(read.as_mut(), &waker) else {
                panic!("the fed read completes");
            };
            assert_eq!(flatten(result.data()), b"ok");
            assert_eq!(
                (counts.clones(), counts.wakes()),
                (6, 2),
                "the completing poll clones the waker only for the deadline, which wakes its \
                 wait's waker as it drops",
            );
            drop(read);
            assert_eq!(counts.live(), 1, "the completed read keeps no waker");
            transport.close(&context).unwrap();
        });
    }

    mod owned_reader {
        use std::cell::Cell;
        use std::future::poll_fn;

        use super::*;
        use crate::byte_stream::read_deadline_batons_for_test;
        use crate::byte_stream::reader_wake_registrations_for_test;

        struct StartupFutureDrop {
            trace: Rc<RefCell<Vec<&'static str>>>,
            panic_on_drop: bool,
        }

        impl Drop for StartupFutureDrop {
            fn drop(&mut self) {
                self.trace.borrow_mut().push("read-future-drop");
                assert!(!self.panic_on_drop, "read startup future drop");
            }
        }

        /// A backend whose `read` drops the transport, which leaves the read
        /// start's upgraded `io` as the backend's last owner, and whose
        /// destruction then panics.
        struct StartupDropIo {
            inner: FakeIo,
            transport: Rc<RefCell<Option<OwnedByteStream>>>,
            trace: Rc<RefCell<Vec<&'static str>>>,
            armed: Cell<bool>,
            panic_on_read_drop: bool,
        }

        impl ByteStreamIo for StartupDropIo {
            fn read(
                &self,
                maximum: usize,
                allocation_size: usize,
            ) -> LocalTransportFuture<Result<TransportRead, HttpError>> {
                let future = self.inner.read(maximum, allocation_size);
                let guard = StartupFutureDrop {
                    trace: Rc::clone(&self.trace),
                    panic_on_drop: self.panic_on_read_drop,
                };
                let transport = self.transport.borrow_mut().take().unwrap();
                self.trace.borrow_mut().push("transport-drop");
                drop(transport);
                self.armed.set(true);
                Box::pin(async move {
                    let _guard = guard;
                    future.await
                })
            }

            fn write(
                &self,
                data: BufQueue,
                progress: WriteProgress,
                flags: ByteEventWriteFlags,
                byte_events: Option<crate::byte_stream::ByteEventWriteState>,
            ) -> LocalTransportFuture<Result<(), HttpError>> {
                self.inner.write(data, progress, flags, byte_events)
            }

            fn cancel_read(&self, error: HttpErrorCode) {
                self.inner.cancel_read(error);
            }

            fn cancel_write(&self, error: HttpErrorCode) {
                self.inner.cancel_write(error);
            }

            fn shutdown_write(&self, context: &WorkerContext) -> Result<(), HttpError> {
                self.inner.shutdown_write(context)
            }

            fn close(&self, context: &WorkerContext, drain_write: bool) -> Result<(), HttpError> {
                self.inner.close(context, drain_write)
            }

            fn drop_transport(&self) {
                self.inner.drop_transport();
            }

            fn close_with_reset(&self) {
                self.inner.close_with_reset();
            }
        }

        impl Drop for StartupDropIo {
            fn drop(&mut self) {
                if self.armed.get() {
                    self.trace.borrow_mut().push("backend-drop");
                    panic!("read startup backend drop");
                }
            }
        }

        /// The backend's destruction panics after the read start admitted its
        /// read: the read is retired during that unwind, before the panic
        /// leaves `start_read`, and a panic in the read's own cleanup does not
        /// replace the backend's.
        fn startup_unwind_cleans_read(panic_on_read_drop: bool) {
            run_worker(move |context| async move {
                let slot = Rc::new(RefCell::new(None));
                let trace = Rc::new(RefCell::new(Vec::new()));
                let transport = OwnedByteStream::new(
                    StartupDropIo {
                        inner: FakeIo {
                            state: Rc::new(RefCell::new(FakeState::new())),
                        },
                        transport: Rc::clone(&slot),
                        trace: Rc::clone(&trace),
                        armed: Cell::new(false),
                        panic_on_read_drop,
                    },
                    context.handle().clone(),
                    &context,
                    ByteStreamLimits::new(64, 64, 1).unwrap(),
                    SocketAddress::new(b"127.0.0.1", 1),
                    SocketAddress::new(b"127.0.0.1", 2),
                    None,
                    ByteEventCapability::Unsupported,
                )
                .unwrap();
                let mut reader = transport
                    .reader(&context, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .unwrap();
                *slot.borrow_mut() = Some(transport);
                let panic = catch_unwind(AssertUnwindSafe(|| reader.start_read(64, 64)))
                    .expect_err("the read start's last backend owner panics at release");
                assert_eq!(
                    panic.downcast_ref::<&'static str>(),
                    Some(&"read startup backend drop")
                );
                let after_start = trace.borrow().clone();
                drop(reader);
                let after_drop = trace.borrow().clone();
                assert_eq!(
                    after_start,
                    ["transport-drop", "backend-drop", "read-future-drop"],
                    "the admitted read is retired before the panic leaves start_read"
                );
                assert_eq!(after_drop, after_start);
            });
        }

        #[test]
        fn owned_reader_startup_unwind_preserves_read_cleanup() {
            startup_unwind_cleans_read(false);
        }

        #[test]
        fn owned_reader_startup_unwind_preserves_backend_panic_when_read_cleanup_panics() {
            startup_unwind_cleans_read(true);
        }

        /// The queued writer of `StartupDropWriteIo`: its `start_write` drops the transport,
        /// which leaves the write start's upgraded `io` as the backend's last owner, and its
        /// write's cleanup (`retire_write`) is traced and optionally panics.
        struct StartupWriter {
            transport: Rc<RefCell<Option<OwnedByteStream>>>,
            trace: Rc<RefCell<Vec<&'static str>>>,
            armed: Rc<Cell<bool>>,
            panic_on_retire: bool,
        }

        impl crate::byte_stream::QueuedWriter for StartupWriter {
            fn start_write(
                &self,
                _data: BufQueue,
                _progress: WriteProgress,
                _flags: ByteEventWriteFlags,
                _byte_events: Option<crate::byte_stream::ByteEventWriteState>,
            ) {
                let transport = self.transport.borrow_mut().take().unwrap();
                self.trace.borrow_mut().push("transport-drop");
                drop(transport);
                self.armed.set(true);
            }

            fn write_through(
                &self,
                data: BufQueue,
                progress: &WriteProgress,
            ) -> crate::byte_stream::WriteThrough {
                self.start_write(data, progress.clone(), ByteEventWriteFlags::default(), None);
                crate::byte_stream::WriteThrough::Held
            }

            fn poll_write(&self, _context: &mut Context<'_>) -> Poll<Result<(), HttpError>> {
                Poll::Pending
            }

            fn retire_write(&self) {
                self.trace.borrow_mut().push("write-future-drop");
                assert!(!self.panic_on_retire, "write startup cleanup");
            }
        }

        /// A backend whose queued writer drops the transport at a write's start, and whose
        /// destruction then panics.
        struct StartupDropWriteIo {
            inner: FakeIo,
            writer: Rc<StartupWriter>,
        }

        impl ByteStreamIo for StartupDropWriteIo {
            fn read(
                &self,
                maximum: usize,
                allocation_size: usize,
            ) -> LocalTransportFuture<Result<TransportRead, HttpError>> {
                self.inner.read(maximum, allocation_size)
            }

            fn write(
                &self,
                data: BufQueue,
                progress: WriteProgress,
                flags: ByteEventWriteFlags,
                byte_events: Option<crate::byte_stream::ByteEventWriteState>,
            ) -> LocalTransportFuture<Result<(), HttpError>> {
                self.inner.write(data, progress, flags, byte_events)
            }

            fn cancel_read(&self, error: HttpErrorCode) {
                self.inner.cancel_read(error);
            }

            fn cancel_write(&self, error: HttpErrorCode) {
                self.inner.cancel_write(error);
            }

            fn shutdown_write(&self, context: &WorkerContext) -> Result<(), HttpError> {
                self.inner.shutdown_write(context)
            }

            fn close(&self, context: &WorkerContext, drain_write: bool) -> Result<(), HttpError> {
                self.inner.close(context, drain_write)
            }

            fn drop_transport(&self) {
                self.inner.drop_transport();
            }

            fn close_with_reset(&self) {
                self.inner.close_with_reset();
            }

            fn queued_writer(&self) -> Option<Rc<dyn crate::byte_stream::QueuedWriter>> {
                Some(Rc::clone(&self.writer) as Rc<dyn crate::byte_stream::QueuedWriter>)
            }
        }

        impl Drop for StartupDropWriteIo {
            fn drop(&mut self) {
                if self.writer.armed.get() {
                    self.writer.trace.borrow_mut().push("backend-drop");
                    panic!("write startup backend drop");
                }
            }
        }

        /// The backend's destruction panics after the write start registered its write: the
        /// write is retired during that unwind, before the panic leaves
        /// `OwnedWriteQueue::start_write`, and a panic in the write's own cleanup does not
        /// replace the backend's. With byte events the queue's write starts through
        /// `start_write_operation`; without them it is written through at entry and, left
        /// pending, registered there.
        fn startup_unwind_cleans_write(byte_events: ByteEventCapability, panic_on_retire: bool) {
            run_worker(move |context| async move {
                let slot = Rc::new(RefCell::new(None));
                let trace = Rc::new(RefCell::new(Vec::new()));
                let transport = OwnedByteStream::new(
                    StartupDropWriteIo {
                        inner: FakeIo {
                            state: Rc::new(RefCell::new(FakeState::new())),
                        },
                        writer: Rc::new(StartupWriter {
                            transport: Rc::clone(&slot),
                            trace: Rc::clone(&trace),
                            armed: Rc::new(Cell::new(false)),
                            panic_on_retire,
                        }),
                    },
                    context.handle().clone(),
                    &context,
                    ByteStreamLimits::new(64, 64, 1).unwrap(),
                    SocketAddress::new(b"127.0.0.1", 1),
                    SocketAddress::new(b"127.0.0.1", 2),
                    None,
                    byte_events,
                )
                .unwrap();
                let mut writer = transport
                    .write_queue(&context, 64, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .expect("a backend with a queued writer offers the queue");
                *slot.borrow_mut() = Some(transport);
                let panic = catch_unwind(AssertUnwindSafe(|| {
                    writer.start_write(queue(&[b"data"], 4))
                }))
                .expect_err("the write start's last backend owner panics at release");
                assert_eq!(
                    panic.downcast_ref::<&'static str>(),
                    Some(&"write startup backend drop")
                );
                let after_start = trace.borrow().clone();
                drop(writer);
                let after_drop = trace.borrow().clone();
                assert_eq!(
                    after_start,
                    ["transport-drop", "backend-drop", "write-future-drop"],
                    "the registered write is retired before the panic leaves start_write"
                );
                assert_eq!(after_drop, after_start);
            });
        }

        #[test]
        fn owned_write_startup_unwind_preserves_write_cleanup() {
            startup_unwind_cleans_write(ByteEventCapability::TransportEvents, false);
        }

        #[test]
        fn owned_write_startup_unwind_preserves_write_cleanup_queued() {
            startup_unwind_cleans_write(ByteEventCapability::Unsupported, false);
        }

        #[test]
        fn owned_write_startup_unwind_preserves_backend_panic_when_write_cleanup_panics() {
            startup_unwind_cleans_write(ByteEventCapability::TransportEvents, true);
            startup_unwind_cleans_write(ByteEventCapability::Unsupported, true);
        }

        #[test]
        fn owned_reader_is_offered_only_untimed() {
            run_worker(|context| async move {
                let (transport, _control) =
                    transport(&context, ByteEventCapability::Unsupported, None);
                assert!(
                    transport
                        .reader(&context, Duration::ZERO, CancellationToken::never())
                        .unwrap()
                        .is_some(),
                    "an untimed owned byte stream offers its poll-based reader",
                );
                assert!(
                    transport
                        .reader(&context, Duration::from_secs(5), CancellationToken::never())
                        .unwrap()
                        .is_none(),
                    "a read timeout keeps the read API, which times each read",
                );
                transport.close(&context).unwrap();
            });
        }

        #[test]
        fn owned_reader_reads_bytes_then_a_deferred_eof() {
            run_worker(|context| async move {
                let (transport, control) =
                    transport(&context, ByteEventCapability::Unsupported, None);
                let mut reader = transport
                    .reader(&context, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .unwrap();
                reader.start_read(1_024, 1_024).unwrap();
                assert!(
                    transport.has_pending_read(&context).unwrap(),
                    "a started read is registered with the transport",
                );
                let signal = Arc::new(WakeSignal::default());
                let waker = Waker::from(Arc::clone(&signal));
                for _ in 0..2 {
                    assert!(
                        reader
                            .poll_read(&mut Context::from_waker(&waker))
                            .is_pending()
                    );
                }
                control.feed(queue(&[b"ab"], 2), true);
                assert!(signal.take(), "the pending read is woken by its bytes");
                let Poll::Ready(Ok(read)) = reader.poll_read(&mut Context::from_waker(&waker))
                else {
                    panic!("the buffered bytes complete the read");
                };
                drop(waker);
                assert_eq!(
                    Arc::strong_count(&signal),
                    1,
                    "completion releases the read's captured wakers"
                );
                assert_eq!(flatten(read.data()), b"ab");
                assert!(
                    !read.is_eof(),
                    "an EOF that arrives with bytes is deferred to the next read",
                );
                assert!(!transport.has_pending_read(&context).unwrap());
                reader.start_read(1_024, 1_024).unwrap();
                let eof = poll_fn(|context| reader.poll_read(context)).await.unwrap();
                assert!(eof.is_eof());
                assert_eq!(eof.bytes_read(), 0);
                assert_eq!(
                    control.reads_started(),
                    1,
                    "the deferred EOF needs no backend read"
                );
                transport.close(&context).unwrap();
            });
        }

        /// `start_read` has `read`'s entry results and clamps: the read offered to the backend,
        /// an empty read for a zero maximum, a zero allocation size refused, and a read refused
        /// while another is pending.
        #[test]
        fn owned_reader_reads_keep_reads_entry_results_and_clamps() {
            run_worker(|context| async move {
                // `transport` caps reads at a 131_072-byte maximum_read_size.
                let (transport, control) =
                    transport(&context, ByteEventCapability::Unsupported, None);
                let mut reader = transport
                    .reader(&context, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .unwrap();
                for (maximum, allocation_size) in [(8_192, 1_024), (100, 4_000), (200_000, 4_000)] {
                    control.feed(queue(&[b"x"], 1), false);
                    reader.start_read(maximum, allocation_size).unwrap();
                    let read = poll_fn(|context| reader.poll_read(context)).await.unwrap();
                    assert_eq!(flatten(read.data()), b"x");
                }
                assert_eq!(
                    control.read_sizes(),
                    vec![(8_192, 1_024), (100, 100), (131_072, 4_000)],
                    "the backend receives the maximum clamped to the limit and the allocation \
                     size clamped to that maximum",
                );

                reader.start_read(0, 0).unwrap();
                let empty = poll_fn(|context| reader.poll_read(context)).await.unwrap();
                assert_eq!(empty.bytes_read(), 0);
                assert!(!empty.is_eof());
                assert_eq!(control.reads_started(), 3, "a zero maximum reads nothing");

                reader.start_read(1_024, 0).unwrap();
                assert_eq!(
                    poll_fn(|context| reader.poll_read(context))
                        .await
                        .unwrap_err()
                        .code(),
                    HttpErrorCode::INVALID_STATE_TRANSITION,
                );

                let mut pending = transport.read(
                    &context,
                    1_024,
                    1_024,
                    Duration::ZERO,
                    CancellationToken::never(),
                );
                assert!(poll_once(pending.as_mut()).is_pending());
                reader.start_read(1_024, 1_024).unwrap();
                assert_eq!(
                    poll_fn(|context| reader.poll_read(context))
                        .await
                        .unwrap_err()
                        .code(),
                    HttpErrorCode::INVALID_STATE_TRANSITION,
                    "a read is refused while another is pending",
                );
                assert_eq!(control.reads_started(), 4);
                drop(pending);
                transport.close(&context).unwrap();
            });
        }

        /// Starting a read while one is started, or polling without one, is refused and changes
        /// nothing.
        #[test]
        fn owned_reader_refuses_misuse() {
            run_worker(|context| async move {
                let (transport, control) =
                    transport(&context, ByteEventCapability::Unsupported, None);
                let mut reader = transport
                    .reader(&context, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .unwrap();
                let Poll::Ready(Err(error)) =
                    reader.poll_read(&mut Context::from_waker(Waker::noop()))
                else {
                    panic!("polling a reader with no started read is refused");
                };
                assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);

                reader.start_read(1_024, 1_024).unwrap();
                assert_eq!(
                    reader.start_read(1_024, 1_024).unwrap_err().code(),
                    HttpErrorCode::INVALID_STATE_TRANSITION,
                    "a second read cannot start before the first is returned",
                );
                control.feed(queue(&[b"ok"], 2), false);
                let read = poll_fn(|context| reader.poll_read(context)).await.unwrap();
                assert_eq!(flatten(read.data()), b"ok");
                assert_eq!(
                    control.reads_started(),
                    1,
                    "the refused start began no read"
                );

                reader.start_read(0, 0).unwrap();
                assert_eq!(
                    reader.start_read(1_024, 1_024).unwrap_err().code(),
                    HttpErrorCode::INVALID_STATE_TRANSITION,
                    "a read that finished at entry is started until it is returned",
                );
                let empty = poll_fn(|context| reader.poll_read(context)).await.unwrap();
                assert_eq!(empty.bytes_read(), 0);
                assert_eq!(control.reads_started(), 1);
                transport.close(&context).unwrap();
            });
        }

        #[test]
        fn a_pending_owned_reader_read_arms_no_deadline_baton() {
            run_worker(|context| async move {
                let (transport, control) =
                    transport(&context, ByteEventCapability::Unsupported, None);
                let mut reader = transport
                    .reader(&context, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .unwrap();
                let batons = read_deadline_batons_for_test();
                reader.start_read(1_024, 1_024).unwrap();
                assert!(
                    reader
                        .poll_read(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                control.feed(queue(&[b"ok"], 2), false);
                poll_fn(|context| reader.poll_read(context)).await.unwrap();
                assert_eq!(
                    read_deadline_batons_for_test(),
                    batons,
                    "an untimed reader read builds no per-read deadline",
                );

                // Control: the read API builds one deadline per read.
                control.feed(queue(&[b"ok"], 2), false);
                transport
                    .read(
                        &context,
                        1_024,
                        1_024,
                        Duration::ZERO,
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap();
                assert_eq!(read_deadline_batons_for_test(), batons + 1);
                transport.close(&context).unwrap();
            });
        }

        /// Cancelling a pending read wakes its task, as the read API's wait does, and the read
        /// fails with `CORO_CANCELLED`; bytes that arrived first still win; a cancelled token
        /// refuses a read at entry.
        #[test]
        fn owned_reader_cancellation_wakes_and_fails_the_pending_read() {
            run_worker(|context| async move {
                let owner = std::thread::current().id();
                let (transport, control) =
                    transport(&context, ByteEventCapability::Unsupported, None);
                let cancel = CancellationSource::new();
                let mut reader = transport
                    .reader(&context, Duration::ZERO, cancel.token())
                    .unwrap()
                    .unwrap();
                reader.start_read(1_024, 1_024).unwrap();
                let signal = Arc::new(WakeSignal::default());
                let waker = Waker::from(Arc::clone(&signal));
                assert!(
                    reader
                        .poll_read(&mut Context::from_waker(&waker))
                        .is_pending()
                );
                cancel.request_cancellation();
                assert!(signal.take(), "cancelling a pending read wakes its task");
                let Poll::Ready(Err(error)) = reader.poll_read(&mut Context::from_waker(&waker))
                else {
                    panic!("the woken read observes its cancellation");
                };
                assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                assert!(!transport.has_pending_read(&context).unwrap());
                assert_eq!(
                    control.actions(),
                    vec![IoAction::CancelRead(HttpErrorCode::CORO_CANCELLED, owner)],
                );
                reader.start_read(1_024, 1_024).unwrap();
                assert_eq!(
                    poll_fn(|context| reader.poll_read(context))
                        .await
                        .unwrap_err()
                        .code(),
                    HttpErrorCode::CORO_CANCELLED,
                    "a cancelled reader refuses a read at entry",
                );
                assert_eq!(control.reads_started(), 1);

                let partial = CancellationSource::new();
                let mut reader = transport
                    .reader(&context, Duration::ZERO, partial.token())
                    .unwrap()
                    .unwrap();
                reader.start_read(2, 2).unwrap();
                let signal = Arc::new(WakeSignal::default());
                let waker = Waker::from(Arc::clone(&signal));
                for _ in 0..2 {
                    assert!(
                        reader
                            .poll_read(&mut Context::from_waker(&waker))
                            .is_pending()
                    );
                }
                control.feed(queue(&[b"ok"], 2), false);
                partial.request_cancellation();
                let Poll::Ready(Ok(read)) = reader.poll_read(&mut Context::from_waker(&waker))
                else {
                    panic!("ready bytes win over the later cancellation");
                };
                drop(reader);
                drop(waker);
                assert_eq!(
                    Arc::strong_count(&signal),
                    1,
                    "completion releases the read's waker, and dropping the reader its \
                     cancellation registration's",
                );
                assert_eq!(flatten(read.data()), b"ok", "bytes that arrived first win");
                assert_eq!(
                    control.actions().len(),
                    1,
                    "a read that returns bytes cancels nothing"
                );
                transport.close(&context).unwrap();
            });
        }

        /// A reader registers its cancellation wake once for all its reads: three reads that
        /// each pend before their bytes arrive build one registration, where a registration per
        /// read would build three.
        #[test]
        fn owned_reader_registers_its_cancellation_once() {
            run_worker(|context| async move {
                let (transport, control) =
                    transport(&context, ByteEventCapability::Unsupported, None);
                let cancel = CancellationSource::new();
                let registrations = reader_wake_registrations_for_test();
                let mut reader = transport
                    .reader(&context, Duration::ZERO, cancel.token())
                    .unwrap()
                    .unwrap();
                for chunk in [&b"one"[..], b"two", b"six"] {
                    reader.start_read(1_024, 1_024).unwrap();
                    assert!(
                        reader
                            .poll_read(&mut Context::from_waker(Waker::noop()))
                            .is_pending()
                    );
                    control.feed(queue(&[chunk], chunk.len()), false);
                    let read = poll_fn(|context| reader.poll_read(context)).await.unwrap();
                    assert_eq!(flatten(read.data()), chunk);
                }
                assert_eq!(
                    reader_wake_registrations_for_test(),
                    registrations + 1,
                    "the reader's reads share one cancellation wake registration",
                );
                transport.close(&context).unwrap();
            });
        }

        /// A later read's cancellation wakes the waker that read registered, not the waker of
        /// an earlier read, and fails that read.
        #[test]
        fn a_later_reader_read_cancellation_wakes_that_reads_waker() {
            run_worker(|context| async move {
                let (transport, control) =
                    transport(&context, ByteEventCapability::Unsupported, None);
                let cancel = CancellationSource::new();
                let mut reader = transport
                    .reader(&context, Duration::ZERO, cancel.token())
                    .unwrap()
                    .unwrap();
                let earlier = Arc::new(WakeSignal::default());
                let earlier_waker = Waker::from(Arc::clone(&earlier));
                reader.start_read(1_024, 1_024).unwrap();
                assert!(
                    reader
                        .poll_read(&mut Context::from_waker(&earlier_waker))
                        .is_pending()
                );
                control.feed(queue(&[b"ok"], 2), false);
                let Poll::Ready(Ok(read)) =
                    reader.poll_read(&mut Context::from_waker(&earlier_waker))
                else {
                    panic!("the fed read completes");
                };
                assert_eq!(flatten(read.data()), b"ok");
                earlier.take();

                let later = Arc::new(WakeSignal::default());
                let later_waker = Waker::from(Arc::clone(&later));
                reader.start_read(1_024, 1_024).unwrap();
                assert!(
                    reader
                        .poll_read(&mut Context::from_waker(&later_waker))
                        .is_pending()
                );
                cancel.request_cancellation();
                assert!(later.take(), "cancelling wakes the pending read's waker");
                assert!(!earlier.take(), "the earlier read's waker is not woken");
                let Poll::Ready(Err(error)) =
                    reader.poll_read(&mut Context::from_waker(&later_waker))
                else {
                    panic!("the woken read observes its cancellation");
                };
                assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                transport.close(&context).unwrap();
            });
        }

        /// `DIV-018`: because the registration outlives each read, a cancellation requested
        /// while no read is started wakes the waker that the last pending read registered.
        #[test]
        fn a_cancellation_between_reader_reads_wakes_the_last_read_waker() {
            run_worker(|context| async move {
                let (transport, control) =
                    transport(&context, ByteEventCapability::Unsupported, None);
                let cancel = CancellationSource::new();
                let mut reader = transport
                    .reader(&context, Duration::ZERO, cancel.token())
                    .unwrap()
                    .unwrap();
                let signal = Arc::new(WakeSignal::default());
                let waker = Waker::from(Arc::clone(&signal));
                reader.start_read(1_024, 1_024).unwrap();
                assert!(
                    reader
                        .poll_read(&mut Context::from_waker(&waker))
                        .is_pending()
                );
                control.feed(queue(&[b"ok"], 2), false);
                let Poll::Ready(Ok(read)) = reader.poll_read(&mut Context::from_waker(&waker))
                else {
                    panic!("the fed read completes");
                };
                assert_eq!(flatten(read.data()), b"ok");
                signal.take();
                assert!(!transport.has_pending_read(&context).unwrap());

                cancel.request_cancellation();
                assert!(
                    signal.take(),
                    "the reader's registration wakes its last waker with no read started",
                );
                reader.start_read(1_024, 1_024).unwrap();
                assert_eq!(
                    poll_fn(|context| reader.poll_read(context))
                        .await
                        .unwrap_err()
                        .code(),
                    HttpErrorCode::CORO_CANCELLED,
                    "the next read is refused at entry",
                );
                transport.close(&context).unwrap();
            });
        }

        /// A reset or a graceful close ends a pending reader read, wakes it, and cancels the
        /// backend read, as for the read API.
        #[test]
        fn owned_reader_close_and_reset_end_the_pending_read() {
            run_worker(|context| async move {
                let owner = std::thread::current().id();
                for graceful in [false, true] {
                    let (transport, control) =
                        transport(&context, ByteEventCapability::Unsupported, None);
                    let mut reader = transport
                        .reader(&context, Duration::ZERO, CancellationToken::never())
                        .unwrap()
                        .unwrap();
                    reader.start_read(1_024, 1_024).unwrap();
                    let first = Arc::new(WakeSignal::default());
                    let first_waker = Waker::from(Arc::clone(&first));
                    let signal = Arc::new(WakeSignal::default());
                    let waker = Waker::from(Arc::clone(&signal));
                    for current in [&first_waker, &first_waker, &waker] {
                        assert!(
                            reader
                                .poll_read(&mut Context::from_waker(current))
                                .is_pending()
                        );
                    }
                    drop(first_waker);
                    assert_eq!(
                        Arc::strong_count(&first),
                        1,
                        "a changed poll target releases the previous waker"
                    );
                    if graceful {
                        transport.close(&context).unwrap();
                    } else {
                        transport.close_with_reset(&context).unwrap();
                    }
                    assert!(!first.take(), "closing does not wake the previous target");
                    assert!(
                        signal.take(),
                        "closing the transport wakes the current read target"
                    );
                    let Poll::Ready(result) = reader.poll_read(&mut Context::from_waker(&waker))
                    else {
                        panic!("the woken read observes the close");
                    };
                    if graceful {
                        assert!(
                            result.unwrap().is_eof(),
                            "a graceful close ends the read at EOF"
                        );
                        assert_eq!(
                            control.actions(),
                            vec![
                                IoAction::CancelRead(HttpErrorCode::TRANSPORT_EOF, owner),
                                IoAction::Close(owner),
                            ],
                        );
                    } else {
                        assert_eq!(
                            result.unwrap_err().code(),
                            HttpErrorCode::TRANSPORT_READ_ERROR
                        );
                        assert_eq!(
                            control.actions(),
                            vec![
                                IoAction::CancelRead(HttpErrorCode::TRANSPORT_READ_ERROR, owner),
                                IoAction::Reset(owner),
                            ],
                        );
                    }
                    assert!(!transport.has_pending_read(&context).unwrap());
                    drop(waker);
                    assert_eq!(
                        Arc::strong_count(&signal),
                        1,
                        "close releases the read's captured wakers"
                    );
                }
            });
        }

        /// Dropping the transport ends a pending reader read with `DROPPED`, wakes it, and
        /// cancels the backend read, as for the read API.
        #[test]
        fn dropping_the_transport_ends_a_pending_reader_read() {
            run_worker(|context| async move {
                let owner = std::thread::current().id();
                let (transport, control) =
                    transport(&context, ByteEventCapability::Unsupported, None);
                let mut reader = transport
                    .reader(&context, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .unwrap();
                reader.start_read(1_024, 1_024).unwrap();
                let signal = Arc::new(WakeSignal::default());
                let waker = Waker::from(Arc::clone(&signal));
                assert!(
                    reader
                        .poll_read(&mut Context::from_waker(&waker))
                        .is_pending()
                );
                drop(transport);
                assert!(
                    signal.take(),
                    "dropping the transport wakes the pending read"
                );
                let Poll::Ready(Err(error)) = reader.poll_read(&mut Context::from_waker(&waker))
                else {
                    panic!("the woken read observes the dropped transport");
                };
                assert_eq!(error.code(), HttpErrorCode::DROPPED);
                assert_eq!(
                    control.actions(),
                    vec![
                        IoAction::CancelRead(HttpErrorCode::DROPPED, owner),
                        IoAction::Close(owner),
                        IoAction::Drop(owner),
                    ],
                );
                drop(waker);
                assert_eq!(
                    Arc::strong_count(&signal),
                    1,
                    "the dropped transport releases the read's captured wakers"
                );
            });
        }

        /// Dropping a reader cancels its pending read, as dropping a read future does, so a
        /// later read can start.
        #[test]
        fn dropping_an_owned_reader_cancels_its_pending_read() {
            run_worker(|context| async move {
                let owner = std::thread::current().id();
                let (transport, control) =
                    transport(&context, ByteEventCapability::Unsupported, None);
                let mut reader = transport
                    .reader(&context, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .unwrap();
                reader.start_read(1_024, 1_024).unwrap();
                let signal = Arc::new(WakeSignal::default());
                let waker = Waker::from(Arc::clone(&signal));
                for _ in 0..2 {
                    assert!(
                        reader
                            .poll_read(&mut Context::from_waker(&waker))
                            .is_pending()
                    );
                }
                drop(reader);
                drop(waker);
                assert_eq!(
                    Arc::strong_count(&signal),
                    1,
                    "dropping the reader releases its captured wakers"
                );
                assert!(
                    !transport.has_pending_read(&context).unwrap(),
                    "the dropped reader's read is unregistered",
                );
                assert_eq!(
                    control.actions(),
                    vec![IoAction::CancelRead(HttpErrorCode::CORO_CANCELLED, owner)],
                );
                control.feed(queue(&[b"ok"], 2), false);
                let read = transport
                    .read(
                        &context,
                        1_024,
                        1_024,
                        Duration::ZERO,
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap();
                assert_eq!(flatten(read.data()), b"ok");
                transport.close(&context).unwrap();
            });
        }

        /// Each reader read returns its bytes within its own bounds: a queue bounded at that
        /// read's maximum, holding the backend's segments, whatever an earlier read's bounds
        /// were.
        #[test]
        fn each_owned_reader_read_keeps_its_own_bounds() {
            run_worker(|context| async move {
                let (transport, control) =
                    transport(&context, ByteEventCapability::Unsupported, None);
                control.bound_reads_at_maximum();
                let mut reader = transport
                    .reader(&context, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .unwrap();
                // (maximum, allocation size, bytes fed, segment lengths returned)
                for (maximum, allocation_size, bytes, segments) in [
                    (8, 8, &b"abcdefgh"[..], vec![8]),
                    (4, 2, b"wxyz", vec![4]),
                    (6, 3, b"pq", vec![2]),
                ] {
                    control.feed(queue(&[bytes], bytes.len()), false);
                    reader.start_read(maximum, allocation_size).unwrap();
                    let read = poll_fn(|context| reader.poll_read(context)).await.unwrap();
                    assert_eq!(read.data().limit(), maximum, "bounded at its own maximum");
                    assert_eq!(
                        read.data()
                            .segments()
                            .map(|segment| segment.len())
                            .collect::<Vec<_>>(),
                        segments,
                        "the backend's segments ({maximum}, {allocation_size})",
                    );
                    assert_eq!(flatten(read.data()), bytes);
                }
                transport.close(&context).unwrap();
            });
        }

        /// A reader read whose poll panics stays started: a new read is refused, and the next
        /// poll finishes the same read with the bytes that arrive.
        #[test]
        fn a_reader_read_whose_poll_panics_stays_started() {
            run_worker(|context| async move {
                let (transport, control) =
                    transport(&context, ByteEventCapability::Unsupported, None);
                let mut reader = transport
                    .reader(&context, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .unwrap();
                reader.start_read(1_024, 1_024).unwrap();
                control.panic_on_next_read_poll();
                assert!(
                    catch_unwind(AssertUnwindSafe(|| {
                        reader.poll_read(&mut Context::from_waker(Waker::noop()))
                    }))
                    .is_err()
                );
                assert!(transport.has_pending_read(&context).unwrap());
                assert_eq!(
                    reader.start_read(1_024, 1_024).unwrap_err().code(),
                    HttpErrorCode::INVALID_STATE_TRANSITION,
                    "the read whose poll panicked is still started",
                );
                control.feed(queue(&[b"ok"], 2), false);
                let read = poll_fn(|context| reader.poll_read(context)).await.unwrap();
                assert_eq!(flatten(read.data()), b"ok");
                assert!(!transport.has_pending_read(&context).unwrap());
                assert_eq!(control.reads_started(), 1, "the same backend read finished");
                assert!(control.actions().is_empty(), "nothing was cancelled");
                transport.close(&context).unwrap();
            });
        }

        /// Dropping a reader whose last read was returned leaves the transport's next read,
        /// from the read API, pending and uncancelled.
        #[test]
        fn dropping_an_idle_owned_reader_cancels_nothing() {
            run_worker(|context| async move {
                let (transport, control) =
                    transport(&context, ByteEventCapability::Unsupported, None);
                let mut reader = transport
                    .reader(&context, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .unwrap();
                control.feed(queue(&[b"ok"], 2), false);
                reader.start_read(1_024, 1_024).unwrap();
                poll_fn(|context| reader.poll_read(context)).await.unwrap();
                let mut pending = transport.read(
                    &context,
                    1_024,
                    1_024,
                    Duration::ZERO,
                    CancellationToken::never(),
                );
                assert!(poll_once(pending.as_mut()).is_pending());
                drop(reader);
                assert!(
                    transport.has_pending_read(&context).unwrap(),
                    "the read API's read stays registered",
                );
                assert!(control.actions().is_empty(), "nothing was cancelled");
                control.feed(queue(&[b"next"], 4), false);
                let read = pending.await.unwrap();
                assert_eq!(flatten(read.data()), b"next");
                transport.close(&context).unwrap();
            });
        }

        /// A pending reader read polled again with the waker it holds keeps that clone: three
        /// polls with one waker clone it once for the read and once per pending poll in the
        /// fake backend, which keeps each pending poll's waker. The reader's never-cancellable
        /// token clones none.
        #[test]
        fn a_reader_read_polled_again_with_its_waker_clones_it_once() {
            run_worker(|context| async move {
                let (transport, control) =
                    transport(&context, ByteEventCapability::Unsupported, None);
                let mut reader = transport
                    .reader(&context, Duration::ZERO, CancellationToken::never())
                    .unwrap()
                    .unwrap();
                let counts = Arc::new(WakerCounts::default());
                let waker = counts.waker();
                reader.start_read(1_024, 1_024).unwrap();
                for _ in 0..3 {
                    assert!(
                        reader
                            .poll_read(&mut Context::from_waker(&waker))
                            .is_pending()
                    );
                }
                assert_eq!(
                    counts.clones(),
                    1 + 3,
                    "the read clones the waker once; the backend clones it per poll",
                );
                control.feed(queue(&[b"ok"], 2), false);
                assert_eq!(counts.wakes(), 1, "the bytes wake the read");
                let Poll::Ready(Ok(read)) = reader.poll_read(&mut Context::from_waker(&waker))
                else {
                    panic!("the fed read completes");
                };
                assert_eq!(flatten(read.data()), b"ok");
                assert_eq!(counts.clones(), 4, "the completing poll clones nothing");
                assert_eq!(counts.live(), 1, "the completed read keeps no waker");
                transport.close(&context).unwrap();
            });
        }
    }
}
