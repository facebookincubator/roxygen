use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomPinned;
use std::num::NonZeroU32;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::panic_any;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::task::Context;
use std::task::Poll;
use std::thread::ThreadId;
use std::time::Duration;

use ::http_body::Body;
use ::http_body::Frame;
use ::http_body::SizeHint;
use bytes::Bytes;
use futures::task::noop_waker;
use http::Request;
use http::Response;
use http_body_util::BodyExt;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::buf_queue::BufQueueError;
use roxygen_core::byte_event::ByteEventType;
use roxygen_core::byte_event::HttpByteEvent;
use roxygen_core::byte_event::HttpByteEventCallback;
use roxygen_core::byte_event::HttpByteEventRegistration;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::events::ResumeStatus;
use roxygen_core::headers::HttpHeaderName;
use roxygen_core::headers::HttpHeaders;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::driver::WorkerDriver;
use roxygen_runtime::managed::RuntimeConfig;
use roxygen_runtime::managed::RuntimeOwner;
use roxygen_runtime::source_proxy::RemoteSourceCreateError;
use roxygen_runtime::source_proxy::RemoteSourceError;
use roxygen_runtime::tokio_host::ManagedWorker;
use roxygen_runtime::worker::CleanupRegistrationError;
use roxygen_runtime::worker::WorkerCleanupObservationError;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerHandle;
use roxygen_runtime::worker::WorkerState;
use tokio::sync::oneshot;

use crate::body::BodyCleanupObserver;
use crate::body::BoxError;
use crate::body::LocalBodyOptions;
use crate::body::RoxygenBody;
use crate::body::admit_owned_bytes;
use crate::body::admit_untrusted_bytes;

fn body_retirement(
    observer: &BodyCleanupObserver,
) -> Poll<Result<(), WorkerCleanupObservationError>> {
    Box::pin(observer.wait())
        .as_mut()
        .poll(&mut Context::from_waker(&noop_waker()))
}

thread_local! {
    static NATIVE_BODY_CONTEXT: RefCell<Option<WorkerContext>> = const { RefCell::new(None) };
}

fn native_body_driver() -> (WorkerHandle, Pin<Box<WorkerDriver>>, WorkerContext) {
    let (handle, driver) = WorkerDriver::new(2).unwrap();
    let mut driver = Box::pin(driver);
    let mut call = Box::pin(
        handle
            .call(|context| NATIVE_BODY_CONTEXT.set(Some(context.clone())))
            .unwrap(),
    );
    assert!(
        driver
            .as_mut()
            .poll(&mut Context::from_waker(&noop_waker()))
            .is_pending()
    );
    assert!(matches!(
        call.as_mut().poll(&mut Context::from_waker(&noop_waker())),
        Poll::Ready(Ok(()))
    ));
    let context = NATIVE_BODY_CONTEXT.take().expect("actual driver context");
    (handle, driver, context)
}

#[derive(Clone)]
struct NativeDropProbe {
    owner: ThreadId,
    polls: Rc<Cell<usize>>,
    drops: Rc<Cell<usize>>,
    stops: Rc<Cell<usize>>,
    observation: Rc<RefCell<Option<BodyCleanupObserver>>>,
}

impl NativeDropProbe {
    fn new() -> Self {
        Self {
            owner: std::thread::current().id(),
            polls: Rc::new(Cell::new(0)),
            drops: Rc::new(Cell::new(0)),
            stops: Rc::new(Cell::new(0)),
            observation: Rc::new(RefCell::new(None)),
        }
    }

    fn dropped(&self) {
        assert_eq!(std::thread::current().id(), self.owner);
        assert!(
            body_retirement(self.observation.borrow().as_ref().expect("export observed"))
                .is_pending()
        );
        self.drops.set(self.drops.get() + 1);
    }
}

#[derive(Clone, Copy)]
enum NativeFutureDisposition {
    Ready,
    Pending,
    PollPanic,
}

struct NativeProbeFuture<T> {
    output: Option<T>,
    probe: NativeDropProbe,
    disposition: NativeFutureDisposition,
}

impl<T: Unpin> Future for NativeProbeFuture<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<T> {
        assert_eq!(std::thread::current().id(), self.probe.owner);
        self.probe.polls.set(self.probe.polls.get() + 1);
        match self.disposition {
            NativeFutureDisposition::Ready => {
                Poll::Ready(self.output.take().expect("one native result"))
            }
            NativeFutureDisposition::Pending => Poll::Pending,
            NativeFutureDisposition::PollPanic => panic!("native future poll"),
        }
    }
}

impl<T> Drop for NativeProbeFuture<T> {
    fn drop(&mut self) {
        self.probe.dropped();
        if !matches!(self.disposition, NativeFutureDisposition::PollPanic) {
            panic!("native future drop");
        }
    }
}

struct NativeFaultBodySource {
    read: Option<LocalSourceFuture<Result<HttpBodyEvent, HttpError>>>,
    probe: NativeDropProbe,
    drop_panics: bool,
}

impl HttpSource for NativeFaultBodySource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        panic!("body export does not reread the native head");
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.read
            .take()
            .expect("no read after failed body or resume")
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {
        assert_eq!(std::thread::current().id(), self.probe.owner);
        self.probe.stops.set(self.probe.stops.get() + 1);
    }

    fn set_read_timeout(&mut self, _timeout: Duration) {}
}

impl Drop for NativeFaultBodySource {
    fn drop(&mut self) {
        self.probe.dropped();
        assert!(!self.drop_panics, "native source drop");
    }
}

#[test]
fn joined_body_cancel_drop_panic_is_unconfirmed() {
    let (handle, mut driver, context) = native_body_driver();
    let probe = NativeDropProbe::new();
    let (prepared, observer) = RoxygenBody::prepare_local(&context, 1).unwrap();
    *probe.observation.borrow_mut() = Some(observer.clone());
    let body = prepared
        .into_body(
            Box::new(NativeFaultBodySource {
                read: None,
                probe: probe.clone(),
                drop_panics: true,
            }),
            NonZeroU32::new(1).unwrap(),
            SizeHint::new(),
        )
        .unwrap();
    drop(body);
    assert!(body_retirement(&observer).is_pending());
    let failure = catch_unwind(AssertUnwindSafe(|| {
        driver
            .as_mut()
            .poll(&mut Context::from_waker(&noop_waker()))
    }))
    .expect_err("native source failure survives cleanup");
    assert_eq!(failure.downcast_ref::<&str>(), Some(&"native source drop"));
    assert_eq!(probe.drops.get(), 1);
    assert_eq!(probe.stops.get(), 1);
    assert_eq!(handle.state(), WorkerState::Closed);
    assert_eq!(
        body_retirement(&observer),
        Poll::Ready(Err(WorkerCleanupObservationError::Unconfirmed))
    );
    assert_eq!(
        body_retirement(&observer.clone()),
        Poll::Ready(Err(WorkerCleanupObservationError::Unconfirmed))
    );
    drop(driver);
}

#[derive(Clone, Copy)]
enum StopRetirementFault {
    HolderDrop,
    Callback,
    CallbackAndDrop,
}

#[derive(Debug)]
struct StopRetirementMarker;

#[derive(Default)]
struct StopRetirementFacts {
    stops: Mutex<Vec<(ThreadId, Option<HttpErrorCode>)>>,
    source_drops: AtomicUsize,
    source_drop_thread: Mutex<Option<ThreadId>>,
    local_actions: AtomicUsize,
    pending_during_drop: AtomicBool,
    destructor_released: AtomicBool,
    payload_drops: AtomicUsize,
    payload_drop_thread: Mutex<Option<ThreadId>>,
    nested_payload_drops: AtomicUsize,
    capture_drops: AtomicUsize,
    capture_drop_thread: Mutex<Option<ThreadId>>,
    capture_released: AtomicBool,
}

struct StopRetirementNestedPayload(Arc<StopRetirementFacts>);

impl Drop for StopRetirementNestedPayload {
    fn drop(&mut self) {
        self.0.nested_payload_drops.fetch_add(1, Ordering::SeqCst);
        panic!("nested stop-retirement payload destructor");
    }
}

struct StopRetirementSecondaryPayload(Arc<StopRetirementFacts>);

impl Drop for StopRetirementSecondaryPayload {
    fn drop(&mut self) {
        self.0.payload_drops.fetch_add(1, Ordering::SeqCst);
        *self.0.payload_drop_thread.lock().unwrap() = Some(std::thread::current().id());
        panic_any(StopRetirementNestedPayload(Arc::clone(&self.0)));
    }
}

struct StopRetirementSource {
    fault: StopRetirementFault,
    first: Arc<StopRetirementMarker>,
    facts: Arc<StopRetirementFacts>,
    local_actions: Rc<Cell<usize>>,
    observer: BodyCleanupObserver,
    dropping: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
}

impl HttpSource for StopRetirementSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        panic!("stop retirement does not read a native head");
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        panic!("stop retirement does not read a native body");
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.local_actions.set(self.local_actions.get() + 1);
        self.facts
            .stops
            .lock()
            .unwrap()
            .push((std::thread::current().id(), error));
        if !matches!(self.fault, StopRetirementFault::HolderDrop) {
            panic_any(Arc::clone(&self.first));
        }
    }

    fn set_read_timeout(&mut self, _timeout: Duration) {}
}

impl Drop for StopRetirementSource {
    fn drop(&mut self) {
        self.local_actions.set(self.local_actions.get() + 1);
        self.facts
            .local_actions
            .store(self.local_actions.get(), Ordering::SeqCst);
        self.facts.source_drops.fetch_add(1, Ordering::SeqCst);
        *self.facts.source_drop_thread.lock().unwrap() = Some(std::thread::current().id());
        self.facts.pending_during_drop.store(
            body_retirement(&self.observer).is_pending(),
            Ordering::SeqCst,
        );
        let _ = self.dropping.send(());
        self.facts.destructor_released.store(
            self.release.recv_timeout(Duration::from_secs(10)).is_ok(),
            Ordering::SeqCst,
        );
        match self.fault {
            StopRetirementFault::HolderDrop => panic_any(Arc::clone(&self.first)),
            StopRetirementFault::Callback => {}
            StopRetirementFault::CallbackAndDrop => {
                panic_any(StopRetirementSecondaryPayload(Arc::clone(&self.facts)));
            }
        }
    }
}

struct StopRetirementCapture(Arc<StopRetirementFacts>);

impl Drop for StopRetirementCapture {
    fn drop(&mut self) {
        self.0.capture_drops.fetch_add(1, Ordering::SeqCst);
        *self.0.capture_drop_thread.lock().unwrap() = Some(std::thread::current().id());
    }
}

struct StopRetirementThread {
    destructor_release: Option<mpsc::Sender<()>>,
    capture_release: Option<oneshot::Sender<()>>,
    force: Option<oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

fn discard_stop_retirement_panic(payload: Box<dyn std::any::Any + Send>) {
    if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        roxygen_runtime::panic::discard_panic(secondary);
    }
}

impl Drop for StopRetirementThread {
    fn drop(&mut self) {
        drop(self.destructor_release.take());
        drop(self.capture_release.take());
        drop(self.force.take());
        let Some(thread) = self.thread.take() else {
            return;
        };
        let Err(payload) = thread.join() else {
            return;
        };
        if std::thread::panicking() {
            discard_stop_retirement_panic(payload);
        } else {
            resume_unwind(payload);
        }
    }
}

fn stop_retirement_case(fault: StopRetirementFault) {
    let first = Arc::new(StopRetirementMarker);
    let facts = Arc::new(StopRetirementFacts::default());
    let worker_first = Arc::clone(&first);
    let worker_facts = Arc::clone(&facts);
    let (setup, prepared) = mpsc::channel();
    let (dropping, drop_started) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let (capture_release, capture_released) = oneshot::channel();
    let (retired, source_retired) = mpsc::channel();
    let (force, forced) = oneshot::channel();
    let (finished, worker_finished) = mpsc::channel();
    let thread = std::thread::spawn(move || {
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            let (handle, driver, context) = native_body_driver();
            let (prepared, observer) = RoxygenBody::prepare_local(&context, 1).unwrap();
            let capture = StopRetirementCapture(Arc::clone(&worker_facts));
            let observed = observer.clone();
            let capture_facts = Arc::clone(&worker_facts);
            let independent = context
                .register_scope(async move {
                    let _ = retired.send(observed.wait().await);
                    capture_facts
                        .capture_released
                        .store(capture_released.await.is_ok(), Ordering::SeqCst);
                    drop(capture);
                })
                .unwrap();
            let independent_observer = independent.observe();
            let _independent_task = independent.activate().unwrap();
            let source = StopRetirementSource {
                fault,
                first: worker_first,
                facts: worker_facts,
                local_actions: Rc::new(Cell::new(0)),
                observer: observer.clone(),
                dropping,
                release: released,
            };
            let source: Box<dyn HttpSource> = if matches!(fault, StopRetirementFault::HolderDrop) {
                Box::new(HttpSourceHolder::new(Box::new(source)))
            } else {
                Box::new(source)
            };
            let body = prepared
                .into_body(source, NonZeroU32::new(1).unwrap(), SizeHint::new())
                .unwrap();
            if setup
                .send((
                    body,
                    observer,
                    handle,
                    independent_observer,
                    std::thread::current().id(),
                ))
                .is_err()
            {
                return;
            }
            futures::executor::block_on(async move {
                if let futures::future::Either::Right((_, driver)) =
                    futures::future::select(driver, Box::pin(forced)).await
                {
                    drop(driver);
                }
            });
        }));
        let _ = finished.send(());
        if let Err(payload) = outcome {
            resume_unwind(payload);
        }
    });
    let mut worker = StopRetirementThread {
        destructor_release: Some(release),
        capture_release: Some(capture_release),
        force: Some(force),
        thread: Some(thread),
    };
    let (body, observer, handle, independent, owner) = prepared
        .recv_timeout(Duration::from_secs(10))
        .expect("worker prepared the actual body scope");
    assert_ne!(owner, std::thread::current().id());
    let expected_stop = if matches!(fault, StopRetirementFault::HolderDrop) {
        None
    } else {
        let abort = body.abort_handle();
        assert!(abort.stop_reading(Some(HttpError::new(HttpErrorCode::CORO_CANCELLED))));
        assert!(!abort.stop_reading(None));
        Some(HttpErrorCode::CORO_CANCELLED)
    };
    drop(body);
    drop_started
        .recv_timeout(Duration::from_secs(10))
        .expect("actual native destructor entered");
    assert_eq!(
        facts.stops.lock().unwrap().as_slice(),
        &[(owner, expected_stop)]
    );
    assert_eq!(facts.source_drops.load(Ordering::SeqCst), 1);
    assert_eq!(facts.local_actions.load(Ordering::SeqCst), 2);
    assert_eq!(*facts.source_drop_thread.lock().unwrap(), Some(owner));
    assert!(facts.pending_during_drop.load(Ordering::SeqCst));
    assert!(body_retirement(&observer).is_pending());
    assert!(body_retirement(&observer.clone()).is_pending());
    assert_eq!(facts.capture_drops.load(Ordering::SeqCst), 0);
    worker.destructor_release.take().unwrap().send(()).unwrap();
    assert_eq!(
        source_retired
            .recv_timeout(Duration::from_secs(10))
            .expect("composed source receipt completed"),
        Err(WorkerCleanupObservationError::Unconfirmed)
    );
    assert!(facts.destructor_released.load(Ordering::SeqCst));
    assert_eq!(
        body_retirement(&observer),
        Poll::Ready(Err(WorkerCleanupObservationError::Unconfirmed))
    );
    assert_eq!(facts.capture_drops.load(Ordering::SeqCst), 0);
    assert!(
        Box::pin(independent.wait())
            .as_mut()
            .poll(&mut Context::from_waker(&noop_waker()))
            .is_pending()
    );
    assert!(!worker.thread.as_ref().unwrap().is_finished());
    worker.capture_release.take().unwrap().send(()).unwrap();
    worker_finished
        .recv_timeout(Duration::from_secs(10))
        .expect("worker drained the independently gated scope");
    let payload = worker
        .thread
        .take()
        .unwrap()
        .join()
        .expect_err("physical worker join retains the original native panic");
    let actual_first = match payload.downcast::<Arc<StopRetirementMarker>>() {
        Ok(first) => first,
        Err(payload) => {
            discard_stop_retirement_panic(payload);
            panic!("physical worker join did not retain the exact typed original payload");
        }
    };
    assert!(Arc::ptr_eq(&actual_first, &first));
    assert_eq!(handle.state(), WorkerState::Closed);
    assert!(matches!(
        handle.call(|_| ()),
        Err(roxygen_runtime::worker::SubmitError::Closed)
    ));
    assert_eq!(facts.source_drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        facts.stops.lock().unwrap().as_slice(),
        &[(owner, expected_stop)]
    );
    assert!(facts.capture_released.load(Ordering::SeqCst));
    assert_eq!(facts.capture_drops.load(Ordering::SeqCst), 1);
    assert_eq!(*facts.capture_drop_thread.lock().unwrap(), Some(owner));
    assert_eq!(
        Box::pin(independent.wait())
            .as_mut()
            .poll(&mut Context::from_waker(&noop_waker())),
        Poll::Ready(Ok(()))
    );
    assert_eq!(
        body_retirement(&observer),
        Poll::Ready(Err(WorkerCleanupObservationError::Unconfirmed))
    );
    assert_eq!(
        body_retirement(&observer.clone()),
        Poll::Ready(Err(WorkerCleanupObservationError::Unconfirmed))
    );
    let secondary_panics = matches!(fault, StopRetirementFault::CallbackAndDrop);
    assert_eq!(
        facts.payload_drops.load(Ordering::SeqCst),
        usize::from(secondary_panics)
    );
    assert_eq!(
        *facts.payload_drop_thread.lock().unwrap(),
        secondary_panics.then_some(owner)
    );
    assert_eq!(
        facts.nested_payload_drops.load(Ordering::SeqCst),
        usize::from(secondary_panics)
    );
}

#[test]
fn joined_body_holder_stop_drop_panic_is_unconfirmed() {
    stop_retirement_case(StopRetirementFault::HolderDrop);
}

#[test]
fn joined_body_stop_callback_panic_is_unconfirmed() {
    stop_retirement_case(StopRetirementFault::Callback);
}

#[test]
fn joined_body_stop_retirement_preserves_first_payload() {
    stop_retirement_case(StopRetirementFault::CallbackAndDrop);
}

#[test]
fn joined_body_native_future_drop_panic_is_unconfirmed() {
    for resume in [false, true] {
        for disposition in [
            NativeFutureDisposition::Ready,
            NativeFutureDisposition::Pending,
            NativeFutureDisposition::PollPanic,
        ] {
            let (handle, mut driver, context) = native_body_driver();
            let probe = NativeDropProbe::new();
            let mut source_probe = NativeDropProbe::new();
            source_probe.observation = Rc::clone(&probe.observation);
            let read: LocalSourceFuture<Result<HttpBodyEvent, HttpError>> = if resume {
                let future = NativeProbeFuture {
                    output: Some(ResumeStatus::Signalled),
                    probe: probe.clone(),
                    disposition,
                };
                Box::pin(std::future::ready(Ok(HttpBodyEvent::suspend(Box::pin(
                    future,
                )))))
            } else {
                Box::pin(NativeProbeFuture {
                    output: Some(Err(HttpError::new(HttpErrorCode::READ_TIMEOUT))),
                    probe: probe.clone(),
                    disposition,
                })
            };
            let source = NativeFaultBodySource {
                read: Some(read),
                probe: source_probe.clone(),
                drop_panics: false,
            };
            let (mut body, observer) = RoxygenBody::try_from_local_with_observer(
                &context,
                Box::new(source),
                LocalBodyOptions::default(),
            )
            .unwrap();
            *probe.observation.borrow_mut() = Some(observer.clone());
            assert!(
                Pin::new(&mut body)
                    .poll_frame(&mut Context::from_waker(&noop_waker()))
                    .is_pending()
            );
            let result = catch_unwind(AssertUnwindSafe(|| {
                driver
                    .as_mut()
                    .poll(&mut Context::from_waker(&noop_waker()))
            }));
            let failure = if matches!(disposition, NativeFutureDisposition::Pending) {
                assert!(
                    result
                        .expect("genuinely pending native future")
                        .is_pending()
                );
                assert_eq!(probe.polls.get(), 1);
                assert_eq!(probe.drops.get(), 0);
                assert!(body_retirement(&observer).is_pending());
                assert!(body.abort_handle().stop_reading(None));
                catch_unwind(AssertUnwindSafe(|| {
                    driver
                        .as_mut()
                        .poll(&mut Context::from_waker(&noop_waker()))
                }))
                .expect_err("pending native disposal failure")
            } else {
                result.expect_err("ready disposal or ordinary native poll panic")
            };
            let (message, expected) = if matches!(disposition, NativeFutureDisposition::PollPanic) {
                ("native future poll", Ok(()))
            } else {
                (
                    "native future drop",
                    Err(WorkerCleanupObservationError::Unconfirmed),
                )
            };
            assert_eq!(failure.downcast_ref::<&str>(), Some(&message));
            assert_eq!(probe.polls.get(), 1);
            assert_eq!(probe.drops.get(), 1);
            assert_eq!(source_probe.drops.get(), 1);
            assert_eq!(handle.state(), WorkerState::Closed);
            assert_eq!(body_retirement(&observer), Poll::Ready(expected));
            assert_eq!(body_retirement(&observer.clone()), Poll::Ready(expected));
            drop(body);
            drop(driver);
        }
    }
}

struct NativeOutputPayload;

impl Drop for NativeOutputPayload {
    fn drop(&mut self) {
        panic!("native output payload drop");
    }
}

struct NativeOutputBytes {
    bytes: [u8; 1],
    owner: ThreadId,
    drops: Arc<AtomicUsize>,
    observation: Arc<Mutex<Option<BodyCleanupObserver>>>,
}

impl AsRef<[u8]> for NativeOutputBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for NativeOutputBytes {
    fn drop(&mut self) {
        assert_eq!(std::thread::current().id(), self.owner);
        assert!(
            body_retirement(
                self.observation
                    .lock()
                    .unwrap()
                    .as_ref()
                    .expect("native output observed")
            )
            .is_pending()
        );
        assert_eq!(self.drops.fetch_add(1, Ordering::SeqCst), 0);
        panic_any(NativeOutputPayload);
    }
}

struct NativeOutputFuture {
    event: Option<HttpBodyEvent>,
    probe: NativeDropProbe,
    fail: bool,
}

impl Future for NativeOutputFuture {
    type Output = Result<HttpBodyEvent, HttpError>;

    fn poll(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        self.probe.polls.set(self.probe.polls.get() + 1);
        Poll::Ready(Ok(self.event.take().expect("one native output")))
    }
}

impl Drop for NativeOutputFuture {
    fn drop(&mut self) {
        self.probe.dropped();
        assert!(!self.fail, "native output future first");
    }
}

#[test]
fn joined_body_output_drop_failure_is_unconfirmed() {
    for future_boundary in [false, true] {
        let (handle, mut driver, context) = native_body_driver();
        let (release, released) = oneshot::channel();
        let independent_done = Rc::new(Cell::new(false));
        let done = Rc::clone(&independent_done);
        let independent = context
            .register_scope(async move {
                released.await.unwrap();
                done.set(true);
            })
            .unwrap();
        let independent_observer = independent.observe();
        drop(independent.activate().unwrap());
        let drops = Arc::new(AtomicUsize::new(0));
        let output_observation = Arc::new(Mutex::new(None));
        let mut data = BufQueue::new(1);
        data.push_bytes(Bytes::from_owner(NativeOutputBytes {
            bytes: *b"x",
            owner: std::thread::current().id(),
            drops: Arc::clone(&drops),
            observation: Arc::clone(&output_observation),
        }))
        .unwrap();
        let future_probe = NativeDropProbe::new();
        let mut source_probe = NativeDropProbe::new();
        source_probe.observation = Rc::clone(&future_probe.observation);
        let future = NativeOutputFuture {
            event: Some(HttpBodyEvent::body(data, true)),
            probe: future_probe.clone(),
            fail: future_boundary,
        };
        let source = NativeFaultBodySource {
            read: Some(Box::pin(future)),
            probe: source_probe.clone(),
            drop_panics: !future_boundary,
        };
        let (mut body, observer) = RoxygenBody::try_from_local_with_observer(
            &context,
            Box::new(source),
            LocalBodyOptions::default(),
        )
        .unwrap();
        *output_observation.lock().unwrap() = Some(observer.clone());
        *future_probe.observation.borrow_mut() = Some(observer.clone());
        assert!(
            Pin::new(&mut body)
                .poll_frame(&mut Context::from_waker(&noop_waker()))
                .is_pending()
        );
        assert!(
            driver
                .as_mut()
                .poll(&mut Context::from_waker(&noop_waker()))
                .is_pending()
        );
        assert_eq!(future_probe.polls.get(), 1);
        assert_eq!(future_probe.drops.get(), 1);
        assert_eq!(source_probe.drops.get(), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(
            body_retirement(&observer),
            Poll::Ready(Err(WorkerCleanupObservationError::Unconfirmed))
        );
        assert!(!independent_done.get());
        assert!(
            Box::pin(independent_observer.wait())
                .as_mut()
                .poll(&mut Context::from_waker(&noop_waker()))
                .is_pending()
        );
        release.send(()).unwrap();
        let failure = catch_unwind(AssertUnwindSafe(|| {
            driver
                .as_mut()
                .poll(&mut Context::from_waker(&noop_waker()))
        }))
        .expect_err("first native failure follows independent cleanup");
        let expected = if future_boundary {
            "native output future first"
        } else {
            "native source drop"
        };
        assert_eq!(failure.downcast_ref::<&str>(), Some(&expected));
        assert!(independent_done.get());
        assert_eq!(handle.state(), WorkerState::Closed);
        assert_eq!(
            Box::pin(independent_observer.wait())
                .as_mut()
                .poll(&mut Context::from_waker(&noop_waker())),
            Poll::Ready(Ok(()))
        );
        assert_eq!(
            body_retirement(&observer.clone()),
            Poll::Ready(Err(WorkerCleanupObservationError::Unconfirmed))
        );
        drop(body);
        drop(driver);
    }
}

#[derive(Clone, Copy)]
enum ProjectionFailure {
    None,
    Message(&'static str),
    NestedPayload,
}

impl ProjectionFailure {
    fn trigger(self) {
        match self {
            Self::None => {}
            Self::Message(message) => panic_any(message),
            Self::NestedPayload => panic_any(NativeOutputPayload),
        }
    }
}

#[derive(Clone)]
struct ProjectionOwnedProbe {
    owner: ThreadId,
    drops: Arc<AtomicUsize>,
    observation: Arc<Mutex<Option<BodyCleanupObserver>>>,
    failure: ProjectionFailure,
}

impl ProjectionOwnedProbe {
    fn assert_pending(&self) {
        assert_eq!(std::thread::current().id(), self.owner);
        assert!(body_retirement(self.observation.lock().unwrap().as_ref().unwrap()).is_pending());
    }

    fn dropped(&self) {
        self.assert_pending();
        assert_eq!(self.drops.fetch_add(1, Ordering::SeqCst), 0);
        self.failure.trigger();
    }
}

struct ProjectionBytes {
    bytes: [u8; 1],
    probe: ProjectionOwnedProbe,
}

impl AsRef<[u8]> for ProjectionBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for ProjectionBytes {
    fn drop(&mut self) {
        self.probe.dropped();
    }
}

struct ProjectionResume {
    probe: ProjectionOwnedProbe,
    polls: Rc<Cell<usize>>,
}

impl Future for ProjectionResume {
    type Output = ResumeStatus;

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<ResumeStatus> {
        self.polls.set(self.polls.get() + 1);
        Poll::Pending
    }
}

impl Drop for ProjectionResume {
    fn drop(&mut self) {
        self.probe.dropped();
    }
}

struct ProjectionRegistration {
    probe: ProjectionOwnedProbe,
    group: u8,
    calls: Rc<RefCell<Vec<(u8, ByteEventType)>>>,
}

impl HttpByteEventCallback for ProjectionRegistration {
    fn on_byte_event(&self, _event: HttpByteEvent) {
        panic!("projection does not claim a transport milestone");
    }

    fn on_byte_event_cancelled(&self, event: HttpByteEvent, error: HttpError) {
        self.probe.assert_pending();
        assert_eq!(error.code(), HttpErrorCode::CANCEL);
        self.calls.borrow_mut().push((self.group, event.event_type));
        if self.group == 0 {
            match event.event_type {
                ByteEventType::TransportWrite => panic!("native registration first"),
                ByteEventType::KernelWrite => panic_any(NativeOutputPayload),
                other => panic!("unexpected first registration kind: {other:?}"),
            }
        }
    }
}

fn check_native_projection_disposal(
    event: HttpBodyEvent,
    observation: Arc<Mutex<Option<BodyCleanupObserver>>>,
    first_failure: Option<&'static str>,
) {
    check_native_projection_disposal_with_panic(
        event,
        observation,
        first_failure.map(ExpectedProjectionPanic::Message),
    );
}

enum ExpectedProjectionPanic {
    Message(&'static str),
    LastCallbackPayload,
}

struct LastCallbackPayload;

impl Drop for LastCallbackPayload {
    fn drop(&mut self) {
        panic!("last callback payload drop");
    }
}

fn check_native_projection_disposal_with_panic(
    event: HttpBodyEvent,
    observation: Arc<Mutex<Option<BodyCleanupObserver>>>,
    first_failure: Option<ExpectedProjectionPanic>,
) {
    let (handle, mut driver, context) = native_body_driver();
    let (release, released) = oneshot::channel();
    let independent_done = Rc::new(Cell::new(false));
    let done = Rc::clone(&independent_done);
    let independent = context
        .register_scope(async move {
            released.await.unwrap();
            done.set(true);
        })
        .unwrap();
    let independent_observer = independent.observe();
    drop(independent.activate().unwrap());
    let future_probe = NativeDropProbe::new();
    let mut source_probe = NativeDropProbe::new();
    source_probe.observation = Rc::clone(&future_probe.observation);
    let source = NativeFaultBodySource {
        read: Some(Box::pin(NativeOutputFuture {
            event: Some(event),
            probe: future_probe.clone(),
            fail: false,
        })),
        probe: source_probe.clone(),
        drop_panics: false,
    };
    let (mut body, observer) = RoxygenBody::try_from_local_with_observer(
        &context,
        Box::new(source),
        LocalBodyOptions::default(),
    )
    .unwrap();
    *observation.lock().unwrap() = Some(observer.clone());
    *future_probe.observation.borrow_mut() = Some(observer.clone());
    assert!(
        Pin::new(&mut body)
            .poll_frame(&mut Context::from_waker(&noop_waker()))
            .is_pending()
    );
    assert!(
        driver
            .as_mut()
            .poll(&mut Context::from_waker(&noop_waker()))
            .is_pending()
    );
    assert_eq!(future_probe.polls.get(), 1);
    assert_eq!(future_probe.drops.get(), 1);
    assert_eq!(source_probe.drops.get(), 1);
    let expected = if first_failure.is_some() {
        Err(WorkerCleanupObservationError::Unconfirmed)
    } else {
        Ok(())
    };
    assert_eq!(body_retirement(&observer), Poll::Ready(expected));
    assert!(!independent_done.get());
    assert!(
        Box::pin(independent_observer.wait())
            .as_mut()
            .poll(&mut Context::from_waker(&noop_waker()))
            .is_pending()
    );
    if first_failure.is_none() {
        let Poll::Ready(Some(Err(error))) =
            Pin::new(&mut body).poll_frame(&mut Context::from_waker(&noop_waker()))
        else {
            panic!("a cleanly discarded push keeps its existing unsupported-event error")
        };
        assert_eq!(
            error.downcast_ref::<HttpError>().unwrap().code(),
            HttpErrorCode::INVALID_STATE_TRANSITION
        );
        assert!(body.is_end_stream());
        handle.begin_shutdown();
    } else {
        assert_eq!(handle.state(), WorkerState::Closing);
    }
    release.send(()).unwrap();
    let completion = catch_unwind(AssertUnwindSafe(|| {
        driver
            .as_mut()
            .poll(&mut Context::from_waker(&noop_waker()))
    }));
    if let Some(first) = first_failure {
        let failure = completion.expect_err("native failure waits for unrelated owned cleanup");
        match first {
            ExpectedProjectionPanic::Message(first) => {
                assert_eq!(failure.downcast_ref::<&str>(), Some(&first));
            }
            ExpectedProjectionPanic::LastCallbackPayload => {
                assert!(failure.is::<LastCallbackPayload>());
                let secondary = catch_unwind(AssertUnwindSafe(|| drop(failure)))
                    .expect_err("the original callback payload still owns its destructor");
                assert_eq!(
                    secondary.downcast_ref::<&str>(),
                    Some(&"last callback payload drop")
                );
            }
        }
    } else {
        assert!(completion.expect("clean projection shutdown").is_ready());
    }
    assert!(independent_done.get());
    assert_eq!(handle.state(), WorkerState::Closed);
    assert_eq!(
        Box::pin(independent_observer.wait())
            .as_mut()
            .poll(&mut Context::from_waker(&noop_waker())),
        Poll::Ready(Ok(()))
    );
    assert_eq!(body_retirement(&observer.clone()), Poll::Ready(expected));
    drop(body);
    drop(driver);
}

#[test]
fn joined_body_registration_disposal_is_guarded_and_reported() {
    for suspended in [false, true] {
        for destructor_panics in [false, true] {
            let observation = Arc::new(Mutex::new(None));
            let probe = ProjectionOwnedProbe {
                owner: std::thread::current().id(),
                drops: Arc::new(AtomicUsize::new(0)),
                observation: Arc::clone(&observation),
                failure: if destructor_panics {
                    ProjectionFailure::NestedPayload
                } else {
                    ProjectionFailure::None
                },
            };
            let resume_polls = Rc::new(Cell::new(0));
            let mut event = if suspended {
                HttpBodyEvent::suspend(Box::pin(ProjectionResume {
                    probe: probe.clone(),
                    polls: Rc::clone(&resume_polls),
                }))
            } else {
                HttpBodyEvent::body(
                    BufQueue::from_bytes(
                        Bytes::from_owner(ProjectionBytes {
                            bytes: *b"x",
                            probe: probe.clone(),
                        }),
                        1,
                    )
                    .unwrap(),
                    true,
                )
            };
            let calls = Rc::new(RefCell::new(Vec::new()));
            let first = Rc::new(ProjectionRegistration {
                probe: probe.clone(),
                group: 0,
                calls: Rc::clone(&calls),
            });
            let second = Rc::new(ProjectionRegistration {
                probe: probe.clone(),
                group: 1,
                calls: Rc::clone(&calls),
            });
            event
                .byte_event_registrations_mut()
                .push(HttpByteEventRegistration::new(
                    None,
                    ByteEventType::TransportWrite.bit() | ByteEventType::KernelWrite.bit(),
                    &first,
                ));
            event
                .byte_event_registrations_mut()
                .push(HttpByteEventRegistration::new(
                    None,
                    ByteEventType::NicTx.bit(),
                    &second,
                ));
            check_native_projection_disposal(event, observation, Some("native registration first"));
            assert_eq!(probe.drops.load(Ordering::SeqCst), 1);
            assert_eq!(resume_polls.get(), 0);
            assert_eq!(
                *calls.borrow(),
                vec![
                    (0, ByteEventType::TransportWrite),
                    (0, ByteEventType::KernelWrite),
                    (1, ByteEventType::NicTx),
                ]
            );
        }
    }
}

struct ReleasingProjectionRegistration {
    probe: ProjectionOwnedProbe,
    last_owner: std::rc::Weak<RefCell<Option<Rc<ReleasingProjectionRegistration>>>>,
    calls: Rc<RefCell<Vec<(u8, ByteEventType)>>>,
    payload_panics: bool,
}

impl HttpByteEventCallback for ReleasingProjectionRegistration {
    fn on_byte_event(&self, _event: HttpByteEvent) {
        panic!("projection does not claim a transport milestone");
    }

    fn on_byte_event_cancelled(&self, event: HttpByteEvent, error: HttpError) {
        self.probe.assert_pending();
        assert_eq!(error.code(), HttpErrorCode::CANCEL);
        self.calls.borrow_mut().push((0, event.event_type));
        let external = self
            .last_owner
            .upgrade()
            .expect("the fixture retains the owner slot")
            .borrow_mut()
            .take()
            .expect("cancellation releases the last external callback owner");
        assert_eq!(Rc::strong_count(&external), 2);
        drop(external);
        if self.payload_panics {
            panic_any(LastCallbackPayload);
        }
        panic!("last callback invocation first");
    }
}

impl Drop for ReleasingProjectionRegistration {
    fn drop(&mut self) {
        self.probe.dropped();
    }
}

#[test]
fn joined_body_last_callback_owner_disposal_is_guarded_and_reported() {
    for payload_panics in [false, true] {
        for callback_failure in [
            ProjectionFailure::None,
            ProjectionFailure::Message("last callback destructor second"),
            ProjectionFailure::NestedPayload,
        ] {
            for body_failure in [ProjectionFailure::None, ProjectionFailure::NestedPayload] {
                let observation = Arc::new(Mutex::new(None));
                let body_probe = ProjectionOwnedProbe {
                    owner: std::thread::current().id(),
                    drops: Arc::new(AtomicUsize::new(0)),
                    observation: Arc::clone(&observation),
                    failure: body_failure,
                };
                let callback_probe = ProjectionOwnedProbe {
                    drops: Arc::new(AtomicUsize::new(0)),
                    failure: callback_failure,
                    ..body_probe.clone()
                };
                let mut event = HttpBodyEvent::body(
                    BufQueue::from_bytes(
                        Bytes::from_owner(ProjectionBytes {
                            bytes: *b"x",
                            probe: body_probe.clone(),
                        }),
                        1,
                    )
                    .unwrap(),
                    true,
                );
                let calls = Rc::new(RefCell::new(Vec::new()));
                let last_owner = Rc::new(RefCell::new(None));
                let first = Rc::new(ReleasingProjectionRegistration {
                    probe: callback_probe.clone(),
                    last_owner: Rc::downgrade(&last_owner),
                    calls: Rc::clone(&calls),
                    payload_panics,
                });
                let weak_first = Rc::downgrade(&first);
                event
                    .byte_event_registrations_mut()
                    .push(HttpByteEventRegistration::new(
                        None,
                        ByteEventType::TransportWrite.bit() | ByteEventType::KernelWrite.bit(),
                        &first,
                    ));
                *last_owner.borrow_mut() = Some(first);
                let second = Rc::new(ProjectionRegistration {
                    probe: body_probe.clone(),
                    group: 1,
                    calls: Rc::clone(&calls),
                });
                event
                    .byte_event_registrations_mut()
                    .push(HttpByteEventRegistration::new(
                        None,
                        ByteEventType::NicTx.bit(),
                        &second,
                    ));
                let expected = if payload_panics {
                    ExpectedProjectionPanic::LastCallbackPayload
                } else {
                    ExpectedProjectionPanic::Message("last callback invocation first")
                };
                check_native_projection_disposal_with_panic(event, observation, Some(expected));
                assert_eq!(callback_probe.drops.load(Ordering::SeqCst), 1);
                assert_eq!(body_probe.drops.load(Ordering::SeqCst), 1);
                assert!(last_owner.borrow().is_none());
                assert!(weak_first.upgrade().is_none());
                assert_eq!(
                    *calls.borrow(),
                    vec![
                        (0, ByteEventType::TransportWrite),
                        (1, ByteEventType::NicTx),
                    ]
                );
            }
        }
    }
}

struct DiscardedProjectionPush {
    probe: ProjectionOwnedProbe,
    reads: Rc<Cell<usize>>,
    stops: Rc<Cell<usize>>,
    stop_failure: ProjectionFailure,
}

impl HttpSource for DiscardedProjectionPush {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        self.reads.set(self.reads.get() + 1);
        panic!("an unsupported push must not be read");
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.reads.set(self.reads.get() + 1);
        panic!("an unsupported push must not be read");
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.probe.assert_pending();
        assert_eq!(error, None);
        self.stops.set(self.stops.get() + 1);
        self.stop_failure.trigger();
    }

    fn set_read_timeout(&mut self, _timeout: Duration) {}
}

impl Drop for DiscardedProjectionPush {
    fn drop(&mut self) {
        self.probe.dropped();
    }
}

#[test]
fn joined_body_discarded_push_disposal_is_guarded_and_reported() {
    for (stop_failure, drop_failure, first) in [
        (ProjectionFailure::None, ProjectionFailure::None, None),
        (
            ProjectionFailure::None,
            ProjectionFailure::Message("discarded push drop first"),
            Some("discarded push drop first"),
        ),
        (
            ProjectionFailure::Message("discarded push stop first"),
            ProjectionFailure::None,
            Some("discarded push stop first"),
        ),
        (
            ProjectionFailure::Message("discarded push stop first"),
            ProjectionFailure::NestedPayload,
            Some("discarded push stop first"),
        ),
    ] {
        let observation = Arc::new(Mutex::new(None));
        let probe = ProjectionOwnedProbe {
            owner: std::thread::current().id(),
            drops: Arc::new(AtomicUsize::new(0)),
            observation: Arc::clone(&observation),
            failure: drop_failure,
        };
        let reads = Rc::new(Cell::new(0));
        let stops = Rc::new(Cell::new(0));
        let event = HttpBodyEvent::push_promise(
            HttpMessage::new(),
            Some(Box::new(DiscardedProjectionPush {
                probe: probe.clone(),
                reads: Rc::clone(&reads),
                stops: Rc::clone(&stops),
                stop_failure,
            })),
            true,
        );
        check_native_projection_disposal(event, observation, first);
        assert_eq!(reads.get(), 0);
        assert_eq!(stops.get(), 1);
        assert_eq!(probe.drops.load(Ordering::SeqCst), 1);
    }
}

struct JoinedBodySource {
    source: ScriptedSource,
    observation: Arc<Mutex<Option<BodyCleanupObserver>>>,
    _pinned: PhantomPinned,
}

impl HttpSource for JoinedBodySource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        self.source.read_header_event()
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.source.read_body_event(maximum)
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.source.stop_reading(error);
    }

    fn set_read_timeout(&mut self, timeout: Duration) {
        self.source.set_read_timeout(timeout);
    }
}

impl Drop for JoinedBodySource {
    fn drop(&mut self) {
        assert!(
            body_retirement(
                self.observation
                    .lock()
                    .unwrap()
                    .as_ref()
                    .expect("scope observed")
            )
            .is_pending()
        );
    }
}

#[test]
fn joined_body_export_is_send_without_send_factory() {
    fn assert_send<Value: Send>() {}
    fn assert_observer<Value: Send + Sync + Clone>() {}
    assert_send::<RoxygenBody>();
    assert_observer::<BodyCleanupObserver>();
    let worker = ManagedWorker::new(2).expect("worker starts");
    let owner_thread = worker.owner_thread();
    let actions = Arc::new(Mutex::new(Vec::new()));
    let error_actions = Arc::new(Mutex::new(Vec::new()));
    let source_actions = Arc::clone(&actions);
    let failure_actions = Arc::clone(&error_actions);
    let data = Bytes::from(vec![b'x', b'd', b'a', b't', b'a', b'y']).slice(1..5);
    let pointer = data.as_ptr();
    runtime().block_on(async {
        tokio::time::timeout(Duration::from_secs(10), async {
            let (mut body, observer, mut failure, failure_observer) = worker.handle().call(move |context| {
                let observation = Arc::new(Mutex::new(None));
                let source = JoinedBodySource {
                    source: ScriptedSource::new(vec![
                        ScriptEvent::OwnedData { segments: vec![data], terminal: false },
                        ScriptEvent::Trailers(vec![(b"x-result", b"done")]),
                    ], source_actions),
                    observation: Arc::clone(&observation),
                    _pinned: PhantomPinned,
                };
                let (body, observer) = RoxygenBody::try_from_local_with_observer(
                    context, Box::new(source), LocalBodyOptions {
                        maximum_read: NonZeroU32::new(4).unwrap(),
                        size_hint: SizeHint::with_exact(4),
                        ..LocalBodyOptions::default()
                    },
                ).unwrap();
                *observation.lock().unwrap() = Some(observer.clone());
                let (failure, failure_observer) = RoxygenBody::try_from_local_with_observer(
                    context,
                    Box::new(ScriptedSource::new(vec![ScriptEvent::Error(HttpErrorCode::READ_TIMEOUT)], failure_actions)),
                    LocalBodyOptions::default(),
                ).unwrap();
                (body, observer, failure, failure_observer)
            }).unwrap().await.unwrap();
            drop(observer.wait());
            let retained = observer.clone();
            drop(observer);
            let error = failure.frame().await.unwrap().expect_err("typed native error");
            assert!(matches!(error.downcast_ref::<RemoteSourceError<HttpError>>(),
                Some(RemoteSourceError::Source(error)) if error.code() == HttpErrorCode::READ_TIMEOUT));
            failure_observer.wait().await.expect("error scope still retires");
            assert!(failure.is_end_stream());
            assert!(body_retirement(&retained).is_pending());
            assert_eq!(body.size_hint().exact(), Some(4));
            let data = frame_data(body.frame().await.unwrap().unwrap());
            assert_eq!(data.as_ptr(), pointer);
            assert_eq!(data.as_ref(), b"data");
            assert_eq!(body.size_hint().exact(), Some(0));
            assert!(!body.is_end_stream());
            let trailers = body.frame().await.unwrap().unwrap().into_trailers().unwrap();
            assert_eq!(trailers["x-result"], "done");
            assert!(body.is_end_stream());
            assert!(body.frame().await.is_none());
            retained.wait().await.expect("actual source scope retired");
        }).await.expect("joined bodies complete within bound");
    });
    worker
        .shutdown()
        .expect("worker physically joined separately");
    assert_eq!(
        recorded_actions(&actions),
        vec![
            SourceAction::Read {
                owner: owner_thread,
                maximum: 4
            },
            SourceAction::Read {
                owner: owner_thread,
                maximum: 4
            },
            SourceAction::Drop(owner_thread),
        ]
    );
    assert_eq!(
        recorded_actions(&error_actions),
        vec![
            SourceAction::Read {
                owner: owner_thread,
                maximum: 64 * 1024
            },
            SourceAction::Drop(owner_thread),
        ]
    );
}

#[test]
fn prepared_body_real_head_during_grace_preserves_frames() {
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 2,
    })
    .unwrap();
    let handle = owner.handle();
    let worker = handle.worker(0).unwrap();
    let owner_thread = worker.owner_thread();
    let actions = Arc::new(Mutex::new(Vec::new()));
    let source_actions = Arc::clone(&actions);
    let data = Bytes::from(vec![b'b'; 8]).slice(2..6);
    let pointer = data.as_ptr();
    runtime().block_on(async {
        tokio::time::timeout(Duration::from_secs(10), async {
            let (prepared_tx, prepared_rx) = oneshot::channel();
            let (release, released) = oneshot::channel();
            let response = worker
                .call_local(move |context| async move {
                    let (prepared, observer) = RoxygenBody::prepare_local(&context, 1).unwrap();
                    let abort = prepared.abort_handle();
                    assert!(prepared_tx.send((observer, abort)).is_ok());
                    let mut source = HttpSourceHolder::new(Box::new(NativeHeaderSource {
                        body: ScriptedSource::new(
                            vec![
                                ScriptEvent::OwnedData {
                                    segments: vec![data],
                                    terminal: false,
                                },
                                ScriptEvent::Trailers(vec![(b"x-result", b"late")]),
                            ],
                            source_actions,
                        ),
                        terminal: false,
                    }));
                    released.await.unwrap();
                    assert!(matches!(
                        RoxygenBody::prepare_local(&context, 1),
                        Err(RemoteSourceCreateError::Cleanup(
                            CleanupRegistrationError::Closed
                        ))
                    ));
                    let head = source.read_header_event().await.unwrap();
                    assert!(!head.eom());
                    prepared.into_body(
                        Box::new(source),
                        NonZeroU32::new(4).unwrap(),
                        SizeHint::with_exact(4),
                    )
                })
                .unwrap();
            let (observer, abort) = prepared_rx.await.unwrap();
            let quiescence = handle.quiesce(Duration::from_secs(60));
            assert!(!worker.is_accepting());
            assert!(body_retirement(&observer).is_pending());
            release.send(()).unwrap();
            let mut body = response
                .await
                .unwrap()
                .expect("real-head binding survives grace");
            let frame = frame_data(body.frame().await.unwrap().unwrap());
            assert_eq!(frame.as_ptr(), pointer);
            assert_eq!(frame.as_ref(), b"bbbb");
            assert_eq!(body.size_hint().exact(), Some(0));
            assert!(!body.is_end_stream());
            let trailers = body
                .frame()
                .await
                .unwrap()
                .unwrap()
                .into_trailers()
                .unwrap();
            assert_eq!(trailers["x-result"], "late");
            assert!(body.is_end_stream());
            observer.wait().await.expect("prepared scope retired");
            assert!(!abort.stop_reading(None));
            quiescence
                .await
                .expect("pre-admitted work completed during grace");
        })
        .await
        .expect("late head/body complete within bound");
    });
    let report = owner.join().expect("runtime physically joined");
    assert_eq!(report.workers_joined, 1);
    assert_eq!(report.workers_forced, 0);
    assert_eq!(
        recorded_actions(&actions),
        vec![
            SourceAction::Header(owner_thread),
            SourceAction::Read {
                owner: owner_thread,
                maximum: 4
            },
            SourceAction::Read {
                owner: owner_thread,
                maximum: 4
            },
            SourceAction::Drop(owner_thread),
        ]
    );
}

#[test]
fn prepared_body_header_eom_retires_unused_reservation() {
    let worker = ManagedWorker::new(2).expect("worker starts");
    let owner_thread = worker.owner_thread();
    let actions = Arc::new(Mutex::new(Vec::new()));
    let source_actions = Arc::clone(&actions);
    let mut body = runtime().block_on(async {
        tokio::time::timeout(Duration::from_secs(10), async {
            let (body, observer) = worker
                .handle()
                .call_local(move |context| async move {
                    let (prepared, observer) = RoxygenBody::prepare_local(&context, 1).unwrap();
                    let mut source = HttpSourceHolder::new(Box::new(NativeHeaderSource {
                        body: ScriptedSource::new(vec![], source_actions),
                        terminal: true,
                    }));
                    let header = source.read_header_event().await.unwrap();
                    assert!(header.eom());
                    assert!(!source.readable());
                    assert!(
                        body_retirement(&observer).is_pending(),
                        "native retirement is not scope retirement"
                    );
                    drop(prepared);
                    assert!(
                        body_retirement(&observer).is_pending(),
                        "reservation Drop only requests cancellation"
                    );
                    (RoxygenBody::empty(), observer)
                })
                .unwrap()
                .await
                .unwrap();
            observer
                .wait()
                .await
                .expect("the real unused scope retired");
            body
        })
        .await
        .expect("EOM reservation retires within bound")
    });
    worker
        .shutdown()
        .expect("worker joined before empty-body polling");
    assert!(body.is_end_stream());
    assert_eq!(body.size_hint().exact(), Some(0));
    for _ in 0..2 {
        assert!(matches!(
            Pin::new(&mut body).poll_frame(&mut Context::from_waker(&noop_waker())),
            Poll::Ready(None)
        ));
    }
    assert!(!body.abort_handle().stop_reading(None));
    assert_eq!(
        recorded_actions(&actions),
        vec![
            SourceAction::Header(owner_thread),
            SourceAction::Drop(owner_thread)
        ]
    );
}

#[test]
fn prepared_body_zero_hint_preserves_trailers() {
    let worker = ManagedWorker::new(2).expect("worker starts");
    let owner_thread = worker.owner_thread();
    let actions = Arc::new(Mutex::new(Vec::new()));
    let source_actions = Arc::clone(&actions);
    runtime().block_on(async {
        tokio::time::timeout(Duration::from_secs(10), async {
            let (mut body, observer) = worker
                .handle()
                .call_local(move |context| async move {
                    let (prepared, observer) = RoxygenBody::prepare_local(&context, 1).unwrap();
                    let mut source = HttpSourceHolder::new(Box::new(NativeHeaderSource {
                        body: ScriptedSource::new(
                            vec![ScriptEvent::Trailers(vec![(b"x-result", b"zero")])],
                            source_actions,
                        ),
                        terminal: false,
                    }));
                    assert!(!source.read_header_event().await.unwrap().eom());
                    assert!(source.readable());
                    let body = prepared
                        .into_body(
                            Box::new(source),
                            NonZeroU32::new(1).unwrap(),
                            SizeHint::with_exact(0),
                        )
                        .unwrap();
                    assert!(body_retirement(&observer).is_pending());
                    (body, observer)
                })
                .unwrap()
                .await
                .unwrap();
            assert_eq!(body.size_hint().exact(), Some(0));
            assert!(!body.is_end_stream());
            let trailers = body
                .frame()
                .await
                .unwrap()
                .unwrap()
                .into_trailers()
                .unwrap();
            assert_eq!(trailers["x-result"], "zero");
            assert!(body.is_end_stream());
            assert!(body.frame().await.is_none());
            observer.wait().await.expect("trailer scope retired");
        })
        .await
        .expect("zero-hint trailers complete within bound");
    });
    worker.shutdown().expect("worker physically joined");
    assert_eq!(
        recorded_actions(&actions),
        vec![
            SourceAction::Header(owner_thread),
            SourceAction::Read {
                owner: owner_thread,
                maximum: 1
            },
            SourceAction::Drop(owner_thread),
        ]
    );
}

struct NativeHeaderSource {
    body: ScriptedSource,
    terminal: bool,
}

impl HttpSource for NativeHeaderSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        record(
            &self.body.actions,
            SourceAction::Header(std::thread::current().id()),
        );
        let mut message = HttpMessage::new();
        message.set_status_code(200).unwrap();
        Box::pin(std::future::ready(Ok(HttpHeaderEvent::new(
            message,
            self.terminal,
        )
        .unwrap())))
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.body.read_body_event(maximum)
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.body.stop_reading(error);
    }

    fn set_read_timeout(&mut self, timeout: Duration) {
        self.body.set_read_timeout(timeout);
    }
}

#[test]
fn consumed_native_header_eom_exports_an_immediately_terminal_body() {
    let worker = ManagedWorker::new(2).expect("worker starts");
    let owner = worker.owner_thread();
    let actions = Arc::new(Mutex::new(Vec::new()));
    let source_actions = Arc::clone(&actions);
    let mut body = runtime().block_on(async {
        tokio::time::timeout(
            Duration::from_secs(10),
            worker
                .handle()
                .call_local(move |_context| async move {
                    let mut source = HttpSourceHolder::new(Box::new(NativeHeaderSource {
                        body: ScriptedSource::new(vec![], source_actions),
                        terminal: true,
                    }));
                    let header = source.read_header_event().await.unwrap();
                    assert!(header.eom());
                    assert!(
                        !source.readable(),
                        "header EOM already retired the original source"
                    );
                    RoxygenBody::empty()
                })
                .expect("header read admitted"),
        )
        .await
        .expect("header read bounded")
        .expect("terminal body returned")
    });
    worker
        .shutdown()
        .expect("worker physically joined before polling empty body");
    assert!(body.is_end_stream());
    assert_eq!(body.size_hint().exact(), Some(0));
    let abort = body.abort_handle();
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut body).poll_frame(&mut context),
        Poll::Ready(None)
    ));
    assert!(matches!(
        Pin::new(&mut body).poll_frame(&mut context),
        Poll::Ready(None)
    ));
    assert!(!abort.stop_reading(None));
    drop(body);
    assert_eq!(
        recorded_actions(&actions),
        vec![SourceAction::Header(owner), SourceAction::Drop(owner)]
    );
}

#[test]
fn nonterminal_native_header_with_zero_hint_still_exports_trailers() {
    let worker = ManagedWorker::new(2).expect("worker starts");
    let owner = worker.owner_thread();
    let actions = Arc::new(Mutex::new(Vec::new()));
    let source_actions = Arc::clone(&actions);
    runtime().block_on(async {
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut body = worker
                .handle()
                .call_local(move |context| async move {
                    let mut source = HttpSourceHolder::new(Box::new(NativeHeaderSource {
                        body: ScriptedSource::new(
                            vec![ScriptEvent::Trailers(vec![(b"x-result", b"done")])],
                            source_actions,
                        ),
                        terminal: false,
                    }));
                    let header = source.read_header_event().await.unwrap();
                    assert!(!header.eom());
                    assert!(source.readable());
                    RoxygenBody::try_from_local(
                        &context,
                        Box::new(source),
                        LocalBodyOptions {
                            maximum_read: NonZeroU32::new(1).unwrap(),
                            size_hint: SizeHint::with_exact(0),
                            ..LocalBodyOptions::default()
                        },
                    )
                    .unwrap()
                })
                .expect("header read admitted")
                .await
                .expect("source exported");
            assert!(!body.is_end_stream());
            assert_eq!(body.size_hint().exact(), Some(0));
            let trailers = body
                .frame()
                .await
                .unwrap()
                .unwrap()
                .into_trailers()
                .unwrap();
            assert_eq!(trailers["x-result"], "done");
            assert!(body.is_end_stream());
            assert!(body.frame().await.is_none());
        })
        .await
        .expect("trailer delivery bounded");
    });
    worker.shutdown().expect("source scope physically joined");
    assert_eq!(
        recorded_actions(&actions),
        vec![
            SourceAction::Header(owner),
            SourceAction::Read { owner, maximum: 1 },
            SourceAction::Drop(owner)
        ]
    );
}

#[test]
fn local_body_export() {
    let worker = ManagedWorker::new(2).expect("worker starts");
    let owner = worker.owner_thread();
    let actions = Arc::new(Mutex::new(Vec::new()));
    let source_actions = Arc::clone(&actions);
    let data = Bytes::from(vec![b'x'; 8]).slice(2..6);
    let pointer = data.as_ptr();
    let mut body = runtime().block_on(async {
        tokio::time::timeout(
            Duration::from_secs(10),
            worker
                .handle()
                .call(move |context| {
                    let local = ScriptedSource::new(
                        vec![ScriptEvent::OwnedData {
                            segments: vec![data],
                            terminal: true,
                        }],
                        source_actions,
                    );
                    RoxygenBody::try_from_local(
                        context,
                        Box::new(local),
                        LocalBodyOptions {
                            maximum_read: NonZeroU32::new(4).unwrap(),
                            size_hint: SizeHint::with_exact(4),
                            ..LocalBodyOptions::default()
                        },
                    )
                })
                .expect("initializer admitted"),
        )
        .await
        .expect("initializer bounded")
        .expect("initializer returns")
        .expect("body exported")
    });
    assert_eq!(body.size_hint().exact(), Some(4));
    let frame = frame_data(next_frame(&mut body).unwrap().unwrap());
    assert_eq!(frame.as_ptr(), pointer);
    assert!(body.is_end_stream());
    assert_eq!(body.size_hint().exact(), Some(0));
    drop(body);
    worker.shutdown().expect("scope and worker join");
    assert_eq!(
        recorded_actions(&actions),
        vec![
            SourceAction::Read { owner, maximum: 4 },
            SourceAction::Drop(owner),
        ]
    );
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SourceAction {
    Header(ThreadId),
    Read {
        owner: ThreadId,
        maximum: u32,
    },
    Stop {
        owner: ThreadId,
        error: Option<HttpErrorCode>,
    },
    Drop(ThreadId),
}

fn record(actions: &Arc<Mutex<Vec<SourceAction>>>, action: SourceAction) {
    actions
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(action);
}

fn recorded_actions(actions: &Arc<Mutex<Vec<SourceAction>>>) -> Vec<SourceAction> {
    actions
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

enum ScriptEvent {
    Data {
        segments: Vec<&'static [u8]>,
        terminal: bool,
    },
    OwnedData {
        segments: Vec<Bytes>,
        terminal: bool,
    },
    Trailers(Vec<(&'static [u8], &'static [u8])>),
    Error(HttpErrorCode),
}

struct ScriptedSource {
    events: VecDeque<ScriptEvent>,
    actions: Arc<Mutex<Vec<SourceAction>>>,
    _local: Rc<()>,
}

impl ScriptedSource {
    fn new(events: Vec<ScriptEvent>, actions: Arc<Mutex<Vec<SourceAction>>>) -> Self {
        Self {
            events: events.into(),
            actions,
            _local: Rc::new(()),
        }
    }
}

impl HttpSource for ScriptedSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(Err(HttpError::with_message(
            HttpErrorCode::INVALID_STATE_TRANSITION,
            b"body-only scripted source",
        ))))
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        record(
            &self.actions,
            SourceAction::Read {
                owner: std::thread::current().id(),
                maximum,
            },
        );
        let event = self.events.pop_front().expect("scripted event exists");
        Box::pin(std::future::ready(match event {
            ScriptEvent::Data { segments, terminal } => {
                let length = segments.iter().map(|segment| segment.len()).sum();
                let mut body = BufQueue::new(length);
                for segment in segments {
                    body.push_bytes(Bytes::from_static(segment))
                        .expect("scripted segment fits its exact limit");
                }
                Ok(HttpBodyEvent::body(body, terminal))
            }
            ScriptEvent::OwnedData { segments, terminal } => {
                let length = segments.iter().map(Bytes::len).sum();
                let mut body = BufQueue::new(length);
                for segment in segments {
                    body.push_bytes(segment)
                        .expect("owned scripted segment fits its exact limit");
                }
                Ok(HttpBodyEvent::body(body, terminal))
            }
            ScriptEvent::Trailers(entries) => {
                let mut trailers = HttpHeaders::new();
                trailers.add_all(
                    entries
                        .into_iter()
                        .map(|(name, value)| (HttpHeaderName::from(name), value)),
                );
                Ok(HttpBodyEvent::trailers(trailers))
            }
            ScriptEvent::Error(code) => Err(HttpError::new(code)),
        }))
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        record(
            &self.actions,
            SourceAction::Stop {
                owner: std::thread::current().id(),
                error,
            },
        );
    }

    fn set_read_timeout(&mut self, _timeout: Duration) {}
}

impl Drop for ScriptedSource {
    fn drop(&mut self) {
        record(
            &self.actions,
            SourceAction::Drop(std::thread::current().id()),
        );
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
}

fn scripted_body(
    worker: &ManagedWorker,
    events: Vec<ScriptEvent>,
    maximum_read: u32,
    hint: SizeHint,
    actions: &Arc<Mutex<Vec<SourceAction>>>,
) -> RoxygenBody {
    let source_actions = Arc::clone(actions);
    RoxygenBody::with_size_hint(
        &worker.handle(),
        1,
        NonZeroU32::new(maximum_read).expect("test read limit is nonzero"),
        hint,
        move |_| Box::new(ScriptedSource::new(events, source_actions)),
    )
    .expect("body source starts")
}

fn next_frame(body: &mut RoxygenBody) -> Option<Result<Frame<Bytes>, BoxError>> {
    runtime().block_on(body.frame())
}

fn frame_data(frame: Frame<Bytes>) -> Bytes {
    frame.into_data().expect("expected data frame")
}

mod hyper_body {
    use super::*;

    #[test]
    fn borrowed_bytes_copy_explicitly() {
        let backing = Bytes::from_static(b"xxpayloadyy");
        let visible = backing.slice(2..9);
        let input_pointer = visible.as_ptr();
        let admitted = admit_untrusted_bytes(visible, 7).expect("visible bytes fit");
        assert_eq!(admitted.chain_length(), 7);
        assert_ne!(
            admitted.front().expect("one admitted segment").as_ptr(),
            input_pointer,
            "public Bytes must be copied into exact backing",
        );
        assert!(matches!(
            admit_untrusted_bytes(Bytes::from_static(b"too large"), 3),
            Err(BufQueueError::LimitExceeded { .. }),
        ));
    }

    #[test]
    fn owned_bytes_preserve_backing() {
        let allocation = Bytes::from(vec![b'x', b'x', b'd', b'a', b't', b'a', b'y', b'y']);
        let visible = allocation.slice(2..6);
        let input_pointer = visible.as_ptr();
        let admitted = admit_owned_bytes(visible.clone(), visible.len())
            .expect("owned visible bytes fit their exact limit");
        assert_eq!(
            admitted.front().expect("one admitted segment").as_ptr(),
            input_pointer,
        );

        let worker = ManagedWorker::new(4).expect("managed worker starts");
        let actions = Arc::new(Mutex::new(Vec::new()));
        let mut body = scripted_body(
            &worker,
            vec![ScriptEvent::OwnedData {
                segments: vec![visible],
                terminal: true,
            }],
            4,
            SizeHint::with_exact(4),
            &actions,
        );
        let frame = frame_data(next_frame(&mut body).unwrap().unwrap());
        assert_eq!(frame.as_ptr(), input_pointer);
        assert_eq!(frame.as_ref(), b"data");

        drop(body);
        worker.shutdown().expect("managed worker shuts down");
    }

    #[test]
    fn bounded_bytes_stream_and_cancel() {
        let worker = ManagedWorker::new(4).expect("managed worker starts");
        let owner = worker.owner_thread();
        let actions = Arc::new(Mutex::new(Vec::new()));
        let mut body = scripted_body(
            &worker,
            vec![
                ScriptEvent::Data {
                    segments: vec![b"one"],
                    terminal: false,
                },
                ScriptEvent::Data {
                    segments: vec![b"not observed"],
                    terminal: true,
                },
            ],
            4,
            SizeHint::new(),
            &actions,
        );

        assert_eq!(
            frame_data(next_frame(&mut body).unwrap().unwrap()).as_ref(),
            b"one",
        );
        assert!(
            body.abort_handle()
                .stop_reading(Some(HttpError::new(HttpErrorCode::CORO_CANCELLED,)))
        );
        let error = next_frame(&mut body).unwrap().unwrap_err();
        assert!(matches!(
            error.downcast_ref::<RemoteSourceError<HttpError>>(),
            Some(RemoteSourceError::Stopped),
        ));
        assert!(body.is_end_stream());
        assert!(next_frame(&mut body).is_none());

        drop(body);
        worker.shutdown().expect("managed worker shuts down");
        assert_eq!(
            recorded_actions(&actions),
            vec![
                SourceAction::Read { owner, maximum: 4 },
                SourceAction::Stop {
                    owner,
                    error: Some(HttpErrorCode::CORO_CANCELLED),
                },
                SourceAction::Drop(owner),
            ],
        );
    }

    async fn buck2_shaped_client(
        request: Request<Bytes>,
        worker: &ManagedWorker,
        actions: Arc<Mutex<Vec<SourceAction>>>,
    ) -> Result<Response<RoxygenBody>, BufQueueError> {
        let request_body = request.into_body();
        let admitted = admit_owned_bytes(request_body, 32)?;
        let response_text: &'static [u8] = if admitted.chain_length() == 4 {
            &b"pong"[..]
        } else {
            &b"bad request"[..]
        };
        let body = scripted_body(
            worker,
            vec![ScriptEvent::Data {
                segments: vec![response_text],
                terminal: true,
            }],
            8,
            SizeHint::with_exact(response_text.len() as u64),
            &actions,
        );
        Ok(Response::new(body))
    }

    #[test]
    fn buck2_future_shaped_client_round_trip() {
        fn assert_send<T: Send>() {}
        assert_send::<RoxygenBody>();

        let worker = ManagedWorker::new(4).expect("managed worker starts");
        let actions = Arc::new(Mutex::new(Vec::new()));
        let mut response = runtime()
            .block_on(buck2_shaped_client(
                Request::new(Bytes::from_static(b"ping")),
                &worker,
                Arc::clone(&actions),
            ))
            .expect("request body fits");
        assert_eq!(
            frame_data(next_frame(response.body_mut()).unwrap().unwrap()).as_ref(),
            b"pong",
        );
        assert!(response.body().is_end_stream());

        drop(response);
        worker.shutdown().expect("managed worker shuts down");
    }

    struct PendingSseSource {
        emitted: bool,
        started: Option<mpsc::Sender<ThreadId>>,
        release: Option<tokio::sync::oneshot::Receiver<()>>,
        actions: Arc<Mutex<Vec<SourceAction>>>,
        _local: Rc<()>,
    }

    impl HttpSource for PendingSseSource {
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            Box::pin(std::future::pending())
        }

        fn read_body_event(
            &mut self,
            maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            let owner = std::thread::current().id();
            record(&self.actions, SourceAction::Read { owner, maximum });
            if !self.emitted {
                self.emitted = true;
                let mut event = BufQueue::new(8);
                event
                    .push_bytes(Bytes::from_static(b"data: 1\n"))
                    .expect("SSE event fits");
                return Box::pin(std::future::ready(Ok(HttpBodyEvent::body(event, false))));
            }

            let started = self.started.take().expect("pending read starts once");
            let release = self.release.take().expect("pending read owns barrier");
            Box::pin(async move {
                started.send(owner).expect("report pending read");
                release.await.expect("test releases pending read");
                Ok(HttpBodyEvent::body(BufQueue::new(0), true))
            })
        }

        fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
            record(
                &self.actions,
                SourceAction::Stop {
                    owner: std::thread::current().id(),
                    error,
                },
            );
        }

        fn set_read_timeout(&mut self, _timeout: Duration) {}
    }

    impl Drop for PendingSseSource {
        fn drop(&mut self) {
            record(
                &self.actions,
                SourceAction::Drop(std::thread::current().id()),
            );
        }
    }

    #[test]
    fn sse_backpressure_cancel_and_terminal_drop() {
        let worker = ManagedWorker::new(4).expect("managed worker starts");
        let owner = worker.owner_thread();
        let actions = Arc::new(Mutex::new(Vec::new()));
        let source_actions = Arc::clone(&actions);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let mut body = RoxygenBody::new(
            &worker.handle(),
            1,
            NonZeroU32::new(8).expect("nonzero SSE chunk limit"),
            move |_| {
                Box::new(PendingSseSource {
                    emitted: false,
                    started: Some(started_tx),
                    release: Some(release_rx),
                    actions: source_actions,
                    _local: Rc::new(()),
                })
            },
        )
        .expect("SSE body starts");
        assert_eq!(
            frame_data(next_frame(&mut body).unwrap().unwrap()).as_ref(),
            b"data: 1\n",
        );

        let mut pending_frame = Box::pin(body.frame());
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);
        assert!(matches!(
            pending_frame.as_mut().poll(&mut context),
            Poll::Pending,
        ));
        assert_eq!(started_rx.recv().expect("pending read started"), owner);
        assert_eq!(
            recorded_actions(&actions)
                .iter()
                .filter(|action| matches!(action, SourceAction::Read { .. }))
                .count(),
            2,
            "the adapter admits only the requested read and does not prefetch",
        );
        drop(pending_frame);
        drop(body);
        worker.shutdown().expect("managed worker shuts down");
        assert!(release_tx.send(()).is_err(), "pending read was cancelled");
        assert_eq!(
            recorded_actions(&actions),
            [
                SourceAction::Read { owner, maximum: 8 },
                SourceAction::Read { owner, maximum: 8 },
                SourceAction::Stop { owner, error: None },
                SourceAction::Drop(owner),
            ],
            "pending-read drop must stop without an explicit error before owner-local destruction",
        );

        let worker = ManagedWorker::new(4).expect("managed worker starts");
        let owner = worker.owner_thread();
        let actions = Arc::new(Mutex::new(Vec::new()));
        let mut body = scripted_body(
            &worker,
            vec![ScriptEvent::Data {
                segments: vec![b"data: done\n"],
                terminal: true,
            }],
            16,
            SizeHint::new(),
            &actions,
        );
        assert_eq!(
            frame_data(next_frame(&mut body).unwrap().unwrap()).as_ref(),
            b"data: done\n",
        );
        assert!(body.is_end_stream());
        drop(body);
        worker.shutdown().expect("managed worker shuts down");
        assert_eq!(
            recorded_actions(&actions).last(),
            Some(&SourceAction::Drop(owner)),
        );
        assert!(
            !recorded_actions(&actions)
                .iter()
                .any(|action| matches!(action, SourceAction::Stop { .. })),
            "terminal completion must not be cancelled during body drop",
        );
    }
}

mod http_body {
    use super::*;

    #[test]
    fn ordered_data_trailers_error_and_terminal_frames() {
        let worker = ManagedWorker::new(4).expect("managed worker starts");
        let actions = Arc::new(Mutex::new(Vec::new()));
        let mut body = scripted_body(
            &worker,
            vec![
                ScriptEvent::Data {
                    segments: vec![b"alpha", b"beta"],
                    terminal: false,
                },
                ScriptEvent::Trailers(vec![(b"x-result", b"complete")]),
            ],
            16,
            SizeHint::with_exact(9),
            &actions,
        );

        assert_eq!(
            frame_data(next_frame(&mut body).unwrap().unwrap()).as_ref(),
            b"alpha",
        );
        assert_eq!(
            frame_data(next_frame(&mut body).unwrap().unwrap()).as_ref(),
            b"beta",
        );
        let trailers = next_frame(&mut body)
            .unwrap()
            .unwrap()
            .into_trailers()
            .expect("expected trailer frame");
        assert_eq!(trailers.get("x-result").unwrap(), "complete");
        assert!(next_frame(&mut body).is_none());
        drop(body);
        worker.shutdown().expect("managed worker shuts down");

        let worker = ManagedWorker::new(4).expect("managed worker starts");
        let actions = Arc::new(Mutex::new(Vec::new()));
        let mut body = scripted_body(
            &worker,
            vec![
                ScriptEvent::Data {
                    segments: vec![b"before-error"],
                    terminal: false,
                },
                ScriptEvent::Error(HttpErrorCode::TRANSPORT_READ_ERROR),
                ScriptEvent::Data {
                    segments: vec![b"after-error"],
                    terminal: true,
                },
            ],
            16,
            SizeHint::new(),
            &actions,
        );
        assert_eq!(
            frame_data(next_frame(&mut body).unwrap().unwrap()).as_ref(),
            b"before-error",
        );
        let error = next_frame(&mut body).unwrap().unwrap_err();
        let remote = error
            .downcast_ref::<RemoteSourceError<HttpError>>()
            .expect("source error retains its remote-source type");
        assert!(matches!(
            remote,
            RemoteSourceError::Source(error)
                if error.code() == HttpErrorCode::TRANSPORT_READ_ERROR
        ));
        assert!(next_frame(&mut body).is_none());
        assert_eq!(
            recorded_actions(&actions)
                .iter()
                .filter(|action| matches!(action, SourceAction::Read { .. }))
                .count(),
            2,
            "no event is read after the first source error",
        );
        drop(body);
        worker.shutdown().expect("managed worker shuts down");
    }

    #[test]
    fn end_stream_tracks_terminal_state() {
        let worker = ManagedWorker::new(4).expect("managed worker starts");
        let actions = Arc::new(Mutex::new(Vec::new()));
        let mut data_body = scripted_body(
            &worker,
            vec![ScriptEvent::Data {
                segments: vec![b"last"],
                terminal: true,
            }],
            8,
            SizeHint::with_exact(4),
            &actions,
        );
        assert!(!data_body.is_end_stream());
        assert_eq!(
            frame_data(next_frame(&mut data_body).unwrap().unwrap()).as_ref(),
            b"last",
        );
        assert!(data_body.is_end_stream());
        assert!(next_frame(&mut data_body).is_none());
        drop(data_body);
        worker.shutdown().expect("managed worker shuts down");

        let worker = ManagedWorker::new(4).expect("managed worker starts");
        let actions = Arc::new(Mutex::new(Vec::new()));
        let mut trailer_body = scripted_body(
            &worker,
            vec![ScriptEvent::Trailers(vec![(b"x-end", b"yes")])],
            8,
            SizeHint::with_exact(0),
            &actions,
        );
        assert!(!trailer_body.is_end_stream());
        assert!(
            next_frame(&mut trailer_body)
                .unwrap()
                .unwrap()
                .is_trailers()
        );
        assert!(trailer_body.is_end_stream());
        drop(trailer_body);
        worker.shutdown().expect("managed worker shuts down");

        let worker = ManagedWorker::new(4).expect("managed worker starts");
        let actions = Arc::new(Mutex::new(Vec::new()));
        let mut error_body = scripted_body(
            &worker,
            vec![ScriptEvent::Error(HttpErrorCode::READ_TIMEOUT)],
            8,
            SizeHint::new(),
            &actions,
        );
        assert!(!error_body.is_end_stream());
        assert!(next_frame(&mut error_body).unwrap().is_err());
        assert!(error_body.is_end_stream());
        assert!(next_frame(&mut error_body).is_none());
        drop(error_body);
        worker.shutdown().expect("managed worker shuts down");
    }

    #[test]
    fn size_hint_bounds_track_remaining_data() {
        let worker = ManagedWorker::new(4).expect("managed worker starts");
        let actions = Arc::new(Mutex::new(Vec::new()));
        let mut body = scripted_body(
            &worker,
            vec![ScriptEvent::Data {
                segments: vec![b"ab", b"cde"],
                terminal: true,
            }],
            8,
            SizeHint::with_exact(5),
            &actions,
        );
        assert_eq!(body.size_hint().exact(), Some(5));
        assert!(!body.is_end_stream());
        assert_eq!(
            frame_data(next_frame(&mut body).unwrap().unwrap()).as_ref(),
            b"ab",
        );
        assert_eq!(body.size_hint().exact(), Some(3));
        assert!(
            !body.is_end_stream(),
            "a terminal multi-segment event remains observable until its last frame",
        );
        assert_eq!(
            frame_data(next_frame(&mut body).unwrap().unwrap()).as_ref(),
            b"cde",
        );
        assert_eq!(body.size_hint().exact(), Some(0));
        assert!(body.is_end_stream());
        drop(body);
        worker.shutdown().expect("managed worker shuts down");

        let worker = ManagedWorker::new(4).expect("managed worker starts");
        let actions = Arc::new(Mutex::new(Vec::new()));
        let mut hint = SizeHint::new();
        hint.set_lower(2);
        hint.set_upper(5);
        let mut body = scripted_body(
            &worker,
            vec![
                ScriptEvent::Data {
                    segments: vec![b"abc"],
                    terminal: false,
                },
                ScriptEvent::Trailers(vec![]),
            ],
            8,
            hint,
            &actions,
        );
        assert_eq!(
            (body.size_hint().lower(), body.size_hint().upper()),
            (2, Some(5)),
        );
        assert_eq!(
            frame_data(next_frame(&mut body).unwrap().unwrap()).as_ref(),
            b"abc",
        );
        assert_eq!(
            (body.size_hint().lower(), body.size_hint().upper()),
            (0, Some(2)),
        );
        assert!(next_frame(&mut body).unwrap().unwrap().is_trailers());
        assert_eq!(
            (body.size_hint().lower(), body.size_hint().upper()),
            (0, Some(0)),
        );
        drop(body);
        worker.shutdown().expect("managed worker shuts down");
    }
}
use std::cell::Cell;
use std::cell::RefCell;
