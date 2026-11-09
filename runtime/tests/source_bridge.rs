use std::cell::Cell;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomPinned;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::panic_any;
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
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Waker;
use std::thread::ThreadId;
use std::time::Duration;

use futures::task::noop_waker;
use thiserror::Error;
use tokio::runtime::Builder;
use tokio::sync::oneshot;

use crate::driver::WorkerDriver;
use crate::managed::RuntimeConfig;
use crate::managed::RuntimeOwner;
use crate::source_proxy::LocalBodyRead;
use crate::source_proxy::LocalHttpSource;
use crate::source_proxy::LocalSourceFuture;
use crate::source_proxy::RemoteHttpSource;
use crate::source_proxy::RemoteSourceCreateError;
use crate::source_proxy::RemoteSourceError;
use crate::source_proxy::RemoteSourceEvent;
use crate::source_proxy::SourceAbortHandle;
use crate::source_proxy::SourceCleanupObserver;
use crate::source_proxy::SourceControlError;
use crate::tokio_host::ManagedWorker;
use crate::worker::CleanupRegistrationError;
use crate::worker::WorkerCleanupObservationError;
use crate::worker::WorkerCleanupObserver;
use crate::worker::WorkerContext;
use crate::worker::WorkerHandle;
use crate::worker::WorkerState;

thread_local! {
    static JOINED_SOURCE_CONTEXT: RefCell<Option<WorkerContext>> = const { RefCell::new(None) };
}

fn source_test_driver() -> (WorkerHandle, Pin<Box<WorkerDriver>>, WorkerContext) {
    let (handle, driver) = WorkerDriver::new(2).expect("driver starts");
    let mut driver = Box::pin(driver);
    let mut call = Box::pin(
        handle
            .call(|context| JOINED_SOURCE_CONTEXT.set(Some(context.clone())))
            .expect("context capture admitted"),
    );
    assert!(poll_once(driver.as_mut()).is_pending());
    assert!(matches!(poll_once(call.as_mut()), Poll::Ready(Ok(()))));
    let context = JOINED_SOURCE_CONTEXT.take().expect("actual driver context");
    (handle, driver, context)
}

fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(&noop_waker()))
}

fn source_retirement(
    observer: &SourceCleanupObserver,
) -> Poll<Result<(), WorkerCleanupObservationError>> {
    poll_once(Box::pin(observer.wait()).as_mut())
}

#[derive(Clone)]
struct OwnedDropProbe {
    owner: ThreadId,
    polls: Rc<Cell<usize>>,
    drops: Rc<Cell<usize>>,
    unwinding: Rc<Cell<bool>>,
    observation: Rc<RefCell<Option<SourceCleanupObserver>>>,
}

impl OwnedDropProbe {
    fn new() -> Self {
        Self {
            owner: std::thread::current().id(),
            polls: Rc::new(Cell::new(0)),
            drops: Rc::new(Cell::new(0)),
            unwinding: Rc::new(Cell::new(false)),
            observation: Rc::new(RefCell::new(None)),
        }
    }

    fn dropped(&self) {
        assert_eq!(std::thread::current().id(), self.owner);
        if let Some(observer) = self.observation.borrow().as_ref() {
            assert!(source_retirement(observer).is_pending());
        }
        self.unwinding.set(std::thread::panicking());
        self.drops.set(self.drops.get() + 1);
    }
}

#[derive(Clone, Copy)]
enum FutureDisposition {
    Ready,
    Pending,
    PollPanic,
}

#[derive(Clone, Copy)]
enum DropFailure {
    None,
    Message(&'static str),
    NestedPayload,
}

struct SecondaryDropPayload;

impl Drop for SecondaryDropPayload {
    fn drop(&mut self) {
        panic!("secondary payload drop");
    }
}

impl DropFailure {
    fn trigger(self) {
        match self {
            Self::None => {}
            Self::Message(message) => panic_any(message),
            Self::NestedPayload => panic_any(SecondaryDropPayload),
        }
    }
}

struct OwnedProbeFuture<T> {
    output: Option<T>,
    disposition: FutureDisposition,
    failure: DropFailure,
    probe: OwnedDropProbe,
}

impl<T: Unpin> Future for OwnedProbeFuture<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<T> {
        assert_eq!(std::thread::current().id(), self.probe.owner);
        self.probe.polls.set(self.probe.polls.get() + 1);
        match self.disposition {
            FutureDisposition::Ready => Poll::Ready(self.output.take().expect("one ready result")),
            FutureDisposition::Pending => Poll::Pending,
            FutureDisposition::PollPanic => panic!("owned future poll"),
        }
    }
}

impl<T> Drop for OwnedProbeFuture<T> {
    fn drop(&mut self) {
        self.probe.dropped();
        self.failure.trigger();
    }
}

#[derive(Clone, Copy)]
enum OwnedFuturePhase {
    Header,
    Body,
    Resume,
}

struct OwnedFaultSource {
    header: Option<LocalSourceFuture<Result<TestHeader, TestError>>>,
    body: Option<LocalSourceFuture<Result<LocalBodyRead<TestBody>, TestError>>>,
    probe: OwnedDropProbe,
    stops: Rc<Cell<usize>>,
    source_failure: DropFailure,
    stop_failure: DropFailure,
}

impl OwnedFaultSource {
    fn empty() -> Self {
        Self {
            header: None,
            body: None,
            probe: OwnedDropProbe::new(),
            stops: Rc::new(Cell::new(0)),
            source_failure: DropFailure::None,
            stop_failure: DropFailure::None,
        }
    }

    fn with_future(
        phase: OwnedFuturePhase,
        disposition: FutureDisposition,
        failure: DropFailure,
        probe: OwnedDropProbe,
    ) -> Self {
        let mut source = Self::empty();
        source.probe.observation = Rc::clone(&probe.observation);
        match phase {
            OwnedFuturePhase::Header => {
                source.header = Some(Box::pin(OwnedProbeFuture {
                    output: Some(Ok(TestHeader {
                        status: 200,
                        terminal: true,
                    })),
                    disposition,
                    failure,
                    probe,
                }))
            }
            OwnedFuturePhase::Body => {
                source.body = Some(Box::pin(OwnedProbeFuture {
                    output: Some(Ok(LocalBodyRead::Event(TestBody {
                        kind: TestBodyKind::Body(1),
                        terminal: true,
                    }))),
                    disposition,
                    failure,
                    probe,
                }))
            }
            OwnedFuturePhase::Resume => {
                let resume = OwnedProbeFuture {
                    output: Some(()),
                    disposition,
                    failure,
                    probe,
                };
                source.body = Some(Box::pin(std::future::ready(Ok(LocalBodyRead::Suspend(
                    Box::pin(resume),
                )))));
            }
        }
        source
    }
}

impl LocalHttpSource for OwnedFaultSource {
    type HeaderEvent = TestHeader;
    type BodyEvent = TestBody;
    type Error = TestError;

    fn read_header_event(&mut self) -> LocalSourceFuture<Result<TestHeader, TestError>> {
        self.header.take().expect("one supplied header future")
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<LocalBodyRead<TestBody>, TestError>> {
        self.body
            .take()
            .expect("no read after failed body or resume")
    }

    fn stop_reading(&mut self, _error: Option<TestError>) {
        assert_eq!(std::thread::current().id(), self.probe.owner);
        self.stops.set(self.stops.get() + 1);
        self.stop_failure.trigger();
    }

    fn set_read_timeout(&mut self, _timeout: Duration) {}
}

impl Drop for OwnedFaultSource {
    fn drop(&mut self) {
        self.probe.dropped();
        self.source_failure.trigger();
    }
}

struct IndependentSourceCleanup {
    released: Rc<Cell<bool>>,
    dropped: Rc<Cell<usize>>,
    first_poll: bool,
    arm_clone_panic: bool,
}

impl Future for IndependentSourceCleanup {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<()> {
        if self.first_poll {
            self.first_poll = false;
            if self.arm_clone_panic {
                SOURCE_STOP_CLONE_ARMED.set(true);
            }
        }
        if self.released.get() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Drop for IndependentSourceCleanup {
    fn drop(&mut self) {
        self.dropped.set(self.dropped.get() + 1);
    }
}

fn independent_source_cleanup(
    context: &WorkerContext,
    arm_clone_panic: bool,
) -> (Rc<Cell<bool>>, Rc<Cell<usize>>, WorkerCleanupObserver) {
    let released = Rc::new(Cell::new(false));
    let dropped = Rc::new(Cell::new(0));
    let scope = context
        .register_scope(IndependentSourceCleanup {
            released: Rc::clone(&released),
            dropped: Rc::clone(&dropped),
            first_poll: true,
            arm_clone_panic,
        })
        .unwrap();
    let observer = scope.observe();
    drop(scope.activate().unwrap());
    (released, dropped, observer)
}

thread_local! {
    static SOURCE_STOP_CLONE_ARMED: Cell<bool> = const { Cell::new(false) };
    static SOURCE_STOP_CLONE_HITS: Cell<usize> = const { Cell::new(0) };
}

fn source_stop_clone_waker() -> Waker {
    fn clone(_data: *const ()) -> RawWaker {
        if SOURCE_STOP_CLONE_ARMED.replace(false) {
            SOURCE_STOP_CLONE_HITS.set(SOURCE_STOP_CLONE_HITS.get() + 1);
            panic!("stop receiver clone");
        }
        raw()
    }
    fn noop(_data: *const ()) {}
    fn raw() -> RawWaker {
        RawWaker::new(
            std::ptr::null(),
            &RawWakerVTable::new(clone, noop, noop, noop),
        )
    }
    unsafe { Waker::from_raw(raw()) }
}

fn owned_fault_read(
    phase: OwnedFuturePhase,
    remote: &mut RemoteHttpSource<TestHeader, TestBody, TestError>,
) -> Pin<Box<dyn Future<Output = ()> + '_>> {
    Box::pin(async move {
        match phase {
            OwnedFuturePhase::Header => {
                let _ = remote.read_header_event().await;
            }
            OwnedFuturePhase::Body | OwnedFuturePhase::Resume => {
                let _ = remote.read_body_event(1).await;
            }
        }
    })
}

fn assert_source_failure_closed(
    handle: &WorkerHandle,
    mut driver: Pin<&mut WorkerDriver>,
    release: &Cell<bool>,
    independent_drops: &Cell<usize>,
    independent_observer: &WorkerCleanupObserver,
    expected_panic: &'static str,
) {
    assert_eq!(handle.state(), WorkerState::Closing);
    assert_eq!(independent_drops.get(), 0);
    assert!(poll_once(Box::pin(independent_observer.wait()).as_mut()).is_pending());
    release.set(true);
    let failure = catch_unwind(AssertUnwindSafe(|| poll_once(driver.as_mut())))
        .expect_err("original failure propagates after all cleanup");
    assert_eq!(failure.downcast_ref::<&str>(), Some(&expected_panic));
    assert_eq!(handle.state(), WorkerState::Closed);
    assert_eq!(handle.quiesce_snapshot(&noop_waker()).1, 0);
    assert_eq!(independent_drops.get(), 1);
    assert_eq!(
        poll_once(Box::pin(independent_observer.wait()).as_mut()),
        Poll::Ready(Ok(()))
    );
}

#[test]
fn joined_source_cancel_poll_drop_panic_is_unconfirmed() {
    for disposition in [
        None,
        Some(FutureDisposition::Ready),
        Some(FutureDisposition::Pending),
    ] {
        let (handle, mut driver, context) = source_test_driver();
        let (release, independent_drops, independent_observer) =
            independent_source_cleanup(&context, false);
        let future_probe = OwnedDropProbe::new();
        let mut source = match disposition {
            Some(disposition) => OwnedFaultSource::with_future(
                OwnedFuturePhase::Body,
                disposition,
                DropFailure::None,
                future_probe.clone(),
            ),
            None => OwnedFaultSource::empty(),
        };
        source.source_failure = DropFailure::Message("owned source drop");
        let probe = source.probe.clone();
        let stops = Rc::clone(&source.stops);
        let (attachment, mut remote, observer) =
            RemoteHttpSource::prepare_local::<OwnedFaultSource>(&context, 1).unwrap();
        *probe.observation.borrow_mut() = Some(observer.clone());
        attachment.attach(source).unwrap();
        let abort = remote.abort_handle();
        let mut read = owned_fault_read(OwnedFuturePhase::Body, &mut remote);
        if disposition.is_some() {
            assert!(poll_once(read.as_mut()).is_pending());
        } else {
            assert!(abort.stop_reading(None));
        }
        assert!(poll_once(driver.as_mut()).is_pending());
        if matches!(disposition, Some(FutureDisposition::Pending)) {
            assert_eq!(future_probe.polls.get(), 1);
            assert_eq!(probe.drops.get(), 0);
            assert!(source_retirement(&observer).is_pending());
            assert!(abort.stop_reading(None));
            assert!(poll_once(driver.as_mut()).is_pending());
        }
        assert_eq!(probe.drops.get(), 1);
        assert_eq!(
            stops.get(),
            usize::from(!matches!(disposition, Some(FutureDisposition::Ready)))
        );
        assert_eq!(
            source_retirement(&observer),
            Poll::Ready(Err(WorkerCleanupObservationError::Unconfirmed))
        );
        assert_source_failure_closed(
            &handle,
            driver.as_mut(),
            &release,
            &independent_drops,
            &independent_observer,
            "owned source drop",
        );
        assert_eq!(
            source_retirement(&observer.clone()),
            Poll::Ready(Err(WorkerCleanupObservationError::Unconfirmed))
        );
    }
}

#[test]
fn prepared_source_stop_waker_clone_panic_preserves_first_failure() {
    SOURCE_STOP_CLONE_ARMED.set(false);
    SOURCE_STOP_CLONE_HITS.set(0);
    let (handle, mut driver, context) = source_test_driver();
    let (release, independent_drops, independent_observer) =
        independent_source_cleanup(&context, true);
    let mut source = OwnedFaultSource::empty();
    source.source_failure = DropFailure::NestedPayload;
    let probe = source.probe.clone();
    let stops = Rc::clone(&source.stops);
    let (attachment, remote, observer) =
        RemoteHttpSource::prepare_local::<OwnedFaultSource>(&context, 1).unwrap();
    *probe.observation.borrow_mut() = Some(observer.clone());
    context.set_newest_cleanup_poll_waker(source_stop_clone_waker());
    attachment.attach(source).unwrap();
    assert_eq!(probe.drops.get(), 0);
    assert!(poll_once(driver.as_mut()).is_pending());
    assert_eq!(SOURCE_STOP_CLONE_HITS.get(), 1);
    assert!(!SOURCE_STOP_CLONE_ARMED.get());
    assert!(
        probe.unwinding.get(),
        "queued owner is disposed during clone-panic unwind"
    );
    assert_eq!(probe.drops.get(), 1);
    assert_eq!(stops.get(), 1);
    assert_eq!(
        source_retirement(&observer),
        Poll::Ready(Err(WorkerCleanupObservationError::Unconfirmed))
    );
    assert_source_failure_closed(
        &handle,
        driver.as_mut(),
        &release,
        &independent_drops,
        &independent_observer,
        "stop receiver clone",
    );
    assert_eq!(
        source_retirement(&observer),
        Poll::Ready(Err(WorkerCleanupObservationError::Unconfirmed))
    );
    drop(remote);
}

#[test]
fn prepared_source_late_rejected_drop_does_not_change_receipt() {
    let (handle, mut driver, context) = source_test_driver();
    let (attachment, remote, observer) =
        RemoteHttpSource::prepare_local::<OwnedFaultSource>(&context, 1).unwrap();
    drop(remote);
    assert!(poll_once(driver.as_mut()).is_pending());
    assert_eq!(source_retirement(&observer), Poll::Ready(Ok(())));
    let mut source = OwnedFaultSource::empty();
    source.source_failure = DropFailure::Message("late rejected drop");
    let probe = source.probe.clone();
    let stops = Rc::clone(&source.stops);
    let failure = catch_unwind(AssertUnwindSafe(|| attachment.attach(source)))
        .expect_err("late input disposal still propagates");
    assert_eq!(failure.downcast_ref::<&str>(), Some(&"late rejected drop"));
    assert_eq!(probe.drops.get(), 1);
    assert_eq!(stops.get(), 1);
    assert_eq!(source_retirement(&observer), Poll::Ready(Ok(())));
    assert_eq!(source_retirement(&observer.clone()), Poll::Ready(Ok(())));
    assert_eq!(handle.quiesce_snapshot(&noop_waker()).1, 0);
    drop(driver);
    assert_eq!(handle.state(), WorkerState::Closed);
}

fn check_owned_future_retirement(disposition: FutureDisposition, failure: DropFailure) {
    for phase in [
        OwnedFuturePhase::Header,
        OwnedFuturePhase::Body,
        OwnedFuturePhase::Resume,
    ] {
        let (handle, mut driver, context) = source_test_driver();
        let (release, independent_drops, independent_observer) =
            independent_source_cleanup(&context, false);
        let probe = OwnedDropProbe::new();
        let source = OwnedFaultSource::with_future(phase, disposition, failure, probe.clone());
        let source_probe = source.probe.clone();
        let (mut remote, observer) =
            RemoteHttpSource::try_from_local_with_observer(&context, 1, source).unwrap();
        *probe.observation.borrow_mut() = Some(observer.clone());
        let abort = remote.abort_handle();
        let mut read = owned_fault_read(phase, &mut remote);
        assert!(poll_once(read.as_mut()).is_pending());
        assert!(poll_once(driver.as_mut()).is_pending());
        if matches!(disposition, FutureDisposition::Pending) {
            assert_eq!(
                probe.polls.get(),
                1,
                "read or resume was genuinely polled Pending"
            );
            assert_eq!(probe.drops.get(), 0);
            assert!(source_retirement(&observer).is_pending());
            assert!(abort.stop_reading(None));
            assert!(poll_once(driver.as_mut()).is_pending());
        }
        let (expected, panic) = if matches!(disposition, FutureDisposition::PollPanic) {
            (Ok(()), "owned future poll")
        } else {
            (
                Err(WorkerCleanupObservationError::Unconfirmed),
                "owned future drop",
            )
        };
        assert_eq!(probe.polls.get(), 1);
        assert_eq!(probe.drops.get(), 1);
        assert_eq!(source_probe.drops.get(), 1);
        assert_eq!(source_retirement(&observer), Poll::Ready(expected));
        assert_source_failure_closed(
            &handle,
            driver.as_mut(),
            &release,
            &independent_drops,
            &independent_observer,
            panic,
        );
        assert_eq!(source_retirement(&observer), Poll::Ready(expected));
    }
}

#[test]
fn joined_source_ready_future_drop_panic_is_unconfirmed() {
    check_owned_future_retirement(
        FutureDisposition::Ready,
        DropFailure::Message("owned future drop"),
    );
}

#[test]
fn joined_source_pending_future_drop_panic_is_unconfirmed() {
    check_owned_future_retirement(
        FutureDisposition::Pending,
        DropFailure::Message("owned future drop"),
    );
}

#[test]
fn joined_source_future_poll_panic_with_successful_drop_is_confirmed() {
    check_owned_future_retirement(FutureDisposition::PollPanic, DropFailure::None);
}

#[test]
fn joined_source_future_drop_during_unwind_preserves_first_panic() {
    for phase in [
        OwnedFuturePhase::Header,
        OwnedFuturePhase::Body,
        OwnedFuturePhase::Resume,
    ] {
        let (handle, mut driver, context) = source_test_driver();
        let (release, independent_drops, independent_observer) =
            independent_source_cleanup(&context, false);
        let probe = OwnedDropProbe::new();
        let mut source = OwnedFaultSource::with_future(
            phase,
            FutureDisposition::Pending,
            DropFailure::NestedPayload,
            probe.clone(),
        );
        source.stop_failure = DropFailure::Message("stop callback first");
        let source_probe = source.probe.clone();
        let (mut remote, observer) =
            RemoteHttpSource::try_from_local_with_observer(&context, 1, source).unwrap();
        *probe.observation.borrow_mut() = Some(observer.clone());
        let abort = remote.abort_handle();
        let mut read = owned_fault_read(phase, &mut remote);
        assert!(poll_once(read.as_mut()).is_pending());
        assert!(poll_once(driver.as_mut()).is_pending());
        assert_eq!(probe.polls.get(), 1);
        assert!(abort.stop_reading(None));
        assert!(poll_once(driver.as_mut()).is_pending());
        assert!(probe.unwinding.get());
        assert_eq!(probe.drops.get(), 1);
        assert_eq!(source_probe.drops.get(), 1);
        assert_eq!(
            source_retirement(&observer),
            Poll::Ready(Err(WorkerCleanupObservationError::Unconfirmed))
        );
        assert_source_failure_closed(
            &handle,
            driver.as_mut(),
            &release,
            &independent_drops,
            &independent_observer,
            "stop callback first",
        );
    }
    let (handle, mut driver, context) = source_test_driver();
    let (release, independent_drops, independent_observer) =
        independent_source_cleanup(&context, false);
    let read_probe = OwnedDropProbe::new();
    let mut resume_probe = OwnedDropProbe::new();
    resume_probe.observation = Rc::clone(&read_probe.observation);
    let resume = OwnedProbeFuture {
        output: Some(()),
        disposition: FutureDisposition::Pending,
        failure: DropFailure::NestedPayload,
        probe: resume_probe.clone(),
    };
    let read = OwnedProbeFuture {
        output: Some(Ok(LocalBodyRead::Suspend(Box::pin(resume)))),
        disposition: FutureDisposition::Ready,
        failure: DropFailure::Message("ready read disposal first"),
        probe: read_probe.clone(),
    };
    let mut source = OwnedFaultSource::empty();
    source.body = Some(Box::pin(read));
    source.probe.observation = Rc::clone(&read_probe.observation);
    let (mut remote, observer) =
        RemoteHttpSource::try_from_local_with_observer(&context, 1, source).unwrap();
    *read_probe.observation.borrow_mut() = Some(observer.clone());
    let mut read = owned_fault_read(OwnedFuturePhase::Body, &mut remote);
    assert!(poll_once(read.as_mut()).is_pending());
    assert!(poll_once(driver.as_mut()).is_pending());
    assert_eq!(read_probe.drops.get(), 1);
    assert_eq!(
        resume_probe.polls.get(),
        0,
        "resume guarded before ready-read disposal"
    );
    assert_eq!(resume_probe.drops.get(), 1);
    assert!(resume_probe.unwinding.get());
    assert_eq!(
        source_retirement(&observer),
        Poll::Ready(Err(WorkerCleanupObservationError::Unconfirmed))
    );
    assert_source_failure_closed(
        &handle,
        driver.as_mut(),
        &release,
        &independent_drops,
        &independent_observer,
        "ready read disposal first",
    );
}

#[derive(Clone)]
struct ReplyDropProbe {
    owner: ThreadId,
    drops: Arc<AtomicUsize>,
    expect_owned: Arc<AtomicBool>,
    observation: Arc<Mutex<Option<SourceCleanupObserver>>>,
}

impl ReplyDropProbe {
    fn new() -> Self {
        Self {
            owner: std::thread::current().id(),
            drops: Arc::new(AtomicUsize::new(0)),
            expect_owned: Arc::new(AtomicBool::new(true)),
            observation: Arc::new(Mutex::new(None)),
        }
    }

    fn dropped(&self) {
        if self.expect_owned.load(Ordering::SeqCst) {
            assert_eq!(std::thread::current().id(), self.owner);
            assert!(
                source_retirement(
                    self.observation
                        .lock()
                        .unwrap()
                        .as_ref()
                        .expect("output observed")
                )
                .is_pending()
            );
        }
        assert_eq!(
            self.drops.fetch_add(1, Ordering::SeqCst),
            0,
            "one output disposal"
        );
    }
}

struct ReplyValue {
    probe: ReplyDropProbe,
    failure: DropFailure,
}

impl std::fmt::Debug for ReplyValue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ReplyValue")
    }
}

impl Drop for ReplyValue {
    fn drop(&mut self) {
        self.probe.dropped();
        self.failure.trigger();
    }
}

#[derive(Debug)]
struct ReplyEvent {
    _value: ReplyValue,
}

impl RemoteSourceEvent for ReplyEvent {
    fn is_terminal(&self) -> bool {
        true
    }
}

#[derive(Debug)]
struct ReplyError {
    _value: ReplyValue,
}

impl std::fmt::Display for ReplyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("owned reply error")
    }
}

impl std::error::Error for ReplyError {}

#[derive(Clone, Copy)]
enum ReplyCase {
    Header,
    Body,
    HeaderError,
    BodyError,
}

impl ReplyCase {
    fn header(self) -> bool {
        matches!(self, Self::Header | Self::HeaderError)
    }

    fn error(self) -> bool {
        matches!(self, Self::HeaderError | Self::BodyError)
    }
}

const REPLY_CASES: [ReplyCase; 4] = [
    ReplyCase::Header,
    ReplyCase::Body,
    ReplyCase::HeaderError,
    ReplyCase::BodyError,
];

struct ReplyReadFuture<T> {
    read: OwnedProbeFuture<T>,
    before_drop: Option<Box<dyn FnOnce()>>,
}

impl<T: Unpin> Future for ReplyReadFuture<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<T> {
        Pin::new(&mut self.read).poll(context)
    }
}

impl<T> Drop for ReplyReadFuture<T> {
    fn drop(&mut self) {
        if let Some(callback) = self.before_drop.take() {
            callback();
        }
    }
}

struct ReplySource {
    header: Option<LocalSourceFuture<Result<ReplyEvent, ReplyError>>>,
    body: Option<LocalSourceFuture<Result<LocalBodyRead<ReplyEvent>, ReplyError>>>,
    probe: OwnedDropProbe,
    failure: DropFailure,
}

impl ReplySource {
    fn new(
        case: ReplyCase,
        value: ReplyValue,
        future_probe: OwnedDropProbe,
        future_failure: DropFailure,
        source_failure: DropFailure,
        before_drop: Option<Box<dyn FnOnce()>>,
    ) -> Self {
        let mut source = Self {
            header: None,
            body: None,
            probe: OwnedDropProbe::new(),
            failure: source_failure,
        };
        source.probe.observation = Rc::clone(&future_probe.observation);
        let result = if case.error() {
            Err(ReplyError { _value: value })
        } else {
            Ok(ReplyEvent { _value: value })
        };
        if case.header() {
            source.header = Some(Box::pin(ReplyReadFuture {
                read: OwnedProbeFuture {
                    output: Some(result),
                    disposition: FutureDisposition::Ready,
                    failure: future_failure,
                    probe: future_probe,
                },
                before_drop,
            }));
        } else {
            source.body = Some(Box::pin(ReplyReadFuture {
                read: OwnedProbeFuture {
                    output: Some(result.map(LocalBodyRead::Event)),
                    disposition: FutureDisposition::Ready,
                    failure: future_failure,
                    probe: future_probe,
                },
                before_drop,
            }));
        }
        source
    }
}

impl LocalHttpSource for ReplySource {
    type HeaderEvent = ReplyEvent;
    type BodyEvent = ReplyEvent;
    type Error = ReplyError;

    fn read_header_event(&mut self) -> LocalSourceFuture<Result<ReplyEvent, ReplyError>> {
        self.header.take().expect("one reply header")
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<LocalBodyRead<ReplyEvent>, ReplyError>> {
        self.body.take().expect("one reply body")
    }

    fn stop_reading(&mut self, _error: Option<ReplyError>) {
        assert_eq!(std::thread::current().id(), self.probe.owner);
    }

    fn set_read_timeout(&mut self, _timeout: Duration) {}
}

impl Drop for ReplySource {
    fn drop(&mut self) {
        self.probe.dropped();
        self.failure.trigger();
    }
}

type ReplyReadResult = Result<ReplyEvent, RemoteSourceError<ReplyError>>;

fn read_reply(
    case: ReplyCase,
    remote: &mut RemoteHttpSource<ReplyEvent, ReplyEvent, ReplyError>,
) -> Pin<Box<dyn Future<Output = ReplyReadResult> + '_>> {
    Box::pin(async move {
        if case.header() {
            remote.read_header_event().await
        } else {
            remote.read_body_event(1).await
        }
    })
}

fn check_ready_reply_unwind(future_boundary: bool) {
    for case in REPLY_CASES {
        let (handle, mut driver, context) = source_test_driver();
        let (release, independent_drops, independent_observer) =
            independent_source_cleanup(&context, false);
        let output_probe = ReplyDropProbe::new();
        let future_probe = OwnedDropProbe::new();
        let first = if future_boundary {
            "ready future first"
        } else {
            "terminal source first"
        };
        let source = ReplySource::new(
            case,
            ReplyValue {
                probe: output_probe.clone(),
                failure: DropFailure::NestedPayload,
            },
            future_probe.clone(),
            if future_boundary {
                DropFailure::Message(first)
            } else {
                DropFailure::None
            },
            if future_boundary {
                DropFailure::None
            } else {
                DropFailure::Message(first)
            },
            None,
        );
        let source_probe = source.probe.clone();
        let (mut remote, observer) =
            RemoteHttpSource::try_from_local_with_observer(&context, 1, source).unwrap();
        *output_probe.observation.lock().unwrap() = Some(observer.clone());
        *future_probe.observation.borrow_mut() = Some(observer.clone());
        let mut read = read_reply(case, &mut remote);
        assert!(poll_once(read.as_mut()).is_pending());
        assert!(poll_once(driver.as_mut()).is_pending());
        assert_eq!(future_probe.polls.get(), 1);
        assert_eq!(future_probe.drops.get(), 1);
        assert_eq!(source_probe.drops.get(), 1);
        assert_eq!(output_probe.drops.load(Ordering::SeqCst), 1);
        assert_eq!(
            source_retirement(&observer),
            Poll::Ready(Err(WorkerCleanupObservationError::Unconfirmed))
        );
        assert_source_failure_closed(
            &handle,
            driver.as_mut(),
            &release,
            &independent_drops,
            &independent_observer,
            first,
        );
        assert_eq!(
            source_retirement(&observer),
            Poll::Ready(Err(WorkerCleanupObservationError::Unconfirmed))
        );
    }
}

#[test]
fn joined_source_ready_output_drop_preserves_future_panic() {
    check_ready_reply_unwind(true);
}

#[test]
fn joined_source_ready_output_drop_preserves_source_panic() {
    check_ready_reply_unwind(false);
}

#[test]
fn joined_source_rejected_reply_drop_is_guarded() {
    for case in REPLY_CASES {
        let (handle, mut driver, context) = source_test_driver();
        let (release, independent_drops, independent_observer) =
            independent_source_cleanup(&context, false);
        let output_probe = ReplyDropProbe::new();
        let future_probe = OwnedDropProbe::new();
        let source = ReplySource::new(
            case,
            ReplyValue {
                probe: output_probe.clone(),
                failure: DropFailure::Message("rejected output drop"),
            },
            future_probe.clone(),
            DropFailure::None,
            DropFailure::None,
            None,
        );
        let source_probe = source.probe.clone();
        let (mut remote, observer) =
            RemoteHttpSource::try_from_local_with_observer(&context, 1, source).unwrap();
        *output_probe.observation.lock().unwrap() = Some(observer.clone());
        *future_probe.observation.borrow_mut() = Some(observer.clone());
        let mut read = read_reply(case, &mut remote);
        assert!(poll_once(read.as_mut()).is_pending());
        drop(read);
        assert!(poll_once(driver.as_mut()).is_pending());
        assert_eq!(source_probe.drops.get(), 1);
        assert_eq!(future_probe.drops.get(), 1);
        assert_eq!(output_probe.drops.load(Ordering::SeqCst), 1);
        assert_eq!(
            source_retirement(&observer),
            Poll::Ready(Err(WorkerCleanupObservationError::Unconfirmed))
        );
        assert_source_failure_closed(
            &handle,
            driver.as_mut(),
            &release,
            &independent_drops,
            &independent_observer,
            "rejected output drop",
        );
    }
}

#[test]
fn joined_source_cancelled_output_drop_is_guarded() {
    for case in REPLY_CASES {
        for forced in [false, true] {
            let (handle, mut driver, context) = source_test_driver();
            let (release, independent_drops, independent_observer) =
                independent_source_cleanup(&context, false);
            let output_probe = ReplyDropProbe::new();
            let future_probe = OwnedDropProbe::new();
            let (attachment, mut remote, observer) =
                RemoteHttpSource::prepare_local::<ReplySource>(&context, 1).unwrap();
            let abort = remote.abort_handle();
            let shutdown = handle.clone();
            let cancelled = Rc::new(Cell::new(false));
            let cancellation = Rc::clone(&cancelled);
            let source = ReplySource::new(
                case,
                ReplyValue {
                    probe: output_probe.clone(),
                    failure: DropFailure::Message("cancelled output drop"),
                },
                future_probe.clone(),
                DropFailure::None,
                DropFailure::None,
                Some(Box::new(move || {
                    cancellation.set(abort.stop_reading(None));
                    if forced {
                        shutdown.begin_shutdown();
                    }
                })),
            );
            let source_probe = source.probe.clone();
            *output_probe.observation.lock().unwrap() = Some(observer.clone());
            *future_probe.observation.borrow_mut() = Some(observer.clone());
            attachment.attach(source).unwrap();
            let mut read = read_reply(case, &mut remote);
            assert!(poll_once(read.as_mut()).is_pending());
            assert!(poll_once(driver.as_mut()).is_pending());
            assert!(
                cancelled.get(),
                "cancellation committed after producing output"
            );
            assert_eq!(source_probe.drops.get(), 1);
            assert_eq!(future_probe.drops.get(), 1);
            assert_eq!(output_probe.drops.load(Ordering::SeqCst), 1);
            assert_eq!(
                source_retirement(&observer),
                Poll::Ready(Err(WorkerCleanupObservationError::Unconfirmed))
            );
            assert_source_failure_closed(
                &handle,
                driver.as_mut(),
                &release,
                &independent_drops,
                &independent_observer,
                "cancelled output drop",
            );
        }
    }
}

#[test]
fn joined_source_transferred_output_drop_keeps_receipt_sticky() {
    for case in REPLY_CASES {
        for consume in [false, true] {
            let (handle, mut driver, context) = source_test_driver();
            let output_probe = ReplyDropProbe::new();
            let future_probe = OwnedDropProbe::new();
            let source = ReplySource::new(
                case,
                ReplyValue {
                    probe: output_probe.clone(),
                    failure: DropFailure::Message("transferred output drop"),
                },
                future_probe.clone(),
                DropFailure::None,
                DropFailure::None,
                None,
            );
            let source_probe = source.probe.clone();
            let (mut remote, observer) =
                RemoteHttpSource::try_from_local_with_observer(&context, 1, source).unwrap();
            *output_probe.observation.lock().unwrap() = Some(observer.clone());
            *future_probe.observation.borrow_mut() = Some(observer.clone());
            let mut read = read_reply(case, &mut remote);
            assert!(poll_once(read.as_mut()).is_pending());
            assert!(poll_once(driver.as_mut()).is_pending());
            assert_eq!(source_probe.drops.get(), 1);
            assert_eq!(output_probe.drops.load(Ordering::SeqCst), 0);
            assert_eq!(source_retirement(&observer), Poll::Ready(Ok(())));
            assert_eq!(handle.quiesce_snapshot(&noop_waker()).1, 0);
            output_probe.expect_owned.store(false, Ordering::SeqCst);
            if consume {
                let Poll::Ready(result) = poll_once(read.as_mut()) else {
                    panic!("committed reply is ready")
                };
                assert_eq!(result.is_err(), case.error());
                std::thread::spawn(move || {
                    let failure = catch_unwind(AssertUnwindSafe(|| drop(result)))
                        .expect_err("consumer-owned disposal");
                    assert_eq!(
                        failure.downcast_ref::<&str>(),
                        Some(&"transferred output drop")
                    );
                })
                .join()
                .expect("consumer thread physically joined");
            } else {
                let failure = catch_unwind(AssertUnwindSafe(|| drop(read)))
                    .expect_err("abandoned accepted reply disposal");
                assert_eq!(
                    failure.downcast_ref::<&str>(),
                    Some(&"transferred output drop")
                );
            }
            assert_eq!(output_probe.drops.load(Ordering::SeqCst), 1);
            assert_eq!(source_retirement(&observer), Poll::Ready(Ok(())));
            drop(driver);
            assert_eq!(handle.state(), WorkerState::Closed);
            assert_eq!(source_retirement(&observer.clone()), Poll::Ready(Ok(())));
        }
    }
}

#[cfg(feature = "observability")]
#[test]
fn joined_source_context_exit_drop_preserves_first_panic() {
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicU64;

    struct ExitSubscriber {
        armed: Arc<AtomicBool>,
        hits: Arc<AtomicUsize>,
        next: AtomicU64,
        metadata: Mutex<BTreeMap<u64, &'static tracing::Metadata<'static>>>,
        stack: Mutex<Vec<tracing::span::Id>>,
    }

    impl tracing::Subscriber for ExitSubscriber {
        fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, attributes: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            let id = self.next.fetch_add(1, Ordering::SeqCst);
            self.metadata
                .lock()
                .unwrap()
                .insert(id, attributes.metadata());
            tracing::span::Id::from_u64(id)
        }
        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
        fn event(&self, _event: &tracing::Event<'_>) {}
        fn enter(&self, span: &tracing::span::Id) {
            self.stack.lock().unwrap().push(span.clone());
        }
        fn exit(&self, span: &tracing::span::Id) {
            assert_eq!(self.stack.lock().unwrap().pop().as_ref(), Some(span));
            if self.armed.swap(false, Ordering::SeqCst) {
                self.hits.fetch_add(1, Ordering::SeqCst);
                panic!("constructor context exit first");
            }
        }
        fn current_span(&self) -> tracing_core::span::Current {
            let current = self.stack.lock().unwrap().last().cloned();
            match current {
                Some(id) => {
                    let metadata = self.metadata.lock().unwrap()[&id.into_u64()];
                    tracing_core::span::Current::new(id, metadata)
                }
                None => tracing_core::span::Current::none(),
            }
        }
    }

    struct ConstructorSource {
        source: OwnedFaultSource,
        armed: Arc<AtomicBool>,
    }

    impl LocalHttpSource for ConstructorSource {
        type HeaderEvent = TestHeader;
        type BodyEvent = TestBody;
        type Error = TestError;
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<TestHeader, TestError>> {
            let future = self.source.read_header_event();
            self.armed.store(true, Ordering::SeqCst);
            future
        }
        fn read_body_event(
            &mut self,
            maximum: u32,
        ) -> LocalSourceFuture<Result<LocalBodyRead<TestBody>, TestError>> {
            let future = self.source.read_body_event(maximum);
            self.armed.store(true, Ordering::SeqCst);
            future
        }
        fn stop_reading(&mut self, error: Option<TestError>) {
            self.source.stop_reading(error);
        }
        fn set_read_timeout(&mut self, timeout: Duration) {
            self.source.set_read_timeout(timeout);
        }
    }

    for phase in [OwnedFuturePhase::Header, OwnedFuturePhase::Body] {
        for destructor_panics in [false, true] {
            let armed = Arc::new(AtomicBool::new(false));
            let hits = Arc::new(AtomicUsize::new(0));
            let subscriber = ExitSubscriber {
                armed: Arc::clone(&armed),
                hits: Arc::clone(&hits),
                next: AtomicU64::new(1),
                metadata: Mutex::new(BTreeMap::new()),
                stack: Mutex::new(Vec::new()),
            };
            tracing::subscriber::with_default(subscriber, || {
                let span = tracing::info_span!("source_constructor_exit");
                let _entered = span.enter();
                let (handle, mut driver, context) = source_test_driver();
                let (release, independent_drops, independent_observer) =
                    independent_source_cleanup(&context, false);
                let future_probe = OwnedDropProbe::new();
                let source = OwnedFaultSource::with_future(
                    phase,
                    FutureDisposition::Pending,
                    if destructor_panics {
                        DropFailure::NestedPayload
                    } else {
                        DropFailure::None
                    },
                    future_probe.clone(),
                );
                let source_probe = source.probe.clone();
                let (mut remote, observer) = RemoteHttpSource::try_from_local_with_observer(
                    &context,
                    1,
                    ConstructorSource {
                        source,
                        armed: Arc::clone(&armed),
                    },
                )
                .unwrap();
                *future_probe.observation.borrow_mut() = Some(observer.clone());
                let mut read = owned_fault_read(phase, &mut remote);
                assert!(poll_once(read.as_mut()).is_pending());
                assert!(poll_once(driver.as_mut()).is_pending());
                assert_eq!(hits.load(Ordering::SeqCst), 1);
                assert!(!armed.load(Ordering::SeqCst));
                assert_eq!(
                    future_probe.polls.get(),
                    0,
                    "context restores before polling the supplied future"
                );
                assert_eq!(future_probe.drops.get(), 1);
                assert_eq!(source_probe.drops.get(), 1);
                assert!(future_probe.unwinding.get());
                let expected = if destructor_panics {
                    Err(WorkerCleanupObservationError::Unconfirmed)
                } else {
                    Ok(())
                };
                assert_eq!(source_retirement(&observer), Poll::Ready(expected));
                assert_source_failure_closed(
                    &handle,
                    driver.as_mut(),
                    &release,
                    &independent_drops,
                    &independent_observer,
                    "constructor context exit first",
                );
                assert_eq!(source_retirement(&observer), Poll::Ready(expected));
            });
        }
    }
}

#[cfg(feature = "observability")]
#[test]
fn joined_source_ready_context_exit_preserves_output_ownership() {
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicU64;

    struct ReadyExitSubscriber {
        armed: Arc<AtomicBool>,
        hits: Arc<AtomicUsize>,
        next: AtomicU64,
        metadata: Mutex<BTreeMap<u64, &'static tracing::Metadata<'static>>>,
        stack: Mutex<Vec<tracing::span::Id>>,
    }

    impl tracing::Subscriber for ReadyExitSubscriber {
        fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, attributes: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            let id = self.next.fetch_add(1, Ordering::SeqCst);
            self.metadata
                .lock()
                .unwrap()
                .insert(id, attributes.metadata());
            tracing::span::Id::from_u64(id)
        }
        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
        fn event(&self, _event: &tracing::Event<'_>) {}
        fn enter(&self, span: &tracing::span::Id) {
            self.stack.lock().unwrap().push(span.clone());
        }
        fn exit(&self, span: &tracing::span::Id) {
            assert_eq!(self.stack.lock().unwrap().pop().as_ref(), Some(span));
            if self.armed.swap(false, Ordering::SeqCst) {
                self.hits.fetch_add(1, Ordering::SeqCst);
                panic!("ready context exit first");
            }
        }
        fn current_span(&self) -> tracing_core::span::Current {
            let current = self.stack.lock().unwrap().last().cloned();
            match current {
                Some(id) => {
                    let metadata = self.metadata.lock().unwrap()[&id.into_u64()];
                    tracing_core::span::Current::new(id, metadata)
                }
                None => tracing_core::span::Current::none(),
            }
        }
    }

    struct ReadyExitFuture<T> {
        future: LocalSourceFuture<T>,
        armed: Arc<AtomicBool>,
    }

    impl<T> Future for ReadyExitFuture<T> {
        type Output = T;

        fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<T> {
            let result = self.future.as_mut().poll(context);
            if result.is_ready() {
                self.armed.store(true, Ordering::SeqCst);
            }
            result
        }
    }

    for case in REPLY_CASES.into_iter().map(Some).chain([None]) {
        for destructor_panics in [false, true] {
            let armed = Arc::new(AtomicBool::new(false));
            let hits = Arc::new(AtomicUsize::new(0));
            let subscriber = ReadyExitSubscriber {
                armed: Arc::clone(&armed),
                hits: Arc::clone(&hits),
                next: AtomicU64::new(1),
                metadata: Mutex::new(BTreeMap::new()),
                stack: Mutex::new(Vec::new()),
            };
            tracing::subscriber::with_default(subscriber, || {
                let span = tracing::info_span!("source_ready_exit");
                let _entered = span.enter();
                let (handle, mut driver, context) = source_test_driver();
                let (release, independent_drops, independent_observer) =
                    independent_source_cleanup(&context, false);
                let future_probe = OwnedDropProbe::new();
                let resume_probe = OwnedDropProbe::new();
                let output_probe = ReplyDropProbe::new();
                let failure = if destructor_panics {
                    DropFailure::NestedPayload
                } else {
                    DropFailure::None
                };
                let mut source = if let Some(case) = case {
                    ReplySource::new(
                        case,
                        ReplyValue {
                            probe: output_probe.clone(),
                            failure,
                        },
                        future_probe.clone(),
                        DropFailure::None,
                        DropFailure::None,
                        None,
                    )
                } else {
                    let resume = OwnedProbeFuture {
                        output: Some(()),
                        disposition: FutureDisposition::Pending,
                        failure,
                        probe: resume_probe.clone(),
                    };
                    let mut probe = OwnedDropProbe::new();
                    probe.observation = Rc::clone(&future_probe.observation);
                    ReplySource {
                        header: None,
                        body: Some(Box::pin(OwnedProbeFuture {
                            output: Some(Ok(LocalBodyRead::Suspend(Box::pin(resume)))),
                            disposition: FutureDisposition::Ready,
                            failure: DropFailure::None,
                            probe: future_probe.clone(),
                        })),
                        probe,
                        failure: DropFailure::None,
                    }
                };
                let read_case = case.unwrap_or(ReplyCase::Body);
                if read_case.header() {
                    source.header = Some(Box::pin(ReadyExitFuture {
                        future: source.header.take().unwrap(),
                        armed: Arc::clone(&armed),
                    }));
                } else {
                    source.body = Some(Box::pin(ReadyExitFuture {
                        future: source.body.take().unwrap(),
                        armed: Arc::clone(&armed),
                    }));
                }
                let source_probe = source.probe.clone();
                let (mut remote, observer) =
                    RemoteHttpSource::try_from_local_with_observer(&context, 1, source).unwrap();
                *future_probe.observation.borrow_mut() = Some(observer.clone());
                *resume_probe.observation.borrow_mut() = Some(observer.clone());
                *output_probe.observation.lock().unwrap() = Some(observer.clone());
                let mut read = read_reply(read_case, &mut remote);
                assert!(poll_once(read.as_mut()).is_pending());
                assert!(poll_once(driver.as_mut()).is_pending());
                assert_eq!(hits.load(Ordering::SeqCst), 1);
                assert!(!armed.load(Ordering::SeqCst));
                assert_eq!(future_probe.polls.get(), 1);
                assert_eq!(future_probe.drops.get(), 1);
                assert_eq!(source_probe.drops.get(), 1);
                if case.is_some() {
                    assert_eq!(output_probe.drops.load(Ordering::SeqCst), 1);
                } else {
                    assert_eq!(resume_probe.polls.get(), 0);
                    assert_eq!(resume_probe.drops.get(), 1);
                    assert!(resume_probe.unwinding.get());
                }
                let expected = if destructor_panics {
                    Err(WorkerCleanupObservationError::Unconfirmed)
                } else {
                    Ok(())
                };
                assert_eq!(source_retirement(&observer), Poll::Ready(expected));
                assert_source_failure_closed(
                    &handle,
                    driver.as_mut(),
                    &release,
                    &independent_drops,
                    &independent_observer,
                    "ready context exit first",
                );
                assert_eq!(source_retirement(&observer.clone()), Poll::Ready(expected));
            });
        }
    }
}

struct SourceRetirementRead {
    observation: Arc<Mutex<Option<SourceCleanupObserver>>>,
    actions: Arc<Mutex<Vec<SourceAction>>>,
}

impl Drop for SourceRetirementRead {
    fn drop(&mut self) {
        assert!(
            source_retirement(
                self.observation
                    .lock()
                    .unwrap()
                    .as_ref()
                    .expect("scope observed")
            )
            .is_pending()
        );
        record(
            &self.actions,
            SourceAction::ReadFutureDrop(std::thread::current().id()),
        );
    }
}

struct JoinedTestSource {
    source: ScriptedSource,
    observation: Arc<Mutex<Option<SourceCleanupObserver>>>,
    panic_on_read: bool,
    panic_on_drop: bool,
    _pinned: PhantomPinned,
}

impl JoinedTestSource {
    fn new(source: ScriptedSource, observation: Arc<Mutex<Option<SourceCleanupObserver>>>) -> Self {
        Self {
            source,
            observation,
            panic_on_read: false,
            panic_on_drop: false,
            _pinned: PhantomPinned,
        }
    }
}

impl LocalHttpSource for JoinedTestSource {
    type HeaderEvent = TestHeader;
    type BodyEvent = TestBody;
    type Error = TestError;

    fn read_header_event(&mut self) -> LocalSourceFuture<Result<TestHeader, TestError>> {
        self.source.read_header_event()
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<LocalBodyRead<TestBody>, TestError>> {
        let read = self.source.read_body_event(maximum);
        let guard = SourceRetirementRead {
            observation: Arc::clone(&self.observation),
            actions: Arc::clone(&self.source.actions),
        };
        let panic_on_read = self.panic_on_read;
        Box::pin(async move {
            let _guard = guard;
            assert!(!panic_on_read, "source read panic");
            read.await
        })
    }

    fn stop_reading(&mut self, error: Option<TestError>) {
        self.source.stop_reading(error);
    }

    fn set_read_timeout(&mut self, timeout: Duration) {
        self.source.set_read_timeout(timeout);
    }
}

impl Drop for JoinedTestSource {
    fn drop(&mut self) {
        assert!(
            source_retirement(
                self.observation
                    .lock()
                    .unwrap()
                    .as_ref()
                    .expect("scope observed")
            )
            .is_pending()
        );
        assert!(!self.panic_on_drop, "source destructor panic");
    }
}

#[test]
fn joined_source_receipt_follows_scope_retirement() {
    let (handle, mut driver, context) = source_test_driver();
    let owner_thread = std::thread::current().id();
    let actions = Arc::new(Mutex::new(Vec::new()));
    let observation = Arc::new(Mutex::new(None));
    let source = JoinedTestSource::new(
        ScriptedSource::new(
            [],
            [Ok(TestBody {
                kind: TestBodyKind::Body(3),
                terminal: true,
            })],
            Arc::clone(&actions),
        ),
        Arc::clone(&observation),
    );
    let (mut remote, observer) =
        RemoteHttpSource::try_from_local_with_observer(&context, 1, source).unwrap();
    *observation.lock().unwrap() = Some(observer.clone());
    assert_eq!(handle.quiesce_snapshot(&noop_waker()).1, 1);
    assert!(source_retirement(&observer).is_pending());
    let mut read = Box::pin(remote.read_body_event(3));
    assert!(poll_once(read.as_mut()).is_pending());
    assert!(poll_once(driver.as_mut()).is_pending());
    assert!(matches!(poll_once(read.as_mut()), Poll::Ready(Ok(event)) if event.terminal));
    assert_eq!(source_retirement(&observer), Poll::Ready(Ok(())));
    assert_eq!(source_retirement(&observer.clone()), Poll::Ready(Ok(())));
    assert_eq!(handle.quiesce_snapshot(&noop_waker()).1, 0);
    assert_eq!(handle.state(), WorkerState::Open);
    assert_eq!(
        recorded_actions(&actions),
        vec![
            SourceAction::Body {
                owner: owner_thread,
                maximum: 3
            },
            SourceAction::ReadFutureDrop(owner_thread),
            SourceAction::Drop(owner_thread),
        ]
    );
    drop(read);
    drop(remote);
    drop(driver);
    assert_eq!(handle.state(), WorkerState::Closed);
}

#[test]
fn joined_source_observer_drop_does_not_cancel() {
    let worker = ManagedWorker::new(2).expect("worker starts");
    let owner_thread = worker.owner_thread();
    let actions = Arc::new(Mutex::new(Vec::new()));
    let source_actions = Arc::clone(&actions);
    runtime().block_on(async {
        tokio::time::timeout(Duration::from_secs(10), async {
            let (mut remote, observer) = worker
                .handle()
                .call(move |context| {
                    RemoteHttpSource::try_from_local_with_observer(
                        context,
                        1,
                        ScriptedSource::new(
                            [],
                            [Ok(TestBody {
                                kind: TestBodyKind::Body(1),
                                terminal: true,
                            })],
                            source_actions,
                        ),
                    )
                    .unwrap()
                })
                .unwrap()
                .await
                .unwrap();
            let wait = observer.wait();
            drop(observer);
            drop(wait);
            assert!(remote.read_body_event(1).await.unwrap().terminal);
            drop(remote);
        })
        .await
        .expect("joined source completes within bound");
    });
    worker.shutdown().expect("worker physically joined");
    assert_eq!(
        recorded_actions(&actions),
        vec![
            SourceAction::Body {
                owner: owner_thread,
                maximum: 1
            },
            SourceAction::Drop(owner_thread),
        ]
    );
}

#[test]
fn prepared_source_attaches_during_grace() {
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
    runtime().block_on(async {
        tokio::time::timeout(Duration::from_secs(10), async {
            let (endpoints_tx, endpoints_rx) = oneshot::channel();
            let (release, released) = oneshot::channel();
            let attachment = worker
                .call_local(move |context| async move {
                    let (attachment, remote, observer) =
                        RemoteHttpSource::prepare_local::<ScriptedSource>(&context, 1).unwrap();
                    assert!(endpoints_tx.send((remote, observer)).is_ok());
                    released.await.unwrap();
                    assert!(matches!(
                        RemoteHttpSource::prepare_local::<ScriptedSource>(&context, 1),
                        Err(RemoteSourceCreateError::Cleanup(
                            CleanupRegistrationError::Closed
                        ))
                    ));
                    attachment.attach(ScriptedSource::new(
                        [Ok(TestHeader {
                            status: 201,
                            terminal: false,
                        })],
                        [Ok(TestBody {
                            kind: TestBodyKind::Body(2),
                            terminal: true,
                        })],
                        source_actions,
                    ))
                })
                .unwrap();
            let (mut remote, observer) = endpoints_rx.await.unwrap();
            let quiescence = handle.quiesce(Duration::from_secs(60));
            assert!(!worker.is_accepting());
            assert_eq!(worker.state(), WorkerState::Open);
            assert!(source_retirement(&observer).is_pending());
            release.send(()).unwrap();
            attachment
                .await
                .unwrap()
                .expect("pre-admitted attachment survives grace");
            assert_eq!(remote.read_header_event().await.unwrap().status, 201);
            assert!(remote.read_body_event(2).await.unwrap().terminal);
            observer.wait().await.unwrap();
            quiescence.await.expect("scope credit retires during grace");
        })
        .await
        .expect("prepared source drains within bound");
    });
    let report = owner.join().expect("root physically joins");
    assert_eq!(report.workers_joined, 1);
    assert_eq!(report.workers_forced, 0);
    assert_eq!(
        recorded_actions(&actions),
        vec![
            SourceAction::Header(owner_thread),
            SourceAction::Body {
                owner: owner_thread,
                maximum: 2
            },
            SourceAction::Drop(owner_thread),
        ]
    );
}

#[test]
fn prepared_source_forced_shutdown_rejects_late_handoff() {
    let (handle, mut driver, context) = source_test_driver();
    let owner_thread = std::thread::current().id();
    let actions = Arc::new(Mutex::new(Vec::new()));
    let (attachment, mut remote, observer) =
        RemoteHttpSource::prepare_local::<ScriptedSource>(&context, 1).unwrap();
    assert!(poll_once(driver.as_mut()).is_pending());
    assert!(handle.begin_shutdown());
    assert!(poll_once(driver.as_mut()).is_ready());
    assert_eq!(source_retirement(&observer), Poll::Ready(Ok(())));
    assert_eq!(handle.state(), WorkerState::Closed);
    assert_eq!(
        attachment.attach(ScriptedSource::new([], [], Arc::clone(&actions))),
        Err(RemoteSourceCreateError::Cleanup(
            CleanupRegistrationError::Closed
        ))
    );
    assert!(matches!(
        RemoteHttpSource::prepare_local::<ScriptedSource>(&context, 1),
        Err(RemoteSourceCreateError::Cleanup(
            CleanupRegistrationError::Closed
        ))
    ));
    assert!(matches!(
        poll_once(Box::pin(remote.read_body_event(1)).as_mut()),
        Poll::Ready(Err(RemoteSourceError::Stopped | RemoteSourceError::Closed))
    ));
    assert_eq!(handle.quiesce_snapshot(&noop_waker()).1, 0);
    assert_eq!(
        recorded_actions(&actions),
        vec![
            SourceAction::Stop {
                owner: owner_thread,
                error: None
            },
            SourceAction::Drop(owner_thread),
        ]
    );
    drop(driver);
}

#[test]
fn prepared_source_unused_drop_retires_scope() {
    let (handle, mut driver, context) = source_test_driver();
    let (attachment, remote, observer) =
        RemoteHttpSource::prepare_local::<ScriptedSource>(&context, 1).unwrap();
    assert!(source_retirement(&observer).is_pending());
    assert_eq!(handle.quiesce_snapshot(&noop_waker()).1, 1);
    drop(attachment);
    assert!(source_retirement(&observer).is_pending());
    assert!(poll_once(driver.as_mut()).is_pending());
    assert_eq!(source_retirement(&observer), Poll::Ready(Ok(())));
    assert_eq!(handle.quiesce_snapshot(&noop_waker()).1, 0);
    assert_eq!(
        remote.set_read_timeout(Duration::from_secs(1)),
        Err(SourceControlError::Closed)
    );
    let (attachment, remote, observer) =
        RemoteHttpSource::prepare_local::<ScriptedSource>(&context, 1).unwrap();
    drop(remote);
    assert!(poll_once(driver.as_mut()).is_pending());
    assert_eq!(source_retirement(&observer), Poll::Ready(Ok(())));
    drop(attachment);
    drop(driver);
    assert_eq!(handle.state(), WorkerState::Closed);
}

#[test]
fn prepared_source_cancel_bypasses_full_queues() {
    for bind_source in [false, true] {
        let (handle, mut driver, context) = source_test_driver();
        let owner_thread = std::thread::current().id();
        let actions = Arc::new(Mutex::new(Vec::new()));
        let (attachment, mut remote, observer) =
            RemoteHttpSource::prepare_local::<ScriptedSource>(&context, 1).unwrap();
        let mut attachment = Some(attachment);
        if bind_source {
            attachment
                .take()
                .unwrap()
                .attach(ScriptedSource::new([], [], Arc::clone(&actions)))
                .unwrap();
        }
        remote.set_read_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(
            remote.set_read_timeout(Duration::from_secs(2)),
            Err(SourceControlError::CommandQueueFull)
        );
        let abort = remote.abort_handle();
        let mut read = Box::pin(remote.read_body_event(1));
        assert!(poll_once(read.as_mut()).is_pending());
        assert!(abort.stop_reading(Some(TestError::Cancelled)));
        assert!(!abort.stop_reading(None));
        assert!(source_retirement(&observer).is_pending());
        assert!(poll_once(driver.as_mut()).is_pending());
        assert_eq!(
            poll_once(read.as_mut()),
            Poll::Ready(Err(RemoteSourceError::Stopped))
        );
        assert_eq!(source_retirement(&observer), Poll::Ready(Ok(())));
        let expected = if bind_source {
            vec![
                SourceAction::Stop {
                    owner: owner_thread,
                    error: Some(TestError::Cancelled),
                },
                SourceAction::Drop(owner_thread),
            ]
        } else {
            vec![]
        };
        assert_eq!(recorded_actions(&actions), expected);
        assert_eq!(handle.quiesce_snapshot(&noop_waker()).1, 0);
        drop(attachment);
        drop(driver);
    }
}

#[test]
fn prepared_source_handoff_cancel_race_retires_owner() {
    for cancel_before_attach in [false, true] {
        let (handle, mut driver, context) = source_test_driver();
        let owner_thread = std::thread::current().id();
        let actions = Arc::new(Mutex::new(Vec::new()));
        let observation = Arc::new(Mutex::new(None));
        let (attachment, remote, observer) =
            RemoteHttpSource::prepare_local::<JoinedTestSource>(&context, 1).unwrap();
        *observation.lock().unwrap() = Some(observer.clone());
        if cancel_before_attach {
            assert!(remote.abort_handle().stop_reading(None));
        }
        let result = attachment.attach(JoinedTestSource::new(
            ScriptedSource::new([], [], Arc::clone(&actions)),
            observation,
        ));
        if cancel_before_attach {
            assert_eq!(
                result,
                Err(RemoteSourceCreateError::Cleanup(
                    CleanupRegistrationError::Closed
                ))
            );
        } else {
            result.expect("handoff accepted before cancellation");
            assert!(recorded_actions(&actions).is_empty());
            assert!(remote.abort_handle().stop_reading(None));
        }
        assert!(source_retirement(&observer).is_pending());
        assert!(poll_once(driver.as_mut()).is_pending());
        assert_eq!(source_retirement(&observer), Poll::Ready(Ok(())));
        assert_eq!(
            recorded_actions(&actions),
            vec![
                SourceAction::Stop {
                    owner: owner_thread,
                    error: None
                },
                SourceAction::Drop(owner_thread),
            ]
        );
        drop(driver);
        assert_eq!(handle.state(), WorkerState::Closed);
    }
}

#[test]
fn joined_source_failures_preserve_retirement_outcome() {
    for panic_on_read in [false, true] {
        let (handle, mut driver, context) = source_test_driver();
        let actions = Arc::new(Mutex::new(Vec::new()));
        let observation = Arc::new(Mutex::new(None));
        let mut source = JoinedTestSource::new(
            ScriptedSource::new([], [Err(TestError::TimedOut)], Arc::clone(&actions)),
            Arc::clone(&observation),
        );
        source.panic_on_read = panic_on_read;
        let (mut remote, observer) =
            RemoteHttpSource::try_from_local_with_observer(&context, 1, source).unwrap();
        *observation.lock().unwrap() = Some(observer.clone());
        let mut read = Box::pin(remote.read_body_event(1));
        assert!(poll_once(read.as_mut()).is_pending());
        let result = catch_unwind(AssertUnwindSafe(|| poll_once(driver.as_mut())));
        if panic_on_read {
            assert_eq!(
                result
                    .expect_err("read panic retained")
                    .downcast_ref::<&str>(),
                Some(&"source read panic")
            );
            assert_eq!(handle.state(), WorkerState::Closed);
        } else {
            assert!(result.unwrap().is_pending());
            assert_eq!(
                poll_once(read.as_mut()),
                Poll::Ready(Err(RemoteSourceError::Source(TestError::TimedOut)))
            );
        }
        assert_eq!(source_retirement(&observer), Poll::Ready(Ok(())));
        assert_eq!(handle.quiesce_snapshot(&noop_waker()).1, 0);
        assert_eq!(
            recorded_actions(&actions).last(),
            Some(&SourceAction::Drop(std::thread::current().id()))
        );
        drop(driver);
    }
    let (handle, driver, context) = source_test_driver();
    let actions = Arc::new(Mutex::new(Vec::new()));
    let observation = Arc::new(Mutex::new(None));
    let (attachment, remote, observer) =
        RemoteHttpSource::prepare_local::<JoinedTestSource>(&context, 1).unwrap();
    *observation.lock().unwrap() = Some(observer.clone());
    let mut source = JoinedTestSource::new(
        ScriptedSource::new([], [], Arc::clone(&actions)),
        observation,
    );
    source.panic_on_drop = true;
    attachment.attach(source).unwrap();
    let panic = catch_unwind(AssertUnwindSafe(|| drop(driver)))
        .expect_err("capture destruction panic retained");
    assert_eq!(
        panic.downcast_ref::<&str>(),
        Some(&"source destructor panic")
    );
    assert_eq!(handle.state(), WorkerState::Closed);
    assert_eq!(handle.quiesce_snapshot(&noop_waker()).1, 0);
    assert_eq!(
        source_retirement(&observer),
        Poll::Ready(Err(WorkerCleanupObservationError::Unconfirmed))
    );
    assert_eq!(
        source_retirement(&observer.clone()),
        Poll::Ready(Err(WorkerCleanupObservationError::Unconfirmed))
    );
    let owner_thread = std::thread::current().id();
    assert_eq!(
        recorded_actions(&actions),
        vec![
            SourceAction::Stop {
                owner: owner_thread,
                error: None
            },
            SourceAction::Drop(owner_thread),
        ]
    );
    drop(remote);
}

#[test]
fn completed_source_abort_is_inert_without_a_runtime() {
    let abort = SourceAbortHandle::<()>::completed();
    let observer = abort.clone();
    assert!(!abort.stop_reading(Some(())));
    drop(abort);
    assert!(!observer.stop_reading(None));
}

#[test]
fn local_source_export() {
    use crate::managed::RuntimeConfig;
    use crate::managed::RuntimeOwner;
    use crate::source_proxy::RemoteSourceCreateError;
    use crate::worker::CleanupRegistrationError;

    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 4,
    })
    .expect("runtime starts");
    let handle = owner.handle();
    let worker = handle.worker(0).expect("worker exists");
    let actions = Arc::new(Mutex::new(Vec::new()));
    let source_actions = Arc::clone(&actions);
    runtime().block_on(async {
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut source = worker
                .call(move |context| {
                    let local = ScriptedSource::new(
                        [Ok(TestHeader {
                            status: 200,
                            terminal: false,
                        })],
                        [Ok(TestBody {
                            kind: TestBodyKind::Body(3),
                            terminal: true,
                        })],
                        source_actions,
                    );
                    RemoteHttpSource::try_from_local(context, 1, local)
                })
                .expect("initializer admitted")
                .await
                .expect("initializer returns")
                .expect("local source exported");
            let (release, released) = oneshot::channel();
            let late = worker
                .call_local(move |context| async move {
                    released.await.expect("late admission released");
                    RemoteHttpSource::try_from_local(
                        &context,
                        1,
                        ScriptedSource::new([], [], Arc::new(Mutex::new(Vec::new()))),
                    )
                    .err()
                })
                .expect("late command admitted before quiescence");
            let quiescence = handle.quiesce(Duration::from_secs(60));
            release.send(()).expect("late command receives release");
            assert_eq!(
                late.await.expect("late command finishes"),
                Some(RemoteSourceCreateError::Cleanup(
                    CleanupRegistrationError::Closed
                ),)
            );
            assert_eq!(source.read_header_event().await.unwrap().status, 200);
            assert!(source.read_body_event(3).await.unwrap().terminal);
            quiescence
                .await
                .expect("scope credit drains after the final read");
            drop(source);
        })
        .await
        .expect("local export drains within the test bound");
    });
    owner.join().expect("workers physically join");
    let actions = recorded_actions(&actions);
    assert_eq!(actions.len(), 3);
    assert!(matches!(&actions[0], SourceAction::Header(_)));
    let SourceAction::Drop(owner_thread) = actions[2] else {
        panic!("source destroyed")
    };
    assert_eq!(actions[0], SourceAction::Header(owner_thread));
    assert_eq!(
        actions[1],
        SourceAction::Body {
            owner: owner_thread,
            maximum: 3
        }
    );
}

#[test]
fn local_source_export_stop_bypasses_full_control_queue() {
    let worker = ManagedWorker::new(2).expect("worker starts");
    let owner = worker.owner_thread();
    let actions = Arc::new(Mutex::new(Vec::new()));
    let source_actions = Arc::clone(&actions);
    runtime().block_on(async {
        tokio::time::timeout(
            Duration::from_secs(10),
            worker
                .handle()
                .call(move |context| {
                    let remote = RemoteHttpSource::try_from_local(
                        context,
                        1,
                        ScriptedSource::new([], [], source_actions),
                    )
                    .expect("source exported");
                    remote
                        .set_read_timeout(Duration::from_secs(1))
                        .expect("control fills queue");
                    assert_eq!(
                        remote.set_read_timeout(Duration::from_secs(2)),
                        Err(SourceControlError::CommandQueueFull)
                    );
                    assert!(remote.abort_handle().stop_reading(None));
                    drop(remote);
                })
                .expect("command admitted"),
        )
        .await
        .expect("command bounded")
        .expect("command completes");
    });
    worker.shutdown().expect("cancelled local scope joins");
    assert_eq!(
        recorded_actions(&actions),
        vec![
            SourceAction::Stop { owner, error: None },
            SourceAction::Drop(owner),
        ]
    );
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
enum TestError {
    #[error("cancelled")]
    Cancelled,
    #[error("timed out")]
    TimedOut,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TestHeader {
    status: u16,
    terminal: bool,
}

impl RemoteSourceEvent for TestHeader {
    fn is_terminal(&self) -> bool {
        self.terminal
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TestBodyKind {
    Body(usize),
    Push(&'static str),
    Trailers(&'static str),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TestBody {
    kind: TestBodyKind,
    terminal: bool,
}

impl RemoteSourceEvent for TestBody {
    fn is_terminal(&self) -> bool {
        self.terminal
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SourceAction {
    Header(ThreadId),
    Body {
        owner: ThreadId,
        maximum: u32,
    },
    Timeout {
        owner: ThreadId,
        timeout: Duration,
    },
    Stop {
        owner: ThreadId,
        error: Option<TestError>,
    },
    ReadFutureDrop(ThreadId),
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

struct ScriptedSource {
    headers: VecDeque<Result<TestHeader, TestError>>,
    bodies: VecDeque<Result<TestBody, TestError>>,
    actions: Arc<Mutex<Vec<SourceAction>>>,
    _local: Rc<()>,
}

impl ScriptedSource {
    fn new(
        headers: impl IntoIterator<Item = Result<TestHeader, TestError>>,
        bodies: impl IntoIterator<Item = Result<TestBody, TestError>>,
        actions: Arc<Mutex<Vec<SourceAction>>>,
    ) -> Self {
        Self {
            headers: headers.into_iter().collect(),
            bodies: bodies.into_iter().collect(),
            actions,
            _local: Rc::new(()),
        }
    }
}

impl LocalHttpSource for ScriptedSource {
    type HeaderEvent = TestHeader;
    type BodyEvent = TestBody;
    type Error = TestError;

    fn read_header_event(&mut self) -> LocalSourceFuture<Result<TestHeader, TestError>> {
        record(
            &self.actions,
            SourceAction::Header(std::thread::current().id()),
        );
        let result = self
            .headers
            .pop_front()
            .expect("scripted header event exists");
        Box::pin(async move { result })
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<LocalBodyRead<TestBody>, TestError>> {
        record(
            &self.actions,
            SourceAction::Body {
                owner: std::thread::current().id(),
                maximum,
            },
        );
        let result = self
            .bodies
            .pop_front()
            .expect("scripted body event exists")
            .map(LocalBodyRead::Event);
        Box::pin(async move { result })
    }

    fn stop_reading(&mut self, error: Option<TestError>) {
        record(
            &self.actions,
            SourceAction::Stop {
                owner: std::thread::current().id(),
                error,
            },
        );
    }

    fn set_read_timeout(&mut self, timeout: Duration) {
        record(
            &self.actions,
            SourceAction::Timeout {
                owner: std::thread::current().id(),
                timeout,
            },
        );
    }
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
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
}

fn owner_barrier(handle: &WorkerHandle) {
    runtime()
        .block_on(handle.call(|_| ()).expect("owner barrier admitted"))
        .expect("owner barrier completed");
}

mod source_proxy {
    use super::*;

    #[test]
    fn cross_worker_events_preserve_order() {
        let worker = ManagedWorker::new(8).expect("managed worker starts");
        let owner = worker.owner_thread();
        let actions = Arc::new(Mutex::new(Vec::new()));
        let source_actions = Arc::clone(&actions);
        let mut remote = RemoteHttpSource::new(&worker.handle(), 2, move |_| {
            ScriptedSource::new(
                [Ok(TestHeader {
                    status: 200,
                    terminal: false,
                })],
                [
                    Ok(TestBody {
                        kind: TestBodyKind::Body(100),
                        terminal: false,
                    }),
                    Ok(TestBody {
                        kind: TestBodyKind::Push("/push"),
                        terminal: false,
                    }),
                    Ok(TestBody {
                        kind: TestBodyKind::Trailers("x-trailer1: trailer1"),
                        terminal: true,
                    }),
                ],
                source_actions,
            )
        })
        .expect("source bridge starts");
        assert_eq!(remote.owner_worker(), worker.handle().id());

        let runtime = runtime();
        assert_eq!(
            runtime.block_on(remote.read_header_event()).unwrap(),
            TestHeader {
                status: 200,
                terminal: false,
            },
        );
        assert_eq!(
            runtime.block_on(remote.read_body_event(100)).unwrap(),
            TestBody {
                kind: TestBodyKind::Body(100),
                terminal: false,
            },
        );
        assert_eq!(
            runtime.block_on(remote.read_body_event(u32::MAX)).unwrap(),
            TestBody {
                kind: TestBodyKind::Push("/push"),
                terminal: false,
            },
        );
        assert_eq!(
            runtime.block_on(remote.read_body_event(1)).unwrap(),
            TestBody {
                kind: TestBodyKind::Trailers("x-trailer1: trailer1"),
                terminal: true,
            },
        );
        assert_eq!(
            runtime.block_on(remote.read_body_event(1)),
            Err(RemoteSourceError::Closed),
        );

        assert_eq!(
            recorded_actions(&actions),
            vec![
                SourceAction::Header(owner),
                SourceAction::Body {
                    owner,
                    maximum: 100,
                },
                SourceAction::Body {
                    owner,
                    maximum: u32::MAX,
                },
                SourceAction::Body { owner, maximum: 1 },
                SourceAction::Drop(owner),
            ],
        );
        drop(remote);
        worker.shutdown().expect("managed worker shuts down");
    }

    #[test]
    fn cross_worker_abort_is_terminal() {
        let worker = ManagedWorker::new(8).expect("managed worker starts");
        let owner = worker.owner_thread();
        let actions = Arc::new(Mutex::new(Vec::new()));
        let source_actions = Arc::clone(&actions);
        let mut remote = RemoteHttpSource::new(&worker.handle(), 2, move |_| {
            ScriptedSource::new(
                [Ok(TestHeader {
                    status: 200,
                    terminal: false,
                })],
                [
                    Ok(TestBody {
                        kind: TestBodyKind::Body(100),
                        terminal: false,
                    }),
                    Err(TestError::Cancelled),
                ],
                source_actions,
            )
        })
        .expect("source bridge starts");
        let abort = remote.abort_handle();

        let runtime = runtime();
        assert!(runtime.block_on(remote.read_header_event()).is_ok());
        assert!(runtime.block_on(remote.read_body_event(100)).is_ok());
        assert_eq!(
            runtime.block_on(remote.read_body_event(100)),
            Err(RemoteSourceError::Source(TestError::Cancelled)),
        );
        assert!(!abort.stop_reading(Some(TestError::TimedOut)));
        assert_eq!(
            runtime.block_on(remote.read_body_event(100)),
            Err(RemoteSourceError::Closed),
        );
        assert_eq!(
            recorded_actions(&actions),
            vec![
                SourceAction::Header(owner),
                SourceAction::Body {
                    owner,
                    maximum: 100,
                },
                SourceAction::Body {
                    owner,
                    maximum: 100,
                },
                SourceAction::Drop(owner),
            ],
        );
        drop(remote);
        worker.shutdown().expect("managed worker shuts down");
    }

    #[test]
    fn timeout_and_suspend_work_remain_on_the_owner() {
        struct SuspendedSource {
            calls: usize,
            timeout_applied: Option<mpsc::Sender<(ThreadId, Duration)>>,
            resumed: Option<mpsc::Sender<ThreadId>>,
            actions: Arc<Mutex<Vec<SourceAction>>>,
            _local: Rc<()>,
        }

        impl LocalHttpSource for SuspendedSource {
            type HeaderEvent = TestHeader;
            type BodyEvent = TestBody;
            type Error = TestError;

            fn read_header_event(&mut self) -> LocalSourceFuture<Result<TestHeader, TestError>> {
                Box::pin(async {
                    Ok(TestHeader {
                        status: 200,
                        terminal: false,
                    })
                })
            }

            fn read_body_event(
                &mut self,
                maximum: u32,
            ) -> LocalSourceFuture<Result<LocalBodyRead<TestBody>, TestError>> {
                record(
                    &self.actions,
                    SourceAction::Body {
                        owner: std::thread::current().id(),
                        maximum,
                    },
                );
                self.calls += 1;
                if self.calls == 1 {
                    let resumed = self.resumed.take().expect("one suspension");
                    Box::pin(async move {
                        Ok(LocalBodyRead::Suspend(Box::pin(async move {
                            resumed
                                .send(std::thread::current().id())
                                .expect("report resume owner");
                        })))
                    })
                } else {
                    Box::pin(async { Err(TestError::TimedOut) })
                }
            }

            fn stop_reading(&mut self, error: Option<TestError>) {
                record(
                    &self.actions,
                    SourceAction::Stop {
                        owner: std::thread::current().id(),
                        error,
                    },
                );
            }

            fn set_read_timeout(&mut self, timeout: Duration) {
                let owner = std::thread::current().id();
                record(&self.actions, SourceAction::Timeout { owner, timeout });
                self.timeout_applied
                    .take()
                    .expect("timeout reported once")
                    .send((owner, timeout))
                    .expect("report timeout owner");
            }
        }

        impl Drop for SuspendedSource {
            fn drop(&mut self) {
                record(
                    &self.actions,
                    SourceAction::Drop(std::thread::current().id()),
                );
            }
        }

        let worker = ManagedWorker::new(8).expect("managed worker starts");
        let owner = worker.owner_thread();
        let actions = Arc::new(Mutex::new(Vec::new()));
        let source_actions = Arc::clone(&actions);
        let (timeout_tx, timeout_rx) = mpsc::channel();
        let (resumed_tx, resumed_rx) = mpsc::channel();
        let mut remote = RemoteHttpSource::new(&worker.handle(), 2, move |_| SuspendedSource {
            calls: 0,
            timeout_applied: Some(timeout_tx),
            resumed: Some(resumed_tx),
            actions: source_actions,
            _local: Rc::new(()),
        })
        .expect("source bridge starts");

        let timeout = Duration::from_millis(125);
        remote
            .set_read_timeout(timeout)
            .expect("timeout command admitted");
        assert_eq!(
            timeout_rx.recv().expect("timeout applied"),
            (owner, timeout)
        );
        assert_eq!(
            runtime().block_on(remote.read_body_event(17)),
            Err(RemoteSourceError::Source(TestError::TimedOut)),
        );
        assert_eq!(resumed_rx.recv().expect("resume ran"), owner);

        let observed = recorded_actions(&actions);
        assert_eq!(observed[0], SourceAction::Timeout { owner, timeout });
        assert_eq!(
            observed[1..],
            [
                SourceAction::Body { owner, maximum: 17 },
                SourceAction::Body { owner, maximum: 17 },
                SourceAction::Drop(owner),
            ],
        );
        drop(remote);
        worker.shutdown().expect("managed worker shuts down");
    }

    #[test]
    fn cloned_control_applies_timeout_while_body_read_is_pending() {
        struct PendingTimeoutSource {
            started: Option<oneshot::Sender<()>>,
            release: Option<oneshot::Receiver<TestBody>>,
            timeout_applied: Option<oneshot::Sender<(ThreadId, Duration)>>,
            _local: Rc<()>,
        }

        impl LocalHttpSource for PendingTimeoutSource {
            type HeaderEvent = TestHeader;
            type BodyEvent = TestBody;
            type Error = TestError;

            fn read_header_event(&mut self) -> LocalSourceFuture<Result<TestHeader, TestError>> {
                Box::pin(std::future::pending())
            }

            fn read_body_event(
                &mut self,
                _maximum: u32,
            ) -> LocalSourceFuture<Result<LocalBodyRead<TestBody>, TestError>> {
                let started = self.started.take().expect("body read starts once");
                let release = self.release.take().expect("body read has one release");
                Box::pin(async move {
                    started.send(()).expect("report pending body read");
                    Ok(LocalBodyRead::Event(
                        release.await.expect("release pending body read"),
                    ))
                })
            }

            fn stop_reading(&mut self, _error: Option<TestError>) {}

            fn set_read_timeout(&mut self, timeout: Duration) {
                self.timeout_applied
                    .take()
                    .expect("timeout applied once")
                    .send((std::thread::current().id(), timeout))
                    .expect("report timeout application");
            }
        }

        let worker = ManagedWorker::new(8).expect("managed worker starts");
        let owner = worker.owner_thread();
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let (timeout_tx, timeout_rx) = oneshot::channel();
        let mut remote =
            RemoteHttpSource::new(&worker.handle(), 1, move |_| PendingTimeoutSource {
                started: Some(started_tx),
                release: Some(release_rx),
                timeout_applied: Some(timeout_tx),
                _local: Rc::new(()),
            })
            .expect("source bridge starts");
        let control = remote.control_handle();
        let control_during_read = control.clone();
        let timeout = Duration::from_millis(125);
        let expected = TestBody {
            kind: TestBodyKind::Trailers("done"),
            terminal: true,
        };

        let result = runtime().block_on(async {
            let read = remote.read_body_event(17);
            let apply_timeout = async {
                started_rx.await.expect("body read reached pending barrier");
                control_during_read
                    .set_read_timeout(timeout)
                    .expect("timeout command admitted during read");
                assert_eq!(
                    timeout_rx
                        .await
                        .expect("timeout applied while read pending"),
                    (owner, timeout),
                );
                release_tx
                    .send(expected.clone())
                    .expect("release pending body read");
            };
            let (result, ()) = tokio::join!(read, apply_timeout);
            result
        });

        assert_eq!(result.unwrap(), expected);
        assert_eq!(
            control.set_read_timeout(Duration::from_secs(1)),
            Err(SourceControlError::Closed),
        );
        drop(remote);
        worker.shutdown().expect("managed worker shuts down");
    }

    #[test]
    fn abandoned_read_remains_in_flight_until_owner_completion() {
        struct PendingThenReadySource {
            receiver: Option<oneshot::Receiver<TestBody>>,
            next: Option<TestBody>,
            started: Option<mpsc::Sender<()>>,
            _local: Rc<()>,
        }

        impl LocalHttpSource for PendingThenReadySource {
            type HeaderEvent = TestHeader;
            type BodyEvent = TestBody;
            type Error = TestError;

            fn read_header_event(&mut self) -> LocalSourceFuture<Result<TestHeader, TestError>> {
                Box::pin(async {
                    Ok(TestHeader {
                        status: 200,
                        terminal: false,
                    })
                })
            }

            fn read_body_event(
                &mut self,
                _maximum: u32,
            ) -> LocalSourceFuture<Result<LocalBodyRead<TestBody>, TestError>> {
                if let Some(receiver) = self.receiver.take() {
                    self.started
                        .take()
                        .expect("first read starts once")
                        .send(())
                        .expect("report pending read");
                    Box::pin(async move {
                        Ok(LocalBodyRead::Event(
                            receiver.await.expect("release pending read"),
                        ))
                    })
                } else {
                    let event = self.next.take().expect("second body event exists");
                    Box::pin(async move { Ok(LocalBodyRead::Event(event)) })
                }
            }

            fn stop_reading(&mut self, _error: Option<TestError>) {}

            fn set_read_timeout(&mut self, _timeout: Duration) {}
        }

        let worker = ManagedWorker::new(8).expect("managed worker starts");
        let handle = worker.handle();
        let (release_tx, release_rx) = oneshot::channel();
        let (started_tx, started_rx) = mpsc::channel();
        let mut remote = RemoteHttpSource::new(&handle, 1, move |_| PendingThenReadySource {
            receiver: Some(release_rx),
            next: Some(TestBody {
                kind: TestBodyKind::Trailers("done"),
                terminal: true,
            }),
            started: Some(started_tx),
            _local: Rc::new(()),
        })
        .expect("source bridge starts");

        let mut first = Box::pin(remote.read_body_event(1));
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);
        assert_eq!(first.as_mut().poll(&mut context), Poll::Pending);
        started_rx.recv().expect("owner started first read");
        drop(first);

        assert_eq!(
            runtime().block_on(remote.read_body_event(1)),
            Err(RemoteSourceError::ReadInFlight),
        );
        assert!(
            release_tx
                .send(TestBody {
                    kind: TestBodyKind::Body(1),
                    terminal: false,
                })
                .is_ok(),
            "release abandoned read",
        );
        owner_barrier(&handle);
        assert_eq!(
            runtime().block_on(remote.read_body_event(1)).unwrap(),
            TestBody {
                kind: TestBodyKind::Trailers("done"),
                terminal: true,
            },
        );

        drop(remote);
        worker.shutdown().expect("managed worker shuts down");
    }

    #[test]
    fn timeout_queue_is_bounded() {
        let (handle, mut guard) =
            crate::tokio_host::caller_driven_worker(8).expect("caller-driven worker starts");
        let remote = RemoteHttpSource::new(&handle, 1, move |_| {
            ScriptedSource::new([], [], Arc::new(Mutex::new(Vec::new())))
        })
        .expect("source bridge starts");
        let control = remote.control_handle();

        control
            .set_read_timeout(Duration::from_millis(1))
            .expect("first timeout admitted");
        assert_eq!(
            control.set_read_timeout(Duration::from_millis(2)),
            Err(SourceControlError::CommandQueueFull),
        );

        drop(remote);
        runtime()
            .block_on(guard.shutdown())
            .expect("caller-driven worker shuts down");
    }
}

mod stream_source_holder {
    use super::*;

    #[test]
    fn outer_drop_off_worker_keeps_inner_drop_local() {
        let consumer = std::thread::current().id();
        let worker = ManagedWorker::new(4).expect("managed worker starts");
        let owner = worker.owner_thread();
        let actions = Arc::new(Mutex::new(Vec::new()));
        let source_actions = Arc::clone(&actions);
        let mut remote = RemoteHttpSource::new(&worker.handle(), 1, move |_| {
            ScriptedSource::new(
                [Ok(TestHeader {
                    status: 200,
                    terminal: false,
                })],
                [
                    Ok(TestBody {
                        kind: TestBodyKind::Body(100),
                        terminal: false,
                    }),
                    Ok(TestBody {
                        kind: TestBodyKind::Body(0),
                        terminal: true,
                    }),
                ],
                source_actions,
            )
        })
        .expect("source bridge starts");
        assert_ne!(owner, consumer);

        let runtime = runtime();
        assert_eq!(
            runtime.block_on(remote.read_header_event()).unwrap().status,
            200
        );
        assert_eq!(
            runtime
                .block_on(remote.read_body_event(u32::MAX))
                .unwrap()
                .kind,
            TestBodyKind::Body(100),
        );
        assert!(
            runtime
                .block_on(remote.read_body_event(u32::MAX))
                .unwrap()
                .terminal
        );
        assert_eq!(
            recorded_actions(&actions).last(),
            Some(&SourceAction::Drop(owner))
        );

        drop(remote);
        assert!(
            !recorded_actions(&actions)
                .iter()
                .any(|action| matches!(action, SourceAction::Stop { .. }))
        );
        worker.shutdown().expect("managed worker shuts down");
    }

    #[test]
    fn early_outer_drop_stops_inner_once_on_owner() {
        struct DropSource {
            stopped: Option<mpsc::Sender<(ThreadId, Option<TestError>)>>,
            dropped: Option<mpsc::Sender<ThreadId>>,
            _local: Rc<()>,
        }

        impl LocalHttpSource for DropSource {
            type HeaderEvent = TestHeader;
            type BodyEvent = TestBody;
            type Error = TestError;

            fn read_header_event(&mut self) -> LocalSourceFuture<Result<TestHeader, TestError>> {
                Box::pin(std::future::pending())
            }

            fn read_body_event(
                &mut self,
                _maximum: u32,
            ) -> LocalSourceFuture<Result<LocalBodyRead<TestBody>, TestError>> {
                Box::pin(std::future::pending())
            }

            fn stop_reading(&mut self, error: Option<TestError>) {
                self.stopped
                    .take()
                    .expect("stop occurs once")
                    .send((std::thread::current().id(), error))
                    .expect("report stop");
            }

            fn set_read_timeout(&mut self, _timeout: Duration) {}
        }

        impl Drop for DropSource {
            fn drop(&mut self) {
                self.dropped
                    .take()
                    .expect("drop occurs once")
                    .send(std::thread::current().id())
                    .expect("report drop");
            }
        }

        let worker = ManagedWorker::new(4).expect("managed worker starts");
        let owner = worker.owner_thread();
        let (constructed_tx, constructed_rx) = mpsc::channel();
        let (stopped_tx, stopped_rx) = mpsc::channel();
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let remote = RemoteHttpSource::new(&worker.handle(), 1, move |_| {
            constructed_tx.send(()).expect("report construction");
            DropSource {
                stopped: Some(stopped_tx),
                dropped: Some(dropped_tx),
                _local: Rc::new(()),
            }
        })
        .expect("source bridge starts");
        constructed_rx.recv().expect("source constructed");

        drop(remote);
        assert_eq!(stopped_rx.recv().expect("source stopped"), (owner, None));
        assert_eq!(dropped_rx.recv().expect("source dropped"), owner);
        worker.shutdown().expect("managed worker shuts down");
    }
}

mod source_bridge {
    use super::*;

    struct FutureDropProbe {
        dropped: Option<mpsc::Sender<ThreadId>>,
        actions: Arc<Mutex<Vec<SourceAction>>>,
    }

    impl Drop for FutureDropProbe {
        fn drop(&mut self) {
            let owner = std::thread::current().id();
            record(&self.actions, SourceAction::ReadFutureDrop(owner));
            self.dropped
                .take()
                .expect("read future drops once")
                .send(owner)
                .expect("report read future drop");
        }
    }

    struct PendingSource {
        started: Option<mpsc::Sender<()>>,
        future_dropped: Option<mpsc::Sender<ThreadId>>,
        stopped: Option<mpsc::Sender<(ThreadId, Option<TestError>)>>,
        dropped: Option<mpsc::Sender<ThreadId>>,
        actions: Arc<Mutex<Vec<SourceAction>>>,
        _local: Rc<()>,
    }

    impl LocalHttpSource for PendingSource {
        type HeaderEvent = TestHeader;
        type BodyEvent = TestBody;
        type Error = TestError;

        fn read_header_event(&mut self) -> LocalSourceFuture<Result<TestHeader, TestError>> {
            Box::pin(std::future::pending())
        }

        fn read_body_event(
            &mut self,
            _maximum: u32,
        ) -> LocalSourceFuture<Result<LocalBodyRead<TestBody>, TestError>> {
            self.started
                .take()
                .expect("read starts once")
                .send(())
                .expect("report read start");
            let probe = FutureDropProbe {
                dropped: self.future_dropped.take(),
                actions: Arc::clone(&self.actions),
            };
            Box::pin(async move {
                let _probe = probe;
                std::future::pending::<Result<LocalBodyRead<TestBody>, TestError>>().await
            })
        }

        fn stop_reading(&mut self, error: Option<TestError>) {
            let owner = std::thread::current().id();
            record(&self.actions, SourceAction::Stop { owner, error });
            self.stopped
                .take()
                .expect("stop occurs once")
                .send((owner, error))
                .expect("report stop");
        }

        fn set_read_timeout(&mut self, _timeout: Duration) {}
    }

    impl Drop for PendingSource {
        fn drop(&mut self) {
            let owner = std::thread::current().id();
            record(&self.actions, SourceAction::Drop(owner));
            self.dropped
                .take()
                .expect("source drops once")
                .send(owner)
                .expect("report source drop");
        }
    }

    #[test]
    fn remote_stop_reading_races_inflight_read_once() {
        let worker = ManagedWorker::new(8).expect("managed worker starts");
        let owner = worker.owner_thread();
        let (started_tx, started_rx) = mpsc::channel();
        let (future_dropped_tx, future_dropped_rx) = mpsc::channel();
        let (stopped_tx, stopped_rx) = mpsc::channel();
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let actions = Arc::new(Mutex::new(Vec::new()));
        let source_actions = Arc::clone(&actions);
        let mut remote = RemoteHttpSource::new(&worker.handle(), 1, move |_| PendingSource {
            started: Some(started_tx),
            future_dropped: Some(future_dropped_tx),
            stopped: Some(stopped_tx),
            dropped: Some(dropped_tx),
            actions: source_actions,
            _local: Rc::new(()),
        })
        .expect("source bridge starts");
        let abort = remote.abort_handle();
        let abort_thread = std::thread::spawn(move || {
            started_rx.recv().expect("owner read is pending");
            assert!(abort.stop_reading(Some(TestError::Cancelled)));
        });

        assert_eq!(
            runtime().block_on(remote.read_body_event(1)),
            Err(RemoteSourceError::Stopped),
        );
        abort_thread.join().expect("abort thread completes");
        assert_eq!(
            stopped_rx.recv().expect("source receives stop"),
            (owner, Some(TestError::Cancelled)),
        );
        assert_eq!(future_dropped_rx.recv().expect("read future drops"), owner,);
        assert_eq!(dropped_rx.recv().expect("source drops"), owner);
        assert_eq!(
            recorded_actions(&actions),
            vec![
                SourceAction::Stop {
                    owner,
                    error: Some(TestError::Cancelled),
                },
                SourceAction::ReadFutureDrop(owner),
                SourceAction::Drop(owner),
            ],
        );
        assert!(!remote.stop_reading(None));

        drop(remote);
        worker.shutdown().expect("managed worker shuts down");
    }

    #[test]
    fn remote_handle_is_send_but_not_sync() {
        macro_rules! assert_not_sync {
            ($name:ident, $type:ty) => {
                const $name: fn() = || {
                    struct Invalid;
                    trait AmbiguousIfSync<A> {
                        fn marker() {}
                    }
                    impl<T: ?Sized> AmbiguousIfSync<()> for T {}
                    impl<T: ?Sized + Sync> AmbiguousIfSync<Invalid> for T {}
                    let _ = <$type as AmbiguousIfSync<_>>::marker;
                };
            };
        }

        fn assert_send<T: Send>() {}

        assert_send::<RemoteHttpSource<TestHeader, TestBody, TestError>>();
        assert_not_sync!(REMOTE_SOURCE_IS_NOT_SYNC, RemoteHttpSource<TestHeader, TestBody, TestError>);
        let _ = REMOTE_SOURCE_IS_NOT_SYNC;
    }
}
