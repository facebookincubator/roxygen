/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;
use std::cell::RefCell;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use roxygen_core::buf_queue::BufQueue;
use roxygen_core::byte_event::HttpByteEventRegistration;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::http::CodecProtocol;
use roxygen_core::http::HttpMethod;
use roxygen_core::http::TransportDirection;
use roxygen_core::ids::StreamId;
use roxygen_core::message::HttpMessage;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::driver::WorkerDriver;
use roxygen_runtime::wait::CancellableBaton;
use roxygen_runtime::worker::WorkerContext;
use roxygen_server::shutdown::AcceptorRegistration;
use roxygen_server::shutdown::AcceptorShutdownHooks;
use roxygen_server::shutdown::ScopedServerShutdown;
use roxygen_server::shutdown::ServerShutdown;
use roxygen_server::shutdown::ServerShutdownError;
use roxygen_server::shutdown::ServerShutdownHooks;
use roxygen_server::shutdown::ServerShutdownState;
use roxygen_transport::byte_event::ByteEventWriteFlags;
use roxygen_transport::byte_stream::ByteEventCapability;
use roxygen_transport::byte_stream::ByteEventRegistrationError;
use roxygen_transport::byte_stream::LocalTransportFuture;
use roxygen_transport::byte_stream::TransportWriteError;
use roxygen_transport::byte_stream::WriteInfo;

use crate::session::LifecycleEvent;
use crate::session::LifecycleObserver;
use crate::session::LifecycleObserverRegistration;
use crate::session::SessionCore;
use crate::shutdown::DrainError;
use crate::shutdown::DrainLifecycleEvent;
use crate::shutdown::DrainLifecycleObserver;
use crate::shutdown::DrainObserverRegistration;
use crate::shutdown::DrainState;
use crate::shutdown::DrainTrigger;
use crate::shutdown::SessionDrainLifetime;
use crate::uniplex::UniplexConfig;
use crate::uniplex::UniplexHandle;
use crate::uniplex::UniplexRead;
use crate::uniplex::UniplexSession;
use crate::uniplex::UniplexTransport;

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(std::task::Waker::noop()))
}

struct RecordingObserver {
    attached: Rc<Cell<usize>>,
    events: Rc<RefCell<Vec<DrainLifecycleEvent>>>,
    callback: Option<Rc<dyn Fn(DrainLifecycleEvent)>>,
}

struct AtomicDestroyObserver {
    destroyed: Arc<AtomicBool>,
}

struct AtomicCoreDestroyObserver {
    destroyed: Arc<AtomicBool>,
}

impl LifecycleObserver for AtomicCoreDestroyObserver {
    fn on_event(&self, event: LifecycleEvent) {
        if event == LifecycleEvent::Destroy {
            self.destroyed.store(true, Ordering::Release);
        }
    }
}

struct CoreRecordingObserver {
    events: Rc<RefCell<Vec<LifecycleEvent>>>,
    attached: Option<Rc<dyn Fn(LifecycleObserverRegistration)>>,
    callback: Option<Rc<dyn Fn(LifecycleEvent)>>,
}

struct CoreSelfRemovingObserver {
    attached: Rc<Cell<bool>>,
    events: Rc<Cell<usize>>,
}

struct CoreSelfRemovingDropPanicObserver {
    registration: RefCell<Option<LifecycleObserverRegistration>>,
    drops: Rc<Cell<usize>>,
}

struct DrainSelfRemovingDropPanicObserver {
    registration: RefCell<Option<DrainObserverRegistration>>,
    drops: Rc<Cell<usize>>,
}

struct CoreAttachPanicDropObserver {
    drops: Rc<Cell<usize>>,
    events: Rc<Cell<usize>>,
}

struct DrainAttachPanicDropObserver {
    drops: Rc<Cell<usize>>,
    events: Rc<Cell<usize>>,
}

struct PanicOnDrop {
    drops: Rc<Cell<usize>>,
}

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
        panic!("captured callback owner drop");
    }
}

impl LifecycleObserver for CoreSelfRemovingObserver {
    fn on_attached_with_registration(&self, registration: LifecycleObserverRegistration) {
        self.attached.set(true);
        assert!(registration.remove());
    }

    fn on_event(&self, _event: LifecycleEvent) {
        self.events.set(self.events.get() + 1);
    }
}

impl LifecycleObserver for CoreSelfRemovingDropPanicObserver {
    fn on_attached_with_registration(&self, registration: LifecycleObserverRegistration) {
        *self.registration.borrow_mut() = Some(registration);
    }

    fn on_event(&self, event: LifecycleEvent) {
        if matches!(event, LifecycleEvent::DrainStarted(_)) {
            assert!(
                self.registration
                    .borrow()
                    .as_ref()
                    .expect("observer registration")
                    .remove()
            );
        }
    }
}

impl Drop for CoreSelfRemovingDropPanicObserver {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
        panic!("core observer drop");
    }
}

impl DrainLifecycleObserver for DrainSelfRemovingDropPanicObserver {
    fn on_attached(&self, registration: DrainObserverRegistration) {
        *self.registration.borrow_mut() = Some(registration);
    }

    fn on_event(&self, event: DrainLifecycleEvent) {
        if matches!(event, DrainLifecycleEvent::DrainStarted(_)) {
            assert!(
                self.registration
                    .borrow()
                    .as_ref()
                    .expect("observer registration")
                    .remove()
            );
        }
    }
}

impl Drop for DrainSelfRemovingDropPanicObserver {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
        panic!("drain observer drop");
    }
}

impl LifecycleObserver for CoreAttachPanicDropObserver {
    fn on_attached_with_registration(&self, registration: LifecycleObserverRegistration) {
        assert!(registration.remove());
        panic!("core observer attachment");
    }

    fn on_event(&self, _event: LifecycleEvent) {
        self.events.set(self.events.get() + 1);
    }
}

impl Drop for CoreAttachPanicDropObserver {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
        panic!("core observer attachment drop");
    }
}

impl DrainLifecycleObserver for DrainAttachPanicDropObserver {
    fn on_attached(&self, registration: DrainObserverRegistration) {
        assert!(registration.remove());
        panic!("drain observer attachment");
    }

    fn on_event(&self, _event: DrainLifecycleEvent) {
        self.events.set(self.events.get() + 1);
    }
}

impl Drop for DrainAttachPanicDropObserver {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
        panic!("drain observer attachment drop");
    }
}

impl LifecycleObserver for CoreRecordingObserver {
    fn on_attached_with_registration(&self, registration: LifecycleObserverRegistration) {
        if let Some(attached) = &self.attached {
            attached(registration);
        }
    }

    fn on_event(&self, event: LifecycleEvent) {
        self.events.borrow_mut().push(event);
        if let Some(callback) = &self.callback {
            callback(event);
        }
    }
}

fn new_core(direction: TransportDirection) -> (SessionCore, WorkerDriver) {
    let (worker, driver) = WorkerDriver::new(16).unwrap();
    (
        SessionCore::new(direction, CodecProtocol::Http11, worker).unwrap(),
        driver,
    )
}

struct NoopUniplexTransport;

struct PendingDrainHandler;

impl HttpHandler for PendingDrainHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(std::future::pending())
    }
}

impl UniplexTransport for NoopUniplexTransport {
    fn read(
        &self,
        _context: &WorkerContext,
        _maximum: usize,
        _allocation_size: usize,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        Box::pin(std::future::pending())
    }

    fn write(
        &self,
        _context: &WorkerContext,
        data: BufQueue,
        _flags: ByteEventWriteFlags,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        Box::pin(std::future::ready(Ok(WriteInfo {
            bytes_written: data.chain_length(),
        })))
    }

    fn shutdown_write(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        Ok(())
    }

    fn close(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        Ok(())
    }

    fn close_with_reset(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        Ok(())
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

impl DrainLifecycleObserver for AtomicDestroyObserver {
    fn on_event(&self, event: DrainLifecycleEvent) {
        if event == DrainLifecycleEvent::Destroy {
            self.destroyed.store(true, Ordering::Release);
        }
    }
}

impl DrainLifecycleObserver for RecordingObserver {
    fn on_attached(&self, _registration: DrainObserverRegistration) {
        self.attached.set(self.attached.get() + 1);
    }

    fn on_event(&self, event: DrainLifecycleEvent) {
        self.events.borrow_mut().push(event);
        if let Some(callback) = &self.callback {
            callback(event);
        }
    }
}

fn observer(events: Rc<RefCell<Vec<DrainLifecycleEvent>>>) -> Rc<dyn DrainLifecycleObserver> {
    Rc::new(RecordingObserver {
        attached: Rc::new(Cell::new(0)),
        events,
        callback: None,
    })
}

fn assert_pending<F: Future>(future: Pin<&mut F>) {
    assert!(poll_once(future).is_pending());
}

type OrderLog = Rc<RefCell<Vec<(&'static str, LifecycleEvent)>>>;
type RegistrationSlot = Rc<RefCell<Option<LifecycleObserverRegistration>>>;

/// Records `(label, event)` into a log shared by several observers.
struct OrderObserver {
    label: &'static str,
    log: OrderLog,
    registration: RegistrationSlot,
    callback: Option<Rc<dyn Fn(LifecycleEvent, &LifecycleObserverRegistration)>>,
}

impl LifecycleObserver for OrderObserver {
    fn on_attached_with_registration(&self, registration: LifecycleObserverRegistration) {
        *self.registration.borrow_mut() = Some(registration);
    }

    fn on_event(&self, event: LifecycleEvent) {
        self.log.borrow_mut().push((self.label, event));
        if let Some(callback) = &self.callback {
            let registration = self.registration.borrow().clone();
            callback(event, registration.as_ref().expect("attached observer"));
        }
    }
}

fn order_observer(
    label: &'static str,
    log: &OrderLog,
    callback: Option<Rc<dyn Fn(LifecycleEvent, &LifecycleObserverRegistration)>>,
) -> (Rc<OrderObserver>, RegistrationSlot) {
    let registration = Rc::new(RefCell::new(None));
    (
        Rc::new(OrderObserver {
            label,
            log: Rc::clone(log),
            registration: Rc::clone(&registration),
            callback,
        }),
        registration,
    )
}

/// The labels that observed `event`, in delivery order.
fn labels_for(log: &OrderLog, event: LifecycleEvent) -> Vec<&'static str> {
    log.borrow()
        .iter()
        .filter(|(_, observed)| *observed == event)
        .map(|(label, _)| *label)
        .collect()
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            payload
                .downcast_ref::<&str>()
                .map(|message| (*message).to_owned())
        })
        .unwrap_or_default()
}

struct NoopLifecycleObserver;

impl LifecycleObserver for NoopLifecycleObserver {}

/// A transport whose reads end only when cancelled and whose writes complete immediately.
struct CancellableReadTransport;

impl UniplexTransport for CancellableReadTransport {
    fn read(
        &self,
        _context: &WorkerContext,
        _maximum: usize,
        _allocation_size: usize,
        _timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        Box::pin(async move {
            let baton = CancellableBaton::new();
            let _ = baton.wait(cancellation).await;
            Err(HttpError::with_message(
                HttpErrorCode::CORO_CANCELLED,
                b"idle read cancelled",
            ))
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
        Box::pin(std::future::ready(Ok(WriteInfo {
            bytes_written: data.chain_length(),
        })))
    }

    fn shutdown_write(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        Ok(())
    }

    fn close(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        Ok(())
    }

    fn close_with_reset(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        Ok(())
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

pub mod h1 {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn h1_upstream_session_test_drain_after_codec_not_reusable() {
        let (mut session, _driver) = new_core(TransportDirection::Upstream);
        let mut transaction = session.reserve_request().unwrap();
        assert!(session.initiate_drain_with(DrainTrigger::CodecNotReusable));
        assert_eq!(
            session.drain_trigger(),
            Some(DrainTrigger::CodecNotReusable)
        );
        assert_eq!(
            session
                .reserve_request()
                .err()
                .expect("drain rejects a later reservation")
                .code(),
            HttpErrorCode::REFUSED_STREAM
        );
        transaction.cancel();
        session.finalize().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn h1_upstream_session_test_receive_goaway() {
        let (mut session, _driver) = new_core(TransportDirection::Upstream);
        let mut active = session.reserve_request().unwrap();
        assert!(session.initiate_drain_with(DrainTrigger::PeerGoaway));
        assert_eq!(session.drain_trigger(), Some(DrainTrigger::PeerGoaway));
        assert!(session.reserve_request().is_err());
        active.cancel();
        session.finalize().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn h1_upstream_session_test_send_goaway() {
        let (mut session, _driver) = new_core(TransportDirection::Upstream);
        let mut active = session.reserve_request().unwrap();
        assert!(session.initiate_drain_with(DrainTrigger::Local));
        assert!(!session.initiate_drain_with(DrainTrigger::PeerGoaway));
        assert_eq!(session.drain_trigger(), Some(DrainTrigger::Local));
        active.cancel();
        session.finalize().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn h1_upstream_session_test_drain_after_failed_upgrade() {
        let (mut session, _driver) = new_core(TransportDirection::Upstream);
        let events = Rc::new(RefCell::new(Vec::new()));
        session.add_lifecycle_observer(Rc::new(CoreRecordingObserver {
            events: Rc::clone(&events),
            attached: None,
            callback: None,
        }));
        assert!(session.initiate_drain_with(DrainTrigger::CodecNotReusable));
        assert!(!session.initiate_drain_with(DrainTrigger::CodecNotReusable));
        assert_eq!(
            events
                .borrow()
                .iter()
                .filter(|event| matches!(event, LifecycleEvent::DrainStarted(_)))
                .count(),
            1
        );
        session.finalize().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn httpupstream_session_test_send_goaway() {
        let (mut session, _driver) = new_core(TransportDirection::Upstream);
        let mut active = session.reserve_request().unwrap();
        assert!(session.initiate_drain());
        assert_eq!(
            session
                .reserve_request()
                .err()
                .expect("GOAWAY rejects a later reservation")
                .code(),
            HttpErrorCode::REFUSED_STREAM
        );
        active.cancel();
        session.finalize().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn httpupstream_session_test_lifecycle_observer_test_nullptr() {
        let session = SessionDrainLifetime::new();
        assert_eq!(
            session.try_add_observer(None),
            Err(DrainError::MissingObserver)
        );
        let _ = session.finish().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn httpupstream_session_test_lifecycle_observer_tests() {
        let (session, _driver) = new_core(TransportDirection::Upstream);
        let first_events = Rc::new(RefCell::new(Vec::new()));
        let later_events = Rc::new(RefCell::new(Vec::new()));
        let later_events_for_callback = Rc::clone(&later_events);
        let registration = Rc::new(RefCell::new(None::<LifecycleObserverRegistration>));
        let registration_on_attach = Rc::clone(&registration);
        let registration_for_callback = Rc::clone(&registration);
        let added = Rc::new(Cell::new(false));
        let added_for_callback = Rc::clone(&added);
        session.add_lifecycle_observer(Rc::new(CoreRecordingObserver {
            events: Rc::clone(&first_events),
            attached: Some(Rc::new(move |value| {
                *registration_on_attach.borrow_mut() = Some(value)
            })),
            callback: Some(Rc::new(move |event| {
                if event == LifecycleEvent::TransactionAttached && !added_for_callback.replace(true)
                {
                    registration_for_callback
                        .borrow()
                        .as_ref()
                        .expect("observer registration")
                        .add_observer(Rc::new(CoreRecordingObserver {
                            events: Rc::clone(&later_events_for_callback),
                            attached: None,
                            callback: None,
                        }));
                }
            })),
        }));
        let self_removed_attached = Rc::new(Cell::new(false));
        let self_removed_events = Rc::new(Cell::new(0));
        session.add_lifecycle_observer(Rc::new(CoreSelfRemovingObserver {
            attached: Rc::clone(&self_removed_attached),
            events: Rc::clone(&self_removed_events),
        }));
        assert!(self_removed_attached.get());
        let mut transaction = session.reserve_request().unwrap();
        assert!(later_events.borrow().is_empty());
        transaction.cancel();
        assert_eq!(
            &*later_events.borrow(),
            &[
                LifecycleEvent::TransactionDetached { stream: None },
                LifecycleEvent::DeactivateConnection,
            ]
        );
        assert_eq!(self_removed_events.get(), 0);
        session.finalize().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn httpupstream_session_test_idle_timeout_no_streams() {
        let (mut session, _driver) = new_core(TransportDirection::Upstream);
        assert!(session.initiate_drain_with(DrainTrigger::IdleTimeout));
        assert_eq!(session.num_transactions(), 0);
        session.finalize().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn httpupstream_session_test_session_cancelled_no_streams() {
        let (mut session, _driver) = new_core(TransportDirection::Upstream);
        session.fail_connection(roxygen_core::error::HttpError::new(
            HttpErrorCode::CORO_CANCELLED,
        ));
        assert_eq!(
            session.connection_error().unwrap().code(),
            HttpErrorCode::CORO_CANCELLED
        );
        session.finalize().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn httpupstream_session_test_session_cancelled_with_stream() {
        let (mut session, _driver) = new_core(TransportDirection::Upstream);
        let mut transaction = session.reserve_request().unwrap();
        session.fail_connection(roxygen_core::error::HttpError::new(
            HttpErrorCode::CORO_CANCELLED,
        ));
        assert_eq!(session.num_transactions(), 1);
        transaction.cancel();
        session.finalize().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn httpupstream_session_test_session_drop_no_streams() {
        let (mut session, _driver) = new_core(TransportDirection::Upstream);
        session.fail_connection(roxygen_core::error::HttpError::new(HttpErrorCode::DROPPED));
        assert_eq!(
            session.connection_error().unwrap().code(),
            HttpErrorCode::DROPPED
        );
        session.finalize().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn httpupstream_session_test_session_drop_with_stream() {
        let (mut session, _driver) = new_core(TransportDirection::Upstream);
        let mut transaction = session.reserve_request().unwrap();
        session.fail_connection(roxygen_core::error::HttpError::new(HttpErrorCode::DROPPED));
        assert_eq!(session.num_transactions(), 1);
        transaction.cancel();
        session.finalize().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn h1_upstream_session_test_drain_start_on_reset() {
        let (mut session, _driver) = new_core(TransportDirection::Upstream);
        let mut transaction = session.reserve_request().unwrap();
        assert!(session.initiate_drain_with(DrainTrigger::StreamReset));
        assert_eq!(session.drain_trigger(), Some(DrainTrigger::StreamReset));
        assert!(session.reserve_request().is_err());
        transaction.cancel();
        session.finalize().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn httpdownstream_session_test_lifecycle_observer() {
        let (mut session, _driver) = new_core(TransportDirection::Upstream);
        let events = Rc::new(RefCell::new(Vec::new()));
        session.add_lifecycle_observer(Rc::new(CoreRecordingObserver {
            events: Rc::clone(&events),
            attached: None,
            callback: None,
        }));
        let mut transaction = session.reserve_request().unwrap();
        session.initiate_drain_with(DrainTrigger::Local);
        transaction.cancel();
        session.finalize().await.unwrap();
        assert_eq!(
            &*events.borrow(),
            &[
                LifecycleEvent::ActivateConnection,
                LifecycleEvent::TransactionAttached,
                LifecycleEvent::DrainStarted(DrainTrigger::Local),
                LifecycleEvent::TransactionDetached { stream: None },
                LifecycleEvent::DeactivateConnection,
                LifecycleEvent::Destroy,
            ]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn httpdownstream_session_test_lifecycle_observer_remove_callback() {
        let (session, _driver) = new_core(TransportDirection::Upstream);
        let removed_events = Rc::new(RefCell::new(Vec::new()));
        let removed_registration = Rc::new(RefCell::new(None::<LifecycleObserverRegistration>));
        let registration_slot = Rc::clone(&removed_registration);
        session.add_lifecycle_observer(Rc::new(CoreRecordingObserver {
            events: Rc::clone(&removed_events),
            attached: Some(Rc::new(move |registration| {
                *registration_slot.borrow_mut() = Some(registration)
            })),
            callback: None,
        }));
        let remover_events = Rc::new(RefCell::new(Vec::new()));
        let registration_for_callback = Rc::clone(&removed_registration);
        session.add_lifecycle_observer(Rc::new(CoreRecordingObserver {
            events: Rc::clone(&remover_events),
            attached: None,
            callback: Some(Rc::new(move |event| {
                if event == LifecycleEvent::TransactionAttached {
                    assert!(
                        registration_for_callback
                            .borrow()
                            .as_ref()
                            .expect("observer registration")
                            .remove()
                    );
                }
            })),
        }));
        let mut transaction = session.reserve_request().unwrap();
        transaction.cancel();
        session.finalize().await.unwrap();
        assert_eq!(
            &*removed_events.borrow(),
            &[
                LifecycleEvent::ActivateConnection,
                LifecycleEvent::TransactionAttached,
            ]
        );
        assert!(remover_events.borrow().len() > removed_events.borrow().len());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn httpdownstream_session_test_multi_lifecycle_observer() {
        let (session, _driver) = new_core(TransportDirection::Upstream);
        let first = Rc::new(RefCell::new(Vec::new()));
        let second = Rc::new(RefCell::new(Vec::new()));
        for events in [Rc::clone(&first), Rc::clone(&second)] {
            session.add_lifecycle_observer(Rc::new(CoreRecordingObserver {
                events,
                attached: None,
                callback: None,
            }));
        }
        let mut transaction = session.reserve_request().unwrap();
        transaction.cancel();
        session.finalize().await.unwrap();
        assert_eq!(&*first.borrow(), &*second.borrow());
        assert_eq!(first.borrow().last(), Some(&LifecycleEvent::Destroy));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn h1_downstream_session_test_non_keep_alive_response() {
        let (mut session, _driver) = new_core(TransportDirection::Upstream);
        let mut transaction = session.reserve_request().unwrap();
        assert!(session.initiate_drain_with(DrainTrigger::CodecNotReusable));
        assert!(session.reserve_request().is_err());
        transaction.cancel();
        session.finalize().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn h1_downstream_session_test_anti_pipeline() {
        let (mut session, _driver) = new_core(TransportDirection::Downstream);
        let first = session.begin_incoming_stream(StreamId::new(1)).unwrap();
        let lookahead = session.begin_incoming_stream(StreamId::new(2)).unwrap();
        session.initiate_drain_with(DrainTrigger::Local);
        assert_eq!(session.num_transactions(), 2);
        assert!(session.begin_incoming_stream(StreamId::new(3)).is_err());
        session.fail_connection(roxygen_core::error::HttpError::new(
            HttpErrorCode::CORO_CANCELLED,
        ));
        drop(first);
        session.process_pending_events().unwrap();
        assert_eq!(session.num_transactions(), 1);
        drop(lookahead);
        session.process_pending_events().unwrap();
        session.finalize().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn h1_downstream_session_test_anti_pipeline_cancel() {
        let (mut session, _driver) = new_core(TransportDirection::Downstream);
        let first = session.begin_incoming_stream(StreamId::new(1)).unwrap();
        let lookahead = session.begin_incoming_stream(StreamId::new(2)).unwrap();
        session.fail_connection(roxygen_core::error::HttpError::new(
            HttpErrorCode::CORO_CANCELLED,
        ));
        assert!(session.begin_incoming_stream(StreamId::new(3)).is_err());
        drop(first);
        drop(lookahead);
        session.process_pending_events().unwrap();
        session.finalize().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn httpdownstream_session_test_hold_context() {
        let (worker, driver) = WorkerDriver::new(8).unwrap();
        worker
            .spawn_local(move |context| async move {
                let mut session = SessionCore::new(
                    TransportDirection::Downstream,
                    CodecProtocol::Http11,
                    context.handle().clone(),
                )
                .unwrap();
                let held_context = session
                    .acquire_session_handle(context.handle().clone())
                    .unwrap();
                session.initiate_drain_with(DrainTrigger::Local);
                let mut finish = Box::pin(session.finalize());
                assert_pending(finish.as_mut());
                std::thread::spawn(move || drop(held_context))
                    .join()
                    .unwrap();
                finish.await.unwrap();
                context.handle().begin_shutdown();
            })
            .unwrap();
        driver.await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn h1_downstream_session_test_receive_goaway() {
        let (mut session, _driver) = new_core(TransportDirection::Upstream);
        let mut transaction = session.reserve_request().unwrap();
        session.initiate_drain_with(DrainTrigger::PeerGoaway);
        assert!(session.reserve_request().is_err());
        transaction.cancel();
        session.finalize().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn httpdownstream_session_test_send_goaway() {
        let (mut session, _driver) = new_core(TransportDirection::Upstream);
        let mut active = session.reserve_request().unwrap();
        session.initiate_drain_with(DrainTrigger::Local);
        assert!(session.reserve_request().is_err());
        active.cancel();
        session.finalize().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn httpdownstream_session_test_close_when_idle_no_streams() {
        let (mut session, _driver) = new_core(TransportDirection::Upstream);
        assert!(session.initiate_drain_with(DrainTrigger::CloseWhenIdle));
        assert_eq!(session.drain_trigger(), Some(DrainTrigger::CloseWhenIdle));
        session.finalize().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn httpdownstream_session_test_close_when_idle_with_stream() {
        let (mut session, _driver) = new_core(TransportDirection::Upstream);
        let mut transaction = session.reserve_request().unwrap();
        assert!(session.initiate_drain_with(DrainTrigger::CloseWhenIdle));
        assert_eq!(session.num_transactions(), 1);
        transaction.cancel();
        session.finalize().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn httpdownstream_session_test_idle_timeout_no_streams() {
        let (mut session, _driver) = new_core(TransportDirection::Upstream);
        session.initiate_drain_with(DrainTrigger::IdleTimeout);
        assert_eq!(session.drain_trigger(), Some(DrainTrigger::IdleTimeout));
        session.finalize().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn uniplex_lifecycle_observers_past_inline_capacity_run_newest_first() {
        let (worker, driver) = WorkerDriver::new(16).unwrap();
        worker
            .spawn_local(move |context| async move {
                let transport: Rc<dyn UniplexTransport> = Rc::new(NoopUniplexTransport);
                let (session, handle) = UniplexSession::new(
                    context.clone(),
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                )
                .unwrap();
                let log = OrderLog::default();
                let mut registrations = Vec::new();
                for label in ["a", "b", "c", "d"] {
                    let (observer, registration) = order_observer(label, &log, None);
                    session.add_lifecycle_observer(observer).unwrap();
                    registrations.push(registration);
                }
                let mut reservation = handle.reserve_request().unwrap();
                assert!(reservation.cancel());
                let detached = LifecycleEvent::TransactionDetached { stream: None };
                assert_eq!(labels_for(&log, detached), ["d", "c", "b", "a"]);

                let middle = registrations[1].borrow().clone().unwrap();
                assert!(middle.remove());
                assert!(!middle.remove());
                log.borrow_mut().clear();
                let mut reservation = handle.reserve_request().unwrap();
                assert!(reservation.cancel());
                assert_eq!(labels_for(&log, detached), ["d", "c", "a"]);

                // An observer added during dispatch receives only later events.
                let later_log = Rc::clone(&log);
                let (adder, _) = order_observer(
                    "e",
                    &log,
                    Some(Rc::new(move |event, registration| {
                        if event == LifecycleEvent::TransactionAttached {
                            let (later, _) = order_observer("f", &later_log, None);
                            registration.add_observer(later);
                        }
                    })),
                );
                session.add_lifecycle_observer(adder).unwrap();
                log.borrow_mut().clear();
                let mut reservation = handle.reserve_request().unwrap();
                assert_eq!(
                    labels_for(&log, LifecycleEvent::TransactionAttached),
                    ["e", "d", "c", "a"]
                );
                assert!(reservation.cancel());
                assert_eq!(labels_for(&log, detached), ["f", "e", "d", "c", "a"]);
                drop(session);
                drop(handle);
                context.handle().begin_shutdown();
            })
            .unwrap();
        driver.await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn uniplex_lifecycle_registration_after_session_drop_reports_false_and_add_panics() {
        let (worker, driver) = WorkerDriver::new(16).unwrap();
        worker
            .spawn_local(move |context| async move {
                let transport: Rc<dyn UniplexTransport> = Rc::new(CancellableReadTransport);
                let (session, handle) = UniplexSession::new(
                    context.clone(),
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                )
                .unwrap();
                let log = OrderLog::default();
                let (observer, slot) = order_observer("a", &log, None);
                session.add_lifecycle_observer(observer).unwrap();
                let registration = slot.borrow_mut().take().unwrap();
                let run = session.run();
                handle.initiate_drain().unwrap();
                run.await.unwrap();
                drop(handle);
                assert!(!registration.remove());
                let panic = catch_unwind(AssertUnwindSafe(|| {
                    registration.add_observer(Rc::new(NoopLifecycleObserver))
                }))
                .unwrap_err();
                assert_eq!(
                    panic_message(panic.as_ref()),
                    "an attachment callback retains its session"
                );
                context.handle().begin_shutdown();
            })
            .unwrap();
        driver.await;
    }
}

pub mod session {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn uniplex_drain_callbacks_reenter_without_driver_state_borrow() {
        let (worker, driver) = WorkerDriver::new(16).unwrap();
        worker
            .spawn_local(move |context| async move {
                for (expected, path) in [
                    (DrainTrigger::StreamReset, 0),
                    (DrainTrigger::Local, 1),
                    (DrainTrigger::Local, 2),
                ] {
                    let transport: Rc<dyn UniplexTransport> = Rc::new(NoopUniplexTransport);
                    let (session, handle) = UniplexSession::new(
                        context.clone(),
                        TransportDirection::Upstream,
                        transport,
                        None,
                        UniplexConfig::default(),
                    )
                    .unwrap();
                    let events = Rc::new(RefCell::new(Vec::new()));
                    let reentered = Rc::new(Cell::new(false));
                    let handle_for_callback = handle.clone();
                    let reentered_for_callback = Rc::clone(&reentered);
                    session
                        .add_lifecycle_observer(Rc::new(CoreRecordingObserver {
                            events: Rc::clone(&events),
                            attached: None,
                            callback: Some(Rc::new(move |event| {
                                if matches!(event, LifecycleEvent::DrainStarted(_)) {
                                    assert!(!handle_for_callback.initiate_drain().unwrap());
                                    assert!(handle_for_callback.reserve_request().is_err());
                                    reentered_for_callback.set(true);
                                    panic!("drain observer");
                                }
                            })),
                        }))
                        .unwrap();

                    let transition = catch_unwind(AssertUnwindSafe(|| match path {
                        0 => {
                            session.trigger_batched_stream_error_drain_for_test(
                                &handle,
                                StreamId::new(1),
                                StreamId::new(3),
                            );
                        }
                        1 => session.trigger_deferred_drain_for_test(&handle).unwrap(),
                        2 => {
                            handle.initiate_drain().unwrap();
                        }
                        _ => unreachable!(),
                    }));

                    assert!(transition.is_err());
                    assert!(reentered.get());
                    assert_eq!(
                        events.borrow().as_slice(),
                        &[LifecycleEvent::DrainStarted(expected)]
                    );
                    assert!(!handle.initiate_drain().unwrap());
                    if path == 0 {
                        assert!(!session.has_ingress_meta_for_test(StreamId::new(3)));
                        assert!(session.reset_after_draining_writes_for_test());
                    }
                    drop(session);
                }
                context.handle().begin_shutdown();
            })
            .unwrap();
        driver.await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn uniplex_ingress_and_egress_lifecycle_callbacks_reenter_safely() {
        let (worker, driver) = WorkerDriver::new(16).unwrap();
        worker
            .spawn_local(move |context| async move {
                let transport: Rc<dyn UniplexTransport> = Rc::new(NoopUniplexTransport);
                let (session, handle) = UniplexSession::new(
                    context.clone(),
                    TransportDirection::Downstream,
                    transport,
                    Some(Rc::new(PendingDrainHandler)),
                    UniplexConfig::default(),
                )
                .unwrap();
                let events = Rc::new(RefCell::new(Vec::new()));
                let handle_for_callback = handle.clone();
                session
                    .add_lifecycle_observer(Rc::new(CoreRecordingObserver {
                        events: Rc::clone(&events),
                        attached: None,
                        callback: Some(Rc::new(move |event| {
                            if matches!(
                                event,
                                LifecycleEvent::ActivateConnection
                                    | LifecycleEvent::TransactionAttached
                                    | LifecycleEvent::RequestBegin { .. }
                                    | LifecycleEvent::RequestEnd { .. }
                            ) {
                                handle_for_callback.initiate_drain().unwrap();
                            }
                        })),
                    }))
                    .unwrap();

                session
                    .trigger_ingress_and_egress_lifecycle_for_test(&handle, StreamId::new(1))
                    .unwrap();
                assert!(
                    events
                        .borrow()
                        .contains(&LifecycleEvent::ActivateConnection)
                );
                assert!(
                    events
                        .borrow()
                        .contains(&LifecycleEvent::TransactionAttached)
                );
                assert!(events.borrow().contains(&LifecycleEvent::RequestBegin {
                    stream: StreamId::new(1),
                    sequence_number: 0,
                }));
                assert!(events.borrow().contains(&LifecycleEvent::RequestEnd {
                    stream: StreamId::new(1),
                }));
                assert_eq!(
                    events
                        .borrow()
                        .iter()
                        .filter(|event| matches!(event, LifecycleEvent::DrainStarted(_)))
                        .count(),
                    1
                );
                drop(session);
                context.handle().begin_shutdown();
            })
            .unwrap();
        driver.await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn uniplex_reservation_release_flushes_detach_lifecycle_immediately() {
        let (worker, driver) = WorkerDriver::new(16).unwrap();
        worker
            .spawn_local(move |context| async move {
                for abandon_prepared in [false, true] {
                    let transport: Rc<dyn UniplexTransport> = Rc::new(NoopUniplexTransport);
                    let (session, handle) = UniplexSession::new(
                        context.clone(),
                        TransportDirection::Upstream,
                        transport,
                        None,
                        UniplexConfig::default(),
                    )
                    .unwrap();
                    let events = Rc::new(RefCell::new(Vec::new()));
                    let reentered = Rc::new(Cell::new(false));
                    let handle_for_callback = handle.clone();
                    let reentered_for_callback = Rc::clone(&reentered);
                    session
                        .add_lifecycle_observer(Rc::new(CoreRecordingObserver {
                            events: Rc::clone(&events),
                            attached: None,
                            callback: Some(Rc::new(move |event| {
                                if event == (LifecycleEvent::TransactionDetached { stream: None }) {
                                    assert!(handle_for_callback.initiate_drain().unwrap());
                                    reentered_for_callback.set(true);
                                }
                            })),
                        }))
                        .unwrap();
                    let mut reservation = handle.reserve_request().unwrap();
                    events.borrow_mut().clear();

                    if abandon_prepared {
                        let mut message = HttpMessage::new();
                        message.set_url(b"/abandoned", false).unwrap();
                        message.set_method(HttpMethod::Get).unwrap();
                        message.set_http_version(1, 1);
                        let prepared = UniplexHandle::prepare_request_with_headers(
                            HttpHeaderEvent::new(message, true).unwrap(),
                            HttpSourceHolder::new(Box::new(HttpFixedSource::new(
                                HttpMessage::new(),
                                None,
                            ))),
                            reservation,
                        );
                        drop(prepared);
                    } else {
                        assert!(reservation.cancel());
                    }

                    assert!(reentered.get());
                    assert_eq!(
                        events.borrow().as_slice(),
                        &[
                            LifecycleEvent::TransactionDetached { stream: None },
                            LifecycleEvent::DeactivateConnection,
                            LifecycleEvent::DrainStarted(DrainTrigger::Local),
                        ]
                    );
                    drop(session);
                }
                context.handle().begin_shutdown();
            })
            .unwrap();
        driver.await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn byte_event_admission_is_scoped_to_an_admitted_transaction() {
        let lifetime = SessionDrainLifetime::new();
        let transaction = lifetime.admit_transaction().unwrap();
        lifetime.initiate_drain(DrainTrigger::Local);
        assert!(matches!(
            lifetime.track_byte_event(),
            Err(DrainError::AdmissionClosed)
        ));
        let event = transaction.track_byte_event().unwrap();
        lifetime.session_cancelled();
        assert!(matches!(
            transaction.track_byte_event(),
            Err(DrainError::AdmissionClosed)
        ));
        drop(event);
        drop(transaction);
        assert_eq!(
            lifetime.finish().await.unwrap().code(),
            HttpErrorCode::CORO_CANCELLED
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn drain_waits_for_external_keepalives_after_tasks() {
        let (worker, driver) = WorkerDriver::new(8).unwrap();
        let destroyed = Arc::new(AtomicBool::new(false));
        let destroyed_in_task = Arc::clone(&destroyed);
        worker
            .spawn_local(move |context| async move {
                let mut session = SessionCore::new(
                    TransportDirection::Upstream,
                    CodecProtocol::Http11,
                    context.handle().clone(),
                )
                .unwrap();
                session.add_lifecycle_observer(Rc::new(AtomicCoreDestroyObserver {
                    destroyed: Arc::clone(&destroyed_in_task),
                }));
                let mut transaction = session.reserve_request().unwrap();
                let external_keep_alive = session
                    .acquire_session_handle(context.handle().clone())
                    .unwrap();
                session.initiate_drain_with(DrainTrigger::Local);
                transaction.cancel();
                let mut finish = Box::pin(session.finalize());
                assert_pending(finish.as_mut());
                std::thread::spawn(move || drop(external_keep_alive))
                    .join()
                    .unwrap();
                finish.await.unwrap();
                assert!(destroyed_in_task.load(Ordering::Acquire));
                context.handle().begin_shutdown();
            })
            .unwrap();
        driver.await;
        assert!(destroyed.load(Ordering::Acquire));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn lifecycle_panic_notifies_siblings_and_preserves_destroy() {
        let lifetime = SessionDrainLifetime::new();
        let sibling_events = Rc::new(RefCell::new(Vec::new()));
        lifetime
            .try_add_observer(Some(observer(Rc::clone(&sibling_events))))
            .unwrap();
        lifetime
            .try_add_observer(Some(Rc::new(RecordingObserver {
                attached: Rc::new(Cell::new(0)),
                events: Rc::new(RefCell::new(Vec::new())),
                callback: Some(Rc::new(|event| {
                    if matches!(event, DrainLifecycleEvent::DrainStarted(_)) {
                        panic!("lifecycle callback");
                    }
                })),
            })))
            .unwrap();

        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                lifetime.initiate_drain(DrainTrigger::Local)
            }))
            .is_err()
        );
        assert!(
            sibling_events
                .borrow()
                .contains(&DrainLifecycleEvent::DrainStarted(DrainTrigger::Local))
        );
        assert!(lifetime.finish().await.is_none());
        assert_eq!(
            sibling_events.borrow().last(),
            Some(&DrainLifecycleEvent::Destroy)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn self_removing_observer_drop_panics_do_not_skip_shutdown_work() {
        let (mut core, _driver) = new_core(TransportDirection::Upstream);
        let core_sibling_events = Rc::new(RefCell::new(Vec::new()));
        core.add_lifecycle_observer(Rc::new(CoreRecordingObserver {
            events: Rc::clone(&core_sibling_events),
            attached: None,
            callback: None,
        }));
        let core_drops = Rc::new(Cell::new(0));
        core.add_lifecycle_observer(Rc::new(CoreSelfRemovingDropPanicObserver {
            registration: RefCell::new(None),
            drops: Rc::clone(&core_drops),
        }));
        let error = HttpError::new(HttpErrorCode::TRANSPORT_READ_ERROR);
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                core.fail_connection(error.clone());
            }))
            .is_err()
        );
        assert_eq!(core_drops.get(), 1);
        assert!(core.is_draining());
        assert_eq!(core.connection_error().unwrap().code(), error.code());
        assert!(
            core_sibling_events
                .borrow()
                .contains(&LifecycleEvent::DrainStarted(DrainTrigger::ConnectionError))
        );
        core.finalize().await.unwrap();

        let lifetime = SessionDrainLifetime::new();
        let drain_sibling_events = Rc::new(RefCell::new(Vec::new()));
        lifetime
            .try_add_observer(Some(observer(Rc::clone(&drain_sibling_events))))
            .unwrap();
        let drain_drops = Rc::new(Cell::new(0));
        lifetime
            .try_add_observer(Some(Rc::new(DrainSelfRemovingDropPanicObserver {
                registration: RefCell::new(None),
                drops: Rc::clone(&drain_drops),
            })))
            .unwrap();
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                lifetime.initiate_drain(DrainTrigger::Local);
            }))
            .is_err()
        );
        assert_eq!(drain_drops.get(), 1);
        assert!(
            drain_sibling_events
                .borrow()
                .contains(&DrainLifecycleEvent::DrainStarted(DrainTrigger::Local))
        );
        assert!(lifetime.finish().await.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn attachment_callback_and_observer_drop_panics_are_isolated() {
        let (mut core, _driver) = new_core(TransportDirection::Upstream);
        let core_drops = Rc::new(Cell::new(0));
        let core_events = Rc::new(Cell::new(0));
        let core_panic = catch_unwind(AssertUnwindSafe(|| {
            core.add_lifecycle_observer(Rc::new(CoreAttachPanicDropObserver {
                drops: Rc::clone(&core_drops),
                events: Rc::clone(&core_events),
            }));
        }))
        .unwrap_err();
        assert_eq!(
            core_panic.downcast_ref::<&str>().copied(),
            Some("core observer attachment")
        );
        assert_eq!(core_drops.get(), 1);
        assert!(core.initiate_drain());
        core.finalize().await.unwrap();
        assert_eq!(core_events.get(), 0);

        let lifetime = SessionDrainLifetime::new();
        let drain_drops = Rc::new(Cell::new(0));
        let drain_events = Rc::new(Cell::new(0));
        let drain_panic = catch_unwind(AssertUnwindSafe(|| {
            lifetime
                .try_add_observer(Some(Rc::new(DrainAttachPanicDropObserver {
                    drops: Rc::clone(&drain_drops),
                    events: Rc::clone(&drain_events),
                })))
                .unwrap();
        }))
        .unwrap_err();
        assert_eq!(
            drain_panic.downcast_ref::<&str>().copied(),
            Some("drain observer attachment")
        );
        assert_eq!(drain_drops.get(), 1);
        assert!(lifetime.initiate_drain(DrainTrigger::Local));
        assert!(lifetime.finish().await.is_none());
        assert_eq!(drain_events.get(), 0);

        let (worker, driver) = WorkerDriver::new(8).unwrap();
        worker
            .spawn_local(move |context| async move {
                let uniplex_drops = Rc::new(Cell::new(0));
                let uniplex_events = Rc::new(Cell::new(0));
                let transport: Rc<dyn UniplexTransport> = Rc::new(NoopUniplexTransport);
                let (session, handle) = UniplexSession::new(
                    context.clone(),
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                )
                .unwrap();
                let panic = catch_unwind(AssertUnwindSafe(|| {
                    session
                        .add_lifecycle_observer(Rc::new(CoreAttachPanicDropObserver {
                            drops: Rc::clone(&uniplex_drops),
                            events: Rc::clone(&uniplex_events),
                        }))
                        .unwrap();
                }))
                .unwrap_err();
                assert_eq!(
                    panic.downcast_ref::<&str>().copied(),
                    Some("core observer attachment")
                );
                assert_eq!(uniplex_drops.get(), 1);
                assert!(handle.initiate_drain().unwrap());
                assert_eq!(uniplex_events.get(), 0);
                drop(session);
                context.handle().begin_shutdown();
            })
            .unwrap();
        driver.await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn detach_reentrant_admission_suppresses_false_deactivate() {
        let lifetime = SessionDrainLifetime::new();
        let handle = lifetime.handle();
        let replacement = Rc::new(RefCell::new(None));
        let replacement_for_callback = Rc::clone(&replacement);
        let handle_for_callback = handle.clone();
        let events = Rc::new(RefCell::new(Vec::new()));
        let observer_id = lifetime
            .try_add_observer(Some(Rc::new(RecordingObserver {
                attached: Rc::new(Cell::new(0)),
                events: Rc::clone(&events),
                callback: Some(Rc::new(move |event| {
                    if event == DrainLifecycleEvent::TransactionDetached
                        && replacement_for_callback.borrow().is_none()
                    {
                        *replacement_for_callback.borrow_mut() =
                            Some(handle_for_callback.admit_transaction().unwrap());
                    }
                })),
            })))
            .unwrap();
        let original = lifetime.admit_transaction().unwrap();
        events.borrow_mut().clear();
        drop(original);
        assert!(
            !events
                .borrow()
                .contains(&DrainLifecycleEvent::DeactivateConnection)
        );
        assert!(lifetime.remove_observer(observer_id));
        drop(replacement.borrow_mut().take());
        let _ = lifetime.finish().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pending_finish_can_be_escalated_through_control_handle() {
        let lifetime = SessionDrainLifetime::new();
        let handle = lifetime.handle();
        let activity = lifetime.admit_task().unwrap();
        lifetime.initiate_drain(DrainTrigger::Local);
        let mut finish = Box::pin(lifetime.finish());
        assert_pending(finish.as_mut());
        assert!(
            handle
                .connection_error(roxygen_core::error::HttpError::new(
                    HttpErrorCode::TRANSPORT_READ_ERROR,
                ))
                .unwrap()
        );
        assert_eq!(handle.state().unwrap(), DrainState::ForceStopping);
        assert!(!handle.session_cancelled().unwrap());
        drop(activity);
        assert_eq!(
            finish.await.unwrap().code(),
            HttpErrorCode::TRANSPORT_READ_ERROR
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn explicit_finish_surfaces_destroy_panic_after_final_state_commit() {
        let lifetime = SessionDrainLifetime::new();
        let destroy_count = Rc::new(Cell::new(0));
        let destroy_count_for_callback = Rc::clone(&destroy_count);
        lifetime
            .try_add_observer(Some(Rc::new(RecordingObserver {
                attached: Rc::new(Cell::new(0)),
                events: Rc::new(RefCell::new(Vec::new())),
                callback: Some(Rc::new(move |event| {
                    if event == DrainLifecycleEvent::Destroy {
                        destroy_count_for_callback.set(destroy_count_for_callback.get() + 1);
                        panic!("destroy callback");
                    }
                })),
            })))
            .unwrap();
        let activity = lifetime.admit_task().unwrap();
        let mut finish = Box::pin(lifetime.finish());
        assert_pending(finish.as_mut());
        drop(activity);
        assert!(catch_unwind(AssertUnwindSafe(|| poll_once(finish.as_mut()))).is_err());
        assert_eq!(destroy_count.get(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn abandoned_tracked_lifetime_waits_for_local_and_external_owners() {
        let (worker, driver) = WorkerDriver::new(8).unwrap();
        let destroyed = Arc::new(AtomicBool::new(false));
        let destroyed_in_task = Arc::clone(&destroyed);
        worker
            .spawn_local(move |context| async move {
                let lifetime = SessionDrainLifetime::new_tracked(&context).unwrap();
                lifetime
                    .try_add_observer(Some(Rc::new(AtomicDestroyObserver {
                        destroyed: Arc::clone(&destroyed_in_task),
                    })))
                    .unwrap();
                let transaction = lifetime.admit_transaction().unwrap();
                let external = lifetime.acquire_external_keep_alive().unwrap();
                drop(lifetime);
                assert!(!destroyed_in_task.load(Ordering::Acquire));
                drop(transaction);
                assert!(!destroyed_in_task.load(Ordering::Acquire));
                std::thread::spawn(move || drop(external)).join().unwrap();
                context.handle().begin_shutdown();
            })
            .unwrap();
        driver.await;
        assert!(destroyed.load(Ordering::Acquire));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn acceptor_panics_do_not_skip_siblings_or_release_order() {
        let server = ServerShutdown::new();
        assert_eq!(
            server.wait_stopped().await,
            Err(ServerShutdownError::DrainNotStarted)
        );
        let calls = Rc::new(RefCell::new(Vec::new()));
        let first_drain = Rc::clone(&calls);
        let nested_force = server.clone();
        let first_force = Rc::clone(&calls);
        let first_release = Rc::clone(&calls);
        let first_drained = Rc::clone(&calls);
        let mut first = server
            .register_acceptor(AcceptorShutdownHooks {
                drain: Rc::new(move || {
                    first_drain.borrow_mut().push("drain-1");
                    assert!(nested_force.force_stop());
                    panic!("drain");
                }),
                force_stop: Rc::new(move || {
                    first_force.borrow_mut().push("force-1");
                    panic!("force");
                }),
                release_worker_keep_alive: Rc::new(move || {
                    first_release.borrow_mut().push("release-1");
                    panic!("release");
                }),
                on_connections_drained: Rc::new(move || {
                    first_drained.borrow_mut().push("drained-1")
                }),
            })
            .unwrap();
        let second_drain = Rc::clone(&calls);
        let second_force = Rc::clone(&calls);
        let second_release = Rc::clone(&calls);
        let second_drained = Rc::clone(&calls);
        let mut second = server
            .register_acceptor(AcceptorShutdownHooks {
                drain: Rc::new(move || second_drain.borrow_mut().push("drain-2")),
                force_stop: Rc::new(move || second_force.borrow_mut().push("force-2")),
                release_worker_keep_alive: Rc::new(move || {
                    second_release.borrow_mut().push("release-2")
                }),
                on_connections_drained: Rc::new(move || {
                    second_drained.borrow_mut().push("drained-2")
                }),
            })
            .unwrap();

        assert!(catch_unwind(AssertUnwindSafe(|| server.drain())).is_err());
        assert!(!server.force_stop());
        assert!(catch_unwind(AssertUnwindSafe(|| first.connections_drained())).is_err());
        assert!(second.connections_drained().unwrap());
        server.worker_joined().unwrap();
        server.wait_stopped().await.unwrap();
        assert_eq!(server.state(), ServerShutdownState::Stopped);
        assert_eq!(
            &*calls.borrow(),
            &[
                "drain-1",
                "drain-2",
                "force-1",
                "force-2",
                "release-1",
                "drained-1",
                "release-2",
                "drained-2",
            ]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn scoped_server_owns_drain_force_acceptor_release_and_join() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let global_drain_order = Arc::clone(&order);
        let global_force_order = Arc::clone(&order);
        let (stop_sender, stop_receiver) = std::sync::mpsc::sync_channel(1);
        let shutdown = ServerShutdown::with_hooks(ServerShutdownHooks {
            drain: Rc::new(move || global_drain_order.lock().unwrap().push("global-drain")),
            force_stop: Rc::new(move || {
                global_force_order.lock().unwrap().push("global-force");
                stop_sender.send(()).unwrap();
            }),
        });
        let state = shutdown.clone();
        let registration = Rc::new(RefCell::new(None::<AcceptorRegistration>));
        let drain_order = Arc::clone(&order);
        let force_order = Arc::clone(&order);
        let release_order = Arc::clone(&order);
        let drained_order = Arc::clone(&order);
        let registration_for_force = Rc::clone(&registration);
        let acceptor = shutdown
            .register_acceptor(AcceptorShutdownHooks {
                drain: Rc::new(move || drain_order.lock().unwrap().push("drain")),
                force_stop: Rc::new(move || {
                    force_order.lock().unwrap().push("force");
                    registration_for_force
                        .borrow_mut()
                        .as_mut()
                        .expect("the scoped server retains its acceptor")
                        .connections_drained()
                        .unwrap();
                }),
                release_worker_keep_alive: Rc::new(move || {
                    release_order.lock().unwrap().push("release")
                }),
                on_connections_drained: Rc::new(move || {
                    drained_order.lock().unwrap().push("drained")
                }),
            })
            .unwrap();
        *registration.borrow_mut() = Some(acceptor);
        let thread_order = Arc::clone(&order);
        let thread = std::thread::spawn(move || {
            stop_receiver.recv().unwrap();
            thread_order.lock().unwrap().push("join");
        });

        drop(ScopedServerShutdown::new(shutdown, thread));
        assert_eq!(state.state(), ServerShutdownState::Stopped);
        assert_eq!(
            &*order.lock().unwrap(),
            &[
                "global-drain",
                "drain",
                "force",
                "release",
                "drained",
                "global-force",
                "join",
            ]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn scoped_zero_acceptor_stops_worker_and_rejects_lingering_acceptor() {
        let (stop_sender, stop_receiver) = std::sync::mpsc::sync_channel(1);
        let shutdown = ServerShutdown::with_hooks(ServerShutdownHooks {
            drain: Rc::new(|| {}),
            force_stop: Rc::new(move || stop_sender.send(()).unwrap()),
        });
        let state = shutdown.clone();
        let thread = std::thread::spawn(move || stop_receiver.recv().unwrap());
        drop(ScopedServerShutdown::new(shutdown, thread));
        assert_eq!(state.state(), ServerShutdownState::Stopped);

        let (stop_sender, stop_receiver) = std::sync::mpsc::sync_channel(1);
        let shutdown = ServerShutdown::with_hooks(ServerShutdownHooks {
            drain: Rc::new(|| {}),
            force_stop: Rc::new(move || stop_sender.send(()).unwrap()),
        });
        let state = shutdown.clone();
        let registration = shutdown
            .register_acceptor(AcceptorShutdownHooks {
                drain: Rc::new(|| {}),
                force_stop: Rc::new(|| {}),
                release_worker_keep_alive: Rc::new(|| {}),
                on_connections_drained: Rc::new(|| {}),
            })
            .unwrap();
        let thread = std::thread::spawn(move || stop_receiver.recv().unwrap());
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                drop(ScopedServerShutdown::new(shutdown, thread));
            }))
            .is_err()
        );
        drop(registration);
        assert_eq!(state.state(), ServerShutdownState::Stopped);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reentrant_acceptor_completion_drop_panic_does_not_skip_force_or_join() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let drops = Rc::new(Cell::new(0));
        let global_force_order = Arc::clone(&order);
        let (stop_sender, stop_receiver) = std::sync::mpsc::sync_channel(1);
        let shutdown = ServerShutdown::with_hooks(ServerShutdownHooks {
            drain: Rc::new(|| {}),
            force_stop: Rc::new(move || {
                global_force_order.lock().unwrap().push("global-force");
                stop_sender.send(()).unwrap();
            }),
        });
        let state = shutdown.clone();
        let first = Rc::new(RefCell::new(None::<AcceptorRegistration>));
        let second = Rc::new(RefCell::new(None::<AcceptorRegistration>));

        let first_drain_order = Arc::clone(&order);
        let first_force_order = Arc::clone(&order);
        let first_release_order = Arc::clone(&order);
        let first_drained_order = Arc::clone(&order);
        let first_for_force = Rc::clone(&first);
        let bomb = PanicOnDrop {
            drops: Rc::clone(&drops),
        };
        *first.borrow_mut() = Some(
            shutdown
                .register_acceptor(AcceptorShutdownHooks {
                    drain: Rc::new(move || first_drain_order.lock().unwrap().push("drain-1")),
                    force_stop: Rc::new(move || {
                        let _ = &bomb;
                        first_force_order.lock().unwrap().push("force-1");
                        first_for_force
                            .borrow_mut()
                            .as_mut()
                            .expect("first registration")
                            .connections_drained()
                            .unwrap();
                    }),
                    release_worker_keep_alive: Rc::new(move || {
                        first_release_order.lock().unwrap().push("release-1")
                    }),
                    on_connections_drained: Rc::new(move || {
                        first_drained_order.lock().unwrap().push("drained-1")
                    }),
                })
                .unwrap(),
        );

        let second_drain_order = Arc::clone(&order);
        let second_force_order = Arc::clone(&order);
        let second_release_order = Arc::clone(&order);
        let second_drained_order = Arc::clone(&order);
        let second_for_force = Rc::clone(&second);
        *second.borrow_mut() = Some(
            shutdown
                .register_acceptor(AcceptorShutdownHooks {
                    drain: Rc::new(move || second_drain_order.lock().unwrap().push("drain-2")),
                    force_stop: Rc::new(move || {
                        second_force_order.lock().unwrap().push("force-2");
                        second_for_force
                            .borrow_mut()
                            .as_mut()
                            .expect("second registration")
                            .connections_drained()
                            .unwrap();
                    }),
                    release_worker_keep_alive: Rc::new(move || {
                        second_release_order.lock().unwrap().push("release-2")
                    }),
                    on_connections_drained: Rc::new(move || {
                        second_drained_order.lock().unwrap().push("drained-2")
                    }),
                })
                .unwrap(),
        );
        let thread_order = Arc::clone(&order);
        let thread = std::thread::spawn(move || {
            stop_receiver.recv().unwrap();
            thread_order.lock().unwrap().push("join");
        });

        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                drop(ScopedServerShutdown::new(shutdown, thread));
            }))
            .is_err()
        );
        assert_eq!(drops.get(), 1);
        assert_eq!(state.state(), ServerShutdownState::Stopped);
        assert_eq!(
            &*order.lock().unwrap(),
            &[
                "drain-1",
                "drain-2",
                "force-1",
                "release-1",
                "drained-1",
                "force-2",
                "release-2",
                "drained-2",
                "global-force",
                "join",
            ]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn lifecycle_observers_past_inline_capacity_run_newest_first() {
        let (session, _driver) = new_core(TransportDirection::Upstream);
        let log = OrderLog::default();
        let mut ids = Vec::new();
        for label in ["a", "b", "c", "d"] {
            let (observer, _) = order_observer(label, &log, None);
            ids.push(session.add_lifecycle_observer(observer));
        }
        let detached = LifecycleEvent::TransactionDetached { stream: None };
        let mut reservation = session.reserve_request().unwrap();
        assert_eq!(
            labels_for(&log, LifecycleEvent::ActivateConnection),
            ["d", "c", "b", "a"]
        );
        assert!(reservation.cancel());
        assert_eq!(labels_for(&log, detached), ["d", "c", "b", "a"]);

        assert!(session.remove_lifecycle_observer(ids[1]));
        assert!(!session.remove_lifecycle_observer(ids[1]));
        log.borrow_mut().clear();
        let mut reservation = session.reserve_request().unwrap();
        assert!(reservation.cancel());
        assert_eq!(labels_for(&log, detached), ["d", "c", "a"]);

        // An observer added during dispatch receives only later events.
        let later_log = Rc::clone(&log);
        let (adder, _) = order_observer(
            "e",
            &log,
            Some(Rc::new(move |event, registration| {
                if event == LifecycleEvent::TransactionAttached {
                    let (later, _) = order_observer("f", &later_log, None);
                    registration.add_observer(later);
                }
            })),
        );
        session.add_lifecycle_observer(adder);
        log.borrow_mut().clear();
        let mut reservation = session.reserve_request().unwrap();
        assert_eq!(
            labels_for(&log, LifecycleEvent::TransactionAttached),
            ["e", "d", "c", "a"]
        );
        assert!(reservation.cancel());
        assert_eq!(labels_for(&log, detached), ["f", "e", "d", "c", "a"]);
        session.finalize().await.unwrap();
        assert_eq!(
            labels_for(&log, LifecycleEvent::Destroy),
            ["f", "e", "d", "c", "a"]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn lifecycle_registration_after_session_drop_reports_false_and_add_panics() {
        let (session, _driver) = new_core(TransportDirection::Upstream);
        let log = OrderLog::default();
        let (observer, slot) = order_observer("a", &log, None);
        session.add_lifecycle_observer(observer);
        let registration = slot.borrow_mut().take().unwrap();
        session.finalize().await.unwrap();
        assert!(!registration.remove());
        let panic = catch_unwind(AssertUnwindSafe(|| {
            registration.add_observer(Rc::new(NoopLifecycleObserver))
        }))
        .unwrap_err();
        assert_eq!(
            panic_message(panic.as_ref()),
            "an attachment callback retains its session"
        );
    }

    struct ReenteringDropObserver {
        registration: RefCell<Option<LifecycleObserverRegistration>>,
        drops: Rc<Cell<usize>>,
        events: Rc<Cell<usize>>,
    }

    impl LifecycleObserver for ReenteringDropObserver {
        fn on_attached_with_registration(&self, registration: LifecycleObserverRegistration) {
            *self.registration.borrow_mut() = Some(registration);
        }

        fn on_event(&self, _event: LifecycleEvent) {
            self.events.set(self.events.get() + 1);
        }
    }

    impl Drop for ReenteringDropObserver {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
            if let Some(registration) = self.registration.borrow_mut().take() {
                registration.add_observer(Rc::new(NoopLifecycleObserver));
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn removed_lifecycle_observer_drops_inside_the_registry_borrow() {
        let (session, _driver) = new_core(TransportDirection::Upstream);
        let drops = Rc::new(Cell::new(0));
        let events = Rc::new(Cell::new(0));
        let id = session.add_lifecycle_observer(Rc::new(ReenteringDropObserver {
            registration: RefCell::new(None),
            drops: Rc::clone(&drops),
            events: Rc::clone(&events),
        }));
        let panic = catch_unwind(AssertUnwindSafe(|| session.remove_lifecycle_observer(id)))
            .expect_err("a drop that re-enters the registry runs under its borrow");
        assert!(
            panic_message(panic.as_ref()).contains("already"),
            "unexpected panic: {}",
            panic_message(panic.as_ref())
        );
        assert_eq!(drops.get(), 1);
        assert!(!session.remove_lifecycle_observer(id));
        let mut reservation = session.reserve_request().unwrap();
        assert!(reservation.cancel());
        assert_eq!(events.get(), 0);
        session.finalize().await.unwrap();
    }
}
