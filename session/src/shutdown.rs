/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Owner-local session drain and destruction ordering.

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::worker::CleanupRegistrationError;
use roxygen_runtime::worker::WorkerCleanupTaskRegistration;
use roxygen_runtime::worker::WorkerContext;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DrainTrigger {
    Local,
    CloseWhenIdle,
    PeerGoaway,
    CodecNotReusable,
    IdleTimeout,
    SessionCancelled,
    SessionDropped,
    ConnectionError,
    StreamReset,
    HeaderRateLimit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DrainState {
    Running,
    Draining,
    ForceStopping,
    Destroyed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DrainLifecycleEvent {
    ActivateConnection,
    TransactionAttached,
    DrainStarted(DrainTrigger),
    TransactionDetached,
    DeactivateConnection,
    Destroy,
}

pub trait DrainLifecycleObserver: 'static {
    fn on_attached(&self, _registration: DrainObserverRegistration) {}

    fn on_event(&self, _event: DrainLifecycleEvent) {}
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct DrainObserverId(u64);

#[derive(Clone)]
pub struct DrainObserverRegistration {
    handle: SessionDrainHandle,
    id: DrainObserverId,
}

impl DrainObserverRegistration {
    pub fn id(&self) -> DrainObserverId {
        self.id
    }

    pub fn remove(&self) -> bool {
        self.handle.remove_observer(self.id)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DrainError {
    #[error("session drain has closed admission")]
    AdmissionClosed,
    #[error("a lifecycle observer is required")]
    MissingObserver,
    #[error("external keep-alives require worker-tracked cleanup ownership")]
    ExternalKeepAliveRequiresWorker,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DrainCounts {
    pub transactions: usize,
    pub tasks: usize,
    pub byte_events: usize,
    pub keep_alives: usize,
}

impl DrainCounts {
    const fn is_zero(self) -> bool {
        self.transactions == 0 && self.tasks == 0 && self.byte_events == 0 && self.keep_alives == 0
    }
}

#[derive(Clone, Copy)]
enum ActivityKind {
    Transaction,
    Task,
    ByteEvent,
    Context,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CleanupMode {
    Pending,
    Finish,
    Abandon,
}

struct ExternalKeepAliveState {
    count: AtomicUsize,
    waiters: Mutex<Vec<Waker>>,
}

impl ExternalKeepAliveState {
    fn waiters(&self) -> std::sync::MutexGuard<'_, Vec<Waker>> {
        self.waiters
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    fn release(&self) {
        let previous = self.count.fetch_sub(1, Ordering::AcqRel);
        assert!(previous > 0, "a live external keep-alive must be accounted");
        if previous == 1 {
            let waiters = {
                let mut waiters = self.waiters();
                std::mem::take(&mut *waiters)
            };
            for waiter in waiters {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    waiter.wake();
                }));
            }
        }
    }
}

struct Shared {
    state: Cell<DrainState>,
    trigger: Cell<Option<DrainTrigger>>,
    terminal_error: RefCell<Option<HttpError>>,
    counts: Cell<DrainCounts>,
    cancellation: CancellationSource,
    next_observer_id: Cell<u64>,
    observers: RefCell<BTreeMap<DrainObserverId, Rc<dyn DrainLifecycleObserver>>>,
    waiters: RefCell<Vec<Waker>>,
    external_keep_alives: Arc<ExternalKeepAliveState>,
    cleanup_mode: Cell<CleanupMode>,
    driver_owned_cleanup: bool,
    destroyed: Cell<bool>,
}

type PanicPayload = Box<dyn std::any::Any + Send>;

fn run_stage(first_panic: &mut Option<PanicPayload>, callback: impl FnOnce()) {
    if let Err(panic) = catch_unwind(AssertUnwindSafe(callback)) {
        if first_panic.is_none() {
            *first_panic = Some(panic);
        }
    }
}

fn resume_first_panic(first_panic: Option<PanicPayload>) {
    if let Some(panic) = first_panic {
        resume_unwind(panic);
    }
}

fn force_stop_shared(shared: &Rc<Shared>, trigger: DrainTrigger, error: HttpError) -> bool {
    let state = shared.state.get();
    if matches!(state, DrainState::ForceStopping | DrainState::Destroyed) {
        return false;
    }
    shared.state.set(DrainState::ForceStopping);
    let notify_drain = state == DrainState::Running;
    if notify_drain {
        shared.trigger.set(Some(trigger));
    }
    if shared.terminal_error.borrow().is_none() {
        *shared.terminal_error.borrow_mut() = Some(error);
    }
    let mut first_panic = None;
    if notify_drain {
        run_stage(&mut first_panic, || {
            shared.notify(DrainLifecycleEvent::DrainStarted(trigger))
        });
    }
    run_stage(&mut first_panic, || {
        shared.cancellation.request_cancellation();
    });
    run_stage(&mut first_panic, || shared.wake_waiters());
    resume_first_panic(first_panic);
    true
}

impl Shared {
    fn try_add_observer(
        self: &Rc<Self>,
        observer: Option<Rc<dyn DrainLifecycleObserver>>,
    ) -> Result<DrainObserverId, DrainError> {
        let observer = observer.ok_or(DrainError::MissingObserver)?;
        let id = DrainObserverId(self.next_observer_id.get());
        self.next_observer_id
            .set(self.next_observer_id.get().wrapping_add(1));
        self.observers.borrow_mut().insert(id, observer.clone());
        let registration = DrainObserverRegistration {
            handle: SessionDrainHandle {
                shared: Rc::downgrade(self),
            },
            id,
        };
        let attached = catch_unwind(AssertUnwindSafe(|| {
            observer.on_attached(registration);
        }));
        if attached.is_err() {
            self.observers.borrow_mut().remove(&id);
        }
        let dropped = catch_unwind(AssertUnwindSafe(|| drop(observer)));
        if let Err(panic) = attached {
            resume_unwind(panic);
        }
        if let Err(panic) = dropped {
            resume_unwind(panic);
        }
        Ok(id)
    }

    fn notify(&self, event: DrainLifecycleEvent) {
        let observers: Vec<_> = self.observers.borrow().values().rev().cloned().collect();
        let mut first_panic = None;
        for observer in observers {
            run_stage(&mut first_panic, || observer.on_event(event));
            run_stage(&mut first_panic, || drop(observer));
        }
        resume_first_panic(first_panic);
    }

    fn is_idle(&self) -> bool {
        self.counts.get().is_zero() && self.external_keep_alives.count.load(Ordering::Acquire) == 0
    }

    fn finalize(&self) {
        if self.destroyed.replace(true) {
            return;
        }
        self.state.set(DrainState::Destroyed);
        self.notify(DrainLifecycleEvent::Destroy);
    }

    fn maybe_finalize_abandoned(&self) {
        if self.cleanup_mode.get() == CleanupMode::Abandon
            && !self.driver_owned_cleanup
            && self.is_idle()
        {
            self.finalize();
        }
    }

    fn wake_waiters(&self) {
        let waiters = {
            let mut waiters = self.waiters.borrow_mut();
            std::mem::take(&mut *waiters)
        };
        let mut first_panic = None;
        for waiter in waiters {
            run_stage(&mut first_panic, || waiter.wake());
        }
        resume_first_panic(first_panic);
    }

    fn release(&self, kind: ActivityKind) {
        let mut counts = self.counts.get();
        let mut first_panic = None;
        match kind {
            ActivityKind::Transaction => {
                counts.transactions = counts
                    .transactions
                    .checked_sub(1)
                    .expect("a live transaction must be accounted");
                self.counts.set(counts);
                run_stage(&mut first_panic, || {
                    self.notify(DrainLifecycleEvent::TransactionDetached)
                });
                if self.counts.get().transactions == 0 {
                    run_stage(&mut first_panic, || {
                        self.notify(DrainLifecycleEvent::DeactivateConnection)
                    });
                }
            }
            ActivityKind::Task => {
                counts.tasks = counts
                    .tasks
                    .checked_sub(1)
                    .expect("a live task must be accounted");
                self.counts.set(counts);
            }
            ActivityKind::ByteEvent => {
                counts.byte_events = counts
                    .byte_events
                    .checked_sub(1)
                    .expect("a live byte event must be accounted");
                self.counts.set(counts);
            }
            ActivityKind::Context => {
                counts.keep_alives = counts
                    .keep_alives
                    .checked_sub(1)
                    .expect("a live context must be accounted");
                self.counts.set(counts);
            }
        }
        run_stage(&mut first_panic, || self.wake_waiters());
        run_stage(&mut first_panic, || self.maybe_finalize_abandoned());
        resume_first_panic(first_panic);
    }
}

pub struct DrainActivity {
    shared: Option<Rc<Shared>>,
    kind: ActivityKind,
}

pub struct DrainTransaction {
    activity: DrainActivity,
}

impl DrainTransaction {
    pub fn track_byte_event(&self) -> Result<DrainActivity, DrainError> {
        let shared = self
            .activity
            .shared
            .as_ref()
            .ok_or(DrainError::AdmissionClosed)?;
        if matches!(
            shared.state.get(),
            DrainState::ForceStopping | DrainState::Destroyed
        ) {
            return Err(DrainError::AdmissionClosed);
        }
        let mut counts = shared.counts.get();
        counts.byte_events = counts
            .byte_events
            .checked_add(1)
            .expect("byte-event count overflow");
        shared.counts.set(counts);
        Ok(DrainActivity {
            shared: Some(Rc::clone(shared)),
            kind: ActivityKind::ByteEvent,
        })
    }

    pub fn release(&mut self) -> bool {
        self.activity.release()
    }
}

pub struct ExternalSessionKeepAlive {
    shared: Option<Arc<ExternalKeepAliveState>>,
}

impl ExternalSessionKeepAlive {
    pub fn release(&mut self) -> bool {
        let Some(shared) = self.shared.take() else {
            return false;
        };
        shared.release();
        true
    }
}

impl Drop for ExternalSessionKeepAlive {
    fn drop(&mut self) {
        self.release();
    }
}

impl DrainActivity {
    pub fn release(&mut self) -> bool {
        let Some(shared) = self.shared.take() else {
            return false;
        };
        shared.release(self.kind);
        true
    }
}

impl Drop for DrainActivity {
    fn drop(&mut self) {
        let _ = catch_unwind(AssertUnwindSafe(|| self.release()));
    }
}

pub struct SessionDrainLifetime {
    shared: Rc<Shared>,
    cleanup_registration: Option<WorkerCleanupTaskRegistration>,
    finished: bool,
}

impl SessionDrainLifetime {
    pub fn new() -> Self {
        Self::new_inner(false, None)
    }

    pub fn new_tracked(context: &WorkerContext) -> Result<Self, CleanupRegistrationError> {
        let shared = Self::new_shared(true);
        let cleanup_shared = Rc::clone(&shared);
        let registration = context.register_cleanup_task(async move {
            let mut first_panic = None;
            if cleanup_shared.cleanup_mode.get() == CleanupMode::Pending {
                cleanup_shared.cleanup_mode.set(CleanupMode::Abandon);
                run_stage(&mut first_panic, || {
                    force_stop_shared(
                        &cleanup_shared,
                        DrainTrigger::SessionDropped,
                        HttpError::new(HttpErrorCode::DROPPED),
                    );
                });
            }
            DrainWait {
                shared: Rc::clone(&cleanup_shared),
            }
            .await;
            run_stage(&mut first_panic, || cleanup_shared.finalize());
            resume_first_panic(first_panic);
        })?;
        Ok(Self::new_inner(true, Some((shared, registration))))
    }

    fn new_shared(driver_owned_cleanup: bool) -> Rc<Shared> {
        Rc::new(Shared {
            state: Cell::new(DrainState::Running),
            trigger: Cell::new(None),
            terminal_error: RefCell::new(None),
            counts: Cell::new(DrainCounts::default()),
            cancellation: CancellationSource::new(),
            next_observer_id: Cell::new(1),
            observers: RefCell::new(BTreeMap::new()),
            waiters: RefCell::new(Vec::new()),
            external_keep_alives: Arc::new(ExternalKeepAliveState {
                count: AtomicUsize::new(0),
                waiters: Mutex::new(Vec::new()),
            }),
            cleanup_mode: Cell::new(CleanupMode::Pending),
            driver_owned_cleanup,
            destroyed: Cell::new(false),
        })
    }

    fn new_inner(
        driver_owned_cleanup: bool,
        tracked: Option<(Rc<Shared>, WorkerCleanupTaskRegistration)>,
    ) -> Self {
        let (shared, cleanup_registration) = match tracked {
            Some((shared, registration)) => (shared, Some(registration)),
            None => (Self::new_shared(driver_owned_cleanup), None),
        };
        Self {
            shared,
            cleanup_registration,
            finished: false,
        }
    }

    pub fn state(&self) -> DrainState {
        self.shared.state.get()
    }

    pub fn trigger(&self) -> Option<DrainTrigger> {
        self.shared.trigger.get()
    }

    pub fn terminal_error(&self) -> Option<HttpError> {
        self.shared.terminal_error.borrow().clone()
    }

    pub fn counts(&self) -> DrainCounts {
        let mut counts = self.shared.counts.get();
        counts.keep_alives = self
            .shared
            .external_keep_alives
            .count
            .load(Ordering::Acquire);
        counts
    }

    pub fn cancellation(&self) -> CancellationToken {
        self.shared.cancellation.token()
    }

    pub fn handle(&self) -> SessionDrainHandle {
        SessionDrainHandle {
            shared: Rc::downgrade(&self.shared),
        }
    }

    pub fn observer_handle(&self) -> SessionDrainHandle {
        self.handle()
    }

    pub fn try_add_observer(
        &self,
        observer: Option<Rc<dyn DrainLifecycleObserver>>,
    ) -> Result<DrainObserverId, DrainError> {
        self.shared.try_add_observer(observer)
    }

    pub fn remove_observer(&self, id: DrainObserverId) -> bool {
        self.shared.observers.borrow_mut().remove(&id).is_some()
    }

    pub fn admit_transaction(&self) -> Result<DrainTransaction, DrainError> {
        self.admit(ActivityKind::Transaction, true)
            .map(|activity| DrainTransaction { activity })
    }

    pub fn admit_task(&self) -> Result<DrainActivity, DrainError> {
        self.admit(ActivityKind::Task, true)
    }

    pub fn track_byte_event(&self) -> Result<DrainActivity, DrainError> {
        self.admit(ActivityKind::ByteEvent, true)
    }

    pub fn hold_context(&self) -> Result<DrainActivity, DrainError> {
        self.admit(ActivityKind::Context, true)
    }

    pub fn acquire_external_keep_alive(&self) -> Result<ExternalSessionKeepAlive, DrainError> {
        if self.shared.state.get() != DrainState::Running {
            return Err(DrainError::AdmissionClosed);
        }
        if !self.shared.driver_owned_cleanup {
            return Err(DrainError::ExternalKeepAliveRequiresWorker);
        }
        self.shared
            .external_keep_alives
            .count
            .fetch_add(1, Ordering::AcqRel);
        Ok(ExternalSessionKeepAlive {
            shared: Some(Arc::clone(&self.shared.external_keep_alives)),
        })
    }

    fn admit(
        &self,
        kind: ActivityKind,
        requires_running: bool,
    ) -> Result<DrainActivity, DrainError> {
        let state = self.shared.state.get();
        if (requires_running && state != DrainState::Running)
            || matches!(state, DrainState::ForceStopping | DrainState::Destroyed)
        {
            return Err(DrainError::AdmissionClosed);
        }
        let mut counts = self.shared.counts.get();
        match kind {
            ActivityKind::Transaction => {
                let activate = counts.transactions == 0;
                counts.transactions = counts
                    .transactions
                    .checked_add(1)
                    .expect("transaction count overflow");
                self.shared.counts.set(counts);
                let activity = DrainActivity {
                    shared: Some(Rc::clone(&self.shared)),
                    kind,
                };
                let mut first_panic = None;
                if activate {
                    run_stage(&mut first_panic, || {
                        self.shared.notify(DrainLifecycleEvent::ActivateConnection)
                    });
                }
                run_stage(&mut first_panic, || {
                    self.shared.notify(DrainLifecycleEvent::TransactionAttached)
                });
                resume_first_panic(first_panic);
                return Ok(activity);
            }
            ActivityKind::Task => {
                counts.tasks = counts.tasks.checked_add(1).expect("task count overflow");
                self.shared.counts.set(counts);
            }
            ActivityKind::ByteEvent => {
                counts.byte_events = counts
                    .byte_events
                    .checked_add(1)
                    .expect("byte-event count overflow");
                self.shared.counts.set(counts);
            }
            ActivityKind::Context => {
                counts.keep_alives = counts
                    .keep_alives
                    .checked_add(1)
                    .expect("context count overflow");
                self.shared.counts.set(counts);
            }
        }
        Ok(DrainActivity {
            shared: Some(Rc::clone(&self.shared)),
            kind,
        })
    }

    pub fn initiate_drain(&self, trigger: DrainTrigger) -> bool {
        if self.shared.state.get() != DrainState::Running {
            return false;
        }
        self.shared.state.set(DrainState::Draining);
        self.shared.trigger.set(Some(trigger));
        let mut first_panic = None;
        run_stage(&mut first_panic, || {
            self.shared
                .notify(DrainLifecycleEvent::DrainStarted(trigger))
        });
        run_stage(&mut first_panic, || self.shared.wake_waiters());
        resume_first_panic(first_panic);
        true
    }

    pub fn close_when_idle(&self) -> bool {
        self.initiate_drain(DrainTrigger::CloseWhenIdle)
    }

    pub fn peer_goaway(&self) -> bool {
        self.initiate_drain(DrainTrigger::PeerGoaway)
    }

    pub fn codec_not_reusable(&self) -> bool {
        self.initiate_drain(DrainTrigger::CodecNotReusable)
    }

    pub fn idle_timeout(&self) -> bool {
        self.initiate_drain(DrainTrigger::IdleTimeout)
    }

    pub fn stream_reset(&self) -> bool {
        self.initiate_drain(DrainTrigger::StreamReset)
    }

    pub fn session_cancelled(&self) -> bool {
        self.force_stop(
            DrainTrigger::SessionCancelled,
            HttpError::new(HttpErrorCode::CORO_CANCELLED),
        )
    }

    pub fn drop_connection(&self) -> bool {
        self.force_stop(
            DrainTrigger::SessionDropped,
            HttpError::new(HttpErrorCode::DROPPED),
        )
    }

    pub fn connection_error(&self, error: HttpError) -> bool {
        self.force_stop(DrainTrigger::ConnectionError, error)
    }

    pub fn force_stop(&self, trigger: DrainTrigger, error: HttpError) -> bool {
        force_stop_shared(&self.shared, trigger, error)
    }

    pub async fn finish(mut self) -> Option<HttpError> {
        let mut first_panic = None;
        run_stage(&mut first_panic, || {
            self.initiate_drain(DrainTrigger::Local);
        });
        self.shared.cleanup_mode.set(CleanupMode::Finish);
        if let Some(registration) = self.cleanup_registration.take() {
            registration
                .activate()
                .expect("a pre-admitted cleanup remains activatable")
                .await;
        } else {
            DrainWait {
                shared: Rc::clone(&self.shared),
            }
            .await;
            run_stage(&mut first_panic, || self.shared.finalize());
        }
        self.finished = true;
        let terminal_error = self.shared.terminal_error.borrow().clone();
        resume_first_panic(first_panic);
        terminal_error
    }
}

#[derive(Clone)]
pub struct SessionDrainHandle {
    shared: Weak<Shared>,
}

impl SessionDrainHandle {
    pub fn state(&self) -> Result<DrainState, DrainError> {
        self.shared
            .upgrade()
            .map(|shared| shared.state.get())
            .ok_or(DrainError::AdmissionClosed)
    }

    pub fn try_add_observer(
        &self,
        observer: Option<Rc<dyn DrainLifecycleObserver>>,
    ) -> Result<DrainObserverId, DrainError> {
        self.shared
            .upgrade()
            .ok_or(DrainError::AdmissionClosed)?
            .try_add_observer(observer)
    }

    pub fn remove_observer(&self, id: DrainObserverId) -> bool {
        self.shared
            .upgrade()
            .is_some_and(|shared| shared.observers.borrow_mut().remove(&id).is_some())
    }

    pub fn force_stop(&self, trigger: DrainTrigger, error: HttpError) -> Result<bool, DrainError> {
        let shared = self.shared.upgrade().ok_or(DrainError::AdmissionClosed)?;
        Ok(force_stop_shared(&shared, trigger, error))
    }

    pub fn connection_error(&self, error: HttpError) -> Result<bool, DrainError> {
        self.force_stop(DrainTrigger::ConnectionError, error)
    }

    pub fn session_cancelled(&self) -> Result<bool, DrainError> {
        self.force_stop(
            DrainTrigger::SessionCancelled,
            HttpError::new(HttpErrorCode::CORO_CANCELLED),
        )
    }

    pub fn admit_transaction(&self) -> Result<DrainTransaction, DrainError> {
        let shared = self.shared.upgrade().ok_or(DrainError::AdmissionClosed)?;
        if shared.state.get() != DrainState::Running {
            return Err(DrainError::AdmissionClosed);
        }
        let mut counts = shared.counts.get();
        let activate = counts.transactions == 0;
        counts.transactions = counts
            .transactions
            .checked_add(1)
            .expect("transaction count overflow");
        shared.counts.set(counts);
        let transaction = DrainTransaction {
            activity: DrainActivity {
                shared: Some(Rc::clone(&shared)),
                kind: ActivityKind::Transaction,
            },
        };
        let mut first_panic = None;
        if activate {
            run_stage(&mut first_panic, || {
                shared.notify(DrainLifecycleEvent::ActivateConnection)
            });
        }
        run_stage(&mut first_panic, || {
            shared.notify(DrainLifecycleEvent::TransactionAttached)
        });
        resume_first_panic(first_panic);
        Ok(transaction)
    }
}

impl Default for SessionDrainLifetime {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for SessionDrainLifetime {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let _ = catch_unwind(AssertUnwindSafe(|| {
            self.force_stop(
                DrainTrigger::SessionDropped,
                HttpError::new(HttpErrorCode::DROPPED),
            );
        }));
        self.shared.cleanup_mode.set(CleanupMode::Abandon);
        if self.cleanup_registration.is_none() {
            let _ = catch_unwind(AssertUnwindSafe(|| self.shared.maybe_finalize_abandoned()));
        }
    }
}

struct DrainWait {
    shared: Rc<Shared>,
}

impl Future for DrainWait {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let local_counts = self.shared.counts.get();
        let external_count = self
            .shared
            .external_keep_alives
            .count
            .load(Ordering::Acquire);
        if local_counts.is_zero() && external_count == 0 {
            return Poll::Ready(());
        }
        {
            let mut waiters = self.shared.waiters.borrow_mut();
            if !waiters
                .iter()
                .any(|waiter| waiter.will_wake(context.waker()))
            {
                waiters.push(context.waker().clone());
            }
        }
        {
            let mut external_waiters = self.shared.external_keep_alives.waiters();
            if !external_waiters
                .iter()
                .any(|waiter| waiter.will_wake(context.waker()))
            {
                external_waiters.push(context.waker().clone());
            }
        }
        if self.shared.counts.get().is_zero()
            && self
                .shared
                .external_keep_alives
                .count
                .load(Ordering::Acquire)
                == 0
        {
            return Poll::Ready(());
        }
        Poll::Pending
    }
}
