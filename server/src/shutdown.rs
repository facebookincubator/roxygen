/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Server and acceptor drain ordering.

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::thread::JoinHandle;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServerShutdownState {
    Running,
    Draining,
    ForceStopping,
    Stopped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServerShutdownError {
    AdmissionClosed,
    DrainNotStarted,
    AcceptorsActive,
}

pub struct ServerShutdownHooks {
    pub drain: Rc<dyn Fn()>,
    pub force_stop: Rc<dyn Fn()>,
}

pub struct AcceptorShutdownHooks {
    pub drain: Rc<dyn Fn()>,
    pub force_stop: Rc<dyn Fn()>,
    pub release_worker_keep_alive: Rc<dyn Fn()>,
    pub on_connections_drained: Rc<dyn Fn()>,
}

struct AcceptorEntry {
    hooks: AcceptorShutdownHooks,
    drain_called: bool,
    force_stop_called: bool,
}

struct Shared {
    state: Cell<ServerShutdownState>,
    next_id: Cell<u64>,
    acceptors: RefCell<BTreeMap<u64, AcceptorEntry>>,
    waiters: RefCell<Vec<Waker>>,
    dispatching_drain: Cell<bool>,
    force_requested: Cell<bool>,
    worker_joined: Cell<bool>,
    hooks: ServerShutdownHooks,
    global_drain_called: Cell<bool>,
    global_force_stop_called: Cell<bool>,
}

type PanicPayload = Box<dyn std::any::Any + Send>;

fn run_stage(first_panic: &mut Option<PanicPayload>, callback: impl FnOnce()) {
    if let Err(panic) = catch_unwind(AssertUnwindSafe(callback)) {
        if first_panic.is_none() {
            *first_panic = Some(panic);
        }
    }
}

fn invoke_and_drop(first_panic: &mut Option<PanicPayload>, callback: Rc<dyn Fn()>) {
    run_stage(first_panic, || callback());
    run_stage(first_panic, || drop(callback));
}

fn drop_callback(first_panic: &mut Option<PanicPayload>, callback: Rc<dyn Fn()>) {
    run_stage(first_panic, || drop(callback));
}

fn resume_first_panic(first_panic: Option<PanicPayload>) {
    if let Some(panic) = first_panic {
        resume_unwind(panic);
    }
}

impl Shared {
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

    fn maybe_mark_stopped(&self) {
        if self.worker_joined.get()
            && self.acceptors.borrow().is_empty()
            && self.state.get() != ServerShutdownState::Running
        {
            self.state.set(ServerShutdownState::Stopped);
        }
    }
}

#[derive(Clone)]
pub struct ServerShutdown {
    shared: Rc<Shared>,
}

impl ServerShutdown {
    pub fn new() -> Self {
        Self::with_hooks(ServerShutdownHooks {
            drain: Rc::new(|| {}),
            force_stop: Rc::new(|| {}),
        })
    }

    pub fn with_hooks(hooks: ServerShutdownHooks) -> Self {
        Self {
            shared: Rc::new(Shared {
                state: Cell::new(ServerShutdownState::Running),
                next_id: Cell::new(1),
                acceptors: RefCell::new(BTreeMap::new()),
                waiters: RefCell::new(Vec::new()),
                dispatching_drain: Cell::new(false),
                force_requested: Cell::new(false),
                worker_joined: Cell::new(false),
                hooks,
                global_drain_called: Cell::new(false),
                global_force_stop_called: Cell::new(false),
            }),
        }
    }

    pub fn state(&self) -> ServerShutdownState {
        self.shared.state.get()
    }

    pub fn active_acceptors(&self) -> usize {
        self.shared.acceptors.borrow().len()
    }

    pub fn register_acceptor(
        &self,
        hooks: AcceptorShutdownHooks,
    ) -> Result<AcceptorRegistration, ServerShutdownError> {
        if self.state() != ServerShutdownState::Running {
            return Err(ServerShutdownError::AdmissionClosed);
        }
        let id = self.shared.next_id.get();
        self.shared.next_id.set(id.wrapping_add(1));
        self.shared.acceptors.borrow_mut().insert(
            id,
            AcceptorEntry {
                hooks,
                drain_called: false,
                force_stop_called: false,
            },
        );
        Ok(AcceptorRegistration {
            shared: Rc::clone(&self.shared),
            id: Some(id),
        })
    }

    pub fn drain(&self) -> bool {
        if self.state() != ServerShutdownState::Running {
            return false;
        }
        self.shared.state.set(ServerShutdownState::Draining);
        let global_drain = (!self.shared.global_drain_called.replace(true))
            .then(|| Rc::clone(&self.shared.hooks.drain));
        let callbacks = {
            let mut acceptors = self.shared.acceptors.borrow_mut();
            acceptors
                .values_mut()
                .filter_map(|entry| {
                    if entry.drain_called {
                        None
                    } else {
                        entry.drain_called = true;
                        Some(Rc::clone(&entry.hooks.drain))
                    }
                })
                .collect::<Vec<_>>()
        };
        self.shared.dispatching_drain.set(true);
        let mut first_panic = None;
        if let Some(callback) = global_drain {
            invoke_and_drop(&mut first_panic, callback);
        }
        for callback in callbacks {
            invoke_and_drop(&mut first_panic, callback);
        }
        self.shared.dispatching_drain.set(false);
        if self.shared.force_requested.get() {
            self.shared.state.set(ServerShutdownState::ForceStopping);
            self.dispatch_force_callbacks(&mut first_panic);
        }
        run_stage(&mut first_panic, || self.shared.wake_waiters());
        resume_first_panic(first_panic);
        true
    }

    pub fn force_stop(&self) -> bool {
        if matches!(
            self.state(),
            ServerShutdownState::ForceStopping | ServerShutdownState::Stopped
        ) {
            return false;
        }
        if self.shared.force_requested.replace(true) {
            return false;
        }
        if self.shared.dispatching_drain.get() {
            return true;
        }
        let mut first_panic = None;
        if self.state() == ServerShutdownState::Running {
            run_stage(&mut first_panic, || {
                self.drain();
            });
            resume_first_panic(first_panic);
            return true;
        }
        self.shared.state.set(ServerShutdownState::ForceStopping);
        self.dispatch_force_callbacks(&mut first_panic);
        run_stage(&mut first_panic, || self.shared.wake_waiters());
        resume_first_panic(first_panic);
        true
    }

    fn dispatch_force_callbacks(&self, first_panic: &mut Option<PanicPayload>) {
        let global_force_stop = (!self.shared.global_force_stop_called.replace(true))
            .then(|| Rc::clone(&self.shared.hooks.force_stop));
        let callbacks = {
            let mut acceptors = self.shared.acceptors.borrow_mut();
            acceptors
                .values_mut()
                .filter_map(|entry| {
                    if entry.force_stop_called {
                        None
                    } else {
                        entry.force_stop_called = true;
                        Some(Rc::clone(&entry.hooks.force_stop))
                    }
                })
                .collect::<Vec<_>>()
        };
        for callback in callbacks {
            invoke_and_drop(first_panic, callback);
        }
        if let Some(callback) = global_force_stop {
            invoke_and_drop(first_panic, callback);
        }
    }

    pub fn worker_joined(&self) -> Result<(), ServerShutdownError> {
        if self.state() == ServerShutdownState::Running {
            return Err(ServerShutdownError::DrainNotStarted);
        }
        self.shared.worker_joined.set(true);
        self.shared.maybe_mark_stopped();
        self.shared.wake_waiters();
        if self.state() == ServerShutdownState::Stopped {
            Ok(())
        } else {
            Err(ServerShutdownError::AcceptorsActive)
        }
    }

    pub async fn wait_stopped(&self) -> Result<(), ServerShutdownError> {
        if self.state() == ServerShutdownState::Running {
            return Err(ServerShutdownError::DrainNotStarted);
        }
        ServerWait {
            shared: Rc::clone(&self.shared),
        }
        .await;
        Ok(())
    }
}

impl Default for ServerShutdown {
    fn default() -> Self {
        Self::new()
    }
}

pub struct AcceptorRegistration {
    shared: Rc<Shared>,
    id: Option<u64>,
}

impl AcceptorRegistration {
    pub fn connections_drained(&mut self) -> Result<bool, ServerShutdownError> {
        if self.shared.state.get() == ServerShutdownState::Running {
            return Err(ServerShutdownError::DrainNotStarted);
        }
        let Some(id) = self.id.take() else {
            return Ok(false);
        };
        let Some(entry) = self.shared.acceptors.borrow_mut().remove(&id) else {
            return Ok(false);
        };
        let AcceptorShutdownHooks {
            drain,
            force_stop,
            release_worker_keep_alive,
            on_connections_drained,
        } = entry.hooks;
        let mut first_panic = None;
        invoke_and_drop(&mut first_panic, release_worker_keep_alive);
        invoke_and_drop(&mut first_panic, on_connections_drained);
        drop_callback(&mut first_panic, drain);
        drop_callback(&mut first_panic, force_stop);
        self.shared.maybe_mark_stopped();
        run_stage(&mut first_panic, || self.shared.wake_waiters());
        resume_first_panic(first_panic);
        Ok(true)
    }
}

impl Drop for AcceptorRegistration {
    fn drop(&mut self) {
        if self.shared.state.get() == ServerShutdownState::Running {
            let Some(id) = self.id.take() else {
                return;
            };
            if let Some(entry) = self.shared.acceptors.borrow_mut().remove(&id) {
                let AcceptorShutdownHooks {
                    drain,
                    force_stop,
                    release_worker_keep_alive,
                    on_connections_drained,
                } = entry.hooks;
                let mut first_panic = None;
                invoke_and_drop(&mut first_panic, release_worker_keep_alive);
                drop_callback(&mut first_panic, on_connections_drained);
                drop_callback(&mut first_panic, drain);
                drop_callback(&mut first_panic, force_stop);
            }
        } else {
            let _ = catch_unwind(AssertUnwindSafe(|| self.connections_drained()));
        }
    }
}

struct ServerWait {
    shared: Rc<Shared>,
}

impl Future for ServerWait {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.shared.state.get() == ServerShutdownState::Stopped {
            return Poll::Ready(());
        }
        let mut waiters = self.shared.waiters.borrow_mut();
        if !waiters
            .iter()
            .any(|waiter| waiter.will_wake(context.waker()))
        {
            waiters.push(context.waker().clone());
        }
        Poll::Pending
    }
}

pub struct ScopedServerShutdown {
    shutdown: Option<ServerShutdown>,
    thread: Option<JoinHandle<()>>,
}

impl ScopedServerShutdown {
    pub fn new(shutdown: ServerShutdown, thread: JoinHandle<()>) -> Self {
        Self {
            shutdown: Some(shutdown),
            thread: Some(thread),
        }
    }

    pub fn shutdown(&self) -> &ServerShutdown {
        self.shutdown
            .as_ref()
            .expect("a scoped server retains shutdown ownership")
    }
}

impl Drop for ScopedServerShutdown {
    fn drop(&mut self) {
        let already_panicking = std::thread::panicking();
        let mut first_panic = None;
        if let Some(shutdown) = self.shutdown.as_ref() {
            run_stage(&mut first_panic, || {
                shutdown.drain();
            });
            run_stage(&mut first_panic, || {
                shutdown.force_stop();
            });
        }
        if let Some(thread) = self.thread.take() {
            if let Err(panic) = thread.join() {
                if first_panic.is_none() {
                    first_panic = Some(panic);
                }
            }
        }
        if let Some(shutdown) = self.shutdown.as_ref() {
            run_stage(&mut first_panic, || {
                shutdown
                    .worker_joined()
                    .expect("scoped shutdown began drain")
            });
        }
        if let Some(shutdown) = self.shutdown.take() {
            run_stage(&mut first_panic, || drop(shutdown));
        }
        if !already_panicking {
            resume_first_panic(first_panic);
        }
    }
}
