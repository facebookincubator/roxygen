/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Runs an accepted connection on an experimental owned driver, and retires it in a worker
//! cleanup task.

use std::cell::Cell;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Context;
use std::task::Poll;

use roxygen_core::error::HttpError;
use roxygen_runtime::panic::discard_panic;
use roxygen_runtime::worker::ContainedPanicScope;
use roxygen_runtime::worker::WorkerCleanupTask;
use roxygen_runtime::worker::WorkerCleanupTaskRegistration;
use roxygen_runtime::worker::WorkerContext;
use roxygen_session::uniplex::UniplexRunSummary;

use super::RunFuture;
use super::handler_application::failure;

/// An owned driver session that `OwnedSession` runs.
pub(crate) trait OwnedDriver: 'static {
    type Handle: Clone + Unpin + 'static;

    /// The site of the contained-panic record of a run that unwound.
    const PANIC_SITE: &'static str;

    fn cancel(handle: &Self::Handle);

    fn run(self) -> RunFuture;
}

/// One owned connection and the worker cleanup that retires it.
pub(crate) struct OwnedSession<D: OwnedDriver> {
    // Declared first, so an unrun session releases its application before `cleanup` activates
    // the application's finalization.
    pub(super) session: D,
    cleanup: Cleanup<D>,
}

/// A worker cleanup task reserved at construction, activated when the run ends or when the
/// session or its run future is dropped. It finishes an abandoned (already cancelled) run,
/// then runs the application's finalization.
struct Cleanup<D: OwnedDriver> {
    registration: WorkerCleanupTaskRegistration,
    shared: Rc<RunShared>,
    handle: D::Handle,
    context: WorkerContext,
}

/// The state that a run, its cleanup task and its `RunFinished` handles share.
struct RunShared {
    /// A cancelled run that the cleanup task finishes.
    abandoned: Cell<Option<RunFuture>>,
    /// Whether the cleanup task has been dropped, finished or not. A run abandoned after that
    /// is dropped at once.
    cleanup_dropped: Cell<bool>,
    finished: Cell<bool>,
}

/// Whether an owned run has ended, however it ended.
#[derive(Clone)]
pub(crate) struct RunFinished(Rc<RunShared>);

impl RunFinished {
    pub(crate) fn get(&self) -> bool {
        self.0.finished.get()
    }
}

/// The cleanup task's reference to `RunShared`. An abandoned run can hold a `RunFinished`,
/// which would keep both alive, so dropping the task, finished or not, drops the run.
struct CleanupShare(Rc<RunShared>);

impl Drop for CleanupShare {
    fn drop(&mut self) {
        self.0.cleanup_dropped.set(true);
        drop(self.0.abandoned.take());
    }
}

impl<D: OwnedDriver> OwnedSession<D> {
    /// A session whose driver and handle `driver` builds from the run's finished flag. The
    /// cleanup runs `finalize` once the run has finished.
    pub(super) fn with_cleanup(
        context: &WorkerContext,
        finalize: impl Future<Output = ()> + 'static,
        driver: impl FnOnce(&RunFinished) -> Result<(D, D::Handle), HttpError>,
    ) -> Result<(Self, D::Handle, RunFinished), HttpError> {
        let shared = Rc::new(RunShared {
            abandoned: Cell::new(None),
            cleanup_dropped: Cell::new(false),
            finished: Cell::new(false),
        });
        let registration = context
            .register_cleanup_task({
                let share = CleanupShare(Rc::clone(&shared));
                async move {
                    if let Some(mut run) = share.0.abandoned.take() {
                        // The run was cancelled when it was abandoned. Its outcome is
                        // unobserved, so its error, or a callback panic that the driver
                        // resumes as it completes, is discarded.
                        std::future::poll_fn(|context| {
                            match catch_unwind(AssertUnwindSafe(|| run.as_mut().poll(context))) {
                                Ok(poll) => poll.map(drop),
                                Err(panic) => {
                                    discard_panic(panic);
                                    Poll::Ready(())
                                }
                            }
                        })
                        .await;
                        share.0.finished.set(true);
                    }
                    finalize.await;
                }
            })
            .map_err(|error| failure(error.to_string().as_bytes()))?;
        let finished = RunFinished(shared);
        let (session, handle) = driver(&finished)?;
        Ok((
            Self {
                session,
                cleanup: Cleanup {
                    registration,
                    shared: Rc::clone(&finished.0),
                    handle: handle.clone(),
                    context: context.clone(),
                },
            },
            handle,
            finished,
        ))
    }

    /// Runs the connection, then waits for its cleanup's finalization. Dropping the returned
    /// future before it completes cancels the connection, whose run then finishes in the
    /// worker cleanup task.
    pub(crate) fn run(self) -> OwnedSessionRun<D> {
        let run = self.session.run();
        #[cfg(test)]
        let run = if RUN_FAULT.take() {
            faulted_run(run)
        } else {
            run
        };
        OwnedSessionRun {
            run: Some(run),
            cleanup: Some(self.cleanup),
            finalizing: None,
            result: None,
        }
    }
}

#[cfg(test)]
thread_local! {
    static RUN_FAULT: Cell<bool> = const { Cell::new(false) };
}

/// Makes the next owned run started on this thread unwind out of its first poll, as a fault in
/// the driver's own code could. The driver is never polled; dropping the faulted run drops it
/// unstarted.
#[cfg(test)]
pub(crate) fn fault_next_owned_run() {
    RUN_FAULT.set(true);
}

#[cfg(test)]
fn faulted_run(unstarted: RunFuture) -> RunFuture {
    Box::pin(std::future::poll_fn(
        move |_: &mut Context<'_>| -> Poll<Result<UniplexRunSummary, HttpError>> {
            let _unstarted = &unstarted;
            panic!("owned run fault")
        },
    ))
}

pub(crate) struct OwnedSessionRun<D: OwnedDriver> {
    run: Option<RunFuture>,
    cleanup: Option<Cleanup<D>>,
    finalizing: Option<WorkerCleanupTask>,
    result: Option<Result<UniplexRunSummary, HttpError>>,
}

impl<D: OwnedDriver> Future for OwnedSessionRun<D> {
    type Output = Result<UniplexRunSummary, HttpError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        if let Some(run) = this.run.as_mut() {
            let result = match catch_unwind(AssertUnwindSafe(|| run.as_mut().poll(context))) {
                Ok(Poll::Pending) => return Poll::Pending,
                Ok(Poll::Ready(result)) => result,
                Err(panic) => {
                    // The run has ended: the HTTP/1.1 driver resumes a callback panic only
                    // after its cleanup has joined, and dropping a run whose own poll unwound
                    // resets its transport. The panic fails only this connection.
                    if let Some(cleanup) = this.cleanup.as_ref() {
                        cleanup.context.record_contained_panic(
                            ContainedPanicScope::Connection,
                            D::PANIC_SITE,
                            None,
                            &*panic,
                        );
                    }
                    discard_panic(panic);
                    Err(failure(b"owned connection panicked"))
                }
            };
            this.run = None;
            let cleanup = this
                .cleanup
                .take()
                .expect("a running session owns its cleanup");
            cleanup.shared.finished.set(true);
            match cleanup.registration.activate() {
                Ok(task) => {
                    this.result = Some(result);
                    this.finalizing = Some(task);
                }
                Err(error) => return Poll::Ready(Err(failure(error.to_string().as_bytes()))),
            }
        }
        let task = this
            .finalizing
            .as_mut()
            .expect("owned run polled after completion");
        std::task::ready!(Pin::new(task).poll(context));
        this.finalizing = None;
        Poll::Ready(this.result.take().expect("a finished run keeps its result"))
    }
}

impl<D: OwnedDriver> Drop for OwnedSessionRun<D> {
    fn drop(&mut self) {
        if let (Some(run), Some(cleanup)) = (self.run.take(), self.cleanup.as_ref()) {
            cleanup.shared.abandoned.set(Some(run));
            D::cancel(&cleanup.handle);
            if cleanup.shared.cleanup_dropped.get() {
                drop(cleanup.shared.abandoned.take());
            }
        }
        // Dropping the registration activates the cleanup task.
        self.cleanup.take();
    }
}

#[cfg(test)]
mod tests {
    use roxygen_runtime::managed::RuntimeConfig;
    use roxygen_runtime::managed::RuntimeOwner;

    use super::*;

    /// A run that stays pending, holds its session's `RunFinished` as a hosted application's
    /// control does, and records its drop.
    struct HeldRun {
        _finished: RunFinished,
        dropped: Rc<Cell<bool>>,
    }

    impl Future for HeldRun {
        type Output = Result<UniplexRunSummary, HttpError>;

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for HeldRun {
        fn drop(&mut self) {
            self.dropped.set(true);
        }
    }

    struct HeldDriver(HeldRun);

    /// Records whether the run was cancelled.
    #[derive(Clone, Default)]
    struct CancelFlag(Rc<Cell<bool>>);

    impl OwnedDriver for HeldDriver {
        type Handle = CancelFlag;

        const PANIC_SITE: &'static str = "held_run";

        fn cancel(handle: &CancelFlag) {
            handle.0.set(true);
        }

        fn run(self) -> RunFuture {
            Box::pin(self.0)
        }
    }

    /// An abandoned run that holds its own `RunFinished` is dropped when its cleanup task is
    /// dropped unfinished, rather than kept alive by the cycle through `RunShared`.
    #[test]
    fn a_dropped_cleanup_task_drops_its_abandoned_run() {
        let shared = Rc::new(RunShared {
            abandoned: Cell::new(None),
            cleanup_dropped: Cell::new(false),
            finished: Cell::new(false),
        });
        let dropped = Rc::new(Cell::new(false));
        shared.abandoned.set(Some(Box::pin(HeldRun {
            _finished: RunFinished(Rc::clone(&shared)),
            dropped: Rc::clone(&dropped),
        })));
        drop(CleanupShare(Rc::clone(&shared)));
        assert!(dropped.get(), "the run drops with its cleanup task");
        assert_eq!(Rc::strong_count(&shared), 1, "the run's reference is gone");
        assert!(shared.cleanup_dropped.get());
    }

    /// A run dropped after the worker has dropped its cleanup task is cancelled and then
    /// dropped at once, because that task can no longer finish it.
    #[test]
    fn a_run_abandoned_after_its_cleanup_task_is_dropped_drops_at_once() {
        let mut owner = RuntimeOwner::start(RuntimeConfig {
            workers: 1,
            command_capacity: 4,
        })
        .expect("owner starts");
        let worker = owner.handle().worker(0).expect("worker exists");
        let call = worker
            .call_local(|context| async move {
                let dropped = Rc::new(Cell::new(false));
                let cancelled = CancelFlag::default();
                let (session, _, finished) =
                    OwnedSession::with_cleanup(&context, async {}, |finished| {
                        let run = HeldRun {
                            _finished: finished.clone(),
                            dropped: Rc::clone(&dropped),
                        };
                        Ok((HeldDriver(run), cancelled.clone()))
                    })
                    .expect("the session is built");
                let mut run = session.run();
                let cleanup = run
                    .cleanup
                    .as_mut()
                    .expect("an unpolled run owns its cleanup");
                let spare = context
                    .register_cleanup_task(async {})
                    .expect("the worker is open");
                let reserved = std::mem::replace(&mut cleanup.registration, spare);
                assert!(
                    reserved.disarm().expect("on the owner thread"),
                    "disarming drops the reserved cleanup task"
                );
                drop(run);
                (cancelled.0.get(), dropped.get(), finished.get())
            })
            .expect("the call is admitted");
        let outcome = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a runtime to wait on")
            .block_on(call)
            .expect("the call completes");
        assert_eq!(
            outcome,
            (true, true, false),
            "the run is cancelled and dropped without finishing"
        );
        owner.shutdown_and_join().expect("the worker joins");
    }
}
