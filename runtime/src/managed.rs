/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Synchronous lifecycle ownership with nonblocking, shareable runtime handles.

use std::future::Future;
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use tokio::runtime::Handle;
use tokio::sync::watch;

use crate::tokio_host::ManagedWorker;
use crate::tokio_host::ManagedWorkerError;
use crate::worker::QuiesceDeadline;
use crate::worker::WorkerHandle;

/// Explicit resource limits for a root-owned runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeConfig {
    /// Number of dedicated owner threads; must be nonzero.
    pub workers: usize,
    /// Bounded queued commands per worker; must be nonzero.
    pub command_capacity: usize,
}

/// A failure associated with one configured worker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerFailure {
    /// Zero-based index in the runtime configuration.
    pub worker: usize,
    /// The worker's startup, task, or thread-join failure.
    pub error: ManagedWorkerError,
}

/// Physical joining, distinct from async quiescence and worker success.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShutdownReport {
    /// Worker threads physically joined by the root.
    pub workers_joined: usize,
    /// Workers with unfinished commands or scopes when grace expired.
    pub workers_forced: usize,
}

/// Completed physical retirement, including failures from the joined workers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetirementReport {
    /// Physical joins and final grace-expiry accounting.
    pub report: ShutdownReport,
    /// Indexed failures retained in notification-then-join order.
    pub failures: Vec<WorkerFailure>,
}

impl RetirementReport {
    fn into_join_result(self) -> Result<ShutdownReport, RuntimeError> {
        finish(self.failures).map(|()| self.report)
    }
}

/// Typed lifecycle failures; a failed quiescence still requires root joining.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RuntimeError {
    #[error("a managed runtime requires at least one worker")]
    ZeroWorkers,
    #[error("worker command capacity must be nonzero")]
    ZeroCapacity,
    #[error("runtime startup and physical joining must run outside Tokio")]
    InsideTokio,
    #[error("worker {worker} failed to start: {source}; rollback failures: {rollback:?}")]
    Startup {
        worker: usize,
        #[source]
        source: ManagedWorkerError,
        rollback: Vec<WorkerFailure>,
    },
    #[error("runtime admission and async cleanup must quiesce before joining")]
    NotQuiesced,
    #[error("worker lifecycle failures: {failures:?}")]
    WorkersFailed { failures: Vec<WorkerFailure> },
}

#[derive(Clone)]
struct WorkerObserver {
    handle: WorkerHandle,
    completion: watch::Receiver<Option<Result<(), ManagedWorkerError>>>,
}

impl WorkerObserver {
    async fn wait(mut self) -> Result<(), ManagedWorkerError> {
        loop {
            let completed = self.completion.borrow_and_update().clone();
            if let Some(result) = completed {
                return result;
            }
            if self.completion.changed().await.is_err() {
                return self
                    .completion
                    .borrow()
                    .clone()
                    .unwrap_or(Err(ManagedWorkerError::CompletionChannelClosed));
            }
        }
    }
}

struct Shared {
    workers: Vec<WorkerObserver>,
    quiescing: AtomicBool,
}

/// A cloneable capability, never an owner of threads or worker-local state.
/// Dropping even the last public handle neither blocks nor replaces shutdown.
#[derive(Clone)]
pub struct RuntimeHandle {
    shared: Arc<Shared>,
}

impl RuntimeHandle {
    /// Number of configured workers, including closed workers.
    pub fn worker_count(&self) -> usize {
        self.shared.workers.len()
    }

    /// Returns a worker's bounded submission handle without transferring ownership.
    /// Admitted scopes must join their child task groups and pre-register forced
    /// cleanup using `WorkerContext::register_scope` before spawning them.
    /// Existing handles reject submission after quiescence starts.
    pub fn worker(&self, index: usize) -> Option<WorkerHandle> {
        self.shared
            .workers
            .get(index)
            .map(|worker| worker.handle.clone())
    }

    /// Closes admission immediately and returns a Send cleanup observer.
    ///
    /// Workers notify admitted scopes through `WorkerContext::quiescing`, drain
    /// commands and scopes until the earliest deadline, then cancel unfinished
    /// commands and finish registered cleanup. This happens even if the returned
    /// future is never polled or is dropped. Repeated requests cannot extend grace.
    ///
    /// All borrowed reactors and application runtimes must keep progressing until
    /// this future completes. Grace bounds draining, not arbitrary user polls,
    /// destructors, or cleanup. Completion does not physically join any thread;
    /// the synchronous root must call `RuntimeOwner::join`, also after failure.
    ///
    /// A notification panic is rethrown only after every worker has started
    /// quiescing. Cleanup continues independently; a later call can observe it.
    pub fn quiesce(&self, grace: Duration) -> QuiesceFuture {
        self.shared.quiescing.store(true, Ordering::Release);
        let deadline = QuiesceDeadline::new(grace);
        let mut first_panic = None;
        for worker in &self.shared.workers {
            let result = catch_unwind(AssertUnwindSafe(|| {
                worker.handle.begin_quiesce(deadline);
            }));
            if first_panic.is_none() {
                first_panic = result.err();
            }
        }
        if let Some(payload) = first_panic {
            resume_unwind(payload);
        }
        let workers = self.shared.workers.clone();
        QuiesceFuture {
            inner: Box::pin(async move {
                let mut failures = Vec::new();
                for (index, worker) in workers.into_iter().enumerate() {
                    if let Err(error) = worker.wait().await {
                        failures.push(WorkerFailure {
                            worker: index,
                            error,
                        });
                    }
                }
                finish(failures)
            }),
        }
    }
}

/// Observation only: dropping this future never abandons worker cleanup.
#[must_use = "await quiescence before physically joining the runtime at its root"]
pub struct QuiesceFuture {
    inner: Pin<Box<dyn Future<Output = Result<(), RuntimeError>> + Send + 'static>>,
}

impl Future for QuiesceFuture {
    type Output = Result<(), RuntimeError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(context)
    }
}

enum Ownership {
    Running(Vec<ManagedWorker>),
    Joined(Result<RetirementReport, RuntimeError>),
}

/// Unique, non-Send lifecycle owner retained by the synchronous bootstrap root.
///
/// Start outside Tokio, distribute handles, await quiescence while application
/// runtimes remain alive, then physically join outside Tokio. Ordinary handles
/// cannot become the last lifecycle owner. No global or detached reaper exists.
/// Drop is a blocking cancellation-and-join fallback and cannot recover a dead
/// borrowed runtime; it is not the normal shutdown path and discards failures.
pub struct RuntimeOwner {
    handle: RuntimeHandle,
    ownership: Ownership,
    _not_send: PhantomData<Rc<()>>,
}

impl RuntimeOwner {
    /// Starts all configured workers, or closes and joins every partial startup.
    pub fn start(config: RuntimeConfig) -> Result<Self, RuntimeError> {
        Self::start_with(config, |_, capacity| ManagedWorker::new(capacity))
    }

    pub(crate) fn start_with<F>(
        config: RuntimeConfig,
        mut start_worker: F,
    ) -> Result<Self, RuntimeError>
    where
        F: FnMut(usize, usize) -> Result<ManagedWorker, ManagedWorkerError>,
    {
        if Handle::try_current().is_ok() {
            return Err(RuntimeError::InsideTokio);
        }
        if config.workers == 0 {
            return Err(RuntimeError::ZeroWorkers);
        }
        if config.command_capacity == 0 {
            return Err(RuntimeError::ZeroCapacity);
        }
        let mut workers = Vec::with_capacity(config.workers);
        for index in 0..config.workers {
            match start_worker(index, config.command_capacity) {
                Ok(worker) => workers.push(worker),
                Err(source) => {
                    let (_, rollback) = join_workers(workers);
                    return Err(RuntimeError::Startup {
                        worker: index,
                        source,
                        rollback,
                    });
                }
            }
        }
        let observers = workers
            .iter()
            .map(|worker| WorkerObserver {
                handle: worker.handle(),
                completion: worker.completion(),
            })
            .collect();
        Ok(Self {
            handle: RuntimeHandle {
                shared: Arc::new(Shared {
                    workers: observers,
                    quiescing: AtomicBool::new(false),
                }),
            },
            ownership: Ownership::Running(workers),
            _not_send: PhantomData,
        })
    }

    /// Shares submission and shutdown observation, not lifecycle ownership.
    pub fn handle(&self) -> RuntimeHandle {
        self.handle.clone()
    }

    /// Physically joins every worker after quiescence, surfacing all failures.
    /// `NotQuiesced` and `InsideTokio` preserve ownership for a later retry.
    /// Completed joins are idempotent and return the cached result.
    pub fn join(&mut self) -> Result<ShutdownReport, RuntimeError> {
        if Handle::try_current().is_ok() {
            return Err(RuntimeError::InsideTokio);
        }
        if let Ownership::Joined(result) = &self.ownership {
            return result.clone()?.into_join_result();
        }
        if !self.handle.shared.quiescing.load(Ordering::Acquire)
            || self
                .handle
                .shared
                .workers
                .iter()
                .any(|worker| worker.completion.borrow().is_none())
        {
            return Err(RuntimeError::NotQuiesced);
        }
        self.shutdown_and_join()?.into_join_result()
    }

    /// Cancels immediately and physically joins without requiring a quiescence observer.
    /// This blocking fallback may shorten draining but never starts or extends grace.
    /// Cleanup dependencies must progress independently of the synchronous root;
    /// arbitrary user cleanup is not bounded by this operation.
    /// `InsideTokio` preserves ownership and is checked even before cached returns.
    /// An `Ok` receipt confirms joining, not worker success: inspect its failures.
    /// Completed receipts are cached, including after an error-bearing `join()`.
    pub fn shutdown_and_join(&mut self) -> Result<RetirementReport, RuntimeError> {
        if Handle::try_current().is_ok() {
            return Err(RuntimeError::InsideTokio);
        }
        if let Ownership::Joined(result) = &self.ownership {
            return result.clone();
        }
        let Ownership::Running(workers) = std::mem::replace(
            &mut self.ownership,
            Ownership::Joined(Err(RuntimeError::NotQuiesced)),
        ) else {
            unreachable!("joined runtimes returned their cached result")
        };
        self.handle.shared.quiescing.store(true, Ordering::Release);
        let (report, failures) = join_workers(workers);
        let result = Ok(RetirementReport { report, failures });
        self.ownership = Ownership::Joined(result.clone());
        result
    }
}

impl Drop for RuntimeOwner {
    fn drop(&mut self) {
        let Ownership::Running(workers) = std::mem::replace(
            &mut self.ownership,
            Ownership::Joined(Err(RuntimeError::NotQuiesced)),
        ) else {
            return;
        };
        self.handle.shared.quiescing.store(true, Ordering::Release);
        let _ = join_workers(workers);
    }
}

fn join_workers(workers: Vec<ManagedWorker>) -> (ShutdownReport, Vec<WorkerFailure>) {
    let mut failures: Vec<_> = workers
        .iter()
        .enumerate()
        .filter_map(|(index, worker)| {
            worker.request_shutdown().err().map(|error| WorkerFailure {
                worker: index,
                error,
            })
        })
        .collect();
    let mut report = ShutdownReport {
        workers_joined: 0,
        workers_forced: 0,
    };
    for (index, worker) in workers.into_iter().enumerate() {
        let handle = worker.handle();
        let result = worker.shutdown();
        report.workers_joined += 1;
        report.workers_forced += usize::from(handle.was_forced());
        if let Err(error) = result {
            failures.push(WorkerFailure {
                worker: index,
                error,
            });
        }
    }
    (report, failures)
}

fn finish(failures: Vec<WorkerFailure>) -> Result<(), RuntimeError> {
    if failures.is_empty() {
        Ok(())
    } else {
        Err(RuntimeError::WorkersFailed { failures })
    }
}
