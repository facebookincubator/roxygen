//! Application tasks hosted on the adapter's existing worker threads.

use super::*;

tokio::task_local! {
    static CURRENT: (Arc<Shared>, usize);
}

pub(super) fn current_slot(shared: &Arc<Shared>) -> Option<usize> {
    CURRENT
        .try_with(|(current, slot)| Arc::ptr_eq(current, shared).then_some(*slot))
        .ok()
        .flatten()
}

/// An executor for application tasks that share a Roxygen worker.
///
/// The application owns and joins its tasks. This handle does not keep the
/// worker alive; join application tasks before dropping the runtime. A worker
/// failure cancels its Tokio tasks, which their join handles report normally.
#[derive(Clone)]
pub struct CohostExecutor {
    executor: tokio::runtime::Handle,
    shared: Arc<Shared>,
    slot: usize,
}

impl CohostExecutor {
    /// Selects this worker's Tokio runtime for task spawning. Drop the returned
    /// guard before awaiting anything. Run each spawned task through [`Self::scope`].
    pub fn enter(&self) -> tokio::runtime::EnterGuard<'_> {
        self.executor.enter()
    }

    /// Pins clients built by `work` to this executor's Roxygen worker. The task
    /// must be spawned while [`Self::enter`] is active. Separately spawned tasks
    /// do not inherit this scope.
    pub async fn scope<F: Future>(&self, work: F) -> F::Output {
        CURRENT
            .scope((Arc::clone(&self.shared), self.slot), work)
            .await
    }
}

/// Completion of an executor lookup. Dropping it starts no application task.
pub struct CohostExecutorRequest {
    result: oneshot::Receiver<CohostExecutor>,
}

impl Future for CohostExecutorRequest {
    type Output = Result<CohostExecutor, ExplicitError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().result)
            .poll(context)
            .map(|result| {
                result.map_err(|_| worker_lost("the worker stopped during executor lookup"))
            })
    }
}

impl ExplicitRuntime {
    /// Chooses a worker for an application probe loop and fetches its executor.
    /// The bounded command queue admits the lookup; no application task starts.
    pub fn cohost_executor(&self) -> Result<CohostExecutorRequest, ExplicitError> {
        let shared = &self.owner.shared;
        let slot = shared.candidates().choose(shared);
        let Observation::Open(handle, _) = shared.slots[slot].observe(shared, None, None) else {
            return Err(worker_lost("no open worker for the application task"));
        };
        let shared = Arc::clone(shared);
        let (sender, result) = oneshot::channel();
        handle
            .submit(move |_| {
                let executor = CohostExecutor {
                    executor: tokio::runtime::Handle::current(),
                    shared,
                    slot,
                };
                contain(|| drop(sender.send(executor)));
            })
            .map_err(|error| match error {
                SubmitError::Closed => worker_lost("the worker closed during executor lookup"),
                SubmitError::Overloaded => overloaded("the worker command queue is full"),
            })?;
        Ok(CohostExecutorRequest { result })
    }
}
