/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Worker threads that own their tasks and connections, with timers,
//! cancellation and task groups.
//!
//! A worker is one thread that polls and drops everything it runs, so the
//! futures, connections and handlers on it do not need to be `Send`. Other
//! threads reach it through a cloneable, thread-safe
//! [`WorkerHandle`](worker::WorkerHandle), which queues commands on a bounded
//! queue. `roxygen_server` starts and stops its own workers. To use `roxygen_client`,
//! start a worker here and make requests from commands that run on it.
//!
//! # Starting workers
//!
//! - [`tokio_host::ManagedWorker`] runs one worker on a dedicated thread with a
//!   current-thread Tokio runtime and a `LocalSet`. Calling `shutdown` or
//!   dropping it cancels and joins the worker.
//! - [`tokio_host::HostedWorker`] is a `Send + Sync` variant that runs each
//!   command as its own Tokio local task and can bound its teardown with
//!   [`shutdown_by`](tokio_host::HostedWorker::shutdown_by).
//! - [`managed::RuntimeOwner`] starts a fixed set of workers from synchronous
//!   code outside Tokio.
//!   [`RuntimeHandle::quiesce`](managed::RuntimeHandle::quiesce) drains them,
//!   and [`join`](managed::RuntimeOwner::join) joins the threads.
//! - [`tokio_host::caller_driven_worker`] runs a worker on the calling thread.
//!   [`driver::WorkerDriver`] is the driver future for an executor that the
//!   application wraps in a [`host::LocalHost`].
//!
//! # Running work
//!
//! [`WorkerHandle::submit`](worker::WorkerHandle::submit) and
//! [`spawn_local`](worker::WorkerHandle::spawn_local) queue work.
//! [`call`](worker::WorkerHandle::call) and
//! [`call_local`](worker::WorkerHandle::call_local) also return a
//! [`WorkerCall`](worker::WorkerCall) future for the result, which any thread
//! can await. The closure passed to `call_local` must be `Send`, but the future
//! it returns runs on the worker and need not be. When the queue is full, these
//! methods return [`SubmitError::Overloaded`](worker::SubmitError::Overloaded).
//! Each command gets a [`WorkerContext`](worker::WorkerContext), which gives
//! access to the worker's [`Host`](host::Host) and registers cleanup that runs
//! when the worker shuts down.
//!
//! The [`host::timer`], [`cancellation`], [`task_group`], [`wait`],
//! [`source_proxy`] and [`context`] modules hold the worker-local wheel timer,
//! cancellation tokens, task groups, batons with timeouts, body sources read
//! from another worker, and request context carried between threads.
//!
//! # Example
//!
//! ```rust
//! use std::rc::Rc;
//!
//! use roxygen_runtime::tokio_host::ManagedWorker;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let worker = ManagedWorker::new(16)?;
//!     // The future runs on the worker thread, so it may hold an `Rc` across
//!     // an `.await`.
//!     let call = worker.handle().call_local(|context| async move {
//!         let owner = Rc::new(context.owner_thread());
//!         tokio::task::yield_now().await;
//!         *owner == std::thread::current().id()
//!     })?;
//!     let ran_on_worker = tokio::runtime::Builder::new_current_thread()
//!         .enable_all()
//!         .build()?
//!         .block_on(call)?;
//!     assert!(ran_on_worker);
//!     worker.shutdown()?;
//!     Ok(())
//! }
//! ```
//!
//! # Features
//!
//! - `observability` reports commands, queue waits, task-group tasks,
//!   cancellations, shutdowns and rejected submissions to `tracing` as INFO
//!   spans named `roxygen.runtime.operation`, with `operation` and `outcome`
//!   fields. A command's span descends from the span that was current when it
//!   was submitted, and is entered while the command is polled. Panics caught
//!   by Roxygen's HTTP drivers become ERROR events with target
//!   `roxygen::contained_panic`. Without this feature the crate does not depend
//!   on `tracing`.

pub mod cancellation;
pub mod context;
pub mod driver;
pub mod host;
pub mod keep_alive;
pub mod managed;
pub mod observability;
pub mod panic;
pub mod source_proxy;
pub mod task_group;
pub mod thread;
pub mod tokio_host;
pub mod wait;
pub mod worker;

pub use tokio_host::BlockingPoolLimits;
pub use tokio_host::ManagedWorkerOptions;

#[cfg(test)]
#[path = "../tests/external_host.rs"]
mod external_host_tests;

#[cfg(test)]
#[path = "../tests/hosting.rs"]
mod hosting_tests;

#[cfg(test)]
#[path = "../tests/hosted_worker.rs"]
mod hosted_worker_tests;

#[cfg(test)]
#[path = "../tests/keepalive_task_lifetime.rs"]
mod keepalive_task_lifetime_tests;

#[cfg(test)]
#[path = "../tests/managed.rs"]
mod managed_tests;

#[cfg(test)]
#[path = "../tests/observability_context.rs"]
mod observability_context_tests;

#[cfg(test)]
#[path = "../tests/poll_stats.rs"]
mod poll_stats_tests;

#[cfg(test)]
#[path = "../tests/source_bridge.rs"]
mod source_bridge_tests;

#[cfg(test)]
#[path = "../tests/task_wrapping.rs"]
mod task_wrapping_tests;

#[cfg(test)]
#[path = "../tests/wait_primitives.rs"]
mod wait_primitives_tests;

#[cfg(test)]
#[path = "../tests/worker_affinity.rs"]
mod worker_affinity_tests;

#[cfg(test)]
#[path = "../tests/woken_cleanup.rs"]
mod woken_cleanup_tests;
