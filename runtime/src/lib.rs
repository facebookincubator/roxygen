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
