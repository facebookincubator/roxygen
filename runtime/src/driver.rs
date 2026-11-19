/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Context;
use std::task::Poll;
use std::thread::ThreadId;

use crate::host::Host;
use crate::observability::TerminalOutcome;
use crate::task_group::TaskGroup;
use crate::thread::current_id;
use crate::worker::LocalTask;
use crate::worker::PanicPayload;
use crate::worker::PassStats;
use crate::worker::WorkerCommand;
use crate::worker::WorkerContext;
use crate::worker::WorkerCreateError;
use crate::worker::WorkerHandle;
use crate::worker::WorkerState;
use crate::worker::discard_panic;
use crate::worker::retain_first_panic;

/// Owner-thread driver for one bounded worker command queue.
///
/// The driver is deliberately `!Send`: all factories, local futures, and their
/// destruction remain on the thread that created it.
pub struct WorkerDriver {
    handle: WorkerHandle,
    context: WorkerContext,
    tasks: VecDeque<LocalTask>,
    hosted_tasks: Option<TaskGroup>,
    /// Commands taken in one pass, empty between passes.
    intake: Vec<WorkerCommand>,
    owner_thread: ThreadId,
    complete: bool,
    shutdown_started: bool,
    shutdown_panic: Option<PanicPayload>,
    cancellation_span: Option<crate::observability::OperationSpan>,
    _not_send: PhantomData<Rc<()>>,
}

impl WorkerDriver {
    /// Polls commands and the activated cleanups whose wakers fired. Each
    /// cleanup is polled at most once per pass, in wake order. Shutdown polls
    /// every unfinished cleanup in registration order.
    pub fn new(command_capacity: usize) -> Result<(WorkerHandle, Self), WorkerCreateError> {
        Self::create(command_capacity, None)
    }

    /// Dispatches each command and activated cleanup as an independent host task.
    /// The returned driver must itself be driven on the host's owner thread until
    /// shutdown completes. Command admission and cleanup ownership stay here.
    pub fn with_host(
        command_capacity: usize,
        host: Host,
    ) -> Result<(WorkerHandle, Self), WorkerCreateError> {
        assert_eq!(current_id(), host.owner_thread());
        Self::create(command_capacity, Some(host))
    }

    fn create(
        command_capacity: usize,
        host: Option<Host>,
    ) -> Result<(WorkerHandle, Self), WorkerCreateError> {
        let owner_thread = current_id();
        let handle = WorkerHandle::new(command_capacity, owner_thread)?;
        let context = match &host {
            Some(host) => WorkerContext::with_host(handle.clone(), host.clone()),
            None => WorkerContext::new(handle.clone()),
        };
        Ok((
            handle.clone(),
            Self {
                handle,
                context,
                tasks: VecDeque::new(),
                hosted_tasks: host.map(TaskGroup::with_host),
                intake: Vec::new(),
                owner_thread,
                complete: false,
                shutdown_started: false,
                shutdown_panic: None,
                cancellation_span: None,
                _not_send: PhantomData,
            },
        ))
    }

    pub fn handle(&self) -> &WorkerHandle {
        &self.handle
    }

    /// Borrows the owner-local context before transferring the driver to its host.
    pub fn context(&self) -> &WorkerContext {
        &self.context
    }

    pub fn owner_thread(&self) -> ThreadId {
        self.owner_thread
    }

    fn record_shutdown_panic(&mut self, panic: Option<PanicPayload>) {
        retain_first_panic(&mut self.shutdown_panic, panic);
    }

    fn start_shutdown(&mut self) {
        if self.shutdown_started {
            return;
        }
        self.shutdown_started = true;
        match catch_unwind(AssertUnwindSafe(|| {
            self.handle.shared.start_cancellation_span()
        })) {
            Ok(span) => self.cancellation_span = Some(span),
            Err(payload) => self.record_shutdown_panic(Some(payload)),
        }
        let queued = self.handle.shared.take_commands_for_shutdown();
        for command in queued {
            let result = catch_unwind(AssertUnwindSafe(|| drop(command)));
            self.record_shutdown_panic(result.err());
        }
        while let Some(task) = self.tasks.pop_front() {
            let result = catch_unwind(AssertUnwindSafe(|| drop(task)));
            self.record_shutdown_panic(result.err());
        }
        if let Some(tasks) = &mut self.hosted_tasks {
            tasks.abort_all();
            while let Some(completion) = tasks.try_join_next() {
                if let Err(error) = completion.result
                    && error != crate::task_group::TaskGroupError::TaskCancelled
                {
                    retain_first_panic(&mut self.shutdown_panic, Some(Box::new(error)));
                }
                completion.discard();
            }
        }
        let cleanup_panic = self.context.drain_cleanups();
        self.record_shutdown_panic(cleanup_panic);
    }

    fn finish_shutdown(&mut self) {
        if let Some(mut cancellation) = self.cancellation_span.take() {
            let outcome = if self.shutdown_panic.is_some() {
                TerminalOutcome::Failure
            } else {
                TerminalOutcome::Success
            };
            let result = catch_unwind(AssertUnwindSafe(|| cancellation.complete(outcome)));
            self.record_shutdown_panic(result.err());
        }
        let close_panic = self.handle.shared.mark_closed();
        self.complete = true;
        self.record_shutdown_panic(close_panic);
        if let Some(payload) = self.shutdown_panic.take() {
            resume_unwind(payload);
        }
    }

    /// Publishes `pass` itself before completing, since completion may unwind.
    fn poll_shutdown(&mut self, context: &mut Context<'_>, pass: &mut PassStats) -> Poll<()> {
        self.start_shutdown();
        let cleanup = self.context.poll_cleanup_tasks(context, true);
        cleanup.add_to(pass);
        self.record_shutdown_panic(cleanup.panic);
        if !cleanup.complete {
            return Poll::Pending;
        }
        self.publish_pass(pass);
        self.finish_shutdown();
        Poll::Ready(())
    }

    fn publish_pass(&self, pass: &PassStats) {
        self.handle
            .shared
            .poll_counters()
            .record_pass(pass, self.hosted_tasks.is_none());
    }

    fn abort_and_close(&mut self) {
        if self.complete {
            return;
        }
        let preparation = catch_unwind(AssertUnwindSafe(|| self.start_shutdown()));
        self.record_shutdown_panic(preparation.err());
        let retirement = catch_unwind(AssertUnwindSafe(|| self.context.abort_cleanup_tasks()));
        self.record_shutdown_panic(match retirement {
            Ok(panic) => panic,
            Err(payload) => Some(payload),
        });
        self.finish_shutdown();
    }
}

impl WorkerDriver {
    fn poll_pass(&mut self, context: &mut Context<'_>, pass: &mut PassStats) -> Poll<()> {
        if self.handle.state() != WorkerState::Open {
            self.handle.shared.register_driver_waker(context.waker());
            return self.poll_shutdown(context, pass);
        }

        #[cfg(test)]
        crate::worker::run_driver_intake_hook(&self.handle);
        self.handle
            .shared
            .take_open_commands(context.waker(), &mut self.intake);
        for command in self.intake.drain(..) {
            let task = command.into_command(self.context.clone());
            if let Some(tasks) = &mut self.hosted_tasks {
                tasks
                    .spawn_command(task)
                    .expect("open worker retains command admission");
            } else {
                self.tasks.push_back(Box::pin(task));
            }
        }
        let mut failure = None;
        if let Some(tasks) = &mut self.hosted_tasks {
            while let Poll::Ready(Some(completion)) = tasks.poll_join_next(context) {
                if let Err(error) = completion.result {
                    failure.get_or_insert(error);
                }
                completion.discard();
            }
        }
        if let Some(error) = failure {
            self.record_shutdown_panic(Some(Box::new(error)));
            self.handle.begin_shutdown();
        }

        if self.handle.state() != WorkerState::Open {
            return self.poll_shutdown(context, pass);
        }

        let tasks_to_poll = self.tasks.len();
        for _ in 0..tasks_to_poll {
            let Some(mut task) = self.tasks.pop_front() else {
                break;
            };
            pass.command_polls += 1;
            if task.as_mut().poll(context).is_pending() {
                self.tasks.push_back(task);
            }
        }

        let cleanup = self.context.poll_cleanup_tasks(context, false);
        cleanup.add_to(pass);
        pass.budget_exhausted = !tokio::task::coop::has_budget_remaining();
        if cleanup.panic.is_some() {
            self.record_shutdown_panic(cleanup.panic);
            let shutdown = catch_unwind(AssertUnwindSafe(|| self.handle.begin_shutdown()));
            self.record_shutdown_panic(shutdown.err());
        }

        if self.handle.state() != WorkerState::Open {
            self.poll_shutdown(context, pass)
        } else {
            Poll::Pending
        }
    }
}

impl Future for WorkerDriver {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        debug_assert_eq!(current_id(), self.owner_thread);
        if self.complete {
            return Poll::Ready(());
        }
        self.handle.shared.poll_counters().record_driver_poll();
        let mut pass = PassStats::default();
        let polled = self.poll_pass(context, &mut pass);
        if polled.is_pending() {
            self.publish_pass(&pass);
        }
        polled
    }
}

impl Drop for WorkerDriver {
    fn drop(&mut self) {
        debug_assert_eq!(current_id(), self.owner_thread);
        let unwinding = std::thread::panicking();
        let shutdown = catch_unwind(AssertUnwindSafe(|| self.handle.begin_shutdown()));
        self.record_shutdown_panic(shutdown.err());
        let retirement = catch_unwind(AssertUnwindSafe(|| self.abort_and_close()));
        self.record_shutdown_panic(retirement.err());
        if let Some(payload) = self.shutdown_panic.take() {
            if unwinding {
                discard_panic(payload);
            } else {
                resume_unwind(payload);
            }
        }
    }
}
