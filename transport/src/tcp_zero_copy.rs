/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![cfg_attr(
    not(any(target_os = "linux", test)),
    expect(
        dead_code,
        reason = "completion accounting is only driven by Linux MSG_ZEROCOPY support"
    )
)]

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
#[cfg(target_os = "linux")]
use std::future::Future;
use std::io;
#[cfg(target_os = "linux")]
use std::mem::MaybeUninit;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use roxygen_core::buf_queue::BufQueue;
use roxygen_runtime::worker::WorkerCleanupRegistration;
use roxygen_runtime::worker::WorkerContext;

const IOV_MAX: usize = 1024;
/// Iovecs that one `sendmsg` passes from the stack. A write of more segments
/// builds a heap array. 64 matches the stack array in folly's `AsyncSocket`.
#[cfg(target_os = "linux")]
const INLINE_IOVECS: usize = 64;
const COMPLETION_BATCH: usize = 64;
const LOAN_BUDGET: usize = 64 * 1024 * 1024;

static QUARANTINED_LOANS: OnceLock<Mutex<Vec<RetainedLoan>>> = OnceLock::new();
static SHARED_LOAN_BUDGET: LoanBudget = LoanBudget::new(LOAN_BUDGET);

#[derive(Debug)]
struct LoanBudget {
    limit: usize,
    used: AtomicUsize,
}

impl LoanBudget {
    const fn new(limit: usize) -> Self {
        Self {
            limit,
            used: AtomicUsize::new(0),
        }
    }

    fn reserve(&'static self, data: &BufQueue) -> Option<Arc<LoanReservation>> {
        // Charge the full backing allocation, not the visible slices. The fixed
        // and per-segment charges also bound loan and metadata counts, including
        // both copies retained by the unwind guard and the pending-cookie map.
        let charge = data
            .backing_capacity_upper_bound()?
            .checked_add(data.segment_count().checked_mul(128)?)?
            .checked_add(1024)?;
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            let total = used
                .checked_add(charge)
                .filter(|total| *total <= self.limit)?;
            match self
                .used
                .compare_exchange_weak(used, total, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(actual) => used = actual,
            }
        }
        Some(Arc::new(LoanReservation {
            budget: self,
            charge,
        }))
    }
}

#[derive(Debug)]
struct LoanReservation {
    budget: &'static LoanBudget,
    charge: usize,
}

impl Drop for LoanReservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.charge, Ordering::AcqRel);
    }
}

#[derive(Debug)]
struct RetainedLoan {
    data: BufQueue,
    reservation: Arc<LoanReservation>,
}

impl RetainedLoan {
    fn clone_shared(&self) -> Self {
        Self {
            data: self.data.clone_shared(),
            reservation: Arc::clone(&self.reservation),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SubmissionPolicy {
    Enabled,
    Backoff(usize),
    Disabled,
}

#[derive(Debug)]
struct PreparedSend {
    cookie: u32,
    panic_guard: Option<RetainedLoan>,
}

impl PreparedSend {
    fn disarm(&mut self) {
        self.panic_guard.take();
    }
}

impl Drop for PreparedSend {
    fn drop(&mut self) {
        if let Some(backing) = self.panic_guard.take() {
            quarantine_queue(backing);
        }
    }
}

impl Drop for ZeroCopyState {
    fn drop(&mut self) {
        self.quarantine();
    }
}

#[derive(Debug)]
pub(crate) enum SendOutcome {
    Written(usize),
    RetryOrdinary,
    Wait,
    Error(io::Error),
}

struct ZeroCopyState {
    threshold: usize,
    reenable_threshold: usize,
    policy: SubmissionPolicy,
    next_cookie: u32,
    pending: BTreeMap<u32, RetainedLoan>,
    preparing: BTreeSet<u32>,
    budget: &'static LoanBudget,
}

impl ZeroCopyState {
    fn new() -> Self {
        Self {
            threshold: 0,
            reenable_threshold: 0,
            policy: SubmissionPolicy::Disabled,
            next_cookie: 0,
            pending: BTreeMap::new(),
            preparing: BTreeSet::new(),
            budget: &SHARED_LOAN_BUDGET,
        }
    }

    fn enable(&mut self, threshold: usize) -> bool {
        self.threshold = threshold;
        self.policy = if threshold == 0 {
            SubmissionPolicy::Disabled
        } else {
            SubmissionPolicy::Enabled
        };
        threshold != 0
    }

    fn set_reenable_threshold(&mut self, threshold: usize) {
        self.reenable_threshold = threshold;
    }

    fn begin_write(&mut self, length: usize) -> bool {
        match self.policy {
            SubmissionPolicy::Enabled => {}
            SubmissionPolicy::Backoff(remaining) => {
                let Some(remaining) = remaining.checked_sub(1) else {
                    return false;
                };
                if remaining != 0 {
                    self.policy = SubmissionPolicy::Backoff(remaining);
                    return false;
                }
                self.policy = SubmissionPolicy::Enabled;
            }
            SubmissionPolicy::Disabled => return false,
        }
        length >= self.threshold
    }

    fn prepare(&mut self, data: &BufQueue) -> Option<PreparedSend> {
        if self.policy != SubmissionPolicy::Enabled
            || data.is_empty()
            || self.pending.contains_key(&self.next_cookie)
        {
            return None;
        }
        let cookie = self.next_cookie;
        let reservation = self.budget.reserve(data)?;
        let retained = RetainedLoan {
            data: data.clone_shared(),
            reservation,
        };
        self.pending.insert(cookie, retained.clone_shared());
        self.preparing.insert(cookie);
        Some(PreparedSend {
            cookie,
            panic_guard: Some(retained),
        })
    }

    fn abort(&mut self, mut prepared: PreparedSend) {
        if self.preparing.remove(&prepared.cookie) {
            self.pending.remove(&prepared.cookie);
        }
        prepared.disarm();
    }

    fn commit(&mut self, mut prepared: PreparedSend, written: usize) -> bool {
        self.next_cookie = self.next_cookie.wrapping_add(1);
        if !self.preparing.remove(&prepared.cookie) {
            prepared.disarm();
            return true;
        }
        let loan = self
            .pending
            .get_mut(&prepared.cookie)
            .expect("a preparing cookie retains its backing");
        if written > loan.data.chain_length() {
            let loan = self
                .pending
                .remove(&prepared.cookie)
                .expect("the invalid submission still owns its loan");
            quarantine_queue(loan);
            prepared.disarm();
            return false;
        }
        let submitted = loan.data.split_at_most(written);
        loan.data = submitted;
        prepared.disarm();
        true
    }

    fn finish_submission(
        &mut self,
        prepared: PreparedSend,
        result: io::Result<usize>,
    ) -> SendOutcome {
        match result {
            Ok(0) => {
                self.abort(prepared);
                SendOutcome::Error(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "TCP zero-copy write made no progress",
                ))
            }
            Ok(written) => {
                if self.commit(prepared, written) {
                    SendOutcome::Written(written)
                } else {
                    self.disable();
                    SendOutcome::Error(io::Error::other(
                        "TCP zero-copy submission exceeded its retained backing",
                    ))
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                self.abort(prepared);
                SendOutcome::Wait
            }
            Err(error) if error.raw_os_error() == Some(libc::ENOBUFS) => {
                self.abort(prepared);
                self.on_enobufs();
                SendOutcome::RetryOrdinary
            }
            Err(error) => {
                self.abort(prepared);
                SendOutcome::Error(error)
            }
        }
    }

    fn on_enobufs(&mut self) {
        self.policy = if self.reenable_threshold == 0 {
            SubmissionPolicy::Disabled
        } else {
            SubmissionPolicy::Backoff(self.reenable_threshold)
        };
    }

    fn complete(&mut self, first: u32, last: u32, copied: bool) -> usize {
        if copied {
            self.policy = SubmissionPolicy::Disabled;
        }
        let completed = self
            .pending
            .keys()
            .copied()
            .filter(|cookie| cookie.wrapping_sub(first) <= last.wrapping_sub(first))
            .collect::<Vec<_>>();
        let count = completed.len();
        completed.into_iter().for_each(|cookie| {
            self.pending.remove(&cookie);
            self.preparing.remove(&cookie);
        });
        count
    }

    fn disable(&mut self) {
        self.policy = SubmissionPolicy::Disabled;
    }

    fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    fn quarantine(&mut self) -> usize {
        let pending = std::mem::take(&mut self.pending);
        self.preparing.clear();
        let count = pending.len();
        pending.into_values().for_each(quarantine_queue);
        count
    }
}

fn quarantine_queue(queue: RetainedLoan) {
    QUARANTINED_LOANS
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(queue);
}

struct CompletionControl {
    state: ZeroCopyState,
    #[cfg(target_os = "linux")]
    completion: Option<Rc<tokio::io::unix::AsyncFd<std::os::fd::OwnedFd>>>,
    drain_waker: Option<std::task::Waker>,
    terminal: bool,
}

#[derive(Debug, Eq, PartialEq)]
enum DrainOutcome {
    Drained(usize),
    More,
}

impl CompletionControl {
    fn new() -> Self {
        Self {
            state: ZeroCopyState::new(),
            #[cfg(target_os = "linux")]
            completion: None,
            drain_waker: None,
            terminal: false,
        }
    }

    fn wake_drain(&mut self) {
        if let Some(waker) = self.drain_waker.take() {
            waker.wake();
        }
    }

    fn apply_completion(&mut self, completion: Completion) -> usize {
        let released = self
            .state
            .complete(completion.first, completion.last, completion.copied);
        if released != 0 {
            self.wake_drain();
        }
        released
    }

    #[cfg(target_os = "linux")]
    fn drain_nonblocking(&mut self) -> Option<DrainOutcome> {
        let Some(completion) = self.completion.clone() else {
            return Some(DrainOutcome::Drained(0));
        };
        self.drain_with(|| receive_completions(completion.get_ref().as_raw_fd()))
    }

    fn drain_with(
        &mut self,
        mut receive: impl FnMut() -> io::Result<Vec<Completion>>,
    ) -> Option<DrainOutcome> {
        let mut malformed = false;
        let mut messages = 0usize;
        for _ in 0..COMPLETION_BATCH {
            match receive() {
                Ok(completions) => {
                    messages = messages.saturating_add(1);
                    completions.into_iter().for_each(|completion| {
                        self.apply_completion(completion);
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    if malformed {
                        self.fail_and_quarantine();
                        return None;
                    }
                    self.wake_drain();
                    return Some(DrainOutcome::Drained(messages));
                }
                Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                    self.state.disable();
                    malformed = true;
                }
                Err(_) => {
                    self.fail_and_quarantine();
                    return None;
                }
            }
        }
        if malformed {
            self.fail_and_quarantine();
            None
        } else {
            Some(DrainOutcome::More)
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn drain_nonblocking(&mut self) -> Option<DrainOutcome> {
        Some(DrainOutcome::Drained(0))
    }

    fn fail_and_quarantine(&mut self) {
        if self.terminal {
            return;
        }
        self.terminal = true;
        self.state.disable();
        #[cfg(target_os = "linux")]
        drop(self.completion.take());
        self.state.quarantine();
        self.wake_drain();
    }

    fn force_teardown(&mut self) {
        if self.terminal {
            return;
        }
        self.state.disable();
        if self.drain_nonblocking().is_none() {
            return;
        }
        self.terminal = true;
        #[cfg(target_os = "linux")]
        drop(self.completion.take());
        self.state.quarantine();
        self.wake_drain();
    }
}

std::thread_local! {
    static NEXT_COMPLETION_PUMP_ID: Cell<u64> = const { Cell::new(1) };
    static COMPLETION_PUMP_REGISTRY: RefCell<Vec<(
        u64,
        Weak<RefCell<CompletionControl>>,
        tokio::sync::watch::Receiver<bool>,
    )>> = const { RefCell::new(Vec::new()) };
}

pub(crate) struct TcpZeroCopy {
    control: Rc<RefCell<CompletionControl>>,
    stop: Option<tokio::sync::watch::Sender<bool>>,
    cleanup: Option<WorkerCleanupRegistration>,
}

impl TcpZeroCopy {
    pub(crate) fn new() -> Self {
        Self {
            control: Rc::new(RefCell::new(CompletionControl::new())),
            stop: None,
            cleanup: None,
        }
    }

    pub(crate) fn enable(
        &mut self,
        #[cfg_attr(
            not(target_os = "linux"),
            expect(
                unused_variables,
                reason = "the completion pump only runs for Linux zero-copy support"
            )
        )]
        context: &WorkerContext,
        #[cfg_attr(
            not(target_os = "linux"),
            expect(
                unused_variables,
                reason = "socket descriptor is only used by Linux zero-copy support"
            )
        )]
        descriptor: i32,
        threshold: usize,
    ) -> io::Result<bool> {
        if threshold == 0 {
            self.control.borrow_mut().state.enable(0);
            return Ok(false);
        }

        #[cfg(not(target_os = "linux"))]
        return Ok(false);

        #[cfg(target_os = "linux")]
        {
            if self.control.borrow().terminal {
                return Ok(false);
            }
            if self.control.borrow().completion.is_some() {
                self.control.borrow_mut().state.enable(threshold);
                return Ok(true);
            }
            if !enable_socket_zero_copy(descriptor) {
                return Ok(false);
            }
            let Some(completion) = completion_descriptor(descriptor) else {
                return Ok(false);
            };
            self.control.borrow_mut().completion = Some(Rc::new(completion));
            if !self.start_completion_pump(context) {
                self.control.borrow_mut().completion.take();
                return Ok(false);
            }
            self.control.borrow_mut().state.enable(threshold);
            Ok(true)
        }
    }

    pub(crate) fn set_reenable_threshold(&mut self, threshold: usize) {
        self.control
            .borrow_mut()
            .state
            .set_reenable_threshold(threshold);
    }

    pub(crate) fn begin_write(&mut self, length: usize) -> bool {
        let mut control = self.control.borrow_mut();
        !control.terminal && control.state.begin_write(length)
    }

    pub(crate) fn allows_borrowed_write(&self) -> bool {
        let control = self.control.borrow();
        !control.terminal && control.state.policy == SubmissionPolicy::Disabled
    }

    pub(crate) fn poll_write(
        &mut self,
        stream: &tokio::net::TcpStream,
        context: &mut Context<'_>,
        data: &BufQueue,
        zero_copy: bool,
    ) -> Poll<io::Result<usize>> {
        self.poll_write_with(
            data,
            zero_copy,
            || stream.poll_write_ready(context),
            |use_zero_copy| try_send(stream, data, use_zero_copy),
        )
    }

    fn poll_write_with(
        &mut self,
        data: &BufQueue,
        zero_copy: bool,
        ready: impl FnMut() -> Poll<io::Result<()>>,
        mut send: impl FnMut(bool) -> io::Result<usize>,
    ) -> Poll<io::Result<usize>> {
        poll_send(ready, || self.write_after_ready(data, zero_copy, &mut send))
    }

    pub(crate) fn poll_drain(&mut self, context: &mut Context<'_>) -> Poll<()> {
        let mut control = self.control.borrow_mut();
        if control.state.is_empty() {
            return Poll::Ready(());
        }
        let replace = control
            .drain_waker
            .as_ref()
            .is_none_or(|registered| !registered.will_wake(context.waker()));
        if replace {
            control.drain_waker = Some(context.waker().clone());
        }
        Poll::Pending
    }

    pub(crate) fn force_teardown(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(true);
        }
        self.control.borrow_mut().force_teardown();
        if let Some(cleanup) = self.cleanup.take() {
            let _ = cleanup.disarm();
        }
    }

    #[cfg(target_os = "linux")]
    fn start_completion_pump(&mut self, context: &WorkerContext) -> bool {
        let Some(id) = NEXT_COMPLETION_PUMP_ID.with(|next| {
            let id = next.get();
            next.set(id.checked_add(1)?);
            Some(id)
        }) else {
            return false;
        };
        let (stop, receiver) = tokio::sync::watch::channel(false);
        let weak = Rc::downgrade(&self.control);
        let cleanup_weak = weak.clone();
        let cleanup_stop = stop.clone();
        let Ok(cleanup) = context.register_cleanup(move || {
            let _ = cleanup_stop.send(true);
            if let Some(control) = cleanup_weak.upgrade() {
                control.borrow_mut().force_teardown();
            }
        }) else {
            return false;
        };
        COMPLETION_PUMP_REGISTRY.with(|registry| {
            registry.borrow_mut().push((id, weak, receiver));
        });
        let spawned = context.handle().spawn_local(move |_context| async move {
            let entry = COMPLETION_PUMP_REGISTRY.with(|registry| {
                let mut registry = registry.borrow_mut();
                registry
                    .iter()
                    .position(|(candidate, _, _)| *candidate == id)
                    .map(|index| registry.swap_remove(index))
            });
            if let Some((_, control, receiver)) = entry {
                completion_pump(control, receiver).await;
            }
        });
        if spawned.is_err() {
            COMPLETION_PUMP_REGISTRY.with(|registry| {
                let mut registry = registry.borrow_mut();
                if let Some(index) = registry
                    .iter()
                    .position(|(candidate, _, _)| *candidate == id)
                {
                    registry.swap_remove(index);
                }
            });
            let _ = cleanup.disarm();
            return false;
        }
        self.stop = Some(stop);
        self.cleanup = Some(cleanup);
        true
    }

    fn write_after_ready(
        &mut self,
        data: &BufQueue,
        zero_copy: bool,
        mut send: impl FnMut(bool) -> io::Result<usize>,
    ) -> SendOutcome {
        let zero_copy_outcome = if zero_copy {
            let mut control = self.control.borrow_mut();
            if control.terminal {
                None
            } else {
                control.state.prepare(data).map(|prepared| {
                    let result = send(true);
                    control.state.finish_submission(prepared, result)
                })
            }
        } else {
            None
        };
        match zero_copy_outcome {
            Some(SendOutcome::Written(written)) => return SendOutcome::Written(written),
            Some(SendOutcome::Wait) => return SendOutcome::Wait,
            Some(SendOutcome::Error(error)) => return SendOutcome::Error(error),
            Some(SendOutcome::RetryOrdinary) | None => {}
        }
        ordinary_outcome(send(false))
    }

    #[cfg(test)]
    pub(crate) fn enable_for_test(&mut self, threshold: usize) -> bool {
        let mut control = self.control.borrow_mut();
        !control.terminal && control.state.enable(threshold)
    }

    #[cfg(test)]
    pub(crate) fn submit_for_test(
        &mut self,
        data: &BufQueue,
        result: io::Result<usize>,
        completion_before_registration: Option<(u32, u32, bool)>,
    ) -> SendOutcome {
        let mut control = self.control.borrow_mut();
        let Some(prepared) = control.state.prepare(data) else {
            return SendOutcome::RetryOrdinary;
        };
        if let Some((first, last, copied)) = completion_before_registration {
            control.apply_completion(Completion {
                first,
                last,
                copied,
            });
        }
        control.state.finish_submission(prepared, result)
    }

    #[cfg(test)]
    pub(crate) fn write_after_ready_for_test(
        &mut self,
        data: &BufQueue,
        zero_copy: bool,
        results: impl IntoIterator<Item = io::Result<usize>>,
    ) -> (SendOutcome, Vec<bool>) {
        let mut results = results.into_iter();
        let mut calls = Vec::new();
        let outcome = self.write_after_ready(data, zero_copy, |use_zero_copy| {
            calls.push(use_zero_copy);
            results.next().expect("each scripted send has a result")
        });
        assert!(
            results.next().is_none(),
            "all scripted send results are used"
        );
        (outcome, calls)
    }

    #[cfg(test)]
    pub(crate) fn complete_for_test(&mut self, first: u32, last: u32, copied: bool) -> usize {
        self.control.borrow_mut().apply_completion(Completion {
            first,
            last,
            copied,
        })
    }

    #[cfg(test)]
    pub(crate) fn set_next_cookie_for_test(&mut self, cookie: u32) {
        self.control.borrow_mut().state.next_cookie = cookie;
    }

    #[cfg(test)]
    pub(crate) fn next_cookie_for_test(&self) -> u32 {
        self.control.borrow().state.next_cookie
    }

    #[cfg(test)]
    pub(crate) fn pending_count_for_test(&self) -> usize {
        self.control.borrow().state.pending.len()
    }

    #[cfg(test)]
    pub(crate) fn pending_segments_for_test(&self, cookie: u32) -> Vec<(*const u8, usize)> {
        self.control
            .borrow()
            .state
            .pending
            .get(&cookie)
            .into_iter()
            .flat_map(|loan| loan.data.segments())
            .map(|segment| (segment.as_ptr(), segment.len()))
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn is_enabled_for_test(&self) -> bool {
        self.control.borrow().state.policy == SubmissionPolicy::Enabled
    }

    #[cfg(test)]
    pub(crate) fn completion_owner_for_test(&self) -> usize {
        Rc::as_ptr(&self.control) as usize
    }

    #[cfg(test)]
    pub(crate) fn abandon_prepared_send_for_test(&mut self, data: &BufQueue) {
        let mut control = self.control.borrow_mut();
        let prepared = control
            .state
            .prepare(data)
            .expect("test controller is enabled and its cookie is free");
        control.state.pending.remove(&prepared.cookie);
        control.state.preparing.remove(&prepared.cookie);
        drop(prepared);
    }

    #[cfg(test)]
    pub(crate) fn is_terminal_for_test(&self) -> bool {
        self.control.borrow().terminal
    }

    #[cfg(test)]
    pub(crate) fn is_quarantined_for_test(pointer: *const u8) -> bool {
        QUARANTINED_LOANS
            .get_or_init(|| Mutex::new(Vec::new()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .flat_map(|loan| loan.data.segments())
            .any(|segment| segment.as_ptr() == pointer)
    }

    #[cfg(all(test, target_os = "linux"))]
    pub(crate) fn drain_malformed_for_test(&mut self, truncated: bool) {
        let (control, bytes) = completion_control_for_test(0, 5, 0, 0, 0);
        let malformed = if truncated {
            parse_completion_control(&control, bytes, libc::MSG_CTRUNC)
        } else {
            parse_completion_control(&control, std::mem::size_of::<libc::cmsghdr>(), 0)
        };
        let mut result = Some(malformed);
        self.control.borrow_mut().drain_with(|| {
            result
                .take()
                .unwrap_or_else(|| Err(io::ErrorKind::WouldBlock.into()))
        });
    }

    #[cfg(all(test, target_os = "linux"))]
    pub(crate) fn completion_parser_validation_for_test() -> bool {
        let (valid, bytes) = completion_control_for_test(0, 5, 1, 7, 9);
        let valid = parse_completion_control(&valid, bytes, 0)
            .is_ok_and(|value| value.len() == 1 && value[0].first == 7 && value[0].last == 9);
        let (wrong_origin, bytes) = completion_control_for_test(0, 4, 0, 0, 0);
        let wrong_origin =
            parse_completion_control(&wrong_origin, bytes, 0).is_ok_and(|value| value.is_empty());
        let (bad_errno, bytes) = completion_control_for_test(1, 5, 0, 0, 0);
        let bad_errno = parse_completion_control(&bad_errno, bytes, 0).is_err();
        let (bad_length, _) = completion_control_for_test(0, 5, 0, 0, 0);
        let bad_length =
            parse_completion_control(&bad_length, std::mem::size_of::<libc::cmsghdr>(), 0).is_err();
        let truncated =
            parse_completion_control(&valid_control(), bytes, libc::MSG_CTRUNC).is_err();
        valid && wrong_origin && bad_errno && bad_length && truncated
    }

    #[cfg(test)]
    pub(crate) fn inherited_enable_fallback_for_test() -> (bool, usize, usize) {
        let mut set_calls = 0;
        let mut get_calls = 0;
        let enabled = enable_socket_zero_copy_with(
            || {
                set_calls += 1;
                if set_calls == 1 {
                    Err(io::ErrorKind::Interrupted.into())
                } else {
                    Err(io::Error::from_raw_os_error(libc::EOPNOTSUPP))
                }
            },
            || {
                get_calls += 1;
                Ok(true)
            },
        );
        (enabled, set_calls, get_calls)
    }
}

impl Drop for TcpZeroCopy {
    fn drop(&mut self) {
        self.force_teardown();
    }
}

/// Writes `data` as `TcpZeroCopy::poll_write` does without zero copy, for a
/// stream that holds no zero-copy state.
pub(crate) fn poll_write_ordinary(
    stream: &tokio::net::TcpStream,
    context: &mut Context<'_>,
    data: &BufQueue,
) -> Poll<io::Result<usize>> {
    poll_send(
        || stream.poll_write_ready(context),
        || ordinary_outcome(try_send(stream, data, false)),
    )
}

fn poll_send(
    mut ready: impl FnMut() -> Poll<io::Result<()>>,
    mut write: impl FnMut() -> SendOutcome,
) -> Poll<io::Result<usize>> {
    loop {
        match ready() {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => return Poll::Pending,
        }
        match write() {
            SendOutcome::Written(written) => return Poll::Ready(Ok(written)),
            // try_io cleared readiness: poll again to register the waker.
            SendOutcome::Wait => continue,
            SendOutcome::Error(error) => return Poll::Ready(Err(error)),
            SendOutcome::RetryOrdinary => {
                unreachable!("the write decision consumes its ordinary retry")
            }
        }
    }
}

fn ordinary_outcome(result: io::Result<usize>) -> SendOutcome {
    match result {
        Ok(0) => SendOutcome::Error(io::Error::new(
            io::ErrorKind::WriteZero,
            "TCP write made no progress",
        )),
        Ok(written) => SendOutcome::Written(written),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => SendOutcome::Wait,
        Err(error) => SendOutcome::Error(error),
    }
}

fn try_send(stream: &tokio::net::TcpStream, data: &BufQueue, zero_copy: bool) -> io::Result<usize> {
    #[cfg(target_os = "linux")]
    return stream.try_io(tokio::io::Interest::WRITABLE, || {
        send_queue(stream, data, zero_copy)
    });

    #[cfg(not(target_os = "linux"))]
    {
        debug_assert!(!zero_copy);
        stream.try_write_vectored(&data.iovecs().take(IOV_MAX).collect::<Vec<_>>())
    }
}

#[cfg(target_os = "linux")]
async fn completion_pump(
    control: Weak<RefCell<CompletionControl>>,
    stop: tokio::sync::watch::Receiver<bool>,
) {
    completion_pump_with(stop, || drain_when_ready(&control)).await;
}

#[cfg(target_os = "linux")]
async fn drain_when_ready(control: &Weak<RefCell<CompletionControl>>) -> Option<DrainOutcome> {
    let completion = control.upgrade()?.borrow().completion.clone()?;
    let ready = completion.ready(tokio::io::Interest::ERROR).await;
    let Ok(mut ready) = ready else {
        if let Some(control) = control.upgrade() {
            control.borrow_mut().force_teardown();
        }
        return None;
    };
    let control = control.upgrade()?;
    let drain = control.borrow_mut().drain_nonblocking();
    if !matches!(drain, Some(DrainOutcome::More)) {
        ready.clear_ready();
    }
    drain
}

#[cfg(target_os = "linux")]
async fn completion_pump_with<F, D>(mut stop: tokio::sync::watch::Receiver<bool>, mut drain: D)
where
    F: Future<Output = Option<DrainOutcome>>,
    D: FnMut() -> F,
{
    loop {
        if *stop.borrow() {
            return;
        }
        let outcome = tokio::select! {
            biased;
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    return;
                }
                continue;
            }
            outcome = drain() => outcome,
        };
        match outcome {
            Some(DrainOutcome::Drained(0)) => {
                // ERROR readiness may recur before a completion is available.
                // Keep loans live and bound retries without starving shutdown.
                tokio::select! {
                    biased;
                    _ = stop.changed() => {}
                    _ = tokio::time::sleep(std::time::Duration::from_millis(1)) => {}
                }
            }
            Some(DrainOutcome::Drained(_)) => {}
            Some(DrainOutcome::More) => tokio::task::yield_now().await,
            None => return,
        }
    }
}

#[cfg(target_os = "linux")]
fn enable_socket_zero_copy(descriptor: i32) -> bool {
    enable_socket_zero_copy_with(
        || {
            let enabled: libc::c_int = 1;
            // SAFETY: `descriptor` names the caller's live TCP socket, and
            // `enabled` remains initialized and readable for its exact reported
            // length during this synchronous syscall.
            let result = unsafe {
                libc::setsockopt(
                    descriptor,
                    libc::SOL_SOCKET,
                    libc::SO_ZEROCOPY,
                    std::ptr::from_ref(&enabled).cast(),
                    std::mem::size_of_val(&enabled) as libc::socklen_t,
                )
            };
            (result == 0)
                .then_some(())
                .ok_or_else(io::Error::last_os_error)
        },
        || {
            let mut inherited: libc::c_int = 0;
            let mut length = std::mem::size_of_val(&inherited) as libc::socklen_t;
            // SAFETY: `descriptor` names the caller's live TCP socket.
            // `inherited` and `length` are initialized writable outputs of the
            // exact sizes supplied and remain live throughout this syscall.
            let result = unsafe {
                libc::getsockopt(
                    descriptor,
                    libc::SOL_SOCKET,
                    libc::SO_ZEROCOPY,
                    std::ptr::from_mut(&mut inherited).cast(),
                    &mut length,
                )
            };
            if result == 0 {
                Ok(length as usize == std::mem::size_of_val(&inherited) && inherited != 0)
            } else {
                Err(io::Error::last_os_error())
            }
        },
    )
}

fn enable_socket_zero_copy_with(
    mut set: impl FnMut() -> io::Result<()>,
    mut inherited: impl FnMut() -> io::Result<bool>,
) -> bool {
    loop {
        match set() {
            Ok(()) => return true,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => break,
        }
    }
    loop {
        match inherited() {
            Ok(enabled) => return enabled,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => return false,
        }
    }
}

#[cfg(target_os = "linux")]
fn completion_descriptor(
    descriptor: i32,
) -> Option<tokio::io::unix::AsyncFd<std::os::fd::OwnedFd>> {
    use std::os::fd::BorrowedFd;

    // SAFETY: the caller keeps the stream owning `descriptor` alive throughout
    // this function. The borrow is used only to create an independent owned fd.
    let borrowed = unsafe { BorrowedFd::borrow_raw(descriptor) };
    let duplicate = borrowed.try_clone_to_owned().ok()?;
    // SAFETY: `duplicate` is a freshly cloned `OwnedFd`, so it is a valid
    // open descriptor. Ownership moves into the `AsyncFd`, keeping it open
    // and unmodified (and `as_raw_fd` stable) for the `AsyncFd` lifetime.
    unsafe {
        tokio::io::unix::AsyncFd::register_with_interest(duplicate, tokio::io::Interest::ERROR).ok()
    }
}

#[derive(Clone, Copy)]
struct Completion {
    first: u32,
    last: u32,
    copied: bool,
}

#[cfg(all(test, target_os = "linux"))]
std::thread_local! {
    static SEND_IOVEC_ARRAYS: Cell<usize> = const { Cell::new(0) };
}

#[cfg(all(test, target_os = "linux"))]
fn record_send_iovec_array() {
    SEND_IOVEC_ARRAYS.with(|count| count.set(count.get() + 1));
}

/// Heap iovec arrays that `send_queue` built on this thread.
#[cfg(all(test, target_os = "linux"))]
pub(crate) fn send_iovec_arrays_for_test() -> usize {
    SEND_IOVEC_ARRAYS.with(Cell::get)
}

#[cfg(target_os = "linux")]
fn send_queue(
    stream: &tokio::net::TcpStream,
    data: &BufQueue,
    zero_copy: bool,
) -> io::Result<usize> {
    use std::os::fd::AsRawFd;

    let count = data.segment_count().min(IOV_MAX);
    let mut inline = [MaybeUninit::<libc::iovec>::uninit(); INLINE_IOVECS];
    let mut spilled = Vec::<libc::iovec>::new();
    let vectors = if count <= INLINE_IOVECS {
        &mut inline[..count]
    } else {
        #[cfg(test)]
        record_send_iovec_array();
        spilled.reserve_exact(count);
        &mut spilled.spare_capacity_mut()[..count]
    };
    let mut filled = 0;
    for (vector, segment) in vectors.iter_mut().zip(data.segments()) {
        vector.write(libc::iovec {
            iov_base: segment.as_ptr().cast_mut().cast(),
            iov_len: segment.len(),
        });
        filled += 1;
    }
    // SAFETY: all-zero is a valid `msghdr`; every pointer field is either null
    // or replaced below with storage that remains borrowed for the syscall.
    let mut message = unsafe { std::mem::zeroed::<libc::msghdr>() };
    message.msg_iov = vectors.as_mut_ptr().cast::<libc::iovec>();
    message.msg_iovlen = filled;
    let flags =
        libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL | if zero_copy { libc::MSG_ZEROCOPY } else { 0 };
    // SAFETY: `stream` owns the live descriptor. `msg_iov` names the `filled`
    // iovecs written above, whose storage outlives this syscall. Each points
    // into an immutable `BufQueue` segment retained by the caller for this
    // syscall. For `MSG_ZEROCOPY`, `ZeroCopyState::prepare` has additionally
    // installed a shared clone before this call; success retains exactly the
    // accepted prefix until `CompletionControl` validates its kernel cookie,
    // while every failure path removes the unsubmitted reservation.
    let result = unsafe { libc::sendmsg(stream.as_raw_fd(), &message, flags) };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result as usize)
    }
}

#[cfg(target_os = "linux")]
fn receive_completions(descriptor: i32) -> io::Result<Vec<Completion>> {
    let mut byte = 0u8;
    let mut vector = libc::iovec {
        iov_base: std::ptr::from_mut(&mut byte).cast(),
        iov_len: 1,
    };
    let mut control = [0usize; 64];
    // SAFETY: all-zero is a valid `msghdr`; pointer fields are populated with
    // the live stack storage above before entering the kernel.
    let mut message = unsafe { std::mem::zeroed::<libc::msghdr>() };
    message.msg_iov = std::ptr::from_mut(&mut vector);
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast();
    message.msg_controllen = std::mem::size_of_val(&control);
    // SAFETY: the descriptor is owned by the controller, and every mutable
    // pointer in `message` names initialized stack storage of the stated size.
    let result = unsafe {
        libc::recvmsg(
            descriptor,
            &mut message,
            libc::MSG_ERRQUEUE | libc::MSG_DONTWAIT,
        )
    };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    parse_completion_control(&control, message.msg_controllen, message.msg_flags)
}

#[cfg(target_os = "linux")]
fn parse_completion_control(
    control: &[usize],
    bytes: usize,
    message_flags: libc::c_int,
) -> io::Result<Vec<Completion>> {
    const SO_EE_ORIGIN_ZEROCOPY: u8 = 5;
    const SO_EE_CODE_ZEROCOPY_COPIED: u8 = 1;

    if message_flags & libc::MSG_CTRUNC != 0 || bytes > std::mem::size_of_val(control) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "truncated TCP zero-copy completion control data",
        ));
    }

    let base = control.as_ptr().cast::<u8>();
    let header_size = std::mem::size_of::<libc::cmsghdr>();
    let payload_offset = cmsg_align(header_size).ok_or_else(malformed_completion)?;
    let required = payload_offset
        .checked_add(std::mem::size_of::<libc::sock_extended_err>())
        .ok_or_else(malformed_completion)?;
    let mut offset = 0usize;
    let mut completions = Vec::new();
    while offset
        .checked_add(header_size)
        .is_some_and(|end| end <= bytes)
    {
        // SAFETY: the bounds check above proves that a complete header lies in
        // the kernel-populated control buffer. `read_unaligned` avoids imposing
        // an alignment assumption on a peer-controlled cmsg offset.
        let header = unsafe { std::ptr::read_unaligned(base.add(offset).cast::<libc::cmsghdr>()) };
        let length = header.cmsg_len;
        if length < header_size || offset.checked_add(length).is_none_or(|end| end > bytes) {
            return Err(malformed_completion());
        }
        let is_ip_error = (header.cmsg_level == libc::SOL_IP
            && header.cmsg_type == libc::IP_RECVERR)
            || (header.cmsg_level == libc::SOL_IPV6 && header.cmsg_type == libc::IPV6_RECVERR);
        if is_ip_error {
            if length < required {
                return Err(malformed_completion());
            }
            // SAFETY: `required <= length` proves that the complete extended
            // error payload lies in this cmsg, and unaligned reads are allowed.
            let error = unsafe {
                std::ptr::read_unaligned(
                    base.add(offset + payload_offset)
                        .cast::<libc::sock_extended_err>(),
                )
            };
            if error.ee_origin == SO_EE_ORIGIN_ZEROCOPY {
                if error.ee_errno != 0 {
                    return Err(malformed_completion());
                }
                completions.push(Completion {
                    first: error.ee_info,
                    last: error.ee_data,
                    copied: error.ee_code & SO_EE_CODE_ZEROCOPY_COPIED != 0,
                });
            }
        }
        let Some(next) = cmsg_align(length).and_then(|length| offset.checked_add(length)) else {
            return Err(malformed_completion());
        };
        if next <= offset {
            return Err(malformed_completion());
        }
        offset = next;
    }
    Ok(completions)
}

#[cfg(target_os = "linux")]
fn cmsg_align(length: usize) -> Option<usize> {
    let alignment = std::mem::size_of::<usize>();
    length
        .checked_add(alignment - 1)
        .map(|length| length & !(alignment - 1))
}

#[cfg(target_os = "linux")]
fn malformed_completion() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "malformed TCP zero-copy completion control data",
    )
}

#[cfg(all(test, target_os = "linux"))]
fn completion_control_for_test(
    errno: u32,
    origin: u8,
    code: u8,
    first: u32,
    last: u32,
) -> ([usize; 8], usize) {
    let mut control = [0usize; 8];
    let header_size = std::mem::size_of::<libc::cmsghdr>();
    let payload_offset = cmsg_align(header_size).expect("cmsg header alignment does not overflow");
    let bytes = payload_offset + std::mem::size_of::<libc::sock_extended_err>();
    let header = libc::cmsghdr {
        cmsg_len: bytes,
        cmsg_level: libc::SOL_IP,
        cmsg_type: libc::IP_RECVERR,
    };
    let error = libc::sock_extended_err {
        ee_errno: errno,
        ee_origin: origin,
        ee_type: 0,
        ee_code: code,
        ee_pad: 0,
        ee_info: first,
        ee_data: last,
    };
    let base = control.as_mut_ptr().cast::<u8>();
    // SAFETY: the fixed test buffer is usize-aligned and large enough for the
    // complete native header and error payload at the validated offsets.
    unsafe {
        std::ptr::write_unaligned(base.cast::<libc::cmsghdr>(), header);
        std::ptr::write_unaligned(
            base.add(payload_offset).cast::<libc::sock_extended_err>(),
            error,
        );
    }
    (control, bytes)
}

#[cfg(all(test, target_os = "linux"))]
fn valid_control() -> [usize; 8] {
    completion_control_for_test(0, 5, 0, 0, 0).0
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::task::Waker;
    use std::time::Duration;

    use super::*;

    /// A budget that lives for the rest of the process, as the shared one
    /// does. `BUDGETS` keeps it reachable so that leak checkers accept it.
    fn private_budget(limit: usize) -> &'static LoanBudget {
        static BUDGETS: Mutex<Vec<&'static LoanBudget>> = Mutex::new(Vec::new());
        let budget = Box::leak(Box::new(LoanBudget::new(limit)));
        BUDGETS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(budget);
        budget
    }

    fn controller(limit: usize) -> (TcpZeroCopy, &'static LoanBudget) {
        let budget = private_budget(limit);
        let mut controller = TcpZeroCopy::new();
        controller.control.borrow_mut().state.budget = budget;
        assert!(controller.enable_for_test(1));
        (controller, budget)
    }

    struct WakeCount(AtomicUsize);

    impl std::task::Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn full_capacity_is_reserved_until_completion() {
        let mut backing = Vec::with_capacity(8192);
        backing.extend_from_slice(b"tiny");
        let capacity = backing.capacity();
        let data = BufQueue::from_vec(backing, 4).unwrap();
        let charge = capacity + 128 + 1024;
        let (mut controller, budget) = controller(charge);
        assert!(matches!(
            controller.submit_for_test(&data, Ok(1), None),
            SendOutcome::Written(1)
        ));
        assert_eq!(budget.used.load(Ordering::Acquire), charge);
        assert_eq!(controller.pending_segments_for_test(0)[0].1, 1);
        let pointer = data.front().unwrap().as_ptr();
        let (outcome, calls) = controller.write_after_ready_for_test(&data, true, [Ok(4)]);
        assert!(matches!(outcome, SendOutcome::Written(4)));
        assert_eq!(calls, [false]);
        assert_eq!(data.front().unwrap().as_ptr(), pointer);
        assert_eq!(controller.complete_for_test(1, 1, false), 0);
        assert_eq!(budget.used.load(Ordering::Acquire), charge);
        assert_eq!(controller.complete_for_test(0, 0, false), 1);
        assert_eq!(controller.complete_for_test(0, 0, false), 0);
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
        assert!(matches!(
            controller.submit_for_test(&data, Ok(4), None),
            SendOutcome::Written(4)
        ));
        assert_eq!(controller.complete_for_test(1, 1, false), 1);
    }

    #[test]
    fn unknown_backing_falls_back_without_copying() {
        let backing = BufQueue::from_vec(vec![b'x'; 8192], 8192).unwrap();
        let bytes = backing.front().unwrap().slice(4096..4097);
        let pointer = bytes.as_ptr();
        let data = BufQueue::from_bytes(bytes, 1).unwrap();
        let (mut controller, budget) = controller(LOAN_BUDGET);
        let (outcome, calls) = controller.write_after_ready_for_test(&data, true, [Ok(1)]);
        assert!(matches!(outcome, SendOutcome::Written(1)));
        assert_eq!(calls, [false]);
        assert_eq!(data.front().unwrap().as_ptr(), pointer);
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
        assert_eq!(controller.pending_count_for_test(), 0);
    }

    #[test]
    fn unsubmitted_and_already_completed_loans_release_budget() {
        let data = BufQueue::copy_from_slice(b"loan", 4).unwrap();
        for result in [
            Ok(0),
            Err(io::ErrorKind::WouldBlock.into()),
            Err(io::Error::from_raw_os_error(libc::ENOBUFS)),
            Err(io::Error::from_raw_os_error(libc::EPIPE)),
        ] {
            let (mut controller, budget) = controller(4096);
            controller.submit_for_test(&data, result, None);
            assert_eq!(budget.used.load(Ordering::Acquire), 0);
            assert_eq!(controller.pending_count_for_test(), 0);
        }
        let (mut controller, budget) = controller(4096);
        controller.submit_for_test(&data, Ok(4), Some((0, 0, false)));
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
        assert_eq!(controller.pending_count_for_test(), 0);
    }

    #[test]
    fn quarantine_and_unwind_guards_keep_budget_reserved() {
        for path in 0..3 {
            let data = BufQueue::copy_from_slice(b"loan", 4).unwrap();
            let pointer = data.front().unwrap().as_ptr();
            let (mut controller, budget) = controller(1156);
            match path {
                0 => {
                    controller.submit_for_test(&data, Ok(4), None);
                    controller.force_teardown();
                }
                1 => controller.abandon_prepared_send_for_test(&data),
                _ => {
                    assert!(matches!(
                        controller.submit_for_test(&data, Ok(5), None),
                        SendOutcome::Error(_)
                    ));
                    assert_eq!(controller.pending_count_for_test(), 0);
                }
            }
            assert!(TcpZeroCopy::is_quarantined_for_test(pointer));
            drop(controller);
            drop(data);
            assert_eq!(budget.used.load(Ordering::Acquire), 1156);
            let next = BufQueue::copy_from_slice(b"next", 4).unwrap();
            assert!(budget.reserve(&next).is_none());
        }
    }

    #[test]
    fn metadata_and_overflow_also_bound_admission() {
        let mut data = BufQueue::new(4);
        for _ in 0..4 {
            data.push_copy(b"x").unwrap();
        }
        let (mut controller, budget) = controller(4 + 128 * 4 + 1024 - 1);
        assert!(matches!(
            controller.submit_for_test(&data, Ok(4), None),
            SendOutcome::RetryOrdinary
        ));
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
        let budget = private_budget(usize::MAX);
        budget.used.store(usize::MAX, Ordering::Release);
        assert!(budget.reserve(&data).is_none());
        assert_eq!(budget.used.load(Ordering::Acquire), usize::MAX);
    }

    #[test]
    fn would_block_repolls_readiness_before_returning_pending() {
        for mode in 0..3 {
            let (mut controller, budget) = controller(4096);
            let data = BufQueue::copy_from_slice(b"again", 5).unwrap();
            let mut ready_polls = 0;
            let mut sends = Vec::new();
            let outcome = controller.poll_write_with(
                &data,
                mode != 0,
                || {
                    ready_polls += 1;
                    if ready_polls == 1 {
                        Poll::Ready(Ok(()))
                    } else {
                        Poll::Pending
                    }
                },
                |zero_copy| {
                    sends.push(zero_copy);
                    if mode == 2 && sends.len() == 1 {
                        Err(io::Error::from_raw_os_error(libc::ENOBUFS))
                    } else {
                        Err(io::ErrorKind::WouldBlock.into())
                    }
                },
            );
            assert!(outcome.is_pending());
            assert_eq!(
                ready_polls, 2,
                "WouldBlock re-polls readiness, and only that re-poll's Pending is returned"
            );
            assert_eq!(
                sends,
                if mode == 2 {
                    vec![true, false]
                } else {
                    vec![mode != 0]
                }
            );
            assert_eq!(budget.used.load(Ordering::Acquire), 0);
            assert_eq!(controller.next_cookie_for_test(), 0);
            assert!(matches!(
                controller.poll_write_with(&data, false, || Poll::Ready(Ok(())), |_| Ok(5)),
                Poll::Ready(Ok(5))
            ));
        }
    }

    /// `send_queue`'s iovec arrays, over a real loopback socket.
    #[cfg(target_os = "linux")]
    mod send_iovecs {
        use super::*;

        /// A connected loopback pair: the Tokio writer and its blocking peer.
        async fn loopback() -> (tokio::net::TcpStream, std::net::TcpStream) {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let stream = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (peer, _) = listener.accept().unwrap();
            (stream, peer)
        }

        /// `count` segments of 8 bytes each; segment `index` holds bytes of `index % 251`.
        fn eight_byte_segments(count: usize) -> BufQueue {
            let mut data = BufQueue::new(count * 8);
            for index in 0..count {
                data.push_copy(&[(index % 251) as u8; 8]).unwrap();
            }
            data
        }

        /// Writes `data` with one `poll_write_ordinary`, as an owned TCP write without zero copy
        /// does, and returns the bytes that one `sendmsg` accepted.
        fn send_once(stream: &tokio::net::TcpStream, data: &BufQueue) -> usize {
            let mut context = Context::from_waker(Waker::noop());
            match poll_write_ordinary(stream, &mut context, data) {
                Poll::Ready(Ok(written)) => written,
                outcome => panic!("a fresh loopback socket accepts the send: {outcome:?}"),
            }
        }

        fn receive_exactly(peer: &mut std::net::TcpStream, length: usize) -> Vec<u8> {
            let mut received = vec![0; length];
            peer.read_exact(&mut received).unwrap();
            received
        }

        #[tokio::test(flavor = "current_thread")]
        async fn a_send_of_up_to_64_segments_builds_no_heap_iovec_array() {
            let (stream, mut peer) = loopback().await;
            for count in [1, 3, 64] {
                let data = eight_byte_segments(count);
                let arrays = send_iovec_arrays_for_test();
                assert_eq!(send_once(&stream, &data), count * 8, "{count} segments");
                assert_eq!(
                    send_iovec_arrays_for_test(),
                    arrays,
                    "a send of {count} segments keeps its iovecs on the stack"
                );
                let expected = (0..count)
                    .flat_map(|index| [(index % 251) as u8; 8])
                    .collect::<Vec<_>>();
                assert_eq!(receive_exactly(&mut peer, count * 8), expected);
            }
        }

        #[tokio::test(flavor = "current_thread")]
        async fn a_send_of_more_than_64_segments_spills_and_sends_at_most_iov_max() {
            let (stream, mut peer) = loopback().await;
            for (count, sent) in [(65, 65), (1_100, IOV_MAX)] {
                let data = eight_byte_segments(count);
                let arrays = send_iovec_arrays_for_test();
                assert_eq!(
                    send_once(&stream, &data),
                    sent * 8,
                    "one sendmsg passes the first {sent} of {count} segments"
                );
                assert_eq!(
                    send_iovec_arrays_for_test(),
                    arrays + 1,
                    "a send of {count} segments builds one heap iovec array"
                );
                let expected = (0..sent)
                    .flat_map(|index| [(index % 251) as u8; 8])
                    .collect::<Vec<_>>();
                assert_eq!(receive_exactly(&mut peer, sent * 8), expected);
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn poll_write_registers_the_callers_waker_when_the_socket_fills() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let stream = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (mut peer, _) = listener.accept().unwrap();
        peer.set_nonblocking(true).unwrap();
        let mut controller = TcpZeroCopy::new();
        let data = BufQueue::copy_from_slice(&[0; 64 * 1024], 64 * 1024).unwrap();
        let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wakes));
        let mut context = Context::from_waker(&waker);

        // Filling the socket can take more readiness polls than tokio's
        // cooperative budget, whose exhaustion would wake the waker itself.
        tokio::task::unconstrained(async {
            let mut written = 0_usize;
            loop {
                match controller.poll_write(&stream, &mut context, &data, false) {
                    Poll::Ready(Ok(progress)) => written += progress,
                    Poll::Ready(Err(error)) => panic!("loopback write failed: {error}"),
                    Poll::Pending => break,
                }
                assert!(
                    written < 1 << 30,
                    "a loopback socket whose peer never reads eventually fills"
                );
            }
        })
        .await;
        assert_eq!(
            wakes.0.load(Ordering::SeqCst),
            0,
            "nothing wakes the writer while the peer has not read"
        );

        let mut buffer = vec![0; 256 * 1024];
        tokio::time::timeout(Duration::from_secs(10), async {
            while wakes.0.load(Ordering::SeqCst) == 0 {
                loop {
                    match peer.read(&mut buffer) {
                        Ok(0) => panic!("the writer keeps its side open"),
                        Ok(_) => {}
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                        Err(error) => panic!("peer read failed: {error}"),
                    }
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("draining the peer wakes the waker whose poll_write returned Pending");
    }

    #[test]
    fn completion_drain_yields_after_a_bounded_batch() {
        for interrupted in [false, true] {
            let mut control = CompletionControl::new();
            let mut calls = 0;
            assert_eq!(
                control.drain_with(|| {
                    calls += 1;
                    if interrupted {
                        Err(io::ErrorKind::Interrupted.into())
                    } else {
                        Ok(Vec::new())
                    }
                }),
                Some(DrainOutcome::More)
            );
            assert_eq!(calls, COMPLETION_BATCH);
            assert_eq!(
                control.drain_with(|| Err(io::ErrorKind::WouldBlock.into())),
                Some(DrainOutcome::Drained(0))
            );
        }
        let mut control = CompletionControl::new();
        let mut calls = 0;
        assert_eq!(
            control.drain_with(|| {
                calls += 1;
                Err(io::ErrorKind::InvalidData.into())
            }),
            None
        );
        assert_eq!(calls, COMPLETION_BATCH);
        assert!(control.terminal);
    }

    #[test]
    fn later_completion_batch_releases_its_reservation() {
        let (controller, budget) = controller(4096);
        let mut control = controller.control.borrow_mut();
        let data = BufQueue::copy_from_slice(b"loan", 4).unwrap();
        let prepared = control.state.prepare(&data).unwrap();
        control.state.finish_submission(prepared, Ok(4));
        let mut received = 0;
        let mut receive = || {
            received += 1;
            if received <= COMPLETION_BATCH {
                Ok(Vec::new())
            } else if received == COMPLETION_BATCH + 1 {
                Ok(vec![Completion {
                    first: 0,
                    last: 0,
                    copied: false,
                }])
            } else {
                Err(io::ErrorKind::WouldBlock.into())
            }
        };
        assert_eq!(control.drain_with(&mut receive), Some(DrainOutcome::More));
        assert_eq!(control.state.pending.len(), 1);
        assert_eq!(budget.used.load(Ordering::Acquire), 1156);
        assert_eq!(
            control.drain_with(&mut receive),
            Some(DrainOutcome::Drained(1))
        );
        assert!(control.state.is_empty());
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
    }
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "current_thread")]
    async fn empty_error_wakes_preserve_loans_until_completion() {
        let (controller, budget) = controller(4096);
        let data = BufQueue::copy_from_slice(b"loan", 4).unwrap();
        {
            let mut control = controller.control.borrow_mut();
            let prepared = control.state.prepare(&data).unwrap();
            control.state.finish_submission(prepared, Ok(4));
        }
        let (_stop, receiver) = tokio::sync::watch::channel(false);
        let mut wakes = 0;
        completion_pump_with(receiver, || {
            wakes += 1;
            let mut control = controller.control.borrow_mut();
            assert!(!control.terminal);
            let outcome = if wakes <= 3 {
                assert_eq!(budget.used.load(Ordering::Acquire), 1156);
                control.drain_with(|| Err(io::ErrorKind::WouldBlock.into()))
            } else if wakes == 4 {
                let mut completion = Some(vec![Completion {
                    first: 0,
                    last: 0,
                    copied: false,
                }]);
                control.drain_with(|| completion.take().ok_or(io::ErrorKind::WouldBlock.into()))
            } else {
                None
            };
            std::future::ready(outcome)
        })
        .await;
        assert_eq!(wakes, 5);
        assert!(controller.control.borrow().state.is_empty());
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "current_thread")]
    async fn stop_interrupts_empty_error_wake_delay() {
        let (stop, receiver) = tokio::sync::watch::channel(false);
        let mut pump = Box::pin(completion_pump_with(receiver, || {
            stop.send(true).unwrap();
            std::future::ready(Some(DrainOutcome::Drained(0)))
        }));
        let mut context = Context::from_waker(std::task::Waker::noop());
        assert!(pump.as_mut().poll(&mut context).is_ready());
    }
}
